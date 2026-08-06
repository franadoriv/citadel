//! PostgreSQL/CockroachDB durable GameScript revision adapter.
//!
//! Every mutating contract method runs inside one database transaction, so the
//! state change and its audit/outbox rows commit or disappear together. The
//! revision id is the content hash and the table's primary key:
//! `INSERT ... ON CONFLICT DO NOTHING` plus a locked re-read makes concurrent
//! submissions of identical content race safely to one row, and makes the
//! dedupe conflict with a concurrent `prune_revisions` delete of that row
//! (see `SELECT_REVISION_FOR_UPDATE_SQL`) instead of committing against a
//! revision that no longer exists.
//!
//! CockroachDB runs its transactions at `SERIALIZABLE` and asks the client to
//! retry a transaction that loses a write race. Pooled calls here are
//! replayable repository transactions, so the adapter retries them with the
//! same bounded policy as the wallet adapter. Unit-of-work callers (none
//! today; GameScript is a standalone pooled feature) would own their
//! transaction and receive the retryable error instead.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgConnection;
use sqlx::postgres::PgRow;

use crate::error::{AppError, AppResult};
use crate::repository::gamescript::{
    AUDIT_ACTION_ACTIVATE, AUDIT_ACTION_PIN, AUDIT_ACTION_SUBMIT, AUDIT_ACTION_UNPIN,
    CreateGameScriptDraftRequest, GameScriptActivation, GameScriptAuditContext,
    GameScriptAuditRecord, GameScriptDiagnostic, GameScriptDiagnosticSeverity, GameScriptDraft,
    GameScriptLimits, GameScriptOutboxKind, GameScriptOutboxRecord, GameScriptRepository,
    GameScriptRevision, GameScriptSubmission, UpdateGameScriptDraftRequest,
    activation_audit_details, draft_not_found, gamescript_revision_content_hash,
    language_from_token, revision_not_found, submit_audit_details, validate_create_draft,
    validate_limit, validate_source,
};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis};

const SELECT_DRAFT_SQL: &str = "SELECT * FROM gamescript_drafts WHERE draft_id = $1";
const SELECT_REVISION_SQL: &str = "SELECT * FROM gamescript_revisions WHERE revision_id = $1";

/// Locked dedupe re-read for `submit_draft`: `ON CONFLICT DO NOTHING` takes no
/// lock on the *existing* row, so an unlocked re-read could race a concurrent
/// `prune_revisions` delete and return a revision that no longer exists at
/// commit time. `FOR UPDATE` (the module-wide row-lock idiom) serializes the
/// dedupe with that delete; a prune that already committed leaves this read
/// empty and the submit loop re-inserts the content as a fresh revision.
const SELECT_REVISION_FOR_UPDATE_SQL: &str =
    "SELECT * FROM gamescript_revisions WHERE revision_id = $1 FOR UPDATE";

/// Bound on insert → locked-re-read alternation in `submit_draft`. Each extra
/// iteration needs another prune (or identical submit) to land in the
/// statement gap, so two passes settle every practical race.
const SUBMIT_DEDUPE_ATTEMPTS: usize = 8;

/// Counter upsert mirroring the chat access-epoch idiom: the first activation
/// of a scope creates the row at 1; later ones increment atomically.
const ALLOCATE_GENERATION_SQL: &str = "\
INSERT INTO gamescript_activation_generations (scope, current_generation) VALUES ($1, 1) \
ON CONFLICT (scope) DO UPDATE SET \
current_generation = gamescript_activation_generations.current_generation + 1 \
RETURNING current_generation";

pub struct PgGameScriptRepository {
    executor: PgExecutor,
    limits: GameScriptLimits,
}

impl PgGameScriptRepository {
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self {
            executor,
            limits: GameScriptLimits::default(),
        }
    }

    /// Run one replayable repository transaction.
    ///
    /// The closure must be safe to re-execute from scratch: on CockroachDB a
    /// serialization loser is rolled back and replayed with bounded backoff
    /// (same policy as `pg::wallet`).
    async fn transaction<T, F>(&self, work: F) -> AppResult<T>
    where
        F: for<'a> Fn(
                &'a mut PgConnection,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = AppResult<T>> + Send + 'a>,
            > + Send
            + Sync,
    {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                const MAX_ATTEMPTS: usize = 8;
                for attempt in 0..MAX_ATTEMPTS {
                    let mut tx = pool.begin().await.map_err(db_err)?;
                    match work(&mut tx).await {
                        Ok(value) => match tx.commit().await.map_err(db_err) {
                            Ok(()) => return Ok(value),
                            Err(error)
                                if cockroach_retryable(&error) && attempt + 1 < MAX_ATTEMPTS =>
                            {
                                cockroach_retry_backoff(attempt).await;
                            }
                            Err(error) => return Err(error),
                        },
                        Err(error) => {
                            let _ = tx.rollback().await;
                            if cockroach_retryable(&error) && attempt + 1 < MAX_ATTEMPTS {
                                cockroach_retry_backoff(attempt).await;
                            } else {
                                return Err(error);
                            }
                        }
                    }
                }
                unreachable!("the bounded CockroachDB retry loop always returns")
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("database transaction is already closed"))?;
                work(tx).await
            }
        }
    }
}

fn cockroach_retryable(error: &AppError) -> bool {
    error.log_detail().is_some_and(|detail| {
        detail.contains("restart transaction")
            || detail.contains("TransactionRetryWithProtoRefreshError")
    })
}

async fn cockroach_retry_backoff(attempt: usize) {
    let delay_ms = 1_u64 << attempt.min(6);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

fn parse_draft(row: &PgRow) -> AppResult<GameScriptDraft> {
    Ok(GameScriptDraft {
        draft_id: get(row, "draft_id")?,
        language: language_from_token(&get::<String>(row, "language")?)?,
        entrypoint: get(row, "entrypoint")?,
        content: get(row, "content")?,
        created_by: get(row, "created_by")?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
        updated_at: millis_to_ts(get(row, "updated_at_unix_ms")?)?,
    })
}

fn parse_revision(row: &PgRow) -> AppResult<GameScriptRevision> {
    Ok(GameScriptRevision {
        revision_id: get(row, "revision_id")?,
        language: language_from_token(&get::<String>(row, "language")?)?,
        entrypoint: get(row, "entrypoint")?,
        content: get(row, "content")?,
        size_bytes: u64::try_from(get::<i64>(row, "size_bytes")?)
            .map_err(|_| AppError::internal("invalid gamescript revision size"))?,
        created_by: get(row, "created_by")?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
    })
}

fn parse_activation(row: &PgRow) -> AppResult<GameScriptActivation> {
    Ok(GameScriptActivation {
        scope: get(row, "scope")?,
        generation: u64::try_from(get::<i64>(row, "generation")?)
            .map_err(|_| AppError::internal("invalid gamescript activation generation"))?,
        revision_id: get(row, "revision_id")?,
        activated_by: get(row, "activated_by")?,
        activated_at: millis_to_ts(get(row, "activated_at_unix_ms")?)?,
    })
}

fn parse_diagnostic(row: &PgRow) -> AppResult<GameScriptDiagnostic> {
    Ok(GameScriptDiagnostic {
        revision_id: get(row, "revision_id")?,
        seq: u64::try_from(get::<i64>(row, "seq")?)
            .map_err(|_| AppError::internal("invalid gamescript diagnostic sequence"))?,
        severity: GameScriptDiagnosticSeverity::from_token(&get::<String>(row, "severity")?)?,
        source: get(row, "source")?,
        message: get(row, "message")?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
    })
}

fn parse_audit(row: &PgRow) -> AppResult<GameScriptAuditRecord> {
    let details: String = get(row, "details")?;
    Ok(GameScriptAuditRecord {
        audit_id: u64::try_from(get::<i64>(row, "audit_id")?)
            .map_err(|_| AppError::internal("invalid gamescript audit id"))?,
        actor: get(row, "actor")?,
        action: get(row, "action")?,
        target: get(row, "target")?,
        details: serde_json::from_str(&details)
            .map_err(|_| AppError::internal("invalid gamescript audit details"))?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
    })
}

fn parse_outbox(row: &PgRow) -> AppResult<GameScriptOutboxRecord> {
    let generation: Option<i64> = get(row, "generation")?;
    Ok(GameScriptOutboxRecord {
        outbox_id: u64::try_from(get::<i64>(row, "outbox_id")?)
            .map_err(|_| AppError::internal("invalid gamescript outbox id"))?,
        kind: GameScriptOutboxKind::from_token(&get::<String>(row, "kind")?)?,
        scope: get(row, "scope")?,
        revision_id: get(row, "revision_id")?,
        generation: generation
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| AppError::internal("invalid gamescript outbox generation"))
            })
            .transpose()?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
    })
}

fn encode_details(details: &GameScriptAuditContext) -> AppResult<String> {
    serde_json::to_string(details)
        .map_err(|_| AppError::internal("failed to encode gamescript audit details"))
}

async fn insert_audit(
    conn: &mut PgConnection,
    actor: &str,
    action: &str,
    target: &str,
    details: &GameScriptAuditContext,
    now: i64,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO gamescript_audit (actor, action, target, details, created_at_unix_ms) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(actor)
    .bind(action)
    .bind(target)
    .bind(encode_details(details)?)
    .bind(now)
    .execute(conn)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn insert_outbox(
    conn: &mut PgConnection,
    kind: GameScriptOutboxKind,
    scope: Option<&str>,
    revision_id: &str,
    generation: Option<i64>,
    now: i64,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO gamescript_outbox (kind, scope, revision_id, generation, \
         created_at_unix_ms) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(kind.as_str())
    .bind(scope)
    .bind(revision_id)
    .bind(generation)
    .bind(now)
    .execute(conn)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn revision_exists(conn: &mut PgConnection, revision_id: &str) -> AppResult<bool> {
    Ok(
        sqlx::query("SELECT 1 FROM gamescript_revisions WHERE revision_id = $1")
            .bind(revision_id)
            .fetch_optional(conn)
            .await
            .map_err(db_err)?
            .is_some(),
    )
}

#[async_trait]
impl GameScriptRepository for PgGameScriptRepository {
    async fn create_draft(
        &self,
        request: CreateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_create_draft(&request, &self.limits)?;
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let request = request.clone();
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO gamescript_drafts (draft_id, language, entrypoint, content, \
                     created_by, created_at_unix_ms, updated_at_unix_ms) \
                     VALUES ($1, $2, $3, $4, $5, $6, $6)",
                )
                .bind(&request.draft_id)
                .bind(request.language.as_str())
                .bind(&request.entrypoint)
                .bind(&request.content)
                .bind(&request.created_by)
                .bind(now)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(GameScriptDraft {
                    draft_id: request.draft_id,
                    language: request.language,
                    entrypoint: request.entrypoint,
                    content: request.content,
                    created_by: request.created_by,
                    created_at: millis_to_ts(now)?,
                    updated_at: millis_to_ts(now)?,
                })
            })
        })
        .await
    }

    async fn update_draft(
        &self,
        draft_id: &str,
        update: UpdateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_source(&update.entrypoint, &update.content, &self.limits)?;
        let draft_id = draft_id.to_owned();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let draft_id = draft_id.clone();
            let update = update.clone();
            Box::pin(async move {
                let row = sqlx::query(SELECT_DRAFT_SQL)
                    .bind(&draft_id)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db_err)?
                    .ok_or_else(|| draft_not_found(&draft_id))?;
                let existing = parse_draft(&row)?;
                sqlx::query(
                    "UPDATE gamescript_drafts SET language = $1, entrypoint = $2, content = $3, \
                     updated_at_unix_ms = $4 WHERE draft_id = $5",
                )
                .bind(update.language.as_str())
                .bind(&update.entrypoint)
                .bind(&update.content)
                .bind(now)
                .bind(&draft_id)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(GameScriptDraft {
                    language: update.language,
                    entrypoint: update.entrypoint,
                    content: update.content,
                    updated_at: millis_to_ts(now)?,
                    ..existing
                })
            })
        })
        .await
    }

    async fn get_draft(&self, draft_id: &str) -> AppResult<Option<GameScriptDraft>> {
        let draft_id = draft_id.to_owned();
        self.transaction(move |conn| {
            let draft_id = draft_id.clone();
            Box::pin(async move {
                sqlx::query(SELECT_DRAFT_SQL)
                    .bind(&draft_id)
                    .fetch_optional(conn)
                    .await
                    .map_err(db_err)?
                    .as_ref()
                    .map(parse_draft)
                    .transpose()
            })
        })
        .await
    }

    async fn list_drafts(&self, limit: usize) -> AppResult<Vec<GameScriptDraft>> {
        validate_limit(limit)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                sqlx::query("SELECT * FROM gamescript_drafts ORDER BY draft_id LIMIT $1")
                    .bind(limit)
                    .fetch_all(conn)
                    .await
                    .map_err(db_err)?
                    .iter()
                    .map(parse_draft)
                    .collect()
            })
        })
        .await
    }

    async fn delete_draft(&self, draft_id: &str) -> AppResult<bool> {
        let draft_id = draft_id.to_owned();
        self.transaction(move |conn| {
            let draft_id = draft_id.clone();
            Box::pin(async move {
                let result = sqlx::query("DELETE FROM gamescript_drafts WHERE draft_id = $1")
                    .bind(&draft_id)
                    .execute(conn)
                    .await
                    .map_err(db_err)?;
                Ok(result.rows_affected() > 0)
            })
        })
        .await
    }

    async fn submit_draft(
        &self,
        draft_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptSubmission> {
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let draft_id = draft_id.to_owned();
        let actor = actor.to_owned();
        let context = context.clone();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let draft_id = draft_id.clone();
            let actor = actor.clone();
            let context = context.clone();
            Box::pin(async move {
                let row = sqlx::query(SELECT_DRAFT_SQL)
                    .bind(&draft_id)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db_err)?
                    .ok_or_else(|| draft_not_found(&draft_id))?;
                let draft = parse_draft(&row)?;
                let revision_id = gamescript_revision_content_hash(
                    draft.language,
                    &draft.entrypoint,
                    &draft.content,
                );
                let size_bytes = i64::try_from(draft.content.len()).map_err(|_| {
                    AppError::validation("gamescript content exceeds the maximum source size")
                })?;
                // The hash primary key deduplicates identical content and
                // resolves the concurrent-submission race to one row. The
                // dedupe re-read is locked (see
                // `SELECT_REVISION_FOR_UPDATE_SQL`); when it comes back empty
                // the conflicting row was pruned in the statement gap and the
                // loop re-runs the insert against the post-prune state.
                let mut resolved = None;
                for _ in 0..SUBMIT_DEDUPE_ATTEMPTS {
                    let inserted = sqlx::query(
                        "INSERT INTO gamescript_revisions (revision_id, language, entrypoint, \
                         content, size_bytes, created_by, created_at_unix_ms) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7) \
                         ON CONFLICT (revision_id) DO NOTHING",
                    )
                    .bind(&revision_id)
                    .bind(draft.language.as_str())
                    .bind(&draft.entrypoint)
                    .bind(&draft.content)
                    .bind(size_bytes)
                    .bind(&actor)
                    .bind(now)
                    .execute(&mut *conn)
                    .await
                    .map_err(db_err)?
                    .rows_affected()
                        > 0;
                    if inserted {
                        let stored = sqlx::query(SELECT_REVISION_SQL)
                            .bind(&revision_id)
                            .fetch_one(&mut *conn)
                            .await
                            .map_err(db_err)?;
                        resolved = Some((parse_revision(&stored)?, false));
                        break;
                    }
                    if let Some(stored) = sqlx::query(SELECT_REVISION_FOR_UPDATE_SQL)
                        .bind(&revision_id)
                        .fetch_optional(&mut *conn)
                        .await
                        .map_err(db_err)?
                    {
                        resolved = Some((parse_revision(&stored)?, true));
                        break;
                    }
                }
                let (revision, deduplicated) = resolved.ok_or_else(|| {
                    AppError::internal("gamescript submit dedupe kept racing revision pruning")
                })?;
                let details = submit_audit_details(&draft_id, &revision, deduplicated, &context);
                insert_audit(
                    &mut *conn,
                    &actor,
                    AUDIT_ACTION_SUBMIT,
                    &revision_id,
                    &details,
                    now,
                )
                .await?;
                if !deduplicated {
                    insert_outbox(
                        &mut *conn,
                        GameScriptOutboxKind::RevisionCreated,
                        None,
                        &revision_id,
                        None,
                        now,
                    )
                    .await?;
                }
                sqlx::query("DELETE FROM gamescript_drafts WHERE draft_id = $1")
                    .bind(&draft_id)
                    .execute(conn)
                    .await
                    .map_err(db_err)?;
                Ok(GameScriptSubmission {
                    revision,
                    deduplicated,
                })
            })
        })
        .await
    }

    async fn get_revision(&self, revision_id: &str) -> AppResult<Option<GameScriptRevision>> {
        let revision_id = revision_id.to_owned();
        self.transaction(move |conn| {
            let revision_id = revision_id.clone();
            Box::pin(async move {
                sqlx::query(SELECT_REVISION_SQL)
                    .bind(&revision_id)
                    .fetch_optional(conn)
                    .await
                    .map_err(db_err)?
                    .as_ref()
                    .map(parse_revision)
                    .transpose()
            })
        })
        .await
    }

    async fn list_revisions(&self, limit: usize) -> AppResult<Vec<GameScriptRevision>> {
        validate_limit(limit)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_revisions \
                     ORDER BY created_at_unix_ms, revision_id LIMIT $1",
                )
                .bind(limit)
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse_revision)
                .collect()
            })
        })
        .await
    }

    async fn append_diagnostic(
        &self,
        revision_id: &str,
        severity: GameScriptDiagnosticSeverity,
        source: &str,
        message: &str,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDiagnostic> {
        let revision_id = revision_id.to_owned();
        let source = source.to_owned();
        let message = message.to_owned();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let revision_id = revision_id.clone();
            let source = source.clone();
            let message = message.clone();
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                let row = sqlx::query(
                    "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq \
                     FROM gamescript_revision_diagnostics WHERE revision_id = $1",
                )
                .bind(&revision_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(db_err)?;
                let next_seq: i64 = get(&row, "next_seq")?;
                sqlx::query(
                    "INSERT INTO gamescript_revision_diagnostics \
                     (revision_id, seq, severity, source, message, created_at_unix_ms) \
                     VALUES ($1, $2, $3, $4, $5, $6)",
                )
                .bind(&revision_id)
                .bind(next_seq)
                .bind(severity.as_str())
                .bind(&source)
                .bind(&message)
                .bind(now)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(GameScriptDiagnostic {
                    revision_id,
                    seq: u64::try_from(next_seq).map_err(|_| {
                        AppError::internal("invalid gamescript diagnostic sequence")
                    })?,
                    severity,
                    source,
                    message,
                    created_at: millis_to_ts(now)?,
                })
            })
        })
        .await
    }

    async fn diagnostics(&self, revision_id: &str) -> AppResult<Vec<GameScriptDiagnostic>> {
        let revision_id = revision_id.to_owned();
        self.transaction(move |conn| {
            let revision_id = revision_id.clone();
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                sqlx::query(
                    "SELECT * FROM gamescript_revision_diagnostics \
                     WHERE revision_id = $1 ORDER BY seq",
                )
                .bind(&revision_id)
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse_diagnostic)
                .collect()
            })
        })
        .await
    }

    async fn pin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                let inserted = sqlx::query(
                    "INSERT INTO gamescript_revision_pins (revision_id, pinned_by, \
                     pinned_at_unix_ms) VALUES ($1, $2, $3) \
                     ON CONFLICT (revision_id) DO NOTHING",
                )
                .bind(&revision_id)
                .bind(&actor)
                .bind(now)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?
                .rows_affected()
                    > 0;
                if inserted {
                    insert_audit(
                        &mut *conn,
                        &actor,
                        AUDIT_ACTION_PIN,
                        &revision_id,
                        &GameScriptAuditContext::new(),
                        now,
                    )
                    .await?;
                }
                Ok(inserted)
            })
        })
        .await
    }

    async fn unpin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            Box::pin(async move {
                let removed =
                    sqlx::query("DELETE FROM gamescript_revision_pins WHERE revision_id = $1")
                        .bind(&revision_id)
                        .execute(&mut *conn)
                        .await
                        .map_err(db_err)?
                        .rows_affected()
                        > 0;
                if removed {
                    insert_audit(
                        &mut *conn,
                        &actor,
                        AUDIT_ACTION_UNPIN,
                        &revision_id,
                        &GameScriptAuditContext::new(),
                        now,
                    )
                    .await?;
                }
                Ok(removed)
            })
        })
        .await
    }

    async fn allocate_activation_generation(
        &self,
        scope: &str,
        revision_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptActivation> {
        if scope.is_empty() {
            return Err(AppError::validation(
                "gamescript activation scope must not be empty",
            ));
        }
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let scope = scope.to_owned();
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let context = context.clone();
        let now = ts_to_millis(now)?;
        self.transaction(move |conn| {
            let scope = scope.clone();
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            let context = context.clone();
            Box::pin(async move {
                // Roll-forward and rollback share this gate: the target must be
                // an existing, non-pruned revision before a generation is spent.
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                let row = sqlx::query(ALLOCATE_GENERATION_SQL)
                    .bind(&scope)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(db_err)?;
                let generation: i64 = get(&row, "current_generation")?;
                sqlx::query(
                    "INSERT INTO gamescript_activations \
                     (scope, generation, revision_id, activated_by, activated_at_unix_ms) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(&scope)
                .bind(generation)
                .bind(&revision_id)
                .bind(&actor)
                .bind(now)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
                let activation = GameScriptActivation {
                    scope: scope.clone(),
                    generation: u64::try_from(generation).map_err(|_| {
                        AppError::internal("invalid gamescript activation generation")
                    })?,
                    revision_id: revision_id.clone(),
                    activated_by: actor.clone(),
                    activated_at: millis_to_ts(now)?,
                };
                let details = activation_audit_details(&activation, &context);
                insert_audit(
                    &mut *conn,
                    &actor,
                    AUDIT_ACTION_ACTIVATE,
                    &revision_id,
                    &details,
                    now,
                )
                .await?;
                insert_outbox(
                    &mut *conn,
                    GameScriptOutboxKind::ActivationCommitted,
                    Some(&scope),
                    &revision_id,
                    Some(generation),
                    now,
                )
                .await?;
                Ok(activation)
            })
        })
        .await
    }

    async fn current_activation(&self, scope: &str) -> AppResult<Option<GameScriptActivation>> {
        let scope = scope.to_owned();
        self.transaction(move |conn| {
            let scope = scope.clone();
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_activations WHERE scope = $1 \
                     ORDER BY generation DESC LIMIT 1",
                )
                .bind(&scope)
                .fetch_optional(conn)
                .await
                .map_err(db_err)?
                .as_ref()
                .map(parse_activation)
                .transpose()
            })
        })
        .await
    }

    async fn list_activations(
        &self,
        scope: &str,
        limit: usize,
    ) -> AppResult<Vec<GameScriptActivation>> {
        validate_limit(limit)?;
        let scope = scope.to_owned();
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            let scope = scope.clone();
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_activations WHERE scope = $1 \
                     ORDER BY generation DESC LIMIT $2",
                )
                .bind(&scope)
                .bind(limit)
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse_activation)
                .collect()
            })
        })
        .await
    }

    async fn prune_drafts(
        &self,
        updated_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let cutoff = ts_to_millis(updated_before)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                let result = sqlx::query(
                    "DELETE FROM gamescript_drafts WHERE draft_id IN ( \
                       SELECT draft_id FROM gamescript_drafts \
                       WHERE updated_at_unix_ms < $1 \
                       ORDER BY updated_at_unix_ms, draft_id LIMIT $2)",
                )
                .bind(cutoff)
                .bind(limit)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(usize::try_from(result.rows_affected()).unwrap_or(usize::MAX))
            })
        })
        .await
    }

    async fn prune_revisions(
        &self,
        created_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let cutoff = ts_to_millis(created_before)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                // Pinned and activation-referenced revisions are excluded here;
                // the activations foreign key is the database-level backstop.
                // Diagnostics and pins cascade with their revision.
                let result = sqlx::query(
                    "DELETE FROM gamescript_revisions WHERE revision_id IN ( \
                       SELECT revision_id FROM gamescript_revisions \
                       WHERE created_at_unix_ms < $1 \
                         AND revision_id NOT IN \
                             (SELECT revision_id FROM gamescript_revision_pins) \
                         AND revision_id NOT IN \
                             (SELECT revision_id FROM gamescript_activations) \
                       ORDER BY created_at_unix_ms, revision_id LIMIT $2)",
                )
                .bind(cutoff)
                .bind(limit)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(usize::try_from(result.rows_affected()).unwrap_or(usize::MAX))
            })
        })
        .await
    }

    async fn audit_log(&self, limit: usize) -> AppResult<Vec<GameScriptAuditRecord>> {
        validate_limit(limit)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_audit \
                     ORDER BY created_at_unix_ms DESC, audit_id DESC LIMIT $1",
                )
                .bind(limit)
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse_audit)
                .collect()
            })
        })
        .await
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<GameScriptOutboxRecord>> {
        validate_limit(limit)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.transaction(move |conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_outbox \
                     ORDER BY created_at_unix_ms, outbox_id LIMIT $1",
                )
                .bind(limit)
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse_outbox)
                .collect()
            })
        })
        .await
    }

    async fn acknowledge_outbox(&self, outbox_id: u64) -> AppResult<bool> {
        let Ok(outbox_id) = i64::try_from(outbox_id) else {
            return Ok(false);
        };
        self.transaction(move |conn| {
            Box::pin(async move {
                let result = sqlx::query("DELETE FROM gamescript_outbox WHERE outbox_id = $1")
                    .bind(outbox_id)
                    .execute(conn)
                    .await
                    .map_err(db_err)?;
                Ok(result.rows_affected() > 0)
            })
        })
        .await
    }
}
