//! SQLite durable GameScript revision adapter.
//!
//! Every mutating contract method runs inside one `BEGIN IMMEDIATE`
//! transaction, so the state change and its audit/outbox rows commit or
//! disappear together. The revision id is the content hash and the table's
//! primary key: `INSERT ... ON CONFLICT DO NOTHING` plus a re-read makes
//! concurrent submissions of identical content race safely to one row.

use async_trait::async_trait;
use sqlx::{SqliteConnection, sqlite::SqliteRow};

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

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis};

const SELECT_DRAFT_SQL: &str = "SELECT * FROM gamescript_drafts WHERE draft_id = ?";
const SELECT_REVISION_SQL: &str = "SELECT * FROM gamescript_revisions WHERE revision_id = ?";

/// Counter upsert mirroring the chat access-epoch idiom: the first activation
/// of a scope creates the row at 1; later ones increment atomically.
const ALLOCATE_GENERATION_SQL: &str = "\
INSERT INTO gamescript_activation_generations (scope, current_generation) VALUES (?, 1) \
ON CONFLICT(scope) DO UPDATE SET \
current_generation = gamescript_activation_generations.current_generation + 1 \
RETURNING current_generation";

pub struct SqliteGameScriptRepository {
    executor: SqliteExecutor,
    limits: GameScriptLimits,
}

impl SqliteGameScriptRepository {
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self {
            executor,
            limits: GameScriptLimits::default(),
        }
    }

    async fn transaction<T>(
        &self,
        f: impl for<'a> FnOnce(
            &'a mut SqliteConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = AppResult<T>> + Send + 'a>,
        >,
    ) -> AppResult<T> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match f(&mut tx).await {
                    Ok(value) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(value)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("database transaction is already closed"))?;
                f(tx).await
            }
        }
    }
}

fn parse_draft(row: &SqliteRow) -> AppResult<GameScriptDraft> {
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

fn parse_revision(row: &SqliteRow) -> AppResult<GameScriptRevision> {
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

fn parse_activation(row: &SqliteRow) -> AppResult<GameScriptActivation> {
    Ok(GameScriptActivation {
        scope: get(row, "scope")?,
        generation: u64::try_from(get::<i64>(row, "generation")?)
            .map_err(|_| AppError::internal("invalid gamescript activation generation"))?,
        revision_id: get(row, "revision_id")?,
        activated_by: get(row, "activated_by")?,
        activated_at: millis_to_ts(get(row, "activated_at_unix_ms")?)?,
    })
}

fn parse_diagnostic(row: &SqliteRow) -> AppResult<GameScriptDiagnostic> {
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

fn parse_audit(row: &SqliteRow) -> AppResult<GameScriptAuditRecord> {
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

fn parse_outbox(row: &SqliteRow) -> AppResult<GameScriptOutboxRecord> {
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
    conn: &mut SqliteConnection,
    actor: &str,
    action: &str,
    target: &str,
    details: &GameScriptAuditContext,
    now: i64,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO gamescript_audit (actor, action, target, details, created_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?)",
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
    conn: &mut SqliteConnection,
    kind: GameScriptOutboxKind,
    scope: Option<&str>,
    revision_id: &str,
    generation: Option<i64>,
    now: i64,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO gamescript_outbox (kind, scope, revision_id, generation, created_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?)",
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

async fn revision_exists(conn: &mut SqliteConnection, revision_id: &str) -> AppResult<bool> {
    Ok(
        sqlx::query("SELECT 1 FROM gamescript_revisions WHERE revision_id = ?")
            .bind(revision_id)
            .fetch_optional(conn)
            .await
            .map_err(db_err)?
            .is_some(),
    )
}

#[async_trait]
impl GameScriptRepository for SqliteGameScriptRepository {
    async fn create_draft(
        &self,
        request: CreateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_create_draft(&request, &self.limits)?;
        let now = ts_to_millis(now)?;
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO gamescript_drafts (draft_id, language, entrypoint, content, \
                     created_by, created_at_unix_ms, updated_at_unix_ms) \
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&request.draft_id)
                .bind(request.language.as_str())
                .bind(&request.entrypoint)
                .bind(&request.content)
                .bind(&request.created_by)
                .bind(now)
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
        self.transaction(|conn| {
            Box::pin(async move {
                let row = sqlx::query(SELECT_DRAFT_SQL)
                    .bind(&draft_id)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db_err)?
                    .ok_or_else(|| draft_not_found(&draft_id))?;
                let existing = parse_draft(&row)?;
                sqlx::query(
                    "UPDATE gamescript_drafts SET language = ?, entrypoint = ?, content = ?, \
                     updated_at_unix_ms = ? WHERE draft_id = ?",
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
        self.transaction(|conn| {
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query("SELECT * FROM gamescript_drafts ORDER BY draft_id LIMIT ?")
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
        self.transaction(|conn| {
            Box::pin(async move {
                let result = sqlx::query("DELETE FROM gamescript_drafts WHERE draft_id = ?")
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
        self.transaction(|conn| {
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
                // The hash primary key deduplicates identical content and
                // resolves the concurrent-submission race to one row.
                let inserted = sqlx::query(
                    "INSERT INTO gamescript_revisions (revision_id, language, entrypoint, \
                     content, size_bytes, created_by, created_at_unix_ms) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) \
                     ON CONFLICT(revision_id) DO NOTHING",
                )
                .bind(&revision_id)
                .bind(draft.language.as_str())
                .bind(&draft.entrypoint)
                .bind(&draft.content)
                .bind(i64::try_from(draft.content.len()).map_err(|_| {
                    AppError::validation("gamescript content exceeds the maximum source size")
                })?)
                .bind(&actor)
                .bind(now)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?
                .rows_affected()
                    > 0;
                let stored = sqlx::query(SELECT_REVISION_SQL)
                    .bind(&revision_id)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(db_err)?;
                let revision = parse_revision(&stored)?;
                let deduplicated = !inserted;
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
                if inserted {
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
                sqlx::query("DELETE FROM gamescript_drafts WHERE draft_id = ?")
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
        self.transaction(|conn| {
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_revisions \
                     ORDER BY created_at_unix_ms, revision_id LIMIT ?",
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
        self.transaction(|conn| {
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                let row = sqlx::query(
                    "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq \
                     FROM gamescript_revision_diagnostics WHERE revision_id = ?",
                )
                .bind(&revision_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(db_err)?;
                let next_seq: i64 = get(&row, "next_seq")?;
                sqlx::query(
                    "INSERT INTO gamescript_revision_diagnostics \
                     (revision_id, seq, severity, source, message, created_at_unix_ms) \
                     VALUES (?, ?, ?, ?, ?, ?)",
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
        self.transaction(|conn| {
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                sqlx::query(
                    "SELECT * FROM gamescript_revision_diagnostics \
                     WHERE revision_id = ? ORDER BY seq",
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
        self.transaction(|conn| {
            Box::pin(async move {
                if !revision_exists(&mut *conn, &revision_id).await? {
                    return Err(revision_not_found(&revision_id));
                }
                let inserted = sqlx::query(
                    "INSERT INTO gamescript_revision_pins (revision_id, pinned_by, \
                     pinned_at_unix_ms) VALUES (?, ?, ?) \
                     ON CONFLICT(revision_id) DO NOTHING",
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
        self.transaction(|conn| {
            Box::pin(async move {
                let removed =
                    sqlx::query("DELETE FROM gamescript_revision_pins WHERE revision_id = ?")
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
        self.transaction(|conn| {
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
                     VALUES (?, ?, ?, ?, ?)",
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_activations WHERE scope = ? \
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_activations WHERE scope = ? \
                     ORDER BY generation DESC LIMIT ?",
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
        self.transaction(|conn| {
            Box::pin(async move {
                let result = sqlx::query(
                    "DELETE FROM gamescript_drafts WHERE draft_id IN ( \
                       SELECT draft_id FROM gamescript_drafts \
                       WHERE updated_at_unix_ms < ? \
                       ORDER BY updated_at_unix_ms, draft_id LIMIT ?)",
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
        self.transaction(|conn| {
            Box::pin(async move {
                // Pinned and activation-referenced revisions are excluded here;
                // the activations foreign key is the database-level backstop.
                // Diagnostics and pins cascade with their revision.
                let result = sqlx::query(
                    "DELETE FROM gamescript_revisions WHERE revision_id IN ( \
                       SELECT revision_id FROM gamescript_revisions \
                       WHERE created_at_unix_ms < ? \
                         AND revision_id NOT IN \
                             (SELECT revision_id FROM gamescript_revision_pins) \
                         AND revision_id NOT IN \
                             (SELECT revision_id FROM gamescript_activations) \
                       ORDER BY created_at_unix_ms, revision_id LIMIT ?)",
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_audit \
                     ORDER BY created_at_unix_ms DESC, audit_id DESC LIMIT ?",
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
        self.transaction(|conn| {
            Box::pin(async move {
                sqlx::query(
                    "SELECT * FROM gamescript_outbox \
                     ORDER BY created_at_unix_ms, outbox_id LIMIT ?",
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
        self.transaction(|conn| {
            Box::pin(async move {
                let result = sqlx::query("DELETE FROM gamescript_outbox WHERE outbox_id = ?")
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
