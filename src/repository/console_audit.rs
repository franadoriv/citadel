//! Durable console action trail. Mirrors the in-process ring's semantics
//! exactly: newest-first, exact `actor`, prefix `action`, conjunctive, bounded.
//! Never stores passwords, bearer tokens, API-key secrets, console session
//! tokens, or raw request/response payloads — `details` is sanitized by
//! construction at every call site before it ever reaches this adapter.
//!
//! `match_id` is optional and is almost always `NULL`: an operator action is
//! not match-scoped, and this layer deliberately never forces one into a match.

use async_trait::async_trait;
use sqlx::{PgPool, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::ids::{sql_i64, sql_u64};
use crate::services::AuditEntry;
use crate::time::TimestampMillis;

const COLUMNS: &str = "audit_id,node_id,time_unix_ms,actor_type,actor,credential_id,key_name,\
     scopes_json,role,action,target,details,match_id";
const COLUMN_COUNT: usize = 13;
/// Chunking ceiling: older SQLite builds cap `SQLITE_MAX_VARIABLE_NUMBER` at
/// 999, so a multi-row insert stays under 900 bind parameters on every build.
const MAX_BIND_PARAMS: usize = 900;

/// Widest page a caller may request; the console over-fetches `limit + 1`.
const MAX_PAGE_LIMIT: usize = 501;
/// Widest single prune batch.
const MAX_PRUNE_LIMIT: usize = 1_000;

/// One trail entry plus the durable identity the ring has no room for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableAuditRow {
    /// Time-ordered primary key. It is also the keyset cursor and the sort
    /// tiebreak: a failed login and the login after it share one millisecond.
    pub audit_id: String,
    pub node_id: String,
    /// Optional match scope. `None` at every operator call site.
    pub match_id: Option<String>,
    pub entry: AuditEntry,
}

/// Conjunctive read filter; `None` matches all.
#[derive(Debug, Clone, Default)]
pub struct DurableAuditFilter {
    /// Exact actor match.
    pub actor: Option<String>,
    /// Action prefix match (`storage` matches `storage.write`).
    pub action_prefix: Option<String>,
    /// `None` matches all, including rows with no match at all.
    pub match_id: Option<String>,
    pub after_audit_id: Option<String>,
    /// Caller pre-clamps. `0` is never passed: in the ring `0` means "capacity",
    /// and a literal `LIMIT 0` here would silently return nothing.
    pub limit: usize,
}

#[async_trait]
pub trait DurableAuditRepository: Send + Sync {
    /// Idempotent append: a retried flush re-sends the whole batch.
    async fn append_batch(&self, rows: &[DurableAuditRow]) -> AppResult<usize>;
    /// Newest-first keyset page over `audit_id`.
    async fn list(&self, filter: &DurableAuditFilter) -> AppResult<Vec<DurableAuditRow>>;
    /// Matching row count, ignoring the cursor and the limit.
    async fn count(&self, filter: &DurableAuditFilter) -> AppResult<u64>;
    async fn prune(&self, before: TimestampMillis, limit: usize) -> AppResult<usize>;
}

#[derive(Clone)]
pub struct SqliteAuditRepository {
    pool: SqlitePool,
}

impl SqliteAuditRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Clone)]
pub struct PgAuditRepository {
    pool: PgPool,
}

impl PgAuditRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db(error: sqlx::Error) -> AppError {
    AppError::database("console audit persistence failed").with_detail(error.to_string())
}

fn encode_scopes(scopes: Option<&Vec<String>>) -> AppResult<Option<String>> {
    scopes
        .map(|scopes| {
            serde_json::to_string(scopes).map_err(|error| {
                AppError::internal("audit scope serialization failed")
                    .with_detail(error.to_string())
            })
        })
        .transpose()
}

fn decode_scopes(value: Option<String>) -> AppResult<Option<Vec<String>>> {
    value
        .map(|value| {
            serde_json::from_str::<Vec<String>>(&value).map_err(|error| {
                AppError::internal("audit scope row is invalid").with_detail(error.to_string())
            })
        })
        .transpose()
}

// `clamp` bounds both values far inside `i64`, so the fallbacks are unreachable.
fn page_limit(limit: usize) -> i64 {
    i64::try_from(limit.clamp(1, MAX_PAGE_LIMIT)).unwrap_or(1)
}

fn prune_limit(limit: usize) -> i64 {
    i64::try_from(limit.clamp(1, MAX_PRUNE_LIMIT)).unwrap_or(1)
}

fn rows_per_chunk() -> usize {
    (MAX_BIND_PARAMS / COLUMN_COUNT).max(1)
}

/// Escape a caller-supplied `LIKE` prefix.
///
/// Mandatory: without it an operator typing `%` in the action filter gets a
/// full wildcard scan instead of the literal prefix they asked for.
fn like_prefix(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 1);
    for ch in value.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('%');
    out
}

/// Row decoding shared by both adapters. A macro because `PgRow` and
/// `SqliteRow` share no object-safe supertrait to write this against.
macro_rules! decode_audit_row {
    ($row:expr) => {{
        let row = $row;
        (|| {
            Ok(DurableAuditRow {
                audit_id: row.try_get::<String, _>("audit_id").map_err(db)?,
                node_id: row.try_get::<String, _>("node_id").map_err(db)?,
                match_id: row.try_get::<Option<String>, _>("match_id").map_err(db)?,
                entry: AuditEntry {
                    time_unix_ms: sql_u64(
                        row.try_get::<i64, _>("time_unix_ms").map_err(db)?,
                        "audit entry time",
                    )?,
                    actor_type: row.try_get::<String, _>("actor_type").map_err(db)?,
                    actor: row.try_get::<String, _>("actor").map_err(db)?,
                    credential_id: row
                        .try_get::<Option<String>, _>("credential_id")
                        .map_err(db)?,
                    key_name: row.try_get::<Option<String>, _>("key_name").map_err(db)?,
                    scopes: decode_scopes(
                        row.try_get::<Option<String>, _>("scopes_json")
                            .map_err(db)?,
                    )?,
                    role: row.try_get::<String, _>("role").map_err(db)?,
                    action: row.try_get::<String, _>("action").map_err(db)?,
                    target: row.try_get::<String, _>("target").map_err(db)?,
                    details: row.try_get::<String, _>("details").map_err(db)?,
                    // Read a second time, deliberately: the row wrapper carries
                    // the scope for the console projection and the entry carries
                    // it so an append -> list round-trip is lossless.
                    match_id: row.try_get::<Option<String>, _>("match_id").map_err(db)?,
                },
            })
        })()
    }};
}

#[async_trait]
impl DurableAuditRepository for SqliteAuditRepository {
    async fn append_batch(&self, rows: &[DurableAuditRow]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in rows.chunks(rows_per_chunk()) {
            let placeholders = vec!["(?,?,?,?,?,?,?,?,?,?,?,?,?)"; chunk.len()].join(",");
            // Targeted `DO NOTHING`, not `INSERT OR IGNORE`: the latter also
            // swallows a CHECK violation.
            let sql = format!(
                "INSERT INTO console_audit_entries ({COLUMNS}) VALUES {placeholders} 
                 ON CONFLICT (audit_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for row in chunk {
                query = query
                    .bind(&row.audit_id)
                    .bind(&row.node_id)
                    .bind(sql_i64(row.entry.time_unix_ms, "audit entry time")?)
                    .bind(&row.entry.actor_type)
                    .bind(&row.entry.actor)
                    .bind(row.entry.credential_id.as_deref())
                    .bind(row.entry.key_name.as_deref())
                    .bind(encode_scopes(row.entry.scopes.as_ref())?)
                    .bind(&row.entry.role)
                    .bind(&row.entry.action)
                    .bind(&row.entry.target)
                    .bind(&row.entry.details)
                    .bind(row.match_id.as_deref());
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn list(&self, filter: &DurableAuditFilter) -> AppResult<Vec<DurableAuditRow>> {
        let action = filter.action_prefix.as_deref().map(like_prefix);
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM console_audit_entries \
             WHERE (? IS NULL OR actor = ?) \
               AND (? IS NULL OR action LIKE ? ESCAPE '\\') \
               AND (? IS NULL OR match_id = ?) \
               AND (? IS NULL OR audit_id < ?) \
             ORDER BY audit_id DESC LIMIT ?"
        ))
        .bind(filter.actor.as_deref())
        .bind(filter.actor.as_deref())
        .bind(action.as_deref())
        .bind(action.as_deref())
        .bind(filter.match_id.as_deref())
        .bind(filter.match_id.as_deref())
        .bind(filter.after_audit_id.as_deref())
        .bind(filter.after_audit_id.as_deref())
        .bind(page_limit(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_audit_row!(row)).collect()
    }

    async fn count(&self, filter: &DurableAuditFilter) -> AppResult<u64> {
        let action = filter.action_prefix.as_deref().map(like_prefix);
        let total = sqlx::query(
            "SELECT COUNT(*) AS total FROM console_audit_entries \
             WHERE (? IS NULL OR actor = ?) \
               AND (? IS NULL OR action LIKE ? ESCAPE '\\') \
               AND (? IS NULL OR match_id = ?)",
        )
        .bind(filter.actor.as_deref())
        .bind(filter.actor.as_deref())
        .bind(action.as_deref())
        .bind(action.as_deref())
        .bind(filter.match_id.as_deref())
        .bind(filter.match_id.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_err(db)?
        .try_get::<i64, _>("total")
        .map_err(db)?;
        sql_u64(total, "audit entry count")
    }

    async fn prune(&self, before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM console_audit_entries WHERE audit_id IN (\
                 SELECT audit_id FROM console_audit_entries WHERE time_unix_ms < ? \
                  ORDER BY audit_id LIMIT ?)",
        )
        .bind(sql_i64(before.unix_millis(), "audit prune horizon")?)
        .bind(prune_limit(limit))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(usize::try_from(affected).unwrap_or(0))
    }
}

#[async_trait]
impl DurableAuditRepository for PgAuditRepository {
    async fn append_batch(&self, rows: &[DurableAuditRow]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in rows.chunks(rows_per_chunk()) {
            let mut next = 1_usize;
            let placeholders = chunk
                .iter()
                .map(|_| {
                    let row = (next..next + COLUMN_COUNT)
                        .map(|index| format!("${index}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    next += COLUMN_COUNT;
                    format!("({row})")
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO console_audit_entries ({COLUMNS}) VALUES {placeholders} \
                 ON CONFLICT (audit_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for row in chunk {
                query = query
                    .bind(&row.audit_id)
                    .bind(&row.node_id)
                    .bind(sql_i64(row.entry.time_unix_ms, "audit entry time")?)
                    .bind(&row.entry.actor_type)
                    .bind(&row.entry.actor)
                    .bind(row.entry.credential_id.as_deref())
                    .bind(row.entry.key_name.as_deref())
                    .bind(encode_scopes(row.entry.scopes.as_ref())?)
                    .bind(&row.entry.role)
                    .bind(&row.entry.action)
                    .bind(&row.entry.target)
                    .bind(&row.entry.details)
                    .bind(row.match_id.as_deref());
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn list(&self, filter: &DurableAuditFilter) -> AppResult<Vec<DurableAuditRow>> {
        let action = filter.action_prefix.as_deref().map(like_prefix);
        // Every nullable predicate parameter carries an explicit `::text`: without
        // it sqlx cannot infer the type of a `NULL` bind and the query fails.
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM console_audit_entries \
             WHERE ($1::text IS NULL OR actor = $1) \
               AND ($2::text IS NULL OR action LIKE $2 ESCAPE '\\') \
               AND ($3::text IS NULL OR match_id = $3) \
               AND ($4::text IS NULL OR audit_id < $4) \
             ORDER BY audit_id DESC LIMIT $5"
        ))
        .bind(filter.actor.as_deref())
        .bind(action.as_deref())
        .bind(filter.match_id.as_deref())
        .bind(filter.after_audit_id.as_deref())
        .bind(page_limit(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_audit_row!(row)).collect()
    }

    async fn count(&self, filter: &DurableAuditFilter) -> AppResult<u64> {
        let action = filter.action_prefix.as_deref().map(like_prefix);
        let total = sqlx::query(
            "SELECT COUNT(*) AS total FROM console_audit_entries \
             WHERE ($1::text IS NULL OR actor = $1) \
               AND ($2::text IS NULL OR action LIKE $2 ESCAPE '\\') \
               AND ($3::text IS NULL OR match_id = $3)",
        )
        .bind(filter.actor.as_deref())
        .bind(action.as_deref())
        .bind(filter.match_id.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_err(db)?
        .try_get::<i64, _>("total")
        .map_err(db)?;
        sql_u64(total, "audit entry count")
    }

    async fn prune(&self, before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM console_audit_entries WHERE audit_id IN (\
                 SELECT audit_id FROM console_audit_entries WHERE time_unix_ms < $1 \
                  ORDER BY audit_id LIMIT $2)",
        )
        .bind(sql_i64(before.unix_millis(), "audit prune horizon")?)
        .bind(prune_limit(limit))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(usize::try_from(affected).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn repository() -> SqliteAuditRepository {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260824092000_create_console_audit_entries.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        SqliteAuditRepository::new(pool)
    }

    fn row(index: u64, actor: &str, action: &str, at_ms: u64) -> DurableAuditRow {
        DurableAuditRow {
            audit_id: format!("au1-{index:029x}"),
            node_id: "node-a".to_string(),
            match_id: None,
            entry: AuditEntry::new(
                TimestampMillis::from_unix_millis(at_ms),
                actor,
                "admin",
                action,
                "-",
                "ok",
            ),
        }
    }

    #[tokio::test]
    async fn sqlite_append_is_idempotent_and_chunks_beyond_the_bind_ceiling() {
        let repository = repository().await;
        let rows = (1..=150_u64)
            .map(|index| row(index, "ops", "storage.write", 1_000 + index))
            .collect::<Vec<_>>();
        assert_eq!(repository.append_batch(&rows).await.expect("bulk"), 150);
        assert_eq!(
            repository.append_batch(&rows).await.expect("retry"),
            0,
            "a retried flush re-sends the batch and stores nothing twice"
        );
        assert_eq!(
            repository
                .count(&DurableAuditFilter::default())
                .await
                .expect("count"),
            150
        );
    }

    #[tokio::test]
    async fn sqlite_orders_by_id_so_entries_sharing_a_millisecond_stay_deterministic() {
        let repository = repository().await;
        // `login_failed` and `login` are recorded from one `now` in the console
        // extractor; only the id can break that tie.
        repository
            .append_batch(&[
                row(1, "ops", "console.login_failed", 5_000),
                row(2, "ops", "console.login", 5_000),
            ])
            .await
            .expect("append");
        let listed = repository
            .list(&DurableAuditFilter {
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(
            listed
                .iter()
                .map(|row| row.entry.action.as_str())
                .collect::<Vec<_>>(),
            vec!["console.login", "console.login_failed"]
        );
    }

    #[tokio::test]
    async fn sqlite_filters_are_conjunctive_and_the_action_prefix_is_escaped() {
        let repository = repository().await;
        repository
            .append_batch(&[
                row(1, "ops", "storage.write", 1_000),
                row(2, "ops", "storage.read", 1_001),
                row(3, "admin", "storage.write", 1_002),
                row(4, "ops", "accounts.ban", 1_003),
                row(5, "ops", "st%range", 1_004),
            ])
            .await
            .expect("append");

        let filtered = repository
            .list(&DurableAuditFilter {
                actor: Some("ops".to_string()),
                action_prefix: Some("storage".to_string()),
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(
            filtered
                .iter()
                .map(|row| row.entry.action.as_str())
                .collect::<Vec<_>>(),
            vec!["storage.read", "storage.write"]
        );

        // A literal `%` must match only itself, never act as a wildcard.
        let literal = repository
            .list(&DurableAuditFilter {
                action_prefix: Some("st%".to_string()),
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(
            literal
                .iter()
                .map(|row| row.entry.action.as_str())
                .collect::<Vec<_>>(),
            vec!["st%range"]
        );
    }

    #[tokio::test]
    async fn sqlite_keeps_operator_actions_out_of_a_match_unless_one_is_asked_for() {
        let repository = repository().await;
        let mut scoped = row(2, "ops", "matchlog.detail", 2_000);
        scoped.match_id = Some("mt1-a".to_string());
        repository
            .append_batch(&[row(1, "ops", "console.login", 1_000), scoped])
            .await
            .expect("append");

        // `None` matches everything, including the rows with no match at all.
        assert_eq!(
            repository
                .count(&DurableAuditFilter::default())
                .await
                .expect("count all"),
            2
        );
        let scoped = repository
            .list(&DurableAuditFilter {
                match_id: Some("mt1-a".to_string()),
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].entry.action, "matchlog.detail");
    }

    #[tokio::test]
    async fn sqlite_round_trips_machine_credential_metadata() {
        let repository = repository().await;
        let mut row = row(1, "key-1", "console.read", 1_000);
        row.entry.actor_type = "api_key".to_string();
        row.entry.credential_id = Some("cred-1".to_string());
        row.entry.key_name = Some("ci poller".to_string());
        row.entry.scopes = Some(vec!["logs:read".to_string(), "matches:read".to_string()]);
        row.entry.role = "api_key".to_string();
        repository
            .append_batch(std::slice::from_ref(&row))
            .await
            .expect("append");
        let stored = repository
            .list(&DurableAuditFilter {
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(stored[0], row);
    }

    #[tokio::test]
    async fn sqlite_pages_by_keyset_and_prunes_oldest_first() {
        let repository = repository().await;
        let rows = (1..=5_u64)
            .map(|index| row(index, "ops", "console.read", 1_000 + index))
            .collect::<Vec<_>>();
        repository.append_batch(&rows).await.expect("append");

        let first = repository
            .list(&DurableAuditFilter {
                limit: 2,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("page");
        assert_eq!(first[0].audit_id, rows[4].audit_id);
        let next = repository
            .list(&DurableAuditFilter {
                after_audit_id: Some(first[1].audit_id.clone()),
                limit: 2,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("next page");
        assert_eq!(next[0].audit_id, rows[2].audit_id);

        assert_eq!(
            repository
                .prune(TimestampMillis::from_unix_millis(10_000), 2)
                .await
                .expect("prune"),
            2
        );
        let remaining = repository
            .list(&DurableAuditFilter {
                limit: 10,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[2].audit_id, rows[2].audit_id);
    }
}
