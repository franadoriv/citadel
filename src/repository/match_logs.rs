//! Free-form game-script log persistence. `payload_json` is author-supplied and
//! is NOT redacted or inspected: it is the operator's own game data. The server
//! never adds a credential, bearer token, session id, participant id, or
//! transport identifier to any column here.
//!
//! `match_id` is optional by design. A game with no match concept at all — an
//! MMORPG world tick, a global scheduled job — still writes its logs; the row
//! simply carries `NULL`. A log outside a match is stored, never rejected.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::ids::{sql_i64, sql_u64};
use crate::time::TimestampMillis;

const COLUMNS: &str = "log_id,match_id,node_id,created_at_ms,level,tag,message,payload_json";
const COLUMN_COUNT: usize = 8;
/// Chunking ceiling: older SQLite builds cap `SQLITE_MAX_VARIABLE_NUMBER` at
/// 999, so a multi-row insert stays under 900 bind parameters on every build.
const MAX_BIND_PARAMS: usize = 900;

/// Widest page a caller may request; the console over-fetches `limit + 1`.
const MAX_PAGE_LIMIT: usize = 201;
/// Widest single prune batch.
const MAX_PRUNE_LIMIT: usize = 1_000;

/// Severity of one script-written log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    /// Parse a level name.
    ///
    /// Strict on purpose: an unrecognized level is a validation error, never a
    /// silent default to `info`. The volatile `citadel.log` may default because
    /// nothing is persisted; a stored row that says `info` when the author
    /// wrote `eror` is a lie an operator cannot detect later.
    ///
    /// # Errors
    /// Returns a validation error for any name outside the five levels.
    pub fn parse(value: &str) -> AppResult<Self> {
        match value {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err(AppError::validation(
                "log level must be trace, debug, info, warn, or error",
            )),
        }
    }
}

/// One stored log line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MatchLogEntry {
    pub log_id: String,
    /// `None` for a log written outside any match-scoped callback.
    pub match_id: Option<String>,
    pub node_id: String,
    pub created_at_ms: u64,
    pub level: LogLevel,
    pub tag: String,
    pub message: String,
    /// Author-supplied and stored verbatim.
    pub payload_json: Option<String>,
}

/// Conjunctive read filter; `None` matches all.
#[derive(Debug, Clone, Default)]
pub struct MatchLogFilter {
    pub match_id: Option<String>,
    pub level: Option<LogLevel>,
    pub tag_prefix: Option<String>,
    pub after_log_id: Option<String>,
    pub limit: usize,
}

#[async_trait]
pub trait DurableMatchLogRepository: Send + Sync {
    /// Idempotent append. A retried flush re-sends the whole batch, so an
    /// already-stored `log_id` is skipped rather than duplicated.
    async fn append_batch(&self, entries: &[MatchLogEntry]) -> AppResult<usize>;
    async fn get(&self, log_id: &str) -> AppResult<Option<MatchLogEntry>>;
    /// Keyset over `log_id` DESC — newest first, since ids are time-ordered.
    async fn list(&self, filter: &MatchLogFilter) -> AppResult<Vec<MatchLogEntry>>;
    async fn count_for_match(&self, match_id: &str) -> AppResult<u64>;
    async fn prune(&self, created_before: TimestampMillis, limit: usize) -> AppResult<usize>;
}

#[derive(Clone)]
pub struct SqliteMatchLogRepository {
    pool: SqlitePool,
}

impl SqliteMatchLogRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Clone)]
pub struct PgMatchLogRepository {
    pool: PgPool,
}

impl PgMatchLogRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db(error: sqlx::Error) -> AppError {
    AppError::database("match log persistence failed").with_detail(error.to_string())
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
/// Without this an operator typing `%` gets a full wildcard scan instead of the
/// literal tag they asked for.
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

/// PostgreSQL renders `payload_json` as text so both dialects decode identically.
fn select_columns(json_as_text: bool) -> String {
    let payload = if json_as_text {
        "payload_json::text AS payload_json"
    } else {
        "payload_json"
    };
    format!("log_id,match_id,node_id,created_at_ms,level,tag,message,{payload}")
}

/// Row decoding shared by both adapters. A macro because `PgRow` and
/// `SqliteRow` share no object-safe supertrait to write this against.
macro_rules! decode_match_log {
    ($row:expr) => {{
        let row = $row;
        (|| {
            Ok(MatchLogEntry {
                log_id: row.try_get::<String, _>("log_id").map_err(db)?,
                match_id: row.try_get::<Option<String>, _>("match_id").map_err(db)?,
                node_id: row.try_get::<String, _>("node_id").map_err(db)?,
                created_at_ms: sql_u64(
                    row.try_get::<i64, _>("created_at_ms").map_err(db)?,
                    "match log time",
                )?,
                level: LogLevel::parse(&row.try_get::<String, _>("level").map_err(db)?)?,
                tag: row.try_get::<String, _>("tag").map_err(db)?,
                message: row.try_get::<String, _>("message").map_err(db)?,
                payload_json: row
                    .try_get::<Option<String>, _>("payload_json")
                    .map_err(db)?,
            })
        })()
    }};
}

#[async_trait]
impl DurableMatchLogRepository for SqliteMatchLogRepository {
    async fn append_batch(&self, entries: &[MatchLogEntry]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in entries.chunks(rows_per_chunk()) {
            let placeholders = vec!["(?,?,?,?,?,?,?,?)"; chunk.len()].join(",");
            // Targeted `DO NOTHING`, not `INSERT OR IGNORE`: the latter also
            // swallows a CHECK violation, so a row with an out-of-range tag or
            // message would vanish instead of failing the flush.
            let sql = format!(
                "INSERT INTO match_logs ({COLUMNS}) VALUES {placeholders} 
                 ON CONFLICT (log_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for entry in chunk {
                query = query
                    .bind(&entry.log_id)
                    .bind(entry.match_id.as_deref())
                    .bind(&entry.node_id)
                    .bind(sql_i64(entry.created_at_ms, "match log time")?)
                    .bind(entry.level.as_str())
                    .bind(&entry.tag)
                    .bind(&entry.message)
                    .bind(entry.payload_json.as_deref());
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn get(&self, log_id: &str) -> AppResult<Option<MatchLogEntry>> {
        let columns = select_columns(false);
        let row = sqlx::query(&format!("SELECT {columns} FROM match_logs WHERE log_id=?"))
            .bind(log_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|row| decode_match_log!(&row)).transpose()
    }

    async fn list(&self, filter: &MatchLogFilter) -> AppResult<Vec<MatchLogEntry>> {
        let columns = select_columns(false);
        let level = filter.level.map(LogLevel::as_str);
        let tag = filter.tag_prefix.as_deref().map(like_prefix);
        let rows = sqlx::query(&format!(
            "SELECT {columns} FROM match_logs \
             WHERE (? IS NULL OR match_id = ?) \
               AND (? IS NULL OR level = ?) \
               AND (? IS NULL OR tag LIKE ? ESCAPE '\\') \
               AND (? IS NULL OR log_id < ?) \
             ORDER BY log_id DESC LIMIT ?"
        ))
        .bind(filter.match_id.as_deref())
        .bind(filter.match_id.as_deref())
        .bind(level)
        .bind(level)
        .bind(tag.as_deref())
        .bind(tag.as_deref())
        .bind(filter.after_log_id.as_deref())
        .bind(filter.after_log_id.as_deref())
        .bind(page_limit(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_match_log!(row)).collect()
    }

    async fn count_for_match(&self, match_id: &str) -> AppResult<u64> {
        let total = sqlx::query("SELECT COUNT(*) AS total FROM match_logs WHERE match_id=?")
            .bind(match_id)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?
            .try_get::<i64, _>("total")
            .map_err(db)?;
        sql_u64(total, "match log count")
    }

    async fn prune(&self, created_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM match_logs WHERE log_id IN (\
                 SELECT log_id FROM match_logs WHERE created_at_ms < ? \
                  ORDER BY log_id LIMIT ?)",
        )
        .bind(sql_i64(
            created_before.unix_millis(),
            "match log prune horizon",
        )?)
        .bind(prune_limit(limit))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(usize::try_from(affected).unwrap_or(0))
    }
}

#[async_trait]
impl DurableMatchLogRepository for PgMatchLogRepository {
    async fn append_batch(&self, entries: &[MatchLogEntry]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in entries.chunks(rows_per_chunk()) {
            let mut next = 1_usize;
            let placeholders = chunk
                .iter()
                .map(|_| {
                    // The last column is JSONB and needs its cast on the
                    // parameter, not on the value.
                    let row = (next..next + COLUMN_COUNT)
                        .map(|index| {
                            if index == next + COLUMN_COUNT - 1 {
                                format!("${index}::jsonb")
                            } else {
                                format!("${index}")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    next += COLUMN_COUNT;
                    format!("({row})")
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO match_logs ({COLUMNS}) VALUES {placeholders} \
                 ON CONFLICT (log_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for entry in chunk {
                query = query
                    .bind(&entry.log_id)
                    .bind(entry.match_id.as_deref())
                    .bind(&entry.node_id)
                    .bind(sql_i64(entry.created_at_ms, "match log time")?)
                    .bind(entry.level.as_str())
                    .bind(&entry.tag)
                    .bind(&entry.message)
                    .bind(entry.payload_json.as_deref());
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn get(&self, log_id: &str) -> AppResult<Option<MatchLogEntry>> {
        let columns = select_columns(true);
        let row = sqlx::query(&format!("SELECT {columns} FROM match_logs WHERE log_id=$1"))
            .bind(log_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|row| decode_match_log!(&row)).transpose()
    }

    async fn list(&self, filter: &MatchLogFilter) -> AppResult<Vec<MatchLogEntry>> {
        let columns = select_columns(true);
        let level = filter.level.map(LogLevel::as_str);
        let tag = filter.tag_prefix.as_deref().map(like_prefix);
        // Every nullable predicate parameter carries an explicit `::text`: without
        // it sqlx cannot infer the type of a `NULL` bind and the query fails.
        let rows = sqlx::query(&format!(
            "SELECT {columns} FROM match_logs \
             WHERE ($1::text IS NULL OR match_id = $1) \
               AND ($2::text IS NULL OR level = $2) \
               AND ($3::text IS NULL OR tag LIKE $3 ESCAPE '\\') \
               AND ($4::text IS NULL OR log_id < $4) \
             ORDER BY log_id DESC LIMIT $5"
        ))
        .bind(filter.match_id.as_deref())
        .bind(level)
        .bind(tag.as_deref())
        .bind(filter.after_log_id.as_deref())
        .bind(page_limit(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_match_log!(row)).collect()
    }

    async fn count_for_match(&self, match_id: &str) -> AppResult<u64> {
        let total = sqlx::query("SELECT COUNT(*) AS total FROM match_logs WHERE match_id=$1")
            .bind(match_id)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?
            .try_get::<i64, _>("total")
            .map_err(db)?;
        sql_u64(total, "match log count")
    }

    async fn prune(&self, created_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM match_logs WHERE log_id IN (\
                 SELECT log_id FROM match_logs WHERE created_at_ms < $1 \
                  ORDER BY log_id LIMIT $2)",
        )
        .bind(sql_i64(
            created_before.unix_millis(),
            "match log prune horizon",
        )?)
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

    async fn repository() -> SqliteMatchLogRepository {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260824091000_create_match_logs.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        SqliteMatchLogRepository::new(pool)
    }

    fn entry(index: u64, match_id: Option<&str>, level: LogLevel, tag: &str) -> MatchLogEntry {
        MatchLogEntry {
            log_id: format!("ml1-{index:029x}"),
            match_id: match_id.map(str::to_string),
            node_id: "node-a".to_string(),
            created_at_ms: 1_000 + index,
            level,
            tag: tag.to_string(),
            message: format!("line {index}"),
            payload_json: None,
        }
    }

    #[test]
    fn levels_round_trip_and_reject_anything_else() {
        for level in [
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
        ] {
            assert_eq!(LogLevel::parse(level.as_str()).expect("parse"), level);
        }
        assert!(LogLevel::parse("INFO").is_err());
        assert!(LogLevel::parse("fatal").is_err());
    }

    #[test]
    fn a_like_prefix_never_smuggles_a_wildcard() {
        assert_eq!(like_prefix("combat"), "combat%");
        assert_eq!(like_prefix("100%"), "100\\%%");
        assert_eq!(like_prefix("a_b"), "a\\_b%");
        assert_eq!(like_prefix("back\\slash"), "back\\\\slash%");
    }

    #[tokio::test]
    async fn sqlite_stores_a_log_written_outside_any_match() {
        let repository = repository().await;
        // A game with no match concept must still be able to write.
        assert_eq!(
            repository
                .append_batch(&[entry(1, None, LogLevel::Info, "world")])
                .await
                .expect("append"),
            1
        );
        let stored = repository
            .get(&entry(1, None, LogLevel::Info, "world").log_id)
            .await
            .expect("get")
            .expect("entry");
        assert_eq!(stored.match_id, None);
        assert_eq!(stored.level, LogLevel::Info);
        assert_eq!(stored.tag, "world");
    }

    #[tokio::test]
    async fn sqlite_append_is_idempotent_and_chunks_beyond_the_bind_ceiling() {
        let repository = repository().await;
        let entries = (1..=200_u64)
            .map(|index| entry(index, Some("mt1-a"), LogLevel::Debug, "bulk"))
            .collect::<Vec<_>>();
        assert_eq!(
            repository.append_batch(&entries).await.expect("bulk"),
            200,
            "a batch wider than the bind ceiling is chunked, not truncated"
        );
        assert_eq!(
            repository.append_batch(&entries).await.expect("retry"),
            0,
            "a retried flush re-sends the batch and stores nothing twice"
        );
        assert_eq!(
            repository.count_for_match("mt1-a").await.expect("count"),
            200
        );
    }

    #[tokio::test]
    async fn sqlite_filters_are_conjunctive_and_page_newest_first() {
        let repository = repository().await;
        repository
            .append_batch(&[
                entry(1, Some("mt1-a"), LogLevel::Info, "combat.hit"),
                entry(2, Some("mt1-a"), LogLevel::Error, "combat.miss"),
                entry(3, Some("mt1-b"), LogLevel::Error, "combat.hit"),
                entry(4, Some("mt1-a"), LogLevel::Error, "economy"),
                entry(5, None, LogLevel::Error, "combat.hit"),
            ])
            .await
            .expect("append");

        let filtered = repository
            .list(&MatchLogFilter {
                match_id: Some("mt1-a".to_string()),
                level: Some(LogLevel::Error),
                tag_prefix: Some("combat".to_string()),
                after_log_id: None,
                limit: 10,
            })
            .await
            .expect("list");
        assert_eq!(
            filtered
                .iter()
                .map(|line| line.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["combat.miss"]
        );

        let first = repository
            .list(&MatchLogFilter {
                limit: 2,
                ..MatchLogFilter::default()
            })
            .await
            .expect("first page");
        assert_eq!(
            first
                .iter()
                .map(|line| line.message.as_str())
                .collect::<Vec<_>>(),
            vec!["line 5", "line 4"]
        );
        let next = repository
            .list(&MatchLogFilter {
                after_log_id: Some(first[1].log_id.clone()),
                limit: 2,
                ..MatchLogFilter::default()
            })
            .await
            .expect("next page");
        assert_eq!(
            next.iter()
                .map(|line| line.message.as_str())
                .collect::<Vec<_>>(),
            vec!["line 3", "line 2"]
        );
    }

    #[tokio::test]
    async fn sqlite_stores_payload_json_verbatim() {
        let repository = repository().await;
        let mut line = entry(1, Some("mt1-a"), LogLevel::Warn, "score");
        line.payload_json = Some("{\"kills\":3,\"note\":\"<b>\"}".to_string());
        repository
            .append_batch(std::slice::from_ref(&line))
            .await
            .expect("append");
        let stored = repository
            .get(&line.log_id)
            .await
            .expect("get")
            .expect("entry");
        assert_eq!(stored.payload_json, line.payload_json);
    }

    #[tokio::test]
    async fn sqlite_prune_is_bounded_and_oldest_first() {
        let repository = repository().await;
        let entries = (1..=5_u64)
            .map(|index| entry(index, None, LogLevel::Info, "retention"))
            .collect::<Vec<_>>();
        repository.append_batch(&entries).await.expect("append");
        let removed = repository
            .prune(TimestampMillis::from_unix_millis(10_000), 2)
            .await
            .expect("prune");
        assert_eq!(removed, 2);
        let remaining = repository
            .list(&MatchLogFilter {
                limit: 10,
                ..MatchLogFilter::default()
            })
            .await
            .expect("list");
        assert_eq!(
            remaining
                .iter()
                .map(|line| line.message.as_str())
                .collect::<Vec<_>>(),
            vec!["line 5", "line 4", "line 3"],
            "the two oldest lines are the ones that go"
        );
    }
}
