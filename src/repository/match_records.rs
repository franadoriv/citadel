//! Durable match lifecycle records. Stores no participant identities, account
//! ids, session ids, or script payloads — only the server-owned shape of a
//! match and the reason it ended. Deliberately independent of `UnitOfWork`: a
//! match row is written from the realtime gateway's synchronous lifecycle
//! funnel through a write-behind queue and never composes with a domain
//! mutation.
//!
//! Open and close are the only mutations, and both are idempotent: a flush that
//! partially succeeded is retried whole.

use async_trait::async_trait;
use sqlx::{PgPool, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::ids::{sql_i64, sql_u64};
use crate::time::TimestampMillis;

/// Columns written by an open, in bind order.
const OPEN_COLUMNS: &str = "match_id,node_id,boot_id,room_id,name,map,mode,max_players,\
     script_revision_id,script_generation,clock_epoch,opened_at_ms";
/// Bind parameters per opened row.
const OPEN_COLUMN_COUNT: usize = 12;
/// Chunking ceiling: older SQLite builds cap `SQLITE_MAX_VARIABLE_NUMBER` at
/// 999, so a multi-row insert stays under 900 bind parameters on every build.
const MAX_BIND_PARAMS: usize = 900;

/// Widest page a caller may request. The console over-fetches `limit + 1` on a
/// `MAX_LIMIT` of 200 to decide whether a next cursor exists.
const MAX_PAGE_LIMIT: usize = 201;
/// Widest single prune batch, so a retention backlog never becomes one
/// unbounded delete.
const MAX_PRUNE_LIMIT: usize = 1_000;

/// The server-owned shape of a match at the moment the room was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchOpen {
    pub match_id: String,
    pub node_id: String,
    pub boot_id: String,
    pub room_id: u64,
    pub name: Option<String>,
    pub map: String,
    pub mode: String,
    pub max_players: u16,
    pub script_revision_id: Option<String>,
    pub script_generation: Option<u64>,
    pub clock_epoch: u64,
    pub opened_at_ms: u64,
}

/// What the gateway knows when a room empties for the last time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchClose {
    pub match_id: String,
    pub closed_at_ms: u64,
    pub termination_reason: String,
    pub peak_participants: u32,
    pub join_total: u32,
    /// Author-supplied result, when the script stamped one before the close.
    pub result_json: Option<String>,
}

/// A stored match row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MatchRecord {
    pub match_id: String,
    pub node_id: String,
    pub boot_id: String,
    pub room_id: u64,
    pub name: Option<String>,
    pub map: String,
    pub mode: String,
    pub max_players: u16,
    pub script_revision_id: Option<String>,
    pub script_generation: Option<u64>,
    pub clock_epoch: u64,
    pub opened_at_ms: u64,
    pub closed_at_ms: Option<u64>,
    pub termination_reason: Option<String>,
    pub peak_participants: u32,
    pub join_total: u32,
    pub result_json: Option<String>,
}

#[async_trait]
pub trait DurableMatchRepository: Send + Sync {
    /// Idempotent open. Re-running a batch is a no-op, so a retried flush never
    /// duplicates a row and never fails.
    async fn open_batch(&self, opens: &[MatchOpen]) -> AppResult<usize>;

    /// Idempotent close, guarded by `closed_at_ms IS NULL`. A second close is a
    /// no-op returning 0. This is the one row in this family an UPDATE touches;
    /// every other durable log table is insert-only.
    async fn close_batch(&self, closes: &[MatchClose]) -> AppResult<usize>;

    /// Stamp a script-supplied result on a still-open match.
    async fn set_result(&self, match_id: &str, result_json: &str) -> AppResult<bool>;

    async fn get(&self, match_id: &str) -> AppResult<Option<MatchRecord>>;

    /// Keyset over `match_id` DESC — newest first, since ids are time-ordered.
    async fn list(
        &self,
        after_match_id: Option<&str>,
        limit: usize,
        open_only: bool,
    ) -> AppResult<Vec<MatchRecord>>;

    /// Oldest-first bounded prune. Only closed matches are eligible.
    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize>;
}

#[derive(Clone)]
pub struct SqliteMatchRepository {
    pool: SqlitePool,
}

impl SqliteMatchRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Clone)]
pub struct PgMatchRepository {
    pool: PgPool,
}

impl PgMatchRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db(error: sqlx::Error) -> AppError {
    AppError::database("match record persistence failed").with_detail(error.to_string())
}

// `clamp` already bounds both values far inside `i64`, so the fallbacks below
// are unreachable and deliberately conservative rather than saturating.
fn page_limit(limit: usize) -> i64 {
    i64::try_from(limit.clamp(1, MAX_PAGE_LIMIT)).unwrap_or(1)
}

fn prune_limit(limit: usize) -> i64 {
    i64::try_from(limit.clamp(1, MAX_PRUNE_LIMIT)).unwrap_or(1)
}

fn rows_per_chunk() -> usize {
    (MAX_BIND_PARAMS / OPEN_COLUMN_COUNT).max(1)
}

fn optional_i64(value: Option<u64>, what: &'static str) -> AppResult<Option<i64>> {
    value.map(|value| sql_i64(value, what)).transpose()
}

fn optional_u64(value: Option<i64>, what: &'static str) -> AppResult<Option<u64>> {
    value.map(|value| sql_u64(value, what)).transpose()
}

fn count_i32(value: u32, what: &'static str) -> AppResult<i32> {
    i32::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

fn count_u32(value: i32, what: &'static str) -> AppResult<u32> {
    u32::try_from(value).map_err(|_| AppError::internal(format!("{what} is negative")))
}

/// The projection every read of this table shares. PostgreSQL renders
/// `result_json` as text so both dialects decode the column identically.
fn select_columns(json_as_text: bool) -> String {
    let result = if json_as_text {
        "result_json::text AS result_json"
    } else {
        "result_json"
    };
    format!(
        "match_id,node_id,boot_id,room_id,name,map,mode,max_players,script_revision_id,\
         script_generation,clock_epoch,opened_at_ms,closed_at_ms,termination_reason,\
         peak_participants,join_total,{result}"
    )
}

/// Row decoding shared by both adapters.
///
/// A macro rather than a function: `PgRow` and `SqliteRow` are distinct types
/// with no common object-safe supertrait, and spelling the generic `sqlx::Row`
/// bounds for seventeen columns costs more than it explains.
macro_rules! decode_match_record {
    ($row:expr) => {{
        let row = $row;
        (|| {
            Ok(MatchRecord {
                match_id: row.try_get::<String, _>("match_id").map_err(db)?,
                node_id: row.try_get::<String, _>("node_id").map_err(db)?,
                boot_id: row.try_get::<String, _>("boot_id").map_err(db)?,
                room_id: sql_u64(
                    row.try_get::<i64, _>("room_id").map_err(db)?,
                    "match room id",
                )?,
                name: row.try_get::<Option<String>, _>("name").map_err(db)?,
                map: row.try_get::<String, _>("map").map_err(db)?,
                mode: row.try_get::<String, _>("mode").map_err(db)?,
                max_players: u16::try_from(row.try_get::<i32, _>("max_players").map_err(db)?)
                    .map_err(|_| AppError::internal("match max players out of range"))?,
                script_revision_id: row
                    .try_get::<Option<String>, _>("script_revision_id")
                    .map_err(db)?,
                script_generation: optional_u64(
                    row.try_get::<Option<i64>, _>("script_generation")
                        .map_err(db)?,
                    "match script generation",
                )?,
                clock_epoch: sql_u64(
                    row.try_get::<i64, _>("clock_epoch").map_err(db)?,
                    "match clock epoch",
                )?,
                opened_at_ms: sql_u64(
                    row.try_get::<i64, _>("opened_at_ms").map_err(db)?,
                    "match open time",
                )?,
                closed_at_ms: optional_u64(
                    row.try_get::<Option<i64>, _>("closed_at_ms").map_err(db)?,
                    "match close time",
                )?,
                termination_reason: row
                    .try_get::<Option<String>, _>("termination_reason")
                    .map_err(db)?,
                peak_participants: count_u32(
                    row.try_get::<i32, _>("peak_participants").map_err(db)?,
                    "match peak participants",
                )?,
                join_total: count_u32(
                    row.try_get::<i32, _>("join_total").map_err(db)?,
                    "match join total",
                )?,
                result_json: row
                    .try_get::<Option<String>, _>("result_json")
                    .map_err(db)?,
            })
        })()
    }};
}

#[async_trait]
impl DurableMatchRepository for SqliteMatchRepository {
    async fn open_batch(&self, opens: &[MatchOpen]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in opens.chunks(rows_per_chunk()) {
            let placeholders = vec!["(?,?,?,?,?,?,?,?,?,?,?,?)"; chunk.len()].join(",");
            // Targeted `DO NOTHING`, not `INSERT OR IGNORE`: the latter also
            // swallows a CHECK violation, so a malformed row would vanish
            // instead of failing the flush.
            let sql = format!(
                "INSERT INTO matches ({OPEN_COLUMNS}) VALUES {placeholders} ON CONFLICT DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for open in chunk {
                query = query
                    .bind(&open.match_id)
                    .bind(&open.node_id)
                    .bind(&open.boot_id)
                    .bind(sql_i64(open.room_id, "match room id")?)
                    .bind(open.name.as_deref())
                    .bind(&open.map)
                    .bind(&open.mode)
                    .bind(i32::from(open.max_players))
                    .bind(open.script_revision_id.as_deref())
                    .bind(optional_i64(
                        open.script_generation,
                        "match script generation",
                    )?)
                    .bind(sql_i64(open.clock_epoch, "match clock epoch")?)
                    .bind(sql_i64(open.opened_at_ms, "match open time")?);
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn close_batch(&self, closes: &[MatchClose]) -> AppResult<usize> {
        let mut closed = 0_usize;
        // One statement per close: a match ends exactly once, so a flush carries
        // a handful of these, not a stream.
        for close in closes {
            let affected = sqlx::query(
                "UPDATE matches SET closed_at_ms=?, termination_reason=?, peak_participants=?, \
                 join_total=?, result_json=COALESCE(?, result_json) \
                 WHERE match_id=? AND closed_at_ms IS NULL",
            )
            .bind(sql_i64(close.closed_at_ms, "match close time")?)
            .bind(&close.termination_reason)
            .bind(count_i32(
                close.peak_participants,
                "match peak participants",
            )?)
            .bind(count_i32(close.join_total, "match join total")?)
            .bind(close.result_json.as_deref())
            .bind(&close.match_id)
            .execute(&self.pool)
            .await
            .map_err(db)?
            .rows_affected();
            closed += usize::try_from(affected).unwrap_or(0);
        }
        Ok(closed)
    }

    async fn set_result(&self, match_id: &str, result_json: &str) -> AppResult<bool> {
        let affected = sqlx::query(
            "UPDATE matches SET result_json=? WHERE match_id=? AND closed_at_ms IS NULL",
        )
        .bind(result_json)
        .bind(match_id)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn get(&self, match_id: &str) -> AppResult<Option<MatchRecord>> {
        let columns = select_columns(false);
        let row = sqlx::query(&format!("SELECT {columns} FROM matches WHERE match_id=?"))
            .bind(match_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|row| decode_match_record!(&row)).transpose()
    }

    async fn list(
        &self,
        after_match_id: Option<&str>,
        limit: usize,
        open_only: bool,
    ) -> AppResult<Vec<MatchRecord>> {
        let columns = select_columns(false);
        let rows = sqlx::query(&format!(
            "SELECT {columns} FROM matches \
             WHERE (? IS NULL OR match_id < ?) AND (?=0 OR closed_at_ms IS NULL) \
             ORDER BY match_id DESC LIMIT ?"
        ))
        .bind(after_match_id)
        .bind(after_match_id)
        .bind(i64::from(open_only))
        .bind(page_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_match_record!(row)).collect()
    }

    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM matches WHERE match_id IN (\
                 SELECT match_id FROM matches \
                  WHERE closed_at_ms IS NOT NULL AND closed_at_ms < ? \
                  ORDER BY match_id LIMIT ?)",
        )
        .bind(sql_i64(closed_before.unix_millis(), "match prune horizon")?)
        .bind(prune_limit(limit))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(usize::try_from(affected).unwrap_or(0))
    }
}

#[async_trait]
impl DurableMatchRepository for PgMatchRepository {
    async fn open_batch(&self, opens: &[MatchOpen]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in opens.chunks(rows_per_chunk()) {
            let mut next = 1_usize;
            let placeholders = chunk
                .iter()
                .map(|_| {
                    let row = (next..next + OPEN_COLUMN_COUNT)
                        .map(|index| format!("${index}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    next += OPEN_COLUMN_COUNT;
                    format!("({row})")
                })
                .collect::<Vec<_>>()
                .join(",");
            // Untargeted `DO NOTHING`: this table carries a primary key *and*
            // the (node_id, boot_id, room_id) uniqueness, and a retried flush
            // can arbitrate on either index.
            let sql = format!(
                "INSERT INTO matches ({OPEN_COLUMNS}) VALUES {placeholders} ON CONFLICT DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for open in chunk {
                query = query
                    .bind(&open.match_id)
                    .bind(&open.node_id)
                    .bind(&open.boot_id)
                    .bind(sql_i64(open.room_id, "match room id")?)
                    .bind(open.name.as_deref())
                    .bind(&open.map)
                    .bind(&open.mode)
                    .bind(i32::from(open.max_players))
                    .bind(open.script_revision_id.as_deref())
                    .bind(optional_i64(
                        open.script_generation,
                        "match script generation",
                    )?)
                    .bind(sql_i64(open.clock_epoch, "match clock epoch")?)
                    .bind(sql_i64(open.opened_at_ms, "match open time")?);
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn close_batch(&self, closes: &[MatchClose]) -> AppResult<usize> {
        let mut closed = 0_usize;
        for close in closes {
            let affected = sqlx::query(
                "UPDATE matches SET closed_at_ms=$1, termination_reason=$2, peak_participants=$3, \
                 join_total=$4, result_json=COALESCE($5::jsonb, result_json) \
                 WHERE match_id=$6 AND closed_at_ms IS NULL",
            )
            .bind(sql_i64(close.closed_at_ms, "match close time")?)
            .bind(&close.termination_reason)
            .bind(count_i32(
                close.peak_participants,
                "match peak participants",
            )?)
            .bind(count_i32(close.join_total, "match join total")?)
            .bind(close.result_json.as_deref())
            .bind(&close.match_id)
            .execute(&self.pool)
            .await
            .map_err(db)?
            .rows_affected();
            closed += usize::try_from(affected).unwrap_or(0);
        }
        Ok(closed)
    }

    async fn set_result(&self, match_id: &str, result_json: &str) -> AppResult<bool> {
        let affected = sqlx::query(
            "UPDATE matches SET result_json=$1::jsonb WHERE match_id=$2 AND closed_at_ms IS NULL",
        )
        .bind(result_json)
        .bind(match_id)
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(affected > 0)
    }

    async fn get(&self, match_id: &str) -> AppResult<Option<MatchRecord>> {
        let columns = select_columns(true);
        let row = sqlx::query(&format!("SELECT {columns} FROM matches WHERE match_id=$1"))
            .bind(match_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|row| decode_match_record!(&row)).transpose()
    }

    async fn list(
        &self,
        after_match_id: Option<&str>,
        limit: usize,
        open_only: bool,
    ) -> AppResult<Vec<MatchRecord>> {
        let columns = select_columns(true);
        let rows = sqlx::query(&format!(
            "SELECT {columns} FROM matches \
             WHERE ($1::text IS NULL OR match_id < $1) AND (NOT $2 OR closed_at_ms IS NULL) \
             ORDER BY match_id DESC LIMIT $3"
        ))
        .bind(after_match_id)
        .bind(open_only)
        .bind(page_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_match_record!(row)).collect()
    }

    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM matches WHERE match_id IN (\
                 SELECT match_id FROM matches \
                  WHERE closed_at_ms IS NOT NULL AND closed_at_ms < $1 \
                  ORDER BY match_id LIMIT $2)",
        )
        .bind(sql_i64(closed_before.unix_millis(), "match prune horizon")?)
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
    use crate::error::ErrorCategory;

    async fn repository() -> SqliteMatchRepository {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260824090000_create_matches.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        SqliteMatchRepository::new(pool)
    }

    fn open(index: u64) -> MatchOpen {
        MatchOpen {
            match_id: format!("mt1-{index:029x}"),
            node_id: "node-a".to_string(),
            boot_id: "bt1-0".to_string(),
            room_id: index,
            name: Some(format!("room {index}")),
            map: "arena".to_string(),
            mode: "deathmatch".to_string(),
            max_players: 8,
            script_revision_id: None,
            script_generation: None,
            clock_epoch: 7,
            opened_at_ms: 1_000 + index,
        }
    }

    fn close(index: u64, at_ms: u64, reason: &str) -> MatchClose {
        MatchClose {
            match_id: open(index).match_id,
            closed_at_ms: at_ms,
            termination_reason: reason.to_string(),
            peak_participants: 4,
            join_total: 9,
            result_json: None,
        }
    }

    #[tokio::test]
    async fn sqlite_open_is_idempotent_and_close_is_single_shot() {
        let repository = repository().await;
        assert_eq!(repository.open_batch(&[open(1)]).await.expect("open"), 1);
        // A retried flush re-sends the whole batch: the row must not duplicate
        // and the statement must not fail.
        assert_eq!(repository.open_batch(&[open(1)]).await.expect("retry"), 0);

        let ended = close(1, 2_000, "final_departure");
        assert_eq!(
            repository
                .close_batch(std::slice::from_ref(&ended))
                .await
                .expect("close"),
            1
        );
        assert_eq!(
            repository.close_batch(&[ended]).await.expect("re-close"),
            0,
            "a second close is a no-op"
        );
        let stored = repository
            .get(&open(1).match_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(stored.closed_at_ms, Some(2_000));
        assert_eq!(stored.peak_participants, 4);
        assert_eq!(stored.join_total, 9);
        assert_eq!(stored.room_id, 1);
        assert_eq!(stored.max_players, 8);
        assert_eq!(stored.name.as_deref(), Some("room 1"));
    }

    #[tokio::test]
    async fn sqlite_set_result_only_applies_to_an_open_match_and_survives_close() {
        let repository = repository().await;
        repository.open_batch(&[open(1)]).await.expect("open");
        let match_id = open(1).match_id;
        assert!(
            repository
                .set_result(&match_id, "{\"winner\":\"kitsune\"}")
                .await
                .expect("stamp")
        );
        // A close carrying no result must not erase a stamped one.
        repository
            .close_batch(&[close(1, 2_000, "server_closed")])
            .await
            .expect("close");
        let stored = repository
            .get(&match_id)
            .await
            .expect("get")
            .expect("record");
        assert_eq!(
            stored.result_json.as_deref(),
            Some("{\"winner\":\"kitsune\"}")
        );
        assert!(
            !repository
                .set_result(&match_id, "{\"winner\":\"okami\"}")
                .await
                .expect("stamp after close"),
            "a closed match no longer accepts a result"
        );
    }

    #[tokio::test]
    async fn sqlite_list_pages_newest_first_and_filters_open_matches() {
        let repository = repository().await;
        for index in 1..=5 {
            repository.open_batch(&[open(index)]).await.expect("open");
        }
        repository
            .close_batch(&[close(5, 9_000, "formation_abandoned")])
            .await
            .expect("close");

        let first = repository.list(None, 2, false).await.expect("page");
        assert_eq!(
            first
                .iter()
                .map(|record| record.room_id)
                .collect::<Vec<_>>(),
            vec![5, 4]
        );
        let next = repository
            .list(Some(&first[1].match_id), 2, false)
            .await
            .expect("next page");
        assert_eq!(
            next.iter().map(|record| record.room_id).collect::<Vec<_>>(),
            vec![3, 2]
        );
        let open_only = repository.list(None, 10, true).await.expect("open only");
        assert_eq!(
            open_only
                .iter()
                .map(|record| record.room_id)
                .collect::<Vec<_>>(),
            vec![4, 3, 2, 1]
        );
    }

    #[tokio::test]
    async fn sqlite_prune_is_bounded_and_spares_open_matches() {
        let repository = repository().await;
        for index in 1..=4 {
            repository.open_batch(&[open(index)]).await.expect("open");
        }
        for index in 1..=3 {
            repository
                .close_batch(&[close(index, 100 + index, "final_departure")])
                .await
                .expect("close");
        }
        let removed = repository
            .prune(TimestampMillis::from_unix_millis(1_000), 2)
            .await
            .expect("prune");
        assert_eq!(removed, 2, "prune honours its batch bound");
        let remaining = repository.list(None, 10, false).await.expect("list");
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining.iter().any(|record| record.closed_at_ms.is_none()),
            "an open match is never pruned"
        );
    }

    #[tokio::test]
    async fn sqlite_rejects_a_termination_reason_outside_the_lifecycle_vocabulary() {
        let repository = repository().await;
        repository.open_batch(&[open(1)]).await.expect("open");
        let error = repository
            .close_batch(&[close(1, 2_000, "because")])
            .await
            .expect_err("check constraint");
        assert_eq!(error.category(), ErrorCategory::Database);
    }

    #[tokio::test]
    async fn sqlite_open_batch_spans_more_rows_than_one_bind_chunk() {
        let repository = repository().await;
        let opens = (1..=200_u64).map(open).collect::<Vec<_>>();
        assert_eq!(
            repository.open_batch(&opens).await.expect("bulk open"),
            200,
            "a batch wider than the bind ceiling is chunked, not truncated"
        );
    }
}
