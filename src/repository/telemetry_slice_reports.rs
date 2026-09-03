//! Durable authoritative-telemetry slice reports. Aggregate-only: never marker
//! text, payloads, participant or account identity, replies, commands,
//! corrected values, or the recorder's `pub(crate)` decision sequence.
//!
//! `match_id` is the durable server-minted match identity resolved inside the
//! sink. The raw process-local room correlation never reaches this table and
//! never reaches an operator response.

use async_trait::async_trait;
use sqlx::{PgPool, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::ids::{sql_i64, sql_u64};
use crate::time::TimestampMillis;

const COLUMNS: &str = "report_id,node_id,match_id,context_kind,close_reason,closed_at_ms,\
     duration_ms,marker_total,truncated,accepted_total,rejected_total,corrected_total";
const COLUMN_COUNT: usize = 12;
/// Chunking ceiling: older SQLite builds cap `SQLITE_MAX_VARIABLE_NUMBER` at
/// 999, so a multi-row insert stays under 900 bind parameters on every build.
const MAX_BIND_PARAMS: usize = 900;

/// Widest page a caller may request; the console over-fetches `limit + 1`.
const MAX_PAGE_LIMIT: usize = 201;
/// Widest single prune batch.
const MAX_PRUNE_LIMIT: usize = 1_000;

/// One closed slice, as aggregates only.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DurableSliceRow {
    pub report_id: String,
    pub node_id: String,
    /// `None` for a slice closed outside any match.
    pub match_id: Option<String>,
    pub context_kind: String,
    pub close_reason: String,
    pub closed_at_ms: u64,
    pub duration_ms: u64,
    /// How many markers the slice saw. The marker text itself is validated,
    /// used, and discarded — there is no column for it and there never will be.
    pub marker_total: u32,
    pub truncated: bool,
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub corrected_total: u64,
}

#[async_trait]
pub trait DurableTelemetrySliceRepository: Send + Sync {
    /// Idempotent insert: a retried flush re-sends the whole batch.
    async fn insert_batch(&self, rows: &[DurableSliceRow]) -> AppResult<usize>;
    async fn get(&self, report_id: &str) -> AppResult<Option<DurableSliceRow>>;
    /// Keyset over `report_id` DESC — newest first, since ids are time-ordered.
    /// `match_id` of `None` matches all rows, including unscoped ones.
    async fn list(
        &self,
        match_id: Option<&str>,
        after_report_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<DurableSliceRow>>;
    async fn count(&self, match_id: Option<&str>) -> AppResult<u64>;
    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize>;
}

#[derive(Clone)]
pub struct SqliteTelemetrySliceRepository {
    pool: SqlitePool,
}

impl SqliteTelemetrySliceRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Clone)]
pub struct PgTelemetrySliceRepository {
    pool: PgPool,
}

impl PgTelemetrySliceRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db(error: sqlx::Error) -> AppError {
    AppError::database("telemetry slice persistence failed").with_detail(error.to_string())
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

fn marker_total_i32(value: u32) -> AppResult<i32> {
    i32::try_from(value).map_err(|_| AppError::internal("slice marker total out of range"))
}

fn marker_total_u32(value: i32) -> AppResult<u32> {
    u32::try_from(value).map_err(|_| AppError::internal("slice marker total is negative"))
}

/// Row decoding shared by both adapters. A macro because `PgRow` and
/// `SqliteRow` share no object-safe supertrait to write this against.
macro_rules! decode_slice_row {
    ($row:expr) => {{
        let row = $row;
        (|| {
            Ok(DurableSliceRow {
                report_id: row.try_get::<String, _>("report_id").map_err(db)?,
                node_id: row.try_get::<String, _>("node_id").map_err(db)?,
                match_id: row.try_get::<Option<String>, _>("match_id").map_err(db)?,
                context_kind: row.try_get::<String, _>("context_kind").map_err(db)?,
                close_reason: row.try_get::<String, _>("close_reason").map_err(db)?,
                closed_at_ms: sql_u64(
                    row.try_get::<i64, _>("closed_at_ms").map_err(db)?,
                    "slice close time",
                )?,
                duration_ms: sql_u64(
                    row.try_get::<i64, _>("duration_ms").map_err(db)?,
                    "slice duration",
                )?,
                marker_total: marker_total_u32(row.try_get::<i32, _>("marker_total").map_err(db)?)?,
                truncated: row.try_get::<bool, _>("truncated").map_err(db)?,
                accepted_total: sql_u64(
                    row.try_get::<i64, _>("accepted_total").map_err(db)?,
                    "slice accepted total",
                )?,
                rejected_total: sql_u64(
                    row.try_get::<i64, _>("rejected_total").map_err(db)?,
                    "slice rejected total",
                )?,
                corrected_total: sql_u64(
                    row.try_get::<i64, _>("corrected_total").map_err(db)?,
                    "slice corrected total",
                )?,
            })
        })()
    }};
}

#[async_trait]
impl DurableTelemetrySliceRepository for SqliteTelemetrySliceRepository {
    async fn insert_batch(&self, rows: &[DurableSliceRow]) -> AppResult<usize> {
        let mut written = 0_usize;
        for chunk in rows.chunks(rows_per_chunk()) {
            let placeholders = vec!["(?,?,?,?,?,?,?,?,?,?,?,?)"; chunk.len()].join(",");
            // Targeted `DO NOTHING`, not `INSERT OR IGNORE`: the latter also
            // swallows a CHECK violation, so a close reason outside the
            // recorder vocabulary would vanish instead of failing the flush.
            let sql = format!(
                "INSERT INTO telemetry_slice_reports ({COLUMNS}) VALUES {placeholders} 
                 ON CONFLICT (report_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for row in chunk {
                query = query
                    .bind(&row.report_id)
                    .bind(&row.node_id)
                    .bind(row.match_id.as_deref())
                    .bind(&row.context_kind)
                    .bind(&row.close_reason)
                    .bind(sql_i64(row.closed_at_ms, "slice close time")?)
                    .bind(sql_i64(row.duration_ms, "slice duration")?)
                    .bind(marker_total_i32(row.marker_total)?)
                    .bind(row.truncated)
                    .bind(sql_i64(row.accepted_total, "slice accepted total")?)
                    .bind(sql_i64(row.rejected_total, "slice rejected total")?)
                    .bind(sql_i64(row.corrected_total, "slice corrected total")?);
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn get(&self, report_id: &str) -> AppResult<Option<DurableSliceRow>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM telemetry_slice_reports WHERE report_id=?"
        ))
        .bind(report_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|row| decode_slice_row!(&row)).transpose()
    }

    async fn list(
        &self,
        match_id: Option<&str>,
        after_report_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<DurableSliceRow>> {
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM telemetry_slice_reports \
             WHERE (? IS NULL OR match_id = ?) AND (? IS NULL OR report_id < ?) \
             ORDER BY report_id DESC LIMIT ?"
        ))
        .bind(match_id)
        .bind(match_id)
        .bind(after_report_id)
        .bind(after_report_id)
        .bind(page_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_slice_row!(row)).collect()
    }

    async fn count(&self, match_id: Option<&str>) -> AppResult<u64> {
        let total = sqlx::query(
            "SELECT COUNT(*) AS total FROM telemetry_slice_reports \
             WHERE (? IS NULL OR match_id = ?)",
        )
        .bind(match_id)
        .bind(match_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?
        .try_get::<i64, _>("total")
        .map_err(db)?;
        sql_u64(total, "slice report count")
    }

    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM telemetry_slice_reports WHERE report_id IN (\
                 SELECT report_id FROM telemetry_slice_reports WHERE closed_at_ms < ? \
                  ORDER BY report_id LIMIT ?)",
        )
        .bind(sql_i64(closed_before.unix_millis(), "slice prune horizon")?)
        .bind(prune_limit(limit))
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();
        Ok(usize::try_from(affected).unwrap_or(0))
    }
}

#[async_trait]
impl DurableTelemetrySliceRepository for PgTelemetrySliceRepository {
    async fn insert_batch(&self, rows: &[DurableSliceRow]) -> AppResult<usize> {
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
                "INSERT INTO telemetry_slice_reports ({COLUMNS}) VALUES {placeholders} \
                 ON CONFLICT (report_id) DO NOTHING"
            );
            let mut query = sqlx::query(&sql);
            for row in chunk {
                query = query
                    .bind(&row.report_id)
                    .bind(&row.node_id)
                    .bind(row.match_id.as_deref())
                    .bind(&row.context_kind)
                    .bind(&row.close_reason)
                    .bind(sql_i64(row.closed_at_ms, "slice close time")?)
                    .bind(sql_i64(row.duration_ms, "slice duration")?)
                    .bind(marker_total_i32(row.marker_total)?)
                    .bind(row.truncated)
                    .bind(sql_i64(row.accepted_total, "slice accepted total")?)
                    .bind(sql_i64(row.rejected_total, "slice rejected total")?)
                    .bind(sql_i64(row.corrected_total, "slice corrected total")?);
            }
            let affected = query.execute(&self.pool).await.map_err(db)?.rows_affected();
            written += usize::try_from(affected).unwrap_or(0);
        }
        Ok(written)
    }

    async fn get(&self, report_id: &str) -> AppResult<Option<DurableSliceRow>> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM telemetry_slice_reports WHERE report_id=$1"
        ))
        .bind(report_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|row| decode_slice_row!(&row)).transpose()
    }

    async fn list(
        &self,
        match_id: Option<&str>,
        after_report_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<DurableSliceRow>> {
        // Every nullable predicate parameter carries an explicit `::text`: without
        // it sqlx cannot infer the type of a `NULL` bind and the query fails.
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM telemetry_slice_reports \
             WHERE ($1::text IS NULL OR match_id = $1) \
               AND ($2::text IS NULL OR report_id < $2) \
             ORDER BY report_id DESC LIMIT $3"
        ))
        .bind(match_id)
        .bind(after_report_id)
        .bind(page_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(|row| decode_slice_row!(row)).collect()
    }

    async fn count(&self, match_id: Option<&str>) -> AppResult<u64> {
        let total = sqlx::query(
            "SELECT COUNT(*) AS total FROM telemetry_slice_reports \
             WHERE ($1::text IS NULL OR match_id = $1)",
        )
        .bind(match_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db)?
        .try_get::<i64, _>("total")
        .map_err(db)?;
        sql_u64(total, "slice report count")
    }

    async fn prune(&self, closed_before: TimestampMillis, limit: usize) -> AppResult<usize> {
        let affected = sqlx::query(
            "DELETE FROM telemetry_slice_reports WHERE report_id IN (\
                 SELECT report_id FROM telemetry_slice_reports WHERE closed_at_ms < $1 \
                  ORDER BY report_id LIMIT $2)",
        )
        .bind(sql_i64(closed_before.unix_millis(), "slice prune horizon")?)
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

    async fn repository() -> SqliteTelemetrySliceRepository {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260824093000_create_telemetry_slice_reports.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        SqliteTelemetrySliceRepository::new(pool)
    }

    fn slice(index: u64, match_id: Option<&str>, close_reason: &str) -> DurableSliceRow {
        DurableSliceRow {
            report_id: format!("ats1-{index:029x}"),
            node_id: "node-a".to_string(),
            match_id: match_id.map(str::to_string),
            context_kind: "match".to_string(),
            close_reason: close_reason.to_string(),
            closed_at_ms: 1_000 + index,
            duration_ms: 500,
            marker_total: 3,
            truncated: index.is_multiple_of(2),
            accepted_total: 10,
            rejected_total: 1,
            corrected_total: 2,
        }
    }

    #[tokio::test]
    async fn sqlite_round_trips_every_aggregate_and_is_idempotent() {
        let repository = repository().await;
        let row = slice(2, Some("mt1-a"), "ttl");
        assert_eq!(
            repository
                .insert_batch(std::slice::from_ref(&row))
                .await
                .expect("insert"),
            1
        );
        assert_eq!(
            repository
                .insert_batch(std::slice::from_ref(&row))
                .await
                .expect("retry"),
            0
        );
        let stored = repository
            .get(&row.report_id)
            .await
            .expect("get")
            .expect("row");
        assert_eq!(stored, row);
        assert!(stored.truncated);
    }

    #[tokio::test]
    async fn sqlite_stores_a_slice_closed_outside_any_match() {
        let repository = repository().await;
        let row = slice(1, None, "finished");
        repository
            .insert_batch(std::slice::from_ref(&row))
            .await
            .expect("insert");
        assert_eq!(repository.count(None).await.expect("count"), 1);
        assert_eq!(
            repository.count(Some("mt1-a")).await.expect("scoped count"),
            0
        );
    }

    #[tokio::test]
    async fn sqlite_filters_by_match_and_pages_newest_first() {
        let repository = repository().await;
        repository
            .insert_batch(&[
                slice(1, Some("mt1-a"), "ttl"),
                slice(2, Some("mt1-b"), "ttl"),
                slice(3, Some("mt1-a"), "active_cap"),
                slice(4, Some("mt1-a"), "marker_cap"),
                slice(5, None, "restarted"),
            ])
            .await
            .expect("insert");
        let scoped = repository
            .list(Some("mt1-a"), None, 2)
            .await
            .expect("first page");
        assert_eq!(
            scoped
                .iter()
                .map(|row| row.close_reason.as_str())
                .collect::<Vec<_>>(),
            vec!["marker_cap", "active_cap"]
        );
        let next = repository
            .list(Some("mt1-a"), Some(&scoped[1].report_id), 2)
            .await
            .expect("next page");
        assert_eq!(
            next.iter()
                .map(|row| row.close_reason.as_str())
                .collect::<Vec<_>>(),
            vec!["ttl"]
        );
        assert_eq!(repository.count(Some("mt1-a")).await.expect("count"), 3);
        assert_eq!(repository.count(None).await.expect("count all"), 5);
    }

    #[tokio::test]
    async fn sqlite_rejects_a_close_reason_outside_the_recorder_vocabulary() {
        let repository = repository().await;
        let error = repository
            .insert_batch(&[slice(1, None, "operator_looked_at_it")])
            .await
            .expect_err("check constraint");
        assert_eq!(error.category(), ErrorCategory::Database);
    }

    #[tokio::test]
    async fn sqlite_prune_is_bounded_and_oldest_first() {
        let repository = repository().await;
        let rows = (1..=5_u64)
            .map(|index| slice(index, None, "ttl"))
            .collect::<Vec<_>>();
        repository.insert_batch(&rows).await.expect("insert");
        assert_eq!(
            repository
                .prune(TimestampMillis::from_unix_millis(10_000), 2)
                .await
                .expect("prune"),
            2
        );
        let remaining = repository.list(None, None, 10).await.expect("list");
        assert_eq!(remaining.len(), 3);
        assert_eq!(remaining[2].report_id, rows[2].report_id);
    }

    #[tokio::test]
    async fn sqlite_insert_batch_spans_more_rows_than_one_bind_chunk() {
        let repository = repository().await;
        let rows = (1..=150_u64)
            .map(|index| slice(index, Some("mt1-a"), "ttl"))
            .collect::<Vec<_>>();
        assert_eq!(
            repository.insert_batch(&rows).await.expect("bulk"),
            150,
            "a batch wider than the bind ceiling is chunked, not truncated"
        );
    }
}
