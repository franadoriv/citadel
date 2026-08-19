//! Durable, report-only persistence for lag diagnostics.
//!
//! This adapter deliberately stores one compact JSON report and never accepts a
//! raw artifact path, MIME, token, JTI, client filename, IP, user agent, or row
//! payload.  It is intentionally independent of `Backend`/`UnitOfWork` until a
//! console command needs to compose it with other domain mutations.

use async_trait::async_trait;
use sqlx::{PgPool, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::lag_analysis::{AnalysisIdentity, LagReport, LagReportCaptureOverview};

#[async_trait]
pub trait DurableLagReportRepository: Send + Sync {
    async fn get(&self, identity: &AnalysisIdentity) -> AppResult<Option<LagReport>>;
    async fn get_by_report_id(&self, report_id: &str) -> AppResult<Option<LagReport>>;
    /// Return the newest immutable report for an artifact regardless of its
    /// analyzer options. This preserves the regeneration chain after a node
    /// restart, when the in-memory worker cache is empty.
    async fn latest_for_artifact(
        &self,
        capture_id: &str,
        generation: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<Option<LagReport>>;
    async fn insert_immutable(
        &self,
        identity: &AnalysisIdentity,
        report: &LagReport,
    ) -> AppResult<LagReport>;
    async fn mark_raw_unavailable(
        &self,
        capture_id: &str,
        generation: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<()>;
    async fn list(&self, after_report_id: Option<&str>, limit: usize) -> AppResult<Vec<LagReport>>;
    async fn list_capture_overviews(
        &self,
        after_capture_id: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<LagReportCaptureOverview>>;
}

#[derive(Clone)]
pub struct SqliteLagReportRepository {
    pool: SqlitePool,
}

impl SqliteLagReportRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Clone)]
pub struct PgLagReportRepository {
    pool: PgPool,
}

impl PgLagReportRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db(error: sqlx::Error) -> AppError {
    AppError::database("lag report persistence failed").with_detail(error.to_string())
}
fn encode(report: &LagReport) -> AppResult<String> {
    serde_json::to_string(report).map_err(|e| {
        AppError::internal("lag report serialization failed").with_detail(e.to_string())
    })
}
fn decode(value: &str) -> AppResult<LagReport> {
    serde_json::from_str(value)
        .map_err(|e| AppError::internal("lag report row is invalid").with_detail(e.to_string()))
}
fn decode_projection(value: impl AsRef<str>, raw_available: bool) -> AppResult<LagReport> {
    let mut report = decode(value.as_ref())?;
    // This is current evidence availability, not a mutation of the immutable
    // derived result embedded in report_json.
    report.raw_available = raw_available;
    Ok(report)
}
fn generation(value: u64) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal("lag report generation out of range"))
}

fn capture_overview(report: LagReport, report_count: i64) -> AppResult<LagReportCaptureOverview> {
    Ok(LagReportCaptureOverview {
        capture_id: report.capture_id,
        generation: report.generation,
        report_count: u32::try_from(report_count)
            .map_err(|_| AppError::internal("lag report count out of range"))?,
        latest_report_status: report.status,
        latest_report_created_at: report.created_at,
    })
}

#[async_trait]
impl DurableLagReportRepository for SqliteLagReportRepository {
    async fn get(&self, identity: &AnalysisIdentity) -> AppResult<Option<LagReport>> {
        let row = sqlx::query("SELECT report_json, CASE WHEN raw_available=1 AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) THEN 1 ELSE 0 END AS raw_available FROM lag_diagnostic_reports WHERE capture_id=? AND generation=? AND artifact_digest_sha256=? AND analyzer_version=? AND options_hash=?")
            .bind(&identity.capture_id).bind(generation(identity.generation)?).bind(&identity.artifact_digest_sha256)
            .bind(i64::from(identity.analyzer_version)).bind(&identity.options_hash).fetch_optional(&self.pool).await.map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn get_by_report_id(&self, report_id: &str) -> AppResult<Option<LagReport>> {
        let row = sqlx::query(
            "SELECT report_json, CASE WHEN raw_available=1 AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) THEN 1 ELSE 0 END AS raw_available FROM lag_diagnostic_reports WHERE report_id=?",
        )
        .bind(report_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn latest_for_artifact(
        &self,
        capture_id: &str,
        generation_value: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<Option<LagReport>> {
        let row = sqlx::query(
            "SELECT report_json, CASE WHEN raw_available=1 AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) THEN 1 ELSE 0 END AS raw_available FROM lag_diagnostic_reports \
             WHERE capture_id=? AND generation=? AND artifact_digest_sha256=? \
             ORDER BY CAST(json_extract(report_json, '$.created_at') AS INTEGER) DESC, report_id DESC LIMIT 1",
        )
        .bind(capture_id)
        .bind(generation(generation_value)?)
        .bind(artifact_digest_sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn insert_immutable(
        &self,
        identity: &AnalysisIdentity,
        report: &LagReport,
    ) -> AppResult<LagReport> {
        let value = encode(report)?;
        let generation_value = generation(identity.generation)?;
        sqlx::query(
            "INSERT OR IGNORE INTO lag_diagnostic_reports \
             (report_id,capture_id,generation,artifact_digest_sha256,decoder_version,analyzer_version,options_hash,status,raw_available,report_json) \
             SELECT ?,?,?,?,?,?,?,?, \
                CASE WHEN EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones \
                    WHERE capture_id=? AND generation=? AND artifact_digest_sha256=?) THEN 0 ELSE ? END, ?",
        )
        .bind(&report.report_id)
        .bind(&identity.capture_id)
        .bind(generation_value)
        .bind(&identity.artifact_digest_sha256)
        .bind(i64::from(report.decoder_version))
        .bind(i64::from(identity.analyzer_version))
        .bind(&identity.options_hash)
        .bind(format!("{:?}", report.status))
        .bind(&identity.capture_id)
        .bind(generation_value)
        .bind(&identity.artifact_digest_sha256)
        .bind(report.raw_available)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        self.get(identity)
            .await?
            .ok_or_else(|| AppError::database("lag report insert did not persist"))
    }
    async fn mark_raw_unavailable(
        &self,
        capture_id: &str,
        generation_value: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<()> {
        let generation_value = generation(generation_value)?;
        sqlx::query(
            "INSERT OR IGNORE INTO lag_diagnostic_raw_tombstones (capture_id,generation,artifact_digest_sha256) VALUES (?,?,?)",
        )
        .bind(capture_id)
        .bind(generation_value)
        .bind(artifact_digest_sha256)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        sqlx::query(
            "UPDATE lag_diagnostic_reports SET raw_available=0 WHERE capture_id=? AND generation=? AND artifact_digest_sha256=?",
        )
        .bind(capture_id)
        .bind(generation_value)
        .bind(artifact_digest_sha256)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }
    async fn list(&self, after: Option<&str>, limit: usize) -> AppResult<Vec<LagReport>> {
        let rows = sqlx::query("SELECT report_json, CASE WHEN raw_available=1 AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) THEN 1 ELSE 0 END AS raw_available FROM lag_diagnostic_reports WHERE (? IS NULL OR report_id > ?) ORDER BY report_id LIMIT ?")
            .bind(after).bind(after).bind(i64::try_from(limit.clamp(1, 101)).unwrap_or(101)).fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                decode_projection(
                    row.try_get::<String, _>("report_json").map_err(db)?,
                    row.try_get::<bool, _>("raw_available").map_err(db)?,
                )
            })
            .collect()
    }
    async fn list_capture_overviews(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<LagReportCaptureOverview>> {
        // Page capture ids before joining report rows. A capture can have an
        // unbounded number of immutable regenerations, so applying a raw-row
        // limit first would make later capture ids disappear from the keyset.
        let rows = sqlx::query(
            "WITH page AS (\
                SELECT capture_id FROM lag_diagnostic_reports \
                WHERE (? IS NULL OR capture_id > ?) \
                GROUP BY capture_id ORDER BY capture_id LIMIT ?\
            ), ranked AS (\
                SELECT r.capture_id, r.report_json, CASE WHEN r.raw_available=1 AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=r.capture_id AND t.generation=r.generation AND t.artifact_digest_sha256=r.artifact_digest_sha256) THEN 1 ELSE 0 END AS raw_available, \
                    COUNT(*) OVER (PARTITION BY r.capture_id) AS report_count, \
                    ROW_NUMBER() OVER (PARTITION BY r.capture_id \
                        ORDER BY CAST(json_extract(r.report_json, '$.created_at') AS INTEGER) DESC, r.report_id DESC) AS row_number \
                FROM lag_diagnostic_reports r INNER JOIN page p ON p.capture_id = r.capture_id\
            ) \
            SELECT report_json, raw_available, report_count FROM ranked \
            WHERE row_number = 1 ORDER BY capture_id",
        )
        .bind(after)
        .bind(after)
        .bind(i64::try_from(limit.clamp(1, 101)).unwrap_or(101))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let report = decode_projection(
                    row.try_get::<String, _>("report_json").map_err(db)?,
                    row.try_get::<bool, _>("raw_available").map_err(db)?,
                )?;
                capture_overview(report, row.try_get::<i64, _>("report_count").map_err(db)?)
            })
            .collect()
    }
}

#[async_trait]
impl DurableLagReportRepository for PgLagReportRepository {
    async fn get(&self, identity: &AnalysisIdentity) -> AppResult<Option<LagReport>> {
        let row = sqlx::query("SELECT report_json::text AS report_json, raw_available AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) AS raw_available FROM lag_diagnostic_reports WHERE capture_id=$1 AND generation=$2 AND artifact_digest_sha256=$3 AND analyzer_version=$4 AND options_hash=$5")
            .bind(&identity.capture_id).bind(generation(identity.generation)?).bind(&identity.artifact_digest_sha256)
            .bind(i32::from(identity.analyzer_version)).bind(&identity.options_hash).fetch_optional(&self.pool).await.map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn get_by_report_id(&self, report_id: &str) -> AppResult<Option<LagReport>> {
        let row = sqlx::query("SELECT report_json::text AS report_json, raw_available AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) AS raw_available FROM lag_diagnostic_reports WHERE report_id=$1")
            .bind(report_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn latest_for_artifact(
        &self,
        capture_id: &str,
        generation_value: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<Option<LagReport>> {
        let row = sqlx::query(
            "SELECT report_json::text AS report_json, raw_available AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) AS raw_available FROM lag_diagnostic_reports \
             WHERE capture_id=$1 AND generation=$2 AND artifact_digest_sha256=$3 \
             ORDER BY (report_json ->> 'created_at')::BIGINT DESC, report_id DESC LIMIT 1",
        )
        .bind(capture_id)
        .bind(generation(generation_value)?)
        .bind(artifact_digest_sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        row.map(|row| {
            decode_projection(
                row.try_get::<String, _>("report_json").map_err(db)?,
                row.try_get::<bool, _>("raw_available").map_err(db)?,
            )
        })
        .transpose()
    }
    async fn insert_immutable(
        &self,
        identity: &AnalysisIdentity,
        report: &LagReport,
    ) -> AppResult<LagReport> {
        let value = encode(report)?;
        let generation_value = generation(identity.generation)?;
        sqlx::query(
            "INSERT INTO lag_diagnostic_reports \
             (report_id,capture_id,generation,artifact_digest_sha256,decoder_version,analyzer_version,options_hash,status,raw_available,report_json) \
             SELECT $1,$2,$3,$4,$5,$6,$7,$8, \
                CASE WHEN EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones \
                    WHERE capture_id=$9 AND generation=$10 AND artifact_digest_sha256=$11) THEN false ELSE $12 END, $13::jsonb \
             ON CONFLICT (capture_id,generation,artifact_digest_sha256,analyzer_version,options_hash) DO NOTHING",
        )
        .bind(&report.report_id)
        .bind(&identity.capture_id)
        .bind(generation_value)
        .bind(&identity.artifact_digest_sha256)
        .bind(i32::from(report.decoder_version))
        .bind(i32::from(identity.analyzer_version))
        .bind(&identity.options_hash)
        .bind(format!("{:?}", report.status))
        .bind(&identity.capture_id)
        .bind(generation_value)
        .bind(&identity.artifact_digest_sha256)
        .bind(report.raw_available)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        self.get(identity)
            .await?
            .ok_or_else(|| AppError::database("lag report insert did not persist"))
    }
    async fn mark_raw_unavailable(
        &self,
        capture_id: &str,
        generation_value: u64,
        artifact_digest_sha256: &str,
    ) -> AppResult<()> {
        let generation_value = generation(generation_value)?;
        sqlx::query("INSERT INTO lag_diagnostic_raw_tombstones (capture_id,generation,artifact_digest_sha256) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(capture_id).bind(generation_value).bind(artifact_digest_sha256).execute(&self.pool).await.map_err(db)?;
        sqlx::query("UPDATE lag_diagnostic_reports SET raw_available=false WHERE capture_id=$1 AND generation=$2 AND artifact_digest_sha256=$3")
            .bind(capture_id).bind(generation_value).bind(artifact_digest_sha256).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    async fn list(&self, after: Option<&str>, limit: usize) -> AppResult<Vec<LagReport>> {
        let rows = sqlx::query("SELECT report_json::text AS report_json, raw_available AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=lag_diagnostic_reports.capture_id AND t.generation=lag_diagnostic_reports.generation AND t.artifact_digest_sha256=lag_diagnostic_reports.artifact_digest_sha256) AS raw_available FROM lag_diagnostic_reports WHERE ($1::text IS NULL OR report_id > $1) ORDER BY report_id LIMIT $2")
            .bind(after).bind(i64::try_from(limit.clamp(1, 101)).unwrap_or(101)).fetch_all(&self.pool).await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                decode_projection(
                    row.try_get::<String, _>("report_json").map_err(db)?,
                    row.try_get::<bool, _>("raw_available").map_err(db)?,
                )
            })
            .collect()
    }
    async fn list_capture_overviews(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<LagReportCaptureOverview>> {
        let rows = sqlx::query(
            "WITH page AS (\
                SELECT capture_id FROM lag_diagnostic_reports \
                WHERE ($1::text IS NULL OR capture_id > $1) \
                GROUP BY capture_id ORDER BY capture_id LIMIT $2\
            ), ranked AS (\
                SELECT r.capture_id, r.report_json::text AS report_json, r.raw_available AND NOT EXISTS (SELECT 1 FROM lag_diagnostic_raw_tombstones t WHERE t.capture_id=r.capture_id AND t.generation=r.generation AND t.artifact_digest_sha256=r.artifact_digest_sha256) AS raw_available, \
                    COUNT(*) OVER (PARTITION BY r.capture_id) AS report_count, \
                    ROW_NUMBER() OVER (PARTITION BY r.capture_id \
                        ORDER BY (r.report_json ->> 'created_at')::BIGINT DESC, r.report_id DESC) AS row_number \
                FROM lag_diagnostic_reports r INNER JOIN page p ON p.capture_id = r.capture_id\
            ) \
            SELECT report_json, raw_available, report_count FROM ranked \
            WHERE row_number = 1 ORDER BY capture_id",
        )
        .bind(after)
        .bind(i64::try_from(limit.clamp(1, 101)).unwrap_or(101))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let report = decode_projection(
                    row.try_get::<String, _>("report_json").map_err(db)?,
                    row.try_get::<bool, _>("raw_available").map_err(db)?,
                )?;
                capture_overview(report, row.try_get::<i64, _>("report_count").map_err(db)?)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lag_analysis::{LagReportStatus, LagTimelineWindow, MetricQuality};
    use crate::time::TimestampMillis;

    fn identity() -> AnalysisIdentity {
        AnalysisIdentity {
            capture_id: "c".to_string(),
            generation: 1,
            artifact_digest_sha256: "d".repeat(64),
            analyzer_version: 1,
            options_hash: "o".repeat(64),
        }
    }

    fn report() -> LagReport {
        LagReport {
            report_id: "lr1-test".to_string(),
            capture_id: "c".to_string(),
            generation: 1,
            artifact_digest_sha256: "d".repeat(64),
            decoder_version: 1,
            analyzer_version: 1,
            options_hash: "o".repeat(64),
            status: LagReportStatus::Complete,
            raw_available: true,
            created_at: TimestampMillis::from_unix_millis(1),
            quality: MetricQuality {
                status: "complete".to_string(),
                sample_count: 3,
                excluded_count: 0,
                overwritten_count: 0,
                malformed_count: 0,
                clock_uncertain: false,
            },
            summaries: Vec::new(),
            windows: Vec::<LagTimelineWindow>::new(),
            supersedes_report_id: None,
        }
    }

    #[tokio::test]
    async fn sqlite_adapter_preserves_immutable_report_when_raw_expires() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819130000_create_lag_diagnostic_reports.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819131000_create_lag_diagnostic_raw_tombstones.sql"
        ))
        .execute(&pool)
        .await
        .expect("tombstone migration");
        let repository = SqliteLagReportRepository::new(pool);
        let key = identity();
        let saved = repository
            .insert_immutable(&key, &report())
            .await
            .expect("insert");
        let mut later_key = key.clone();
        later_key.options_hash = "n".repeat(64);
        let mut later = report();
        later.report_id = "lr1-later".to_string();
        later.options_hash = later_key.options_hash.clone();
        later.created_at = TimestampMillis::from_unix_millis(2);
        repository
            .insert_immutable(&later_key, &later)
            .await
            .expect("insert successor");
        let latest = repository
            .latest_for_artifact("c", 1, &key.artifact_digest_sha256)
            .await
            .expect("latest")
            .expect("successor");
        assert_eq!(latest.report_id, "lr1-later");
        repository
            .mark_raw_unavailable("c", 1, &key.artifact_digest_sha256)
            .await
            .expect("expiry");
        let current = repository.get(&key).await.expect("get").expect("report");
        assert_eq!(saved.status, LagReportStatus::Complete);
        assert_eq!(current.status, LagReportStatus::Complete);
        assert!(!current.raw_available);
        assert!(current.summaries.is_empty());
        let listed = repository.list(None, 10).await.expect("list");
        assert_eq!(listed.len(), 2);
        assert!(
            listed
                .iter()
                .all(|report| report.status == LagReportStatus::Complete)
        );
        assert!(listed.iter().all(|report| !report.raw_available));
    }

    #[tokio::test]
    async fn sqlite_adapter_projects_raw_availability_by_exact_digest() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819130000_create_lag_diagnostic_reports.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819131000_create_lag_diagnostic_raw_tombstones.sql"
        ))
        .execute(&pool)
        .await
        .expect("tombstone migration");
        let repository = SqliteLagReportRepository::new(pool);
        let first_key = identity();
        let first_report = report();
        repository
            .insert_immutable(&first_key, &first_report)
            .await
            .expect("first insert");
        let second_key = AnalysisIdentity {
            artifact_digest_sha256: "e".repeat(64),
            options_hash: "p".repeat(64),
            ..first_key.clone()
        };
        let second_report = LagReport {
            report_id: "lr1-other".to_string(),
            artifact_digest_sha256: second_key.artifact_digest_sha256.clone(),
            options_hash: second_key.options_hash.clone(),
            ..first_report
        };
        repository
            .insert_immutable(&second_key, &second_report)
            .await
            .expect("second insert");
        repository
            .mark_raw_unavailable("c", 1, &first_key.artifact_digest_sha256)
            .await
            .expect("project first raw expiry");
        // A worker that starts after retention/delete must not recreate a
        // `raw_available=true` projection for the tombstoned source.
        let after_delete_key = AnalysisIdentity {
            options_hash: "q".repeat(64),
            ..first_key.clone()
        };
        let after_delete_report = LagReport {
            report_id: "lr1-after-delete".to_string(),
            options_hash: after_delete_key.options_hash.clone(),
            ..report()
        };
        let inserted_after_delete = repository
            .insert_immutable(&after_delete_key, &after_delete_report)
            .await
            .expect("insert after deletion");
        assert!(!inserted_after_delete.raw_available);
        assert!(
            !repository
                .get(&first_key)
                .await
                .expect("first get")
                .expect("first report")
                .raw_available
        );
        assert!(
            repository
                .get(&second_key)
                .await
                .expect("second get")
                .expect("second report")
                .raw_available
        );
    }

    #[tokio::test]
    async fn sqlite_capture_keyset_is_not_truncated_by_many_regenerations() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819130000_create_lag_diagnostic_reports.sql"
        ))
        .execute(&pool)
        .await
        .expect("migration");
        sqlx::raw_sql(include_str!(
            "../../migrations-sqlite/20260819131000_create_lag_diagnostic_raw_tombstones.sql"
        ))
        .execute(&pool)
        .await
        .expect("tombstone migration");
        let repository = SqliteLagReportRepository::new(pool);

        for index in 0..102_u16 {
            let capture_id = format!("capture-{index:03}");
            // More than 64 immutable generations per earlier capture must not
            // consume the capture keyset page intended for later captures.
            let regenerations: u16 = if index < 100 { 65 } else { 1 };
            for revision in 0..regenerations {
                let mut report = report();
                report.report_id = format!("report-{index:03}-{revision:03}");
                report.capture_id = capture_id.clone();
                report.created_at =
                    TimestampMillis::from_unix_millis(u64::from(index) * 100 + u64::from(revision));
                let mut key = identity();
                key.capture_id = capture_id.clone();
                key.options_hash = format!("{index:032x}{revision:032x}");
                report.options_hash = key.options_hash.clone();
                repository
                    .insert_immutable(&key, &report)
                    .await
                    .expect("insert report");
            }
        }

        let first_page = repository
            .list_capture_overviews(None, 100)
            .await
            .expect("first page");
        assert_eq!(first_page.len(), 100);
        assert_eq!(first_page.last().expect("last").capture_id, "capture-099");
        let second_page = repository
            .list_capture_overviews(Some("capture-099"), 100)
            .await
            .expect("second page");
        assert_eq!(
            second_page
                .iter()
                .map(|capture| capture.capture_id.as_str())
                .collect::<Vec<_>>(),
            vec!["capture-100", "capture-101"]
        );
    }
}
