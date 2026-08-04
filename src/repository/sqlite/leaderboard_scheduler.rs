//! SQLite durable adapter for leaderboard reset scheduling.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::leaderboard_scheduler::{
    LeaderboardResetRepository, LeaderboardResetSnapshot, ResetEpoch, ResetOutboxRecord,
    SchedulerFencingToken, SchedulerLease,
};
use crate::repository::LeaderboardRecord;
use crate::time::{DurationMillis, TimestampMillis};

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

const SELECT_LEASE_SQL: &str = "\
SELECT node_id, fencing_token, expires_at_unix_ms \
FROM leaderboard_reset_scheduler_lease WHERE lease_key = 'leaderboards'";
const INSERT_LEASE_SQL: &str = "\
INSERT INTO leaderboard_reset_scheduler_lease \
(lease_key, node_id, fencing_token, expires_at_unix_ms) VALUES ('leaderboards', ?, ?, ?)";
const UPDATE_LEASE_SQL: &str = "\
UPDATE leaderboard_reset_scheduler_lease \
SET node_id = ?, fencing_token = ?, expires_at_unix_ms = ? WHERE lease_key = 'leaderboards'";
const SELECT_CURRENT_LEASE_SQL: &str = "\
SELECT fencing_token FROM leaderboard_reset_scheduler_lease \
WHERE lease_key = 'leaderboards' AND fencing_token = ? AND expires_at_unix_ms > ?";
const INSERT_EPOCH_SQL: &str = "\
INSERT INTO leaderboard_reset_epochs \
(leaderboard_id, due_at_unix_ms, fencing_token, claimed_at_unix_ms) \
VALUES (?, ?, ?, ?) ON CONFLICT (leaderboard_id, due_at_unix_ms) DO NOTHING";
const INSERT_OUTBOX_SQL: &str = "\
INSERT INTO leaderboard_reset_outbox \
(leaderboard_id, due_at_unix_ms, fencing_token, created_at_unix_ms) VALUES (?, ?, ?, ?)";
const SNAPSHOT_RECORDS_SQL: &str = "\
INSERT INTO leaderboard_reset_snapshot_records \
(leaderboard_id, due_at_unix_ms, owner_id, score, subscore, metadata, submissions, updated_at_unix_ms) \
SELECT ?, ?, owner_id, score, subscore, metadata, submissions, updated_at_unix_ms \
FROM leaderboard_records WHERE leaderboard_id = ?";
const DELETE_LIVE_RECORDS_SQL: &str = "DELETE FROM leaderboard_records WHERE leaderboard_id = ?";
const SELECT_SNAPSHOT_SQL: &str = "\
SELECT owner_id, score, subscore, metadata, submissions, updated_at_unix_ms \
FROM leaderboard_reset_snapshot_records \
WHERE leaderboard_id = ? AND due_at_unix_ms = ? ORDER BY owner_id";
const SELECT_EPOCH_SQL: &str = "\
SELECT 1 FROM leaderboard_reset_epochs WHERE leaderboard_id = ? AND due_at_unix_ms = ?";
const SELECT_OUTBOX_SQL: &str = "\
SELECT leaderboard_id, due_at_unix_ms, fencing_token FROM leaderboard_reset_outbox \
ORDER BY created_at_unix_ms, leaderboard_id, due_at_unix_ms LIMIT ?";
const DELETE_OUTBOX_SQL: &str = "\
DELETE FROM leaderboard_reset_outbox WHERE leaderboard_id = ? AND due_at_unix_ms = ?";

/// SQLite [`LeaderboardResetRepository`] backed by the scheduler tables.
pub struct SqliteLeaderboardResetRepository {
    executor: SqliteExecutor,
}

impl SqliteLeaderboardResetRepository {
    /// Bind the scheduler repository to an execution handle.
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_write_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                let result = {
                    let $conn = &mut *tx;
                    $body
                };
                match result {
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
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

macro_rules! with_conn {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl LeaderboardResetRepository for SqliteLeaderboardResetRepository {
    async fn acquire_lease(
        &self,
        node_id: &str,
        now: TimestampMillis,
        ttl: DurationMillis,
    ) -> AppResult<Option<SchedulerLease>> {
        with_write_tx!(self, conn => acquire_lease_conn(conn, node_id, now, ttl).await)
    }

    async fn claim_epoch(
        &self,
        epoch: ResetEpoch,
        token: SchedulerFencingToken,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_write_tx!(self, conn => claim_epoch_conn(conn, epoch, token, now).await)
    }

    async fn snapshot(&self, epoch: &ResetEpoch) -> AppResult<Option<LeaderboardResetSnapshot>> {
        with_conn!(self, conn => snapshot_conn(conn, epoch).await)
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<ResetOutboxRecord>> {
        with_conn!(self, conn => pending_outbox_conn(conn, limit).await)
    }

    async fn acknowledge_outbox(&self, epoch: &ResetEpoch) -> AppResult<()> {
        with_write_tx!(self, conn => acknowledge_outbox_conn(conn, epoch).await)
    }
}

fn token_to_i64(token: SchedulerFencingToken) -> AppResult<i64> {
    i64::try_from(token.get())
        .map_err(|_| AppError::internal("scheduler fencing token out of range"))
}

fn token_from_i64(token: i64) -> AppResult<SchedulerFencingToken> {
    u64::try_from(token)
        .map(SchedulerFencingToken::new)
        .map_err(|_| AppError::internal("invalid scheduler fencing token"))
}

fn parse_lease(row: &SqliteRow) -> AppResult<SchedulerLease> {
    Ok(SchedulerLease::new(
        get(row, "node_id")?,
        token_from_i64(get(row, "fencing_token")?)?,
        millis_to_ts(get(row, "expires_at_unix_ms")?)?,
    ))
}

async fn acquire_lease_conn(
    conn: &mut SqliteConnection,
    node_id: &str,
    now: TimestampMillis,
    ttl: DurationMillis,
) -> AppResult<Option<SchedulerLease>> {
    let expires_at = now.checked_add(ttl)?;
    let row = sqlx::query(SELECT_LEASE_SQL)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let Some(existing) = row.as_ref().map(parse_lease).transpose()? else {
        let lease = SchedulerLease::new(
            node_id.to_owned(),
            SchedulerFencingToken::new(1),
            expires_at,
        );
        sqlx::query(INSERT_LEASE_SQL)
            .bind(node_id)
            .bind(token_to_i64(lease.fencing_token)?)
            .bind(ts_to_millis(expires_at)?)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        return Ok(Some(lease));
    };
    if existing.is_current_at(now) && existing.node_id != node_id {
        return Ok(None);
    }
    let token = if existing.is_current_at(now) {
        existing.fencing_token
    } else {
        SchedulerFencingToken::new(
            existing
                .fencing_token
                .get()
                .checked_add(1)
                .ok_or_else(|| AppError::internal("scheduler fencing token overflowed"))?,
        )
    };
    let lease = SchedulerLease::new(node_id.to_owned(), token, expires_at);
    sqlx::query(UPDATE_LEASE_SQL)
        .bind(node_id)
        .bind(token_to_i64(token)?)
        .bind(ts_to_millis(expires_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(Some(lease))
}

async fn claim_epoch_conn(
    conn: &mut SqliteConnection,
    epoch: ResetEpoch,
    token: SchedulerFencingToken,
    now: TimestampMillis,
) -> AppResult<bool> {
    let now_millis = ts_to_millis(now)?;
    let token = token_to_i64(token)?;
    let current = sqlx::query(SELECT_CURRENT_LEASE_SQL)
        .bind(token)
        .bind(now_millis)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    if current.is_none() {
        return Err(AppError::conflict("scheduler lease is no longer current"));
    }
    let due_at = ts_to_millis(epoch.due_at)?;
    let inserted = sqlx::query(INSERT_EPOCH_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(due_at)
        .bind(token)
        .bind(now_millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    if inserted.rows_affected() == 0 {
        return Ok(false);
    }
    sqlx::query(SNAPSHOT_RECORDS_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(due_at)
        .bind(&epoch.leaderboard_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(DELETE_LIVE_RECORDS_SQL)
        .bind(&epoch.leaderboard_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(INSERT_OUTBOX_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(due_at)
        .bind(token)
        .bind(now_millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(true)
}

async fn snapshot_conn(
    conn: &mut SqliteConnection,
    epoch: &ResetEpoch,
) -> AppResult<Option<LeaderboardResetSnapshot>> {
    let due_at = ts_to_millis(epoch.due_at)?;
    let exists = sqlx::query(SELECT_EPOCH_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(due_at)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    if exists.is_none() {
        return Ok(None);
    }
    let rows = sqlx::query(SELECT_SNAPSHOT_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(due_at)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let records = rows
        .iter()
        .map(|row| {
            let metadata: Option<String> = get(row, "metadata")?;
            let submissions: i64 = get(row, "submissions")?;
            Ok(LeaderboardRecord {
                user_id: get(row, "owner_id")?,
                score: get(row, "score")?,
                subscore: get(row, "subscore")?,
                metadata: metadata
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|_| {
                        AppError::internal("invalid leaderboard reset snapshot metadata")
                    })?,
                submissions: u32::try_from(submissions)
                    .map_err(|_| AppError::internal("snapshot submissions out of range"))?,
                updated_at: millis_to_ts(get(row, "updated_at_unix_ms")?)?,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(Some(LeaderboardResetSnapshot {
        epoch: epoch.clone(),
        records,
    }))
}

async fn pending_outbox_conn(
    conn: &mut SqliteConnection,
    limit: usize,
) -> AppResult<Vec<ResetOutboxRecord>> {
    let limit =
        i64::try_from(limit).map_err(|_| AppError::validation("outbox limit out of range"))?;
    let rows = sqlx::query(SELECT_OUTBOX_SQL)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter()
        .map(|row| {
            Ok(ResetOutboxRecord {
                epoch: ResetEpoch::new(
                    get(row, "leaderboard_id")?,
                    millis_to_ts(get(row, "due_at_unix_ms")?)?,
                ),
                fencing_token: token_from_i64(get(row, "fencing_token")?)?,
            })
        })
        .collect()
}

async fn acknowledge_outbox_conn(conn: &mut SqliteConnection, epoch: &ResetEpoch) -> AppResult<()> {
    sqlx::query(DELETE_OUTBOX_SQL)
        .bind(&epoch.leaderboard_id)
        .bind(ts_to_millis(epoch.due_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}
