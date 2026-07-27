//! Postgres leaderboards repository.
//!
//! [`PgLeaderboardsRepository`] is the durable backend for
//! [`LeaderboardsRepository`](crate::repository::LeaderboardsRepository).
//! Definitions live in a `leaderboards` table (a caller-supplied `text` primary
//! key, `sort_order`/`operator` tokens, optional `reset_schedule`, and the
//! creation timestamp); per-user records live in `leaderboard_records`
//! (`PRIMARY KEY (leaderboard_id, owner_id)`, `metadata` as `jsonb`,
//! `ON DELETE CASCADE` from `leaderboards`). The score-write [`Operator`] rules
//! and the rank/pagination are reused from the shared pure helpers in
//! [`crate::repository::leaderboards`], so this backend cannot drift from the
//! in-memory reference or the SQLite sibling.
//!
//! A submission is a read-modify-write: it runs in a transaction and locks the
//! board row with `SELECT … FOR UPDATE`, so concurrent submissions against the
//! same board serialize, then upserts the record computed by [`apply_submission`].
//! Ranks are derived on read (all records loaded, then ordered by
//! [`rank_page`]); a durable rank cache is out of scope.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};

use crate::error::{AppError, AppResult};
use crate::repository::LeaderboardsRepository;
use crate::repository::leaderboards::{
    CreateLeaderboardRequest, LeaderboardDefinition, LeaderboardRecord, LeaderboardSummary,
    Operator, RecordsPage, SortOrder, apply_submission, board_not_found, rank_page,
};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const INSERT_BOARD_SQL: &str = "\
INSERT INTO leaderboards (id, sort_order, operator, reset_schedule, created_at_unix_ms) \
VALUES ($1, $2, $3, $4, $5)";

const SELECT_BOARD_SQL: &str = "\
SELECT id, sort_order, operator, reset_schedule, created_at_unix_ms FROM leaderboards \
WHERE id = $1";

const SELECT_BOARD_LOCK_SQL: &str = "\
SELECT id, sort_order, operator, reset_schedule, created_at_unix_ms FROM leaderboards \
WHERE id = $1 FOR UPDATE";

const LIST_BOARDS_SQL: &str = "\
SELECT id, sort_order, operator, reset_schedule, created_at_unix_ms FROM leaderboards ORDER BY id";

const COUNT_RECORDS_SQL: &str =
    "SELECT count(*) AS n FROM leaderboard_records WHERE leaderboard_id = $1";

const DELETE_BOARD_SQL: &str = "DELETE FROM leaderboards WHERE id = $1";

const SELECT_RECORD_SQL: &str = "\
SELECT owner_id, score, subscore, metadata, submissions, updated_at_unix_ms \
FROM leaderboard_records WHERE leaderboard_id = $1 AND owner_id = $2";

const SELECT_RECORDS_SQL: &str = "\
SELECT owner_id, score, subscore, metadata, submissions, updated_at_unix_ms \
FROM leaderboard_records WHERE leaderboard_id = $1";

const UPSERT_RECORD_SQL: &str = "\
INSERT INTO leaderboard_records \
(leaderboard_id, owner_id, score, subscore, metadata, submissions, updated_at_unix_ms) \
VALUES ($1, $2, $3, $4, $5, $6, $7) \
ON CONFLICT (leaderboard_id, owner_id) DO UPDATE SET \
score = EXCLUDED.score, subscore = EXCLUDED.subscore, metadata = EXCLUDED.metadata, \
submissions = EXCLUDED.submissions, updated_at_unix_ms = EXCLUDED.updated_at_unix_ms";

const DELETE_RECORD_SQL: &str =
    "DELETE FROM leaderboard_records WHERE leaderboard_id = $1 AND owner_id = $2";

// --- mapping helpers --------------------------------------------------------

fn parse_definition(row: &PgRow) -> AppResult<LeaderboardDefinition> {
    let id: String = get(row, "id")?;
    let sort: String = get(row, "sort_order")?;
    let operator: String = get(row, "operator")?;
    let reset_schedule: Option<String> = get(row, "reset_schedule")?;
    let created: i64 = get(row, "created_at_unix_ms")?;
    Ok(LeaderboardDefinition {
        id,
        sort: SortOrder::from_token(&sort)?,
        operator: Operator::from_token(&operator)?,
        reset_schedule,
        created_at: millis_to_ts(created)?,
    })
}

fn parse_record(row: &PgRow) -> AppResult<LeaderboardRecord> {
    let user_id: String = get(row, "owner_id")?;
    let score: i64 = get(row, "score")?;
    let subscore: i64 = get(row, "subscore")?;
    let metadata: Option<serde_json::Value> = get(row, "metadata")?;
    let submissions: i64 = get(row, "submissions")?;
    let submissions = u32::try_from(submissions)
        .map_err(|_| AppError::internal("leaderboard submissions out of range"))?;
    let updated: i64 = get(row, "updated_at_unix_ms")?;
    Ok(LeaderboardRecord {
        user_id,
        score,
        subscore,
        metadata,
        updated_at: millis_to_ts(updated)?,
        submissions,
    })
}

// --- repository -------------------------------------------------------------

/// Postgres [`LeaderboardsRepository`].
pub struct PgLeaderboardsRepository {
    executor: PgExecutor,
}

impl PgLeaderboardsRepository {
    /// Bind a leaderboards repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
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
            PgExecutor::Tx(cell) => {
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
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl LeaderboardsRepository for PgLeaderboardsRepository {
    async fn create(
        &self,
        request: CreateLeaderboardRequest,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardDefinition> {
        with_conn!(self, conn => create_conn(conn, request, now).await)
    }

    async fn list(&self) -> AppResult<Vec<LeaderboardSummary>> {
        with_conn!(self, conn => list_conn(conn).await)
    }

    async fn get(&self, id: &str) -> AppResult<Option<LeaderboardDefinition>> {
        with_conn!(self, conn => get_conn(conn, id).await)
    }

    async fn delete(&self, id: &str) -> AppResult<bool> {
        with_conn!(self, conn => delete_conn(conn, id).await)
    }

    async fn submit(
        &self,
        board: &str,
        user_id: &str,
        score: i64,
        subscore: i64,
        metadata: Option<serde_json::Value>,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardRecord> {
        with_tx!(self, conn => submit_conn(conn, board, user_id, score, subscore, metadata, now).await)
    }

    async fn records(&self, board: &str, limit: usize, offset: usize) -> AppResult<RecordsPage> {
        with_conn!(self, conn => records_conn(conn, board, limit, offset).await)
    }

    async fn delete_record(&self, board: &str, user_id: &str) -> AppResult<bool> {
        with_conn!(self, conn => delete_record_conn(conn, board, user_id).await)
    }
}

/// Load a board definition, taking a `FOR UPDATE` row lock when `lock` is set (for
/// the submit read-modify-write). Absence maps to `NotFound`.
async fn require_definition(
    conn: &mut PgConnection,
    board: &str,
    lock: bool,
) -> AppResult<LeaderboardDefinition> {
    let sql = if lock {
        SELECT_BOARD_LOCK_SQL
    } else {
        SELECT_BOARD_SQL
    };
    let row = sqlx::query(sql)
        .bind(board)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let Some(row) = row else {
        return Err(board_not_found(board));
    };
    parse_definition(&row)
}

async fn create_conn(
    conn: &mut PgConnection,
    request: CreateLeaderboardRequest,
    now: TimestampMillis,
) -> AppResult<LeaderboardDefinition> {
    let created = ts_to_millis(now)?;
    sqlx::query(INSERT_BOARD_SQL)
        .bind(&request.id)
        .bind(request.sort.as_str())
        .bind(request.operator.as_str())
        .bind(request.reset_schedule.as_deref())
        .bind(created)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(LeaderboardDefinition {
        id: request.id,
        sort: request.sort,
        operator: request.operator,
        reset_schedule: request.reset_schedule,
        created_at: now,
    })
}

async fn list_conn(conn: &mut PgConnection) -> AppResult<Vec<LeaderboardSummary>> {
    let rows = sqlx::query(LIST_BOARDS_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let mut summaries = Vec::with_capacity(rows.len());
    for row in &rows {
        let definition = parse_definition(row)?;
        let records = count_records(conn, &definition.id).await?;
        summaries.push(LeaderboardSummary {
            definition,
            records,
        });
    }
    Ok(summaries)
}

async fn count_records(conn: &mut PgConnection, board: &str) -> AppResult<usize> {
    let row = sqlx::query(COUNT_RECORDS_SQL)
        .bind(board)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let n: i64 = get(&row, "n")?;
    usize::try_from(n).map_err(|_| AppError::internal("leaderboard record count out of range"))
}

async fn get_conn(conn: &mut PgConnection, id: &str) -> AppResult<Option<LeaderboardDefinition>> {
    let row = sqlx::query(SELECT_BOARD_SQL)
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(parse_definition).transpose()
}

async fn delete_conn(conn: &mut PgConnection, id: &str) -> AppResult<bool> {
    let result = sqlx::query(DELETE_BOARD_SQL)
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(result.rows_affected() > 0)
}

async fn load_record(
    conn: &mut PgConnection,
    board: &str,
    user_id: &str,
) -> AppResult<Option<LeaderboardRecord>> {
    let row = sqlx::query(SELECT_RECORD_SQL)
        .bind(board)
        .bind(user_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(parse_record).transpose()
}

async fn upsert_record(
    conn: &mut PgConnection,
    board: &str,
    record: &LeaderboardRecord,
) -> AppResult<()> {
    sqlx::query(UPSERT_RECORD_SQL)
        .bind(board)
        .bind(&record.user_id)
        .bind(record.score)
        .bind(record.subscore)
        .bind(record.metadata.as_ref().map(sqlx::types::Json))
        .bind(i64::from(record.submissions))
        .bind(ts_to_millis(record.updated_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_conn(
    conn: &mut PgConnection,
    board: &str,
    user_id: &str,
    score: i64,
    subscore: i64,
    metadata: Option<serde_json::Value>,
    now: TimestampMillis,
) -> AppResult<LeaderboardRecord> {
    let definition = require_definition(conn, board, true).await?;
    let existing = load_record(conn, board, user_id).await?;
    let record = apply_submission(
        definition.operator,
        definition.sort,
        existing.as_ref(),
        user_id,
        score,
        subscore,
        metadata,
        now,
    );
    upsert_record(conn, board, &record).await?;
    Ok(record)
}

async fn records_conn(
    conn: &mut PgConnection,
    board: &str,
    limit: usize,
    offset: usize,
) -> AppResult<RecordsPage> {
    let definition = require_definition(conn, board, false).await?;
    let rows = sqlx::query(SELECT_RECORDS_SQL)
        .bind(board)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let records = rows
        .iter()
        .map(parse_record)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(rank_page(definition.sort, records, limit, offset))
}

async fn delete_record_conn(
    conn: &mut PgConnection,
    board: &str,
    user_id: &str,
) -> AppResult<bool> {
    // Board existence is authoritative: a missing board is NotFound, distinct from
    // a present board with no such record (Ok(false)).
    require_definition(conn, board, false).await?;
    let result = sqlx::query(DELETE_RECORD_SQL)
        .bind(board)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(result.rows_affected() > 0)
}
