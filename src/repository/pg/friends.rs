//! Postgres friends repository.
//!
//! [`PgFriendsRepository`] is the durable backend for
//! [`FriendsRepository`](crate::repository::FriendsRepository). It stores the
//! two directed edges of each relationship as rows in `friend_edges`
//! (`PRIMARY KEY (owner_id, other_id)`) and reproduces the reference impl's state
//! machine exactly by reusing the shared pure
//! [`plan_add`](crate::repository::friends::plan_add):
//!
//! - `add` opens a transaction, reads both edges (`FOR UPDATE` where they exist,
//!   so a concurrent add against the same pair serializes), computes the plan,
//!   and upserts both edges with the call timestamp. The row may be absent, so
//!   the read-then-upsert (rather than a pure `UPDATE`) is required.
//! - `block` upserts the blocker's one-sided `blocked` edge and deletes the
//!   other side's edge in one transaction.
//! - `remove` deletes both directed edges in one statement, returning whether
//!   anything was removed.
//!
//! State is stored as its stable [`FriendState::as_str`] token and parsed back
//! with [`FriendState::from_token`]; timestamps use the shared bigint-millis
//! conversion so no datetime/locale handling is needed.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};

use crate::error::AppResult;
use crate::repository::FriendsRepository;
use crate::repository::friends::{FriendRow, FriendState, plan_add};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

/// Read (and lock) one directed edge's state for the read-modify-write in `add`.
const LOCK_EDGE_SQL: &str =
    "SELECT state FROM friend_edges WHERE owner_id = $1 AND other_id = $2 FOR UPDATE";

/// Upsert one directed edge (state + timestamp).
const UPSERT_EDGE_SQL: &str = "\
INSERT INTO friend_edges (owner_id, other_id, state, updated_unix_ms) \
VALUES ($1, $2, $3, $4) \
ON CONFLICT (owner_id, other_id) DO UPDATE \
SET state = EXCLUDED.state, updated_unix_ms = EXCLUDED.updated_unix_ms";

/// Delete one directed edge (used to drop the blocked side's view).
const DELETE_EDGE_SQL: &str = "DELETE FROM friend_edges WHERE owner_id = $1 AND other_id = $2";

/// Delete both directed edges of a relationship in one statement.
const DELETE_BOTH_EDGES_SQL: &str = "\
DELETE FROM friend_edges \
WHERE (owner_id = $1 AND other_id = $2) OR (owner_id = $2 AND other_id = $1)";

/// List an owner's edges, other-id-ordered.
const LIST_EDGES_SQL: &str = "\
SELECT other_id, state, updated_unix_ms FROM friend_edges \
WHERE owner_id = $1 ORDER BY other_id";

const ADVANCE_CHAT_ACCESS_EPOCH_SQL: &str = "\
INSERT INTO chat_access_epochs (access_key, epoch, updated_at_unix_ms) VALUES ($1, 1, $2) \
ON CONFLICT(access_key) DO UPDATE SET epoch = chat_access_epochs.epoch + 1, \
updated_at_unix_ms = excluded.updated_at_unix_ms";

// --- mapping helpers --------------------------------------------------------

fn row_to_friend(row: &PgRow) -> AppResult<FriendRow> {
    let user_id: String = get(row, "other_id")?;
    let state: String = get(row, "state")?;
    let millis: i64 = get(row, "updated_unix_ms")?;
    Ok(FriendRow {
        user_id,
        state: FriendState::from_token(&state)?,
        updated_unix_ms: millis_to_ts(millis)?.unix_millis(),
    })
}

// --- repository -------------------------------------------------------------

/// Postgres [`FriendsRepository`].
pub struct PgFriendsRepository {
    executor: PgExecutor,
}

impl PgFriendsRepository {
    /// Bind a friends repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl FriendsRepository for PgFriendsRepository {
    async fn add(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<FriendState> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                match add_conn(&mut tx, user, other, now).await {
                    Ok(state) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(state)
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
                add_conn(&mut *tx, user, other, now).await
            }
        }
    }

    async fn remove(&self, user: &str, other: &str) -> AppResult<bool> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                match remove_conn(&mut tx, user, other).await {
                    Ok(removed) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(removed)
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
                remove_conn(&mut *tx, user, other).await
            }
        }
    }

    async fn block(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<()> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                match block_conn(&mut tx, user, other, now).await {
                    Ok(()) => tx.commit().await.map_err(db_err),
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                block_conn(&mut *tx, user, other, now).await
            }
        }
    }

    async fn list(&self, user: &str) -> AppResult<Vec<FriendRow>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_conn(&mut conn, user).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_conn(&mut *tx, user).await
            }
        }
    }
}

async fn read_edge_state(
    conn: &mut PgConnection,
    owner: &str,
    other: &str,
) -> AppResult<Option<FriendState>> {
    let row = sqlx::query(LOCK_EDGE_SQL)
        .bind(owner)
        .bind(other)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    match row {
        Some(row) => {
            let token: String = get(&row, "state")?;
            Ok(Some(FriendState::from_token(&token)?))
        }
        None => Ok(None),
    }
}

async fn upsert_edge(
    conn: &mut PgConnection,
    owner: &str,
    other: &str,
    state: FriendState,
    millis: i64,
) -> AppResult<()> {
    sqlx::query(UPSERT_EDGE_SQL)
        .bind(owner)
        .bind(other)
        .bind(state.as_str())
        .bind(millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

fn direct_access_key(user: &str, other: &str) -> String {
    let (lower, higher) = if user < other {
        (user, other)
    } else {
        (other, user)
    };
    format!("direct:{lower}:{higher}")
}

async fn advance_chat_access_epoch(
    conn: &mut PgConnection,
    user: &str,
    other: &str,
    millis: i64,
) -> AppResult<()> {
    sqlx::query(ADVANCE_CHAT_ACCESS_EPOCH_SQL)
        .bind(direct_access_key(user, other))
        .bind(millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn add_conn(
    conn: &mut PgConnection,
    user: &str,
    other: &str,
    now: TimestampMillis,
) -> AppResult<FriendState> {
    let forward = read_edge_state(conn, user, other).await?;
    let backward = read_edge_state(conn, other, user).await?;
    let plan = plan_add(forward, backward)?;
    let millis = ts_to_millis(now)?;
    upsert_edge(conn, user, other, plan.owner_state, millis).await?;
    upsert_edge(conn, other, user, plan.other_state, millis).await?;
    advance_chat_access_epoch(conn, user, other, millis).await?;
    Ok(plan.owner_state)
}

async fn remove_conn(conn: &mut PgConnection, user: &str, other: &str) -> AppResult<bool> {
    let result = sqlx::query(DELETE_BOTH_EDGES_SQL)
        .bind(user)
        .bind(other)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let removed = result.rows_affected() > 0;
    if removed {
        advance_chat_access_epoch(conn, user, other, 0).await?;
    }
    Ok(removed)
}

async fn block_conn(
    conn: &mut PgConnection,
    user: &str,
    other: &str,
    now: TimestampMillis,
) -> AppResult<()> {
    let millis = ts_to_millis(now)?;
    upsert_edge(conn, user, other, FriendState::Blocked, millis).await?;
    sqlx::query(DELETE_EDGE_SQL)
        .bind(other)
        .bind(user)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    advance_chat_access_epoch(conn, user, other, millis).await?;
    Ok(())
}

async fn list_conn(conn: &mut PgConnection, user: &str) -> AppResult<Vec<FriendRow>> {
    let rows = sqlx::query(LIST_EDGES_SQL)
        .bind(user)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_friend).collect()
}
