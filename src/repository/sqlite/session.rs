//! SQLite session repository.
//!
//! [`SqliteSessionRepository`] is the durable single-file backend for
//! [`SessionRepository`](crate::repository::SessionRepository). It reproduces the
//! in-memory reference impl and the Postgres impl exactly:
//!
//! - `update_session` is a compare-and-set. The current row is read inside a
//!   `BEGIN IMMEDIATE` transaction (SQLite has no `SELECT ... FOR UPDATE`, so the
//!   up-front writer slot serializes the decision like the Postgres row lock),
//!   rehydrated into a [`Session`], and the identical guards are re-applied on the
//!   domain objects: immutable facts (`user_id`, `issued_at`, `owner_node`) may
//!   not change, and a terminal stored session accepts only a byte-identical
//!   write. A stale refresh can therefore never resurrect a session that was
//!   revoked or expired since it was read.
//! - `revoke_user_sessions` is atomic and idempotent: within one transaction it
//!   scans every non-terminal session of the user, transitions each through the
//!   domain [`Session::revoke_at`] method, and returns exactly the count it newly
//!   revoked.
//!
//! # Token handling
//!
//! Only the non-secret [`SessionTokenRef`](crate::session::SessionTokenRef) is
//! ever persisted (in the `token_ref` lookup column and inside the serialized
//! record); the bearer [`SessionTokenSecret`](crate::session::SessionTokenSecret)
//! is never stored — it is not even serializable — so no secret material reaches
//! the database.
//!
//! # Record shape
//!
//! The lifecycle state is a private enum on [`Session`] reachable only through
//! `Deserialize` (its documented rehydration path), so the authoritative record
//! is the full session serialized into the `TEXT` `data` column (SQLite has no
//! `jsonb`; the repository serializes at the boundary). Flat query columns (`id`,
//! `user_id`, `token_ref`, `state_kind`) are projected out for lookups and the
//! revoke scan. The `token_ref` column is the lookup index: on a bulk revoke it is
//! cleared to `NULL` (so a revoked session is no longer resolvable by its token)
//! while `data` keeps the original reference, mirroring the in-memory split
//! between the by-id store and the by-token index.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::identity::UserId;
use crate::repository::SessionRepository;
use crate::session::{RevocationReason, Session, SessionId, SessionTokenRef};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, get, tx_closed};

// --- SQL --------------------------------------------------------------------

const GET_SESSION_SQL: &str = "SELECT data FROM sessions WHERE id = ?";

const GET_SESSION_BY_TOKEN_REF_SQL: &str = "SELECT data FROM sessions WHERE token_ref = ? LIMIT 1";

const INSERT_SESSION_SQL: &str = "\
INSERT INTO sessions (id, user_id, token_ref, state_kind, data) \
VALUES (?, ?, ?, ?, ?)";

const GET_SESSION_FOR_UPDATE_SQL: &str = "SELECT data FROM sessions WHERE id = ?";

const UPDATE_SESSION_SQL: &str = "\
UPDATE sessions SET token_ref = ?, state_kind = ?, data = ? WHERE id = ?";

/// Scan every non-terminal (`active`) session of a user for a bulk revoke.
const SELECT_ACTIVE_SESSIONS_SQL: &str =
    "SELECT data FROM sessions WHERE user_id = ? AND state_kind = 'active'";

/// Materialize a bulk revoke, clearing the token lookup index while `data` keeps
/// the reference (mirrors the in-memory by-token removal on revoke).
const REVOKE_SESSION_SQL: &str = "\
UPDATE sessions SET token_ref = NULL, state_kind = 'revoked', data = ? WHERE id = ?";

// --- mapping helpers --------------------------------------------------------

fn session_from_row(row: &SqliteRow) -> AppResult<Session> {
    let data: String = get(row, "data")?;
    serde_json::from_str(&data).map_err(|e| {
        AppError::internal("failed to decode session record").with_detail(e.to_string())
    })
}

fn session_to_json(session: &Session) -> AppResult<String> {
    serde_json::to_string(session).map_err(|e| {
        AppError::internal("failed to encode session record").with_detail(e.to_string())
    })
}

fn token_ref_str(session: &Session) -> Option<&str> {
    session.token_ref.as_ref().map(SessionTokenRef::as_str)
}

// --- repository -------------------------------------------------------------

/// SQLite [`SessionRepository`].
pub struct SqliteSessionRepository {
    executor: SqliteExecutor,
}

impl SqliteSessionRepository {
    /// Bind a session repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl SessionRepository for SqliteSessionRepository {
    async fn get_session(&self, id: &SessionId) -> AppResult<Option<Session>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_session_conn(&mut conn, id).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_session_conn(&mut *tx, id).await
            }
        }
    }

    async fn get_session_by_token_ref(
        &self,
        token_ref: &SessionTokenRef,
    ) -> AppResult<Option<Session>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_session_by_token_ref_conn(&mut conn, token_ref).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_session_by_token_ref_conn(&mut *tx, token_ref).await
            }
        }
    }

    async fn create_session(&self, session: Session) -> AppResult<Session> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                create_session_conn(&mut conn, session).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                create_session_conn(&mut *tx, session).await
            }
        }
    }

    async fn update_session(&self, session: Session) -> AppResult<Session> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                // `BEGIN IMMEDIATE` serializes the compare-and-set decision like
                // the Postgres `SELECT ... FOR UPDATE` row lock.
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match update_session_conn(&mut tx, session).await {
                    Ok(session) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(session)
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
                update_session_conn(&mut *tx, session).await
            }
        }
    }

    async fn revoke_user_sessions(
        &self,
        user_id: &UserId,
        revoked_at: TimestampMillis,
        reason: RevocationReason,
    ) -> AppResult<usize> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match revoke_user_sessions_conn(&mut tx, user_id, revoked_at, reason).await {
                    Ok(count) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(count)
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
                revoke_user_sessions_conn(&mut *tx, user_id, revoked_at, reason).await
            }
        }
    }
}

async fn get_session_conn(
    conn: &mut SqliteConnection,
    id: &SessionId,
) -> AppResult<Option<Session>> {
    let row = sqlx::query(GET_SESSION_SQL)
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(session_from_row).transpose()
}

async fn get_session_by_token_ref_conn(
    conn: &mut SqliteConnection,
    token_ref: &SessionTokenRef,
) -> AppResult<Option<Session>> {
    let row = sqlx::query(GET_SESSION_BY_TOKEN_REF_SQL)
        .bind(token_ref.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(session_from_row).transpose()
}

async fn create_session_conn(conn: &mut SqliteConnection, session: Session) -> AppResult<Session> {
    let data = session_to_json(&session)?;
    sqlx::query(INSERT_SESSION_SQL)
        .bind(session.id.as_str())
        .bind(session.user_id.as_str())
        .bind(token_ref_str(&session))
        .bind(session.state_kind().as_str())
        .bind(data)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(session)
}

async fn update_session_conn(conn: &mut SqliteConnection, session: Session) -> AppResult<Session> {
    let existing = sqlx::query(GET_SESSION_FOR_UPDATE_SQL)
        .bind(session.id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let Some(row) = existing else {
        return Err(AppError::not_found("session does not exist"));
    };
    let existing = session_from_row(&row)?;

    // Immutable session facts must never change on update.
    if existing.user_id != session.user_id
        || existing.issued_at != session.issued_at
        || existing.owner_node != session.owner_node
    {
        return Err(AppError::conflict("immutable session fields cannot change"));
    }

    // Compare-and-set: a terminal stored session accepts only an identical write
    // (idempotent). Any differing write — a stale refresh, or a switch from one
    // terminal state to another — is a conflict.
    if existing.state().is_terminal() && existing != session {
        return Err(AppError::conflict(
            "cannot update a terminal session (compare-and-set failed)",
        ));
    }

    let data = session_to_json(&session)?;
    sqlx::query(UPDATE_SESSION_SQL)
        .bind(token_ref_str(&session))
        .bind(session.state_kind().as_str())
        .bind(data)
        .bind(session.id.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(session)
}

async fn revoke_user_sessions_conn(
    conn: &mut SqliteConnection,
    user_id: &UserId,
    revoked_at: TimestampMillis,
    reason: RevocationReason,
) -> AppResult<usize> {
    let rows = sqlx::query(SELECT_ACTIVE_SESSIONS_SQL)
        .bind(user_id.as_str())
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;

    let mut count = 0usize;
    for row in &rows {
        let mut session = session_from_row(row)?;
        // Only `active`-kind sessions were selected, so this transition always
        // applies (including lapsed-but-not-materialized sessions), matching the
        // in-memory `!state.is_terminal` scope.
        session.revoke_at(revoked_at, reason)?;
        let data = session_to_json(&session)?;
        sqlx::query(REVOKE_SESSION_SQL)
            .bind(data)
            .bind(session.id.as_str())
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::NodeId;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn session(id: &str, user: &str, token: &str) -> Session {
        Session::new(
            SessionId::new(id).expect("sid"),
            UserId::new(user).expect("uid"),
            NodeId::new("node-a").expect("node"),
            ts(100),
            ts(200),
            Some(ts(400)),
            Some(SessionTokenRef::new(token).expect("ref")),
        )
        .expect("session")
    }

    #[test]
    fn session_json_round_trips() {
        let session = session("s-1", "u-1", "t-1");
        let json = session_to_json(&session).expect("encode");
        let decoded: Session = serde_json::from_str(&json).expect("decode");
        assert_eq!(decoded, session);
        assert_eq!(token_ref_str(&session), Some("t-1"));
    }

    #[test]
    fn state_kind_token_is_active_for_new_session() {
        assert_eq!(session("s-1", "u-1", "t-1").state_kind().as_str(), "active");
    }
}
