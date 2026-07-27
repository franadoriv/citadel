//! SQLite notifications repository.
//!
//! [`SqliteNotificationsRepository`] is the durable single-file backend for
//! [`NotificationsRepository`](crate::repository::NotificationsRepository) and the
//! sibling of the Postgres impl in `../pg/notifications.rs`. All state lives in one
//! `notifications` table: each row is one notification addressed to a single
//! account (`recipient_id`) or to everyone (a broadcast, `recipient_id IS NULL`).
//! The capacity/eviction bound and the visibility-filtered newest-first paging are
//! reused from the shared pure helpers in
//! [`crate::repository::notifications`], so the two backends cannot drift.
//!
//! SQLite has no `SELECT … FOR UPDATE`, so an enqueue runs under `BEGIN IMMEDIATE`,
//! which takes the writer slot up front and serializes the read-modify-write the
//! way the Postgres row lock does. JSON `content` is stored as `TEXT` (serialized
//! at the boundary), `read_at_unix_ms` is a nullable `INTEGER`, and timestamps use
//! the shared integer-millis representation.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::repository::NotificationsRepository;
use crate::repository::notifications::{
    Notification, NotificationPage, Recipient, notification_not_found, overflow_evictions,
    page_notifications,
};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const SELECT_HEAD_SQL: &str = "SELECT id FROM notifications ORDER BY id DESC LIMIT 1";

const INSERT_SQL: &str = "\
INSERT INTO notifications \
(id, recipient_id, subject, content, code, created_at_unix_ms, read_at_unix_ms) \
VALUES (?, ?, ?, ?, ?, ?, NULL)";

const COUNT_SQL: &str = "SELECT count(*) AS n FROM notifications";

const EVICT_SQL: &str =
    "DELETE FROM notifications WHERE id IN (SELECT id FROM notifications ORDER BY id ASC LIMIT ?)";

const LIST_SQL: &str = "\
SELECT id, recipient_id, subject, content, code, created_at_unix_ms, read_at_unix_ms \
FROM notifications ORDER BY id";

const DELETE_SQL: &str = "DELETE FROM notifications WHERE id = ?";

const MARK_READ_SQL: &str = "UPDATE notifications SET read_at_unix_ms = ? WHERE id = ?";

// --- mapping helpers --------------------------------------------------------

fn parse_notification(row: &SqliteRow) -> AppResult<Notification> {
    let id: i64 = get(row, "id")?;
    let recipient_id: Option<String> = get(row, "recipient_id")?;
    let subject: String = get(row, "subject")?;
    let content_text: String = get(row, "content")?;
    let content: serde_json::Value = serde_json::from_str(&content_text).map_err(|e| {
        AppError::internal("failed to decode stored notification content")
            .with_detail(e.to_string())
    })?;
    let code: i32 = get(row, "code")?;
    let created: i64 = get(row, "created_at_unix_ms")?;
    let read_at: Option<i64> = get(row, "read_at_unix_ms")?;
    Ok(Notification {
        id: to_u64(id, "notification id")?,
        recipient: Recipient::from_column(recipient_id),
        subject,
        content,
        code,
        created_at: millis_to_ts(created)?,
        read: read_at.is_some(),
    })
}

fn to_u64(value: i64, what: &str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

fn to_i64(value: u64, what: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

// --- repository -------------------------------------------------------------

/// SQLite [`NotificationsRepository`].
pub struct SqliteNotificationsRepository {
    executor: SqliteExecutor,
}

impl SqliteNotificationsRepository {
    /// Bind a notifications repository to an execution handle (pool or
    /// transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_tx {
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
impl NotificationsRepository for SqliteNotificationsRepository {
    async fn enqueue(
        &self,
        recipient: Recipient,
        subject: &str,
        content: &serde_json::Value,
        code: i32,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        with_tx!(self, conn =>
            enqueue_conn(conn, recipient, subject, content, code, capacity, now).await)
    }

    async fn list(
        &self,
        user_id_filter: Option<&str>,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<NotificationPage> {
        with_conn!(self, conn => list_conn(conn, user_id_filter, limit, before_id).await)
    }

    async fn count(&self) -> AppResult<usize> {
        with_conn!(self, conn => count_conn(conn).await)
    }

    async fn delete(&self, id: u64) -> AppResult<()> {
        with_conn!(self, conn => delete_conn(conn, id).await)
    }

    async fn mark_read(&self, id: u64, now: TimestampMillis) -> AppResult<()> {
        with_conn!(self, conn => mark_read_conn(conn, id, now).await)
    }
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_conn(
    conn: &mut SqliteConnection,
    recipient: Recipient,
    subject: &str,
    content: &serde_json::Value,
    code: i32,
    capacity: usize,
    now: TimestampMillis,
) -> AppResult<u64> {
    let head = sqlx::query(SELECT_HEAD_SQL)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let max_id = match head {
        Some(row) => {
            let id: i64 = get(&row, "id")?;
            to_u64(id, "notification id")?
        }
        None => 0,
    };
    let new_id = max_id + 1;
    let content_text = serde_json::to_string(content).map_err(|e| {
        AppError::internal("failed to encode notification content").with_detail(e.to_string())
    })?;
    sqlx::query(INSERT_SQL)
        .bind(to_i64(new_id, "notification id")?)
        .bind(recipient.user_id())
        .bind(subject)
        .bind(content_text)
        .bind(code)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let total = count_conn(conn).await?;
    let evict = overflow_evictions(total, capacity);
    if evict > 0 {
        sqlx::query(EVICT_SQL)
            .bind(to_i64(evict as u64, "notification eviction count")?)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }
    Ok(new_id)
}

async fn list_conn(
    conn: &mut SqliteConnection,
    user_id_filter: Option<&str>,
    limit: usize,
    before_id: Option<u64>,
) -> AppResult<NotificationPage> {
    let rows = sqlx::query(LIST_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let chronological = rows
        .iter()
        .map(parse_notification)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(page_notifications(
        chronological,
        user_id_filter,
        limit,
        before_id,
    ))
}

async fn count_conn(conn: &mut SqliteConnection) -> AppResult<usize> {
    let row = sqlx::query(COUNT_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let n: i64 = get(&row, "n")?;
    usize::try_from(n).map_err(|_| AppError::internal("notification count out of range"))
}

async fn delete_conn(conn: &mut SqliteConnection, id: u64) -> AppResult<()> {
    let result = sqlx::query(DELETE_SQL)
        .bind(to_i64(id, "notification id")?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    if result.rows_affected() == 0 {
        return Err(notification_not_found());
    }
    Ok(())
}

async fn mark_read_conn(
    conn: &mut SqliteConnection,
    id: u64,
    now: TimestampMillis,
) -> AppResult<()> {
    let result = sqlx::query(MARK_READ_SQL)
        .bind(ts_to_millis(now)?)
        .bind(to_i64(id, "notification id")?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    if result.rows_affected() == 0 {
        return Err(notification_not_found());
    }
    Ok(())
}
