//! Postgres chat repository.
//!
//! [`PgChatRepository`] is the durable backend for
//! [`ChatRepository`](crate::repository::ChatRepository). All state lives in one
//! `chat_messages` table keyed by `(channel_id, id)`: a channel exists iff a row
//! bears its id, its fixed [`ChannelType`] is denormalized onto every row
//! (constant per channel), and its activity summary is derived from the retained
//! rows. The retention/eviction bound, the newest-first paging, and the channel
//! listing sort are reused from the shared pure helpers in
//! [`crate::repository::chat`], so this backend cannot drift from the in-memory
//! reference or the SQLite sibling.
//!
//! An append is a read-modify-write: it runs in a transaction, reads (and locks)
//! the channel's newest row with `SELECT … FOR UPDATE` to serialize concurrent
//! appends to the same channel, assigns `MAX(id) + 1`, inserts, then evicts every
//! row at or below [`eviction_high_watermark`]. Because only the oldest rows are
//! evicted, `MAX(id)` stays the channel's total-ever-appended counter.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};

use crate::error::{AppError, AppResult};
use crate::repository::ChatRepository;
use crate::repository::chat::{
    ChannelSummary, ChannelType, ChatChannel, ChatDeliveryOutboxRecord, ChatDeliveryRequest,
    ChatMessage, ChatModerationAudit, ChatRateLimit, channel_not_found, eviction_high_watermark,
    finish_channel_listing, message_not_found, new_opaque_channel_id, page_history,
    serialize_delivery_event,
};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const SELECT_HEAD_LOCK_SQL: &str = "\
SELECT id, channel_type FROM chat_messages WHERE channel_id = $1 ORDER BY id DESC LIMIT 1 FOR UPDATE";

const INSERT_SQL: &str = "\
INSERT INTO chat_messages \
(channel_id, id, channel_type, sender_id, content, deleted, created_at_unix_ms, updated_at_unix_ms, revision, last_event_id) \
VALUES ($1, $2, $3, $4, $5, false, $6, $7, 1, $8)";

const NEXT_EVENT_ID_SQL: &str = "\
SELECT COALESCE(MAX(event_id), 0) AS event_id FROM chat_events WHERE channel_id = $1";
// Lock the current event head before reserving the next id. This serializes an
// edit/delete of an older row against a concurrent append on the same channel.
const LOCK_EVENT_HEAD_SQL: &str = "\
SELECT event_id FROM chat_events WHERE channel_id = $1 ORDER BY event_id DESC LIMIT 1 FOR UPDATE";
const INSERT_EVENT_SQL: &str = "\
INSERT INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms) \
VALUES ($1, $2, 'created', $3, 1, $4)";
const INSERT_UPDATE_EVENT_SQL: &str = "\
INSERT INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms) \
VALUES ($1, $2, 'updated', $3, $4, $5)";
const INSERT_DELETE_EVENT_SQL: &str = "\
INSERT INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms) \
VALUES ($1, $2, 'deleted', $3, $4, $5)";
const INSERT_MODERATION_AUDIT_SQL: &str = "\
INSERT INTO chat_moderation_audit \
(occurred_at_unix_ms, actor_kind, actor_id_hash, action, reason_code, channel_id_hash, message_id, author_id_hash, authority_epoch, correlation_id, node_id) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
const DELETE_EXPIRED_MODERATION_AUDIT_SQL: &str = "\
DELETE FROM chat_moderation_audit WHERE audit_id IN \
(SELECT audit_id FROM chat_moderation_audit WHERE occurred_at_unix_ms < $1 ORDER BY audit_id LIMIT $2)";
const COUNT_MODERATION_AUDIT_SQL: &str = "SELECT count(*) AS n FROM chat_moderation_audit";
const ENSURE_RATE_LIMIT_SQL: &str = "\
INSERT INTO chat_rate_limits (rate_key, window_started_at_unix_ms, used) VALUES ($1, $2, 0) \
ON CONFLICT(rate_key, window_started_at_unix_ms) DO NOTHING";
const SELECT_RATE_LIMIT_SQL: &str = "\
SELECT used FROM chat_rate_limits WHERE rate_key = $1 AND window_started_at_unix_ms = $2 FOR UPDATE";
const INCREMENT_RATE_LIMIT_SQL: &str = "\
UPDATE chat_rate_limits SET used = used + 1 WHERE rate_key = $1 AND window_started_at_unix_ms = $2";
const DELETE_EXPIRED_RATE_LIMIT_SQL: &str = "\
DELETE FROM chat_rate_limits WHERE (rate_key, window_started_at_unix_ms) IN \
(SELECT rate_key, window_started_at_unix_ms FROM chat_rate_limits WHERE window_started_at_unix_ms < $1 ORDER BY window_started_at_unix_ms LIMIT $2)";
const INSERT_OUTBOX_SQL: &str = "\
INSERT INTO chat_delivery_outbox \
(channel_id, event_id, authority_epoch, payload, created_at_unix_ms, expires_at_unix_ms) VALUES ($1, $2, $3, $4, $5, $6) \
ON CONFLICT(channel_id, event_id) DO NOTHING";
const ACTIVE_OUTBOX_SQL: &str = "\
SELECT channel_id, event_id, authority_epoch, payload, created_at_unix_ms, expires_at_unix_ms \
FROM chat_delivery_outbox WHERE expires_at_unix_ms > $1 ORDER BY channel_id, event_id LIMIT $2";
const ACKNOWLEDGE_OUTBOX_SQL: &str =
    "DELETE FROM chat_delivery_outbox WHERE channel_id = $1 AND event_id = $2";
const DELETE_EXPIRED_OUTBOX_SQL: &str = "\
DELETE FROM chat_delivery_outbox WHERE outbox_id IN \
(SELECT outbox_id FROM chat_delivery_outbox WHERE expires_at_unix_ms <= $1 \
ORDER BY expires_at_unix_ms, outbox_id LIMIT $2)";

const EVICT_SQL: &str = "DELETE FROM chat_messages WHERE channel_id = $1 AND id <= $2";

const LIST_CHANNELS_SQL: &str = "\
SELECT channel_id, max(channel_type) AS channel_type, max(id) AS messages, \
max(created_at_unix_ms) AS last_activity_unix_ms \
FROM chat_messages GROUP BY channel_id";

const COUNT_CHANNELS_SQL: &str = "SELECT count(DISTINCT channel_id) AS n FROM chat_messages";

const HISTORY_SQL: &str = "\
SELECT id, sender_id, content, deleted, created_at_unix_ms, updated_at_unix_ms, revision, last_event_id \
FROM chat_messages WHERE channel_id = $1 ORDER BY id";

const CHANNEL_EXISTS_SQL: &str = "SELECT 1 FROM chat_messages WHERE channel_id = $1 LIMIT 1";

const SELECT_MESSAGE_FULL_SQL: &str = "\
SELECT id, sender_id, content, deleted, created_at_unix_ms, updated_at_unix_ms, revision, last_event_id \
FROM chat_messages WHERE channel_id = $1 AND id = $2 FOR UPDATE";
const UPDATE_MESSAGE_SQL: &str = "\
UPDATE chat_messages SET content = $1, revision = revision + 1, updated_at_unix_ms = $2, last_event_id = $3 \
WHERE channel_id = $4 AND id = $5";

const TOMBSTONE_SQL: &str = "\
UPDATE chat_messages SET deleted = true, content = '', revision = revision + 1, \
updated_at_unix_ms = $1, last_event_id = $2 WHERE channel_id = $3 AND id = $4";

const INSERT_CANONICAL_CHANNEL_SQL: &str = "\
INSERT INTO chat_channels (channel_id, channel_type, canonical_key, created_at_unix_ms) \
VALUES ($1, $2, $3, $4) ON CONFLICT(canonical_key) DO NOTHING";

const SELECT_CANONICAL_CHANNEL_SQL: &str = "\
SELECT channel_id, channel_type, canonical_key FROM chat_channels WHERE canonical_key = $1";

const SELECT_ACCESS_EPOCH_LOCK_SQL: &str =
    "SELECT epoch FROM chat_access_epochs WHERE access_key = $1 FOR UPDATE";

const ENSURE_ACCESS_EPOCH_SQL: &str = "\
INSERT INTO chat_access_epochs (access_key, epoch, updated_at_unix_ms) VALUES ($1, 0, $2) \
ON CONFLICT(access_key) DO NOTHING";

const ADVANCE_ACCESS_EPOCH_SQL: &str = "\
INSERT INTO chat_access_epochs (access_key, epoch, updated_at_unix_ms) VALUES ($1, 1, $2) \
ON CONFLICT(access_key) DO UPDATE SET epoch = chat_access_epochs.epoch + 1, \
updated_at_unix_ms = excluded.updated_at_unix_ms RETURNING epoch";

const SELECT_ACCESS_EPOCH_FOR_CHECK_SQL: &str =
    "SELECT epoch FROM chat_access_epochs WHERE access_key = $1 FOR UPDATE";

// --- mapping helpers --------------------------------------------------------

fn parse_message(row: &PgRow) -> AppResult<ChatMessage> {
    let id: i64 = get(row, "id")?;
    let sender: String = get(row, "sender_id")?;
    let content: String = get(row, "content")?;
    let deleted: bool = get(row, "deleted")?;
    let created: i64 = get(row, "created_at_unix_ms")?;
    let updated: i64 = get(row, "updated_at_unix_ms")?;
    let revision: i64 = get(row, "revision")?;
    let last_event_id: i64 = get(row, "last_event_id")?;
    Ok(ChatMessage {
        id: to_u64(id, "chat message id")?,
        sender,
        content,
        created_at_unix_ms: to_u64(created, "chat message timestamp")?,
        updated_at_unix_ms: to_u64(updated, "chat message update timestamp")?,
        revision: to_u64(revision, "chat message revision")?,
        last_event_id: to_u64(last_event_id, "chat event id")?,
        deleted,
    })
}

fn parse_summary(row: &PgRow) -> AppResult<ChannelSummary> {
    let channel: String = get(row, "channel_id")?;
    let token: String = get(row, "channel_type")?;
    let messages: i64 = get(row, "messages")?;
    let last: i64 = get(row, "last_activity_unix_ms")?;
    Ok(ChannelSummary {
        channel,
        channel_type: ChannelType::from_token(&token)?.as_str(),
        messages: to_u64(messages, "chat message count")?,
        last_activity_unix_ms: to_u64(last, "chat activity timestamp")?,
    })
}

fn parse_outbox_record(row: &PgRow) -> AppResult<ChatDeliveryOutboxRecord> {
    let event_id: i64 = get(row, "event_id")?;
    let authority_epoch: i64 = get(row, "authority_epoch")?;
    let created_at: i64 = get(row, "created_at_unix_ms")?;
    let expires_at: i64 = get(row, "expires_at_unix_ms")?;
    Ok(ChatDeliveryOutboxRecord {
        channel_id: get(row, "channel_id")?,
        event_id: to_u64(event_id, "chat delivery event id")?,
        authority_epoch: to_u64(authority_epoch, "chat delivery authority epoch")?,
        payload: get(row, "payload")?,
        created_at: TimestampMillis::from_unix_millis(to_u64(
            created_at,
            "chat delivery creation",
        )?),
        expires_at: TimestampMillis::from_unix_millis(to_u64(expires_at, "chat delivery expiry")?),
    })
}

fn parse_canonical_channel(row: &PgRow) -> AppResult<ChatChannel> {
    let id: String = get(row, "channel_id")?;
    let token: String = get(row, "channel_type")?;
    let canonical_key: String = get(row, "canonical_key")?;
    Ok(ChatChannel {
        id,
        channel_type: ChannelType::from_token(&token)?,
        canonical_key,
    })
}

fn to_u64(value: i64, what: &str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

fn to_i64(value: u64, what: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

// --- repository -------------------------------------------------------------

/// Postgres [`ChatRepository`].
pub struct PgChatRepository {
    executor: PgExecutor,
}

impl PgChatRepository {
    /// Bind a chat repository to an execution handle (pool or transaction).
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
impl ChatRepository for PgChatRepository {
    async fn resolve_canonical_channel(
        &self,
        canonical_key: &str,
        channel_type: ChannelType,
        now: TimestampMillis,
    ) -> AppResult<ChatChannel> {
        with_tx!(self, conn =>
            resolve_canonical_channel_conn(conn, canonical_key, channel_type, now).await)
    }

    async fn current_access_epoch(&self, access_key: &str) -> AppResult<u64> {
        with_tx!(self, conn => current_access_epoch_conn(conn, access_key, TimestampMillis::from_unix_millis(0)).await)
    }

    async fn advance_access_epoch(&self, access_key: &str, now: TimestampMillis) -> AppResult<u64> {
        with_tx!(self, conn => advance_access_epoch_conn(conn, access_key, now).await)
    }

    async fn post_message(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        with_tx!(self, conn =>
            post_message_conn(conn, channel, channel_type, sender, content, capacity, now).await)
    }

    async fn post_message_authorized(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            post_message_conn(conn, channel, channel_type, sender, content, capacity, now).await
        })
    }

    async fn post_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        access_key: &str,
        expected_access_epoch: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            let id = post_message_conn(conn, channel, channel_type, sender, content, capacity, now).await?;
            let message = channel_history_conn(conn, channel, 0, None).await?.into_iter()
                .find(|message| message.id == id)
                .ok_or_else(|| AppError::internal("created chat message was not retained"))?;
            stage_delivery_outbox_conn(conn, &ChatDeliveryOutboxRecord {
                channel_id: channel.to_owned(), event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(channel, channel_type, delivery.event_type, &message)?,
                created_at: now, expires_at: delivery.expires_at,
            }).await?;
            Ok(message)
        })
    }

    async fn list_channels(
        &self,
        filter: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<ChannelSummary>> {
        with_conn!(self, conn => list_channels_conn(conn, filter, limit).await)
    }

    async fn channel_count(&self) -> AppResult<usize> {
        with_conn!(self, conn => channel_count_conn(conn).await)
    }

    async fn channel_history(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<Vec<ChatMessage>> {
        with_conn!(self, conn => channel_history_conn(conn, channel, limit, before_id).await)
    }

    async fn channel_history_authorized(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
        access_key: &str,
        expected_access_epoch: u64,
    ) -> AppResult<Vec<ChatMessage>> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            channel_history_conn(conn, channel, limit, before_id).await
        })
    }

    async fn edit_message(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        with_tx!(self, conn => edit_message_conn(conn, channel, id, content, now).await)
    }

    async fn edit_message_authorized(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            edit_message_conn(conn, channel, id, content, now).await
        })
    }

    async fn edit_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            let message = edit_message_conn(conn, channel, id, content, now).await?;
            stage_delivery_outbox_conn(conn, &ChatDeliveryOutboxRecord {
                channel_id: channel.to_owned(), event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(channel, channel_type, delivery.event_type, &message)?,
                created_at: now, expires_at: delivery.expires_at,
            }).await?;
            Ok(message)
        })
    }

    async fn delete_message(
        &self,
        channel: &str,
        id: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_tx!(self, conn => delete_message_conn(conn, channel, id, now).await)
    }

    async fn delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            delete_message_conn(conn, channel, id, now).await
        })
    }

    async fn delete_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<Option<ChatMessage>> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            if !delete_message_conn(conn, channel, id, now).await? { return Ok(None); }
            let message = channel_history_conn(conn, channel, 0, None).await?.into_iter()
                .find(|message| message.id == id)
                .ok_or_else(|| AppError::internal("tombstoned chat message was not retained"))?;
            stage_delivery_outbox_conn(conn, &ChatDeliveryOutboxRecord {
                channel_id: channel.to_owned(), event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(channel, channel_type, delivery.event_type, &message)?,
                created_at: now, expires_at: delivery.expires_at,
            }).await?;
            Ok(Some(message))
        })
    }

    async fn moderate_delete_message(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_tx!(self, conn => {
            let deleted = delete_message_conn(conn, channel, id, now).await?;
            if deleted {
                insert_moderation_audit_conn(conn, audit).await?;
            }
            Ok(deleted)
        })
    }

    async fn moderate_delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_tx!(self, conn => {
            ensure_access_epoch_conn(conn, access_key, expected_access_epoch).await?;
            let deleted = delete_message_conn(conn, channel, id, now).await?;
            if deleted {
                insert_moderation_audit_conn(conn, audit).await?;
            }
            Ok(deleted)
        })
    }

    async fn cleanup_moderation_audit(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        with_tx!(self, conn => cleanup_moderation_audit_conn(conn, before, limit).await)
    }

    async fn moderation_audit_count(&self) -> AppResult<usize> {
        with_conn!(self, conn => moderation_audit_count_conn(conn).await)
    }

    async fn consume_rate_limits(
        &self,
        limits: &[ChatRateLimit],
        now: TimestampMillis,
    ) -> AppResult<()> {
        with_tx!(self, conn => consume_rate_limits_conn(conn, limits, now).await)
    }

    async fn cleanup_rate_limits(&self, before: TimestampMillis, limit: usize) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        with_tx!(self, conn => cleanup_rate_limits_conn(conn, before, limit).await)
    }

    async fn stage_delivery_outbox(&self, record: ChatDeliveryOutboxRecord) -> AppResult<bool> {
        if record.expires_at <= record.created_at {
            return Err(AppError::validation(
                "chat delivery outbox expiry must be after creation",
            ));
        }
        with_tx!(self, conn => stage_delivery_outbox_conn(conn, &record).await)
    }

    async fn active_delivery_outbox(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> AppResult<Vec<ChatDeliveryOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        with_conn!(self, conn => active_delivery_outbox_conn(conn, now, limit).await)
    }

    async fn acknowledge_delivery_outbox(
        &self,
        channel_id: &str,
        event_id: u64,
    ) -> AppResult<bool> {
        with_tx!(self, conn => acknowledge_delivery_outbox_conn(conn, channel_id, event_id).await)
    }

    async fn cleanup_delivery_outbox(
        &self,
        through: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        with_tx!(self, conn => cleanup_delivery_outbox_conn(conn, through, limit).await)
    }
}

async fn stage_delivery_outbox_conn(
    conn: &mut PgConnection,
    record: &ChatDeliveryOutboxRecord,
) -> AppResult<bool> {
    if record.expires_at <= record.created_at {
        return Err(AppError::validation(
            "chat delivery outbox expiry must be after creation",
        ));
    }
    let event_id = to_i64(record.event_id, "chat delivery event id")?;
    let result = sqlx::query(INSERT_OUTBOX_SQL)
        .bind(&record.channel_id)
        .bind(event_id)
        .bind(to_i64(
            record.authority_epoch,
            "chat delivery authority epoch",
        )?)
        .bind(&record.payload)
        .bind(ts_to_millis(record.created_at)?)
        .bind(ts_to_millis(record.expires_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(result.rows_affected() == 1)
}

async fn active_delivery_outbox_conn(
    conn: &mut PgConnection,
    now: TimestampMillis,
    limit: usize,
) -> AppResult<Vec<ChatDeliveryOutboxRecord>> {
    let limit =
        i64::try_from(limit).map_err(|_| AppError::internal("outbox limit out of range"))?;
    let rows = sqlx::query(ACTIVE_OUTBOX_SQL)
        .bind(ts_to_millis(now)?)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(parse_outbox_record).collect()
}

async fn acknowledge_delivery_outbox_conn(
    conn: &mut PgConnection,
    channel_id: &str,
    event_id: u64,
) -> AppResult<bool> {
    let event_id = to_i64(event_id, "chat delivery event id")?;
    let result = sqlx::query(ACKNOWLEDGE_OUTBOX_SQL)
        .bind(channel_id)
        .bind(event_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(result.rows_affected() == 1)
}

async fn cleanup_delivery_outbox_conn(
    conn: &mut PgConnection,
    through: TimestampMillis,
    limit: usize,
) -> AppResult<usize> {
    let limit =
        i64::try_from(limit).map_err(|_| AppError::internal("outbox limit out of range"))?;
    let result = sqlx::query(DELETE_EXPIRED_OUTBOX_SQL)
        .bind(ts_to_millis(through)?)
        .bind(limit)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    usize::try_from(result.rows_affected())
        .map_err(|_| AppError::internal("outbox cleanup count out of range"))
}

async fn resolve_canonical_channel_conn(
    conn: &mut PgConnection,
    canonical_key: &str,
    channel_type: ChannelType,
    now: TimestampMillis,
) -> AppResult<ChatChannel> {
    if let Some(row) = sqlx::query(SELECT_CANONICAL_CHANNEL_SQL)
        .bind(canonical_key)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?
    {
        let channel = parse_canonical_channel(&row)?;
        if channel.channel_type != channel_type {
            return Err(AppError::internal("chat descriptor type conflict"));
        }
        return Ok(channel);
    }

    let id = new_opaque_channel_id()?;
    sqlx::query(INSERT_CANONICAL_CHANNEL_SQL)
        .bind(&id)
        .bind(channel_type.as_str())
        .bind(canonical_key)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let row = sqlx::query(SELECT_CANONICAL_CHANNEL_SQL)
        .bind(canonical_key)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let channel = parse_canonical_channel(&row)?;
    if channel.channel_type != channel_type {
        return Err(AppError::internal("chat descriptor type conflict"));
    }
    Ok(channel)
}

async fn current_access_epoch_conn(
    conn: &mut PgConnection,
    access_key: &str,
    now: TimestampMillis,
) -> AppResult<u64> {
    sqlx::query(ENSURE_ACCESS_EPOCH_SQL)
        .bind(access_key)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let row = sqlx::query(SELECT_ACCESS_EPOCH_LOCK_SQL)
        .bind(access_key)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let epoch: i64 = get(&row, "epoch")?;
    to_u64(epoch, "chat access epoch")
}

async fn advance_access_epoch_conn(
    conn: &mut PgConnection,
    access_key: &str,
    now: TimestampMillis,
) -> AppResult<u64> {
    let row = sqlx::query(ADVANCE_ACCESS_EPOCH_SQL)
        .bind(access_key)
        .bind(ts_to_millis(now)?)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let epoch: i64 = get(&row, "epoch")?;
    to_u64(epoch, "chat access epoch")
}

async fn ensure_access_epoch_conn(
    conn: &mut PgConnection,
    access_key: &str,
    expected_access_epoch: u64,
) -> AppResult<()> {
    let actual = match sqlx::query(SELECT_ACCESS_EPOCH_FOR_CHECK_SQL)
        .bind(access_key)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?
    {
        Some(row) => to_u64(get::<i64>(&row, "epoch")?, "chat access epoch")?,
        None => 0,
    };
    if actual == expected_access_epoch {
        Ok(())
    } else {
        Err(AppError::permission("CHAT_UNAVAILABLE"))
    }
}

#[allow(clippy::too_many_arguments)]
async fn post_message_conn(
    conn: &mut PgConnection,
    channel: &str,
    channel_type: ChannelType,
    sender: &str,
    content: &str,
    capacity: usize,
    now: TimestampMillis,
) -> AppResult<u64> {
    let head = sqlx::query(SELECT_HEAD_LOCK_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let (max_id, effective_type) = match head {
        Some(row) => {
            let id: i64 = get(&row, "id")?;
            let token: String = get(&row, "channel_type")?;
            (
                to_u64(id, "chat message id")?,
                ChannelType::from_token(&token)?,
            )
        }
        None => (0, channel_type),
    };
    let new_id = max_id + 1;
    let _event_head = sqlx::query(LOCK_EVENT_HEAD_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let event_row = sqlx::query(NEXT_EVENT_ID_SQL)
        .bind(channel)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let prior_event: i64 = get(&event_row, "event_id")?;
    let event_id = to_u64(prior_event, "chat event id")?
        .checked_add(1)
        .ok_or_else(|| AppError::internal("chat event id overflow"))?;
    sqlx::query(INSERT_SQL)
        .bind(channel)
        .bind(to_i64(new_id, "chat message id")?)
        .bind(effective_type.as_str())
        .bind(sender)
        .bind(content)
        .bind(ts_to_millis(now)?)
        .bind(ts_to_millis(now)?)
        .bind(to_i64(event_id, "chat event id")?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(INSERT_EVENT_SQL)
        .bind(channel)
        .bind(to_i64(event_id, "chat event id")?)
        .bind(to_i64(new_id, "chat message id")?)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let watermark = eviction_high_watermark(new_id, capacity);
    if watermark > 0 {
        sqlx::query(EVICT_SQL)
            .bind(channel)
            .bind(to_i64(watermark, "chat eviction watermark")?)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }
    Ok(new_id)
}

async fn list_channels_conn(
    conn: &mut PgConnection,
    filter: Option<&str>,
    limit: usize,
) -> AppResult<Vec<ChannelSummary>> {
    let rows = sqlx::query(LIST_CHANNELS_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let summaries = rows
        .iter()
        .map(parse_summary)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(finish_channel_listing(summaries, filter, limit))
}

async fn channel_count_conn(conn: &mut PgConnection) -> AppResult<usize> {
    let row = sqlx::query(COUNT_CHANNELS_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let n: i64 = get(&row, "n")?;
    usize::try_from(n).map_err(|_| AppError::internal("chat channel count out of range"))
}

async fn channel_history_conn(
    conn: &mut PgConnection,
    channel: &str,
    limit: usize,
    before_id: Option<u64>,
) -> AppResult<Vec<ChatMessage>> {
    let rows = sqlx::query(HISTORY_SQL)
        .bind(channel)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let chronological = rows
        .iter()
        .map(parse_message)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(page_history(chronological, limit, before_id))
}

async fn edit_message_conn(
    conn: &mut PgConnection,
    channel: &str,
    id: u64,
    content: &str,
    now: TimestampMillis,
) -> AppResult<ChatMessage> {
    let exists = sqlx::query(CHANNEL_EXISTS_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    if exists.is_none() {
        return Err(channel_not_found());
    }
    let id_i64 = to_i64(id, "chat message id")?;
    let row = sqlx::query(SELECT_MESSAGE_FULL_SQL)
        .bind(channel)
        .bind(id_i64)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?
        .ok_or_else(message_not_found)?;
    let message = parse_message(&row)?;
    if message.deleted {
        return Err(AppError::conflict("chat message is tombstoned"));
    }
    let _event_head = sqlx::query(LOCK_EVENT_HEAD_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let event_row = sqlx::query(NEXT_EVENT_ID_SQL)
        .bind(channel)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let prior_event: i64 = get(&event_row, "event_id")?;
    let event_id = to_u64(prior_event, "chat event id")?
        .checked_add(1)
        .ok_or_else(|| AppError::internal("chat event id overflow"))?;
    let revision = message
        .revision
        .checked_add(1)
        .ok_or_else(|| AppError::internal("chat message revision overflow"))?;
    sqlx::query(UPDATE_MESSAGE_SQL)
        .bind(content)
        .bind(ts_to_millis(now)?)
        .bind(to_i64(event_id, "chat event id")?)
        .bind(channel)
        .bind(id_i64)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(INSERT_UPDATE_EVENT_SQL)
        .bind(channel)
        .bind(to_i64(event_id, "chat event id")?)
        .bind(id_i64)
        .bind(to_i64(revision, "chat message revision")?)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(ChatMessage {
        content: content.to_owned(),
        updated_at_unix_ms: now.unix_millis(),
        revision,
        last_event_id: event_id,
        ..message
    })
}

async fn delete_message_conn(
    conn: &mut PgConnection,
    channel: &str,
    id: u64,
    now: TimestampMillis,
) -> AppResult<bool> {
    let exists = sqlx::query(CHANNEL_EXISTS_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    if exists.is_none() {
        return Err(channel_not_found());
    }
    let id_i64 = to_i64(id, "chat message id")?;
    let row = sqlx::query(SELECT_MESSAGE_FULL_SQL)
        .bind(channel)
        .bind(id_i64)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let message = parse_message(&row.ok_or_else(message_not_found)?)?;
    if message.deleted {
        return Ok(false);
    }
    let _event_head = sqlx::query(LOCK_EVENT_HEAD_SQL)
        .bind(channel)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let event_row = sqlx::query(NEXT_EVENT_ID_SQL)
        .bind(channel)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let prior_event: i64 = get(&event_row, "event_id")?;
    let event_id = to_u64(prior_event, "chat event id")?
        .checked_add(1)
        .ok_or_else(|| AppError::internal("chat event id overflow"))?;
    let revision = message
        .revision
        .checked_add(1)
        .ok_or_else(|| AppError::internal("chat message revision overflow"))?;
    sqlx::query(TOMBSTONE_SQL)
        .bind(ts_to_millis(now)?)
        .bind(to_i64(event_id, "chat event id")?)
        .bind(channel)
        .bind(id_i64)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(INSERT_DELETE_EVENT_SQL)
        .bind(channel)
        .bind(to_i64(event_id, "chat event id")?)
        .bind(id_i64)
        .bind(to_i64(revision, "chat message revision")?)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(true)
}

async fn insert_moderation_audit_conn(
    conn: &mut PgConnection,
    audit: &ChatModerationAudit,
) -> AppResult<()> {
    sqlx::query(INSERT_MODERATION_AUDIT_SQL)
        .bind(to_i64(
            audit.occurred_at_unix_ms,
            "chat moderation timestamp",
        )?)
        .bind(&audit.actor_kind)
        .bind(&audit.actor_id_hash)
        .bind(&audit.action)
        .bind(&audit.reason_code)
        .bind(&audit.channel_id_hash)
        .bind(to_i64(audit.message_id, "chat moderation message id")?)
        .bind(&audit.author_id_hash)
        .bind(to_i64(audit.authority_epoch, "chat access epoch")?)
        .bind(&audit.correlation_id)
        .bind(&audit.node_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn cleanup_moderation_audit_conn(
    conn: &mut PgConnection,
    before: TimestampMillis,
    limit: usize,
) -> AppResult<usize> {
    let result = sqlx::query(DELETE_EXPIRED_MODERATION_AUDIT_SQL)
        .bind(ts_to_millis(before)?)
        .bind(to_i64(
            u64::try_from(limit)
                .map_err(|_| AppError::internal("audit cleanup limit out of range"))?,
            "audit cleanup limit",
        )?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    usize::try_from(result.rows_affected())
        .map_err(|_| AppError::internal("audit cleanup count out of range"))
}

async fn moderation_audit_count_conn(conn: &mut PgConnection) -> AppResult<usize> {
    let row = sqlx::query(COUNT_MODERATION_AUDIT_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    usize::try_from(get::<i64>(&row, "n")?)
        .map_err(|_| AppError::internal("chat moderation audit count out of range"))
}

async fn consume_rate_limits_conn(
    conn: &mut PgConnection,
    limits: &[ChatRateLimit],
    now: TimestampMillis,
) -> AppResult<()> {
    let mut windows = Vec::with_capacity(limits.len());
    for rule in limits {
        if rule.limit == 0 || rule.window_ms == 0 || rule.key.is_empty() {
            return Err(AppError::internal("invalid chat rate-limit rule"));
        }
        let started_at = to_i64(
            now.unix_millis() / rule.window_ms * rule.window_ms,
            "chat rate-limit window",
        )?;
        sqlx::query(ENSURE_RATE_LIMIT_SQL)
            .bind(&rule.key)
            .bind(started_at)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        let row = sqlx::query(SELECT_RATE_LIMIT_SQL)
            .bind(&rule.key)
            .bind(started_at)
            .fetch_one(&mut *conn)
            .await
            .map_err(db_err)?;
        let used: i64 = get(&row, "used")?;
        if used < 0
            || u64::try_from(used)
                .map_err(|_| AppError::internal("chat rate-limit count out of range"))?
                >= u64::from(rule.limit)
        {
            return Err(AppError::permission("CHAT_RATE_LIMITED"));
        }
        windows.push((&rule.key, started_at));
    }
    for (key, started_at) in windows {
        sqlx::query(INCREMENT_RATE_LIMIT_SQL)
            .bind(key)
            .bind(started_at)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn cleanup_rate_limits_conn(
    conn: &mut PgConnection,
    before: TimestampMillis,
    limit: usize,
) -> AppResult<usize> {
    let result = sqlx::query(DELETE_EXPIRED_RATE_LIMIT_SQL)
        .bind(ts_to_millis(before)?)
        .bind(to_i64(
            u64::try_from(limit)
                .map_err(|_| AppError::internal("rate-limit cleanup limit out of range"))?,
            "rate-limit cleanup limit",
        )?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    usize::try_from(result.rows_affected())
        .map_err(|_| AppError::internal("rate-limit cleanup count out of range"))
}
