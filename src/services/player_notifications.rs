//! Player-addressed durable notification inbox.
//!
//! This deliberately does not reuse the operator-console notification feed.
//! Inbox rows live in two private storage collections, so the existing
//! transaction-capable storage backend supplies identical in-memory, SQLite,
//! Postgres, and CockroachDB persistence without changing the console schema.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::repository::{Backend, StorageRepository};
use crate::storage::{
    Accessor, Collection, Key, ListQuery, ObjectId, Owner, Permissions, Precondition,
    StorageObject, StorageValue, UserId, WriteRequest,
};
use crate::time::{Clock, SystemClock, TimestampMillis};

const INBOX_COLLECTION: &str = "citadel.player-notifications";
const DELIVERY_KEY_COLLECTION: &str = "citadel.player-notification-keys";
const MAX_PAGE_SIZE: usize = 100;
static ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Best-effort local delivery performed only after an inbox row has committed.
///
/// A failed live delivery never changes the durable send result: clients repair
/// an offline, full, or disconnected stream by listing their inbox.
pub trait PlayerNotificationDelivery: Send + Sync {
    /// Route a committed notification to any locally present recipient sessions.
    fn deliver(&self, recipient: &str, notification: &PlayerNotification);
}

/// One durable, player-addressed notification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerNotification {
    /// Opaque stable identifier. Clients deduplicate live delivery using it.
    pub id: String,
    /// Application or system classification code.
    pub code: i32,
    /// Short displayable subject.
    pub subject: String,
    /// Structured payload (always a JSON object).
    pub content: serde_json::Value,
    /// Optional attributable sender. It never grants authority.
    pub sender: Option<String>,
    /// Unix timestamp in milliseconds.
    pub created_at_unix_ms: u64,
    /// Unix timestamp in milliseconds when read, if any.
    pub read_at_unix_ms: Option<u64>,
}

/// Input accepted from authoritative game logic.
#[derive(Debug, Clone, PartialEq)]
pub struct SendPlayerNotification {
    /// Inbox owner.
    pub recipient: String,
    /// Application/system classification code.
    pub code: i32,
    /// Short displayable subject.
    pub subject: String,
    /// Structured payload.
    pub content: serde_json::Value,
    /// Optional attributed sender.
    pub sender: Option<String>,
    /// Stable producer key. Reusing it with the same payload is idempotent.
    pub delivery_key: Option<String>,
}

/// Result of a send, distinguishing a newly persisted row from a retry.
#[derive(Debug, Clone, PartialEq)]
pub struct SendPlayerNotificationOutcome {
    /// The persisted notification (new or original retry result).
    pub notification: PlayerNotification,
    /// True when a matching `(recipient, delivery_key)` already existed.
    pub duplicate: bool,
}

/// Recipient-scoped inbox page.
#[derive(Debug, Clone, PartialEq)]
pub struct PlayerNotificationPage {
    /// Newest-first items.
    pub items: Vec<PlayerNotification>,
    /// Opaque resume cursor, if another page exists.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeliveryKeyRecord {
    inbox_key: String,
    fingerprint: String,
}

/// Typed service for the player inbox.
///
/// Every persistent send uses one backend unit of work: its inbox row and its
/// optional delivery-key record become durable together, before any caller may
/// attempt realtime fan-out.
#[derive(Clone)]
pub struct PlayerNotificationService {
    backend: Arc<dyn Backend>,
    delivery: Arc<RwLock<Option<Arc<dyn PlayerNotificationDelivery>>>>,
}

impl PlayerNotificationService {
    /// Construct the service over the selected transactional backend.
    #[must_use]
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            delivery: Arc::new(RwLock::new(None)),
        }
    }

    /// Attach the process-local realtime delivery sink.
    ///
    /// This is installed after the gateway exists during transport bootstrap.
    /// It is intentionally replaceable for tests and does not participate in
    /// transaction success: persistence is the source of truth.
    pub fn set_delivery_sink(&self, delivery: Arc<dyn PlayerNotificationDelivery>) {
        if let Ok(mut sink) = self.delivery.write() {
            *sink = Some(delivery);
        }
    }

    /// Persist one notification, returning the original row for an idempotent
    /// retry. A repeated key with a different payload is a conflict.
    pub async fn send(
        &self,
        request: SendPlayerNotification,
        now: TimestampMillis,
    ) -> AppResult<SendPlayerNotificationOutcome> {
        validate_send(&request)?;
        let recipient_key = request.recipient.clone();
        let recipient = UserId::new(recipient_key.clone())?;
        let inbox_collection = collection(INBOX_COLLECTION)?;
        let keys_collection = collection(DELIVERY_KEY_COLLECTION)?;
        let fingerprint = fingerprint(&request);
        let uow = self.backend.begin().await?;
        let storage = uow.storage_repository();

        if let Some(delivery_key) = request.delivery_key.as_deref() {
            let key_id = object_id(
                &recipient,
                &keys_collection,
                &delivery_map_key(delivery_key),
            )?;
            if let Some(existing) = storage.read(&Accessor::Runtime, &key_id).await? {
                let record: DeliveryKeyRecord = decode(existing)?;
                if record.fingerprint != fingerprint {
                    uow.rollback().await?;
                    return Err(AppError::conflict(
                        "delivery_key already exists with a different notification payload",
                    ));
                }
                let inbox_id = object_id(&recipient, &inbox_collection, &record.inbox_key)?;
                let notification = storage
                    .read(&Accessor::Runtime, &inbox_id)
                    .await?
                    .ok_or_else(|| AppError::internal("notification delivery-key index is corrupt"))
                    .and_then(decode)?;
                uow.commit().await?;
                return Ok(SendPlayerNotificationOutcome {
                    notification,
                    duplicate: true,
                });
            }
        }

        let inbox_key = inbox_key(now);
        let notification = PlayerNotification {
            id: inbox_key.clone(),
            code: request.code,
            subject: request.subject,
            content: request.content,
            sender: request.sender,
            created_at_unix_ms: now.unix_millis(),
            read_at_unix_ms: None,
        };
        let inbox_id = object_id(&recipient, &inbox_collection, &inbox_key)?;
        write_json(
            storage.as_ref(),
            inbox_id,
            &notification,
            Precondition::MustNotExist,
        )
        .await?;
        if let Some(delivery_key) = request.delivery_key {
            let key_id = object_id(
                &recipient,
                &keys_collection,
                &delivery_map_key(&delivery_key),
            )?;
            let record = DeliveryKeyRecord {
                inbox_key,
                fingerprint,
            };
            write_json(
                storage.as_ref(),
                key_id,
                &record,
                Precondition::MustNotExist,
            )
            .await?;
        }
        uow.commit().await?;
        let outcome = SendPlayerNotificationOutcome {
            notification,
            duplicate: false,
        };
        // Commit precedes every live attempt. Do not fan out an idempotent retry:
        // the original successful attempt may already have reached the client and
        // the durable inbox remains the recovery path.
        if let Ok(sink) = self.delivery.read()
            && let Some(sink) = sink.as_ref()
        {
            sink.deliver(&recipient_key, &outcome.notification);
        }
        Ok(outcome)
    }

    /// List only `recipient`'s inbox, newest first. `cursor` is opaque.
    pub async fn list(
        &self,
        recipient: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> AppResult<PlayerNotificationPage> {
        let recipient = UserId::new(recipient.to_owned())?;
        let collection = collection(INBOX_COLLECTION)?;
        let mut query = ListQuery::for_owner(
            Owner::User(recipient),
            collection,
            limit.clamp(1, MAX_PAGE_SIZE),
        );
        if let Some(cursor) = cursor {
            query = query.after(crate::storage::Cursor::from_token(cursor.to_owned()));
        }
        let page = self
            .backend
            .storage_repository()
            .list(&Accessor::Runtime, &query)
            .await?;
        let items = page
            .items
            .into_iter()
            .map(decode)
            .collect::<AppResult<Vec<_>>>()?;
        Ok(PlayerNotificationPage {
            items,
            next_cursor: page.next.map(|next| next.as_str().to_owned()),
        })
    }

    /// Mark only `recipient`'s rows read. Missing ids are ignored and an
    /// already-read row is unchanged, so reconnect/retry is safe.
    pub async fn mark_read(
        &self,
        recipient: &str,
        ids: &[String],
        now: TimestampMillis,
    ) -> AppResult<Vec<String>> {
        let recipient = UserId::new(recipient.to_owned())?;
        let collection = collection(INBOX_COLLECTION)?;
        let uow = self.backend.begin().await?;
        let storage = uow.storage_repository();
        let mut changed = Vec::new();
        for id in ids.iter().take(MAX_PAGE_SIZE) {
            let object_id = object_id(&recipient, &collection, id)?;
            let Some(object) = storage.read(&Accessor::Runtime, &object_id).await? else {
                continue;
            };
            let mut notification: PlayerNotification = decode(object.clone())?;
            if notification.read_at_unix_ms.is_none() {
                notification.read_at_unix_ms = Some(now.unix_millis());
                write_json(
                    storage.as_ref(),
                    object_id,
                    &notification,
                    Precondition::Match(object.version),
                )
                .await?;
                changed.push(id.clone());
            }
        }
        uow.commit().await?;
        Ok(changed)
    }

    /// Convenience for trusted runtime producers using the system clock.
    pub async fn send_now(
        &self,
        request: SendPlayerNotification,
    ) -> AppResult<SendPlayerNotificationOutcome> {
        self.send(request, SystemClock.now()).await
    }
}

fn collection(value: &str) -> AppResult<Collection> {
    Collection::new(value)
}

fn object_id(user: &UserId, collection: &Collection, key: &str) -> AppResult<ObjectId> {
    Ok(ObjectId::new(
        Owner::User(user.clone()),
        collection.clone(),
        Key::new(key.to_owned())?,
    ))
}

fn validate_send(request: &SendPlayerNotification) -> AppResult<()> {
    UserId::new(request.recipient.clone())?;
    if request.subject.trim().is_empty() {
        return Err(AppError::validation("subject must not be empty"));
    }
    if !request.content.is_object() {
        return Err(AppError::validation("content must be a JSON object"));
    }
    if request
        .delivery_key
        .as_deref()
        .is_some_and(|key| key.is_empty() || key.len() > 128 || key.chars().any(char::is_control))
    {
        return Err(AppError::validation(
            "delivery_key must be 1..=128 non-control bytes",
        ));
    }
    Ok(())
}

fn fingerprint(request: &SendPlayerNotification) -> String {
    let body = serde_json::json!({"code": request.code, "subject": request.subject, "content": request.content, "sender": request.sender});
    hex(Sha256::digest(body.to_string().as_bytes()).as_slice())
}

fn delivery_map_key(delivery_key: &str) -> String {
    format!(
        "d-{}",
        hex(Sha256::digest(delivery_key.as_bytes()).as_slice())
    )
}

fn inbox_key(now: TimestampMillis) -> String {
    let inverted = u64::MAX - now.unix_millis();
    let sequence = ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("n-{inverted:020}-{sequence:020}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode<T: for<'de> Deserialize<'de>>(object: StorageObject) -> AppResult<T> {
    serde_json::from_value(object.value.into_json()).map_err(|error| {
        AppError::internal("corrupt player notification record").with_detail(error.to_string())
    })
}

async fn write_json<T: Serialize>(
    storage: &dyn StorageRepository,
    id: ObjectId,
    value: &T,
    expected: Precondition,
) -> AppResult<()> {
    let value = serde_json::to_value(value).map_err(|error| {
        AppError::internal("failed to encode player notification").with_detail(error.to_string())
    })?;
    storage
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(id, StorageValue::new(value)?, Permissions::runtime_only())
                .expecting(expected),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryBackend;

    fn service() -> PlayerNotificationService {
        PlayerNotificationService::new(Arc::new(InMemoryBackend::new()))
    }

    fn request(key: Option<&str>) -> SendPlayerNotification {
        SendPlayerNotification {
            recipient: "alice".to_owned(),
            code: 42,
            subject: "Reward ready".to_owned(),
            content: serde_json::json!({ "coins": 10 }),
            sender: Some("server".to_owned()),
            delivery_key: key.map(ToOwned::to_owned),
        }
    }

    #[tokio::test]
    async fn persistent_send_is_idempotent_per_recipient_and_key() {
        let service = service();
        let first = service
            .send(
                request(Some("reward:1")),
                TimestampMillis::from_unix_millis(10),
            )
            .await
            .expect("first send");
        let retry = service
            .send(
                request(Some("reward:1")),
                TimestampMillis::from_unix_millis(20),
            )
            .await
            .expect("retry");
        assert!(!first.duplicate);
        assert!(retry.duplicate);
        assert_eq!(first.notification, retry.notification);
        assert_eq!(
            service
                .list("alice", 10, None)
                .await
                .expect("list")
                .items
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn same_delivery_key_with_different_payload_conflicts() {
        let service = service();
        service
            .send(
                request(Some("reward:1")),
                TimestampMillis::from_unix_millis(10),
            )
            .await
            .expect("first send");
        let mut changed = request(Some("reward:1"));
        changed.subject = "Different reward".to_owned();
        assert_eq!(
            service
                .send(changed, TimestampMillis::from_unix_millis(11))
                .await
                .expect_err("conflict")
                .category(),
            crate::error::ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn inbox_is_recipient_scoped_newest_first_and_read_is_idempotent() {
        let service = service();
        let first = service
            .send(request(None), TimestampMillis::from_unix_millis(10))
            .await
            .expect("first send");
        let second = service
            .send(request(None), TimestampMillis::from_unix_millis(20))
            .await
            .expect("second send");
        let page = service.list("alice", 10, None).await.expect("list");
        assert_eq!(page.items[0].id, second.notification.id);
        assert_eq!(page.items[1].id, first.notification.id);
        assert!(
            service
                .list("bob", 10, None)
                .await
                .expect("other list")
                .items
                .is_empty()
        );
        let ids = vec![first.notification.id.clone()];
        assert_eq!(
            service
                .mark_read("alice", &ids, TimestampMillis::from_unix_millis(30))
                .await
                .expect("mark read"),
            ids
        );
        assert!(
            service
                .mark_read("alice", &ids, TimestampMillis::from_unix_millis(31))
                .await
                .expect("retry read")
                .is_empty()
        );
    }
}
