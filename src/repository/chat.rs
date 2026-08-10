//! Chat channel history repository contract.
//!
//! Persists chat channels and their per-channel bounded message history behind the
//! same repository seam as identity/session/storage/friends/groups/leaderboards,
//! so channels and the messages appended to them survive a node restart. A channel
//! is created implicitly by its first appended message; the [`ChannelType`] chosen
//! at creation is fixed for the channel's lifetime. Message ids are per-channel,
//! sequential, and monotonic starting at `1` — never reused even past eviction, so
//! a page's `before` cursor stays meaningful across the channel's whole lifetime.
//!
//! Following the friends/groups/leaderboards template, the non-trivial logic — the
//! retention/eviction bound, the newest-first history paging, and the channel
//! listing sort/filter — lives in exactly one place: the pure
//! [`eviction_high_watermark`] / [`page_history`] / [`finish_channel_listing`]
//! helpers, unit-tested directly here. Every backend
//! ([`InMemoryChatRepository`], the Postgres `PgChatRepository`, the SQLite
//! `SqliteChatRepository`) only does (lock/transaction) read → apply the pure
//! decision → write, so the three implementations cannot drift.
//!
//! There is deliberately no separate `channels` table: a channel exists iff a row
//! bears its id, its fixed type is denormalized onto every message row (constant
//! per channel), and its activity summary (`messages` = the monotonic append
//! counter, `last_activity`) is derived from the retained rows. Because eviction
//! removes the *oldest* rows, the newest row is always retained, so `MAX(id)` is
//! the channel's total-ever-appended counter even after eviction.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Default bound on retained messages per channel; oldest is evicted first.
pub const DEFAULT_CHANNEL_HISTORY_CAP: usize = 1000;

/// The kind of chat channel (mirrors Nakama's three channel shapes:
/// direct/group/room — see ``).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    /// A named, joinable room (the common "world chat"/lobby shape).
    Room,
    /// A group's channel.
    Group,
    /// A direct (1:1) channel between two accounts.
    Direct,
}

impl ChannelType {
    /// Stable lowercase token used in responses, the durable `channel_type`
    /// column, and parsed from requests.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Room => "room",
            Self::Group => "group",
            Self::Direct => "direct",
        }
    }

    /// Parse a channel type token (`room`, `group`, `direct`) from a request.
    ///
    /// # Errors
    /// Returns a [`Validation`](crate::error::ErrorCategory::Validation) error
    /// for any other token.
    pub fn parse(value: &str) -> AppResult<Self> {
        match value {
            "room" => Ok(Self::Room),
            "group" => Ok(Self::Group),
            "direct" => Ok(Self::Direct),
            other => Err(AppError::validation(format!(
                "unknown chat channel type: {other}"
            ))),
        }
    }

    /// Parse a stored `channel_type` token back into a [`ChannelType`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the known values —
    /// a corrupt/foreign row rather than a client-visible condition.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "room" => Ok(Self::Room),
            "group" => Ok(Self::Group),
            "direct" => Ok(Self::Direct),
            other => Err(AppError::internal(format!(
                "unknown chat channel type token `{other}`"
            ))),
        }
    }
}

/// One retained chat message, or its tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChatMessage {
    /// Per-channel sequential id (starts at 1, monotonic, never reused).
    pub id: u64,
    /// The sending identity, as presented by the producer. Not validated
    /// against the account/session stack at this layer.
    pub sender: String,
    /// Message body. Blanked once [`Self::deleted`] is set.
    pub content: String,
    /// When the message was appended (unix millis).
    pub created_at_unix_ms: u64,
    /// Last time the visible state changed (unix millis). Equals creation time
    /// until the author edits or a moderator tombstones the message.
    pub updated_at_unix_ms: u64,
    /// Monotonic revision of the message state. Creation starts at one.
    pub revision: u64,
    /// Per-channel durable event that produced this visible state.
    pub last_event_id: u64,
    /// Tombstoned (moderated away): the row remains, with `content` blanked,
    /// so ids and paging stay contiguous.
    pub deleted: bool,
}

/// One durable chat-event row awaiting bounded live delivery attempts.
///
/// It contains an opaque channel event payload only. Destination nodes are
/// resolved from current leases by the dispatcher and are deliberately not
/// persisted as a socket or participant capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatDeliveryOutboxRecord {
    /// Stable node that committed the mutation and exclusively owns delivery
    /// and acknowledgement of this row.
    pub origin_node_id: String,
    /// Opaque durable channel identifier.
    pub channel_id: String,
    /// Stable channel-scoped event identity.
    pub event_id: u64,
    /// Authorization fence captured by the committed mutation. A destination
    /// must match it before it may fan out the event locally.
    pub authority_epoch: u64,
    /// Serialized `KIND_CHAT_EVENT` body, bounded before persistence.
    pub payload: String,
    /// When the enclosing durable mutation committed.
    pub created_at: TimestampMillis,
    /// Exclusive retry deadline; history is the recovery path afterwards.
    pub expires_at: TimestampMillis,
}

/// Delivery metadata supplied with a committed chat mutation.
///
/// Repository implementations derive the event id and serialized event body
/// from the mutation result and insert the resulting outbox row before the
/// enclosing mutation transaction commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatDeliveryRequest {
    /// Stable node that is committing and will dispatch this mutation.
    pub origin_node_id: String,
    /// Authorization fence captured when the caller was authorized.
    pub authority_epoch: u64,
    /// Exclusive retry deadline for bounded live delivery.
    pub expires_at: TimestampMillis,
    /// Stable client-visible event discriminator.
    pub event_type: &'static str,
}

/// Serialize the one client-visible durable event form used by both immediate
/// local fan-out and persisted remote delivery. Keeping it at the repository
/// boundary lets durable backends write the exact payload in the transaction
/// that reserves `last_event_id`.
pub fn serialize_delivery_event(
    channel_id: &str,
    channel_type: ChannelType,
    event_type: &str,
    message: &ChatMessage,
) -> AppResult<String> {
    serde_json::to_string(&serde_json::json!({
        "version": 1,
        "type": event_type,
        "channel_id": channel_id,
        "channel_type": channel_type.as_str(),
        "event_id": message.last_event_id,
        "message": message,
    }))
    .map_err(|error| AppError::internal(format!("serialize chat delivery event: {error}")))
}

impl ChatDeliveryOutboxRecord {
    /// Whether the dispatcher may still attempt live delivery at `now`.
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        self.expires_at > now
    }
}

/// Redacted, durable evidence for a moderation tombstone. It deliberately has
/// no message text, channel locator, session id, or other replayable secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChatModerationAudit {
    /// When the moderation action committed.
    pub occurred_at_unix_ms: u64,
    /// Bounded authority class, such as `operator` or `room_authority`.
    pub actor_kind: String,
    /// SHA-256 of the actor id, rather than the raw identity.
    pub actor_id_hash: String,
    /// Bounded action token. This slice only writes `tombstone`.
    pub action: String,
    /// Bounded machine-readable reason code, never free-form text.
    pub reason_code: String,
    /// SHA-256 of the canonical opaque channel id.
    pub channel_id_hash: String,
    /// Logical message id inside the channel.
    pub message_id: u64,
    /// SHA-256 of the immutable message author id.
    pub author_id_hash: String,
    /// Captured access epoch, or zero for operator-only legacy channels.
    pub authority_epoch: u64,
    /// Correlation token supplied by a trusted boundary, if any.
    pub correlation_id: String,
    /// Stable server node id that performed the action.
    pub node_id: String,
}

/// One hashed, fixed-window counter requirement. Callers must never place a
/// raw account id or channel locator in `key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatRateLimit {
    /// Opaque key suitable for durable counter storage.
    pub key: String,
    /// Maximum successful actions in the fixed window.
    pub limit: u32,
    /// Window duration in milliseconds.
    pub window_ms: u64,
}

impl ChatModerationAudit {
    /// Construct a redacted tombstone audit record from trusted identities.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn tombstone(
        actor_kind: &str,
        actor_id: &str,
        reason_code: &str,
        channel: &str,
        message_id: u64,
        author_id: &str,
        authority_epoch: u64,
        correlation_id: &str,
        node_id: &str,
        now: TimestampMillis,
    ) -> Self {
        Self {
            occurred_at_unix_ms: now.unix_millis(),
            actor_kind: actor_kind.to_owned(),
            actor_id_hash: hash_audit_identifier(actor_id),
            action: "tombstone".to_owned(),
            reason_code: reason_code.to_owned(),
            channel_id_hash: hash_audit_identifier(channel),
            message_id,
            author_id_hash: hash_audit_identifier(author_id),
            authority_epoch,
            correlation_id: correlation_id.to_owned(),
            node_id: node_id.to_owned(),
        }
    }
}

fn hash_audit_identifier(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    format!("{digest:x}")
}

/// A channel summary row returned by [`ChatRepository::list_channels`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelSummary {
    /// The channel id, chosen by whoever first appended to it.
    pub channel: String,
    /// The channel's type.
    pub channel_type: &'static str,
    /// Total messages ever appended (monotonic; unaffected by eviction or
    /// tombstoning — this is the channel's activity counter, not the
    /// currently-retained row count).
    pub messages: u64,
    /// The most recent append's time (unix millis).
    pub last_activity_unix_ms: u64,
}

/// A durable server-owned chat descriptor.
///
/// `id` is intentionally opaque and is the only channel locator returned to a
/// player. `canonical_key` is kept server-side so direct pairs, groups, and live
/// rooms cannot be selected by a raw player-provided channel string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatChannel {
    /// Random opaque locator safe to return after authorization succeeds.
    pub id: String,
    /// Immutable channel kind.
    pub channel_type: ChannelType,
    /// Normalized server-owned descriptor subject.
    pub canonical_key: String,
}

// --- Pure decision helpers (the unit-tested logic) ---------------------------

/// The highest message id to evict for a channel after appending `new_id`, given
/// the retention `capacity`: every row with `id <= eviction_high_watermark(..)`
/// is removed. Returns `0` when nothing should be evicted.
///
/// Because ids are assigned `1, 2, 3, …` and only the *oldest* rows are evicted,
/// the retained set is always the contiguous suffix
/// `[new_id - capacity + 1 ..= new_id]`, so evicting everything at or below
/// `new_id - capacity` keeps exactly `capacity` rows. This is the single place the
/// durable backends compute eviction, mirroring the in-memory ring.
#[must_use]
pub fn eviction_high_watermark(new_id: u64, capacity: usize) -> u64 {
    let capacity = capacity.max(1) as u64;
    new_id.saturating_sub(capacity)
}

/// Page one channel's history newest-first from its retained rows.
///
/// `chronological` is the channel's retained messages in ascending-id (oldest
/// first) order. `before_id`, when present, resumes a page: only messages with
/// `id < before_id` are returned. `limit == 0` means unbounded (still capped by
/// how many messages are retained). The single place the read/paging semantics
/// live, so every backend returns identical pages.
#[must_use]
pub fn page_history(
    chronological: Vec<ChatMessage>,
    limit: usize,
    before_id: Option<u64>,
) -> Vec<ChatMessage> {
    let limit = if limit == 0 { usize::MAX } else { limit };
    chronological
        .into_iter()
        .rev()
        .filter(|message| before_id.is_none_or(|before| message.id < before))
        .take(limit)
        .collect()
}

/// Finish a channel listing: apply the optional case-sensitive substring `filter`
/// over the channel id, sort most-recently-active first (ties broken by ascending
/// channel id), and truncate to `limit` (`0` = unbounded). The single place the
/// listing order lives, shared by every backend.
#[must_use]
pub fn finish_channel_listing(
    mut rows: Vec<ChannelSummary>,
    filter: Option<&str>,
    limit: usize,
) -> Vec<ChannelSummary> {
    if let Some(filter) = filter {
        rows.retain(|row| row.channel.contains(filter));
    }
    rows.sort_by(|a, b| {
        b.last_activity_unix_ms
            .cmp(&a.last_activity_unix_ms)
            .then_with(|| a.channel.cmp(&b.channel))
    });
    let limit = if limit == 0 { usize::MAX } else { limit };
    rows.truncate(limit);
    rows
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for chat channels and their bounded message history.
///
/// A channel is created on first [`post_message`](ChatRepository::post_message);
/// its type is fixed at creation. Reads (`list_channels`, `channel_count`,
/// `channel_history`) never require a channel to exist — an unknown channel lists
/// nothing / returns an empty page. Only [`delete_message`](
/// ChatRepository::delete_message) is existence-authoritative (unknown channel or
/// id is `NotFound`).
#[async_trait]
pub trait ChatRepository: Send + Sync {
    /// Resolve a canonical server-side descriptor to its durable opaque channel.
    ///
    /// The descriptor is unique across the selected backend. A caller must run
    /// authorization before invoking this method; it never accepts a player
    /// channel name or a caller-selected channel type.
    async fn resolve_canonical_channel(
        &self,
        canonical_key: &str,
        channel_type: ChannelType,
        now: TimestampMillis,
    ) -> AppResult<ChatChannel>;

    /// Return the version for an authority subject, creating epoch zero lazily.
    ///
    /// Access epochs live with chat persistence so independent server nodes can
    /// fence an authorization grant against a social or group revocation.
    async fn current_access_epoch(&self, access_key: &str) -> AppResult<u64>;

    /// Advance an authority subject's epoch after a social/group transition.
    /// The caller must serialize the source transition with this advance; the
    /// chat mutation path compares its captured value before it writes.
    async fn advance_access_epoch(&self, access_key: &str, now: TimestampMillis) -> AppResult<u64>;

    /// Append a message to `channel`, creating it (as `channel_type`) on first
    /// use and enforcing the retention `capacity` (oldest rows evicted). If the
    /// channel already exists, `channel_type` is ignored — the type it was
    /// created with wins. Returns the assigned per-channel message id.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn post_message(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64>;

    /// Append only if the authority subject still has `expected_access_epoch`.
    /// The check and write are one repository transaction on durable backends,
    /// closing the grant-revocation race for player-visible mutations.
    #[allow(clippy::too_many_arguments)]
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
    ) -> AppResult<u64>;

    /// Commit an authorized append and its bounded remote-delivery source row
    /// in one transaction. The returned message contains the reserved event id.
    #[allow(clippy::too_many_arguments)]
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
    ) -> AppResult<ChatMessage>;

    /// List channels, most-recently-active first (ties by channel id), with an
    /// optional case-sensitive substring `filter` over the channel id and a
    /// `limit` on rows (`0` = unbounded).
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list_channels(
        &self,
        filter: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<ChannelSummary>>;

    /// Total channel count, unaffected by any filter/limit.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn channel_count(&self) -> AppResult<usize>;

    /// Page one channel's history, newest-first by id. `before_id` resumes a page
    /// (only ids `< before_id`); `limit == 0` is unbounded. An unknown channel is
    /// an empty page, not an error.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn channel_history(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<Vec<ChatMessage>>;

    /// Read history only if the authority subject still has
    /// `expected_access_epoch`. An epoch mismatch intentionally returns the
    /// generic unavailable error so protected history cannot be enumerated.
    async fn channel_history_authorized(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
        access_key: &str,
        expected_access_epoch: u64,
    ) -> AppResult<Vec<ChatMessage>>;

    /// Replace the visible content of a retained non-tombstoned message and
    /// reserve its next state event. Authorization and edit-window checks live
    /// in the chat service; this operation keeps the durable state transition
    /// atomic on each backend.
    async fn edit_message(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage>;

    /// Edit through a still-current authority grant. The epoch check and
    /// revision/event transition share one transaction on durable backends.
    #[allow(clippy::too_many_arguments)]
    async fn edit_message_authorized(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage>;

    /// Commit an authorized edit and its delivery source row atomically.
    #[allow(clippy::too_many_arguments)]
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
    ) -> AppResult<ChatMessage>;

    /// Tombstone one message: blank its content and mark it deleted. Idempotent —
    /// deleting an already-tombstoned message returns `Ok(false)`.
    ///
    /// # Errors
    /// - `NotFound` when the channel or the message id within it is unknown.
    /// - A backend error on failure.
    async fn delete_message(&self, channel: &str, id: u64, now: TimestampMillis)
    -> AppResult<bool>;

    /// Tombstone through a still-current authority grant. As for edits, this
    /// makes a committed revocation win over a stale mutation.
    #[allow(clippy::too_many_arguments)]
    async fn delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Commit an authorized tombstone and its delivery source row atomically.
    /// A repeat tombstone returns `None` and creates no source row.
    #[allow(clippy::too_many_arguments)]
    async fn delete_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<Option<ChatMessage>>;

    /// Tombstone a message and retain one redacted moderation audit record in
    /// the same transaction. A no-op repeat tombstone writes no second audit.
    async fn moderate_delete_message(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Tombstone with a redacted moderation audit through a still-current
    /// authority grant. Durable backends check the access epoch, write the
    /// tombstone, and retain the audit record in one transaction.
    #[allow(clippy::too_many_arguments)]
    async fn moderate_delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Remove at most `limit` durable moderation audits older than `before`.
    /// The independent audit retention window never changes message retention.
    async fn cleanup_moderation_audit(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize>;

    /// Number of retained moderation audits. This supports operator health and
    /// backend contract tests without exposing audit content to player paths.
    async fn moderation_audit_count(&self) -> AppResult<usize>;

    /// Atomically consume every supplied fixed-window counter or none of them.
    /// This repository-owned boundary is shared by all nodes using the same
    /// durable database and fails closed on a persistence error.
    async fn consume_rate_limits(
        &self,
        limits: &[ChatRateLimit],
        now: TimestampMillis,
    ) -> AppResult<()>;

    /// Bounded maintenance for expired fixed-window counters.
    async fn cleanup_rate_limits(&self, before: TimestampMillis, limit: usize) -> AppResult<usize>;

    /// Idempotently stage one committed source event for bounded live delivery.
    /// Destination leases are deliberately resolved only by the dispatcher.
    async fn stage_delivery_outbox(&self, record: ChatDeliveryOutboxRecord) -> AppResult<bool>;

    /// Read active source rows owned by `origin_node_id` only.
    async fn active_delivery_outbox(
        &self,
        origin_node_id: &str,
        now: TimestampMillis,
        limit: usize,
    ) -> AppResult<Vec<ChatDeliveryOutboxRecord>>;

    /// Remove an owned source row only after terminal dispatcher acknowledgement.
    async fn acknowledge_delivery_outbox(
        &self,
        origin_node_id: &str,
        channel_id: &str,
        event_id: u64,
    ) -> AppResult<bool>;

    /// Remove at most `limit` source rows whose exclusive retry deadline has
    /// elapsed at or before `through`.
    async fn cleanup_delivery_outbox(
        &self,
        through: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize>;
}

/// The stable "no such chat channel" error, shared by every backend.
pub(crate) fn channel_not_found() -> AppError {
    AppError::not_found("chat channel not found")
}

/// The stable "no such chat message" error, shared by every backend.
pub(crate) fn message_not_found() -> AppError {
    AppError::not_found("chat message not found")
}

// --- In-memory reference implementation --------------------------------------

/// One channel's mutable state: identity, activity bookkeeping, and its bounded
/// message ring.
#[derive(Debug)]
struct ChannelState {
    channel_type: ChannelType,
    last_activity_unix_ms: u64,
    /// The id assigned to the most recently appended message (`0` before the
    /// first append, which cannot happen — a channel is only ever created by an
    /// append).
    next_id: u64,
    /// Per-channel durable state-event sequence, independent of message ids.
    next_event_id: u64,
    /// Newest-appended-last ring, bounded to the per-call `capacity`.
    history: VecDeque<ChatMessage>,
}

/// The channel store: `channel id -> ChannelState`. A named alias keeps the guard
/// types readable.
type ChannelStore = HashMap<String, ChannelState>;

/// A contract-faithful, in-memory [`ChatRepository`] (the reference impl).
///
/// Single-process and not durable, but it enforces the full channel/history/
/// eviction/tombstone contract through the shared pure helpers, so the contract
/// tests in `tests/chat_repository_contract.rs` can be reused against the durable
/// backends.
#[derive(Debug, Default)]
pub struct InMemoryChatRepository {
    channels: Mutex<ChannelStore>,
    canonical_channels: Mutex<HashMap<String, ChatChannel>>,
    access_epochs: Mutex<HashMap<String, u64>>,
    moderation_audit: Mutex<Vec<ChatModerationAudit>>,
    rate_limits: Mutex<HashMap<(String, u64), u32>>,
    outbox: Mutex<Vec<ChatDeliveryOutboxRecord>>,
}

impl InMemoryChatRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, ChannelStore>> {
        self.channels
            .lock()
            .map_err(|_| AppError::internal("chat repository mutex poisoned"))
    }

    fn canonical_guard(
        &self,
    ) -> AppResult<std::sync::MutexGuard<'_, HashMap<String, ChatChannel>>> {
        self.canonical_channels
            .lock()
            .map_err(|_| AppError::internal("chat descriptor mutex poisoned"))
    }

    fn access_epochs_guard(&self) -> AppResult<std::sync::MutexGuard<'_, HashMap<String, u64>>> {
        self.access_epochs
            .lock()
            .map_err(|_| AppError::internal("chat access epoch mutex poisoned"))
    }

    /// Stage one source event for live delivery. This mirrors the durable
    /// backend's `(channel_id, event_id)` idempotency boundary for deterministic
    /// local tests; the in-memory repository itself is intentionally volatile.
    pub fn stage_delivery_outbox(&self, record: ChatDeliveryOutboxRecord) -> AppResult<bool> {
        if record.origin_node_id.is_empty() {
            return Err(AppError::validation(
                "chat delivery outbox origin node is required",
            ));
        }
        if record.expires_at <= record.created_at {
            return Err(AppError::validation(
                "chat delivery outbox expiry must be after creation",
            ));
        }
        let mut outbox = self
            .outbox
            .lock()
            .map_err(|_| AppError::internal("chat delivery outbox mutex poisoned"))?;
        if outbox.iter().any(|current| {
            current.channel_id == record.channel_id && current.event_id == record.event_id
        }) {
            return Ok(false);
        }
        outbox.push(record);
        Ok(true)
    }

    /// Read active source rows in deterministic channel/event order.
    pub fn active_delivery_outbox(
        &self,
        origin_node_id: &str,
        now: TimestampMillis,
        limit: usize,
    ) -> AppResult<Vec<ChatDeliveryOutboxRecord>> {
        if origin_node_id.is_empty() {
            return Err(AppError::validation(
                "chat delivery outbox origin node is required",
            ));
        }
        let outbox = self
            .outbox
            .lock()
            .map_err(|_| AppError::internal("chat delivery outbox mutex poisoned"))?;
        let mut active: Vec<_> = outbox
            .iter()
            .filter(|record| record.origin_node_id == origin_node_id && record.is_current_at(now))
            .cloned()
            .collect();
        active.sort_by(|left, right| {
            (&left.channel_id, left.event_id).cmp(&(&right.channel_id, right.event_id))
        });
        active.truncate(limit);
        Ok(active)
    }

    /// Remove one source row after the dispatcher receives its terminal
    /// acknowledgement. Retrying or merely reading a row never consumes it.
    pub fn acknowledge_delivery_outbox(
        &self,
        origin_node_id: &str,
        channel_id: &str,
        event_id: u64,
    ) -> AppResult<bool> {
        if origin_node_id.is_empty() {
            return Err(AppError::validation(
                "chat delivery outbox origin node is required",
            ));
        }
        let mut outbox = self
            .outbox
            .lock()
            .map_err(|_| AppError::internal("chat delivery outbox mutex poisoned"))?;
        let initial_len = outbox.len();
        outbox.retain(|record| {
            record.origin_node_id != origin_node_id
                || record.channel_id != channel_id
                || record.event_id != event_id
        });
        Ok(outbox.len() != initial_len)
    }

    /// Purge a bounded number of expired source rows.
    pub fn cleanup_delivery_outbox(
        &self,
        through: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut outbox = self
            .outbox
            .lock()
            .map_err(|_| AppError::internal("chat delivery outbox mutex poisoned"))?;
        let mut removed = 0;
        outbox.retain(|record| {
            let expired = record.expires_at <= through && removed < limit;
            removed += usize::from(expired);
            !expired
        });
        Ok(removed)
    }
}

#[async_trait]
impl ChatRepository for InMemoryChatRepository {
    async fn resolve_canonical_channel(
        &self,
        canonical_key: &str,
        channel_type: ChannelType,
        _now: TimestampMillis,
    ) -> AppResult<ChatChannel> {
        let mut channels = self.canonical_guard()?;
        if let Some(channel) = channels.get(canonical_key) {
            if channel.channel_type != channel_type {
                return Err(AppError::internal("chat descriptor type conflict"));
            }
            return Ok(channel.clone());
        }
        for _ in 0..8 {
            let id = new_opaque_channel_id()?;
            if channels.values().all(|channel| channel.id != id) {
                let channel = ChatChannel {
                    id,
                    channel_type,
                    canonical_key: canonical_key.to_owned(),
                };
                channels.insert(canonical_key.to_owned(), channel.clone());
                return Ok(channel);
            }
        }
        Err(AppError::internal(
            "could not allocate a unique chat channel id",
        ))
    }

    async fn current_access_epoch(&self, access_key: &str) -> AppResult<u64> {
        let mut epochs = self.access_epochs_guard()?;
        Ok(*epochs.entry(access_key.to_owned()).or_insert(0))
    }

    async fn advance_access_epoch(
        &self,
        access_key: &str,
        _now: TimestampMillis,
    ) -> AppResult<u64> {
        let mut epochs = self.access_epochs_guard()?;
        let epoch = epochs.entry(access_key.to_owned()).or_insert(0);
        *epoch = epoch.saturating_add(1);
        Ok(*epoch)
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
        let capacity = capacity.max(1);
        let mut channels = self.guard()?;
        let state = channels
            .entry(channel.to_string())
            .or_insert_with(|| ChannelState {
                channel_type,
                last_activity_unix_ms: now.unix_millis(),
                next_id: 0,
                next_event_id: 0,
                history: VecDeque::new(),
            });
        state.next_id += 1;
        let id = state.next_id;
        state.next_event_id += 1;
        let event_id = state.next_event_id;
        state.last_activity_unix_ms = now.unix_millis();
        // Evict oldest until pushing keeps at most `capacity` retained, mirroring
        // the durable `eviction_high_watermark`.
        while state.history.len() >= capacity {
            state.history.pop_front();
        }
        state.history.push_back(ChatMessage {
            id,
            sender: sender.to_string(),
            content: content.to_string(),
            created_at_unix_ms: now.unix_millis(),
            updated_at_unix_ms: now.unix_millis(),
            revision: 1,
            last_event_id: event_id,
            deleted: false,
        });
        Ok(id)
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
        if self.current_access_epoch(access_key).await? != expected_access_epoch {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        }
        self.post_message(channel, channel_type, sender, content, capacity, now)
            .await
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
        let id = self
            .post_message_authorized(
                channel,
                channel_type,
                sender,
                content,
                capacity,
                access_key,
                expected_access_epoch,
                now,
            )
            .await?;
        let message = self
            .channel_history(channel, 0, None)
            .await?
            .into_iter()
            .find(|message| message.id == id)
            .ok_or_else(|| AppError::internal("created chat message was not retained"))?;
        InMemoryChatRepository::stage_delivery_outbox(
            self,
            ChatDeliveryOutboxRecord {
                origin_node_id: delivery.origin_node_id.clone(),
                channel_id: channel.to_owned(),
                event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(
                    channel,
                    channel_type,
                    delivery.event_type,
                    &message,
                )?,
                created_at: now,
                expires_at: delivery.expires_at,
            },
        )?;
        Ok(message)
    }

    async fn list_channels(
        &self,
        filter: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<ChannelSummary>> {
        let channels = self.guard()?;
        let rows = channels
            .iter()
            .map(|(name, state)| ChannelSummary {
                channel: name.clone(),
                channel_type: state.channel_type.as_str(),
                messages: state.next_id,
                last_activity_unix_ms: state.last_activity_unix_ms,
            })
            .collect();
        Ok(finish_channel_listing(rows, filter, limit))
    }

    async fn channel_count(&self) -> AppResult<usize> {
        Ok(self.guard()?.len())
    }

    async fn channel_history(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<Vec<ChatMessage>> {
        let channels = self.guard()?;
        let Some(state) = channels.get(channel) else {
            return Ok(Vec::new());
        };
        let chronological = state.history.iter().cloned().collect();
        Ok(page_history(chronological, limit, before_id))
    }

    async fn channel_history_authorized(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
        access_key: &str,
        expected_access_epoch: u64,
    ) -> AppResult<Vec<ChatMessage>> {
        if self.current_access_epoch(access_key).await? != expected_access_epoch {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        }
        self.channel_history(channel, limit, before_id).await
    }

    async fn edit_message(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let mut channels = self.guard()?;
        let state = channels.get_mut(channel).ok_or_else(channel_not_found)?;
        let event_id = state
            .next_event_id
            .checked_add(1)
            .ok_or_else(|| AppError::internal("chat event id overflow"))?;
        let message = state
            .history
            .iter_mut()
            .find(|message| message.id == id)
            .ok_or_else(message_not_found)?;
        if message.deleted {
            return Err(AppError::conflict("chat message is tombstoned"));
        }
        message.content = content.to_owned();
        message.revision = message
            .revision
            .checked_add(1)
            .ok_or_else(|| AppError::internal("chat message revision overflow"))?;
        message.updated_at_unix_ms = now.unix_millis();
        message.last_event_id = event_id;
        state.next_event_id = event_id;
        Ok(message.clone())
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
        if self.current_access_epoch(access_key).await? != expected_access_epoch {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        }
        self.edit_message(channel, id, content, now).await
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
        let message = self
            .edit_message_authorized(channel, id, content, access_key, expected_access_epoch, now)
            .await?;
        InMemoryChatRepository::stage_delivery_outbox(
            self,
            ChatDeliveryOutboxRecord {
                origin_node_id: delivery.origin_node_id.clone(),
                channel_id: channel.to_owned(),
                event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(
                    channel,
                    channel_type,
                    delivery.event_type,
                    &message,
                )?,
                created_at: now,
                expires_at: delivery.expires_at,
            },
        )?;
        Ok(message)
    }

    async fn delete_message(
        &self,
        channel: &str,
        id: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let mut channels = self.guard()?;
        let state = channels.get_mut(channel).ok_or_else(channel_not_found)?;
        let event_id = state
            .next_event_id
            .checked_add(1)
            .ok_or_else(|| AppError::internal("chat event id overflow"))?;
        let message = state
            .history
            .iter_mut()
            .find(|message| message.id == id)
            .ok_or_else(message_not_found)?;
        if message.deleted {
            return Ok(false);
        }
        message.deleted = true;
        message.content.clear();
        message.revision = message
            .revision
            .checked_add(1)
            .ok_or_else(|| AppError::internal("chat message revision overflow"))?;
        message.updated_at_unix_ms = now.unix_millis();
        message.last_event_id = event_id;
        state.next_event_id = event_id;
        Ok(true)
    }

    async fn delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        if self.current_access_epoch(access_key).await? != expected_access_epoch {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        }
        self.delete_message(channel, id, now).await
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
        if !self
            .delete_message_authorized(channel, id, access_key, expected_access_epoch, now)
            .await?
        {
            return Ok(None);
        }
        let message = self
            .channel_history(channel, 0, None)
            .await?
            .into_iter()
            .find(|message| message.id == id)
            .ok_or_else(|| AppError::internal("tombstoned chat message was not retained"))?;
        InMemoryChatRepository::stage_delivery_outbox(
            self,
            ChatDeliveryOutboxRecord {
                origin_node_id: delivery.origin_node_id.clone(),
                channel_id: channel.to_owned(),
                event_id: message.last_event_id,
                authority_epoch: delivery.authority_epoch,
                payload: serialize_delivery_event(
                    channel,
                    channel_type,
                    delivery.event_type,
                    &message,
                )?,
                created_at: now,
                expires_at: delivery.expires_at,
            },
        )?;
        Ok(Some(message))
    }

    async fn moderate_delete_message(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let deleted = self.delete_message(channel, id, now).await?;
        if deleted {
            self.moderation_audit
                .lock()
                .map_err(|_| AppError::internal("chat moderation audit mutex poisoned"))?
                .push(audit.clone());
        }
        Ok(deleted)
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
        if self.current_access_epoch(access_key).await? != expected_access_epoch {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        }
        self.moderate_delete_message(channel, id, audit, now).await
    }

    async fn cleanup_moderation_audit(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut audit = self
            .moderation_audit
            .lock()
            .map_err(|_| AppError::internal("chat moderation audit mutex poisoned"))?;
        let mut removed = 0;
        audit.retain(|entry| {
            let expired = entry.occurred_at_unix_ms < before.unix_millis() && removed < limit;
            removed += usize::from(expired);
            !expired
        });
        Ok(removed)
    }

    async fn moderation_audit_count(&self) -> AppResult<usize> {
        Ok(self
            .moderation_audit
            .lock()
            .map_err(|_| AppError::internal("chat moderation audit mutex poisoned"))?
            .len())
    }

    async fn consume_rate_limits(
        &self,
        limits: &[ChatRateLimit],
        now: TimestampMillis,
    ) -> AppResult<()> {
        let mut counters = self
            .rate_limits
            .lock()
            .map_err(|_| AppError::internal("chat rate-limit mutex poisoned"))?;
        let mut normalized = Vec::with_capacity(limits.len());
        for rule in limits {
            if rule.limit == 0 || rule.window_ms == 0 || rule.key.is_empty() {
                return Err(AppError::internal("invalid chat rate-limit rule"));
            }
            let window = now.unix_millis() / rule.window_ms * rule.window_ms;
            let used = counters
                .get(&(rule.key.clone(), window))
                .copied()
                .unwrap_or(0);
            if used >= rule.limit {
                return Err(AppError::permission("CHAT_RATE_LIMITED"));
            }
            normalized.push((rule.key.clone(), window));
        }
        for key in normalized {
            *counters.entry(key).or_insert(0) += 1;
        }
        Ok(())
    }

    async fn cleanup_rate_limits(&self, before: TimestampMillis, limit: usize) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut counters = self
            .rate_limits
            .lock()
            .map_err(|_| AppError::internal("chat rate-limit mutex poisoned"))?;
        let mut removed = 0;
        counters.retain(|(_, window), _| {
            let expired = *window < before.unix_millis() && removed < limit;
            removed += usize::from(expired);
            !expired
        });
        Ok(removed)
    }

    async fn stage_delivery_outbox(&self, record: ChatDeliveryOutboxRecord) -> AppResult<bool> {
        InMemoryChatRepository::stage_delivery_outbox(self, record)
    }

    async fn active_delivery_outbox(
        &self,
        origin_node_id: &str,
        now: TimestampMillis,
        limit: usize,
    ) -> AppResult<Vec<ChatDeliveryOutboxRecord>> {
        InMemoryChatRepository::active_delivery_outbox(self, origin_node_id, now, limit)
    }

    async fn acknowledge_delivery_outbox(
        &self,
        origin_node_id: &str,
        channel_id: &str,
        event_id: u64,
    ) -> AppResult<bool> {
        InMemoryChatRepository::acknowledge_delivery_outbox(
            self,
            origin_node_id,
            channel_id,
            event_id,
        )
    }

    async fn cleanup_delivery_outbox(
        &self,
        through: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        InMemoryChatRepository::cleanup_delivery_outbox(self, through, limit)
    }
}

/// Generate an opaque 192-bit server channel id.
pub(crate) fn new_opaque_channel_id() -> AppResult<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes).map_err(|error| {
        AppError::internal("could not generate a chat channel id").with_detail(error.to_string())
    })?;
    Ok(format!(
        "ch_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    #[test]
    fn outbox_record_is_live_only_before_its_exclusive_expiry() {
        let record = ChatDeliveryOutboxRecord {
            origin_node_id: "node-a".to_owned(),
            channel_id: "ch_1".to_owned(),
            event_id: 4,
            authority_epoch: 2,
            payload: "{}".to_owned(),
            created_at: ts(10),
            expires_at: ts(20),
        };
        assert!(record.is_current_at(ts(19)));
        assert!(!record.is_current_at(ts(20)));
    }

    #[test]
    fn in_memory_outbox_is_idempotent_acknowledged_and_expires_records() {
        let repository = InMemoryChatRepository::new();
        let active = ChatDeliveryOutboxRecord {
            origin_node_id: "node-a".to_owned(),
            channel_id: "ch_b".to_owned(),
            event_id: 2,
            authority_epoch: 2,
            payload: "{}".to_owned(),
            created_at: ts(1),
            expires_at: ts(10),
        };
        assert!(
            repository
                .stage_delivery_outbox(active.clone())
                .expect("staged")
        );
        assert!(
            !repository
                .stage_delivery_outbox(active.clone())
                .expect("idempotent")
        );
        assert_eq!(
            repository
                .active_delivery_outbox("node-a", ts(9), 1)
                .expect("active rows"),
            vec![active.clone()]
        );
        assert!(
            repository
                .acknowledge_delivery_outbox("node-a", "ch_b", 2)
                .expect("acknowledged")
        );
        assert!(
            !repository
                .acknowledge_delivery_outbox("node-a", "ch_b", 2)
                .expect("idempotent acknowledgement")
        );
        assert!(
            repository
                .active_delivery_outbox("node-a", ts(9), 1)
                .expect("acknowledged rows")
                .is_empty()
        );
        let expired = ChatDeliveryOutboxRecord {
            origin_node_id: "node-a".to_owned(),
            channel_id: "ch_b".to_owned(),
            event_id: 3,
            authority_epoch: 2,
            payload: "{}".to_owned(),
            created_at: ts(1),
            expires_at: ts(10),
        };
        assert!(
            repository
                .stage_delivery_outbox(expired)
                .expect("staged expired row")
        );
        assert!(
            repository
                .active_delivery_outbox("node-a", ts(10), 1)
                .expect("expired rows")
                .is_empty()
        );
        assert_eq!(
            repository
                .cleanup_delivery_outbox(ts(10), 1)
                .expect("purged expired row"),
            1
        );
    }

    #[test]
    fn in_memory_outbox_rejects_a_non_positive_retry_window() {
        let repository = InMemoryChatRepository::new();
        let error = repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                origin_node_id: "node-a".to_owned(),
                channel_id: "ch_window".to_owned(),
                event_id: 1,
                authority_epoch: 2,
                payload: "{}".to_owned(),
                created_at: ts(10),
                expires_at: ts(10),
            })
            .expect_err("equal timestamps must not create a retry window");

        assert_eq!(error.category(), crate::error::ErrorCategory::Validation);
        assert!(
            repository
                .active_delivery_outbox("node-a", ts(10), 1)
                .expect("no invalid row was staged")
                .is_empty()
        );
    }

    fn message(id: u64, content: &str) -> ChatMessage {
        ChatMessage {
            id,
            sender: "u".to_string(),
            content: content.to_string(),
            created_at_unix_ms: id,
            updated_at_unix_ms: id,
            revision: 1,
            last_event_id: id,
            deleted: false,
        }
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn channel_type_tokens_round_trip() {
        for kind in [ChannelType::Room, ChannelType::Group, ChannelType::Direct] {
            assert_eq!(ChannelType::parse(kind.as_str()).expect("parse"), kind);
            assert_eq!(ChannelType::from_token(kind.as_str()).expect("token"), kind);
        }
        assert!(ChannelType::parse("bogus").is_err());
        assert!(ChannelType::from_token("bogus").is_err());
    }

    #[test]
    fn eviction_high_watermark_keeps_capacity_newest() {
        // new_id 5, cap 3 -> evict ids <= 2 (retain 3,4,5).
        assert_eq!(eviction_high_watermark(5, 3), 2);
        // Not yet full: nothing to evict.
        assert_eq!(eviction_high_watermark(3, 3), 0);
        assert_eq!(eviction_high_watermark(1, 1000), 0);
        // Zero capacity clamps to one (retain only the newest).
        assert_eq!(eviction_high_watermark(5, 0), 4);
    }

    #[test]
    fn page_history_is_newest_first_with_before_and_limit() {
        let all = vec![
            message(1, "a"),
            message(2, "b"),
            message(3, "c"),
            message(4, "d"),
        ];
        let ids: Vec<u64> = page_history(all.clone(), 0, None)
            .iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(ids, vec![4, 3, 2, 1], "newest first, unbounded");

        let ids: Vec<u64> = page_history(all.clone(), 2, None)
            .iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(ids, vec![4, 3]);

        let ids: Vec<u64> = page_history(all, 2, Some(3)).iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![2, 1], "before cursor excludes id >= before");
    }

    #[test]
    fn finish_channel_listing_filters_sorts_and_limits() {
        let rows = vec![
            ChannelSummary {
                channel: "lobby-na".to_string(),
                channel_type: "room",
                messages: 1,
                last_activity_unix_ms: 2,
            },
            ChannelSummary {
                channel: "lobby-eu".to_string(),
                channel_type: "room",
                messages: 1,
                last_activity_unix_ms: 1,
            },
            ChannelSummary {
                channel: "raid-1".to_string(),
                channel_type: "group",
                messages: 1,
                last_activity_unix_ms: 3,
            },
        ];
        let filtered = finish_channel_listing(rows.clone(), Some("lobby"), 0);
        assert_eq!(filtered.len(), 2);
        // Most-recently-active first across the whole set (ties by channel id).
        let names: Vec<String> = finish_channel_listing(rows.clone(), None, 0)
            .into_iter()
            .map(|r| r.channel)
            .collect();
        assert_eq!(names, vec!["raid-1", "lobby-na", "lobby-eu"]);
        let limited = finish_channel_listing(rows, None, 1);
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].channel, "raid-1");
    }

    // --- InMemoryChatRepository (reference impl) ----------------------------

    #[tokio::test]
    async fn append_auto_creates_channel_and_assigns_sequential_ids() {
        let repo = InMemoryChatRepository::new();
        let first = repo
            .post_message("lobby", ChannelType::Room, "alice", "hi", 1000, ts(1))
            .await
            .expect("first");
        let second = repo
            .post_message("lobby", ChannelType::Room, "bob", "hey", 1000, ts(2))
            .await
            .expect("second");
        assert_eq!((first, second), (1, 2));

        let channels = repo.list_channels(None, 0).await.expect("list");
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel, "lobby");
        assert_eq!(channels[0].channel_type, "room");
        assert_eq!(channels[0].messages, 2);
        assert_eq!(channels[0].last_activity_unix_ms, 2);
        assert_eq!(repo.channel_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn existing_channel_type_is_not_overridden_by_a_later_append() {
        let repo = InMemoryChatRepository::new();
        repo.post_message("g-1", ChannelType::Group, "a", "hi", 1000, ts(1))
            .await
            .expect("first");
        repo.post_message("g-1", ChannelType::Room, "b", "hey", 1000, ts(2))
            .await
            .expect("second");
        let channels = repo.list_channels(None, 0).await.expect("list");
        assert_eq!(channels[0].channel_type, "group");
    }

    #[tokio::test]
    async fn bounded_history_evicts_oldest_but_ids_keep_incrementing() {
        let repo = InMemoryChatRepository::new();
        for seq in 1..=5u64 {
            repo.post_message(
                "lobby",
                ChannelType::Room,
                "a",
                &format!("m{seq}"),
                3,
                ts(seq),
            )
            .await
            .expect("append");
        }
        let ids: Vec<u64> = repo
            .channel_history("lobby", 0, None)
            .await
            .expect("history")
            .iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(ids, vec![5, 4, 3], "only the last 3 retained, newest first");
        let next = repo
            .post_message("lobby", ChannelType::Room, "a", "m6", 3, ts(6))
            .await
            .expect("append");
        assert_eq!(next, 6, "eviction never rewinds the sequence");
        // Total-ever-appended counter reflects all 6 appends.
        assert_eq!(
            repo.list_channels(None, 0).await.expect("list")[0].messages,
            6
        );
    }

    #[tokio::test]
    async fn history_for_unknown_channel_is_empty_not_error() {
        let repo = InMemoryChatRepository::new();
        assert!(
            repo.channel_history("nope", 10, None)
                .await
                .expect("history")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_tombstones_blanks_content_and_is_idempotent() {
        let repo = InMemoryChatRepository::new();
        let id = repo
            .post_message("lobby", ChannelType::Room, "a", "secret", 1000, ts(1))
            .await
            .expect("append");
        assert!(
            repo.delete_message("lobby", id, ts(2))
                .await
                .expect("first delete")
        );
        let page = repo
            .channel_history("lobby", 0, None)
            .await
            .expect("history");
        assert!(page[0].deleted);
        assert_eq!(page[0].content, "");
        assert!(
            !repo
                .delete_message("lobby", id, ts(3))
                .await
                .expect("second")
        );
    }

    #[tokio::test]
    async fn delete_unknown_channel_or_id_is_not_found() {
        let repo = InMemoryChatRepository::new();
        assert_eq!(
            repo.delete_message("nope", 1, ts(1))
                .await
                .expect_err("unknown channel")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
        let id = repo
            .post_message("lobby", ChannelType::Room, "a", "hi", 1000, ts(1))
            .await
            .expect("append");
        assert_eq!(
            repo.delete_message("lobby", id + 1, ts(2))
                .await
                .expect_err("unknown id")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
    }
}
