//! Chat channel history and moderation service (, persisted in
//! ).
//!
//! `ChatService` is a thin validate-then-delegate layer over a
//! [`ChatRepository`](crate::repository::ChatRepository): it holds the per-channel
//! retention `capacity` and forwards every operation to the selected persistence
//! backend, so chat channels and their message history now survive a node restart
//! on the Postgres and SQLite backends (the in-memory backend stays non-durable by
//! design).
//!
//! This is a **history and moderation model only** — it does not touch the
//! realtime wire/gateway. Realtime chat delivery over the socket is separate
//! future work;
//! today the console is the producer (`POST .../messages`) so the model is
//! exercisable before wire delivery lands.
//!
//! The channel/history/eviction/tombstone model and the value types
//! ([`ChannelType`], [`ChatMessage`], [`ChannelSummary`]) live in the repository
//! layer (`src/repository/chat.rs`) — the retention bound, newest-first paging,
//! and listing order are pure, unit-tested helpers shared by all three backends.
//! The types are re-exported here so existing console/HTTP consumers keep their
//! `crate::services::…` paths. Message ids are per-channel, sequential, and
//! monotonic starting at 1 — never reused even past eviction, so a page's `before`
//! cursor stays meaningful across the channel's whole lifetime.

use std::sync::Arc;

use crate::error::AppResult;
use crate::repository::ChatRepository;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::ChannelType` / `ChatMessage` / `ChannelSummary` /
// `DEFAULT_CHANNEL_HISTORY_CAP` keep resolving for console/HTTP consumers.
pub use crate::repository::chat::{
    ChannelSummary, ChannelType, ChatChannel, ChatDeliveryRequest, ChatMessage,
    ChatModerationAudit, DEFAULT_CHANNEL_HISTORY_CAP,
};

/// Maximum accepted UTF-8 payload size for one player-authored chat message.
pub const MAX_CHAT_CONTENT_BYTES: usize = 2_048;
/// Default author edit window, counted from message creation.
pub const DEFAULT_AUTHOR_EDIT_WINDOW_MS: u64 = 300_000;
/// Default author delete window, counted from message creation.
pub const DEFAULT_AUTHOR_DELETE_WINDOW_MS: u64 = 86_400_000;

/// Validate player-authored chat content before any rate-limit or persistence
/// work. Storage preserves the submitted valid UTF-8 exactly; this function
/// only enforces the shared boundary contract.
///
/// # Errors
/// Returns a validation error for empty/whitespace-only, overlong, or control
/// character content. Line feed and carriage return remain valid so clients can
/// intentionally format a short multiline message.
pub fn validate_chat_content(content: &str) -> AppResult<()> {
    if content.is_empty() || content.trim().is_empty() {
        return Err(crate::error::AppError::validation(
            "chat content must not be empty or whitespace only",
        ));
    }
    if content.len() > MAX_CHAT_CONTENT_BYTES {
        return Err(crate::error::AppError::validation(format!(
            "chat content exceeds {MAX_CHAT_CONTENT_BYTES} bytes"
        )));
    }
    if content
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r'))
    {
        return Err(crate::error::AppError::validation(
            "chat content contains an unsupported control character",
        ));
    }
    Ok(())
}

/// Chat channel history/moderation service backed by a persistence repository.
///
/// Holds an `Arc<dyn ChatRepository>` from the selected backend plus the
/// per-channel retention `capacity`. All methods are `async` and delegate to the
/// repository.
#[derive(Clone)]
pub struct ChatService {
    repo: Arc<dyn ChatRepository>,
    capacity: usize,
}

impl ChatService {
    /// Create a service over a chat repository (from the selected backend) using
    /// the default per-channel retention bound ([`DEFAULT_CHANNEL_HISTORY_CAP`]).
    #[must_use]
    pub fn new(repo: Arc<dyn ChatRepository>) -> Self {
        Self::with_capacity(repo, DEFAULT_CHANNEL_HISTORY_CAP)
    }

    /// Create a service retaining at most `capacity` messages per channel
    /// (minimum 1).
    #[must_use]
    pub fn with_capacity(repo: Arc<dyn ChatRepository>, capacity: usize) -> Self {
        Self {
            repo,
            capacity: capacity.max(1),
        }
    }

    /// The per-channel retention bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Resolve a server-derived canonical descriptor to its durable opaque
    /// channel id. Player-facing callers must be authorized before they reach
    /// this method; it never accepts a client-selected raw channel identifier.
    pub async fn resolve_canonical_channel(
        &self,
        canonical_key: &str,
        channel_type: ChannelType,
        now: TimestampMillis,
    ) -> AppResult<ChatChannel> {
        self.repo
            .resolve_canonical_channel(canonical_key, channel_type, now)
            .await
    }

    /// Append a message, creating the channel (as `channel_type`) on first use.
    /// Returns the assigned per-channel message id.
    ///
    /// If the channel already exists, `channel_type` is ignored — the type it was
    /// created with wins, since a channel's type never changes.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn append(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: impl Into<String>,
        content: impl Into<String>,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        let sender = sender.into();
        let content = content.into();
        validate_chat_content(&content)?;
        self.repo
            .post_message(channel, channel_type, &sender, &content, self.capacity, now)
            .await
    }

    /// Append through a previously authorized canonical target. The repository
    /// compares `expected_access_epoch` in the same mutation transaction, so a
    /// revocation that committed after authorization cannot leave a stale write.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_authorized(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: impl Into<String>,
        content: impl Into<String>,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        let sender = sender.into();
        let content = content.into();
        validate_chat_content(&content)?;
        self.repo
            .post_message_authorized(
                channel,
                channel_type,
                &sender,
                &content,
                self.capacity,
                access_key,
                expected_access_epoch,
                now,
            )
            .await
    }

    /// Append and stage the exact event payload for bounded remote delivery in
    /// the same durable repository transaction.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: impl Into<String>,
        content: impl Into<String>,
        access_key: &str,
        expected_access_epoch: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let sender = sender.into();
        let content = content.into();
        validate_chat_content(&content)?;
        self.repo
            .post_message_authorized_with_delivery(
                channel,
                channel_type,
                &sender,
                &content,
                self.capacity,
                access_key,
                expected_access_epoch,
                delivery,
                now,
            )
            .await
    }

    /// List channels, most-recently-active first (ties broken by channel id),
    /// with an optional case-sensitive substring `filter` over the channel id and
    /// a `limit` on rows returned (`0` = unbounded).
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn channels(
        &self,
        filter: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<ChannelSummary>> {
        self.repo.list_channels(filter, limit).await
    }

    /// Total channel count, unaffected by any `filter`/`limit`.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn channel_count(&self) -> AppResult<usize> {
        self.repo.channel_count().await
    }

    /// Page one channel's history, newest-first by id. `before_id` resumes a page
    /// (only ids `< before_id`); `limit == 0` is unbounded. An unknown channel
    /// returns an empty page.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn messages(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<Vec<ChatMessage>> {
        self.repo.channel_history(channel, limit, before_id).await
    }

    /// Return a history page only while the captured authority epoch is still
    /// current. Player callers should use this rather than [`Self::messages`].
    pub async fn authorized_messages(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
        access_key: &str,
        expected_access_epoch: u64,
    ) -> AppResult<Vec<ChatMessage>> {
        self.repo
            .channel_history_authorized(
                channel,
                limit,
                before_id,
                access_key,
                expected_access_epoch,
            )
            .await
    }

    /// Apply an author-approved edit after validating the replacement text.
    /// Caller identity and the edit window are enforced by the secure boundary;
    /// the repository atomically advances revision and event state.
    pub async fn edit_message(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        validate_chat_content(content)?;
        self.repo.edit_message(channel, id, content, now).await
    }

    /// Edit a message only when its immutable author still owns it and the
    /// configured author window has not expired.
    #[allow(clippy::too_many_arguments)]
    pub async fn edit_as_author(
        &self,
        channel: &str,
        id: u64,
        actor: &str,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        window_ms: u64,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let message = self
            .message_for_author(channel, id, actor, window_ms, now)
            .await?;
        if message.deleted {
            return Err(crate::error::AppError::permission("CHAT_UNAVAILABLE"));
        }
        validate_chat_content(content)?;
        self.repo
            .edit_message_authorized(channel, id, content, access_key, expected_access_epoch, now)
            .await
    }

    /// Author edit with atomically staged bounded remote delivery.
    #[allow(clippy::too_many_arguments)]
    pub async fn edit_as_author_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        actor: &str,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        window_ms: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let message = self
            .message_for_author(channel, id, actor, window_ms, now)
            .await?;
        if message.deleted {
            return Err(crate::error::AppError::permission("CHAT_UNAVAILABLE"));
        }
        validate_chat_content(content)?;
        self.repo
            .edit_message_authorized_with_delivery(
                channel,
                channel_type,
                id,
                content,
                access_key,
                expected_access_epoch,
                delivery,
                now,
            )
            .await
    }

    /// Tombstone a message only while its immutable author retains access and
    /// the author delete window is open.
    #[allow(clippy::too_many_arguments)]
    pub async fn delete_as_author(
        &self,
        channel: &str,
        id: u64,
        actor: &str,
        access_key: &str,
        expected_access_epoch: u64,
        window_ms: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let message = self
            .message_for_author(channel, id, actor, window_ms, now)
            .await?;
        if message.deleted {
            return Ok(false);
        }
        self.repo
            .delete_message_authorized(channel, id, access_key, expected_access_epoch, now)
            .await
    }

    /// Author tombstone with atomically staged bounded remote delivery.
    #[allow(clippy::too_many_arguments)]
    pub async fn delete_as_author_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        actor: &str,
        access_key: &str,
        expected_access_epoch: u64,
        window_ms: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<Option<ChatMessage>> {
        let message = self
            .message_for_author(channel, id, actor, window_ms, now)
            .await?;
        if message.deleted {
            return Ok(None);
        }
        self.repo
            .delete_message_authorized_with_delivery(
                channel,
                channel_type,
                id,
                access_key,
                expected_access_epoch,
                delivery,
                now,
            )
            .await
    }

    async fn message_for_author(
        &self,
        channel: &str,
        id: u64,
        actor: &str,
        window_ms: u64,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let message = self
            .messages(channel, 0, None)
            .await?
            .into_iter()
            .find(|message| message.id == id)
            .ok_or_else(|| crate::error::AppError::permission("CHAT_UNAVAILABLE"))?;
        if message.sender != actor
            || now.unix_millis().saturating_sub(message.created_at_unix_ms) > window_ms
        {
            return Err(crate::error::AppError::permission("CHAT_UNAVAILABLE"));
        }
        Ok(message)
    }

    /// Tombstone one message: blank its content and mark it deleted. Idempotent
    /// (returns `Ok(false)` for an already-tombstoned message).
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) when the channel
    /// or the message id within it is unknown, or a backend error on failure.
    pub async fn delete_message(
        &self,
        channel: &str,
        id: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        self.repo.delete_message(channel, id, now).await
    }

    /// Tombstone with an atomic, redacted moderation audit record. Only trusted
    /// console/runtime boundaries construct this request; player deletion uses
    /// [`Self::delete_as_author`] instead.
    #[allow(clippy::too_many_arguments)]
    pub async fn moderate_delete_message(
        &self,
        channel: &str,
        id: u64,
        actor_kind: &str,
        actor_id: &str,
        reason_code: &str,
        authority_epoch: u64,
        correlation_id: &str,
        node_id: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let message = self
            .messages(channel, 0, None)
            .await?
            .into_iter()
            .find(|message| message.id == id)
            .ok_or_else(crate::repository::chat::message_not_found)?;
        if message.deleted {
            return Ok(false);
        }
        let audit = ChatModerationAudit::tombstone(
            actor_kind,
            actor_id,
            reason_code,
            channel,
            id,
            &message.sender,
            authority_epoch,
            correlation_id,
            node_id,
            now,
        );
        self.repo
            .moderate_delete_message(channel, id, &audit, now)
            .await
    }

    /// Tombstone with an atomic, redacted moderation audit while fencing the
    /// mutation against a still-current channel authority lease. Player-facing
    /// callers must separately verify their group role before invoking this
    /// trusted service boundary.
    #[allow(clippy::too_many_arguments)]
    pub async fn moderate_delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        actor_kind: &str,
        actor_id: &str,
        reason_code: &str,
        access_key: &str,
        expected_access_epoch: u64,
        correlation_id: &str,
        node_id: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let message = self
            .messages(channel, 0, None)
            .await?
            .into_iter()
            .find(|message| message.id == id)
            .ok_or_else(crate::repository::chat::message_not_found)?;
        if message.deleted {
            return Ok(false);
        }
        let audit = ChatModerationAudit::tombstone(
            actor_kind,
            actor_id,
            reason_code,
            channel,
            id,
            &message.sender,
            expected_access_epoch,
            correlation_id,
            node_id,
            now,
        );
        self.repo
            .moderate_delete_message_authorized(
                channel,
                id,
                &audit,
                access_key,
                expected_access_epoch,
                now,
            )
            .await
    }

    /// Perform one bounded, idempotent moderation-audit retention pass.
    pub async fn cleanup_moderation_audit(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        self.repo.cleanup_moderation_audit(before, limit).await
    }

    /// Consume one fully validated, hashed multi-key rate-limit plan. The
    /// repository either consumes all counters or none and fails closed.
    pub async fn consume_rate_limits(
        &self,
        limits: &[crate::repository::ChatRateLimit],
        now: TimestampMillis,
    ) -> AppResult<()> {
        self.repo.consume_rate_limits(limits, now).await
    }

    /// Run a bounded cleanup pass for expired rate-limit windows.
    pub async fn cleanup_rate_limits(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        self.repo.cleanup_rate_limits(before, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryChatRepository;

    fn service() -> ChatService {
        ChatService::new(Arc::new(InMemoryChatRepository::new()))
    }

    fn now(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    #[test]
    fn content_validation_rejects_blank_controls_and_overlong_input() {
        for value in ["", " \n ", "hello\u{0007}"] {
            assert!(validate_chat_content(value).is_err(), "{value:?}");
        }
        assert!(validate_chat_content(&"x".repeat(MAX_CHAT_CONTENT_BYTES + 1)).is_err());
        assert!(validate_chat_content("line one\nline two").is_ok());
    }

    #[tokio::test]
    async fn append_auto_creates_the_channel_and_assigns_sequential_ids() {
        let chat = service();
        let first = chat
            .append("lobby", ChannelType::Room, "alice", "hi", now(1))
            .await
            .expect("first");
        let second = chat
            .append("lobby", ChannelType::Room, "bob", "hey", now(2))
            .await
            .expect("second");
        assert_eq!((first, second), (1, 2));

        let channels = chat.channels(None, 0).await.expect("channels");
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel, "lobby");
        assert_eq!(channels[0].channel_type, "room");
        assert_eq!(channels[0].messages, 2);
        assert_eq!(chat.channel_count().await.expect("count"), 1);
    }

    #[tokio::test]
    async fn messages_page_newest_first_with_before_cursor() {
        let chat = service();
        for seq in 1..=5u64 {
            chat.append(
                "lobby",
                ChannelType::Room,
                "alice",
                format!("m{seq}"),
                now(seq),
            )
            .await
            .expect("append");
        }
        let first_page = chat.messages("lobby", 2, None).await.expect("page");
        assert_eq!(
            first_page.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![5, 4]
        );
        let next_page = chat.messages("lobby", 2, Some(4)).await.expect("page");
        assert_eq!(
            next_page.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![3, 2]
        );
    }

    #[tokio::test]
    async fn bounded_capacity_evicts_oldest_but_ids_keep_incrementing() {
        let chat = ChatService::with_capacity(Arc::new(InMemoryChatRepository::new()), 3);
        assert_eq!(chat.capacity(), 3);
        for seq in 1..=5u64 {
            chat.append(
                "lobby",
                ChannelType::Room,
                "alice",
                format!("m{seq}"),
                now(seq),
            )
            .await
            .expect("append");
        }
        let ids: Vec<u64> = chat
            .messages("lobby", 0, None)
            .await
            .expect("page")
            .iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(ids, vec![5, 4, 3]);
    }

    #[tokio::test]
    async fn delete_message_tombstones_and_is_idempotent() {
        let chat = service();
        let id = chat
            .append("lobby", ChannelType::Room, "alice", "secret", now(1))
            .await
            .expect("append");
        assert!(
            chat.delete_message("lobby", id, now(2))
                .await
                .expect("first delete")
        );
        let page = chat.messages("lobby", 0, None).await.expect("page");
        assert!(page[0].deleted);
        assert_eq!(page[0].content, "");
        assert!(
            !chat
                .delete_message("lobby", id, now(3))
                .await
                .expect("second")
        );
    }

    #[tokio::test]
    async fn delete_message_unknown_channel_or_id_is_not_found() {
        let chat = service();
        assert_eq!(
            chat.delete_message("nope", 1, now(1))
                .await
                .expect_err("unknown channel")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
        let id = chat
            .append("lobby", ChannelType::Room, "alice", "hi", now(1))
            .await
            .expect("append");
        assert_eq!(
            chat.delete_message("lobby", id + 1, now(2))
                .await
                .expect_err("unknown id")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn author_edit_advances_revision_and_rejects_other_authors_or_expired_windows() {
        let chat = service();
        let id = chat
            .append("direct", ChannelType::Direct, "alice", "before", now(10))
            .await
            .expect("append");
        let edited = chat
            .edit_as_author("direct", id, "alice", "after", "direct", 0, 100, now(20))
            .await
            .expect("author edit");
        assert_eq!(edited.content, "after");
        assert_eq!(edited.revision, 2);
        assert_eq!(edited.last_event_id, 2);
        assert!(
            chat.edit_as_author("direct", id, "bob", "no", "direct", 0, 100, now(21))
                .await
                .is_err()
        );
        assert!(
            chat.edit_as_author("direct", id, "alice", "late", "direct", 0, 1, now(20_000))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn zero_capacity_is_clamped_to_one() {
        let chat = ChatService::with_capacity(Arc::new(InMemoryChatRepository::new()), 0);
        assert_eq!(chat.capacity(), 1);
        chat.append("lobby", ChannelType::Room, "a", "one", now(1))
            .await
            .expect("append");
        chat.append("lobby", ChannelType::Room, "a", "two", now(2))
            .await
            .expect("append");
        assert_eq!(
            chat.messages("lobby", 0, None).await.expect("page").len(),
            1
        );
    }
}
