//! Notifications repository contract.
//!
//! Persists the console notification store behind the same repository seam as
//! identity/session/storage/friends/groups/leaderboards/chat, so the targeted-
//! or-broadcast notifications an operator sends survive a node restart on the
//! durable backends. The model is unchanged from the original in-process store
//!: a single global, newest-first, bounded ring of notifications, each
//! addressed to one account (`recipient_id`) or to everyone (a broadcast, stored
//! as a `NULL` recipient). A reader sees their own targeted notifications plus
//! every broadcast; the unfiltered view (no `user_id`) sees everything.
//!
//! Following the friends/groups/leaderboards/chat template, the non-trivial logic
//! — the capacity/eviction bound and the visibility-filtered, newest-first paging
//! — lives in exactly one place: the pure [`overflow_evictions`] /
//! [`page_notifications`] helpers, unit-tested directly here. Every backend
//! ([`InMemoryNotificationsRepository`], the Postgres `PgNotificationsRepository`,
//! the SQLite `SqliteNotificationsRepository`) only does (lock/transaction) read →
//! apply the pure decision → write, so the three implementations cannot drift.
//!
//! Ids are a single global sequence computed as `MAX(id) + 1` inside the enqueue
//! transaction (never a database serial), so the CockroachDB flavor has no
//! identity-column quirks. Because the capacity bound only ever evicts the
//! *oldest* rows, the newest row is always retained and the sequence never
//! rewinds under eviction. (An operator hard-deleting the single newest
//! notification and then sending another can reuse that id — an accepted quirk of
//! the operator-only console tool, since notifications carry no cross-page
//! `before` cursor obligation beyond the retained window.)

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Default bound on retained notifications; the oldest are evicted beyond it.
pub const DEFAULT_NOTIFICATION_CAPACITY: usize = 10_000;

/// Who a notification is delivered to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Recipient {
    /// Targeted at one account by user id.
    User(String),
    /// Delivered to every account (fan-out), stored as a `NULL` recipient.
    Broadcast,
}

impl Recipient {
    /// The targeted user id, or `None` for a broadcast.
    #[must_use]
    pub fn user_id(&self) -> Option<&str> {
        match self {
            Self::User(id) => Some(id.as_str()),
            Self::Broadcast => None,
        }
    }

    /// Build a recipient from a stored nullable `recipient_id` column.
    #[must_use]
    pub fn from_column(user_id: Option<String>) -> Self {
        match user_id {
            Some(id) => Self::User(id),
            None => Self::Broadcast,
        }
    }

    /// Whether this recipient is visible to `user_id`: their own targeted
    /// notifications plus every broadcast.
    #[must_use]
    fn visible_to(&self, user_id: &str) -> bool {
        match self {
            Self::User(id) => id == user_id,
            Self::Broadcast => true,
        }
    }
}

/// One stored notification.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Notification {
    /// Monotonically increasing id (a single global sequence). Stable sort/cursor
    /// key while the entry is retained.
    pub id: u64,
    /// Who receives it.
    pub recipient: Recipient,
    /// Short human-readable subject line.
    pub subject: String,
    /// Arbitrary JSON payload. Always a JSON object (validated by the service).
    pub content: serde_json::Value,
    /// Application-defined status/kind code.
    pub code: i32,
    /// When it was sent.
    pub created_at: TimestampMillis,
    /// Whether the recipient has marked it read (durably: `read_at_unix_ms IS NOT
    /// NULL`).
    pub read: bool,
}

/// A newest-first page of notifications plus the total visible to the requested
/// filter (independent of paging).
#[derive(Debug, Clone)]
pub struct NotificationPage {
    /// Page items, newest first.
    pub items: Vec<Notification>,
    /// Total notifications visible to `user_id_filter`, ignoring `before_id` and
    /// `limit`.
    pub total: usize,
}

// --- Pure decision helpers (the unit-tested logic) ---------------------------

/// How many of the oldest notifications to evict so that at most `capacity`
/// remain, given `retained` rows are present after an insert. The single place the
/// durable backends compute eviction, mirroring the in-memory ring.
#[must_use]
pub fn overflow_evictions(retained: usize, capacity: usize) -> usize {
    retained.saturating_sub(capacity.max(1))
}

/// Assemble a newest-first, visibility-filtered page from the full retained set.
///
/// `chronological` is every retained notification in ascending-id (oldest first)
/// order. `user_id_filter`, when present, restricts to that user's own targeted
/// notifications plus every broadcast; `None` returns everything (the operator-
/// wide view). `before_id`, when present, only returns notifications strictly
/// older than that id (a resume cursor). `limit` bounds the page size (a literal
/// count — `0` yields an empty page). `total` counts every visible notification,
/// ignoring `before_id`/`limit`. The single place the read/paging semantics live,
/// so every backend returns identical pages.
#[must_use]
pub fn page_notifications(
    chronological: Vec<Notification>,
    user_id_filter: Option<&str>,
    limit: usize,
    before_id: Option<u64>,
) -> NotificationPage {
    let visible =
        |n: &Notification| user_id_filter.is_none_or(|user_id| n.recipient.visible_to(user_id));
    let total = chronological.iter().filter(|n| visible(n)).count();
    let items = chronological
        .into_iter()
        .rev()
        .filter(|n| visible(n))
        .filter(|n| before_id.is_none_or(|before| n.id < before))
        .take(limit)
        .collect();
    NotificationPage { items, total }
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for the console notification store.
///
/// Mirrors the public surface of the notifications service: enqueue a targeted or
/// broadcast notification (evicting the oldest beyond `capacity`), read a
/// visibility-filtered newest-first page, count the retained set, delete by id,
/// and mark read by id.
#[async_trait]
pub trait NotificationsRepository: Send + Sync {
    /// Store a notification, evicting the oldest entries beyond `capacity`, and
    /// return the assigned id. `content` is assumed already validated as a JSON
    /// object by the caller.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn enqueue(
        &self,
        recipient: Recipient,
        subject: &str,
        content: &serde_json::Value,
        code: i32,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64>;

    /// Read a newest-first page. See [`page_notifications`] for the filter/paging
    /// semantics.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list(
        &self,
        user_id_filter: Option<&str>,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<NotificationPage>;

    /// Total notifications currently retained (before any filter).
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn count(&self) -> AppResult<usize>;

    /// Delete one notification by id.
    ///
    /// # Errors
    /// - `NotFound` when `id` is unknown.
    /// - A backend error on failure.
    async fn delete(&self, id: u64) -> AppResult<()>;

    /// Mark one notification read (idempotent). `now` records when it was read.
    ///
    /// # Errors
    /// - `NotFound` when `id` is unknown.
    /// - A backend error on failure.
    async fn mark_read(&self, id: u64, now: TimestampMillis) -> AppResult<()>;
}

/// The stable "no such notification" error, shared by every backend.
pub(crate) fn notification_not_found() -> AppError {
    AppError::not_found("notification not found")
}

// --- In-memory reference implementation --------------------------------------

/// Mutable state behind the lock: the newest-last ring.
///
/// The next id is derived as `max(existing ids) + 1`, matching the durable
/// `MAX(id) + 1` rule so all backends assign ids identically.
type NotificationStore = VecDeque<Notification>;

/// A contract-faithful, in-memory [`NotificationsRepository`] (the reference
/// impl).
///
/// Single-process and not durable, but it enforces the full visibility/paging/
/// eviction/read/delete contract through the shared pure helpers, so the contract
/// tests in `tests/notifications_repository_contract.rs` can be reused against the
/// durable backends.
#[derive(Debug, Default)]
pub struct InMemoryNotificationsRepository {
    entries: Mutex<NotificationStore>,
}

impl InMemoryNotificationsRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, NotificationStore>> {
        self.entries
            .lock()
            .map_err(|_| AppError::internal("notifications repository mutex poisoned"))
    }
}

#[async_trait]
impl NotificationsRepository for InMemoryNotificationsRepository {
    async fn enqueue(
        &self,
        recipient: Recipient,
        subject: &str,
        content: &serde_json::Value,
        code: i32,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        let mut entries = self.guard()?;
        let id = entries.iter().map(|n| n.id).max().unwrap_or(0) + 1;
        entries.push_back(Notification {
            id,
            recipient,
            subject: subject.to_string(),
            content: content.clone(),
            code,
            created_at: now,
            read: false,
        });
        for _ in 0..overflow_evictions(entries.len(), capacity) {
            entries.pop_front();
        }
        Ok(id)
    }

    async fn list(
        &self,
        user_id_filter: Option<&str>,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<NotificationPage> {
        let entries = self.guard()?;
        let chronological = entries.iter().cloned().collect();
        Ok(page_notifications(
            chronological,
            user_id_filter,
            limit,
            before_id,
        ))
    }

    async fn count(&self) -> AppResult<usize> {
        Ok(self.guard()?.len())
    }

    async fn delete(&self, id: u64) -> AppResult<()> {
        let mut entries = self.guard()?;
        let position = entries
            .iter()
            .position(|n| n.id == id)
            .ok_or_else(notification_not_found)?;
        entries.remove(position);
        Ok(())
    }

    async fn mark_read(&self, id: u64, _now: TimestampMillis) -> AppResult<()> {
        let mut entries = self.guard()?;
        let entry = entries
            .iter_mut()
            .find(|n| n.id == id)
            .ok_or_else(notification_not_found)?;
        entry.read = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    fn obj() -> serde_json::Value {
        serde_json::json!({ "k": "v" })
    }

    fn note(id: u64, recipient: Recipient) -> Notification {
        Notification {
            id,
            recipient,
            subject: format!("n{id}"),
            content: obj(),
            code: 0,
            created_at: ts(id),
            read: false,
        }
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn overflow_evictions_keeps_capacity_newest() {
        assert_eq!(overflow_evictions(5, 3), 2);
        assert_eq!(overflow_evictions(3, 3), 0);
        assert_eq!(overflow_evictions(1, 10_000), 0);
        // Zero capacity clamps to one.
        assert_eq!(overflow_evictions(2, 0), 1);
    }

    #[test]
    fn page_notifications_is_newest_first_with_before_and_limit() {
        let all = vec![
            note(1, Recipient::Broadcast),
            note(2, Recipient::Broadcast),
            note(3, Recipient::Broadcast),
            note(4, Recipient::Broadcast),
        ];
        let ids: Vec<u64> = page_notifications(all.clone(), None, 10, None)
            .items
            .iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(ids, vec![4, 3, 2, 1], "newest first");

        let page = page_notifications(all.clone(), None, 2, None);
        assert_eq!(
            page.items.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![4, 3]
        );
        assert_eq!(page.total, 4, "total ignores the limit");

        let page = page_notifications(all, None, 10, Some(3));
        assert_eq!(
            page.items.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(page.total, 4, "total ignores the before cursor");
    }

    #[test]
    fn page_notifications_filters_by_recipient_visibility() {
        let all = vec![
            note(1, Recipient::User("u-1".to_string())),
            note(2, Recipient::User("u-2".to_string())),
            note(3, Recipient::Broadcast),
        ];
        let for_u1 = page_notifications(all.clone(), Some("u-1"), 10, None);
        assert_eq!(
            for_u1.items.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![3, 1],
            "u-1 sees its own targeted plus the broadcast"
        );
        assert_eq!(for_u1.total, 2);

        let for_u2 = page_notifications(all.clone(), Some("u-2"), 10, None);
        assert_eq!(
            for_u2.items.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![3, 2]
        );

        let unfiltered = page_notifications(all, None, 10, None);
        assert_eq!(unfiltered.total, 3, "no filter sees everything");
    }

    #[test]
    fn recipient_from_column_and_user_id_round_trip() {
        assert_eq!(
            Recipient::from_column(Some("u-1".to_string())),
            Recipient::User("u-1".to_string())
        );
        assert_eq!(Recipient::from_column(None), Recipient::Broadcast);
        assert_eq!(Recipient::User("u-1".to_string()).user_id(), Some("u-1"));
        assert_eq!(Recipient::Broadcast.user_id(), None);
    }

    // --- InMemoryNotificationsRepository (reference impl) --------------------

    #[tokio::test]
    async fn enqueue_assigns_sequential_ids_and_lists_newest_first() {
        let repo = InMemoryNotificationsRepository::new();
        let a = repo
            .enqueue(Recipient::Broadcast, "a", &obj(), 0, 1000, ts(1))
            .await
            .expect("a");
        let b = repo
            .enqueue(Recipient::Broadcast, "b", &obj(), 0, 1000, ts(2))
            .await
            .expect("b");
        assert_eq!((a, b), (1, 2));
        let page = repo.list(None, 10, None).await.expect("list");
        assert_eq!(
            page.items
                .iter()
                .map(|n| n.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        assert_eq!(repo.count().await.expect("count"), 2);
    }

    #[tokio::test]
    async fn ring_never_exceeds_capacity_and_drops_oldest() {
        let repo = InMemoryNotificationsRepository::new();
        for i in 1..=5u64 {
            repo.enqueue(Recipient::Broadcast, &format!("n{i}"), &obj(), 0, 3, ts(i))
                .await
                .expect("send");
        }
        assert_eq!(repo.count().await.expect("count"), 3);
        let page = repo.list(None, 10, None).await.expect("list");
        assert_eq!(
            page.items
                .iter()
                .map(|n| n.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["n5", "n4", "n3"]
        );
    }

    #[tokio::test]
    async fn delete_removes_and_unknown_id_is_not_found() {
        let repo = InMemoryNotificationsRepository::new();
        let id = repo
            .enqueue(Recipient::Broadcast, "gone", &obj(), 0, 1000, ts(1))
            .await
            .expect("send");
        repo.delete(id).await.expect("delete");
        assert_eq!(repo.count().await.expect("count"), 0);
        assert_eq!(
            repo.delete(id).await.expect_err("already gone").category(),
            crate::error::ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn mark_read_sets_flag_and_unknown_id_is_not_found() {
        let repo = InMemoryNotificationsRepository::new();
        let id = repo
            .enqueue(Recipient::Broadcast, "read me", &obj(), 0, 1000, ts(1))
            .await
            .expect("send");
        assert!(!repo.list(None, 10, None).await.expect("list").items[0].read);
        repo.mark_read(id, ts(2)).await.expect("mark read");
        assert!(repo.list(None, 10, None).await.expect("list").items[0].read);
        assert_eq!(
            repo.mark_read(9_999, ts(3))
                .await
                .expect_err("unknown")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
    }
}
