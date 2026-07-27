//! Console notification store (, persisted in ).
//!
//! `NotificationService` is a thin validate-then-delegate layer over a
//! [`NotificationsRepository`](crate::repository::NotificationsRepository): it
//! holds the retention `capacity`, validates input, and forwards every operation
//! to the selected persistence backend, so the console notification store now
//! survives a node restart on the Postgres and SQLite backends (the in-memory
//! backend stays non-durable by design).
//!
//! The model is a
//! single, global, newest-first, bounded ring of targeted-or-broadcast messages:
//! a targeted notification (`Recipient::User`) is visible only to that account,
//! and a broadcast (`Recipient::Broadcast`) is visible to everyone. The oldest
//! entries are evicted beyond [`DEFAULT_NOTIFICATION_CAPACITY`]. Durable
//! persistence is now in place; realtime push delivery (deliver-if-online over the
//! session/routing seam) remains out of scope — see
//! `website/src/content/docs/reference/admin-api/notifications.mdx`.
//!
//! The value types ([`Notification`], [`NotificationPage`], [`Recipient`]) and the
//! visibility/paging/eviction rules live in the repository layer
//! (`src/repository/notifications.rs`) as pure, unit-tested helpers shared by all
//! three backends. The types are re-exported here so existing console/HTTP
//! consumers keep their `crate::services::…` paths.

use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::NotificationsRepository;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::Notification` / `NotificationPage` / `Recipient` /
// `DEFAULT_NOTIFICATION_CAPACITY` keep resolving for console/HTTP consumers.
pub use crate::repository::notifications::{
    DEFAULT_NOTIFICATION_CAPACITY, Notification, NotificationPage, Recipient,
};

/// Console notification store backed by a persistence repository.
///
/// Holds an `Arc<dyn NotificationsRepository>` from the selected backend plus the
/// retention `capacity`. All operations are `async` and delegate to the
/// repository; `send` validates its input first.
#[derive(Clone)]
pub struct NotificationService {
    repo: Arc<dyn NotificationsRepository>,
    capacity: usize,
}

impl NotificationService {
    /// Create a service over a notifications repository (from the selected
    /// backend) using the default retention bound
    /// ([`DEFAULT_NOTIFICATION_CAPACITY`]).
    #[must_use]
    pub fn new(repo: Arc<dyn NotificationsRepository>) -> Self {
        Self::with_capacity(repo, DEFAULT_NOTIFICATION_CAPACITY)
    }

    /// Create a service retaining at most `capacity` notifications (minimum 1).
    #[must_use]
    pub fn with_capacity(repo: Arc<dyn NotificationsRepository>, capacity: usize) -> Self {
        Self {
            repo,
            capacity: capacity.max(1),
        }
    }

    /// The retention bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Send (store) a notification, evicting the oldest entry beyond
    /// [`Self::capacity`] when full. Returns the assigned id.
    ///
    /// # Errors
    /// - [`Validation`](crate::error::ErrorCategory::Validation) if `subject` is
    ///   blank or `content` is not a JSON object.
    /// - A backend error on failure.
    pub async fn send(
        &self,
        recipient: Recipient,
        subject: String,
        content: serde_json::Value,
        code: i32,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        if subject.trim().is_empty() {
            return Err(AppError::validation("subject must not be empty"));
        }
        if !content.is_object() {
            return Err(AppError::validation("content must be a JSON object"));
        }
        self.repo
            .enqueue(recipient, &subject, &content, code, self.capacity, now)
            .await
    }

    /// Read a newest-first page.
    ///
    /// `user_id_filter`, when present, restricts to that user's own targeted
    /// notifications plus every broadcast; `None` returns everything (the
    /// operator-wide view). `before_id`, when present, only returns notifications
    /// strictly older than that id (a resume cursor). `limit` bounds the page size
    /// — callers are responsible for clamping it to a sane range.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn list(
        &self,
        user_id_filter: Option<&str>,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<NotificationPage> {
        self.repo.list(user_id_filter, limit, before_id).await
    }

    /// Number of notifications currently retained (before any filter).
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn count(&self) -> AppResult<usize> {
        self.repo.count().await
    }

    /// Delete a notification by id.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if `id` is
    /// unknown, or a backend error on failure.
    pub async fn delete(&self, id: u64) -> AppResult<()> {
        self.repo.delete(id).await
    }

    /// Mark a notification read (idempotent). `now` records when it was read.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if `id` is
    /// unknown, or a backend error on failure.
    pub async fn mark_read(&self, id: u64, now: TimestampMillis) -> AppResult<()> {
        self.repo.mark_read(id, now).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryNotificationsRepository;

    fn service() -> NotificationService {
        NotificationService::new(Arc::new(InMemoryNotificationsRepository::new()))
    }

    fn now(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    fn obj() -> serde_json::Value {
        serde_json::json!({ "k": "v" })
    }

    #[tokio::test]
    async fn targeted_notifications_are_visible_only_to_their_recipient() {
        let svc = service();
        svc.send(
            Recipient::User("u-1".to_string()),
            "hi u1".to_string(),
            obj(),
            0,
            now(1),
        )
        .await
        .expect("send");
        svc.send(
            Recipient::User("u-2".to_string()),
            "hi u2".to_string(),
            obj(),
            0,
            now(2),
        )
        .await
        .expect("send");

        let for_u1 = svc.list(Some("u-1"), 10, None).await.expect("list");
        assert_eq!(for_u1.items.len(), 1);
        assert_eq!(for_u1.items[0].subject, "hi u1");
        assert_eq!(for_u1.total, 1);
    }

    #[tokio::test]
    async fn broadcasts_are_visible_to_every_user_and_the_unfiltered_view() {
        let svc = service();
        svc.send(
            Recipient::User("u-1".to_string()),
            "targeted".to_string(),
            obj(),
            0,
            now(1),
        )
        .await
        .expect("send");
        svc.send(
            Recipient::Broadcast,
            "server news".to_string(),
            obj(),
            0,
            now(2),
        )
        .await
        .expect("send");

        assert_eq!(
            svc.list(Some("u-1"), 10, None)
                .await
                .expect("list")
                .items
                .len(),
            2
        );
        let for_other = svc.list(Some("u-2"), 10, None).await.expect("list");
        assert_eq!(for_other.items.len(), 1);
        assert_eq!(for_other.items[0].subject, "server news");
        assert_eq!(svc.list(None, 10, None).await.expect("list").items.len(), 2);
    }

    #[tokio::test]
    async fn list_is_newest_first_with_before_cursor() {
        let svc = service();
        let mut ids = Vec::new();
        for i in 1..=5u64 {
            ids.push(
                svc.send(Recipient::Broadcast, format!("n{i}"), obj(), 0, now(i))
                    .await
                    .expect("send"),
            );
        }
        let first = svc.list(None, 2, None).await.expect("list");
        assert_eq!(first.items[0].id, ids[4]);
        assert_eq!(first.items[1].id, ids[3]);
        let next = svc
            .list(None, 2, Some(first.items[1].id))
            .await
            .expect("list");
        assert_eq!(
            next.items
                .iter()
                .map(|n| n.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["n3", "n2"]
        );
        assert_eq!(next.total, 5, "total ignores the cursor");
    }

    #[tokio::test]
    async fn ring_never_exceeds_capacity_and_drops_oldest() {
        let svc =
            NotificationService::with_capacity(Arc::new(InMemoryNotificationsRepository::new()), 3);
        for i in 1..=5u64 {
            svc.send(Recipient::Broadcast, format!("n{i}"), obj(), 0, now(i))
                .await
                .expect("send");
        }
        assert_eq!(svc.count().await.expect("count"), 3);
        let page = svc.list(None, 10, None).await.expect("list");
        assert_eq!(
            page.items
                .iter()
                .map(|n| n.subject.as_str())
                .collect::<Vec<_>>(),
            vec!["n5", "n4", "n3"]
        );
    }

    #[tokio::test]
    async fn non_object_content_and_blank_subject_are_rejected() {
        let svc = service();
        for bad in [
            serde_json::json!("a string"),
            serde_json::json!(42),
            serde_json::json!([1, 2, 3]),
            serde_json::json!(null),
        ] {
            let err = svc
                .send(Recipient::Broadcast, "subj".to_string(), bad, 0, now(1))
                .await
                .expect_err("non-object content rejected");
            assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
        }
        let err = svc
            .send(Recipient::Broadcast, "   ".to_string(), obj(), 0, now(1))
            .await
            .expect_err("blank subject rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[tokio::test]
    async fn delete_and_mark_read_reject_unknown_ids() {
        let svc = service();
        let id = svc
            .send(
                Recipient::Broadcast,
                "read me".to_string(),
                obj(),
                0,
                now(1),
            )
            .await
            .expect("send");
        assert!(!svc.list(None, 10, None).await.expect("list").items[0].read);
        svc.mark_read(id, now(2)).await.expect("mark read");
        assert!(svc.list(None, 10, None).await.expect("list").items[0].read);

        svc.delete(id).await.expect("delete");
        assert_eq!(svc.count().await.expect("count"), 0);
        assert_eq!(
            svc.delete(id).await.expect_err("already gone").category(),
            crate::error::ErrorCategory::NotFound
        );
        assert_eq!(
            svc.mark_read(9_999, now(3))
                .await
                .expect_err("unknown")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn zero_capacity_is_clamped_to_one() {
        let svc =
            NotificationService::with_capacity(Arc::new(InMemoryNotificationsRepository::new()), 0);
        assert_eq!(svc.capacity(), 1);
        svc.send(Recipient::Broadcast, "a".to_string(), obj(), 0, now(1))
            .await
            .expect("send");
        svc.send(Recipient::Broadcast, "b".to_string(), obj(), 0, now(2))
            .await
            .expect("send");
        assert_eq!(svc.count().await.expect("count"), 1);
    }
}
