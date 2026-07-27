//! Friend relationships (, persisted in ).
//!
//! `FriendsService` is a thin validate-then-delegate layer over a
//! [`FriendsRepository`](crate::repository::FriendsRepository): it keeps the
//! `user == other` self-friendship rejection and forwards every operation to the
//! selected persistence backend, so friend relations now survive a node restart
//! on the Postgres and SQLite backends (the in-memory backend stays non-durable
//! by design).
//!
//! The pairwise, directed-edge model and the invite→mutual / blocked state
//! machine live in the repository layer (`src/repository/friends.rs`); the value
//! types [`FriendRow`] and [`FriendState`] are re-exported here so existing
//! console/HTTP consumers keep their `crate::services::…` paths.

use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::FriendsRepository;
use crate::services::ChatAccessCoordinator;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::FriendRow` / `FriendState` keep resolving for console/HTTP.
pub use crate::repository::friends::{FriendRow, FriendState};

/// Friend-relationship service backed by a persistence repository.
///
/// Holds an `Arc<dyn FriendsRepository>` from the selected backend. All methods
/// are `async` and delegate after the self-friendship check.
#[derive(Clone)]
pub struct FriendsService {
    repo: Arc<dyn FriendsRepository>,
    chat_access: Arc<ChatAccessCoordinator>,
}

impl FriendsService {
    /// Create a service over a friends repository (from the selected backend).
    #[must_use]
    pub fn new(repo: Arc<dyn FriendsRepository>) -> Self {
        Self {
            repo,
            chat_access: Arc::new(ChatAccessCoordinator::new()),
        }
    }

    /// Use a shared authority coordinator so friendship changes fence concurrent
    /// secure-chat operations.
    #[must_use]
    pub fn with_chat_access_coordinator(mut self, chat_access: Arc<ChatAccessCoordinator>) -> Self {
        self.chat_access = chat_access;
        self
    }

    /// Invite `other`, or accept their pending invite (mutual invite =>
    /// friends). Re-inviting an existing friend is a no-op success.
    ///
    /// # Errors
    /// - `Validation` when `user == other`.
    /// - `Conflict` when either side blocked the other.
    /// - A backend error on failure.
    pub async fn add(
        &self,
        user: &str,
        other: &str,
        now: TimestampMillis,
    ) -> AppResult<FriendState> {
        Self::distinct(user, other)?;
        let _fence = self.chat_access.fence().await;
        let result = self.repo.add(user, other, now).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::direct_key(user, other))
                .await?;
        }
        result
    }

    /// Remove any relationship between the two users (both directions).
    ///
    /// Returns whether anything was removed. Removing a block is how the blocker
    /// unblocks.
    ///
    /// # Errors
    /// - `Validation` when `user == other`.
    /// - A backend error on failure.
    pub async fn remove(&self, user: &str, other: &str) -> AppResult<bool> {
        Self::distinct(user, other)?;
        let _fence = self.chat_access.fence().await;
        let result = self.repo.remove(user, other).await;
        if result.as_ref().is_ok_and(|removed| *removed) {
            self.chat_access
                .advance(&ChatAccessCoordinator::direct_key(user, other))
                .await?;
        }
        result
    }

    /// Block `other`: the blocker keeps a one-sided `blocked` state, the other
    /// side's view of the relation is dropped.
    ///
    /// # Errors
    /// - `Validation` when `user == other`.
    /// - A backend error on failure.
    pub async fn block(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<()> {
        Self::distinct(user, other)?;
        let _fence = self.chat_access.fence().await;
        self.repo.block(user, other, now).await?;
        self.chat_access
            .advance(&ChatAccessCoordinator::direct_key(user, other))
            .await?;
        Ok(())
    }

    /// This user's relations, other-id-ordered.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn list(&self, user: &str) -> AppResult<Vec<FriendRow>> {
        self.repo.list(user).await
    }

    fn distinct(user: &str, other: &str) -> AppResult<()> {
        if user == other {
            return Err(AppError::validation("cannot befriend yourself"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryFriendsRepository;

    fn service() -> FriendsService {
        FriendsService::new(Arc::new(InMemoryFriendsRepository::new()))
    }

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    #[tokio::test]
    async fn self_friendship_is_rejected_before_touching_the_repo() {
        let friends = service();
        assert!(friends.add("a", "a", ts(1)).await.is_err());
        assert!(friends.block("a", "a", ts(1)).await.is_err());
        assert!(friends.remove("a", "a").await.is_err());
    }

    #[tokio::test]
    async fn delegates_invite_and_list_to_the_repository() {
        let friends = service();
        assert_eq!(
            friends.add("a", "b", ts(1)).await.expect("invite"),
            FriendState::InvitedSent
        );
        let rows = friends.list("a").await.expect("list");
        assert_eq!(rows[0].user_id, "b");
        assert_eq!(rows[0].state, FriendState::InvitedSent);
    }
}
