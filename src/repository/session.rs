//! Session repository contract.
//!
//! The early `SessionRepository` boundary from the feature-parity plan. It is
//! async (via [`async_trait`]) and object-safe, matching the other repository
//! contracts; a future Postgres/sqlx backend implements the same contract behind
//! the same domain types ( / ). The in-memory reference impl
//! keeps synchronous bodies (ready futures, no `.await`).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::identity::UserId;
use crate::session::{RevocationReason, Session, SessionId, SessionTokenRef};
use crate::time::TimestampMillis;

/// Persistence boundary for sessions.
#[async_trait]
pub trait SessionRepository: Send + Sync {
    /// Fetch a session by id.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get_session(&self, id: &SessionId) -> AppResult<Option<Session>>;

    /// Fetch a session by its non-secret token reference.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get_session_by_token_ref(
        &self,
        token_ref: &SessionTokenRef,
    ) -> AppResult<Option<Session>>;

    /// Persist a new session.
    ///
    /// # Errors
    /// Returns a conflict error if the session id already exists.
    async fn create_session(&self, session: Session) -> AppResult<Session>;

    /// Update a session (state transitions, refreshed expiry).
    ///
    /// Contract requirement: an implementation must not let a stale write
    /// resurrect a terminal session. A concurrent refresh must never overwrite a
    /// session that has since been revoked or expired; implementations enforce
    /// this with a compare-and-set on state/version (the exact precondition shape
    /// is finalized by the persistence task).
    ///
    /// # Errors
    /// Returns a not-found error if the session does not exist, and a conflict
    /// error if the write would clobber a newer terminal state.
    async fn update_session(&self, session: Session) -> AppResult<Session>;

    /// Revoke every active session for a user, returning the count revoked.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn revoke_user_sessions(
        &self,
        user_id: &UserId,
        revoked_at: TimestampMillis,
        reason: RevocationReason,
    ) -> AppResult<usize>;
}

/// A contract-faithful, in-memory [`SessionRepository`].
///
/// Enforces the hardening deferred by :
///
/// - `update_session` is a compare-and-set: it refuses to change immutable
///   session facts (`id`, `user_id`, `issued_at`, `owner_node`) and refuses to
///   overwrite a terminal (`Expired`/`Revoked`) session with a differing value,
///   so a stale refresh can never resurrect a session that was revoked/expired
///   since it was read.
/// - The `token_ref` index is kept consistent with session state under one lock.
/// - `revoke_user_sessions` is atomic and idempotent, revoking every
///   non-terminal session of the user (including lapsed-but-not-materialized
///   ones) and returning exactly the count it newly transitioned.
#[derive(Debug, Default)]
pub struct InMemorySessionRepository {
    inner: Mutex<SessionStore>,
}

#[derive(Debug, Default)]
struct SessionStore {
    by_id: HashMap<SessionId, Session>,
    by_ref: HashMap<SessionTokenRef, SessionId>,
}

impl InMemorySessionRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, SessionStore>> {
        self.inner
            .lock()
            .map_err(|_| AppError::internal("session repository mutex poisoned"))
    }

    /// Compensating delete used only by the in-memory unit of work to roll back a
    /// session it created within an aborted transaction.
    ///
    /// Best-effort and synchronous (callable from `Drop`); no-ops on a poisoned
    /// lock. Also clears the `token_ref` index entry when it still points at this
    /// session. Not part of the public `SessionRepository` contract.
    pub(crate) fn remove_session_for_rollback(&self, id: &SessionId) {
        if let Ok(mut store) = self.inner.lock()
            && let Some(session) = store.by_id.remove(id)
            && let Some(token_ref) = &session.token_ref
            && store.by_ref.get(token_ref) == Some(id)
        {
            store.by_ref.remove(token_ref);
        }
    }
}

#[async_trait]
impl SessionRepository for InMemorySessionRepository {
    async fn get_session(&self, id: &SessionId) -> AppResult<Option<Session>> {
        Ok(self.guard()?.by_id.get(id).cloned())
    }

    async fn get_session_by_token_ref(
        &self,
        token_ref: &SessionTokenRef,
    ) -> AppResult<Option<Session>> {
        let store = self.guard()?;
        Ok(store
            .by_ref
            .get(token_ref)
            .and_then(|id| store.by_id.get(id))
            .cloned())
    }

    async fn create_session(&self, session: Session) -> AppResult<Session> {
        let mut store = self.guard()?;
        if store.by_id.contains_key(&session.id) {
            return Err(AppError::conflict("session id already exists"));
        }
        if let Some(token_ref) = &session.token_ref {
            store.by_ref.insert(token_ref.clone(), session.id.clone());
        }
        store.by_id.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    async fn update_session(&self, session: Session) -> AppResult<Session> {
        let mut store = self.guard()?;
        let existing = store
            .by_id
            .get(&session.id)
            .ok_or_else(|| AppError::not_found("session does not exist"))?;

        // Immutable session facts must never change on update.
        if existing.user_id != session.user_id
            || existing.issued_at != session.issued_at
            || existing.owner_node != session.owner_node
        {
            return Err(AppError::conflict("immutable session fields cannot change"));
        }

        // Compare-and-set: a terminal stored session accepts only an identical
        // write (idempotent). Any differing write — a stale refresh, or a switch
        // from one terminal state to another — is a conflict.
        if existing.state().is_terminal() && *existing != session {
            return Err(AppError::conflict(
                "cannot update a terminal session (compare-and-set failed)",
            ));
        }

        let old_ref = existing.token_ref.clone();
        // Keep the token_ref index consistent with the new state.
        if old_ref != session.token_ref {
            if let Some(old) = &old_ref {
                // Only remove the mapping if it still points at this session.
                if store.by_ref.get(old) == Some(&session.id) {
                    store.by_ref.remove(old);
                }
            }
            if let Some(new_ref) = &session.token_ref {
                store.by_ref.insert(new_ref.clone(), session.id.clone());
            }
        }

        store.by_id.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    async fn revoke_user_sessions(
        &self,
        user_id: &UserId,
        revoked_at: TimestampMillis,
        reason: RevocationReason,
    ) -> AppResult<usize> {
        let mut store = self.guard()?;
        let mut revoked_refs: Vec<SessionTokenRef> = Vec::new();
        let mut count = 0;
        for session in store.by_id.values_mut() {
            if &session.user_id == user_id && !session.state().is_terminal() {
                session.revoke_at(revoked_at, reason)?;
                if let Some(token_ref) = &session.token_ref {
                    revoked_refs.push(token_ref.clone());
                }
                count += 1;
            }
        }
        for token_ref in revoked_refs {
            store.by_ref.remove(&token_ref);
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
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

    #[tokio::test]
    async fn create_indexes_token_ref_and_rejects_duplicates() {
        let repo = InMemorySessionRepository::new();
        repo.create_session(session("s-1", "u-1", "t-1"))
            .await
            .expect("create");
        assert!(
            repo.get_session_by_token_ref(&SessionTokenRef::new("t-1").expect("test value"))
                .await
                .expect("get")
                .is_some()
        );
        assert_eq!(
            repo.create_session(session("s-1", "u-1", "t-2"))
                .await
                .expect_err("dup")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn update_requires_existing_and_immutable_facts() {
        let repo = InMemorySessionRepository::new();
        assert_eq!(
            repo.update_session(session("missing", "u-1", "t-1"))
                .await
                .expect_err("missing")
                .category(),
            ErrorCategory::NotFound
        );

        repo.create_session(session("s-1", "u-1", "t-1"))
            .await
            .expect("create");
        // Changing user_id is rejected.
        let mut tampered = session("s-1", "u-2", "t-1");
        tampered.expires_at = ts(200);
        assert_eq!(
            repo.update_session(tampered)
                .await
                .expect_err("immutable")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn cas_blocks_stale_refresh_of_revoked_session() {
        let repo = InMemorySessionRepository::new();
        repo.create_session(session("s-1", "u-1", "t-1"))
            .await
            .expect("create");

        // Read a copy (the "stale" refresh working set).
        let mut stale = repo
            .get_session(&SessionId::new("s-1").expect("test value"))
            .await
            .expect("get")
            .expect("present");

        // Meanwhile the stored session is revoked.
        let mut current = repo
            .get_session(&SessionId::new("s-1").expect("test value"))
            .await
            .expect("get")
            .expect("present");
        current
            .revoke_at(ts(150), RevocationReason::Logout)
            .expect("revoke");
        repo.update_session(current).await.expect("store revoke");

        // The stale refresh (still Active, new expiry) must not resurrect it.
        stale
            .refresh_at(ts(150), ts(500), Some(ts(800)), None)
            .expect("refresh stale copy");
        assert_eq!(
            repo.update_session(stale)
                .await
                .expect_err("stale refresh conflicts")
                .category(),
            ErrorCategory::Conflict
        );

        // Stored state remains revoked.
        let stored = repo
            .get_session(&SessionId::new("s-1").expect("test value"))
            .await
            .expect("get")
            .expect("present");
        assert!(stored.state().is_terminal());
    }

    #[tokio::test]
    async fn terminal_to_different_terminal_is_rejected() {
        let repo = InMemorySessionRepository::new();
        repo.create_session(session("s-1", "u-1", "t-1"))
            .await
            .expect("create");
        let mut revoked = repo
            .get_session(&SessionId::new("s-1").expect("test value"))
            .await
            .expect("test value")
            .expect("test value");
        revoked
            .revoke_at(ts(150), RevocationReason::Logout)
            .expect("test value");
        repo.update_session(revoked).await.expect("store revoke");

        // A stale copy tries to expire the (now revoked) session.
        let mut stale = session("s-1", "u-1", "t-1");
        stale.expire_at(ts(250)).expect("test value");
        assert_eq!(
            repo.update_session(stale)
                .await
                .expect_err("terminal swap")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn revoke_user_sessions_is_atomic_idempotent_and_scoped() {
        let repo = InMemorySessionRepository::new();
        repo.create_session(session("s-1", "u-1", "t-1"))
            .await
            .expect("create");
        repo.create_session(session("s-2", "u-1", "t-2"))
            .await
            .expect("create");
        repo.create_session(session("s-3", "u-2", "t-3"))
            .await
            .expect("create");

        let revoked = repo
            .revoke_user_sessions(
                &UserId::new("u-1").expect("test value"),
                ts(150),
                RevocationReason::Admin,
            )
            .await
            .expect("revoke");
        assert_eq!(revoked, 2, "both u-1 sessions revoked");

        // Idempotent: a second call revokes nothing.
        assert_eq!(
            repo.revoke_user_sessions(
                &UserId::new("u-1").expect("test value"),
                ts(160),
                RevocationReason::Admin
            )
            .await
            .expect("revoke again"),
            0
        );

        // Other user's session is untouched.
        let other = repo
            .get_session(&SessionId::new("s-3").expect("test value"))
            .await
            .expect("test value")
            .expect("test value");
        assert!(!other.state().is_terminal());

        // Revoked sessions' token refs are cleared.
        assert!(
            repo.get_session_by_token_ref(&SessionTokenRef::new("t-1").expect("test value"))
                .await
                .expect("get")
                .is_none()
        );
    }
}
