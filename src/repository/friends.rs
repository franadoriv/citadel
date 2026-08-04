//! Friend-relationship repository contract.
//!
//! Persists the pairwise, directed friend graph designed in
//! `website/src/content/docs/reference/client-sdk/friends.mdx` behind the same
//! repository seam as identity/session/storage, so friend relations survive a
//! node restart. Two directed edges represent one relationship (`(owner, other)`
//! and `(other, owner)`), each carrying one of Nakama's four
//! [`FriendState`]s.
//!
//! The invite→mutual / blocked-pair state machine lives in exactly one place —
//! the pure [`plan_add`] function — and is unit-tested directly here. Every
//! backend (`InMemoryFriendsRepository`, the Postgres `PgFriendsRepository`, the
//! SQLite `SqliteFriendsRepository`) only does (lock/transaction) read → apply
//! `plan_add` → write, so the three implementations cannot drift on the business
//! rules.
//!
//! The service layer ([`crate::services::friends`]) keeps the `user == other`
//! self-friendship rejection; the repository never sees a self pair as a domain
//! rule (it is a service-level validation), so the contract here is purely about
//! the two-edge state machine and durability.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// One side's view of a pairwise relationship.
///
/// The four states mirror Nakama exactly. Each stored as its stable lowercase
/// [`FriendState::as_str`] token in the durable backends; [`FriendState::from_token`]
/// parses it back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FriendState {
    /// This user sent an invite that is still pending.
    InvitedSent,
    /// This user received an invite that is still pending.
    InvitedReceived,
    /// Mutual friends.
    Friend,
    /// This user blocked the other.
    Blocked,
}

impl FriendState {
    /// Stable lowercase token for responses, logs, and the durable `state`
    /// column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvitedSent => "invited_sent",
            Self::InvitedReceived => "invited_received",
            Self::Friend => "friend",
            Self::Blocked => "blocked",
        }
    }

    /// Parse a stored `state` token back into a [`FriendState`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the four known
    /// states — a corrupt/foreign row rather than a client-visible condition.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "invited_sent" => Ok(Self::InvitedSent),
            "invited_received" => Ok(Self::InvitedReceived),
            "friend" => Ok(Self::Friend),
            "blocked" => Ok(Self::Blocked),
            other => Err(AppError::internal(format!(
                "unknown friend state token `{other}`"
            ))),
        }
    }
}

/// One row in a user's friend list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FriendRow {
    /// The other account.
    pub user_id: String,
    /// This side's state.
    pub state: FriendState,
    /// When the relation last changed (Unix millis).
    pub updated_unix_ms: u64,
}

/// The two edge states an [`FriendsRepository::add`] must write.
///
/// Produced by the pure [`plan_add`] state machine. `owner_state` is what the
/// acting user's edge becomes (and what `add` returns to the caller);
/// `other_state` is what the other user's edge becomes. Both edges are always
/// (re)written with the call's timestamp, matching the reference behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddPlan {
    /// The acting user's `(user, other)` edge state after the add.
    pub owner_state: FriendState,
    /// The other user's `(other, user)` edge state after the add.
    pub other_state: FriendState,
}

/// The invite→mutual / blocked-pair state machine, as a pure function.
///
/// `forward` is the acting user's current `(user, other)` edge state (if any);
/// `backward` is the other user's current `(other, user)` edge state (if any).
/// The rules (identical to the original in-process `FriendsService::add`):
///
/// - If either side has blocked the other, a new invite is a
///   [`Conflict`](crate::error::ErrorCategory::Conflict).
/// - If the other side already invited the acting user
///   ([`InvitedSent`](FriendState::InvitedSent)) or they are already
///   [`Friend`](FriendState::Friend)s, both edges become `friend` (a matching
///   invite is how an accept happens; re-inviting an existing friend is a no-op
///   success).
/// - Otherwise the acting user's edge becomes `invited_sent` and the other's
///   becomes `invited_received`.
///
/// # Errors
/// Returns a `Conflict` error when either side has blocked the other.
pub fn plan_add(forward: Option<FriendState>, backward: Option<FriendState>) -> AppResult<AddPlan> {
    if forward == Some(FriendState::Blocked) || backward == Some(FriendState::Blocked) {
        return Err(AppError::conflict("relationship is blocked"));
    }
    let plan = match backward {
        // The other side already invited (or is already a friend): mutual.
        Some(FriendState::InvitedSent | FriendState::Friend) => AddPlan {
            owner_state: FriendState::Friend,
            other_state: FriendState::Friend,
        },
        _ => AddPlan {
            owner_state: FriendState::InvitedSent,
            other_state: FriendState::InvitedReceived,
        },
    };
    Ok(plan)
}

/// Persistence boundary for the pairwise friend graph.
///
/// Directed edges, both directions stored explicitly. The service layer performs
/// the `user == other` self-check before delegating, so implementations may
/// assume `user != other`.
#[async_trait]
pub trait FriendsRepository: Send + Sync {
    /// Invite `other`, or accept their pending invite (a matching invite makes
    /// them mutual friends). Returns the acting user's resulting edge state.
    ///
    /// # Errors
    /// - `Conflict` when either side has blocked the other.
    /// - A backend error on failure.
    async fn add(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<FriendState>;

    /// Remove any relationship between the two users (both directions).
    ///
    /// Returns whether anything was removed. Removing a block is how the blocker
    /// unblocks.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn remove(&self, user: &str, other: &str) -> AppResult<bool>;

    /// Block `other`: the blocker keeps a one-sided `blocked` state and the other
    /// side's view of the relation is dropped.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn block(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<()>;

    /// This user's relations, ordered by the other user's id.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list(&self, user: &str) -> AppResult<Vec<FriendRow>>;
}

/// The directed-edge store: `(owner, other) -> (state, updated_millis)`, both
/// directions stored explicitly. A named alias keeps the `Mutex`/guard types
/// readable (mirrors `SessionStore` in `session.rs`).
pub(crate) type EdgeStore = HashMap<(String, String), (FriendState, u64)>;

/// A contract-faithful, in-memory [`FriendsRepository`] (the reference impl).
///
/// `(user, other) -> (state, updated_millis)`; both directions stored
/// explicitly. Single-process and not durable, but it enforces the full state
/// machine through the shared [`plan_add`], so the contract tests in
/// `tests/friends_repository_contract.rs` can be reused against the durable
/// backends.
#[derive(Debug, Default)]
pub struct InMemoryFriendsRepository {
    inner: Mutex<EdgeStore>,
}

impl InMemoryFriendsRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, EdgeStore>> {
        self.inner
            .lock()
            .map_err(|_| AppError::internal("friends repository mutex poisoned"))
    }

    /// Capture the complete edge store for an enclosing in-memory transaction.
    pub(crate) fn snapshot_for_rollback(&self) -> AppResult<EdgeStore> {
        Ok(self.guard()?.clone())
    }

    /// Restore a transaction snapshot without applying domain transitions.
    pub(crate) fn restore_for_rollback(&self, snapshot: EdgeStore) {
        if let Ok(mut state) = self.inner.lock() {
            *state = snapshot;
        }
    }
}

#[async_trait]
impl FriendsRepository for InMemoryFriendsRepository {
    async fn add(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<FriendState> {
        let mut map = self.guard()?;
        let millis = now.unix_millis();
        let forward = map.get(&key(user, other)).map(|(state, _)| *state);
        let backward = map.get(&key(other, user)).map(|(state, _)| *state);
        let plan = plan_add(forward, backward)?;
        map.insert(key(user, other), (plan.owner_state, millis));
        map.insert(key(other, user), (plan.other_state, millis));
        Ok(plan.owner_state)
    }

    async fn remove(&self, user: &str, other: &str) -> AppResult<bool> {
        let mut map = self.guard()?;
        let a = map.remove(&key(user, other)).is_some();
        let b = map.remove(&key(other, user)).is_some();
        Ok(a || b)
    }

    async fn block(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<()> {
        let mut map = self.guard()?;
        map.insert(key(user, other), (FriendState::Blocked, now.unix_millis()));
        map.remove(&key(other, user));
        Ok(())
    }

    async fn list(&self, user: &str) -> AppResult<Vec<FriendRow>> {
        let map = self.guard()?;
        let mut rows: Vec<FriendRow> = map
            .iter()
            .filter(|((owner, _), _)| owner == user)
            .map(|((_, other), (state, updated))| FriendRow {
                user_id: other.clone(),
                state: *state,
                updated_unix_ms: *updated,
            })
            .collect();
        rows.sort_by(|a, b| a.user_id.cmp(&b.user_id));
        Ok(rows)
    }
}

fn key(a: &str, b: &str) -> (String, String) {
    (a.to_string(), b.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    // --- plan_add (the pure state machine) ----------------------------------

    #[test]
    fn plan_add_with_no_existing_edges_is_a_fresh_invite() {
        let plan = plan_add(None, None).expect("plan");
        assert_eq!(plan.owner_state, FriendState::InvitedSent);
        assert_eq!(plan.other_state, FriendState::InvitedReceived);
    }

    #[test]
    fn plan_add_against_pending_incoming_invite_becomes_mutual() {
        // The other side already sent us an invite (their edge is invited_sent).
        let plan = plan_add(
            Some(FriendState::InvitedReceived),
            Some(FriendState::InvitedSent),
        )
        .expect("plan");
        assert_eq!(plan.owner_state, FriendState::Friend);
        assert_eq!(plan.other_state, FriendState::Friend);
    }

    #[test]
    fn plan_add_reinviting_existing_friend_stays_friend() {
        let plan = plan_add(Some(FriendState::Friend), Some(FriendState::Friend)).expect("plan");
        assert_eq!(plan.owner_state, FriendState::Friend);
        assert_eq!(plan.other_state, FriendState::Friend);
    }

    #[test]
    fn plan_add_reinviting_still_pending_is_idempotent() {
        let plan = plan_add(
            Some(FriendState::InvitedSent),
            Some(FriendState::InvitedReceived),
        )
        .expect("plan");
        assert_eq!(plan.owner_state, FriendState::InvitedSent);
        assert_eq!(plan.other_state, FriendState::InvitedReceived);
    }

    #[test]
    fn plan_add_rejects_when_either_side_blocked() {
        assert_eq!(
            plan_add(Some(FriendState::Blocked), None)
                .expect_err("owner blocked")
                .category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            plan_add(None, Some(FriendState::Blocked))
                .expect_err("other blocked")
                .category(),
            ErrorCategory::Conflict
        );
    }

    // --- FriendState token round-trip ---------------------------------------

    #[test]
    fn state_tokens_round_trip() {
        for state in [
            FriendState::InvitedSent,
            FriendState::InvitedReceived,
            FriendState::Friend,
            FriendState::Blocked,
        ] {
            assert_eq!(
                FriendState::from_token(state.as_str()).expect("parse"),
                state
            );
        }
        assert!(FriendState::from_token("bogus").is_err());
    }

    // --- InMemoryFriendsRepository (reference impl) --------------------------

    #[tokio::test]
    async fn invite_then_accept_becomes_mutual_friendship() {
        let repo = InMemoryFriendsRepository::new();
        assert_eq!(
            repo.add("a", "b", ts(1)).await.expect("invite"),
            FriendState::InvitedSent
        );
        assert_eq!(
            repo.list("b").await.expect("list")[0].state,
            FriendState::InvitedReceived
        );
        assert_eq!(
            repo.add("b", "a", ts(2)).await.expect("accept"),
            FriendState::Friend
        );
        assert_eq!(
            repo.list("a").await.expect("list")[0].state,
            FriendState::Friend
        );
        assert_eq!(
            repo.list("b").await.expect("list")[0].state,
            FriendState::Friend
        );
    }

    #[tokio::test]
    async fn remove_clears_both_sides_and_is_idempotent() {
        let repo = InMemoryFriendsRepository::new();
        repo.add("a", "b", ts(1)).await.expect("invite");
        repo.add("b", "a", ts(2)).await.expect("accept");
        assert!(repo.remove("a", "b").await.expect("remove"));
        assert!(repo.list("a").await.expect("list").is_empty());
        assert!(repo.list("b").await.expect("list").is_empty());
        assert!(
            !repo.remove("a", "b").await.expect("idempotent"),
            "second remove is a no-op"
        );
    }

    #[tokio::test]
    async fn block_is_one_sided_and_stops_reinvites() {
        let repo = InMemoryFriendsRepository::new();
        repo.add("a", "b", ts(1)).await.expect("invite");
        repo.block("b", "a", ts(2)).await.expect("block");
        assert_eq!(
            repo.list("b").await.expect("list")[0].state,
            FriendState::Blocked
        );
        assert!(
            repo.list("a").await.expect("list").is_empty(),
            "blocked side's view dropped"
        );
        assert_eq!(
            repo.add("a", "b", ts(3))
                .await
                .expect_err("re-invite blocked")
                .category(),
            ErrorCategory::Conflict
        );
        repo.remove("b", "a").await.expect("unblock");
        assert!(repo.add("a", "b", ts(4)).await.is_ok());
    }

    #[tokio::test]
    async fn list_is_ordered_by_other_id() {
        let repo = InMemoryFriendsRepository::new();
        repo.add("me", "zed", ts(1)).await.expect("invite");
        repo.add("me", "amy", ts(2)).await.expect("invite");
        let rows = repo.list("me").await.expect("list");
        assert_eq!(rows[0].user_id, "amy");
        assert_eq!(rows[1].user_id, "zed");
    }
}
