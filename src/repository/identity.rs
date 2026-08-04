//! Identity repository contracts.
//!
//! These define the early `UserRepository` and `AuthIdentityRepository`
//! boundaries listed in the feature-parity plan. Like
//! [`StorageRepository`](crate::repository::StorageRepository), they are async
//! (via [`async_trait`]) and object-safe so services can hold `Arc<dyn ..>`
//! while a future Postgres/sqlx backend implements the same contract behind the
//! same domain types ( / ). The in-memory reference impls keep
//! synchronous bodies (ready futures, no `.await`).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::identity::{AccountState, AuthCredential, AuthIdentity, User, UserId, Username};
use crate::time::TimestampMillis;

/// One page of an administrative account listing.
#[derive(Debug, Clone, PartialEq)]
pub struct UserPage {
    /// The page of accounts, username-ordered.
    pub users: Vec<User>,
    /// Total accounts matching the filter (across all pages).
    pub total: u64,
}

/// The outcome of a current-account scoped unlink attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnlinkResult {
    /// The credential was removed from the caller's identity set.
    Unlinked,
    /// The credential is absent or belongs to another account.
    NotOwned,
    /// Removing the credential would leave the account with none.
    LastCredential,
}

/// Persistence boundary for user accounts.
#[async_trait]
pub trait UserRepository: Send + Sync {
    /// Fetch a user by id.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>>;

    /// Fetch a user by username.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>>;

    /// Create a new account.
    ///
    /// # Errors
    /// Returns a conflict error if the id or username already exists.
    async fn create_user(&self, user: User) -> AppResult<User>;

    /// Update mutable account fields.
    ///
    /// # Errors
    /// Returns a not-found error if the account does not exist.
    async fn update_user(&self, user: User) -> AppResult<User>;

    /// Transition an account's lifecycle state.
    ///
    /// # Errors
    /// Returns a not-found error if the account does not exist.
    async fn set_user_state(
        &self,
        id: &UserId,
        state: AccountState,
        updated_at: TimestampMillis,
    ) -> AppResult<User>;

    /// Administrative account listing, username-ordered.
    ///
    /// `filter` is a case-sensitive substring match over the account id and
    /// username; `None` matches every account. `offset`/`limit` page the
    /// result; `total` in the returned page counts every match. Includes
    /// disabled and tombstoned accounts — the console must see them.
    ///
    /// # Errors
    /// - `Validation` if `limit` is zero.
    /// - A backend error on failure.
    async fn list_users(
        &self,
        filter: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> AppResult<UserPage>;
}

/// Persistence boundary for credential-to-account links.
#[async_trait]
pub trait AuthIdentityRepository: Send + Sync {
    /// Resolve the account linked to a credential.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>>;

    /// List every identity linked to an account.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>>;

    /// Link a credential to an account.
    ///
    /// # Errors
    /// Returns a conflict error if the credential is already linked to another
    /// account.
    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity>;

    /// Unlink a credential.
    ///
    /// # Errors
    /// Returns a backend error; unlinking an absent credential is idempotent.
    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()>;

    /// Remove a credential only when it belongs to `user_id`, refusing to leave
    /// that account without an authentication credential. Implementations make
    /// the ownership check, count, and delete one atomic operation.
    async fn unlink_auth_identity_for_user(
        &self,
        user_id: &UserId,
        credential: &AuthCredential,
    ) -> AppResult<UnlinkResult> {
        let Some(identity) = self.get_auth_identity(credential).await? else {
            return Ok(UnlinkResult::NotOwned);
        };
        if &identity.user_id != user_id {
            return Ok(UnlinkResult::NotOwned);
        }
        if self.list_auth_identities(user_id).await?.len() <= 1 {
            return Ok(UnlinkResult::LastCredential);
        }
        self.unlink_auth_identity(credential).await?;
        Ok(UnlinkResult::Unlinked)
    }
}

/// A contract-faithful, in-memory [`UserRepository`].
///
/// Single-process and not durable, but it enforces the full contract:
/// id and username uniqueness are kept consistent under one lock, `created_at`
/// is immutable across updates, and every write preserves the account timestamp
/// invariant. The reusable contract tests run against this so any future
/// persistence backend can be held to the same behavior.
#[derive(Debug, Default)]
pub struct InMemoryUserRepository {
    inner: Mutex<UserStore>,
}

#[derive(Debug, Default)]
struct UserStore {
    by_id: HashMap<UserId, User>,
    username_to_id: HashMap<Username, UserId>,
}

impl InMemoryUserRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, UserStore>> {
        self.inner
            .lock()
            .map_err(|_| AppError::internal("user repository mutex poisoned"))
    }

    /// Compensating delete used only by the in-memory unit of work to roll back a
    /// user it created within an aborted transaction.
    ///
    /// Best-effort and synchronous so it is callable from `Drop`: it removes the
    /// account and its username index entry, and silently no-ops on a poisoned
    /// lock (the process is already unwinding). Never part of the public
    /// `UserRepository` contract — a user delete is not a supported operation.
    pub(crate) fn remove_user_for_rollback(&self, id: &UserId) {
        if let Ok(mut store) = self.inner.lock()
            && let Some(user) = store.by_id.remove(id)
        {
            // Only drop the username index if it still points at this account.
            if store.username_to_id.get(&user.username) == Some(id) {
                store.username_to_id.remove(&user.username);
            }
        }
    }
}

#[async_trait]
impl UserRepository for InMemoryUserRepository {
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>> {
        Ok(self.guard()?.by_id.get(id).cloned())
    }

    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>> {
        let store = self.guard()?;
        Ok(store
            .username_to_id
            .get(username)
            .and_then(|id| store.by_id.get(id))
            .cloned())
    }

    async fn create_user(&self, user: User) -> AppResult<User> {
        let mut store = self.guard()?;
        if store.by_id.contains_key(&user.id) {
            return Err(AppError::conflict("user id already exists"));
        }
        if store.username_to_id.contains_key(&user.username) {
            return Err(AppError::conflict("username already exists"));
        }
        store
            .username_to_id
            .insert(user.username.clone(), user.id.clone());
        store.by_id.insert(user.id.clone(), user.clone());
        Ok(user)
    }

    async fn update_user(&self, user: User) -> AppResult<User> {
        let mut store = self.guard()?;
        let existing = store
            .by_id
            .get(&user.id)
            .ok_or_else(|| AppError::not_found("user does not exist"))?;
        // `created_at` is immutable; refuse to rewrite account history.
        if existing.created_at != user.created_at {
            return Err(AppError::conflict("user created_at is immutable"));
        }
        // Keep the username index unique and consistent.
        if existing.username != user.username {
            if let Some(owner) = store.username_to_id.get(&user.username)
                && owner != &user.id
            {
                return Err(AppError::conflict("username already exists"));
            }
            let old_username = existing.username.clone();
            store.username_to_id.remove(&old_username);
            store
                .username_to_id
                .insert(user.username.clone(), user.id.clone());
        }
        store.by_id.insert(user.id.clone(), user.clone());
        Ok(user)
    }

    async fn set_user_state(
        &self,
        id: &UserId,
        state: AccountState,
        updated_at: TimestampMillis,
    ) -> AppResult<User> {
        let mut store = self.guard()?;
        let existing = store
            .by_id
            .get(id)
            .ok_or_else(|| AppError::not_found("user does not exist"))?;
        let updated = User::new(
            existing.id.clone(),
            existing.username.clone(),
            existing.display_name.clone(),
            existing.metadata.clone(),
            existing.created_at,
            updated_at,
            state,
        )?;
        store.by_id.insert(id.clone(), updated.clone());
        Ok(updated)
    }

    async fn list_users(
        &self,
        filter: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> AppResult<UserPage> {
        if limit == 0 {
            return Err(AppError::validation("list limit must be greater than zero"));
        }
        let store = self.guard()?;
        let mut matched: Vec<&User> = store
            .by_id
            .values()
            .filter(|user| {
                filter.is_none_or(|needle| {
                    user.id.as_str().contains(needle) || user.username.as_str().contains(needle)
                })
            })
            .collect();
        matched.sort_by(|a, b| {
            a.username
                .as_str()
                .cmp(b.username.as_str())
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        let total = matched.len() as u64;
        let users = matched
            .into_iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        Ok(UserPage { users, total })
    }
}

/// A contract-faithful, in-memory [`AuthIdentityRepository`].
///
/// Enforces the one-credential-to-one-account invariant: linking a credential to
/// a different account than it already maps to is a conflict, re-linking the same
/// pair is idempotent, and unlinking an absent credential is a no-op.
#[derive(Debug, Default)]
pub struct InMemoryAuthIdentityRepository {
    inner: Mutex<HashMap<AuthCredential, AuthIdentity>>,
}

impl InMemoryAuthIdentityRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, HashMap<AuthCredential, AuthIdentity>>> {
        self.inner
            .lock()
            .map_err(|_| AppError::internal("auth identity repository mutex poisoned"))
    }

    /// Compensating unlink used only by the in-memory unit of work to roll back a
    /// credential link it created within an aborted transaction.
    ///
    /// Best-effort and synchronous (callable from `Drop`); no-ops on a poisoned
    /// lock. Distinct from the public idempotent `unlink_auth_identity` only in
    /// that it is a rollback primitive, not a contract operation.
    pub(crate) fn remove_credential_for_rollback(&self, credential: &AuthCredential) {
        if let Ok(mut store) = self.inner.lock() {
            store.remove(credential);
        }
    }
}

#[async_trait]
impl AuthIdentityRepository for InMemoryAuthIdentityRepository {
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>> {
        Ok(self.guard()?.get(credential).cloned())
    }

    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>> {
        let store = self.guard()?;
        let mut identities: Vec<AuthIdentity> = store
            .values()
            .filter(|identity| &identity.user_id == user_id)
            .cloned()
            .collect();
        // Deterministic order for stable listings/tests.
        identities.sort_by(|a, b| {
            a.provider()
                .as_str()
                .cmp(b.provider().as_str())
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        Ok(identities)
    }

    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity> {
        let mut store = self.guard()?;
        if let Some(existing) = store.get(&identity.credential) {
            if existing.user_id != identity.user_id {
                return Err(AppError::conflict(
                    "credential already linked to another account",
                ));
            }
            // Idempotent re-link of the same credential/account pair.
            return Ok(existing.clone());
        }
        store.insert(identity.credential.clone(), identity.clone());
        Ok(identity)
    }

    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()> {
        self.guard()?.remove(credential);
        Ok(())
    }

    async fn unlink_auth_identity_for_user(
        &self,
        user_id: &UserId,
        credential: &AuthCredential,
    ) -> AppResult<UnlinkResult> {
        let mut store = self.guard()?;
        let Some(identity) = store.get(credential) else {
            return Ok(UnlinkResult::NotOwned);
        };
        if &identity.user_id != user_id {
            return Ok(UnlinkResult::NotOwned);
        }
        if store
            .values()
            .filter(|identity| &identity.user_id == user_id)
            .count()
            <= 1
        {
            return Ok(UnlinkResult::LastCredential);
        }
        store.remove(credential);
        Ok(UnlinkResult::Unlinked)
    }
}

#[cfg(test)]
mod list_users_tests {
    use super::*;
    use crate::identity::{AccountState, User, Username};
    use crate::storage::UserId;

    fn user(id: &str, username: &str) -> User {
        let now = TimestampMillis::from_unix_millis(1_000);
        User::new(
            UserId::new(id).expect("id"),
            Username::new(username).expect("username"),
            None,
            None,
            now,
            now,
            AccountState::Active,
        )
        .expect("user")
    }

    #[tokio::test]
    async fn lists_username_ordered_with_filter_paging_and_total() {
        let repo = InMemoryUserRepository::new();
        for (id, name) in [("u-3", "carol"), ("u-1", "alice"), ("u-2", "bob")] {
            repo.create_user(user(id, name)).await.expect("create");
        }

        let all = repo.list_users(None, 10, 0).await.expect("list");
        assert_eq!(all.total, 3);
        let names: Vec<&str> = all.users.iter().map(|u| u.username.as_str()).collect();
        assert_eq!(names, vec!["alice", "bob", "carol"], "username-ordered");

        // Paging: offset walks the ordered list; total stays global.
        let page = repo.list_users(None, 1, 1).await.expect("page");
        assert_eq!(page.users[0].username.as_str(), "bob");
        assert_eq!(page.total, 3);

        // Filter matches id or username substring.
        let by_name = repo.list_users(Some("ali"), 10, 0).await.expect("filter");
        assert_eq!(by_name.total, 1);
        let by_id = repo.list_users(Some("u-2"), 10, 0).await.expect("filter");
        assert_eq!(by_id.users[0].username.as_str(), "bob");

        // Zero limit is a validation error, matching the storage contract.
        assert!(repo.list_users(None, 0, 0).await.is_err());
    }

    #[tokio::test]
    async fn listing_includes_disabled_and_tombstoned_accounts() {
        let repo = InMemoryUserRepository::new();
        repo.create_user(user("u-1", "alice"))
            .await
            .expect("create");
        repo.set_user_state(
            &UserId::new("u-1").expect("id"),
            AccountState::Disabled,
            TimestampMillis::from_unix_millis(2_000),
        )
        .await
        .expect("disable");
        let all = repo.list_users(None, 10, 0).await.expect("list");
        assert_eq!(all.total, 1, "the console must see banned accounts");
        assert_eq!(all.users[0].state, AccountState::Disabled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use crate::identity::{CustomId, DeviceId, Username};

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn user(id: &str, username: &str) -> User {
        User::new(
            UserId::new(id).expect("uid"),
            Username::new(username).expect("username"),
            None,
            None,
            ts(100),
            ts(100),
            AccountState::Active,
        )
        .expect("user")
    }

    #[tokio::test]
    async fn create_rejects_duplicate_id_and_username() {
        let repo = InMemoryUserRepository::new();
        repo.create_user(user("u-1", "alice"))
            .await
            .expect("create");

        // Duplicate id.
        let dup_id = user("u-1", "bob");
        assert_eq!(
            repo.create_user(dup_id)
                .await
                .expect_err("dup id")
                .category(),
            ErrorCategory::Conflict
        );
        // Duplicate username.
        let dup_name = user("u-2", "alice");
        assert_eq!(
            repo.create_user(dup_name)
                .await
                .expect_err("dup name")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn get_by_username_and_id_round_trip() {
        let repo = InMemoryUserRepository::new();
        repo.create_user(user("u-1", "alice"))
            .await
            .expect("create");
        assert_eq!(
            repo.get_user(&UserId::new("u-1").expect("test value"))
                .await
                .expect("get")
                .expect("present")
                .username
                .as_str(),
            "alice"
        );
        assert_eq!(
            repo.get_user_by_username(&Username::new("alice").expect("test value"))
                .await
                .expect("get")
                .expect("present")
                .id
                .as_str(),
            "u-1"
        );
    }

    #[tokio::test]
    async fn update_preserves_username_uniqueness_and_created_at() {
        let repo = InMemoryUserRepository::new();
        repo.create_user(user("u-1", "alice"))
            .await
            .expect("create");
        repo.create_user(user("u-2", "bob")).await.expect("create");

        // Rename u-2 to a free username: ok.
        let mut renamed = user("u-2", "charlie");
        renamed.updated_at = ts(200);
        repo.update_user(renamed).await.expect("rename ok");
        assert!(
            repo.get_user_by_username(&Username::new("bob").expect("test value"))
                .await
                .expect("get")
                .is_none()
        );

        // Rename u-2 to a taken username: conflict.
        let clash = user("u-2", "alice");
        assert_eq!(
            repo.update_user(clash).await.expect_err("clash").category(),
            ErrorCategory::Conflict
        );

        // Changing created_at is rejected.
        let mut history = user("u-1", "alice");
        history.created_at = ts(999);
        assert_eq!(
            repo.update_user(history)
                .await
                .expect_err("immutable created_at")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn set_state_updates_and_requires_existing() {
        let repo = InMemoryUserRepository::new();
        repo.create_user(user("u-1", "alice"))
            .await
            .expect("create");
        let disabled = repo
            .set_user_state(
                &UserId::new("u-1").expect("test value"),
                AccountState::Disabled,
                ts(300),
            )
            .await
            .expect("set state");
        assert_eq!(disabled.state, AccountState::Disabled);
        assert_eq!(disabled.updated_at, ts(300));
        assert_eq!(disabled.created_at, ts(100));

        assert_eq!(
            repo.set_user_state(
                &UserId::new("missing").expect("test value"),
                AccountState::Disabled,
                ts(300)
            )
            .await
            .expect_err("missing")
            .category(),
            ErrorCategory::NotFound
        );
    }

    fn device_identity(device: &str, user_id: &str) -> AuthIdentity {
        AuthIdentity::new(
            AuthCredential::Device(DeviceId::new(device).expect("device")),
            UserId::new(user_id).expect("uid"),
            ts(100),
            ts(100),
        )
        .expect("identity")
    }

    #[tokio::test]
    async fn link_enforces_one_credential_one_account() {
        let repo = InMemoryAuthIdentityRepository::new();
        let cred = AuthCredential::Device(DeviceId::new("d-1").expect("test value"));
        repo.link_auth_identity(device_identity("d-1", "u-1"))
            .await
            .expect("link");

        // Re-link same pair: idempotent.
        repo.link_auth_identity(device_identity("d-1", "u-1"))
            .await
            .expect("idempotent re-link");

        // Link to a different account: conflict.
        assert_eq!(
            repo.link_auth_identity(device_identity("d-1", "u-2"))
                .await
                .expect_err("conflict")
                .category(),
            ErrorCategory::Conflict
        );

        // Resolve and list.
        assert_eq!(
            repo.get_auth_identity(&cred)
                .await
                .expect("get")
                .expect("present")
                .user_id
                .as_str(),
            "u-1"
        );
        assert_eq!(
            repo.list_auth_identities(&UserId::new("u-1").expect("test value"))
                .await
                .expect("list")
                .len(),
            1
        );

        // Unlink is idempotent.
        repo.unlink_auth_identity(&cred).await.expect("unlink");
        repo.unlink_auth_identity(&cred)
            .await
            .expect("idempotent unlink");
        assert!(repo.get_auth_identity(&cred).await.expect("get").is_none());
    }

    #[tokio::test]
    async fn scoped_unlink_refuses_foreign_and_last_credential() {
        let repo = InMemoryAuthIdentityRepository::new();
        let alice = UserId::new("alice").expect("alice");
        let bob = UserId::new("bob").expect("bob");
        let alice_device = AuthCredential::Device(DeviceId::new("alice-device").expect("device"));
        let alice_custom = AuthCredential::Custom(CustomId::new("alice-custom").expect("custom"));
        let bob_device = AuthCredential::Device(DeviceId::new("bob-device").expect("device"));
        repo.link_auth_identity(
            AuthIdentity::new(alice_device.clone(), alice.clone(), ts(1), ts(1)).expect("identity"),
        )
        .await
        .expect("link");
        repo.link_auth_identity(
            AuthIdentity::new(alice_custom.clone(), alice.clone(), ts(1), ts(1)).expect("identity"),
        )
        .await
        .expect("link");
        repo.link_auth_identity(
            AuthIdentity::new(bob_device.clone(), bob, ts(1), ts(1)).expect("identity"),
        )
        .await
        .expect("link");

        assert_eq!(
            repo.unlink_auth_identity_for_user(&alice, &bob_device)
                .await
                .expect("foreign no-op"),
            UnlinkResult::NotOwned
        );
        assert_eq!(
            repo.unlink_auth_identity_for_user(&alice, &alice_device)
                .await
                .expect("unlink"),
            UnlinkResult::Unlinked
        );
        assert_eq!(
            repo.unlink_auth_identity_for_user(&alice, &alice_custom)
                .await
                .expect("last refused"),
            UnlinkResult::LastCredential
        );
        assert!(
            repo.get_auth_identity(&alice_custom)
                .await
                .expect("get")
                .is_some()
        );
    }
}
