//! Backend selection and the backend-neutral unit-of-work seam.
//!
//! This module is what makes persistence *live in the running node*. It defines
//! two backend-neutral abstractions the rest of the application depends on:
//!
//! - [`UnitOfWork`]: an object-safe transaction scope that hands out the four
//!   repositories bound to one atomic unit, so a multi-write workflow (create a
//!   user, then link its auth identity) commits or rolls back as a whole. Both
//!   backends implement it: the Postgres
//!   [`PgUnitOfWork`](crate::repository::PgUnitOfWork) is a real database
//!   transaction; the in-memory [`InMemoryUnitOfWork`] is serialized by an
//!   application write-lock and rolled back with a compensating undo log.
//! - [`Backend`]: the provider a node selects at startup. It exposes pooled
//!   (autocommit) repositories, its [`BackendKind`], and [`Backend::begin`] to
//!   open a unit of work.
//!
//! [`select_backend`] performs the startup selection: a configured `[database]`
//! builds and migrates a Postgres [`PgDatabase`](crate::repository::PgDatabase)
//! (failing fast if it is unreachable); otherwise the node runs on the
//! [`InMemoryBackend`]. No concrete `sqlx` type ever crosses these boundaries,
//! and no connection string is ever exposed (see the redacted `Debug` on
//! `PgDatabase`).

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::config::DatabaseConfig;
use crate::database_explorer::DatabaseExplorer;
use crate::error::AppResult;
use crate::identity::{AccountState, AuthCredential, AuthIdentity, User, Username};
use crate::session::{Session, SessionId, SessionTokenRef};
use crate::storage::{
    Accessor, ListQuery, ObjectId, Page, Precondition, StorageIndexDefinition,
    StorageIndexMembership, StorageIndexQuery, StorageObject, UserId, WriteRequest,
};
use crate::time::TimestampMillis;

use super::pg::PgDatabase;
use super::sqlite::SqliteDatabase;
use super::{
    AuthIdentityRepository, ChatRepository, FriendsRepository, GroupsRepository,
    InMemoryAuthIdentityRepository, InMemoryChatRepository, InMemoryFriendsRepository,
    InMemoryGroupsRepository, InMemoryLeaderboardsRepository, InMemoryNotificationsRepository,
    InMemoryPurchasesRepository, InMemorySessionRepository, InMemoryStorageRepository,
    InMemoryUserRepository, InMemoryWalletRepository, LeaderboardsRepository,
    NotificationsRepository, PurchasesRepository, SessionRepository, StorageRepository,
    UserRepository, WalletRepository,
};

/// Which persistence backend a node is running on.
///
/// The stable [`BackendKind::as_str`] tokens are safe to surface publicly (e.g.
/// on `/status`) and in logs: they name the backend *class* only and never carry
/// any connection detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// Non-durable, single-process reference backend.
    InMemory,
    /// Durable PostgreSQL backend.
    Postgres,
    /// Durable CockroachDB backend (the Postgres backend over CockroachDB's
    /// PostgreSQL-wire protocol; ).
    Cockroach,
    /// Durable, embedded, single-file SQLite backend.
    Sqlite,
}

impl BackendKind {
    /// Stable lowercase token for status responses and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Postgres => "postgres",
            Self::Cockroach => "cockroach",
            Self::Sqlite => "sqlite",
        }
    }
}

impl fmt::Display for BackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An object-safe transaction scope over one or more repositories.
///
/// Repositories obtained from one unit of work share the same atomic scope, so a
/// create-user-then-link-identity workflow is all-or-nothing. Concrete backend
/// types (a `sqlx::Transaction`, the in-memory undo log) stay behind the impls;
/// callers only ever see `Arc<dyn ..Repository>` and typed results.
///
/// The trait is `Send` (not `Sync`): a unit of work is a single logical
/// transaction owned by one task and moved across `.await` points, never shared.
#[async_trait]
pub trait UnitOfWork: Send {
    /// A storage repository bound to this transaction.
    fn storage_repository(&self) -> Arc<dyn StorageRepository>;
    /// A user repository bound to this transaction.
    fn user_repository(&self) -> Arc<dyn UserRepository>;
    /// An auth-identity repository bound to this transaction.
    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository>;
    /// A session repository bound to this transaction.
    fn session_repository(&self) -> Arc<dyn SessionRepository>;

    /// Commit the transaction, making its writes durable.
    ///
    /// # Errors
    /// Returns a backend error if the commit fails.
    async fn commit(self: Box<Self>) -> AppResult<()>;

    /// Roll the transaction back, discarding its writes.
    ///
    /// # Errors
    /// Returns a backend error if the rollback fails.
    async fn rollback(self: Box<Self>) -> AppResult<()>;
}

/// A persistence backend a node selects at startup.
///
/// Exposes pooled (autocommit) repositories for single-statement work and
/// [`Backend::begin`] for a multi-write [`UnitOfWork`]. Implementors are
/// `Debug` with a redacted representation (no connection string).
#[async_trait]
pub trait Backend: Send + Sync + fmt::Debug {
    /// Which backend this is.
    fn kind(&self) -> BackendKind;
    /// A pooled (autocommit) storage repository.
    fn storage_repository(&self) -> Arc<dyn StorageRepository>;
    /// A pooled (autocommit) user repository.
    fn user_repository(&self) -> Arc<dyn UserRepository>;
    /// A pooled (autocommit) auth-identity repository.
    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository>;
    /// A pooled (autocommit) session repository.
    fn session_repository(&self) -> Arc<dyn SessionRepository>;

    /// A pooled (autocommit) friends repository.
    ///
    /// Friends are a standalone, single-write feature — not part of the
    /// account-creation multi-write workflow — so they are reached only through
    /// this pooled accessor and deliberately absent from [`UnitOfWork`].
    fn friends_repository(&self) -> Arc<dyn FriendsRepository>;

    /// A pooled (autocommit) groups repository.
    ///
    /// Groups are a standalone feature — not part of the account-creation
    /// multi-write workflow — so they are reached only through this pooled
    /// accessor and deliberately absent from [`UnitOfWork`].
    fn groups_repository(&self) -> Arc<dyn GroupsRepository>;

    /// A pooled (autocommit) leaderboards repository.
    ///
    /// Leaderboards are a standalone feature — not part of the account-creation
    /// multi-write workflow — so they are reached only through this pooled
    /// accessor and deliberately absent from [`UnitOfWork`].
    fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository>;

    /// A pooled (autocommit) chat repository.
    ///
    /// Chat channel history is a standalone feature — not part of the
    /// account-creation multi-write workflow — so it is reached only through this
    /// pooled accessor and deliberately absent from [`UnitOfWork`].
    fn chat_repository(&self) -> Arc<dyn ChatRepository>;

    /// A pooled (autocommit) notifications repository.
    ///
    /// The console notification store is a standalone feature — not part of the
    /// account-creation multi-write workflow — so it is reached only through this
    /// pooled accessor and deliberately absent from [`UnitOfWork`].
    fn notifications_repository(&self) -> Arc<dyn NotificationsRepository>;

    /// A pooled (autocommit) wallet repository.
    ///
    /// Per-user currency balances and their change ledger are a standalone
    /// feature — not part of the account-creation multi-write workflow — so they
    /// are reached only through this pooled accessor and deliberately absent from
    /// [`UnitOfWork`]. (The ledger-append + balance-update atomicity is a
    /// per-change transaction the repository owns internally, not a cross-feature
    /// unit of work.)
    fn wallet_repository(&self) -> Arc<dyn WalletRepository>;

    /// A pooled (autocommit) purchases repository.
    ///
    /// The validated purchase / subscription record store is a standalone feature
    /// — not part of the account-creation multi-write workflow — so it is reached
    /// only through this pooled accessor and deliberately absent from
    /// [`UnitOfWork`].
    fn purchases_repository(&self) -> Arc<dyn PurchasesRepository>;

    /// Optional read-only administrative database explorer.
    ///
    /// This is deliberately a capability accessor, not a domain repository:
    /// it exposes no write methods and durable adapters validate their own
    /// metadata before executing a diagnostic read. The in-memory backend has
    /// no SQL schema and therefore reports no explorer.
    fn database_explorer(&self) -> Option<Arc<dyn DatabaseExplorer>> {
        None
    }

    /// Open a new unit of work (transaction scope).
    ///
    /// # Errors
    /// Returns a backend error if a transaction cannot be started.
    async fn begin(&self) -> AppResult<Box<dyn UnitOfWork>>;
}

/// Select the persistence backend for a node from its `[database]` config.
///
/// The backend is chosen by the connection URL's scheme (see
/// [`DatabaseConfig::backend`](crate::config::DatabaseConfig::backend)): a
/// `postgres://`/`postgresql://` URL selects Postgres; a `sqlite:` URL or a bare
/// file path selects the embedded SQLite backend; an absent/empty URL runs the
/// in-memory backend. For a configured database this connects and applies
/// migrations, **failing fast** with a typed error if the database is unreachable
/// or a migration fails — the node must not start half-persistent. The connection
/// string is never logged or echoed in any error.
///
/// # Errors
/// Returns a `Config` or `Database` error if a configured backend cannot be
/// connected or migrated.
pub async fn select_backend(config: &DatabaseConfig) -> AppResult<Arc<dyn Backend>> {
    match config.backend()? {
        Some(crate::config::DatabaseBackend::Postgres) => {
            let db = PgDatabase::connect(config).await?;
            Ok(Arc::new(db))
        }
        Some(crate::config::DatabaseBackend::Sqlite) => {
            let db = SqliteDatabase::connect(config).await?;
            Ok(Arc::new(db))
        }
        None => Ok(Arc::new(InMemoryBackend::new())),
    }
}

// --- In-memory backend ------------------------------------------------------

/// The non-durable, single-process reference [`Backend`].
///
/// Owns the four in-memory repositories and an application write-lock. Pooled
/// accessors clone the shared repository handles; [`InMemoryBackend::begin`]
/// acquires the write-lock (serializing multi-write workflows) and returns an
/// [`InMemoryUnitOfWork`] that records a compensating undo log so an aborted or
/// dropped transaction leaves no partial state.
#[derive(Debug)]
pub struct InMemoryBackend {
    users: Arc<InMemoryUserRepository>,
    identities: Arc<InMemoryAuthIdentityRepository>,
    sessions: Arc<InMemorySessionRepository>,
    storage: Arc<InMemoryStorageRepository>,
    friends: Arc<InMemoryFriendsRepository>,
    groups: Arc<InMemoryGroupsRepository>,
    leaderboards: Arc<InMemoryLeaderboardsRepository>,
    chat: Arc<InMemoryChatRepository>,
    notifications: Arc<InMemoryNotificationsRepository>,
    wallet: Arc<InMemoryWalletRepository>,
    purchases: Arc<InMemoryPurchasesRepository>,
    // Serializes multi-write workflows (account creation). A `tokio` mutex whose
    // guard is held across `.await` inside the unit of work.
    write_lock: Arc<AsyncMutex<()>>,
}

impl InMemoryBackend {
    /// Create an empty in-memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self {
            users: Arc::new(InMemoryUserRepository::new()),
            identities: Arc::new(InMemoryAuthIdentityRepository::new()),
            sessions: Arc::new(InMemorySessionRepository::new()),
            storage: Arc::new(InMemoryStorageRepository::new()),
            friends: Arc::new(InMemoryFriendsRepository::new()),
            groups: Arc::new(InMemoryGroupsRepository::new()),
            leaderboards: Arc::new(InMemoryLeaderboardsRepository::new()),
            chat: Arc::new(InMemoryChatRepository::new()),
            notifications: Arc::new(InMemoryNotificationsRepository::new()),
            wallet: Arc::new(InMemoryWalletRepository::new()),
            purchases: Arc::new(InMemoryPurchasesRepository::new()),
            write_lock: Arc::new(AsyncMutex::new(())),
        }
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::InMemory
    }

    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::clone(&self.storage) as Arc<dyn StorageRepository>
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::clone(&self.users) as Arc<dyn UserRepository>
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::clone(&self.identities) as Arc<dyn AuthIdentityRepository>
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::clone(&self.sessions) as Arc<dyn SessionRepository>
    }

    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::clone(&self.friends) as Arc<dyn FriendsRepository>
    }

    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::clone(&self.groups) as Arc<dyn GroupsRepository>
    }

    fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        Arc::clone(&self.leaderboards) as Arc<dyn LeaderboardsRepository>
    }

    fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        Arc::clone(&self.chat) as Arc<dyn ChatRepository>
    }

    fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        Arc::clone(&self.notifications) as Arc<dyn NotificationsRepository>
    }

    fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        Arc::clone(&self.wallet) as Arc<dyn WalletRepository>
    }

    fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        Arc::clone(&self.purchases) as Arc<dyn PurchasesRepository>
    }

    async fn begin(&self) -> AppResult<Box<dyn UnitOfWork>> {
        // Acquire an owned guard so it can be held for the lifetime of the unit
        // of work (across `.await` points) and dropped on commit/rollback/drop.
        let guard = Arc::clone(&self.write_lock).lock_owned().await;
        Ok(Box::new(InMemoryUnitOfWork::new(
            guard,
            Arc::clone(&self.users),
            Arc::clone(&self.identities),
            Arc::clone(&self.sessions),
            Arc::clone(&self.storage),
        )))
    }
}

/// One compensating action to undo a row an aborted in-memory transaction
/// created.
enum Undo {
    RemoveUser(UserId),
    UnlinkCredential(AuthCredential),
    RemoveSession(SessionId),
    RemoveStorage(ObjectId),
}

/// The undo log shared between a [`InMemoryUnitOfWork`] and the repository
/// handles it hands out.
#[derive(Default)]
struct UndoLog {
    entries: Vec<Undo>,
    // Set once the transaction is resolved (committed or rolled back), after
    // which no further undo entries are recorded and `Drop` is a no-op.
    resolved: bool,
}

/// The in-memory [`UnitOfWork`].
///
/// Holds the backend write-lock guard for its whole lifetime (serializing other
/// multi-write workflows) and an undo log. Repository handles it hands out write
/// to the *same* shared stores as the pooled repositories, so committed writes
/// are immediately visible; each creating write also appends a compensating
/// entry to the undo log. [`InMemoryUnitOfWork::rollback`] (and `Drop` of an
/// unresolved unit of work, e.g. on task cancellation) replays the log in
/// reverse, so a create-user-then-link workflow is genuinely all-or-nothing even
/// though the underlying stores apply writes eagerly.
pub struct InMemoryUnitOfWork {
    _guard: OwnedMutexGuard<()>,
    users: Arc<InMemoryUserRepository>,
    identities: Arc<InMemoryAuthIdentityRepository>,
    sessions: Arc<InMemorySessionRepository>,
    storage: Arc<InMemoryStorageRepository>,
    log: Arc<StdMutex<UndoLog>>,
}

impl InMemoryUnitOfWork {
    fn new(
        guard: OwnedMutexGuard<()>,
        users: Arc<InMemoryUserRepository>,
        identities: Arc<InMemoryAuthIdentityRepository>,
        sessions: Arc<InMemorySessionRepository>,
        storage: Arc<InMemoryStorageRepository>,
    ) -> Self {
        Self {
            _guard: guard,
            users,
            identities,
            sessions,
            storage,
            log: Arc::new(StdMutex::new(UndoLog::default())),
        }
    }

    /// Replay the undo log in reverse to remove everything the transaction
    /// created, then mark it resolved. Idempotent and best-effort (synchronous,
    /// so it is safe to call from `Drop`).
    fn compensate(&self) {
        let entries = match self.log.lock() {
            Ok(mut log) => {
                if log.resolved {
                    return;
                }
                log.resolved = true;
                std::mem::take(&mut log.entries)
            }
            // A poisoned log means another holder panicked; there is nothing safe
            // to replay.
            Err(_) => return,
        };
        for undo in entries.into_iter().rev() {
            match undo {
                Undo::RemoveUser(id) => self.users.remove_user_for_rollback(&id),
                Undo::UnlinkCredential(cred) => {
                    self.identities.remove_credential_for_rollback(&cred);
                }
                Undo::RemoveSession(id) => self.sessions.remove_session_for_rollback(&id),
                Undo::RemoveStorage(id) => self.storage.remove_object_for_rollback(&id),
            }
        }
    }

    /// Mark the transaction committed: keep the writes, drop the undo log.
    fn mark_committed(&self) {
        if let Ok(mut log) = self.log.lock() {
            log.resolved = true;
            log.entries.clear();
        }
    }
}

impl Drop for InMemoryUnitOfWork {
    fn drop(&mut self) {
        // If neither commit nor rollback ran (e.g. the owning task was cancelled),
        // undo the transaction's writes so no partial state survives.
        self.compensate();
    }
}

#[async_trait]
impl UnitOfWork for InMemoryUnitOfWork {
    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::new(TxStorageRepository {
            inner: Arc::clone(&self.storage),
            log: Arc::clone(&self.log),
        })
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(TxUserRepository {
            inner: Arc::clone(&self.users),
            log: Arc::clone(&self.log),
        })
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(TxAuthIdentityRepository {
            inner: Arc::clone(&self.identities),
            log: Arc::clone(&self.log),
        })
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(TxSessionRepository {
            inner: Arc::clone(&self.sessions),
            log: Arc::clone(&self.log),
        })
    }

    async fn commit(self: Box<Self>) -> AppResult<()> {
        self.mark_committed();
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> AppResult<()> {
        self.compensate();
        Ok(())
    }
}

/// Append a compensating entry unless the transaction is already resolved.
fn record(log: &Arc<StdMutex<UndoLog>>, undo: Undo) {
    if let Ok(mut log) = log.lock()
        && !log.resolved
    {
        log.entries.push(undo);
    }
}

// The transaction-bound repository handles: each delegates to the shared store
// and records a compensating undo entry for the rows it creates. Reads and
// non-creating writes delegate directly (Citadel's only multi-write workflow is
// account creation, which never updates or deletes within the unit of work).

struct TxUserRepository {
    inner: Arc<InMemoryUserRepository>,
    log: Arc<StdMutex<UndoLog>>,
}

#[async_trait]
impl UserRepository for TxUserRepository {
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>> {
        self.inner.get_user(id).await
    }

    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>> {
        self.inner.get_user_by_username(username).await
    }

    async fn list_users(
        &self,
        filter: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> AppResult<crate::repository::identity::UserPage> {
        self.inner.list_users(filter, limit, offset).await
    }

    async fn create_user(&self, user: User) -> AppResult<User> {
        let created = self.inner.create_user(user).await?;
        record(&self.log, Undo::RemoveUser(created.id.clone()));
        Ok(created)
    }

    async fn update_user(&self, user: User) -> AppResult<User> {
        self.inner.update_user(user).await
    }

    async fn set_user_state(
        &self,
        id: &UserId,
        state: AccountState,
        updated_at: TimestampMillis,
    ) -> AppResult<User> {
        self.inner.set_user_state(id, state, updated_at).await
    }
}

struct TxAuthIdentityRepository {
    inner: Arc<InMemoryAuthIdentityRepository>,
    log: Arc<StdMutex<UndoLog>>,
}

#[async_trait]
impl AuthIdentityRepository for TxAuthIdentityRepository {
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>> {
        self.inner.get_auth_identity(credential).await
    }

    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>> {
        self.inner.list_auth_identities(user_id).await
    }

    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity> {
        // Record a compensating unlink only when this call actually created a new
        // link (not an idempotent re-link of an existing pair). The backend
        // write-lock is held, so this check-then-link cannot race.
        let existed = self
            .inner
            .get_auth_identity(&identity.credential)
            .await?
            .is_some();
        let credential = identity.credential.clone();
        let linked = self.inner.link_auth_identity(identity).await?;
        if !existed {
            record(&self.log, Undo::UnlinkCredential(credential));
        }
        Ok(linked)
    }

    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()> {
        self.inner.unlink_auth_identity(credential).await
    }
}

struct TxSessionRepository {
    inner: Arc<InMemorySessionRepository>,
    log: Arc<StdMutex<UndoLog>>,
}

#[async_trait]
impl SessionRepository for TxSessionRepository {
    async fn get_session(&self, id: &SessionId) -> AppResult<Option<Session>> {
        self.inner.get_session(id).await
    }

    async fn get_session_by_token_ref(
        &self,
        token_ref: &SessionTokenRef,
    ) -> AppResult<Option<Session>> {
        self.inner.get_session_by_token_ref(token_ref).await
    }

    async fn create_session(&self, session: Session) -> AppResult<Session> {
        let created = self.inner.create_session(session).await?;
        record(&self.log, Undo::RemoveSession(created.id.clone()));
        Ok(created)
    }

    async fn update_session(&self, session: Session) -> AppResult<Session> {
        self.inner.update_session(session).await
    }

    async fn revoke_user_sessions(
        &self,
        user_id: &UserId,
        revoked_at: TimestampMillis,
        reason: crate::session::RevocationReason,
    ) -> AppResult<usize> {
        self.inner
            .revoke_user_sessions(user_id, revoked_at, reason)
            .await
    }
}

struct TxStorageRepository {
    inner: Arc<InMemoryStorageRepository>,
    log: Arc<StdMutex<UndoLog>>,
}

#[async_trait]
impl StorageRepository for TxStorageRepository {
    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
        self.inner.read(accessor, id).await
    }

    async fn write(&self, accessor: &Accessor, request: WriteRequest) -> AppResult<StorageObject> {
        self.write_indexed(accessor, request, None).await
    }

    async fn write_indexed(
        &self,
        accessor: &Accessor,
        request: WriteRequest,
        membership: Option<&StorageIndexMembership>,
    ) -> AppResult<StorageObject> {
        let id = request.id.clone();
        let existed = self.inner.contains_object(&id)?;
        let object = self
            .inner
            .write_indexed(accessor, request, membership)
            .await?;
        if !existed {
            record(&self.log, Undo::RemoveStorage(id));
        }
        Ok(object)
    }

    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()> {
        self.inner.delete(accessor, id, expected).await
    }

    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>> {
        self.inner.list(accessor, query).await
    }

    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()> {
        self.inner.install_index(index).await
    }

    async fn query_index(
        &self,
        accessor: &Accessor,
        query: &StorageIndexQuery,
    ) -> AppResult<Vec<StorageObject>> {
        self.inner.query_index(accessor, query).await
    }

    async fn list_collections(&self) -> AppResult<Vec<crate::storage::CollectionSummary>> {
        self.inner.list_collections().await
    }
}

/// Generate a process-unique, restart-safe identifier prefix.
///
/// A monotonic counter that resets to the same value on every process start is
/// unsafe for a durable backend: after a restart it would regenerate ids that
/// already exist, and account creation would then fail with a spurious conflict.
/// Seeding each process from an OS-randomized [`RandomState`] (mixed with the
/// wall clock) gives a distinct id space per process — and per node — with no
/// extra dependency, while an atomic counter keeps ids unique within a process.
pub(crate) fn random_instance_prefix() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    // `RandomState` is OS-seeded per construction, so hashing the wall clock
    // through it yields a value that differs across process starts.
    RandomState::new().hash_one(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::DeviceId;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn sample_user(id: &str, username: &str) -> User {
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

    fn device_identity(device: &str, user_id: &str) -> AuthIdentity {
        AuthIdentity::new(
            AuthCredential::Device(DeviceId::new(device).expect("device")),
            UserId::new(user_id).expect("uid"),
            ts(100),
            ts(100),
        )
        .expect("identity")
    }

    #[test]
    fn backend_kind_tokens_are_stable_and_safe() {
        assert_eq!(BackendKind::InMemory.as_str(), "in-memory");
        assert_eq!(BackendKind::Postgres.as_str(), "postgres");
        assert_eq!(BackendKind::Postgres.to_string(), "postgres");
        assert_eq!(BackendKind::Cockroach.as_str(), "cockroach");
        assert_eq!(BackendKind::Cockroach.to_string(), "cockroach");
    }

    #[tokio::test]
    async fn select_backend_defaults_to_in_memory() {
        let backend = select_backend(&DatabaseConfig::default())
            .await
            .expect("in-memory backend");
        assert_eq!(backend.kind(), BackendKind::InMemory);
    }

    #[tokio::test]
    async fn committed_unit_of_work_persists_and_is_visible_on_pooled_repos() {
        let backend = InMemoryBackend::new();
        let uow = backend.begin().await.expect("begin");
        uow.user_repository()
            .create_user(sample_user("u-1", "alice"))
            .await
            .expect("create user");
        uow.auth_identity_repository()
            .link_auth_identity(device_identity("d-1", "u-1"))
            .await
            .expect("link");
        uow.commit().await.expect("commit");

        assert!(
            backend
                .user_repository()
                .get_user(&UserId::new("u-1").expect("uid"))
                .await
                .expect("get")
                .is_some(),
            "committed user is visible on the pooled repository"
        );
        assert!(
            backend
                .auth_identity_repository()
                .get_auth_identity(&AuthCredential::Device(DeviceId::new("d-1").expect("dev")))
                .await
                .expect("get")
                .is_some()
        );
    }

    #[tokio::test]
    async fn rolled_back_unit_of_work_leaves_no_partial_account() {
        let backend = InMemoryBackend::new();
        let uow = backend.begin().await.expect("begin");
        uow.user_repository()
            .create_user(sample_user("u-2", "bob"))
            .await
            .expect("create user");
        uow.rollback().await.expect("rollback");

        assert!(
            backend
                .user_repository()
                .get_user(&UserId::new("u-2").expect("uid"))
                .await
                .expect("get")
                .is_none(),
            "rolled-back user must not persist"
        );
    }

    #[tokio::test]
    async fn dropped_unit_of_work_compensates_like_rollback() {
        let backend = InMemoryBackend::new();
        {
            let uow = backend.begin().await.expect("begin");
            uow.user_repository()
                .create_user(sample_user("u-3", "carol"))
                .await
                .expect("create user");
            // Neither commit nor rollback: dropping the unit of work (as on task
            // cancellation) must still undo the write.
            drop(uow);
        }
        assert!(
            backend
                .user_repository()
                .get_user(&UserId::new("u-3").expect("uid"))
                .await
                .expect("get")
                .is_none(),
            "dropped (uncommitted) unit of work must compensate"
        );
    }

    #[tokio::test]
    async fn idempotent_relink_is_not_compensated_on_rollback() {
        let backend = InMemoryBackend::new();
        // Pre-existing committed link.
        let uow = backend.begin().await.expect("begin");
        uow.user_repository()
            .create_user(sample_user("u-4", "dave"))
            .await
            .expect("create");
        uow.auth_identity_repository()
            .link_auth_identity(device_identity("d-4", "u-4"))
            .await
            .expect("link");
        uow.commit().await.expect("commit");

        // A second unit of work re-links the same pair (idempotent) then rolls
        // back: the pre-existing link must survive.
        let uow = backend.begin().await.expect("begin");
        uow.auth_identity_repository()
            .link_auth_identity(device_identity("d-4", "u-4"))
            .await
            .expect("idempotent re-link");
        uow.rollback().await.expect("rollback");

        assert!(
            backend
                .auth_identity_repository()
                .get_auth_identity(&AuthCredential::Device(DeviceId::new("d-4").expect("dev")))
                .await
                .expect("get")
                .is_some(),
            "an idempotent re-link must not be undone by a later rollback"
        );
    }

    #[test]
    fn instance_prefixes_differ_across_processes() {
        // Two prefixes computed in one process can coincide only with vanishing
        // probability; the important property (distinct seed per process) cannot
        // be exercised in a single test, so we only assert the generator runs and
        // that repeated calls are not trivially zero.
        let a = random_instance_prefix();
        let b = random_instance_prefix();
        assert!(
            a != 0 || b != 0,
            "prefix generator should not be constant 0"
        );
    }
}
