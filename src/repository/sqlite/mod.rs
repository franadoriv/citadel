//! SQLite persistence backend.
//!
//! SQLite is a *sibling* backend to Postgres, not a replacement: it lives behind
//! the exact same [`Backend`](crate::repository::backend::Backend) /
//! [`UnitOfWork`](crate::repository::backend::UnitOfWork) seam so the rest of the
//! server is unchanged, and it exists to power the minimalist self-hosted story —
//! a single executable, a `game/` scripts folder, and one `data.sqlite` file with
//! zero external infrastructure.
//!
//! Because SQLite is embedded (no server), its storage repository is exercised by
//! the SAME `tests/storage_repository_contract.rs` suite as the in-memory and
//! Postgres backends, run **un-gated** in `scripts/check.sh` — real SQL-backend
//! coverage on every check.
//!
//! Layout mirrors [`crate::repository::pg`]: every SQLite-specific choice stays
//! behind this module. Nothing here leaks a `sqlx::SqlitePool` or
//! `sqlx::Transaction` across a repository contract — callers only ever see
//! `Arc<dyn StorageRepository>` (and the other repository trait objects),
//! [`SqliteUnitOfWork`], and typed [`AppError`](crate::error::AppError)s.
//!
//! Dialect differences handled here (mirroring Postgres semantics exactly):
//!
//! - JSON is stored as `TEXT` (no `jsonb`); the repository serializes at the
//!   boundary.
//! - There is no advisory lock or `SELECT ... FOR UPDATE`: SQLite allows only one
//!   writer at a time, so a write/delete runs inside a transaction and the
//!   `PRIMARY KEY` unique constraint is the final backstop for the concurrent
//!   create race. A `busy_timeout` lets a contending writer wait rather than fail.
//! - Unique-violation detection uses the portable [`sqlx::error::ErrorKind`]
//!   rather than the Postgres `23505` SQLSTATE.
//! - Timestamps stay Unix-epoch integers and ids/keys stay `TEXT`, matching the
//!   Postgres schema choices.
//!
//! Queries are **runtime-checked** ([`sqlx::query`] with `try_get` decoding),
//! never the compile-time `query!` macro.
//!
//! The backend serves all four repositories natively: storage plus
//! [`mod@identity`] ([`SqliteUserRepository`], [`SqliteAuthIdentityRepository`])
//! and [`mod@session`] ([`SqliteSessionRepository`]). A workflow such
//! as create-user-then-link-identity is driven through one [`SqliteUnitOfWork`],
//! so account creation commits or rolls back atomically as a single SQLite
//! transaction, matching the Postgres semantics exactly.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
};
use sqlx::{Row, Sqlite, Transaction};
use tokio::sync::Mutex;

use crate::config::DatabaseConfig;
use crate::database_explorer::{DatabaseExplorer, SqliteMetadataExplorer};
use crate::error::{AppError, AppResult};
use crate::repository::backend::{Backend as BackendTrait, BackendKind, UnitOfWork};
use crate::repository::{
    AuthIdentityRepository, ChatRepository, FriendsRepository, GroupsRepository,
    LeaderboardsRepository, NotificationsRepository, PurchasesRepository, SessionRepository,
    StorageRepository, UserRepository, WalletRepository,
};
use crate::time::TimestampMillis;

mod chat;
mod friends;
mod groups;
mod identity;
mod leaderboards;
mod notifications;
mod purchases;
mod session;
mod storage;
mod wallet;

pub use chat::SqliteChatRepository;
pub use friends::SqliteFriendsRepository;
pub use groups::SqliteGroupsRepository;
pub use identity::{SqliteAuthIdentityRepository, SqliteUserRepository};
pub use leaderboards::SqliteLeaderboardsRepository;
pub use notifications::SqliteNotificationsRepository;
pub use purchases::SqlitePurchasesRepository;
pub use session::SqliteSessionRepository;
pub use storage::SqliteStorageRepository;
pub use wallet::SqliteWalletRepository;

// --- Shared error / decode helpers ------------------------------------------

/// Map a raw `sqlx::Error` to a typed [`AppError`].
///
/// A unique-constraint violation means a row was created concurrently, which is a
/// [`Conflict`](crate::error::ErrorCategory::Conflict) — the same meaning the
/// Postgres backend derives from SQLSTATE `23505`, but detected via the portable
/// [`sqlx::error::ErrorKind::UniqueViolation`] since SQLite reports extended
/// result codes (`SQLITE_CONSTRAINT_PRIMARYKEY`) instead. Every other backend
/// failure maps to [`Database`](crate::error::ErrorCategory::Database) with
/// sanitized detail — never `Permission`, `Conflict`, or `Auth`.
fn db_err(error: sqlx::Error) -> AppError {
    if let sqlx::Error::Database(db) = &error
        && matches!(db.kind(), sqlx::error::ErrorKind::UniqueViolation)
    {
        return AppError::conflict("resource already exists");
    }
    AppError::database("database backend error").with_detail(error.to_string())
}

fn tx_closed() -> AppError {
    AppError::internal("database transaction is already closed")
}

/// Fetch a typed column, mapping decode failures to an internal error.
///
/// Shared by the identity/session repositories (the storage repository keeps a
/// private copy near its row decoders). Mirrors `pg::get`.
fn get<'r, T>(row: &'r SqliteRow, column: &str) -> AppResult<T>
where
    T: sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get::<T, _>(column).map_err(|e| {
        AppError::internal(format!("failed to decode column `{column}`")).with_detail(e.to_string())
    })
}

// --- Domain <-> integer millis conversion -----------------------------------

/// Convert a domain [`TimestampMillis`] to the `INTEGER` value stored in SQLite.
///
/// Domain time is Unix epoch milliseconds (`u64`); a SQLite `INTEGER` is a signed
/// 64-bit integer. Any realistic timestamp fits and the round-trip is exact (no
/// datetime/locale conversion), matching the Postgres `bigint` mapping.
fn ts_to_millis(ts: TimestampMillis) -> AppResult<i64> {
    i64::try_from(ts.unix_millis())
        .map_err(|_| AppError::internal("timestamp out of range for integer column"))
}

/// Convert a decoded `INTEGER` millis value back to a domain [`TimestampMillis`].
fn millis_to_ts(millis: i64) -> AppResult<TimestampMillis> {
    u64::try_from(millis)
        .map(TimestampMillis::from_unix_millis)
        .map_err(|_| AppError::internal("negative integer invalid for domain timestamp"))
}

// --- Execution model --------------------------------------------------------

/// Where a repository runs its statements: the pool (autocommit) or a shared
/// transaction cell. Mirrors `pg::PgExecutor`.
pub(crate) enum SqliteExecutor {
    /// Autocommit: each write/delete runs in its own short transaction on a
    /// pooled connection; reads run directly on a pooled connection.
    Pool(SqlitePool),
    /// Shared transaction: statements run on the unit of work's transaction and
    /// are durable only after [`SqliteUnitOfWork::commit`].
    Tx(Arc<SqliteTransactionCell>),
}

/// Holds the live transaction so `&self` repository methods can borrow it mutably
/// (via the async mutex) as sqlx requires.
pub(crate) struct SqliteTransactionCell {
    tx: Mutex<Option<Transaction<'static, Sqlite>>>,
}

impl SqliteTransactionCell {
    /// Lock the shared transaction; the guard yields `None` once committed or
    /// rolled back.
    pub(crate) async fn lock(
        &self,
    ) -> tokio::sync::MutexGuard<'_, Option<Transaction<'static, Sqlite>>> {
        self.tx.lock().await
    }
}

// --- Connect-options helpers ------------------------------------------------

/// Whether the target is an in-memory database (which cannot be shared across
/// separate connections, so the pool must hold a single connection).
fn is_memory(url: &str) -> bool {
    url.contains(":memory:") || url.contains("mode=memory")
}

/// Build the SQLite connect options from a `sqlite:` URL or a bare file path.
///
/// `create_if_missing` makes the single-file self-hosted story "just work" (the
/// file is created on first run); `busy_timeout` lets a contending writer wait
/// instead of failing immediately; WAL is enabled for file-backed databases
/// (it is not applicable to `:memory:`).
fn sqlite_options(url: &str) -> AppResult<SqliteConnectOptions> {
    let base = if url.starts_with("sqlite:") {
        url.parse::<SqliteConnectOptions>()
            .map_err(|e: sqlx::Error| {
                AppError::config("invalid database.url").with_detail(e.to_string())
            })?
    } else {
        SqliteConnectOptions::new().filename(url)
    };
    let base = base
        .create_if_missing(true)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    if is_memory(url) {
        Ok(base)
    } else {
        Ok(base.journal_mode(SqliteJournalMode::Wal))
    }
}

// --- Provider + unit of work ------------------------------------------------

/// A SQLite-backed repository provider.
///
/// Owns the connection pool, runs migrations, and hands out repositories. The
/// pool is never exposed; callers get `Arc<dyn ..Repository>` for pooled
/// (autocommit) use or a [`SqliteUnitOfWork`] for a multi-statement transaction.
pub struct SqliteDatabase {
    pool: SqlitePool,
    storage: Arc<dyn StorageRepository>,
    explorer: Arc<SqliteMetadataExplorer>,
}

impl SqliteDatabase {
    /// Connect to SQLite (opening or creating the file / in-memory database) and
    /// apply migrations.
    ///
    /// # Errors
    /// - `Config` if `config.url` is missing or unparseable.
    /// - `Database` on a connection/timeout failure or a migration failure.
    pub async fn connect(config: &DatabaseConfig) -> AppResult<Self> {
        let url = config
            .url
            .as_deref()
            .ok_or_else(|| AppError::config("database.url is required to connect to SQLite"))?;
        let options = sqlite_options(url)?;

        // Separate connections to an in-memory database do not share data, so a
        // memory-backed pool must be a single connection to behave like one DB.
        let max_connections = if is_memory(url) {
            1
        } else {
            config.max_connections
        };

        let connect = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
            .connect_with(options);
        let pool = tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .map_err(|_| AppError::database("timed out connecting to SQLite"))?
            .map_err(db_err)?;

        let db = Self {
            storage: Arc::new(SqliteStorageRepository::new(SqliteExecutor::Pool(
                pool.clone(),
            ))),
            explorer: Arc::new(SqliteMetadataExplorer::new(pool.clone())),
            pool,
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Apply all embedded SQLite migrations (idempotent).
    ///
    /// Migrations are embedded at compile time via `sqlx::migrate!`, so this needs
    /// no external tooling and no database at build time.
    ///
    /// # Errors
    /// Returns a `Database` error if a migration fails to apply.
    pub async fn migrate(&self) -> AppResult<()> {
        sqlx::migrate!("./migrations-sqlite")
            .run(&self.pool)
            .await
            .map_err(|e| {
                AppError::database("failed to apply SQLite migrations").with_detail(e.to_string())
            })?;
        Ok(())
    }

    /// A pooled (autocommit) storage repository handle.
    #[must_use]
    pub fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::clone(&self.storage)
    }

    /// Read-only metadata adapter for the administrative database explorer.
    ///
    /// The adapter exposes only logical, allowlisted metadata and remains
    /// separate from the domain repository write boundary.
    #[must_use]
    pub fn database_explorer(&self) -> Arc<SqliteMetadataExplorer> {
        Arc::clone(&self.explorer)
    }

    /// A pooled (autocommit) user repository handle.
    #[must_use]
    pub fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(SqliteUserRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) auth-identity repository handle.
    #[must_use]
    pub fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(SqliteAuthIdentityRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) session repository handle.
    #[must_use]
    pub fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(SqliteSessionRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) friends repository handle.
    #[must_use]
    pub fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(SqliteFriendsRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) groups repository handle.
    #[must_use]
    pub fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(SqliteGroupsRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) leaderboards repository handle.
    #[must_use]
    pub fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        Arc::new(SqliteLeaderboardsRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) chat repository handle.
    #[must_use]
    pub fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        Arc::new(SqliteChatRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) notifications repository handle.
    #[must_use]
    pub fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        Arc::new(SqliteNotificationsRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) wallet repository handle.
    #[must_use]
    pub fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        Arc::new(SqliteWalletRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) purchases repository handle.
    #[must_use]
    pub fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        Arc::new(SqlitePurchasesRepository::new(SqliteExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// Begin an explicit transaction scope.
    ///
    /// Repositories obtained from the returned [`SqliteUnitOfWork`] share the same
    /// database transaction; nothing is durable until [`SqliteUnitOfWork::commit`].
    ///
    /// # Errors
    /// Returns a `Database` error if the transaction cannot be started.
    pub async fn begin(&self) -> AppResult<SqliteUnitOfWork> {
        // `BEGIN IMMEDIATE` takes the writer slot at the start of the unit of
        // work, serializing multi-statement workflows against other writers the
        // way the Postgres transaction does — rather than SQLite's default
        // deferred begin, which only locks on the first write and can then fail a
        // reader-turned-writer with `SQLITE_BUSY`.
        let tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE;")
            .await
            .map_err(db_err)?;
        Ok(SqliteUnitOfWork {
            cell: Arc::new(SqliteTransactionCell {
                tx: Mutex::new(Some(tx)),
            }),
        })
    }

    /// Test-only: remove every stored row across all repository tables.
    ///
    /// Used by the un-gated SQLite contract tests to isolate scenarios. Not part
    /// of the supported API. No cross-table foreign keys are declared, so the
    /// repositories can be reset independently.
    #[doc(hidden)]
    pub async fn reset_storage_for_tests(&self) -> AppResult<()> {
        for table in [
            "storage_objects",
            "sessions",
            "auth_identities",
            "users",
            "friend_edges",
            "group_memberships",
            "groups",
            "leaderboard_records",
            "leaderboards",
            "chat_messages",
            "chat_events",
            "chat_moderation_audit",
            "chat_rate_limits",
            "chat_channels",
            "chat_access_epochs",
            "chat_delivery_outbox",
            "notifications",
            "wallet_ledger",
            "wallet_balances",
            "purchases",
        ] {
            sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&self.pool)
                .await
                .map_err(db_err)?;
        }
        Ok(())
    }
}

/// An explicit transaction scope over one or more repositories.
///
/// The concrete `sqlx::Transaction` stays private inside a shared cell; callers
/// only see `Arc<dyn ..Repository>` and typed results.
pub struct SqliteUnitOfWork {
    cell: Arc<SqliteTransactionCell>,
}

impl SqliteUnitOfWork {
    /// A storage repository bound to this transaction.
    #[must_use]
    pub fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::new(SqliteStorageRepository::new(SqliteExecutor::Tx(
            Arc::clone(&self.cell),
        )))
    }

    /// A user repository bound to this transaction.
    #[must_use]
    pub fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(SqliteUserRepository::new(SqliteExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// An auth-identity repository bound to this transaction.
    #[must_use]
    pub fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(SqliteAuthIdentityRepository::new(SqliteExecutor::Tx(
            Arc::clone(&self.cell),
        )))
    }

    /// A session repository bound to this transaction.
    #[must_use]
    pub fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(SqliteSessionRepository::new(SqliteExecutor::Tx(
            Arc::clone(&self.cell),
        )))
    }

    /// A friends repository bound to this transaction.
    #[must_use]
    pub fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(SqliteFriendsRepository::new(SqliteExecutor::Tx(
            Arc::clone(&self.cell),
        )))
    }

    /// A groups repository bound to this transaction.
    #[must_use]
    pub fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(SqliteGroupsRepository::new(SqliteExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// Commit the transaction, making its writes durable.
    ///
    /// # Errors
    /// Returns an `Internal` error if the transaction was already consumed, or a
    /// `Database` error if the commit fails.
    pub async fn commit(self) -> AppResult<()> {
        let tx = self.cell.tx.lock().await.take().ok_or_else(tx_closed)?;
        tx.commit().await.map_err(db_err)
    }

    /// Roll the transaction back, discarding its writes.
    ///
    /// # Errors
    /// Returns an `Internal` error if the transaction was already consumed, or a
    /// `Database` error if the rollback fails.
    pub async fn rollback(self) -> AppResult<()> {
        let tx = self.cell.tx.lock().await.take().ok_or_else(tx_closed)?;
        tx.rollback().await.map_err(db_err)
    }
}

#[async_trait]
impl BackendTrait for SqliteDatabase {
    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        SqliteDatabase::storage_repository(self)
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        SqliteDatabase::user_repository(self)
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        SqliteDatabase::auth_identity_repository(self)
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        SqliteDatabase::session_repository(self)
    }

    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        SqliteDatabase::friends_repository(self)
    }

    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        SqliteDatabase::groups_repository(self)
    }

    fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        SqliteDatabase::leaderboards_repository(self)
    }

    fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        SqliteDatabase::chat_repository(self)
    }

    fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        SqliteDatabase::notifications_repository(self)
    }

    fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        SqliteDatabase::wallet_repository(self)
    }

    fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        SqliteDatabase::purchases_repository(self)
    }

    fn database_explorer(&self) -> Option<Arc<dyn DatabaseExplorer>> {
        Some(Arc::clone(&self.explorer) as Arc<dyn DatabaseExplorer>)
    }

    async fn begin(&self) -> AppResult<Box<dyn UnitOfWork>> {
        let uow = SqliteDatabase::begin(self).await?;
        Ok(Box::new(uow))
    }
}

/// Redacted `Debug` for the provider (never prints the pool/connection detail).
impl std::fmt::Debug for SqliteDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteDatabase").finish_non_exhaustive()
    }
}

#[async_trait]
impl UnitOfWork for SqliteUnitOfWork {
    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        SqliteUnitOfWork::storage_repository(self)
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        SqliteUnitOfWork::user_repository(self)
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        SqliteUnitOfWork::auth_identity_repository(self)
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        SqliteUnitOfWork::session_repository(self)
    }

    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        SqliteUnitOfWork::friends_repository(self)
    }

    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        SqliteUnitOfWork::groups_repository(self)
    }

    async fn commit(self: Box<Self>) -> AppResult<()> {
        SqliteUnitOfWork::commit(*self).await
    }

    async fn rollback(self: Box<Self>) -> AppResult<()> {
        SqliteUnitOfWork::rollback(*self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_urls_are_detected() {
        assert!(is_memory("sqlite::memory:"));
        assert!(is_memory("file:memdb?mode=memory&cache=shared"));
        assert!(!is_memory("sqlite:data.sqlite"));
        assert!(!is_memory("./data.sqlite"));
    }

    #[test]
    fn sqlite_options_accepts_url_and_bare_path() {
        assert!(sqlite_options("sqlite::memory:").is_ok());
        assert!(sqlite_options("sqlite:data.sqlite").is_ok());
        // The shipped release config uses the `sqlite://` (authority-style) form.
        assert!(sqlite_options("sqlite://data.sqlite").is_ok());
        assert!(sqlite_options("./data.sqlite").is_ok());
    }

    #[test]
    fn db_err_maps_pool_closed_to_database() {
        let mapped = db_err(sqlx::Error::PoolClosed);
        assert_eq!(mapped.category(), crate::error::ErrorCategory::Database);
    }

    #[tokio::test]
    async fn connect_migrate_and_round_trip_in_memory() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate in-memory sqlite");
        // A committed unit of work is visible on the pooled repository.
        assert_eq!(BackendTrait::kind(&db), BackendKind::Sqlite);
        let tables = db
            .database_explorer()
            .list_tables()
            .await
            .expect("explorer metadata");
        assert!(tables.iter().any(|table| table.table.table == "users"));
        db.reset_storage_for_tests().await.expect("reset");
    }

    #[tokio::test]
    async fn chat_delivery_outbox_round_trips_and_acknowledges() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate in-memory sqlite");
        let repository = db.chat_repository();
        let record = crate::repository::chat::ChatDeliveryOutboxRecord {
            channel_id: "ch_delivery".to_owned(),
            event_id: 7,
            authority_epoch: 4,
            payload: r#"{"event":"created"}"#.to_owned(),
            created_at: TimestampMillis::from_unix_millis(10),
            expires_at: TimestampMillis::from_unix_millis(20),
        };

        assert!(
            repository
                .stage_delivery_outbox(record.clone())
                .await
                .expect("stage row")
        );
        assert!(
            !repository
                .stage_delivery_outbox(record.clone())
                .await
                .expect("duplicate row")
        );
        assert_eq!(
            repository
                .active_delivery_outbox(TimestampMillis::from_unix_millis(19), 10)
                .await
                .expect("active rows"),
            vec![record.clone()]
        );
        assert!(
            repository
                .acknowledge_delivery_outbox("ch_delivery", 7)
                .await
                .expect("acknowledge row")
        );
        assert!(
            repository
                .active_delivery_outbox(TimestampMillis::from_unix_millis(19), 10)
                .await
                .expect("acknowledged rows")
                .is_empty()
        );
        assert!(
            repository
                .stage_delivery_outbox(record.clone())
                .await
                .expect("stage reset row")
        );
        db.reset_storage_for_tests().await.expect("reset outbox");
        assert!(
            repository
                .active_delivery_outbox(TimestampMillis::from_unix_millis(19), 10)
                .await
                .expect("reset rows")
                .is_empty()
        );
        assert!(
            repository
                .stage_delivery_outbox(record)
                .await
                .expect("stage expired row")
        );
        assert_eq!(
            repository
                .cleanup_delivery_outbox(TimestampMillis::from_unix_millis(20), 1)
                .await
                .expect("purge expired row"),
            1
        );
    }

    #[tokio::test]
    async fn failed_delivery_staging_rolls_back_the_chat_mutation() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_owned()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("sqlite database");
        let repository = db.chat_repository();
        let now = TimestampMillis::from_unix_millis(100);
        let delivery = crate::repository::chat::ChatDeliveryRequest {
            authority_epoch: 0,
            expires_at: now,
            event_type: "message.create",
        };

        let error = repository
            .post_message_authorized_with_delivery(
                "ch_rollback",
                crate::repository::chat::ChannelType::Room,
                "alice",
                "before rollback",
                20,
                "room:rollback",
                0,
                &delivery,
                now,
            )
            .await
            .expect_err("invalid delivery window must abort the transaction");
        assert_eq!(error.category(), crate::error::ErrorCategory::Validation);
        assert!(
            repository
                .channel_history("ch_rollback", 0, None)
                .await
                .expect("history after rollback")
                .is_empty()
        );
        assert!(
            repository
                .active_delivery_outbox(TimestampMillis::from_unix_millis(101), 10)
                .await
                .expect("outbox after rollback")
                .is_empty()
        );

        let id = repository
            .post_message(
                "ch_rollback",
                crate::repository::chat::ChannelType::Room,
                "alice",
                "original",
                20,
                now,
            )
            .await
            .expect("seed message");
        assert!(
            repository
                .edit_message_authorized_with_delivery(
                    "ch_rollback",
                    crate::repository::chat::ChannelType::Room,
                    id,
                    "must not persist",
                    "room:rollback",
                    0,
                    &delivery,
                    now,
                )
                .await
                .is_err()
        );
        assert!(
            repository
                .delete_message_authorized_with_delivery(
                    "ch_rollback",
                    crate::repository::chat::ChannelType::Room,
                    id,
                    "room:rollback",
                    0,
                    &delivery,
                    now,
                )
                .await
                .is_err()
        );
        let message = repository
            .channel_history("ch_rollback", 0, None)
            .await
            .expect("history after edit/delete rollbacks")
            .into_iter()
            .find(|message| message.id == id)
            .expect("seed message retained");
        assert_eq!(message.content, "original");
        assert!(!message.deleted);
        assert_eq!(message.revision, 1);
        assert!(
            repository
                .active_delivery_outbox(TimestampMillis::from_unix_millis(101), 10)
                .await
                .expect("outbox after edit/delete rollbacks")
                .is_empty()
        );
    }
}
