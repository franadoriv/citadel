//! PostgreSQL persistence backend (, ).
//!
//! This module holds Citadel's durable repository implementations and the
//! transaction machinery they share. It follows
//! `website/src/content/docs/guides/choose-a-database.mdx`: every Postgres-specific choice
//! stays behind this module. Nothing here leaks a `sqlx::PgPool` or
//! `sqlx::Transaction` across a repository contract — callers only ever see
//! `Arc<dyn StorageRepository>` / `Arc<dyn UserRepository>` /
//! `Arc<dyn AuthIdentityRepository>` / `Arc<dyn SessionRepository>`,
//! [`PgUnitOfWork`], and typed [`AppError`](crate::error::AppError)s.
//!
//! Layout:
//!
//! - [`mod@storage`]: the Postgres [`StorageRepository`](
//!   crate::repository::StorageRepository).
//! - [`mod@identity`]: the Postgres [`UserRepository`](
//!   crate::repository::UserRepository) and [`AuthIdentityRepository`](
//!   crate::repository::AuthIdentityRepository).
//! - [`mod@session`]: the Postgres [`SessionRepository`](
//!   crate::repository::SessionRepository).
//!
//! All four repositories share one execution model ([`PgExecutor`]): a repository
//! is bound either to the connection pool (autocommit) or to a single shared
//! transaction cell. A workflow such as authenticate-or-create-user (create the
//! user, then link its auth identity) is driven through one [`PgUnitOfWork`], so
//! all of its statements commit or roll back atomically as a single database
//! transaction — never an application-level mutex.
//!
//! Queries are **runtime-checked** ([`sqlx::query`] with `try_get` decoding),
//! never the compile-time `query!` macro, so `cargo build` and
//! `scripts/check.sh` never require a live database. The Postgres integration
//! path is exercised by the gated tests in `tests/storage_repository_contract.rs`
//! and `tests/identity_session_reference_impls.rs` (opt-in via `DATABASE_URL`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::{Postgres, Row, Transaction};
use tokio::sync::Mutex;

use crate::config::{DatabaseConfig, PgFlavor};
use crate::database_explorer::{DatabaseExplorer, PgMetadataExplorer};
use crate::error::{AppError, AppResult};
use crate::repository::backend::{Backend as BackendTrait, BackendKind, UnitOfWork};
use crate::repository::{
    AuthIdentityRepository, ChatRepository, FriendsRepository, GameScriptRepository,
    GroupsRepository, LeaderboardsRepository, NotificationsRepository, PurchasesRepository,
    SessionRepository, StorageRepository, TournamentsRepository, UserRepository, WalletRepository,
};
use crate::time::TimestampMillis;

mod chat;
mod friends;
mod gamescript;
mod groups;
mod identity;
mod leaderboard_scheduler;
mod leaderboards;
mod notifications;
mod purchases;
mod session;
mod storage;
mod tournaments;
mod wallet;

pub use chat::PgChatRepository;
pub use friends::PgFriendsRepository;
pub use gamescript::PgGameScriptRepository;
pub use groups::PgGroupsRepository;
pub use identity::{PgAuthIdentityRepository, PgUserRepository};
pub use leaderboard_scheduler::PgLeaderboardResetRepository;
pub use leaderboards::PgLeaderboardsRepository;
pub use notifications::PgNotificationsRepository;
pub use purchases::PgPurchasesRepository;
pub use session::PgSessionRepository;
pub use storage::PgStorageRepository;
pub use tournaments::PgTournamentsRepository;
pub use wallet::PgWalletRepository;

// --- Shared error / decode helpers ------------------------------------------

/// Map a raw `sqlx::Error` to a typed [`AppError`].
///
/// A unique-violation (`23505`) means a row was created concurrently, which is a
/// [`Conflict`](crate::error::ErrorCategory::Conflict). Every other backend
/// failure (pool timeout, connection loss, protocol, check violations) maps to
/// [`Database`](crate::error::ErrorCategory::Database) with sanitized detail —
/// never `Permission`, `Conflict`, or `Auth`. The detail carries the raw sqlx
/// message but never a credential value, so mapping a unique violation cannot
/// become a credential-existence oracle: callers only learn "a conflicting row
/// exists", never which field or value collided.
fn db_err(error: sqlx::Error) -> AppError {
    if let sqlx::Error::Database(db) = &error
        && db.code().as_deref() == Some("23505")
    {
        return AppError::conflict("resource already exists");
    }
    AppError::database("database backend error").with_detail(error.to_string())
}

fn tx_closed() -> AppError {
    AppError::internal("database transaction is already closed")
}

/// Fetch a typed column, mapping decode failures to an internal error.
fn get<'r, T>(row: &'r PgRow, column: &str) -> AppResult<T>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<T, _>(column).map_err(|e| {
        AppError::internal(format!("failed to decode column `{column}`")).with_detail(e.to_string())
    })
}

// --- Domain <-> bigint millis conversion ------------------------------------

/// Convert a domain [`TimestampMillis`] to the `bigint` value stored in the
/// database.
///
/// Domain time is Unix epoch milliseconds (`u64`); Postgres `bigint` is a signed
/// 64-bit integer. Any realistic timestamp fits, and the round-trip is exact (no
/// datetime/locale conversion), so no `chrono`/`time` sqlx feature is required.
fn ts_to_millis(ts: TimestampMillis) -> AppResult<i64> {
    i64::try_from(ts.unix_millis())
        .map_err(|_| AppError::internal("timestamp out of range for bigint column"))
}

/// Convert a decoded `bigint` millis value back to a domain [`TimestampMillis`].
fn millis_to_ts(millis: i64) -> AppResult<TimestampMillis> {
    u64::try_from(millis)
        .map(TimestampMillis::from_unix_millis)
        .map_err(|_| AppError::internal("negative bigint invalid for domain timestamp"))
}

// --- Execution model --------------------------------------------------------

/// Where a repository runs its statements: the pool (autocommit) or a shared
/// transaction cell.
///
/// This is the single execution seam every Postgres repository is built on. A
/// repository obtained from [`PgDatabase`] holds [`PgExecutor::Pool`]; one
/// obtained from a [`PgUnitOfWork`] holds [`PgExecutor::Tx`] pointing at the same
/// live transaction, so multiple repositories can cooperate in one atomic scope.
pub(crate) enum PgExecutor {
    /// Autocommit: each statement (or the repository's own short transaction)
    /// runs on a pooled connection.
    Pool(PgPool),
    /// Shared transaction: statements run on the unit of work's transaction and
    /// are durable only after [`PgUnitOfWork::commit`].
    Tx(Arc<PgTransactionCell>),
}

/// Holds the live transaction so `&self` repository methods can borrow it
/// mutably (via the async mutex) as sqlx requires.
pub(crate) struct PgTransactionCell {
    tx: Mutex<Option<Transaction<'static, Postgres>>>,
}

impl PgTransactionCell {
    /// Lock the shared transaction; the guard yields `None` once committed or
    /// rolled back. Repository statement bodies call `guard.as_mut` and map a
    /// `None` to [`tx_closed`].
    pub(crate) async fn lock(
        &self,
    ) -> tokio::sync::MutexGuard<'_, Option<Transaction<'static, Postgres>>> {
        self.tx.lock().await
    }
}

// --- Provider + unit of work ------------------------------------------------

/// A Postgres-backed repository provider.
///
/// Owns the connection pool, runs migrations, and hands out repositories. The
/// pool is never exposed; callers get `Arc<dyn ..Repository>` for pooled
/// (autocommit) use or a [`PgUnitOfWork`] for a multi-statement transaction.
pub struct PgDatabase {
    pool: PgPool,
    flavor: PgFlavor,
    storage: Arc<dyn StorageRepository>,
    explorer: Arc<PgMetadataExplorer>,
}

/// Repository tables cleared by the database-backed contract suites.
///
/// Keep this list in dependency-safe delete order. The CockroachDB migration
/// coverage test below makes a new entry fail fast unless its table is also
/// present in the CRDB migration set.
const TEST_RESET_TABLES: &[&str] = &[
    "gamescript_outbox",
    "gamescript_audit",
    "gamescript_activations",
    "gamescript_activation_generations",
    "gamescript_revision_diagnostics",
    "gamescript_revision_pins",
    "gamescript_revisions",
    "gamescript_drafts",
    "tournament_results",
    "tournament_entries",
    "tournament_settlement_outbox",
    "tournaments",
    "leaderboard_reset_snapshot_records",
    "leaderboard_reset_outbox",
    "leaderboard_reset_epochs",
    "leaderboard_reset_scheduler_lease",
    "storage_index_memberships",
    "storage_index_definitions",
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
];

/// Rewrite a `cockroach://`/`cockroachdb://` URL scheme to `postgres://` so
/// `sqlx`'s [`PgConnectOptions`] parser (which only knows the `postgres`/
/// `postgresql` schemes) accepts a CockroachDB connection string unchanged apart
/// from its scheme. Any other URL is returned as-is. The rest of the string
/// (host, port, credentials, query) is preserved exactly.
fn normalize_pg_scheme(url: &str) -> std::borrow::Cow<'_, str> {
    for prefix in ["cockroachdb://", "cockroach://"] {
        if let Some(rest) = url.strip_prefix(prefix) {
            return std::borrow::Cow::Owned(format!("postgres://{rest}"));
        }
    }
    std::borrow::Cow::Borrowed(url)
}

impl PgDatabase {
    /// Connect to PostgreSQL (or a CockroachDB cluster over the PostgreSQL wire
    /// protocol) and apply migrations.
    ///
    /// The dialect flavor is taken from the URL scheme
    /// ([`DatabaseConfig::pg_flavor`]): a `cockroach://`/`cockroachdb://` URL
    /// selects the CockroachDB flavor, which uses the `migrations-crdb/` DDL and
    /// disables the PostgreSQL-only advisory locks. Every other Postgres-backend
    /// URL uses standard PostgreSQL.
    ///
    /// # Errors
    /// - `Config` if `config.url` is missing or unparseable.
    /// - `Database` on a connection/timeout failure or a migration failure.
    pub async fn connect(config: &DatabaseConfig) -> AppResult<Self> {
        let url = config
            .url
            .as_deref()
            .ok_or_else(|| AppError::config("database.url is required to connect to Postgres"))?;
        let flavor = config.pg_flavor();
        let normalized = normalize_pg_scheme(url);
        let options: PgConnectOptions = normalized.parse().map_err(|e: sqlx::Error| {
            AppError::config("invalid database.url").with_detail(e.to_string())
        })?;

        let connect = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
            .connect_with(options);
        let pool = tokio::time::timeout(Duration::from_millis(config.connect_timeout_ms), connect)
            .await
            .map_err(|_| AppError::database("timed out connecting to Postgres"))?
            .map_err(db_err)?;

        let db = Self {
            storage: Arc::new(PgStorageRepository::new(
                PgExecutor::Pool(pool.clone()),
                flavor,
            )),
            explorer: Arc::new(PgMetadataExplorer::new(pool.clone())),
            flavor,
            pool,
        };
        db.migrate().await?;
        Ok(db)
    }

    /// Apply all embedded migrations (idempotent).
    ///
    /// Migrations are embedded at compile time via `sqlx::migrate!`, so this
    /// needs no external tooling and no `DATABASE_URL` at build time. The
    /// migration set is chosen by [`flavor`](PgDatabase::flavor): standard
    /// PostgreSQL uses `migrations/`; CockroachDB uses the `migrations-crdb/` DDL
    /// (no `COLLATE "C"`) and disables SQLx's advisory-lock-based migration
    /// serialization, which CockroachDB does not implement.
    ///
    /// # Errors
    /// Returns a `Database` error if a migration fails to apply.
    pub async fn migrate(&self) -> AppResult<()> {
        let result = match self.flavor {
            PgFlavor::Postgres => sqlx::migrate!("./migrations").run(&self.pool).await,
            PgFlavor::Cockroach => {
                let mut last_error = None;
                for attempt in 0..5 {
                    let result = {
                        let mut migrator = sqlx::migrate!("./migrations-crdb");
                        // CockroachDB does not implement the advisory locks SQLx uses to
                        // serialize concurrent migrators, so disable locking. A single
                        // node applies migrations once at startup, so this is safe.
                        migrator.set_locking(false);
                        migrator.run(&self.pool).await
                    };
                    match result {
                        Ok(()) => return Ok(()),
                        Err(error)
                            if error.to_string().contains("being backfilled") && attempt < 4 =>
                        {
                            last_error = Some(error);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(error) => {
                            return Err(AppError::database("failed to apply database migrations")
                                .with_detail(error.to_string()));
                        }
                    }
                }
                return Err(
                    AppError::database("failed to apply database migrations").with_detail(
                        last_error
                            .map(|error| error.to_string())
                            .unwrap_or_else(|| "Cockroach migration retry exhausted".to_string()),
                    ),
                );
            }
        };
        result.map_err(|e| {
            AppError::database("failed to apply database migrations").with_detail(e.to_string())
        })?;
        Ok(())
    }

    /// Read-only metadata adapter for the administrative database explorer.
    ///
    /// It starts from portable `information_schema` metadata, so the same
    /// adapter type is usable for PostgreSQL and CockroachDB. Capability gaps
    /// remain the adapter's responsibility and are never filled from private
    /// PostgreSQL catalogs by default.
    #[must_use]
    pub fn database_explorer(&self) -> Arc<PgMetadataExplorer> {
        Arc::clone(&self.explorer)
    }

    /// A pooled (autocommit) storage repository handle.
    #[must_use]
    pub fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::clone(&self.storage)
    }

    /// A pooled (autocommit) user repository handle.
    #[must_use]
    pub fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(PgUserRepository::new(PgExecutor::Pool(self.pool.clone())))
    }

    /// A pooled (autocommit) auth-identity repository handle.
    #[must_use]
    pub fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(PgAuthIdentityRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) session repository handle.
    #[must_use]
    pub fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(PgSessionRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) friends repository handle.
    #[must_use]
    pub fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(PgFriendsRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) groups repository handle.
    #[must_use]
    pub fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(PgGroupsRepository::new(PgExecutor::Pool(self.pool.clone())))
    }

    /// A pooled (autocommit) leaderboards repository handle.
    #[must_use]
    pub fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        Arc::new(PgLeaderboardsRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled durable tournament repository.
    #[must_use]
    pub fn tournaments_repository(&self) -> Arc<dyn TournamentsRepository> {
        Arc::new(PgTournamentsRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled durable repository for leaderboard reset scheduler state.
    #[must_use]
    pub fn leaderboard_reset_repository(
        &self,
    ) -> Arc<dyn crate::leaderboard_scheduler::LeaderboardResetRepository> {
        Arc::new(PgLeaderboardResetRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) chat repository handle.
    #[must_use]
    pub fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        Arc::new(PgChatRepository::new(PgExecutor::Pool(self.pool.clone())))
    }

    /// A pooled (autocommit) notifications repository handle.
    #[must_use]
    pub fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        Arc::new(PgNotificationsRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) wallet repository handle.
    #[must_use]
    pub fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        Arc::new(PgWalletRepository::new(
            PgExecutor::Pool(self.pool.clone()),
            self.flavor,
        ))
    }

    /// A pooled durable GameScript revision repository.
    #[must_use]
    pub fn gamescript_repository(&self) -> Arc<dyn GameScriptRepository> {
        Arc::new(PgGameScriptRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// A pooled (autocommit) purchases repository handle.
    #[must_use]
    pub fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        Arc::new(PgPurchasesRepository::new(PgExecutor::Pool(
            self.pool.clone(),
        )))
    }

    /// Begin an explicit transaction scope.
    ///
    /// Repositories obtained from the returned [`PgUnitOfWork`] share the same
    /// database transaction; nothing is durable until [`PgUnitOfWork::commit`].
    ///
    /// # Errors
    /// Returns a `Database` error if the transaction cannot be started.
    pub async fn begin(&self) -> AppResult<PgUnitOfWork> {
        let tx = self.pool.begin().await.map_err(db_err)?;
        Ok(PgUnitOfWork {
            cell: Arc::new(PgTransactionCell {
                tx: Mutex::new(Some(tx)),
            }),
            flavor: self.flavor,
        })
    }

    /// Test-only: remove every stored row across all repository tables.
    ///
    /// Used by the gated Postgres contract tests to isolate scenarios. Not part
    /// of the supported API.
    #[doc(hidden)]
    pub async fn reset_storage_for_tests(&self) -> AppResult<()> {
        // `TEST_RESET_TABLES` is kept in dependency-safe delete order (children
        // before the tables they reference, e.g. gamescript_activations before
        // gamescript_revisions). `DELETE FROM` (plain DML) is
        // used rather than `TRUNCATE` because on CockroachDB `TRUNCATE` is an
        // asynchronous schema change (it drops and recreates the table's indexes);
        // calling it repeatedly between contract scenarios races the pending index
        // jobs and fails with "cannot perform TRUNCATE ... which has indexes being
        // dropped". `DELETE FROM` is portable across PostgreSQL and CockroachDB and
        // needs no sequence reset (ids are opaque domain strings, not serials).
        for table in TEST_RESET_TABLES {
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
/// only see `Arc<dyn ..Repository>` and typed results. Repositories obtained here
/// share the transaction, so a create-user-then-link-identity workflow is one
/// atomic unit.
pub struct PgUnitOfWork {
    cell: Arc<PgTransactionCell>,
    flavor: PgFlavor,
}

impl PgUnitOfWork {
    /// A storage repository bound to this transaction.
    #[must_use]
    pub fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::new(PgStorageRepository::new(
            PgExecutor::Tx(Arc::clone(&self.cell)),
            self.flavor,
        ))
    }

    /// A user repository bound to this transaction.
    #[must_use]
    pub fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(PgUserRepository::new(PgExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// An auth-identity repository bound to this transaction.
    #[must_use]
    pub fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(PgAuthIdentityRepository::new(PgExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// A session repository bound to this transaction.
    #[must_use]
    pub fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(PgSessionRepository::new(PgExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// A friends repository bound to this transaction.
    #[must_use]
    pub fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(PgFriendsRepository::new(PgExecutor::Tx(Arc::clone(
            &self.cell,
        ))))
    }

    /// A groups repository bound to this transaction.
    #[must_use]
    pub fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(PgGroupsRepository::new(PgExecutor::Tx(Arc::clone(
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
impl BackendTrait for PgDatabase {
    fn kind(&self) -> BackendKind {
        match self.flavor {
            PgFlavor::Postgres => BackendKind::Postgres,
            PgFlavor::Cockroach => BackendKind::Cockroach,
        }
    }

    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        PgDatabase::storage_repository(self)
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        PgDatabase::user_repository(self)
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        PgDatabase::auth_identity_repository(self)
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        PgDatabase::session_repository(self)
    }

    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        PgDatabase::friends_repository(self)
    }

    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        PgDatabase::groups_repository(self)
    }

    fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        PgDatabase::leaderboards_repository(self)
    }

    fn leaderboard_reset_repository(
        &self,
    ) -> Arc<dyn crate::leaderboard_scheduler::LeaderboardResetRepository> {
        PgDatabase::leaderboard_reset_repository(self)
    }

    fn tournaments_repository(&self) -> Arc<dyn TournamentsRepository> {
        PgDatabase::tournaments_repository(self)
    }

    fn gamescript_repository(&self) -> Arc<dyn GameScriptRepository> {
        PgDatabase::gamescript_repository(self)
    }

    fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        PgDatabase::chat_repository(self)
    }

    fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        PgDatabase::notifications_repository(self)
    }

    fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        PgDatabase::wallet_repository(self)
    }

    fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        PgDatabase::purchases_repository(self)
    }

    fn database_explorer(&self) -> Option<Arc<dyn DatabaseExplorer>> {
        Some(Arc::clone(&self.explorer) as Arc<dyn DatabaseExplorer>)
    }

    async fn begin(&self) -> AppResult<Box<dyn UnitOfWork>> {
        let uow = PgDatabase::begin(self).await?;
        Ok(Box::new(uow))
    }
}

/// Redacted `Debug` for the provider.
///
/// Never prints the pool configuration or connection string — the URL can carry
/// credentials. `Backend` requires `Debug`, and this keeps that requirement from
/// leaking connection detail into any log or diagnostic.
impl std::fmt::Debug for PgDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDatabase").finish_non_exhaustive()
    }
}

#[async_trait]
impl UnitOfWork for PgUnitOfWork {
    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        PgUnitOfWork::storage_repository(self)
    }

    fn user_repository(&self) -> Arc<dyn UserRepository> {
        PgUnitOfWork::user_repository(self)
    }

    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        PgUnitOfWork::auth_identity_repository(self)
    }

    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        PgUnitOfWork::session_repository(self)
    }

    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        PgUnitOfWork::friends_repository(self)
    }

    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        PgUnitOfWork::groups_repository(self)
    }

    async fn commit(self: Box<Self>) -> AppResult<()> {
        PgUnitOfWork::commit(*self).await
    }

    async fn rollback(self: Box<Self>) -> AppResult<()> {
        PgUnitOfWork::rollback(*self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CRDB_STORAGE: &str =
        include_str!("../../../migrations-crdb/20260702090000_create_storage_objects.sql");
    const CRDB_IDENTITY_SESSION: &str =
        include_str!("../../../migrations-crdb/20260702100000_create_identity_session.sql");
    const CRDB_FRIENDS: &str =
        include_str!("../../../migrations-crdb/20260709120000_create_friend_edges.sql");
    const CRDB_GROUPS: &str =
        include_str!("../../../migrations-crdb/20260709130000_create_groups.sql");
    const CRDB_LEADERBOARDS: &str =
        include_str!("../../../migrations-crdb/20260709140000_create_leaderboards.sql");
    const CRDB_CHAT: &str =
        include_str!("../../../migrations-crdb/20260709150000_create_chat_messages.sql");
    const CRDB_CHAT_CHANNELS: &str =
        include_str!("../../../migrations-crdb/20260715100000_create_chat_channels.sql");
    const CRDB_CHAT_ACCESS_EPOCHS: &str =
        include_str!("../../../migrations-crdb/20260715110000_create_chat_access_epochs.sql");
    const CRDB_CHAT_EVENTS: &str = include_str!(
        "../../../migrations-crdb/20260715120000_chat_message_revisions_and_events.sql"
    );
    const CRDB_CHAT_DELIVERY_OUTBOX: &str =
        include_str!("../../../migrations-crdb/20260726130000_create_chat_delivery_outbox.sql");
    const CRDB_STORAGE_INDEX_MEMBERSHIPS: &str = include_str!(
        "../../../migrations-crdb/20260713150000_create_storage_index_memberships.sql"
    );
    const CRDB_NOTIFICATIONS: &str =
        include_str!("../../../migrations-crdb/20260709160000_create_notifications.sql");
    const CRDB_WALLET: &str =
        include_str!("../../../migrations-crdb/20260709170000_create_wallet.sql");
    const CRDB_TOURNAMENTS: &str =
        include_str!("../../../migrations-crdb/20260803150000_create_tournaments.sql");
    const CRDB_TOURNAMENT_SETTLEMENT_OUTBOX: &str = include_str!(
        "../../../migrations-crdb/20260803151000_create_tournament_settlement_outbox.sql"
    );
    const CRDB_LEADERBOARD_RESET_SCHEDULER: &str = include_str!(
        "../../../migrations-crdb/20260803141000_create_leaderboard_reset_scheduler.sql"
    );
    const CRDB_LEADERBOARD_RESET_SNAPSHOTS: &str =
        include_str!("../../../migrations-crdb/20260803142000_add_leaderboard_reset_snapshots.sql");
    const CRDB_GAMESCRIPT: &str =
        include_str!("../../../migrations-crdb/20260805100000_create_gamescript_revisions.sql");

    #[test]
    fn timestamp_round_trips_through_millis() {
        let ts = TimestampMillis::from_unix_millis(1_700_000_000_123);
        let millis = ts_to_millis(ts).expect("to millis");
        assert_eq!(millis_to_ts(millis).expect("from millis"), ts);
    }

    #[test]
    fn timestamp_zero_round_trips() {
        let ts = TimestampMillis::from_unix_millis(0);
        let millis = ts_to_millis(ts).expect("to millis");
        assert_eq!(millis_to_ts(millis).expect("from millis"), ts);
    }

    #[test]
    fn negative_millis_is_rejected() {
        assert!(millis_to_ts(-1).is_err());
    }

    #[test]
    fn db_err_maps_pool_closed_to_database() {
        let mapped = db_err(sqlx::Error::PoolClosed);
        assert_eq!(mapped.category(), crate::error::ErrorCategory::Database);
    }

    #[test]
    fn cockroach_migrations_cover_every_contract_reset_table() {
        let coverage = [
            ("gamescript_outbox", CRDB_GAMESCRIPT),
            ("gamescript_audit", CRDB_GAMESCRIPT),
            ("gamescript_activations", CRDB_GAMESCRIPT),
            ("gamescript_activation_generations", CRDB_GAMESCRIPT),
            ("gamescript_revision_diagnostics", CRDB_GAMESCRIPT),
            ("gamescript_revision_pins", CRDB_GAMESCRIPT),
            ("gamescript_revisions", CRDB_GAMESCRIPT),
            ("gamescript_drafts", CRDB_GAMESCRIPT),
            ("tournament_results", CRDB_TOURNAMENTS),
            ("tournament_entries", CRDB_TOURNAMENTS),
            (
                "tournament_settlement_outbox",
                CRDB_TOURNAMENT_SETTLEMENT_OUTBOX,
            ),
            ("tournaments", CRDB_TOURNAMENTS),
            (
                "leaderboard_reset_snapshot_records",
                CRDB_LEADERBOARD_RESET_SNAPSHOTS,
            ),
            ("leaderboard_reset_outbox", CRDB_LEADERBOARD_RESET_SCHEDULER),
            ("leaderboard_reset_epochs", CRDB_LEADERBOARD_RESET_SCHEDULER),
            (
                "leaderboard_reset_scheduler_lease",
                CRDB_LEADERBOARD_RESET_SCHEDULER,
            ),
            ("storage_index_memberships", CRDB_STORAGE_INDEX_MEMBERSHIPS),
            ("storage_index_definitions", CRDB_STORAGE_INDEX_MEMBERSHIPS),
            ("storage_objects", CRDB_STORAGE),
            ("sessions", CRDB_IDENTITY_SESSION),
            ("auth_identities", CRDB_IDENTITY_SESSION),
            ("users", CRDB_IDENTITY_SESSION),
            ("friend_edges", CRDB_FRIENDS),
            ("group_memberships", CRDB_GROUPS),
            ("groups", CRDB_GROUPS),
            ("leaderboard_records", CRDB_LEADERBOARDS),
            ("leaderboards", CRDB_LEADERBOARDS),
            ("chat_messages", CRDB_CHAT),
            ("chat_events", CRDB_CHAT_EVENTS),
            ("chat_moderation_audit", CRDB_CHAT_EVENTS),
            ("chat_rate_limits", CRDB_CHAT_EVENTS),
            ("chat_channels", CRDB_CHAT_CHANNELS),
            ("chat_access_epochs", CRDB_CHAT_ACCESS_EPOCHS),
            ("chat_delivery_outbox", CRDB_CHAT_DELIVERY_OUTBOX),
            ("notifications", CRDB_NOTIFICATIONS),
            ("wallet_ledger", CRDB_WALLET),
            ("wallet_balances", CRDB_WALLET),
            ("purchases", CRDB_WALLET),
        ];

        for table in TEST_RESET_TABLES {
            let migration = coverage
                .iter()
                .find_map(|(covered_table, ddl)| (*covered_table == *table).then_some(*ddl));
            assert!(
                migration.is_some(),
                "{table} has no CockroachDB migration coverage"
            );
            if let Some(migration) = migration {
                assert!(
                    migration.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
                    "CockroachDB migration does not create reset table {table}"
                );
            }
        }
    }

    #[test]
    fn cockroach_migration_versions_match_postgres() {
        fn migration_versions(directory: &str) -> Vec<String> {
            let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(directory);
            let mut versions = std::fs::read_dir(directory)
                .expect("read migration directory")
                .map(|entry| entry.expect("read migration entry"))
                .filter_map(|entry| {
                    entry
                        .file_type()
                        .expect("read migration entry type")
                        .is_file()
                        .then(|| entry.file_name().to_string_lossy().into_owned())
                })
                .collect::<Vec<_>>();
            versions.sort();
            versions
        }

        assert_eq!(
            migration_versions("migrations-crdb"),
            migration_versions("migrations"),
            "CockroachDB must have a migration for every PostgreSQL schema version"
        );
    }

    #[test]
    fn cockroach_chat_delivery_outbox_migration_is_idempotent_and_indexed() {
        assert!(
            CRDB_CHAT_DELIVERY_OUTBOX.contains("CREATE TABLE IF NOT EXISTS chat_delivery_outbox")
        );
        assert!(CRDB_CHAT_DELIVERY_OUTBOX.contains("UNIQUE (channel_id, event_id)"));
        assert!(CRDB_CHAT_DELIVERY_OUTBOX.contains("authority_epoch INT8 NOT NULL"));
        assert!(CRDB_CHAT_DELIVERY_OUTBOX.contains("chat_delivery_outbox_expiry_idx"));
    }
}
