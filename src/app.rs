//! Application assembly and bootstrap for Citadel.
//!
//! Bootstrap scope: this module defines the assembled [`App`]
//! value that holds resolved configuration and identifying metadata, plus
//! aggregate health reporting. It deliberately does not start listeners, spawn
//! tasks, or initialize observability; that wiring belongs to
//! (observability) and  (server bootstrap). Keeping assembly in the
//! library keeps the binary thin.

use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::config::{Config, RuntimeConfig};
use crate::deferred_storage::DeferredStorageWriter;
use crate::error::{AppError, AppResult};
use crate::error_journal::ErrorJournal;
use crate::error_reporting;
use crate::host_telemetry::{HostTelemetryService, HostTelemetrySnapshot};
use crate::observability::NodeMetrics;
use crate::repository::{Backend, BackendKind, InMemoryBackend, select_backend};
use crate::services::{
    AuditLog, AuthenticationRateLimitPolicy, AuthenticationService, AuthenticationServiceImpl,
    ChatAccessCoordinator, ChatRateLimitPolicy, ChatService, ConsoleTokenStore,
    DatabaseExplorerRateLimiter, FriendsService, GroupsService, Health, InMemorySessionService,
    LeaderboardService, NotificationService, PlayerNotificationService, PurchaseService,
    SharedSessionService, WalletService,
};
use crate::time::{Clock, SystemClock};

/// Crate version, surfaced for `--version` and health responses.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The assembled Citadel application.
///
/// Holds resolved configuration, node identity, a process start instant for
/// uptime, shared [`NodeMetrics`] and host-resource telemetry surfaced by the
/// dashboard, the selected persistence [`Backend`], and the composed
/// identity/session services.
/// It is cheap to [`Clone`] (config is small; metrics, backend, and services are
/// shared behind an `Arc`), so it can serve as axum shared state without
/// wrapping.
///
/// The identity/session services are built once at assembly and shared for the
/// life of the node so the reference session-token index (kept in-process by the
/// session service) persists across requests. The [`AuthenticationService`] and
/// [`SessionService`] trait objects are not [`Debug`], so [`App`] implements
/// `Debug` by hand, omitting them.
#[derive(Clone)]
pub struct App {
    config: Config,
    started_at: Instant,
    metrics: Arc<NodeMetrics>,
    host_telemetry: Arc<HostTelemetryService>,
    backend: Arc<dyn Backend>,
    auth: Arc<dyn AuthenticationService>,
    sessions: SharedSessionService,
    console_tokens: Arc<ConsoleTokenStore>,
    audit: Arc<AuditLog>,
    error_journal: Arc<ErrorJournal>,
    chat_access: Arc<ChatAccessCoordinator>,
    groups: Arc<GroupsService>,
    chat: Arc<ChatService>,
    chat_rate_limits: Arc<ChatRateLimitPolicy>,
    auth_rate_limits: Arc<AuthenticationRateLimitPolicy>,
    auth_clock: Arc<dyn Clock + Send + Sync>,
    database_explorer_rate_limiter: Arc<DatabaseExplorerRateLimiter>,
    runtime_http_endpoint_rate_limiter: Arc<crate::runtime::RuntimeHttpEndpointRateLimiter>,
    runtime_event_bus: Arc<crate::runtime::RuntimeEventBus>,
    runtime_shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
    notifications: Arc<NotificationService>,
    player_notifications: Arc<PlayerNotificationService>,
    leaderboards: Arc<LeaderboardService>,
    purchases: Arc<PurchaseService>,
    realtime: Arc<OnceLock<Arc<crate::realtime::Gateway>>>,
    wallet: Arc<WalletService>,
    friends: Arc<FriendsService>,
    deferred_storage: Option<Arc<DeferredStorageWriter>>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("node_id", &self.config.server.node_id)
            .field("backend", &self.backend.kind())
            .finish_non_exhaustive()
    }
}

impl App {
    /// Assemble an application from resolved configuration on the in-memory
    /// backend.
    ///
    /// This is the synchronous constructor used by tests and any path that does
    /// not need durable persistence. The running node uses
    /// [`App::bootstrap`], which selects the backend from `[database]`.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_backend(config, Arc::new(InMemoryBackend::new()))
    }

    /// Assemble an application over an explicit backend.
    ///
    /// Composes the identity/session services over the backend once here so the
    /// session service (and its in-process reference token index) is shared for
    /// the life of the node rather than rebuilt per request.
    #[must_use]
    pub fn with_backend(config: Config, backend: Arc<dyn Backend>) -> Self {
        // Construct only when explicitly enabled. This keeps the default
        // in-memory application usable while preventing an enabled volatile
        // writer from being mistaken for a durable in-memory backend.
        let deferred_storage = if config.storage.deferred.enabled {
            Some(
                DeferredStorageWriter::new(
                    config.storage.deferred.clone(),
                    backend.storage_repository(),
                    backend.kind(),
                )
                .expect("validated deferred storage configuration and durable backend"),
            )
        } else {
            None
        };
        let metrics = Arc::new(NodeMetrics::new());
        let sessions: SharedSessionService = Arc::new(InMemorySessionService::with_secure_issuer(
            backend.session_repository(),
        ));
        let auth: Arc<dyn AuthenticationService> = Arc::new(AuthenticationServiceImpl::new(
            Arc::clone(&backend),
            Arc::clone(&sessions),
        ));
        let console_tokens = Arc::new(ConsoleTokenStore::from_config(&config.console));
        let error_journal = error_reporting::journal_for_config(&config.errors);
        let chat = Arc::new(ChatService::new(backend.chat_repository()));
        let chat_rate_limits = Arc::new(ChatRateLimitPolicy::new(config.chat.limits.clone()));
        let auth_rate_limits = Arc::new(AuthenticationRateLimitPolicy::new(
            config.authentication.limits.clone(),
        ));
        let database_explorer_rate_limiter = Arc::new(DatabaseExplorerRateLimiter::default());
        let runtime_http_endpoint_rate_limiter =
            Arc::new(crate::runtime::RuntimeHttpEndpointRateLimiter::default());
        let runtime_event_bus = Arc::new(crate::runtime::RuntimeEventBus::new(
            crate::runtime::RuntimeEventPolicy::from(&config.runtime.capabilities.events),
            Arc::clone(&metrics),
        ));
        let runtime_shared_cache = Arc::new(crate::runtime::RuntimeSharedCache::new(
            crate::runtime::RuntimeSharedCachePolicy::from(
                &config.runtime.capabilities.shared_cache,
            ),
            Arc::clone(&metrics),
        ));
        let chat_access = Arc::new(ChatAccessCoordinator::with_repository(
            backend.chat_repository(),
        ));
        let friends = Arc::new(
            FriendsService::new(backend.friends_repository())
                .with_chat_access_coordinator(Arc::clone(&chat_access)),
        );
        let groups = Arc::new(
            GroupsService::new(backend.groups_repository())
                .with_chat_access_coordinator(Arc::clone(&chat_access)),
        );
        let leaderboards = Arc::new(LeaderboardService::new(backend.leaderboards_repository()));
        let notifications = Arc::new(NotificationService::new(backend.notifications_repository()));
        let player_notifications = Arc::new(PlayerNotificationService::new(Arc::clone(&backend)));
        let wallet = Arc::new(WalletService::new(backend.wallet_repository()));
        let purchases = Arc::new(PurchaseService::new(backend.purchases_repository()));
        Self {
            config,
            started_at: Instant::now(),
            metrics,
            host_telemetry: Arc::new(HostTelemetryService::new()),
            backend,
            auth,
            sessions,
            console_tokens,
            audit: Arc::new(AuditLog::default()),
            error_journal,
            chat_access,
            groups,
            chat,
            chat_rate_limits,
            auth_rate_limits,
            auth_clock: Arc::new(SystemClock),
            database_explorer_rate_limiter,
            runtime_http_endpoint_rate_limiter,
            runtime_event_bus,
            runtime_shared_cache,
            notifications,
            player_notifications,
            leaderboards,
            purchases,
            realtime: Arc::new(OnceLock::new()),
            wallet,
            friends,
            deferred_storage,
        }
    }

    /// Replace the local incident journal used by this application instance.
    ///
    /// The normal server bootstrap installs the process journal derived from
    /// the executable directory. Embedders and isolated tests can supply a
    /// separate journal without changing the process-wide default.
    #[must_use]
    pub fn with_error_journal(mut self, error_journal: Arc<ErrorJournal>) -> Self {
        self.error_journal = error_journal;
        self
    }

    /// Assemble an application, selecting the persistence backend from config and
    /// bootstrapping the on-disk state a standalone node needs.
    ///
    /// This is the drop-and-run startup path. It:
    ///
    /// - ensures the runtime scripts directory (`runtime.scripts_dir`, default
    ///   `./game`) exists when the runtime is enabled, creating it empty on first
    ///   run so an operator can drop a runtime entrypoint in later (until then the
    ///   built-in relay serves);
    /// - selects the persistence backend from `[database]` by URL scheme. A
    ///   `sqlite:`/bare-path URL opens (creating the file on first run) and
    ///   migrates a single-file SQLite database; a `postgres://` URL connects and
    ///   migrates Postgres; a `mongodb://`/`mongodb+srv://` URL verifies a
    ///   transaction-capable deployment and reconciles its foundation schema.
    ///   Every configured durable backend fails fast if unreachable or invalid —
    ///   the node must not start half-persistent; an absent URL runs in-memory.
    ///
    /// The selected backend class is logged (never the connection string).
    ///
    /// # Errors
    /// Returns a `Config` error if the scripts directory cannot be created, or a
    /// `Config`/`Database` error if a configured backend cannot be connected or
    /// migrated.
    pub async fn bootstrap(config: Config) -> AppResult<Self> {
        ensure_scripts_dir(&config.runtime)?;
        ensure_maps_dir(&config.runtime)?;
        let storage_indexes = config.storage.index_definitions()?;
        let backend = select_backend(&config.database).await?;
        let storage = backend.storage_repository();
        for index in &storage_indexes {
            storage.install_index(index).await?;
        }
        // Kept at debug so the startup banner (which reports the selected
        // backend) is the prominent line on a normal run.
        tracing::debug!(
            backend = backend.kind().as_str(),
            node_id = %config.server.node_id,
            "selected persistence backend"
        );
        Ok(Self::with_backend(config, backend))
    }

    /// Borrow the resolved configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The selected persistence backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    /// Which persistence backend this node is running on.
    #[must_use]
    pub fn backend_kind(&self) -> BackendKind {
        self.backend.kind()
    }

    /// The composed identity/session services over the selected backend.
    ///
    /// This is the composition the node uses: the session service issues and
    /// tracks sessions over the backend's session repository, and the
    /// authentication service runs account creation through the backend's unit
    /// of work. Both are built once at assembly and shared, so callers observe
    /// the same session-token index. Returned as trait objects so callers depend
    /// only on the contracts.
    #[must_use]
    pub fn build_identity_services(
        &self,
    ) -> (Arc<dyn AuthenticationService>, SharedSessionService) {
        (Arc::clone(&self.auth), Arc::clone(&self.sessions))
    }

    /// The composed authentication service (device/custom auth) for this node.
    #[must_use]
    pub fn authentication_service(&self) -> &Arc<dyn AuthenticationService> {
        &self.auth
    }

    /// The composed session service for this node.
    #[must_use]
    pub fn session_service(&self) -> &SharedSessionService {
        &self.sessions
    }

    /// The shared runtime metrics registry.
    #[must_use]
    pub fn metrics(&self) -> &Arc<NodeMetrics> {
        &self.metrics
    }

    /// Return host CPU, memory, and filesystem capacity telemetry.
    ///
    /// The collector is shared across [`App`] clones so CPU usage is calculated
    /// from consecutive samples rather than from an untrustworthy first read.
    /// OS inspection is scheduled on Tokio's blocking pool, so dashboard reads
    /// do not block an asynchronous HTTP worker.
    pub async fn host_telemetry(&self) -> HostTelemetrySnapshot {
        self.host_telemetry.snapshot().await
    }

    /// The in-process store of issued admin-console bearer tokens.
    #[must_use]
    pub fn console_tokens(&self) -> &Arc<ConsoleTokenStore> {
        &self.console_tokens
    }

    /// The console action audit trail.
    #[must_use]
    pub fn audit_log(&self) -> &Arc<AuditLog> {
        &self.audit
    }

    /// The local, redacted process incident journal shown in the console.
    #[must_use]
    pub fn error_journal(&self) -> &Arc<ErrorJournal> {
        &self.error_journal
    }

    /// The node-local, best-effort runtime event bus. It is never durable or
    /// replicated; runtime construction borrows this handle for all language
    /// adapters so their scripts share one local queue.
    #[must_use]
    pub fn runtime_event_bus(&self) -> &Arc<crate::runtime::RuntimeEventBus> {
        &self.runtime_event_bus
    }

    /// Node-local, non-durable cache shared by embedded runtime callbacks.
    #[must_use]
    pub fn runtime_shared_cache(&self) -> &Arc<crate::runtime::RuntimeSharedCache> {
        &self.runtime_shared_cache
    }

    /// Shared local fence for chat authorization and social/group revocations.
    #[must_use]
    pub fn chat_access(&self) -> &Arc<ChatAccessCoordinator> {
        &self.chat_access
    }

    /// The groups/clans service (, persisted in ).
    ///
    /// Backed by the selected backend's groups repository, so groups and their
    /// membership survive a node restart on Postgres/SQLite (the in-memory
    /// backend stays non-durable by design).
    #[must_use]
    pub fn groups(&self) -> &Arc<GroupsService> {
        &self.groups
    }

    /// The chat channel history and moderation service (, persisted in
    /// ).
    ///
    /// Backed by the selected backend's chat repository, so channels and their
    /// message history survive a node restart on Postgres/SQLite (the in-memory
    /// backend stays non-durable by design).
    #[must_use]
    pub fn chat(&self) -> &Arc<ChatService> {
        &self.chat
    }

    /// The configured policy used to build secure chat rate-limit plans.
    #[must_use]
    pub fn chat_rate_limits(&self) -> &Arc<ChatRateLimitPolicy> {
        &self.chat_rate_limits
    }

    /// The configured durable admission policy for public authentication.
    #[must_use]
    pub fn auth_rate_limits(&self) -> &Arc<AuthenticationRateLimitPolicy> {
        &self.auth_rate_limits
    }

    /// Replace the clock used by HTTP authentication admission.
    ///
    /// Production composition retains [`SystemClock`]; isolated HTTP tests can
    /// supply a controllable clock without changing fixed-window semantics.
    #[must_use]
    pub fn with_auth_clock(mut self, clock: Arc<dyn Clock + Send + Sync>) -> Self {
        self.auth_clock = clock;
        self
    }

    /// The clock used when admitting public authentication attempts.
    #[must_use]
    pub fn auth_clock(&self) -> &Arc<dyn Clock + Send + Sync> {
        &self.auth_clock
    }

    /// Per-operator admission bound for console database exploration.
    #[must_use]
    pub fn database_explorer_rate_limiter(&self) -> &Arc<DatabaseExplorerRateLimiter> {
        &self.database_explorer_rate_limiter
    }

    /// Node-local admission limiter for script-defined HTTP endpoints.
    #[must_use]
    pub fn runtime_http_endpoint_rate_limiter(
        &self,
    ) -> &Arc<crate::runtime::RuntimeHttpEndpointRateLimiter> {
        &self.runtime_http_endpoint_rate_limiter
    }

    /// The console notification store (, persisted in ).
    ///
    /// Backed by the selected backend's notifications repository, so targeted and
    /// broadcast notifications survive a node restart on the Postgres and SQLite
    /// backends (the in-memory backend stays non-durable by design).
    #[must_use]
    pub fn notifications(&self) -> &Arc<NotificationService> {
        &self.notifications
    }

    /// The player-addressed persistent inbox, separate from console notices.
    #[must_use]
    pub fn player_notifications(&self) -> &Arc<PlayerNotificationService> {
        &self.player_notifications
    }

    /// The leaderboards service (, persisted in ).
    ///
    /// Backed by the selected backend's leaderboards repository, so boards and the
    /// scores submitted to them survive a node restart on the Postgres and SQLite
    /// backends (the in-memory backend stays non-durable by design).
    #[must_use]
    pub fn leaderboards(&self) -> &Arc<LeaderboardService> {
        &self.leaderboards
    }

    /// Scheduler persistence owned by the selected backend.
    ///
    /// Durable backends return their transactional scheduler adapter; the
    /// in-memory backend returns its process-local reference adapter for tests
    /// and development.
    #[must_use]
    pub fn leaderboard_reset_repository(
        &self,
    ) -> Arc<dyn crate::leaderboard_scheduler::LeaderboardResetRepository> {
        self.backend.leaderboard_reset_repository()
    }

    /// The validated purchase / subscription record store (, persisted
    /// in ).
    ///
    /// Backed by the selected backend's purchases repository behind the
    /// deterministic dev receipt validator, so validated purchases survive a node
    /// restart on the Postgres and SQLite backends (the in-memory backend stays
    /// non-durable by design). Real store validators remain recorded follow-up
    /// work.
    #[must_use]
    pub fn purchases(&self) -> &Arc<PurchaseService> {
        &self.purchases
    }

    /// Attach the realtime gateway once the transports start.
    ///
    /// Idempotent — the first attachment wins; later calls are ignored. The
    /// slot is shared across `App` clones, so the HTTP surface built before
    /// the transports observes the gateway as soon as it exists.
    pub fn attach_realtime_gateway(&self, gateway: Arc<crate::realtime::Gateway>) {
        let _ = self.realtime.set(gateway);
    }

    /// The realtime gateway, or `None` before the transports have started
    /// (or on nodes running without realtime transports).
    #[must_use]
    pub fn realtime_gateway(&self) -> Option<Arc<crate::realtime::Gateway>> {
        self.realtime.get().cloned()
    }

    /// Compose durable session revocation with exact local live-session
    /// fencing. The gateway is optional only for HTTP-only nodes, where no
    /// connection can be live; callers always use this coordinator rather than
    /// bypassing the close boundary.
    #[must_use]
    pub fn session_revocation_coordinator(&self) -> crate::services::SessionRevocationCoordinator {
        crate::services::SessionRevocationCoordinator::new(
            Arc::clone(&self.sessions),
            self.realtime_gateway(),
        )
    }

    /// The per-user virtual-currency wallet store (, persisted in
    /// ).
    ///
    /// Backed by the selected backend's wallet repository, so per-user balances
    /// and their change ledger survive a node restart on the Postgres and SQLite
    /// backends (the in-memory backend stays non-durable by design).
    #[must_use]
    pub fn wallet(&self) -> &Arc<WalletService> {
        &self.wallet
    }

    /// The friend-relationship store.
    #[must_use]
    pub fn friends(&self) -> &Arc<FriendsService> {
        &self.friends
    }

    /// The optional volatile deferred writer. Normal storage APIs never use it;
    /// callers must explicitly opt in and accept its queue-only receipt.
    #[must_use]
    pub fn deferred_storage(&self) -> Option<&Arc<DeferredStorageWriter>> {
        self.deferred_storage.as_ref()
    }

    /// How long this process has been assembled (monotonic).
    #[must_use]
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Stable node identity for this process.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.config.server.node_id
    }

    /// Crate version string.
    #[must_use]
    pub fn version(&self) -> &'static str {
        VERSION
    }

    /// Aggregate health for the application.
    ///
    /// With no services registered yet, a successfully assembled app is
    /// [`Health::Healthy`]. Later tasks aggregate registered service health
    /// here.
    #[must_use]
    pub fn health(&self) -> Health {
        Health::Healthy
    }
}

/// Create the runtime scripts directory on first run when the runtime is enabled.
///
/// A standalone node ships with an empty (or absent) `game/` folder; creating it
/// here means the "unzip and run" flow works with no manual `mkdir`, and an
/// operator immediately sees where to drop `main.lua` or `main.py`. When the
/// runtime is disabled, or the directory already exists, this is a no-op. The
/// built-in relay still serves until an entrypoint is present, so an empty
/// directory changes nothing about behavior — it only advertises the extension
/// point.
///
/// # Errors
/// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the
/// directory cannot be created (e.g. a file already exists at that path).
fn ensure_scripts_dir(runtime: &RuntimeConfig) -> AppResult<()> {
    if !runtime.enabled {
        return Ok(());
    }
    let dir = Path::new(&runtime.scripts_dir);
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir).map_err(|e| {
        AppError::config(format!(
            "failed to create runtime scripts directory: {}",
            dir.display()
        ))
        .with_detail(e.to_string())
    })?;
    tracing::info!(
        scripts_dir = %runtime.scripts_dir,
        "created empty game scripts directory; drop a main.lua or main.py here to add game logic \
         (the built-in relay serves until then)"
    );
    Ok(())
}

/// Create the maps directory on first run when the runtime is enabled.
///
/// Mirrors [`ensure_scripts_dir`]: the "unzip and run" flow works with no manual
/// `mkdir`, and an operator immediately sees where to drop cooked `.map` files. An
/// absent or empty directory is fine — the server just loads no server-side
/// geometry. When the runtime is disabled, or the directory already exists, this
/// is a no-op.
///
/// # Errors
/// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the directory
/// cannot be created (e.g. a file already exists at that path).
fn ensure_maps_dir(runtime: &RuntimeConfig) -> AppResult<()> {
    if !runtime.enabled {
        return Ok(());
    }
    let dir = Path::new(&runtime.maps_dir);
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir).map_err(|e| {
        AppError::config(format!(
            "failed to create maps directory: {}",
            dir.display()
        ))
        .with_detail(e.to_string())
    })?;
    tracing::info!(
        maps_dir = %runtime.maps_dir,
        "created empty maps directory; drop cooked .map files here for server-side geometry"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_exposes_node_id_from_config() {
        let app = App::new(Config::default());
        assert_eq!(app.node_id(), "dev-1");
    }

    #[test]
    fn default_app_uses_the_in_memory_backend() {
        let app = App::new(Config::default());
        assert_eq!(app.backend_kind(), crate::repository::BackendKind::InMemory);
    }

    #[tokio::test]
    async fn bootstrap_without_a_database_selects_in_memory() {
        let app = App::bootstrap(Config::default())
            .await
            .expect("bootstrap with no database");
        assert_eq!(app.backend_kind(), crate::repository::BackendKind::InMemory);
    }

    #[tokio::test]
    async fn built_identity_services_create_an_account_over_the_backend() {
        use crate::identity::DeviceId;
        use crate::services::{AuthenticationOptions, DeviceAuthenticationRequest};
        use crate::session::NodeId;
        use crate::time::{DurationMillis, TimestampMillis};

        let app = App::new(Config::default());
        let (auth, _sessions) = app.build_identity_services();
        let outcome = auth
            .authenticate_device(DeviceAuthenticationRequest {
                device_id: DeviceId::new("device-x").expect("device"),
                options: AuthenticationOptions {
                    create_account: true,
                    username: Some(crate::identity::Username::new("composed").expect("username")),
                    display_name: None,
                    metadata: None,
                    now: TimestampMillis::from_unix_millis(1_000),
                    owner_node: NodeId::new("dev-1").expect("node"),
                    session_ttl: DurationMillis::from_millis(1_000),
                    refresh_ttl: Some(DurationMillis::from_millis(5_000)),
                },
            })
            .await
            .expect("register through composed services");
        assert!(outcome.account_created);
        // The account is visible on the backend's pooled repository.
        assert!(
            app.backend()
                .user_repository()
                .get_user(&outcome.user.id)
                .await
                .expect("get")
                .is_some()
        );
    }

    #[test]
    fn version_is_non_empty() {
        let app = App::new(Config::default());
        assert!(!app.version().is_empty());
        assert_eq!(app.version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn freshly_assembled_app_is_healthy() {
        let app = App::new(Config::default());
        assert_eq!(app.health(), Health::Healthy);
        assert!(app.health().is_serviceable());
    }

    #[test]
    fn config_is_borrowable() {
        let app = App::new(Config::default());
        assert_eq!(app.config(), &Config::default());
    }

    #[test]
    fn metrics_registry_is_shared_across_clones() {
        let app = App::new(Config::default());
        let clone = app.clone();
        app.metrics().record_http_request();
        // Both handles observe the same underlying registry.
        assert_eq!(clone.metrics().snapshot().http_requests_total, 1);
    }

    #[test]
    fn uptime_is_non_decreasing() {
        let app = App::new(Config::default());
        let first = app.uptime();
        let second = app.uptime();
        assert!(second >= first);
    }

    fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("citadel-{tag}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn ensure_scripts_dir_creates_a_missing_directory() {
        let base = unique_temp_dir("scripts-create");
        let scripts = base.join("game");
        let runtime = RuntimeConfig {
            scripts_dir: scripts.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };
        assert!(!scripts.exists());
        ensure_scripts_dir(&runtime).expect("creates missing scripts dir");
        assert!(scripts.is_dir(), "scripts dir created on first run");
        // Idempotent: a second call with the dir present is a no-op success.
        ensure_scripts_dir(&runtime).expect("idempotent when present");
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn ensure_scripts_dir_is_a_noop_when_runtime_disabled() {
        let base = unique_temp_dir("scripts-disabled");
        let scripts = base.join("game");
        let runtime = RuntimeConfig {
            enabled: false,
            scripts_dir: scripts.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };
        ensure_scripts_dir(&runtime).expect("disabled runtime never touches the fs");
        assert!(
            !scripts.exists(),
            "disabled runtime must not create the dir"
        );
    }

    #[test]
    fn ensure_scripts_dir_errors_when_a_file_blocks_the_path() {
        let base = unique_temp_dir("scripts-blocked");
        std::fs::create_dir_all(&base).expect("base dir");
        let blocking = base.join("game");
        std::fs::write(&blocking, b"not a dir").expect("write blocking file");
        let runtime = RuntimeConfig {
            scripts_dir: blocking.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };
        let err = ensure_scripts_dir(&runtime).expect_err("a file blocking the path must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        std::fs::remove_dir_all(&base).ok();
    }
}
