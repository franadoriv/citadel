//! Typed configuration model for Citadel.
//!
//!  defined the typed structs and defaults.  adds layered
//! loading and validation per `docs/architecture/cli-and-config.md`:
//!
//! 1. built-in defaults
//! 2. config file (default path or `--config`)
//! 3. environment variables with the `CITADEL_` prefix
//! 4. narrow CLI flag overrides
//!
//! [`Config::load`] resolves that precedence; [`Config::validate`] enforces
//! address and enum/required-field rules and maps failures to the
//! [`Config`](crate::error::ErrorCategory::Config) error category. Diagnostics
//! never print secrets.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::session::NodeId;
use crate::storage::{
    Collection, Key, StorageIndexDefinition, StorageIndexField, StorageIndexName,
};

/// Filename of the zero-flag default config discovered next to the binary.
///
/// With no `--config` flag, [`Config::load`] loads `./<DEFAULT_CONFIG_FILE>` from
/// the current working directory when present, so the release UX is "unzip and
/// run" with no arguments. An explicit `--config` always wins.
pub const DEFAULT_CONFIG_FILE: &str = "citadel.toml";

/// Return the default config path inside `dir` if a `citadel.toml` file exists
/// there, otherwise `None`.
///
/// This is the discovery primitive behind [`Config::load`]'s zero-flag behavior.
/// It is pure (a filesystem existence check against an explicit directory) so the
/// discovery rule is unit-testable without depending on the process working
/// directory.
#[must_use]
pub fn discover_config_in(dir: &Path) -> Option<PathBuf> {
    let candidate = dir.join(DEFAULT_CONFIG_FILE);
    candidate.is_file().then_some(candidate)
}

/// Top-level Citadel configuration.
///
/// Sections mirror `docs/architecture/cli-and-config.md`. Only the sections
/// needed by the current skeleton are modeled; database, runtime, cluster, and
/// socket sections are introduced by their owning tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Node identity and lifecycle settings.
    pub server: ServerConfig,
    /// HTTP listener settings.
    pub http: HttpConfig,
    /// Logging and tracing settings.
    pub logging: LoggingConfig,
    /// Realtime transport listener settings.
    pub transport: TransportConfig,
    /// Embedded game-logic runtime settings.
    pub runtime: RuntimeConfig,
    /// Operator-declared storage indexes.
    pub storage: StorageConfig,
    /// Optional PostgreSQL persistence settings.
    pub database: DatabaseConfig,
    /// Admin console authentication settings.
    pub console: ConsoleConfig,
    /// Optional mutually-authenticated node-control plane and live matchmaker.
    pub cluster: ClusterConfig,
    /// Secure chat mutation, history, and moderation controls.
    pub chat: ChatConfig,
    /// Public HTTP authentication abuse controls. This is distinct from
    /// `transport.auth`, which configures realtime handshake behavior.
    pub authentication: AuthenticationAbuseConfig,
}

/// Authentication-specific abuse-control configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AuthenticationAbuseConfig {
    /// Fixed-window limits applied before authentication work.
    pub limits: AuthLimitsConfig,
}

/// One validated authentication fixed-window allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthRateLimitRule {
    /// Requests permitted in one window.
    pub limit: u32,
    /// Fixed-window duration in milliseconds.
    pub window_ms: u64,
}

impl AuthRateLimitRule {
    const fn new(limit: u32, window_ms: u64) -> Self {
        Self { limit, window_ms }
    }
}

/// Explicit multi-key limits for public authentication. Source-address limits
/// contain broad request floods; email limits protect one password verifier
/// across distributed sources; registration has its own tighter source limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthLimitsConfig {
    pub source: AuthRateLimitRule,
    pub email: AuthRateLimitRule,
    pub registration_source: AuthRateLimitRule,
}

impl Default for AuthLimitsConfig {
    fn default() -> Self {
        Self {
            source: AuthRateLimitRule::new(30, 60_000),
            email: AuthRateLimitRule::new(10, 900_000),
            registration_source: AuthRateLimitRule::new(10, 3_600_000),
        }
    }
}

impl AuthLimitsConfig {
    fn validate(&self) -> AppResult<()> {
        for (name, rule) in [
            ("auth.limits.source", self.source),
            ("auth.limits.email", self.email),
            ("auth.limits.registration_source", self.registration_source),
        ] {
            if rule.limit == 0 || rule.limit > 1_000_000 {
                return Err(AppError::config(format!(
                    "{name}.limit must be between 1 and 1000000"
                )));
            }
            if rule.window_ms == 0 || rule.window_ms > 86_400_000 {
                return Err(AppError::config(format!(
                    "{name}.window_ms must be between 1 and 86400000"
                )));
            }
        }
        Ok(())
    }
}

/// Chat-specific policy configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ChatConfig {
    /// Fixed-window abuse controls applied by the secure chat boundary.
    pub limits: ChatLimitsConfig,
}

/// One validated fixed-window allowance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatRateLimitRule {
    /// Successful actions permitted in one window.
    pub limit: u32,
    /// Fixed-window duration in milliseconds.
    pub window_ms: u64,
}

impl ChatRateLimitRule {
    const fn new(limit: u32, window_ms: u64) -> Self {
        Self { limit, window_ms }
    }
}

/// Multi-key fixed-window policies. Each field is intentionally explicit so a
/// TOML configuration cannot silently combine unrelated abuse controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChatLimitsConfig {
    pub join: ChatRateLimitRule,
    pub history: ChatRateLimitRule,
    pub send_user: ChatRateLimitRule,
    pub send_user_channel: ChatRateLimitRule,
    pub send_channel: ChatRateLimitRule,
    pub typing_user: ChatRateLimitRule,
    pub typing_user_channel: ChatRateLimitRule,
    pub mutation_user: ChatRateLimitRule,
    pub mutation_user_channel: ChatRateLimitRule,
    pub moderation_operator: ChatRateLimitRule,
    pub moderation_channel: ChatRateLimitRule,
}

impl Default for ChatLimitsConfig {
    fn default() -> Self {
        Self {
            join: ChatRateLimitRule::new(12, 60_000),
            history: ChatRateLimitRule::new(60, 60_000),
            send_user: ChatRateLimitRule::new(8, 10_000),
            send_user_channel: ChatRateLimitRule::new(12, 10_000),
            send_channel: ChatRateLimitRule::new(160, 10_000),
            typing_user: ChatRateLimitRule::new(20, 10_000),
            typing_user_channel: ChatRateLimitRule::new(12, 10_000),
            mutation_user: ChatRateLimitRule::new(4, 60_000),
            mutation_user_channel: ChatRateLimitRule::new(8, 60_000),
            moderation_operator: ChatRateLimitRule::new(30, 60_000),
            moderation_channel: ChatRateLimitRule::new(60, 60_000),
        }
    }
}

impl ChatLimitsConfig {
    fn validate(&self) -> AppResult<()> {
        for (name, rule) in [
            ("chat.limits.join", self.join),
            ("chat.limits.history", self.history),
            ("chat.limits.send_user", self.send_user),
            ("chat.limits.send_user_channel", self.send_user_channel),
            ("chat.limits.send_channel", self.send_channel),
            ("chat.limits.typing_user", self.typing_user),
            ("chat.limits.typing_user_channel", self.typing_user_channel),
            ("chat.limits.mutation_user", self.mutation_user),
            (
                "chat.limits.mutation_user_channel",
                self.mutation_user_channel,
            ),
            ("chat.limits.moderation_operator", self.moderation_operator),
            ("chat.limits.moderation_channel", self.moderation_channel),
        ] {
            if rule.limit == 0 || rule.limit > 1_000_000 {
                return Err(AppError::config(format!(
                    "{name}.limit must be between 1 and 1000000"
                )));
            }
            if rule.window_ms == 0 || rule.window_ms > 86_400_000 {
                return Err(AppError::config(format!(
                    "{name}.window_ms must be between 1 and 86400000"
                )));
            }
        }
        Ok(())
    }
}

/// Distributed-node configuration for the live matchmaker control plane.
/// Disabled by default so a standalone server keeps the single-node path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// Enables the durable cross-node matchmaker path.
    pub enabled: bool,
    /// TCP address for the narrow mTLS node-control listener.
    pub control_bind: String,
    /// Queue shard this compact cluster MVP resolves and owns/forwards.
    pub matchmaker_shard: u16,
    /// Lease duration used for durable acquisition/renewal.
    pub lease_ttl_ms: u64,
    /// Match handoff capability lifetime.
    pub handoff_ttl_ms: u64,
    /// Deadline for an authenticated node-control command.
    pub command_timeout_ms: u64,
    /// Explicit authenticated peer registrations (production seed baseline).
    pub peers: Vec<ClusterPeerConfig>,
    /// Local mTLS identity. Peer leaf certificates are supplied per peer.
    pub tls: ClusterTlsConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            control_bind: "127.0.0.1:7390".to_owned(),
            matchmaker_shard: 0,
            lease_ttl_ms: 5_000,
            handoff_ttl_ms: 30_000,
            command_timeout_ms: 2_000,
            peers: Vec::new(),
            tls: ClusterTlsConfig::default(),
        }
    }
}

/// One explicitly registered node-control endpoint and its pinned certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPeerConfig {
    /// Stable peer node identity bound to its leaf certificate fingerprint.
    pub node_id: String,
    /// TCP address of the peer's control listener.
    pub control_addr: String,
    /// DNS name verified by TLS for this endpoint.
    pub server_name: String,
    /// PEM file holding the peer's leaf certificate (or first chain member).
    pub certificate_file: String,
}

/// Local certificate/key material for the mTLS node-control listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterTlsConfig {
    /// PEM certificate for the private cluster CA trusted for mTLS peers.
    pub ca_certificate_file: String,
    /// PEM certificate chain for this node.
    pub certificate_file: String,
    /// PKCS#8 private-key PEM for this node.
    pub private_key_file: String,
}

impl ClusterConfig {
    fn validate(&self, local_node: &str, database: &DatabaseConfig) -> AppResult<()> {
        if !self.enabled {
            return Ok(());
        }
        if !database.is_enabled() {
            return Err(AppError::config(
                "cluster.enabled requires database.url so matchmaker fencing survives node restart",
            ));
        }
        NodeId::new(local_node.to_owned())?;
        validate_socket_addr("cluster.control_bind", &self.control_bind)?;
        if self.lease_ttl_ms == 0 || self.handoff_ttl_ms == 0 || self.command_timeout_ms == 0 {
            return Err(AppError::config(
                "cluster lease_ttl_ms, handoff_ttl_ms, and command_timeout_ms must be >= 1",
            ));
        }
        if self.tls.certificate_file.trim().is_empty()
            || self.tls.private_key_file.trim().is_empty()
            || self.tls.ca_certificate_file.trim().is_empty()
        {
            return Err(AppError::config(
                "cluster.tls.ca_certificate_file, certificate_file, and private_key_file are required when cluster.enabled",
            ));
        }
        let mut peers = std::collections::BTreeSet::new();
        for peer in &self.peers {
            let node = NodeId::new(peer.node_id.clone())?;
            if node.as_str() == local_node || !peers.insert(node.as_str().to_owned()) {
                return Err(AppError::config(
                    "cluster.peers must contain distinct non-local node_id values",
                ));
            }
            validate_socket_addr("cluster.peers.control_addr", &peer.control_addr)?;
            if peer.server_name.trim().is_empty() || peer.certificate_file.trim().is_empty() {
                return Err(AppError::config(
                    "cluster peers require non-empty server_name and certificate_file",
                ));
            }
        }
        Ok(())
    }
}

/// Which durable persistence backend a connection URL selects.
///
/// Chosen purely by the URL scheme so a single `[database]` section serves both
/// backends: a `postgres://`/`postgresql://` URL selects Postgres; a `sqlite:`
/// URL or a bare file path (anything without another `://` scheme) selects the
/// embedded, single-file SQLite backend. A `cockroach://`/`cockroachdb://` URL
/// also selects the Postgres backend (CockroachDB speaks the PostgreSQL wire
/// protocol) but flags the CockroachDB dialect flavor — see [`PgFlavor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseBackend {
    /// A networked PostgreSQL server (or a PostgreSQL-wire-compatible server such
    /// as CockroachDB — see [`DatabaseConfig::pg_flavor`]).
    Postgres,
    /// An embedded, single-file (or in-memory) SQLite database.
    Sqlite,
    /// MongoDB, requiring a replica set or sharded cluster for transactions.
    MongoDb,
}

/// Which PostgreSQL-wire dialect flavor a Postgres-backend URL targets
///.
///
/// CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its
/// Postgres backend (`repository::pg`) for both. The flavor is the small set of
/// runtime differences the backend must honor: CockroachDB rejects `COLLATE "C"`
/// (so it uses the `migrations-crdb/` DDL), does not implement
/// `pg_advisory_xact_lock` (the storage repository skips it — CockroachDB's
/// default `SERIALIZABLE` isolation plus the primary-key constraint already close
/// the absent-row race the lock guards on PostgreSQL), and does not support the
/// advisory locks SQLx uses to serialize migrations (so migration locking is
/// disabled). The flavor is chosen purely by the URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PgFlavor {
    /// A standard PostgreSQL server.
    #[default]
    Postgres,
    /// A CockroachDB cluster reached over the PostgreSQL wire protocol.
    Cockroach,
}

impl PgFlavor {
    /// Stable lowercase token for status responses and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Cockroach => "cockroach",
        }
    }
}

/// Durable persistence settings (, ).
///
/// The whole section is optional: with no `url` (and no `CITADEL_DATABASE_URL`)
/// the node keeps using the in-memory repositories, so a default config still
/// runs with no database. When a `url` is present the node selects a durable
/// backend by scheme (see [`DatabaseBackend`]): `postgres://` builds a
/// `repository::pg::PgDatabase`; `sqlite:`/a bare file path builds a
/// `repository::sqlite::SqliteDatabase` (the zero-infra single-file story). The
/// URL can carry credentials and is never echoed in diagnostics; use
/// [`redact_url_credentials`](crate::error::redact_url_credentials) for logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Connection URL selecting the backend by scheme: `postgres://user:pass@host/db`
    /// (Postgres), `sqlite:data.sqlite` / `sqlite::memory:` / a bare path like
    /// `./data.sqlite` (SQLite). `None` runs the in-memory backend. Also settable
    /// via `CITADEL_DATABASE_URL`.
    pub url: Option<String>,
    /// Maximum size of the connection pool.
    pub max_connections: u32,
    /// Timeout, in milliseconds, for establishing the initial connection.
    pub connect_timeout_ms: u64,
    /// Timeout, in milliseconds, for acquiring a connection from the pool.
    pub acquire_timeout_ms: u64,
    /// Transactional Mongo reads must use the primary.
    pub mongodb_read_preference: String,
    /// Transactional Mongo writes must be majority acknowledged.
    pub mongodb_write_concern: String,
    /// Transactional Mongo reads must use majority read concern.
    pub mongodb_read_concern: String,
}

/// Redacted `Debug`: never prints the connection URL (it can carry credentials).
///
/// `Config` and [`App`](crate::App) derive `Debug` and embed this section, so a
/// stray `{config:?}` / `{app:?}` must not leak the connection string. Only
/// whether a URL is configured is shown, plus the non-secret tunables.
impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &self.url.as_ref().map(|_| "<redacted>"))
            .field("max_connections", &self.max_connections)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("acquire_timeout_ms", &self.acquire_timeout_ms)
            .field("mongodb_read_preference", &self.mongodb_read_preference)
            .field("mongodb_write_concern", &self.mongodb_write_concern)
            .field("mongodb_read_concern", &self.mongodb_read_concern)
            .finish()
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: None,
            max_connections: 10,
            connect_timeout_ms: 5_000,
            acquire_timeout_ms: 5_000,
            mongodb_read_preference: "primary".to_owned(),
            mongodb_write_concern: "majority".to_owned(),
            mongodb_read_concern: "majority".to_owned(),
        }
    }
}

/// Operator-owned storage-index declarations.
///
/// Indexes are static server configuration rather than a player or script DDL
/// surface. Durable backends install a matching physical JSON-expression index
/// during bootstrap; in-memory mode preserves the same query semantics for
/// development and contract tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Declared physical indexes. TOML uses `[[storage.indexes]]` entries.
    pub indexes: Vec<StorageIndexConfig>,
}

/// One static index declaration in the `[storage]` configuration section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageIndexConfig {
    /// Stable operator-selected index name.
    pub name: String,
    /// Storage collection covered by this index.
    pub collection: String,
    /// Optional object-key restriction within the collection.
    pub key: Option<String>,
    /// Unique top-level JSON fields available to equality filters.
    pub fields: Vec<String>,
}

impl StorageConfig {
    /// Convert configuration-only strings into the portable storage contract.
    ///
    /// The conversion is shared by config validation, application bootstrap,
    /// and runtime assembly so an accepted config cannot later construct a
    /// subtly different index definition.
    pub fn index_definitions(&self) -> AppResult<Vec<StorageIndexDefinition>> {
        let mut names = std::collections::BTreeSet::new();
        self.indexes
            .iter()
            .map(|index| {
                if !names.insert(index.name.clone()) {
                    return Err(AppError::config(format!(
                        "storage.indexes contains duplicate index name `{}`",
                        index.name
                    )));
                }
                let definition = StorageIndexDefinition::new(
                    StorageIndexName::new(&index.name)
                        .map_err(|error| storage_index_config_error(&index.name, error))?,
                    Collection::new(&index.collection)
                        .map_err(|error| storage_index_config_error(&index.name, error))?,
                    index
                        .key
                        .as_ref()
                        .map(Key::new)
                        .transpose()
                        .map_err(|error| storage_index_config_error(&index.name, error))?,
                    index
                        .fields
                        .iter()
                        .map(StorageIndexField::new)
                        .collect::<AppResult<Vec<_>>>()
                        .map_err(|error| storage_index_config_error(&index.name, error))?,
                )
                .map_err(|error| storage_index_config_error(&index.name, error))?;
                Ok(definition)
            })
            .collect()
    }
}

fn storage_index_config_error(index_name: &str, error: AppError) -> AppError {
    AppError::config(format!(
        "storage.indexes entry `{index_name}` is invalid: {}",
        error.message()
    ))
}

/// The URL scheme prefixes that select the Postgres backend, paired with the
/// dialect flavor each implies.
///
/// `postgres://`/`postgresql://` are standard PostgreSQL; `cockroach://`/
/// `cockroachdb://` are CockroachDB reached over the same wire protocol (see
/// [`PgFlavor`]). Shared by [`classify_url`] and [`DatabaseConfig::pg_flavor`] so
/// scheme classification and flavor detection never disagree.
const PG_SCHEMES: &[(&str, PgFlavor)] = &[
    ("postgres://", PgFlavor::Postgres),
    ("postgresql://", PgFlavor::Postgres),
    ("cockroach://", PgFlavor::Cockroach),
    ("cockroachdb://", PgFlavor::Cockroach),
];

/// Classify a connection URL into a durable [`DatabaseBackend`], or `Err()` if
/// the scheme is unrecognized (e.g. `mysql://`).
///
/// Shared by [`DatabaseConfig::backend`] and [`DatabaseConfig::validate`] so
/// selection and validation never disagree. A `postgres://`/`postgresql://` or
/// `cockroach://`/`cockroachdb://` URL is the Postgres backend; a `sqlite:` URL or
/// a bare path (no `://` scheme) is SQLite; any other `scheme://` is rejected. The
/// URL contents are never echoed by callers.
fn classify_url(url: &str) -> Result<DatabaseBackend, ()> {
    let url = url.trim();
    if PG_SCHEMES.iter().any(|(scheme, _)| url.starts_with(scheme)) {
        Ok(DatabaseBackend::Postgres)
    } else if url.starts_with("sqlite:") {
        Ok(DatabaseBackend::Sqlite)
    } else if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
        Ok(DatabaseBackend::MongoDb)
    } else if url.contains("://") {
        Err(())
    } else {
        // A bare file path (e.g. `./data.sqlite`) is the embedded SQLite backend.
        Ok(DatabaseBackend::Sqlite)
    }
}

impl DatabaseConfig {
    /// Whether a durable backend is configured (a non-empty `url` is present).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.url.as_ref().is_some_and(|url| !url.trim().is_empty())
    }

    /// The durable backend selected by the URL scheme, or `None` when no `url` is
    /// configured (the node runs in-memory).
    ///
    /// # Errors
    /// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the URL
    /// scheme is unrecognized. The offending URL is never echoed (it can carry
    /// credentials).
    pub fn backend(&self) -> AppResult<Option<DatabaseBackend>> {
        match &self.url {
            Some(url) if !url.trim().is_empty() => classify_url(url).map(Some).map_err(|()| {
                AppError::config(
                    "database.url must be a postgres://, postgresql://, cockroach://, \
                     cockroachdb://, mongodb://, mongodb+srv://, or sqlite: URL, or a file path",
                )
            }),
            _ => Ok(None),
        }
    }

    /// Validate the explicit Mongo consistency policy. URI TLS/auth options are
    /// passed unchanged to the official driver, preserving SCRAM/X.509 support.
    pub fn validate_mongodb_policy(&self) -> AppResult<()> {
        if self.mongodb_read_preference != "primary" {
            return Err(AppError::config(
                "database.mongodb_read_preference must be `primary` for transactional consistency",
            ));
        }
        if self.mongodb_write_concern != "majority" {
            return Err(AppError::config(
                "database.mongodb_write_concern must be `majority` for transactional consistency",
            ));
        }
        if self.mongodb_read_concern != "majority" {
            return Err(AppError::config(
                "database.mongodb_read_concern must be `majority` for transactional consistency",
            ));
        }
        Ok(())
    }

    /// The PostgreSQL-wire dialect flavor selected by the URL scheme.
    ///
    /// A `cockroach://`/`cockroachdb://` URL targets [`PgFlavor::Cockroach`];
    /// every other Postgres-backend URL (and any non-Postgres or absent URL)
    /// targets [`PgFlavor::Postgres`]. This is only meaningful when
    /// [`backend`](Self::backend) is `Postgres`; the Postgres backend uses it to
    /// pick the CockroachDB-compatible migrations and skip PostgreSQL-only
    /// advisory locks. The URL contents are never echoed.
    #[must_use]
    pub fn pg_flavor(&self) -> PgFlavor {
        let Some(url) = self.url.as_deref() else {
            return PgFlavor::Postgres;
        };
        let url = url.trim();
        PG_SCHEMES
            .iter()
            .find(|(scheme, _)| url.starts_with(scheme))
            .map_or(PgFlavor::Postgres, |(_, flavor)| *flavor)
    }
}

/// Runtime language selected for server-side game logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLanguage {
    /// Embedded Lua (`main.lua`), available in every build.
    Lua,
    /// Embedded Python (`main.py`), available with the `runtime-python` feature.
    Python,
    /// Embedded capped JavaScript (`main.js`), available with the `runtime-js` feature.
    #[serde(rename = "js", alias = "javascript")]
    Js,
}

impl RuntimeLanguage {
    /// Stable lowercase token used in config, logs, and status JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lua => "lua",
            Self::Python => "python",
            Self::Js => "js",
        }
    }

    /// Conventional entrypoint filenames for this language.
    #[must_use]
    pub const fn entry_files(self) -> &'static [&'static str] {
        match self {
            Self::Lua => &["main.lua"],
            Self::Python => &["main.py"],
            Self::Js => &["main.js"],
        }
    }

    /// Languages considered by autodetection, in product priority order.
    #[must_use]
    pub const fn autodetect_order() -> &'static [RuntimeLanguage] {
        &[Self::Lua, Self::Python, Self::Js]
    }
}

/// Runtime hosting adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeAdapter {
    /// In-process embedded interpreter/runtime.
    #[default]
    Embedded,
    /// Supervised child process over the runtime-worker transport.
    #[serde(rename = "external-worker", alias = "external_worker")]
    ExternalWorker,
    /// WASM component/module hosted by Citadel.
    Wasm,
}

impl RuntimeAdapter {
    /// Stable lowercase token used in config, logs, and status JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::ExternalWorker => "external-worker",
            Self::Wasm => "wasm",
        }
    }
}

/// Runtime trust tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTier {
    /// Single-operator trusted code, full language power.
    #[default]
    Trusted,
    /// Multi-tenant/untrusted code with enforced capabilities.
    Hardened,
}

/// Lua's in-process capability mode.
///
/// This is deliberately separate from [`RuntimeTier`]. The latter describes the
/// product hosting tier; this setting controls whether the embedded Lua adapter
/// exposes machine-level Lua standard libraries on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LuaExecutionMode {
    /// The safe default: only Citadel's bounded host API and scoped module
    /// loader are available.
    #[default]
    Sandboxed,
    /// Operator-owned Lua with the complete standard library. This is an
    /// explicit opt-in because it grants filesystem, process, and unrestricted
    /// module-loading access.
    Trusted,
}

impl LuaExecutionMode {
    /// Stable lowercase token used in config, logs, and status JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sandboxed => "sandboxed",
            Self::Trusted => "trusted",
        }
    }
}

impl RuntimeTier {
    /// Stable lowercase token used in config, logs, and status JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trusted => "trusted",
            Self::Hardened => "hardened",
        }
    }
}

/// Whether a runtime language came from config or filesystem autodetection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeSelectionSource {
    /// `[runtime] language = ...` selected the language.
    Explicit,
    /// The entrypoint file in `scripts_dir` selected the language.
    Autodetected,
}

impl RuntimeSelectionSource {
    /// Stable lowercase token used in logs and status JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Autodetected => "autodetected",
        }
    }
}

/// Resolved runtime selection for a present game entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSelection {
    /// Selected language.
    pub language: RuntimeLanguage,
    /// Runtime hosting adapter.
    pub adapter: RuntimeAdapter,
    /// Runtime trust tier.
    pub tier: RuntimeTier,
    /// Entry point file that exists on disk.
    pub entrypoint: PathBuf,
    /// Whether the language came from config or autodetection.
    pub source: RuntimeSelectionSource,
}

/// Embedded game-logic runtime settings.
///
/// When `enabled`, the node loads an entrypoint from `scripts_dir` and routes
/// inbound realtime messages to the selected runtime's handlers. If no entrypoint
/// is present the node silently falls back to the built-in relay, so the default
/// (enabled, `./game`) is safe to ship.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Whether the embedded script runtime is consulted for inbound messages.
    pub enabled: bool,
    /// Explicit runtime language. `None` means autodetect from `scripts_dir`.
    pub language: Option<RuntimeLanguage>,
    /// Runtime hosting adapter. Only `embedded` is implemented today.
    pub adapter: RuntimeAdapter,
    /// Runtime trust tier. Only `trusted` is implemented today.
    pub tier: RuntimeTier,
    /// Capability mode for the embedded Lua adapter. Sandboxed is the safe
    /// default; trusted machine access requires an explicit opt-in.
    pub lua_execution_mode: LuaExecutionMode,
    /// Directory holding the game scripts (`main.<ext>` is the entrypoint).
    pub scripts_dir: String,
    /// Directory holding cooked `.map` level geometry (CMAP files). Scanned once
    /// at startup; a room's `map` name resolves to a loaded map here. Absent or
    /// empty is fine — the server just has no server-side geometry.
    pub maps_dir: String,
    /// Optional read-only root for static gameplay JSON/CSV files. This is kept
    /// deliberately separate from `scripts_dir`; Lua receives only a narrow,
    /// cached loader rooted here, never general filesystem access.
    pub static_data_dir: Option<String>,
    /// Maximum bytes Citadel will read for one JSON or CSV static-data file.
    /// Ignored while `static_data_dir` is unset.
    pub static_data_max_file_bytes: usize,
    /// Per-invocation time budget for message and lifecycle handlers, in ms.
    pub deadline_ms: u64,
    /// Server game-loop rate in ticks per second. `0` (the default) disables the
    /// `citadel.on_tick` loop entirely; no periodic task is spawned.
    pub tick_hz: u32,
    /// Per-tick time budget in milliseconds. `None` (the default) derives an
    /// automatic budget of `min(50ms, half the tick period)` (at least 1ms), so
    /// a tick gets its own SLO independent of the message `deadline_ms`.
    pub tick_deadline_ms: Option<u64>,
    /// Whether the node watches the selected script entrypoint and reloads it
    /// live on change. Opt-in (`false` by default): a development convenience,
    /// not for production. Reloads are failure-safe — a broken edit is rejected
    /// and the previously-loaded script keeps serving.
    pub hot_reload: bool,
    /// How often (in milliseconds) to poll the script for changes when
    /// `hot_reload` is enabled. Defaults to 500ms. Ignored when `hot_reload` is
    /// off.
    pub hot_reload_poll_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            language: None,
            adapter: RuntimeAdapter::Embedded,
            tier: RuntimeTier::Trusted,
            lua_execution_mode: LuaExecutionMode::Sandboxed,
            scripts_dir: "./game".to_string(),
            maps_dir: "./maps".to_string(),
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            deadline_ms: crate::runtime::DEFAULT_DEADLINE_MS,
            tick_hz: 0,
            tick_deadline_ms: None,
            hot_reload: false,
            hot_reload_poll_ms: 500,
        }
    }
}

/// Realtime transport listener settings.
///
/// Each transport is independently enabled and bound. QUIC is the primary
/// real-time transport; WebSocket is the fallback. Both are disabled by default
/// so the current skeleton keeps only the HTTP listener until operators opt in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// TLS material shared by the public QUIC and WebTransport listeners.
    pub tls: TransportTlsConfig,
    /// QUIC listener settings.
    pub quic: QuicConfig,
    /// WebSocket fallback listener settings.
    pub websocket: WebSocketConfig,
    /// WebTransport (browser) listener settings.
    pub webtransport: WebTransportConfig,
    /// Realtime authentication handshake stance (shared by all transports).
    pub auth: AuthConfig,
    /// Authoritative transform synchronization.
    pub transform_sync: TransformSyncConfig,
}

/// Optional PEM certificate chain and private key for public UDP transports.
///
/// When both paths are empty, enabled QUIC and WebTransport listeners generate
/// their existing short-lived development certificates. Production operators
/// must set both values to use a CA-issued certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct TransportTlsConfig {
    /// PEM certificate chain presented by QUIC and WebTransport.
    pub certificate_file: Option<String>,
    /// PEM private key corresponding to `certificate_file`.
    pub private_key_file: Option<String>,
}

impl TransportTlsConfig {
    /// Whether both PEM paths were provided.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.certificate_file.is_some() && self.private_key_file.is_some()
    }

    fn validate(&self) -> AppResult<()> {
        match (&self.certificate_file, &self.private_key_file) {
            (None, None) => Ok(()),
            (Some(cert), Some(key)) if !cert.trim().is_empty() && !key.trim().is_empty() => Ok(()),
            _ => Err(AppError::config(
                "transport.tls.certificate_file and transport.tls.private_key_file must be set together and not be empty",
            )),
        }
    }
}

/// Authoritative transform-sync settings (, design
/// `docs/architecture/transform-sync.md`).
///
/// When `enabled`, the shared gateway attaches a transform-sync hub: clients that
/// send `KIND_TSYNC_HELLO` negotiate the world/precision and then receive
/// per-client delta snapshots on the unreliable path at `send_rate_hz`, built
/// from a world advanced at `sim_hz`. `demo_movers` optionally spawns N
/// server-simulated circling avatars so the palpable two-client demo works with
/// no game script.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransformSyncConfig {
    /// Whether transform sync is active. Off by default (additive to the legacy
    /// `KIND_POSITION` relay, which is unaffected).
    pub enabled: bool,
    /// Snapshot send rate (packets/sec). Clamped to at least 1.
    pub send_rate_hz: u8,
    /// Server simulation rate (ticks/sec). Clamped to at least 1.
    pub sim_hz: u8,
    /// Max object updates per snapshot. Defaults to 16 so a full baseline stays
    /// below the conservative 1,200-byte QUIC datagram payload budget; `0`
    /// deliberately opts into an unbounded application budget.
    pub budget: usize,
    /// Number of built-in server-simulated demo avatars to spawn (`0` = none).
    ///
    /// These move on their own (no client input) so a two-client demo shows
    /// smooth interpolation with no game script. Ignored when `player_slots > 0`.
    pub demo_movers: usize,
    /// Number of client-owned player slots (`0` = none). When `> 0`, the server
    /// hands each connecting transform-sync client ownership of one object
    /// (`OwnerPredicted`), by join order, from the id pool `1..=player_slots`,
    /// and releases it on disconnect. The owner drives that object with input
    /// (client-side prediction + server authority) while every other client sees
    /// it interpolated. Takes precedence over `demo_movers` (they would share the
    /// same low object ids), so a node runs one demo mode or the other.
    pub player_slots: u32,
    /// Networked-Actor archetypes whose owners use server-authoritative movement
    /// with client-side prediction. Every other Networked-Actor archetype keeps
    /// the byte-identical `Relay` default and continues to send `KIND_NA_STATE`.
    ///
    /// The input path reuses `KIND_TSYNC_INPUT`; this list is server policy, not
    /// a client-controlled wire flag, so a client cannot opt itself out of
    /// authoritative validation.
    pub predicted_authoritative_archetypes: Vec<u16>,
}

impl Default for TransformSyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            send_rate_hz: 20,
            sim_hz: 60,
            budget: 16,
            demo_movers: 0,
            player_slots: 0,
            predicted_authoritative_archetypes: Vec::new(),
        }
    }
}

impl TransformSyncConfig {
    /// The snapshot send period, or `None` when disabled.
    #[must_use]
    pub fn send_period(&self) -> Option<std::time::Duration> {
        if !self.enabled {
            return None;
        }
        Some(std::time::Duration::from_secs_f64(
            1.0 / f64::from(self.send_rate_hz.max(1)),
        ))
    }

    /// Seconds per sim tick.
    #[must_use]
    pub fn sim_dt(&self) -> f32 {
        1.0 / f32::from(self.sim_hz.max(1))
    }

    /// Wall-clock spacing between **sim** steps (`1 / sim_hz`). `None` when
    /// disabled. The transform loop advances the world every sim step (so
    /// `server_tick` and the physics run at `sim_hz`, matching the `sim_rate_hz`
    /// advertised in the `HELLO`), and emits a snapshot only every
    /// [`snapshot_every`](Self::snapshot_every) steps.
    #[must_use]
    pub fn sim_period(&self) -> Option<std::time::Duration> {
        if !self.enabled {
            return None;
        }
        Some(std::time::Duration::from_secs_f64(
            1.0 / f64::from(self.sim_hz.max(1)),
        ))
    }

    /// How many sim steps elapse per emitted snapshot (`round(sim_hz /
    /// send_rate_hz)`, at least 1). Keeps the world ticking at `sim_hz` while
    /// snapshots go out at ~`send_rate_hz`; if `send_rate_hz >= sim_hz` a snapshot
    /// is emitted every sim step.
    #[must_use]
    pub fn snapshot_every(&self) -> u32 {
        let sim = f64::from(self.sim_hz.max(1));
        let send = f64::from(self.send_rate_hz.max(1));
        ((sim / send).round() as u32).max(1)
    }
}

/// Realtime authentication handshake stance.
///
/// Governs how a connecting client is bound to an account. The handshake itself
/// (a `KIND_AUTH` first frame carrying the session token) is uniform across all
/// transports; this only decides which outcomes are accepted:
///
/// - `require_auth = false`, `allow_guests = true` (the default): a valid token
///   authenticates; an explicit token-less connect is accepted as a guest, and —
///   for backwards compatibility with pre-handshake clients/demos — a first frame
///   that is not a handshake is also accepted as an implicit guest and processed
///   normally. An *invalid* token is always rejected (never downgraded to guest).
/// - `require_auth = true`: only a valid token is accepted; guest and token-less
///   connects are refused. `allow_guests` is ignored in this mode (auth-required
///   never falls back to guest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Whether a valid session token is required to connect. When `true`, guest
    /// and token-less connects are rejected.
    pub require_auth: bool,
    /// Whether anonymous/guest participants are accepted (ignored when
    /// `require_auth` is `true`). Enables the token-less demo relay by default.
    pub allow_guests: bool,
    /// How long (milliseconds) to wait for the client's handshake frame before
    /// closing the connection. Bounds a client that connects but never presents
    /// a `KIND_AUTH` frame. Clamped to at least 1ms.
    pub handshake_timeout_ms: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            require_auth: false,
            allow_guests: true,
            handshake_timeout_ms: 5_000,
        }
    }
}

impl AuthConfig {
    /// The handshake wait as a [`Duration`](std::time::Duration), clamped to at
    /// least 1ms.
    #[must_use]
    pub fn handshake_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.handshake_timeout_ms.max(1))
    }
}

/// QUIC transport listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuicConfig {
    /// Whether the QUIC listener is started.
    pub enabled: bool,
    /// UDP socket address the QUIC endpoint binds to.
    pub bind: String,
    /// Per-connection outbound queue capacity (envelopes).
    pub outbound_queue_capacity: usize,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:7351".to_string(),
            outbound_queue_capacity: 1024,
        }
    }
}

/// WebSocket fallback transport listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketConfig {
    /// Whether the WebSocket listener is started.
    pub enabled: bool,
    /// TCP socket address the WebSocket endpoint binds to.
    pub bind: String,
    /// Per-connection outbound queue capacity (envelopes).
    pub outbound_queue_capacity: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:7352".to_string(),
            outbound_queue_capacity: 1024,
        }
    }
}

/// WebTransport (browser) transport listener settings.
///
/// WebTransport negotiates the HTTP/3 ALPN `h3` and therefore runs on its own
/// UDP endpoint, separate from native QUIC. Disabled by default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebTransportConfig {
    /// Whether the WebTransport listener is started.
    pub enabled: bool,
    /// UDP socket address the WebTransport endpoint binds to.
    pub bind: String,
    /// Per-connection outbound queue capacity (envelopes).
    pub outbound_queue_capacity: usize,
}

impl Default for WebTransportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:7353".to_string(),
            outbound_queue_capacity: 1024,
        }
    }
}

/// Node identity and lifecycle settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Stable identifier for this node within a (future) cluster.
    pub node_id: String,
    /// Address other nodes/clients use to reach this node.
    pub public_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: "dev-1".to_string(),
            public_addr: "127.0.0.1:7350".to_string(),
        }
    }
}

/// HTTP listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// Socket address the HTTP server binds to.
    pub bind: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7350".to_string(),
        }
    }
}

/// Admin console authentication settings.
///
/// Static operator credentials, Nakama-style: `username` + `password` grant the
/// `admin` role (full access); the optional `viewer_password` grants the
/// read-only `viewer` role for the same username. Console sessions are opaque
/// bearer tokens that expire after `token_expiry_sec`.
///
/// The defaults (`admin` / `password`) exist so the drop-and-run flow works out
/// of the box; the startup banner warns while they are unchanged.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsoleConfig {
    /// Operator login username.
    pub username: String,
    /// Operator password granting the `admin` role. Also settable via
    /// `CITADEL_CONSOLE_PASSWORD`.
    pub password: String,
    /// Optional password granting the read-only `viewer` role.
    pub viewer_password: Option<String>,
    /// Console session token lifetime, in whole seconds.
    pub token_expiry_sec: u64,
}

/// Redacted `Debug`: never prints console passwords. `Config` derives `Debug`
/// and embeds this section, so a stray `{config:?}` must not leak credentials.
impl std::fmt::Debug for ConsoleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field(
                "viewer_password",
                &self.viewer_password.as_ref().map(|_| "<redacted>"),
            )
            .field("token_expiry_sec", &self.token_expiry_sec)
            .finish()
    }
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        Self {
            username: "admin".to_string(),
            password: "password".to_string(),
            viewer_password: None,
            token_expiry_sec: 3_600,
        }
    }
}

impl ConsoleConfig {
    /// Whether the section still carries the insecure built-in credentials.
    ///
    /// Used by the startup banner to warn operators; never echoed with values.
    #[must_use]
    pub fn uses_default_credentials(&self) -> bool {
        let d = Self::default();
        self.username == d.username && self.password == d.password
    }

    /// Validate the console section.
    ///
    /// Errors name the offending field without echoing secrets.
    fn validate(&self) -> AppResult<()> {
        if self.username.trim().is_empty() {
            return Err(AppError::config("console.username must not be empty"));
        }
        if self.password.is_empty() {
            return Err(AppError::config("console.password must not be empty"));
        }
        if self.viewer_password.as_deref() == Some("") {
            return Err(AppError::config(
                "console.viewer_password must not be empty when set (omit the key to disable the viewer role)",
            ));
        }
        if self.token_expiry_sec == 0 {
            return Err(AppError::config("console.token_expiry_sec must be >= 1"));
        }
        Ok(())
    }
}

/// Supported log output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Compact, human-readable logs for local development.
    #[default]
    Pretty,
    /// Structured JSON logs for production aggregation.
    Json,
}

/// Logging and tracing settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Log level filter directive (e.g. `info`, `debug`, `citadel=trace`).
    pub level: String,
    /// Output format.
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Pretty,
        }
    }
}

/// Narrow CLI flag overrides applied last in the precedence chain.
///
/// Only high-signal startup options are exposed as flags; most settings belong
/// in files or environment variables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigOverrides {
    /// Override `logging.level`.
    pub log_level: Option<String>,
    /// Override `http.bind`.
    pub bind: Option<String>,
    /// Override `server.node_id`.
    pub node_id: Option<String>,
}

impl ConfigOverrides {
    /// Whether any override is set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.log_level.is_none() && self.bind.is_none() && self.node_id.is_none()
    }
}

impl LogFormat {
    /// Stable lowercase token used in config and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pretty => "pretty",
            Self::Json => "json",
        }
    }
}

impl Config {
    /// Parse a config from a TOML string layered over defaults.
    ///
    /// Returns a [`Config`](crate::error::ErrorCategory::Config) error on
    /// malformed TOML or unknown keys.
    pub fn from_toml_str(contents: &str) -> AppResult<Self> {
        toml::from_str(contents)
            .map_err(|e| AppError::config("failed to parse config TOML").with_detail(e.to_string()))
    }

    /// Serialize this config to pretty TOML and write it to `path`.
    ///
    /// Used by the first-run wizard to persist a chosen database so the next run
    /// is non-interactive. Parent directories are created as needed. Any existing
    /// file is overwritten with the full resolved config.
    ///
    /// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the
    /// config cannot be serialized or the file cannot be written. The path is
    /// included in diagnostics (paths are not secret); the connection URL, which
    /// may carry credentials, is written to the file the operator asked for but
    /// never echoed in error messages.
    pub fn write_to(&self, path: &Path) -> AppResult<()> {
        let toml = toml::to_string_pretty(self).map_err(|e| {
            AppError::config("failed to serialize config to TOML").with_detail(e.to_string())
        })?;
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                AppError::config(format!(
                    "failed to create config directory: {}",
                    parent.display()
                ))
                .with_detail(e.to_string())
            })?;
        }
        std::fs::write(path, toml).map_err(|e| {
            AppError::config(format!("failed to write config file: {}", path.display()))
                .with_detail(e.to_string())
        })
    }

    /// Read and parse a config file from disk.
    ///
    /// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the
    /// file cannot be read or parsed. The file path is included in the message
    /// (paths are not secret); contents are not echoed.
    pub fn from_file(path: &Path) -> AppResult<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            AppError::config(format!("cannot read config file: {}", path.display()))
                .with_detail(e.to_string())
        })?;
        Self::from_toml_str(&contents)
    }

    /// Apply `CITADEL_`-prefixed environment overrides in place.
    ///
    /// Supported keys: `CITADEL_LOG_LEVEL`, `CITADEL_HTTP_BIND`,
    /// `CITADEL_NODE_ID`, `CITADEL_PUBLIC_ADDR`, `CITADEL_DATABASE_URL`,
    /// `CITADEL_CONSOLE_PASSWORD`. Unknown `CITADEL_` variables are ignored so
    /// future keys do not break older binaries.
    fn apply_env(&mut self, vars: &[(String, String)]) {
        for (key, value) in vars {
            match key.as_str() {
                "CITADEL_LOG_LEVEL" => self.logging.level = value.clone(),
                "CITADEL_HTTP_BIND" => self.http.bind = value.clone(),
                "CITADEL_NODE_ID" => self.server.node_id = value.clone(),
                "CITADEL_PUBLIC_ADDR" => self.server.public_addr = value.clone(),
                "CITADEL_DATABASE_URL" => self.database.url = Some(value.clone()),
                "CITADEL_CONSOLE_PASSWORD" => self.console.password = value.clone(),
                _ => {}
            }
        }
    }

    /// Apply CLI flag overrides in place (highest precedence).
    fn apply_overrides(&mut self, overrides: &ConfigOverrides) {
        if let Some(level) = &overrides.log_level {
            self.logging.level = level.clone();
        }
        if let Some(bind) = &overrides.bind {
            self.http.bind = bind.clone();
        }
        if let Some(node_id) = &overrides.node_id {
            self.server.node_id = node_id.clone();
        }
    }

    /// Resolve configuration through the full precedence chain.
    ///
    /// Order: defaults, then the config file, then `CITADEL_` environment
    /// variables, then CLI `overrides`. The resolved config is validated before
    /// being returned.
    ///
    /// The config file is selected as follows: an explicit `path` (from
    /// `--config`) is always used as-is. When `path` is `None`, a `citadel.toml`
    /// in the current working directory is discovered and loaded if present
    /// (the zero-flag "unzip and run" default); otherwise the built-in defaults
    /// are used. Explicit-`--config` behavior is therefore unchanged.
    pub fn load(path: Option<&Path>, overrides: &ConfigOverrides) -> AppResult<Self> {
        // Only discover a default config when no explicit path was given.
        let discovered = match path {
            Some(_) => None,
            None => std::env::current_dir()
                .ok()
                .and_then(|dir| discover_config_in(&dir)),
        };
        let effective = path.or(discovered.as_deref());
        let env: Vec<(String, String)> = std::env::vars()
            .filter(|(k, _)| k.starts_with("CITADEL_"))
            .collect();
        Self::load_from(effective, &env, overrides)
    }

    /// Resolve configuration with an explicit environment slice.
    ///
    /// Pure relative to process environment so precedence is unit-testable.
    pub fn load_from(
        path: Option<&Path>,
        env: &[(String, String)],
        overrides: &ConfigOverrides,
    ) -> AppResult<Self> {
        let mut config = match path {
            Some(path) => Self::from_file(path)?,
            None => Self::default(),
        };
        config.apply_env(env);
        config.apply_overrides(overrides);
        config.validate()?;
        Ok(config)
    }

    /// Validate the resolved configuration.
    ///
    /// Checks socket address syntax and required non-empty identifiers. Errors
    /// map to the [`Config`](crate::error::ErrorCategory::Config) category and
    /// name the offending field without echoing secrets.
    pub fn validate(&self) -> AppResult<()> {
        if self.server.node_id.trim().is_empty() {
            return Err(AppError::config("server.node_id must not be empty"));
        }
        validate_socket_addr("http.bind", &self.http.bind)?;
        validate_socket_addr("server.public_addr", &self.server.public_addr)?;
        if self.logging.level.trim().is_empty() {
            return Err(AppError::config("logging.level must not be empty"));
        }
        self.cluster
            .validate(&self.server.node_id, &self.database)?;
        self.transport.tls.validate()?;
        if self.transport.quic.enabled {
            validate_socket_addr("transport.quic.bind", &self.transport.quic.bind)?;
            if self.transport.quic.outbound_queue_capacity == 0 {
                return Err(AppError::config(
                    "transport.quic.outbound_queue_capacity must be >= 1",
                ));
            }
        }
        if self.transport.websocket.enabled {
            validate_socket_addr("transport.websocket.bind", &self.transport.websocket.bind)?;
            if self.transport.websocket.outbound_queue_capacity == 0 {
                return Err(AppError::config(
                    "transport.websocket.outbound_queue_capacity must be >= 1",
                ));
            }
        }
        if self.transport.webtransport.enabled {
            validate_socket_addr(
                "transport.webtransport.bind",
                &self.transport.webtransport.bind,
            )?;
            if self.transport.webtransport.outbound_queue_capacity == 0 {
                return Err(AppError::config(
                    "transport.webtransport.outbound_queue_capacity must be >= 1",
                ));
            }
        }
        if self.runtime.enabled {
            self.runtime.validate_hosting()?;
            if self.runtime.scripts_dir.trim().is_empty() {
                return Err(AppError::config(
                    "runtime.scripts_dir must not be empty when the runtime is enabled",
                ));
            }
            if self
                .runtime
                .static_data_dir
                .as_deref()
                .is_some_and(|directory| directory.trim().is_empty())
            {
                return Err(AppError::config(
                    "runtime.static_data_dir must not be empty when set",
                ));
            }
            if self.runtime.static_data_dir.is_some()
                && self.runtime.static_data_max_file_bytes == 0
            {
                return Err(AppError::config(
                    "runtime.static_data_max_file_bytes must be >= 1 when static_data_dir is set",
                ));
            }
            if self.runtime.deadline_ms == 0 {
                return Err(AppError::config("runtime.deadline_ms must be >= 1"));
            }
            if self.runtime.tick_deadline_ms == Some(0) {
                return Err(AppError::config(
                    "runtime.tick_deadline_ms must be >= 1 when set",
                ));
            }
            if self.runtime.hot_reload && self.runtime.hot_reload_poll_ms == 0 {
                return Err(AppError::config(
                    "runtime.hot_reload_poll_ms must be >= 1 when hot_reload is enabled",
                ));
            }
        }
        self.storage.index_definitions()?;
        self.chat.limits.validate()?;
        self.authentication.limits.validate()?;
        self.database.validate()?;
        self.console.validate()?;
        Ok(())
    }
}

impl DatabaseConfig {
    /// Validate the database section.
    ///
    /// Only enforced when a `url` is present; an absent section is always valid
    /// (the node runs on the in-memory backend). The URL scheme is checked (via
    /// [`DatabaseConfig::backend`]) but its contents are never echoed in error
    /// messages (it may carry credentials). Errors map to the
    /// [`Config`](crate::error::ErrorCategory::Config) category.
    fn validate(&self) -> AppResult<()> {
        if let Some(url) = &self.url {
            if url.trim().is_empty() {
                return Err(AppError::config(
                    "database.url must not be empty when set (omit the key to disable persistence)",
                ));
            }
            // Rejects unrecognized schemes (e.g. `mysql://`) without echoing the URL.
            let backend = self.backend()?;
            if self.max_connections == 0 {
                return Err(AppError::config("database.max_connections must be >= 1"));
            }
            if self.connect_timeout_ms == 0 {
                return Err(AppError::config("database.connect_timeout_ms must be >= 1"));
            }
            if self.acquire_timeout_ms == 0 {
                return Err(AppError::config("database.acquire_timeout_ms must be >= 1"));
            }
            if backend == Some(DatabaseBackend::MongoDb) {
                self.validate_mongodb_policy()?;
            }
        }
        Ok(())
    }
}

impl RuntimeConfig {
    /// Validate runtime adapter/tier combinations implemented by this build.
    fn validate_hosting(&self) -> AppResult<()> {
        if self.adapter != RuntimeAdapter::Embedded {
            return Err(AppError::config(format!(
                "runtime.adapter '{}' is not implemented yet; use 'embedded'",
                self.adapter.as_str()
            )));
        }
        if self.tier != RuntimeTier::Trusted {
            return Err(AppError::config(format!(
                "runtime.tier '{}' is not implemented yet; use 'trusted'",
                self.tier.as_str()
            )));
        }
        Ok(())
    }

    /// Resolve the active runtime language, if an entrypoint exists.
    ///
    /// `runtime.language` has precedence over autodetection. Without an explicit
    /// language, exactly one known entrypoint may exist in `scripts_dir`; multiple
    /// entrypoints are a config error so the operator picks one deliberately.
    pub fn resolve_selection(&self) -> AppResult<Option<RuntimeSelection>> {
        if !self.enabled {
            return Ok(None);
        }
        self.validate_hosting()?;
        let scripts_dir = Path::new(&self.scripts_dir);
        match self.language {
            Some(language) => self.resolve_explicit_selection(scripts_dir, language),
            None => self.resolve_autodetected_selection(scripts_dir),
        }
    }

    fn resolve_explicit_selection(
        &self,
        scripts_dir: &Path,
        language: RuntimeLanguage,
    ) -> AppResult<Option<RuntimeSelection>> {
        let mut found = Vec::new();
        for entry_file in language.entry_files() {
            let entrypoint = scripts_dir.join(entry_file);
            if entrypoint.is_file() {
                found.push(entrypoint);
            }
        }
        if found.len() > 1 {
            return Err(AppError::config(format!(
                "multiple runtime entrypoints for language '{}': {}; remove extras",
                language.as_str(),
                format_paths(&found)
            )));
        }
        Ok(found.into_iter().next().map(|entrypoint| RuntimeSelection {
            language,
            adapter: self.adapter,
            tier: self.tier,
            entrypoint,
            source: RuntimeSelectionSource::Explicit,
        }))
    }

    fn resolve_autodetected_selection(
        &self,
        scripts_dir: &Path,
    ) -> AppResult<Option<RuntimeSelection>> {
        for language in RuntimeLanguage::autodetect_order() {
            for entry_file in language.entry_files() {
                let entrypoint = scripts_dir.join(entry_file);
                if entrypoint.is_file() {
                    return Ok(Some(RuntimeSelection {
                        language: *language,
                        adapter: self.adapter,
                        tier: self.tier,
                        entrypoint,
                        source: RuntimeSelectionSource::Autodetected,
                    }));
                }
            }
        }
        Ok(None)
    }

    /// The wall-clock period between ticks, or `None` when the tick is disabled.
    #[must_use]
    pub fn tick_period(&self) -> Option<std::time::Duration> {
        if self.tick_hz == 0 {
            return None;
        }
        Some(std::time::Duration::from_secs_f64(
            1.0 / f64::from(self.tick_hz),
        ))
    }

    /// The effective per-tick time budget for `period`.
    ///
    /// Uses the explicit `tick_deadline_ms` when set, otherwise derives
    /// `min(50ms, period / 2)` clamped to at least 1ms so the tick has an SLO
    /// distinct from the message `deadline_ms`.
    #[must_use]
    pub fn tick_budget(&self, period: std::time::Duration) -> std::time::Duration {
        use std::time::Duration;
        match self.tick_deadline_ms {
            Some(ms) => Duration::from_millis(ms.max(1)),
            None => (period / 2)
                .min(Duration::from_millis(50))
                .max(Duration::from_millis(1)),
        }
    }

    /// The script-watch poll interval, or `None` when hot-reload is disabled.
    ///
    /// Returns `Some` only when `hot_reload` is on; the interval is clamped to at
    /// least 1ms so a misconfigured `0` (already a validation error) never yields
    /// a zero-period timer.
    #[must_use]
    pub fn hot_reload_interval(&self) -> Option<std::time::Duration> {
        if !self.hot_reload {
            return None;
        }
        Some(std::time::Duration::from_millis(
            self.hot_reload_poll_ms.max(1),
        ))
    }
}

fn format_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Validate that `value` parses as a socket address, naming `field` on failure.
fn validate_socket_addr(field: &str, value: &str) -> AppResult<()> {
    value.parse::<SocketAddr>().map(|_| ()).map_err(|e| {
        AppError::config(format!("{field} is not a valid socket address: {value}"))
            .with_detail(e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_documented_local_defaults() {
        let config = Config::default();
        assert_eq!(config.server.node_id, "dev-1");
        assert_eq!(config.server.public_addr, "127.0.0.1:7350");
        assert_eq!(config.http.bind, "127.0.0.1:7350");
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, LogFormat::Pretty);
    }

    #[test]
    fn empty_toml_yields_defaults() {
        let config: Config = toml::from_str("").expect("empty toml parses");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn partial_toml_overrides_only_specified_fields() {
        let toml_src = r#"
            [logging]
            level = "debug"
            format = "json"
        "#;
        let config: Config = toml::from_str(toml_src).expect("partial toml parses");
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.format, LogFormat::Json);
        // Unspecified sections fall back to defaults.
        assert_eq!(config.server, ServerConfig::default());
        assert_eq!(config.http, HttpConfig::default());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let toml_src = r#"
            [logging]
            level = "info"
            not_a_real_key = true
        "#;
        let result: Result<Config, _> = toml::from_str(toml_src);
        assert!(result.is_err(), "unknown keys must be rejected");
    }

    #[test]
    fn config_round_trips_through_toml() {
        let original = Config::default();
        let serialized = toml::to_string(&original).expect("serialize");
        let parsed: Config = toml::from_str(&serialized).expect("deserialize");
        assert_eq!(original, parsed);
    }

    #[test]
    fn default_config_validates() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn invalid_bind_address_is_rejected() {
        let mut config = Config::default();
        config.http.bind = "not-an-address".to_string();
        let err = config.validate().expect_err("invalid bind must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(err.message().contains("http.bind"));
    }

    #[test]
    fn empty_node_id_is_rejected() {
        let mut config = Config::default();
        config.server.node_id = "  ".to_string();
        let err = config.validate().expect_err("empty node id must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn from_toml_str_reports_config_category_on_bad_toml() {
        let err = Config::from_toml_str("this is = = invalid").expect_err("bad toml");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn load_from_applies_precedence_defaults_env_overrides() {
        // No file: start from defaults. Env overrides level and bind; CLI
        // override wins over env for bind.
        let env = vec![
            ("CITADEL_LOG_LEVEL".to_string(), "debug".to_string()),
            (
                "CITADEL_HTTP_BIND".to_string(),
                "127.0.0.1:9000".to_string(),
            ),
            ("CITADEL_UNKNOWN".to_string(), "ignored".to_string()),
        ];
        let overrides = ConfigOverrides {
            bind: Some("127.0.0.1:9999".to_string()),
            ..ConfigOverrides::default()
        };
        let config = Config::load_from(None, &env, &overrides).expect("loads");
        assert_eq!(config.logging.level, "debug"); // from env
        assert_eq!(config.http.bind, "127.0.0.1:9999"); // CLI override beats env
        assert_eq!(config.server.node_id, "dev-1"); // default
    }

    #[test]
    fn load_from_validates_result() {
        let overrides = ConfigOverrides {
            bind: Some("bogus".to_string()),
            ..ConfigOverrides::default()
        };
        let err = Config::load_from(None, &[], &overrides).expect_err("invalid bind");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    /// A process/time-unique temp directory path (not created).
    fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("citadel-{tag}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn discover_config_in_finds_present_file_and_falls_back_otherwise() {
        // An empty temp dir: nothing to discover.
        let dir = unique_temp_dir("cfg-discovery");
        std::fs::create_dir_all(&dir).expect("temp dir");
        assert!(
            discover_config_in(&dir).is_none(),
            "no citadel.toml => discovery returns None"
        );

        // Once a citadel.toml exists, discovery returns its path.
        let cfg = dir.join(DEFAULT_CONFIG_FILE);
        std::fs::write(&cfg, "").expect("write citadel.toml");
        assert_eq!(
            discover_config_in(&dir).as_deref(),
            Some(cfg.as_path()),
            "citadel.toml present => discovery returns its path"
        );
        // A discovered (empty) file parses to the built-in defaults.
        let loaded = Config::from_file(&cfg).expect("discovered config loads");
        assert_eq!(loaded, Config::default());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discover_config_ignores_a_directory_named_citadel_toml() {
        // A *directory* named citadel.toml must not be treated as a config file.
        let dir = unique_temp_dir("cfg-dir");
        std::fs::create_dir_all(dir.join(DEFAULT_CONFIG_FILE)).expect("dir named citadel.toml");
        assert!(discover_config_in(&dir).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_config_file_reports_config_error() {
        let path = std::path::Path::new("/nonexistent/citadel/does-not-exist.toml");
        let err = Config::from_file(path).expect_err("missing file");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn overrides_is_empty_reports_correctly() {
        assert!(ConfigOverrides::default().is_empty());
        assert!(
            !ConfigOverrides {
                node_id: Some("n".to_string()),
                ..ConfigOverrides::default()
            }
            .is_empty()
        );
    }

    #[test]
    fn log_format_as_str_is_stable() {
        assert_eq!(LogFormat::Pretty.as_str(), "pretty");
        assert_eq!(LogFormat::Json.as_str(), "json");
    }

    #[test]
    fn runtime_defaults_enable_the_game_folder() {
        let rc = RuntimeConfig::default();
        assert!(rc.enabled);
        assert_eq!(rc.language, None);
        assert_eq!(rc.adapter, RuntimeAdapter::Embedded);
        assert_eq!(rc.tier, RuntimeTier::Trusted);
        assert_eq!(rc.lua_execution_mode, LuaExecutionMode::Sandboxed);
        assert_eq!(rc.scripts_dir, "./game");
        assert_eq!(rc.static_data_dir, None);
        assert_eq!(
            rc.static_data_max_file_bytes,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES
        );
        assert_eq!(rc.deadline_ms, crate::runtime::DEFAULT_DEADLINE_MS);
        // A default config (runtime enabled) still validates.
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn live_matchmaker_cluster_requires_durable_storage_and_mtls_paths() {
        let mut config = Config {
            cluster: ClusterConfig {
                enabled: true,
                control_bind: "127.0.0.1:7390".to_owned(),
                matchmaker_shard: 0,
                lease_ttl_ms: 5_000,
                handoff_ttl_ms: 30_000,
                command_timeout_ms: 2_000,
                peers: vec![ClusterPeerConfig {
                    node_id: "node-b".to_owned(),
                    control_addr: "127.0.0.1:7391".to_owned(),
                    server_name: "node-b.test".to_owned(),
                    certificate_file: "node-b.pem".to_owned(),
                }],
                tls: ClusterTlsConfig {
                    ca_certificate_file: "cluster-ca.pem".to_owned(),
                    certificate_file: "node-a.pem".to_owned(),
                    private_key_file: "node-a-key.pem".to_owned(),
                },
            },
            database: DatabaseConfig {
                url: Some("sqlite::memory:".to_owned()),
                ..DatabaseConfig::default()
            },
            ..Config::default()
        };
        assert!(config.validate().is_ok());
        config.cluster.tls.ca_certificate_file.clear();
        assert!(config.validate().is_err());
        config.cluster.tls.ca_certificate_file = "cluster-ca.pem".to_owned();
        config.database.url = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn runtime_config_parses_from_toml() {
        let toml_src = r#"
            [runtime]
            enabled = true
            language = "lua"
            adapter = "embedded"
            tier = "trusted"
            scripts_dir = "/srv/game"
            deadline_ms = 250
        "#;
        let config: Config = toml::from_str(toml_src).expect("runtime toml parses");
        assert_eq!(config.runtime.language, Some(RuntimeLanguage::Lua));
        assert_eq!(config.runtime.adapter, RuntimeAdapter::Embedded);
        assert_eq!(config.runtime.tier, RuntimeTier::Trusted);
        assert_eq!(config.runtime.scripts_dir, "/srv/game");
        assert_eq!(config.runtime.deadline_ms, 250);
    }

    #[test]
    fn runtime_selection_autodetects_lua_entrypoint() {
        let dir = unique_temp_dir("runtime-detect-lua");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.lua"), "-- lua").expect("main.lua");
        let rc = RuntimeConfig {
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };

        let selection = rc
            .resolve_selection()
            .expect("selection resolves")
            .expect("entrypoint detected");

        assert_eq!(selection.language, RuntimeLanguage::Lua);
        assert_eq!(selection.source, RuntimeSelectionSource::Autodetected);
        assert_eq!(selection.entrypoint, dir.join("main.lua"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn runtime_selection_none_when_no_entrypoint_exists() {
        let dir = unique_temp_dir("runtime-detect-none");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let rc = RuntimeConfig {
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };

        assert_eq!(rc.resolve_selection().expect("selection resolves"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn runtime_selection_uses_lua_first_when_multiple_entrypoints_exist() {
        let dir = unique_temp_dir("runtime-detect-priority");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.lua"), "-- lua").expect("main.lua");
        std::fs::write(dir.join("main.py"), "# python").expect("main.py");
        std::fs::write(dir.join("main.js"), "// js").expect("main.js");
        let rc = RuntimeConfig {
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };

        let selection = rc
            .resolve_selection()
            .expect("selection resolves")
            .expect("entrypoint detected");
        assert_eq!(selection.language, RuntimeLanguage::Lua);
        assert_eq!(selection.source, RuntimeSelectionSource::Autodetected);
        assert_eq!(selection.entrypoint, dir.join("main.lua"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_runtime_language_takes_precedence_over_autodetect_conflict() {
        let dir = unique_temp_dir("runtime-explicit-precedence");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.lua"), "-- lua").expect("main.lua");
        std::fs::write(dir.join("main.py"), "# python").expect("main.py");
        let rc = RuntimeConfig {
            language: Some(RuntimeLanguage::Python),
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };

        let selection = rc
            .resolve_selection()
            .expect("explicit language resolves")
            .expect("python entrypoint selected");

        assert_eq!(selection.language, RuntimeLanguage::Python);
        assert_eq!(selection.source, RuntimeSelectionSource::Explicit);
        assert_eq!(selection.entrypoint, dir.join("main.py"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn runtime_selection_accepts_explicit_javascript_entrypoint() {
        let dir = unique_temp_dir("runtime-explicit-js");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.js"), "// js").expect("main.js");
        let rc = RuntimeConfig {
            language: Some(RuntimeLanguage::Js),
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };

        let selection = rc
            .resolve_selection()
            .expect("explicit js selection resolves")
            .expect("js entrypoint selected");
        assert_eq!(selection.language, RuntimeLanguage::Js);
        assert_eq!(selection.source, RuntimeSelectionSource::Explicit);
        assert_eq!(selection.entrypoint, dir.join("main.js"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn runtime_config_accepts_javascript_alias() {
        let dir = unique_temp_dir("runtime-js-alias");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.js"), "// js").expect("main.js");
        let scripts_dir = dir.to_string_lossy().replace('\\', "/");
        let toml_src = format!(
            r#"
            [runtime]
            enabled = true
            language = "javascript"
            scripts_dir = "{scripts_dir}"
        "#
        );
        let config: Config = toml::from_str(&toml_src).expect("runtime toml parses");
        assert_eq!(config.runtime.language, Some(RuntimeLanguage::Js));
        let selection = config
            .runtime
            .resolve_selection()
            .expect("alias resolves")
            .expect("js entrypoint selected");
        assert_eq!(selection.language, RuntimeLanguage::Js);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn enabled_runtime_rejects_unimplemented_adapter_and_tier() {
        let config = Config {
            runtime: RuntimeConfig {
                adapter: RuntimeAdapter::ExternalWorker,
                ..RuntimeConfig::default()
            },
            ..Config::default()
        };
        let err = config
            .validate()
            .expect_err("external worker not implemented");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(err.message().contains("runtime.adapter"));

        let config = Config {
            runtime: RuntimeConfig {
                tier: RuntimeTier::Hardened,
                ..RuntimeConfig::default()
            },
            ..Config::default()
        };
        let err = config.validate().expect_err("hardened not implemented");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(err.message().contains("runtime.tier"));
    }

    #[test]
    fn runtime_tick_defaults_are_disabled() {
        let rc = RuntimeConfig::default();
        assert_eq!(rc.tick_hz, 0);
        assert_eq!(rc.tick_deadline_ms, None);
        assert!(
            rc.tick_period().is_none(),
            "tick_hz 0 => no period => disabled"
        );
    }

    #[test]
    fn tick_config_parses_and_derives_budget() {
        let toml_src = r#"
            [runtime]
            tick_hz = 20
        "#;
        let config: Config = toml::from_str(toml_src).expect("tick toml parses");
        let period = config.runtime.tick_period().expect("20 Hz has a period");
        assert_eq!(period, std::time::Duration::from_millis(50));
        // Auto budget: min(50ms, period/2) = 25ms.
        assert_eq!(
            config.runtime.tick_budget(period),
            std::time::Duration::from_millis(25)
        );
    }

    #[test]
    fn explicit_tick_deadline_overrides_auto_budget() {
        let mut rc = RuntimeConfig {
            tick_hz: 60,
            tick_deadline_ms: Some(5),
            ..RuntimeConfig::default()
        };
        let period = rc.tick_period().expect("60 Hz has a period");
        assert_eq!(rc.tick_budget(period), std::time::Duration::from_millis(5));
        // A None budget for 60 Hz derives min(50, ~8ms) ~= 8ms (period/2).
        rc.tick_deadline_ms = None;
        assert!(rc.tick_budget(period) <= std::time::Duration::from_millis(50));
    }

    #[test]
    fn enabled_runtime_rejects_zero_tick_deadline() {
        let mut config = Config::default();
        config.runtime.tick_hz = 30;
        config.runtime.tick_deadline_ms = Some(0);
        let err = config
            .validate()
            .expect_err("explicit zero tick deadline must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn hot_reload_defaults_off_with_no_interval() {
        let rc = RuntimeConfig::default();
        assert!(!rc.hot_reload);
        assert_eq!(rc.hot_reload_poll_ms, 500);
        assert!(
            rc.hot_reload_interval().is_none(),
            "hot_reload off => no watch interval"
        );
    }

    #[test]
    fn hot_reload_interval_resolves_when_enabled() {
        let rc = RuntimeConfig {
            hot_reload: true,
            hot_reload_poll_ms: 250,
            ..RuntimeConfig::default()
        };
        assert_eq!(
            rc.hot_reload_interval(),
            Some(std::time::Duration::from_millis(250))
        );
    }

    #[test]
    fn enabled_runtime_rejects_zero_hot_reload_poll() {
        let mut config = Config::default();
        config.runtime.hot_reload = true;
        config.runtime.hot_reload_poll_ms = 0;
        let err = config
            .validate()
            .expect_err("zero poll interval with hot_reload on must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn database_section_is_optional_and_disabled_by_default() {
        let config = Config::default();
        assert_eq!(config.database, DatabaseConfig::default());
        assert!(!config.database.is_enabled());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn database_config_parses_from_toml() {
        let toml_src = r#"
            [database]
            url = "postgres://citadel:secret@localhost:5432/citadel"
            max_connections = 20
            connect_timeout_ms = 3000
            acquire_timeout_ms = 4000
        "#;
        let config: Config = toml::from_str(toml_src).expect("database toml parses");
        assert!(config.database.is_enabled());
        assert_eq!(config.database.max_connections, 20);
        assert_eq!(config.database.connect_timeout_ms, 3000);
        assert_eq!(config.database.acquire_timeout_ms, 4000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn database_url_env_enables_backend() {
        let env = vec![(
            "CITADEL_DATABASE_URL".to_string(),
            "postgres://localhost/citadel".to_string(),
        )];
        let config = Config::load_from(None, &env, &ConfigOverrides::default()).expect("loads");
        assert!(config.database.is_enabled());
        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://localhost/citadel")
        );
    }

    #[test]
    fn database_debug_never_leaks_the_connection_string() {
        let secret = "postgres://citadel:supersecret@localhost:5432/citadel";
        let config = Config {
            database: DatabaseConfig {
                url: Some(secret.to_string()),
                ..DatabaseConfig::default()
            },
            ..Config::default()
        };
        // Both the section and the whole config (as embedded in App) must redact.
        let section = format!("{:?}", config.database);
        let whole = format!("{config:?}");
        assert!(!section.contains("supersecret"));
        assert!(!whole.contains("supersecret"));
        assert!(section.contains("<redacted>"));
    }

    #[test]
    fn database_rejects_non_postgres_url() {
        let mut config = Config::default();
        config.database.url = Some("mysql://localhost/citadel".to_string());
        let err = config.validate().expect_err("non-postgres url rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        // The offending URL must not be echoed (it can carry credentials).
        assert!(!err.message().contains("mysql://localhost/citadel"));
    }

    #[test]
    fn cockroach_urls_select_the_postgres_backend_with_the_cockroach_flavor() {
        for url in [
            "cockroach://root@localhost:26257/citadel?sslmode=disable",
            "cockroachdb://root@host:26257/db",
            "  cockroach://root@localhost:26257/citadel  ", // trimmed
        ] {
            let config = DatabaseConfig {
                url: Some(url.to_string()),
                ..DatabaseConfig::default()
            };
            assert_eq!(
                config.backend().expect("classify"),
                Some(DatabaseBackend::Postgres),
                "cockroach URL routes through the Postgres backend: {url}"
            );
            assert_eq!(
                config.pg_flavor(),
                PgFlavor::Cockroach,
                "cockroach URL selects the CockroachDB flavor: {url}"
            );
            // A cockroach:// URL is a valid, enabled database configuration.
            assert!(config.validate().is_ok(), "cockroach URL validates: {url}");
        }
    }

    #[test]
    fn plain_postgres_and_non_postgres_urls_keep_the_postgres_flavor_default() {
        // Standard Postgres URLs are the Postgres flavor.
        for url in ["postgres://localhost/citadel", "postgresql://localhost/db"] {
            let config = DatabaseConfig {
                url: Some(url.to_string()),
                ..DatabaseConfig::default()
            };
            assert_eq!(config.pg_flavor(), PgFlavor::Postgres, "{url}");
        }
        // Non-Postgres / absent URLs report the default flavor (only meaningful
        // for the Postgres backend, so the default is harmless elsewhere).
        assert_eq!(
            DatabaseConfig {
                url: Some("sqlite::memory:".to_string()),
                ..DatabaseConfig::default()
            }
            .pg_flavor(),
            PgFlavor::Postgres
        );
        assert_eq!(DatabaseConfig::default().pg_flavor(), PgFlavor::Postgres);
        assert_eq!(PgFlavor::Cockroach.as_str(), "cockroach");
        assert_eq!(PgFlavor::Postgres.as_str(), "postgres");
    }

    #[test]
    fn database_rejects_zero_pool_and_timeouts_when_enabled() {
        let base = DatabaseConfig {
            url: Some("postgres://localhost/citadel".to_string()),
            ..DatabaseConfig::default()
        };
        for mutate in [
            (|c: &mut DatabaseConfig| c.max_connections = 0) as fn(&mut DatabaseConfig),
            |c: &mut DatabaseConfig| c.connect_timeout_ms = 0,
            |c: &mut DatabaseConfig| c.acquire_timeout_ms = 0,
        ] {
            let mut database = base.clone();
            mutate(&mut database);
            let config = Config {
                database,
                ..Config::default()
            };
            let err = config.validate().expect_err("invalid database tunable");
            assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        }
    }

    #[test]
    fn storage_index_config_parses_and_rejects_duplicate_names() {
        let config = Config::from_toml_str(
            r#"
[[storage.indexes]]
name = "profiles_by_score"
collection = "profiles"
fields = ["score", "region"]
"#,
        )
        .expect("parse");
        config.validate().expect("valid index config");
        let indexes = config.storage.index_definitions().expect("definitions");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name().as_str(), "profiles_by_score");

        let duplicate = Config::from_toml_str(
            r#"
[[storage.indexes]]
name = "profiles_by_score"
collection = "profiles"
fields = ["score"]

[[storage.indexes]]
name = "profiles_by_score"
collection = "profiles"
fields = ["region"]
"#,
        )
        .expect("parse duplicate");
        let err = duplicate.validate().expect_err("duplicate name rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn enabled_runtime_rejects_zero_deadline_and_empty_dir() {
        let mut config = Config::default();
        config.runtime.deadline_ms = 0;
        let err = config.validate().expect_err("zero deadline must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);

        let mut config = Config::default();
        config.runtime.scripts_dir = "   ".to_string();
        let err = config.validate().expect_err("empty scripts_dir must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn static_data_config_is_opt_in_and_requires_a_nonzero_limit() {
        let config: Config = toml::from_str(
            r#"
            [runtime]
            static_data_dir = "./common"
            static_data_max_file_bytes = 4096
            "#,
        )
        .expect("static-data TOML parses");
        assert_eq!(config.runtime.static_data_dir.as_deref(), Some("./common"));
        assert_eq!(config.runtime.static_data_max_file_bytes, 4096);
        config.validate().expect("configured static data validates");

        let mut empty = Config::default();
        empty.runtime.static_data_dir = Some("  ".to_string());
        let error = empty.validate().expect_err("blank data root is invalid");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);

        let mut zero = Config::default();
        zero.runtime.static_data_dir = Some("./common".to_string());
        zero.runtime.static_data_max_file_bytes = 0;
        let error = zero.validate().expect_err("zero data limit is invalid");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn transport_tls_paths_must_be_configured_together() {
        let configured = Config::from_toml_str(
            r#"
            [transport.tls]
            certificate_file = "/etc/citadel/fullchain.pem"
            private_key_file = "/etc/citadel/privkey.pem"
            "#,
        )
        .expect("parse");
        assert!(configured.transport.tls.is_configured());
        configured
            .validate()
            .expect("complete TLS config validates");

        let partial = Config::from_toml_str(
            r#"
            [transport.tls]
            certificate_file = "/etc/citadel/fullchain.pem"
            "#,
        )
        .expect("parse");
        let error = partial.validate().expect_err("partial TLS config rejected");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
    }
}
