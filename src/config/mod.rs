//! Typed configuration model for Citadel.
//!
//!  defined the typed structs and defaults.  adds layered
//! loading and validation per `website/src/content/docs/reference/operations/cli.md`:
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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use base64::Engine as _;
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
/// Sections mirror `website/src/content/docs/reference/operations/cli.md`. Only the sections
/// needed by the current skeleton are modeled; database, runtime, cluster, and
/// socket sections are introduced by their owning tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Node identity and lifecycle settings.
    pub server: ServerConfig,
    /// HTTP listener settings.
    pub http: HttpConfig,
    /// Opt-in capture and private artifact ingestion for lag diagnostics.
    pub lag_diagnostics: LagDiagnosticsConfig,
    /// Logging and tracing settings.
    pub logging: LoggingConfig,
    /// Local incident-journal retention settings.
    pub errors: ErrorJournalConfig,
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
    /// Admission window for `POST /console/v1/login`. Deliberately much tighter
    /// than the player rules: the operator credential is static, unhashed, and
    /// grants full read/write over every console section.
    pub console_login: AuthRateLimitRule,
}

impl Default for AuthLimitsConfig {
    fn default() -> Self {
        Self {
            source: AuthRateLimitRule::new(30, 60_000),
            email: AuthRateLimitRule::new(10, 900_000),
            registration_source: AuthRateLimitRule::new(10, 3_600_000),
            console_login: AuthRateLimitRule::new(5, 300_000),
        }
    }
}

impl AuthLimitsConfig {
    fn validate(&self) -> AppResult<()> {
        for (name, rule) in [
            ("auth.limits.source", self.source),
            ("auth.limits.email", self.email),
            ("auth.limits.registration_source", self.registration_source),
            ("auth.limits.console_login", self.console_login),
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
        if matches!(database.backend()?, Some(DatabaseBackend::Sqlite)) {
            return Err(AppError::config(
                "cluster.enabled does not support SQLite; use PostgreSQL or CockroachDB for durable multi-node party and matchmaker fencing",
            ));
        }
        if matches!(database.backend()?, Some(DatabaseBackend::MongoDb)) {
            return Err(AppError::config(
                "cluster.enabled does not support MongoDB because its StorageRepository lacks atomic_batch; use PostgreSQL or CockroachDB for durable multi-node party authority",
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
    /// Optional loss-tolerant runtime-only write buffer. Disabled by default.
    pub deferred: crate::deferred_storage::DeferredStorageConfig,
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
    /// Whether matches require a validated, loaded GameScript. When `true`, the
    /// node refuses to list, create, or admit players into matches until a
    /// script is loaded and its execution backend is healthy; a missing
    /// entrypoint boots the node not-ready instead of silently falling back to
    /// the built-in relay. `false` (the default) preserves the unzip-and-run
    /// relay behavior byte for byte. The first-run wizard enables this when it
    /// scaffolds a scripted project.
    pub require_script: bool,
    /// Explicit runtime language. `None` means autodetect from `scripts_dir`.
    pub language: Option<RuntimeLanguage>,
    /// Runtime hosting adapter. `embedded` executes game scripts in-process.
    /// `external-worker` (unix and windows) executes them in a supervised
    /// worker process instead: matches run in per-match engine contexts over
    /// the authenticated data plane, while the match-independent surface
    /// (global messages, RPC, lifecycle hooks) is not routed to the worker
    /// yet. `wasm` is not implemented.
    pub adapter: RuntimeAdapter,
    /// Runtime trust tier. Only `trusted` is implemented today.
    pub tier: RuntimeTier,
    /// Operator-declared capability policy for runtime extensions. Existing
    /// outbound HTTP remains enabled by default for backwards compatibility;
    /// every new externally-reachable or shared capability defaults off.
    pub capabilities: RuntimeCapabilitiesConfig,
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
    /// Authoritative gameplay bridge quotas + capabilities. Applied only when
    /// `require_script` is enabled (authoritative matches); ignored otherwise.
    pub bridge: BridgeConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_script: false,
            language: None,
            adapter: RuntimeAdapter::Embedded,
            tier: RuntimeTier::Trusted,
            capabilities: RuntimeCapabilitiesConfig::default(),
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
            bridge: BridgeConfig::default(),
        }
    }
}

/// Shared, validated capability policy for runtime extension surfaces.
///
/// This is deliberately configuration-only foundation. The owning feature
/// tasks consume the individual grants and limits when their host APIs ship;
/// accepting a capability here never exposes a new runtime API by itself.
/// Runtime script hot reload never changes this policy: operators restart the
/// node after changing it so all runtime instances observe one configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeCapabilitiesConfig {
    /// Bounded outbound HTTP is already shipped, so it remains enabled by
    /// default while its egress policy is hardened separately.
    pub outbound_http: OutboundHttpCapabilityConfig,
    /// Script-defined public HTTP routes are opt-in.
    pub custom_http_endpoints: CustomHttpEndpointsCapabilityConfig,
    /// Generic runtime events are opt-in.
    pub events: RuntimeEventsCapabilityConfig,
    /// Mutable shared runtime state is opt-in.
    pub shared_cache: SharedCacheCapabilityConfig,
}

/// Authoritative gameplay bridge quotas + capabilities (`[runtime.bridge]`).
///
/// Quota defaults are **PROVISIONAL** — measure-first values that mirror the
/// existing per-invocation command-sink precedents (`MAX_OUTBOUND_COMMANDS`,
/// the 1 MiB aggregate cap, `MAX_OUTBOUND_BODY_BYTES`); the bench harness fixes
/// the real numbers. Every capability defaults **off** (opt-in), matching the
/// runtime capability policy: a script may only emit Persist/Schedule/physics
/// commands in an authoritative match when the operator grants the capability
/// here. This is deployment-wide until the revision store declares capabilities
/// per revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BridgeConfig {
    /// Max script commands per batch.
    pub max_commands: usize,
    /// Max aggregate command body bytes per batch.
    pub max_command_body_bytes: usize,
    /// Max bytes for one input-outcome reply.
    pub max_reply_bytes: usize,
    /// Max recipients in one multicast.
    pub max_recipients: usize,
    /// Max persistence ops per batch.
    pub max_persist_ops: usize,
    /// Max schedule ops per batch.
    pub max_schedule_ops: usize,
    /// Permit `Persist` commands (storage writes).
    pub allow_persist: bool,
    /// Permit `Schedule` commands (deferred re-entry).
    pub allow_schedule: bool,
    /// Permit kinematic-body commands (`SetPhysics`/`ApplyImpulse`/`SetMoveIntent`).
    pub allow_physics: bool,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        // PROVISIONAL — measure first; these mirror `BridgeQuotas::default()`.
        Self {
            max_commands: 1_024,
            max_command_body_bytes: 1 << 20,
            max_reply_bytes: 64 << 10,
            max_recipients: 1_024,
            max_persist_ops: 64,
            max_schedule_ops: 64,
            allow_persist: false,
            allow_schedule: false,
            allow_physics: false,
        }
    }
}

/// Quotas for the existing outbound HTTP capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutboundHttpCapabilityConfig {
    pub enabled: bool,
    pub max_concurrent_requests: u32,
    pub max_requests_per_minute: u32,
    /// Exact DNS hostnames permitted for egress. An empty list permits any
    /// public DNS hostname; IP-literal URLs are never accepted.
    pub allowed_hosts: Vec<String>,
    /// TCP ports permitted for egress. Restricting this prevents the runtime
    /// from treating public HTTP hosts as arbitrary TCP service proxies.
    pub allowed_ports: Vec<u16>,
    /// Permit resolved private, loopback, link-local, and other non-public
    /// addresses. This is false by default and intended only for an explicit
    /// operator-controlled private integration.
    pub allow_private_networks: bool,
}

impl Default for OutboundHttpCapabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_requests: 16,
            max_requests_per_minute: 120,
            allowed_hosts: Vec::new(),
            allowed_ports: vec![80, 443],
            allow_private_networks: false,
        }
    }
}

/// Quotas for opt-in script-defined HTTP endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CustomHttpEndpointsCapabilityConfig {
    pub enabled: bool,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_requests_per_minute: u32,
}

impl Default for CustomHttpEndpointsCapabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 1024 * 1024,
            max_requests_per_minute: 120,
        }
    }
}

/// Quotas for opt-in node-local runtime events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeEventsCapabilityConfig {
    pub enabled: bool,
    pub queue_capacity: usize,
    pub max_event_bytes: usize,
    pub max_events_per_minute: u32,
}

impl Default for RuntimeEventsCapabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            queue_capacity: 1_024,
            max_event_bytes: 16 * 1024,
            max_events_per_minute: 600,
        }
    }
}

/// Quotas for the opt-in, node-local mutable runtime cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SharedCacheCapabilityConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_value_bytes: usize,
    pub max_ttl_ms: u64,
}

impl Default for SharedCacheCapabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_entries: 1_024,
            max_value_bytes: 64 * 1024,
            max_ttl_ms: 3_600_000,
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
    /// Optional server-authoritative NetworkPeer property replication.
    pub network_peer: NetworkPeerConfig,
}

/// Server-side NetworkPeer replication settings.
///
/// This is deliberately disabled by default. Enabling it only attaches the
/// authority boundary; classes and objects are still registered exclusively by
/// trusted server lifecycle code, never by a client frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkPeerConfig {
    /// Attach the NetworkPeer authority to the production gateway.
    pub enabled: bool,
    /// Reuse prepared quantized values for identical outbound bunches.
    pub shared_quantized_state: bool,
    /// Uniform InterestGrid cell size, in world units.
    pub interest_cell_size: u32,
    /// Enter-relevancy distance, in world units.
    pub interest_inner: u32,
    /// Exit-relevancy distance, in world units. Must be >= `interest_inner`.
    pub interest_outer: u32,
}

impl Default for NetworkPeerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shared_quantized_state: false,
            interest_cell_size: 100,
            interest_inner: 100,
            interest_outer: 125,
        }
    }
}

impl NetworkPeerConfig {
    fn validate(&self) -> AppResult<()> {
        if self.interest_cell_size == 0 {
            return Err(AppError::config(
                "transport.network_peer.interest_cell_size must be positive",
            ));
        }
        if self.interest_inner == 0 || self.interest_outer < self.interest_inner {
            return Err(AppError::config(
                "transport.network_peer interest distances must be positive and outer >= inner",
            ));
        }
        Ok(())
    }
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
    /// Permit the generated development certificate on a reachable bind.
    ///
    /// The generated certificate carries only a `localhost` SAN, so no real
    /// client can validate it; accepting it off-loopback pushes integrators
    /// toward disabling verification, which turns a configuration gap into a
    /// fleet-wide interception risk. Set this only for a closed LAN test where
    /// that trade-off is understood and deliberate.
    pub allow_self_signed: bool,
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

    /// Reject a reachable listener that would fall back to the generated
    /// development certificate.
    ///
    /// Previously this was announced with a single `info` line and the node
    /// started anyway, so a deployment could serve real traffic on a throwaway
    /// certificate without anything failing.
    fn validate_listener_exposure(&self, listener: &str, bind: &str) -> AppResult<()> {
        if self.is_configured() || self.allow_self_signed || bind_is_loopback_only(bind) {
            return Ok(());
        }
        Err(AppError::config(format!(
            "{listener} binds '{bind}', which is reachable beyond this host, but \
             transport.tls is not configured. Set transport.tls.certificate_file \
             and transport.tls.private_key_file, or set \
             transport.tls.allow_self_signed = true to accept the generated \
             development certificate on this bind."
        )))
    }
}

/// Authoritative transform-sync settings (, design
/// `website/src/content/docs/reference/client-sdk/transform-sync.md`).
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
    /// Interval between native WebSocket Ping control frames after authentication.
    /// Set to `0` to disable transport liveness probing.
    pub heartbeat_interval_ms: u64,
    /// Maximum time to wait for the matching Pong before closing a connection.
    pub heartbeat_timeout_ms: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:7352".to_string(),
            outbound_queue_capacity: 1024,
            heartbeat_interval_ms: 15_000,
            heartbeat_timeout_ms: 45_000,
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
    /// Optional PEM material terminating TLS directly on this listener.
    ///
    /// Set this to serve `https://` without a reverse proxy. It is independent
    /// of `transport.tls`, which covers the QUIC and WebTransport listeners:
    /// the two surfaces are usually issued different certificates, and a
    /// deployment may terminate one without the other.
    pub tls: HttpTlsConfig,
    /// Acknowledge that a TLS-terminating reverse proxy fronts this listener.
    ///
    /// The HTTP surface carries the operator console password, console bearer
    /// tokens, and every player session token. Serving it unencrypted on a
    /// reachable address is only safe when something else provides the
    /// encryption, so the server requires that to be stated rather than
    /// assumed.
    pub behind_tls_proxy: bool,
}

/// Server-owned capture/upload policy for opt-in lag diagnostics.
///
/// The feature is deliberately disabled by default. Enabling it requires a
/// private filesystem root and an explicit HMAC keyring; it never falls back to
/// a process-generated key because that would make restart/replay semantics
/// ambiguous.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LagDiagnosticsConfig {
    /// Enables native capture control and its state-gated HTTP ingest route.
    pub enabled: bool,
    /// Absolute filesystem root for raw artifacts, manifests, staging, and
    /// replay markers. It must not be a public static-file directory.
    pub raw_root: Option<String>,
    /// Active key id used to sign newly minted one-use upload capabilities.
    pub active_key_id: Option<String>,
    /// Base64url-no-padding HMAC-SHA256 keyring. Keys are redacted from Debug.
    pub upload_hmac_keys: BTreeMap<String, String>,
    /// Exact browser origins allowed to perform a bearer upload. Empty means
    /// CORS is disabled; native/same-origin clients may still upload.
    pub allowed_origins: Vec<String>,
    /// Global compressed-body cap, also narrowed per grant.
    pub max_compressed_bytes: u32,
    /// Maximum decompressed CLAG bytes after gzip validation.
    pub max_decompressed_bytes: u32,
    /// Maximum decompressed/compressed expansion ratio.
    pub max_decompression_ratio: u32,
    /// Simultaneous uploads admitted by this node.
    pub max_concurrent_uploads: u16,
    /// Private raw artifact quota across this node's raw root.
    pub max_raw_bytes: u64,
    /// Age after which unreferenced raw artifacts may be removed by maintenance.
    pub retention_hours: u64,
    /// Set only when a shared, non-database raw store plus capture control plane
    /// makes a clustered upload route safe. Node-local raw disk fails closed.
    pub shared_raw_store: bool,
}

impl std::fmt::Debug for LagDiagnosticsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagDiagnosticsConfig")
            .field("enabled", &self.enabled)
            .field("raw_root", &self.raw_root)
            .field("active_key_id", &self.active_key_id)
            .field("upload_hmac_keys", &"[redacted]")
            .field("allowed_origins", &self.allowed_origins)
            .field("max_compressed_bytes", &self.max_compressed_bytes)
            .field("max_decompressed_bytes", &self.max_decompressed_bytes)
            .field("max_decompression_ratio", &self.max_decompression_ratio)
            .field("max_concurrent_uploads", &self.max_concurrent_uploads)
            .field("max_raw_bytes", &self.max_raw_bytes)
            .field("retention_hours", &self.retention_hours)
            .field("shared_raw_store", &self.shared_raw_store)
            .finish()
    }
}

impl Default for LagDiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            raw_root: None,
            active_key_id: None,
            upload_hmac_keys: BTreeMap::new(),
            allowed_origins: Vec::new(),
            max_compressed_bytes: 4 * 1024 * 1024,
            max_decompressed_bytes: 64 * 1024 * 1024,
            max_decompression_ratio: 32,
            max_concurrent_uploads: 4,
            max_raw_bytes: 4 * 1024 * 1024 * 1024,
            retention_hours: 168,
            shared_raw_store: false,
        }
    }
}

impl LagDiagnosticsConfig {
    fn validate(&self, cluster_enabled: bool) -> AppResult<()> {
        if !self.enabled {
            return Ok(());
        }
        let raw_root = self
            .raw_root
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| AppError::config("lag_diagnostics.raw_root is required when enabled"))?;
        if !Path::new(raw_root).is_absolute() {
            return Err(AppError::config(
                "lag_diagnostics.raw_root must be an absolute private filesystem path",
            ));
        }
        let active_key_id = self
            .active_key_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AppError::config("lag_diagnostics.active_key_id is required when enabled")
            })?;
        if !valid_key_id(active_key_id)
            || !self.upload_hmac_keys.keys().all(|key| valid_key_id(key))
        {
            return Err(AppError::config(
                "lag_diagnostics upload key ids must be 1-64 ASCII alphanumeric, '-' or '_' bytes",
            ));
        }
        let key = self.upload_hmac_keys.get(active_key_id).ok_or_else(|| {
            AppError::config("lag_diagnostics.active_key_id is absent from upload_hmac_keys")
        })?;
        let key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(key)
            .map_err(|_| AppError::config("lag_diagnostics active HMAC key is not base64url"))?;
        if key_bytes.len() < 32 {
            return Err(AppError::config(
                "lag_diagnostics active HMAC key must decode to at least 32 bytes",
            ));
        }
        if self.max_compressed_bytes == 0
            || self.max_decompressed_bytes == 0
            || self.max_compressed_bytes > self.max_decompressed_bytes
            || self.max_decompressed_bytes > 64 * 1024 * 1024
            || self.max_decompression_ratio == 0
            || self.max_decompression_ratio > 128
            || self.max_concurrent_uploads == 0
            || self.max_concurrent_uploads > 64
            || self.max_raw_bytes < u64::from(self.max_compressed_bytes)
            || self.retention_hours == 0
            || self.retention_hours > 24 * 365
        {
            return Err(AppError::config("invalid lag_diagnostics upload limits"));
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| !valid_diagnostics_origin(origin))
        {
            return Err(AppError::config(
                "lag_diagnostics.allowed_origins must contain exact HTTPS origins (or loopback HTTP origins), never wildcards, paths, queries, or fragments",
            ));
        }
        // This implementation owns its pending/consumed leases and raw artifacts on
        // the local filesystem. Merely declaring a shared root would not make the
        // one-use token state or recovery protocol cluster-safe, so keep the MVP
        // fail-closed until a shared store *and* capture control plane exist.
        if cluster_enabled {
            return Err(AppError::config(
                "lag_diagnostics.enabled rejects cluster mode: the current raw ingest implementation is node-local",
            ));
        }
        Ok(())
    }
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_diagnostics_origin(value: &str) -> bool {
    if value.is_empty()
        || value.contains('*')
        || value.contains('/') && !value.starts_with("https://") && !value.starts_with("http://")
    {
        return false;
    }
    let Some((scheme, authority)) = value.split_once("://") else {
        return false;
    };
    if authority.is_empty() || authority.contains(['/', '?', '#', '@']) {
        return false;
    }
    match scheme {
        "https" => true,
        "http" => {
            authority.eq_ignore_ascii_case("localhost")
                || authority.starts_with("localhost:")
                || authority.starts_with("127.0.0.1:")
                || authority.starts_with("[::1]")
        }
        _ => false,
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7350".to_string(),
            tls: HttpTlsConfig::default(),
            behind_tls_proxy: false,
        }
    }
}

/// Optional PEM certificate chain and private key for the HTTP listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct HttpTlsConfig {
    /// PEM certificate chain presented to HTTP clients.
    pub certificate_file: Option<String>,
    /// PEM private key corresponding to `certificate_file`.
    pub private_key_file: Option<String>,
}

impl HttpTlsConfig {
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
                "http.tls.certificate_file and http.tls.private_key_file must be set together and not be empty",
            )),
        }
    }
}

impl HttpConfig {
    /// Whether [`Self::bind`] can only be reached from this host.
    #[must_use]
    pub fn binds_loopback_only(&self) -> bool {
        bind_is_loopback_only(&self.bind)
    }
}

/// Whether a `host:port` bind string can only be reached from this host.
///
/// Parsed as a socket address first so IPv6 forms (`[::1]:7350`) and the
/// unspecified addresses (`0.0.0.0`, `[::]`) are classified correctly. Anything
/// that does not parse is treated as exposed: a hostname may resolve anywhere,
/// and guessing in the permissive direction would defeat the guards this feeds.
#[must_use]
pub fn bind_is_loopback_only(bind: &str) -> bool {
    use std::net::SocketAddr;

    let bind = bind.trim();
    if let Ok(addr) = bind.parse::<SocketAddr>() {
        return addr.ip().is_loopback();
    }
    // Accept the common `localhost:PORT` spelling, which is not a valid
    // `SocketAddr` but is unambiguously loopback.
    matches!(
        bind.rsplit_once(':'),
        Some((host, _)) if host.eq_ignore_ascii_case("localhost")
    )
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

/// Retention settings for the local, redacted incident journal.
///
/// The journal path is intentionally not configurable here: by default its
/// files live beside the running executable, which keeps a standalone install
/// self-contained. Operators can tune only bounded retention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ErrorJournalConfig {
    /// Maximum size of the JSONL journal before its oldest incidents are pruned.
    pub max_bytes: u64,
    /// Maximum retained incident records, independent of byte size.
    pub max_entries: usize,
}

impl Default for ErrorJournalConfig {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_entries: 2_000,
        }
    }
}

impl ErrorJournalConfig {
    fn validate(&self) -> AppResult<()> {
        const MIN_BYTES: u64 = 64 * 1024;
        const MAX_BYTES: u64 = 1024 * 1024 * 1024;
        const MAX_ENTRIES: usize = 100_000;

        if !(MIN_BYTES..=MAX_BYTES).contains(&self.max_bytes) {
            return Err(AppError::config(format!(
                "errors.max_bytes must be between {MIN_BYTES} and {MAX_BYTES}"
            )));
        }
        if self.max_entries == 0 || self.max_entries > MAX_ENTRIES {
            return Err(AppError::config(format!(
                "errors.max_entries must be between 1 and {MAX_ENTRIES}"
            )));
        }
        Ok(())
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
        self.errors.validate()?;
        self.cluster
            .validate(&self.server.node_id, &self.database)?;
        self.transport.tls.validate()?;
        self.transport.network_peer.validate()?;
        if self.transport.quic.enabled {
            validate_socket_addr("transport.quic.bind", &self.transport.quic.bind)?;
            self.transport
                .tls
                .validate_listener_exposure("transport.quic.bind", &self.transport.quic.bind)?;
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
            if self.transport.websocket.heartbeat_interval_ms > 0
                && self.transport.websocket.heartbeat_timeout_ms == 0
            {
                return Err(AppError::config(
                    "transport.websocket.heartbeat_timeout_ms must be >= 1 when heartbeat_interval_ms is enabled",
                ));
            }
        }
        if self.transport.webtransport.enabled {
            validate_socket_addr(
                "transport.webtransport.bind",
                &self.transport.webtransport.bind,
            )?;
            self.transport.tls.validate_listener_exposure(
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
            self.runtime.validate_capabilities()?;
        }
        if self.runtime.require_script && !self.runtime.enabled {
            // A gate that can never open is a misconfiguration, not a stance:
            // require_script demands a loadable script runtime.
            return Err(AppError::config(
                "runtime.require_script requires runtime.enabled = true",
            ));
        }
        self.storage.index_definitions()?;
        self.storage.deferred.validate()?;
        self.chat.limits.validate()?;
        self.authentication.limits.validate()?;
        self.database.validate()?;
        self.console.validate()?;
        self.http.tls.validate()?;
        self.lag_diagnostics.validate(self.cluster.enabled)?;
        self.validate_console_exposure()?;
        self.validate_http_exposure()?;
        Ok(())
    }

    /// Refuse to start an internet-reachable node that still carries the
    /// built-in console credentials.
    ///
    /// The console grants full read/write over accounts, wallets, chat, groups,
    /// and the database explorer, so shipping the documented default password on
    /// a public bind is a takeover waiting to happen. Loopback binds stay
    /// permitted: the zero-setup local demo is the reason the default exists.
    fn validate_console_exposure(&self) -> AppResult<()> {
        if self.console.uses_default_credentials() && !self.http.binds_loopback_only() {
            return Err(AppError::config(format!(
                "console.password is still the built-in default while http.bind is \
                 '{}', which is reachable beyond this host. Set console.username and \
                 console.password (or CITADEL_CONSOLE_USERNAME and \
                 CITADEL_CONSOLE_PASSWORD) before binding a non-loopback address.",
                self.http.bind
            )));
        }
        Ok(())
    }

    /// Refuse to serve the HTTP surface in cleartext on a reachable address.
    ///
    /// This listener carries the operator console password, the console bearer
    /// tokens issued from it, and every player session token. Encryption is
    /// therefore not optional off-loopback; the only question is who provides
    /// it. Terminate here with `http.tls`, or state that something in front
    /// does with `http.behind_tls_proxy`.
    fn validate_http_exposure(&self) -> AppResult<()> {
        if self.http.tls.is_configured()
            || self.http.behind_tls_proxy
            || self.http.binds_loopback_only()
        {
            return Ok(());
        }
        Err(AppError::config(format!(
            "http.bind is '{}', which is reachable beyond this host, but the HTTP \
             surface would be served in cleartext. It carries the console password, \
             console bearer tokens, and player session tokens. Set \
             http.tls.certificate_file and http.tls.private_key_file to terminate \
             TLS here, or set http.behind_tls_proxy = true if a TLS-terminating \
             reverse proxy fronts this listener.",
            self.http.bind
        )))
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
    const MAX_RUNTIME_CAPABILITY_CONCURRENCY: u64 = 1_024;
    const MAX_RUNTIME_CAPABILITY_RATE_PER_MINUTE: u64 = 1_000_000;
    const MAX_RUNTIME_CAPABILITY_BYTES: u64 = 16 * 1024 * 1024;
    const MAX_RUNTIME_EVENT_QUEUE_CAPACITY: u64 = 65_536;
    const MAX_RUNTIME_EVENT_QUEUE_BYTES: u64 = 64 * 1024 * 1024;
    const MAX_RUNTIME_CACHE_ENTRIES: u64 = 65_536;
    const MAX_RUNTIME_CACHE_BYTES: u64 = 128 * 1024 * 1024;
    const MAX_RUNTIME_CACHE_TTL_MS: u64 = 365 * 24 * 60 * 60 * 1_000;

    /// Validate all runtime-extension quota fields before a host API can use
    /// them. Errors name only configuration keys and never reveal secrets.
    fn validate_capability_quota(field: &str, value: u64, maximum: u64) -> AppResult<()> {
        if value == 0 {
            return Err(AppError::config(format!("{field} must be >= 1")));
        }
        if value > maximum {
            return Err(AppError::config(format!("{field} must be <= {maximum}")));
        }
        Ok(())
    }

    fn validate_capability_config(capabilities: &RuntimeCapabilitiesConfig) -> AppResult<()> {
        Self::validate_capability_quota(
            "runtime.capabilities.outbound_http.max_concurrent_requests",
            u64::from(capabilities.outbound_http.max_concurrent_requests),
            Self::MAX_RUNTIME_CAPABILITY_CONCURRENCY,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.outbound_http.max_requests_per_minute",
            u64::from(capabilities.outbound_http.max_requests_per_minute),
            Self::MAX_RUNTIME_CAPABILITY_RATE_PER_MINUTE,
        )?;
        if capabilities.outbound_http.allowed_ports.is_empty() {
            return Err(AppError::config(
                "runtime.capabilities.outbound_http.allowed_ports must not be empty",
            ));
        }
        if capabilities.outbound_http.allowed_ports.len() > 128 {
            return Err(AppError::config(
                "runtime.capabilities.outbound_http.allowed_ports must contain at most 128 entries",
            ));
        }
        if capabilities.outbound_http.allowed_hosts.len() > 128 {
            return Err(AppError::config(
                "runtime.capabilities.outbound_http.allowed_hosts must contain at most 128 entries",
            ));
        }
        for host in &capabilities.outbound_http.allowed_hosts {
            if !Self::is_valid_outbound_http_hostname(host) {
                return Err(AppError::config(format!(
                    "runtime.capabilities.outbound_http.allowed_hosts contains invalid hostname '{host}'"
                )));
            }
        }
        Self::validate_capability_quota(
            "runtime.capabilities.custom_http_endpoints.max_request_bytes",
            capabilities.custom_http_endpoints.max_request_bytes as u64,
            Self::MAX_RUNTIME_CAPABILITY_BYTES,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.custom_http_endpoints.max_response_bytes",
            capabilities.custom_http_endpoints.max_response_bytes as u64,
            Self::MAX_RUNTIME_CAPABILITY_BYTES,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.custom_http_endpoints.max_requests_per_minute",
            u64::from(capabilities.custom_http_endpoints.max_requests_per_minute),
            Self::MAX_RUNTIME_CAPABILITY_RATE_PER_MINUTE,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.events.queue_capacity",
            capabilities.events.queue_capacity as u64,
            Self::MAX_RUNTIME_EVENT_QUEUE_CAPACITY,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.events.max_event_bytes",
            capabilities.events.max_event_bytes as u64,
            Self::MAX_RUNTIME_CAPABILITY_BYTES,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.events.max_events_per_minute",
            u64::from(capabilities.events.max_events_per_minute),
            Self::MAX_RUNTIME_CAPABILITY_RATE_PER_MINUTE,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.shared_cache.max_entries",
            capabilities.shared_cache.max_entries as u64,
            Self::MAX_RUNTIME_CACHE_ENTRIES,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.shared_cache.max_value_bytes",
            capabilities.shared_cache.max_value_bytes as u64,
            Self::MAX_RUNTIME_CAPABILITY_BYTES,
        )?;
        Self::validate_capability_quota(
            "runtime.capabilities.shared_cache.max_ttl_ms",
            capabilities.shared_cache.max_ttl_ms,
            Self::MAX_RUNTIME_CACHE_TTL_MS,
        )?;
        let event_budget = (capabilities.events.queue_capacity as u64)
            .saturating_mul(capabilities.events.max_event_bytes as u64);
        Self::validate_capability_quota(
            "runtime.capabilities.events queue memory budget",
            event_budget,
            Self::MAX_RUNTIME_EVENT_QUEUE_BYTES,
        )?;
        let cache_budget = (capabilities.shared_cache.max_entries as u64)
            .saturating_mul(capabilities.shared_cache.max_value_bytes as u64);
        Self::validate_capability_quota(
            "runtime.capabilities.shared_cache memory budget",
            cache_budget,
            Self::MAX_RUNTIME_CACHE_BYTES,
        )
    }

    fn is_valid_outbound_http_hostname(host: &str) -> bool {
        if host.is_empty() || host.len() > 253 || host.parse::<std::net::IpAddr>().is_ok() {
            return false;
        }
        host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    }

    fn validate_capabilities(&self) -> AppResult<()> {
        Self::validate_capability_config(&self.capabilities)
    }

    /// Validate runtime adapter/tier combinations implemented by this build.
    fn validate_hosting(&self) -> AppResult<()> {
        match self.adapter {
            RuntimeAdapter::Embedded => {}
            // The serve lifecycle spawns and supervises the worker process on
            // unix and windows; a present script entrypoint is executed
            // inside it through the match data plane.
            #[cfg(any(unix, windows))]
            RuntimeAdapter::ExternalWorker => {}
            #[cfg(not(any(unix, windows)))]
            RuntimeAdapter::ExternalWorker => {
                return Err(AppError::config(
                    "runtime.adapter 'external-worker' requires a unix or windows host; use 'embedded'",
                ));
            }
            RuntimeAdapter::Wasm => {
                return Err(AppError::config(
                    "runtime.adapter 'wasm' is not implemented yet; use 'embedded'",
                ));
            }
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
        let selection = match self.language {
            Some(language) => self.resolve_explicit_selection(scripts_dir, language),
            None => self.resolve_autodetected_selection(scripts_dir),
        }?;
        Ok(selection)
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
        assert_eq!(config.errors.max_bytes, 8 * 1024 * 1024);
        assert_eq!(config.errors.max_entries, 2_000);
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
    fn enabled_lag_ingest_fails_closed_in_cluster_mode() {
        use base64::Engine as _;

        let mut config = Config::default();
        config.cluster.enabled = true;
        config.lag_diagnostics.enabled = true;
        config.lag_diagnostics.raw_root = Some(std::env::temp_dir().display().to_string());
        config.lag_diagnostics.active_key_id = Some("current".to_string());
        config.lag_diagnostics.upload_hmac_keys.insert(
            "current".to_string(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([9_u8; 32]),
        );

        let error = config
            .lag_diagnostics
            .validate(true)
            .expect_err("node-local ingest must fail closed");
        assert!(error.message().contains("node-local"));
    }

    #[test]
    fn error_journal_retention_bounds_are_validated() {
        let mut config = Config::default();
        config.errors.max_bytes = 1;
        let err = config.validate().expect_err("too-small journal must fail");
        assert!(err.message().contains("errors.max_bytes"));

        let mut config = Config::default();
        config.errors.max_entries = 0;
        let err = config.validate().expect_err("zero entries must fail");
        assert!(err.message().contains("errors.max_entries"));
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
        assert!(rc.capabilities.outbound_http.enabled);
        assert!(!rc.capabilities.custom_http_endpoints.enabled);
        assert!(!rc.capabilities.events.enabled);
        assert!(!rc.capabilities.shared_cache.enabled);
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
    fn runtime_capability_quotas_must_be_nonzero() {
        let mut config = Config::default();
        config.runtime.capabilities.events.max_event_bytes = 0;
        let err = config
            .validate()
            .expect_err("zero capability quota must be rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(
            err.message()
                .contains("runtime.capabilities.events.max_event_bytes")
        );
    }

    #[test]
    fn runtime_capability_memory_budgets_are_bounded() {
        let mut config = Config::default();
        config.runtime.capabilities.shared_cache.max_entries = 2_049;
        let err = config
            .validate()
            .expect_err("cache memory budget must be bounded");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(
            err.message()
                .contains("runtime.capabilities.shared_cache memory budget")
        );
    }

    #[test]
    fn runtime_outbound_http_egress_policy_rejects_unsafe_configuration() {
        let mut config = Config::default();
        config
            .runtime
            .capabilities
            .outbound_http
            .allowed_ports
            .clear();
        let err = config
            .validate()
            .expect_err("an empty egress port policy must be rejected");
        assert!(err.message().contains("outbound_http.allowed_ports"));

        let mut config = Config::default();
        config.runtime.capabilities.outbound_http.allowed_hosts = vec!["127.0.0.1".to_string()];
        let err = config
            .validate()
            .expect_err("IP literals must not be configured as trusted hostnames");
        assert!(err.message().contains("outbound_http.allowed_hosts"));
    }

    #[test]
    fn runtime_capabilities_parse_from_toml() {
        let config: Config = toml::from_str(
            r#"
            [runtime.capabilities.custom_http_endpoints]
            enabled = true
            max_request_bytes = 4096
            max_response_bytes = 8192
            max_requests_per_minute = 25

            [runtime.capabilities.outbound_http]
            allowed_hosts = ["api.example.test"]
            allowed_ports = [443]
            "#,
        )
        .expect("capability configuration parses");
        assert!(config.runtime.capabilities.custom_http_endpoints.enabled);
        assert_eq!(
            config
                .runtime
                .capabilities
                .custom_http_endpoints
                .max_request_bytes,
            4096
        );
        assert_eq!(
            config.runtime.capabilities.outbound_http.allowed_hosts,
            vec!["api.example.test"]
        );
        assert!(config.validate().is_ok());
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
                url: Some("postgres://citadel@localhost/citadel".to_owned()),
                ..DatabaseConfig::default()
            },
            ..Config::default()
        };
        assert!(config.validate().is_ok());
        config.database.url = Some("sqlite::memory:".to_owned());
        assert!(config.validate().is_err());
        config.database.url = Some("mongodb://localhost/citadel".to_owned());
        let mongo_error = config.validate().expect_err("MongoDB cluster must fail");
        assert!(mongo_error.to_string().contains("atomic_batch"));
        config.database.url = Some("postgres://citadel@localhost/citadel".to_owned());
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
    fn bridge_config_parses_and_defaults_capabilities_off() {
        // Defaults: capabilities off (opt-in), quotas mirror BridgeQuotas.
        let defaults = BridgeConfig::default();
        assert!(!defaults.allow_persist);
        assert!(!defaults.allow_schedule);
        assert!(!defaults.allow_physics);
        assert_eq!(defaults.max_commands, 1_024);

        let toml_src = r#"
            [runtime.bridge]
            max_commands = 256
            allow_physics = true
        "#;
        let config: Config = toml::from_str(toml_src).expect("bridge toml parses");
        assert_eq!(config.runtime.bridge.max_commands, 256);
        assert!(config.runtime.bridge.allow_physics);
        assert!(
            !config.runtime.bridge.allow_persist,
            "unlisted keys keep the off default"
        );
        assert_eq!(
            config.runtime.bridge.max_reply_bytes,
            BridgeConfig::default().max_reply_bytes
        );
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
                adapter: RuntimeAdapter::Wasm,
                ..RuntimeConfig::default()
            },
            ..Config::default()
        };
        let err = config.validate().expect_err("wasm not implemented");
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

    #[cfg(not(any(unix, windows)))]
    #[test]
    fn external_worker_adapter_requires_a_supported_host() {
        let config = Config {
            runtime: RuntimeConfig {
                adapter: RuntimeAdapter::ExternalWorker,
                ..RuntimeConfig::default()
            },
            ..Config::default()
        };
        let err = config
            .validate()
            .expect_err("external worker needs a unix or windows host");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
        assert!(err.message().contains("unix or windows"));
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn external_worker_adapter_is_accepted_without_scripts() {
        let dir = unique_temp_dir("runtime-external-worker-empty");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let config = Config {
            runtime: RuntimeConfig {
                adapter: RuntimeAdapter::ExternalWorker,
                scripts_dir: dir.to_string_lossy().into_owned(),
                ..RuntimeConfig::default()
            },
            ..Config::default()
        };
        config
            .validate()
            .expect("external worker validates on a supported host");
        assert_eq!(
            config
                .runtime
                .resolve_selection()
                .expect("selection resolves"),
            None,
            "no embedded script runtime is selected under external-worker"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn external_worker_adapter_accepts_a_script_entrypoint() {
        let dir = unique_temp_dir("runtime-external-worker-script");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("main.lua"), "-- lua").expect("main.lua");
        let rc = RuntimeConfig {
            adapter: RuntimeAdapter::ExternalWorker,
            scripts_dir: dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        };
        let selection = rc
            .resolve_selection()
            .expect("a script under external-worker resolves")
            .expect("the entrypoint is selected");
        assert_eq!(selection.adapter, RuntimeAdapter::ExternalWorker);
        assert_eq!(selection.language, RuntimeLanguage::Lua);
        std::fs::remove_dir_all(&dir).ok();
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
    fn require_script_defaults_off_and_demands_an_enabled_runtime() {
        // Default off: unzip-and-run relay onboarding stays untouched.
        assert!(!RuntimeConfig::default().require_script);
        assert!(Config::default().validate().is_ok());

        let mut gated = Config::default();
        gated.runtime.require_script = true;
        gated
            .validate()
            .expect("require_script with enabled runtime");

        // A gate that can never open is a config error, not a stance.
        let mut contradictory = Config::default();
        contradictory.runtime.require_script = true;
        contradictory.runtime.enabled = false;
        let err = contradictory
            .validate()
            .expect_err("require_script with a disabled runtime must fail");
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

    #[test]
    fn loopback_binds_are_classified_correctly() {
        let loopback = [
            "127.0.0.1:7350",
            "127.1.2.3:7350",
            "[::1]:7350",
            "localhost:7350",
            "LOCALHOST:7350",
        ];
        for bind in loopback {
            let http = HttpConfig {
                bind: bind.to_string(),
                ..HttpConfig::default()
            };
            assert!(http.binds_loopback_only(), "{bind} is loopback");
        }

        let exposed = [
            "0.0.0.0:7350",
            "[::]:7350",
            "192.168.0.10:7350",
            "203.0.113.7:7350",
            // A hostname may resolve anywhere, so it must not be trusted.
            "citadel.example.com:7350",
        ];
        for bind in exposed {
            let http = HttpConfig {
                bind: bind.to_string(),
                ..HttpConfig::default()
            };
            assert!(!http.binds_loopback_only(), "{bind} is not loopback");
        }
    }

    #[test]
    fn self_signed_transports_are_rejected_on_a_reachable_bind() {
        let mut config = Config::default();
        config.transport.quic.enabled = true;

        // Loopback keeps the zero-setup local demo and the test suite working.
        config.transport.quic.bind = "127.0.0.1:7351".to_string();
        config.validate().expect("loopback dev cert allowed");

        // A reachable bind with no PEM would have served real traffic on a
        // throwaway certificate after only an info-level log line.
        config.transport.quic.bind = "0.0.0.0:7351".to_string();
        let error = config
            .validate()
            .expect_err("public bind without TLS material rejected");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
        let rendered = format!("{error}");
        assert!(
            rendered.contains("transport.quic.bind"),
            "names the listener"
        );
        assert!(
            rendered.contains("allow_self_signed"),
            "offers the explicit opt-in"
        );

        // The opt-in is honoured for a deliberate closed-network test.
        config.transport.tls.allow_self_signed = true;
        config.validate().expect("explicit opt-in allowed");

        // Real PEM material clears the guard without the opt-in.
        config.transport.tls.allow_self_signed = false;
        config.transport.tls.certificate_file = Some("/etc/citadel/fullchain.pem".to_string());
        config.transport.tls.private_key_file = Some("/etc/citadel/privkey.pem".to_string());
        config.validate().expect("configured PEM allowed");

        // The same guard covers the browser listener.
        let mut webtransport = Config::default();
        webtransport.transport.webtransport.enabled = true;
        webtransport.transport.webtransport.bind = "0.0.0.0:7352".to_string();
        let error = webtransport
            .validate()
            .expect_err("public webtransport bind without TLS rejected");
        assert!(
            format!("{error}").contains("transport.webtransport.bind"),
            "names the listener"
        );
    }

    #[test]
    fn cleartext_http_is_rejected_on_a_reachable_bind() {
        let mut config = Config::default();
        // Keep the console credentials out of the picture: this guard is about
        // the transport, not the password.
        config.console.password = "an-operator-chosen-secret".to_string();

        // Loopback stays cleartext so the zero-setup local demo still works.
        config.http.bind = "127.0.0.1:7350".to_string();
        config.validate().expect("loopback cleartext allowed");

        // A reachable bind would put the console password, the console bearer
        // tokens and every player session token on the wire in the clear.
        config.http.bind = "0.0.0.0:7350".to_string();
        let error = config
            .validate()
            .expect_err("reachable cleartext bind rejected");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
        let rendered = format!("{error}");
        assert!(
            rendered.contains("0.0.0.0:7350"),
            "names the offending bind"
        );
        assert!(
            rendered.contains("http.tls"),
            "offers in-process termination"
        );
        assert!(
            rendered.contains("http.behind_tls_proxy"),
            "offers the proxy acknowledgement"
        );

        // Terminating in-process clears the guard.
        config.http.tls.certificate_file = Some("/etc/citadel/fullchain.pem".to_string());
        config.http.tls.private_key_file = Some("/etc/citadel/privkey.pem".to_string());
        config.validate().expect("configured http TLS allowed");

        // So does acknowledging that a proxy terminates in front.
        config.http.tls = HttpTlsConfig::default();
        config.http.behind_tls_proxy = true;
        config.validate().expect("acknowledged proxy allowed");
    }

    #[test]
    fn partial_http_tls_material_is_rejected() {
        let mut config = Config::default();
        config.http.tls.certificate_file = Some("/etc/citadel/fullchain.pem".to_string());
        let error = config.validate().expect_err("half-configured TLS rejected");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
        assert!(format!("{error}").contains("must be set together"));
    }

    #[test]
    fn default_console_credentials_are_rejected_on_a_public_bind() {
        let mut config = Config::default();
        assert!(
            config.console.uses_default_credentials(),
            "fixture relies on the built-in defaults"
        );
        // Isolate the credential guard from the cleartext-transport guard, which
        // would otherwise reject the reachable bind for its own reason.
        config.http.behind_tls_proxy = true;

        // Loopback keeps the zero-setup local demo working.
        config.http.bind = "127.0.0.1:7350".to_string();
        config.validate().expect("loopback default console allowed");

        // Exposing the node with the documented password must fail closed.
        config.http.bind = "0.0.0.0:7350".to_string();
        let error = config
            .validate()
            .expect_err("public bind with default console credentials rejected");
        assert_eq!(error.category(), crate::error::ErrorCategory::Config);
        let rendered = format!("{error}");
        // A substring check against the credential itself is meaningless here:
        // the built-in password is literally "password", which also appears in
        // the `console.password` key name the message must cite. Assert instead
        // that the message is actionable — it names the offending bind and the
        // settings to change — and note that the guard never interpolates the
        // credential value.
        assert!(
            rendered.contains("0.0.0.0:7350"),
            "names the offending bind"
        );
        assert!(
            rendered.contains("CITADEL_CONSOLE_PASSWORD"),
            "points at the remediation"
        );

        // Setting a real credential clears the guard.
        config.console.password = "an-operator-chosen-secret".to_string();
        config
            .validate()
            .expect("public bind with custom console credentials allowed");
    }
}
