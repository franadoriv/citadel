//! Node status data model and JSON endpoint (, ).
//!
//! [`STATUS_PATH`] (`/status`) returns a machine-readable JSON [`NodeStatus`]:
//! health, identity, build, uptime, configured transports, and the
//! [`NodeMetrics`](crate::observability::NodeMetrics) snapshot. It is a thin,
//! read-only adapter over shared [`App`] state that never mutates domain data;
//! the only side effect is bumping the `http_requests_total` counter. Host
//! resource telemetry is intentionally kept on the authenticated console API.
//!
//! The human-facing [`DASHBOARD_PATH`] (`/dashboard`) is served by the
//! [`console`](super::console) module, whose Status section consumes this JSON
//! to render live, auto-refreshing gauges.

use axum::Json;
use axum::extract::State;
use serde::Serialize;

use crate::app::App;
use crate::config::RuntimeConfig;
use crate::deferred_storage::DeferredStorageMetricsSnapshot;
use crate::observability::NodeMetricsSnapshot;

/// Path for the machine-readable node status endpoint.
pub const STATUS_PATH: &str = "/status";

/// Path for the human-facing HTML dashboard.
pub const DASHBOARD_PATH: &str = "/dashboard";

/// Compile-time build facts surfaced for operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BuildInfo {
    /// Target operating system (e.g. `linux`, `windows`, `macos`).
    pub os: &'static str,
    /// Target architecture (e.g. `x86_64`, `aarch64`).
    pub arch: &'static str,
    /// Build profile (`debug` or `release`).
    pub profile: &'static str,
}

impl BuildInfo {
    /// Read the build facts for the running binary.
    #[must_use]
    pub const fn current() -> Self {
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

/// The configured state of one realtime transport listener.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportListenerStatus {
    /// Whether the listener is enabled in configuration.
    pub enabled: bool,
    /// The address the listener binds to.
    pub bind: String,
}

/// The configured realtime transports for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportStatus {
    /// Native QUIC listener.
    pub quic: TransportListenerStatus,
    /// WebSocket fallback listener.
    pub websocket: TransportListenerStatus,
    /// WebTransport (browser) listener.
    pub webtransport: TransportListenerStatus,
}

/// Runtime configuration and entrypoint selection surfaced on `/status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeStatus {
    /// Whether `[runtime]` is enabled in configuration.
    pub enabled: bool,
    /// Explicitly configured language, if set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configured_language: Option<&'static str>,
    /// Selected language after explicit config or autodetection, if an entrypoint
    /// exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_language: Option<&'static str>,
    /// `explicit` or `autodetected`, when selected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_source: Option<&'static str>,
    /// Selected entrypoint path, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    /// Runtime hosting adapter.
    pub adapter: &'static str,
    /// Runtime trust tier.
    pub tier: &'static str,
    /// Lua capability mode. `trusted` means operator-provided Lua can access
    /// machine-level standard-library facilities.
    pub lua_execution_mode: &'static str,
    /// Runtime scripts directory.
    pub scripts_dir: String,
    /// Operator-configured runtime-extension policy and public quota values.
    /// This is not a claim that a future host API has shipped, and never
    /// contains endpoint credentials, allowlists, or other secrets.
    pub configured_capabilities: RuntimeCapabilitiesStatus,
    /// Selection error, if the filesystem contains an ambiguous entrypoint set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_error: Option<String>,
}

/// Secret-safe runtime extension status exposed from `/status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeCapabilitiesStatus {
    pub outbound_http_enabled: bool,
    pub outbound_http_max_concurrent_requests: u32,
    pub outbound_http_max_requests_per_minute: u32,
    pub custom_http_endpoints_enabled: bool,
    pub custom_http_endpoints_max_request_bytes: usize,
    pub custom_http_endpoints_max_response_bytes: usize,
    pub custom_http_endpoints_max_requests_per_minute: u32,
    pub events_enabled: bool,
    pub events_queue_capacity: usize,
    pub events_max_event_bytes: usize,
    pub events_max_events_per_minute: u32,
    pub shared_cache_enabled: bool,
    pub shared_cache_max_entries: usize,
    pub shared_cache_max_value_bytes: usize,
    pub shared_cache_max_ttl_ms: u64,
}

impl RuntimeStatus {
    fn from_config(runtime: &RuntimeConfig) -> Self {
        let selection = runtime.resolve_selection();
        let (selected_language, selection_source, entrypoint, selection_error) = match selection {
            Ok(Some(selection)) => (
                Some(selection.language.as_str()),
                Some(selection.source.as_str()),
                Some(selection.entrypoint.display().to_string()),
                None,
            ),
            Ok(None) => (None, None, None, None),
            Err(err) => (None, None, None, Some(err.message().to_string())),
        };
        Self {
            enabled: runtime.enabled,
            configured_language: runtime.language.map(|language| language.as_str()),
            selected_language,
            selection_source,
            entrypoint,
            adapter: runtime.adapter.as_str(),
            tier: runtime.tier.as_str(),
            lua_execution_mode: runtime.lua_execution_mode.as_str(),
            scripts_dir: runtime.scripts_dir.clone(),
            configured_capabilities: RuntimeCapabilitiesStatus {
                outbound_http_enabled: runtime.capabilities.outbound_http.enabled,
                outbound_http_max_concurrent_requests: runtime
                    .capabilities
                    .outbound_http
                    .max_concurrent_requests,
                outbound_http_max_requests_per_minute: runtime
                    .capabilities
                    .outbound_http
                    .max_requests_per_minute,
                custom_http_endpoints_enabled: runtime.capabilities.custom_http_endpoints.enabled,
                custom_http_endpoints_max_request_bytes: runtime
                    .capabilities
                    .custom_http_endpoints
                    .max_request_bytes,
                custom_http_endpoints_max_response_bytes: runtime
                    .capabilities
                    .custom_http_endpoints
                    .max_response_bytes,
                custom_http_endpoints_max_requests_per_minute: runtime
                    .capabilities
                    .custom_http_endpoints
                    .max_requests_per_minute,
                events_enabled: runtime.capabilities.events.enabled,
                events_queue_capacity: runtime.capabilities.events.queue_capacity,
                events_max_event_bytes: runtime.capabilities.events.max_event_bytes,
                events_max_events_per_minute: runtime.capabilities.events.max_events_per_minute,
                shared_cache_enabled: runtime.capabilities.shared_cache.enabled,
                shared_cache_max_entries: runtime.capabilities.shared_cache.max_entries,
                shared_cache_max_value_bytes: runtime.capabilities.shared_cache.max_value_bytes,
                shared_cache_max_ttl_ms: runtime.capabilities.shared_cache.max_ttl_ms,
            },
            selection_error,
        }
    }
}

/// A complete node status snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeStatus {
    /// Aggregate health: `healthy`, `degraded`, or `unhealthy`.
    pub status: &'static str,
    /// Node identity serving the request.
    pub node_id: String,
    /// Server version.
    pub version: &'static str,
    /// Build facts.
    pub build: BuildInfo,
    /// Whole-seconds process uptime.
    pub uptime_seconds: u64,
    /// Selected persistence backend: `in-memory` or `postgres`. Never carries a
    /// connection string.
    pub backend: &'static str,
    /// Configured realtime transports.
    pub transports: TransportStatus,
    /// Runtime config and selected entrypoint.
    pub runtime: RuntimeStatus,
    /// Runtime counters snapshot.
    pub metrics: NodeMetricsSnapshot,
    /// Volatile deferred-storage queue metrics when that opt-in service is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferred_storage: Option<DeferredStorageMetricsSnapshot>,
}

impl NodeStatus {
    /// Assemble a status snapshot from the application state.
    #[must_use]
    pub fn from_app(app: &App) -> Self {
        let transport = &app.config().transport;
        Self {
            status: app.health().as_str(),
            node_id: app.node_id().to_string(),
            version: app.version(),
            build: BuildInfo::current(),
            uptime_seconds: app.uptime().as_secs(),
            backend: app.backend_kind().as_str(),
            transports: TransportStatus {
                quic: TransportListenerStatus {
                    enabled: transport.quic.enabled,
                    bind: transport.quic.bind.clone(),
                },
                websocket: TransportListenerStatus {
                    enabled: transport.websocket.enabled,
                    bind: transport.websocket.bind.clone(),
                },
                webtransport: TransportListenerStatus {
                    enabled: transport.webtransport.enabled,
                    bind: transport.webtransport.bind.clone(),
                },
            },
            runtime: RuntimeStatus::from_config(&app.config().runtime),
            metrics: app.metrics().snapshot(),
            deferred_storage: app
                .deferred_storage()
                .map(|writer| writer.metrics().snapshot()),
        }
    }
}

/// JSON status endpoint handler.
pub(super) async fn status_handler(State(app): State<App>) -> Json<NodeStatus> {
    app.metrics().record_http_request();
    Json(NodeStatus::from_app(&app))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::config::{Config, LuaExecutionMode};

    #[test]
    fn status_reflects_app_and_config() {
        let app = App::new(Config::default());
        let status = NodeStatus::from_app(&app);
        assert_eq!(status.status, "healthy");
        assert_eq!(status.node_id, "dev-1");
        assert_eq!(status.backend, "in-memory");
        assert!(!status.transports.quic.enabled);
        assert_eq!(status.transports.websocket.bind, "127.0.0.1:7352");
        assert!(status.runtime.enabled);
        assert_eq!(status.runtime.adapter, "embedded");
        assert_eq!(status.runtime.tier, "trusted");
        assert_eq!(status.runtime.lua_execution_mode, "sandboxed");
        assert!(status.runtime.configured_capabilities.outbound_http_enabled);
        assert!(
            !status
                .runtime
                .configured_capabilities
                .custom_http_endpoints_enabled
        );
        assert!(!status.runtime.configured_capabilities.events_enabled);
        assert!(!status.runtime.configured_capabilities.shared_cache_enabled);
        assert_eq!(status.metrics.http_requests_total, 0);
        assert!(status.deferred_storage.is_none());
    }

    #[test]
    fn status_serializes_to_expected_json_shape() {
        let app = App::new(Config::default());
        let value = serde_json::to_value(NodeStatus::from_app(&app)).expect("serializes");
        assert_eq!(value["status"], "healthy");
        assert_eq!(value["node_id"], "dev-1");
        assert_eq!(value["backend"], "in-memory");
        assert!(value["build"]["os"].is_string());
        assert!(value["transports"]["quic"]["enabled"].is_boolean());
        assert_eq!(value["runtime"]["adapter"], "embedded");
        assert_eq!(value["runtime"]["tier"], "trusted");
        assert_eq!(value["runtime"]["lua_execution_mode"], "sandboxed");
        assert_eq!(
            value["runtime"]["configured_capabilities"]["outbound_http_enabled"],
            true
        );
        assert_eq!(
            value["runtime"]["configured_capabilities"]["custom_http_endpoints_enabled"],
            false
        );
        assert!(value["metrics"]["http_requests_total"].is_number());
        assert!(value.get("host").is_none(), "host capacity stays private");
    }

    #[test]
    fn status_surfaces_explicit_trusted_lua_mode() {
        let mut config = Config::default();
        config.runtime.lua_execution_mode = LuaExecutionMode::Trusted;
        let status = NodeStatus::from_app(&App::new(config));
        assert_eq!(status.runtime.lua_execution_mode, "trusted");
    }

    #[tokio::test]
    async fn public_status_does_not_expose_host_capacity() {
        let response = crate::http::router(App::new(Config::default()))
            .oneshot(
                Request::builder()
                    .uri(STATUS_PATH)
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("status body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("status JSON");
        assert!(value.get("host").is_none());
    }
}
