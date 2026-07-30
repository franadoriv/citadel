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
    /// Selection error, if the filesystem contains an ambiguous entrypoint set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_error: Option<String>,
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
        assert_eq!(status.metrics.http_requests_total, 0);
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
