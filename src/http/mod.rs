//! HTTP surface for Citadel.
//!
//!  scope: build the `axum` router, expose the health endpoint, and
//! run the listener with graceful shutdown. The health endpoint is a thin
//! adapter over [`App`] health, not an independent global. Auth, sessions,
//! WebSockets, and API routes are added by later tasks.

pub mod auth;
pub mod console;
pub mod console_api;
pub mod dashboard;
pub mod error;
mod headers;
pub mod lag_diagnostics;
pub mod player;
mod runtime_endpoint;
pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use serde::Serialize;
use tokio::net::TcpListener;

use crate::app::App;
use crate::error::{AppError, AppResult};
use crate::services::Health;

pub use auth::{
    AuthRequest, AuthResponse, CUSTOM_AUTH_PATH, DEVICE_AUTH_PATH, EMAIL_AUTH_PATH,
    EmailAuthRequest,
};
pub use console_api::{CONSOLE_API_PREFIX, LOGIN_PATH, ME_PATH, SECTION_PATHS};
pub use dashboard::{DASHBOARD_PATH, NodeStatus, STATUS_PATH};
pub use error::{ApiError, ErrorBody};
pub use player::{ACCOUNT_PATH, PLAYER_LOOKUP_PATH, SESSION_LOGOUT_PATH, SESSION_REFRESH_PATH};

/// Path for the liveness/health endpoint.
///
/// Centralized here so the server bootstrap and integration tests agree on a
/// single route definition.
pub const HEALTH_PATH: &str = "/health";

/// JSON body returned by the health endpoint.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HealthBody {
    /// `healthy`, `degraded`, or `unhealthy`.
    pub status: &'static str,
    /// Node identity serving the request.
    pub node_id: String,
    /// Server version.
    pub version: &'static str,
}

impl HealthBody {
    /// Build a health body snapshot from the application state.
    #[must_use]
    pub fn from_app(app: &App) -> Self {
        Self {
            status: app.health().as_str(),
            node_id: app.node_id().to_string(),
            version: app.version(),
        }
    }
}

/// Map application health to an HTTP status code.
///
/// Serviceable states (`Healthy`, `Degraded`) return `200 OK`; `Unhealthy`
/// returns `503 Service Unavailable`.
#[must_use]
pub fn health_status_code(health: Health) -> StatusCode {
    if health.is_serviceable() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Build the Citadel HTTP router with the given application as shared state.
pub fn router(app: App) -> Router {
    Router::new()
        .route(HEALTH_PATH, get(health_handler))
        .route(STATUS_PATH, get(dashboard::status_handler))
        .route(DASHBOARD_PATH, get(console::console_handler))
        .merge(auth::routes())
        .merge(player::routes())
        .merge(console_api::routes())
        .merge(lag_diagnostics::routes())
        .merge(runtime_endpoint::routes())
        // Applied last so it wraps every route above, including the console SPA
        // and the 404 fallback.
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            headers::apply,
        ))
        .with_state(app)
}

/// Health endpoint handler: a thin adapter over application health.
async fn health_handler(State(app): State<App>) -> (StatusCode, Json<HealthBody>) {
    app.metrics().record_http_request();
    let code = health_status_code(app.health());
    (code, Json(HealthBody::from_app(&app)))
}

/// Bind a TCP listener on the application's configured HTTP address.
///
/// Returns a [`Transport`](crate::error::ErrorCategory::Transport) error if the
/// configured bind address cannot be parsed or bound.
pub async fn bind(app: &App) -> AppResult<TcpListener> {
    let bind = &app.config().http.bind;
    let addr: SocketAddr = bind.parse().map_err(|e: std::net::AddrParseError| {
        AppError::config(format!("http.bind is not a valid socket address: {bind}"))
            .with_detail(e.to_string())
    })?;
    TcpListener::bind(addr).await.map_err(|e| {
        AppError::new(
            crate::error::ErrorCategory::Transport,
            format!("failed to bind HTTP listener on {bind}"),
        )
        .with_detail(e.to_string())
    })
}

/// Serve HTTP requests on `listener` until `shutdown` resolves.
///
/// The future completes after the server has stopped accepting connections and
/// drained in-flight requests.
pub async fn serve<F>(listener: TcpListener, app: App, shutdown: F) -> AppResult<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let api_keys = Arc::clone(app.api_keys());
    let serve_result = axum::serve(
        listener,
        router(app).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .map_err(|e| {
        AppError::new(
            crate::error::ErrorCategory::Transport,
            "HTTP server terminated with an error",
        )
        .with_detail(e.to_string())
    });
    let flush_result = api_keys.flush_last_used().await;
    serve_result?;
    flush_result
}

/// Serve on `listener` using whichever scheme the configuration selects.
///
/// With `http.tls` configured this terminates TLS in-process; otherwise it
/// serves cleartext, which configuration validation only permits on loopback or
/// with `http.behind_tls_proxy` acknowledged.
///
/// # Errors
/// Propagates the underlying serve error, or a
/// [`Config`](crate::error::ErrorCategory::Config) error if the TLS material
/// cannot be loaded.
pub async fn serve_configured<F>(listener: TcpListener, app: App, shutdown: F) -> AppResult<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let http_tls = app.config().http.tls.clone();
    let Some((certificate_file, private_key_file)) = http_tls
        .certificate_file
        .as_deref()
        .zip(http_tls.private_key_file.as_deref())
    else {
        tracing::info!(
            behind_tls_proxy = app.config().http.behind_tls_proxy,
            "serving HTTP without in-process TLS"
        );
        return serve(listener, app, shutdown).await;
    };

    let config = tls::server_config(
        std::path::Path::new(certificate_file),
        std::path::Path::new(private_key_file),
    )?;
    tracing::info!(addr = ?tls::local_addr(&listener).ok(), "serving HTTPS with configured PEM TLS");
    tls::serve(listener, app, config, shutdown).await
}

/// A future that resolves on an interactive Ctrl-C or a container stop signal.
///
/// Used as the default graceful-shutdown trigger for `citadel serve`. A failure
/// to install the handler resolves the future immediately so the server can
/// stop rather than run unsupervised. Unix additionally listens for `SIGTERM`,
/// which is the signal Docker and most orchestrators send before their stop
/// grace period expires.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::error!("failed to install Ctrl-C handler; shutting down");
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    tracing::error!(%error, "failed to install SIGTERM handler; shutting down");
                }
            }
        };
        tokio::select! {
            () = ctrl_c => {}
            () = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

/// Bind the configured address and serve until Ctrl-C or SIGTERM.
///
/// This is the orchestration entry point for `citadel serve`: it binds the HTTP
/// listener, starts any enabled realtime transports (QUIC, etc.) under a shared
/// cancellation token, serves with graceful shutdown, and on shutdown cancels
/// and joins the transports. The binary stays thin by delegating here.
pub async fn run(app: App) -> AppResult<()> {
    let listener = bind(&app).await?;
    let local_addr = listener.local_addr().map_err(|e| {
        AppError::new(
            crate::error::ErrorCategory::Transport,
            "failed to read local listener address",
        )
        .with_detail(e.to_string())
    })?;
    // Detailed bind diagnostics stay at debug so the banner is the prominent,
    // readable thing on a normal run (see ).
    tracing::debug!(
        node_id = app.node_id(),
        version = app.version(),
        addr = %local_addr,
        "citadel serve: HTTP listener bound"
    );

    // Start enabled realtime transports under a shared cancellation token so
    // they stop together with the HTTP server.
    let cancel = crate::lifecycle::CancellationToken::new();
    let mut transports = crate::transport::start_enabled(&app, cancel.clone()).await?;
    // Deferred writes are a separately supervised service: normal repository
    // calls remain synchronous and never pass through this worker.
    if let Some(writer) = app.deferred_storage() {
        transports.spawn(Arc::clone(writer));
    }

    // The server is ready: print the startup banner once. Written to stdout so
    // it is visible regardless of the configured log level/format.
    {
        let banner = crate::startup::build_banner(app.config(), app.backend_kind(), app.version());
        println!("{banner}");
    }

    // HTTP serves until Ctrl-C or SIGTERM; when it returns we cancel transports.
    let shutdown_cancel = cancel.clone();
    let shutdown = async move {
        shutdown_signal().await;
        shutdown_cancel.cancel();
    };

    let http_result = serve_configured(listener, app, shutdown).await;
    // Ensure transports are signalled and joined regardless of the HTTP result.
    cancel.cancel();
    let transport_result = transports.shutdown().await;
    http_result.and(transport_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn health_path_is_absolute() {
        assert!(HEALTH_PATH.starts_with('/'));
        assert_eq!(HEALTH_PATH, "/health");
    }

    #[test]
    fn health_body_reflects_app_state() {
        let app = App::new(Config::default());
        let body = HealthBody::from_app(&app);
        assert_eq!(body.status, "healthy");
        assert_eq!(body.node_id, "dev-1");
        assert_eq!(body.version, app.version());
    }

    #[test]
    fn serviceable_health_maps_to_200() {
        assert_eq!(health_status_code(Health::Healthy), StatusCode::OK);
        assert_eq!(health_status_code(Health::Degraded), StatusCode::OK);
        assert_eq!(
            health_status_code(Health::Unhealthy),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn health_body_serializes_to_expected_json_shape() {
        let app = App::new(Config::default());
        let body = HealthBody::from_app(&app);
        let value = serde_json::to_value(&body).expect("serializes");
        assert_eq!(value["status"], "healthy");
        assert_eq!(value["node_id"], "dev-1");
        assert!(value.get("version").is_some());
    }
}
