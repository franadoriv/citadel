//! Prometheus text exposition for the dedicated authenticated scrape endpoint.
//!
//! The primary node metrics come from the application-owned
//! [`prometheus_client`](https://docs.rs/prometheus-client) registry. Runtime
//! custom metrics remain a separately bounded extension point and are appended
//! only after that registry has encoded its fixed metric set.

use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::app::App;
use crate::repository::ApiKeyScope;
use crate::runtime::RuntimeMetricSnapshot;
use crate::time::{Clock, SystemClock};

/// Default Prometheus scrape path.
pub const METRICS_PATH: &str = "/metrics";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// `GET /metrics`: API-key-only Prometheus scrape.
///
/// Console and player bearer credentials are never accepted here. The endpoint
/// only verifies the API-key namespace and requires `telemetry:read`.
pub async fn get_handler(State(app): State<App>, headers: HeaderMap) -> Response {
    let started = Instant::now();
    if let Err(status) = authorize_telemetry_key(&app, &headers).await {
        return status.into_response();
    }

    app.metrics().record_http_request();
    app.prometheus_metrics()
        .observe_http_response("GET", StatusCode::OK.as_u16(), METRICS_PATH);
    let snapshot = app.metrics().snapshot();
    app.prometheus_metrics().observe_node_snapshot(&snapshot);
    let mut output = match app.prometheus_metrics().encode() {
        Ok(output) => output,
        Err(error) => {
            tracing::error!(%error, "failed to encode Prometheus metrics registry");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    append_runtime_custom_metrics(&mut output, &app.runtime_metrics().snapshot());
    app.prometheus_metrics()
        .observe_scrape_duration(started.elapsed());

    ([(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], output).into_response()
}

/// Record public HTTP responses with a bounded Prometheus label set.
///
/// MatchedPath supplies route templates rather than concrete identifiers. The
/// registry further collapses runtime-defined routes and malformed values, so
/// a request cannot create a high-cardinality series.
pub async fn observe_public_http(State(app): State<App>, request: Request, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let response = next.run(request).await;
    app.prometheus_metrics()
        .observe_http_response(&method, response.status().as_u16(), &route);
    response
}

/// Append custom trusted-runtime metrics after the registry's fixed metric set.
///
/// The runtime registry validates metric names and caps metric cardinality before
/// values reach this function. It never accepts external labels, identities, or
/// payload data.
fn append_runtime_custom_metrics(output: &mut String, runtime_metrics: &[RuntimeMetricSnapshot]) {
    for metric in runtime_metrics {
        match metric {
            RuntimeMetricSnapshot::Counter { name, value } => {
                output.push_str(&format!(
                    "# HELP citadel_runtime_custom_{name}_total Bounded custom runtime counter.\n# TYPE citadel_runtime_custom_{name}_total counter\ncitadel_runtime_custom_{name}_total {value}\n"
                ));
            }
            RuntimeMetricSnapshot::Gauge { name, value } => {
                output.push_str(&format!(
                    "# HELP citadel_runtime_custom_{name} Bounded custom runtime gauge.\n# TYPE citadel_runtime_custom_{name} gauge\ncitadel_runtime_custom_{name} {value}\n"
                ));
            }
            RuntimeMetricSnapshot::Timer {
                name,
                count,
                sum_seconds,
            } => {
                output.push_str(&format!(
                    "# HELP citadel_runtime_custom_{name}_seconds Bounded custom runtime timer.\n# TYPE citadel_runtime_custom_{name}_seconds summary\ncitadel_runtime_custom_{name}_seconds_count {count}\ncitadel_runtime_custom_{name}_seconds_sum {sum_seconds}\n"
                ));
            }
        }
    }
}

async fn authorize_telemetry_key(app: &App, headers: &HeaderMap) -> Result<(), StatusCode> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next().ok_or(StatusCode::UNAUTHORIZED)?;
    if values.next().is_some() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !token.starts_with("ctdl_k1_")
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let principal = app
        .api_keys()
        .authenticate(token, SystemClock.now())
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    if principal.scopes.contains(&ApiKeyScope::TelemetryRead) {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}
