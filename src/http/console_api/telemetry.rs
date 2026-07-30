//! Authenticated host telemetry for the operator Status dashboard.
//!
//! Capacity and live resource pressure are operational data. Unlike the
//! public node-health status, this route requires a console bearer token so a
//! publicly bound server does not disclose host sizing or free-space details.

use axum::Json;
use axum::extract::State;

use crate::app::App;
use crate::host_telemetry::HostTelemetrySnapshot;
use crate::services::ConsoleIdentity;

/// Authenticated host-resource telemetry route.
pub const TELEMETRY_PATH: &str = "/console/v1/telemetry";

/// `GET /console/v1/telemetry`: host CPU, memory, and mounted-storage use.
///
/// Both `admin` and `viewer` console roles can observe operational health; the
/// route performs no mutation and records the request in node metrics.
pub(super) async fn get_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
) -> Json<HostTelemetrySnapshot> {
    app.metrics().record_http_request();
    Json(app.host_telemetry().await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::config::Config;

    #[test]
    fn telemetry_is_an_authenticated_console_route() {
        assert!(TELEMETRY_PATH.starts_with(super::super::CONSOLE_API_PREFIX));
        assert!(!super::super::SECTION_PATHS.contains(&TELEMETRY_PATH));
    }

    #[tokio::test]
    async fn telemetry_rejects_a_request_without_console_bearer() {
        let response = crate::http::router(App::new(Config::default()))
            .oneshot(
                Request::builder()
                    .uri(TELEMETRY_PATH)
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
