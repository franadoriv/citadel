use citadel::config::Config;

#[test]
fn metrics_defaults_to_disabled_loopback_scrape_listener() {
    let metrics = Config::default().metrics;

    assert!(!metrics.enabled);
    assert_eq!(metrics.bind, "127.0.0.1:9464");
    assert_eq!(metrics.path, "/metrics");
    assert!(metrics.require_api_key_on_non_loopback);
}

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn enabled_metrics_route_requires_an_api_credential() {
    let mut config = Config::default();
    config.metrics.enabled = true;
    let response = citadel::http::metrics_router(citadel::App::new(config))
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn telemetry_key_can_scrape_prometheus_text() {
    use citadel::repository::ApiKeyScope;
    use citadel::services::CreateApiKeyRequest;
    use citadel::time::{Clock, SystemClock};

    let mut config = Config::default();
    config.metrics.enabled = true;
    let app = citadel::App::new(config);
    app.metrics().record_http_request();
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "prometheus".to_owned(),
                scopes: vec![ApiKeyScope::TelemetryRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("issue telemetry key");

    let response = citadel::http::metrics_router(app)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {}", issued.secret))
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("read response");
    let body = String::from_utf8(body.to_vec()).expect("utf8 metrics");
    assert!(
        body.contains("# TYPE citadel_http_requests counter"),
        "{body}"
    );
    assert!(
        body.contains("citadel_realtime_connections_active 0\n"),
        "{body}"
    );
    assert!(
        body.contains("# TYPE citadel_metrics_scrape_duration_seconds histogram\n"),
        "{body}"
    );
    assert!(
        body.contains("citadel_metrics_scrape_duration_seconds_bucket{le=\"0.001\"}"),
        "{body}"
    );
}

#[test]
fn non_loopback_metrics_listener_is_rejected_until_tls_is_supported() {
    let mut config = Config::default();
    config.metrics.enabled = true;
    config.metrics.bind = "0.0.0.0:9464".to_owned();

    let error = config
        .validate()
        .expect_err("unsafe listener must be rejected");
    assert!(
        error
            .to_string()
            .contains("metrics.bind must remain loopback-only")
    );
}

#[test]
fn runtime_custom_metrics_enforce_a_bounded_safe_name_and_type() {
    let metrics = citadel::runtime::RuntimeMetrics::default();
    metrics
        .counter("matches_started", 1)
        .expect("valid counter");
    assert!(metrics.gauge("matches_started", 1.0).is_err());
    assert!(metrics.counter("player-123", 1).is_err());
}

#[tokio::test]
async fn scrape_includes_bounded_runtime_custom_metrics() {
    use citadel::repository::ApiKeyScope;
    use citadel::services::CreateApiKeyRequest;
    use citadel::time::{Clock, SystemClock};

    let mut config = Config::default();
    config.metrics.enabled = true;
    let app = citadel::App::new(config);
    app.runtime_metrics()
        .counter("matches_started", 3)
        .expect("record runtime metric");
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "scrape".into(),
                scopes: vec![ApiKeyScope::TelemetryRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("issue key");
    let response = citadel::http::metrics_router(app)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {}", issued.secret))
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("response");
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("read body");
    let body = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(
        body.contains("citadel_runtime_custom_matches_started_total 3\n"),
        "{body}"
    );
}

#[tokio::test]
async fn metrics_rejects_console_bearer_tokens() {
    use citadel::services::{ConsoleIdentity, ConsoleRole};

    let mut config = Config::default();
    config.metrics.enabled = true;
    let app = citadel::App::new(config);
    let token = app
        .console_tokens()
        .issue(ConsoleIdentity {
            username: "operator".into(),
            role: ConsoleRole::Admin,
        })
        .expect("issue console token");
    let response = citadel::http::metrics_router(app)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn metrics_rejects_api_keys_without_telemetry_read_scope() {
    use citadel::repository::ApiKeyScope;
    use citadel::services::CreateApiKeyRequest;
    use citadel::time::{Clock, SystemClock};

    let mut config = Config::default();
    config.metrics.enabled = true;
    let app = citadel::App::new(config);
    let key = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "audit-only".into(),
                scopes: vec![ApiKeyScope::AuditRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("issue API key");

    let response = citadel::http::metrics_router(app)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {}", key.secret))
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

async fn listener_request(addr: std::net::SocketAddr, method: &str, token: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect metrics listener");
    let request = format!(
        "{method} /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    String::from_utf8(response).expect("HTTP response is utf8")
}

#[tokio::test]
async fn dedicated_listener_serves_get_and_head_with_a_telemetry_key() {
    use citadel::repository::ApiKeyScope;
    use citadel::services::CreateApiKeyRequest;
    use citadel::time::{Clock, SystemClock};

    let mut config = Config::default();
    config.metrics.enabled = true;
    config.metrics.bind = "127.0.0.1:0".to_owned();
    let app = citadel::App::new(config);
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "listener scrape".into(),
                scopes: vec![ApiKeyScope::TelemetryRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("issue key");
    let listener = citadel::http::bind_metrics(&app)
        .await
        .expect("bind metrics listener");
    let addr = listener.local_addr().expect("listener address");
    let cancel = citadel::lifecycle::CancellationToken::new();
    let server = tokio::spawn(citadel::http::serve_metrics(listener, app, cancel.clone()));

    let get = listener_request(addr, "GET", &issued.secret).await;
    assert!(get.starts_with("HTTP/1.1 200"), "{get}");
    assert!(
        get.contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
        "{get}"
    );
    assert!(
        get.contains("# TYPE citadel_http_requests counter"),
        "{get}"
    );
    let head = listener_request(addr, "HEAD", &issued.secret).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(
        head.contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
        "{head}"
    );
    assert_eq!(
        head.split_once("\r\n\r\n").map(|(_, body)| body),
        Some(""),
        "{head}"
    );

    cancel.cancel();
    server
        .await
        .expect("metrics task joins")
        .expect("metrics server shuts down");
}

#[tokio::test]
async fn bind_metrics_rejects_an_unsafe_unvalidated_app_config() {
    let mut config = Config::default();
    config.metrics.enabled = true;
    config.metrics.bind = "0.0.0.0:0".to_owned();
    let app = citadel::App::new(config);

    let error = citadel::http::bind_metrics(&app)
        .await
        .expect_err("unsafe metrics bind must be rejected at the side-effecting boundary");
    assert!(error.to_string().contains("loopback-only"));
}
