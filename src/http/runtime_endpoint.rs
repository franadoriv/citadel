//! Transport-owned boundary for script-defined HTTP endpoints.
//!
//! The route is deliberately one reserved catch-all (`/ext/*`): static Citadel
//! routes are registered separately and a runtime never receives router access.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;

use crate::app::App;
use crate::error::AppError;
use crate::identity::User;
use crate::runtime::{
    RUNTIME_HTTP_ENDPOINT_PREFIX, RuntimeHttpAuth, RuntimeHttpEndpoint, RuntimeHttpMethod,
    RuntimeHttpOutcome, RuntimeHttpRequest,
};
use crate::services::{AuditEntry, ValidateSessionRequest};
use crate::session::SessionValidation;
use crate::time::{Clock, SystemClock};

use super::error::ApiError;
use super::player::access_bearer;

/// Mount the reserved runtime-owned route without exposing it in the normal
/// static API namespaces (`/v1`, `/console`, `/dashboard`, ...).
pub(super) fn routes() -> Router<App> {
    Router::new().route("/ext/*path", any(handler))
}

async fn handler(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    uri: Uri,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    app.metrics().record_http_request();
    let policy = &app.config().runtime.capabilities.custom_http_endpoints;
    if !policy.enabled {
        return Ok(StatusCode::NOT_FOUND.into_response());
    }
    let Some(method) = RuntimeHttpMethod::parse(method.as_str()) else {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    };
    let path = runtime_relative_path(&uri)
        .ok_or_else(|| AppError::validation("invalid runtime endpoint path"))?;
    let endpoint = RuntimeHttpEndpoint::new(method, path.clone(), RuntimeHttpAuth::Public)
        .map_err(|_| AppError::validation("invalid runtime endpoint path"))?;
    let runtime = app
        .realtime_gateway()
        .and_then(|gateway| gateway.runtime().cloned())
        .ok_or_else(|| AppError::not_found("runtime endpoint not found"))?;
    let Some(declared) = runtime
        .http_endpoints()
        .into_iter()
        .find(|declared| declared.method == endpoint.method && declared.path == endpoint.path)
    else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let user_id = authenticate_if_required(&app, &headers, declared.auth).await?;
    let limiter_key = format!(
        "{}:{}:{}",
        declared.full_path(),
        user_id.as_deref().unwrap_or("anon"),
        peer.ip()
    );
    if !app
        .runtime_http_endpoint_rate_limiter()
        .allow(limiter_key, policy.max_requests_per_minute)
    {
        return Ok(StatusCode::TOO_MANY_REQUESTS.into_response());
    }
    let headers = bounded_headers(headers)?;
    let body = to_bytes(body, policy.max_request_bytes)
        .await
        .map_err(|_| {
            AppError::validation("runtime endpoint request body exceeds configured limit")
        })?;
    let outcome = runtime.call_http_endpoint(RuntimeHttpRequest {
        method: declared.method,
        path: declared.path.clone(),
        headers,
        body: body.to_vec(),
        user_id: user_id.clone(),
    });
    record_invocation_audit(&app, &declared, user_id.as_deref(), &outcome);
    match outcome {
        RuntimeHttpOutcome::NotFound => Ok(StatusCode::NOT_FOUND.into_response()),
        RuntimeHttpOutcome::Failed => Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        RuntimeHttpOutcome::Response(response) => {
            bounded_response(response, policy.max_response_bytes)
        }
    }
}

/// Record the endpoint outcome without retaining request payloads, headers, or
/// bearer credentials. This is intentionally separate from console auditing:
/// these are externally invoked runtime routes, not operator mutations.
fn record_invocation_audit(
    app: &App,
    endpoint: &RuntimeHttpEndpoint,
    user_id: Option<&str>,
    outcome: &RuntimeHttpOutcome,
) {
    let (role, actor) = match user_id {
        Some(user_id) => ("player", user_id),
        None => ("anonymous", "-"),
    };
    let details = match outcome {
        RuntimeHttpOutcome::NotFound => "handler missing".to_string(),
        RuntimeHttpOutcome::Failed => "handler failed".to_string(),
        RuntimeHttpOutcome::Response(response) => format!("responded {}", response.status),
    };
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        actor,
        role,
        "runtime.http",
        endpoint.full_path(),
        details,
    ));
}

/// Request URI is the source of truth and avoids accepting decoded/normalized
/// route variants through an extracted wildcard path.
fn runtime_relative_path(uri: &Uri) -> Option<String> {
    uri.path()
        .strip_prefix(RUNTIME_HTTP_ENDPOINT_PREFIX)
        .map(str::to_string)
}

async fn authenticate_if_required(
    app: &App,
    headers: &HeaderMap,
    auth: RuntimeHttpAuth,
) -> Result<Option<String>, ApiError> {
    if auth == RuntimeHttpAuth::Public {
        return Ok(None);
    }
    let token = access_bearer(headers)?.ok_or_else(|| AppError::auth("authentication required"))?;
    let validation = app
        .session_service()
        .validate_session(ValidateSessionRequest {
            access_token: token,
            now: SystemClock.now(),
        })
        .await?;
    let SessionValidation::Valid(session) = validation else {
        return Err(AppError::auth("authentication failed").into());
    };
    let user = app
        .backend()
        .user_repository()
        .get_user(&session.user_id)
        .await?
        .filter(User::is_active)
        .ok_or_else(|| AppError::auth("authentication failed"))?;
    Ok(Some(user.id.as_str().to_string()))
}

fn bounded_headers(headers: HeaderMap) -> Result<BTreeMap<String, String>, ApiError> {
    const MAX_HEADERS: usize = 64;
    const MAX_HEADER_BYTES: usize = 16 * 1024;
    if headers.len() > MAX_HEADERS {
        return Err(AppError::validation(
            "runtime endpoint request headers exceed configured limit",
        )
        .into());
    }
    let mut total = 0usize;
    let mut result = BTreeMap::new();
    for (name, value) in &headers {
        // Session authentication is resolved by the transport; the bearer (or
        // cookie) must never be re-exposed to a script. It receives only the
        // resolved user id for an endpoint that explicitly requires a session.
        if is_sensitive_request_header(name.as_str()) {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| AppError::validation("runtime endpoint request headers are invalid"))?;
        total = total
            .saturating_add(name.as_str().len())
            .saturating_add(value.len());
        if total > MAX_HEADER_BYTES {
            return Err(AppError::validation(
                "runtime endpoint request headers exceed configured limit",
            )
            .into());
        }
        result.insert(name.as_str().to_string(), value.to_string());
    }
    Ok(result)
}

fn is_sensitive_request_header(name: &str) -> bool {
    matches!(
        name,
        "authorization" | "cookie" | "proxy-authorization" | "set-cookie"
    )
}

fn bounded_response(
    response: crate::runtime::RuntimeHttpResponse,
    max_response_bytes: usize,
) -> Result<Response, ApiError> {
    const MAX_HEADERS: usize = 64;
    const MAX_HEADER_BYTES: usize = 16 * 1024;
    if response.body.len() > max_response_bytes {
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    if response.headers.len() > MAX_HEADERS {
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    let status = StatusCode::from_u16(response.status)
        .map_err(|_| AppError::internal("runtime endpoint returned invalid status"))?;
    let mut builder = Response::builder().status(status);
    let mut header_bytes = 0usize;
    for (name, value) in response.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| AppError::internal("runtime endpoint returned invalid headers"))?;
        if is_forbidden_response_header(name.as_str()) {
            return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        let value = HeaderValue::from_str(&value)
            .map_err(|_| AppError::internal("runtime endpoint returned invalid headers"))?;
        header_bytes = header_bytes
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len());
        if header_bytes > MAX_HEADER_BYTES {
            return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(response.body))
        .map_err(|_| AppError::internal("runtime endpoint response could not be built").into())
}

fn is_forbidden_response_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "content-length"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::{Config, LuaExecutionMode};
    use crate::realtime::Gateway;
    use crate::runtime::outbound_http::OutboundHttpPolicy;
    use crate::runtime::{
        DEFAULT_STATIC_DATA_MAX_FILE_BYTES, LuaRuntime, RuntimeHttpEndpointPolicy,
    };

    use super::*;

    fn temp_scripts_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "citadel-runtime-endpoint-http-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create scripts directory");
        path
    }

    #[tokio::test]
    async fn reserved_runtime_route_dispatches_to_lua_without_shadowing_health() {
        let scripts = temp_scripts_dir();
        std::fs::write(
            scripts.join("main.lua"),
            r#"
                citadel.http.register("POST", "/echo", nil, function(request)
                    return {
                        status = 201,
                        headers = { ["content-type"] = "text/plain" },
                        body = request.headers.authorization or request.body,
                    }
                end)
            "#,
        )
        .expect("write script");
        let mut config = Config::default();
        config.runtime.capabilities.custom_http_endpoints.enabled = true;
        let runtime = LuaRuntime::load_with_static_data_and_mode_and_capability_policies(
            &scripts,
            100,
            None,
            DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            LuaExecutionMode::Sandboxed,
            OutboundHttpPolicy::default(),
            RuntimeHttpEndpointPolicy::from(&config.runtime.capabilities.custom_http_endpoints),
        )
        .expect("load runtime")
        .expect("entrypoint present");
        let app = App::new(config);
        let audit_log = Arc::clone(app.audit_log());
        let gateway = Arc::new(Gateway::with_metrics_and_runtime(
            Arc::clone(app.metrics()),
            Some(Arc::new(runtime)),
        ));
        app.attach_realtime_gateway(gateway);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HTTP server");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                super::super::router(app).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        });
        tokio::task::yield_now().await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let response = client
            .post(format!("http://{address}/ext/echo"))
            .header("authorization", "Bearer must-not-reach-script")
            .body("hello")
            .send()
            .await
            .expect("endpoint response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.text().await.expect("body"), "hello");
        let audit = audit_log.list(&Default::default());
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].action, "runtime.http");
        assert_eq!(audit[0].target, "/ext/echo");
        let health = client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);
        server.abort();
        std::fs::remove_dir_all(scripts).ok();
    }

    #[test]
    fn request_headers_redact_transport_credentials() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("cookie", HeaderValue::from_static("session=secret"));
        headers.insert("x-trace-id", HeaderValue::from_static("safe"));
        let headers = bounded_headers(headers).expect("bounded headers");
        assert_eq!(headers.get("x-trace-id"), Some(&"safe".to_string()));
        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("cookie"));
    }

    #[test]
    fn response_rejects_all_hop_by_hop_headers() {
        let response = bounded_response(
            crate::runtime::RuntimeHttpResponse {
                status: 200,
                headers: [("Content-Length".to_string(), "99".to_string())]
                    .into_iter()
                    .collect(),
                body: Vec::new(),
            },
            1024,
        )
        .expect("response built");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
