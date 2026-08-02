//! HTTP device/custom authentication routes.
//!
//! Two `POST` endpoints let a client register or log in over the network with a
//! device id or a custom id and receive a session token:
//!
//! - [`DEVICE_AUTH_PATH`] (`/v1/auth/device`)
//! - [`CUSTOM_AUTH_PATH`] (`/v1/auth/custom`)
//! - [`EMAIL_AUTH_PATH`] (`/v1/auth/email`)
//!
//! Both run through the node's composed, transactional
//! [`AuthenticationService`](crate::services::AuthenticationService): a
//! `(provider, external_id)` credential maps to exactly one account, account
//! creation is one transaction on the selected backend, and a session is issued
//! via the composed [`SessionService`](crate::services::SessionService). There is
//! no password: device/custom auth is id-based.
//!
//! Security properties (see the module-level docs in
//! [`super::error`]): a uniform `401` for every credential/account-status failure
//! (no existence oracle), typed `400`s for malformed input, generic `500`s that
//! never leak internals, and no logging of ids, usernames, metadata, or tokens.
//! `create` defaults to **false**: account creation is an explicit opt-in so the
//! endpoints cannot be used to amplify signups by accident.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Router, http::StatusCode};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, ErrorCategory};

use crate::app::App;
use crate::error::AppResult;
use crate::identity::{
    CustomId, DeviceId, DisplayName, EmailAddress, Password, UserMetadata, Username,
};
use crate::services::{
    AuthenticationOptions, AuthenticationOutcome, CustomAuthenticationRequest,
    DeviceAuthenticationRequest, EmailAuthenticationRequest,
};
use crate::session::NodeId;
use crate::time::{Clock, DurationMillis, SystemClock, TimestampMillis};

use super::error::ApiError;

/// Path for device authentication.
pub const DEVICE_AUTH_PATH: &str = "/v1/auth/device";
/// Path for custom authentication.
pub const CUSTOM_AUTH_PATH: &str = "/v1/auth/custom";
/// Path for email/password authentication.
pub const EMAIL_AUTH_PATH: &str = "/v1/auth/email";

/// Maximum accepted request body size (bytes). Auth bodies are tiny; a small cap
/// keeps a hostile client from streaming a large payload through JSON parsing.
const MAX_AUTH_BODY_BYTES: usize = 16 * 1024;

/// Default access-token lifetime: one hour.
pub(crate) const DEFAULT_SESSION_TTL: DurationMillis = DurationMillis::from_millis(60 * 60 * 1_000);
/// Default refresh-token lifetime: thirty days.
pub(crate) const DEFAULT_REFRESH_TTL: DurationMillis =
    DurationMillis::from_millis(30 * 24 * 60 * 60 * 1_000);

/// The JSON request body accepted by both auth routes.
///
/// `id` is the presented credential (device id or custom id). `create` defaults
/// to `false`; a client that wants registration must opt in explicitly. Unknown
/// fields are rejected (`deny_unknown_fields`): at an auth boundary a silently
/// dropped field (a typo'd `usernam`, a smuggled parameter) is a hazard, so a
/// stray field is a `400` rather than a confusing downstream failure. Field
/// values are validated by the domain newtypes, so oversized/malformed ids and
/// usernames become a typed `400`, never a panic.
///
/// `Debug` is redacted by hand: the `id` is a bearer-like credential and the
/// username/metadata are PII, so the derived `Debug` would turn any accidental
/// log/panic/trace into a credential disclosure.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthRequest {
    /// The presented device or custom id.
    pub id: String,
    /// Whether to create an account when the credential is unknown (default
    /// `false`).
    #[serde(default)]
    pub create: bool,
    /// Username to assign when creating an account (required on the create path).
    #[serde(default)]
    pub username: Option<String>,
    /// Optional display name for a newly created account.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Optional metadata (a JSON object) for a newly created account.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// The JSON request body accepted by the email/password route.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmailAuthRequest {
    /// Email address; normalization/validation happens in the domain type.
    pub email: String,
    /// Plaintext password, redacted from Debug and never persisted directly.
    pub password: String,
    /// Whether to create an account when the email is unknown.
    #[serde(default)]
    pub create: bool,
    /// Username required only when `create` is true.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

impl std::fmt::Debug for EmailAuthRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailAuthRequest")
            .field("email", &"[redacted]")
            .field("password", &"[redacted]")
            .field("create", &self.create)
            .field("username", &"[redacted]")
            .field("display_name", &"[redacted]")
            .field("metadata", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for AuthRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the credential or PII fields.
        f.debug_struct("AuthRequest")
            .field("id", &"[redacted]")
            .field("create", &self.create)
            .field("username", &"[redacted]")
            .field("display_name", &"[redacted]")
            .field("metadata", &"[redacted]")
            .finish()
    }
}

/// The JSON success response for both auth routes.
///
/// `token` is the access token; `refresh_token` is present only when the session
/// is refreshable. Token secrets are read out of the redacted
/// [`SessionTokenSecret`](crate::session::SessionTokenSecret) only here, at the
/// response boundary.
///
/// `Debug` is implemented by hand to redact the token fields: the derived
/// `Debug` would let a session secret escape into any log line, panic message,
/// or captured tracing span. The tokens leave the process only through the
/// serialized JSON body.
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct AuthResponse {
    /// The access token used to authenticate subsequent requests.
    pub token: String,
    /// The refresh token, if the session is refreshable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// The authenticated account's id.
    pub user_id: String,
    /// The account's username.
    pub username: String,
    /// Whether this request created a new account.
    pub created: bool,
}

impl std::fmt::Debug for AuthResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthResponse")
            .field("token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("user_id", &self.user_id)
            .field("username", &self.username)
            .field("created", &self.created)
            .finish()
    }
}

impl AuthResponse {
    /// Build the response body from an authentication outcome, exposing the token
    /// secrets only at this boundary.
    fn from_outcome(outcome: &AuthenticationOutcome) -> Self {
        Self {
            token: outcome.tokens.access.expose_secret().to_string(),
            refresh_token: outcome
                .tokens
                .refresh
                .as_ref()
                .map(|secret| secret.expose_secret().to_string()),
            user_id: outcome.user.id.as_str().to_string(),
            username: outcome.user.username.as_str().to_string(),
            created: outcome.account_created,
        }
    }
}

/// Register the auth routes on a router, applying a small body-size limit so a
/// hostile client cannot stream a large payload into JSON parsing.
pub(super) fn routes() -> Router<App> {
    Router::new()
        .route(DEVICE_AUTH_PATH, post(device_auth_handler))
        .route(CUSTOM_AUTH_PATH, post(custom_auth_handler))
        .route(EMAIL_AUTH_PATH, post(email_auth_handler))
        .layer(DefaultBodyLimit::max(MAX_AUTH_BODY_BYTES))
}

/// Translate the request into shared authentication options, validating every
/// user-supplied field through its domain newtype (each failure is a typed
/// `Validation` error → `400`). `now` is read from the system clock here — the
/// single production time seam — and the owning node comes from config.
fn build_options(app: &App, request: &AuthRequest) -> AppResult<AuthenticationOptions> {
    let username = request
        .username
        .as_ref()
        .map(|value| Username::new(value.clone()))
        .transpose()?;
    let display_name = request
        .display_name
        .as_ref()
        .map(|value| DisplayName::new(value.clone()))
        .transpose()?;
    let metadata = request
        .metadata
        .clone()
        .map(UserMetadata::new)
        .transpose()?;
    let now: TimestampMillis = SystemClock.now();
    let owner_node = NodeId::new(app.node_id().to_string())?;
    Ok(AuthenticationOptions {
        create_account: request.create,
        username,
        display_name,
        metadata,
        now,
        owner_node,
        session_ttl: DEFAULT_SESSION_TTL,
        refresh_ttl: Some(DEFAULT_REFRESH_TTL),
    })
}

/// Normalize a body-extraction rejection (malformed JSON, wrong/missing
/// content-type, unknown field, oversized body, wrong types) into the single
/// sanitized request-shape error. The rejection detail is never forwarded to the
/// client so a parser message cannot leak; it collapses to `400 invalid_request`.
fn map_body_rejection(rejection: JsonRejection) -> AppError {
    // Keep the operator-facing detail server-side only (logged by `ApiError`);
    // the client sees a fixed generic message.
    AppError::validation("invalid request body").with_detail(rejection.body_text())
}

/// Extract and validate the request body, taking a rejection into the uniform
/// request-shape error rather than axum's default response.
fn parse_request(body: Result<Json<AuthRequest>, JsonRejection>) -> Result<AuthRequest, ApiError> {
    match body {
        Ok(Json(request)) => Ok(request),
        Err(rejection) => Err(map_body_rejection(rejection).into()),
    }
}

/// `POST /v1/auth/device`: authenticate-or-create via a device id.
async fn device_auth_handler(
    State(app): State<App>,
    peer: Option<ConnectInfo<std::net::SocketAddr>>,
    body: Result<Json<AuthRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    // Count every auth attempt, including those rejected during body extraction,
    // so hostile traffic against the boundary is always visible in metrics.
    app.metrics().record_http_request();
    let request = parse_request(body)?;
    admit_opaque_auth(&app, peer_source(peer), request.create).await?;
    let device_id = DeviceId::new(request.id.clone())?;
    let options = build_options(&app, &request)?;
    let outcome = app
        .authentication_service()
        .authenticate_device(DeviceAuthenticationRequest { device_id, options })
        .await?;
    Ok(success(&outcome).into_response())
}

/// `POST /v1/auth/custom`: authenticate-or-create via a custom id.
async fn custom_auth_handler(
    State(app): State<App>,
    peer: Option<ConnectInfo<std::net::SocketAddr>>,
    body: Result<Json<AuthRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    app.metrics().record_http_request();
    let request = parse_request(body)?;
    admit_opaque_auth(&app, peer_source(peer), request.create).await?;
    let custom_id = CustomId::new(request.id.clone())?;
    let options = build_options(&app, &request)?;
    let outcome = app
        .authentication_service()
        .authenticate_custom(CustomAuthenticationRequest { custom_id, options })
        .await?;
    Ok(success(&outcome).into_response())
}

/// `POST /v1/auth/email`: register or sign in using an email and password.
async fn email_auth_handler(
    State(app): State<App>,
    peer: Option<ConnectInfo<std::net::SocketAddr>>,
    body: Result<Json<EmailAuthRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    app.metrics().record_http_request();
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => return Err(map_body_rejection(rejection).into()),
    };
    let email = EmailAddress::new(&request.email)?;
    let password = Password::new(request.password)?;
    admit_email_auth(&app, peer_source(peer), email.as_str(), request.create).await?;
    let options = build_options(
        &app,
        &AuthRequest {
            id: String::new(),
            create: request.create,
            username: request.username,
            display_name: request.display_name,
            metadata: request.metadata,
        },
    )?;
    let outcome = app
        .authentication_service()
        .authenticate_email(EmailAuthenticationRequest {
            email,
            password,
            options,
        })
        .await?;
    Ok(success(&outcome).into_response())
}

/// Return the address observed on Citadel's direct TCP connection. Deliberately
/// ignore `X-Forwarded-For`: unless a deployment configures and authenticates a
/// trusted reverse proxy, that header is client-controlled spoofing input.
fn peer_source(peer: Option<ConnectInfo<std::net::SocketAddr>>) -> String {
    peer.map_or_else(
        || "unavailable-peer".to_string(),
        |peer| peer.0.ip().to_string(),
    )
}

async fn admit_opaque_auth(app: &App, source: String, registration: bool) -> Result<(), ApiError> {
    let plan = app
        .auth_rate_limits()
        .opaque_credential(&source, registration);
    admit(app, &plan).await
}

async fn admit_email_auth(
    app: &App,
    source: String,
    email: &str,
    registration: bool,
) -> Result<(), ApiError> {
    let plan = app.auth_rate_limits().email(&source, email, registration);
    admit(app, &plan).await
}

/// Consume every admission counter before expensive credential verification.
/// The counter repository's fixed-window operation is atomic; a rejected plan
/// consumes no individual key. A `Permission` result can only come from this
/// dedicated limiter call and is deliberately rendered as one uniform 429.
async fn admit(app: &App, plan: &[crate::repository::ChatRateLimit]) -> Result<(), ApiError> {
    match app
        .chat()
        .consume_rate_limits(plan, app.auth_clock().now())
        .await
    {
        Ok(()) => Ok(()),
        Err(error) if error.category() == ErrorCategory::Permission => {
            let retry_after = plan
                .iter()
                .map(|rule| rule.window_ms.div_ceil(1_000))
                .max()
                .unwrap_or(1);
            Err(rate_limited(retry_after))
        }
        Err(error) => Err(error.into()),
    }
}

fn rate_limited(retry_after: u64) -> ApiError {
    ApiError::rate_limited(retry_after)
}

/// Build the success tuple: `201 Created` when a new account was registered,
/// `200 OK` for a returning account.
fn success(outcome: &AuthenticationOutcome) -> (StatusCode, Json<AuthResponse>) {
    let status = if outcome.account_created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    (status, Json(AuthResponse::from_outcome(outcome)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_defaults_to_false_when_absent() {
        let request: AuthRequest = serde_json::from_str(r#"{"id":"device-1"}"#).expect("parse");
        assert!(!request.create);
        assert!(request.username.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        // A stray/typo'd field is a parse error at the auth boundary, not a
        // silently ignored value.
        assert!(serde_json::from_str::<AuthRequest>(r#"{"id":"device-1","future":true}"#).is_err());
    }

    #[test]
    fn debug_redacts_credential_and_token_fields() {
        let request = AuthRequest {
            id: "secret-device-id".to_string(),
            create: true,
            username: Some("secret-name".to_string()),
            display_name: Some("secret-display".to_string()),
            metadata: Some(serde_json::json!({"k": "secret-meta"})),
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("secret-device-id"));
        assert!(!rendered.contains("secret-name"));
        assert!(!rendered.contains("secret-meta"));
        assert!(rendered.contains("[redacted]"));

        let response = AuthResponse {
            token: "access-secret".to_string(),
            refresh_token: Some("refresh-secret".to_string()),
            user_id: "user-1".to_string(),
            username: "player".to_string(),
            created: false,
        };
        let rendered = format!("{response:?}");
        assert!(!rendered.contains("access-secret"));
        assert!(!rendered.contains("refresh-secret"));
        assert!(rendered.contains("[redacted]"));
        // Non-secret fields remain visible for diagnostics.
        assert!(rendered.contains("user-1"));
    }

    #[test]
    fn response_omits_refresh_token_when_absent() {
        let response = AuthResponse {
            token: "access".to_string(),
            refresh_token: None,
            user_id: "user-1".to_string(),
            username: "player".to_string(),
            created: true,
        };
        let value = serde_json::to_value(&response).expect("serialize");
        assert!(value.get("refresh_token").is_none());
        assert_eq!(value["token"], "access");
        assert_eq!(value["created"], true);
    }

    #[test]
    fn default_ttls_are_consistent_and_positive() {
        // Refresh window must not be shorter than the access window, and both
        // must be non-zero, or the composed session service rejects issuance.
        assert!(DEFAULT_SESSION_TTL.as_millis() > 0);
        assert!(DEFAULT_REFRESH_TTL.as_millis() >= DEFAULT_SESSION_TTL.as_millis());
    }

    #[test]
    fn build_options_rejects_malformed_username() {
        let app = App::new(crate::config::Config::default());
        let request = AuthRequest {
            id: "device-1".to_string(),
            create: true,
            username: Some("bad\nname".to_string()),
            display_name: None,
            metadata: None,
        };
        let err = build_options(&app, &request).expect_err("control char rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[test]
    fn build_options_rejects_non_object_metadata() {
        let app = App::new(crate::config::Config::default());
        let request = AuthRequest {
            id: "device-1".to_string(),
            create: true,
            username: Some("player".to_string()),
            display_name: None,
            metadata: Some(serde_json::json!([1, 2, 3])),
        };
        let err = build_options(&app, &request).expect_err("array metadata rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[test]
    fn build_options_uses_defaults_and_node_owner() {
        let app = App::new(crate::config::Config::default());
        let request = AuthRequest {
            id: "device-1".to_string(),
            create: false,
            username: None,
            display_name: None,
            metadata: None,
        };
        let options = build_options(&app, &request).expect("options");
        assert!(!options.create_account);
        assert_eq!(options.owner_node.as_str(), app.node_id());
        assert_eq!(options.session_ttl, DEFAULT_SESSION_TTL);
        assert_eq!(options.refresh_ttl, Some(DEFAULT_REFRESH_TTL));
    }
}
