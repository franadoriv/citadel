//! The admin console JSON API (`/console/v1/*`, ).
//!
//! This is the operator-facing HTTP surface behind the console SPA. It is
//! deliberately separate from the game-client API (`/v1/*`): different
//! credentials (static `[console]` operator login, not player sessions),
//! different authorization model (coarse `admin`/`viewer` roles), and no
//! participation in the client SDK contract, so changes here never trigger the
//! SDK-sync fan-out.
//!
//! Routes:
//!
//! - [`LOGIN_PATH`] (`POST`, public): exchange `[console]` credentials for an
//!   opaque bearer token (see [`ConsoleTokenStore`](crate::services::ConsoleTokenStore)).
//! - [`ME_PATH`] (`GET`): the authenticated operator identity.
//! - [`TELEMETRY_PATH`] (`GET`): authenticated host CPU, memory, and mounted
//!   filesystem capacity for the Status dashboard.
//! - One route per console section ([`SECTION_PATHS`]). Sections whose backend
//!   has not landed yet answer `501 Not Implemented` — authenticated, routed,
//!   and JSON-shaped, so the SPA can treat them uniformly. Each section task
//!   (..) replaces its stub in its own module.
//!
//! Security: bearer auth on everything but login; a uniform `401` for bad or
//! expired tokens; `403` when a `viewer` attempts a mutation (enforced by the
//! owning handlers via [`ConsoleIdentity::require_admin`]); small body limits;
//! no token or password ever logged.

pub mod accounts;
pub mod audit;
pub mod chat;
pub mod config;
pub mod database;
pub mod errors;
pub mod groups;
pub mod leaderboards;
pub mod matches;
pub mod notifications;
pub mod purchases;
pub mod runtime;
pub mod storage;
pub mod telemetry;
pub mod tournaments;

use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::{AppError, ErrorCategory};
use crate::services::{AuditEntry, ConsoleIdentity, verify_login};
use crate::time::{Clock, SystemClock};

use super::error::{ApiError, ErrorBody};

pub use accounts::ACCOUNTS_PATH;
pub use audit::AUDIT_PATH;
pub use chat::CHAT_PATH;
pub use config::CONFIG_PATH;
pub use database::DATABASE_PATH;
pub use errors::ERRORS_PATH;
pub use groups::GROUPS_PATH;
pub use leaderboards::LEADERBOARDS_PATH;
pub use matches::MATCHES_PATH;
pub use notifications::NOTIFICATIONS_PATH;
pub use purchases::{PURCHASES_PATH, SUBSCRIPTIONS_PATH};
pub use runtime::RUNTIME_PATH;
pub use storage::STORAGE_PATH;
pub use telemetry::TELEMETRY_PATH;
pub use tournaments::TOURNAMENTS_PATH;

/// Path prefix shared by every console API route.
pub const CONSOLE_API_PREFIX: &str = "/console/v1";

/// Console operator login route (`POST`, the only unauthenticated route).
pub const LOGIN_PATH: &str = "/console/v1/login";

/// Authenticated operator identity route (`GET`).
pub const ME_PATH: &str = "/console/v1/me";

/// One `GET` route per console sidebar section.
///
/// Exposed so integration tests can assert every section is routed (a stub
/// answers `501`, never `404`) without duplicating the path strings.
pub const SECTION_PATHS: &[&str] = &[
    "/console/v1/config",
    "/console/v1/audit",
    "/console/v1/errors",
    "/console/v1/storage",
    "/console/v1/database",
    "/console/v1/matches",
    "/console/v1/runtime",
    "/console/v1/accounts",
    "/console/v1/groups",
    "/console/v1/chat",
    "/console/v1/notifications",
    "/console/v1/leaderboards",
    "/console/v1/tournaments",
    "/console/v1/purchases",
    "/console/v1/subscriptions",
];

/// Section routes whose backend has landed (served by their own module).
///
/// Every [`SECTION_PATHS`] entry NOT listed here is registered as a `501`
/// stub. A section task moves its path from the stub set to this list in the
/// same change that adds its module — a path present in both would panic at
/// router build (duplicate route), which is the desired failure mode.
pub const IMPLEMENTED_SECTION_PATHS: &[&str] = &[
    audit::AUDIT_PATH,
    errors::ERRORS_PATH,
    config::CONFIG_PATH,
    storage::STORAGE_PATH,
    database::DATABASE_PATH,
    matches::MATCHES_PATH,
    runtime::RUNTIME_PATH,
    accounts::ACCOUNTS_PATH,
    groups::GROUPS_PATH,
    chat::CHAT_PATH,
    notifications::NOTIFICATIONS_PATH,
    leaderboards::LEADERBOARDS_PATH,
    tournaments::TOURNAMENTS_PATH,
    purchases::PURCHASES_PATH,
    purchases::SUBSCRIPTIONS_PATH,
];

/// Maximum accepted request body size (bytes). Console bodies are small JSON
/// documents; the cap keeps a hostile client from streaming large payloads
/// through JSON parsing. Section tasks with bigger payloads (e.g. storage
/// object writes) may raise their own route-local limit deliberately.
const MAX_CONSOLE_BODY_BYTES: usize = 64 * 1024;

/// The JSON body accepted by [`LOGIN_PATH`].
///
/// `Debug` is redacted by hand: the password is a credential and must not
/// escape into logs, panics, or traces.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    /// Configured operator username.
    pub username: String,
    /// Operator password (admin or viewer).
    pub password: String,
}

impl std::fmt::Debug for LoginRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginRequest")
            .field("username", &self.username)
            .field("password", &"[redacted]")
            .finish()
    }
}

/// The JSON success response for [`LOGIN_PATH`].
///
/// `Debug` is redacted by hand so the bearer token only leaves the process in
/// the serialized response body.
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct LoginResponse {
    /// Opaque bearer token for subsequent `/console/v1/*` requests.
    pub token: String,
    /// Granted role: `admin` or `viewer`.
    pub role: &'static str,
    /// Token lifetime from now, in whole seconds.
    pub expires_in_sec: u64,
}

impl std::fmt::Debug for LoginResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginResponse")
            .field("token", &"[redacted]")
            .field("role", &self.role)
            .field("expires_in_sec", &self.expires_in_sec)
            .finish()
    }
}

/// The JSON response for [`ME_PATH`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MeResponse {
    /// Operator username the token was issued to.
    pub username: String,
    /// Operator role: `admin` or `viewer`.
    pub role: &'static str,
}

/// Bearer-token extractor: any handler taking [`ConsoleIdentity`] is an
/// authenticated console route.
///
/// Every failure mode (missing header, malformed header, unknown token,
/// expired token) collapses to the uniform `401` via
/// [`ApiError`](super::error::ApiError), so the boundary leaks nothing about
/// which part was wrong.
#[async_trait::async_trait]
impl FromRequestParts<App> for ConsoleIdentity {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(unauthorized)?;
        app.console_tokens()
            .validate(token)
            .ok_or_else(unauthorized)
    }
}

/// The uniform console auth failure (401 at the boundary).
fn unauthorized() -> ApiError {
    AppError::auth("console authentication failed").into()
}

/// Register the console API routes, applying the shared body-size limit.
pub(super) fn routes() -> Router<App> {
    let mut router = Router::new()
        .route(LOGIN_PATH, post(login_handler))
        .route(ME_PATH, get(me_handler))
        .route(audit::AUDIT_PATH, get(audit::list_handler))
        .route(errors::ERRORS_PATH, get(errors::list_handler))
        .route(telemetry::TELEMETRY_PATH, get(telemetry::get_handler))
        .route(config::CONFIG_PATH, get(config::get_handler))
        .route(
            accounts::ACCOUNTS_PATH,
            get(accounts::list_handler).post(accounts::create_handler),
        )
        .route(
            accounts::ACCOUNT_DETAIL_PATH,
            get(accounts::detail_handler)
                .put(accounts::update_handler)
                .delete(accounts::delete_handler),
        )
        .route(accounts::ACCOUNT_BAN_PATH, post(accounts::ban_handler))
        .route(accounts::ACCOUNT_UNBAN_PATH, post(accounts::unban_handler))
        .route(accounts::ACCOUNT_EXPORT_PATH, get(accounts::export_handler))
        .route(
            accounts::ACCOUNT_WALLET_PATH,
            get(accounts::wallet_handler).post(accounts::wallet_adjust_handler),
        )
        .route(
            accounts::ACCOUNT_FRIENDS_PATH,
            get(accounts::friends_handler).post(accounts::friend_add_handler),
        )
        .route(
            accounts::ACCOUNT_FRIEND_PATH,
            axum::routing::delete(accounts::friend_remove_handler),
        )
        .route(runtime::RUNTIME_PATH, get(runtime::get_handler))
        .route(runtime::RUNTIME_RPC_PATH, post(runtime::rpc_handler))
        .route(matches::MATCHES_PATH, get(matches::list_handler))
        .route(matches::MATCH_DETAIL_PATH, get(matches::detail_handler))
        .route(storage::STORAGE_PATH, get(storage::collections_handler))
        .route(database::DATABASE_PATH, get(database::tables_handler))
        .route(database::DATABASE_ROWS_PATH, post(database::rows_handler))
        .route(database::DATABASE_ROW_PATH, post(database::row_handler))
        .route(database::DATABASE_TABLE_PATH, get(database::table_handler))
        .route(storage::STORAGE_COLLECTION_PATH, get(storage::list_handler))
        .route(
            storage::STORAGE_OBJECT_PATH,
            get(storage::get_handler)
                .put(storage::write_handler)
                .delete(storage::delete_handler)
                // Object values can exceed the console default; the inner
                // route layer overrides the router-wide body limit below.
                .route_layer(storage::body_limit()),
        )
        .route(
            groups::GROUPS_PATH,
            get(groups::list_handler).post(groups::create_handler),
        )
        .route(
            groups::GROUP_DETAIL_PATH,
            get(groups::detail_handler)
                .put(groups::update_handler)
                .delete(groups::delete_handler),
        )
        .route(groups::GROUP_MEMBERS_PATH, post(groups::add_member_handler))
        .route(
            groups::GROUP_MEMBER_PROMOTE_PATH,
            post(groups::promote_handler),
        )
        .route(
            groups::GROUP_MEMBER_DEMOTE_PATH,
            post(groups::demote_handler),
        )
        .route(groups::GROUP_MEMBER_KICK_PATH, post(groups::kick_handler))
        .route(chat::CHAT_PATH, get(chat::channels_handler))
        .route(
            chat::CHAT_MESSAGES_PATH,
            get(chat::messages_handler).post(chat::append_handler),
        )
        .route(
            chat::CHAT_MESSAGE_PATH,
            delete(chat::delete_message_handler),
        )
        .route(
            notifications::NOTIFICATIONS_PATH,
            get(notifications::list_handler).post(notifications::send_handler),
        )
        .route(
            notifications::NOTIFICATION_ID_PATH,
            delete(notifications::delete_handler),
        )
        .route(
            leaderboards::LEADERBOARDS_PATH,
            get(leaderboards::list_handler).post(leaderboards::create_handler),
        )
        .route(
            leaderboards::LEADERBOARD_PATH,
            delete(leaderboards::delete_handler),
        )
        .route(
            leaderboards::LEADERBOARD_RECORDS_PATH,
            get(leaderboards::records_handler).post(leaderboards::submit_handler),
        )
        .route(
            leaderboards::LEADERBOARD_RECORD_PATH,
            delete(leaderboards::delete_record_handler),
        )
        .route(
            tournaments::TOURNAMENTS_PATH,
            get(tournaments::list_handler).post(tournaments::create_handler),
        )
        .route(
            tournaments::TOURNAMENT_PATH,
            get(tournaments::detail_handler),
        )
        .route(
            tournaments::TOURNAMENT_TRANSITION_PATH,
            post(tournaments::transition_handler),
        )
        .route(
            tournaments::TOURNAMENT_ENTRIES_PATH,
            get(tournaments::entries_handler),
        )
        .route(
            tournaments::TOURNAMENT_RESULTS_PATH,
            get(tournaments::results_handler),
        )
        .route(
            purchases::PURCHASES_PATH,
            get(purchases::list_handler).post(purchases::validate_handler),
        )
        .route(
            purchases::PURCHASE_DETAIL_PATH,
            get(purchases::detail_handler),
        )
        .route(
            purchases::SUBSCRIPTIONS_PATH,
            get(purchases::subscriptions_handler),
        );
    for path in SECTION_PATHS
        .iter()
        .filter(|path| !IMPLEMENTED_SECTION_PATHS.contains(path))
    {
        router = router.route(path, get(stub_handler));
    }
    router.layer(DefaultBodyLimit::max(MAX_CONSOLE_BODY_BYTES))
}

/// `POST /console/v1/login`: exchange operator credentials for a bearer token.
async fn login_handler(
    State(app): State<App>,
    peer: Option<ConnectInfo<std::net::SocketAddr>>,
    body: Result<Json<LoginRequest>, JsonRejection>,
) -> Result<Json<LoginResponse>, ApiError> {
    app.metrics().record_http_request();
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    // Consume the admission counters before verifying the credential, so a
    // password-guessing campaign is bounded regardless of the outcome.
    admit_console_login(&app, &peer_source(peer), &request.username).await?;
    let now = SystemClock.now();
    let Some(role) = verify_login(&app.config().console, &request.username, &request.password)
    else {
        // Record the attempt (presented username only — never the password) so
        // operators can see brute-force pressure in the trail.
        app.audit_log().record(AuditEntry::new(
            now,
            request.username,
            "-",
            "console.login_failed",
            "console",
            "invalid credentials",
        ));
        return Err(unauthorized());
    };
    let token = app.console_tokens().issue(ConsoleIdentity {
        username: request.username.clone(),
        role,
    })?;
    app.audit_log().record(AuditEntry::new(
        now,
        request.username,
        role.as_str(),
        "console.login",
        "console",
        format!("login succeeded ({})", role.as_str()),
    ));
    Ok(Json(LoginResponse {
        token,
        role: role.as_str(),
        expires_in_sec: app.console_tokens().ttl().as_secs(),
    }))
}

/// Return the address observed on Citadel's direct TCP connection. Deliberately
/// ignore `X-Forwarded-For`, for the same reason as the player auth surface:
/// unless a deployment configures and authenticates a trusted reverse proxy,
/// that header is client-controlled spoofing input.
fn peer_source(peer: Option<ConnectInfo<std::net::SocketAddr>>) -> String {
    peer.map_or_else(
        || "unavailable-peer".to_string(),
        |peer| peer.0.ip().to_string(),
    )
}

/// Consume the console login admission counters before credential verification.
///
/// The counter repository's fixed-window operation is atomic; a rejected plan
/// consumes no individual key. A `Permission` result can only come from this
/// dedicated limiter call and is rendered as one uniform 429, so it never leaks
/// whether the presented username exists.
async fn admit_console_login(app: &App, source: &str, username: &str) -> Result<(), ApiError> {
    let plan = app.auth_rate_limits().console_login(source, username);
    match app
        .chat()
        .consume_rate_limits(&plan, app.auth_clock().now())
        .await
    {
        Ok(()) => Ok(()),
        Err(error) if error.category() == ErrorCategory::Permission => {
            let retry_after = plan
                .iter()
                .map(|rule| rule.window_ms.div_ceil(1_000))
                .max()
                .unwrap_or(1);
            Err(ApiError::rate_limited(retry_after))
        }
        Err(error) => Err(error.into()),
    }
}

/// `GET /console/v1/me`: the authenticated operator identity.
async fn me_handler(State(app): State<App>, operator: ConsoleIdentity) -> Json<MeResponse> {
    app.metrics().record_http_request();
    Json(MeResponse {
        username: operator.username,
        role: operator.role.as_str(),
    })
}

/// Shared `501` stub for sections whose backend task has not landed yet.
///
/// Requires authentication like every real section will, so the SPA exercises
/// the same auth path regardless of section maturity.
async fn stub_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
) -> (StatusCode, Json<ErrorBody>) {
    app.metrics().record_http_request();
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorBody {
            code: "not_implemented",
            message: "this console section's backend is not implemented yet".to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_paths_live_under_the_console_prefix() {
        assert!(LOGIN_PATH.starts_with(CONSOLE_API_PREFIX));
        assert!(ME_PATH.starts_with(CONSOLE_API_PREFIX));
        for path in SECTION_PATHS {
            assert!(
                path.starts_with(CONSOLE_API_PREFIX),
                "section path outside prefix: {path}"
            );
        }
    }

    #[test]
    fn section_paths_are_unique_and_cover_the_sidebar() {
        let unique: std::collections::HashSet<_> = SECTION_PATHS.iter().collect();
        assert_eq!(unique.len(), SECTION_PATHS.len());
        // One route per placeholder sidebar section (purchases splits into
        // purchases + subscriptions, and Status stays on the public /status).
        assert_eq!(SECTION_PATHS.len(), 15);
    }

    #[test]
    fn implemented_sections_are_a_subset_of_section_paths() {
        for path in IMPLEMENTED_SECTION_PATHS {
            assert!(
                SECTION_PATHS.contains(path),
                "implemented path missing from SECTION_PATHS: {path}"
            );
        }
    }

    #[test]
    fn login_request_rejects_unknown_fields_and_redacts_debug() {
        assert!(
            serde_json::from_str::<LoginRequest>(
                r#"{"username":"admin","password":"x","extra":1}"#
            )
            .is_err()
        );
        let request = LoginRequest {
            username: "admin".to_string(),
            password: "super-secret".to_string(),
        };
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("super-secret"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn login_response_redacts_token_in_debug() {
        let response = LoginResponse {
            token: "bearer-secret".to_string(),
            role: "admin",
            expires_in_sec: 3_600,
        };
        let rendered = format!("{response:?}");
        assert!(!rendered.contains("bearer-secret"));
        assert!(rendered.contains("admin"));
    }
}
