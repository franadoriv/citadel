//! Authenticated player account and session lifecycle HTTP routes.
//!
//! This module intentionally exposes only a small, privacy-preserving player
//! surface. It is not a user directory: lookups are exact, authenticated, and
//! omit every inactive or unknown account. Account metadata, credential links,
//! and lifecycle state never cross this boundary.

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::{AppError, ErrorCategory};
use crate::identity::{DisplayName, User, UserId, Username};
use crate::services::{RefreshSessionRequest, SessionRevocationCommand, ValidateSessionRequest};
use crate::session::{RevocationReason, SessionId, SessionTokenSecret, SessionValidation};
use crate::time::{Clock, SystemClock, TimestampMillis};

use super::auth::{AuthResponse, DEFAULT_REFRESH_TTL, DEFAULT_SESSION_TTL};
use super::error::ApiError;

/// Read or update the caller's account.
pub const ACCOUNT_PATH: &str = "/v1/account";
/// Exact, authenticated lookup of known player profiles.
pub const PLAYER_LOOKUP_PATH: &str = "/v1/users/lookup";
/// Rotate a refresh token into a replacement session token pair.
pub const SESSION_REFRESH_PATH: &str = "/v1/session/refresh";
/// Revoke a session using its access bearer token and/or refresh token.
pub const SESSION_LOGOUT_PATH: &str = "/v1/session/logout";

const MAX_PLAYER_BODY_BYTES: usize = 16 * 1024;
const MAX_LOOKUP_VALUES: usize = 100;

/// The deliberately small public representation of a player account.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PublicProfile {
    pub user_id: String,
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl From<&User> for PublicProfile {
    fn from(user: &User) -> Self {
        Self {
            user_id: user.id.as_str().to_string(),
            username: user.username.as_str().to_string(),
            display_name: user
                .display_name
                .as_ref()
                .map(|display_name| display_name.as_str().to_string()),
        }
    }
}

/// PATCH body. `null` clears a display name, while an absent field preserves it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAccountRequest {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub display_name: Option<Option<String>>,
}

/// Exact lookup input. Supplying neither kind of key is rejected; at least one
/// exact id or exact username must be explicitly named.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupUsersRequest {
    #[serde(default)]
    pub user_ids: Vec<String>,
    #[serde(default)]
    pub usernames: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LookupUsersResponse {
    pub users: Vec<PublicProfile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefreshRequest {
    refresh_token: String,
}

impl std::fmt::Debug for RefreshRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshRequest")
            .field("refresh_token", &"[redacted]")
            .finish()
    }
}

/// An omitted `refresh_token` means logout uses the access bearer token only.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogoutRequest {
    #[serde(default)]
    refresh_token: Option<String>,
}

impl std::fmt::Debug for LogoutRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogoutRequest")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

pub(super) fn routes() -> Router<App> {
    Router::new()
        .route(
            ACCOUNT_PATH,
            get(get_account_handler).patch(update_account_handler),
        )
        .route(PLAYER_LOOKUP_PATH, post(lookup_users_handler))
        .route(SESSION_REFRESH_PATH, post(refresh_handler))
        .route(SESSION_LOGOUT_PATH, post(logout_handler))
        .layer(DefaultBodyLimit::max(MAX_PLAYER_BODY_BYTES))
}

fn now() -> TimestampMillis {
    SystemClock.now()
}

fn body_error(rejection: JsonRejection) -> ApiError {
    AppError::validation("invalid request body")
        .with_detail(rejection.body_text())
        .into()
}

/// Extract an opaque access bearer secret without placing it in errors/logs.
pub(crate) fn access_bearer(headers: &HeaderMap) -> Result<Option<SessionTokenSecret>, ApiError> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| AppError::auth("invalid bearer token"))?;
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(AppError::auth("invalid bearer token").into());
    };
    if token.is_empty() || token.contains(char::is_whitespace) {
        return Err(AppError::auth("invalid bearer token").into());
    }
    Ok(Some(
        SessionTokenSecret::new(token.to_string())
            .map_err(|_| AppError::auth("invalid bearer token"))?,
    ))
}

/// Validate the caller's access bearer and make sure its account is still
/// active. Every failure becomes the uniform authentication response.
async fn authenticated_user(app: &App, headers: &HeaderMap) -> Result<User, ApiError> {
    let token = access_bearer(headers)?.ok_or_else(|| AppError::auth("authentication required"))?;
    let validation = app
        .session_service()
        .validate_session(ValidateSessionRequest {
            access_token: token,
            now: now(),
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
    Ok(user)
}

async fn get_account_handler(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<PublicProfile>, ApiError> {
    app.metrics().record_http_request();
    let user = authenticated_user(&app, &headers).await?;
    Ok(Json(PublicProfile::from(&user)))
}

async fn update_account_handler(
    State(app): State<App>,
    headers: HeaderMap,
    body: Result<Json<UpdateAccountRequest>, JsonRejection>,
) -> Result<Json<PublicProfile>, ApiError> {
    app.metrics().record_http_request();
    let request = body.map_err(body_error)?.0;
    if request.username.is_none() && request.display_name.is_none() {
        return Err(AppError::validation("at least one mutable field is required").into());
    }
    let current = authenticated_user(&app, &headers).await?;
    let username = request
        .username
        .map(Username::new)
        .transpose()?
        .unwrap_or(current.username.clone());
    let display_name = request
        .display_name
        .map(|value| value.map(DisplayName::new).transpose())
        .transpose()?
        .unwrap_or(current.display_name.clone());
    let updated = User::new(
        current.id,
        username,
        display_name,
        current.metadata,
        current.created_at,
        now(),
        current.state,
    )?;
    let stored = app.backend().user_repository().update_user(updated).await?;
    Ok(Json(PublicProfile::from(&stored)))
}

async fn lookup_users_handler(
    State(app): State<App>,
    headers: HeaderMap,
    body: Result<Json<LookupUsersRequest>, JsonRejection>,
) -> Result<Json<LookupUsersResponse>, ApiError> {
    app.metrics().record_http_request();
    let request = body.map_err(body_error)?.0;
    let total = request.user_ids.len() + request.usernames.len();
    if total == 0 || total > MAX_LOOKUP_VALUES {
        return Err(AppError::validation("provide between 1 and 100 exact lookup keys").into());
    }
    let _caller = authenticated_user(&app, &headers).await?;
    let repo = app.backend().user_repository();
    let mut users = Vec::new();
    for id in request.user_ids {
        let id = UserId::new(id)?;
        if let Some(user) = repo.get_user(&id).await?.filter(User::is_active) {
            users.push(user);
        }
    }
    for username in request.usernames {
        let username = Username::new(username)?;
        if let Some(user) = repo
            .get_user_by_username(&username)
            .await?
            .filter(User::is_active)
            && !users.iter().any(|existing| existing.id == user.id)
        {
            users.push(user);
        }
    }
    Ok(Json(LookupUsersResponse {
        users: users.iter().map(PublicProfile::from).collect(),
    }))
}

async fn refresh_handler(
    State(app): State<App>,
    body: Result<Json<RefreshRequest>, JsonRejection>,
) -> Result<Json<AuthResponse>, ApiError> {
    app.metrics().record_http_request();
    let request = body.map_err(body_error)?.0;
    let refreshed = app
        .session_service()
        .refresh_session(RefreshSessionRequest {
            refresh_token: SessionTokenSecret::new(request.refresh_token)
                .map_err(|_| AppError::auth("invalid refresh token"))?,
            now: now(),
            owner_node: crate::session::NodeId::new(app.node_id().to_string())?,
            session_ttl: DEFAULT_SESSION_TTL,
            refresh_ttl: Some(DEFAULT_REFRESH_TTL),
        })
        .await?;
    let user = app
        .backend()
        .user_repository()
        .get_user(&refreshed.session.user_id)
        .await?
        .filter(User::is_active);
    let Some(user) = user else {
        // A state change racing refresh cannot leave a newly minted usable
        // session behind. The client receives the same uniform auth failure.
        let _ = revoke_for_refresh_security(&app, refreshed.session.id).await;
        return Err(AppError::auth("authentication failed").into());
    };
    Ok(Json(AuthResponse {
        token: refreshed.tokens.access.expose_secret().to_string(),
        refresh_token: refreshed
            .tokens
            .refresh
            .as_ref()
            .map(|token| token.expose_secret().to_string()),
        user_id: user.id.as_str().to_string(),
        username: user.username.as_str().to_string(),
        created: false,
    }))
}

async fn logout_handler(
    State(app): State<App>,
    headers: HeaderMap,
    body: Option<Json<LogoutRequest>>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    let request = body.map_or(
        LogoutRequest {
            refresh_token: None,
        },
        |body| body.0,
    );
    let access_session = match access_bearer(&headers)? {
        Some(token) => match app
            .session_service()
            .validate_session(ValidateSessionRequest {
                access_token: token,
                now: now(),
            })
            .await?
        {
            SessionValidation::Valid(session) => Some(session.session_id),
            SessionValidation::Invalid(_) => None,
        },
        None => None,
    };
    let refresh_session = match request.refresh_token {
        Some(token) => app
            .session_service()
            .session_for_refresh_token(
                SessionTokenSecret::new(token)
                    .map_err(|_| AppError::auth("invalid refresh token"))?,
            )
            .await?
            .map(|session| session.id),
        None => None,
    };

    // Both supplied credentials must name the same session. A mismatch is a
    // successful no-op, which is deliberately indistinguishable from a retry.
    let target = match (access_session, refresh_session) {
        (Some(access), Some(refresh)) if access == refresh => Some(access),
        (Some(_), Some(_)) => None,
        (Some(access), None) => Some(access),
        (None, Some(refresh)) => Some(refresh),
        (None, None) => None,
    };
    if let Some(session_id) = target {
        revoke_for_logout(&app, session_id).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

fn revocation_command(
    source: &'static str,
    session_id: SessionId,
    revoked_at: TimestampMillis,
) -> SessionRevocationCommand {
    // The session id and time make retries safe: duplicate commands fence an
    // already-closing connection, while a later revocation remains harmless.
    SessionRevocationCommand {
        revocation_id: format!(
            "{source}:{}:{}",
            session_id.as_str(),
            revoked_at.unix_millis()
        ),
        session_id,
        expected_generation: None,
    }
}

async fn revoke_for_refresh_security(app: &App, session_id: SessionId) -> Result<(), ApiError> {
    let revoked_at = now();
    app.session_revocation_coordinator()
        .revoke_local(
            revocation_command("refresh-security", session_id, revoked_at),
            revoked_at,
            RevocationReason::Security,
        )
        .await?;
    Ok(())
}

async fn revoke_for_logout(app: &App, session_id: SessionId) -> Result<(), ApiError> {
    let revoked_at = now();
    match app
        .session_revocation_coordinator()
        .revoke_local(
            revocation_command("logout", session_id, revoked_at),
            revoked_at,
            RevocationReason::Logout,
        )
        .await
    {
        Ok(_) => Ok(()),
        // A retry racing another logout or a terminal/unknown session is still
        // a successful logout. This endpoint intentionally never leaks state.
        Err(error)
            if matches!(
                error.category(),
                ErrorCategory::Conflict | ErrorCategory::NotFound
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::config::Config;
    use crate::realtime::{Gateway, Outbound, ParticipantIdentity, SessionHandle};
    use crate::services::CreateSessionRequest;
    use crate::session::NodeId;
    use crate::time::DurationMillis;
    use crate::transport::{Envelope, TransportKind};

    #[tokio::test]
    async fn refresh_race_security_revocation_fences_the_exact_live_session() {
        let app = App::new(Config::default());
        let gateway = Arc::new(Gateway::new());
        app.attach_realtime_gateway(Arc::clone(&gateway));
        let user_id = UserId::new("refresh-race-user").expect("user");
        let issued_at = now();
        let primary = app
            .session_service()
            .create_session(CreateSessionRequest {
                user_id: user_id.clone(),
                owner_node: NodeId::new(app.node_id().to_owned()).expect("node"),
                now: issued_at,
                session_ttl: DurationMillis::from_millis(60_000),
                refresh_ttl: Some(DurationMillis::from_millis(60_000)),
            })
            .await
            .expect("primary session");
        let sibling = app
            .session_service()
            .create_session(CreateSessionRequest {
                user_id: user_id.clone(),
                owner_node: NodeId::new(app.node_id().to_owned()).expect("node"),
                now: issued_at,
                session_ttl: DurationMillis::from_millis(60_000),
                refresh_ttl: Some(DurationMillis::from_millis(60_000)),
            })
            .await
            .expect("sibling session");
        let target = gateway.next_participant_id();
        let (target_tx, mut target_rx) = tokio::sync::mpsc::channel(4);
        gateway.register_session(SessionHandle {
            id: target,
            kind: TransportKind::WebSocket,
            outbound: target_tx,
            identity: Some(ParticipantIdentity {
                user_id: user_id.clone(),
                session_id: primary.session.id.clone(),
                expires_at: primary.session.expires_at,
            }),
        });
        let sibling_id = gateway.next_participant_id();
        let (sibling_tx, mut sibling_rx) = tokio::sync::mpsc::channel(4);
        gateway.register_session(SessionHandle {
            id: sibling_id,
            kind: TransportKind::WebSocket,
            outbound: sibling_tx,
            identity: Some(ParticipantIdentity {
                user_id,
                session_id: sibling.session.id,
                expires_at: sibling.session.expires_at,
            }),
        });
        assert!(gateway.registry().send_to(
            target,
            &Outbound::reliable(Envelope::new(700, b"queued".to_vec()))
        ));

        revoke_for_refresh_security(&app, primary.session.id)
            .await
            .expect("security revoke");
        assert!(
            !target_rx
                .try_recv()
                .expect("queued outbound")
                .is_deliverable(),
            "the close fence invalidates a queued reliable envelope"
        );
        assert_eq!(
            gateway.handle_inbound(target, &Envelope::new(701, b"late".to_vec())),
            0
        );
        assert!(!gateway.registry().send_to(
            target,
            &Outbound::reliable(Envelope::new(702, b"late".to_vec()))
        ));
        assert!(gateway.registry().send_to(
            sibling_id,
            &Outbound::reliable(Envelope::new(703, b"sibling".to_vec()))
        ));
        assert_eq!(
            sibling_rx
                .recv()
                .await
                .expect("sibling outbound")
                .envelope
                .body
                .as_ref(),
            b"sibling"
        );
    }
}
