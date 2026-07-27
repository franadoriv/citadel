//! Realtime authentication handshake policy and resolution.
//!
//! This is the security seam that binds an HTTP-issued session token to a
//! realtime connection. The [`Authenticator`] holds the node's
//! [`SessionService`](crate::services::SessionService) (for token validation) and
//! the configured auth stance, and resolves what the client presented in its
//! `KIND_AUTH` handshake frame into a single [`AuthOutcome`].
//!
//! Design (reviewed adversarially through adversarial review):
//!
//! - Account identity is resolved **only** through the session service, never
//!   from client payload; the resolved `user_id`/`session_id` is what binds the
//!   participant.
//! - A rejection is **coarse**: an unknown/expired/revoked/malformed token all
//!   collapse to [`RejectReason::AuthFailed`], so the handshake is not an
//!   enumeration oracle. A backend error also fails closed as `AuthFailed`.
//! - Auth-required mode **never** falls back to guest: a guest/token-less connect
//!   (and a non-handshake first frame) is refused.
//! - The token secret is never logged, traced, or embedded in an outcome.

use citadel_wire::protocol::{
    AUTH_REASON_AUTH_FAILED, AUTH_REASON_AUTH_REQUIRED, AUTH_REASON_PROTOCOL,
    encode_auth_authenticated, encode_auth_guest, encode_auth_rejected,
};

use crate::services::{SharedSessionService, ValidateSessionRequest};
use crate::session::{SessionTokenSecret, SessionValidation};
use crate::time::{Clock, SystemClock};

use super::registry::ParticipantIdentity;

/// What a client presented in (or in place of) its `KIND_AUTH` handshake frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresentedCredential {
    /// A non-empty `KIND_AUTH` body: a session access token to validate.
    Token(SessionTokenSecret),
    /// An empty `KIND_AUTH` body: an explicit request to connect as a guest.
    Guest,
    /// The first frame was not a `KIND_AUTH` frame (a pre-handshake/legacy
    /// client). Accepted as an implicit guest only when the stance allows it.
    NoHandshake,
    /// The `KIND_AUTH` body was present but malformed (non-utf8 or oversized):
    /// it cannot be a valid token, so it is refused as an auth failure.
    MalformedToken,
}

/// The coarse, non-leaking reason a handshake was refused. Mirrors the wire
/// `AUTH_REASON_*` classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The presented token failed validation (unknown/expired/revoked/malformed,
    /// all collapsed), or the backend could not validate it (fail closed).
    AuthFailed,
    /// A guest/token-less connect was refused because auth is required.
    AuthRequired,
    /// The handshake violated the protocol (a non-handshake first frame under
    /// auth-required mode).
    Protocol,
}

impl RejectReason {
    /// The coarse wire reason class sent to the client (never the precise cause).
    #[must_use]
    pub fn wire_reason_class(self) -> u8 {
        match self {
            Self::AuthFailed => AUTH_REASON_AUTH_FAILED,
            Self::AuthRequired => AUTH_REASON_AUTH_REQUIRED,
            Self::Protocol => AUTH_REASON_PROTOCOL,
        }
    }
}

/// The resolved outcome of a realtime handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthOutcome {
    /// The token validated; the connection binds to this account identity.
    Authenticated(ParticipantIdentity),
    /// The connection is accepted as an anonymous guest (no account bound).
    Guest,
    /// The handshake is refused; the connection must be closed without any
    /// registry/gauge state.
    Rejected(RejectReason),
}

impl AuthOutcome {
    /// Whether this outcome accepts the connection (authenticated or guest).
    #[must_use]
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Authenticated(_) | Self::Guest)
    }

    /// The identity to bind for this outcome: `Some` for authenticated, `None`
    /// for guest/rejected.
    #[must_use]
    pub fn identity(&self) -> Option<ParticipantIdentity> {
        match self {
            Self::Authenticated(identity) => Some(identity.clone()),
            _ => None,
        }
    }

    /// The `KIND_AUTH_RESULT` body to send the client for this outcome. A
    /// rejection carries only the coarse reason class; an authenticated result
    /// carries the resolved `user_id`; guest carries nothing.
    #[must_use]
    pub fn result_body(&self) -> Vec<u8> {
        match self {
            Self::Authenticated(identity) => encode_auth_authenticated(identity.user_id.as_str()),
            Self::Guest => encode_auth_guest(),
            Self::Rejected(reason) => encode_auth_rejected(reason.wire_reason_class()),
        }
    }
}

/// Resolves realtime handshakes against the node's session service and stance.
#[derive(Clone)]
pub struct Authenticator {
    /// The session service used to validate presented tokens. `None` only in
    /// standalone/test gateways with no identity backend; a token presented then
    /// fails closed.
    sessions: Option<SharedSessionService>,
    /// Whether a valid token is required (guests/token-less refused).
    require_auth: bool,
    /// Whether guests are accepted (ignored when `require_auth`).
    allow_guests: bool,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("has_session_service", &self.sessions.is_some())
            .field("require_auth", &self.require_auth)
            .field("allow_guests", &self.allow_guests)
            .finish()
    }
}

impl Authenticator {
    /// Build an authenticator with an explicit session service and stance.
    #[must_use]
    pub fn new(
        sessions: Option<SharedSessionService>,
        require_auth: bool,
        allow_guests: bool,
    ) -> Self {
        Self {
            sessions,
            require_auth,
            allow_guests,
        }
    }

    /// A permissive guest-only authenticator with no session backend, used by
    /// standalone/test gateways: tokens fail closed, guests are accepted.
    #[must_use]
    pub fn guest_only() -> Self {
        Self {
            sessions: None,
            require_auth: false,
            allow_guests: true,
        }
    }

    /// Whether the stance requires a valid token to connect.
    #[must_use]
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    /// Resolve a presented credential into an outcome.
    ///
    /// Never panics, never leaks the token or the precise validation failure.
    /// A backend error during validation fails closed (`AuthFailed`).
    pub async fn resolve(&self, presented: PresentedCredential) -> AuthOutcome {
        match presented {
            PresentedCredential::Token(token) => self.resolve_token(token).await,
            PresentedCredential::MalformedToken => AuthOutcome::Rejected(RejectReason::AuthFailed),
            PresentedCredential::Guest => self.resolve_guest(RejectReason::AuthRequired),
            PresentedCredential::NoHandshake => self.resolve_guest(RejectReason::Protocol),
        }
    }

    /// Validate a presented token and bind identity, failing closed on any error.
    async fn resolve_token(&self, token: SessionTokenSecret) -> AuthOutcome {
        let Some(sessions) = &self.sessions else {
            // No backend to validate against: never accept a token we cannot
            // verify.
            tracing::debug!("realtime auth: token presented but no session service; rejecting");
            return AuthOutcome::Rejected(RejectReason::AuthFailed);
        };
        let now = SystemClock.now();
        let request = ValidateSessionRequest {
            access_token: token,
            now,
        };
        match sessions.validate_session(request).await {
            Ok(SessionValidation::Valid(session)) => {
                AuthOutcome::Authenticated(ParticipantIdentity {
                    user_id: session.user_id,
                    session_id: session.session_id,
                    expires_at: session.expires_at,
                })
            }
            Ok(SessionValidation::Invalid(_reason)) => {
                // Collapse every invalidity to a single coarse failure so the
                // handshake is not an enumeration oracle.
                AuthOutcome::Rejected(RejectReason::AuthFailed)
            }
            Err(_e) => {
                // Fail closed on a backend error; never surface the detail.
                tracing::warn!("realtime auth: session validation backend error; failing closed");
                AuthOutcome::Rejected(RejectReason::AuthFailed)
            }
        }
    }

    /// Resolve a guest / non-handshake connect per the stance.
    ///
    /// `required_reject` is the reason used when auth is required: an explicit
    /// guest is `AuthRequired`, a non-handshake first frame is `Protocol`.
    fn resolve_guest(&self, required_reject: RejectReason) -> AuthOutcome {
        if self.require_auth {
            // Auth-required never falls back to guest.
            AuthOutcome::Rejected(required_reject)
        } else if self.allow_guests {
            AuthOutcome::Guest
        } else {
            AuthOutcome::Rejected(RejectReason::AuthRequired)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn token_without_backend_fails_closed() {
        let auth = Authenticator::guest_only();
        let token = SessionTokenSecret::new("some-token").expect("token");
        assert_eq!(
            auth.resolve(PresentedCredential::Token(token)).await,
            AuthOutcome::Rejected(RejectReason::AuthFailed)
        );
    }

    #[tokio::test]
    async fn guest_allowed_by_default() {
        let auth = Authenticator::guest_only();
        assert_eq!(
            auth.resolve(PresentedCredential::Guest).await,
            AuthOutcome::Guest
        );
        // A pre-handshake first frame is an implicit guest when allowed.
        assert_eq!(
            auth.resolve(PresentedCredential::NoHandshake).await,
            AuthOutcome::Guest
        );
    }

    #[tokio::test]
    async fn auth_required_refuses_guest_and_non_handshake() {
        let auth = Authenticator::new(None, true, true);
        assert_eq!(
            auth.resolve(PresentedCredential::Guest).await,
            AuthOutcome::Rejected(RejectReason::AuthRequired)
        );
        assert_eq!(
            auth.resolve(PresentedCredential::NoHandshake).await,
            AuthOutcome::Rejected(RejectReason::Protocol)
        );
    }

    #[tokio::test]
    async fn guests_disabled_refuses_guest() {
        let auth = Authenticator::new(None, false, false);
        assert_eq!(
            auth.resolve(PresentedCredential::Guest).await,
            AuthOutcome::Rejected(RejectReason::AuthRequired)
        );
    }

    #[tokio::test]
    async fn malformed_token_is_auth_failed() {
        let auth = Authenticator::guest_only();
        assert_eq!(
            auth.resolve(PresentedCredential::MalformedToken).await,
            AuthOutcome::Rejected(RejectReason::AuthFailed)
        );
    }
}
