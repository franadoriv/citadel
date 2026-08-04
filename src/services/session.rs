//! Session service and directory contracts.
//!
//! [`SessionService`] owns session issuance and lifecycle (create, validate,
//! refresh, revoke) independent of any storage or transport. [`SessionDirectory`]
//! owns the explicit session-to-node ownership resolution described in
//! `website/src/content/docs/guides/distributed-matchmaker.md`.
//!
//! Both traits are async (via [`async_trait`]) and object-safe, matching the
//! async repository contracts they build on. Token signing,
//! persistence, and the distributed directory substrate are later tasks; this
//! task fixes the contract shape.

use async_trait::async_trait;

use crate::error::AppResult;
use crate::session::{
    IssuedSessionTokens, NodeId, ResolveSessionOwnerRequest, RevocationReason, Session,
    SessionDirectoryEntry, SessionId, SessionOwnership, SessionTokenSecret, SessionValidation,
};
use crate::storage::UserId;
use crate::time::{DurationMillis, TimestampMillis};

use super::ServiceLifecycle;

/// A request to create a new session for an authenticated user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSessionRequest {
    /// The authenticated account.
    pub user_id: UserId,
    /// The node that will own the session.
    pub owner_node: NodeId,
    /// The current time (Unix millis).
    pub now: TimestampMillis,
    /// Time-to-live for the access token.
    pub session_ttl: DurationMillis,
    /// Optional time-to-live for the refresh token.
    pub refresh_ttl: Option<DurationMillis>,
}

/// A newly created session and its minted tokens.
///
/// `tokens` carries redacted secrets that must never be logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedSession {
    /// The persisted session.
    pub session: Session,
    /// The freshly minted tokens (secret; do not log).
    pub tokens: IssuedSessionTokens,
}

/// A request to validate an access token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateSessionRequest {
    /// The bearer access token (secret).
    pub access_token: SessionTokenSecret,
    /// The current time (Unix millis).
    pub now: TimestampMillis,
}

/// A request to refresh a session using its refresh token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshSessionRequest {
    /// The bearer refresh token (secret).
    pub refresh_token: SessionTokenSecret,
    /// The current time (Unix millis).
    pub now: TimestampMillis,
    /// The node handling the refresh (for issuance/logging). Refresh keeps the
    /// session's existing owner; moving ownership to a different node is a
    /// separate [`SessionDirectory`] operation, not a side effect of refresh.
    pub owner_node: NodeId,
    /// Time-to-live for the new access token.
    pub session_ttl: DurationMillis,
    /// Optional time-to-live for the new refresh token.
    pub refresh_ttl: Option<DurationMillis>,
}

/// A request to revoke a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeSessionRequest {
    /// The session to revoke.
    pub session_id: SessionId,
    /// When the revocation occurs (Unix millis).
    pub revoked_at: TimestampMillis,
    /// Why the session is revoked.
    pub reason: RevocationReason,
}

/// The session lifecycle boundary.
///
/// Implementations should return an internal [`SessionValidation::Invalid`]
/// distinguishing the cause, and normalize attacker-visible responses to a
/// single sanitized auth error at the API boundary.
#[async_trait]
pub trait SessionService: ServiceLifecycle + Send + Sync {
    /// Create and persist a new session, minting its tokens.
    ///
    /// # Errors
    /// Returns a validation error for inconsistent TTLs and repository errors on
    /// a backend failure.
    async fn create_session(&self, request: CreateSessionRequest) -> AppResult<CreatedSession>;

    /// Validate an access token at a point in time.
    ///
    /// # Errors
    /// Returns a repository error on a backend failure. A well-formed but
    /// unknown/expired/revoked token yields `Ok(SessionValidation::Invalid(..))`,
    /// not an error, so the caller decides how to surface it.
    async fn validate_session(
        &self,
        request: ValidateSessionRequest,
    ) -> AppResult<SessionValidation>;

    /// Resolve a refresh token to its session without rotating it.
    ///
    /// This narrow operation exists for logout: a caller holding only a refresh
    /// token may revoke that same session without first minting a new token.
    /// An unknown token yields `Ok(None)` so the HTTP boundary can make logout
    /// idempotent without turning it into a token-existence oracle.
    ///
    /// # Errors
    /// Returns a repository error on backend failure.
    async fn session_for_refresh_token(
        &self,
        refresh_token: SessionTokenSecret,
    ) -> AppResult<Option<Session>>;

    /// Refresh a session, issuing a new access (and optional refresh) token.
    ///
    /// # Errors
    /// Returns an auth error for an invalid/expired refresh token and a conflict
    /// error if the underlying session cannot be refreshed.
    async fn refresh_session(&self, request: RefreshSessionRequest) -> AppResult<CreatedSession>;

    /// Revoke a session.
    ///
    /// # Errors
    /// Returns a not-found error for an unknown session. Replaying a revoke of
    /// an already-terminal session is successful and returns that terminal
    /// session, making durable revocation safe to retry at the control boundary.
    async fn revoke_session(&self, request: RevokeSessionRequest) -> AppResult<Session>;
}

/// The explicit session-to-node ownership boundary.
///
/// Resolution never parses ownership out of a [`SessionId`]; it consults the
/// directory and returns [`SessionOwnership`].
///
/// Lease semantics (enforced by implementations): a higher
/// [`OwnershipGeneration`](crate::session::OwnershipGeneration) always wins;
/// `bind`/`renew` for a lower or equal-but-different-owner generation must fail
/// with a conflict, and `unbind`/`renew` apply only when the caller still holds
/// the current lease. The precise expected-generation preconditions are
/// finalized alongside the concrete directory implementation.
#[async_trait]
pub trait SessionDirectory: Send + Sync {
    /// Resolve which node owns a session.
    ///
    /// # Errors
    /// Returns a repository/transport error on a backend failure.
    async fn resolve_session_owner(
        &self,
        request: &ResolveSessionOwnerRequest,
    ) -> AppResult<SessionOwnership>;

    /// Bind (claim) ownership of a session for a node.
    ///
    /// # Errors
    /// Returns a conflict error if a newer lease already owns the session.
    async fn bind_session_owner(&self, entry: SessionDirectoryEntry) -> AppResult<()>;

    /// Release ownership of a session held by `owner_node`.
    ///
    /// # Errors
    /// Returns a backend error; releasing an unowned session is idempotent.
    async fn unbind_session_owner(
        &self,
        session_id: &SessionId,
        owner_node: &NodeId,
    ) -> AppResult<()>;

    /// Renew (extend) an existing ownership lease.
    ///
    /// # Errors
    /// Returns a conflict error if the lease no longer matches the current owner.
    async fn renew_session_owner(&self, entry: SessionDirectoryEntry) -> AppResult<()>;
}
