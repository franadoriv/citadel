//! Session domain contracts.
//!
//! A [`Session`] binds an authenticated [`UserId`] to an explicit owning
//! [`NodeId`] for a bounded lifetime, and moves through a small, explicit state
//! machine ([`SessionState`]). Every lifecycle decision takes an explicit
//! `now: TimestampMillis` so expiry, refresh, and revocation are deterministic
//! and unit-testable without the wall clock (see [`crate::time`]).
//!
//! This module is contract-only: it defines the domain types, the state machine,
//! and the session-token and ownership value objects. Concrete persistence
//! (session repository) and issuance (token signing) are provided by later tasks
//! behind [`crate::repository`] and [`crate::services`].

pub mod id;
pub mod ownership;
pub mod state;
pub mod token;

pub use id::{NodeId, SessionId};
pub use ownership::{
    OwnershipGeneration, ResolveSessionOwnerRequest, SessionDirectoryEntry, SessionOwnerLease,
    SessionOwnership,
};
pub use state::{
    RevocationReason, SessionInvalidity, SessionState, SessionStateKind, SessionValidation,
    ValidatedSession,
};
pub use token::{IssuedSessionTokens, SessionTokenRef, SessionTokenSecret};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::storage::UserId;
use crate::time::TimestampMillis;

/// An authenticated session bound to a user and an owning node.
///
/// Construct with [`Session::new`] (which enforces the timestamp invariants and
/// starts in [`SessionState::Active`]); drive lifecycle changes with
/// [`Session::refresh_at`], [`Session::expire_at`], and [`Session::revoke_at`].
///
/// The lifecycle `state` is intentionally private and reachable only through
/// [`Session::state`]/[`Session::state_kind`], so a terminal (`Expired`/
/// `Revoked`) session cannot be resurrected to `Active` by assigning the field
/// directly; every transition goes through the guarded methods. The private
/// field also forces construction through [`Session::new`] (a struct literal is
/// impossible outside this module), so the timestamp invariants always hold for
/// freshly built sessions. `Deserialize` is the one trusted rehydration path
/// used by the persistence layer to reload an already-validated record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// Opaque session identity (carries no routing information).
    pub id: SessionId,
    /// The authenticated account.
    pub user_id: UserId,
    /// The node that owns this session.
    pub owner_node: NodeId,
    /// When the session was issued (Unix millis).
    pub issued_at: TimestampMillis,
    /// When the access token expires (Unix millis); must be `> issued_at`.
    pub expires_at: TimestampMillis,
    /// When the refresh window closes (Unix millis), if refreshable; must be
    /// `>= expires_at`.
    pub refresh_expires_at: Option<TimestampMillis>,
    /// A non-secret handle for locating the session's token, if any.
    pub token_ref: Option<SessionTokenRef>,
    /// Current lifecycle state (private; mutate only via the transition methods).
    state: SessionState,
}

impl Session {
    /// Assemble a new `Active` session, validating the timestamp invariants.
    ///
    /// Invariants: `expires_at > issued_at`, and `refresh_expires_at`, when
    /// present, is `>= expires_at`.
    ///
    /// # Errors
    /// Returns a validation error if any invariant is violated.
    pub fn new(
        id: SessionId,
        user_id: UserId,
        owner_node: NodeId,
        issued_at: TimestampMillis,
        expires_at: TimestampMillis,
        refresh_expires_at: Option<TimestampMillis>,
        token_ref: Option<SessionTokenRef>,
    ) -> AppResult<Self> {
        if expires_at <= issued_at {
            return Err(AppError::validation(
                "session expires_at must be after issued_at",
            ));
        }
        if let Some(refresh) = refresh_expires_at
            && refresh < expires_at
        {
            return Err(AppError::validation(
                "session refresh_expires_at must not precede expires_at",
            ));
        }
        Ok(Self {
            id,
            user_id,
            owner_node,
            issued_at,
            expires_at,
            refresh_expires_at,
            token_ref,
            state: SessionState::Active,
        })
    }

    /// The current lifecycle state.
    #[must_use]
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    /// The coarse kind of the current state.
    #[must_use]
    pub fn state_kind(&self) -> SessionStateKind {
        self.state.kind()
    }

    /// Validate the session at `now`.
    ///
    /// A materialized `Expired`/`Revoked` state reports the corresponding
    /// invalidity. An `Active` session whose `expires_at` has passed is reported
    /// as `Expired` even though it has not been materialized yet, so callers
    /// never treat a lapsed session as valid.
    #[must_use]
    pub fn validate_at(&self, now: TimestampMillis) -> SessionValidation {
        match &self.state {
            SessionState::Revoked { .. } => SessionValidation::Invalid(SessionInvalidity::Revoked),
            SessionState::Expired { .. } => SessionValidation::Invalid(SessionInvalidity::Expired),
            SessionState::Active => {
                if now >= self.expires_at {
                    SessionValidation::Invalid(SessionInvalidity::Expired)
                } else {
                    SessionValidation::Valid(ValidatedSession {
                        session_id: self.id.clone(),
                        user_id: self.user_id.clone(),
                        owner_node: self.owner_node.clone(),
                        expires_at: self.expires_at,
                    })
                }
            }
        }
    }

    /// Whether the session can be refreshed at `now`.
    ///
    /// Refresh is allowed only from the `Active` state (never after explicit
    /// expiry or revocation) and only within the refresh window. An access token
    /// that has lapsed can still be refreshed while the refresh window is open.
    #[must_use]
    pub fn can_refresh_at(&self, now: TimestampMillis) -> bool {
        if self.state != SessionState::Active {
            return false;
        }
        match self.refresh_expires_at {
            Some(refresh_expires_at) => now < refresh_expires_at,
            None => false,
        }
    }

    /// Refresh the session, extending its access (and optional refresh) window.
    ///
    /// Keeps the state `Active` and updates the expiry timestamps and token
    /// reference. The new access expiry must be strictly after `now`, and the
    /// new refresh expiry (if any) must not precede the new access expiry.
    ///
    /// # Errors
    /// Returns [`ErrorCategory::Conflict`](crate::error::ErrorCategory::Conflict)
    /// if the session cannot be refreshed at `now` (terminal state or the refresh
    /// window has closed), or a validation error if the new timestamps are
    /// inconsistent.
    pub fn refresh_at(
        &mut self,
        now: TimestampMillis,
        new_expires_at: TimestampMillis,
        new_refresh_expires_at: Option<TimestampMillis>,
        new_token_ref: Option<SessionTokenRef>,
    ) -> AppResult<()> {
        if !self.can_refresh_at(now) {
            return Err(AppError::conflict(
                "session cannot be refreshed in its current state",
            ));
        }
        if new_expires_at <= now {
            return Err(AppError::validation(
                "refreshed session expires_at must be after now",
            ));
        }
        if let Some(refresh) = new_refresh_expires_at
            && refresh < new_expires_at
        {
            return Err(AppError::validation(
                "refreshed session refresh_expires_at must not precede expires_at",
            ));
        }
        self.expires_at = new_expires_at;
        self.refresh_expires_at = new_refresh_expires_at;
        self.token_ref = new_token_ref;
        Ok(())
    }

    /// Materialize the session as `Expired` at `now`.
    ///
    /// Only a live (`Active`) session past its `expires_at` boundary may be
    /// expired; this makes expiry idempotently reject terminal sessions and
    /// premature calls.
    ///
    /// # Errors
    /// Returns [`ErrorCategory::Conflict`](crate::error::ErrorCategory::Conflict)
    /// if the session is already terminal or `now` is before `expires_at`.
    pub fn expire_at(&mut self, now: TimestampMillis) -> AppResult<()> {
        if self.state != SessionState::Active {
            return Err(AppError::conflict(
                "only an active session can be marked expired",
            ));
        }
        if now < self.expires_at {
            return Err(AppError::conflict(
                "session cannot be expired before its expiry boundary",
            ));
        }
        self.state = SessionState::Expired { expired_at: now };
        Ok(())
    }

    /// Revoke the session at `now` for `reason`.
    ///
    /// # Errors
    /// Returns [`ErrorCategory::Conflict`](crate::error::ErrorCategory::Conflict)
    /// if the session is already terminal (expired or revoked).
    pub fn revoke_at(&mut self, now: TimestampMillis, reason: RevocationReason) -> AppResult<()> {
        if self.state.is_terminal() {
            return Err(AppError::conflict("only an active session can be revoked"));
        }
        self.state = SessionState::Revoked {
            revoked_at: now,
            reason,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;

    fn uid() -> UserId {
        UserId::new("u-1").expect("valid user id")
    }

    fn sid() -> SessionId {
        SessionId::new("sess-1").expect("valid session id")
    }

    fn node() -> NodeId {
        NodeId::new("node-a").expect("valid node id")
    }

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    /// A session issued at 100, access-expiring at 200, refreshable to 400.
    fn session() -> Session {
        Session::new(sid(), uid(), node(), ts(100), ts(200), Some(ts(400)), None)
            .expect("valid session")
    }

    #[test]
    fn new_enforces_timestamp_invariants() {
        // expires_at must be after issued_at.
        assert!(Session::new(sid(), uid(), node(), ts(100), ts(100), None, None).is_err());
        assert!(Session::new(sid(), uid(), node(), ts(100), ts(99), None, None).is_err());
        // refresh window must not precede access expiry.
        assert!(Session::new(sid(), uid(), node(), ts(100), ts(200), Some(ts(150)), None).is_err());
        assert!(Session::new(sid(), uid(), node(), ts(100), ts(200), Some(ts(200)), None).is_ok());
    }

    #[test]
    fn validate_at_reports_active_and_expired() {
        let s = session();
        // Before expiry: valid.
        let validation = s.validate_at(ts(150));
        assert!(validation.is_valid());
        if let SessionValidation::Valid(v) = validation {
            assert_eq!(v.session_id, sid());
            assert_eq!(v.user_id, uid());
            assert_eq!(v.owner_node, node());
            assert_eq!(v.expires_at, ts(200));
        }
        // At/after expiry: invalid (expired) even though state is still Active.
        assert_eq!(
            s.validate_at(ts(200)).invalidity(),
            Some(SessionInvalidity::Expired)
        );
        assert_eq!(
            s.validate_at(ts(999)).invalidity(),
            Some(SessionInvalidity::Expired)
        );
    }

    #[test]
    fn refresh_within_window_extends_and_stays_active() {
        let mut s = session();
        // Even past access expiry (250), still refreshable while window (<400) open.
        assert!(s.can_refresh_at(ts(250)));
        s.refresh_at(ts(250), ts(500), Some(ts(800)), None)
            .expect("refresh succeeds");
        assert_eq!(s.state_kind(), SessionStateKind::Active);
        assert_eq!(s.expires_at, ts(500));
        assert_eq!(s.refresh_expires_at, Some(ts(800)));
        assert!(s.validate_at(ts(300)).is_valid());
    }

    #[test]
    fn refresh_outside_window_is_rejected() {
        let mut s = session();
        assert!(!s.can_refresh_at(ts(400)));
        let err = s
            .refresh_at(ts(400), ts(600), None, None)
            .expect_err("refresh after window closes");
        assert_eq!(err.category(), ErrorCategory::Conflict);
    }

    #[test]
    fn refresh_rejects_backwards_new_expiry() {
        let mut s = session();
        let err = s
            .refresh_at(ts(250), ts(250), None, None)
            .expect_err("new expiry must be after now");
        assert_eq!(err.category(), ErrorCategory::Validation);
    }

    #[test]
    fn expire_requires_active_and_past_boundary() {
        let mut early = session();
        let err = early
            .expire_at(ts(150))
            .expect_err("cannot expire before boundary");
        assert_eq!(err.category(), ErrorCategory::Conflict);

        let mut s = session();
        s.expire_at(ts(250)).expect("expire after boundary");
        assert_eq!(s.state_kind(), SessionStateKind::Expired);
        // Idempotent re-expire is rejected (terminal state).
        assert_eq!(
            s.expire_at(ts(300)).expect_err("re-expire").category(),
            ErrorCategory::Conflict
        );
    }

    #[test]
    fn revoke_then_terminal_transitions_are_rejected() {
        let mut s = session();
        s.revoke_at(ts(150), RevocationReason::Logout)
            .expect("revoke active session");
        assert_eq!(s.state_kind(), SessionStateKind::Revoked);
        assert_eq!(
            s.validate_at(ts(150)).invalidity(),
            Some(SessionInvalidity::Revoked)
        );

        // Revoked is terminal: cannot re-revoke, expire, or refresh.
        assert_eq!(
            s.revoke_at(ts(160), RevocationReason::Admin)
                .expect_err("re-revoke")
                .category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            s.expire_at(ts(250)).expect_err("expire revoked").category(),
            ErrorCategory::Conflict
        );
        assert!(!s.can_refresh_at(ts(160)));
    }

    #[test]
    fn expired_session_cannot_be_revoked_or_refreshed() {
        let mut s = session();
        s.expire_at(ts(250)).expect("expire");
        assert_eq!(
            s.revoke_at(ts(260), RevocationReason::Security)
                .expect_err("revoke expired")
                .category(),
            ErrorCategory::Conflict
        );
        assert!(!s.can_refresh_at(ts(260)));
    }

    #[test]
    fn non_refreshable_session_reports_no_refresh() {
        let mut s =
            Session::new(sid(), uid(), node(), ts(100), ts(200), None, None).expect("session");
        assert!(!s.can_refresh_at(ts(150)));
        assert_eq!(
            s.refresh_at(ts(150), ts(300), None, None)
                .expect_err("no refresh window")
                .category(),
            ErrorCategory::Conflict
        );
    }
}
