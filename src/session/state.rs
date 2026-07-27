//! Session lifecycle state and validation outcomes.
//!
//! A session moves through a small, explicit state machine. The transition
//! methods live on [`Session`](crate::session::Session); this module defines the
//! state values and the validation result types that distinguish *why* a session
//! is not usable.

use serde::{Deserialize, Serialize};

use crate::storage::UserId;
use crate::time::TimestampMillis;

use super::id::{NodeId, SessionId};

/// The coarse kind of a [`SessionState`], without its associated data.
///
/// Useful for logs, metrics labels, and match arms that only care about the
/// state category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionStateKind {
    /// The session is live (subject to an expiry check).
    Active,
    /// The session passed its expiry boundary.
    Expired,
    /// The session was explicitly revoked.
    Revoked,
}

impl SessionStateKind {
    /// Stable lowercase token for logs and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

/// Why a session was revoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RevocationReason {
    /// The user logged out.
    Logout,
    /// An operator/admin revoked the session.
    Admin,
    /// The owning account was disabled or tombstoned.
    UserDisabled,
    /// A newer session superseded this one.
    Superseded,
    /// Revoked for a security reason (suspected compromise).
    Security,
}

impl RevocationReason {
    /// Stable lowercase token for logs and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Logout => "logout",
            Self::Admin => "admin",
            Self::UserDisabled => "user_disabled",
            Self::Superseded => "superseded",
            Self::Security => "security",
        }
    }
}

/// The lifecycle state of a session, with the data captured at each transition.
///
/// `Expired` and `Revoked` are terminal: a session never returns to `Active`
/// from them. Note that `Active` is not the same as "valid"; an `Active` session
/// whose `expires_at` is in the past is treated as expired at validation time
/// (see [`Session::validate_at`](crate::session::Session::validate_at)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    /// Live; validity still depends on the expiry timestamp.
    Active,
    /// Materialized as expired at `expired_at`.
    Expired {
        /// When the session was marked expired.
        expired_at: TimestampMillis,
    },
    /// Revoked at `revoked_at` for `reason`.
    Revoked {
        /// When the session was revoked.
        revoked_at: TimestampMillis,
        /// Why the session was revoked.
        reason: RevocationReason,
    },
}

impl SessionState {
    /// The coarse kind of this state.
    #[must_use]
    pub const fn kind(&self) -> SessionStateKind {
        match self {
            Self::Active => SessionStateKind::Active,
            Self::Expired { .. } => SessionStateKind::Expired,
            Self::Revoked { .. } => SessionStateKind::Revoked,
        }
    }

    /// Whether this is a terminal state (`Expired` or `Revoked`).
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Active)
    }
}

/// Why a session is not currently usable.
///
/// These causes are kept distinct internally so services and operators can
/// reason about them, but attacker-facing responses should normalize all of
/// them to a single sanitized auth error so callers cannot distinguish, for
/// example, an unknown session from an expired one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionInvalidity {
    /// No such session (or its secret did not resolve).
    Unknown,
    /// The session passed its expiry boundary.
    Expired,
    /// The session was explicitly revoked.
    Revoked,
    /// The owning account is disabled or tombstoned.
    DisabledUser,
    /// The session's ownership lease is stale (node changed/expired).
    StaleOwnership,
}

impl SessionInvalidity {
    /// Stable lowercase token for logs and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::DisabledUser => "disabled_user",
            Self::StaleOwnership => "stale_ownership",
        }
    }
}

/// The identity and routing facts of a validated, currently-usable session.
///
/// Deliberately excludes any secret material; it carries only what an authorized
/// request handler needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedSession {
    /// The session identity.
    pub session_id: SessionId,
    /// The authenticated account.
    pub user_id: UserId,
    /// The node that owns this session.
    pub owner_node: NodeId,
    /// When the session expires (Unix millis).
    pub expires_at: TimestampMillis,
}

/// The result of validating a session at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionValidation {
    /// The session is usable; carries the sanitized session facts.
    Valid(ValidatedSession),
    /// The session is not usable; carries the internal reason.
    Invalid(SessionInvalidity),
}

impl SessionValidation {
    /// Whether the session validated successfully.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        matches!(self, Self::Valid(_))
    }

    /// The invalidity reason, if any.
    #[must_use]
    pub const fn invalidity(&self) -> Option<SessionInvalidity> {
        match self {
            Self::Valid(_) => None,
            Self::Invalid(reason) => Some(*reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_kind_and_terminality() {
        assert_eq!(SessionState::Active.kind(), SessionStateKind::Active);
        assert!(!SessionState::Active.is_terminal());

        let expired = SessionState::Expired {
            expired_at: TimestampMillis::from_unix_millis(1),
        };
        assert_eq!(expired.kind(), SessionStateKind::Expired);
        assert!(expired.is_terminal());

        let revoked = SessionState::Revoked {
            revoked_at: TimestampMillis::from_unix_millis(1),
            reason: RevocationReason::Logout,
        };
        assert_eq!(revoked.kind(), SessionStateKind::Revoked);
        assert!(revoked.is_terminal());
    }

    #[test]
    fn stable_label_tokens() {
        assert_eq!(SessionStateKind::Active.as_str(), "active");
        assert_eq!(RevocationReason::UserDisabled.as_str(), "user_disabled");
        assert_eq!(
            SessionInvalidity::StaleOwnership.as_str(),
            "stale_ownership"
        );
    }

    #[test]
    fn validation_helpers() {
        let invalid = SessionValidation::Invalid(SessionInvalidity::Expired);
        assert!(!invalid.is_valid());
        assert_eq!(invalid.invalidity(), Some(SessionInvalidity::Expired));
    }
}
