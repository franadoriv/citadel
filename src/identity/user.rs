//! User account domain types.
//!
//! These model a Citadel account independent of any persistence backend or
//! transport. The account identity itself is [`crate::storage::UserId`], reused
//! here rather than redefined so storage, identity, and future services all
//! speak the same id type.

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::storage::UserId;
use crate::time::TimestampMillis;
use crate::validate;

/// Maximum byte length for a username.
const MAX_USERNAME_LEN: usize = 128;
/// Maximum byte length for a display name.
const MAX_DISPLAY_NAME_LEN: usize = 255;

/// A validated, unique-per-account username handle.
///
/// Usernames are single-line, non-empty, bounded, and free of control
/// characters. Uniqueness and case-folding policy are enforced by the
/// repository/service layer, not by this value type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Username(String);

impl Username {
    /// Construct a username, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 128
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("username", &value, MAX_USERNAME_LEN)?;
        Ok(Self(value))
    }

    /// The raw username string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated, human-facing display name.
///
/// Display names allow spaces but not control characters. A blank display name
/// should be represented as `None` at the call site rather than an empty
/// [`DisplayName`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplayName(String);

impl DisplayName {
    /// Construct a display name, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 255
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("display name", &value, MAX_DISPLAY_NAME_LEN)?;
        Ok(Self(value))
    }

    /// The raw display name string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Arbitrary account metadata as a JSON object.
///
/// Mirrors the storage value shape: the top level must be a JSON object so it
/// maps cleanly onto a future `jsonb` column. Size limits are a later policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserMetadata(serde_json::Value);

impl UserMetadata {
    /// Construct metadata, requiring a top-level JSON object.
    ///
    /// # Errors
    /// Returns a validation error if `value` is not a JSON object.
    pub fn new(value: serde_json::Value) -> AppResult<Self> {
        if !value.is_object() {
            return Err(AppError::validation("user metadata must be a JSON object"));
        }
        Ok(Self(value))
    }

    /// Borrow the underlying JSON.
    #[must_use]
    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    /// Consume into the underlying JSON.
    #[must_use]
    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

/// The lifecycle state of an account.
///
/// `Disabled` accounts exist but cannot authenticate (an operator/admin ban).
/// `Tombstoned` accounts are logically deleted and retained only for referential
/// integrity; they can never authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccountState {
    /// Normal, usable account.
    Active,
    /// Temporarily disabled (banned); cannot authenticate until re-enabled.
    Disabled,
    /// Logically deleted; retained for integrity, never authenticatable.
    Tombstoned,
}

impl AccountState {
    /// Whether an account in this state may authenticate.
    #[must_use]
    pub const fn can_authenticate(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Stable lowercase token for logs and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Tombstoned => "tombstoned",
        }
    }
}

/// A Citadel user account.
///
/// Timestamps are explicit [`TimestampMillis`] rather than wall-clock reads so
/// account construction stays deterministic and testable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct User {
    /// Stable account identity (shared with storage ownership).
    pub id: UserId,
    /// Unique handle.
    pub username: Username,
    /// Optional human-facing display name.
    pub display_name: Option<DisplayName>,
    /// Optional account metadata.
    pub metadata: Option<UserMetadata>,
    /// Creation time (Unix millis).
    pub created_at: TimestampMillis,
    /// Last-update time (Unix millis); must be `>= created_at`.
    pub updated_at: TimestampMillis,
    /// Lifecycle state.
    pub state: AccountState,
}

impl User {
    /// Assemble a user, validating timestamp ordering.
    ///
    /// # Errors
    /// Returns a validation error if `updated_at < created_at`.
    pub fn new(
        id: UserId,
        username: Username,
        display_name: Option<DisplayName>,
        metadata: Option<UserMetadata>,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
        state: AccountState,
    ) -> AppResult<Self> {
        if updated_at < created_at {
            return Err(AppError::validation(
                "user updated_at must not precede created_at",
            ));
        }
        Ok(Self {
            id,
            username,
            display_name,
            metadata,
            created_at,
            updated_at,
            state,
        })
    }

    /// Whether the account is in the `Active` state.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == AccountState::Active
    }

    /// Ensure this account may authenticate, or return an auth error.
    ///
    /// # Errors
    /// Returns an [`ErrorCategory::Auth`](crate::error::ErrorCategory::Auth)
    /// error if the account is disabled or tombstoned.
    pub fn ensure_authenticatable(&self) -> AppResult<()> {
        if self.state.can_authenticate() {
            Ok(())
        } else {
            // Sanitized: do not distinguish disabled from tombstoned to callers.
            Err(AppError::auth("account cannot authenticate")
                .with_detail(format!("account state is {}", self.state.as_str())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn uid() -> UserId {
        UserId::new("u-1").expect("valid user id")
    }

    fn name() -> Username {
        Username::new("player-1").expect("valid username")
    }

    #[test]
    fn username_and_display_name_validate() {
        assert!(Username::new("").is_err());
        assert!(Username::new("with\nnewline").is_err());
        assert!(Username::new("player-1").is_ok());
        assert!(DisplayName::new("  ").is_err());
        assert!(DisplayName::new("Cool Player").is_ok());
    }

    #[test]
    fn metadata_requires_object() {
        assert!(UserMetadata::new(json!({"level": 3})).is_ok());
        assert!(UserMetadata::new(json!([1, 2, 3])).is_err());
        assert!(UserMetadata::new(json!("nope")).is_err());
    }

    #[test]
    fn account_state_authentication_and_tokens() {
        assert!(AccountState::Active.can_authenticate());
        assert!(!AccountState::Disabled.can_authenticate());
        assert!(!AccountState::Tombstoned.can_authenticate());
        assert_eq!(AccountState::Active.as_str(), "active");
        assert_eq!(AccountState::Disabled.as_str(), "disabled");
        assert_eq!(AccountState::Tombstoned.as_str(), "tombstoned");
    }

    #[test]
    fn user_rejects_reversed_timestamps() {
        let created = TimestampMillis::from_unix_millis(100);
        let before = TimestampMillis::from_unix_millis(99);
        let err = User::new(
            uid(),
            name(),
            None,
            None,
            created,
            before,
            AccountState::Active,
        )
        .expect_err("updated before created is rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[test]
    fn active_user_can_authenticate_others_cannot() {
        let now = TimestampMillis::from_unix_millis(100);
        let active = User::new(uid(), name(), None, None, now, now, AccountState::Active)
            .expect("valid user");
        assert!(active.is_active());
        assert!(active.ensure_authenticatable().is_ok());

        let disabled = User::new(uid(), name(), None, None, now, now, AccountState::Disabled)
            .expect("valid user");
        let err = disabled
            .ensure_authenticatable()
            .expect_err("disabled user rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Auth);
    }
}
