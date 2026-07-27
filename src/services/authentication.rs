//! Authentication service contract.
//!
//! [`AuthenticationService`] is the boundary for device and custom auth. It maps
//! a presented credential to a Citadel account, optionally creating the account,
//! and issues a session. Concrete implementations (backed by
//! [`UserRepository`](crate::repository::UserRepository),
//! [`AuthIdentityRepository`](crate::repository::AuthIdentityRepository), and a
//! token issuer) are provided by later tasks; this task fixes the contract shape.
//!
//! The trait is async (via [`async_trait`]) and object-safe, matching the async
//! repository/session contracts it builds on, and depends only on
//! domain types, never on a concrete storage or transport. It does not implement
//! password/email/social auth or token signing.

use async_trait::async_trait;

use crate::error::AppResult;
use crate::identity::{
    AuthIdentity, CustomId, DeviceId, DisplayName, EmailAddress, Password, User, UserMetadata,
    Username,
};
use crate::session::{IssuedSessionTokens, NodeId, Session};
use crate::time::{DurationMillis, TimestampMillis};

use super::ServiceLifecycle;

/// Options shared by every authentication request.
///
/// `now` and `owner_node` are supplied by the caller so account/session creation
/// stays deterministic and every issued session has an explicit owning node.
///
/// Not `Eq`: `metadata` wraps `serde_json::Value`, which is only `PartialEq`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthenticationOptions {
    /// Whether to create a new account when the credential is unknown.
    pub create_account: bool,
    /// Username to assign when creating an account (required by the
    /// implementation only on the create path).
    pub username: Option<Username>,
    /// Optional display name for a newly created account.
    pub display_name: Option<DisplayName>,
    /// Optional metadata for a newly created account.
    pub metadata: Option<UserMetadata>,
    /// The current time (Unix millis).
    pub now: TimestampMillis,
    /// The node that will own the issued session.
    pub owner_node: NodeId,
    /// Time-to-live for the issued access token.
    pub session_ttl: DurationMillis,
    /// Optional time-to-live for the issued refresh token.
    pub refresh_ttl: Option<DurationMillis>,
}

/// A device-auth request.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceAuthenticationRequest {
    /// The device credential presented.
    pub device_id: DeviceId,
    /// Shared authentication options.
    pub options: AuthenticationOptions,
}

/// A custom-auth request.
#[derive(Debug, Clone, PartialEq)]
pub struct CustomAuthenticationRequest {
    /// The custom credential presented.
    pub custom_id: CustomId,
    /// Shared authentication options.
    pub options: AuthenticationOptions,
}

/// An email/password authentication request.
#[derive(Debug, Clone, PartialEq)]
pub struct EmailAuthenticationRequest {
    /// Normalized email identity.
    pub email: EmailAddress,
    /// Plaintext password; its `Debug` implementation is redacted.
    pub password: Password,
    /// Shared authentication options.
    pub options: AuthenticationOptions,
}

/// The successful result of authentication.
///
/// Not `Eq`: `user` may carry `serde_json::Value` metadata, which is only
/// `PartialEq`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthenticationOutcome {
    /// The authenticated account.
    pub user: User,
    /// The credential-to-account link that was matched or created.
    pub identity: AuthIdentity,
    /// The issued session.
    pub session: Session,
    /// The freshly minted tokens (secret; do not log).
    pub tokens: IssuedSessionTokens,
    /// Whether a new account was created by this request.
    pub account_created: bool,
    /// Whether a new identity link was created by this request.
    pub identity_created: bool,
}

/// The authentication boundary for device and custom credentials.
///
/// Implementations must reject disabled/tombstoned accounts with an
/// [`ErrorCategory::Auth`](crate::error::ErrorCategory::Auth) error and must not
/// leak whether a credential exists when `create_account` is `false`.
#[async_trait]
pub trait AuthenticationService: ServiceLifecycle + Send + Sync {
    /// Authenticate (and optionally register) via a device id.
    ///
    /// # Errors
    /// Returns an auth error for a disabled account or an unknown credential when
    /// `create_account` is `false`; a validation error for a create request
    /// missing required fields; and repository errors otherwise.
    async fn authenticate_device(
        &self,
        request: DeviceAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome>;

    /// Authenticate (and optionally register) via a custom id.
    ///
    /// # Errors
    /// See [`AuthenticationService::authenticate_device`].
    async fn authenticate_custom(
        &self,
        request: CustomAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome>;

    /// Authenticate (and optionally register) via email and password.
    async fn authenticate_email(
        &self,
        request: EmailAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome>;
}
