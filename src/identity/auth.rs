//! Authentication identity domain types.
//!
//! An [`AuthIdentity`] maps an external credential (a device id, custom id, or email)
//! to a Citadel [`UserId`]. This is the contract seam for account creation and
//! login: device and custom auth are the first providers, with email, social,
//! and token-signing providers slotting in behind the same
//! credential-to-account mapping later.
//!
//! Out of scope for this task and intentionally absent: password hashing, email
//! verification, social-provider token exchange, and JWT signing.

use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::storage::UserId;
use crate::time::TimestampMillis;
use crate::validate;

/// Maximum byte length for a device or custom id.
const MAX_AUTH_ID_LEN: usize = 128;
/// Maximum byte length of a normalized email address.
const MAX_EMAIL_LEN: usize = 254;
/// Passwords are intentionally bounded before Argon2 processes them.
const MAX_PASSWORD_LEN: usize = 1_024;

/// The provider that issued a credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuthProvider {
    /// Anonymous device-based auth (Nakama-style device id).
    Device,
    /// Application-supplied custom id auth.
    Custom,
    /// Email/password authentication.
    Email,
}

impl AuthProvider {
    /// Stable lowercase token for logs and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Device => "device",
            Self::Custom => "custom",
            Self::Email => "email",
        }
    }
}

/// A normalized email address used as a player identity.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EmailAddress(String);

impl std::fmt::Debug for EmailAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EmailAddress([redacted])")
    }
}

impl EmailAddress {
    /// Validate and normalize an ASCII email address for identity lookup.
    pub fn new(value: impl AsRef<str>) -> AppResult<Self> {
        let value = value.as_ref().trim().to_ascii_lowercase();
        if value.is_empty()
            || value.len() > MAX_EMAIL_LEN
            || value.chars().any(char::is_control)
            || !value.is_ascii()
        {
            return Err(crate::error::AppError::validation("invalid email address"));
        }
        let Some((local, domain)) = value.split_once('@') else {
            return Err(crate::error::AppError::validation("invalid email address"));
        };
        if local.is_empty()
            || domain.is_empty()
            || domain.starts_with('.')
            || domain.ends_with('.')
            || !domain.contains('.')
            || value.matches('@').count() != 1
        {
            return Err(crate::error::AppError::validation("invalid email address"));
        }
        Ok(Self(value))
    }

    /// The normalized value. Keep it at identity/persistence boundaries only.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A plaintext password accepted only at the authentication boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct Password(String);

impl std::fmt::Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Password([redacted])")
    }
}

impl Password {
    /// Validate a password without normalizing it; every byte is significant.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        if value.len() < 8
            || value.len() > MAX_PASSWORD_LEN
            || value.chars().any(char::is_control)
            || value.trim().is_empty()
        {
            return Err(crate::error::AppError::validation("invalid password"));
        }
        Ok(Self(value))
    }

    /// Expose the plaintext only to the password hashing/verification boundary.
    #[must_use]
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

/// Redacted encoded Argon2id PHC verifier persisted for an email identity.
#[derive(Clone, PartialEq, Eq)]
pub struct PasswordVerifier(String);

impl std::fmt::Debug for PasswordVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordVerifier([redacted])")
    }
}

impl PasswordVerifier {
    /// Construct from an encoded PHC verifier already validated by the hash layer.
    pub fn new(encoded: String) -> AppResult<Self> {
        if encoded.is_empty() || encoded.len() > 1_024 {
            return Err(crate::error::AppError::validation(
                "invalid password verifier",
            ));
        }
        Ok(Self(encoded))
    }

    #[must_use]
    pub(crate) fn encoded(&self) -> &str {
        &self.0
    }
}

/// A validated device identifier used for anonymous device auth.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeviceId(String);

impl DeviceId {
    /// Construct a device id, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 128
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("device id", &value, MAX_AUTH_ID_LEN)?;
        Ok(Self(value))
    }

    /// The raw device id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated application-supplied custom identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CustomId(String);

impl CustomId {
    /// Construct a custom id, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 128
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("custom id", &value, MAX_AUTH_ID_LEN)?;
        Ok(Self(value))
    }

    /// The raw custom id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A concrete credential presented for authentication.
///
/// New providers (email, social, etc.) extend this enum behind the same
/// `credential -> UserId` mapping; the service and repository contracts do not
/// change shape when a provider is added.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuthCredential {
    /// Device-id credential.
    Device(DeviceId),
    /// Custom-id credential.
    Custom(CustomId),
    /// Normalized email identity, whose password verifier is stored separately
    /// on the identity record.
    Email(EmailAddress),
}

impl AuthCredential {
    /// The provider family for this credential.
    #[must_use]
    pub const fn provider(&self) -> AuthProvider {
        match self {
            Self::Device(_) => AuthProvider::Device,
            Self::Custom(_) => AuthProvider::Custom,
            Self::Email(_) => AuthProvider::Email,
        }
    }
}

/// A stored link between an external credential and a Citadel account.
///
/// One credential maps to exactly one [`UserId`]; a single account may have
/// several identities (device + custom, and later social/email). Uniqueness of
/// `credential` is enforced by the repository layer.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthIdentity {
    /// The external credential.
    pub credential: AuthCredential,
    /// The account this credential authenticates.
    pub user_id: UserId,
    /// Link creation time (Unix millis).
    pub created_at: TimestampMillis,
    /// Last-update time (Unix millis); must be `>= created_at`.
    pub updated_at: TimestampMillis,
    password_verifier: Option<PasswordVerifier>,
}

impl std::fmt::Debug for AuthIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthIdentity")
            .field("credential", &self.credential)
            .field("user_id", &self.user_id)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field(
                "password_verifier",
                &self.password_verifier.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

impl AuthIdentity {
    /// Assemble an identity link, validating timestamp ordering.
    ///
    /// # Errors
    /// Returns a validation error if `updated_at < created_at`.
    pub fn new(
        credential: AuthCredential,
        user_id: UserId,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
    ) -> AppResult<Self> {
        if updated_at < created_at {
            return Err(crate::error::AppError::validation(
                "auth identity updated_at must not precede created_at",
            ));
        }
        Ok(Self {
            credential,
            user_id,
            created_at,
            updated_at,
            password_verifier: None,
        })
    }

    /// The provider family for the linked credential.
    #[must_use]
    pub const fn provider(&self) -> AuthProvider {
        self.credential.provider()
    }

    /// Attach the verifier required by an email identity.
    pub fn with_password_verifier(mut self, verifier: PasswordVerifier) -> AppResult<Self> {
        if self.provider() != AuthProvider::Email {
            return Err(crate::error::AppError::validation(
                "password verifier requires an email identity",
            ));
        }
        self.password_verifier = Some(verifier);
        Ok(self)
    }

    /// The stored verifier, available only to the authentication service.
    #[must_use]
    pub(crate) fn password_verifier(&self) -> Option<&PasswordVerifier> {
        self.password_verifier.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uid() -> UserId {
        UserId::new("u-1").expect("valid user id")
    }

    #[test]
    fn device_and_custom_ids_validate() {
        assert!(DeviceId::new("").is_err());
        assert!(DeviceId::new("with\ttab").is_err());
        assert!(DeviceId::new("x".repeat(129)).is_err());
        assert!(DeviceId::new("device-abc").is_ok());
        assert!(CustomId::new("custom-xyz").is_ok());
    }

    #[test]
    fn credential_reports_provider() {
        let device = AuthCredential::Device(DeviceId::new("d-1").expect("device"));
        let custom = AuthCredential::Custom(CustomId::new("c-1").expect("custom"));
        assert_eq!(device.provider(), AuthProvider::Device);
        assert_eq!(custom.provider(), AuthProvider::Custom);
        assert_eq!(AuthProvider::Device.as_str(), "device");
        assert_eq!(AuthProvider::Custom.as_str(), "custom");
        assert_eq!(AuthProvider::Email.as_str(), "email");
    }

    #[test]
    fn email_normalizes_and_password_redacts() {
        let email = EmailAddress::new("  Player@Example.COM ").expect("email");
        assert_eq!(email.as_str(), "player@example.com");
        assert!(EmailAddress::new("bad-address").is_err());
        assert!(Password::new("short").is_err());
        assert!(Password::new("correct horse battery staple").is_ok());
        assert!(
            !format!(
                "{:?}",
                Password::new("correct horse battery staple").expect("password")
            )
            .contains("correct horse")
        );
    }

    #[test]
    fn identity_rejects_reversed_timestamps() {
        let credential = AuthCredential::Device(DeviceId::new("d-1").expect("device"));
        let created = TimestampMillis::from_unix_millis(10);
        let before = TimestampMillis::from_unix_millis(9);
        assert!(AuthIdentity::new(credential.clone(), uid(), created, before).is_err());
        let identity = AuthIdentity::new(credential, uid(), created, created).expect("valid");
        assert_eq!(identity.provider(), AuthProvider::Device);
    }
}
