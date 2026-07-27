//! Session token types.
//!
//! Two distinct concerns are modeled separately:
//!
//! - [`SessionTokenSecret`] is a bearer secret (an access or refresh token). It
//!   must never be logged or serialized: its [`std::fmt::Debug`] is redacted, it
//!   has no [`std::fmt::Display`], and it intentionally does not implement
//!   `Serialize`/`Deserialize`. The raw value is reachable only through the
//!   explicit [`SessionTokenSecret::expose_secret`] method.
//! - [`SessionTokenRef`] is a non-secret handle (for example a token id or hash)
//!   that may be stored on a [`Session`](crate::session::Session) and logged, so
//!   a session can be located without persisting the secret itself.
//!
//! Token signing, hashing, and verification are out of scope for this contract
//! task; these types define the seam those implementations will fill.

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::validate;

/// Maximum byte length for a token secret (generous enough for signed tokens).
const MAX_TOKEN_SECRET_LEN: usize = 4096;
/// Maximum byte length for a token reference handle.
const MAX_TOKEN_REF_LEN: usize = 256;

/// An opaque bearer token secret.
///
/// Never appears in logs: [`std::fmt::Debug`] prints `SessionTokenSecret([redacted])`,
/// there is no `Display`, and the type is intentionally not serializable. Read
/// the value only at the boundary that must transmit it, via
/// [`SessionTokenSecret::expose_secret`].
///
/// Deliberately does not derive `Hash`: hashing routes the raw bytes through a
/// caller-supplied `Hasher`, which would be an unintended second way to observe
/// the secret. `expose_secret` is the only sanctioned reveal.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionTokenSecret(String);

impl SessionTokenSecret {
    /// Construct a token secret, validating length.
    ///
    /// The secret must be non-empty and at most 4096 bytes. Unlike other
    /// newtypes it is not shape-checked for control characters, since token
    /// encodings vary and the value is never rendered.
    ///
    /// # Errors
    /// Returns a validation error if empty or longer than 4096 bytes.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(AppError::validation("session token must not be empty"));
        }
        if value.len() > MAX_TOKEN_SECRET_LEN {
            return Err(AppError::validation(format!(
                "session token must not exceed {MAX_TOKEN_SECRET_LEN} bytes"
            )));
        }
        Ok(Self(value))
    }

    /// Reveal the raw secret. Call only where the value must be transmitted; the
    /// result must never be logged.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SessionTokenSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never leak the secret through Debug (used by tracing, panics, etc.).
        write!(f, "SessionTokenSecret([redacted])")
    }
}

/// A non-secret handle for locating a session (for example a token id or hash).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionTokenRef(String);

impl SessionTokenRef {
    /// Construct a token reference, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 256
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("session token reference", &value, MAX_TOKEN_REF_LEN)?;
        Ok(Self(value))
    }

    /// The raw reference string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The token pair minted when a session is created or refreshed.
///
/// `refresh` is optional: short-lived or non-refreshable sessions omit it. Both
/// fields are redacted secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedSessionTokens {
    /// The access token used to authenticate requests.
    pub access: SessionTokenSecret,
    /// The optional refresh token used to mint a new access token.
    pub refresh: Option<SessionTokenSecret>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_validates_length() {
        assert!(SessionTokenSecret::new("").is_err());
        assert!(SessionTokenSecret::new("a".repeat(MAX_TOKEN_SECRET_LEN + 1)).is_err());
        let secret = SessionTokenSecret::new("s3cr3t").expect("valid");
        assert_eq!(secret.expose_secret(), "s3cr3t");
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = SessionTokenSecret::new("super-secret-token").expect("valid");
        let rendered = format!("{secret:?}");
        assert_eq!(rendered, "SessionTokenSecret([redacted])");
        assert!(!rendered.contains("super-secret-token"));

        // Also redacted when nested inside another Debug-printed structure.
        let tokens = IssuedSessionTokens {
            access: secret,
            refresh: Some(SessionTokenSecret::new("refresh-secret").expect("valid")),
        };
        let nested = format!("{tokens:?}");
        assert!(!nested.contains("super-secret-token"));
        assert!(!nested.contains("refresh-secret"));
    }

    #[test]
    fn token_ref_validates_shape() {
        assert!(SessionTokenRef::new("").is_err());
        assert!(SessionTokenRef::new("with\nnewline").is_err());
        let token_ref = SessionTokenRef::new("tok-123").expect("valid");
        assert_eq!(token_ref.as_str(), "tok-123");
    }
}
