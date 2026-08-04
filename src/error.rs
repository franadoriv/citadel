//! Typed application error model for Citadel.
//!
//! This module defines the stable error categories from
//! `website/src/content/docs/reference/operations/telemetry.md` and a minimal [`AppError`]
//! type that library code returns. The categories are the contract surface;
//! later tasks (observability, HTTP mapping, repositories, runtime) attach
//! richer context and source errors without changing the category set.
//!
//!  extends this with an operator-facing log line, optional
//! internal-only detail kept out of client-facing `Display`, and a [`redact`]
//! helper for sensitive values. HTTP status mapping and database/runtime source
//! conversions still belong to their owning tasks.

use std::fmt;

/// Stable, operator-facing error categories.
///
/// Categories are intentionally coarse and stable so client-facing responses,
/// metrics labels, and logs can rely on them across releases. The exhaustive
/// set mirrors `website/src/content/docs/reference/operations/telemetry.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Invalid CLI/config, missing files, invalid runtime manifest.
    Config,
    /// Invalid credentials, expired tokens, disabled accounts.
    Auth,
    /// Authenticated caller is not allowed to perform an operation.
    Permission,
    /// Malformed input, invalid payload, unsupported enum/value.
    Validation,
    /// Missing user, object, match, stream, handler, or node.
    NotFound,
    /// Version mismatch, duplicate unique value, state transition race.
    Conflict,
    /// Operation exceeded a caller or server deadline.
    Deadline,
    /// Operation cancelled by caller, shutdown, disconnect, or timeout.
    Cancelled,
    /// Repository or migration failure after typed mapping.
    Database,
    /// Gamecode failure, missing handler, worker unavailable.
    Runtime,
    /// HTTP/WebSocket/RPC framing, serialization, or network errors.
    Transport,
    /// Invariant violation or unexpected server failure.
    Internal,
}

impl ErrorCategory {
    /// Stable lowercase code suitable for metrics labels and structured logs.
    ///
    /// Values are part of the observability contract and must remain stable.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Auth => "auth",
            Self::Permission => "permission",
            Self::Validation => "validation",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Deadline => "deadline",
            Self::Cancelled => "cancelled",
            Self::Database => "database",
            Self::Runtime => "runtime",
            Self::Transport => "transport",
            Self::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// The library-level error type returned by Citadel services.
///
/// An `AppError` carries a stable [`ErrorCategory`], a sanitized
/// operator-facing message, and optional internal-only detail used for logs.
/// The public [`Display`](fmt::Display) output (which may reach clients) never
/// includes the internal detail; only category and the sanitized message are
/// exposed there.
#[derive(Debug, thiserror::Error)]
#[error("{category}: {message}")]
pub struct AppError {
    category: ErrorCategory,
    message: String,
    /// Internal-only context for operator logs. Never shown in `Display`.
    detail: Option<String>,
}

impl AppError {
    /// Construct an error with an explicit category and sanitized message.
    #[must_use]
    pub fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            detail: None,
        }
    }

    /// Attach internal-only detail for operator logs.
    ///
    /// Surfaced by [`AppError::log_detail`] and [`AppError::operator_log`] but
    /// never by [`Display`](fmt::Display), so it does not leak to client-facing
    /// surfaces. Use [`redact`] for any value that might be sensitive.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Construct a [`ErrorCategory::Config`] error.
    #[must_use]
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Config, message)
    }

    /// Construct a [`ErrorCategory::Validation`] error.
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Validation, message)
    }

    /// Construct a [`ErrorCategory::Auth`] error.
    ///
    /// Used by identity/session workflows for invalid credentials, unknown or
    /// expired sessions, and disabled accounts. Callers should keep the message
    /// sanitized (for example a single `"invalid session"`) so attacker-visible
    /// responses do not distinguish unknown from expired or revoked sessions.
    #[must_use]
    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Auth, message)
    }

    /// Construct a [`ErrorCategory::Internal`] error.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Internal, message)
    }

    /// Construct a [`ErrorCategory::NotFound`] error.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::NotFound, message)
    }

    /// Construct a [`ErrorCategory::Permission`] error.
    #[must_use]
    pub fn permission(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permission, message)
    }

    /// Construct a [`ErrorCategory::Conflict`] error.
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Conflict, message)
    }

    /// Construct a [`ErrorCategory::Database`] error.
    #[must_use]
    pub fn database(message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Database, message)
    }

    /// The stable category for this error.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    /// The sanitized, operator-facing message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Internal-only detail, if any. Never shown to clients.
    #[must_use]
    pub fn log_detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// A single sanitized operator log line: `code message` or
    /// `code message (detail)`.
    ///
    /// Safe for operator logs. It does not include secrets unless a caller
    /// placed them in `message`/`detail`, which the conventions forbid.
    #[must_use]
    pub fn operator_log(&self) -> String {
        match &self.detail {
            Some(detail) => format!("{} {} ({detail})", self.category.code(), self.message),
            None => format!("{} {}", self.category.code(), self.message),
        }
    }
}

/// Convenient result alias for fallible Citadel library paths.
pub type AppResult<T> = Result<T, AppError>;

/// Replace a potentially sensitive value with a redaction marker for logs.
///
/// Secrets such as tokens, passwords, connection strings, and provider
/// payloads must never be logged in cleartext. This helper returns a stable
/// `"[redacted]"` marker so log call sites can keep field structure without
/// exposing the value.
#[must_use]
pub fn redact(_value: &str) -> &'static str {
    "[redacted]"
}

/// Redact the userinfo (credentials) portion of a connection URL for logs.
///
/// Given e.g. `postgres://user:pass@host/db`, returns
/// `postgres://[redacted]@host/db`. If no userinfo is present, the input is
/// returned unchanged. This is best-effort sanitization for operator logs, not
/// a security boundary.
#[must_use]
pub fn redact_url_credentials(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = scheme_end + 3;
    let rest = &url[after_scheme..];
    match rest.find('@') {
        Some(at) => {
            let host_part = &rest[at + 1..];
            format!("{}[redacted]@{host_part}", &url[..after_scheme])
        }
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_codes_are_stable_and_lowercase() {
        let all = [
            ErrorCategory::Config,
            ErrorCategory::Auth,
            ErrorCategory::Permission,
            ErrorCategory::Validation,
            ErrorCategory::NotFound,
            ErrorCategory::Conflict,
            ErrorCategory::Deadline,
            ErrorCategory::Cancelled,
            ErrorCategory::Database,
            ErrorCategory::Runtime,
            ErrorCategory::Transport,
            ErrorCategory::Internal,
        ];

        for category in all {
            let code = category.code();
            assert!(!code.is_empty(), "category code must not be empty");
            assert_eq!(
                code,
                code.to_ascii_lowercase(),
                "category code must be lowercase"
            );
        }
    }

    #[test]
    fn category_codes_are_unique() {
        let all = [
            ErrorCategory::Config,
            ErrorCategory::Auth,
            ErrorCategory::Permission,
            ErrorCategory::Validation,
            ErrorCategory::NotFound,
            ErrorCategory::Conflict,
            ErrorCategory::Deadline,
            ErrorCategory::Cancelled,
            ErrorCategory::Database,
            ErrorCategory::Runtime,
            ErrorCategory::Transport,
            ErrorCategory::Internal,
        ];

        let mut codes: Vec<&str> = all.iter().map(|c| c.code()).collect();
        codes.sort_unstable();
        let unique = {
            let mut deduped = codes.clone();
            deduped.dedup();
            deduped.len()
        };
        assert_eq!(codes.len(), unique, "category codes must be unique");
    }

    #[test]
    fn constructors_set_expected_category() {
        assert_eq!(AppError::config("bad").category(), ErrorCategory::Config);
        assert_eq!(
            AppError::validation("bad").category(),
            ErrorCategory::Validation
        );
        assert_eq!(AppError::auth("bad").category(), ErrorCategory::Auth);
        assert_eq!(
            AppError::internal("bad").category(),
            ErrorCategory::Internal
        );
        assert_eq!(
            AppError::not_found("bad").category(),
            ErrorCategory::NotFound
        );
        assert_eq!(
            AppError::permission("bad").category(),
            ErrorCategory::Permission
        );
        assert_eq!(
            AppError::conflict("bad").category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            AppError::database("bad").category(),
            ErrorCategory::Database
        );
    }

    #[test]
    fn display_includes_category_and_message() {
        let err = AppError::config("missing bind address");
        assert_eq!(err.to_string(), "config: missing bind address");
        assert_eq!(err.message(), "missing bind address");
    }

    #[test]
    fn detail_is_excluded_from_display() {
        let err = AppError::config("invalid database section")
            .with_detail("url parse failed at position 12");
        // Display (client-facing) must not leak the internal detail.
        assert_eq!(err.to_string(), "config: invalid database section");
        assert_eq!(err.log_detail(), Some("url parse failed at position 12"));
    }

    #[test]
    fn operator_log_includes_detail_when_present() {
        let with = AppError::internal("boom").with_detail("invariant X violated");
        assert_eq!(with.operator_log(), "internal boom (invariant X violated)");

        let without = AppError::internal("boom");
        assert_eq!(without.operator_log(), "internal boom");
    }

    #[test]
    fn redact_masks_arbitrary_values() {
        assert_eq!(redact("super-secret-token"), "[redacted]");
        assert_eq!(redact(""), "[redacted]");
    }

    #[test]
    fn redact_url_credentials_masks_userinfo() {
        assert_eq!(
            redact_url_credentials("postgres://user:pass@localhost/citadel"),
            "postgres://[redacted]@localhost/citadel"
        );
    }

    #[test]
    fn redact_url_credentials_passes_through_without_userinfo() {
        assert_eq!(
            redact_url_credentials("postgres://localhost/citadel"),
            "postgres://localhost/citadel"
        );
        assert_eq!(redact_url_credentials("not-a-url"), "not-a-url");
    }
}
