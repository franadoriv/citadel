//! HTTP error mapping for JSON API routes.
//!
//! [`ApiError`] wraps an [`AppError`] and renders it as a sanitized JSON body
//! with a sensible HTTP status. The mapping is deliberately conservative for a
//! security boundary:
//!
//! - Every [`ErrorCategory::Auth`] failure collapses to a single `401` with the
//!   generic `authentication failed` message, so a caller cannot tell an unknown
//!   credential from a disabled account (no existence oracle). The auth service
//!   already returns one uniform error for those cases; this mapping guarantees
//!   the boundary never re-widens it.
//! - Internal-class errors (`Config`/`Database`/`Runtime`/`Transport`/
//!   `Internal`) collapse to a generic `500` and never expose the operator
//!   message, detail, or a stack trace. The operator-facing line is logged
//!   server-side instead.
//! - Only request-shaped categories (`Validation`, `Conflict`) forward their
//!   already-sanitized message, since those describe the request, not whether an
//!   account exists.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::error::{AppError, ErrorCategory};
use crate::error_reporting;

/// A JSON error body: a stable machine-readable `code` and a human-readable,
/// sanitized `message`. Never carries internal detail or secrets.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ErrorBody {
    /// Stable, lowercase error code for programmatic handling.
    pub code: &'static str,
    /// Sanitized, client-safe message.
    pub message: String,
}

/// An [`AppError`] rendered at the HTTP boundary.
#[derive(Debug)]
pub struct ApiError {
    error: AppError,
    retry_after_seconds: Option<u64>,
}

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        Self {
            error,
            retry_after_seconds: None,
        }
    }
}

impl ApiError {
    /// Map the wrapped error to an HTTP status, a stable code, and a sanitized
    /// client-facing message.
    fn parts(&self) -> (StatusCode, &'static str, String) {
        if self.retry_after_seconds.is_some() {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many authentication attempts".to_string(),
            );
        }
        match self.error.category() {
            // Uniform auth failure: one status, one code, one generic message,
            // regardless of the underlying cause. No existence oracle.
            ErrorCategory::Auth => (
                StatusCode::UNAUTHORIZED,
                "authentication_failed",
                "authentication failed".to_string(),
            ),
            // Request-shaped errors forward their sanitized message.
            ErrorCategory::Validation => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                self.error.message().to_string(),
            ),
            ErrorCategory::Conflict => (
                StatusCode::CONFLICT,
                "conflict",
                self.error.message().to_string(),
            ),
            ErrorCategory::Permission => {
                (StatusCode::FORBIDDEN, "forbidden", "forbidden".to_string())
            }
            ErrorCategory::NotFound => {
                (StatusCode::NOT_FOUND, "not_found", "not found".to_string())
            }
            ErrorCategory::Deadline => (
                StatusCode::GATEWAY_TIMEOUT,
                "deadline_exceeded",
                "deadline exceeded".to_string(),
            ),
            ErrorCategory::Cancelled => (
                StatusCode::REQUEST_TIMEOUT,
                "request_cancelled",
                "request cancelled".to_string(),
            ),
            // Everything else is an internal failure: never leak the message or
            // detail to the client.
            ErrorCategory::Config
            | ErrorCategory::Database
            | ErrorCategory::Runtime
            | ErrorCategory::Transport
            | ErrorCategory::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal server error".to_string(),
            ),
        }
    }

    /// Build the uniform response for a public admission-control rejection.
    /// The supplied duration is bounded by configured policy and emitted as an
    /// integer `Retry-After` header, never as a key or identity diagnostic.
    #[must_use]
    pub fn rate_limited(retry_after_seconds: u64) -> Self {
        Self {
            error: AppError::permission("authentication rate limited"),
            retry_after_seconds: Some(retry_after_seconds.max(1)),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.parts();
        // Log server-side. 5xx is an operator-visible failure; log the full
        // operator line (which may carry internal detail) at error level. Client
        // (4xx) failures are logged at debug and never include the raw request.
        if status.is_server_error() {
            tracing::error!(error = %self.error.operator_log(), "auth request failed");
            error_reporting::report_app_error("http.api", &self.error);
        } else {
            tracing::debug!(
                category = %self.error.category().code(),
                rate_limited = self.retry_after_seconds.is_some(),
                "auth request rejected"
            );
        }
        let mut response = (
            status,
            Json(ErrorBody {
                code,
                message: message.clone(),
            }),
        )
            .into_response();
        if let Some(retry_after) = self.retry_after_seconds
            && let Ok(value) = HeaderValue::from_str(&retry_after.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(err: AppError) -> (StatusCode, &'static str, String) {
        ApiError::from(err).parts()
    }

    #[test]
    fn auth_errors_collapse_to_uniform_401() {
        // Two different underlying auth messages must render identically so the
        // boundary is not an existence oracle.
        let (s1, c1, m1) = body(AppError::auth("authentication failed"));
        let (s2, c2, m2) = body(AppError::auth("account cannot authenticate"));
        assert_eq!(s1, StatusCode::UNAUTHORIZED);
        assert_eq!((s1, c1, &m1), (s2, c2, &m2));
        assert_eq!(c1, "authentication_failed");
        assert_eq!(m1, "authentication failed");
    }

    #[test]
    fn validation_forwards_sanitized_message_as_400() {
        let (status, code, message) = body(AppError::validation("username is required"));
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(code, "invalid_request");
        assert_eq!(message, "username is required");
    }

    #[test]
    fn internal_classes_collapse_to_generic_500() {
        for err in [
            AppError::internal("invariant X").with_detail("secret trace"),
            AppError::database("connection refused to postgres://u:p@h/db"),
            AppError::config("bad thing"),
        ] {
            let (status, code, message) = body(err);
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(code, "internal_error");
            assert_eq!(message, "internal server error");
            // The generic message never carries the operator detail.
            assert!(!message.contains("secret"));
            assert!(!message.contains("postgres"));
        }
    }

    #[test]
    fn conflict_and_notfound_map_expected_statuses() {
        assert_eq!(
            body(AppError::conflict("username taken")).0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            body(AppError::not_found("missing")).0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(body(AppError::permission("no")).0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn rate_limit_is_a_uniform_429() {
        let (status, code, message) = ApiError::rate_limited(1).parts();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(code, "rate_limited");
        assert_eq!(message, "too many authentication attempts");
    }
}
