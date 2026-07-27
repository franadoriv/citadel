//! Small, crate-internal validation helpers shared by domain newtypes.
//!
//! The identity and session contracts introduce several validated
//! string newtypes (usernames, device/custom ids, session/node ids, token
//! references). They all share the same baseline shape rules as the storage
//! labels in [`crate::storage`], so the rule lives here once rather than being
//! copied into every constructor.

use crate::error::{AppError, AppResult};

/// Validate a bounded, single-line label: non-empty (ignoring surrounding
/// whitespace), at most `max` bytes, and free of control characters.
///
/// Spaces are allowed so human-facing values such as display names pass, but
/// control characters (including newlines and tabs) are rejected to keep values
/// safe for logs, headers, and single-line rendering.
///
/// # Errors
/// Returns an [`ErrorCategory::Validation`](crate::error::ErrorCategory::Validation)
/// error naming `kind` when the value is empty/whitespace-only, exceeds `max`
/// bytes, or contains control characters.
pub(crate) fn label(kind: &str, value: &str, max: usize) -> AppResult<()> {
    if value.trim().is_empty() {
        return Err(AppError::validation(format!("{kind} must not be empty")));
    }
    if value.len() > max {
        return Err(AppError::validation(format!(
            "{kind} must not exceed {max} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(AppError::validation(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_whitespace() {
        assert!(label("field", "", 32).is_err());
        assert!(label("field", "   ", 32).is_err());
    }

    #[test]
    fn rejects_overlong_and_control() {
        assert!(label("field", &"a".repeat(33), 32).is_err());
        assert!(label("field", "with\nnewline", 32).is_err());
        assert!(label("field", "with\ttab", 32).is_err());
    }

    #[test]
    fn accepts_spaces_and_normal_values() {
        assert!(label("field", "value", 32).is_ok());
        assert!(label("field", "Display Name", 32).is_ok());
    }
}
