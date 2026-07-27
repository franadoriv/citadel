//! Console Audit Logs section (`GET /console/v1/audit`, ).
//!
//! Reads the in-process [`AuditLog`](crate::services::AuditLog) newest-first
//! with optional `actor` (exact) and `action` (prefix) filters and a bounded
//! `limit`. Reading the trail is not itself audited (it would flood the ring),
//! and both roles may read it — the trail is how a viewer verifies what admins
//! did.

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::services::{AuditEntry, AuditFilter, ConsoleIdentity};

/// The Audit Logs section route.
pub const AUDIT_PATH: &str = "/console/v1/audit";

/// Default page size when `limit` is absent.
const DEFAULT_LIMIT: usize = 100;
/// Hard ceiling on one page, independent of the requested `limit`.
const MAX_LIMIT: usize = 500;

/// Accepted query parameters for [`AUDIT_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditQuery {
    /// Maximum entries to return (default 100, capped at 500).
    pub limit: Option<usize>,
    /// Exact actor filter.
    pub actor: Option<String>,
    /// Action prefix filter (`storage` matches `storage.write`).
    pub action: Option<String>,
}

/// The JSON response for [`AUDIT_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct AuditPage {
    /// Matching entries, newest first.
    pub entries: Vec<AuditEntry>,
    /// Total entries currently retained in the ring (unfiltered).
    pub retained: usize,
    /// The ring's retention bound.
    pub capacity: usize,
}

/// Clamp a requested page size to `[1, MAX_LIMIT]`, defaulting when absent.
fn effective_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// `GET /console/v1/audit`: read the trail newest-first with filters.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Query(query): Query<AuditQuery>,
) -> Json<AuditPage> {
    app.metrics().record_http_request();
    let filter = AuditFilter {
        actor: query.actor,
        action: query.action,
        limit: effective_limit(query.limit),
    };
    let log = app.audit_log();
    Json(AuditPage {
        entries: log.list(&filter),
        retained: log.len(),
        capacity: log.capacity(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_defaulted_clamped_and_capped() {
        assert_eq!(effective_limit(None), DEFAULT_LIMIT);
        assert_eq!(effective_limit(Some(0)), 1, "zero never means unbounded");
        assert_eq!(effective_limit(Some(50)), 50);
        assert_eq!(effective_limit(Some(9_999)), MAX_LIMIT);
    }

    #[test]
    fn audit_path_is_a_registered_console_section() {
        assert!(super::super::SECTION_PATHS.contains(&AUDIT_PATH));
    }
}
