//! Console Audit Logs section (`GET /console/v1/audit`, ).
//!
//! Reads the console action trail newest-first with optional `actor` (exact),
//! `action` (prefix), and `match_id` (exact) filters, a bounded `limit`, and a
//! keyset `after` cursor. Reading the trail is not itself audited for humans
//! (it would flood the ring), and both roles may read it — the trail is how a
//! viewer verifies what admins did.
//!
//! The page comes from the durable trail on a backend that stores one and from
//! the in-process ring otherwise, and `durable` says which. A ring-sourced page
//! has no cursor: its rows carry no durable key, so it never advertises a
//! `next_after` it could not honour.

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::ids::{SHORT_PREFIX_ID_LEN, valid_id};
use crate::repository::DurableAuditFilter;
use crate::services::{AuditEntry, ConsolePrincipal};

use super::super::error::ApiError;

/// The Audit Logs section route.
pub const AUDIT_PATH: &str = "/console/v1/audit";

/// Prefix every durable trail id carries; also the cursor's shape.
const AUDIT_ID_PREFIX: &str = "au1-";

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
    /// Exact durable match filter. Absent matches every entry, including the
    /// operator actions that belong to no match at all.
    pub match_id: Option<String>,
    /// Keyset cursor: the `next_after` of the previous page.
    pub after: Option<String>,
}

/// The JSON response for [`AUDIT_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct AuditPage {
    /// Matching entries, newest first.
    pub entries: Vec<AuditEntry>,
    /// Entries currently retained: the ring's unfiltered depth, or the matching
    /// row count when the trail is durable.
    pub retained: usize,
    /// The configured retention bound.
    pub capacity: usize,
    /// Cursor for the next page, absent on the last one and on every
    /// ring-sourced page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
    /// Whether this page came from a table rather than the in-process ring. An
    /// operator is never shown a process-local cache as durable history.
    pub durable: bool,
    /// Records the write-behind queues dropped since boot, so a quiet trail is
    /// distinguishable from a lossy one.
    pub dropped_total: u64,
}

/// Clamp a requested page size to `[1, MAX_LIMIT]`, defaulting when absent.
fn effective_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Whether a supplied cursor is a well-formed durable trail id.
fn valid_cursor(value: &str) -> bool {
    valid_id(value, AUDIT_ID_PREFIX, SHORT_PREFIX_ID_LEN)
}

/// `GET /console/v1/audit`: read the trail newest-first with filters.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(query): Query<AuditQuery>,
) -> Result<Json<AuditPage>, ApiError> {
    app.metrics().record_http_request();
    let cursor = query.after.as_deref();
    if cursor.is_some_and(|value| !valid_cursor(value)) {
        return Err(AppError::validation("invalid audit cursor").into());
    }
    let limit = effective_limit(query.limit);
    let filter = DurableAuditFilter {
        actor: query.actor,
        action_prefix: query.action,
        match_id: query.match_id,
        after_audit_id: query.after,
        // Over-fetch one so `next_after` is set from a row that exists rather
        // than guessed from a full page.
        limit: limit.saturating_add(1),
    };
    let mut rows = app.list_audit(&filter).await?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let durable = app.audit_is_durable();
    // A ring row has an empty durable key, so a ring page never advertises a
    // cursor the next request could not resolve.
    let next_after = match rows.last() {
        Some(row) if has_more && !row.audit_id.is_empty() => Some(row.audit_id.clone()),
        _ => None,
    };
    let retained = if durable {
        usize::try_from(app.count_audit(&filter).await?).unwrap_or(usize::MAX)
    } else {
        app.audit_log().len()
    };
    Ok(Json(AuditPage {
        entries: rows
            .into_iter()
            .map(|row| {
                let mut entry = row.entry;
                // The column is the durable truth for a stored row; a
                // ring-sourced row carries the reference on the entry itself.
                entry.match_id = row.match_id.or(entry.match_id);
                entry
            })
            .collect(),
        retained,
        capacity: app.audit_log().capacity(),
        next_after,
        durable,
        dropped_total: app
            .durable_logs()
            .map_or(0, |writer| writer.dropped_total()),
    }))
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

    #[test]
    fn only_a_well_formed_trail_id_is_accepted_as_a_cursor() {
        assert!(valid_cursor(&format!("au1-{:029x}", 1_u64)));
        assert!(!valid_cursor(""), "an empty ring cursor is never honoured");
        assert!(!valid_cursor(&format!("ml1-{:029x}", 1_u64)));
        assert!(!valid_cursor(&format!("au1-{:028x}", 1_u64)));
        assert!(!valid_cursor(&format!("au1-{}", "z".repeat(29))));
    }
}
