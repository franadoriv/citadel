//! Durable game-script log stream (`/console/v1/logs`).
//!
//! Every row on this surface was written by the operator's own game script
//! through `citadel.log.write`. `message` and `payload_json` are author-supplied
//! and are returned verbatim: the server adds no credential, bearer token,
//! session id, participant id, or transport identifier to any column here, and
//! it redacts nothing the author chose to write. A script that writes a secret
//! into a payload has published it to every console operator.
//!
//! There is no in-process ring behind this surface — script logs are either
//! persisted or discarded. When the selected backend has no durable log table
//! the endpoints answer `200` with an empty page and `durable: false` rather
//! than an error: an operator is never shown a process-local cache as durable
//! history, and never a `503` at the exact moment history is what they need.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::ids;
use crate::repository::{LogLevel, MatchLogEntry, MatchLogFilter};
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

use super::super::error::ApiError;

/// Script log listing route (readable by `viewer`).
pub const LOGS_PATH: &str = "/console/v1/logs";
/// One stored log line, payload included (readable by `viewer`).
pub const LOG_DETAIL_PATH: &str = "/console/v1/logs/:log_id";

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
/// Widest accepted `tag` prefix, matching the column's own `CHECK`.
const MAX_TAG_PREFIX_BYTES: usize = 64;

/// Accepted query parameters for [`LOGS_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogsQuery {
    /// Restrict to one durable match (`mt1-…`). Absent means every line,
    /// including the ones written outside any match.
    pub match_id: Option<String>,
    /// Exact severity: `trace`, `debug`, `info`, `warn`, or `error`.
    pub level: Option<String>,
    /// Tag **prefix** match — `combat` matches `combat.round`.
    pub tag: Option<String>,
    /// Opaque keyset cursor: the previous page's `next_after`.
    pub after: Option<String>,
    /// Page size, newest-first. Default 50, capped at 200.
    pub limit: Option<usize>,
}

/// Accepted query parameters for a child keyset page.
///
/// Shared with the match drill-down, which keysets the same `log_id` column.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogPageQuery {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

/// One stored log line as the console sees it.
///
/// The listing elides `payload_json` and reports only whether one exists: a
/// page of 200 author-supplied payloads is a large response for a row the
/// operator has not chosen to open yet. The detail route returns it in full.
#[derive(Debug, Clone, Serialize)]
pub struct MatchLogEntryView {
    pub log_id: String,
    /// Absent for a line written outside any match — the common case for a
    /// game with no match concept at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_id: Option<String>,
    pub node_id: String,
    pub created_at_unix_ms: u64,
    pub level: LogLevel,
    pub tag: String,
    pub message: String,
    /// Whether the author attached a payload. Always present, so a console row
    /// can offer the detail link without fetching the payload first.
    pub has_payload: bool,
    /// Author-supplied JSON, stored and returned verbatim. Present only on the
    /// detail projection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_json: Option<String>,
}

impl MatchLogEntryView {
    pub(super) fn from_entry(entry: MatchLogEntry, include_payload: bool) -> Self {
        let has_payload = entry.payload_json.is_some();
        Self {
            log_id: entry.log_id,
            match_id: entry.match_id,
            node_id: entry.node_id,
            created_at_unix_ms: entry.created_at_ms,
            level: entry.level,
            tag: entry.tag,
            message: entry.message,
            has_payload,
            payload_json: if include_payload {
                entry.payload_json
            } else {
                None
            },
        }
    }
}

/// Cursor page of stored log lines.
#[derive(Debug, Clone, Serialize)]
pub struct LogsPage {
    pub items: Vec<MatchLogEntryView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
    /// Whether these rows came from a table. `false` means this backend keeps
    /// no script-log history at all, and the empty page is the whole truth.
    pub durable: bool,
    /// Lines the bounded write-behind queue dropped since boot. A non-zero
    /// value means the node produced logs faster than it could flush them.
    pub dropped_total: u64,
}

/// `GET /console/v1/logs`: keyset page of the durable script log stream.
pub(super) async fn list_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<LogsQuery>,
) -> Result<Json<LogsPage>, ApiError> {
    app.metrics().record_http_request();
    let filter = log_filter(&query)?;
    let limit = filter.limit.saturating_sub(1);
    let mut entries = app.list_match_logs(&filter).await?;
    let has_more = entries.len() > limit;
    entries.truncate(limit);
    let next_after = if has_more {
        entries.last().map(|entry| entry.log_id.clone())
    } else {
        None
    };
    let returned = entries.len();
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "logs.list",
        query.match_id.as_deref().unwrap_or("logs"),
        format!("returned={returned}"),
    ));
    Ok(Json(LogsPage {
        items: entries
            .into_iter()
            .map(|entry| MatchLogEntryView::from_entry(entry, false))
            .collect(),
        next_after,
        durable: app.match_logs_are_durable(),
        dropped_total: dropped_total(&app),
    }))
}

/// `GET /console/v1/logs/{log_id}`: one stored line with its full payload.
pub(super) async fn detail_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(log_id): Path<String>,
) -> Result<Json<MatchLogEntryView>, ApiError> {
    app.metrics().record_http_request();
    if !valid_log_id(&log_id) {
        return Err(AppError::not_found("log entry not found").into());
    }
    let entry = app
        .match_log_by_id(&log_id)
        .await?
        .ok_or_else(|| AppError::not_found("log entry not found"))?;
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "logs.detail",
        log_id,
        "returned=1",
    ));
    Ok(Json(MatchLogEntryView::from_entry(entry, true)))
}

/// Build the repository filter, rejecting every malformed parameter before it
/// reaches a query. `limit` comes back as the over-fetch width (`limit + 1`),
/// which is how the caller learns whether a next cursor exists.
fn log_filter(query: &LogsQuery) -> Result<MatchLogFilter, ApiError> {
    if query
        .match_id
        .as_deref()
        .is_some_and(|match_id| !valid_match_id(match_id))
    {
        return Err(AppError::validation("invalid match id").into());
    }
    if query
        .after
        .as_deref()
        .is_some_and(|after| !valid_log_id(after))
    {
        return Err(AppError::validation("invalid log cursor").into());
    }
    if query
        .tag
        .as_deref()
        .is_some_and(|tag| tag.is_empty() || tag.len() > MAX_TAG_PREFIX_BYTES)
    {
        return Err(AppError::validation("invalid tag filter").into());
    }
    // Strict, like the write path: an unrecognized level is a bad request, not
    // a silent widening to "everything".
    let level = query.level.as_deref().map(LogLevel::parse).transpose()?;
    Ok(MatchLogFilter {
        match_id: query.match_id.clone(),
        level,
        tag_prefix: query.tag.clone(),
        after_log_id: query.after.clone(),
        limit: effective_limit(query.limit).saturating_add(1),
    })
}

pub(super) fn effective_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

pub(super) fn valid_log_id(value: &str) -> bool {
    ids::valid_id(value, "ml1-", ids::SHORT_PREFIX_ID_LEN)
}

pub(super) fn valid_match_id(value: &str) -> bool {
    ids::valid_id(value, "mt1-", ids::SHORT_PREFIX_ID_LEN)
}

/// Records the write-behind queue dropped since boot, or `0` when durable
/// logging is off — a node that never queued anything never dropped anything.
pub(super) fn dropped_total(app: &App) -> u64 {
    app.durable_logs()
        .map_or(0, |writer| writer.dropped_total())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(payload: Option<&str>) -> MatchLogEntry {
        MatchLogEntry {
            log_id: format!("ml1-{}", "a".repeat(29)),
            match_id: Some(format!("mt1-{}", "b".repeat(29))),
            node_id: "node-a".to_string(),
            created_at_ms: 1_751_791_000_000,
            level: LogLevel::Warn,
            tag: "combat.round".to_string(),
            message: "round ended".to_string(),
            payload_json: payload.map(str::to_string),
        }
    }

    #[test]
    fn limits_are_clamped_to_one_page() {
        assert_eq!(effective_limit(None), DEFAULT_LIMIT);
        assert_eq!(effective_limit(Some(0)), 1);
        assert_eq!(effective_limit(Some(10_000)), MAX_LIMIT);
    }

    #[test]
    fn ids_and_cursors_are_validated_before_any_query() {
        assert!(valid_log_id(&format!("ml1-{}", "a".repeat(29))));
        assert!(!valid_log_id("../log"));
        assert!(!valid_log_id(&format!("mt1-{}", "a".repeat(29))));
        assert!(valid_match_id(&format!("mt1-{}", "0".repeat(29))));
        assert!(!valid_match_id("mt1-short"));
    }

    #[test]
    fn the_listing_elides_the_payload_and_the_detail_returns_it() {
        let listed = MatchLogEntryView::from_entry(entry(Some(r#"{"kills":3}"#)), false);
        assert!(listed.has_payload);
        assert!(listed.payload_json.is_none());
        let detailed = MatchLogEntryView::from_entry(entry(Some(r#"{"kills":3}"#)), true);
        assert!(detailed.has_payload);
        assert_eq!(detailed.payload_json.as_deref(), Some(r#"{"kills":3}"#));
        let empty = MatchLogEntryView::from_entry(entry(None), true);
        assert!(!empty.has_payload);
        assert!(empty.payload_json.is_none());
    }

    #[test]
    fn the_payload_is_never_rewritten_on_the_way_out() {
        // Verbatim is the contract: re-serializing would reorder keys and
        // renormalize numbers, so an operator would not be reading what their
        // script wrote.
        let author = r#"{ "b":1, "a":2, "n":1.50 }"#;
        let view = MatchLogEntryView::from_entry(entry(Some(author)), true);
        assert_eq!(view.payload_json.as_deref(), Some(author));
    }

    #[test]
    fn a_filter_over_fetches_one_row_to_discover_the_next_cursor() {
        let filter = log_filter(&LogsQuery {
            limit: Some(10),
            ..LogsQuery::default()
        })
        .expect("valid filter");
        assert_eq!(filter.limit, 11);
        assert!(filter.level.is_none());
        assert!(filter.match_id.is_none());
    }

    #[test]
    fn malformed_filters_are_rejected_without_touching_the_store() {
        assert!(
            log_filter(&LogsQuery {
                match_id: Some("not-a-match".to_string()),
                ..LogsQuery::default()
            })
            .is_err()
        );
        assert!(
            log_filter(&LogsQuery {
                after: Some("not-a-cursor".to_string()),
                ..LogsQuery::default()
            })
            .is_err()
        );
        assert!(
            log_filter(&LogsQuery {
                level: Some("eror".to_string()),
                ..LogsQuery::default()
            })
            .is_err()
        );
        assert!(
            log_filter(&LogsQuery {
                tag: Some("t".repeat(MAX_TAG_PREFIX_BYTES + 1)),
                ..LogsQuery::default()
            })
            .is_err()
        );
        assert!(
            log_filter(&LogsQuery {
                tag: Some(String::new()),
                ..LogsQuery::default()
            })
            .is_err()
        );
    }

    #[test]
    fn a_well_formed_filter_carries_every_parameter_through() {
        let match_id = format!("mt1-{}", "c".repeat(29));
        let after = format!("ml1-{}", "d".repeat(29));
        let filter = log_filter(&LogsQuery {
            match_id: Some(match_id.clone()),
            level: Some("error".to_string()),
            tag: Some("combat".to_string()),
            after: Some(after.clone()),
            limit: Some(200),
        })
        .expect("valid filter");
        assert_eq!(filter.match_id.as_deref(), Some(match_id.as_str()));
        assert_eq!(filter.level, Some(LogLevel::Error));
        assert_eq!(filter.tag_prefix.as_deref(), Some("combat"));
        assert_eq!(filter.after_log_id.as_deref(), Some(after.as_str()));
        assert_eq!(filter.limit, MAX_LIMIT + 1);
    }
}
