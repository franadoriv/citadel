//! Durable match records and the per-match drill-down (`/console/v1/matchlogs`).
//!
//! This is history, not the live room registry: `/console/v1/matches` answers
//! "what is happening right now" from the realtime gateway, and this answers
//! "what happened" from the `matches` table, one match later. The two are
//! deliberately separate routes because they are separate questions and because
//! a durable record outlives the process that produced it.
//!
//! A match record is server-owned. Its id, membership shape, clock, and
//! termination reason are selected by the gateway and cannot be supplied or
//! replaced by game code; the one author-supplied column is `result_json`,
//! stamped through `citadel.match.set_result` and returned verbatim. The record
//! stores no participant identity, account id, session id, or transport
//! identifier — per-match detail lives in the child domains this drill-down
//! joins.
//!
//! Namespace note: the durable list is **not** `/console/v1/matches/history`.
//! axum 0.7 routes through matchit 0.7, which panics at router build when a
//! static segment and a `:param` occupy the same position, and
//! `/console/v1/matches/:id` already claims that position.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::lag_analysis::LagReportStatus;
use crate::repository::{DurableAuditFilter, DurableSliceRow, MatchLogFilter, MatchRecord};
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

use super::super::error::ApiError;
use super::logs::{
    LogPageQuery, MatchLogEntryView, dropped_total, effective_limit, valid_log_id, valid_match_id,
};

/// Durable match record listing route (readable by `viewer`).
pub const MATCHLOGS_PATH: &str = "/console/v1/matchlogs";
/// One durable match record plus its per-domain counts (readable by `viewer`).
pub const MATCHLOG_DETAIL_PATH: &str = "/console/v1/matchlogs/:match_id";
/// The drill-down: one match with a page of its logs and its other domains.
pub const MATCHLOG_ENTRIES_PATH: &str = "/console/v1/matchlogs/:match_id/entries";

/// Widest child list the drill-down inlines per domain. The counts alongside
/// them are exact, so a truncated inline list never hides the real total.
const MAX_DRILLDOWN_ROWS: usize = 50;

/// How many of the newest lag reports the drill-down scans for a match scope.
///
/// Lag reports carry no per-match read path on [`App`]: the column and its
/// index shipped ahead of the write path, so this filter is bounded, always
/// empty today, and correct the moment the column is populated.
const LAG_SCAN_LIMIT: usize = 50;

/// Accepted query parameters for [`MATCHLOGS_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchRecordsQuery {
    /// `true` restricts the page to matches that have not been closed yet.
    /// Absent or `false` lists every record.
    pub open: Option<bool>,
    /// Opaque keyset cursor: the previous page's `next_after`.
    pub after: Option<String>,
    /// Page size, newest-first. Default 50, capped at 200.
    pub limit: Option<usize>,
}

/// The server-owned shape of one recorded match.
#[derive(Debug, Clone, Serialize)]
pub struct MatchRecordView {
    pub match_id: String,
    pub node_id: String,
    /// Which run of that node opened the match. `room_id` is a per-process
    /// counter, so this is what makes the pair an identity across restarts.
    pub boot_id: String,
    pub room_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub map: String,
    pub mode: String,
    pub max_players: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_revision_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_generation: Option<u64>,
    pub clock_epoch: u64,
    pub opened_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_unix_ms: Option<u64>,
    /// `final_departure`, `server_closed`, or `formation_abandoned`; absent
    /// while the match is still open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    pub peak_participants: u32,
    pub join_total: u32,
    /// Derived: a record with no close timestamp is still running.
    pub open: bool,
    /// Author-supplied JSON stamped by the game script, returned verbatim.
    /// Present only on the detail and drill-down projections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_json: Option<String>,
}

impl MatchRecordView {
    fn from_record(record: MatchRecord, include_result: bool) -> Self {
        Self {
            match_id: record.match_id,
            node_id: record.node_id,
            boot_id: record.boot_id,
            room_id: record.room_id,
            name: record.name,
            map: record.map,
            mode: record.mode,
            max_players: record.max_players,
            script_revision_id: record.script_revision_id,
            script_generation: record.script_generation,
            clock_epoch: record.clock_epoch,
            opened_at_unix_ms: record.opened_at_ms,
            closed_at_unix_ms: record.closed_at_ms,
            termination_reason: record.termination_reason,
            peak_participants: record.peak_participants,
            join_total: record.join_total,
            open: record.closed_at_ms.is_none(),
            result_json: if include_result {
                record.result_json
            } else {
                None
            },
        }
    }
}

/// Cursor page of durable match records.
#[derive(Debug, Clone, Serialize)]
pub struct MatchRecordsPage {
    pub items: Vec<MatchRecordView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
    /// Whether these rows came from a table. `false` means this backend records
    /// no match history, and the empty page is the whole truth.
    pub durable: bool,
    /// Records the bounded write-behind queue dropped since boot.
    pub dropped_total: u64,
}

/// Aggregate-only projection of one closed telemetry slice.
///
/// There is no marker text here and there never will be: the recorder counts
/// markers, validates them, and discards them.
#[derive(Debug, Clone, Serialize)]
pub struct SliceView {
    pub report_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_id: Option<String>,
    pub context_kind: String,
    pub close_reason: String,
    pub closed_at_unix_ms: u64,
    pub duration_ms: u64,
    pub marker_total: u32,
    pub truncated: bool,
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub corrected_total: u64,
}

impl From<DurableSliceRow> for SliceView {
    fn from(row: DurableSliceRow) -> Self {
        Self {
            report_id: row.report_id,
            match_id: row.match_id,
            context_kind: row.context_kind,
            close_reason: row.close_reason,
            closed_at_unix_ms: row.closed_at_ms,
            duration_ms: row.duration_ms,
            marker_total: row.marker_total,
            truncated: row.truncated,
            accepted_total: row.accepted_total,
            rejected_total: row.rejected_total,
            corrected_total: row.corrected_total,
        }
    }
}

/// Enough of a lag report to open it on the Lag Diagnostics page. It carries no
/// artifact digest, raw path, raw availability, or packet row — that surface
/// owns its own redaction and its own admin gate.
#[derive(Debug, Clone, Serialize)]
pub struct LagReportSummaryView {
    pub report_id: String,
    pub capture_id: String,
    pub status: LagReportStatus,
    pub created_at_unix_ms: u64,
}

/// Exact per-domain totals for one match, independent of the inline caps.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct MatchDrillDownCounts {
    pub logs: u64,
    pub telemetry_slices: u64,
    pub lag_reports: u64,
    pub audit: u64,
}

/// The JSON response for [`MATCHLOG_DETAIL_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct MatchRecordDetail {
    pub record: MatchRecordView,
    pub counts: MatchDrillDownCounts,
}

/// The JSON response for [`MATCHLOG_ENTRIES_PATH`].
///
/// `logs` is the only paged child — it is the only one that grows without a
/// natural bound. The others are inlined up to a fixed cap beside their exact
/// count. `audit` is usually empty on purpose: operator actions are not
/// match-scoped and are deliberately never forced into a match.
#[derive(Debug, Clone, Serialize)]
pub struct MatchDrillDown {
    pub record: MatchRecordView,
    pub logs: Vec<MatchLogEntryView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_next_after: Option<String>,
    pub telemetry_slices: Vec<SliceView>,
    pub lag_reports: Vec<LagReportSummaryView>,
    pub audit: Vec<AuditEntry>,
    pub counts: MatchDrillDownCounts,
}

/// `GET /console/v1/matchlogs`: keyset page of durable match records.
pub(super) async fn list_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<MatchRecordsQuery>,
) -> Result<Json<MatchRecordsPage>, ApiError> {
    app.metrics().record_http_request();
    if query
        .after
        .as_deref()
        .is_some_and(|after| !valid_match_id(after))
    {
        return Err(AppError::validation("invalid match cursor").into());
    }
    let limit = effective_limit(query.limit);
    let mut records = app
        .list_matches(
            query.after.as_deref(),
            limit.saturating_add(1),
            query.open.unwrap_or(false),
        )
        .await?;
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_after = if has_more {
        records.last().map(|record| record.match_id.clone())
    } else {
        None
    };
    let returned = records.len();
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "matchlog.list",
        "matchlogs",
        format!("returned={returned}"),
    ));
    Ok(Json(MatchRecordsPage {
        items: records
            .into_iter()
            .map(|record| MatchRecordView::from_record(record, false))
            .collect(),
        next_after,
        durable: app.matches_are_durable(),
        dropped_total: dropped_total(&app),
    }))
}

/// `GET /console/v1/matchlogs/{match_id}`: one record with its domain counts.
pub(super) async fn detail_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(match_id): Path<String>,
) -> Result<Json<MatchRecordDetail>, ApiError> {
    app.metrics().record_http_request();
    let record = load_record(&app, &match_id).await?;
    let lag_reports = lag_report_summaries(&app, &match_id).await?;
    let counts = domain_counts(&app, &match_id, row_count(lag_reports.len())).await?;
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "matchlog.detail",
        match_id,
        format!("returned=1 logs={}", counts.logs),
    ));
    Ok(Json(MatchRecordDetail {
        record: MatchRecordView::from_record(record, true),
        counts,
    }))
}

/// `GET /console/v1/matchlogs/{match_id}/entries`: the drill-down.
///
/// Paging is keyset on the child (`log_id`), which is the correct shape for a
/// single parent. The match *list* pages its own parent key instead: limiting a
/// joined result would make later matches vanish from the keyset as soon as one
/// match wrote thousands of lines.
pub(super) async fn entries_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(match_id): Path<String>,
    Query(query): Query<LogPageQuery>,
) -> Result<Json<MatchDrillDown>, ApiError> {
    app.metrics().record_http_request();
    if query
        .after
        .as_deref()
        .is_some_and(|after| !valid_log_id(after))
    {
        return Err(AppError::validation("invalid log cursor").into());
    }
    let record = load_record(&app, &match_id).await?;
    let limit = effective_limit(query.limit);
    let mut entries = app
        .list_match_logs(&MatchLogFilter {
            match_id: Some(match_id.clone()),
            level: None,
            tag_prefix: None,
            after_log_id: query.after.clone(),
            limit: limit.saturating_add(1),
        })
        .await?;
    let has_more = entries.len() > limit;
    entries.truncate(limit);
    let logs_next_after = if has_more {
        entries.last().map(|entry| entry.log_id.clone())
    } else {
        None
    };
    let telemetry_slices = app
        .list_slices(Some(&match_id), None, MAX_DRILLDOWN_ROWS)
        .await?;
    let lag_reports = lag_report_summaries(&app, &match_id).await?;
    let audit = app
        .list_audit(&DurableAuditFilter {
            match_id: Some(match_id.clone()),
            limit: MAX_DRILLDOWN_ROWS,
            ..DurableAuditFilter::default()
        })
        .await?;
    let counts = domain_counts(&app, &match_id, row_count(lag_reports.len())).await?;
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "matchlog.entries",
        match_id.as_str(),
        format!(
            "returned={} slices={} lag={} audit={}",
            entries.len(),
            telemetry_slices.len(),
            lag_reports.len(),
            audit.len()
        ),
    ));
    Ok(Json(MatchDrillDown {
        record: MatchRecordView::from_record(record, true),
        logs: entries
            .into_iter()
            .map(|entry| MatchLogEntryView::from_entry(entry, false))
            .collect(),
        logs_next_after,
        telemetry_slices: telemetry_slices.into_iter().map(Into::into).collect(),
        lag_reports,
        audit: audit.into_iter().map(|row| row.entry).collect(),
        counts,
    }))
}

/// Resolve one record, rejecting a malformed id as `404` rather than looking it
/// up: a path parameter that cannot be an id names nothing.
async fn load_record(app: &App, match_id: &str) -> Result<MatchRecord, ApiError> {
    if !valid_match_id(match_id) {
        return Err(AppError::not_found("match record not found").into());
    }
    app.match_record_by_id(match_id)
        .await?
        .ok_or_else(|| AppError::not_found("match record not found").into())
}

/// Exact totals per domain. Each is independent of the inline caps above, so a
/// truncated drill-down still tells an operator how much they are not seeing.
///
/// `lag_reports` is passed in rather than recounted: its only source is a
/// bounded scan the caller has already paid for.
async fn domain_counts(
    app: &App,
    match_id: &str,
    lag_reports: u64,
) -> Result<MatchDrillDownCounts, ApiError> {
    let audit_filter = DurableAuditFilter {
        match_id: Some(match_id.to_string()),
        limit: MAX_DRILLDOWN_ROWS,
        ..DurableAuditFilter::default()
    };
    Ok(MatchDrillDownCounts {
        logs: app.count_match_logs(match_id).await?,
        telemetry_slices: app.count_slices(Some(match_id)).await?,
        lag_reports,
        audit: app.count_audit(&audit_filter).await?,
    })
}

fn row_count(rows: usize) -> u64 {
    u64::try_from(rows).unwrap_or(u64::MAX)
}

/// Lag reports scoped to one match.
///
/// Bounded scan rather than a repository filter: `App` exposes no per-match lag
/// read, and the durable column has no writer yet (a capture is node-scoped and
/// nothing outside tests builds a capture participant carrying a match). The
/// result is therefore always empty today — honestly so, rather than by
/// omitting the field — and becomes correct without a change here once the
/// write path lands.
async fn lag_report_summaries(
    app: &App,
    match_id: &str,
) -> Result<Vec<LagReportSummaryView>, ApiError> {
    Ok(app
        .list_lag_reports(None, LAG_SCAN_LIMIT)
        .await?
        .into_iter()
        .filter(|report| report.match_id.as_deref() == Some(match_id))
        .map(|report| LagReportSummaryView {
            report_id: report.report_id,
            capture_id: report.capture_id,
            status: report.status,
            created_at_unix_ms: report.created_at.unix_millis(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(closed: Option<u64>) -> MatchRecord {
        MatchRecord {
            match_id: format!("mt1-{}", "a".repeat(29)),
            node_id: "node-a".to_string(),
            boot_id: "bt1-0123456789abcdef0123456789abcdef".to_string(),
            room_id: 7,
            name: Some("arena".to_string()),
            map: "corneria".to_string(),
            mode: "versus".to_string(),
            max_players: 8,
            script_revision_id: None,
            script_generation: None,
            clock_epoch: 1_751_790_000_000,
            opened_at_ms: 1_751_791_000_000,
            closed_at_ms: closed,
            termination_reason: closed.map(|_| "final_departure".to_string()),
            peak_participants: 6,
            join_total: 11,
            result_json: Some(r#"{"winner":"kitsune"}"#.to_string()),
        }
    }

    #[test]
    fn the_durable_namespace_never_collides_with_the_live_registry_param() {
        // matchit 0.7 panics at router build when a static segment and a
        // `:param` share a position, and `/console/v1/matches/:id` exists.
        assert!(MATCHLOGS_PATH.starts_with("/console/v1/matchlogs"));
        assert!(!MATCHLOGS_PATH.starts_with("/console/v1/matches"));
        assert_eq!(MATCHLOG_DETAIL_PATH, "/console/v1/matchlogs/:match_id");
        assert_eq!(
            MATCHLOG_ENTRIES_PATH,
            "/console/v1/matchlogs/:match_id/entries"
        );
    }

    #[test]
    fn an_unclosed_record_reads_as_open_and_hides_its_result_in_a_listing() {
        let listed = MatchRecordView::from_record(record(None), false);
        assert!(listed.open);
        assert!(listed.closed_at_unix_ms.is_none());
        assert!(listed.termination_reason.is_none());
        assert!(listed.result_json.is_none());
        let detailed = MatchRecordView::from_record(record(None), true);
        assert_eq!(
            detailed.result_json.as_deref(),
            Some(r#"{"winner":"kitsune"}"#)
        );
    }

    #[test]
    fn a_closed_record_reports_its_termination_reason() {
        let view = MatchRecordView::from_record(record(Some(1_751_791_900_000)), true);
        assert!(!view.open);
        assert_eq!(view.closed_at_unix_ms, Some(1_751_791_900_000));
        assert_eq!(view.termination_reason.as_deref(), Some("final_departure"));
    }

    #[test]
    fn the_slice_projection_carries_aggregates_and_no_marker_text() {
        let view = SliceView::from(DurableSliceRow {
            report_id: format!("ats1-{}", "b".repeat(29)),
            node_id: "node-a".to_string(),
            match_id: Some(format!("mt1-{}", "a".repeat(29))),
            context_kind: "match".to_string(),
            close_reason: "finished".to_string(),
            closed_at_ms: 1_751_791_500_000,
            duration_ms: 500_000,
            marker_total: 12,
            truncated: false,
            accepted_total: 900,
            rejected_total: 3,
            corrected_total: 1,
        });
        assert_eq!(view.marker_total, 12);
        let rendered = serde_json::to_string(&view).expect("slice view serializes");
        assert!(!rendered.contains("marker_text"));
        assert!(rendered.contains("\"accepted_total\":900"));
    }

    #[test]
    fn counts_default_to_zero_so_a_backend_without_tables_still_answers() {
        let counts = MatchDrillDownCounts::default();
        assert_eq!(counts.logs, 0);
        assert_eq!(counts.telemetry_slices, 0);
        assert_eq!(counts.lag_reports, 0);
        assert_eq!(counts.audit, 0);
    }
}
