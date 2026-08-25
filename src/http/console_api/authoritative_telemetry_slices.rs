//! Authenticated operator projections for closed authoritative telemetry slices.
//!
//! There is no public route and no mutation route. Responses contain the service's
//! closed aggregate-only reports; active slices, recorder rows, correlations,
//! payloads, identities, and raw state remain private.
//!
//! Reports are read from the durable table when the backend has one, and from
//! the bounded in-process list otherwise. The two sources are projected through
//! one response type and the page says which it came from, so a process-local
//! cache is never presented as durable history. The durable row is also the only
//! source that carries a match scope: an in-process report deliberately holds no
//! correlation at all, and resolving one to a match happens in the write path.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;

use crate::app::App;
use crate::authoritative_telemetry_slices::{ClosedTelemetrySliceReport, valid_report_id};
use crate::error::AppError;
use crate::repository::DurableSliceRow;
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

use super::super::error::ApiError;

/// Authenticated closed-report list (no public counterpart exists).
pub const AUTHORITATIVE_TELEMETRY_SLICES_PATH: &str = "/console/v1/telemetry/slices";
/// One authenticated closed report identified only by its server-generated id.
pub const AUTHORITATIVE_TELEMETRY_SLICE_DETAIL_PATH: &str =
    "/console/v1/telemetry/slices/:report_id";

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SliceListQuery {
    /// Durable match scope. Absent lists every retained report, scoped or not:
    /// a slice closed outside a match is stored unscoped rather than dropped,
    /// and hiding those would misreport the node's history.
    pub match_id: Option<String>,
    /// Opaque keyset cursor: the `report_id` of the last item of the previous
    /// page. Report ids are time-ordered, so this pages strictly backwards.
    pub after: Option<String>,
    pub limit: Option<usize>,
}

/// JSON-safe projection of a closed redacted report. This intentionally excludes
/// internal correlations and all raw data.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClosedSliceReportResponse {
    pub report_id: String,
    /// Durable server-minted match identity. Absent for a slice closed outside
    /// any match and for a report read from the in-process list, which never
    /// held a correlation to resolve. The raw room correlation is never here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_id: Option<String>,
    pub context_kind: String,
    pub close_reason: String,
    pub closed_at_ms: u64,
    pub duration_ms: u64,
    /// How many markers the slice validated. The marker text itself is counted
    /// and discarded at ingest; nothing stores it and nothing can return it.
    pub marker_total: u32,
    pub truncated: bool,
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub corrected_total: u64,
}

impl From<ClosedTelemetrySliceReport> for ClosedSliceReportResponse {
    fn from(report: ClosedTelemetrySliceReport) -> Self {
        Self {
            report_id: report.report_id,
            match_id: None,
            context_kind: report.context_kind.to_string(),
            close_reason: report.close_reason.to_string(),
            closed_at_ms: report.closed_at_ms,
            duration_ms: report.duration_ms,
            marker_total: report.marker_total,
            truncated: report.truncated,
            accepted_total: report.accepted_total,
            rejected_total: report.rejected_total,
            corrected_total: report.corrected_total,
        }
    }
}

impl From<DurableSliceRow> for ClosedSliceReportResponse {
    /// `node_id` is deliberately dropped: this API answers for one node, and a
    /// column the console never reads is surface without a reader.
    fn from(row: DurableSliceRow) -> Self {
        Self {
            report_id: row.report_id,
            match_id: row.match_id,
            context_kind: row.context_kind,
            close_reason: row.close_reason,
            closed_at_ms: row.closed_at_ms,
            duration_ms: row.duration_ms,
            marker_total: row.marker_total,
            truncated: row.truncated,
            accepted_total: row.accepted_total,
            rejected_total: row.rejected_total,
            corrected_total: row.corrected_total,
        }
    }
}

/// A bounded list response containing only closed, redacted reports.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClosedSliceReportsPage {
    pub items: Vec<ClosedSliceReportResponse>,
    /// Cursor for the next page, absent on the last one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
    /// Whether these reports came from the durable table. `false` means the
    /// bounded in-process list is the whole history this node retains.
    pub durable: bool,
    /// Records the bounded write-behind queues dropped since boot, so a quiet
    /// trail is distinguishable from a lossy one.
    pub dropped_total: u64,
}

/// `GET /console/v1/telemetry/slices`: read closed reports only.
pub(super) async fn list_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<SliceListQuery>,
) -> Result<Json<ClosedSliceReportsPage>, ApiError> {
    app.metrics().record_http_request();
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    if query
        .after
        .as_deref()
        .is_some_and(|cursor| !valid_report_id(cursor))
    {
        return Err(AppError::validation("invalid telemetry slice cursor").into());
    }
    // Reap first: a slice past its TTL must be closed — and, with a durable
    // sink attached, queued — before this read decides what exists.
    if let Some(service) = app.telemetry_slices() {
        service.reap(SystemClock.now().unix_millis());
    }
    let durable = app.slices_are_durable();
    let (items, next_after) = if durable {
        page_durable(&app, &query, limit).await?
    } else {
        page_in_process(&app, &query, limit)
    };
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "telemetry.slice.list",
        "closed_reports",
        format!("returned={}", items.len()),
    ));
    Ok(Json(ClosedSliceReportsPage {
        items,
        next_after,
        durable,
        dropped_total: app.durable_logs().map_or(0, |logs| logs.dropped_total()),
    }))
}

/// `GET /console/v1/telemetry/slices/{id}`: return one closed redacted report.
pub(super) async fn detail_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(report_id): Path<String>,
) -> Result<Json<ClosedSliceReportResponse>, ApiError> {
    app.metrics().record_http_request();
    if !valid_report_id(&report_id) {
        return Err(AppError::not_found("telemetry slice report not found").into());
    }
    if let Some(service) = app.telemetry_slices() {
        service.reap(SystemClock.now().unix_millis());
    }
    // The stored row wins: it outlives the bounded in-process list and is the
    // only copy carrying the match scope resolved when the slice closed.
    let stored = match app.durable_log_repositories().telemetry_slices {
        Some(repository) => repository.get(&report_id).await?,
        None => None,
    };
    let report = match stored {
        Some(row) => ClosedSliceReportResponse::from(row),
        None => app
            .telemetry_slices()
            .and_then(|service| service.closed_by_id(&report_id))
            .map(ClosedSliceReportResponse::from)
            .ok_or_else(|| AppError::not_found("telemetry slice report not found"))?,
    };
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "telemetry.slice.detail",
        report_id,
        "read closed redacted telemetry slice report",
    ));
    Ok(Json(report))
}

/// One keyset page from the durable table, over-fetched by one to decide whether
/// a next cursor exists.
async fn page_durable(
    app: &App,
    query: &SliceListQuery,
    limit: usize,
) -> Result<(Vec<ClosedSliceReportResponse>, Option<String>), ApiError> {
    let mut rows = app
        .list_slices(
            query.match_id.as_deref(),
            query.after.as_deref(),
            limit.saturating_add(1),
        )
        .await?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_after = if has_more {
        rows.last().map(|row| row.report_id.clone())
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(ClosedSliceReportResponse::from)
        .collect();
    Ok((items, next_after))
}

/// The same page shape over the bounded in-process list, which the policy caps
/// well below any requested limit.
fn page_in_process(
    app: &App,
    query: &SliceListQuery,
    limit: usize,
) -> (Vec<ClosedSliceReportResponse>, Option<String>) {
    // An in-process report carries no match scope at all, so a match filter
    // matches none of them. Answering with the unfiltered list instead would
    // attribute every slice on the node to the requested match.
    let reports = match (query.match_id.as_deref(), app.telemetry_slices()) {
        (None, Some(service)) => service.list_closed(usize::MAX),
        _ => Vec::new(),
    };
    let mut items: Vec<ClosedSliceReportResponse> = reports
        .into_iter()
        .filter(|report| {
            query
                .after
                .as_deref()
                .is_none_or(|cursor| report.report_id.as_str() < cursor)
        })
        .take(limit.saturating_add(1))
        .map(ClosedSliceReportResponse::from)
        .collect();
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_after = if has_more {
        items.last().map(|item| item.report_id.clone())
    } else {
        None
    };
    (items, next_after)
}
