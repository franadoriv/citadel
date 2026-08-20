//! Authenticated operator projections for closed authoritative telemetry slices.
//!
//! There is no public route and no mutation route. Responses contain the service's
//! closed aggregate-only reports; active slices, recorder rows, correlations,
//! payloads, identities, and raw state remain private.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;

use crate::app::App;
use crate::authoritative_telemetry_slices::{ClosedTelemetrySliceReport, valid_report_id};
use crate::error::AppError;
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
    pub limit: Option<usize>,
}

/// JSON-safe projection of a closed redacted report. This intentionally excludes
/// internal correlations and all raw data.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClosedSliceReportResponse {
    pub report_id: String,
    pub context_kind: &'static str,
    pub close_reason: &'static str,
    pub closed_at_ms: u64,
    pub duration_ms: u64,
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
            context_kind: report.context_kind,
            close_reason: report.close_reason,
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

/// A bounded list response containing only closed, redacted reports.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClosedSliceReportsPage {
    pub items: Vec<ClosedSliceReportResponse>,
}

/// `GET /console/v1/telemetry/slices`: read closed reports only.
pub(super) async fn list_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<SliceListQuery>,
) -> Result<Json<ClosedSliceReportsPage>, ApiError> {
    app.metrics().record_http_request();
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let items = app.telemetry_slices().map_or_else(Vec::new, |service| {
        service.reap(SystemClock.now().unix_millis());
        service
            .list_closed(limit)
            .into_iter()
            .map(ClosedSliceReportResponse::from)
            .collect()
    });
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "telemetry.slice.list",
        "closed_reports",
        format!("returned={}", items.len()),
    ));
    Ok(Json(ClosedSliceReportsPage { items }))
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
    let report = app
        .telemetry_slices()
        .and_then(|service| service.closed_by_id(&report_id))
        .ok_or_else(|| AppError::not_found("telemetry slice report not found"))?;
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "telemetry.slice.detail",
        report_id,
        "read closed redacted telemetry slice report",
    ));
    Ok(Json(ClosedSliceReportResponse::from(report)))
}
