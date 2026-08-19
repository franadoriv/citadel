//! Operator-only lag diagnostic reports and raw-artifact lifecycle.
//!
//! Report responses are compact projections of derived metrics. Raw CLAG stays
//! inside the private ingest service: an administrator may receive it only as
//! a non-cacheable attachment after presenting the console bearer, never in a
//! JSON document or at a static filesystem-derived URL.

use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::lag_analysis::{
    AnalysisOptions, AnalysisWorkResult, ArtifactAnalysisRequest, LagObservationSummary, LagReport,
    LagReportCaptureOverview, LagReportStatus, LagTimelineWindow, MetricQuality,
};
use crate::lag_diagnostics::{LagDiagnosticsError, PrivateCaptureOverview, PrivateRawArtifact};
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

use super::super::error::ApiError;

/// Derived-report list route (readable by `viewer`).
pub const LAG_REPORTS_PATH: &str = "/console/v1/lag/reports";
/// One explicit derived-report detail projection (readable by `viewer`).
pub const LAG_REPORT_DETAIL_PATH: &str = "/console/v1/lag/reports/:report_id";
/// Bounded timeline windows for one opaque report id (readable by `viewer`).
pub const LAG_REPORT_WINDOWS_PATH: &str = "/console/v1/lag/reports/:report_id/windows";
/// Capture overview index (readable by `viewer`, retention fields redacted).
pub const LAG_CAPTURES_PATH: &str = "/console/v1/lag/captures";
/// Raw artifact handle list (administrator only).
pub const LAG_CAPTURE_RAW_PATH: &str = "/console/v1/lag/captures/:capture_id/raw";
/// One opaque raw artifact handle (administrator only, GET attachment / DELETE).
pub const LAG_CAPTURE_RAW_HANDLE_PATH: &str = "/console/v1/lag/captures/:capture_id/raw/:handle";
/// Request a fresh derived report from a retained opaque raw handle (administrator only).
pub const LAG_CAPTURE_REGENERATE_PATH: &str = "/console/v1/lag/captures/:capture_id/regenerate";

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;
const MAX_REPORT_PAGE_LIMIT: usize = 20;
const MAX_REPORT_VIEW_SUMMARIES: usize = 32;
const MAX_WINDOWS_PER_RESPONSE: usize = 64;

/// Cursor pagination shared by report and raw-handle lists.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageQuery {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

/// Safe report projection; it intentionally has no artifact digest, raw path,
/// MIME, upload capability, JTI, capture bytes, or client packet rows.
#[derive(Debug, Clone, Serialize)]
pub struct ReportView {
    pub report_id: String,
    pub capture_id: String,
    pub generation: u64,
    pub decoder_version: u16,
    pub analyzer_version: u16,
    pub options_hash: String,
    pub status: LagReportStatus,
    /// Raw retention state is useful to admins but redacted for viewers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_available: Option<bool>,
    pub created_at_unix_ms: u64,
    pub quality: MetricQuality,
    pub summaries: Vec<LagObservationSummary>,
    /// The immutable stored report is never rewritten; this flag indicates a
    /// defensive Console response projection cap was applied.
    pub truncated: bool,
}

impl ReportView {
    fn from_report(report: LagReport, can_see_raw_state: bool) -> Self {
        let mut quality = report.quality;
        let mut summaries = report.summaries;
        let truncated = summaries.len() > MAX_REPORT_VIEW_SUMMARIES;
        if truncated {
            let excluded = summaries.len().saturating_sub(MAX_REPORT_VIEW_SUMMARIES);
            summaries.truncate(MAX_REPORT_VIEW_SUMMARIES);
            quality.excluded_count = quality
                .excluded_count
                .saturating_add(u32::try_from(excluded).unwrap_or(u32::MAX));
            quality.status = "partial".to_string();
        }
        Self {
            report_id: report.report_id,
            capture_id: report.capture_id,
            generation: report.generation,
            decoder_version: report.decoder_version,
            analyzer_version: report.analyzer_version,
            options_hash: report.options_hash,
            status: report.status,
            raw_available: can_see_raw_state.then_some(report.raw_available),
            created_at_unix_ms: report.created_at.unix_millis(),
            quality,
            summaries,
            truncated,
        }
    }
}

/// Cursor page of redacted report projections.
#[derive(Debug, Clone, Serialize)]
pub struct ReportsPage {
    pub items: Vec<ReportView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

/// Capture-level report and retention projection. Viewer responses omit every
/// raw-retention field; neither role sees paths, raw bytes, client packet rows,
/// MIME values, grants, or upload tokens.
#[derive(Debug, Clone, Serialize)]
pub struct CaptureView {
    pub capture_id: String,
    pub generation: u64,
    pub report_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_report_status: Option<LagReportStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_available: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_artifact_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_compressed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_raw_received_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapturesPage {
    pub items: Vec<CaptureView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

/// Bounded aggregate windows for one report; never individual CLAG rows.
#[derive(Debug, Clone, Serialize)]
pub struct WindowsResponse {
    pub report_id: String,
    pub windows: Vec<LagTimelineWindow>,
    pub truncated: bool,
}

/// Admin-only opaque raw artifact metadata. `handle` cannot be converted to a
/// path by the caller and is accepted only after strict server-side validation.
#[derive(Debug, Clone, Serialize)]
pub struct RawArtifactView {
    pub handle: String,
    pub generation: u64,
    pub received_at_unix_ms: u64,
    pub compressed_bytes: u64,
    pub record_count: u32,
}

impl From<PrivateRawArtifact> for RawArtifactView {
    fn from(value: PrivateRawArtifact) -> Self {
        Self {
            handle: value.handle,
            generation: value.generation,
            received_at_unix_ms: value.received_utc_ms,
            compressed_bytes: value.compressed_bytes,
            record_count: value.record_count,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RawArtifactsPage {
    pub items: Vec<RawArtifactView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

/// Regeneration input contains only an opaque handle and analysis policy.
/// Participant attribution is derived from the retained private manifest, not
/// supplied by an operator request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegenerateBody {
    pub handle: String,
    #[serde(default)]
    pub options: AnalysisOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegenerateResponse {
    pub outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report_id: Option<String>,
}

/// `GET /console/v1/lag/reports`: paged report-only diagnostics for both roles.
pub(super) async fn list_reports_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<PageQuery>,
) -> Result<Json<ReportsPage>, ApiError> {
    app.metrics().record_http_request();
    let after = query.after.as_deref();
    if after.is_some_and(|value| !valid_report_id(value)) {
        return Err(AppError::validation("invalid report cursor").into());
    }
    let limit = effective_report_limit(query.limit);
    let mut reports = app.list_lag_reports(after, limit.saturating_add(1)).await?;
    let has_more = reports.len() > limit;
    reports.truncate(limit);
    let next_after = has_more.then(|| {
        reports
            .last()
            .expect("a full report page has a final opaque cursor")
            .report_id
            .clone()
    });
    let can_see_raw_state = operator.require_admin().is_ok();
    Ok(Json(ReportsPage {
        items: reports
            .into_iter()
            .map(|report| ReportView::from_report(report, can_see_raw_state))
            .collect(),
        next_after,
    }))
}

/// `GET /console/v1/lag/reports/{id}`: explicit redacted report detail.
pub(super) async fn report_detail_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(report_id): Path<String>,
) -> Result<Json<ReportView>, ApiError> {
    app.metrics().record_http_request();
    if !valid_report_id(&report_id) {
        return Err(AppError::not_found("report not found").into());
    }
    let report = app
        .lag_report_by_id(&report_id)
        .await?
        .ok_or_else(|| AppError::not_found("report not found"))?;
    Ok(Json(ReportView::from_report(
        report,
        operator.require_admin().is_ok(),
    )))
}

/// `GET /console/v1/lag/captures`: bounded capture keyset overview. This
/// combines retained private-manifest aggregates with report rows so a report
/// remains visible after its raw artifact has expired or been deleted.
pub(super) async fn list_captures_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Query(query): Query<PageQuery>,
) -> Result<Json<CapturesPage>, ApiError> {
    app.metrics().record_http_request();
    let after = query.after.clone();
    if after
        .as_deref()
        .is_some_and(|value| !valid_capture_id(value))
    {
        return Err(AppError::validation("invalid capture cursor").into());
    }
    let service = Arc::clone(app.lag_diagnostics());
    let after_for_worker = after.clone();
    let raw = tokio::task::spawn_blocking(move || {
        service.list_private_capture_overviews(after_for_worker.as_deref(), MAX_LIMIT + 1)
    })
    .await
    .map_err(|_| AppError::internal("lag capture listing failed"))?;
    let raw = match raw {
        Ok(value) => value,
        // Existing report rows remain useful when diagnostic recording is
        // disabled or raw retention has been removed.
        Err(LagDiagnosticsError::Disabled | LagDiagnosticsError::Rejected) => Vec::new(),
        Err(LagDiagnosticsError::InvalidRequest | LagDiagnosticsError::NotFlushing) => Vec::new(),
        Err(LagDiagnosticsError::Storage) => {
            return Err(AppError::internal("lag capture storage unavailable").into());
        }
    };
    let mut captures = std::collections::BTreeMap::<String, CaptureAggregate>::new();
    for raw in raw {
        match captures.entry(raw.capture_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(CaptureAggregate::from_raw(raw));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge_raw(raw);
            }
        }
    }
    for report in app
        .list_lag_capture_overviews(after.as_deref(), MAX_LIMIT + 1)
        .await?
    {
        match captures.entry(report.capture_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(CaptureAggregate::from_report_overview(report));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge_report_overview(report);
            }
        }
    }
    let limit = effective_limit(query.limit);
    let mut page = captures
        .into_values()
        .filter(|capture| {
            after
                .as_deref()
                .is_none_or(|cursor| capture.capture_id.as_str() > cursor)
        })
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let has_more = page.len() > limit;
    page.truncate(limit);
    let is_admin = operator.require_admin().is_ok();
    let next_after = has_more.then(|| {
        page.last()
            .expect("full capture page has a final opaque cursor")
            .capture_id
            .clone()
    });
    Ok(Json(CapturesPage {
        items: page
            .into_iter()
            .map(|capture| capture.into_view(is_admin))
            .collect(),
        next_after,
    }))
}

/// `GET /console/v1/lag/reports/{id}/windows`: return aggregate windows only.
pub(super) async fn windows_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(report_id): Path<String>,
) -> Result<Json<WindowsResponse>, ApiError> {
    app.metrics().record_http_request();
    let _ = operator;
    if !valid_report_id(&report_id) {
        return Err(AppError::not_found("report not found").into());
    }
    let report = app
        .lag_report_by_id(&report_id)
        .await?
        .ok_or_else(|| AppError::not_found("report not found"))?;
    let mut windows = report.windows;
    let truncated = windows.len() > MAX_WINDOWS_PER_RESPONSE;
    windows.truncate(MAX_WINDOWS_PER_RESPONSE);
    Ok(Json(WindowsResponse {
        report_id,
        windows,
        truncated,
    }))
}

/// `GET /console/v1/lag/captures/{capture}/raw`: admin-only opaque handles.
pub(super) async fn list_raw_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(capture_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<RawArtifactsPage>, ApiError> {
    app.metrics().record_http_request();
    require_lag_admin(&app, &operator, "lag.raw.list")?;
    if !valid_capture_id(&capture_id) {
        record_lag_audit(&app, &operator, "lag.raw.list", None, "invalid_request");
        return Err(AppError::validation("invalid capture id").into());
    }
    let after = query.after.as_deref();
    if after.is_some_and(|value| !valid_raw_handle(value)) {
        record_lag_audit(
            &app,
            &operator,
            "lag.raw.list",
            Some(&capture_id),
            "invalid_request",
        );
        return Err(AppError::validation("invalid raw artifact cursor").into());
    }
    let limit = effective_limit(query.limit);
    let service = Arc::clone(app.lag_diagnostics());
    let capture_for_worker = capture_id.clone();
    let after_for_worker = after.map(str::to_owned);
    let result = tokio::task::spawn_blocking(move || {
        service.list_private_raw_artifacts(
            &capture_for_worker,
            after_for_worker.as_deref(),
            limit.saturating_add(1),
        )
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(_) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.list",
                Some(&capture_id),
                "worker_failed",
            );
            return Err(AppError::internal("lag raw listing failed").into());
        }
    };
    let mut items = match result {
        Ok(items) => items,
        Err(error) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.list",
                Some(&capture_id),
                "storage_unavailable",
            );
            return Err(raw_not_found(error).into());
        }
    };
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_after = has_more.then(|| {
        items
            .last()
            .expect("a full raw page has a final opaque cursor")
            .handle
            .clone()
    });
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "lag.raw.list",
        capture_id,
        "listed opaque raw artifact handles",
    ));
    Ok(Json(RawArtifactsPage {
        items: items.into_iter().map(Into::into).collect(),
        next_after,
    }))
}

/// `GET /console/v1/lag/captures/{capture}/raw/{handle}`: raw attachment.
pub(super) async fn download_raw_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((capture_id, handle)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    app.metrics().record_http_request();
    require_lag_admin(&app, &operator, "lag.raw.download")?;
    if !valid_capture_id(&capture_id) || !valid_raw_handle(&handle) {
        record_lag_audit(&app, &operator, "lag.raw.download", None, "invalid_request");
        return Err(AppError::validation("invalid raw artifact request").into());
    }
    let service = Arc::clone(app.lag_diagnostics());
    let capture_for_worker = capture_id.clone();
    let handle_for_worker = handle.clone();
    let result = tokio::task::spawn_blocking(move || {
        service.download_private_raw_artifact(&capture_for_worker, &handle_for_worker)
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(_) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.download",
                Some(&capture_id),
                "worker_failed",
            );
            return Err(AppError::internal("lag raw download failed").into());
        }
    };
    let raw = match result {
        Ok(raw) => raw,
        Err(error) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.download",
                Some(&capture_id),
                "not_found",
            );
            return Err(raw_not_found(error).into());
        }
    };
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "lag.raw.download",
        capture_id,
        "downloaded opaque raw artifact attachment",
    ));
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CONTENT_DISPOSITION, "attachment"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        Body::from(raw.bytes),
    )
        .into_response())
}

/// `DELETE /console/v1/lag/captures/{capture}/raw/{handle}`: first make the
/// exact report source unavailable in durable storage, then remove its private
/// bytes and manifest. A subsequent regeneration has no private artifact to
/// load, even when a storage race prevents the physical cleanup.
pub(super) async fn delete_raw_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((capture_id, handle)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    require_lag_admin(&app, &operator, "lag.raw.delete")?;
    if !valid_capture_id(&capture_id) || !valid_raw_handle(&handle) {
        record_lag_audit(&app, &operator, "lag.raw.delete", None, "invalid_request");
        return Err(AppError::validation("invalid raw artifact request").into());
    }
    let service = Arc::clone(app.lag_diagnostics());
    let capture_for_worker = capture_id.clone();
    let handle_for_worker = handle.clone();
    let inspected = tokio::task::spawn_blocking(move || {
        service.inspect_private_raw_artifact(&capture_for_worker, &handle_for_worker)
    })
    .await;
    let inspected = match inspected {
        Ok(inspected) => inspected,
        Err(_) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.delete",
                Some(&capture_id),
                "worker_failed",
            );
            return Err(AppError::internal("lag raw deletion failed").into());
        }
    };
    let inspected = match inspected {
        Ok(inspected) => inspected,
        Err(error) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.delete",
                Some(&capture_id),
                "not_found",
            );
            return Err(raw_not_found(error).into());
        }
    };
    if let Err(error) = app
        .mark_lag_raw_unavailable(
            &inspected.capture_id,
            inspected.generation,
            &inspected.digest_sha256,
        )
        .await
    {
        record_lag_audit(
            &app,
            &operator,
            "lag.raw.delete",
            Some(&capture_id),
            "projection_failed",
        );
        return Err(error.into());
    }
    let service = Arc::clone(app.lag_diagnostics());
    let capture_for_worker = capture_id.clone();
    let handle_for_worker = handle.clone();
    let result = tokio::task::spawn_blocking(move || {
        service.delete_private_raw_artifact(&capture_for_worker, &handle_for_worker)
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(_) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.raw.delete",
                Some(&capture_id),
                "worker_failed",
            );
            return Err(AppError::internal("lag raw deletion failed").into());
        }
    };
    if let Err(error) = result {
        record_lag_audit(
            &app,
            &operator,
            "lag.raw.delete",
            Some(&capture_id),
            "storage_failed",
        );
        return Err(raw_not_found(error).into());
    }
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        &operator,
        "lag.raw.delete",
        capture_id,
        "deleted opaque raw artifact and disabled regeneration",
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /console/v1/lag/captures/{capture}/regenerate`: regenerates only from
/// an extant private artifact bound to the same capture. It never accepts raw
/// data in the request body.
pub(super) async fn regenerate_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(capture_id): Path<String>,
    body: Result<Json<RegenerateBody>, JsonRejection>,
) -> Result<Json<RegenerateResponse>, ApiError> {
    app.metrics().record_http_request();
    require_lag_admin(&app, &operator, "lag.regenerate")?;
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => {
            record_lag_audit(&app, &operator, "lag.regenerate", None, "invalid_request");
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    if !valid_capture_id(&capture_id) || !valid_raw_handle(&body.handle) {
        record_lag_audit(&app, &operator, "lag.regenerate", None, "invalid_request");
        return Err(AppError::validation("invalid regeneration request").into());
    }

    let now = SystemClock.now();
    // Resolve/bind the handle first without reading its body. The real worker
    // then performs the single digest-verified load on its blocking task.
    let service = Arc::clone(app.lag_diagnostics());
    let capture_for_check = capture_id.clone();
    let handle_for_check = body.handle.clone();
    let available = tokio::task::spawn_blocking(move || {
        service.inspect_private_raw_artifact(&capture_for_check, &handle_for_check)
    })
    .await;
    let available = match available {
        Ok(available) => available,
        Err(_) => {
            record_lag_audit(
                &app,
                &operator,
                "lag.regenerate",
                Some(&capture_id),
                "worker_failed",
            );
            return Err(AppError::internal("lag regeneration preflight failed").into());
        }
    };
    if available.is_err() {
        app.audit_log().record(AuditEntry::for_principal(
            now,
            &operator,
            "lag.regenerate",
            capture_id.clone(),
            "regeneration outcome=raw_unavailable",
        ));
        return Err(AppError::conflict("raw artifact unavailable for regeneration").into());
    }

    let outcome = app
        .lag_analysis_worker()
        .analyze_artifact_async(
            Arc::clone(app.lag_diagnostics()),
            ArtifactAnalysisRequest {
                artifact_id: body.handle,
                analyze: true,
                options: body.options,
            },
            now,
        )
        .await;
    let response = regenerate_response(app.persist_lag_analysis(outcome).await);
    app.audit_log().record(AuditEntry::for_principal(
        now,
        &operator,
        "lag.regenerate",
        capture_id,
        format!("regeneration outcome={}", response.outcome),
    ));
    Ok(Json(response))
}

fn effective_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

fn effective_report_limit(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_REPORT_PAGE_LIMIT)
}

fn valid_report_id(value: &str) -> bool {
    value.len() == 28
        && value.starts_with("lr1-")
        && value[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_raw_handle(value: &str) -> bool {
    value.len() == 36
        && value.starts_with("lc1-")
        && value[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_capture_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, Clone)]
struct CaptureAggregate {
    capture_id: String,
    generation: u64,
    report_count: u32,
    latest_report_status: Option<LagReportStatus>,
    latest_report_at: u64,
    raw_artifact_count: u32,
    raw_compressed_bytes: u64,
    latest_raw_received_at_unix_ms: u64,
}

impl CaptureAggregate {
    fn from_raw(raw: PrivateCaptureOverview) -> Self {
        Self {
            capture_id: raw.capture_id,
            generation: raw.generation,
            report_count: 0,
            latest_report_status: None,
            latest_report_at: 0,
            raw_artifact_count: raw.raw_artifact_count,
            raw_compressed_bytes: raw.raw_compressed_bytes,
            latest_raw_received_at_unix_ms: raw.latest_received_utc_ms,
        }
    }

    fn from_report_overview(report: LagReportCaptureOverview) -> Self {
        Self {
            capture_id: report.capture_id,
            generation: report.generation,
            report_count: report.report_count,
            latest_report_status: Some(report.latest_report_status),
            latest_report_at: report.latest_report_created_at.unix_millis(),
            raw_artifact_count: 0,
            raw_compressed_bytes: 0,
            latest_raw_received_at_unix_ms: 0,
        }
    }

    fn merge_raw(&mut self, raw: PrivateCaptureOverview) {
        self.generation = self.generation.max(raw.generation);
        self.raw_artifact_count = self.raw_artifact_count.max(raw.raw_artifact_count);
        self.raw_compressed_bytes = self.raw_compressed_bytes.max(raw.raw_compressed_bytes);
        self.latest_raw_received_at_unix_ms = self
            .latest_raw_received_at_unix_ms
            .max(raw.latest_received_utc_ms);
    }

    fn merge_report_overview(&mut self, report: LagReportCaptureOverview) {
        self.generation = self.generation.max(report.generation);
        self.report_count = self.report_count.saturating_add(report.report_count);
        if report.latest_report_created_at.unix_millis() >= self.latest_report_at {
            self.latest_report_at = report.latest_report_created_at.unix_millis();
            self.latest_report_status = Some(report.latest_report_status);
        }
    }

    fn into_view(self, is_admin: bool) -> CaptureView {
        CaptureView {
            capture_id: self.capture_id,
            generation: self.generation,
            report_count: self.report_count,
            latest_report_status: self.latest_report_status,
            raw_available: is_admin.then_some(self.raw_artifact_count > 0),
            raw_artifact_count: is_admin.then_some(self.raw_artifact_count),
            raw_compressed_bytes: is_admin.then_some(self.raw_compressed_bytes),
            latest_raw_received_at_unix_ms: is_admin.then_some(self.latest_raw_received_at_unix_ms),
        }
    }
}

fn raw_not_found(error: LagDiagnosticsError) -> AppError {
    match error {
        LagDiagnosticsError::Storage => AppError::internal("lag raw storage unavailable"),
        LagDiagnosticsError::Disabled
        | LagDiagnosticsError::InvalidRequest
        | LagDiagnosticsError::NotFlushing
        | LagDiagnosticsError::Rejected => AppError::not_found("raw artifact not found"),
    }
}

fn record_lag_audit(
    app: &App,
    operator: &ConsolePrincipal,
    action: &str,
    capture_id: Option<&str>,
    outcome: &str,
) {
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        operator,
        action,
        capture_id.unwrap_or("lag-capture"),
        format!("outcome={outcome}"),
    ));
}

fn require_lag_admin(app: &App, operator: &ConsolePrincipal, action: &str) -> Result<(), ApiError> {
    operator.require_admin().map_err(|error| {
        record_lag_audit(app, operator, action, None, "forbidden");
        ApiError::from(error)
    })
}

fn regenerate_response(outcome: AnalysisWorkResult) -> RegenerateResponse {
    match outcome {
        AnalysisWorkResult::Completed(report) | AnalysisWorkResult::Existing(report) => {
            RegenerateResponse {
                outcome: "complete",
                report_id: Some(report.report_id),
            }
        }
        AnalysisWorkResult::Joined => RegenerateResponse {
            outcome: "joined",
            report_id: None,
        },
        AnalysisWorkResult::Busy => RegenerateResponse {
            outcome: "busy",
            report_id: None,
        },
        AnalysisWorkResult::NoAnalysis => RegenerateResponse {
            outcome: "no_analysis",
            report_id: None,
        },
        AnalysisWorkResult::RawUnavailable => RegenerateResponse {
            outcome: "raw_unavailable",
            report_id: None,
        },
        AnalysisWorkResult::Failed => RegenerateResponse {
            outcome: "failed",
            report_id: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write as _;
    use std::path::PathBuf;

    use axum::body::to_bytes;
    use axum::http::Request;
    use base64::Engine as _;
    use citadel_wire::diagnostics::{CaptureId, UploadContentEncoding, UploadContentType};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use sha2::{Digest, Sha256};
    use tower::ServiceExt as _;
    use uuid::Uuid;

    use super::*;
    use crate::config::{Config, LagDiagnosticsConfig};
    use crate::lag_analysis::AnalysisWorkResult;
    use crate::lag_diagnostics::{CaptureFlushPlan, CaptureParticipant};
    use crate::services::{ConsoleIdentity, ConsoleRole};
    use crate::time::TimestampMillis;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("citadel-console-lag-{}", Uuid::new_v4())))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn app(root: &TestRoot) -> App {
        let mut config = Config::default();
        config.console.viewer_password = Some("viewer-secret".to_string());
        let mut keys = BTreeMap::new();
        keys.insert(
            "current".to_string(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7_u8; 32]),
        );
        config.lag_diagnostics = LagDiagnosticsConfig {
            enabled: true,
            raw_root: Some(root.0.display().to_string()),
            active_key_id: Some("current".to_string()),
            upload_hmac_keys: keys,
            allowed_origins: Vec::new(),
            max_compressed_bytes: 1024 * 1024,
            max_decompressed_bytes: 1024 * 1024,
            max_decompression_ratio: 32,
            max_concurrent_uploads: 2,
            max_raw_bytes: 4 * 1024 * 1024,
            retention_hours: 1,
            shared_raw_store: false,
        };
        App::new(config)
    }

    fn gzip_clag(capture: CaptureId) -> Vec<u8> {
        let mut plain = vec![0_u8; 128];
        plain[0..4].copy_from_slice(b"CLAG");
        plain[4..6].copy_from_slice(&1_u16.to_be_bytes());
        plain[6..8].copy_from_slice(&128_u16.to_be_bytes());
        plain[8..10].copy_from_slice(&48_u16.to_be_bytes());
        plain[10..12].copy_from_slice(&0x0005_u16.to_be_bytes());
        plain[48..64].copy_from_slice(&capture.bytes());
        plain[64..68].copy_from_slice(&1_u32.to_be_bytes());
        plain[72..80].copy_from_slice(&1_u64.to_be_bytes());
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(&plain).expect("gzip input");
        gzip.finish().expect("gzip finish")
    }

    fn publish(app: &App, capture: CaptureId) -> (String, String) {
        let now = 1_000_u64;
        let service = app.lag_diagnostics();
        service
            .register_recording(capture, 1, now + 10_000)
            .expect("recording");
        let grant = service
            .open_flush(
                CaptureFlushPlan {
                    capture_id: capture,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: now + 5_000,
                    max_compressed_bytes: 1024 * 1024,
                    required_uploads: 1,
                    analyze: false,
                    participants: vec![CaptureParticipant {
                        participant_id: 1,
                        session_id: "session".to_string(),
                        tenant_id: "tenant".to_string(),
                        match_id: "match".to_string(),
                    }],
                },
                TimestampMillis::from_unix_millis(now),
            )
            .expect("flush")
            .pop()
            .expect("grant");
        let payload = gzip_clag(capture);
        let lease = service
            .begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                Some(payload.len() as u64),
                None,
                TimestampMillis::from_unix_millis(now),
            )
            .expect("lease");
        let mut staging = std::fs::OpenOptions::new()
            .write(true)
            .open(lease.staging_path())
            .expect("staging");
        staging.write_all(&payload).expect("write staging");
        staging.sync_all().expect("sync staging");
        let receipt = service
            .validate_and_publish(
                lease,
                payload.len() as u64,
                Sha256::digest(&payload).into(),
                TimestampMillis::from_unix_millis(now),
            )
            .expect("publish");
        (receipt.artifact_id, hex_capture(capture))
    }

    fn hex_capture(capture: CaptureId) -> String {
        capture
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn token(app: &App, role: ConsoleRole) -> String {
        app.console_tokens()
            .issue(ConsoleIdentity {
                username: role.as_str().to_string(),
                role,
            })
            .expect("token")
    }

    async fn request(
        app: App,
        bearer: &str,
        method: &str,
        uri: String,
        body: Body,
    ) -> axum::response::Response {
        crate::http::router(app)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .expect("request"),
            )
            .await
            .expect("response")
    }

    #[test]
    fn report_ids_and_limits_are_bounded() {
        assert!(valid_report_id(&format!("lr1-{}", "a".repeat(24))));
        assert!(!valid_report_id("../report"));
        assert!(valid_raw_handle(&format!("lc1-{}", "a".repeat(32))));
        assert!(!valid_raw_handle("../raw"));
        assert_eq!(effective_limit(None), DEFAULT_LIMIT);
        assert_eq!(effective_limit(Some(0)), 1);
        assert_eq!(effective_limit(Some(10_000)), MAX_LIMIT);
    }

    #[test]
    fn raw_download_never_uses_a_filename_or_json_mime() {
        let response = (
            [
                (header::CONTENT_TYPE, "application/octet-stream"),
                (header::CONTENT_DISPOSITION, "attachment"),
            ],
            Body::from("raw"),
        )
            .into_response();
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("content type header"),
            "application/octet-stream"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_DISPOSITION)
                .expect("content disposition header"),
            "attachment"
        );
    }

    #[tokio::test]
    async fn viewer_reads_redacted_reports_while_admin_owns_raw_lifecycle_and_audit() {
        let root = TestRoot::new();
        let app = app(&root);
        let capture = CaptureId::new([12; 16]).expect("capture");
        let (handle, capture_id) = publish(&app, capture);
        let analyzed = app
            .lag_analysis_worker()
            .analyze_artifact_async(
                Arc::clone(app.lag_diagnostics()),
                ArtifactAnalysisRequest {
                    artifact_id: handle.clone(),
                    analyze: true,
                    options: AnalysisOptions::default(),
                },
                TimestampMillis::from_unix_millis(2_000),
            )
            .await;
        assert!(matches!(&analyzed, AnalysisWorkResult::Completed(_)));
        let AnalysisWorkResult::Completed(report) = analyzed else {
            return;
        };
        let regenerated_with_new_options = app
            .lag_analysis_worker()
            .analyze_artifact_async(
                Arc::clone(app.lag_diagnostics()),
                ArtifactAnalysisRequest {
                    artifact_id: handle.clone(),
                    analyze: true,
                    options: AnalysisOptions {
                        send_rate_hz: Some(30),
                        max_windows: 2,
                    },
                },
                TimestampMillis::from_unix_millis(2_001),
            )
            .await;
        assert!(matches!(
            regenerated_with_new_options,
            AnalysisWorkResult::Completed(_)
        ));
        let viewer = token(&app, ConsoleRole::Viewer);
        let admin = token(&app, ConsoleRole::Admin);

        let viewer_reports = request(
            app.clone(),
            &viewer,
            "GET",
            LAG_REPORTS_PATH.to_string(),
            Body::empty(),
        )
        .await;
        assert_eq!(viewer_reports.status(), StatusCode::OK);
        let viewer_body = to_bytes(viewer_reports.into_body(), 64 * 1024)
            .await
            .expect("body");
        let viewer_json: serde_json::Value = serde_json::from_slice(&viewer_body).expect("json");
        assert!(viewer_json.to_string().contains(&report.report_id));
        assert!(viewer_json.get("artifact_digest_sha256").is_none());
        assert!(!viewer_json.to_string().contains("raw_available"));

        let first_report_page = request(
            app.clone(),
            &viewer,
            "GET",
            format!("{LAG_REPORTS_PATH}?limit=1"),
            Body::empty(),
        )
        .await;
        let first_page_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(first_report_page.into_body(), 64 * 1024)
                .await
                .expect("body"),
        )
        .expect("json");
        let report_cursor = first_page_json["next_after"]
            .as_str()
            .expect("lookahead cursor")
            .to_string();
        let second_report_page = request(
            app.clone(),
            &viewer,
            "GET",
            format!("{LAG_REPORTS_PATH}?limit=1&after={report_cursor}"),
            Body::empty(),
        )
        .await;
        let second_page_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(second_report_page.into_body(), 64 * 1024)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_ne!(
            first_page_json["items"][0]["report_id"],
            second_page_json["items"][0]["report_id"]
        );

        let viewer_detail = request(
            app.clone(),
            &viewer,
            "GET",
            format!("/console/v1/lag/reports/{}", report.report_id),
            Body::empty(),
        )
        .await;
        assert_eq!(viewer_detail.status(), StatusCode::OK);
        let detail_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(viewer_detail.into_body(), 64 * 1024)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(detail_json["report_id"], report.report_id);
        assert!(detail_json.get("raw_available").is_none());

        let captures = request(
            app.clone(),
            &viewer,
            "GET",
            LAG_CAPTURES_PATH.to_string(),
            Body::empty(),
        )
        .await;
        assert_eq!(captures.status(), StatusCode::OK);
        let captures_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(captures.into_body(), 64 * 1024)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(captures_json["items"][0]["capture_id"], capture_id);
        assert_eq!(captures_json["items"][0]["report_count"], 2);
        assert!(captures_json["items"][0].get("raw_available").is_none());
        assert!(
            captures_json["items"][0]
                .get("raw_artifact_count")
                .is_none()
        );

        let raw_path = format!("/console/v1/lag/captures/{capture_id}/raw");
        let viewer_raw =
            request(app.clone(), &viewer, "GET", raw_path.clone(), Body::empty()).await;
        assert_eq!(viewer_raw.status(), StatusCode::FORBIDDEN);

        let raw_list = request(app.clone(), &admin, "GET", raw_path.clone(), Body::empty()).await;
        assert_eq!(raw_list.status(), StatusCode::OK);
        let raw_list_body = to_bytes(raw_list.into_body(), 64 * 1024)
            .await
            .expect("body");
        let raw_json: serde_json::Value = serde_json::from_slice(&raw_list_body).expect("json");
        assert_eq!(raw_json["items"][0]["handle"], handle);
        assert!(raw_json["items"][0].get("participant_id").is_none());
        assert!(!raw_json.to_string().contains("raw_path"));

        let download_path = format!("{raw_path}/{handle}");
        let download = request(
            app.clone(),
            &admin,
            "GET",
            download_path.clone(),
            Body::empty(),
        )
        .await;
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(
            download
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("content type header"),
            "application/octet-stream"
        );
        assert_eq!(
            download
                .headers()
                .get(header::CONTENT_DISPOSITION)
                .expect("content disposition header"),
            "attachment"
        );

        let deleted = request(app.clone(), &admin, "DELETE", download_path, Body::empty()).await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        let persisted = app
            .lag_reports()
            .find_by_report_id(&report.report_id)
            .expect("report retained after raw deletion");
        assert_eq!(persisted.status, report.status);
        assert!(!persisted.raw_available);
        let admin_reports = request(
            app.clone(),
            &admin,
            "GET",
            LAG_REPORTS_PATH.to_string(),
            Body::empty(),
        )
        .await;
        let admin_body = to_bytes(admin_reports.into_body(), 64 * 1024)
            .await
            .expect("body");
        let admin_json: serde_json::Value = serde_json::from_slice(&admin_body).expect("json");
        assert_eq!(admin_json["items"][0]["raw_available"], false);

        let regenerate = request(
            app.clone(),
            &admin,
            "POST",
            format!("/console/v1/lag/captures/{capture_id}/regenerate"),
            Body::from(format!(r#"{{"handle":"{handle}"}}"#)),
        )
        .await;
        assert_eq!(regenerate.status(), StatusCode::CONFLICT);
        let audit = app.audit_log().list(&Default::default());
        assert!(audit.iter().any(|entry| entry.action == "lag.raw.list"));
        assert!(audit.iter().any(|entry| entry.action == "lag.raw.download"));
        assert!(audit.iter().any(|entry| entry.action == "lag.raw.delete"));
        assert!(audit.iter().any(|entry| entry.action == "lag.regenerate"));
    }

    #[tokio::test]
    async fn opaque_handle_cursor_and_path_rejections_are_bounded() {
        let root = TestRoot::new();
        let app = app(&root);
        let capture = CaptureId::new([13; 16]).expect("capture");
        let (handle, capture_id) = publish(&app, capture);
        let admin = token(&app, ConsoleRole::Admin);
        let raw_path = format!("/console/v1/lag/captures/{capture_id}/raw");
        let page = request(
            app.clone(),
            &admin,
            "GET",
            format!("{raw_path}?limit=1"),
            Body::empty(),
        )
        .await;
        let page_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(page.into_body(), 64 * 1024).await.expect("body"))
                .expect("json");
        assert!(page_json.get("next_after").is_none());
        let after = request(
            app.clone(),
            &admin,
            "GET",
            format!("{raw_path}?after={handle}"),
            Body::empty(),
        )
        .await;
        let after_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(after.into_body(), 64 * 1024).await.expect("body"))
                .expect("json");
        assert!(after_json["items"].as_array().expect("array").is_empty());
        let invalid_cursor = request(
            app.clone(),
            &admin,
            "GET",
            format!("{raw_path}?after=not-an-artifact-handle"),
            Body::empty(),
        )
        .await;
        assert_eq!(invalid_cursor.status(), StatusCode::BAD_REQUEST);
        assert!(
            app.audit_log()
                .list(&Default::default())
                .iter()
                .any(|entry| {
                    entry.action == "lag.raw.list" && entry.details == "outcome=invalid_request"
                })
        );
        let traversal = request(
            app,
            &admin,
            "GET",
            format!("{raw_path}/..%2Foutside"),
            Body::empty(),
        )
        .await;
        assert!(matches!(
            traversal.status(),
            StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST
        ));
    }
}
