//! Bounded, redacted telemetry slices derived from authoritative decisions.
//!
//! This service accepts only server-derived contexts and bounded namespaced
//! markers. It never accepts report identifiers, payloads, identities, raw
//! state, or decision correlations from callers. Closed reports contain only
//! aggregate authoritative-decision outcomes.

use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::authoritative_decision_telemetry::{
    AuthoritativeDecisionOutcome, AuthoritativeDecisionRecorder,
};

const MAX_MARKER_BYTES: usize = 64;
const MAX_CONTEXTS: usize = 256;
/// One slice may span a long-lived operation, but never retain in-process state
/// for longer than one day without a new bounded report being closed.
const MAX_TTL_MS: u64 = 86_400_000;

thread_local! {
    /// The authoritative room being dispatched on this thread. Runtime adapters
    /// set it while constructing a server-owned invocation context; scripts can
    /// never supply or mutate this correlation.
    static ACTIVE_RUNTIME_SCOPE: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Set the server-owned active runtime scope for one synchronous invocation.
pub(crate) fn set_active_runtime_scope(room_id: Option<u64>) {
    ACTIVE_RUNTIME_SCOPE.with(|scope| scope.set(room_id));
}

/// Return the context derived from the active runtime invocation, if any.
pub(crate) fn active_runtime_context() -> Option<TelemetrySliceContext> {
    ACTIVE_RUNTIME_SCOPE.with(|scope| scope.get().map(TelemetrySliceContext::match_context))
}

/// A trusted, server-derived scope used to correlate a slice with recorder data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TelemetrySliceContext {
    kind: SliceContextKind,
    correlation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SliceContextKind {
    Match,
    Scope,
}

impl TelemetrySliceContext {
    /// Construct a context only from one of the supported generic server scopes.
    pub fn new(kind: &str, correlation: u64) -> Result<Self, TelemetrySliceError> {
        let kind = match kind {
            "match" => SliceContextKind::Match,
            "scope" => SliceContextKind::Scope,
            _ => return Err(TelemetrySliceError::InvalidContext),
        };
        Ok(Self { kind, correlation })
    }

    /// Construct a context derived from an authoritative match correlation.
    #[must_use]
    pub const fn match_context(correlation: u64) -> Self {
        Self {
            kind: SliceContextKind::Match,
            correlation,
        }
    }

    /// Construct a context derived from a server-owned long-lived scope correlation.
    #[must_use]
    pub const fn scope_context(correlation: u64) -> Self {
        Self {
            kind: SliceContextKind::Scope,
            correlation,
        }
    }

    const fn kind_code(self) -> &'static str {
        match self.kind {
            SliceContextKind::Match => "match",
            SliceContextKind::Scope => "scope",
        }
    }
}

/// Hard bounds for active slices and retained closed reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetrySlicePolicy {
    max_active: usize,
    max_markers: usize,
    ttl_ms: u64,
    max_closed_reports: usize,
}

impl TelemetrySlicePolicy {
    /// Create a policy. Every parameter is bounded and must be nonzero.
    pub fn new(
        max_active: usize,
        max_markers: usize,
        ttl_ms: u64,
        max_closed_reports: usize,
    ) -> Result<Self, TelemetrySliceError> {
        if max_active == 0
            || max_active > MAX_CONTEXTS
            || max_markers == 0
            || max_markers > MAX_CONTEXTS
            || ttl_ms == 0
            || ttl_ms > MAX_TTL_MS
            || max_closed_reports == 0
            || max_closed_reports > MAX_CONTEXTS
        {
            return Err(TelemetrySliceError::InvalidPolicy);
        }
        Ok(Self {
            max_active,
            max_markers,
            ttl_ms,
            max_closed_reports,
        })
    }
}

impl Default for TelemetrySlicePolicy {
    fn default() -> Self {
        // All retained state is in-process and intentionally small.
        Self::new(32, 32, 300_000, 128).expect("constant telemetry slice policy is valid")
    }
}

/// A closed, aggregate-only report suitable for an authenticated operator surface.
#[cfg_attr(not(test), derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedTelemetrySliceReport {
    /// Server-generated opaque report identifier; no caller can select it.
    pub report_id: String,
    /// Generic context class, never the context's raw correlation.
    pub context_kind: &'static str,
    /// Why the server closed the slice.
    pub close_reason: &'static str,
    /// Server-clock close time in Unix milliseconds.
    pub closed_at_ms: u64,
    /// Bounded duration computed by the server clock.
    pub duration_ms: u64,
    /// Server-validated marker names only; no marker payload exists.
    pub markers: Vec<String>,
    /// Whether bounded recorder eviction may have removed decisions from this slice.
    pub truncated: bool,
    /// Aggregate decision counts derived only from decisions recorded after begin.
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub corrected_total: u64,
}

/// Slice operation failure, deliberately without sensitive context or state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySliceError {
    InvalidContext,
    InvalidPolicy,
    InvalidMarker,
    NotActive,
}

impl fmt::Display for TelemetrySliceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidContext => "telemetry slice context is invalid",
            Self::InvalidPolicy => "telemetry slice policy is invalid",
            Self::InvalidMarker => "telemetry slice marker is invalid",
            Self::NotActive => "telemetry slice is not active",
        })
    }
}

impl std::error::Error for TelemetrySliceError {}

#[derive(Debug)]
struct ActiveSlice {
    context: TelemetrySliceContext,
    started_at_ms: u64,
    start_sequence: u64,
    start_evictions: u64,
    markers: Vec<String>,
}

#[derive(Debug, Default)]
struct SliceState {
    active: BTreeMap<TelemetrySliceContext, ActiveSlice>,
    closed: VecDeque<ClosedTelemetrySliceReport>,
    next_report: u64,
}

/// Process-local service for bounded authoritative telemetry investigations.
#[derive(Debug)]
pub struct TelemetrySliceService {
    recorder: Arc<AuthoritativeDecisionRecorder>,
    policy: TelemetrySlicePolicy,
    state: Mutex<SliceState>,
}

impl TelemetrySliceService {
    #[must_use]
    pub fn new(recorder: Arc<AuthoritativeDecisionRecorder>, policy: TelemetrySlicePolicy) -> Self {
        Self {
            recorder,
            policy,
            state: Mutex::new(SliceState::default()),
        }
    }

    /// Begin a slice for a trusted context. Existing same-context slices are closed.
    pub fn begin(
        &self,
        context: TelemetrySliceContext,
        now_ms: u64,
    ) -> Result<(), TelemetrySliceError> {
        let mut state = self.lock();
        self.close_expired(&mut state, now_ms);
        if state.active.contains_key(&context) {
            self.close_one(&mut state, context, now_ms, "restarted");
        }
        while state.active.len() >= self.policy.max_active {
            let Some(oldest) = state
                .active
                .values()
                .min_by_key(|slice| slice.started_at_ms)
                .map(|slice| slice.context)
            else {
                break;
            };
            self.close_one(&mut state, oldest, now_ms, "active_cap");
        }
        let metrics = self.recorder.metrics();
        state.active.insert(
            context,
            ActiveSlice {
                context,
                started_at_ms: now_ms,
                start_sequence: metrics.recorded_total,
                start_evictions: metrics.evicted_total,
                markers: Vec::new(),
            },
        );
        Ok(())
    }

    /// Add one bounded namespaced marker. Hitting the marker bound closes the slice.
    pub fn mark(
        &self,
        context: TelemetrySliceContext,
        marker: &str,
        now_ms: u64,
    ) -> Result<Option<ClosedTelemetrySliceReport>, TelemetrySliceError> {
        if !valid_marker(marker) {
            return Err(TelemetrySliceError::InvalidMarker);
        }
        let mut state = self.lock();
        self.close_expired(&mut state, now_ms);
        let slice = state
            .active
            .get_mut(&context)
            .ok_or(TelemetrySliceError::NotActive)?;
        if slice.markers.len() >= self.policy.max_markers {
            return Ok(self.close_one(&mut state, context, now_ms, "marker_cap"));
        }
        slice.markers.push(marker.to_owned());
        Ok(None)
    }

    /// Finish an active slice and return its server-generated closed report.
    pub fn finish(
        &self,
        context: TelemetrySliceContext,
        now_ms: u64,
    ) -> Result<ClosedTelemetrySliceReport, TelemetrySliceError> {
        let mut state = self.lock();
        self.close_expired(&mut state, now_ms);
        self.close_one(&mut state, context, now_ms, "finished")
            .ok_or(TelemetrySliceError::NotActive)
    }

    /// Auto-close expired slices. Intended to be called by trusted lifecycle work.
    pub fn reap(&self, now_ms: u64) {
        self.close_expired(&mut self.lock(), now_ms);
    }

    /// List closed reports newest-first, bounded by the caller's requested limit.
    #[must_use]
    pub fn list_closed(&self, limit: usize) -> Vec<ClosedTelemetrySliceReport> {
        self.lock()
            .closed
            .iter()
            .rev()
            .take(limit.min(self.policy.max_closed_reports))
            .cloned()
            .collect()
    }

    /// Read one server-generated closed report only.
    #[must_use]
    pub fn closed_by_id(&self, report_id: &str) -> Option<ClosedTelemetrySliceReport> {
        if !valid_report_id(report_id) {
            return None;
        }
        self.lock()
            .closed
            .iter()
            .find(|report| report.report_id == report_id)
            .cloned()
    }

    fn close_expired(&self, state: &mut SliceState, now_ms: u64) {
        let expired: Vec<_> = state
            .active
            .values()
            .filter(|slice| now_ms.saturating_sub(slice.started_at_ms) >= self.policy.ttl_ms)
            .map(|slice| slice.context)
            .collect();
        for context in expired {
            self.close_one(state, context, now_ms, "ttl");
        }
    }

    fn close_one(
        &self,
        state: &mut SliceState,
        context: TelemetrySliceContext,
        now_ms: u64,
        close_reason: &'static str,
    ) -> Option<ClosedTelemetrySliceReport> {
        let slice = state.active.remove(&context)?;
        let mut accepted_total = 0_u64;
        let mut rejected_total = 0_u64;
        let mut corrected_total = 0_u64;
        for record in self.recorder.records().into_iter().filter(|record| {
            record.sequence > slice.start_sequence
                && record.correlation.match_id == context.correlation
        }) {
            match record.outcome {
                AuthoritativeDecisionOutcome::Accepted => {
                    accepted_total = accepted_total.saturating_add(1)
                }
                AuthoritativeDecisionOutcome::Rejected => {
                    rejected_total = rejected_total.saturating_add(1)
                }
                AuthoritativeDecisionOutcome::Corrected => {
                    corrected_total = corrected_total.saturating_add(1)
                }
            }
        }
        state.next_report = state.next_report.saturating_add(1);
        let report = ClosedTelemetrySliceReport {
            report_id: format!("ats1-{:024x}", state.next_report),
            context_kind: context.kind_code(),
            close_reason,
            closed_at_ms: now_ms,
            duration_ms: now_ms.saturating_sub(slice.started_at_ms),
            markers: slice.markers,
            truncated: self.recorder.metrics().evicted_total > slice.start_evictions,
            accepted_total,
            rejected_total,
            corrected_total,
        };
        if state.closed.len() == self.policy.max_closed_reports {
            state.closed.pop_front();
        }
        state.closed.push_back(report.clone());
        Some(report)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SliceState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn valid_marker(marker: &str) -> bool {
    if marker.is_empty()
        || marker.len() > MAX_MARKER_BYTES
        || !marker.contains('.')
        || marker.starts_with('.')
        || marker.ends_with('.')
    {
        return false;
    }
    let mut previous_dot = false;
    for byte in marker.bytes() {
        let valid =
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'.';
        if !valid || (previous_dot && byte == b'.') {
            return false;
        }
        previous_dot = byte == b'.';
    }
    true
}

/// Validate an opaque server-generated closed-report id before lookup.
pub fn valid_report_id(value: &str) -> bool {
    value.len() == 29
        && value.starts_with("ats1-")
        && value[5..].bytes().all(|byte| byte.is_ascii_hexdigit())
}
