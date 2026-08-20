//! Bounded, privacy-safe telemetry for validated authoritative decisions.
//!
//! The recorder retains only opaque numeric correlations and generic decision
//! classifications. It never accepts or stores participant identity, payload
//! bytes, replies, commands, or corrected values.

use std::collections::VecDeque;
use std::sync::Mutex;

/// Default number of decision records retained in process memory.
pub const DEFAULT_AUTHORITATIVE_DECISION_CAPACITY: usize = 1_024;

/// Opaque numeric identifiers that correlate one validated decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoritativeDecisionCorrelation {
    /// Opaque authoritative-match correlation.
    pub match_id: u64,
    /// Opaque command-batch correlation.
    pub batch_id: u64,
    /// Opaque per-batch event correlation.
    pub event_id: u64,
}

impl AuthoritativeDecisionCorrelation {
    /// Create an opaque correlation tuple.
    #[must_use]
    pub const fn new(match_id: u64, batch_id: u64, event_id: u64) -> Self {
        Self {
            match_id,
            batch_id,
            event_id,
        }
    }
}

/// Stable generic classification of a validated authoritative decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativeDecisionOutcome {
    /// The canonical input effect was accepted.
    Accepted,
    /// The canonical input effect was rejected.
    Rejected,
    /// A server-authoritative replacement was selected.
    Corrected,
}

impl AuthoritativeDecisionOutcome {
    /// Stable lowercase code for aggregate consumers.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Corrected => "corrected",
        }
    }
}

/// Stable generic reason representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativeDecisionReason {
    /// The decision has no reason code.
    NotApplicable,
    /// An opaque numeric code supplied by the authoritative decision producer.
    OpaqueCode(u16),
}

impl AuthoritativeDecisionReason {
    /// Stable lowercase category for aggregate consumers.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::OpaqueCode(_) => "opaque_code",
        }
    }
}

/// One retained authoritative decision record.
///
/// This deliberately contains no payload, reply, command, identity, or value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoritativeDecisionRecord {
    /// Process-local monotonic position, used only to derive bounded slice windows.
    /// It is never emitted through the operator API.
    pub(crate) sequence: u64,
    /// Opaque numeric correlations only.
    pub correlation: AuthoritativeDecisionCorrelation,
    /// Generic decision classification.
    pub outcome: AuthoritativeDecisionOutcome,
    /// Generic reason representation.
    pub reason: AuthoritativeDecisionReason,
}

/// Aggregate counters for the bounded recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AuthoritativeDecisionMetrics {
    /// Current number of retained records.
    pub retained: usize,
    /// Total retained-record writes since process start.
    pub recorded_total: u64,
    /// Total accepted decisions since process start.
    pub accepted_total: u64,
    /// Total rejected decisions since process start.
    pub rejected_total: u64,
    /// Total corrected decisions since process start.
    pub corrected_total: u64,
    /// Total oldest records removed due to the retention bound.
    pub evicted_total: u64,
}

#[derive(Debug, Default)]
struct RecorderState {
    records: VecDeque<AuthoritativeDecisionRecord>,
    metrics: AuthoritativeDecisionMetrics,
}

/// Bounded FIFO recorder for already-validated authoritative decisions.
#[derive(Debug)]
pub struct AuthoritativeDecisionRecorder {
    capacity: usize,
    state: Mutex<RecorderState>,
}

impl AuthoritativeDecisionRecorder {
    /// Create a recorder retaining at least one record.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(RecorderState::default()),
        }
    }

    /// Record one already-validated decision, evicting the oldest record first.
    pub fn record(
        &self,
        correlation: AuthoritativeDecisionCorrelation,
        outcome: AuthoritativeDecisionOutcome,
        reason: AuthoritativeDecisionReason,
    ) {
        let mut state = self.lock();
        if state.records.len() == self.capacity {
            state.records.pop_front();
            state.metrics.evicted_total = state.metrics.evicted_total.saturating_add(1);
        }
        state.metrics.recorded_total = state.metrics.recorded_total.saturating_add(1);
        let sequence = state.metrics.recorded_total;
        state.records.push_back(AuthoritativeDecisionRecord {
            sequence,
            correlation,
            outcome,
            reason,
        });
        match outcome {
            AuthoritativeDecisionOutcome::Accepted => {
                state.metrics.accepted_total = state.metrics.accepted_total.saturating_add(1);
            }
            AuthoritativeDecisionOutcome::Rejected => {
                state.metrics.rejected_total = state.metrics.rejected_total.saturating_add(1);
            }
            AuthoritativeDecisionOutcome::Corrected => {
                state.metrics.corrected_total = state.metrics.corrected_total.saturating_add(1);
            }
        }
        state.metrics.retained = state.records.len();
    }

    /// Return retained records in deterministic oldest-first FIFO order.
    #[must_use]
    pub fn records(&self) -> Vec<AuthoritativeDecisionRecord> {
        self.lock().records.iter().copied().collect()
    }

    /// Return aggregate recorder metrics.
    #[must_use]
    pub fn metrics(&self) -> AuthoritativeDecisionMetrics {
        self.lock().metrics
    }

    /// Return the configured retention bound.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
