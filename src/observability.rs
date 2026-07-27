//! Tracing and logging initialization for Citadel.
//!
//!  scope: provide a single entry point that initializes the global
//! tracing subscriber from [`LoggingConfig`], selecting pretty logs for local
//! development and structured JSON for production. Metrics exporters and
//! OpenTelemetry are out of scope and arrive with later tasks.
//!
//! Initialization is global and process-wide. [`init`] uses a fallible
//! installer so that repeated calls (for example across multiple tests in one
//! process) do not panic; the first successful install wins.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use serde::Serialize;

use crate::config::{LogFormat, LoggingConfig};
use crate::error::{AppError, AppResult};

use tracing_subscriber::EnvFilter;

/// Process-wide runtime counters surfaced by the node dashboard.
///
/// The registry is cheap and lock-free: counters are monotonic `u64` totals and
/// gauges are signed `i64` current values, all updated with relaxed atomics
/// because the dashboard reports an approximate, eventually-consistent snapshot
/// rather than a linearizable one. Hold it behind an
/// [`Arc`](std::sync::Arc) (see [`App`](crate::app::App)) and increment it from
/// the paths that own each event (HTTP handlers today; transport listeners and
/// the session service wire their gauges as those paths grow).
#[derive(Debug, Default)]
pub struct NodeMetrics {
    http_requests_total: AtomicU64,
    connections_active: AtomicI64,
    connections_accepted_total: AtomicU64,
    participants_active: AtomicI64,
    sessions_active: AtomicI64,
    messages_in_total: AtomicU64,
    messages_out_total: AtomicU64,
    bytes_in_total: AtomicU64,
    bytes_out_total: AtomicU64,
    notification_live_delivered_total: AtomicU64,
    notification_live_dropped_total: AtomicU64,
}

impl NodeMetrics {
    /// Create an all-zero registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one served HTTP request.
    pub fn record_http_request(&self) {
        self.http_requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a newly accepted realtime connection (gauge +1, total +1).
    pub fn connection_opened(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a closed realtime connection (gauge -1).
    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a realtime participant registering (gauge +1).
    ///
    /// Counts every registered participant — guest or authenticated — so
    /// `participants_active` reflects realtime presence. Paired with
    /// [`participant_closed`](NodeMetrics::participant_closed) on disconnect.
    pub fn participant_opened(&self) {
        self.participants_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a realtime participant unregistering (gauge -1).
    pub fn participant_closed(&self) {
        self.participants_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record an *authenticated* session becoming active (gauge +1).
    ///
    /// Incremented only when a participant is bound to an account via the
    /// realtime handshake; guests never move this gauge, so
    /// `sessions_active` counts authenticated sessions distinctly from
    /// `participants_active`.
    pub fn session_opened(&self) {
        self.sessions_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authenticated session ending (gauge -1).
    pub fn session_closed(&self) {
        self.sessions_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record an inbound message of `bytes` bytes.
    pub fn record_message_in(&self, bytes: u64) {
        self.messages_in_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_in_total.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an outbound message of `bytes` bytes.
    pub fn record_message_out(&self, bytes: u64) {
        self.messages_out_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_out_total.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record one notification queued to a local realtime session.
    pub fn record_notification_live_delivered(&self) {
        self.notification_live_delivered_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a notification skipped because its bounded local queue could not
    /// accept it. The durable inbox is unaffected.
    pub fn record_notification_live_dropped(&self) {
        self.notification_live_dropped_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent-enough snapshot for reporting.
    ///
    /// Values are read independently, so a snapshot may interleave concurrent
    /// updates by a few events; this is intentional for a status view.
    #[must_use]
    pub fn snapshot(&self) -> NodeMetricsSnapshot {
        NodeMetricsSnapshot {
            http_requests_total: self.http_requests_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed).max(0) as u64,
            connections_accepted_total: self.connections_accepted_total.load(Ordering::Relaxed),
            participants_active: self.participants_active.load(Ordering::Relaxed).max(0) as u64,
            sessions_active: self.sessions_active.load(Ordering::Relaxed).max(0) as u64,
            messages_in_total: self.messages_in_total.load(Ordering::Relaxed),
            messages_out_total: self.messages_out_total.load(Ordering::Relaxed),
            bytes_in_total: self.bytes_in_total.load(Ordering::Relaxed),
            bytes_out_total: self.bytes_out_total.load(Ordering::Relaxed),
            notification_live_delivered_total: self
                .notification_live_delivered_total
                .load(Ordering::Relaxed),
            notification_live_dropped_total: self
                .notification_live_dropped_total
                .load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of [`NodeMetrics`], serialized by the dashboard.
///
/// Gauges are clamped to `0` on the (transient) chance a decrement is observed
/// before its paired increment, so the reported view never goes negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NodeMetricsSnapshot {
    /// Total HTTP requests served.
    pub http_requests_total: u64,
    /// Currently open realtime connections.
    pub connections_active: u64,
    /// Total realtime connections accepted since start.
    pub connections_accepted_total: u64,
    /// Currently registered realtime participants (guests + authenticated).
    pub participants_active: u64,
    /// Currently active authenticated sessions (account-bound participants only).
    pub sessions_active: u64,
    /// Total inbound realtime messages.
    pub messages_in_total: u64,
    /// Total outbound realtime messages.
    pub messages_out_total: u64,
    /// Total inbound bytes.
    pub bytes_in_total: u64,
    /// Total outbound bytes.
    pub bytes_out_total: u64,
    /// Notification envelopes accepted by local bounded realtime queues.
    pub notification_live_delivered_total: u64,
    /// Notification envelopes dropped because a local bounded queue was full or closed.
    pub notification_live_dropped_total: u64,
}

/// Build the [`EnvFilter`] for the given level directive.
///
/// The directive follows `tracing_subscriber` syntax (e.g. `info`, `debug`,
/// `citadel=trace,info`). An invalid directive maps to a [`ErrorCategory::Config`]
/// error rather than panicking.
///
/// [`ErrorCategory::Config`]: crate::error::ErrorCategory::Config
fn build_filter(level: &str) -> AppResult<EnvFilter> {
    EnvFilter::try_new(level)
        .map_err(|e| AppError::config("invalid log level directive").with_detail(e.to_string()))
}

/// Initialize the global tracing subscriber from configuration.
///
/// Returns `Ok(true)` if this call installed the subscriber, and `Ok(false)`
/// if a global subscriber was already installed (for example by an earlier
/// call or another test). Returns a [`ErrorCategory::Config`] error if the log
/// level directive is invalid.
///
/// [`ErrorCategory::Config`]: crate::error::ErrorCategory::Config
pub fn init(config: &LoggingConfig) -> AppResult<bool> {
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    // Validate the directive up front so an invalid level is a typed error
    // regardless of whether a subscriber is already installed.
    let make_filter = || build_filter(&config.level);

    let installed = match config.format {
        LogFormat::Pretty => {
            let layer = fmt::layer().with_target(true).compact();
            tracing_subscriber::registry()
                .with(make_filter()?)
                .with(layer)
                .try_init()
                .is_ok()
        }
        LogFormat::Json => {
            let layer = fmt::layer().with_target(true).json();
            tracing_subscriber::registry()
                .with(make_filter()?)
                .with(layer)
                .try_init()
                .is_ok()
        }
    };

    Ok(installed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_filter_accepts_valid_directives() {
        assert!(build_filter("info").is_ok());
        assert!(build_filter("citadel=trace,info").is_ok());
    }

    #[test]
    fn build_filter_rejects_invalid_directive() {
        // A directive with an unknown level value is rejected by EnvFilter.
        let err = build_filter("citadel=notalevel").expect_err("should reject");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }

    #[test]
    fn init_is_idempotent_and_does_not_panic() {
        let config = LoggingConfig::default();
        // First call may or may not win depending on test ordering within the
        // process, but it must never panic and must return Ok either way.
        let first = init(&config);
        assert!(first.is_ok());
        let second = init(&config);
        assert!(second.is_ok());
    }

    #[test]
    fn metrics_counters_and_gauges_track_events() {
        let m = NodeMetrics::new();
        m.record_http_request();
        m.record_http_request();
        m.connection_opened();
        m.connection_opened();
        m.connection_closed();
        // Two participants register; one of them authenticates.
        m.participant_opened();
        m.participant_opened();
        m.session_opened();
        m.record_message_in(100);
        m.record_message_out(40);

        let snap = m.snapshot();
        assert_eq!(snap.http_requests_total, 2);
        assert_eq!(snap.connections_accepted_total, 2);
        assert_eq!(snap.connections_active, 1);
        assert_eq!(snap.participants_active, 2);
        assert_eq!(
            snap.sessions_active, 1,
            "only the authenticated participant moves sessions_active"
        );
        assert_eq!(snap.messages_in_total, 1);
        assert_eq!(snap.bytes_in_total, 100);
        assert_eq!(snap.messages_out_total, 1);
        assert_eq!(snap.bytes_out_total, 40);
    }

    #[test]
    fn metrics_gauge_never_reports_negative() {
        let m = NodeMetrics::new();
        // A stray close before any open must not surface as a negative gauge.
        m.connection_closed();
        assert_eq!(m.snapshot().connections_active, 0);
    }

    #[test]
    fn metrics_snapshot_serializes_to_expected_shape() {
        let m = NodeMetrics::new();
        m.record_http_request();
        let value = serde_json::to_value(m.snapshot()).expect("serializes");
        assert_eq!(value["http_requests_total"], 1);
        assert_eq!(value["connections_active"], 0);
        assert!(value.get("bytes_out_total").is_some());
    }

    #[test]
    fn init_rejects_invalid_level_before_install() {
        let config = LoggingConfig {
            level: "citadel=notalevel".to_string(),
            ..LoggingConfig::default()
        };
        let err = init(&config).expect_err("invalid level must error");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }
}
