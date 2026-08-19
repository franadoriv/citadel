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
    websocket_pings_sent_total: AtomicU64,
    websocket_pongs_received_total: AtomicU64,
    websocket_liveness_timeouts_total: AtomicU64,
    runtime_events_queued_total: AtomicU64,
    runtime_events_dropped_total: AtomicU64,
    runtime_shared_cache_evictions_total: AtomicU64,
    /// Provider-validation requests started by the server-owned purchase client.
    purchase_validation_requests_total: AtomicU64,
    /// Provider-validation requests that did not produce a validated response.
    purchase_validation_failures_total: AtomicU64,
    party_owner_lease_acquire_total: AtomicU64,
    party_owner_lease_renew_total: AtomicU64,
    party_owner_failover_total: AtomicU64,
    party_owner_stale_reject_total: AtomicU64,
    party_owner_forward_total: AtomicU64,
    party_resync_total: AtomicU64,
    /// Current GameScript readiness state as its stable gauge value (see
    /// `ScriptReadinessState::gauge_value`). Meaningful only on nodes running
    /// with `runtime.require_script`; stays 0 (`no_script`) otherwise.
    script_readiness_state: AtomicI64,
    /// One counter per readiness-gate enforcement surface, indexed by
    /// [`ScriptGateSurface`].
    script_gate_rejections: [AtomicU64; ScriptGateSurface::COUNT],
}

/// The enforcement surfaces guarded by the GameScript readiness gate.
///
/// Every surface that can advertise, create, or admit into a match records
/// its fail-closed rejections under its own stable label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScriptGateSurface {
    /// `KIND_ROOM_CREATE` join-or-create.
    RoomCreate,
    /// `KIND_ROOM_JOIN` admission.
    RoomJoin,
    /// `matchmaker.add` ticket queueing.
    MatchmakerQueue,
    /// Local matchmaker cohort activation (room birth on the 250ms tick).
    MatchmakerActivate,
    /// `matchmaker.accept` trusted admission.
    MatchmakerAccept,
    /// Live matchmaker cohort formation (room birth on the shard owner).
    LiveForm,
    /// Live matchmaker acceptance into a locally owned match.
    LiveAcceptLocal,
    /// Live matchmaker acceptance completion for a remotely owned match.
    LiveAcceptRemote,
    /// Live fenced remote-member admission on the match-owner node.
    LiveAdmitRemote,
    /// Cluster control-plane remote-member admission on the match-owner node.
    ClusterAdmitRemote,
    /// Console match listing/detail reads.
    ConsoleList,
}

impl ScriptGateSurface {
    /// Number of gated surfaces (array size for the counters).
    pub const COUNT: usize = 11;

    /// Every surface, in stable index order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::RoomCreate,
        Self::RoomJoin,
        Self::MatchmakerQueue,
        Self::MatchmakerActivate,
        Self::MatchmakerAccept,
        Self::LiveForm,
        Self::LiveAcceptLocal,
        Self::LiveAcceptRemote,
        Self::LiveAdmitRemote,
        Self::ClusterAdmitRemote,
        Self::ConsoleList,
    ];

    /// Stable lowercase label for logs and metrics.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RoomCreate => "room_create",
            Self::RoomJoin => "room_join",
            Self::MatchmakerQueue => "matchmaker_queue",
            Self::MatchmakerActivate => "matchmaker_activate",
            Self::MatchmakerAccept => "matchmaker_accept",
            Self::LiveForm => "live_form",
            Self::LiveAcceptLocal => "live_accept_local",
            Self::LiveAcceptRemote => "live_accept_remote",
            Self::LiveAdmitRemote => "live_admit_remote",
            Self::ClusterAdmitRemote => "cluster_admit_remote",
            Self::ConsoleList => "console_list",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::RoomCreate => 0,
            Self::RoomJoin => 1,
            Self::MatchmakerQueue => 2,
            Self::MatchmakerActivate => 3,
            Self::MatchmakerAccept => 4,
            Self::LiveForm => 5,
            Self::LiveAcceptLocal => 6,
            Self::LiveAcceptRemote => 7,
            Self::LiveAdmitRemote => 8,
            Self::ClusterAdmitRemote => 9,
            Self::ConsoleList => 10,
        }
    }
}

/// Per-surface readiness-gate rejection totals, one stable field per surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct ScriptGateRejectionsSnapshot {
    /// `KIND_ROOM_CREATE` requests refused.
    pub room_create: u64,
    /// `KIND_ROOM_JOIN` requests refused.
    pub room_join: u64,
    /// `matchmaker.add` submissions refused.
    pub matchmaker_queue: u64,
    /// Local matchmaker activation passes refused (no cohort became a room).
    pub matchmaker_activate: u64,
    /// `matchmaker.accept` admissions refused.
    pub matchmaker_accept: u64,
    /// Live matchmaker formations refused on the shard owner.
    pub live_form: u64,
    /// Live local acceptances refused.
    pub live_accept_local: u64,
    /// Live remote acceptance completions refused.
    pub live_accept_remote: u64,
    /// Live fenced remote-member admissions refused on the owner node.
    pub live_admit_remote: u64,
    /// Cluster control-plane remote admissions refused on the owner node.
    pub cluster_admit_remote: u64,
    /// Console listing/detail reads refused.
    pub console_list: u64,
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

    /// Record a native WebSocket Ping control frame sent by the transport.
    pub fn record_websocket_ping_sent(&self) {
        self.websocket_pings_sent_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a native WebSocket Pong control frame received from a peer.
    pub fn record_websocket_pong_received(&self) {
        self.websocket_pongs_received_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WebSocket connection closed after a missed Pong deadline.
    pub fn record_websocket_liveness_timeout(&self) {
        self.websocket_liveness_timeouts_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one event accepted by the local runtime event queue.
    pub fn record_runtime_event_queued(&self) {
        self.runtime_events_queued_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one best-effort runtime event rejected by a local bound.
    pub fn record_runtime_event_dropped(&self) {
        self.runtime_events_dropped_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one entry evicted from the bounded node-local runtime cache.
    pub fn record_runtime_shared_cache_eviction(&self) {
        self.runtime_shared_cache_evictions_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one server-owned receipt-validation request. This counter never
    /// includes account ids, receipt contents, tokens, or provider credentials.
    pub fn record_purchase_validation_request(&self) {
        self.purchase_validation_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one redacted provider-validation failure or deadline.
    pub fn record_purchase_validation_failure(&self) {
        self.purchase_validation_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a durable party-owner lease acquisition. No party, account, or
    /// request identifiers are retained in node metrics.
    pub fn record_party_owner_lease_acquire(&self) {
        self.party_owner_lease_acquire_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful renewal of the local fenced party-owner lease.
    pub fn record_party_owner_lease_renew(&self) {
        self.party_owner_lease_renew_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed higher-generation party-owner recovery.
    pub fn record_party_owner_failover(&self) {
        self.party_owner_failover_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a stale party command/reply rejection.
    pub fn record_party_owner_stale_reject(&self) {
        self.party_owner_stale_reject_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a party command forwarded to its current durable owner.
    pub fn record_party_owner_forward(&self) {
        self.party_owner_forward_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one durable, generation-fenced party resync transition.
    pub fn record_party_resync(&self) {
        self.party_resync_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the current GameScript readiness state gauge value.
    pub fn set_script_readiness_state(&self, value: i64) {
        self.script_readiness_state.store(value, Ordering::Relaxed);
    }

    /// Record one fail-closed readiness-gate rejection on `surface`.
    pub fn record_script_gate_rejection(&self, surface: ScriptGateSurface) {
        self.script_gate_rejections[surface.index()].fetch_add(1, Ordering::Relaxed);
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
            websocket_pings_sent_total: self.websocket_pings_sent_total.load(Ordering::Relaxed),
            websocket_pongs_received_total: self
                .websocket_pongs_received_total
                .load(Ordering::Relaxed),
            websocket_liveness_timeouts_total: self
                .websocket_liveness_timeouts_total
                .load(Ordering::Relaxed),
            runtime_events_queued_total: self.runtime_events_queued_total.load(Ordering::Relaxed),
            runtime_events_dropped_total: self.runtime_events_dropped_total.load(Ordering::Relaxed),
            runtime_shared_cache_evictions_total: self
                .runtime_shared_cache_evictions_total
                .load(Ordering::Relaxed),
            purchase_validation_requests_total: self
                .purchase_validation_requests_total
                .load(Ordering::Relaxed),
            purchase_validation_failures_total: self
                .purchase_validation_failures_total
                .load(Ordering::Relaxed),
            party_owner_lease_acquire_total: self
                .party_owner_lease_acquire_total
                .load(Ordering::Relaxed),
            party_owner_lease_renew_total: self
                .party_owner_lease_renew_total
                .load(Ordering::Relaxed),
            party_owner_failover_total: self.party_owner_failover_total.load(Ordering::Relaxed),
            party_owner_stale_reject_total: self
                .party_owner_stale_reject_total
                .load(Ordering::Relaxed),
            party_owner_forward_total: self.party_owner_forward_total.load(Ordering::Relaxed),
            party_resync_total: self.party_resync_total.load(Ordering::Relaxed),
            script_readiness_state: self.script_readiness_state.load(Ordering::Relaxed),
            script_gate_rejections: ScriptGateRejectionsSnapshot {
                room_create: self.script_gate_rejection(ScriptGateSurface::RoomCreate),
                room_join: self.script_gate_rejection(ScriptGateSurface::RoomJoin),
                matchmaker_queue: self.script_gate_rejection(ScriptGateSurface::MatchmakerQueue),
                matchmaker_activate: self
                    .script_gate_rejection(ScriptGateSurface::MatchmakerActivate),
                matchmaker_accept: self.script_gate_rejection(ScriptGateSurface::MatchmakerAccept),
                live_form: self.script_gate_rejection(ScriptGateSurface::LiveForm),
                live_accept_local: self.script_gate_rejection(ScriptGateSurface::LiveAcceptLocal),
                live_accept_remote: self.script_gate_rejection(ScriptGateSurface::LiveAcceptRemote),
                live_admit_remote: self.script_gate_rejection(ScriptGateSurface::LiveAdmitRemote),
                cluster_admit_remote: self
                    .script_gate_rejection(ScriptGateSurface::ClusterAdmitRemote),
                console_list: self.script_gate_rejection(ScriptGateSurface::ConsoleList),
            },
        }
    }

    fn script_gate_rejection(&self, surface: ScriptGateSurface) -> u64 {
        self.script_gate_rejections[surface.index()].load(Ordering::Relaxed)
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
    /// Native WebSocket Ping control frames sent by the server.
    pub websocket_pings_sent_total: u64,
    /// Native WebSocket Pong control frames received by the server.
    pub websocket_pongs_received_total: u64,
    /// WebSocket sessions closed after their Pong deadline elapsed.
    pub websocket_liveness_timeouts_total: u64,
    /// Events accepted by the node-local runtime event bus.
    pub runtime_events_queued_total: u64,
    /// Events dropped by the node-local runtime event bus because a configured
    /// capacity, payload, or rate bound rejected them.
    pub runtime_events_dropped_total: u64,
    /// Entries evicted because the node-local runtime cache reached its entry bound.
    pub runtime_shared_cache_evictions_total: u64,
    /// Server-owned receipt-validation requests; aggregate-only and redacted.
    pub purchase_validation_requests_total: u64,
    /// Receipt-validation failures or deadlines; aggregate-only and redacted.
    pub purchase_validation_failures_total: u64,
    /// Durable party-owner lease acquisitions on this node.
    pub party_owner_lease_acquire_total: u64,
    /// Durable party-owner lease renewals on this node.
    pub party_owner_lease_renew_total: u64,
    /// Successful higher-generation durable party-owner recoveries.
    pub party_owner_failover_total: u64,
    /// Fenced party commands or replies rejected as stale.
    pub party_owner_stale_reject_total: u64,
    /// Party commands forwarded to a remote durable owner.
    pub party_owner_forward_total: u64,
    /// Generation-fenced party client resync transitions emitted locally.
    pub party_resync_total: u64,
    /// Current GameScript readiness state gauge value (0 when ungated).
    pub script_readiness_state: i64,
    /// Fail-closed readiness-gate rejections per enforcement surface.
    pub script_gate_rejections: ScriptGateRejectionsSnapshot,
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
        m.record_purchase_validation_request();
        m.record_purchase_validation_failure();

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
        assert_eq!(snap.purchase_validation_requests_total, 1);
        assert_eq!(snap.purchase_validation_failures_total, 1);
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
    fn party_recovery_metrics_are_redacted_monotonic_counters() {
        let m = NodeMetrics::new();
        m.record_party_owner_lease_acquire();
        m.record_party_owner_lease_renew();
        m.record_party_owner_failover();
        m.record_party_owner_stale_reject();
        m.record_party_owner_forward();
        m.record_party_resync();

        let snapshot = m.snapshot();
        assert_eq!(snapshot.party_owner_lease_acquire_total, 1);
        assert_eq!(snapshot.party_owner_lease_renew_total, 1);
        assert_eq!(snapshot.party_owner_failover_total, 1);
        assert_eq!(snapshot.party_owner_stale_reject_total, 1);
        assert_eq!(snapshot.party_owner_forward_total, 1);
        assert_eq!(snapshot.party_resync_total, 1);

        // This telemetry is aggregate-only: neither client payloads nor
        // identity-bearing member lists are represented in the snapshot.
        let value = serde_json::to_value(snapshot).expect("serializes");
        assert!(value.get("party_id").is_none());
        assert!(value.get("members").is_none());
        assert!(value.get("payload").is_none());
        assert!(value.get("token").is_none());
    }

    #[test]
    fn script_gate_surfaces_have_unique_stable_labels_and_counters() {
        // Eleven enforcement surfaces, each with a distinct label and index.
        let mut codes: Vec<&str> = ScriptGateSurface::ALL.iter().map(|s| s.code()).collect();
        codes.sort_unstable();
        let mut deduped = codes.clone();
        deduped.dedup();
        assert_eq!(codes.len(), ScriptGateSurface::COUNT);
        assert_eq!(deduped.len(), ScriptGateSurface::COUNT, "labels unique");

        let m = NodeMetrics::new();
        for surface in ScriptGateSurface::ALL {
            m.record_script_gate_rejection(surface);
        }
        m.record_script_gate_rejection(ScriptGateSurface::ClusterAdmitRemote);
        let snapshot = m.snapshot().script_gate_rejections;
        assert_eq!(snapshot.room_create, 1);
        assert_eq!(snapshot.room_join, 1);
        assert_eq!(snapshot.matchmaker_queue, 1);
        assert_eq!(snapshot.matchmaker_activate, 1);
        assert_eq!(snapshot.matchmaker_accept, 1);
        assert_eq!(snapshot.live_form, 1);
        assert_eq!(snapshot.live_accept_local, 1);
        assert_eq!(snapshot.live_accept_remote, 1);
        assert_eq!(snapshot.live_admit_remote, 1);
        assert_eq!(snapshot.cluster_admit_remote, 2, "extra rejection counted");
        assert_eq!(snapshot.console_list, 1);

        m.set_script_readiness_state(2);
        assert_eq!(m.snapshot().script_readiness_state, 2);
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
