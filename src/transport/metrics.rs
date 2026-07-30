//! Per-transport observability counters.
//!
//! Lightweight, dependency-free in-process counters that each transport updates
//! over its connection lifecycle. They are exposed as an atomic-backed handle
//! that can be cloned into per-connection tasks and snapshotted for logging or
//! tests. A full metrics exporter (Prometheus/OpenTelemetry) is a later task;
//! this gives observable state and a stable shape now without adding a
//! dependency.
//!
//! Counters are labelled by [`TransportKind`](crate::transport::TransportKind)
//! at the call site; the handle itself is transport-agnostic so the same type
//! serves QUIC and WebSocket.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A point-in-time snapshot of transport counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransportMetricsSnapshot {
    /// Connections accepted since start.
    pub connections_accepted: u64,
    /// Connections currently active.
    pub connections_active: u64,
    /// Envelopes received and successfully decoded.
    pub envelopes_received: u64,
    /// Envelopes sent.
    pub envelopes_sent: u64,
    /// Inbound payloads that failed to decode.
    pub decode_errors: u64,
    /// Native WebSocket Ping control frames sent.
    pub pings_sent: u64,
    /// Native WebSocket Pong control frames received.
    pub pongs_received: u64,
    /// Connections closed after a missed Pong deadline.
    pub liveness_timeouts: u64,
}

/// Shared, atomic-backed transport counters.
///
/// Clone freely; all clones share the same underlying counters.
#[derive(Debug, Clone, Default)]
pub struct TransportMetrics {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    connections_accepted: AtomicU64,
    connections_active: AtomicU64,
    envelopes_received: AtomicU64,
    envelopes_sent: AtomicU64,
    decode_errors: AtomicU64,
    pings_sent: AtomicU64,
    pongs_received: AtomicU64,
    liveness_timeouts: AtomicU64,
}

impl TransportMetrics {
    /// Create a fresh set of zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a newly accepted connection (also increments active).
    pub fn connection_opened(&self) {
        self.inner
            .connections_accepted
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .connections_active
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a closed connection (decrements active, saturating at zero).
    pub fn connection_closed(&self) {
        // Saturating decrement: never wrap below zero.
        let mut current = self.inner.connections_active.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                break;
            }
            match self.inner.connections_active.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    /// Record a successfully received+decoded envelope.
    pub fn envelope_received(&self) {
        self.inner
            .envelopes_received
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a sent envelope.
    pub fn envelope_sent(&self) {
        self.inner.envelopes_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an inbound payload that failed to decode.
    pub fn decode_error(&self) {
        self.inner.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a native Ping control frame sent.
    pub fn ping_sent(&self) {
        self.inner.pings_sent.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a native Pong control frame received.
    pub fn pong_received(&self) {
        self.inner.pongs_received.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a connection closed after its liveness deadline.
    pub fn liveness_timeout(&self) {
        self.inner.liveness_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of the current counter values.
    #[must_use]
    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            connections_accepted: self.inner.connections_accepted.load(Ordering::Relaxed),
            connections_active: self.inner.connections_active.load(Ordering::Relaxed),
            envelopes_received: self.inner.envelopes_received.load(Ordering::Relaxed),
            envelopes_sent: self.inner.envelopes_sent.load(Ordering::Relaxed),
            decode_errors: self.inner.decode_errors.load(Ordering::Relaxed),
            pings_sent: self.inner.pings_sent.load(Ordering::Relaxed),
            pongs_received: self.inner.pongs_received.load(Ordering::Relaxed),
            liveness_timeouts: self.inner.liveness_timeouts.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_open_close_and_messages() {
        let m = TransportMetrics::new();
        m.connection_opened();
        m.connection_opened();
        m.envelope_received();
        m.envelope_sent();
        m.decode_error();
        m.ping_sent();
        m.pong_received();
        m.liveness_timeout();
        m.connection_closed();

        let snap = m.snapshot();
        assert_eq!(snap.connections_accepted, 2);
        assert_eq!(snap.connections_active, 1);
        assert_eq!(snap.envelopes_received, 1);
        assert_eq!(snap.envelopes_sent, 1);
        assert_eq!(snap.decode_errors, 1);
        assert_eq!(snap.pings_sent, 1);
        assert_eq!(snap.pongs_received, 1);
        assert_eq!(snap.liveness_timeouts, 1);
    }

    #[test]
    fn active_count_saturates_at_zero() {
        let m = TransportMetrics::new();
        // More closes than opens must not underflow.
        m.connection_closed();
        m.connection_closed();
        assert_eq!(m.snapshot().connections_active, 0);
    }

    #[test]
    fn clones_share_counters() {
        let a = TransportMetrics::new();
        let b = a.clone();
        a.connection_opened();
        b.envelope_sent();
        let snap = a.snapshot();
        assert_eq!(snap.connections_accepted, 1);
        assert_eq!(snap.envelopes_sent, 1);
    }
}
