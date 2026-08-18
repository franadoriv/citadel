//! Application-owned Prometheus metric registry and text encoder.
//!
//! This module deliberately owns the collector registry in [`App`](crate::App),
//! rather than constructing a global registry at scrape time. Its registered
//! metric set has no unbounded labels: node-local counters and gauges are
//! snapshots, and the scrape latency histogram uses fixed buckets.

use std::fmt;
use std::time::Duration;

use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::observability::NodeMetricsSnapshot;

/// Bounded labels for HTTP request metrics. The route is normalized to a fixed
/// vocabulary before it reaches the family; callers cannot create series from a
/// concrete player ID, runtime path, query string, or request payload.
#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct HttpRequestLabels {
    method: String,
    status: String,
    route: String,
}

/// App-owned Prometheus registry and the stable metrics registered in it.
#[derive(Debug)]
pub struct PrometheusMetrics {
    registry: Registry,
    http_requests_total: Family<HttpRequestLabels, Counter>,
    realtime_connections_active: Gauge,
    realtime_sessions_active: Gauge,
    scrape_duration_seconds: Histogram,
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMetrics {
    /// Register Citadel's bounded, node-local Prometheus metrics.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("citadel");

        let http_requests_total = Family::<HttpRequestLabels, Counter>::default();
        registry.register(
            "http_requests",
            "HTTP requests served by this node.",
            http_requests_total.clone(),
        );

        let realtime_connections_active = Gauge::default();
        registry.register(
            "realtime_connections_active",
            "Open realtime connections on this node.",
            realtime_connections_active.clone(),
        );

        let realtime_sessions_active = Gauge::default();
        registry.register(
            "realtime_sessions_active",
            "Authenticated realtime sessions on this node.",
            realtime_sessions_active.clone(),
        );

        let scrape_duration_seconds =
            Histogram::new([0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]);
        registry.register(
            "metrics_scrape_duration_seconds",
            "Time spent generating the Prometheus scrape response in seconds.",
            scrape_duration_seconds.clone(),
        );

        Self {
            registry,
            http_requests_total,
            realtime_connections_active,
            realtime_sessions_active,
            scrape_duration_seconds,
        }
    }

    /// Synchronize bounded node snapshots into the registered Prometheus metrics.
    ///
    /// Node counters are monotonic for an application's lifetime. Keeping the
    /// last observed value makes repeated scrapes idempotent while preserving
    /// the Prometheus counter type.
    pub fn observe_node_snapshot(&self, snapshot: &NodeMetricsSnapshot) {
        self.realtime_connections_active
            .set(i64::try_from(snapshot.connections_active).unwrap_or(i64::MAX));
        self.realtime_sessions_active
            .set(i64::try_from(snapshot.sessions_active).unwrap_or(i64::MAX));
    }

    /// Record one public HTTP response with a bounded method/status/route set.
    pub fn observe_http_response(&self, method: &str, status: u16, route: &str) {
        let method = normalize_http_method(method);
        let status = status.to_string();
        let route = normalize_http_route(route);
        self.http_requests_total
            .get_or_create(&HttpRequestLabels {
                method,
                status,
                route,
            })
            .inc();
    }

    /// Record the duration taken to generate one scrape response.
    pub fn observe_scrape_duration(&self, duration: Duration) {
        self.scrape_duration_seconds.observe(duration.as_secs_f64());
    }

    /// Encode the current registry using `prometheus-client`'s text encoder.
    pub fn encode(&self) -> Result<String, fmt::Error> {
        let mut output = String::new();
        encode(&mut output, &self.registry)?;
        Ok(output)
    }
}

fn normalize_http_method(method: &str) -> String {
    match method {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" => method.to_owned(),
        _ => "OTHER".to_owned(),
    }
}

fn normalize_http_route(route: &str) -> String {
    // Axum matched paths are templates. Runtime-defined endpoints are collapsed:
    // scripts must never allocate Prometheus series from their route names.
    if route.starts_with("/v1/runtime/") || route.starts_with("/runtime/") {
        "runtime_endpoint".to_owned()
    } else if route.len() <= 96
        && route.starts_with('/')
        && route.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'{' | b'}')
        })
    {
        route.to_owned()
    } else {
        "other".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_metrics_encode_as_counter_gauge_and_histogram() {
        let metrics = PrometheusMetrics::new();
        metrics.observe_node_snapshot(&NodeMetricsSnapshot {
            http_requests_total: 2,
            connections_active: 1,
            connections_accepted_total: 0,
            participants_active: 0,
            sessions_active: 0,
            messages_in_total: 0,
            messages_out_total: 0,
            bytes_in_total: 0,
            bytes_out_total: 0,
            notification_live_delivered_total: 0,
            notification_live_dropped_total: 0,
            websocket_pings_sent_total: 0,
            websocket_pongs_received_total: 0,
            websocket_liveness_timeouts_total: 0,
            runtime_events_queued_total: 0,
            runtime_events_dropped_total: 0,
            runtime_shared_cache_evictions_total: 0,
            party_owner_lease_acquire_total: 0,
            party_owner_lease_renew_total: 0,
            party_owner_failover_total: 0,
            party_owner_stale_reject_total: 0,
            party_owner_forward_total: 0,
            party_resync_total: 0,
            script_readiness_state: 0,
            script_gate_rejections: Default::default(),
        });
        metrics.observe_http_response("GET", 200, "/health");
        metrics.observe_scrape_duration(Duration::from_millis(1));

        let encoded = metrics.encode().expect("encode metric registry");
        assert!(encoded.contains("# TYPE citadel_http_requests counter"));
        assert!(encoded.contains("GET"));
        assert!(encoded.contains("200"));
        assert!(encoded.contains("/health"));
        assert!(encoded.contains("# TYPE citadel_realtime_connections_active gauge"));
        assert!(encoded.contains("# TYPE citadel_metrics_scrape_duration_seconds histogram"));
    }
}
