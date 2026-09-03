//! Durable persistence for closed authoritative telemetry slices.
//!
//! This is the concrete [`TelemetrySliceSink`] and the one place that resolves a
//! slice's process-local correlation to a durable match identity. It lives
//! outside `crate::authoritative_telemetry_slices` because
//! `tests/authoritative_telemetry_slices_unit.rs` compiles that module
//! standalone through `#[path]`, resolving `crate::` against the test crate
//! root: a single crate import there breaks the test binary.
//!
//! What reaches the table is what a closed report already is — aggregates. The
//! marker text a slice validated is counted and discarded before this sink ever
//! sees a report, and there is no column for it. Payloads, identities, replies,
//! script commands, corrected values, and the recorder's decision sequence are
//! likewise absent by construction rather than by filtering.

use std::sync::Arc;

use crate::authoritative_telemetry_slices::{
    ClosedTelemetrySliceReport, TelemetrySliceService, TelemetrySliceSink,
};
use crate::durable_logs::DurableLogWriter;
use crate::ids::NodeIdentity;
use crate::match_recorder::MatchRecorder;
use crate::realtime::RoomId;
use crate::repository::DurableSliceRow;

/// The one context class whose correlation is a room id.
///
/// Mirrors `TelemetrySliceContext::kind_code`. A `scope` correlation is an
/// unrelated server-owned number living in the same `u64` space, so resolving it
/// through the room directory would attribute a slice to a match it never ran in.
const MATCH_CONTEXT_KIND: &str = "match";

/// Writes closed slice reports to the durable log queue, resolving each match
/// context to the server-minted match id its room is playing.
#[derive(Debug)]
pub struct DurableTelemetrySliceSink {
    writer: Arc<DurableLogWriter>,
    directory: Option<Arc<MatchRecorder>>,
}

impl DurableTelemetrySliceSink {
    /// A sink with no room directory: every row is stored with `match_id` NULL.
    ///
    /// That is a supported state, not a degraded one. A game with no match
    /// concept never has a room to resolve, and a slice closed outside a match
    /// is stored unscoped rather than rejected.
    #[must_use]
    pub fn new(writer: Arc<DurableLogWriter>) -> Self {
        Self {
            writer,
            directory: None,
        }
    }

    /// Resolve match contexts through `directory` while their rooms are live.
    #[must_use]
    pub fn with_directory(mut self, directory: Arc<MatchRecorder>) -> Self {
        self.directory = Some(directory);
        self
    }

    /// The durable row for one closed report.
    fn row(&self, report: &ClosedTelemetrySliceReport, correlation: u64) -> DurableSliceRow {
        DurableSliceRow {
            report_id: report.report_id.clone(),
            node_id: self.writer.identity().node_id().to_string(),
            match_id: self.match_id_for(report, correlation),
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

    fn match_id_for(
        &self,
        report: &ClosedTelemetrySliceReport,
        correlation: u64,
    ) -> Option<String> {
        if report.context_kind != MATCH_CONTEXT_KIND {
            return None;
        }
        let room_id: RoomId = correlation;
        self.directory.as_ref()?.match_id_of(room_id)
    }
}

impl TelemetrySliceSink for DurableTelemetrySliceSink {
    fn publish(&self, report: &ClosedTelemetrySliceReport, correlation: u64) {
        // Runs under the slice service's state lock: build the row, read the
        // directory, hand it to a bounded queue, return. No await, no database.
        self.writer.enqueue_slice(self.row(report, correlation));
    }
}

/// Give a freshly built slice service its node-unique id salt and, when a
/// durable store is attached, its persistence sink.
///
/// The salt is applied whether or not a store is attached: a report id must
/// already be unique across reboots and nodes when an operator reads it, and the
/// id an in-process report carries is the id its row will be keyed by.
#[must_use]
pub fn attach_durable_sink(
    service: TelemetrySliceService,
    identity: &NodeIdentity,
    writer: Option<&Arc<DurableLogWriter>>,
    directory: Option<&Arc<MatchRecorder>>,
) -> TelemetrySliceService {
    let service = service.with_identity(identity.salt());
    let Some(writer) = writer else {
        return service;
    };
    let mut sink = DurableTelemetrySliceSink::new(Arc::clone(writer));
    if let Some(directory) = directory {
        sink = sink.with_directory(Arc::clone(directory));
    }
    service.with_sink(Arc::new(sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authoritative_decision_telemetry::AuthoritativeDecisionRecorder;
    use crate::authoritative_telemetry_slices::{TelemetrySliceContext, TelemetrySlicePolicy};
    use crate::config::LogsConfig;

    fn writer() -> Arc<DurableLogWriter> {
        Arc::new(DurableLogWriter::new(
            Arc::new(NodeIdentity::new("node-a")),
            LogsConfig::default(),
        ))
    }

    fn report(context_kind: &'static str) -> ClosedTelemetrySliceReport {
        ClosedTelemetrySliceReport {
            // 13 hex of close time, 4 of node salt, 12 of per-boot sequence.
            report_id: "ats1-00000000000010001000000000001".to_string(),
            context_kind,
            close_reason: "finished",
            closed_at_ms: 1_700_000_000_000,
            duration_ms: 250,
            marker_total: 4,
            truncated: false,
            accepted_total: 9,
            rejected_total: 2,
            corrected_total: 1,
        }
    }

    #[test]
    fn a_match_slice_is_stored_against_the_room_s_durable_match() {
        let writer = writer();
        let recorder = Arc::new(MatchRecorder::new(Arc::clone(&writer)));
        recorder.bind(7, "mt1-a".to_string());
        let sink = DurableTelemetrySliceSink::new(Arc::clone(&writer)).with_directory(recorder);
        let row = sink.row(&report("match"), 7);
        assert_eq!(row.match_id.as_deref(), Some("mt1-a"));
        assert_eq!(row.node_id, "node-a");
        assert_eq!(row.context_kind, "match");
        assert_eq!(row.close_reason, "finished");
        assert_eq!(row.marker_total, 4);
        assert_eq!(row.accepted_total, 9);
    }

    #[test]
    fn a_scope_correlation_is_never_looked_up_as_a_room() {
        let writer = writer();
        let recorder = Arc::new(MatchRecorder::new(Arc::clone(&writer)));
        // The same number is a live room and, separately, a scope correlation.
        recorder.bind(7, "mt1-a".to_string());
        let sink = DurableTelemetrySliceSink::new(Arc::clone(&writer)).with_directory(recorder);
        assert_eq!(sink.row(&report("scope"), 7).match_id, None);
    }

    #[test]
    fn a_slice_outside_any_match_is_stored_unscoped_rather_than_dropped() {
        let writer = writer();
        let sink = DurableTelemetrySliceSink::new(Arc::clone(&writer));
        // No directory at all, and a match context with no live room.
        assert_eq!(sink.row(&report("match"), 7).match_id, None);
        sink.publish(&report("match"), 7);
        assert_eq!(writer.queued_total(), 1);
        assert_eq!(writer.dropped_total(), 0);
    }

    #[test]
    fn a_persisted_row_carries_counts_only_and_never_marker_text() {
        let sink = DurableTelemetrySliceSink::new(writer());
        let row = sink.row(&report("match"), 7);
        let json = serde_json::to_value(&row).expect("a slice row serializes");
        let object = json.as_object().expect("a slice row is a JSON object");
        for forbidden in [
            "marker",
            "markers",
            "marker_text",
            "payload",
            "correlation",
            "room_id",
            "sequence",
            "participants",
            "account_id",
        ] {
            assert!(
                !object.contains_key(forbidden),
                "a durable slice report must never retain {forbidden}"
            );
        }
        assert_eq!(
            object
                .get("marker_total")
                .and_then(serde_json::Value::as_u64),
            Some(4),
            "the marker count is the only trace a marker leaves"
        );
    }

    #[test]
    fn closing_a_slice_queues_exactly_one_row_per_report() {
        let writer = writer();
        let recorder = Arc::new(MatchRecorder::new(Arc::clone(&writer)));
        recorder.bind(5, "mt1-b".to_string());
        let service = attach_durable_sink(
            TelemetrySliceService::new(
                Arc::new(AuthoritativeDecisionRecorder::new(8)),
                TelemetrySlicePolicy::new(2, 2, 10_000, 8).expect("bounded policy"),
            ),
            &NodeIdentity::new("node-a"),
            Some(&writer),
            Some(&recorder),
        );
        let context = TelemetrySliceContext::match_context(5);
        service.begin(context, 100).expect("begin");
        assert_eq!(writer.queued_total(), 0, "an open slice writes nothing");
        let closed = service.finish(context, 400).expect("finish");
        assert_eq!(closed.duration_ms, 300);
        assert_eq!(writer.queued_total(), 1);
    }

    #[test]
    fn a_service_without_a_durable_store_still_mints_salted_ids() {
        let identity = NodeIdentity::new("node-a");
        let service = attach_durable_sink(
            TelemetrySliceService::new(
                Arc::new(AuthoritativeDecisionRecorder::new(8)),
                TelemetrySlicePolicy::new(2, 2, 10_000, 8).expect("bounded policy"),
            ),
            &identity,
            None,
            None,
        );
        let context = TelemetrySliceContext::scope_context(3);
        service.begin(context, 1).expect("begin");
        let closed = service.finish(context, 2).expect("finish");
        // `ats1-` + 13 hex of close time puts the salt at bytes 18..22.
        let salt_hex = format!("{:04x}", identity.salt());
        assert_eq!(
            &closed.report_id[18..22],
            salt_hex.as_str(),
            "the salt is applied even with nothing to persist to"
        );
    }
}
