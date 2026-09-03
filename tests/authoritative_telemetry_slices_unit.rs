#![allow(dead_code)]

#[path = "../src/authoritative_decision_telemetry.rs"]
mod authoritative_decision_telemetry;
#[path = "../src/authoritative_telemetry_slices.rs"]
mod authoritative_telemetry_slices;

use authoritative_decision_telemetry::{
    AuthoritativeDecisionCorrelation, AuthoritativeDecisionOutcome, AuthoritativeDecisionReason,
    AuthoritativeDecisionRecorder,
};
use authoritative_telemetry_slices::{
    ClosedTelemetrySliceReport, RuntimeScopeGuard, TelemetrySliceContext, TelemetrySlicePolicy,
    TelemetrySliceService, TelemetrySliceSink, active_runtime_context, set_active_runtime_scope,
    valid_report_id,
};
use std::sync::{Arc, Mutex};

/// A durable sink stands in for `crate::telemetry_slice_persistence`, which this
/// binary cannot reach: the module under test is compiled standalone through
/// `#[path]`, so `crate::` resolves to this test binary and not to the server.
#[derive(Debug, Default)]
struct RecordingSink {
    published: Mutex<Vec<(String, &'static str, u64)>>,
}

impl RecordingSink {
    fn published(&self) -> Vec<(String, &'static str, u64)> {
        self.published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TelemetrySliceSink for RecordingSink {
    fn publish(&self, report: &ClosedTelemetrySliceReport, correlation: u64) {
        self.published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((report.report_id.clone(), report.close_reason, correlation));
    }
}

#[test]
fn closed_slice_is_redacted_and_derived_from_its_context_decisions() {
    let recorder = Arc::new(AuthoritativeDecisionRecorder::new(16));
    let service = TelemetrySliceService::new(
        Arc::clone(&recorder),
        TelemetrySlicePolicy::new(2, 2, 10_000, 8).expect("bounded policy"),
    );
    let context = TelemetrySliceContext::match_context(7);
    recorder.record(
        AuthoritativeDecisionCorrelation::new(7, 98, 100),
        AuthoritativeDecisionOutcome::Rejected,
        AuthoritativeDecisionReason::OpaqueCode(8),
    );
    service
        .begin(context, 100)
        .expect("context-derived slice begins");
    service
        .mark(context, "core.phase", 150)
        .expect("namespaced marker is accepted");
    recorder.record(
        AuthoritativeDecisionCorrelation::new(7, 99, 101),
        AuthoritativeDecisionOutcome::Accepted,
        AuthoritativeDecisionReason::NotApplicable,
    );
    recorder.record(
        AuthoritativeDecisionCorrelation::new(8, 99, 102),
        AuthoritativeDecisionOutcome::Rejected,
        AuthoritativeDecisionReason::OpaqueCode(9),
    );
    let report = service.finish(context, 200).expect("slice closes");
    assert_eq!(report.context_kind, "match");
    assert_eq!(report.close_reason, "finished");
    assert_eq!(report.duration_ms, 100);
    assert_eq!(report.marker_total, 1);
    assert!(!report.truncated);
    assert_eq!(report.accepted_total, 1);
    assert_eq!(report.rejected_total, 0);
    assert_eq!(report.corrected_total, 0);
}

#[test]
fn ttl_and_marker_cap_close_without_callers_supplying_report_ids() {
    let recorder = Arc::new(AuthoritativeDecisionRecorder::new(16));
    let service = TelemetrySliceService::new(
        recorder,
        TelemetrySlicePolicy::new(1, 1, 10, 8).expect("bounded policy"),
    );
    let first = TelemetrySliceContext::scope_context(20);
    let second = TelemetrySliceContext::scope_context(21);
    service.begin(first, 100).expect("first begins");
    assert!(service.mark(first, "core.expired", 110).is_err());
    assert_eq!(service.list_closed(8)[0].close_reason, "ttl");
    service.begin(first, 120).expect("first starts again");
    service.mark(first, "core.one", 121).expect("first marker");
    let cap = service
        .mark(first, "core.two", 122)
        .expect("marker cap is processed")
        .expect("marker cap closes");
    assert_eq!(cap.close_reason, "marker_cap");
    service.begin(first, 130).expect("first starts again");
    service
        .begin(second, 131)
        .expect("active capacity closes oldest");
    let reports = service.list_closed(8);
    assert!(
        reports
            .iter()
            .any(|report| report.close_reason == "active_cap")
    );
    assert!(
        reports
            .iter()
            .all(|report| report.report_id.starts_with("ats1-"))
    );
}

#[test]
fn contexts_and_markers_reject_unbounded_or_non_namespaced_input() {
    assert!(TelemetrySliceContext::new("match", 1).is_ok());
    assert!(TelemetrySliceContext::new("unknown", 1).is_err());
    assert!(TelemetrySlicePolicy::new(1, 1, u64::MAX, 1).is_err());
    let service = TelemetrySliceService::new(
        Arc::new(AuthoritativeDecisionRecorder::new(1)),
        TelemetrySlicePolicy::new(1, 1, 10, 1).expect("policy"),
    );
    let context = TelemetrySliceContext::scope_context(1);
    service.begin(context, 0).expect("begin");
    for marker in ["plain", "Core.phase", "core.", "core.a..b"] {
        assert!(
            service.mark(context, marker, 1).is_err(),
            "must reject {marker}"
        );
    }
}

#[test]
fn recorder_eviction_marks_closed_slice_as_truncated() {
    let recorder = Arc::new(AuthoritativeDecisionRecorder::new(1));
    let service = TelemetrySliceService::new(
        Arc::clone(&recorder),
        TelemetrySlicePolicy::new(1, 1, 10_000, 1).expect("policy"),
    );
    let context = TelemetrySliceContext::match_context(5);
    service.begin(context, 1).expect("begin");
    recorder.record(
        AuthoritativeDecisionCorrelation::new(5, 1, 1),
        AuthoritativeDecisionOutcome::Accepted,
        AuthoritativeDecisionReason::NotApplicable,
    );
    recorder.record(
        AuthoritativeDecisionCorrelation::new(5, 1, 2),
        AuthoritativeDecisionOutcome::Rejected,
        AuthoritativeDecisionReason::OpaqueCode(3),
    );
    let report = service.finish(context, 2).expect("finish");
    assert!(
        report.truncated,
        "eviction after begin is explicit in the report"
    );
    assert_eq!(report.accepted_total, 0);
    assert_eq!(report.rejected_total, 1);
}

#[test]
fn active_runtime_scope_is_server_set_and_can_be_cleared() {
    set_active_runtime_scope(None);
    assert!(active_runtime_context().is_none());
    set_active_runtime_scope(Some(42));
    assert_eq!(
        active_runtime_context(),
        Some(TelemetrySliceContext::match_context(42))
    );
    set_active_runtime_scope(None);
    assert!(active_runtime_context().is_none());
}

#[test]
#[allow(clippy::panic)]
fn runtime_scope_guard_overrides_and_restores_a_prior_scope() {
    set_active_runtime_scope(Some(41));
    {
        let _guard = RuntimeScopeGuard::enter(Some(42));
        assert_eq!(
            active_runtime_context(),
            Some(TelemetrySliceContext::match_context(42)),
            "a native lifecycle callback must not inherit the prior match"
        );
    }
    assert_eq!(
        active_runtime_context(),
        Some(TelemetrySliceContext::match_context(41)),
        "the previous scope is restored after the callback"
    );
    let panic = std::panic::catch_unwind(|| {
        let _guard = RuntimeScopeGuard::enter(Some(43));
        panic!("simulated lifecycle failure");
    });
    assert!(panic.is_err());
    assert_eq!(
        active_runtime_context(),
        Some(TelemetrySliceContext::match_context(41)),
        "unwinding a lifecycle callback must restore the previous scope"
    );
    set_active_runtime_scope(None);
}

#[test]
fn report_ids_are_salted_so_two_nodes_never_mint_the_same_id() {
    let policy = TelemetrySlicePolicy::new(1, 1, 10_000, 4).expect("bounded policy");
    let mint = |salt: u16| {
        let service =
            TelemetrySliceService::new(Arc::new(AuthoritativeDecisionRecorder::new(1)), policy)
                .with_identity(salt);
        let context = TelemetrySliceContext::scope_context(1);
        service.begin(context, 1_700_000_000_000).expect("begin");
        service
            .finish(context, 1_700_000_000_100)
            .expect("finish")
            .report_id
    };
    // Two boots of two nodes at the same millisecond, each on its first report:
    // every component of the id is identical except the salt.
    let first = mint(0x0001);
    let second = mint(0xfffe);
    assert_ne!(first, second, "a shared close time must not mint one id");
    assert_eq!(first.len(), 34);
    assert!(first.starts_with("ats1-"));
    assert_eq!(
        &first[5..18],
        &second[5..18],
        "close time leads, so lexicographic order stays chronological"
    );
    assert_eq!(&first[18..22], "0001");
    assert_eq!(&second[18..22], "fffe");
    assert_eq!(
        &first[22..],
        &second[22..],
        "the per-boot sequence restarts at the same value, which is why the salt exists"
    );
    assert!(valid_report_id(&first));
    // The pre-salt shape was `ats1-` plus 24 hex digits and must no longer pass.
    assert!(!valid_report_id("ats1-000000000000000000000000"));
}

#[test]
fn every_close_reaches_the_durable_sink_with_its_private_correlation() {
    let sink = Arc::new(RecordingSink::default());
    let service = TelemetrySliceService::new(
        Arc::new(AuthoritativeDecisionRecorder::new(4)),
        TelemetrySlicePolicy::new(1, 1, 50, 4).expect("bounded policy"),
    )
    .with_identity(0xbeef)
    .with_sink(Arc::clone(&sink) as Arc<dyn TelemetrySliceSink>);
    let context = TelemetrySliceContext::match_context(9);
    service.begin(context, 10).expect("begin");
    assert!(
        sink.published().is_empty(),
        "an open slice is in-process state and must publish nothing"
    );
    let finished = service.finish(context, 20).expect("finish");
    // A server-closed slice publishes too: the reaper is the only thing that
    // closes an abandoned match, and its reports are the ones worth keeping.
    service.begin(context, 30).expect("begin again");
    service.reap(200);
    let published = sink.published();
    assert_eq!(published.len(), 2);
    assert_eq!(published[0].0, finished.report_id);
    assert_eq!(published[0].1, "finished");
    assert_eq!(published[1].1, "ttl");
    for (_, _, correlation) in &published {
        assert_eq!(
            *correlation, 9,
            "the sink is the only place a slice's correlation is visible"
        );
    }
    assert!(
        service
            .list_closed(8)
            .iter()
            .all(|report| report.report_id.starts_with("ats1-")),
        "publishing does not change what an operator reads"
    );
}
