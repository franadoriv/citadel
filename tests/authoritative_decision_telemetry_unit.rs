#![allow(dead_code)]

#[path = "../src/authoritative_decision_telemetry.rs"]
mod authoritative_decision_telemetry;

use authoritative_decision_telemetry::{
    AuthoritativeDecisionCorrelation, AuthoritativeDecisionOutcome, AuthoritativeDecisionReason,
    AuthoritativeDecisionRecorder,
};

#[test]
fn recorder_keeps_only_opaque_correlations_and_aggregate_decisions() {
    let recorder = AuthoritativeDecisionRecorder::new(2);

    recorder.record(
        AuthoritativeDecisionCorrelation::new(11, 22, 33),
        AuthoritativeDecisionOutcome::Accepted,
        AuthoritativeDecisionReason::NotApplicable,
    );

    let records = recorder.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].correlation.match_id, 11);
    assert_eq!(records[0].correlation.batch_id, 22);
    assert_eq!(records[0].correlation.event_id, 33);
    assert_eq!(records[0].outcome, AuthoritativeDecisionOutcome::Accepted);
    assert_eq!(
        records[0].reason,
        AuthoritativeDecisionReason::NotApplicable
    );

    let metrics = recorder.metrics();
    assert_eq!(metrics.retained, 1);
    assert_eq!(metrics.recorded_total, 1);
    assert_eq!(metrics.accepted_total, 1);
    assert_eq!(metrics.rejected_total, 0);
    assert_eq!(metrics.corrected_total, 0);
    assert_eq!(metrics.evicted_total, 0);
}

#[test]
fn recorder_evicts_oldest_record_first_and_keeps_aggregate_totals() {
    let recorder = AuthoritativeDecisionRecorder::new(2);

    recorder.record(
        AuthoritativeDecisionCorrelation::new(1, 1, 1),
        AuthoritativeDecisionOutcome::Accepted,
        AuthoritativeDecisionReason::NotApplicable,
    );
    recorder.record(
        AuthoritativeDecisionCorrelation::new(1, 1, 2),
        AuthoritativeDecisionOutcome::Rejected,
        AuthoritativeDecisionReason::OpaqueCode(9),
    );
    recorder.record(
        AuthoritativeDecisionCorrelation::new(1, 1, 3),
        AuthoritativeDecisionOutcome::Corrected,
        AuthoritativeDecisionReason::NotApplicable,
    );

    let records = recorder.records();
    assert_eq!(
        records
            .iter()
            .map(|record| record.correlation.event_id)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        records[0].reason,
        AuthoritativeDecisionReason::OpaqueCode(9)
    );
    assert_eq!(AuthoritativeDecisionOutcome::Accepted.code(), "accepted");
    assert_eq!(AuthoritativeDecisionOutcome::Rejected.code(), "rejected");
    assert_eq!(AuthoritativeDecisionOutcome::Corrected.code(), "corrected");
    assert_eq!(
        AuthoritativeDecisionReason::OpaqueCode(9).code(),
        "opaque_code"
    );

    let metrics = recorder.metrics();
    assert_eq!(metrics.retained, 2);
    assert_eq!(metrics.recorded_total, 3);
    assert_eq!(metrics.accepted_total, 1);
    assert_eq!(metrics.rejected_total, 1);
    assert_eq!(metrics.corrected_total, 1);
    assert_eq!(metrics.evicted_total, 1);
}
