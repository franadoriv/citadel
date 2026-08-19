use citadel::authoritative_decision_telemetry::{
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
fn app_composes_a_configured_bounded_recorder() {
    let config = citadel::Config::from_toml_str(
        r#"
        [telemetry.authoritative_decisions]
        enabled = true
        capacity = 3
        "#,
    )
    .expect("telemetry configuration parses");

    let app = citadel::App::new(config);
    let recorder = app
        .authoritative_decision_recorder()
        .expect("enabled recorder is composed into the app");
    assert_eq!(recorder.capacity(), 3);
}

#[test]
fn disabled_config_does_not_compose_a_recorder() {
    let config = citadel::Config::from_toml_str(
        r#"
        [telemetry.authoritative_decisions]
        enabled = false
        "#,
    )
    .expect("telemetry configuration parses");

    assert!(
        citadel::App::new(config)
            .authoritative_decision_recorder()
            .is_none()
    );
}

#[test]
fn enabled_telemetry_rejects_a_zero_retention_bound() {
    let mut config = citadel::Config::default();
    config.telemetry.authoritative_decisions.capacity = 0;

    let error = config
        .validate()
        .expect_err("enabled telemetry must be bounded");
    assert!(
        error
            .message()
            .contains("telemetry.authoritative_decisions.capacity")
    );
}
