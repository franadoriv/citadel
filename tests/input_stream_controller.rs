use citadel::realtime::{
    InputStreamController, InputStreamControllerConfig, InputStreamControllerError, ParticipantId,
};

#[test]
fn production_mint_uses_nonzero_unique_tokens_and_server_issued_stream_ids() {
    let controller = InputStreamController::new(InputStreamControllerConfig::new(2));
    let participant = ParticipantId::from_raw(7);

    let first = controller
        .mint(11, participant, 3, 5)
        .expect("production entropy mints the first lease");
    let second = controller
        .mint(12, participant, 3, 5)
        .expect("production entropy mints an independent lease");

    assert_ne!(first.token(), second.token());
    assert_ne!(first.stream_id(), second.stream_id());
    assert!(first.stream_id() < second.stream_id());
}

#[test]
fn server_issued_stream_ids_are_monotonic_across_controllers() {
    let first_controller = InputStreamController::new(InputStreamControllerConfig::new(1));
    let second_controller = InputStreamController::new(InputStreamControllerConfig::new(1));

    let first = first_controller
        .mint(11, ParticipantId::from_raw(1), 3, 5)
        .expect("first controller mints");
    let second = second_controller
        .mint(12, ParticipantId::from_raw(2), 3, 5)
        .expect("second controller mints");

    assert!(first.stream_id() < second.stream_id());
}

#[test]
fn renew_and_revoke_require_the_exact_current_lease() {
    let controller = InputStreamController::new(InputStreamControllerConfig::new(1));
    let lease = controller
        .mint(11, ParticipantId::from_raw(7), 3, 5)
        .expect("initial lease mints");

    let renewed = controller.renew(&lease).expect("current lease renews");

    assert_eq!(renewed.match_id(), lease.match_id());
    assert_eq!(renewed.stream_id(), lease.stream_id());
    assert_eq!(renewed.binding_generation(), lease.binding_generation());
    assert_eq!(renewed.clock_epoch(), lease.clock_epoch());
    assert_ne!(renewed.token(), lease.token());
    assert_eq!(
        controller.renew(&lease),
        Err(InputStreamControllerError::StaleLease)
    );
    assert_eq!(
        controller.revoke(&lease),
        Err(InputStreamControllerError::StaleLease)
    );
    assert_eq!(controller.revoke(&renewed), Ok(()));
    assert_eq!(controller.active_lease_count(), Ok(0));
}

#[test]
fn binding_and_clock_retirement_are_scoped_to_the_supplied_room() {
    let controller = InputStreamController::new(InputStreamControllerConfig::new(2));
    let first = controller
        .mint(11, ParticipantId::from_raw(1), 3, 5)
        .expect("first room mints");
    let second = controller
        .mint(12, ParticipantId::from_raw(2), 3, 5)
        .expect("second room mints");

    assert_eq!(controller.retire_binding_generation(11, 3), Ok(1));
    assert_eq!(
        controller.revoke(&first),
        Err(InputStreamControllerError::StaleLease)
    );
    assert_eq!(controller.renew(&second).map(|_| ()), Ok(()));

    let retained = controller
        .mint(11, ParticipantId::from_raw(1), 4, 5)
        .expect("retired room slot is reusable");
    assert_eq!(controller.retire_clock_epoch(12, 5), Ok(1));
    assert_eq!(
        controller.revoke(&second),
        Err(InputStreamControllerError::StaleLease)
    );
    assert_eq!(controller.revoke(&retained), Ok(()));
    assert_eq!(controller.active_lease_count(), Ok(0));
}
