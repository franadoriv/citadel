//! Contract tests for the identity and session domain types.
//!
//! These exercise the lifecycle invariants and state machine through the public
//! API only, with fixed timestamps so they are fully deterministic. They are
//! written to remain valid against any future concrete `SessionService` /
//! repository implementation, which must preserve these same domain rules.

use citadel::error::ErrorCategory;
use citadel::identity::{
    AccountState, AuthCredential, AuthIdentity, DeviceId, User, UserMetadata, Username,
};
use citadel::session::{
    NodeId, OwnershipGeneration, RevocationReason, Session, SessionId, SessionInvalidity,
    SessionOwnerLease, SessionState, SessionStateKind, SessionValidation,
};
use citadel::storage::UserId;
use citadel::time::TimestampMillis;
use serde_json::json;

fn uid() -> UserId {
    UserId::new("u-1").expect("valid user id")
}

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn active_session() -> Session {
    Session::new(
        SessionId::new("sess-1").expect("session id"),
        uid(),
        NodeId::new("node-a").expect("node id"),
        ts(1_000),
        ts(2_000),
        Some(ts(4_000)),
        None,
    )
    .expect("valid session")
}

#[test]
fn user_account_lifecycle_gates_authentication() {
    let created = ts(500);
    let user = User::new(
        uid(),
        Username::new("player-1").expect("username"),
        None,
        Some(UserMetadata::new(json!({"level": 1})).expect("metadata")),
        created,
        created,
        AccountState::Active,
    )
    .expect("valid user");
    assert!(user.is_active());
    assert!(user.ensure_authenticatable().is_ok());

    let disabled = User::new(
        uid(),
        Username::new("player-1").expect("username"),
        None,
        None,
        created,
        created,
        AccountState::Disabled,
    )
    .expect("valid user");
    let err = disabled
        .ensure_authenticatable()
        .expect_err("disabled account cannot authenticate");
    assert_eq!(err.category(), ErrorCategory::Auth);
}

#[test]
fn auth_identity_maps_credential_to_account() {
    let credential = AuthCredential::Device(DeviceId::new("device-1").expect("device"));
    let identity =
        AuthIdentity::new(credential.clone(), uid(), ts(10), ts(10)).expect("valid identity");
    assert_eq!(identity.user_id, uid());
    assert_eq!(identity.credential, credential);
}

#[test]
fn active_session_validates_until_expiry() {
    let session = active_session();
    // Before expiry: valid with sanitized routing facts and no secret material.
    let validation = session.validate_at(ts(1_500));
    assert!(validation.is_valid());
    if let SessionValidation::Valid(validated) = validation {
        assert_eq!(validated.user_id, uid());
        assert_eq!(validated.owner_node.as_str(), "node-a");
        assert_eq!(validated.expires_at, ts(2_000));
    }
    // At/after expiry: invalid even though the state has not been materialized.
    assert_eq!(
        session.validate_at(ts(2_000)).invalidity(),
        Some(SessionInvalidity::Expired)
    );
}

#[test]
fn refresh_extends_within_window_but_not_after() {
    let mut session = active_session();
    // Access token lapsed (2_500) but refresh window (<4_000) is still open.
    assert!(session.can_refresh_at(ts(2_500)));
    session
        .refresh_at(ts(2_500), ts(6_000), Some(ts(9_000)), None)
        .expect("refresh within window");
    assert_eq!(session.state_kind(), SessionStateKind::Active);
    assert!(session.validate_at(ts(3_000)).is_valid());

    // A second session whose refresh window has fully closed cannot refresh.
    let mut closed = active_session();
    assert!(!closed.can_refresh_at(ts(4_000)));
    let err = closed
        .refresh_at(ts(4_000), ts(8_000), None, None)
        .expect_err("refresh after window is a conflict");
    assert_eq!(err.category(), ErrorCategory::Conflict);
}

#[test]
fn revoked_and_expired_are_terminal_and_distinguishable() {
    let mut revoked = active_session();
    revoked
        .revoke_at(ts(1_200), RevocationReason::Logout)
        .expect("revoke active session");
    assert_eq!(
        revoked.validate_at(ts(1_200)).invalidity(),
        Some(SessionInvalidity::Revoked)
    );

    let mut expired = active_session();
    expired.expire_at(ts(2_500)).expect("expire past boundary");
    assert_eq!(
        expired.validate_at(ts(2_500)).invalidity(),
        Some(SessionInvalidity::Expired)
    );

    // Terminal sessions reject every further transition.
    assert_eq!(
        revoked
            .expire_at(ts(2_500))
            .expect_err("expire revoked")
            .category(),
        ErrorCategory::Conflict
    );
    assert_eq!(
        expired
            .revoke_at(ts(2_600), RevocationReason::Admin)
            .expect_err("revoke expired")
            .category(),
        ErrorCategory::Conflict
    );
}

#[test]
fn session_state_round_trips_through_serde() {
    let mut session = active_session();
    session
        .revoke_at(ts(1_500), RevocationReason::Security)
        .expect("revoke");
    let encoded = serde_json::to_string(&session).expect("serialize session");
    let decoded: Session = serde_json::from_str(&encoded).expect("deserialize session");
    assert_eq!(decoded, session);
    assert!(matches!(decoded.state(), SessionState::Revoked { .. }));
}

#[test]
fn ownership_lease_currency_follows_expiry() {
    let lease = SessionOwnerLease {
        node_id: NodeId::new("node-a").expect("node id"),
        generation: OwnershipGeneration::new(3),
        expires_at: ts(5_000),
    };
    assert!(lease.is_current_at(ts(4_999)));
    assert!(!lease.is_current_at(ts(5_000)));
    assert!(OwnershipGeneration::new(4) > lease.generation);
}
