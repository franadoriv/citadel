//! MongoDB durable contract for current-account credential unlinking.
//!
//! Requires a transaction-capable replica set or sharded MongoDB deployment:
//! `CITADEL_TEST_MONGODB_URL='mongodb://localhost:27017/citadel_test?replicaSet=rs0' \
//! cargo test --test mongodb_identity_unlink_repository -- --nocapture`

use citadel::config::DatabaseConfig;
use citadel::identity::{
    AuthCredential, AuthIdentity, DeviceId, EmailAddress, PasswordVerifier, UserId,
};
use citadel::repository::{Backend, MongoDatabase, UnitOfWork, UnlinkResult};
use citadel::time::TimestampMillis;
use futures_util::TryStreamExt;
use mongodb::bson::{Document, doc};

fn test_database_url() -> Option<String> {
    std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

async fn connect() -> Option<MongoDatabase> {
    let url = test_database_url()?;
    Some(
        MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect transaction-capable MongoDB"),
    )
}

fn email_identity(credential: AuthCredential, user_id: UserId) -> AuthIdentity {
    AuthIdentity::new(
        credential,
        user_id,
        TimestampMillis::from_unix_millis(1),
        TimestampMillis::from_unix_millis(1),
    )
    .expect("email identity")
    .with_password_verifier(PasswordVerifier::new("test-verifier".to_owned()).expect("verifier"))
    .expect("attach verifier")
}

fn device_identity(credential: AuthCredential, user_id: UserId) -> AuthIdentity {
    AuthIdentity::new(
        credential,
        user_id,
        TimestampMillis::from_unix_millis(1),
        TimestampMillis::from_unix_millis(1),
    )
    .expect("device identity")
}

#[tokio::test]
async fn mongodb_scoped_unlink_is_atomic_and_stages_only_a_redacted_outbox_event() {
    let Some(db) = connect().await else {
        eprintln!("skipping MongoDB scoped unlink contract: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_identity_session_data_for_tests()
        .await
        .expect("reset identity projections and outbox");

    let owner = UserId::new("unlink-owner").expect("owner");
    let other = UserId::new("unlink-other").expect("other");
    let removed = AuthCredential::Email(EmailAddress::new("unlink@example.test").expect("email"));
    let retained = AuthCredential::Device(DeviceId::new("unlink-device").expect("device"));
    let repo = db.auth_identity_repository();
    repo.link_auth_identity(email_identity(removed.clone(), owner.clone()))
        .await
        .expect("link email identity");
    repo.link_auth_identity(device_identity(retained, owner.clone()))
        .await
        .expect("link retained device identity");

    assert_eq!(
        repo.unlink_auth_identity_for_user(&other, &removed)
            .await
            .expect("other user attempt"),
        UnlinkResult::NotOwned,
        "a credential owned by another account must remain opaque"
    );
    let outbox = db
        .database_for_tests()
        .collection::<Document>("identity_change_outbox");
    let transaction = db.begin().await.expect("begin scoped-unlink transaction");
    assert_eq!(
        transaction
            .auth_identity_repository()
            .unlink_auth_identity_for_user(&owner, &removed)
            .await
            .expect("unlink within transaction"),
        UnlinkResult::Unlinked
    );
    transaction
        .rollback()
        .await
        .expect("roll back scoped unlink");
    assert!(
        repo.get_auth_identity(&removed)
            .await
            .expect("read identity after rollback")
            .is_some(),
        "the identity and its verifier roll back with the outbox event"
    );
    assert_eq!(
        outbox
            .count_documents(doc! { "user_id": owner.as_str() })
            .await
            .expect("count rolled-back outbox events"),
        0,
        "rollback must not leak an identity-change event"
    );

    assert_eq!(
        repo.unlink_auth_identity_for_user(&owner, &removed)
            .await
            .expect("unlink email identity"),
        UnlinkResult::Unlinked
    );
    assert!(
        repo.get_auth_identity(&removed)
            .await
            .expect("read deleted identity")
            .is_none(),
        "the email identity and its verifier must be deleted"
    );
    assert_eq!(
        repo.unlink_auth_identity_for_user(&owner, &removed)
            .await
            .expect("idempotent retry"),
        UnlinkResult::NotOwned,
        "a retry must not stage a second event"
    );

    let outbox = db
        .database_for_tests()
        .collection::<Document>("identity_change_outbox");
    let events: Vec<Document> = outbox
        .find(doc! { "user_id": owner.as_str() })
        .await
        .expect("read identity-change outbox")
        .try_collect()
        .await
        .expect("collect identity-change outbox");
    assert_eq!(events.len(), 1, "only the successful unlink is recorded");
    let event = &events[0];
    assert_eq!(
        event.get_str("event_type").expect("event type"),
        "credential_unlinked"
    );
    assert_eq!(event.get_str("provider").expect("provider"), "email");
    assert_eq!(
        event
            .get_str("external_id_redacted")
            .expect("redacted external id"),
        "[redacted]"
    );
    assert!(
        event.get("password_verifier").is_none()
            || event
                .get("password_verifier")
                .is_some_and(|v| v.as_null().is_some()),
        "outbox must never retain a password verifier"
    );
    assert!(
        !event.contains_key("external_id"),
        "outbox must never retain the credential identifier"
    );

    assert_eq!(
        repo.unlink_auth_identity_for_user(
            &owner,
            &AuthCredential::Device(DeviceId::new("unlink-device").expect("device"))
        )
        .await
        .expect("last credential attempt"),
        UnlinkResult::LastCredential,
        "the last authentication credential is protected"
    );
}
