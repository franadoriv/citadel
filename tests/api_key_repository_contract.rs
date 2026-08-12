//! Shared API-key persistence contract.
//!
//! The reference and embedded SQLite adapters always run. PostgreSQL and
//! CockroachDB run when `DATABASE_URL`/`CITADEL_TEST_DATABASE_URL` selects the
//! corresponding PostgreSQL-wire backend, and MongoDB runs when
//! `CITADEL_TEST_MONGODB_URL` is set.

use std::sync::Arc;

use citadel::config::DatabaseConfig;
use citadel::database_explorer::TableRef;
use citadel::error::ErrorCategory;
use citadel::repository::{
    ApiKeyRepository, Backend, InMemoryApiKeyRepository, MongoDatabase, PgDatabase, SqliteDatabase,
};
use citadel::services::{ApiKeyScope, ApiKeyService, CreateApiKeyRequest};
use citadel::time::TimestampMillis;

fn at(millis: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(millis)
}

async fn create_key(
    service: &ApiKeyService,
    name: &str,
    now: u64,
    expires_at: Option<u64>,
) -> citadel::services::ApiKeySecretResponse {
    service
        .create(
            CreateApiKeyRequest {
                name: name.to_owned(),
                scopes: vec![ApiKeyScope::TelemetryRead, ApiKeyScope::AuditRead],
                expires_at: expires_at.map(at),
            },
            at(now),
        )
        .await
        .expect("create API key")
}

async fn contract(repo: Arc<dyn ApiKeyRepository>) {
    let service = ApiKeyService::new(Arc::clone(&repo));

    let first = create_key(&service, "metrics-reader", 1_000, None).await;
    assert!(first.secret.starts_with("ctdl_k1_"));
    assert!(!format!("{first:?}").contains(&first.secret));

    let stored = repo
        .get(&first.key.id)
        .await
        .expect("get API key")
        .expect("stored API key");
    assert_eq!(stored.name, "metrics-reader");
    assert_eq!(stored.generation, 1);
    let debug = format!("{stored:?}");
    assert!(!debug.contains("verifier"));
    assert!(!debug.contains("[1, 1, 1"));

    let metadata_json = serde_json::to_value(
        service
            .get(&first.key.id)
            .await
            .expect("get metadata")
            .expect("metadata exists"),
    )
    .expect("serialize metadata");
    assert!(metadata_json.get("verifier").is_none());

    let duplicate = repo
        .create(stored.clone())
        .await
        .expect_err("duplicate id must conflict");
    assert_eq!(duplicate.category(), ErrorCategory::Conflict);

    let second = create_key(&service, "audit-reader", 2_000, None).await;

    let before_pre_creation_mutations = repo
        .get(&second.key.id)
        .await
        .expect("get key before pre-creation mutations")
        .expect("key exists before pre-creation mutations");
    let rotate_before_creation = repo
        .rotate(
            &second.key.id,
            second.key.generation,
            before_pre_creation_mutations.verifier.clone(),
            at(1_999),
        )
        .await
        .expect_err("rotation before creation must conflict");
    assert_eq!(rotate_before_creation.category(), ErrorCategory::Conflict);
    assert_eq!(
        repo.get(&second.key.id)
            .await
            .expect("get after rejected rotate"),
        Some(before_pre_creation_mutations.clone()),
        "rejected pre-creation rotation must not mutate the key"
    );

    let revoke_before_creation = repo
        .revoke(&second.key.id, second.key.generation, at(1_999))
        .await
        .expect_err("revocation before creation must conflict");
    assert_eq!(revoke_before_creation.category(), ErrorCategory::Conflict);
    assert_eq!(
        repo.get(&second.key.id)
            .await
            .expect("get after rejected revoke"),
        Some(before_pre_creation_mutations.clone()),
        "rejected pre-creation revocation must not mutate the key"
    );

    repo.update_last_used(&second.key.id, second.key.generation, at(1_999))
        .await
        .expect("pre-creation usage is a harmless no-op");
    assert_eq!(
        repo.get(&second.key.id)
            .await
            .expect("get after pre-creation usage"),
        Some(before_pre_creation_mutations),
        "pre-creation usage must not mutate the key"
    );

    let listed = repo.list().await.expect("list API keys");
    assert_eq!(
        listed.iter().map(|key| &key.id).collect::<Vec<_>>(),
        vec![&second.key.id, &first.key.id],
        "list order is newest first with id as the deterministic tie-breaker"
    );

    let rotated = service
        .rotate(&first.key.id, 1, at(2_100))
        .await
        .expect("rotate active key");
    assert_eq!(rotated.key.generation, 2);
    assert!(
        service
            .authenticate(&first.secret, at(2_101))
            .await
            .is_err()
    );
    service
        .authenticate(&rotated.secret, at(2_101))
        .await
        .expect("rotated secret authenticates");

    let stale = service
        .rotate(&first.key.id, 1, at(2_102))
        .await
        .expect_err("stale generation must conflict");
    assert_eq!(stale.category(), ErrorCategory::Conflict);

    let expired = create_key(&service, "temporary", 3_000, Some(3_100)).await;
    let expired_row = repo
        .get(&expired.key.id)
        .await
        .expect("get expiring key")
        .expect("expiring key exists");
    let error = repo
        .rotate(
            &expired.key.id,
            expired_row.generation,
            expired_row.verifier.clone(),
            at(3_100),
        )
        .await
        .expect_err("expired keys are terminal and cannot rotate");
    assert_eq!(error.category(), ErrorCategory::Conflict);

    let revoked = service
        .revoke(&first.key.id, 2, at(4_000))
        .await
        .expect("revoke active key");
    assert_eq!(revoked.revoked_at, Some(at(4_000)));
    let replay = service
        .revoke(&first.key.id, 2, at(4_999))
        .await
        .expect("revoke replay is idempotent");
    assert_eq!(replay.revoked_at, Some(at(4_000)));
    assert!(
        service
            .authenticate(&rotated.secret, at(4_001))
            .await
            .is_err()
    );
    assert!(service.rotate(&first.key.id, 2, at(4_001)).await.is_err());

    repo.update_last_used(&second.key.id, second.key.generation, at(8_000))
        .await
        .expect("advance last use");
    repo.update_last_used(&second.key.id, second.key.generation, at(7_000))
        .await
        .expect("older observation is harmless");
    assert_eq!(
        repo.get(&second.key.id)
            .await
            .expect("get last use")
            .expect("second key exists")
            .last_used_at,
        Some(at(8_000))
    );

    let stale_pending = create_key(&service, "stale-last-use", 8_100, None).await;
    let rotated_pending = service
        .rotate(&stale_pending.key.id, 1, at(8_200))
        .await
        .expect("rotate pending key");
    repo.update_last_used(&stale_pending.key.id, 1, at(8_150))
        .await
        .expect("stale generation update is a harmless no-op");
    assert_eq!(
        repo.get(&stale_pending.key.id)
            .await
            .expect("get rotated key")
            .expect("rotated key exists")
            .last_used_at,
        None,
        "old-generation observation must not update the rotated credential"
    );
    service
        .revoke(
            &stale_pending.key.id,
            rotated_pending.key.generation,
            at(8_300),
        )
        .await
        .expect("revoke rotated key");
    repo.update_last_used(
        &stale_pending.key.id,
        rotated_pending.key.generation,
        at(8_250),
    )
    .await
    .expect("revoked update is a harmless no-op");
    assert_eq!(
        repo.get(&stale_pending.key.id)
            .await
            .expect("get revoked key")
            .expect("revoked key exists")
            .last_used_at,
        None,
        "active-state predicate must reject observations after revoke"
    );

    let race = create_key(&service, "rotation-race", 9_000, None).await;
    let race_row = repo
        .get(&race.key.id)
        .await
        .expect("get race key")
        .expect("race key exists");
    let (left, right) = tokio::join!(
        repo.rotate(
            &race.key.id,
            race_row.generation,
            race_row.verifier.clone(),
            at(9_001),
        ),
        repo.rotate(
            &race.key.id,
            race_row.generation,
            race_row.verifier,
            at(9_001),
        )
    );
    assert_eq!(
        usize::from(left.is_ok()) + usize::from(right.is_ok()),
        1,
        "conditional rotation has exactly one winner"
    );
    let loser = left.err().or_else(|| right.err()).expect("one loser");
    assert_eq!(loser.category(), ErrorCategory::Conflict);
}

#[tokio::test]
async fn in_memory_api_key_repository_contract() {
    contract(Arc::new(InMemoryApiKeyRepository::new())).await;
}

#[tokio::test]
async fn sqlite_api_key_repository_contract() {
    let database = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate SQLite");
    database
        .reset_storage_for_tests()
        .await
        .expect("clear SQLite fixtures");
    contract(database.api_key_repository()).await;
}

#[tokio::test]
async fn sqlite_explorer_redacts_the_binary_api_key_verifier() {
    let database = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate SQLite");
    let description = database
        .database_explorer()
        .describe_table(&TableRef::new("main", "api_keys").expect("table reference"))
        .await
        .expect("describe api_keys");
    let verifier = description
        .columns
        .iter()
        .find(|column| column.name == "secret_verifier")
        .expect("secret verifier column");
    assert!(
        verifier.sensitive,
        "verifier bytes must be explorer-redacted"
    );
}

fn test_database_url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
}

#[tokio::test]
async fn postgres_or_cockroach_api_key_repository_contract() {
    let Some(url) = test_database_url() else {
        eprintln!(
            "skipping PostgreSQL/CockroachDB API-key contract: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
        );
        return;
    };
    let database = PgDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate PostgreSQL-wire backend");
    database
        .reset_storage_for_tests()
        .await
        .expect("clear PostgreSQL-wire fixtures");
    contract(database.api_key_repository()).await;
}

#[tokio::test]
async fn mongodb_api_key_repository_contract() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping MongoDB API-key contract: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and reconcile MongoDB");
    database
        .clear_api_key_data_for_tests()
        .await
        .expect("clear MongoDB API-key fixtures");
    contract(database.api_key_repository()).await;
}
