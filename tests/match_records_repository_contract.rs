//! Shared durable match-record persistence contract.
//!
//! The embedded SQLite adapter always runs. PostgreSQL and CockroachDB run when
//! `DATABASE_URL`/`CITADEL_TEST_DATABASE_URL` selects the corresponding
//! PostgreSQL-wire backend. The in-memory and MongoDB backends deliberately
//! expose no match repository at all, so they have no contract to satisfy.

use std::sync::Arc;

use citadel::config::DatabaseConfig;
use citadel::error::ErrorCategory;
use citadel::ids::NodeIdentity;
use citadel::repository::{
    Backend, DurableMatchRepository, InMemoryBackend, MatchClose, MatchOpen, MongoDatabase,
    PgDatabase, SqliteDatabase,
};
use citadel::time::TimestampMillis;

fn open(identity: &NodeIdentity, room_id: u64, opened_at_ms: u64) -> MatchOpen {
    MatchOpen {
        match_id: identity.mint("mt1-", opened_at_ms),
        node_id: identity.node_id().to_string(),
        boot_id: identity.boot_id().to_string(),
        room_id,
        name: Some(format!("room {room_id}")),
        map: "arena".to_string(),
        mode: "deathmatch".to_string(),
        max_players: 8,
        script_revision_id: Some("gsr1-abc".to_string()),
        script_generation: Some(3),
        clock_epoch: 11,
        opened_at_ms,
    }
}

fn close(match_id: &str, closed_at_ms: u64, reason: &str) -> MatchClose {
    MatchClose {
        match_id: match_id.to_string(),
        closed_at_ms,
        termination_reason: reason.to_string(),
        peak_participants: 6,
        join_total: 14,
        result_json: None,
    }
}

async fn contract(repository: Arc<dyn DurableMatchRepository>) {
    let identity = NodeIdentity::new("contract-node");

    // An open is idempotent: a write-behind flush that partially succeeded is
    // retried whole, and the retry must neither duplicate nor fail.
    let first = open(&identity, 1, 1_000);
    assert_eq!(
        repository
            .open_batch(std::slice::from_ref(&first))
            .await
            .expect("open"),
        1
    );
    assert_eq!(
        repository
            .open_batch(std::slice::from_ref(&first))
            .await
            .expect("retry open"),
        0
    );

    let stored = repository
        .get(&first.match_id)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(stored.room_id, 1);
    assert_eq!(stored.node_id, identity.node_id());
    assert_eq!(stored.boot_id, identity.boot_id());
    assert_eq!(stored.map, "arena");
    assert_eq!(stored.max_players, 8);
    assert_eq!(stored.script_generation, Some(3));
    assert_eq!(stored.clock_epoch, 11);
    assert_eq!(stored.closed_at_ms, None);
    assert_eq!(stored.termination_reason, None);
    assert_eq!(stored.result_json, None);

    // Only a script may supply a result, and only while the match is open.
    assert!(
        repository
            .set_result(&first.match_id, "{\"winner\":\"kitsune\"}")
            .await
            .expect("stamp result")
    );

    // A close is idempotent too, and never erases a result it did not carry.
    let ended = close(&first.match_id, 5_000, "final_departure");
    assert_eq!(
        repository
            .close_batch(std::slice::from_ref(&ended))
            .await
            .expect("close"),
        1
    );
    assert_eq!(
        repository
            .close_batch(std::slice::from_ref(&ended))
            .await
            .expect("re-close"),
        0
    );
    let closed = repository
        .get(&first.match_id)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(closed.closed_at_ms, Some(5_000));
    assert_eq!(
        closed.termination_reason.as_deref(),
        Some("final_departure")
    );
    assert_eq!(closed.peak_participants, 6);
    assert_eq!(closed.join_total, 14);
    assert_eq!(
        closed
            .result_json
            .as_deref()
            .map(|value| value.contains("kitsune")),
        Some(true)
    );
    assert!(
        !repository
            .set_result(&first.match_id, "{\"winner\":\"okami\"}")
            .await
            .expect("stamp after close"),
        "a closed match no longer accepts a result"
    );

    let second = open(&identity, 2, 2_000);
    let third = open(&identity, 3, 3_000);
    repository
        .open_batch(&[second.clone(), third.clone()])
        .await
        .expect("open more");

    // Keyset paging is newest-first over the time-ordered id.
    let page = repository.list(None, 2, false).await.expect("page");
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].match_id, third.match_id);
    let next = repository
        .list(Some(&page[1].match_id), 2, false)
        .await
        .expect("next page");
    assert!(next.iter().all(|record| record.match_id < page[1].match_id));

    let open_only = repository.list(None, 50, true).await.expect("open only");
    assert!(
        open_only.iter().all(|record| record.closed_at_ms.is_none()),
        "the open filter never returns a closed match"
    );
    assert!(
        open_only
            .iter()
            .any(|record| record.match_id == second.match_id)
    );

    // Prune is bounded and touches only closed matches.
    let removed = repository
        .prune(TimestampMillis::from_unix_millis(10_000), 10)
        .await
        .expect("prune");
    assert_eq!(removed, 1, "exactly the one closed match is eligible");
    assert!(
        repository
            .get(&first.match_id)
            .await
            .expect("get pruned")
            .is_none()
    );
    assert!(
        repository
            .get(&second.match_id)
            .await
            .expect("get open")
            .is_some(),
        "an open match is never pruned"
    );

    // A batch wider than one bind chunk is chunked, not truncated.
    let bulk = (100..=250_u64)
        .map(|room| open(&identity, room, 10_000 + room))
        .collect::<Vec<_>>();
    assert_eq!(
        repository.open_batch(&bulk).await.expect("bulk open"),
        bulk.len()
    );
}

#[tokio::test]
async fn in_memory_and_mongodb_backends_expose_no_match_repository() {
    // Deliberate: those backends keep no durable match history, and every read
    // API still answers rather than erroring.
    assert!(InMemoryBackend::new().match_repository().is_none());
}

#[tokio::test]
async fn sqlite_match_repository_contract() {
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
    contract(
        database
            .match_repository()
            .expect("SQLite match repository"),
    )
    .await;
}

#[tokio::test]
async fn sqlite_rejects_a_termination_reason_outside_the_lifecycle_vocabulary() {
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
    let repository = database
        .match_repository()
        .expect("SQLite match repository");
    let identity = NodeIdentity::new("contract-node");
    let opened = open(&identity, 1, 1_000);
    repository
        .open_batch(std::slice::from_ref(&opened))
        .await
        .expect("open");
    let error = repository
        .close_batch(&[close(&opened.match_id, 2_000, "operator_said_so")])
        .await
        .expect_err("schema rejects an unknown reason");
    assert_eq!(error.category(), ErrorCategory::Database);
}

fn test_database_url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
}

#[tokio::test]
async fn postgres_or_cockroach_match_repository_contract() {
    let Some(url) = test_database_url() else {
        eprintln!(
            "skipping PostgreSQL/CockroachDB match-record contract: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
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
    contract(
        database
            .match_repository()
            .expect("PostgreSQL-wire match repository"),
    )
    .await;
}

#[tokio::test]
async fn mongodb_match_repository_is_absent() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!(
            "skipping MongoDB match-record capability check: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    let database = MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and reconcile MongoDB");
    assert!(
        database.match_repository().is_none(),
        "MongoDB deliberately inherits the None capability default"
    );
}
