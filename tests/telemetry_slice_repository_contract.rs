//! Shared durable telemetry-slice persistence contract.
//!
//! The embedded SQLite adapter always runs. PostgreSQL and CockroachDB run when
//! `DATABASE_URL`/`CITADEL_TEST_DATABASE_URL` selects the corresponding
//! PostgreSQL-wire backend. The in-memory and MongoDB backends deliberately
//! expose no slice repository and keep the process-local ring.
//!
//! Every assertion here is about aggregates. Marker text is validated, counted,
//! and discarded upstream; there is no column for it and there never will be.

use std::sync::Arc;

use citadel::config::DatabaseConfig;
use citadel::error::ErrorCategory;
use citadel::ids::NodeIdentity;
use citadel::repository::{
    Backend, DurableSliceRow, DurableTelemetrySliceRepository, InMemoryBackend, MongoDatabase,
    PgDatabase, SqliteDatabase,
};
use citadel::time::TimestampMillis;

fn slice(
    identity: &NodeIdentity,
    closed_at_ms: u64,
    match_id: Option<&str>,
    close_reason: &str,
) -> DurableSliceRow {
    DurableSliceRow {
        report_id: identity.mint("ats1-", closed_at_ms),
        node_id: identity.node_id().to_string(),
        match_id: match_id.map(str::to_string),
        context_kind: "match".to_string(),
        close_reason: close_reason.to_string(),
        closed_at_ms,
        duration_ms: 750,
        marker_total: 4,
        truncated: true,
        accepted_total: 120,
        rejected_total: 3,
        corrected_total: 7,
    }
}

async fn contract(repository: Arc<dyn DurableTelemetrySliceRepository>) {
    let identity = NodeIdentity::new("contract-node");

    let first = slice(&identity, 1_000, Some("mt1-a"), "ttl");
    assert_eq!(
        repository
            .insert_batch(std::slice::from_ref(&first))
            .await
            .expect("insert"),
        1
    );
    // Insert is idempotent: a partially applied flush is retried whole.
    assert_eq!(
        repository
            .insert_batch(std::slice::from_ref(&first))
            .await
            .expect("retry insert"),
        0
    );
    let stored = repository
        .get(&first.report_id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(stored, first, "every aggregate round-trips unchanged");

    // A slice closed outside any match is stored with a NULL match.
    let unscoped = slice(&identity, 1_100, None, "finished");
    repository
        .insert_batch(std::slice::from_ref(&unscoped))
        .await
        .expect("insert unscoped");
    assert_eq!(
        repository
            .get(&unscoped.report_id)
            .await
            .expect("get")
            .expect("row")
            .match_id,
        None
    );

    repository
        .insert_batch(&[
            slice(&identity, 2_000, Some("mt1-a"), "active_cap"),
            slice(&identity, 2_100, Some("mt1-b"), "marker_cap"),
            slice(&identity, 2_200, Some("mt1-a"), "restarted"),
        ])
        .await
        .expect("insert batch");

    // `None` counts everything, including the unscoped row.
    assert_eq!(repository.count(None).await.expect("count all"), 5);
    assert_eq!(
        repository.count(Some("mt1-a")).await.expect("count mt1-a"),
        3
    );
    assert_eq!(
        repository.count(Some("mt1-b")).await.expect("count mt1-b"),
        1
    );

    // Keyset paging is newest-first over the time-ordered id and never repeats.
    let page = repository
        .list(Some("mt1-a"), None, 2)
        .await
        .expect("first page");
    assert_eq!(
        page.iter()
            .map(|row| row.close_reason.as_str())
            .collect::<Vec<_>>(),
        vec!["restarted", "active_cap"]
    );
    let next = repository
        .list(Some("mt1-a"), Some(&page[1].report_id), 2)
        .await
        .expect("next page");
    assert_eq!(
        next.iter()
            .map(|row| row.close_reason.as_str())
            .collect::<Vec<_>>(),
        vec!["ttl"]
    );

    // Prune is bounded and takes the oldest rows first.
    assert_eq!(
        repository
            .prune(TimestampMillis::from_unix_millis(2_150), 1)
            .await
            .expect("prune"),
        1
    );
    assert!(
        repository
            .get(&first.report_id)
            .await
            .expect("get pruned")
            .is_none()
    );

    // A batch wider than one bind chunk is chunked, not truncated.
    let bulk = (0..200_u64)
        .map(|index| slice(&identity, 50_000 + index, Some("mt1-bulk"), "ttl"))
        .collect::<Vec<_>>();
    assert_eq!(
        repository.insert_batch(&bulk).await.expect("bulk insert"),
        bulk.len()
    );
    assert_eq!(
        repository
            .count(Some("mt1-bulk"))
            .await
            .expect("bulk count"),
        200
    );
}

#[tokio::test]
async fn the_in_memory_backend_exposes_no_telemetry_slice_repository() {
    assert!(
        InMemoryBackend::new()
            .telemetry_slice_repository()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_telemetry_slice_repository_contract() {
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
            .telemetry_slice_repository()
            .expect("SQLite telemetry slice repository"),
    )
    .await;
}

#[tokio::test]
async fn sqlite_rejects_a_close_reason_outside_the_recorder_vocabulary() {
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
        .telemetry_slice_repository()
        .expect("SQLite telemetry slice repository");
    let identity = NodeIdentity::new("contract-node");
    let error = repository
        .insert_batch(&[slice(&identity, 1_000, None, "operator_page_load")])
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
async fn postgres_or_cockroach_telemetry_slice_repository_contract() {
    let Some(url) = test_database_url() else {
        eprintln!(
            "skipping PostgreSQL/CockroachDB telemetry-slice contract: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
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
            .telemetry_slice_repository()
            .expect("PostgreSQL-wire telemetry slice repository"),
    )
    .await;
}

#[tokio::test]
async fn mongodb_telemetry_slice_repository_is_absent() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!(
            "skipping MongoDB telemetry-slice capability check: CITADEL_TEST_MONGODB_URL is unset"
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
        database.telemetry_slice_repository().is_none(),
        "MongoDB deliberately inherits the None capability default"
    );
}
