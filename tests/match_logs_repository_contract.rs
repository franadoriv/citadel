//! Shared durable game-script log persistence contract.
//!
//! The embedded SQLite adapter always runs. PostgreSQL and CockroachDB run when
//! `DATABASE_URL`/`CITADEL_TEST_DATABASE_URL` selects the corresponding
//! PostgreSQL-wire backend. The in-memory and MongoDB backends deliberately
//! expose no log repository, so the console reports `durable: false` there
//! rather than presenting a process-local cache as history.

use std::sync::Arc;

use citadel::config::DatabaseConfig;
use citadel::ids::NodeIdentity;
use citadel::repository::{
    Backend, DurableMatchLogRepository, InMemoryBackend, LogLevel, MatchLogEntry, MatchLogFilter,
    MongoDatabase, PgDatabase, SqliteDatabase,
};
use citadel::time::TimestampMillis;

fn entry(
    identity: &NodeIdentity,
    at_ms: u64,
    match_id: Option<&str>,
    level: LogLevel,
    tag: &str,
) -> MatchLogEntry {
    MatchLogEntry {
        log_id: identity.mint("ml1-", at_ms),
        match_id: match_id.map(str::to_string),
        node_id: identity.node_id().to_string(),
        created_at_ms: at_ms,
        level,
        tag: tag.to_string(),
        message: format!("line at {at_ms}"),
        payload_json: None,
    }
}

async fn contract(repository: Arc<dyn DurableMatchLogRepository>) {
    let identity = NodeIdentity::new("contract-node");

    // A log written outside any match is stored with a NULL match, never
    // rejected. This is the whole point of the nullable column: a game with no
    // match concept must still be able to write.
    let unscoped = entry(&identity, 1_000, None, LogLevel::Info, "world");
    assert_eq!(
        repository
            .append_batch(std::slice::from_ref(&unscoped))
            .await
            .expect("append"),
        1
    );
    let stored = repository
        .get(&unscoped.log_id)
        .await
        .expect("get")
        .expect("entry");
    assert_eq!(stored.match_id, None);
    assert_eq!(stored.level, LogLevel::Info);
    assert_eq!(stored.node_id, identity.node_id());

    // Append is idempotent: a partially applied flush is retried whole.
    assert_eq!(
        repository
            .append_batch(std::slice::from_ref(&unscoped))
            .await
            .expect("retry append"),
        0
    );

    // The author's payload is stored verbatim — not reformatted, not redacted.
    let mut payload = entry(&identity, 1_100, Some("mt1-a"), LogLevel::Warn, "score");
    payload.payload_json = Some("{\"kills\":3,\"note\":\"<b>&amp;\"}".to_string());
    repository
        .append_batch(std::slice::from_ref(&payload))
        .await
        .expect("append payload");
    let stored = repository
        .get(&payload.log_id)
        .await
        .expect("get")
        .expect("entry");
    assert_eq!(
        stored
            .payload_json
            .as_deref()
            .map(|value| value.contains("<b>&amp;")),
        Some(true)
    );

    let mut rows = Vec::new();
    for (index, (match_id, level, tag)) in [
        (Some("mt1-a"), LogLevel::Error, "combat.hit"),
        (Some("mt1-a"), LogLevel::Debug, "combat.miss"),
        (Some("mt1-b"), LogLevel::Error, "combat.hit"),
        (Some("mt1-a"), LogLevel::Error, "economy.sale"),
    ]
    .into_iter()
    .enumerate()
    {
        let at_ms = 2_000 + u64::try_from(index).unwrap_or(0);
        rows.push(entry(&identity, at_ms, match_id, level, tag));
    }
    repository.append_batch(&rows).await.expect("append batch");

    // Filters are conjunctive; a `None` field matches everything.
    let filtered = repository
        .list(&MatchLogFilter {
            match_id: Some("mt1-a".to_string()),
            level: Some(LogLevel::Error),
            tag_prefix: Some("combat".to_string()),
            after_log_id: None,
            limit: 50,
        })
        .await
        .expect("filtered list");
    assert_eq!(
        filtered
            .iter()
            .map(|line| line.tag.as_str())
            .collect::<Vec<_>>(),
        vec!["combat.hit"]
    );

    // A literal wildcard in the tag filter matches only itself.
    let literal = entry(&identity, 2_500, Some("mt1-a"), LogLevel::Info, "ab%cd");
    repository
        .append_batch(std::slice::from_ref(&literal))
        .await
        .expect("append literal");
    let escaped = repository
        .list(&MatchLogFilter {
            tag_prefix: Some("ab%".to_string()),
            limit: 50,
            ..MatchLogFilter::default()
        })
        .await
        .expect("escaped list");
    assert_eq!(
        escaped
            .iter()
            .map(|line| line.tag.as_str())
            .collect::<Vec<_>>(),
        vec!["ab%cd"]
    );

    // Counting is per-match and ignores unscoped rows entirely.
    assert_eq!(
        repository
            .count_for_match("mt1-a")
            .await
            .expect("count mt1-a"),
        5
    );
    assert_eq!(
        repository
            .count_for_match("mt1-b")
            .await
            .expect("count mt1-b"),
        1
    );

    // Keyset paging is newest-first over the time-ordered id and never repeats.
    let first = repository
        .list(&MatchLogFilter {
            limit: 3,
            ..MatchLogFilter::default()
        })
        .await
        .expect("first page");
    assert_eq!(first.len(), 3);
    let next = repository
        .list(&MatchLogFilter {
            after_log_id: Some(first[2].log_id.clone()),
            limit: 3,
            ..MatchLogFilter::default()
        })
        .await
        .expect("next page");
    assert!(next.iter().all(|line| line.log_id < first[2].log_id));

    // Prune is bounded and takes the oldest rows first.
    let removed = repository
        .prune(TimestampMillis::from_unix_millis(2_002), 2)
        .await
        .expect("prune");
    assert_eq!(removed, 2);
    assert!(
        repository
            .get(&unscoped.log_id)
            .await
            .expect("get pruned")
            .is_none()
    );

    // A batch wider than one bind chunk is chunked, not truncated.
    let bulk = (0..250_u64)
        .map(|index| {
            entry(
                &identity,
                50_000 + index,
                Some("mt1-bulk"),
                LogLevel::Trace,
                "bulk",
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        repository.append_batch(&bulk).await.expect("bulk append"),
        bulk.len()
    );
    assert_eq!(
        repository
            .count_for_match("mt1-bulk")
            .await
            .expect("bulk count"),
        250
    );
}

#[tokio::test]
async fn the_in_memory_backend_exposes_no_match_log_repository() {
    assert!(InMemoryBackend::new().match_log_repository().is_none());
}

#[tokio::test]
async fn sqlite_match_log_repository_contract() {
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
            .match_log_repository()
            .expect("SQLite match log repository"),
    )
    .await;
}

fn test_database_url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
}

#[tokio::test]
async fn postgres_or_cockroach_match_log_repository_contract() {
    let Some(url) = test_database_url() else {
        eprintln!(
            "skipping PostgreSQL/CockroachDB match-log contract: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
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
            .match_log_repository()
            .expect("PostgreSQL-wire match log repository"),
    )
    .await;
}

#[tokio::test]
async fn mongodb_match_log_repository_is_absent() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping MongoDB match-log capability check: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and reconcile MongoDB");
    assert!(
        database.match_log_repository().is_none(),
        "MongoDB deliberately inherits the None capability default"
    );
}
