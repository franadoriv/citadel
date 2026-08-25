//! Shared durable console-audit persistence contract.
//!
//! The embedded SQLite adapter always runs. PostgreSQL and CockroachDB run when
//! `DATABASE_URL`/`CITADEL_TEST_DATABASE_URL` selects the corresponding
//! PostgreSQL-wire backend. The in-memory and MongoDB backends deliberately
//! expose no audit repository and keep the bounded in-process ring as their
//! only trail.

use std::sync::Arc;

use citadel::config::DatabaseConfig;
use citadel::ids::NodeIdentity;
use citadel::repository::{
    Backend, DurableAuditFilter, DurableAuditRepository, DurableAuditRow, InMemoryBackend,
    MongoDatabase, PgDatabase, SqliteDatabase,
};
use citadel::services::AuditEntry;
use citadel::time::TimestampMillis;

fn row(identity: &NodeIdentity, at_ms: u64, actor: &str, action: &str) -> DurableAuditRow {
    DurableAuditRow {
        audit_id: identity.mint("au1-", at_ms),
        node_id: identity.node_id().to_string(),
        match_id: None,
        entry: AuditEntry::new(
            TimestampMillis::from_unix_millis(at_ms),
            actor,
            "admin",
            action,
            "-",
            "ok",
        ),
    }
}

async fn contract(repository: Arc<dyn DurableAuditRepository>) {
    let identity = NodeIdentity::new("contract-node");

    // Two entries recorded from one `now` — the console extractor does exactly
    // this for `login_failed` followed by `login`. Only the time-ordered id can
    // break the tie, so `ORDER BY audit_id DESC` is the whole ordering rule.
    let failed = row(&identity, 5_000, "ops", "console.login_failed");
    let succeeded = row(&identity, 5_000, "ops", "console.login");
    assert_eq!(
        repository
            .append_batch(&[failed.clone(), succeeded.clone()])
            .await
            .expect("append"),
        2
    );
    let ordered = repository
        .list(&DurableAuditFilter {
            limit: 50,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("list");
    assert_eq!(
        ordered
            .iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["console.login", "console.login_failed"]
    );

    // Append is idempotent: a partially applied flush is retried whole.
    assert_eq!(
        repository
            .append_batch(&[failed.clone(), succeeded])
            .await
            .expect("retry append"),
        0
    );

    // Machine-credential metadata round-trips, including an absent scope list.
    let mut machine = row(&identity, 6_000, "key-1", "console.read");
    machine.entry.actor_type = "api_key".to_string();
    machine.entry.credential_id = Some("cred-1".to_string());
    machine.entry.key_name = Some("ci poller".to_string());
    machine.entry.scopes = Some(vec!["logs:read".to_string(), "matches:read".to_string()]);
    machine.entry.role = "api_key".to_string();
    repository
        .append_batch(std::slice::from_ref(&machine))
        .await
        .expect("append machine entry");
    let stored = repository
        .list(&DurableAuditFilter {
            actor: Some("key-1".to_string()),
            limit: 50,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("list machine entry");
    assert_eq!(stored, vec![machine]);

    // An operator action is not match-scoped, and a `None` filter matches every
    // row — including the ones with no match at all.
    let mut scoped = row(&identity, 6_500, "ops", "matchlog.detail");
    scoped.match_id = Some("mt1-a".to_string());
    repository
        .append_batch(std::slice::from_ref(&scoped))
        .await
        .expect("append scoped entry");
    assert_eq!(
        repository
            .count(&DurableAuditFilter::default())
            .await
            .expect("count all"),
        4
    );
    let by_match = repository
        .list(&DurableAuditFilter {
            match_id: Some("mt1-a".to_string()),
            limit: 50,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("list by match");
    assert_eq!(by_match.len(), 1);
    assert_eq!(by_match[0].entry.action, "matchlog.detail");

    // Filters are conjunctive: exact actor, prefix action.
    repository
        .append_batch(&[
            row(&identity, 7_000, "ops", "storage.write"),
            row(&identity, 7_001, "ops", "storage.read"),
            row(&identity, 7_002, "other", "storage.write"),
            row(&identity, 7_003, "ops", "st%range"),
        ])
        .await
        .expect("append filter fixtures");
    let filtered = repository
        .list(&DurableAuditFilter {
            actor: Some("ops".to_string()),
            action_prefix: Some("storage".to_string()),
            limit: 50,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("filtered list");
    assert_eq!(
        filtered
            .iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["storage.read", "storage.write"]
    );
    assert_eq!(
        repository
            .count(&DurableAuditFilter {
                actor: Some("ops".to_string()),
                action_prefix: Some("storage".to_string()),
                ..DurableAuditFilter::default()
            })
            .await
            .expect("filtered count"),
        2
    );

    // A literal `%` typed by an operator must match only itself.
    let literal = repository
        .list(&DurableAuditFilter {
            action_prefix: Some("st%".to_string()),
            limit: 50,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("escaped list");
    assert_eq!(
        literal
            .iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["st%range"]
    );

    // Keyset paging is newest-first and never repeats a row.
    let page = repository
        .list(&DurableAuditFilter {
            limit: 3,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("first page");
    assert_eq!(page.len(), 3);
    let next = repository
        .list(&DurableAuditFilter {
            after_audit_id: Some(page[2].audit_id.clone()),
            limit: 3,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("next page");
    assert!(next.iter().all(|row| row.audit_id < page[2].audit_id));

    // Prune is bounded and takes the oldest rows first.
    assert_eq!(
        repository
            .prune(TimestampMillis::from_unix_millis(7_000), 1)
            .await
            .expect("prune"),
        1
    );
    assert!(
        repository
            .list(&DurableAuditFilter {
                limit: 50,
                ..DurableAuditFilter::default()
            })
            .await
            .expect("list after prune")
            .iter()
            .all(|row| row.audit_id != failed.audit_id)
    );

    // A batch wider than one bind chunk is chunked, not truncated.
    let bulk = (0..250_u64)
        .map(|index| row(&identity, 50_000 + index, "bulk", "console.read"))
        .collect::<Vec<_>>();
    assert_eq!(
        repository.append_batch(&bulk).await.expect("bulk append"),
        bulk.len()
    );
}

#[tokio::test]
async fn the_in_memory_backend_exposes_no_audit_repository() {
    assert!(InMemoryBackend::new().audit_repository().is_none());
}

#[tokio::test]
async fn sqlite_audit_repository_contract() {
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
            .audit_repository()
            .expect("SQLite audit repository"),
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
async fn postgres_or_cockroach_audit_repository_contract() {
    let Some(url) = test_database_url() else {
        eprintln!(
            "skipping PostgreSQL/CockroachDB console-audit contract: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
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
            .audit_repository()
            .expect("PostgreSQL-wire audit repository"),
    )
    .await;
}

#[tokio::test]
async fn mongodb_audit_repository_is_absent() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!(
            "skipping MongoDB console-audit capability check: CITADEL_TEST_MONGODB_URL is unset"
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
        database.audit_repository().is_none(),
        "MongoDB deliberately inherits the None capability default"
    );
}
