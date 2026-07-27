//! Contract tests for the notifications repository.
//!
//! These assert the enqueue / visibility-filtered newest-first paging / capacity
//! eviction / read-state / delete semantics that *any*
//! [`NotificationsRepository`] implementation must honor. Each scenario is written
//! against `&dyn NotificationsRepository` and is run against every backend:
//!
//! - always against [`InMemoryNotificationsRepository`] (the reference impl),
//! - always against a real embedded SQLite backend (un-gated; no server), and
//! - against a real Postgres backend when `DATABASE_URL` (or
//!   `CITADEL_TEST_DATABASE_URL`) is set, proving all three behave identically.
//!   The Postgres run is skipped when neither variable is set, so
//!   `bash scripts/check.sh` stays green without a database.
//!
//! Run the Postgres side locally with:
//!
//! ```text
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo test --test notifications_repository_contract
//! ```

use citadel::error::ErrorCategory;
use citadel::repository::{InMemoryNotificationsRepository, NotificationsRepository, Recipient};
use citadel::time::TimestampMillis;

/// A roomy capacity for scenarios that are not exercising eviction.
const CAP: usize = 1000;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn obj() -> serde_json::Value {
    serde_json::json!({ "k": "v" })
}

async fn enqueue(
    repo: &dyn NotificationsRepository,
    recipient: Recipient,
    subject: &str,
    capacity: usize,
    now: u64,
) -> u64 {
    repo.enqueue(recipient, subject, &obj(), 0, capacity, ts(now))
        .await
        .expect("enqueue")
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_enqueue_assigns_sequential_ids_and_lists_newest_first(
    repo: &dyn NotificationsRepository,
) {
    let a = enqueue(repo, Recipient::Broadcast, "a", CAP, 1).await;
    let b = enqueue(repo, Recipient::Broadcast, "b", CAP, 2).await;
    assert_eq!((a, b), (1, 2), "global sequential ids");

    // Durability: a fresh read returns the notifications newest-first, with the
    // JSON payload round-tripped.
    let page = repo.list(None, 10, None).await.expect("list");
    assert_eq!(
        page.items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "a"]
    );
    assert_eq!(page.items[0].content, obj());
    assert_eq!(page.total, 2);
    assert_eq!(repo.count().await.expect("count"), 2);
}

async fn scenario_targeted_visible_only_to_recipient_plus_broadcast(
    repo: &dyn NotificationsRepository,
) {
    enqueue(repo, Recipient::User("u-1".to_string()), "for u1", CAP, 1).await;
    enqueue(repo, Recipient::User("u-2".to_string()), "for u2", CAP, 2).await;
    enqueue(repo, Recipient::Broadcast, "news", CAP, 3).await;

    let for_u1 = repo.list(Some("u-1"), 10, None).await.expect("list");
    assert_eq!(
        for_u1
            .items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["news", "for u1"]
    );
    assert_eq!(for_u1.total, 2);

    let for_u2 = repo.list(Some("u-2"), 10, None).await.expect("list");
    assert_eq!(
        for_u2
            .items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["news", "for u2"]
    );

    // The unfiltered operator view sees everything.
    assert_eq!(repo.list(None, 10, None).await.expect("list").total, 3);
}

async fn scenario_before_cursor_pages_backward(repo: &dyn NotificationsRepository) {
    let mut ids = Vec::new();
    for i in 1..=5u64 {
        ids.push(enqueue(repo, Recipient::Broadcast, &format!("n{i}"), CAP, i).await);
    }
    let first = repo.list(None, 2, None).await.expect("list");
    assert_eq!(
        first.items.iter().map(|n| n.id).collect::<Vec<_>>(),
        vec![ids[4], ids[3]]
    );
    let next = repo.list(None, 2, Some(ids[3])).await.expect("list");
    assert_eq!(
        next.items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["n3", "n2"]
    );
    assert_eq!(next.total, 5, "total ignores the cursor");
}

async fn scenario_capacity_evicts_oldest(repo: &dyn NotificationsRepository) {
    for i in 1..=5u64 {
        enqueue(repo, Recipient::Broadcast, &format!("n{i}"), 3, i).await;
    }
    assert_eq!(repo.count().await.expect("count"), 3);
    let page = repo.list(None, 10, None).await.expect("list");
    assert_eq!(
        page.items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["n5", "n4", "n3"],
        "only the newest 3 retained"
    );
    // The next enqueue still advances the id past the evicted ones.
    let next = enqueue(repo, Recipient::Broadcast, "n6", 3, 6).await;
    assert_eq!(next, 6, "eviction never rewinds the sequence");
}

async fn scenario_delete_removes_durably_and_unknown_is_not_found(
    repo: &dyn NotificationsRepository,
) {
    enqueue(repo, Recipient::Broadcast, "keep", CAP, 1).await;
    let target = enqueue(repo, Recipient::Broadcast, "gone", CAP, 2).await;
    repo.delete(target).await.expect("delete");

    let page = repo.list(None, 10, None).await.expect("list");
    assert_eq!(
        page.items
            .iter()
            .map(|n| n.subject.as_str())
            .collect::<Vec<_>>(),
        vec!["keep"]
    );
    assert_eq!(repo.count().await.expect("count"), 1);

    assert_eq!(
        repo.delete(target)
            .await
            .expect_err("already gone")
            .category(),
        ErrorCategory::NotFound
    );
    assert_eq!(
        repo.delete(9_999)
            .await
            .expect_err("never existed")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_mark_read_persists_and_unknown_is_not_found(repo: &dyn NotificationsRepository) {
    let id = enqueue(repo, Recipient::Broadcast, "read me", CAP, 1).await;
    assert!(!repo.list(None, 10, None).await.expect("list").items[0].read);

    repo.mark_read(id, ts(2)).await.expect("mark read");
    // Durability: a fresh read shows the read flag set.
    assert!(repo.list(None, 10, None).await.expect("list").items[0].read);
    // Idempotent: marking an already-read notification is a no-op success.
    repo.mark_read(id, ts(3)).await.expect("idempotent");

    assert_eq!(
        repo.mark_read(9_999, ts(4))
            .await
            .expect_err("unknown")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_empty_store_reads_are_empty(repo: &dyn NotificationsRepository) {
    let page = repo.list(None, 10, None).await.expect("list");
    assert!(page.items.is_empty());
    assert_eq!(page.total, 0);
    assert_eq!(repo.count().await.expect("count"), 0);
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn NotificationsRepository) -> ScenarioFuture<'_>,
);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn NotificationsRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_enqueue_assigns_sequential_ids_and_lists_newest_first,
        scenario_targeted_visible_only_to_recipient_plus_broadcast,
        scenario_before_cursor_pages_backward,
        scenario_capacity_evicts_oldest,
        scenario_delete_removes_durably_and_unknown_is_not_found,
        scenario_mark_read_persists_and_unknown_is_not_found,
        scenario_empty_store_reads_are_empty,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryNotificationsRepository::new();
        run(&repo).await;
        let _ = name;
    }
}

// --- SQLite run (always; embedded, no server) -------------------------------

mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    #[tokio::test]
    async fn sqlite_backend_satisfies_the_contract() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate against an in-memory SQLite database");
        let repo = db.notifications_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("sqlite scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}

// --- Postgres run (opt-in via DATABASE_URL) ---------------------------------

mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
    }

    #[tokio::test]
    async fn postgres_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping Postgres notifications contract: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run it"
            );
            return;
        };

        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = PgDatabase::connect(&config)
            .await
            .expect("connect + migrate against the test Postgres");
        let repo = db.notifications_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}
