//! Contract tests for the leaderboards repository.
//!
//! These assert the create/list/get/delete + submit-operator + rank/pagination +
//! metadata-durability semantics that *any*
//! [`LeaderboardsRepository`] implementation must honor. Each scenario is written
//! against `&dyn LeaderboardsRepository` and is run against every backend:
//!
//! - always against [`InMemoryLeaderboardsRepository`] (the reference impl),
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
//!   cargo test --test leaderboards_repository_contract
//! ```

use citadel::error::ErrorCategory;
use citadel::repository::{
    CreateLeaderboardRequest, InMemoryLeaderboardsRepository, LeaderboardsRepository, Operator,
    SortOrder,
};
use citadel::time::TimestampMillis;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn create_request(id: &str, sort: SortOrder, operator: Operator) -> CreateLeaderboardRequest {
    CreateLeaderboardRequest {
        id: id.to_string(),
        sort,
        operator,
        reset_schedule: None,
    }
}

async fn make_board(
    repo: &dyn LeaderboardsRepository,
    id: &str,
    sort: SortOrder,
    operator: Operator,
) {
    repo.create(create_request(id, sort, operator), ts(1))
        .await
        .expect("create board");
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_create_get_and_list_round_trip(repo: &dyn LeaderboardsRepository) {
    repo.create(
        CreateLeaderboardRequest {
            reset_schedule: Some("0 0 * * *".to_string()),
            ..create_request("race", SortOrder::Asc, Operator::Best)
        },
        ts(42),
    )
    .await
    .expect("create");

    // Durability: a fresh read returns the same definition.
    let fetched = repo.get("race").await.expect("get").expect("present");
    assert_eq!(fetched.id, "race");
    assert_eq!(fetched.sort, SortOrder::Asc);
    assert_eq!(fetched.operator, Operator::Best);
    assert_eq!(fetched.reset_schedule.as_deref(), Some("0 0 * * *"));
    assert_eq!(fetched.created_at, ts(42));

    assert!(
        repo.get("missing").await.expect("get missing").is_none(),
        "missing board is None"
    );

    let summaries = repo.list().await.expect("list");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].definition.id, "race");
    assert_eq!(summaries[0].records, 0, "no records yet");
}

async fn scenario_create_enforces_unique_id(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "race", SortOrder::Asc, Operator::Best).await;
    assert_eq!(
        repo.create(
            create_request("race", SortOrder::Desc, Operator::Set),
            ts(2)
        )
        .await
        .expect_err("duplicate id")
        .category(),
        ErrorCategory::Conflict
    );
}

async fn scenario_list_is_id_ordered(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "beta", SortOrder::Desc, Operator::Set).await;
    make_board(repo, "alpha", SortOrder::Desc, Operator::Set).await;
    make_board(repo, "gamma", SortOrder::Desc, Operator::Set).await;
    let ids: Vec<String> = repo
        .list()
        .await
        .expect("list")
        .into_iter()
        .map(|summary| summary.definition.id)
        .collect();
    assert_eq!(ids, vec!["alpha", "beta", "gamma"], "id-ordered ascending");
}

async fn scenario_set_operator_overwrites_and_counts(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    repo.submit("board", "u1", 100, 1, None, ts(1))
        .await
        .expect("first");
    let record = repo
        .submit(
            "board",
            "u1",
            10,
            9,
            Some(serde_json::json!({"a": 1})),
            ts(2),
        )
        .await
        .expect("overwrite even though worse");
    assert_eq!(record.score, 10);
    assert_eq!(record.subscore, 9);
    assert_eq!(record.metadata, Some(serde_json::json!({"a": 1})));
    assert_eq!(record.submissions, 2);
    assert_eq!(record.updated_at, ts(2));

    // Durability: the record survives a fresh read.
    let page = repo.records("board", 10, 0).await.expect("records");
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].score, 10);
    assert_eq!(page.items[0].metadata, Some(serde_json::json!({"a": 1})));
    assert_eq!(page.items[0].submissions, 2);
}

async fn scenario_incr_operator_adds(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Incr).await;
    repo.submit("board", "u1", 5, 1, None, ts(1))
        .await
        .expect("init");
    let record = repo
        .submit("board", "u1", 3, 2, None, ts(2))
        .await
        .expect("add");
    assert_eq!(record.score, 8);
    assert_eq!(record.subscore, 3);
    assert_eq!(record.submissions, 2);
}

async fn scenario_best_operator_desc_keeps_better(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Best).await;
    repo.submit("board", "u1", 50, 1, None, ts(1))
        .await
        .expect("first");
    // Worse score ignored (still counted).
    let worse = repo
        .submit("board", "u1", 40, 99, None, ts(2))
        .await
        .expect("worse ignored");
    assert_eq!(worse.score, 50);
    assert_eq!(worse.subscore, 1);
    assert_eq!(worse.submissions, 2);
    // Better score replaces.
    let better = repo
        .submit("board", "u1", 60, 0, None, ts(3))
        .await
        .expect("better replaces");
    assert_eq!(better.score, 60);
    // Tied score, higher subscore wins for Desc.
    let tie = repo
        .submit("board", "u1", 60, 5, None, ts(4))
        .await
        .expect("tie higher subscore wins");
    assert_eq!(tie.subscore, 5);
}

async fn scenario_best_operator_asc_keeps_lower(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Asc, Operator::Best).await;
    repo.submit("board", "u1", 100, 5, None, ts(1))
        .await
        .expect("first");
    // Better (lower) score replaces.
    let better = repo
        .submit("board", "u1", 50, 9, None, ts(2))
        .await
        .expect("lower wins");
    assert_eq!(better.score, 50);
    // Tied score, higher subscore loses for Asc.
    let tie = repo
        .submit("board", "u1", 50, 30, None, ts(3))
        .await
        .expect("tie higher subscore loses");
    assert_eq!(tie.subscore, 9, "unchanged");
}

async fn scenario_submit_against_unknown_board_is_not_found(repo: &dyn LeaderboardsRepository) {
    assert_eq!(
        repo.submit("ghost", "u1", 1, 0, None, ts(1))
            .await
            .expect_err("unknown board")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_records_are_ranked_and_paged(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    for (user, score) in [("bravo", 50), ("alpha", 50), ("charlie", 90), ("delta", 10)] {
        repo.submit("board", user, score, 0, None, ts(1))
            .await
            .expect("submit");
    }
    let page = repo.records("board", 10, 0).await.expect("records");
    assert_eq!(page.total, 4);
    let order: Vec<(&str, u64)> = page
        .items
        .iter()
        .map(|r| (r.user_id.as_str(), r.rank))
        .collect();
    // charlie(90) first; alpha/bravo tie at 50 break by user_id; delta(10) last.
    assert_eq!(
        order,
        vec![("charlie", 1), ("alpha", 2), ("bravo", 3), ("delta", 4)]
    );

    // Rank offset + limit.
    let page = repo.records("board", 2, 1).await.expect("paged");
    assert_eq!(page.total, 4, "total ignores paging");
    let users: Vec<&str> = page.items.iter().map(|r| r.user_id.as_str()).collect();
    assert_eq!(users, vec!["alpha", "bravo"]);
    assert_eq!(page.items[0].rank, 2);
}

async fn scenario_zero_limit_is_an_empty_bounded_page(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    repo.submit("board", "u1", 10, 0, None, ts(1))
        .await
        .expect("submit");
    let page = repo.records("board", 0, 0).await.expect("records");
    assert!(page.items.is_empty(), "zero limit never means unbounded");
    assert_eq!(page.total, 1);
}

async fn scenario_records_against_unknown_board_is_not_found(repo: &dyn LeaderboardsRepository) {
    assert_eq!(
        repo.records("ghost", 10, 0)
            .await
            .expect_err("unknown board")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_delete_board_cascades_records(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    repo.submit("board", "u1", 10, 0, None, ts(1))
        .await
        .expect("submit");
    assert!(repo.delete("board").await.expect("delete"), "removed");
    assert!(
        repo.get("board").await.expect("get").is_none(),
        "board gone"
    );
    assert!(
        !repo.delete("board").await.expect("idempotent"),
        "second delete removes nothing"
    );
    // Records are gone with the board (cascade); a fresh board of the same id
    // starts empty.
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    assert_eq!(
        repo.records("board", 10, 0).await.expect("records").total,
        0
    );
}

async fn scenario_delete_record_semantics(repo: &dyn LeaderboardsRepository) {
    make_board(repo, "board", SortOrder::Desc, Operator::Set).await;
    repo.submit("board", "u1", 10, 0, None, ts(1))
        .await
        .expect("submit");
    assert!(
        repo.delete_record("board", "u1").await.expect("delete"),
        "record removed"
    );
    assert!(
        !repo.delete_record("board", "u1").await.expect("idempotent"),
        "second delete removes nothing"
    );
    // Deleting a record on an unknown board is NotFound.
    assert_eq!(
        repo.delete_record("ghost", "u1")
            .await
            .expect_err("unknown board")
            .category(),
        ErrorCategory::NotFound
    );
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn LeaderboardsRepository) -> ScenarioFuture<'_>,
);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn LeaderboardsRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_create_get_and_list_round_trip,
        scenario_create_enforces_unique_id,
        scenario_list_is_id_ordered,
        scenario_set_operator_overwrites_and_counts,
        scenario_incr_operator_adds,
        scenario_best_operator_desc_keeps_better,
        scenario_best_operator_asc_keeps_lower,
        scenario_submit_against_unknown_board_is_not_found,
        scenario_records_are_ranked_and_paged,
        scenario_zero_limit_is_an_empty_bounded_page,
        scenario_records_against_unknown_board_is_not_found,
        scenario_delete_board_cascades_records,
        scenario_delete_record_semantics,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryLeaderboardsRepository::new();
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
        let repo = db.leaderboards_repository();

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
                "skipping Postgres leaderboards contract: set DATABASE_URL or \
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
        let repo = db.leaderboards_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}

// --- MongoDB run (opt-in via CITADEL_TEST_MONGODB_URL) ---------------------

mod mongodb {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, MongoDatabase};
    use std::sync::Arc;

    fn test_database_url() -> Option<String> {
        std::env::var("CITADEL_TEST_MONGODB_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
    }

    #[tokio::test]
    async fn mongodb_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!("skipping MongoDB leaderboards contract: set CITADEL_TEST_MONGODB_URL");
            return;
        };
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = MongoDatabase::connect(&config)
            .await
            .expect("connect + reconcile against MongoDB replica set");
        let repo = db.leaderboards_repository();

        for (name, run) in all_scenarios() {
            db.clear_leaderboards_data_for_tests()
                .await
                .expect("reset leaderboards between scenarios");
            eprintln!("mongodb scenario: {name}");
            run(repo.as_ref()).await;
        }
    }

    #[tokio::test]
    async fn mongodb_concurrent_increments_are_serializable() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping MongoDB leaderboard concurrency contract: set CITADEL_TEST_MONGODB_URL"
            );
            return;
        };
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = MongoDatabase::connect(&config).await.expect("connect");
        db.clear_leaderboards_data_for_tests().await.expect("reset");
        let repo = Arc::new(db.leaderboards_repository());
        repo.create(
            create_request("concurrent", SortOrder::Desc, Operator::Incr),
            ts(1),
        )
        .await
        .expect("create");

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let repo = Arc::clone(&repo);
            tasks.push(tokio::spawn(async move {
                repo.submit("concurrent", "u1", 1, 0, None, ts(2)).await
            }));
        }
        for task in tasks {
            task.await
                .expect("task joins")
                .expect("submit succeeds after bounded retry");
        }
        let page = repo.records("concurrent", 10, 0).await.expect("records");
        assert_eq!(page.items[0].score, 8);
        assert_eq!(page.items[0].submissions, 8);
    }
}
