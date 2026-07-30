//! Contract tests for the friends repository.
//!
//! These assert the invite→mutual / one-sided-block / self-independent state
//! machine and durability semantics that *any* [`FriendsRepository`]
//! implementation must honor. Each scenario is written against
//! `&dyn FriendsRepository` and is run against every backend:
//!
//! - always against [`InMemoryFriendsRepository`] (the reference impl),
//! - always against a real embedded SQLite backend (un-gated; no server), and
//! - against a real Postgres backend when `DATABASE_URL` (or
//!   `CITADEL_TEST_DATABASE_URL`) is set, proving all three behave identically.
//! - against a real MongoDB replica set when `CITADEL_TEST_MONGODB_URL` is set.
//!   The Postgres run is skipped when neither variable is set, so
//!   `bash scripts/check.sh` stays green without a database.
//!
//! Run the Postgres side locally with:
//!
//! ```text
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo test --test friends_repository_contract
//! ```

use citadel::error::ErrorCategory;
use citadel::repository::{FriendState, FriendsRepository, InMemoryFriendsRepository};
use citadel::time::TimestampMillis;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_invite_then_accept_becomes_mutual(repo: &dyn FriendsRepository) {
    assert_eq!(
        repo.add("a", "b", ts(1)).await.expect("invite"),
        FriendState::InvitedSent
    );
    // The invitee sees a pending incoming invite.
    let b_rows = repo.list("b").await.expect("list b");
    assert_eq!(b_rows.len(), 1);
    assert_eq!(b_rows[0].user_id, "a");
    assert_eq!(b_rows[0].state, FriendState::InvitedReceived);
    assert_eq!(b_rows[0].updated_unix_ms, 1);

    // A matching invite from the other side upgrades both to friends.
    assert_eq!(
        repo.add("b", "a", ts(2)).await.expect("accept"),
        FriendState::Friend
    );
    assert_eq!(
        repo.list("a").await.expect("list a")[0].state,
        FriendState::Friend
    );
    assert_eq!(
        repo.list("b").await.expect("list b")[0].state,
        FriendState::Friend
    );
}

async fn scenario_reinvite_existing_friend_is_noop_success(repo: &dyn FriendsRepository) {
    repo.add("a", "b", ts(1)).await.expect("invite");
    repo.add("b", "a", ts(2)).await.expect("accept");
    assert_eq!(
        repo.add("a", "b", ts(3)).await.expect("re-invite friend"),
        FriendState::Friend
    );
    assert_eq!(
        repo.list("a").await.expect("list")[0].state,
        FriendState::Friend
    );
}

async fn scenario_remove_clears_both_sides_and_is_idempotent(repo: &dyn FriendsRepository) {
    repo.add("a", "b", ts(1)).await.expect("invite");
    repo.add("b", "a", ts(2)).await.expect("accept");
    assert!(repo.remove("a", "b").await.expect("remove"));
    assert!(repo.list("a").await.expect("list a").is_empty());
    assert!(repo.list("b").await.expect("list b").is_empty());
    // Removing again removes nothing.
    assert!(!repo.remove("a", "b").await.expect("idempotent remove"));
    // Direction-independent: removing from the other side is also a no-op now.
    assert!(!repo.remove("b", "a").await.expect("idempotent reverse"));
}

async fn scenario_block_is_one_sided_and_blocks_reinvites_and_unblocks(
    repo: &dyn FriendsRepository,
) {
    repo.add("a", "b", ts(1)).await.expect("invite");
    repo.block("b", "a", ts(2)).await.expect("block");

    // Blocker keeps a one-sided `blocked` edge; the blocked side's view is gone.
    let b_rows = repo.list("b").await.expect("list b");
    assert_eq!(b_rows.len(), 1);
    assert_eq!(b_rows[0].state, FriendState::Blocked);
    assert!(repo.list("a").await.expect("list a").is_empty());

    // A new invite while blocked is a conflict (from either direction).
    assert_eq!(
        repo.add("a", "b", ts(3))
            .await
            .expect_err("blocked re-invite")
            .category(),
        ErrorCategory::Conflict
    );
    assert_eq!(
        repo.add("b", "a", ts(3))
            .await
            .expect_err("blocker re-invite while blocked")
            .category(),
        ErrorCategory::Conflict
    );

    // Removing the block (unblock) lets invites flow again.
    assert!(repo.remove("b", "a").await.expect("unblock"));
    assert_eq!(
        repo.add("a", "b", ts(4))
            .await
            .expect("invite after unblock"),
        FriendState::InvitedSent
    );
}

async fn scenario_list_is_ordered_by_other_id(repo: &dyn FriendsRepository) {
    repo.add("me", "zed", ts(1)).await.expect("invite zed");
    repo.add("me", "amy", ts(2)).await.expect("invite amy");
    repo.add("me", "mia", ts(3)).await.expect("invite mia");
    let rows = repo.list("me").await.expect("list");
    let ids: Vec<&str> = rows.iter().map(|r| r.user_id.as_str()).collect();
    assert_eq!(ids, ["amy", "mia", "zed"]);
}

async fn scenario_relationships_are_independent_across_pairs(repo: &dyn FriendsRepository) {
    repo.add("a", "b", ts(1)).await.expect("a->b");
    repo.add("a", "c", ts(2)).await.expect("a->c");
    repo.block("a", "d", ts(3)).await.expect("a blocks d");
    let rows = repo.list("a").await.expect("list a");
    assert_eq!(rows.len(), 3);
    // Removing one pair leaves the others intact.
    assert!(repo.remove("a", "b").await.expect("remove a-b"));
    let rows = repo.list("a").await.expect("list a again");
    let ids: Vec<&str> = rows.iter().map(|r| r.user_id.as_str()).collect();
    assert_eq!(ids, ["c", "d"]);
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn FriendsRepository) -> ScenarioFuture<'_>,
);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn FriendsRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_invite_then_accept_becomes_mutual,
        scenario_reinvite_existing_friend_is_noop_success,
        scenario_remove_clears_both_sides_and_is_idempotent,
        scenario_block_is_one_sided_and_blocks_reinvites_and_unblocks,
        scenario_list_is_ordered_by_other_id,
        scenario_relationships_are_independent_across_pairs,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryFriendsRepository::new();
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
        let repo = db.friends_repository();

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
                "skipping Postgres friends contract: set DATABASE_URL or \
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
        let repo = db.friends_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}

// --- MongoDB run (opt-in via CITADEL_TEST_MONGODB_URL) ----------------------

mod mongodb {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, MongoDatabase};

    #[tokio::test]
    async fn mongodb_backend_satisfies_the_contract() {
        let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
            eprintln!("skipping MongoDB friends contract: CITADEL_TEST_MONGODB_URL is unset");
            return;
        };
        let db = MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + reconcile against a MongoDB replica set");
        let repo = db.friends_repository();

        for (name, run) in all_scenarios() {
            db.clear_friends_data_for_tests()
                .await
                .expect("clear friend edges between scenarios");
            eprintln!("mongodb scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}
