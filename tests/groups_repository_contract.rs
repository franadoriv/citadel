//! Contract tests for the groups repository.
//!
//! These assert the create/list/get/update/delete + membership/role-ladder +
//! last-superadmin-invariant semantics and durability that *any*
//! [`GroupsRepository`] implementation must honor. Each scenario is written
//! against `&dyn GroupsRepository` and is run against every backend:
//!
//! - always against [`InMemoryGroupsRepository`] (the reference impl),
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
//!   cargo test --test groups_repository_contract
//! ```
//!
//! Group ids are assigned durably by the database identity column (or the
//! in-memory counter), so scenarios never assume an absolute id value — they use
//! the id returned by `create` and only rely on ascending id ordering.

use citadel::error::ErrorCategory;
use citadel::repository::{
    CreateGroupRequest, GroupFilter, GroupRole, GroupsRepository, InMemoryGroupsRepository,
    UpdateGroupRequest,
};
use citadel::time::TimestampMillis;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn create_request(name: &str, creator: &str) -> CreateGroupRequest {
    CreateGroupRequest {
        name: name.to_string(),
        description: "a test group".to_string(),
        open: true,
        max_size: 0,
        creator_user_id: creator.to_string(),
        now: ts(1),
    }
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_create_makes_creator_a_superadmin(repo: &dyn GroupsRepository) {
    let group = repo
        .create(create_request("raiders", "u-1"))
        .await
        .expect("create");
    assert_eq!(group.name, "raiders");
    assert_eq!(group.member_count(), 1);
    let member = group.find_member("u-1").expect("creator is a member");
    assert_eq!(member.role, GroupRole::Superadmin);

    // Durability: a fresh read returns the same group + member roll.
    let fetched = repo.get(group.id).await.expect("get").expect("present");
    assert_eq!(fetched.name, "raiders");
    assert_eq!(
        fetched.find_member("u-1").expect("creator").role,
        GroupRole::Superadmin
    );
}

async fn scenario_create_enforces_unique_names(repo: &dyn GroupsRepository) {
    repo.create(create_request("raiders", "u-1"))
        .await
        .expect("first create");
    assert_eq!(
        repo.create(create_request("raiders", "u-2"))
            .await
            .expect_err("duplicate name")
            .category(),
        ErrorCategory::Conflict
    );
}

async fn scenario_get_missing_is_none_and_list_is_id_ordered(repo: &dyn GroupsRepository) {
    assert!(
        repo.get(9_999_999).await.expect("get missing").is_none(),
        "missing group is None"
    );
    let a = repo
        .create(create_request("alpha", "u-1"))
        .await
        .expect("a");
    let b = repo.create(create_request("beta", "u-2")).await.expect("b");
    assert!(a.id < b.id, "ids are monotonically increasing");

    let page = repo.list(&GroupFilter::default()).await.expect("list");
    assert_eq!(page.total, 2);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, a.id, "id-ordered ascending");
    assert_eq!(page.items[1].id, b.id);
}

async fn scenario_list_filters_by_substring_and_pages(repo: &dyn GroupsRepository) {
    repo.create(create_request("alpha-raiders", "u-1"))
        .await
        .expect("a");
    repo.create(create_request("beta-raiders", "u-2"))
        .await
        .expect("b");
    repo.create(create_request("gamma-scouts", "u-3"))
        .await
        .expect("c");

    let raiders = repo
        .list(&GroupFilter {
            name_contains: Some("raiders".to_string()),
            ..GroupFilter::default()
        })
        .await
        .expect("filter");
    assert_eq!(raiders.total, 2);
    assert_eq!(raiders.items.len(), 2);

    let paged = repo
        .list(&GroupFilter {
            limit: 1,
            offset: 1,
            ..GroupFilter::default()
        })
        .await
        .expect("page");
    assert_eq!(paged.total, 3, "total ignores paging");
    assert_eq!(paged.items.len(), 1);
}

async fn scenario_update_applies_only_provided_fields(repo: &dyn GroupsRepository) {
    let group = repo
        .create(CreateGroupRequest {
            max_size: 5,
            ..create_request("raiders", "u-1")
        })
        .await
        .expect("create");
    let updated = repo
        .update(
            group.id,
            UpdateGroupRequest {
                description: Some("new description".to_string()),
                open: Some(false),
                max_size: None,
            },
        )
        .await
        .expect("update");
    assert_eq!(updated.description, "new description");
    assert!(!updated.open);
    assert_eq!(updated.max_size, 5, "untouched field is unchanged");

    // Durable: re-read reflects the patch.
    let fetched = repo.get(group.id).await.expect("get").expect("present");
    assert_eq!(fetched.description, "new description");
    assert!(!fetched.open);

    // Updating a missing group is NotFound.
    assert_eq!(
        repo.update(9_999_999, UpdateGroupRequest::default())
            .await
            .expect_err("missing")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_delete_removes_group_and_is_reported(repo: &dyn GroupsRepository) {
    let group = repo
        .create(create_request("raiders", "u-1"))
        .await
        .expect("create");
    // Add a member so we also prove the cascade removes the membership roll.
    repo.add_member(group.id, "u-2", ts(2)).await.expect("add");

    assert!(repo.delete(group.id).await.expect("delete"), "removed");
    assert!(
        repo.get(group.id).await.expect("get").is_none(),
        "group gone"
    );
    assert!(
        !repo.delete(group.id).await.expect("idempotent"),
        "second delete removes nothing"
    );
}

async fn scenario_add_member_enforces_uniqueness_and_cap(repo: &dyn GroupsRepository) {
    let group = repo
        .create(CreateGroupRequest {
            max_size: 2,
            ..create_request("raiders", "u-1")
        })
        .await
        .expect("create");
    let group = repo.add_member(group.id, "u-2", ts(2)).await.expect("add");
    assert_eq!(group.member_count(), 2);

    // At the cap now.
    assert_eq!(
        repo.add_member(group.id, "u-3", ts(3))
            .await
            .expect_err("full")
            .category(),
        ErrorCategory::Conflict
    );
    // Duplicate member.
    assert_eq!(
        repo.add_member(group.id, "u-1", ts(4))
            .await
            .expect_err("duplicate")
            .category(),
        ErrorCategory::Conflict
    );
    // Missing group.
    assert_eq!(
        repo.add_member(9_999_999, "u-9", ts(5))
            .await
            .expect_err("missing group")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_promote_and_demote_walk_the_ladder(repo: &dyn GroupsRepository) {
    let group = repo
        .create(create_request("raiders", "u-1"))
        .await
        .expect("create");
    repo.add_member(group.id, "u-2", ts(2)).await.expect("add");

    let promoted = repo.promote(group.id, "u-2").await.expect("member->admin");
    assert_eq!(
        promoted.find_member("u-2").expect("member").role,
        GroupRole::Admin
    );
    let promoted = repo
        .promote(group.id, "u-2")
        .await
        .expect("admin->superadmin");
    assert_eq!(
        promoted.find_member("u-2").expect("member").role,
        GroupRole::Superadmin
    );
    // Already at the top.
    assert_eq!(
        repo.promote(group.id, "u-2")
            .await
            .expect_err("already top")
            .category(),
        ErrorCategory::Conflict
    );

    // Two superadmins now: demoting u-2 down the ladder works.
    let demoted = repo.demote(group.id, "u-2").await.expect("super->admin");
    assert_eq!(
        demoted.find_member("u-2").expect("member").role,
        GroupRole::Admin
    );
    let demoted = repo.demote(group.id, "u-2").await.expect("admin->member");
    assert_eq!(
        demoted.find_member("u-2").expect("member").role,
        GroupRole::Member
    );
    // Already at the bottom.
    assert_eq!(
        repo.demote(group.id, "u-2")
            .await
            .expect_err("already bottom")
            .category(),
        ErrorCategory::Conflict
    );
}

async fn scenario_last_superadmin_is_protected(repo: &dyn GroupsRepository) {
    let group = repo
        .create(create_request("raiders", "u-1"))
        .await
        .expect("create");
    // The founding, sole superadmin cannot be demoted or kicked.
    assert_eq!(
        repo.demote(group.id, "u-1")
            .await
            .expect_err("demote last superadmin")
            .category(),
        ErrorCategory::Conflict
    );
    assert_eq!(
        repo.kick_member(group.id, "u-1")
            .await
            .expect_err("kick last superadmin")
            .category(),
        ErrorCategory::Conflict
    );

    // With a second superadmin, one of them may be demoted/kicked.
    repo.add_member(group.id, "u-2", ts(2)).await.expect("add");
    repo.promote(group.id, "u-2").await.expect("->admin");
    repo.promote(group.id, "u-2").await.expect("->superadmin");
    let kicked = repo
        .kick_member(group.id, "u-2")
        .await
        .expect("second superadmin kicked");
    assert_eq!(kicked.superadmin_count(), 1);
    assert_eq!(kicked.member_count(), 1);
}

async fn scenario_missing_member_is_not_found(repo: &dyn GroupsRepository) {
    let group = repo
        .create(create_request("raiders", "u-1"))
        .await
        .expect("create");
    for op in ["kick", "promote", "demote"] {
        let result = match op {
            "kick" => repo.kick_member(group.id, "ghost").await,
            "promote" => repo.promote(group.id, "ghost").await,
            _ => repo.demote(group.id, "ghost").await,
        };
        assert_eq!(
            result.expect_err("missing member").category(),
            ErrorCategory::NotFound,
            "{op} of an absent member is NotFound"
        );
    }
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn GroupsRepository) -> ScenarioFuture<'_>,
);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn GroupsRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_create_makes_creator_a_superadmin,
        scenario_create_enforces_unique_names,
        scenario_get_missing_is_none_and_list_is_id_ordered,
        scenario_list_filters_by_substring_and_pages,
        scenario_update_applies_only_provided_fields,
        scenario_delete_removes_group_and_is_reported,
        scenario_add_member_enforces_uniqueness_and_cap,
        scenario_promote_and_demote_walk_the_ladder,
        scenario_last_superadmin_is_protected,
        scenario_missing_member_is_not_found,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryGroupsRepository::new();
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
        let repo = db.groups_repository();

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
                "skipping Postgres groups contract: set DATABASE_URL or \
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
        let repo = db.groups_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}
