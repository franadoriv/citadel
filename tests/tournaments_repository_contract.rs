//! Contract tests for the smallest durable tournament lifecycle.

use citadel::config::DatabaseConfig;
use citadel::error::ErrorCategory;
use citadel::leaderboard_scheduler::ResetEpoch;
use citadel::repository::{
    CreateLeaderboardRequest, CreateTournamentRequest, InMemoryTournamentsRepository, Operator,
    SortOrder, SqliteDatabase, TournamentState, TournamentsRepository,
};
use citadel::time::TimestampMillis;

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

fn request(id: &str, leaderboard_id: &str) -> CreateTournamentRequest {
    CreateTournamentRequest {
        id: id.to_owned(),
        leaderboard_id: leaderboard_id.to_owned(),
        registration_opens_at: ts(10),
        registration_closes_at: ts(20),
        starts_at: ts(20),
        ends_at: ts(30),
    }
}

#[tokio::test]
async fn lifecycle_registration_and_epoch_settlement_are_durable_and_idempotent() {
    let repo = InMemoryTournamentsRepository::new();
    let created = repo
        .create(request("weekly", "scores"), ts(0))
        .await
        .expect("create");
    assert_eq!(created.state, TournamentState::Draft);

    let open = repo
        .transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    assert_eq!(open.state, TournamentState::RegistrationOpen);
    repo.register("weekly", "alice", ts(11))
        .await
        .expect("register alice");
    repo.register("weekly", "bob", ts(12))
        .await
        .expect("register bob");
    repo.transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");

    let epoch = ResetEpoch::new("scores".to_owned(), ts(30));
    assert!(
        repo.settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("settle")
    );
    assert!(
        !repo
            .settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("settlement replay")
    );

    let settled = repo
        .get("weekly")
        .await
        .expect("get")
        .expect("durable tournament");
    assert_eq!(settled.state, TournamentState::Completed);
    assert_eq!(settled.settled_epoch, Some(epoch));
    assert_eq!(repo.entries("weekly").await.expect("entries").len(), 2);
}

#[tokio::test]
async fn settlement_rejects_an_epoch_for_another_leaderboard_without_mutating_state() {
    let repo = InMemoryTournamentsRepository::new();
    repo.create(request("weekly", "scores"), ts(0))
        .await
        .expect("create");
    repo.transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    repo.transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");

    let error = repo
        .settle_from_epoch(
            "weekly",
            ResetEpoch::new("other".to_owned(), ts(30)),
            ts(31),
        )
        .await
        .expect_err("wrong leaderboard epoch");
    assert_eq!(error.category(), ErrorCategory::Conflict);
    assert_eq!(
        repo.get("weekly")
            .await
            .expect("get")
            .expect("exists")
            .state,
        TournamentState::Running
    );
}

#[tokio::test]
async fn sqlite_adapter_persists_lifecycle_and_rejects_an_unclaimed_epoch() {
    let db = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate sqlite");
    let leaderboards = db.leaderboards_repository();
    leaderboards
        .create(
            CreateLeaderboardRequest {
                id: "scores".to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: None,
            },
            ts(0),
        )
        .await
        .expect("create bound leaderboard");
    leaderboards
        .submit("scores", "alice", 100, 0, None, ts(25))
        .await
        .expect("seed settlement snapshot");
    let repo = db.tournaments_repository();
    repo.create(request("weekly", "scores"), ts(0))
        .await
        .expect("create");
    repo.transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    repo.register("weekly", "alice", ts(11))
        .await
        .expect("register");
    repo.transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");

    let epoch = ResetEpoch::new("scores".to_owned(), ts(30));
    assert_eq!(
        repo.settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect_err("unclaimed reset epoch")
            .category(),
        ErrorCategory::Conflict
    );

    let resets = db.leaderboard_reset_repository();
    let lease = resets
        .acquire_lease(
            "test-node",
            ts(0),
            citadel::time::DurationMillis::from_millis(100),
        )
        .await
        .expect("acquire reset lease")
        .expect("lease available");
    assert!(
        resets
            .claim_epoch(epoch.clone(), lease.fencing_token, ts(31))
            .await
            .expect("commit scheduler epoch")
    );
    assert!(
        repo.settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("atomic epoch-backed settlement")
    );
    assert_eq!(
        repo.results("weekly")
            .await
            .expect("immutable settlement results")
            .len(),
        1
    );
    assert_eq!(
        repo.get("weekly")
            .await
            .expect("read settled tournament")
            .expect("present")
            .settled_epoch,
        Some(epoch)
    );
}
