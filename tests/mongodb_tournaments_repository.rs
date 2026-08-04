//! MongoDB durable tournament repository contract.

use citadel::config::DatabaseConfig;
use citadel::leaderboard_scheduler::ResetEpoch;
use citadel::repository::{
    Backend, CreateLeaderboardRequest, CreateTournamentRequest, MongoDatabase, Operator, SortOrder,
    TournamentState,
};
use citadel::time::{DurationMillis, TimestampMillis};
use mongodb::bson::doc;

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

#[tokio::test]
async fn mongo_tournament_settlement_copies_one_immutable_ranked_epoch_snapshot() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
        eprintln!("skipping MongoDB tournament repository: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let db = MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect MongoDB");
    db.clear_leaderboards_data_for_tests()
        .await
        .expect("reset leaderboards fixture");
    db.clear_leaderboard_reset_data_for_tests()
        .await
        .expect("reset scheduler fixture");
    for collection in [
        "tournament_results",
        "tournament_entries",
        "tournament_settlement_outbox",
        "tournaments",
    ] {
        db.database_for_tests()
            .collection::<mongodb::bson::Document>(collection)
            .delete_many(doc! {})
            .await
            .expect("reset tournament fixture");
    }
    let boards = db.leaderboards_repository();
    boards
        .create(
            CreateLeaderboardRequest {
                id: "scores".into(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: None,
            },
            ts(0),
        )
        .await
        .expect("board");
    boards
        .submit("scores", "alice", 10, 1, None, ts(21))
        .await
        .expect("alice score");
    boards
        .submit("scores", "bob", 20, 2, None, ts(22))
        .await
        .expect("bob score");

    let tournaments = db.tournaments_repository();
    tournaments
        .create(
            CreateTournamentRequest {
                id: "weekly".into(),
                leaderboard_id: "scores".into(),
                registration_opens_at: ts(10),
                registration_closes_at: ts(20),
                starts_at: ts(20),
                ends_at: ts(30),
            },
            ts(0),
        )
        .await
        .expect("tournament");
    tournaments
        .transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    tournaments
        .transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");

    let epoch = ResetEpoch::new("scores".into(), ts(30));
    let resets = db.leaderboard_reset_repository();
    let lease = resets
        .acquire_lease(
            "mongo-tournament-test",
            ts(0),
            DurationMillis::from_millis(100),
        )
        .await
        .expect("lease")
        .expect("owned");
    assert!(
        resets
            .claim_epoch(epoch.clone(), lease.fencing_token, ts(30))
            .await
            .expect("epoch")
    );
    assert!(
        tournaments
            .settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("settle")
    );
    assert!(
        !tournaments
            .settle_from_epoch("weekly", epoch.clone(), ts(32))
            .await
            .expect("replay")
    );
    assert_eq!(
        tournaments
            .pending_settlement_outbox(10)
            .await
            .expect("outbox"),
        vec![citadel::repository::TournamentSettlementOutboxRecord {
            tournament_id: "weekly".into(),
            epoch: epoch.clone(),
        }]
    );
    tournaments
        .acknowledge_settlement_outbox("weekly", &epoch)
        .await
        .expect("acknowledge outbox");
    assert!(
        tournaments
            .pending_settlement_outbox(10)
            .await
            .expect("acknowledged outbox")
            .is_empty()
    );
    assert_eq!(
        tournaments
            .results("weekly")
            .await
            .expect("results")
            .iter()
            .map(|result| (&result.user_id, result.rank))
            .collect::<Vec<_>>(),
        vec![(&"bob".to_owned(), 1), (&"alice".to_owned(), 2)]
    );
    assert_eq!(
        tournaments
            .get("weekly")
            .await
            .expect("tournament")
            .expect("present")
            .settled_epoch,
        Some(epoch)
    );
}
