//! PostgreSQL/CockroachDB durable tournament repository contract.

use citadel::config::DatabaseConfig;
use citadel::leaderboard_scheduler::ResetEpoch;
use citadel::repository::{
    CreateLeaderboardRequest, CreateTournamentRequest, Operator, PgDatabase, SortOrder,
    TournamentState,
};
use citadel::time::{DurationMillis, TimestampMillis};

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

fn url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
}

#[tokio::test]
async fn postgres_tournament_settlement_copies_one_immutable_ranked_epoch_snapshot() {
    let Some(url) = url() else {
        eprintln!("skipping PostgreSQL tournament repository: DATABASE_URL is unset");
        return;
    };
    let db = PgDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate PostgreSQL");
    db.reset_storage_for_tests().await.expect("reset fixture");
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
        .register("weekly", "alice", ts(11))
        .await
        .expect("entry");
    tournaments
        .transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");
    let epoch = ResetEpoch::new("scores".into(), ts(30));
    let lease = db
        .leaderboard_reset_repository()
        .acquire_lease("node", ts(0), DurationMillis::from_millis(100))
        .await
        .expect("lease")
        .expect("owned");
    assert!(
        db.leaderboard_reset_repository()
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
            .map(|r| (&r.user_id, r.rank))
            .collect::<Vec<_>>(),
        vec![(&"bob".to_owned(), 1), (&"alice".to_owned(), 2)]
    );
}
