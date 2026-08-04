//! Contract for durable, at-least-once tournament post-settlement delivery.

use std::sync::Arc;

use async_trait::async_trait;
use citadel::error::{AppError, AppResult};
use citadel::leaderboard_scheduler::ResetEpoch;
use citadel::repository::{
    CreateLeaderboardRequest, CreateTournamentRequest, InMemoryTournamentsRepository, Operator,
    SortOrder, SqliteDatabase, TournamentSettlementCallback, TournamentSettlementOutboxDispatcher,
    TournamentState, TournamentsRepository,
};
use citadel::time::TimestampMillis;

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

fn request() -> CreateTournamentRequest {
    CreateTournamentRequest {
        id: "weekly".to_owned(),
        leaderboard_id: "scores".to_owned(),
        registration_opens_at: ts(10),
        registration_closes_at: ts(20),
        starts_at: ts(20),
        ends_at: ts(30),
    }
}

async fn running_repository() -> InMemoryTournamentsRepository {
    let repository = InMemoryTournamentsRepository::new();
    repository.create(request(), ts(0)).await.expect("create");
    repository
        .transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    repository
        .transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");
    repository
}

#[derive(Default)]
struct RecordingCallback {
    deliveries: std::sync::Mutex<Vec<String>>,
    fail: bool,
}

#[async_trait]
impl TournamentSettlementCallback for RecordingCallback {
    async fn on_tournament_settled(
        &self,
        settlement: &citadel::repository::TournamentSettlementOutboxRecord,
    ) -> AppResult<()> {
        if self.fail {
            return Err(AppError::internal("reward processor unavailable"));
        }
        self.deliveries
            .lock()
            .expect("recording callback lock")
            .push(settlement.idempotency_key());
        Ok(())
    }
}

#[tokio::test]
async fn settlement_stages_one_durable_reward_callback_and_retries_it_with_a_stable_key() {
    let repository = Arc::new(running_repository().await);
    let epoch = ResetEpoch::new("scores".to_owned(), ts(30));

    assert!(
        repository
            .settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("settle and stage outbox")
    );
    assert!(
        !repository
            .settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("replayed settlement")
    );

    let staged = repository
        .pending_settlement_outbox(10)
        .await
        .expect("outbox");
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].tournament_id, "weekly");
    assert_eq!(staged[0].epoch, epoch);
    assert_eq!(staged[0].idempotency_key(), "weekly:30");

    let failing = Arc::new(RecordingCallback {
        deliveries: std::sync::Mutex::new(Vec::new()),
        fail: true,
    });
    let failing_dispatcher =
        TournamentSettlementOutboxDispatcher::new(Arc::clone(&repository), failing);
    assert_eq!(
        failing_dispatcher
            .dispatch_pending(10)
            .await
            .expect("dispatch"),
        0
    );
    assert_eq!(
        repository
            .pending_settlement_outbox(10)
            .await
            .expect("outbox")
            .len(),
        1
    );

    let callback = Arc::new(RecordingCallback::default());
    let dispatcher =
        TournamentSettlementOutboxDispatcher::new(Arc::clone(&repository), Arc::clone(&callback));
    assert_eq!(dispatcher.dispatch_pending(10).await.expect("retry"), 1);
    assert_eq!(
        callback
            .deliveries
            .lock()
            .expect("recording callback lock")
            .as_slice(),
        ["weekly:30"]
    );
    assert!(
        repository
            .pending_settlement_outbox(10)
            .await
            .expect("acknowledged")
            .is_empty()
    );
}

#[tokio::test]
async fn sqlite_settlement_commits_its_outbox_record_with_the_immutable_results() {
    let db = SqliteDatabase::connect(&citadel::config::DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..citadel::config::DatabaseConfig::default()
    })
    .await
    .expect("connect sqlite");
    db.leaderboards_repository()
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
        .expect("create leaderboard");
    let repository = db.tournaments_repository();
    repository.create(request(), ts(0)).await.expect("create");
    repository
        .transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open");
    repository
        .transition("weekly", TournamentState::Running, ts(20))
        .await
        .expect("start");
    let epoch = ResetEpoch::new("scores".to_owned(), ts(30));
    let resets = db.leaderboard_reset_repository();
    let lease = resets
        .acquire_lease(
            "settlement-outbox-test",
            ts(0),
            citadel::time::DurationMillis::from_millis(100),
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
        repository
            .settle_from_epoch("weekly", epoch.clone(), ts(31))
            .await
            .expect("settle")
    );
    assert_eq!(
        repository
            .pending_settlement_outbox(10)
            .await
            .expect("durably staged outbox"),
        vec![citadel::repository::TournamentSettlementOutboxRecord {
            tournament_id: "weekly".to_owned(),
            epoch: epoch.clone(),
        }]
    );
    repository
        .acknowledge_settlement_outbox("weekly", &epoch)
        .await
        .expect("acknowledge");
    assert!(
        repository
            .pending_settlement_outbox(10)
            .await
            .expect("empty acknowledged outbox")
            .is_empty()
    );
}
