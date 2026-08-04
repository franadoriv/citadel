use std::sync::Arc;

use async_trait::async_trait;
use citadel::AppResult;
use citadel::leaderboard_scheduler::{
    InMemoryLeaderboardResetRepository, LeaderboardResetCallback, LeaderboardResetRepository,
    LeaderboardResetSchedulerService, RuntimeLeaderboardResetCallback, SchedulerFencingToken,
};
use citadel::repository::{
    CreateLeaderboardRequest, InMemoryLeaderboardsRepository, LeaderboardsRepository, Operator,
    SortOrder,
};
use citadel::runtime::LuaRuntime;
use citadel::time::{DurationMillis, TimestampMillis};
use citadel::{App, Config};

#[derive(Default)]
struct RecordingCallback {
    calls: std::sync::Mutex<Vec<(String, u64, u64)>>,
}

#[async_trait]
impl LeaderboardResetCallback for RecordingCallback {
    async fn on_leaderboard_reset(
        &self,
        epoch: &citadel::leaderboard_scheduler::ResetEpoch,
        fencing_token: SchedulerFencingToken,
    ) -> AppResult<()> {
        self.calls.lock().expect("calls mutex").push((
            epoch.leaderboard_id.clone(),
            epoch.due_at.unix_millis(),
            fencing_token.get(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn scheduler_service_discovers_due_reset_claims_bounded_epochs_and_dispatches_outbox() {
    let leaderboards = Arc::new(InMemoryLeaderboardsRepository::new());
    leaderboards
        .create(
            CreateLeaderboardRequest {
                id: "minute".to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: Some("0 * * * * *".to_owned()),
            },
            TimestampMillis::from_unix_millis(0),
        )
        .await
        .expect("create scheduled board");
    let resets = Arc::new(InMemoryLeaderboardResetRepository::new());
    let callback = Arc::new(RecordingCallback::default());
    let service = LeaderboardResetSchedulerService::new(
        leaderboards,
        Arc::clone(&resets),
        callback.clone(),
        "node-a".to_owned(),
        DurationMillis::from_millis(30_000),
        1,
        16,
    );

    let run = service
        .run_once(
            TimestampMillis::from_unix_millis(120_000),
            TimestampMillis::from_unix_millis(0),
        )
        .await
        .expect("scheduler pass");

    assert_eq!(run.claimed_epochs.len(), 1, "catch-up is bounded");
    assert_eq!(run.claimed_epochs[0].leaderboard_id, "minute");
    assert_eq!(run.delivered_callbacks, 1);
    assert_eq!(
        *callback.calls.lock().expect("calls mutex"),
        vec![("minute".to_owned(), 60_000, 1)]
    );
    assert!(resets.pending_outbox(16).await.expect("outbox").is_empty());
}

#[tokio::test]
async fn runtime_callback_bridge_invokes_the_registered_leaderboard_reset_hook() {
    let runtime = Arc::new(
        LuaRuntime::from_source(
            "citadel.on_leaderboard_reset(function(ctx) error('callback reached: ' .. ctx.leaderboard_id) end)",
            "leaderboard-reset.lua",
            100,
        )
        .expect("runtime loads"),
    );
    let callback = RuntimeLeaderboardResetCallback::new(runtime);
    let epoch = citadel::leaderboard_scheduler::ResetEpoch::new(
        "weekly".to_owned(),
        TimestampMillis::from_unix_millis(60_000),
    );

    let error = callback
        .on_leaderboard_reset(&epoch, SchedulerFencingToken::new(7))
        .await
        .expect_err("registered runtime callback is invoked");

    assert!(
        error
            .message()
            .contains("leaderboard reset callback failed")
    );
}

#[tokio::test]
async fn app_exposes_a_backend_owned_leaderboard_reset_repository() {
    let app = App::new(Config::default());
    let repository = app.leaderboard_reset_repository();

    let lease = repository
        .acquire_lease(
            app.node_id(),
            TimestampMillis::from_unix_millis(1),
            DurationMillis::from_millis(1_000),
        )
        .await
        .expect("scheduler repository is available")
        .expect("local node acquires empty repository lease");

    assert_eq!(lease.node_id, app.node_id());
}
