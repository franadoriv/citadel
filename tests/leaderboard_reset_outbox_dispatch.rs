use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use citadel::error::{AppError, AppResult};
use citadel::leaderboard_scheduler::{
    InMemoryLeaderboardResetRepository, LeaderboardResetCallback, LeaderboardResetOutboxDispatcher,
    LeaderboardResetRepository, ResetEpoch, SchedulerFencingToken,
};
use citadel::time::{DurationMillis, TimestampMillis};

#[derive(Default)]
struct RetryingCallback {
    attempts: Mutex<usize>,
}

#[async_trait]
impl LeaderboardResetCallback for RetryingCallback {
    async fn on_leaderboard_reset(
        &self,
        _epoch: &ResetEpoch,
        _fencing_token: SchedulerFencingToken,
    ) -> AppResult<()> {
        let mut attempts = self.attempts.lock().expect("attempt mutex");
        *attempts += 1;
        if *attempts == 1 {
            return Err(AppError::internal("transient callback failure"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn outbox_dispatcher_retries_unacknowledged_callback_after_failure() {
    let repository = Arc::new(InMemoryLeaderboardResetRepository::new());
    let now = TimestampMillis::from_unix_millis(10);
    let lease = repository
        .acquire_lease("node-a", now, DurationMillis::from_millis(100))
        .await
        .expect("lease")
        .expect("initial lease");
    let epoch = ResetEpoch::new("daily".to_owned(), now);
    assert!(
        repository
            .claim_epoch(epoch.clone(), lease.fencing_token, now)
            .await
            .expect("stage outbox")
    );

    let callback = Arc::new(RetryingCallback::default());
    let dispatcher =
        LeaderboardResetOutboxDispatcher::new(Arc::clone(&repository), Arc::clone(&callback));

    assert_eq!(
        dispatcher.dispatch_pending(10).await.expect("first pass"),
        0
    );
    assert_eq!(
        repository.pending_outbox(10).await.expect("pending").len(),
        1
    );

    assert_eq!(
        dispatcher.dispatch_pending(10).await.expect("retry pass"),
        1
    );
    assert!(
        repository
            .pending_outbox(10)
            .await
            .expect("acknowledged")
            .is_empty()
    );
    assert_eq!(*callback.attempts.lock().expect("attempt mutex"), 2);
}
