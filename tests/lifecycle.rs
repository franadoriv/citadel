//! Integration test for the async service lifecycle.
//!
//! Verifies the public contract a transport listener will rely on: a supervised
//! async service starts, serves until the shared cancellation token fires, and
//! is joined cleanly by `Supervisor::shutdown`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use citadel::lifecycle::{AsyncService, CancellationToken, Supervisor};

struct EchoLoop {
    serving: Arc<AtomicBool>,
}

impl AsyncService for EchoLoop {
    fn name(&self) -> &str {
        "echo-loop"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> citadel::AppResult<()> {
        self.serving.store(true, Ordering::SeqCst);
        // Simulate a serve loop that only exits on cancellation.
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
        self.serving.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn supervised_service_serves_until_shutdown() {
    let serving = Arc::new(AtomicBool::new(false));
    let mut sup = Supervisor::new();
    sup.spawn(EchoLoop {
        serving: serving.clone(),
    });

    // Let the loop start serving.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(serving.load(Ordering::SeqCst), "service should be serving");

    // Shutdown cancels the shared token and joins the task.
    let result = tokio::time::timeout(Duration::from_secs(2), sup.shutdown())
        .await
        .expect("shutdown should complete promptly");
    result.expect("clean shutdown");
    assert!(
        !serving.load(Ordering::SeqCst),
        "service should have stopped serving"
    );
}

#[tokio::test]
async fn external_token_can_trigger_shutdown() {
    let mut sup = Supervisor::new();
    let token = sup.cancel_token();
    sup.spawn(EchoLoop {
        serving: Arc::new(AtomicBool::new(false)),
    });
    // Cancelling via an external clone of the token must stop the service.
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), sup.shutdown())
        .await
        .expect("shutdown completes")
        .expect("clean shutdown");
}
