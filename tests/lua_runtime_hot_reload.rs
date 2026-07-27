//! End-to-end tests for Lua script hot-reload.
//!
//! Exercises the real wiring: a file-backed `LuaRuntime` attached to a `Gateway`,
//! with the `LuaReloadService` and `LuaTickService` running together on a
//! `Supervisor`. Proves that editing `main.lua` swaps in new handlers live, that
//! a reload does not wedge the concurrent tick loop, and that a broken edit is
//! rejected while the previous script keeps serving over the gateway.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::observability::NodeMetrics;
use citadel::realtime::registry::{Outbound, SessionHandle};
use citadel::realtime::{Gateway, LuaReloadService, LuaTickService};
use citadel::runtime::{LuaRuntime, Runtime};
use citadel::transport::TransportKind;
use tokio::sync::mpsc;

/// A throwaway temp directory (no `tempfile` dependency), removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("citadel-hotreload-it-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        Self(dir)
    }

    fn main_lua(&self) -> PathBuf {
        self.0.join("main.lua")
    }

    fn write(&self, src: &str) {
        std::fs::write(self.main_lua(), src).expect("write main.lua");
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn register(gw: &Gateway) -> mpsc::Receiver<Outbound> {
    let id = gw.next_participant_id();
    let (tx, rx) = mpsc::channel(256);
    gw.registry().register(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound: tx,
        identity: None,
    });
    rx
}

/// Drain the channel until an envelope of `kind` is seen, or fail after a bound.
async fn wait_for_kind(rx: &mut mpsc::Receiver<Outbound>, kind: u16, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        // A timeout or a closed channel simply re-checks the deadline above; only
        // a matching envelope returns. Avoids `panic!`/`unwrap` (lint-clean).
        if let Ok(Some(out)) = tokio::time::timeout(remaining, rx.recv()).await
            && out.envelope.kind == kind
        {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn editing_the_script_swaps_handlers_without_wedging_the_tick() {
    let dir = TempDir::new();
    // v1: the tick broadcasts kind 20.
    dir.write(
        r#"
        citadel.on_tick(function(dt)
            citadel.broadcast(20, "v1", true)
        end)
    "#,
    );
    let runtime: Arc<dyn Runtime> = Arc::new(
        LuaRuntime::load(&dir.0, 100)
            .expect("loads")
            .expect("present"),
    );
    let gateway = Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::clone(&runtime)),
    ));

    let mut rx = register(&gateway);

    let period = Duration::from_millis(10);
    let mut supervisor = Supervisor::new();
    supervisor.spawn(LuaTickService::new(
        Arc::clone(&gateway),
        period,
        period,
        Duration::from_millis(20),
    ));
    supervisor.spawn(LuaReloadService::new(
        Arc::clone(&runtime),
        dir.main_lua(),
        Duration::from_millis(20),
    ));

    // The v1 tick loop is delivering kind 20.
    wait_for_kind(&mut rx, 20, "initial v1 tick (kind 20)").await;

    // Edit the tick to broadcast a new kind (21). The watcher must swap it in and
    // the tick loop must keep firing across the reload (not wedged).
    dir.write(
        r#"
        citadel.on_tick(function(dt)
            -- edited game loop, now emitting a different kind entirely
            citadel.broadcast(21, "v2", true)
        end)
    "#,
    );

    // Seeing kind 21 proves both: the reload took effect AND the tick loop
    // continued to run through the swap.
    wait_for_kind(&mut rx, 21, "post-reload v2 tick (kind 21)").await;

    tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
        .await
        .expect("shutdown completes")
        .expect("clean shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_edit_keeps_the_previous_script_serving_over_the_gateway() {
    let dir = TempDir::new();
    // v1: a message handler for kind 1 -> broadcast kind 2 body "ok".
    dir.write(
        r#"
        citadel.on_message(1, function(ctx, body)
            citadel.broadcast(2, "ok", false)
        end)
    "#,
    );
    let runtime: Arc<dyn Runtime> = Arc::new(
        LuaRuntime::load(&dir.0, 100)
            .expect("loads")
            .expect("present"),
    );
    let gateway = Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::clone(&runtime)),
    ));

    let sender = gateway.next_participant_id();
    let (stx, _srx) = mpsc::channel(8);
    gateway.registry().register(SessionHandle {
        id: sender,
        kind: TransportKind::WebSocket,
        outbound: stx,
        identity: None,
    });
    let mut peer_rx = register(&gateway);

    let mut supervisor = Supervisor::new();
    supervisor.spawn(LuaReloadService::new(
        Arc::clone(&runtime),
        dir.main_lua(),
        Duration::from_millis(20),
    ));
    // Let the watcher establish its baseline before we break the file.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A broken edit: the watcher observes it and its internal reload is rejected.
    dir.write("this is not lua ==");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The previous, valid handler still routes through the gateway.
    let delivered = gateway.handle_inbound(
        sender,
        &citadel::transport::Envelope::new(1, b"ping".to_vec()),
    );
    assert_eq!(
        delivered, 1,
        "the previous script keeps serving after a reject"
    );
    let out = peer_rx.recv().await.expect("peer receives");
    assert_eq!(out.envelope.kind, 2);
    assert_eq!(out.envelope.body, b"ok".to_vec());

    tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
        .await
        .expect("shutdown completes")
        .expect("clean shutdown");
}
