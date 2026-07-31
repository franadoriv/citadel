//! End-to-end tests for the richer Lua host API.
//!
//! Complements the unit tests by exercising the real wiring: a `LuaRuntime`
//! attached to a `Gateway`, lifecycle hooks driven through
//! `register_session`/`unregister_session`, and the periodic `LuaTickService`
//! spawned on a `Supervisor`, all delivering over the gateway's outbound sinks.

use std::sync::Arc;
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::observability::NodeMetrics;
use citadel::realtime::registry::{Outbound, SessionHandle};
use citadel::realtime::{Gateway, LuaTickService};
use citadel::runtime::LuaRuntime;
use citadel::transport::TransportKind;
use tokio::sync::mpsc;

const SCRIPT: &str = r#"
    citadel.on_join(function(ctx)
        citadel.log("join " .. ctx.sender)
        citadel.broadcast(10, string.pack(">I8", ctx.sender), false)
    end)
    citadel.on_leave(function(ctx)
        citadel.broadcast(11, string.pack(">I8", ctx.sender), false)
    end)
    citadel.on_tick(function(dt)
        citadel.broadcast(20, "tick", true)
    end)
"#;

fn gateway() -> Arc<Gateway> {
    let rt = LuaRuntime::from_source(SCRIPT, "lifecycle-tick-test", 100).expect("script loads");
    Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::new(rt)),
    ))
}

fn register(gw: &Gateway) -> (u64, mpsc::Receiver<Outbound>) {
    let id = gw.next_participant_id();
    let (tx, rx) = mpsc::channel(32);
    gw.register_session(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound: tx,
        identity: None,
    });
    (id.get(), rx)
}

#[tokio::test]
async fn lifecycle_hooks_fire_on_register_and_unregister() {
    let gw = gateway();
    // A joins (no peers). B joins: A is told to spawn B.
    let (_a, mut ra) = register(&gw);
    let (b_id, _rb) = register(&gw);
    let joined = ra.recv().await.expect("A learns B joined");
    assert_eq!(joined.envelope.kind, 10);
    assert_eq!(joined.envelope.body, b_id.to_be_bytes().to_vec());

    // B leaves: A is told to despawn B.
    gw.unregister_session(citadel::realtime::ParticipantId::from_raw(b_id));
    let left = ra.recv().await.expect("A learns B left");
    assert_eq!(left.envelope.kind, 11);
    assert_eq!(left.envelope.body, b_id.to_be_bytes().to_vec());
}

#[tokio::test]
async fn tick_service_drives_the_game_loop_until_shutdown() {
    let gw = gateway();
    // Register directly on the registry so only the tick delivers (no join noise).
    let id = gw.next_participant_id();
    let (tx, _reliable_rx) = mpsc::channel(64);
    let unreliable_rx = gw.registry().register(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound: tx,
        identity: None,
    });

    // 100 Hz tick (10ms period): a few ticks land well within the test timeout.
    let period = Duration::from_millis(10);
    let mut supervisor = Supervisor::new();
    supervisor.spawn(LuaTickService::new(
        Arc::clone(&gw),
        period,
        period,
        Duration::from_millis(20),
    ));

    // The periodic loop must deliver on_tick broadcasts.
    let first = tokio::time::timeout(Duration::from_secs(2), unreliable_rx.recv())
        .await
        .expect("a tick arrives within the timeout");
    assert_eq!(first.envelope.kind, 20);
    assert_eq!(first.envelope.body, b"tick".to_vec());

    // A second tick confirms the loop is periodic, not a one-shot.
    let second = tokio::time::timeout(Duration::from_secs(2), unreliable_rx.recv())
        .await
        .expect("a second tick arrives");
    assert_eq!(second.envelope.kind, 20);

    // Cancellation stops the loop cleanly.
    tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
        .await
        .expect("shutdown completes")
        .expect("clean shutdown");
}
