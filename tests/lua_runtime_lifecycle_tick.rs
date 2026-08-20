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
use citadel::realtime::{Gateway, LuaTickService, RoomLabel};
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
    local match_events = {}
    citadel.on_match_created(function(ctx) table.insert(match_events, "created") end)
    citadel.on_match_started(function(ctx) table.insert(match_events, "started") end)
    citadel.on_match_join(function(ctx) table.insert(match_events, "join") end)
    citadel.on_match_tick(function(ctx)
        table.insert(match_events, "tick")
        citadel.broadcast(97, table.concat(match_events, ","), false)
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
async fn native_match_lifecycle_hooks_follow_production_room_order() {
    let gw = gateway();
    let (first, mut first_rx) = register(&gw);
    let (second, _second_rx) = register(&gw);
    // Registering the second session exercises the unrelated global on_join
    // fixture hook; discard that setup notification before asserting match scope.
    let _ = first_rx.try_recv().expect("global join setup notification");
    let room = gw
        .create_room(RoomLabel::with_map("lua-lifecycle"))
        .expect("match-capable Lua creates a room");
    gw.join_room(citadel::realtime::ParticipantId::from_raw(first), room)
        .expect("first joins");
    gw.join_room(citadel::realtime::ParticipantId::from_raw(second), room)
        .expect("second joins");

    gw.tick(Duration::from_millis(16), Duration::from_millis(100));
    let lifecycle = loop {
        let outbound = first_rx.recv().await.expect("tick callback broadcast");
        if outbound.envelope.kind == 97 {
            break outbound;
        }
        assert_eq!(
            outbound.envelope.kind, 20,
            "only the fixture's global tick may precede match scope"
        );
    };
    assert_eq!(lifecycle.envelope.kind, 97);
    assert_eq!(
        lifecycle.envelope.body,
        b"created,started,join,join,tick".to_vec(),
        "Lua receives Created -> Started -> Join exactly once per production transition"
    );
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
