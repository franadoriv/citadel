//! Live end-to-end tests for the match data plane: a real spawned worker
//! process (the same binary, `runtime-worker` subcommand) over the real
//! authenticated IPC transport (unix socket / named pipe), hosting real Lua
//! matches.

#![cfg(any(unix, windows))]

use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use citadel::config::RuntimeLanguage;
use citadel::runtime::{OutboundCommand, Runtime};
use citadel::runtime::external_worker::{
    ExternalWorkerRuntime, MatchCommandSink, WorkerScriptSpec,
};
use citadel::runtime::worker_data_protocol::MatchCloseReason;
use citadel::runtime::worker_supervisor::{
    RestartController, SupervisedWorker, WorkerDataPlaneBridge, WorkerSupervisionPolicy,
};

mod common;

/// Counter script: kind 1 answers with a per-match running count; kind 2 is a
/// non-yielding pure Lua loop bounded only by the engine's deadline hook.
const COUNTER_SCRIPT: &str = r#"
count = 0
citadel.on_message(1, function(ctx, body)
    count = count + 1
    citadel.send(ctx.sender, 99, tostring(count))
end)
citadel.on_message(2, function(ctx, body)
    while true do end
end)
"#;

struct RecordingSink {
    commands: Mutex<Vec<(u64, Vec<OutboundCommand>)>>,
    closed: Mutex<Vec<(u64, MatchCloseReason)>>,
}

impl RecordingSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            commands: Mutex::new(Vec::new()),
            closed: Mutex::new(Vec::new()),
        })
    }

    /// Bodies of `Send` commands observed for `room_id`, in arrival order.
    fn sent_bodies(&self, room_id: u64) -> Vec<Vec<u8>> {
        self.commands
            .lock()
            .expect("commands lock")
            .iter()
            .filter(|(room, _)| *room == room_id)
            .flat_map(|(_, commands)| commands.clone())
            .filter_map(|command| match command {
                OutboundCommand::Send { body, .. } => Some(body),
                _ => None,
            })
            .collect()
    }

    fn closed_rooms(&self) -> Vec<(u64, MatchCloseReason)> {
        self.closed.lock().expect("closed lock").clone()
    }
}

impl MatchCommandSink for RecordingSink {
    fn apply_match_commands(&self, room_id: u64, commands: Vec<OutboundCommand>) -> usize {
        let delivered = commands.len();
        self.commands
            .lock()
            .expect("commands lock")
            .push((room_id, commands));
        delivered
    }

    fn on_match_closed(&self, room_id: u64, reason: MatchCloseReason) {
        self.closed
            .lock()
            .expect("closed lock")
            .push((room_id, reason));
    }
}

struct ScriptDir {
    dir: PathBuf,
}

impl ScriptDir {
    fn create(label: &str, source: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("citadel-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("script dir");
        std::fs::write(dir.join("main.lua"), source).expect("main.lua");
        Self { dir }
    }

    fn entrypoint(&self) -> PathBuf {
        self.dir.join("main.lua")
    }
}

impl Drop for ScriptDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + deadline;
    while Instant::now() < until {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    check()
}

struct Harness {
    runtime: Arc<ExternalWorkerRuntime>,
    sink: Arc<RecordingSink>,
    controller: RestartController,
    _script: ScriptDir,
}

fn harness(label: &str, source: &str, deadline_ms: u64) -> Harness {
    let script = ScriptDir::create(label, source);
    let runtime = Arc::new(
        ExternalWorkerRuntime::load(WorkerScriptSpec {
            language: RuntimeLanguage::Lua,
            entrypoint: script.entrypoint(),
            deadline_ms,
            tick_ms: 10,
        })
        .expect("load adapter"),
    );
    let sink = RecordingSink::new();
    runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
    let controller = RestartController::new(
        PathBuf::from(env!("CARGO_BIN_EXE_citadel")),
        std::env::temp_dir(),
        WorkerSupervisionPolicy::default().with_restart_limit(3),
    )
    .with_data_plane(WorkerDataPlaneBridge::new(Arc::clone(&runtime)));
    Harness {
        runtime,
        sink,
        controller,
        _script: script,
    }
}

#[test]
fn external_worker_executes_isolated_lua_matches_over_live_ipc() {
    let mut harness = harness("data-plane-exec", COUNTER_SCRIPT, 100);
    let mut worker = harness.controller.start().expect("boot script worker");

    // Two events into match 1, one into match 2: per-match mlua states must
    // not share the counter global, end to end through the real process.
    harness.runtime.dispatch_in_room(7, None, 1, 1, b"");
    harness.runtime.dispatch_in_room(7, None, 1, 1, b"");
    harness.runtime.dispatch_in_room(8, None, 2, 1, b"");
    assert!(
        wait_until(Duration::from_secs(10), || {
            harness.sink.sent_bodies(1).len() >= 2 && !harness.sink.sent_bodies(2).is_empty()
        }),
        "worker must answer both matches over live IPC; got match1={:?} match2={:?}",
        harness.sink.sent_bodies(1),
        harness.sink.sent_bodies(2),
    );
    assert_eq!(
        harness.sink.sent_bodies(1),
        vec![b"1".to_vec(), b"2".to_vec()],
        "match 1 counts its own events"
    );
    assert_eq!(
        harness.sink.sent_bodies(2),
        vec![b"1".to_vec()],
        "match 2 starts from a fresh per-match state"
    );
    worker
        .shutdown(Duration::from_secs(5))
        .expect("orderly shutdown");
}

#[test]
fn non_yielding_lua_match_closes_only_itself_over_live_ipc() {
    // A short budget so each poisonous quantum burns quickly.
    let mut harness = harness("data-plane-deadline", COUNTER_SCRIPT, 30);
    let mut worker = harness.controller.start().expect("boot script worker");

    // Match 1 receives the non-yielding kind enough times to exhaust the
    // overrun policy (default limit 3); match 2 stays healthy throughout.
    for _ in 0..3 {
        harness.runtime.dispatch_in_room(7, None, 1, 2, b"");
    }
    harness.runtime.dispatch_in_room(8, None, 2, 1, b"");
    assert!(
        wait_until(Duration::from_secs(10), || {
            !harness.sink.closed_rooms().is_empty()
        }),
        "the non-yielding match must be closed end to end"
    );
    assert_eq!(
        harness.sink.closed_rooms(),
        vec![(1, MatchCloseReason::ServerError)],
        "only match 1 may close, as a server error"
    );
    // Match 2 keeps serving after its neighbor died: a fresh event still
    // gets answered by the same live worker.
    harness.runtime.dispatch_in_room(8, None, 2, 1, b"");
    assert!(
        wait_until(Duration::from_secs(10), || {
            harness.sink.sent_bodies(2).len() >= 2
        }),
        "match 2 must keep being served; got {:?}",
        harness.sink.sent_bodies(2),
    );
    assert_eq!(harness.sink.sent_bodies(2), vec![b"1".to_vec(), b"2".to_vec()]);
    // The worker itself stayed healthy: one bad match is match-local.
    worker
        .health_check(Duration::from_secs(5))
        .expect("worker stays healthy after a match-local closure");
    worker
        .shutdown(Duration::from_secs(5))
        .expect("orderly shutdown");
}

#[test]
fn worker_crash_replacement_never_resumes_matches() {
    let mut harness = harness("data-plane-crash", COUNTER_SCRIPT, 100);
    let mut active = Some(harness.controller.start().expect("boot script worker"));
    let first_worker = active.as_ref().expect("active worker").id();

    harness.runtime.dispatch_in_room(7, None, 1, 1, b"");
    harness.runtime.dispatch_in_room(7, None, 1, 1, b"");
    assert!(
        wait_until(Duration::from_secs(10), || {
            harness.sink.sent_bodies(1).len() >= 2
        }),
        "pre-crash events must be answered"
    );

    // Kill the real process out from under the supervisor and recover: the
    // replacement completes a fresh authenticated handshake (fresh secret)
    // and a fresh data plane (fresh epoch).
    active.as_mut().expect("active worker").kill().expect("kill");
    let mut replaced = false;
    for _ in 0..5 {
        let available = harness
            .controller
            .monitor_health(&mut active, Duration::from_secs(2))
            .expect("recovery must boot a replacement");
        let current = active.as_ref().map(SupervisedWorker::id);
        if available && current.is_some_and(|id| id != first_worker) {
            replaced = true;
            break;
        }
    }
    assert!(replaced, "the crash must be recovered with a new process");

    // The replacement starts empty: the same room id is opened fresh, so its
    // counter restarts at 1 — nothing about the dead match was resumed.
    harness.runtime.dispatch_in_room(7, None, 1, 1, b"");
    assert!(
        wait_until(Duration::from_secs(10), || {
            harness.sink.sent_bodies(1).len() >= 3
        }),
        "the replacement must serve the reopened match; got {:?}",
        harness.sink.sent_bodies(1),
    );
    assert_eq!(
        harness.sink.sent_bodies(1),
        vec![b"1".to_vec(), b"2".to_vec(), b"1".to_vec()],
        "the reopened match starts from fresh per-match state"
    );
    active
        .as_mut()
        .expect("replacement worker")
        .shutdown(Duration::from_secs(5))
        .expect("orderly shutdown");
}

/// The full stack, end to end: a real WebSocket client joins a real gateway
/// whose runtime is the external-worker adapter, the script executes in a
/// real spawned worker process over the real IPC transport, and when the
/// client's match wedges in a non-yielding loop the client itself receives
/// the reliable `KIND_MATCH_CLOSED` carrying the requeue hint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn members_receive_match_closed_with_requeue_hint_over_live_ipc() {
    use citadel::observability::NodeMetrics;
    use citadel::realtime::{Authenticator, Gateway};
    use citadel::transport::codec::Envelope;
    use citadel::transport::websocket::WebSocketServer;
    use citadel_wire::protocol::{KIND_MATCH_CLOSED, KIND_ROOM_CREATE, KIND_ROOM_JOINED};
    use citadel_wire::room::{MATCH_CLOSE_REASON_SERVER_ERROR, MatchClosed, RoomCreate, RoomJoined};
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let script = ScriptDir::create("data-plane-full-stack", COUNTER_SCRIPT);
    let runtime = Arc::new(
        ExternalWorkerRuntime::load(WorkerScriptSpec {
            language: RuntimeLanguage::Lua,
            entrypoint: script.entrypoint(),
            deadline_ms: 30,
            tick_ms: 10,
        })
        .expect("load adapter"),
    );
    let gateway = Arc::new(Gateway::with_metrics_runtime_auth(
        Arc::new(NodeMetrics::new()),
        Some(Arc::clone(&runtime) as Arc<dyn Runtime>),
        Authenticator::guest_only(),
    ));
    runtime.attach_sink(Arc::downgrade(&gateway) as Weak<dyn MatchCommandSink>);

    // Boot the real worker process (blocking accept + handshake).
    let controller_runtime = Arc::clone(&runtime);
    let (mut controller, mut worker) = tokio::task::spawn_blocking(move || {
        let mut controller = RestartController::new(
            PathBuf::from(env!("CARGO_BIN_EXE_citadel")),
            std::env::temp_dir(),
            WorkerSupervisionPolicy::default().with_restart_limit(3),
        )
        .with_data_plane(WorkerDataPlaneBridge::new(controller_runtime));
        let worker = controller.start().expect("boot script worker");
        (controller, worker)
    })
    .await
    .expect("worker boots");

    // Real WebSocket transport on the same gateway.
    let ws_server = WebSocketServer::bind_with_gateway(
        std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        Arc::clone(&gateway),
    )
    .await
    .expect("bind ws");
    let ws_addr = ws_server.local_addr();
    let mut supervisor = citadel::lifecycle::Supervisor::new();
    supervisor.spawn(ws_server);

    let (mut ws, _resp) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(format!("ws://{ws_addr}/")),
    )
    .await
    .expect("ws connect did not time out")
    .expect("ws connected");
    common::ws_guest_handshake(&mut ws).await;

    // Create the match room; the gateway echoes ROOM_JOINED with its id.
    ws.send(Message::Binary(
        Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: b"Arena".to_vec(),
            }
            .encode(),
        )
        .encode_framed()
        .to_vec(),
    ))
    .await
    .expect("send room create");
    let joined = loop {
        let envelope = common::decode_one(&common::ws_next_binary(&mut ws).await);
        if envelope.kind == KIND_ROOM_JOINED {
            break RoomJoined::decode(&envelope.body).expect("room joined decodes");
        }
    };

    // First prove the script answers this member through the whole stack.
    ws.send(Message::Binary(
        Envelope::new(1, b"ping".to_vec()).encode_framed().to_vec(),
    ))
    .await
    .expect("send counter event");
    let answer = loop {
        let envelope = common::decode_one(&common::ws_next_binary(&mut ws).await);
        if envelope.kind == 99 {
            break envelope;
        }
    };
    assert_eq!(
        answer.body.as_ref(),
        b"1",
        "the per-match Lua counter answers over the full stack"
    );

    // Now wedge the match: enough non-yielding invocations to exhaust the
    // overrun policy. The member must receive the requeue-hinted close.
    for _ in 0..3 {
        ws.send(Message::Binary(
            Envelope::new(2, Vec::new()).encode_framed().to_vec(),
        ))
        .await
        .expect("send poison event");
    }
    let closed = loop {
        let envelope = common::decode_one(&common::ws_next_binary(&mut ws).await);
        if envelope.kind == KIND_MATCH_CLOSED {
            break MatchClosed::decode(&envelope.body).expect("match closed decodes");
        }
    };
    assert_eq!(closed.room_id, joined.room_id);
    assert_eq!(closed.reason, MATCH_CLOSE_REASON_SERVER_ERROR);
    assert!(
        closed.requeue_hint,
        "the member is prompted to requeue for a new match"
    );

    ws.close(None).await.ok();
    supervisor.shutdown().await.expect("ws shutdown");
    tokio::task::spawn_blocking(move || {
        worker
            .shutdown(Duration::from_secs(5))
            .expect("orderly worker shutdown");
        drop(controller);
    })
    .await
    .expect("worker shutdown");
}

#[test]
fn broken_script_fails_the_worker_bootstrap() {
    let mut harness = harness("data-plane-broken", "this is not lua(", 100);
    let Err(error) = harness.controller.start() else {
        unreachable!("a broken script must fail the boot, not every match");
    };
    // The worker exits before readiness, so the bootstrap surface reports a
    // protocol-level failure rather than a script traceback.
    assert!(
        matches!(
            error.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::InvalidData
                | std::io::ErrorKind::UnexpectedEof
        ),
        "unexpected bootstrap error: {error:?}"
    );
}
