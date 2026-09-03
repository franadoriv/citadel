#![allow(clippy::unwrap_used, clippy::panic)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use citadel::config::LogsConfig;
use citadel::durable_logs::DurableLogWriter;
use citadel::ids::{NodeIdentity, SHORT_PREFIX_ID_LEN, valid_id};
use citadel::match_recorder::{MatchLogWriter, MatchRecorder, MatchRecorderError};
use citadel::observability::NodeMetrics;
use citadel::realtime::registry::{Outbound, ParticipantIdentity, SessionHandle};
use citadel::realtime::rooms::{JoinError, MATCH_ID_PREFIX};
use citadel::realtime::{Gateway, ParticipantId, RoomLabel};
use citadel::runtime::{
    GameScriptReadiness, LifecycleHook, NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
    NativeMatchContext, NativeMatchLifecycleHook, NativeMatchLifecycleUnavailable, OutboundCommand,
    RoomSpec, RpcOutcome, Runtime, RuntimeIntrospection,
};
use citadel::session::SessionId;
use citadel::storage::UserId;
use citadel::time::{Clock, SystemClock, TimestampMillis};
use citadel::transport::TransportKind;
use citadel_wire::Envelope;
use citadel_wire::protocol::{
    KIND_MATCHMAKER_MATCHED, KIND_ROOM_CREATE, KIND_ROOM_JOIN, KIND_ROOM_LEAVE, KIND_ROOM_REJECT,
    KIND_RPC_REQUEST, KIND_RPC_RESPONSE,
};
use citadel_wire::room::{RoomCreate, RoomJoin, RoomLeave, RoomReject};
use tokio::sync::mpsc;

#[test]
fn native_match_context_is_server_owned_and_carries_match_scope() {
    let context = NativeMatchContext {
        match_id: 7,
        lifecycle_generation: 3,
        clock_epoch: 11,
        tick: 19,
        participants: vec![2, 5],
        map: "arena".to_owned(),
        mode: "duel".to_owned(),
        max_players: 2,
        open: false,
        termination_reason: Some("server_closed".to_owned()),
    };

    assert_eq!(context.match_id, 7);
    assert_eq!(context.participants, vec![2, 5]);
    assert_eq!(context.termination_reason.as_deref(), Some("server_closed"));
}

struct RecordingRuntime {
    events: Mutex<Vec<(NativeMatchLifecycleHook, NativeMatchContext)>>,
    native_lifecycle_available: bool,
}

impl Default for RecordingRuntime {
    fn default() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            native_lifecycle_available: true,
        }
    }
}

impl Runtime for RecordingRuntime {
    fn dispatch(&self, _: u64, _: Option<&str>, _: u16, _: &[u8]) -> Vec<OutboundCommand> {
        Vec::new()
    }

    fn dispatch_lifecycle(
        &self,
        _: LifecycleHook,
        _: u64,
        _: Option<&str>,
    ) -> Vec<OutboundCommand> {
        Vec::new()
    }

    fn dispatch_match_lifecycle(
        &self,
        hook: NativeMatchLifecycleHook,
        context: NativeMatchContext,
        _: Duration,
    ) -> Vec<OutboundCommand> {
        self.events.lock().unwrap().push((hook, context));
        Vec::new()
    }

    fn supports_native_match_lifecycle(&self) -> bool {
        self.native_lifecycle_available
    }

    fn tick(&self, _: Duration, _: Duration) -> Vec<OutboundCommand> {
        Vec::new()
    }

    fn call_rpc(&self, _: u64, _: Option<&str>, _: &str, _: &[u8]) -> RpcOutcome {
        RpcOutcome::Err("unused".to_owned())
    }

    fn call_room_create(&self, _: u64, _: Option<&str>, _: &[u8]) -> Option<RoomSpec> {
        None
    }

    fn call_room_join(&self, _: u64, _: Option<&str>, _: u64) -> bool {
        true
    }

    fn has_tick_handler(&self) -> bool {
        false
    }

    fn budget(&self) -> Duration {
        Duration::from_millis(10)
    }

    fn introspect(&self) -> RuntimeIntrospection {
        RuntimeIntrospection {
            source: "recording".to_owned(),
            reloadable: false,
            deadline_ms: 10,
            rpcs: Vec::new(),
            message_kinds: Vec::new(),
            hooks: Vec::new(),
        }
    }
}

fn register(gateway: &Gateway) -> ParticipantId {
    let id = gateway.next_participant_id();
    let (outbound, _rx) = mpsc::channel(8);
    gateway.register_session(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound,
        identity: None,
    });
    id
}

fn register_authenticated(
    gateway: &Gateway,
    user_id: &str,
) -> (ParticipantId, mpsc::Receiver<Outbound>) {
    let id = gateway.next_participant_id();
    let (outbound, receiver) = mpsc::channel(8);
    gateway.register_session(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound,
        identity: Some(ParticipantIdentity {
            user_id: UserId::new(user_id).unwrap(),
            session_id: SessionId::new(format!("native-match-{user_id}")).unwrap(),
            expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
        }),
    });
    (id, receiver)
}

/// A gateway wired to a recorder whose writer has no repository behind it: the
/// queue is the observable, which is exactly what the funnel must fill.
fn recorded_gateway() -> (Gateway, Arc<MatchRecorder>) {
    let recorder = Arc::new(MatchRecorder::new(Arc::new(DurableLogWriter::new(
        Arc::new(NodeIdentity::new("native-match-node")),
        LogsConfig::default(),
    ))));
    let gateway = Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::new(RecordingRuntime::default())),
    )
    .with_match_recorder(Arc::clone(&recorder));
    (gateway, recorder)
}

fn matchmaker_rpc(request_id: u64, method: &str, body: serde_json::Value) -> Envelope {
    Envelope::new(
        KIND_RPC_REQUEST,
        citadel_wire::protocol::encode_rpc_request(request_id, method, body.to_string().as_bytes()),
    )
}

fn response_json(receiver: &mut mpsc::Receiver<Outbound>) -> serde_json::Value {
    let response = receiver.try_recv().expect("synchronous RPC response");
    assert_eq!(response.envelope.kind, KIND_RPC_RESPONSE);
    let response = citadel_wire::protocol::decode_rpc_response(&response.envelope.body)
        .expect("decode RPC response");
    assert_eq!(response.status, 0);
    serde_json::from_slice(response.payload).expect("JSON RPC payload")
}

#[test]
fn unsupported_native_lifecycle_rejects_room_create_before_match_creation() {
    let runtime = Arc::new(RecordingRuntime {
        events: Mutex::new(Vec::new()),
        native_lifecycle_available: false,
    });
    let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
    readiness.record_loaded("sha256:unsupported", Clock::now(&SystemClock));
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()))
            .with_script_readiness(readiness);
    let participant = gateway.next_participant_id();
    let (outbound, mut receiver) = mpsc::channel(8);
    gateway.register_session(SessionHandle {
        id: participant,
        kind: TransportKind::WebSocket,
        outbound,
        identity: None,
    });

    gateway.handle_inbound(
        participant,
        &Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: b"blocked".to_vec(),
            }
            .encode(),
        ),
    );

    let rejection = receiver.try_recv().expect("visible room-create rejection");
    assert_eq!(rejection.envelope.kind, KIND_ROOM_REJECT);
    let rejection = RoomReject::decode(&rejection.envelope.body).expect("decode room rejection");
    assert_eq!(rejection.request_kind, KIND_ROOM_CREATE);
    assert_eq!(rejection.reason, NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE);
    assert!(
        gateway.room_snapshot().is_empty(),
        "a runtime without native lifecycle frames must not create a room"
    );
    assert!(
        runtime.events.lock().unwrap().is_empty(),
        "no lifecycle callback may be silently dropped after a room is created"
    );
}

#[test]
fn unsupported_native_lifecycle_refuses_trusted_room_boundaries_before_mutation() {
    let runtime = Arc::new(RecordingRuntime {
        events: Mutex::new(Vec::new()),
        native_lifecycle_available: false,
    });
    let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
    readiness.record_loaded("sha256:unsupported", Clock::now(&SystemClock));
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()))
            .with_script_readiness(readiness);
    let participant = register(&gateway);

    assert_eq!(
        gateway.create_room(RoomLabel::with_map("blocked")),
        Err(NativeMatchLifecycleUnavailable),
        "trusted creation must expose a typed lifecycle refusal"
    );
    assert_eq!(
        gateway.join_room(participant, 1),
        Err(JoinError::NativeMatchLifecycleUnavailable),
        "trusted admission must expose the same typed lifecycle refusal"
    );
    assert_eq!(
        gateway.join_or_create_room(participant, "blocked", || RoomLabel::with_map("blocked")),
        Err(JoinError::NativeMatchLifecycleUnavailable),
        "trusted join-or-create must refuse before it can create or admit"
    );
    assert!(gateway.room_snapshot().is_empty());
    assert!(runtime.events.lock().unwrap().is_empty());
}

#[test]
fn gateway_drives_native_match_lifecycle_without_global_lifecycle_regression() {
    let runtime = Arc::new(RecordingRuntime::default());
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()));
    let first = register(&gateway);
    let second = register(&gateway);

    let room = gateway
        .create_room(RoomLabel {
            map: "arena".to_owned(),
            mode: "duel".to_owned(),
            max_players: 2,
            open: true,
        })
        .expect("recording runtime supports lifecycle");
    gateway.join_room(first, room).unwrap();
    gateway.join_room(second, room).unwrap();
    gateway.tick(Duration::from_millis(16), Duration::from_millis(10));
    gateway.unregister_session(second);
    gateway.close_match(room);

    let events = runtime.events.lock().unwrap();
    let hooks: Vec<_> = events.iter().map(|(hook, _)| *hook).collect();
    assert_eq!(
        hooks,
        vec![
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Tick,
            NativeMatchLifecycleHook::Leave,
            NativeMatchLifecycleHook::Leave,
            NativeMatchLifecycleHook::Ended,
        ]
    );
    assert!(events.iter().all(|(_, context)| context.match_id == room));
    assert_eq!(events[3].1.participants, vec![first.get(), second.get()]);
    assert_eq!(
        events.last().unwrap().1.termination_reason.as_deref(),
        Some("server_closed")
    );
}

#[test]
fn room_protocol_create_join_and_idempotent_rejoin_dispatch_native_lifecycle_once() {
    let runtime = Arc::new(RecordingRuntime::default());
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()));
    let creator = register(&gateway);
    let joiner = register(&gateway);

    gateway.handle_inbound(
        creator,
        &Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: b"protocol-room".to_vec(),
            }
            .encode(),
        ),
    );
    let room_id = gateway.room_snapshot().pop().expect("created room").id;
    let join = Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode());
    gateway.handle_inbound(joiner, &join);
    gateway.handle_inbound(joiner, &join);

    let events = runtime.events.lock().unwrap();
    assert_eq!(
        events.iter().map(|(hook, _)| *hook).collect::<Vec<_>>(),
        vec![
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Join,
        ],
        "ROOM_CREATE and ROOM_JOIN must use the same one-shot lifecycle transitions as trusted helpers"
    );
    assert_eq!(
        events.last().expect("join event").1.participants,
        vec![creator.get(), joiner.get()]
    );
}

#[test]
fn trusted_move_and_final_departure_emit_leave_then_end_once() {
    let runtime = Arc::new(RecordingRuntime::default());
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()));
    let participant = register(&gateway);

    let first = gateway
        .create_room(RoomLabel::with_map("first"))
        .expect("recording runtime supports lifecycle");
    gateway.join_room(participant, first).expect("first join");
    let second = gateway
        .create_room(RoomLabel::with_map("second"))
        .expect("recording runtime supports lifecycle");
    gateway
        .join_room(participant, second)
        .expect("move into second room");
    gateway.handle_inbound(
        participant,
        &Envelope::new(KIND_ROOM_LEAVE, RoomLeave { room_id: second }.encode()),
    );

    let events = runtime.events.lock().unwrap();
    assert_eq!(
        events.iter().map(|(hook, _)| *hook).collect::<Vec<_>>(),
        vec![
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Leave,
            NativeMatchLifecycleHook::Ended,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Leave,
            NativeMatchLifecycleHook::Ended,
        ]
    );
    assert_eq!(
        events[5].1.termination_reason.as_deref(),
        Some("final_departure")
    );
    assert_eq!(
        events[9].1.termination_reason.as_deref(),
        Some("final_departure")
    );
}

#[test]
fn matchmaker_birth_and_admission_dispatch_native_lifecycle_once() {
    let runtime = Arc::new(RecordingRuntime::default());
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()));
    let (alice, mut alice_rx) = register_authenticated(&gateway, "alice");
    let (bob, mut bob_rx) = register_authenticated(&gateway, "bob");
    let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });

    gateway.handle_inbound(alice, &matchmaker_rpc(1, "matchmaker.add", request.clone()));
    let _ = response_json(&mut alice_rx);
    gateway.handle_inbound(bob, &matchmaker_rpc(2, "matchmaker.add", request));
    let _ = response_json(&mut bob_rx);

    let alice_handoff = alice_rx.try_recv().expect("alice handoff");
    let bob_handoff = bob_rx.try_recv().expect("bob handoff");
    assert_eq!(alice_handoff.envelope.kind, KIND_MATCHMAKER_MATCHED);
    assert_eq!(bob_handoff.envelope.kind, KIND_MATCHMAKER_MATCHED);
    let alice_handoff: serde_json::Value =
        serde_json::from_slice(&alice_handoff.envelope.body).unwrap();
    let bob_handoff: serde_json::Value =
        serde_json::from_slice(&bob_handoff.envelope.body).unwrap();

    for (participant, receiver, handoff, request_id) in [
        (alice, &mut alice_rx, alice_handoff, 3),
        (bob, &mut bob_rx, bob_handoff, 4),
    ] {
        gateway.handle_inbound(
            participant,
            &matchmaker_rpc(
                request_id,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": handoff["ticket_id"],
                    "join_token": handoff["join_token"],
                }),
            ),
        );
        let _ = response_json(receiver);
        let _ = receiver.try_recv().expect("ROOM_JOINED after acceptance");
    }

    assert_eq!(
        runtime
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(hook, _)| *hook)
            .collect::<Vec<_>>(),
        vec![
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Join,
        ]
    );
}

#[test]
fn the_gateway_opens_and_closes_a_durable_record_for_every_match() {
    let (gateway, recorder) = recorded_gateway();
    let first = register(&gateway);
    let second = register(&gateway);

    let room = gateway
        .create_room(RoomLabel::with_map("arena"))
        .expect("recording runtime supports lifecycle");
    let match_id = recorder
        .match_id_of(room)
        .expect("Created binds the room to its durable match before any handler runs");
    assert!(valid_id(&match_id, MATCH_ID_PREFIX, SHORT_PREFIX_ID_LEN));
    assert_eq!(
        gateway
            .room_snapshot()
            .first()
            .map(|snapshot| snapshot.match_id.clone()),
        Some(match_id.clone()),
        "the room carries the identity its record is keyed by"
    );
    assert_eq!(
        recorder.writer().queued_total(),
        1,
        "room birth queues exactly one open"
    );

    gateway.join_room(first, room).unwrap();
    gateway.join_room(second, room).unwrap();
    let entry = recorder.entry(room).expect("the match is still open");
    assert_eq!(entry.match_id, match_id, "a live match never changes key");
    assert_eq!(entry.join_total, 2);
    assert_eq!(
        entry.peak_participants, 2,
        "the watermark counts local plus remote membership, not the context's participants"
    );

    gateway.close_match(room);
    assert_eq!(
        recorder.match_id_of(room),
        None,
        "Ended releases the directory row only after the handler returned"
    );
    assert_eq!(
        recorder.writer().queued_total(),
        2,
        "the close joins the open; no lifecycle transition queues anything else"
    );
}

#[test]
fn every_room_is_recorded_under_its_own_identity_and_final_departure_closes_it() {
    let (gateway, recorder) = recorded_gateway();
    let participant = register(&gateway);

    let first = gateway
        .create_room(RoomLabel::with_map("first"))
        .expect("recording runtime supports lifecycle");
    let second = gateway
        .create_room(RoomLabel::with_map("second"))
        .expect("recording runtime supports lifecycle");
    let first_id = recorder.match_id_of(first).expect("first is tracked");
    let second_id = recorder.match_id_of(second).expect("second is tracked");
    assert_ne!(
        first_id, second_id,
        "a per-process room counter is not an identity; the minted id is"
    );
    assert_eq!(recorder.len(), 2);

    gateway.join_room(participant, first).unwrap();
    gateway.join_room(participant, second).unwrap();
    assert_eq!(
        recorder.match_id_of(first),
        None,
        "emptying a room is a final departure, and the server closes its record"
    );
    assert_eq!(
        recorder.match_id_of(second).as_deref(),
        Some(second_id.as_str()),
        "the room the participant moved into stays open"
    );
    assert_eq!(
        recorder.writer().queued_total(),
        3,
        "two opens and the one close"
    );
}

#[test]
fn a_script_result_is_stamped_only_while_the_server_owned_match_is_open() {
    let (gateway, recorder) = recorded_gateway();
    let participant = register(&gateway);
    let room = gateway
        .create_room(RoomLabel::with_map("arena"))
        .expect("recording runtime supports lifecycle");
    gateway.join_room(participant, room).unwrap();

    let writer = MatchLogWriter::new(Arc::clone(&recorder));
    writer
        .set_result(Some(room), r#"{"winner":"kitsune"}"#.to_owned())
        .expect("an open match accepts its own result");
    assert_eq!(
        recorder
            .entry(room)
            .and_then(|entry| entry.result_json)
            .as_deref(),
        Some(r#"{"winner":"kitsune"}"#),
        "the result is held until the server writes the close it belongs to"
    );

    gateway.close_match(room);
    assert_eq!(
        writer.set_result(Some(room), "{}".to_owned()),
        Err(MatchRecorderError::NoActiveMatch),
        "game code can neither reopen a closed match nor rewrite its record"
    );
    assert_eq!(
        writer.set_result(None, "{}".to_owned()),
        Err(MatchRecorderError::NoActiveMatch),
        "a result outside a match-scoped callback has no row to land on"
    );
}

#[test]
fn a_gateway_without_a_recorder_drives_the_lifecycle_unchanged() {
    let runtime = Arc::new(RecordingRuntime::default());
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime.clone()));
    let participant = register(&gateway);
    let room = gateway
        .create_room(RoomLabel::with_map("arena"))
        .expect("recording runtime supports lifecycle");
    gateway.join_room(participant, room).unwrap();
    for snapshot in gateway.room_snapshot() {
        assert!(
            valid_id(&snapshot.match_id, MATCH_ID_PREFIX, SHORT_PREFIX_ID_LEN),
            "a room always mints an identity, even where nothing records it"
        );
    }
    gateway.close_match(room);

    assert_eq!(
        runtime
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(hook, _)| *hook)
            .collect::<Vec<_>>(),
        vec![
            NativeMatchLifecycleHook::Created,
            NativeMatchLifecycleHook::Started,
            NativeMatchLifecycleHook::Join,
            NativeMatchLifecycleHook::Leave,
            NativeMatchLifecycleHook::Ended,
        ],
        "durable recording is additive: a node with no durable store is byte for byte unchanged"
    );
}
