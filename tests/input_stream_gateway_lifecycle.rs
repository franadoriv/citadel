#![allow(clippy::unwrap_used, clippy::panic)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use citadel::observability::NodeMetrics;
use citadel::realtime::registry::{Outbound, ParticipantIdentity, SessionHandle};
use citadel::realtime::{
    Gateway, InputStreamControllerConfig, ParticipantId, RoomLabel, TransformHub,
    TransformHubConfig,
};
use citadel::runtime::{
    BridgeCommandSink, BridgeQuotas, DEFAULT_DEADLINE_MS, Decision, GameScriptReadiness,
    InputOutcome, LifecycleHook, LuaRuntime, NativeMatchContext, NativeMatchLifecycleHook,
    NormalizedEventBatch, OutboundCommand, RoomSpec, RpcOutcome, Runtime, RuntimeIntrospection,
};
use citadel::session::SessionId;
use citadel::storage::UserId;
use citadel::time::TimestampMillis;
use citadel::transport::TransportKind;
use citadel_wire::Envelope;
use citadel_wire::authoritative_input::{
    AuthoritativeInputDisposition, CapabilityAcceptance, CapabilityOffer, InputReceipt,
    InputStreamControl, SequencedInput,
};
use citadel_wire::protocol::{
    KIND_AUTHORITATIVE_INPUT, KIND_CAPABILITY_ACCEPTANCE, KIND_CAPABILITY_OFFER,
    KIND_INPUT_STREAM_CONTROL, KIND_ROOM_LEAVE,
};
use citadel_wire::room::RoomLeave;
use tokio::sync::mpsc;

fn readiness() -> Arc<GameScriptReadiness> {
    let readiness = Arc::new(GameScriptReadiness::new(TimestampMillis::from_unix_millis(
        0,
    )));
    readiness.record_loaded(
        "sha256:input-stream-test",
        TimestampMillis::from_unix_millis(1),
    );
    readiness
}

fn authoritative_gateway() -> (Gateway, Arc<GameScriptReadiness>) {
    let readiness = readiness();
    let runtime: Arc<dyn Runtime> = Arc::new(
        LuaRuntime::from_source("", "input-stream-test", DEFAULT_DEADLINE_MS)
            .expect("embedded Lua runtime"),
    );
    let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("transform hub"));
    (
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
            .with_transform_hub(hub)
            .with_script_readiness(Arc::clone(&readiness))
            .with_bridge(BridgeQuotas::default(), HashSet::new()),
        readiness,
    )
}

fn identity(name: &str) -> ParticipantIdentity {
    ParticipantIdentity {
        user_id: UserId::new(format!("user-{name}")).expect("test user id"),
        session_id: SessionId::new(format!("session-{name}")).expect("test session id"),
        expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
    }
}

fn register_authenticated(
    gateway: &Gateway,
    name: &str,
) -> (ParticipantId, mpsc::Receiver<Outbound>) {
    let participant = gateway.next_participant_id();
    let (outbound, receiver) = mpsc::channel(32);
    gateway.register_session(SessionHandle {
        id: participant,
        kind: TransportKind::WebSocket,
        outbound,
        identity: Some(identity(name)),
    });
    (participant, receiver)
}

fn next_offer(receiver: &mut mpsc::Receiver<Outbound>) -> CapabilityOffer {
    let outbound = receiver
        .try_recv()
        .expect("authenticated registration offers capability");
    assert_eq!(outbound.envelope.kind, KIND_CAPABILITY_OFFER);
    CapabilityOffer::decode(&outbound.envelope.body).expect("offer has canonical exact body")
}

fn accept(gateway: &Gateway, participant: ParticipantId, offer: CapabilityOffer) {
    assert_eq!(
        gateway.handle_inbound(
            participant,
            &Envelope::new(
                KIND_CAPABILITY_ACCEPTANCE,
                CapabilityAcceptance::from_offer(offer).encode(),
            ),
        ),
        0,
        "capability controls never relay or reach runtime",
    );
}

fn bind_authoritative(
    gateway: &Gateway,
    readiness: &GameScriptReadiness,
    participant: ParticipantId,
    name: &str,
) -> u64 {
    let binding = readiness.gate().expect("server readiness owns the binding");
    gateway
        .join_or_create_room_bound(participant, name, Some(binding), || {
            RoomLabel::with_map(name)
        })
        .expect("server-authorized authoritative admission")
        .0
}

fn next_advertise(
    receiver: &mut mpsc::Receiver<Outbound>,
) -> (
    u64,
    u64,
    citadel_wire::authoritative_input::InputStreamToken,
) {
    let outbound = receiver
        .try_recv()
        .expect("accepted authoritative admission advertises");
    assert_eq!(outbound.envelope.kind, KIND_INPUT_STREAM_CONTROL);
    match InputStreamControl::decode(&outbound.envelope.body).expect("canonical control") {
        InputStreamControl::Advertise {
            match_id,
            stream_id,
            token,
        } => (match_id, stream_id, token),
        InputStreamControl::Revoke { .. } => panic!("expected advertise"),
    }
}

fn stream_input(
    token: citadel_wire::authoritative_input::InputStreamToken,
    sequence: u64,
    body: &[u8],
) -> Envelope {
    Envelope::new(
        KIND_AUTHORITATIVE_INPUT,
        SequencedInput {
            stream_token: token,
            sequence,
            original_custom_kind: 900,
            body: body.to_vec(),
        }
        .encode()
        .expect("test input encodes"),
    )
}

#[derive(Default)]
struct BatchProbe {
    batches: Mutex<Vec<NormalizedEventBatch>>,
}

impl BatchProbe {
    fn take(&self) -> Vec<NormalizedEventBatch> {
        std::mem::take(&mut *self.batches.lock().expect("probe lock"))
    }
}

impl Runtime for BatchProbe {
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
        _: NativeMatchLifecycleHook,
        _: NativeMatchContext,
        _: std::time::Duration,
    ) -> Vec<OutboundCommand> {
        Vec::new()
    }
    fn supports_native_match_lifecycle(&self) -> bool {
        true
    }
    fn deliver_event_batch(&self, batch: NormalizedEventBatch) {
        self.batches.lock().expect("probe lock").push(batch);
    }
    fn tick(&self, _: std::time::Duration, _: std::time::Duration) -> Vec<OutboundCommand> {
        Vec::new()
    }
    fn call_rpc(&self, _: u64, _: Option<&str>, _: &str, _: &[u8]) -> RpcOutcome {
        RpcOutcome::Err("unavailable".to_owned())
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
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_millis(DEFAULT_DEADLINE_MS)
    }
    fn introspect(&self) -> RuntimeIntrospection {
        RuntimeIntrospection {
            source: "batch-probe".to_owned(),
            reloadable: false,
            deadline_ms: DEFAULT_DEADLINE_MS,
            rpcs: Vec::new(),
            message_kinds: Vec::new(),
            hooks: Vec::new(),
        }
    }
}

#[test]
fn capability_offer_requires_a_canonical_one_use_echo_from_its_authenticated_participant() {
    let metrics = Arc::new(NodeMetrics::new());
    let readiness = readiness();
    let runtime = Arc::new(BatchProbe::default());
    let gateway = Gateway::with_metrics_and_runtime(
        Arc::clone(&metrics),
        Some(Arc::clone(&runtime) as Arc<dyn Runtime>),
    )
    .with_transform_hub(Arc::new(
        TransformHub::new(TransformHubConfig::default()).expect("transform hub"),
    ))
    .with_script_readiness(Arc::clone(&readiness))
    .with_bridge(BridgeQuotas::default(), HashSet::new());
    let (first, mut first_rx) = register_authenticated(&gateway, "first");
    let (second, mut second_rx) = register_authenticated(&gateway, "second");
    let first_offer = next_offer(&mut first_rx);
    let second_offer = next_offer(&mut second_rx);
    let before = metrics.snapshot();

    accept(&gateway, second, first_offer);
    let mut forged = CapabilityAcceptance::from_offer(first_offer).encode();
    forged[2] ^= 0xff;
    assert_eq!(
        gateway.handle_inbound(first, &Envelope::new(KIND_CAPABILITY_ACCEPTANCE, forged)),
        0
    );
    assert!(first_rx.try_recv().is_err() && second_rx.try_recv().is_err());
    assert!(runtime.take().is_empty());
    assert_eq!(
        metrics.snapshot().messages_in_total,
        before.messages_in_total
    );

    accept(&gateway, first, first_offer);
    accept(&gateway, first, first_offer);
    let first_room = bind_authoritative(&gateway, &readiness, first, "first-room");
    let (match_id, _, _) = next_advertise(&mut first_rx);
    assert_eq!(match_id, first_room);
    assert!(
        second_rx.try_recv().is_err(),
        "other participant cannot consume or inherit an offer"
    );
    assert!(
        gateway.handle_inbound(
            first,
            &Envelope::new(
                KIND_CAPABILITY_ACCEPTANCE,
                CapabilityAcceptance::from_offer(second_offer).encode()
            )
        ) == 0
    );
}

#[test]
fn leave_then_rejoin_requires_a_fresh_offer_and_never_resumes_prior_acceptance_or_token() {
    let (gateway, readiness) = authoritative_gateway();
    let (participant, mut receiver) = register_authenticated(&gateway, "rejoin");
    let offer = next_offer(&mut receiver);
    accept(&gateway, participant, offer);
    let first_room = bind_authoritative(&gateway, &readiness, participant, "first");
    let (_, first_stream, first_token) = next_advertise(&mut receiver);

    gateway.handle_inbound(
        participant,
        &Envelope::new(
            KIND_ROOM_LEAVE,
            RoomLeave {
                room_id: first_room,
            }
            .encode(),
        ),
    );
    match InputStreamControl::decode(&receiver.try_recv().expect("leave revokes").envelope.body)
        .expect("control")
    {
        InputStreamControl::Revoke {
            match_id,
            stream_id,
        } => assert_eq!((match_id, stream_id), (first_room, first_stream)),
        InputStreamControl::Advertise { .. } => panic!("leave must revoke first"),
    }
    let second_room = bind_authoritative(&gateway, &readiness, participant, "second");
    let second_offer = next_offer(&mut receiver);
    assert_ne!(
        second_offer.challenge(),
        offer.challenge(),
        "rejoin gets a fresh one-use offer"
    );
    assert!(
        receiver.try_recv().is_err(),
        "prior acceptance cannot produce another bearer lease"
    );

    accept(&gateway, participant, offer);
    assert!(
        receiver.try_recv().is_err(),
        "a replayed pre-leave offer remains unusable"
    );
    accept(&gateway, participant, second_offer);
    let (match_id, second_stream, second_token) = next_advertise(&mut receiver);
    assert_eq!(match_id, second_room);
    assert_ne!(second_stream, first_stream);
    assert_ne!(second_token, first_token);
    gateway.handle_inbound(participant, &stream_input(first_token, 1, b"stale"));
    assert_eq!(
        gateway.queued_authoritative_input_count(),
        0,
        "prior bearer cannot resume after rejoin"
    );
}

#[test]
fn forged_replayed_wrong_participant_and_generation_acceptances_have_no_runtime_or_metric_side_effects()
 {
    let metrics = Arc::new(NodeMetrics::new());
    let gateway = Gateway::with_metrics(Arc::clone(&metrics));
    let (first, mut first_rx) = register_authenticated(&gateway, "generation-first");
    let (other, mut other_rx) = register_authenticated(&gateway, "generation-other");
    let stale_generation_offer = next_offer(&mut first_rx);
    let _other_offer = next_offer(&mut other_rx);
    let (replacement_tx, mut replacement_rx) = mpsc::channel(32);
    gateway.register_session(SessionHandle {
        id: first,
        kind: TransportKind::WebSocket,
        outbound: replacement_tx,
        identity: Some(identity("generation-replacement")),
    });
    let current_offer = next_offer(&mut replacement_rx);
    let before = metrics.snapshot();

    accept(&gateway, other, stale_generation_offer);
    accept(&gateway, first, stale_generation_offer);
    let mut malformed = CapabilityAcceptance::from_offer(current_offer).encode();
    malformed.push(0);
    assert_eq!(
        gateway.handle_inbound(first, &Envelope::new(KIND_CAPABILITY_ACCEPTANCE, malformed)),
        0
    );
    assert!(
        first_rx.try_recv().is_err()
            && other_rx.try_recv().is_err()
            && replacement_rx.try_recv().is_err()
    );
    assert_eq!(
        metrics.snapshot().messages_in_total,
        before.messages_in_total,
        "wrong participant, stale generation, and malformed echoes are control-plane no-ops"
    );

    accept(&gateway, first, current_offer);
    accept(&gateway, first, current_offer);
    assert_eq!(
        metrics.snapshot().messages_in_total,
        before.messages_in_total,
        "successful and replayed controls remain outside traffic accounting"
    );
}

#[test]
fn authoritative_receipt_is_sent_only_after_a_fenced_validated_decision_materializes() {
    let readiness = readiness();
    let runtime = Arc::new(BatchProbe::default());
    let gateway = Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::clone(&runtime) as Arc<dyn Runtime>),
    )
    .with_transform_hub(Arc::new(
        TransformHub::new(TransformHubConfig::default()).expect("transform hub"),
    ))
    .with_input_stream_config(InputStreamControllerConfig::new(8), 8)
    .with_script_readiness(Arc::clone(&readiness))
    .with_bridge(BridgeQuotas::default(), HashSet::new());
    let (participant, mut receiver) = register_authenticated(&gateway, "receipt");
    let offer = next_offer(&mut receiver);
    accept(&gateway, participant, offer);
    let room_id = bind_authoritative(&gateway, &readiness, participant, "receipt-room");
    let (match_id, stream_id, token) = next_advertise(&mut receiver);
    assert_eq!(match_id, room_id);

    gateway.handle_inbound(participant, &stream_input(token, 1, b"opaque"));
    gateway.tick(
        std::time::Duration::from_millis(16),
        std::time::Duration::from_millis(5),
    );
    assert!(
        receiver.try_recv().is_err(),
        "queueing and bridge issue do not send a premature receipt"
    );
    let batch = runtime
        .take()
        .pop()
        .expect("fixed tick issued bridge batch");
    let mut answer = citadel::runtime::ScriptCommandBatch::answering(&batch);
    answer.input_outcomes.push(InputOutcome {
        event_id: batch.events[0].event_id,
        decision: Decision::Reject { reason_code: 7 },
        reply: Some(vec![0, 0xff, 0x80]),
    });
    let replay = answer.clone();
    BridgeCommandSink::deliver_command_batch(&gateway, answer);

    let outbound = receiver
        .try_recv()
        .expect("validated materialization emits receipt");
    assert_eq!(outbound.envelope.kind, KIND_AUTHORITATIVE_INPUT);
    assert_eq!(
        InputReceipt::decode(&outbound.envelope.body).expect("canonical receipt"),
        InputReceipt {
            match_id,
            stream_id,
            stream_token: token,
            acknowledged_sequence: 1,
            decided_sequence: 1,
            disposition: AuthoritativeInputDisposition::Rejected,
            authoritative_tick: batch.tick,
            correction: Some(vec![0, 0xff, 0x80]),
        }
    );
    BridgeCommandSink::deliver_command_batch(&gateway, replay);
    assert!(
        receiver.try_recv().is_err(),
        "a replayed bridge answer cannot emit a second receipt or advance its acknowledgement"
    );
}

#[test]
fn receipt_is_not_emitted_when_leave_fences_the_pending_decision() {
    let readiness = readiness();
    let runtime = Arc::new(BatchProbe::default());
    let gateway = Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::clone(&runtime) as Arc<dyn Runtime>),
    )
    .with_transform_hub(Arc::new(
        TransformHub::new(TransformHubConfig::default()).expect("transform hub"),
    ))
    .with_script_readiness(Arc::clone(&readiness))
    .with_bridge(BridgeQuotas::default(), HashSet::new());
    let (participant, mut receiver) = register_authenticated(&gateway, "receipt-fence");
    let offer = next_offer(&mut receiver);
    accept(&gateway, participant, offer);
    let room_id = bind_authoritative(&gateway, &readiness, participant, "receipt-fence-room");
    let (_, _, token) = next_advertise(&mut receiver);
    gateway.handle_inbound(participant, &stream_input(token, 1, b"opaque"));
    gateway.tick(
        std::time::Duration::from_millis(16),
        std::time::Duration::from_millis(5),
    );
    let batch = runtime.take().pop().expect("batch");
    gateway.handle_inbound(
        participant,
        &Envelope::new(KIND_ROOM_LEAVE, RoomLeave { room_id }.encode()),
    );
    let revoke = receiver.try_recv().expect("leave revoke");
    assert_eq!(revoke.envelope.kind, KIND_INPUT_STREAM_CONTROL);
    let fresh_offer = receiver
        .try_recv()
        .expect("leave refreshes capability offer");
    assert_eq!(fresh_offer.envelope.kind, KIND_CAPABILITY_OFFER);
    let mut answer = citadel::runtime::ScriptCommandBatch::answering(&batch);
    answer.input_outcomes.push(InputOutcome {
        event_id: batch.events[0].event_id,
        decision: Decision::Accept,
        reply: None,
    });
    BridgeCommandSink::deliver_command_batch(&gateway, answer);
    assert!(
        receiver.try_recv().is_err(),
        "stale materialization cannot emit a receipt on a retired stream"
    );
}

#[test]
fn legacy_unnegotiated_sessions_keep_product_neutral_custom_kind_behavior() {
    let (gateway, readiness) = authoritative_gateway();
    let participant = gateway.next_participant_id();
    let (outbound, mut receiver) = mpsc::channel(8);
    gateway.register_session(SessionHandle {
        id: participant,
        kind: TransportKind::WebSocket,
        outbound,
        identity: None,
    });
    let _room = bind_authoritative(&gateway, &readiness, participant, "legacy");
    assert!(
        receiver.try_recv().is_err(),
        "unauthenticated legacy transport receives no offer or bearer control"
    );
    assert_eq!(
        gateway.handle_inbound(
            participant,
            &Envelope::new(KIND_INPUT_STREAM_CONTROL, b"legacy".to_vec())
        ),
        0
    );
}
