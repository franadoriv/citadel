//! Embedded game-logic runtime for Citadel (, MVP Phase 1).
//!
//! This module hosts an embedded Lua virtual machine that lets a `game/` scripts
//! folder handle inbound realtime messages instead of (or on top of) the built-in
//! relay. It is the smallest real "write your game logic in a script" slice.
//!
//! Design (see the task's decision log and `website/src/content/docs/reference/server-sdk/lua-runtime.md`):
//!
//! - The runtime is a **serialized, bounded, fallible command generator**. It owns
//!   no transport or registry state. [`LuaRuntime::dispatch`] runs the registered
//!   Lua handler for a message kind and returns a `Vec<`[`OutboundCommand`]`>`; the
//!   [`Gateway`](crate::realtime::Gateway) applies those commands to its
//!   `SessionRegistry` outside the Lua lock.
//! - Concurrency: `mlua`'s `Lua` is `!Sync`, so the VM lives behind a `Mutex` and
//!   every handler runs single-threaded under that lock. With the synchronous
//!   `Gateway::handle_inbound(..) -> usize` contract this is simpler and no more
//!   blocking than a dedicated actor thread would be for the MVP.
//! - Safety: a script error, a blown time budget, or even a Rust panic inside a
//!   callback is caught, logged, and turned into "no outbound commands". A bad
//!   script can never crash the node. A per-invocation deadline (an instruction
//!   hook) aborts a hung handler such as `while true do end`.
//!
//! Beyond message dispatch the host API exposes participant lifecycle hooks
//! (`citadel.on_join`/`on_leave`, invoked by the gateway on
//! register/unregister), a server game loop (`citadel.on_tick(dt)`, driven by a
//! periodic task at `runtime.tick_hz`), request/response RPC handlers
//! (`citadel.on_rpc(method, fn)`, invoked by the gateway on a `KIND_RPC_REQUEST`
//! via [`LuaRuntime::call_rpc`], which — unlike the fire-and-forget command
//! paths — RETURNS the handler's reply for the gateway to send back to the
//! caller only, correlated by request id), and `citadel.log`. Lifecycle, tick,
//! and RPC invocations reuse the same serialized lock, per-invocation deadline,
//! and error isolation as message dispatch, so a slow or erroring hook can never
//! wedge the node. Per-game state lives in Lua globals (a single shared VM).
//!
//! Language-neutral trajectory: this is Lua-only today, but the host API
//! (`on_message`/`on_join`/`on_leave`/`on_tick`/`broadcast`/`send`/`log`) and
//! the command model line up with the runtime contract
//! (`website/src/content/docs/reference/server-sdk/index.mdx`) so other languages can adopt the
//! same shape later.

pub mod bridge_protocol;
pub mod bridge_validator;
pub mod cache_lease;
pub mod cluster;
pub mod engine_host;
pub mod event_bus;
pub mod external_worker;
pub mod host_api_spec;
pub mod host_services;
pub mod http_endpoint;
#[cfg(feature = "runtime-js")]
pub mod js;
pub mod lua;
pub mod metrics;
pub mod outbound_http;
#[cfg(feature = "runtime-python")]
pub mod python;
#[cfg(feature = "runtime-python")]
mod python_bundle;
pub mod python_payload;
pub mod readiness;
pub mod shared_cache;
pub(crate) mod static_data;
pub mod text_policy;
#[cfg(unix)]
pub mod worker_bootstrap;
#[cfg(any(unix, windows))]
pub mod worker_data_plane;
pub mod worker_data_protocol;
pub mod worker_engine;
#[cfg(any(unix, windows))]
pub mod worker_ipc;
pub mod worker_protocol;
#[cfg(any(unix, windows))]
pub mod worker_supervisor;

use std::time::Duration;

use crate::error::AppResult;
use crate::leaderboard_scheduler::{ResetEpoch, SchedulerFencingToken};

pub use bridge_protocol::{
    BridgeCommandSink, BridgePhysicsOptions, BridgeRepField, BridgeRepValue, BridgeShape,
    BridgeTransform, Correction, Decision, FireIntent, GS_BRIDGE_PROTOCOL_VERSION, InputOutcome,
    MAX_BRIDGE_PAYLOAD_BYTES, NormalizedEvent, NormalizedEventBatch, NormalizedPayload, PersistOp,
    RewindHit, RewindQuery, RewindResult, ScriptCommand, ScriptCommandBatch,
};
pub use bridge_validator::{
    BatchRejection, BridgeMatchContext, BridgeQuotas, Capability, EventDraft, MAX_RESERVED_KIND,
    PendingBatchLedger, Quota, ValidatedBatch, ValidatedOutcome,
};
pub use event_bus::{
    MAX_RUNTIME_EVENTS_PER_INVOCATION, RuntimeEvent, RuntimeEventBus, RuntimeEventBusHandle,
    RuntimeEventEmitOutcome, RuntimeEventError, RuntimeEventPolicy, RuntimeEventPublisher,
    disabled_runtime_event_bus_handle, is_valid_runtime_event_name, runtime_event_bus,
    set_runtime_event_bus,
};
pub use host_api_spec::{HOST_API_SURFACE, HostApiCategory, HostApiFn, HostApiStatus};
pub use host_services::{
    DomainHost, FriendRowDto, ServiceDomainHost, StorageIndexObjectDto, StorageObjectDto,
    StorageWriteInput,
};
pub use http_endpoint::{
    RUNTIME_HTTP_ENDPOINT_PREFIX, RuntimeHttpAuth, RuntimeHttpEndpoint, RuntimeHttpEndpointError,
    RuntimeHttpEndpointPolicy, RuntimeHttpEndpointRateLimiter, RuntimeHttpMethod,
    RuntimeHttpOutcome, RuntimeHttpRequest, RuntimeHttpResponse,
};
#[cfg(feature = "runtime-js")]
pub use js::{JS_ENTRYPOINT, JsRuntime};
pub use lua::{
    DEFAULT_DEADLINE_MS, LifecycleHook, LuaRuntime, OutboundCommand, PhysicsOptions, ReloadOutcome,
    RoomSpec, RpcOutcome, RuntimeIntrospection,
};
pub use metrics::{
    RuntimeMetricError, RuntimeMetricSnapshot, RuntimeMetrics, is_valid_runtime_metric_name,
};
#[cfg(feature = "runtime-python")]
pub use python::PythonRuntime;
#[cfg(feature = "runtime-python")]
pub use python_bundle::{
    BundledPythonEnv, configure_bundled_python_runtime, detect_bundled_python_runtime,
};
pub use python_payload::{
    PayloadCache, PayloadFile, PayloadManifest, PythonPayloadError, VerifiedPayload,
};
#[cfg(any(unix, windows))]
pub use readiness::SupervisedWorkerSource;
pub use readiness::{
    DEFAULT_DEGRADED_HOLD, GameScriptReadiness, InProcessRuntimeSource, ReadinessSnapshot,
    ReadinessSource, RecoveryView, SCRIPT_UNAVAILABLE_MESSAGE, ScriptBinding, ScriptReadinessState,
    script_content_identity,
};
pub use shared_cache::{
    RuntimeCachePublisher, RuntimeSharedCache, RuntimeSharedCacheError, RuntimeSharedCacheHandle,
    RuntimeSharedCachePolicy, RuntimeSharedCacheValue, disabled_runtime_shared_cache_handle,
    runtime_shared_cache, set_runtime_shared_cache,
};
pub(crate) use static_data::DEFAULT_STATIC_DATA_MAX_FILE_BYTES;

/// Keep event-snapshot delivery within the same aggregate fan-out envelope as
/// one ordinary runtime invocation. Each subscriber has its own bounded sink,
/// but a full queue must not multiply that bound by the queue capacity.
pub(crate) fn append_runtime_event_commands(
    commands: &mut Vec<OutboundCommand>,
    event_commands: Vec<OutboundCommand>,
    source_label: &str,
) {
    const MAX_COMMANDS: usize = 1_024;
    const MAX_BODY_BYTES: usize = 1 << 20;

    let mut body_bytes = commands
        .iter()
        .map(outbound_command_body_bytes)
        .sum::<usize>();
    for command in event_commands {
        let command_bytes = outbound_command_body_bytes(&command);
        if commands.len() >= MAX_COMMANDS
            || body_bytes.saturating_add(command_bytes) > MAX_BODY_BYTES
        {
            tracing::warn!(
                script = %source_label,
                "runtime event commands exceeded the per-invocation aggregate limit; dropping remainder"
            );
            break;
        }
        body_bytes = body_bytes.saturating_add(command_bytes);
        commands.push(command);
    }
}

fn outbound_command_body_bytes(command: &OutboundCommand) -> usize {
    match command {
        OutboundCommand::Broadcast { body, .. } | OutboundCommand::Send { body, .. } => body.len(),
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Cross-engine authoritative-bridge marshaling.
//
// The embedded Lua adapter marshals normalized events into native tables and
// parses on_input decisions directly. The JS and Python adapters share one JSON
// bridge instead: the event is serialized here (a flat object whose field names
// match the Lua event table, so game code reads the same shape in every engine),
// handed to the engine's on_input dispatcher, and the handler's normalized
// decision comes back as JSON parsed here. All three adapters map the drained
// OutboundCommand sink to the fenced ScriptCommand twins with the one function
// below, so batch-level effects flow through the validator identically.
// ---------------------------------------------------------------------------

/// Serialize one normalized event into the flat JSON object handed to a JS or
/// Python `on_input` handler.
#[cfg(any(feature = "runtime-js", feature = "runtime-python"))]
pub(crate) fn bridge_event_json(batch: &NormalizedEventBatch, event: &NormalizedEvent) -> String {
    use serde_json::json;
    let mut value = json!({
        "event_id": event.event_id,
        "participant": event.participant,
        "user_id": event.user_id,
        "match_id": batch.match_id,
        "tick": batch.tick,
    });
    let map = value.as_object_mut().expect("json object");
    match &event.payload {
        NormalizedPayload::TransformInput {
            object_id,
            ownership_epoch,
            input_seq,
            sim_tick,
            dt,
            move_velocity,
            payload,
            fire,
        } => {
            map.insert("kind".into(), json!("transform_input"));
            map.insert("object_id".into(), json!(*object_id));
            map.insert("ownership_epoch".into(), json!(*ownership_epoch));
            map.insert("input_seq".into(), json!(*input_seq));
            map.insert("sim_tick".into(), json!(*sim_tick));
            map.insert("dt".into(), json!(*dt));
            map.insert("move_velocity".into(), json!(move_velocity));
            map.insert("payload".into(), json!(payload));
            map.insert("has_fire".into(), json!(fire.is_some()));
            if let Some(fire) = fire {
                map.insert(
                    "fire".into(),
                    json!({
                        "origin": fire.origin,
                        "direction": fire.direction,
                        "weapon": fire.weapon,
                    }),
                );
            }
        }
        NormalizedPayload::ActorStateReport {
            object_id,
            transform,
        } => {
            map.insert("kind".into(), json!("actor_state"));
            map.insert("object_id".into(), json!(*object_id));
            map.insert("transform".into(), bridge_transform_json(transform));
        }
        NormalizedPayload::ReplicatedVarWrite {
            object_id,
            class_id,
            result_id,
            fields,
            ..
        } => {
            map.insert("kind".into(), json!("replicated_var"));
            map.insert("object_id".into(), json!(*object_id));
            map.insert("class_id".into(), json!(*class_id));
            map.insert("result_id".into(), json!(*result_id));
            map.insert("field_count".into(), json!(fields.len()));
        }
        NormalizedPayload::SpawnRequest {
            archetype_id,
            transform,
        } => {
            map.insert("kind".into(), json!("spawn_request"));
            map.insert("archetype_id".into(), json!(*archetype_id));
            map.insert("transform".into(), bridge_transform_json(transform));
        }
        NormalizedPayload::MatchMessage { kind, body } => {
            map.insert("kind".into(), json!("message"));
            map.insert("message_kind".into(), json!(*kind));
            map.insert("body".into(), json!(body));
        }
        NormalizedPayload::ParticipantJoined => {
            map.insert("kind".into(), json!("join"));
        }
        NormalizedPayload::ParticipantLeft => {
            map.insert("kind".into(), json!("leave"));
        }
    }
    value.to_string()
}

#[cfg(any(feature = "runtime-js", feature = "runtime-python"))]
fn bridge_transform_json(transform: &BridgeTransform) -> serde_json::Value {
    serde_json::json!({
        "position": transform.position,
        "rotation": transform.rotation,
        "velocity": transform.velocity,
    })
}

/// Parse the normalized decision JSON a JS/Python `on_input` dispatcher returns
/// into an [`InputOutcome`]. The engine bootstrap normalizes the handler's
/// return (nil/true/"accept" ⇒ accept, false/"reject" ⇒ reject, object ⇒
/// verbatim) into `{ decision, reason_code?, reply?, transform? }` with an
/// array-based transform. Any parse failure fails the whole batch closed.
#[cfg(any(feature = "runtime-js", feature = "runtime-python"))]
pub(crate) fn bridge_input_outcome_from_json(
    event_id: u64,
    decision_json: &str,
) -> Result<InputOutcome, String> {
    #[derive(serde::Deserialize)]
    struct DecisionJson {
        decision: String,
        #[serde(default)]
        reason_code: u16,
        #[serde(default)]
        reply: Option<String>,
        #[serde(default)]
        transform: Option<TransformJson>,
    }
    #[derive(serde::Deserialize)]
    struct TransformJson {
        position: [f32; 3],
        rotation: [f32; 4],
        #[serde(default)]
        velocity: [f32; 3],
    }
    let parsed: DecisionJson = serde_json::from_str(decision_json).map_err(|e| e.to_string())?;
    let reply = parsed.reply.map(String::into_bytes);
    let decision = match parsed.decision.as_str() {
        "accept" => Decision::Accept,
        "reject" => Decision::Reject {
            reason_code: parsed.reason_code,
        },
        "correct" => {
            let transform = parsed
                .transform
                .ok_or_else(|| "a correct decision requires a transform".to_string())?;
            Decision::Correct {
                correction: Correction::Transform(BridgeTransform {
                    position: transform.position,
                    rotation: transform.rotation,
                    velocity: transform.velocity,
                }),
            }
        }
        other => return Err(format!("on_input returned an unknown decision {other:?}")),
    };
    Ok(InputOutcome {
        event_id,
        decision,
        reply,
    })
}

/// Map one drained [`OutboundCommand`] to its fenced [`ScriptCommand`] twin, so
/// every adapter's batch-level effects (broadcast/send/actor) flow through the
/// validator. Rep/persist/schedule/multicast have no `OutboundCommand` today and
/// are emitted through dedicated APIs later.
pub(crate) fn script_command_from_outbound(command: OutboundCommand) -> ScriptCommand {
    match command {
        OutboundCommand::Broadcast {
            kind,
            body,
            unreliable,
        } => ScriptCommand::BroadcastMatch {
            kind,
            body,
            unreliable,
            exclude: None,
        },
        OutboundCommand::Send {
            session,
            kind,
            body,
            unreliable,
        } => ScriptCommand::SendTo {
            participant: session,
            kind,
            body,
            unreliable,
        },
        OutboundCommand::SpawnActor {
            object_id,
            archetype,
            position,
        } => ScriptCommand::SpawnActor {
            object_id,
            archetype,
            position,
        },
        OutboundCommand::MoveActor {
            object_id,
            position,
            rotation,
            velocity,
        } => ScriptCommand::ApplyTransform {
            object_id,
            transform: BridgeTransform {
                position,
                rotation,
                velocity,
            },
        },
        OutboundCommand::SetPhysics { object_id, opts } => ScriptCommand::SetPhysics {
            object_id,
            opts: opts.map(Into::into),
        },
        OutboundCommand::ApplyImpulse { object_id, impulse } => {
            ScriptCommand::ApplyImpulse { object_id, impulse }
        }
        OutboundCommand::SetMoveIntent { object_id, intent } => {
            ScriptCommand::SetMoveIntent { object_id, intent }
        }
        OutboundCommand::DespawnActor { object_id } => ScriptCommand::DespawnActor { object_id },
    }
}

/// Decision returned by a runtime's pre-dispatch realtime interceptor.
///
/// The gateway fails closed when an embedded runtime cannot produce a valid
/// decision, so a runtime implementation must return [`Drop`](Self::Drop) for
/// an error, timeout, or panic in its before hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeInterception {
    /// Continue through the gateway's normal router.
    Continue,
    /// Stop routing this envelope. No outbound deliveries are queued.
    Drop,
}

/// Result made visible to an observer after one realtime gateway dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeAfterOutcome {
    /// Whether the before hook vetoed this envelope.
    pub dropped: bool,
    /// Number of local outbound deliveries synchronously queued by the gateway.
    pub delivered: usize,
}

/// A language runtime that turns inbound game events into outbound commands.
///
/// Implemented today by [`LuaRuntime`]. Future adapters (Python, JS/TS, native
/// Rust, WASM) implement the same language-neutral surface so the gateway can
/// hold an `Arc<dyn Runtime>` and avoid naming a concrete scripting engine.
pub trait Runtime: Send + Sync + 'static {
    /// Inspect one post-registration, non-auth realtime envelope before routing.
    ///
    /// The default preserves existing runtime adapters. Implementations expose
    /// this as their `before_realtime` host hook and must fail closed if the hook
    /// cannot produce a valid decision.
    fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        let _ = (sender, user_id, room_id, kind, body);
        RealtimeInterception::Continue
    }

    /// Observe the outcome of one eligible realtime envelope after routing.
    ///
    /// Implementations must isolate errors and discard any outbound commands the
    /// observer attempts to enqueue; this callback cannot alter a completed
    /// gateway result.
    fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        let _ = (sender, user_id, room_id, kind, body, outcome);
    }

    /// Run the registered message handler for `kind` and return its commands.
    fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand>;

    /// Attach the sink where this runtime's authoritative-bridge answers land.
    ///
    /// Held weakly to avoid an `Arc` cycle: the gateway owns both the runtime
    /// and the sink. The default ignores it — an adapter that has not
    /// implemented the bridge never produces a [`ScriptCommandBatch`], so the
    /// sink would never be called anyway (fail-closed).
    fn attach_bridge_sink(&self, sink: std::sync::Weak<dyn BridgeCommandSink>) {
        let _ = sink;
    }

    /// Deliver one authoritative match's normalized-event batch to the script.
    ///
    /// The bridge is asynchronous (see [`BridgeCommandSink`]): this returns
    /// nothing. Embedded adapters evaluate the batch inline and hand the fenced
    /// [`ScriptCommandBatch`] to the attached sink before returning; the worker
    /// adapter forwards the batch over the data plane and the answer arrives on
    /// the worker's cadence. The default drops the batch — an adapter that has
    /// not implemented the bridge never materializes protected gameplay, the
    /// fail-closed owner failure policy.
    fn deliver_event_batch(&self, batch: NormalizedEventBatch) {
        let _ = batch;
    }

    /// Run a message handler in the scope of one authoritative match.
    ///
    /// The default preserves adapters that do not need a room-aware context.
    /// Shipped adapters override it so `ctx.room_id` identifies the match; the
    /// gateway still scopes resulting broadcasts independently of the adapter.
    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        let _ = room_id;
        self.dispatch(sender, user_id, kind, body)
    }

    /// Run the `on_join`/`on_leave` lifecycle handler for `sender`.
    fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand>;

    /// Invoke the registered leaderboard-reset hook for one durable epoch.
    ///
    /// Scheduler delivery uses this result to decide whether to acknowledge the
    /// durable outbox record; implementations must surface handler failures so
    /// the occurrence can be retried.
    fn on_leaderboard_reset(
        &self,
        epoch: &ResetEpoch,
        fencing_token: SchedulerFencingToken,
    ) -> AppResult<()> {
        let _ = (epoch, fencing_token);
        Ok(())
    }

    /// Run the server game-loop handler with elapsed `dt`, bounded by `budget`.
    fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand>;

    /// Advance one authoritative match. Adapters that expose a match-aware tick
    /// override this; the default keeps the global tick behavior unchanged.
    fn tick_in_room(&self, room_id: u64, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        let _ = room_id;
        self.tick(dt, budget)
    }

    /// Observe a server-side match closure so a process-hosting adapter can
    /// release the match's execution context. The default preserves embedded
    /// adapters, whose per-match state lives with the gateway's rooms.
    fn on_match_closed(&self, room_id: u64) {
        let _ = room_id;
    }

    /// Run the RPC handler for `method`.
    fn call_rpc(&self, sender: u64, user_id: Option<&str>, method: &str, body: &[u8])
    -> RpcOutcome;

    /// Run the room-create handler.
    fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec>;

    /// Run the room-join admission gate.
    fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool;

    /// Snapshot the endpoint declarations registered while this runtime was
    /// initialized. The HTTP transport uses this only to route under its
    /// reserved prefix; it never permits a runtime to replace Citadel routes.
    fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        Vec::new()
    }

    /// Invoke one endpoint handler selected by the transport from
    /// [`Self::http_endpoints`]. Implementations must apply the same error
    /// isolation and invocation deadline as other synchronous host calls.
    fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        let _ = request;
        RuntimeHttpOutcome::NotFound
    }

    /// Whether a game-loop (`on_tick`) handler is registered.
    fn has_tick_handler(&self) -> bool;

    /// The per-invocation handler time budget.
    fn budget(&self) -> Duration;

    /// A point-in-time description of registered handlers.
    fn introspect(&self) -> RuntimeIntrospection;

    /// Whether this runtime is backed by an on-disk source that can be reloaded.
    fn is_reloadable(&self) -> bool {
        false
    }

    /// Rebuild from the backing source and swap in, failure-safe.
    fn reload(&self) -> ReloadOutcome {
        ReloadOutcome::NotReloadable
    }

    /// Files whose changes should cause this on-disk runtime to reload.
    ///
    /// The default has no dependencies. File-backed adapters return their
    /// entrypoint plus any data dependencies discovered during initialization.
    /// This is intentionally separate from dispatch so polling metadata never
    /// introduces filesystem I/O on a message or tick path.
    fn reload_watch_paths(&self) -> Vec<std::path::PathBuf> {
        Vec::new()
    }
}

#[cfg(test)]
mod bridge_seam_tests {
    use std::sync::{Arc, Mutex, Weak};

    use super::*;

    /// A runtime that overrides nothing bridge-related: it inherits the
    /// fail-closed default `deliver_event_batch` and `attach_bridge_sink`.
    struct FailClosedRuntime;

    impl Runtime for FailClosedRuntime {
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
        fn tick(&self, _: Duration, _: Duration) -> Vec<OutboundCommand> {
            Vec::new()
        }
        fn call_rpc(&self, _: u64, _: Option<&str>, _: &str, _: &[u8]) -> RpcOutcome {
            RpcOutcome::Err("no rpc".to_string())
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
            Duration::from_millis(1)
        }
        fn introspect(&self) -> RuntimeIntrospection {
            RuntimeIntrospection {
                source: "fail-closed".to_string(),
                reloadable: false,
                deadline_ms: 1,
                rpcs: Vec::new(),
                message_kinds: Vec::new(),
                hooks: Vec::new(),
            }
        }
    }

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<ScriptCommandBatch>>);

    impl BridgeCommandSink for RecordingSink {
        fn deliver_command_batch(&self, answer: ScriptCommandBatch) {
            self.0.lock().expect("sink lock").push(answer);
        }
    }

    #[test]
    fn default_runtime_never_answers_a_delivered_batch() {
        // The fail-closed failure policy: an adapter that has not implemented
        // the bridge materializes nothing. A batch delivered to it produces no
        // answer at the sink, ever — so the gateway would time the match out
        // rather than apply a default-accept.
        let runtime = FailClosedRuntime;
        let sink = Arc::new(RecordingSink::default());
        runtime.attach_bridge_sink(Arc::downgrade(&sink) as Weak<dyn BridgeCommandSink>);
        runtime.deliver_event_batch(NormalizedEventBatch::new(1, 42, 9, 100, 1));
        assert!(
            sink.0.lock().expect("sink lock").is_empty(),
            "the default bridge surface must never answer"
        );
    }
}
