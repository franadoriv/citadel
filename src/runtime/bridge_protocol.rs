//! Authoritative GameScript gameplay bridge — the semantic wire contract.
//!
//! This module defines the two fenced payloads that flow across the per-match
//! execution boundary:
//!
//! - [`NormalizedEventBatch`] (Rust → GameScript): every protected client
//!   gameplay action, decoded and ownership-verified by Rust's *structural*
//!   stage, delivered to the match's script as typed intents. The script never
//!   sees malformed, foreign, replayed, or out-of-bounds raw input.
//! - [`ScriptCommandBatch`] (GameScript → Rust): the script's fenced,
//!   batch-atomic answer — one [`InputOutcome`] per event plus any
//!   script-originated [`ScriptCommand`]s. Rust validates it against the
//!   per-match ledger (see [`super::bridge_validator`]) and only then
//!   materializes state, replication, or delivery.
//!
//! Both payloads are the **semantic** content that rides the existing data
//! plane: for the external worker they are encoded into
//! [`DataFrame::MatchEvent`](super::worker_data_protocol::DataFrame::MatchEvent)
//! / [`DataFrame::MatchCommands`](super::worker_data_protocol::DataFrame::MatchCommands);
//! for the in-process adapters the same Rust types cross the call boundary
//! directly. The design is normatively specified in
//! `INV-20260805-GAMESCRIPT-AUTHORITATIVE-COMMAND-AND-REPLICATION` §3.3/§3.4.
//!
//! # Fencing
//!
//! Every batch — in both directions — carries the six mandatory fencing fields
//! ([`protocol_version`](NormalizedEventBatch::protocol_version),
//! `generation`, `match_id`, `clock_epoch`, `tick`, `batch_id`) plus the
//! per-event correlation ids. The answer must echo them exactly; any mismatch
//! fails the *whole* batch closed (batch-atomic, owner decision 2). The
//! validator ([`super::bridge_validator`]) is the sole authority on
//! acceptance.

use serde::{Deserialize, Serialize};

use super::worker_protocol::ProtocolError;

/// Version of the bridge semantic contract. Independent of the data-plane
/// frame version ([`DATA_PROTOCOL_VERSION`](super::worker_data_protocol::DATA_PROTOCOL_VERSION))
/// and the control-plane version: the transport framing and the gameplay
/// contract evolve separately. A new version is a new top-level payload, never
/// an in-place field reinterpretation (the reserved-kind precedent,
/// `crates/citadel-wire/src/protocol.rs`).
pub const GS_BRIDGE_PROTOCOL_VERSION: u16 = 1;

/// Symmetric fail-closed encode/decode cap for one bridge payload. Reuses the
/// data-plane frame cap ([`MAX_DATA_FRAME_BYTES`](super::worker_data_protocol::MAX_DATA_FRAME_BYTES),
/// 1 MiB): a batch is carried in exactly one data frame, so it can never be
/// larger than the frame that must hold it. PROVISIONAL: the eventual value is
/// "measure first" — p99 observed batch size plus headroom from the bench
/// harness (Fase 5), the way `MAX_CONTROL_FRAME_BYTES` was set for the control
/// plane.
pub const MAX_BRIDGE_PAYLOAD_BYTES: usize = super::worker_data_protocol::MAX_DATA_FRAME_BYTES;

// ---------------------------------------------------------------------------
// Shared value types (serde-friendly mirrors of the wire/runtime types).
// ---------------------------------------------------------------------------

/// A transform triple. Mirrors [`citadel_wire::na::NaTransform`] but is
/// serde-derivable so it can ride the data plane and cross the FFI boundary.
///
/// `rotation` is a quaternion in `xyzw` order.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BridgeTransform {
    /// World position (cm), `[x, y, z]`.
    pub position: [f32; 3],
    /// Rotation quaternion, `[x, y, z, w]`.
    pub rotation: [f32; 4],
    /// Linear velocity (cm/s), `[x, y, z]`.
    pub velocity: [f32; 3],
}

impl BridgeTransform {
    /// A transform at the origin, identity rotation, zero velocity.
    #[must_use]
    pub fn identity() -> Self {
        Self {
            position: [0.0; 3],
            rotation: [0.0, 0.0, 0.0, 1.0],
            velocity: [0.0; 3],
        }
    }
}

impl From<citadel_wire::na::NaTransform> for BridgeTransform {
    fn from(t: citadel_wire::na::NaTransform) -> Self {
        Self {
            position: t.position,
            rotation: t.rotation,
            velocity: t.velocity,
        }
    }
}

impl From<BridgeTransform> for citadel_wire::na::NaTransform {
    fn from(t: BridgeTransform) -> Self {
        Self {
            position: t.position,
            rotation: t.rotation,
            velocity: t.velocity,
        }
    }
}

/// A single replicated-field value. Serde-friendly mirror of
/// [`citadel_wire::netpeer::RepValue`]; floats are the logical values,
/// quantization is applied by the field codec on encode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BridgeRepValue {
    /// A boolean.
    Bool(bool),
    /// A bounded integer (widened to `i64`).
    Int(i64),
    /// A scalar `f32`.
    Scalar(f32),
    /// A position vector.
    Vector3([f32; 3]),
    /// A rotation quaternion `(x, y, z, w)`.
    Quat([f32; 4]),
    /// A length-delimited byte blob.
    Bytes(Vec<u8>),
}

impl From<citadel_wire::netpeer::RepValue> for BridgeRepValue {
    fn from(v: citadel_wire::netpeer::RepValue) -> Self {
        use citadel_wire::netpeer::RepValue as V;
        match v {
            V::Bool(b) => Self::Bool(b),
            V::Int(i) => Self::Int(i),
            V::Scalar(s) => Self::Scalar(s),
            V::Vector3(v) => Self::Vector3(v),
            V::Quat(q) => Self::Quat(q),
            V::Bytes(b) => Self::Bytes(b),
        }
    }
}

impl From<BridgeRepValue> for citadel_wire::netpeer::RepValue {
    fn from(v: BridgeRepValue) -> Self {
        match v {
            BridgeRepValue::Bool(b) => Self::Bool(b),
            BridgeRepValue::Int(i) => Self::Int(i),
            BridgeRepValue::Scalar(s) => Self::Scalar(s),
            BridgeRepValue::Vector3(v) => Self::Vector3(v),
            BridgeRepValue::Quat(q) => Self::Quat(q),
            BridgeRepValue::Bytes(b) => Self::Bytes(b),
        }
    }
}

/// One `(field_id, value)` replicated write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeRepField {
    /// The field's stable id within its `RepLayout`.
    pub field_id: u16,
    /// The logical value.
    pub value: BridgeRepValue,
}

/// Collision shape for an opt-in kinematic body. Serde mirror of the physics
/// shape carried by [`super::PhysicsOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BridgeShape {
    /// An upright capsule.
    Capsule {
        /// Capsule radius (cm).
        radius: f32,
        /// Capsule height (cm).
        height: f32,
    },
    /// An axis-aligned box.
    Aabb {
        /// Half extents `[x, y, z]` (cm).
        half_extents: [f32; 3],
    },
}

/// Serde-friendly mirror of [`super::PhysicsOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BridgePhysicsOptions {
    /// Whether a body is attached (`false` detaches).
    pub enabled: bool,
    /// Collision shape.
    pub shape: BridgeShape,
    /// Gravity acceleration (cm/s²).
    pub gravity: f32,
    /// Buoyancy factor.
    pub buoyancy: f32,
    /// Linear drag.
    pub drag: f32,
    /// Maximum body speed (cm/s).
    pub max_speed: f32,
}

impl From<super::PhysicsOptions> for BridgePhysicsOptions {
    fn from(opts: super::PhysicsOptions) -> Self {
        Self {
            enabled: opts.enabled,
            shape: match opts.config.shape {
                citadel_physics::Shape::Capsule { radius, height } => {
                    BridgeShape::Capsule { radius, height }
                }
                citadel_physics::Shape::Aabb { half_extents } => BridgeShape::Aabb { half_extents },
            },
            gravity: opts.config.gravity,
            buoyancy: opts.config.buoyancy,
            drag: opts.config.drag,
            max_speed: opts.config.max_speed,
        }
    }
}

impl From<BridgePhysicsOptions> for super::PhysicsOptions {
    fn from(opts: BridgePhysicsOptions) -> Self {
        Self {
            enabled: opts.enabled,
            config: citadel_physics::PhysicsConfig {
                shape: match opts.shape {
                    BridgeShape::Capsule { radius, height } => {
                        citadel_physics::Shape::Capsule { radius, height }
                    }
                    BridgeShape::Aabb { half_extents } => {
                        citadel_physics::Shape::Aabb { half_extents }
                    }
                },
                gravity: opts.gravity,
                buoyancy: opts.buoyancy,
                drag: opts.drag,
                max_speed: opts.max_speed,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Normalized events: Rust → GameScript.
// ---------------------------------------------------------------------------

/// A fire/shot intent carried inside a [`NormalizedPayload::TransformInput`].
///
/// This is *intent only* — origin and direction the client reported. Per owner
/// decision 1 the lag-compensated hit geometry stays a bounded Rust host API
/// ([`RewindQuery`]); the script receives the query result and decides the
/// consequence (damage/death/cooldown).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FireIntent {
    /// Muzzle/origin in world space (cm).
    pub origin: [f32; 3],
    /// Normalized aim direction.
    pub direction: [f32; 3],
    /// Weapon/mode selector the client reported (opaque to Rust).
    pub weapon: u16,
}

/// One protected gameplay action, normalized after the structural stage.
///
/// Events are only ever built *after* Rust's structural checks pass
/// (ownership, ownership-epoch, sequence dedup, clock-epoch, finite floats,
/// bounds, room membership, rate). The script therefore only ever sees decoded,
/// typed, ownership-verified intents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    /// Unique + monotonic within the match; the correlation key an
    /// [`InputOutcome`] must answer exactly once.
    pub event_id: u64,
    /// Originating participant (raw session id, Rust-authenticated).
    pub participant: u64,
    /// Resolved account id, if the participant is not a guest.
    pub user_id: Option<String>,
    /// The typed intent.
    pub payload: NormalizedPayload,
}

/// The typed body of a [`NormalizedEvent`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NormalizedPayload {
    /// `KIND_TSYNC_INPUT` / `KIND_TSYNC_V2_INPUT` frame that passed structural
    /// checks (ownership, `ownership_epoch`, `input_seq` dedup, clock epoch).
    TransformInput {
        /// The owned object this input drives.
        object_id: u32,
        /// The frame's ownership epoch (already matched against the registry).
        ownership_epoch: u32,
        /// The frame's input sequence number (already dedup-checked).
        input_seq: u32,
        /// The sim tick the client stamped.
        sim_tick: u32,
        /// Delta time (s), already clamped to the structural limit.
        dt: f32,
        /// Requested movement velocity (cm/s), `[x, y, z]`.
        move_velocity: [f32; 3],
        /// Opaque per-game input payload (bounded by the structural stage).
        payload: Vec<u8>,
        /// Fire/shot intent bundled with this input, if any.
        fire: Option<FireIntent>,
    },
    /// `KIND_NA_STATE` relay-mode owner report that passed the (new) structural
    /// checks — ownership, room, rate, finite floats, speed/bounds clamp,
    /// quaternion normalization, sequence.
    ActorStateReport {
        /// The owned object being reported.
        object_id: u32,
        /// Reported transform (finite, normalized rotation).
        transform: BridgeTransform,
    },
    /// `KIND_REP_DELTA` write on a `ClientOwned` field that passed
    /// schema/ownership/bounds decode.
    ReplicatedVarWrite {
        /// The replicated object.
        object_id: u32,
        /// The object's class id.
        class_id: u32,
        /// The layout schema hash the client's frame carried.
        schema_hash: [u8; 16],
        /// The client's bunch id (idempotency echo).
        result_id: u64,
        /// Decoded, bounds-checked field writes.
        fields: Vec<BridgeRepField>,
    },
    /// `KIND_NA_PRESENCE` spawn request (hybrid gate: Rust structural, script
    /// authorizes the actual spawn/fan-out).
    SpawnRequest {
        /// Client archetype the participant asked to instantiate.
        archetype_id: u16,
        /// Requested initial transform (finite, normalized rotation).
        transform: BridgeTransform,
    },
    /// Any unreserved kind (replaces the raw `on_message` dispatch inside a
    /// match).
    MatchMessage {
        /// Wire kind of the inbound envelope.
        kind: u16,
        /// Opaque envelope body (bounded by the structural stage).
        body: Vec<u8>,
    },
    /// A participant joined the match (replaces `dispatch_lifecycle` join in the
    /// match scope).
    ParticipantJoined,
    /// A participant left the match.
    ParticipantLeft,
}

/// One batch per (match, script turn) — the delivery unit into the match
/// mailbox. Carries all six fencing fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEventBatch {
    /// Must equal [`GS_BRIDGE_PROTOCOL_VERSION`].
    pub protocol_version: u16,
    /// Active script-revision generation (activation fencing). Interim source:
    /// the runtime reload counter; any reload = a new generation.
    pub generation: u64,
    /// `RoomId` of the authoritative match.
    pub match_id: u64,
    /// Gameplay-clock epoch (`u64`, matching
    /// [`citadel_wire::tsync::GameplayClockMetadata`]).
    pub clock_epoch: u64,
    /// Gameplay-clock tick at batch build time.
    pub tick: u64,
    /// Monotonic per (`match_id`, `generation`), starts at 1.
    pub batch_id: u64,
    /// The events to answer.
    pub events: Vec<NormalizedEvent>,
}

impl NormalizedEventBatch {
    /// Build an empty (tick-only) batch with the given fencing.
    #[must_use]
    pub fn new(generation: u64, match_id: u64, clock_epoch: u64, tick: u64, batch_id: u64) -> Self {
        Self {
            protocol_version: GS_BRIDGE_PROTOCOL_VERSION,
            generation,
            match_id,
            clock_epoch,
            tick,
            batch_id,
            events: Vec::new(),
        }
    }

    /// Encode this batch for the data plane (serde_json, consistent with the
    /// control/data planes; the codec is swappable by construction).
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let bytes = serde_json::to_vec(self).map_err(|_| ProtocolError::MalformedFrame)?;
        if bytes.len() > MAX_BRIDGE_PAYLOAD_BYTES {
            return Err(ProtocolError::FrameTooLarge);
        }
        Ok(bytes)
    }

    /// Decode a batch, rejecting oversized payloads before parsing (fail-closed
    /// decode order, the same contract as the data frame itself).
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() > MAX_BRIDGE_PAYLOAD_BYTES {
            return Err(ProtocolError::FrameTooLarge);
        }
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::MalformedFrame)
    }
}

// ---------------------------------------------------------------------------
// Script command batch: GameScript → Rust.
// ---------------------------------------------------------------------------

/// The decision a script returns for one normalized input event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Decision {
    /// Materialize the event's canonical effect (apply the client's input).
    Accept,
    /// No mutation, no replication; a bounded reason the client may reconcile
    /// against.
    Reject {
        /// Game-defined reason code (opaque to Rust).
        reason_code: u16,
    },
    /// Materialize the script's value instead of the client's.
    Correct {
        /// What the script wants materialized.
        correction: Correction,
    },
}

/// The authoritative value a script substitutes via [`Decision::Correct`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Correction {
    /// Override the transform (position/rotation/velocity).
    Transform(BridgeTransform),
    /// Override replicated field values.
    ReplicatedVars {
        /// The corrected field writes.
        fields: Vec<BridgeRepField>,
    },
    /// Override the spawn archetype/transform.
    Spawn {
        /// Archetype to instantiate.
        archetype_id: u16,
        /// Initial transform.
        transform: BridgeTransform,
    },
}

/// Exactly one per `event_id` issued in the answered batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputOutcome {
    /// The event this outcome answers.
    pub event_id: u64,
    /// Accept / reject / correct.
    pub decision: Decision,
    /// Optional structured response, unicast to the event's sender (bounded).
    pub reply: Option<Vec<u8>>,
}

/// A bounded persistence operation, executed through the existing storage host
/// APIs via the `DomainHost` seam. Capability-gated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistOp {
    /// Storage collection/namespace (validated against the match's scope).
    pub collection: String,
    /// Record key.
    pub key: String,
    /// Opaque value bytes (bounded).
    pub value: Vec<u8>,
}

/// A script-originated effect (beyond the per-event input outcomes). Every
/// variant is validated by [`super::bridge_validator`] before it materializes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScriptCommand {
    // -- permitted state mutations (materialized by TransformHub/RepAuthority) --
    /// Set an object's authoritative transform.
    ApplyTransform {
        /// Target object (must belong to this match's world).
        object_id: u32,
        /// The transform to write.
        transform: BridgeTransform,
    },
    /// Set replicated field values (must be within `FieldBounds`).
    SetReplicatedVars {
        /// Target object.
        object_id: u32,
        /// The field writes.
        fields: Vec<BridgeRepField>,
    },
    /// Spawn a server-owned actor.
    SpawnActor {
        /// Script-assigned object id (server-owned range).
        object_id: u32,
        /// Client archetype to instantiate.
        archetype: u16,
        /// Initial world position (cm).
        position: [f32; 3],
    },
    /// Despawn a server-owned actor.
    DespawnActor {
        /// Target object.
        object_id: u32,
    },
    /// Attach/reconfigure/detach an opt-in kinematic body.
    SetPhysics {
        /// Target object.
        object_id: u32,
        /// Physics settings, or `None` to detach.
        opts: Option<BridgePhysicsOptions>,
    },
    /// Add an instantaneous velocity change to a bodied actor.
    ApplyImpulse {
        /// Target object.
        object_id: u32,
        /// Velocity delta (cm/s).
        impulse: [f32; 3],
    },
    /// Set a bodied actor's desired control velocity.
    SetMoveIntent {
        /// Target object.
        object_id: u32,
        /// Desired velocity (cm/s).
        intent: [f32; 3],
    },
    // -- messaging (all scope-validated against match membership) --
    /// Unicast to one match member.
    SendTo {
        /// Recipient participant (must be a current member of the match).
        participant: u64,
        /// Wire kind.
        kind: u16,
        /// Opaque body.
        body: Vec<u8>,
        /// Best-effort delivery.
        unreliable: bool,
    },
    /// Multicast to an explicit recipient list (all must be match members).
    SendToMany {
        /// Recipients (every one must be a current member of the match).
        participants: Vec<u64>,
        /// Wire kind.
        kind: u16,
        /// Opaque body.
        body: Vec<u8>,
        /// Best-effort delivery.
        unreliable: bool,
    },
    /// Broadcast to every member of the match.
    BroadcastMatch {
        /// Wire kind.
        kind: u16,
        /// Opaque body.
        body: Vec<u8>,
        /// Best-effort delivery.
        unreliable: bool,
        /// Optional participant to exclude (usually the triggering sender).
        exclude: Option<u64>,
    },
    // -- bounded host-API side effects (capability-gated) --
    /// Persist a record through the storage host API.
    Persist {
        /// The operation.
        op: PersistOp,
    },
    /// Schedule a payload to re-enter as a future match event.
    Schedule {
        /// Delay in gameplay ticks.
        after_ticks: u64,
        /// Opaque payload.
        payload: Vec<u8>,
    },
}

/// The script's fenced, batch-atomic answer to one [`NormalizedEventBatch`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptCommandBatch {
    // ---- fencing (all mandatory; any mismatch => whole batch rejected) ----
    /// Must equal [`GS_BRIDGE_PROTOCOL_VERSION`].
    pub protocol_version: u16,
    /// Must equal the generation the events were issued under.
    pub generation: u64,
    /// Must equal the mailbox's match.
    pub match_id: u64,
    /// Must equal the current gameplay-clock epoch.
    pub clock_epoch: u64,
    /// The tick of the batch being answered (staleness check).
    pub tick: u64,
    /// Must echo [`NormalizedEventBatch::batch_id`], exactly once.
    pub batch_id: u64,
    // ---- input outcomes: exactly one per event_id in the answered batch ----
    /// One outcome per issued event; no missing, duplicate, or foreign ids.
    pub input_outcomes: Vec<InputOutcome>,
    // ---- additional commands (script-originated effects) ----
    /// Script-originated effects.
    pub commands: Vec<ScriptCommand>,
}

impl ScriptCommandBatch {
    /// Build an answer that echoes `batch`'s fencing, with no outcomes/commands
    /// yet. Script adapters fill `input_outcomes`/`commands`.
    #[must_use]
    pub fn answering(batch: &NormalizedEventBatch) -> Self {
        Self {
            protocol_version: batch.protocol_version,
            generation: batch.generation,
            match_id: batch.match_id,
            clock_epoch: batch.clock_epoch,
            tick: batch.tick,
            batch_id: batch.batch_id,
            input_outcomes: Vec::new(),
            commands: Vec::new(),
        }
    }

    /// Encode this batch for the data plane.
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let bytes = serde_json::to_vec(self).map_err(|_| ProtocolError::MalformedFrame)?;
        if bytes.len() > MAX_BRIDGE_PAYLOAD_BYTES {
            return Err(ProtocolError::FrameTooLarge);
        }
        Ok(bytes)
    }

    /// Decode a batch, rejecting oversized payloads before parsing.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() > MAX_BRIDGE_PAYLOAD_BYTES {
            return Err(ProtocolError::FrameTooLarge);
        }
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::MalformedFrame)
    }
}

// ---------------------------------------------------------------------------
// Fire/hit rewind host API (owner decision 1).
// ---------------------------------------------------------------------------

/// A bounded lag-compensated hit query the script issues while evaluating a
/// [`FireIntent`]. Rust owns the rewind geometry (favor-the-shooter, hit
/// radius, RTT cutoff); the script receives [`RewindResult`] and decides the
/// consequence. This is a host API, not a materializing command: it never
/// mutates state.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RewindQuery {
    /// The shooter's participant id (its RTT drives the rewind amount).
    pub shooter: u64,
    /// Ray origin in world space (cm).
    pub origin: [f32; 3],
    /// Normalized ray direction.
    pub direction: [f32; 3],
    /// The gameplay tick the shooter's client stamped.
    pub tick: u64,
}

/// One candidate hit returned by [`RewindQuery`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RewindHit {
    /// The object that was hit.
    pub object_id: u32,
    /// The participant that owns it (0 for server-owned).
    pub participant: u64,
    /// Impact point in world space (cm).
    pub point: [f32; 3],
    /// Distance from origin to impact (cm).
    pub distance: f32,
}

/// The bounded result of a [`RewindQuery`]. The script decides consequences.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RewindResult {
    /// Candidate hits, nearest first (bounded count).
    pub hits: Vec<RewindHit>,
}

// ---------------------------------------------------------------------------
// Asynchronous delivery seam.
// ---------------------------------------------------------------------------

/// Where a script's fenced answer to a delivered [`NormalizedEventBatch`] lands.
///
/// The authoritative bridge is asynchronous by construction. The key reason is
/// the external worker: [`ExternalWorkerRuntime`](super::external_worker::ExternalWorkerRuntime)
/// schedules matches fairly on its own cadence, so a delivered batch produces
/// no answer inline — the answer returns later as a fenced data-plane frame.
/// [`Runtime::deliver_event_batch`](super::Runtime::deliver_event_batch)
/// therefore returns nothing; the script's [`ScriptCommandBatch`] arrives here,
/// resolved inline for the embedded adapters and over the data plane for the
/// worker. Unifying both behind one sink keeps the gateway path identical
/// regardless of where the script runs.
///
/// The gateway implements this: it validates the answer against the answered
/// match's [`PendingBatchLedger`](super::bridge_validator::PendingBatchLedger)
/// and materializes state, replication, or delivery only when every §3.5 check
/// passes. A never-delivered answer (timeout, worker death) materializes
/// nothing — the fail-closed failure policy.
pub trait BridgeCommandSink: Send + Sync + 'static {
    /// Hand one script answer to the gateway for validation + materialization.
    ///
    /// The batch's own fencing (`match_id`, `generation`, `clock_epoch`,
    /// `batch_id`) selects the pending batch; a foreign, stale, duplicate, or
    /// otherwise invalid answer is rejected whole by the validator and
    /// materializes nothing (owner decision 2, batch-atomic).
    fn deliver_command_batch(&self, answer: ScriptCommandBatch);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_transform() -> BridgeTransform {
        BridgeTransform {
            position: [1.0, 2.0, 3.0],
            rotation: [0.0, 0.0, 0.0, 1.0],
            velocity: [4.0, 5.0, 6.0],
        }
    }

    #[test]
    fn protocol_version_is_one() {
        assert_eq!(GS_BRIDGE_PROTOCOL_VERSION, 1);
    }

    #[test]
    fn event_batch_carries_all_six_fencing_fields() {
        // Building via the constructor stamps the protocol version; the other
        // five fields are mandatory positional arguments.
        let batch = NormalizedEventBatch::new(7, 42, 9, 100, 1);
        assert_eq!(batch.protocol_version, GS_BRIDGE_PROTOCOL_VERSION);
        assert_eq!(batch.generation, 7);
        assert_eq!(batch.match_id, 42);
        assert_eq!(batch.clock_epoch, 9);
        assert_eq!(batch.tick, 100);
        assert_eq!(batch.batch_id, 1);
    }

    #[test]
    fn answer_echoes_fencing() {
        let batch = NormalizedEventBatch::new(7, 42, 9, 100, 3);
        let answer = ScriptCommandBatch::answering(&batch);
        assert_eq!(answer.protocol_version, batch.protocol_version);
        assert_eq!(answer.generation, batch.generation);
        assert_eq!(answer.match_id, batch.match_id);
        assert_eq!(answer.clock_epoch, batch.clock_epoch);
        assert_eq!(answer.tick, batch.tick);
        assert_eq!(answer.batch_id, batch.batch_id);
    }

    #[test]
    fn event_batch_round_trips_through_serde() {
        let mut batch = NormalizedEventBatch::new(2, 5, 11, 250, 4);
        batch.events.push(NormalizedEvent {
            event_id: 1,
            participant: 1001,
            user_id: Some("alice".into()),
            payload: NormalizedPayload::TransformInput {
                object_id: 7,
                ownership_epoch: 3,
                input_seq: 88,
                sim_tick: 249,
                dt: 0.016,
                move_velocity: [10.0, 0.0, -5.0],
                payload: vec![1, 2, 3],
                fire: Some(FireIntent {
                    origin: [0.0, 1.0, 0.0],
                    direction: [1.0, 0.0, 0.0],
                    weapon: 2,
                }),
            },
        });
        batch.events.push(NormalizedEvent {
            event_id: 2,
            participant: 1002,
            user_id: None,
            payload: NormalizedPayload::ActorStateReport {
                object_id: 9,
                transform: sample_transform(),
            },
        });
        batch.events.push(NormalizedEvent {
            event_id: 3,
            participant: 1003,
            user_id: None,
            payload: NormalizedPayload::ReplicatedVarWrite {
                object_id: 12,
                class_id: 4,
                schema_hash: [7; 16],
                result_id: 99,
                fields: vec![BridgeRepField {
                    field_id: 1,
                    value: BridgeRepValue::Vector3([1.0, 2.0, 3.0]),
                }],
            },
        });

        let bytes = batch.encode().expect("encode");
        let decoded = NormalizedEventBatch::decode(&bytes).expect("decode");
        assert_eq!(batch, decoded);
    }

    #[test]
    fn command_batch_round_trips_through_serde() {
        let src = NormalizedEventBatch::new(2, 5, 11, 250, 4);
        let mut answer = ScriptCommandBatch::answering(&src);
        answer.input_outcomes.push(InputOutcome {
            event_id: 1,
            decision: Decision::Accept,
            reply: None,
        });
        answer.input_outcomes.push(InputOutcome {
            event_id: 2,
            decision: Decision::Reject { reason_code: 42 },
            reply: Some(vec![9, 9]),
        });
        answer.input_outcomes.push(InputOutcome {
            event_id: 3,
            decision: Decision::Correct {
                correction: Correction::Transform(sample_transform()),
            },
            reply: None,
        });
        answer.commands.push(ScriptCommand::ApplyTransform {
            object_id: 7,
            transform: sample_transform(),
        });
        answer.commands.push(ScriptCommand::SetReplicatedVars {
            object_id: 12,
            fields: vec![BridgeRepField {
                field_id: 2,
                value: BridgeRepValue::Int(7),
            }],
        });
        answer.commands.push(ScriptCommand::BroadcastMatch {
            kind: 100,
            body: vec![1, 2, 3],
            unreliable: true,
            exclude: Some(1001),
        });
        answer.commands.push(ScriptCommand::Persist {
            op: PersistOp {
                collection: "scores".into(),
                key: "alice".into(),
                value: vec![0, 0, 0, 1],
            },
        });

        let bytes = answer.encode().expect("encode");
        let decoded = ScriptCommandBatch::decode(&bytes).expect("decode");
        assert_eq!(answer, decoded);
    }

    #[test]
    fn oversized_payload_is_rejected_before_parse() {
        // A body just over the cap decodes to FrameTooLarge, never a parse.
        let oversized = vec![0u8; MAX_BRIDGE_PAYLOAD_BYTES + 1];
        assert!(matches!(
            NormalizedEventBatch::decode(&oversized),
            Err(ProtocolError::FrameTooLarge)
        ));
        assert!(matches!(
            ScriptCommandBatch::decode(&oversized),
            Err(ProtocolError::FrameTooLarge)
        ));
    }

    #[test]
    fn transform_and_rep_value_conversions_are_lossless() {
        let t = sample_transform();
        let na: citadel_wire::na::NaTransform = t.into();
        assert_eq!(BridgeTransform::from(na), t);

        let v = BridgeRepValue::Quat([0.1, 0.2, 0.3, 0.4]);
        let wire: citadel_wire::netpeer::RepValue = v.clone().into();
        assert_eq!(BridgeRepValue::from(wire), v);
    }
}
