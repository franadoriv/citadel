//! Transform-sync frame bodies (, kinds 7-11 reserved by ).
//!
//! This module defines the *bodies* of the transform-sync envelopes on the
//! reserved [`crate::protocol`] kind range; the discriminants themselves live in
//! `protocol.rs`. Every body reuses the shared foundation from  rather
//! than reinventing it:
//!
//! - the hot-path [`Snapshot`] is bit-packed through [`crate::bits::BitWriter`]
//!   / [`crate::bits::BitReader`] with the [`crate::codec`] quantizers
//!   ([`VectorQuant`] position/velocity, smallest-three [`QuatMode`] rotation);
//! - the control frames ([`Hello`], [`Ack`], [`Role`]) use fixed big-endian byte
//!   layouts because they ride reliable/rare paths and gain nothing from bit
//!   packing.
//!
//! # Delta-vs-baseline over unordered datagrams (design §6.3, review)
//!
//! Snapshots ride QUIC/WebTransport **unreliable, unordered** datagrams. Every
//! [`Snapshot`] carries an **absolute** `snapshot_id` and the absolute
//! `base_snapshot_id` it was diffed against (`0` = full baseline). A receiver
//! reconstructs `full[id] = full[base_id] + updates − removals`; it discards any
//! snapshot whose `base_snapshot_id` it does not hold and applies a snapshot only
//! if `snapshot_id` is newer than the last it applied (monotonic guard). The
//! server always diffs against a base the client provably holds (its newest acked
//! id), so a delta can never reference a baseline the client lacks. `gen_epoch`
//! guards object-id reuse/respawn; area-of-interest enter/exit is expressed by
//! set membership (a re-entering object is simply absent from the base and sent
//! full again), not by bumping `gen_epoch`.

use crate::bits::{BitError, BitReader, BitWriter};
use crate::codec::{CodecError, QuatMode, VectorQuant, WorldBounds};

/// Per-object sync role, mirrored on the wire so every peer agrees who drives an
/// object's transform (design §2). Encoded as a single byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRole {
    /// This object is owned + predicted by one client; server authoritative.
    OwnerPredicted,
    /// Server-owned, rendered interpolated in the past on every client.
    RemoteInterpolated,
    /// Server/physics/Lua drives it; clients never predict it.
    ServerSimulated,
    /// One-shot + rare updates; dormancy-eligible.
    StaticReplicated,
}

impl SyncRole {
    /// The stable wire byte for this role.
    #[must_use]
    pub const fn to_wire(self) -> u8 {
        match self {
            SyncRole::OwnerPredicted => 0,
            SyncRole::RemoteInterpolated => 1,
            SyncRole::ServerSimulated => 2,
            SyncRole::StaticReplicated => 3,
        }
    }

    /// Decode a role byte; unknown values are rejected.
    #[must_use]
    pub const fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(SyncRole::OwnerPredicted),
            1 => Some(SyncRole::RemoteInterpolated),
            2 => Some(SyncRole::ServerSimulated),
            3 => Some(SyncRole::StaticReplicated),
            _ => None,
        }
    }
}

/// An error encoding/decoding a transform-sync frame body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TsyncError {
    /// The body was shorter than the fixed layout requires.
    #[error("transform-sync body too short: needed {needed}, got {got}")]
    TooShort {
        /// Bytes the layout requires.
        needed: usize,
        /// Bytes actually present.
        got: usize,
    },
    /// A field carried a value outside its allowed set (bad role, quat mode, …).
    #[error("transform-sync field out of range: {0}")]
    OutOfRange(&'static str),
    /// The declared object/removal count did not match the bytes/bits present.
    #[error("transform-sync count mismatch: {0}")]
    CountMismatch(&'static str),
    /// A quantized codec rejected a value/code.
    #[error("transform-sync codec error: {0}")]
    Codec(String),
    /// The underlying bit reader/writer failed.
    #[error("transform-sync bit error: {0}")]
    Bit(String),
}

impl From<CodecError> for TsyncError {
    fn from(e: CodecError) -> Self {
        TsyncError::Codec(e.to_string())
    }
}

impl From<BitError> for TsyncError {
    fn from(e: BitError) -> Self {
        TsyncError::Bit(e.to_string())
    }
}

/// A quantization grade for the smallest-three quaternion, carried in [`Hello`].
///
/// A wire byte so the negotiated mode is explicit and validated.
#[must_use]
pub const fn quat_mode_to_wire(mode: QuatMode) -> u8 {
    match mode {
        QuatMode::Bits9 => 9,
        QuatMode::Bits10 => 10,
        QuatMode::Bits15 => 15,
    }
}

/// The `HELLO` negotiation (kind 7, reliable, either direction).
///
/// The server advertises the world it quantizes against; both sides then build
/// the identical [`TransformCodec`]. Byte layout (big-endian):
/// `min[3] f32 · max[3] f32 · values_per_unit u32` (position bounds), the same 28
/// bytes for velocity bounds, `quat_mode u8`, `send_rate_hz u8`, `sim_rate_hz u8`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hello {
    /// Position world bounds + precision (cm).
    pub position_bounds: WorldBounds,
    /// Velocity bounds + precision (cm/s), used when velocity is replicated.
    pub velocity_bounds: WorldBounds,
    /// Smallest-three quaternion grade (9/10/15 bits per component).
    pub quat_mode: QuatMode,
    /// Default snapshot send rate (packets/sec).
    pub send_rate_hz: u8,
    /// Server simulation rate (ticks/sec).
    pub sim_rate_hz: u8,
}

/// Bytes in a serialized [`WorldBounds`]: `min[3]` + `max[3]` f32 + `vpu` u32.
const WORLD_BOUNDS_BYTES: usize = 3 * 4 + 3 * 4 + 4;
/// Bytes in a serialized [`Hello`] body.
const HELLO_BYTES: usize = WORLD_BOUNDS_BYTES * 2 + 3;

fn put_bounds(buf: &mut Vec<u8>, b: &WorldBounds) {
    for v in b.min {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    for v in b.max {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    buf.extend_from_slice(&b.values_per_unit.to_be_bytes());
}

fn get_f32(bytes: &[u8], off: &mut usize) -> f32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&bytes[*off..*off + 4]);
    *off += 4;
    f32::from_be_bytes(a)
}

fn get_u32(bytes: &[u8], off: &mut usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&bytes[*off..*off + 4]);
    *off += 4;
    u32::from_be_bytes(a)
}

fn get_bounds(bytes: &[u8], off: &mut usize) -> WorldBounds {
    let min = [
        get_f32(bytes, off),
        get_f32(bytes, off),
        get_f32(bytes, off),
    ];
    let max = [
        get_f32(bytes, off),
        get_f32(bytes, off),
        get_f32(bytes, off),
    ];
    let values_per_unit = get_u32(bytes, off);
    WorldBounds {
        min,
        max,
        values_per_unit,
    }
}

impl Hello {
    /// Encode the negotiation body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HELLO_BYTES);
        put_bounds(&mut buf, &self.position_bounds);
        put_bounds(&mut buf, &self.velocity_bounds);
        buf.push(quat_mode_to_wire(self.quat_mode));
        buf.push(self.send_rate_hz);
        buf.push(self.sim_rate_hz);
        buf
    }

    /// Decode a negotiation body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        if body.len() < HELLO_BYTES {
            return Err(TsyncError::TooShort {
                needed: HELLO_BYTES,
                got: body.len(),
            });
        }
        let mut off = 0usize;
        let position_bounds = get_bounds(body, &mut off);
        let velocity_bounds = get_bounds(body, &mut off);
        let quat_mode =
            QuatMode::from_bits(u32::from(body[off])).ok_or(TsyncError::OutOfRange("quat_mode"))?;
        let send_rate_hz = body[off + 1];
        let sim_rate_hz = body[off + 2];
        Ok(Self {
            position_bounds,
            velocity_bounds,
            quat_mode,
            send_rate_hz,
            sim_rate_hz,
        })
    }
}

/// The shared codec built from a [`Hello`]: the quantizers both sides use so an
/// encoded snapshot decodes bit-for-bit identically.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TransformCodec {
    /// Position quantizer.
    pub position: VectorQuant,
    /// Velocity quantizer.
    pub velocity: VectorQuant,
    /// Rotation grade.
    pub quat_mode: QuatMode,
}

impl TransformCodec {
    /// Build the codec from negotiated [`Hello`] params.
    pub fn from_hello(hello: &Hello) -> Result<Self, TsyncError> {
        Ok(Self {
            position: VectorQuant::new(hello.position_bounds)?,
            velocity: VectorQuant::new(hello.velocity_bounds)?,
            quat_mode: hello.quat_mode,
        })
    }
}

/// The transform fields carried for one object in a snapshot. `None` fields are
/// omitted on the wire (delta compression); the receiver fills them from its base.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TransformFields {
    /// Position `(x, y, z)` in cm, if present this snapshot.
    pub position: Option<[f32; 3]>,
    /// Rotation quaternion `(x, y, z, w)`, if present this snapshot.
    pub rotation: Option<[f32; 4]>,
    /// Velocity `(x, y, z)` in cm/s, if present this snapshot.
    pub velocity: Option<[f32; 3]>,
}

impl TransformFields {
    /// Whether no field is present (an object listed but with nothing changed).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.position.is_none() && self.rotation.is_none() && self.velocity.is_none()
    }

    fn changed_bits(&self) -> u64 {
        (u64::from(self.position.is_some()) << 2)
            | (u64::from(self.rotation.is_some()) << 1)
            | u64::from(self.velocity.is_some())
    }
}

/// One object's entry in a [`Snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ObjectUpdate {
    /// Match-unique replicated-object id (wire width: 32 bits, design §8).
    pub object_id: u32,
    /// Replication generation; guards object-id reuse/respawn.
    pub gen_epoch: u16,
    /// The present transform fields (delta or full).
    pub fields: TransformFields,
    /// For an `OwnerPredicted` object owned by the receiving client, the
    /// **highest *contiguous* input seq** the server has applied to it
    /// (design §5.1, P2/). `None` for objects the client does not own.
    /// Present on the wire only when set (a 4th `changed` bit), so
    /// remote-interpolated objects pay no extra bytes.
    pub last_input_seq: Option<u32>,
}

/// Object-id wire width in bits.
pub const OBJECT_ID_BITS: u32 = 32;
/// `gen_epoch` wire width in bits.
pub const GEN_EPOCH_BITS: u32 = 16;
/// Changed/presence bitfield width (pos/rot/vel + owner `last_input_seq`).
pub const CHANGED_BITS: u32 = 4;
/// The `changed` bit marking a present per-owner `last_input_seq` (design §5.1).
pub const INPUT_SEQ_BIT: u64 = 0b1000;
/// `last_input_seq` wire width in bits.
pub const INPUT_SEQ_BITS: u32 = 32;

/// A per-client delta snapshot (kind 8, unreliable). See the module docs for the
/// absolute-id delta-vs-baseline contract.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// The completed sim tick this snapshot describes (latched frame, design §7.5).
    pub server_tick: u32,
    /// Absolute id of this snapshot.
    pub snapshot_id: u32,
    /// Absolute id it was diffed against; `0` = full baseline.
    pub base_snapshot_id: u32,
    /// Current effective send rate (packets/sec) so the client sizes its buffer.
    pub send_rate_hz: u8,
    /// Objects removed relative to the base (AOI-exit / despawn), by id.
    pub removed: Vec<u32>,
    /// Object updates (full for new/base-less objects, delta otherwise).
    pub updates: Vec<ObjectUpdate>,
}

impl Snapshot {
    /// Bit-pack the snapshot body with `codec`. Header (fixed): `server_tick 32 ·
    /// snapshot_id 32 · base_snapshot_id 32 · send_rate_hz 8 · removed_count 16 ·
    /// update_count 16`, then each removed id (32 bits), then each update.
    pub fn encode(&self, codec: &TransformCodec) -> Result<Vec<u8>, TsyncError> {
        let mut w = BitWriter::new();
        w.write_bits(u64::from(self.server_tick), 32)?;
        w.write_bits(u64::from(self.snapshot_id), 32)?;
        w.write_bits(u64::from(self.base_snapshot_id), 32)?;
        w.write_bits(u64::from(self.send_rate_hz), 8)?;
        let removed_count = u16::try_from(self.removed.len())
            .map_err(|_| TsyncError::CountMismatch("removed too many"))?;
        let update_count = u16::try_from(self.updates.len())
            .map_err(|_| TsyncError::CountMismatch("updates too many"))?;
        w.write_bits(u64::from(removed_count), 16)?;
        w.write_bits(u64::from(update_count), 16)?;
        for &id in &self.removed {
            w.write_bits(u64::from(id), OBJECT_ID_BITS)?;
        }
        for u in &self.updates {
            w.write_bits(u64::from(u.object_id), OBJECT_ID_BITS)?;
            w.write_bits(u64::from(u.gen_epoch), GEN_EPOCH_BITS)?;
            let mut changed = u.fields.changed_bits();
            if u.last_input_seq.is_some() {
                changed |= INPUT_SEQ_BIT;
            }
            w.write_bits(changed, CHANGED_BITS)?;
            if let Some(pos) = u.fields.position {
                codec.position.write(&mut w, pos)?;
            }
            if let Some(rot) = u.fields.rotation {
                crate::codec::encode_quat(&mut w, rot, codec.quat_mode)?;
            }
            if let Some(vel) = u.fields.velocity {
                codec.velocity.write(&mut w, vel)?;
            }
            if let Some(seq) = u.last_input_seq {
                w.write_bits(u64::from(seq), INPUT_SEQ_BITS)?;
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode a snapshot body previously produced by [`encode`](Snapshot::encode).
    pub fn decode(body: &[u8], codec: &TransformCodec) -> Result<Self, TsyncError> {
        let mut r = BitReader::over_bytes(body);
        let server_tick = r.read_bits(32)? as u32;
        let snapshot_id = r.read_bits(32)? as u32;
        let base_snapshot_id = r.read_bits(32)? as u32;
        let send_rate_hz = r.read_bits(8)? as u8;
        let removed_count = r.read_bits(16)? as usize;
        let update_count = r.read_bits(16)? as usize;
        let mut removed = Vec::with_capacity(removed_count);
        for _ in 0..removed_count {
            removed.push(r.read_bits(OBJECT_ID_BITS)? as u32);
        }
        let mut updates = Vec::with_capacity(update_count);
        for _ in 0..update_count {
            let object_id = r.read_bits(OBJECT_ID_BITS)? as u32;
            let gen_epoch = r.read_bits(GEN_EPOCH_BITS)? as u16;
            let changed = r.read_bits(CHANGED_BITS)?;
            let mut fields = TransformFields::default();
            if changed & 0b100 != 0 {
                fields.position = Some(codec.position.read(&mut r)?);
            }
            if changed & 0b010 != 0 {
                fields.rotation = Some(crate::codec::decode_quat(&mut r, codec.quat_mode)?);
            }
            if changed & 0b001 != 0 {
                fields.velocity = Some(codec.velocity.read(&mut r)?);
            }
            let last_input_seq = if changed & INPUT_SEQ_BIT != 0 {
                Some(r.read_bits(INPUT_SEQ_BITS)? as u32)
            } else {
                None
            };
            updates.push(ObjectUpdate {
                object_id,
                gen_epoch,
                fields,
                last_input_seq,
            });
        }
        // Canonical termination: only zero padding to the byte boundary may remain.
        r.finish()?;
        Ok(Self {
            server_tick,
            snapshot_id,
            base_snapshot_id,
            send_rate_hz,
            removed,
            updates,
        })
    }
}

/// A snapshot acknowledgement (kind 10, unreliable): the newest snapshot id the
/// client fully applied plus a 32-bit history window so a lost ack is recovered.
///
/// Byte layout (big-endian): `acked_snapshot_id u32 · history u32`. Mirrors the
/// shared [`crate::baseline::AckField`] window (32 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    /// Newest fully-applied absolute snapshot id (`0` = nothing yet).
    pub acked_snapshot_id: u32,
    /// Bitfield acking the 32 ids immediately preceding `acked_snapshot_id`.
    pub history: u32,
}

/// Bytes in a serialized [`Ack`] body.
pub const ACK_BYTES: usize = 8;

impl Ack {
    /// Encode the ack body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ACK_BYTES);
        buf.extend_from_slice(&self.acked_snapshot_id.to_be_bytes());
        buf.extend_from_slice(&self.history.to_be_bytes());
        buf
    }

    /// Decode an ack body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        if body.len() < ACK_BYTES {
            return Err(TsyncError::TooShort {
                needed: ACK_BYTES,
                got: body.len(),
            });
        }
        let mut off = 0usize;
        let acked_snapshot_id = get_u32(body, &mut off);
        let history = get_u32(body, &mut off);
        Ok(Self {
            acked_snapshot_id,
            history,
        })
    }
}

/// A role/ownership/relevancy transition (kind 11, reliable, idempotent).
///
/// Byte layout (big-endian): `object_id u32 · role u8 · owner u64 (0 = none) ·
/// ownership_epoch u32 · gen_epoch u16 · event u8`. Reordered/stale events are
/// ignored by the receiver via `ownership_epoch`/`gen_epoch` (design §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Role {
    /// Target object.
    pub object_id: u32,
    /// New role.
    pub role: SyncRole,
    /// Owning participant id (`0` = none/server-owned).
    pub owner: u64,
    /// Monotonic ownership epoch guarding reordered handoffs.
    pub ownership_epoch: u32,
    /// Replication generation guarding object-id reuse.
    pub gen_epoch: u16,
    /// What happened (assign/handoff/relevancy-enter/relevancy-exit).
    pub event: RoleEvent,
}

/// The kind of transition a [`Role`] frame announces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleEvent {
    /// Initial role/owner assignment.
    Assign,
    /// Ownership handoff to a new participant.
    Handoff,
    /// The object entered the receiver's area of interest (full baseline follows).
    RelevancyEnter,
    /// The object left the receiver's area of interest (stop streaming).
    RelevancyExit,
}

impl RoleEvent {
    /// Stable wire byte.
    #[must_use]
    pub const fn to_wire(self) -> u8 {
        match self {
            RoleEvent::Assign => 0,
            RoleEvent::Handoff => 1,
            RoleEvent::RelevancyEnter => 2,
            RoleEvent::RelevancyExit => 3,
        }
    }

    /// Decode a wire byte.
    #[must_use]
    pub const fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(RoleEvent::Assign),
            1 => Some(RoleEvent::Handoff),
            2 => Some(RoleEvent::RelevancyEnter),
            3 => Some(RoleEvent::RelevancyExit),
            _ => None,
        }
    }
}

/// Bytes in a serialized [`Role`] body.
pub const ROLE_BYTES: usize = 4 + 1 + 8 + 4 + 2 + 1;

impl Role {
    /// Encode the role body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ROLE_BYTES);
        buf.extend_from_slice(&self.object_id.to_be_bytes());
        buf.push(self.role.to_wire());
        buf.extend_from_slice(&self.owner.to_be_bytes());
        buf.extend_from_slice(&self.ownership_epoch.to_be_bytes());
        buf.extend_from_slice(&self.gen_epoch.to_be_bytes());
        buf.push(self.event.to_wire());
        buf
    }

    /// Decode a role body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        if body.len() < ROLE_BYTES {
            return Err(TsyncError::TooShort {
                needed: ROLE_BYTES,
                got: body.len(),
            });
        }
        let mut off = 0usize;
        let object_id = get_u32(body, &mut off);
        let role = SyncRole::from_wire(body[off]).ok_or(TsyncError::OutOfRange("role"))?;
        off += 1;
        let mut owner_bytes = [0u8; 8];
        owner_bytes.copy_from_slice(&body[off..off + 8]);
        let owner = u64::from_be_bytes(owner_bytes);
        off += 8;
        let ownership_epoch = get_u32(body, &mut off);
        let mut gen_bytes = [0u8; 2];
        gen_bytes.copy_from_slice(&body[off..off + 2]);
        let gen_epoch = u16::from_be_bytes(gen_bytes);
        off += 2;
        let event = RoleEvent::from_wire(body[off]).ok_or(TsyncError::OutOfRange("event"))?;
        Ok(Self {
            object_id,
            role,
            owner,
            ownership_epoch,
            gen_epoch,
            event,
        })
    }
}

/// A lag-compensated fire command carried inside an [`InputFrame`] (design §5.2).
///
/// The command rides the owner's input bundle so it is processed **exactly once
/// in seq order** (keyed by the carrying frame's `input_seq`) and the client
/// **never resolves the hit**. The client supplies only the geometry it observed
/// (a world-space ray); the **server** computes and clamps the rewind time from
/// its own per-connection state, so no client-supplied timestamp is trusted.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FireCommand {
    /// Ray origin in cm (the shooter's eye/muzzle as the client saw it).
    pub origin: [f32; 3],
    /// Ray direction (need not be normalized; the server normalizes).
    pub direction: [f32; 3],
}

/// One individually-sequenced owner input frame (design §5.1, §8).
///
/// Each frame carries its own monotonic `input_seq`, the fixed-step `sim_tick`
/// and `dt` it was produced for, the target `object_id`, and the object's
/// `ownership_epoch` (so a reordered/late handoff cannot misapply it). The
/// `move_velocity` is the kinematic movement intent the server integrates
/// (CMC-style, §9.2); `payload` is opaque game data the wire never interprets.
/// Frames are **individually sequenced**, never coalesced into a "highest seq".
#[derive(Debug, Clone, PartialEq)]
pub struct InputFrame {
    /// Monotonic per-object input sequence number (`0` reserved for "none").
    pub input_seq: u32,
    /// Fixed-step simulation tick this input was produced for.
    pub sim_tick: u32,
    /// Timestep in seconds this input covers.
    pub dt: f32,
    /// Target object id.
    pub object_id: u32,
    /// The object's ownership epoch when the input was produced.
    pub ownership_epoch: u32,
    /// Kinematic movement intent in cm/s the server integrates over `dt`.
    pub move_velocity: [f32; 3],
    /// Opaque game-defined input payload (never interpreted by the wire).
    pub payload: Vec<u8>,
    /// An optional lag-compensated fire command riding this frame.
    pub fire: Option<FireCommand>,
}

/// A redundant bundle of the owner's last N input frames (kind 9, unreliable).
///
/// Redundant bundling (the last N frames each packet) makes a single datagram
/// loss self-heal (design §2.5, §2.8): the server dedups by `input_seq`. The
/// piggybacked `acked_snapshot_id` / `last_seen_snapshot_id` fold the snapshot
/// ack (kind 10) and the client's last-seen snapshot hint into the same packet.
#[derive(Debug, Clone, PartialEq)]
pub struct InputBundle {
    /// Newest fully-applied snapshot id the client acks (`0` = none).
    pub acked_snapshot_id: u32,
    /// Newest snapshot id the client has *seen* (rewind-time hint only; the
    /// server never trusts it as authority, design §5.2).
    pub last_seen_snapshot_id: u32,
    /// The redundant input frames, oldest first.
    pub frames: Vec<InputFrame>,
}

/// Fixed per-frame byte overhead in an [`InputBundle`] before the payload and the
/// optional fire command: `input_seq 4 · sim_tick 4 · dt 4 · object_id 4 ·
/// ownership_epoch 4 · move_velocity 12 · flags 1 · payload_len 2`.
const INPUT_FRAME_FIXED_BYTES: usize = 4 + 4 + 4 + 4 + 4 + 12 + 1 + 2;
/// Bytes in a serialized [`FireCommand`]: `origin[3] + direction[3]` f32.
const FIRE_BYTES: usize = 3 * 4 + 3 * 4;
/// Fire-present flag in an [`InputFrame`]'s `flags` byte.
const INPUT_FLAG_FIRE: u8 = 0b0000_0001;
/// Max input frames per bundle (bounds decode work; redundancy is only ~3-8).
pub const MAX_INPUT_FRAMES: usize = 32;

fn put_f32(buf: &mut Vec<u8>, v: f32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_vec3(buf: &mut Vec<u8>, v: [f32; 3]) {
    for c in v {
        put_f32(buf, c);
    }
}

fn get_vec3(bytes: &[u8], off: &mut usize) -> [f32; 3] {
    [
        get_f32(bytes, off),
        get_f32(bytes, off),
        get_f32(bytes, off),
    ]
}

fn get_u16(bytes: &[u8], off: &mut usize) -> u16 {
    let v = u16::from_be_bytes([bytes[*off], bytes[*off + 1]]);
    *off += 2;
    v
}

impl InputBundle {
    /// Encode the input bundle body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.acked_snapshot_id.to_be_bytes());
        buf.extend_from_slice(&self.last_seen_snapshot_id.to_be_bytes());
        let count = self.frames.len().min(MAX_INPUT_FRAMES) as u8;
        buf.push(count);
        for f in self.frames.iter().take(MAX_INPUT_FRAMES) {
            buf.extend_from_slice(&f.input_seq.to_be_bytes());
            buf.extend_from_slice(&f.sim_tick.to_be_bytes());
            put_f32(&mut buf, f.dt);
            buf.extend_from_slice(&f.object_id.to_be_bytes());
            buf.extend_from_slice(&f.ownership_epoch.to_be_bytes());
            put_vec3(&mut buf, f.move_velocity);
            let mut flags = 0u8;
            if f.fire.is_some() {
                flags |= INPUT_FLAG_FIRE;
            }
            buf.push(flags);
            let payload_len = f.payload.len().min(u16::MAX as usize) as u16;
            buf.extend_from_slice(&payload_len.to_be_bytes());
            buf.extend_from_slice(&f.payload[..payload_len as usize]);
            if let Some(fire) = f.fire {
                put_vec3(&mut buf, fire.origin);
                put_vec3(&mut buf, fire.direction);
            }
        }
        buf
    }

    /// Decode an input bundle body. Rejects truncated or over-long bundles.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        if body.len() < 9 {
            return Err(TsyncError::TooShort {
                needed: 9,
                got: body.len(),
            });
        }
        let mut off = 0usize;
        let acked_snapshot_id = get_u32(body, &mut off);
        let last_seen_snapshot_id = get_u32(body, &mut off);
        let count = body[off] as usize;
        off += 1;
        if count > MAX_INPUT_FRAMES {
            return Err(TsyncError::CountMismatch("too many input frames"));
        }
        let mut frames = Vec::with_capacity(count);
        for _ in 0..count {
            if body.len() < off + INPUT_FRAME_FIXED_BYTES {
                return Err(TsyncError::TooShort {
                    needed: off + INPUT_FRAME_FIXED_BYTES,
                    got: body.len(),
                });
            }
            let input_seq = get_u32(body, &mut off);
            let sim_tick = get_u32(body, &mut off);
            let dt = get_f32(body, &mut off);
            let object_id = get_u32(body, &mut off);
            let ownership_epoch = get_u32(body, &mut off);
            let move_velocity = get_vec3(body, &mut off);
            let flags = body[off];
            off += 1;
            let payload_len = get_u16(body, &mut off) as usize;
            if body.len() < off + payload_len {
                return Err(TsyncError::TooShort {
                    needed: off + payload_len,
                    got: body.len(),
                });
            }
            let payload = body[off..off + payload_len].to_vec();
            off += payload_len;
            let fire = if flags & INPUT_FLAG_FIRE != 0 {
                if body.len() < off + FIRE_BYTES {
                    return Err(TsyncError::TooShort {
                        needed: off + FIRE_BYTES,
                        got: body.len(),
                    });
                }
                let origin = get_vec3(body, &mut off);
                let direction = get_vec3(body, &mut off);
                Some(FireCommand { origin, direction })
            } else {
                None
            };
            frames.push(InputFrame {
                input_seq,
                sim_tick,
                dt,
                object_id,
                ownership_epoch,
                move_velocity,
                payload,
                fire,
            });
        }
        Ok(Self {
            acked_snapshot_id,
            last_seen_snapshot_id,
            frames,
        })
    }
}

/// The authoritative result of a lag-compensated fire (kind 12, S→C, reliable).
///
/// Returned for a [`FireCommand`] that rode an [`InputFrame`]; correlated by that
/// frame's `input_seq`. The client applies this result and never resolves the hit
/// itself (design §5.2). `rewind_tick` is the server tick the world was rewound to
/// (the server computed + clamped it from its own state) — carried for client-side
/// transparency/debug only, never as authority.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RewindResult {
    /// The input seq of the fire command this answers.
    pub input_seq: u32,
    /// Whether a target was hit.
    pub hit: bool,
    /// The object hit (`0` = none/miss).
    pub object_id: u32,
    /// The impact point in cm (zeroed on a miss).
    pub hit_point: [f32; 3],
    /// The server tick the world was rewound to (server-computed, clamped).
    pub rewind_tick: u32,
}

/// Bytes in a serialized [`RewindResult`] body: `input_seq 4 · flags 1 ·
/// object_id 4 · hit_point 12 · rewind_tick 4`.
pub const REWIND_RESULT_BYTES: usize = 4 + 1 + 4 + 12 + 4;
/// Hit flag in a [`RewindResult`]'s flags byte.
const REWIND_FLAG_HIT: u8 = 0b0000_0001;

impl RewindResult {
    /// Encode the rewind-result body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(REWIND_RESULT_BYTES);
        buf.extend_from_slice(&self.input_seq.to_be_bytes());
        buf.push(if self.hit { REWIND_FLAG_HIT } else { 0 });
        buf.extend_from_slice(&self.object_id.to_be_bytes());
        put_vec3(&mut buf, self.hit_point);
        buf.extend_from_slice(&self.rewind_tick.to_be_bytes());
        buf
    }

    /// Decode a rewind-result body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        if body.len() < REWIND_RESULT_BYTES {
            return Err(TsyncError::TooShort {
                needed: REWIND_RESULT_BYTES,
                got: body.len(),
            });
        }
        let mut off = 0usize;
        let input_seq = get_u32(body, &mut off);
        let hit = body[off] & REWIND_FLAG_HIT != 0;
        off += 1;
        let object_id = get_u32(body, &mut off);
        let hit_point = get_vec3(body, &mut off);
        let rewind_tick = get_u32(body, &mut off);
        Ok(Self {
            input_seq,
            hit,
            object_id,
            hit_point,
            rewind_tick,
        })
    }
}

/// The default interpolation-grade velocity bounds: ±32768 cm/s per axis at 4
/// codes/cm/s (~0.25 cm/s precision). Chosen so a fast avatar (a few hundred
/// cm/s) quantizes finely while covering projectile-class speeds.
pub const DEFAULT_VELOCITY_BOUNDS: WorldBounds = WorldBounds {
    min: [-32768.0, -32768.0, -32768.0],
    max: [32768.0, 32768.0, 32768.0],
    values_per_unit: 4,
};

impl Default for Hello {
    fn default() -> Self {
        Self {
            position_bounds: crate::codec::DEFAULT_WORLD_BOUNDS,
            velocity_bounds: DEFAULT_VELOCITY_BOUNDS,
            quat_mode: QuatMode::Bits10,
            send_rate_hz: 20,
            sim_rate_hz: 60,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::baseline::AckField;

    fn codec() -> TransformCodec {
        TransformCodec::from_hello(&Hello::default()).unwrap()
    }

    #[test]
    fn hello_round_trips() {
        let hello = Hello::default();
        let back = Hello::decode(&hello.encode()).unwrap();
        assert_eq!(hello, back);
    }

    #[test]
    fn hello_rejects_short_and_bad_quat() {
        assert!(matches!(
            Hello::decode(&[0u8; 4]),
            Err(TsyncError::TooShort { .. })
        ));
        let mut body = Hello::default().encode();
        // Corrupt the quat_mode byte (after the two bounds blocks).
        body[WORLD_BOUNDS_BYTES * 2] = 7;
        assert!(matches!(
            Hello::decode(&body),
            Err(TsyncError::OutOfRange("quat_mode"))
        ));
    }

    #[test]
    fn role_round_trips_and_rejects_bad_bytes() {
        let role = Role {
            object_id: 42,
            role: SyncRole::ServerSimulated,
            owner: 7,
            ownership_epoch: 3,
            gen_epoch: 1,
            event: RoleEvent::RelevancyEnter,
        };
        let back = Role::decode(&role.encode()).unwrap();
        assert_eq!(role, back);

        let mut body = role.encode();
        body[4] = 99; // bad role byte
        assert!(matches!(
            Role::decode(&body),
            Err(TsyncError::OutOfRange("role"))
        ));
    }

    #[test]
    fn ack_round_trips_and_matches_ackfield_window() {
        let ack = Ack {
            acked_snapshot_id: 100,
            history: 0b1011,
        };
        assert_eq!(Ack::decode(&ack.encode()).unwrap(), ack);

        // The wire ack maps straight onto the shared AckField window.
        let field = AckField::from_wire(u64::from(ack.acked_snapshot_id), ack.history).unwrap();
        assert_eq!(field.latest(), Some(100));
        assert!(field.is_acked(100));
    }

    #[test]
    fn snapshot_full_round_trips() {
        let codec = codec();
        let snap = Snapshot {
            server_tick: 123,
            snapshot_id: 5,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![ObjectUpdate {
                object_id: 1,
                gen_epoch: 0,
                fields: TransformFields {
                    position: Some([100.0, -200.5, 30.0]),
                    rotation: Some([0.0, 0.0, 0.0, 1.0]),
                    velocity: Some([10.0, 0.0, 0.0]),
                },
                last_input_seq: None,
            }],
        };
        let bytes = snap.encode(&codec).unwrap();
        let back = Snapshot::decode(&bytes, &codec).unwrap();
        assert_eq!(back.server_tick, 123);
        assert_eq!(back.snapshot_id, 5);
        assert_eq!(back.base_snapshot_id, 0);
        assert_eq!(back.updates.len(), 1);
        let u = back.updates[0];
        assert_eq!(u.object_id, 1);
        let pos = u.fields.position.unwrap();
        assert!((pos[0] - 100.0).abs() <= 0.0625);
        assert!((pos[1] - (-200.5)).abs() <= 0.0625);
        assert!(u.fields.rotation.is_some());
        let vel = u.fields.velocity.unwrap();
        assert!((vel[0] - 10.0).abs() <= 0.25);
    }

    #[test]
    fn snapshot_delta_omits_unchanged_fields() {
        let codec = codec();
        // Only rotation present (a delta where position/velocity were unchanged).
        let snap = Snapshot {
            server_tick: 200,
            snapshot_id: 9,
            base_snapshot_id: 5,
            send_rate_hz: 20,
            removed: vec![77],
            updates: vec![ObjectUpdate {
                object_id: 3,
                gen_epoch: 2,
                fields: TransformFields {
                    position: None,
                    rotation: Some([
                        0.0,
                        core::f32::consts::FRAC_1_SQRT_2,
                        0.0,
                        core::f32::consts::FRAC_1_SQRT_2,
                    ]),
                    velocity: None,
                },
                last_input_seq: None,
            }],
        };
        let bytes = snap.encode(&codec).unwrap();
        let back = Snapshot::decode(&bytes, &codec).unwrap();
        assert_eq!(back.base_snapshot_id, 5);
        assert_eq!(back.removed, vec![77]);
        let u = back.updates[0];
        assert!(u.fields.position.is_none());
        assert!(u.fields.velocity.is_none());
        assert!(u.fields.rotation.is_some());
        assert_eq!(u.gen_epoch, 2);
    }

    #[test]
    fn snapshot_empty_object_list_round_trips() {
        let codec = codec();
        let snap = Snapshot {
            server_tick: 1,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 30,
            removed: vec![],
            updates: vec![],
        };
        let bytes = snap.encode(&codec).unwrap();
        let back = Snapshot::decode(&bytes, &codec).unwrap();
        assert_eq!(back, snap);
    }

    #[test]
    fn snapshot_carries_owner_last_input_seq() {
        let codec = codec();
        let snap = Snapshot {
            server_tick: 7,
            snapshot_id: 3,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![
                // Owned object: full fields + last_input_seq.
                ObjectUpdate {
                    object_id: 1,
                    gen_epoch: 0,
                    fields: TransformFields {
                        position: Some([1.0, 2.0, 3.0]),
                        rotation: Some([0.0, 0.0, 0.0, 1.0]),
                        velocity: None,
                    },
                    last_input_seq: Some(42),
                },
                // Remote object: no input seq (pays no extra bytes).
                ObjectUpdate {
                    object_id: 2,
                    gen_epoch: 0,
                    fields: TransformFields {
                        position: Some([4.0, 5.0, 6.0]),
                        rotation: Some([0.0, 0.0, 0.0, 1.0]),
                        velocity: None,
                    },
                    last_input_seq: None,
                },
            ],
        };
        let back = Snapshot::decode(&snap.encode(&codec).unwrap(), &codec).unwrap();
        assert_eq!(back.updates[0].last_input_seq, Some(42));
        assert_eq!(back.updates[1].last_input_seq, None);
    }

    #[test]
    fn input_bundle_round_trips_with_and_without_fire() {
        let bundle = InputBundle {
            acked_snapshot_id: 12,
            last_seen_snapshot_id: 13,
            frames: vec![
                InputFrame {
                    input_seq: 100,
                    sim_tick: 500,
                    dt: 1.0 / 60.0,
                    object_id: 7,
                    ownership_epoch: 2,
                    move_velocity: [300.0, 0.0, -50.0],
                    payload: vec![1, 2, 3],
                    fire: None,
                },
                InputFrame {
                    input_seq: 101,
                    sim_tick: 501,
                    dt: 1.0 / 60.0,
                    object_id: 7,
                    ownership_epoch: 2,
                    move_velocity: [0.0; 3],
                    payload: vec![],
                    fire: Some(FireCommand {
                        origin: [10.0, 20.0, 30.0],
                        direction: [1.0, 0.0, 0.0],
                    }),
                },
            ],
        };
        let back = InputBundle::decode(&bundle.encode()).unwrap();
        assert_eq!(back, bundle);
    }

    #[test]
    fn input_bundle_rejects_too_many_and_truncated() {
        // count claims more frames than present.
        let mut body = InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![],
        }
        .encode();
        body[8] = 5; // count = 5 but no frames follow
        assert!(matches!(
            InputBundle::decode(&body),
            Err(TsyncError::TooShort { .. })
        ));
        // count above the hard cap is rejected outright.
        body[8] = (MAX_INPUT_FRAMES + 1) as u8;
        assert!(matches!(
            InputBundle::decode(&body),
            Err(TsyncError::CountMismatch(_))
        ));
    }

    #[test]
    fn rewind_result_round_trips_hit_and_miss() {
        let hit = RewindResult {
            input_seq: 55,
            hit: true,
            object_id: 9,
            hit_point: [1.5, -2.5, 3.5],
            rewind_tick: 480,
        };
        assert_eq!(RewindResult::decode(&hit.encode()).unwrap(), hit);
        let miss = RewindResult {
            input_seq: 56,
            hit: false,
            object_id: 0,
            hit_point: [0.0; 3],
            rewind_tick: 481,
        };
        assert_eq!(RewindResult::decode(&miss.encode()).unwrap(), miss);
        assert!(matches!(
            RewindResult::decode(&[0u8; 3]),
            Err(TsyncError::TooShort { .. })
        ));
    }

    #[test]
    fn snapshot_decode_rejects_truncated_body() {
        let codec = codec();
        let snap = Snapshot {
            server_tick: 1,
            snapshot_id: 2,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![ObjectUpdate {
                object_id: 1,
                gen_epoch: 0,
                fields: TransformFields {
                    position: Some([0.0, 0.0, 0.0]),
                    rotation: Some([0.0, 0.0, 0.0, 1.0]),
                    velocity: None,
                },
                last_input_seq: None,
            }],
        };
        let mut bytes = snap.encode(&codec).unwrap();
        bytes.truncate(bytes.len() - 2);
        assert!(Snapshot::decode(&bytes, &codec).is_err());
    }
}
