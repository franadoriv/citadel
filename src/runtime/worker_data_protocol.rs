//! Versioned data-plane frames for per-match execution in the worker.
//!
//! The control plane (`worker_protocol`) carries supervision traffic: hello,
//! health, shutdown. This module is the data plane: the frames that move one
//! live match's events into the supervised GameScript worker and its command
//! batches back out. It reuses the control plane's framing idiom — length
//! prefix, symmetric fail-closed size cap on both the writer and the reader,
//! versioned self-describing payloads — with a data-sized cap.
//!
//! Every frame carries a `(match_id, epoch, seq)` header:
//!
//! - `match_id` scopes the frame to one live match (worker-scoped
//!   [`DataFrame::EngineReport`] frames use the reserved id `0`, which is
//!   never a match: room ids start at 1).
//! - `epoch` fences worker generations. The gateway bumps the epoch every
//!   time the worker process is (re)booted, so a frame from a previous worker
//!   generation can never be mistaken for current traffic and a restarted
//!   worker can never resume a match it no longer hosts.
//! - `seq` is a per-match monotone counter; a replayed or reordered frame
//!   fails validation instead of mutating match state twice.
//!
//! Reception is fail-closed: [`DataPlaneRx`] drops (and counts) every frame
//! that is not a well-formed, current-epoch, in-sequence frame for a match
//! the gateway actually opened.

use serde::{Deserialize, Serialize};

use super::worker_protocol::ProtocolError;

/// Version of the data-plane frame set. Independent of the control-plane
/// version so supervision and match execution can evolve separately.
pub const DATA_PROTOCOL_VERSION: u16 = 1;

/// Symmetric fail-closed frame cap, enforced on write and read exactly like
/// `MAX_CONTROL_FRAME_BYTES` on the control plane. The value reuses the
/// existing per-invocation aggregate command budget (`MAX_BODY_BYTES` in
/// `runtime::append_runtime_event_commands`, 1 MiB) rather than inventing a
/// new number: one frame carries at most one invocation's worth of commands.
pub const MAX_DATA_FRAME_BYTES: usize = 1 << 20;

/// Reserved `match_id` for worker-scoped frames ([`DataFrame::EngineReport`]).
/// Never a real match: room ids are assigned starting at 1.
pub const WORKER_SCOPE_MATCH_ID: u64 = 0;

/// `(match_id, epoch, seq)` carried by every data-plane frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameHeader {
    /// The live match this frame belongs to ([`WORKER_SCOPE_MATCH_ID`] for
    /// worker-scoped frames).
    pub match_id: u64,
    /// Worker-generation fence; bumped by the gateway on every worker boot.
    pub epoch: u64,
    /// Per-match monotone sequence number (starts at 1 for each match).
    pub seq: u64,
}

/// Why a match was closed by the worker (or by the gateway on its behalf).
///
/// Every reason maps to the same client-facing outcome — the members receive
/// a server-error `KIND_MATCH_CLOSED` with a requeue hint — but the reasons
/// stay distinct on the wire for diagnostics and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchCloseReason {
    /// A script fault (error, panic isolation, or blown per-invocation
    /// deadline) exceeded the per-match overrun policy.
    ServerError,
    /// The match's bounded mailbox overflowed; the match fails closed rather
    /// than growing without bound.
    MailboxOverflow,
    /// The match's execution context wedged inside a non-reclaimable call and
    /// its thread was quarantined.
    Quarantined,
    /// The engine hosting this match died (e.g. the Python interpreter is
    /// unrecoverable); the worker process is replaced.
    EngineDead,
    /// Orderly close during worker shutdown.
    Shutdown,
}

/// Worker-scoped engine status reports (heartbeat payload extensions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineReport {
    /// Periodic scheduler-liveness heartbeat.
    Heartbeat {
        /// Monotone count of completed fair-scheduling rounds.
        scheduler_rounds: u64,
        /// Number of currently open matches.
        live_matches: u32,
        /// Number of execution threads quarantined since boot.
        quarantined_threads: u32,
    },
    /// The single hosted engine is dead and the worker must be replaced.
    EngineDead {
        /// Stable engine token (`lua` / `js` / `python`).
        engine: String,
    },
}

/// One data-plane frame. All variants carry [`FrameHeader`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataFrame {
    /// Gateway → worker: open a per-match execution context.
    MatchOpen {
        protocol_version: u16,
        header: FrameHeader,
        /// Identity of the script revision the match must run under, mirroring
        /// `WorkerReady.script_identity` for revision fencing.
        script_identity: Option<String>,
    },
    /// Gateway → worker: one inbound invocation of the existing runtime
    /// dispatch surface, scoped to the match.
    MatchEvent {
        protocol_version: u16,
        header: FrameHeader,
        /// Originating participant (raw session id); `0` for sender-less
        /// invocations such as ticks.
        sender: u64,
        /// Authenticated user id, when the participant is not a guest.
        user_id: Option<String>,
        /// Wire kind of the inbound envelope (or a tick marker kind).
        kind: u16,
        /// Opaque envelope body bytes.
        body: Vec<u8>,
    },
    /// Worker → gateway: the command batch produced by one invocation.
    MatchCommands {
        protocol_version: u16,
        header: FrameHeader,
        /// Encoded outbound-command batch (the existing runtime dispatch
        /// results; the semantic command contract is carried opaquely here).
        commands: Vec<u8>,
    },
    /// Worker → gateway (or gateway-local): the match is closed.
    MatchClosed {
        protocol_version: u16,
        header: FrameHeader,
        reason: MatchCloseReason,
    },
    /// Worker → gateway: worker-scoped engine status
    /// (`header.match_id == WORKER_SCOPE_MATCH_ID`).
    EngineReport {
        protocol_version: u16,
        header: FrameHeader,
        report: EngineReport,
    },
}

impl DataFrame {
    /// The frame's `(match_id, epoch, seq)` header.
    #[must_use]
    pub fn header(&self) -> FrameHeader {
        match self {
            Self::MatchOpen { header, .. }
            | Self::MatchEvent { header, .. }
            | Self::MatchCommands { header, .. }
            | Self::MatchClosed { header, .. }
            | Self::EngineReport { header, .. } => *header,
        }
    }

    /// The frame's declared protocol version.
    #[must_use]
    pub fn protocol_version(&self) -> u16 {
        match self {
            Self::MatchOpen {
                protocol_version, ..
            }
            | Self::MatchEvent {
                protocol_version, ..
            }
            | Self::MatchCommands {
                protocol_version, ..
            }
            | Self::MatchClosed {
                protocol_version, ..
            }
            | Self::EngineReport {
                protocol_version, ..
            } => *protocol_version,
        }
    }
}

/// Encode one data frame (no length prefix).
pub fn encode_data_frame(frame: &DataFrame) -> Result<Vec<u8>, ProtocolError> {
    serde_json::to_vec(frame).map_err(|_| ProtocolError::MalformedFrame)
}

/// Decode one data frame, rejecting oversized payloads before parsing.
pub fn decode_data_frame(bytes: &[u8]) -> Result<DataFrame, ProtocolError> {
    if bytes.len() > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::MalformedFrame)
}

/// Write one length-prefixed data frame; nothing reaches the transport for an
/// oversized frame so the stream never desynchronizes (the same symmetric
/// fail-closed contract as `write_control_frame`).
pub fn write_data_frame(
    stream: &mut impl std::io::Write,
    frame: &DataFrame,
) -> Result<(), ProtocolError> {
    let payload = encode_data_frame(frame)?;
    if payload.len() > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(|_| ProtocolError::MalformedFrame)?;
    stream
        .write_all(&payload)
        .map_err(|_| ProtocolError::MalformedFrame)
}

/// Read one length-prefixed data frame, rejecting an oversized length prefix
/// before buffering anything.
pub fn read_data_frame(stream: &mut impl std::io::Read) -> Result<DataFrame, ProtocolError> {
    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|_| ProtocolError::MalformedFrame)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .map_err(|_| ProtocolError::MalformedFrame)?;
    decode_data_frame(&payload)
}

/// Async twin of [`write_data_frame`], byte-for-byte identical on the wire.
///
/// The parent pumps the data plane through tokio (the Windows named-pipe
/// transport is async-only) while the worker writes frames synchronously;
/// both variants share the encoder and the symmetric fail-closed size cap.
pub async fn write_data_frame_async<W>(
    stream: &mut W,
    frame: &DataFrame,
) -> Result<(), ProtocolError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let payload = encode_data_frame(frame)?;
    if payload.len() > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(|_| ProtocolError::MalformedFrame)?;
    stream
        .write_all(&payload)
        .await
        .map_err(|_| ProtocolError::MalformedFrame)
}

/// Async twin of [`read_data_frame`] with the same fail-closed limits.
pub async fn read_data_frame_async<R>(stream: &mut R) -> Result<DataFrame, ProtocolError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|_| ProtocolError::MalformedFrame)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|_| ProtocolError::MalformedFrame)?;
    decode_data_frame(&payload)
}

/// Serializable twin of the runtime's collision shape.
///
/// The physics types live in `citadel-physics`, which deliberately carries no
/// serde dependency; the data plane keeps its own wire twins instead of
/// leaking a serialization format into the physics crate.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
enum WireShape {
    Capsule { radius: f32, height: f32 },
    Aabb { half_extents: [f32; 3] },
}

/// Serializable twin of [`crate::runtime::PhysicsOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct WirePhysicsOptions {
    enabled: bool,
    shape: WireShape,
    gravity: f32,
    buoyancy: f32,
    drag: f32,
    max_speed: f32,
}

impl From<crate::runtime::PhysicsOptions> for WirePhysicsOptions {
    fn from(opts: crate::runtime::PhysicsOptions) -> Self {
        Self {
            enabled: opts.enabled,
            shape: match opts.config.shape {
                citadel_physics::Shape::Capsule { radius, height } => {
                    WireShape::Capsule { radius, height }
                }
                citadel_physics::Shape::Aabb { half_extents } => WireShape::Aabb { half_extents },
            },
            gravity: opts.config.gravity,
            buoyancy: opts.config.buoyancy,
            drag: opts.config.drag,
            max_speed: opts.config.max_speed,
        }
    }
}

impl From<WirePhysicsOptions> for crate::runtime::PhysicsOptions {
    fn from(opts: WirePhysicsOptions) -> Self {
        Self {
            enabled: opts.enabled,
            config: citadel_physics::PhysicsConfig {
                shape: match opts.shape {
                    WireShape::Capsule { radius, height } => {
                        citadel_physics::Shape::Capsule { radius, height }
                    }
                    WireShape::Aabb { half_extents } => {
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

/// Serializable twin of [`crate::runtime::OutboundCommand`], carried opaquely
/// in [`DataFrame::MatchCommands`]. The variants mirror the runtime command
/// surface one-to-one so the gateway applies exactly what the worker's match
/// produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum WireCommand {
    Broadcast {
        kind: u16,
        body: Vec<u8>,
        unreliable: bool,
    },
    Send {
        session: u64,
        kind: u16,
        body: Vec<u8>,
        unreliable: bool,
    },
    SpawnActor {
        object_id: u32,
        archetype: u16,
        position: [f32; 3],
    },
    MoveActor {
        object_id: u32,
        position: [f32; 3],
        rotation: [f32; 4],
        velocity: [f32; 3],
    },
    SetPhysics {
        object_id: u32,
        opts: Option<WirePhysicsOptions>,
    },
    ApplyImpulse {
        object_id: u32,
        impulse: [f32; 3],
    },
    SetMoveIntent {
        object_id: u32,
        intent: [f32; 3],
    },
    DespawnActor {
        object_id: u32,
    },
}

impl From<crate::runtime::OutboundCommand> for WireCommand {
    fn from(command: crate::runtime::OutboundCommand) -> Self {
        use crate::runtime::OutboundCommand as Cmd;
        match command {
            Cmd::Broadcast {
                kind,
                body,
                unreliable,
            } => Self::Broadcast {
                kind,
                body,
                unreliable,
            },
            Cmd::Send {
                session,
                kind,
                body,
                unreliable,
            } => Self::Send {
                session,
                kind,
                body,
                unreliable,
            },
            Cmd::SpawnActor {
                object_id,
                archetype,
                position,
            } => Self::SpawnActor {
                object_id,
                archetype,
                position,
            },
            Cmd::MoveActor {
                object_id,
                position,
                rotation,
                velocity,
            } => Self::MoveActor {
                object_id,
                position,
                rotation,
                velocity,
            },
            Cmd::SetPhysics { object_id, opts } => Self::SetPhysics {
                object_id,
                opts: opts.map(WirePhysicsOptions::from),
            },
            Cmd::ApplyImpulse { object_id, impulse } => Self::ApplyImpulse { object_id, impulse },
            Cmd::SetMoveIntent { object_id, intent } => Self::SetMoveIntent { object_id, intent },
            Cmd::DespawnActor { object_id } => Self::DespawnActor { object_id },
        }
    }
}

impl From<WireCommand> for crate::runtime::OutboundCommand {
    fn from(command: WireCommand) -> Self {
        match command {
            WireCommand::Broadcast {
                kind,
                body,
                unreliable,
            } => Self::Broadcast {
                kind,
                body,
                unreliable,
            },
            WireCommand::Send {
                session,
                kind,
                body,
                unreliable,
            } => Self::Send {
                session,
                kind,
                body,
                unreliable,
            },
            WireCommand::SpawnActor {
                object_id,
                archetype,
                position,
            } => Self::SpawnActor {
                object_id,
                archetype,
                position,
            },
            WireCommand::MoveActor {
                object_id,
                position,
                rotation,
                velocity,
            } => Self::MoveActor {
                object_id,
                position,
                rotation,
                velocity,
            },
            WireCommand::SetPhysics { object_id, opts } => Self::SetPhysics {
                object_id,
                opts: opts.map(crate::runtime::PhysicsOptions::from),
            },
            WireCommand::ApplyImpulse { object_id, impulse } => {
                Self::ApplyImpulse { object_id, impulse }
            }
            WireCommand::SetMoveIntent { object_id, intent } => {
                Self::SetMoveIntent { object_id, intent }
            }
            WireCommand::DespawnActor { object_id } => Self::DespawnActor { object_id },
        }
    }
}

/// Encode one invocation's command batch for [`DataFrame::MatchCommands`].
pub fn encode_commands(
    commands: &[crate::runtime::OutboundCommand],
) -> Result<Vec<u8>, ProtocolError> {
    let wire: Vec<WireCommand> = commands.iter().cloned().map(WireCommand::from).collect();
    serde_json::to_vec(&wire).map_err(|_| ProtocolError::MalformedFrame)
}

/// Decode a [`DataFrame::MatchCommands`] batch, rejecting oversized payloads
/// before parsing (the same fail-closed bound as the frame itself).
pub fn decode_commands(
    bytes: &[u8],
) -> Result<Vec<crate::runtime::OutboundCommand>, ProtocolError> {
    if bytes.len() > MAX_DATA_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let wire: Vec<WireCommand> =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::MalformedFrame)?;
    Ok(wire
        .into_iter()
        .map(crate::runtime::OutboundCommand::from)
        .collect())
}

/// Why [`DataPlaneRx`] rejected a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RxRejection {
    /// The frame declared an unsupported protocol version.
    UnsupportedVersion,
    /// The frame's epoch is not the current worker generation.
    StaleEpoch,
    /// The frame targets a match the gateway never opened (or already closed).
    UnknownMatch,
    /// The frame's sequence number was already observed for this match.
    ReplayedSeq,
}

/// Monotone drop counters, one per rejection class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RxCounters {
    pub unsupported_version: u64,
    pub stale_epoch: u64,
    pub unknown_match: u64,
    pub replayed_seq: u64,
}

/// Fail-closed receive-side validator for data-plane frames.
///
/// The gateway owns one per worker connection. Every accepted frame must be
/// current-epoch, target an open match (worker-scoped frames use
/// [`WORKER_SCOPE_MATCH_ID`]), and advance that match's sequence number.
/// Everything else is dropped and counted; a rejected frame mutates nothing.
#[derive(Debug)]
pub struct DataPlaneRx {
    epoch: u64,
    /// Highest accepted `seq` per open match (`0` = none yet). Presence in
    /// this table is what "open" means; `advance_epoch` clears it so a
    /// restarted worker can never resume old matches.
    open_matches: std::collections::HashMap<u64, u64>,
    counters: RxCounters,
}

impl DataPlaneRx {
    /// A validator fenced to `epoch` with no open matches.
    #[must_use]
    pub fn new(epoch: u64) -> Self {
        let mut open_matches = std::collections::HashMap::new();
        // The worker-scoped stream (heartbeats, engine reports) is always
        // open; it obeys the same epoch and sequence rules as match streams.
        open_matches.insert(WORKER_SCOPE_MATCH_ID, 0);
        Self {
            epoch,
            open_matches,
            counters: RxCounters::default(),
        }
    }

    /// The current worker-generation epoch frames must carry.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Point-in-time copy of the drop counters.
    #[must_use]
    pub fn counters(&self) -> RxCounters {
        self.counters
    }

    /// Ids of currently open matches (excluding the worker scope), unordered.
    #[must_use]
    pub fn open_match_ids(&self) -> Vec<u64> {
        self.open_matches
            .keys()
            .copied()
            .filter(|&id| id != WORKER_SCOPE_MATCH_ID)
            .collect()
    }

    /// Register `match_id` as open so its frames validate.
    pub fn open_match(&mut self, match_id: u64) {
        self.open_matches.entry(match_id).or_insert(0);
    }

    /// Remove `match_id`; its remaining in-flight frames fail as unknown.
    pub fn close_match(&mut self, match_id: u64) {
        if match_id != WORKER_SCOPE_MATCH_ID {
            self.open_matches.remove(&match_id);
        }
    }

    /// Fence to a new worker generation: every open match is dropped, so a
    /// restarted worker must observe a fresh `MatchOpen` before any of its
    /// frames validate, and every replayed old-epoch frame fails closed.
    pub fn advance_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
        self.open_matches.clear();
        self.open_matches.insert(WORKER_SCOPE_MATCH_ID, 0);
    }

    /// Validate one received frame. `Ok` frames advanced their match's
    /// sequence; rejected frames mutate nothing beyond their drop counter.
    pub fn accept(&mut self, frame: &DataFrame) -> Result<(), RxRejection> {
        if frame.protocol_version() != DATA_PROTOCOL_VERSION {
            self.counters.unsupported_version += 1;
            return Err(RxRejection::UnsupportedVersion);
        }
        let header = frame.header();
        if header.epoch != self.epoch {
            self.counters.stale_epoch += 1;
            return Err(RxRejection::StaleEpoch);
        }
        let Some(last_seq) = self.open_matches.get_mut(&header.match_id) else {
            self.counters.unknown_match += 1;
            return Err(RxRejection::UnknownMatch);
        };
        if header.seq <= *last_seq {
            self.counters.replayed_seq += 1;
            return Err(RxRejection::ReplayedSeq);
        }
        *last_seq = header.seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::runtime::{OutboundCommand, PhysicsOptions};

    fn header(match_id: u64, epoch: u64, seq: u64) -> FrameHeader {
        FrameHeader {
            match_id,
            epoch,
            seq,
        }
    }

    fn event(match_id: u64, epoch: u64, seq: u64) -> DataFrame {
        DataFrame::MatchEvent {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: header(match_id, epoch, seq),
            sender: 7,
            user_id: Some("user-a".to_string()),
            kind: 42,
            body: vec![1, 2, 3],
        }
    }

    #[test]
    fn every_data_frame_variant_round_trips() {
        let frames = [
            DataFrame::MatchOpen {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: header(1, 3, 1),
                script_identity: Some("sha256:abc".to_string()),
            },
            event(1, 3, 2),
            DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: header(1, 3, 3),
                commands: vec![9, 9],
            },
            DataFrame::MatchClosed {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: header(1, 3, 4),
                reason: MatchCloseReason::ServerError,
            },
            DataFrame::EngineReport {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: header(WORKER_SCOPE_MATCH_ID, 3, 5),
                report: EngineReport::Heartbeat {
                    scheduler_rounds: 10,
                    live_matches: 2,
                    quarantined_threads: 0,
                },
            },
        ];
        for frame in frames {
            let mut wire = Vec::new();
            write_data_frame(&mut wire, &frame).expect("write");
            assert_eq!(read_data_frame(&mut wire.as_slice()).expect("read"), frame);
            assert_eq!(frame.protocol_version(), DATA_PROTOCOL_VERSION);
        }
    }

    #[test]
    fn writer_rejects_oversized_frames_fail_closed() {
        let frame = DataFrame::MatchCommands {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: header(1, 1, 1),
            commands: vec![7; MAX_DATA_FRAME_BYTES + 1],
        };
        let mut wire = Vec::new();
        assert_eq!(
            write_data_frame(&mut wire, &frame),
            Err(ProtocolError::FrameTooLarge)
        );
        assert!(
            wire.is_empty(),
            "no bytes may reach the transport for an oversized frame"
        );
    }

    #[test]
    fn reader_rejects_oversized_length_prefixes_before_reading() {
        let mut wire = ((MAX_DATA_FRAME_BYTES + 1) as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(&[b' '; 8]);
        assert_eq!(
            read_data_frame(&mut wire.as_slice()),
            Err(ProtocolError::FrameTooLarge)
        );
    }

    #[test]
    fn reader_rejects_truncated_frames_fail_closed() {
        let mut wire = Vec::new();
        write_data_frame(&mut wire, &event(1, 1, 1)).expect("write");
        wire.truncate(wire.len() - 3);
        assert_eq!(
            read_data_frame(&mut wire.as_slice()),
            Err(ProtocolError::MalformedFrame)
        );
    }

    #[test]
    fn reader_rejects_malformed_payloads_fail_closed() {
        let payload = b"not a data frame";
        let mut wire = (payload.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(payload);
        assert_eq!(
            read_data_frame(&mut wire.as_slice()),
            Err(ProtocolError::MalformedFrame)
        );
    }

    #[test]
    fn stale_or_cross_match_reply_is_dropped() {
        let mut rx = DataPlaneRx::new(5);
        rx.open_match(1);
        assert_eq!(rx.accept(&event(1, 5, 1)), Ok(()));

        // Cross-match: match 2 was never opened. Nothing mutates.
        assert_eq!(rx.accept(&event(2, 5, 1)), Err(RxRejection::UnknownMatch));
        // Old epoch: a previous worker generation's frame fails closed.
        assert_eq!(rx.accept(&event(1, 4, 2)), Err(RxRejection::StaleEpoch));
        // Replayed sequence for the open match.
        assert_eq!(rx.accept(&event(1, 5, 1)), Err(RxRejection::ReplayedSeq));
        // Unsupported version fails before any state checks.
        let bad_version = DataFrame::MatchEvent {
            protocol_version: DATA_PROTOCOL_VERSION + 1,
            header: header(1, 5, 9),
            sender: 0,
            user_id: None,
            kind: 1,
            body: Vec::new(),
        };
        assert_eq!(
            rx.accept(&bad_version),
            Err(RxRejection::UnsupportedVersion)
        );

        assert_eq!(
            rx.counters(),
            RxCounters {
                unsupported_version: 1,
                stale_epoch: 1,
                unknown_match: 1,
                replayed_seq: 1,
            }
        );
        // The rejections mutated nothing: the very next in-sequence frame for
        // the open match is still accepted.
        assert_eq!(rx.accept(&event(1, 5, 2)), Ok(()));
    }

    #[test]
    fn advance_epoch_requires_a_fresh_match_open() {
        let mut rx = DataPlaneRx::new(1);
        rx.open_match(1);
        assert_eq!(rx.accept(&event(1, 1, 1)), Ok(()));

        // The worker restarted: the gateway fences to a new generation.
        rx.advance_epoch(2);
        assert!(
            rx.open_match_ids().is_empty(),
            "no match survives a restart"
        );
        // A replayed old-epoch frame is dropped and counted.
        assert_eq!(rx.accept(&event(1, 1, 2)), Err(RxRejection::StaleEpoch));
        // Even a current-epoch frame fails until MatchOpen re-registers it.
        assert_eq!(rx.accept(&event(1, 2, 1)), Err(RxRejection::UnknownMatch));
        rx.open_match(1);
        assert_eq!(rx.accept(&event(1, 2, 1)), Ok(()));
        assert_eq!(rx.counters().stale_epoch, 1);
        assert_eq!(rx.counters().unknown_match, 1);
    }

    #[test]
    fn every_outbound_command_round_trips_through_the_wire_codec() {
        // One of each `OutboundCommand` variant: the wire twin must preserve
        // the full command surface so the gateway applies exactly what the
        // worker's match produced.
        let commands = vec![
            OutboundCommand::Broadcast {
                kind: 40,
                body: vec![1, 2],
                unreliable: true,
            },
            OutboundCommand::Send {
                session: 9,
                kind: 41,
                body: vec![3],
                unreliable: false,
            },
            OutboundCommand::SpawnActor {
                object_id: 0x4000_0001,
                archetype: 7,
                position: [1.0, 2.0, 3.0],
            },
            OutboundCommand::MoveActor {
                object_id: 0x4000_0001,
                position: [4.0, 5.0, 6.0],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [7.0, 8.0, 9.0],
            },
            OutboundCommand::SetPhysics {
                object_id: 0x4000_0001,
                opts: Some(PhysicsOptions::default()),
            },
            OutboundCommand::SetPhysics {
                object_id: 0x4000_0002,
                opts: None,
            },
            OutboundCommand::ApplyImpulse {
                object_id: 0x4000_0001,
                impulse: [0.0, 100.0, 0.0],
            },
            OutboundCommand::SetMoveIntent {
                object_id: 0x4000_0001,
                intent: [50.0, 0.0, 0.0],
            },
            OutboundCommand::DespawnActor {
                object_id: 0x4000_0001,
            },
        ];
        let encoded = encode_commands(&commands).expect("encode");
        assert_eq!(decode_commands(&encoded).expect("decode"), commands);
    }

    #[test]
    fn wire_codec_preserves_aabb_physics_shapes() {
        let commands = vec![OutboundCommand::SetPhysics {
            object_id: 1,
            opts: Some(PhysicsOptions {
                enabled: false,
                config: citadel_physics::PhysicsConfig {
                    shape: citadel_physics::Shape::Aabb {
                        half_extents: [10.0, 20.0, 30.0],
                    },
                    gravity: 1.0,
                    buoyancy: 2.0,
                    drag: 3.0,
                    max_speed: 4.0,
                },
            }),
        }];
        let encoded = encode_commands(&commands).expect("encode");
        assert_eq!(decode_commands(&encoded).expect("decode"), commands);
    }

    #[test]
    fn command_decoder_rejects_oversized_and_malformed_payloads() {
        assert_eq!(
            decode_commands(&vec![b' '; MAX_DATA_FRAME_BYTES + 1]),
            Err(ProtocolError::FrameTooLarge)
        );
        assert_eq!(
            decode_commands(b"not a command batch"),
            Err(ProtocolError::MalformedFrame)
        );
    }

    #[tokio::test]
    async fn async_and_sync_data_framing_share_one_wire_format() {
        // The parent pumps the data plane through tokio while the worker
        // writes frames synchronously (and vice versa); both sides must agree
        // on the exact length-prefixed encoding.
        let frame = event(1, 3, 2);
        let mut wire = Vec::new();
        write_data_frame(&mut wire, &frame).expect("sync write");
        assert_eq!(
            read_data_frame_async(&mut wire.as_slice())
                .await
                .expect("async read"),
            frame
        );
        let mut wire = Vec::new();
        write_data_frame_async(&mut wire, &frame)
            .await
            .expect("async write");
        assert_eq!(
            read_data_frame(&mut wire.as_slice()).expect("sync read"),
            frame
        );
    }

    #[tokio::test]
    async fn async_data_reader_rejects_oversized_length_prefixes_before_reading() {
        let mut wire = ((MAX_DATA_FRAME_BYTES + 1) as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(&[b' '; 8]);
        assert_eq!(
            read_data_frame_async(&mut wire.as_slice()).await,
            Err(ProtocolError::FrameTooLarge)
        );
    }

    #[tokio::test]
    async fn async_data_writer_rejects_oversized_frames_fail_closed() {
        let frame = DataFrame::MatchCommands {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: header(1, 1, 1),
            commands: vec![7; MAX_DATA_FRAME_BYTES + 1],
        };
        let mut wire = Vec::new();
        assert_eq!(
            write_data_frame_async(&mut wire, &frame).await,
            Err(ProtocolError::FrameTooLarge)
        );
        assert!(
            wire.is_empty(),
            "no bytes may reach the transport for an oversized frame"
        );
    }

    #[test]
    fn worker_scope_stream_is_always_open_and_sequenced() {
        let mut rx = DataPlaneRx::new(3);
        let report = DataFrame::EngineReport {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: header(WORKER_SCOPE_MATCH_ID, 3, 1),
            report: EngineReport::EngineDead {
                engine: "python".to_string(),
            },
        };
        assert_eq!(rx.accept(&report), Ok(()));
        assert_eq!(rx.accept(&report), Err(RxRejection::ReplayedSeq));
        // Closing the worker scope is a no-op; it cannot be evicted.
        rx.close_match(WORKER_SCOPE_MATCH_ID);
        let next = DataFrame::EngineReport {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: header(WORKER_SCOPE_MATCH_ID, 3, 2),
            report: EngineReport::Heartbeat {
                scheduler_rounds: 1,
                live_matches: 0,
                quarantined_threads: 0,
            },
        };
        assert_eq!(rx.accept(&next), Ok(()));
    }
}
