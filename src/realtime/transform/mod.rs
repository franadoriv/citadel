//! Authoritative transform synchronization (, transform-sync P1).
//!
//! This is the server half of the transform-sync MVP (design
//! `website/src/content/docs/reference/client-sdk/transform-sync.md` §7): an authoritative-first snapshot
//! system for `RemoteInterpolated` / `ServerSimulated` / `StaticReplicated`
//! objects (no owner prediction — that is P2/). It consumes the shared
//!  wire foundation (`citadel_wire::{bits, codec, baseline, interest,
//! tsync}`) rather than reinventing bit packing, quantizers, the ack window, or
//! the interest grid.
//!
//! Pieces:
//!
//! - [`TransformWorld`] holds every [`TransformAuthority`] and the
//!   [`citadel_wire::interest::InterestGrid`]. A **sim tick** advances the world
//!   (kinematic velocity integration + `set_transform`) and, once complete,
//!   **latches** an immutable [`Frame`] (double-buffer, design §7.5). The snapshot
//!   tick reads only the latest latched frame, so a snapshot can never mix two
//!   sim ticks.
//! - [`ClientSnapshotState`] builds one client's delta snapshot against the
//!   Quake3-style per-client ring of reconstructed full states (see its docs):
//!   each snapshot is diffed against a base the client provably holds, and the
//!   server reconstructs exactly what the client will, so loss/reorder self-heal
//!   over unordered datagrams.
//! - [`RemoteWorldView`] is the reusable client runtime core: it decodes
//!   snapshots, reconstructs full state against the held base, feeds a per-object
//!   jitter buffer, and renders in the past with Hermite (position, when velocity
//!   is replicated) + slerp (rotation) and bounded extrapolation on drain.
//! - [`TransformHub`] ties the world, the per-client builders, and the negotiated
//!   codec together for the gateway: it owns the sim tick, the snapshot fan-out,
//!   and the `HELLO`/`ACK` handling.

mod authority;
mod congestion;
mod hub;
mod input;
mod prediction;
mod rewind;
mod snapshot;
mod world;

pub use authority::TransformAuthority;
// The client-side reconstruction and the shared per-object state now live in
// the `citadel-transform` leaf crate, so the engine SDK can use them without
// depending on this crate. Re-exported here so server code and the existing
// `crate::realtime::transform::*` paths are unchanged.
pub use citadel_transform::{RemoteObject, RemoteWorldView, TransformState};
pub use congestion::{CongestionConfig, CongestionController, CongestionSignals, SendMode};
pub use hub::{HubOutbound, OwnerMovementMode, TransformHub, TransformHubConfig};
pub use input::{InputLimits, InputOutcome, OwnerInputQueue, integrate_owner_frame};
pub use prediction::{PredictedInput, PredictionRing, ReconcileConfig};
pub use rewind::{
    HitOutcome, HitRay, HitTarget, LagProfile, RewindBuffer, RewindConfig, compute_rewind_tick,
    lag_comp_enabled, resolve_hit,
};
pub use snapshot::ClientSnapshotState;
pub use world::{Frame, FrameObject, PhysicsState, TransformWorld};

// The object id and the wire role are defined alongside the shared state.
pub use citadel_transform::{ObjectId, SyncRole};
