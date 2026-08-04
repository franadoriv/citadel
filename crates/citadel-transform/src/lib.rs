//! Shared transform-synchronization types and client-side world reconstruction.
//!
//! This crate holds the half of transform sync that both sides of the wire need:
//! the per-object [`TransformState`], and [`RemoteWorldView`], the client runtime
//! core that decodes snapshots, reconstructs full state against the held base,
//! feeds a per-object jitter buffer, and renders in the past with Hermite
//! interpolation and bounded extrapolation.
//!
//! It exists as a leaf beside `citadel-wire` so the engine SDK C ABI can do that
//! reconstruction without depending on the server crate. Before the split,
//! building the plugin shipped inside Unity, Unreal and Godot compiled the entire
//! game server — sqlx, mongodb, axum, quinn, a vendored Lua — to link two structs.
//!
//! The server-side authority (`TransformAuthority`, the world, the snapshot
//! builders, the hub) deliberately stays in the server: it is not something a
//! client needs, and keeping it out preserves this crate as a leaf.

pub mod client;
pub mod state;

pub use client::{RemoteObject, RemoteWorldView};
pub use state::TransformState;

/// A match-unique replicated-object id. 32-bit on the wire (design §8); widened
/// to the interest grid's `u64` key internally.
pub type ObjectId = u32;

// Re-export the wire role so both sides speak one vocabulary with the wire.
pub use citadel_wire::tsync::SyncRole;
