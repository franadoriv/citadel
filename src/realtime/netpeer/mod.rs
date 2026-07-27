//! `NetworkPeer` property replication — Phase 1: the property table + push/shadow
//! dirty tracking. This is the Rust mirror of the Unreal
//! `UCitadelNetworkPeer` / `FCitadelRepLayout` change-detection layer described in
//! `docs/architecture/network-peer-property-replication.md` §2-§3.
//!
//! # What this module provides
//!
//! - [`RepLayout`] / [`FieldDesc`]: the immutable, per-class property table built
//!   **once at registration** (never per frame — the single biggest performance
//!   rule, §2.1), keyed by registration-order `field_id`, carrying the codec id,
//!   [`RepCondition`], [`FieldAuthority`], and [`FieldBounds`]. Its identity is a
//!   wide canonical [`citadel_wire::schema::SchemaHash`] (128-bit, ) plus
//!   a `layout_version`.
//! - [`NetworkPeer`]: push-model [`DirtyMask`] change tracking with the mandatory
//!   shadow-diff safety net and the dev/CI **pre-encode audit** that fails closed
//!   on a "forgot to mark dirty" bug rather than silently desyncing (§3.2).
//!
//! # Out of scope (later phases)
//!
//! - The `DeltaBunch` wire encode/decode, `KIND_REP_*` bodies, and per-connection
//!   baseline/ack: .
//! - The server untrusted-input validate/apply/rebroadcast pipeline: .
//!
//! # Reflection-once invariant
//!
//! On the Unreal side the `CPF_Net` reflection walk runs once at class
//! registration and caches an `FCitadelRepLayout`. The Rust mirror expresses the
//! same rule by building a [`RepLayout`] once (typically behind a
//! `std::sync::OnceLock`) and holding a `&'static` reference from every
//! [`NetworkPeer`]; the per-tick change-tracking calls never rebuild the layout.

pub mod authority;
pub mod delta;
pub mod dirty;
pub mod layout;
pub mod peer;

pub use authority::{
    RateLimits, RepAuthority, RepAuthorityMetrics, RepInterestConfig, RepOutbound, RepReject,
    RepVetoContext, RepVetoHook, Validated,
};
pub use delta::{
    BuiltDelta, CollectionState, MAX_COLLECTION_BASELINE, MAX_PENDING_PER_RECEIVER,
    ObjectReplicator, ReceiverId, RepSnapshot,
};
pub use dirty::DirtyMask;
pub use layout::{
    FieldAuthority, FieldBounds, FieldDesc, FieldId, LayoutError, MAX_FIELDS, RepCondition,
    RepLayout, RepLayoutBuilder, TypeTag, combined_bounds_shape, stable_key_from_name,
};
pub use peer::{FieldValue, NetworkPeer, Replicated, ShadowBuffer, UnmarkedChanges};
