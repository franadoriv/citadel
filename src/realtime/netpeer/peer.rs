//! [`NetworkPeer`]: per-object change tracking for the Rust mirror of the Unreal
//! `UCitadelNetworkPeer` (design §3). It combines the push-model dirty mask
//! (fast path) with the mandatory shadow-diff safety net for non-enforceable
//! fields, plus the dev/CI **pre-encode audit** that turns a "forgot to mark
//! dirty" bug into a hard failure instead of a silent desync (design §3.2,
//! review findings 1-2).
//!
//! Phase 1 (this task) implements marking, shadow detection, and the audit. It
//! does **not** build a `DeltaBunch` or touch per-connection baselines/acks — the
//! wire encode is . [`NetworkPeer::advance_after_encode`] models the
//! per-tick reset that the encode path will drive.

use super::dirty::DirtyMask;
use super::layout::{FieldId, RepLayout};

/// A readable snapshot of a single replicated field, used by the shadow-diff and
/// the audit to detect change without depending on the (Phase 2) wire codec.
///
/// Floats are stored as their raw bit pattern so equality is total and
/// deterministic (including NaN and signed-zero handling); the shadow only asks
/// "did the bits change", which is exactly the replication-relevant question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    /// A boolean field.
    Bool(bool),
    /// A signed integer field (widened to `i64`).
    Int(i64),
    /// An unsigned integer field (widened to `u64`).
    Uint(u64),
    /// A scalar `f32` field, stored as raw bits.
    F32Bits(u32),
    /// A three-component vector, stored as raw bits per axis.
    Vector3Bits([u32; 3]),
    /// A quaternion, stored as raw bits per component.
    QuatBits([u32; 4]),
    /// A length-delimited byte blob (string / packed struct / collection digest).
    Bytes(Vec<u8>),
}

impl FieldValue {
    /// Build a scalar value from an `f32` (stored as raw bits).
    #[must_use]
    pub fn f32(v: f32) -> Self {
        FieldValue::F32Bits(v.to_bits())
    }

    /// Build a vector value from three `f32`s.
    #[must_use]
    pub fn vector3(v: [f32; 3]) -> Self {
        FieldValue::Vector3Bits([v[0].to_bits(), v[1].to_bits(), v[2].to_bits()])
    }

    /// Build a quaternion value from four `f32`s.
    #[must_use]
    pub fn quat(v: [f32; 4]) -> Self {
        FieldValue::QuatBits([
            v[0].to_bits(),
            v[1].to_bits(),
            v[2].to_bits(),
            v[3].to_bits(),
        ])
    }
}

/// A replicated object exposes its layout and per-field snapshot values.
///
/// The Rust mirror of the Unreal reflection walk: [`Replicated::rep_layout`]
/// returns the layout built **once** at registration (never per frame — see the
/// module note), and [`Replicated::field_value`] reads one field's current value
/// for the shadow/audit path.
pub trait Replicated {
    /// The immutable per-class layout (built once, cached for the process life).
    fn rep_layout(&self) -> &'static RepLayout;

    /// The current value of field `field_id`. Implementations must return a
    /// stable variant per field id (matching the layout's `TypeTag`).
    fn field_value(&self, field_id: FieldId) -> FieldValue;
}

/// A per-registered-field shadow snapshot (design §3.2). Holds **only registered
/// fields** (indexed by `field_id`), never a full reflection copy, so a
/// shadow-diff is O(registered fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowBuffer {
    values: Vec<FieldValue>,
}

impl ShadowBuffer {
    /// Snapshot every registered field of `obj` in `field_id` order.
    fn snapshot(layout: &RepLayout, obj: &impl Replicated) -> Self {
        let values = layout
            .fields()
            .iter()
            .map(|f| obj.field_value(f.id))
            .collect();
        Self { values }
    }

    fn get(&self, id: FieldId) -> Option<&FieldValue> {
        self.values.get(id as usize)
    }

    fn set(&mut self, id: FieldId, value: FieldValue) {
        if let Some(slot) = self.values.get_mut(id as usize) {
            *slot = value;
        }
    }

    fn resync(&mut self, layout: &RepLayout, obj: &impl Replicated) {
        for f in layout.fields() {
            self.set(f.id, obj.field_value(f.id));
        }
    }
}

/// The pre-encode audit found fields that changed without being marked dirty.
///
/// This is a **hard failure**, not a warning: it runs before the tick's delta is
/// built so a "forgot to mark dirty" regression fails the test/CI rather than
/// silently shipping stale state (design §3.2, review findings 1-2).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unmarked replicated changes on field(s) {0:?}: a write bypassed mark_dirty and the shadow net"
)]
pub struct UnmarkedChanges(pub Vec<FieldId>);

/// Per-object change tracking (dirty mask + shadow safety net) for one
/// replicated object. See the module docs for scope.
#[derive(Debug, Clone)]
pub struct NetworkPeer {
    layout: &'static RepLayout,
    dirty: DirtyMask,
    shadow: ShadowBuffer,
}

impl NetworkPeer {
    /// Create tracking for `obj`, snapshotting its current state as the clean
    /// shadow baseline. No fields start dirty.
    #[must_use]
    pub fn new(obj: &impl Replicated) -> Self {
        let layout = obj.rep_layout();
        let shadow = ShadowBuffer::snapshot(layout, obj);
        Self {
            layout,
            dirty: DirtyMask::new(layout.len()),
            shadow,
        }
    }

    /// The object's immutable layout.
    #[must_use]
    pub fn layout(&self) -> &'static RepLayout {
        self.layout
    }

    /// Mark `field_id` dirty (the push-model fast path, design §3.1). Returns
    /// `true` if `field_id` is a valid registered field, `false` otherwise.
    /// Cost is O(1) — O(fields actually written) across a tick.
    pub fn mark_dirty(&mut self, field_id: FieldId) -> bool {
        self.dirty.set(field_id as usize)
    }

    /// Whether `field_id` is currently marked dirty.
    #[must_use]
    pub fn is_dirty(&self, field_id: FieldId) -> bool {
        self.dirty.get(field_id as usize)
    }

    /// Whether any field is dirty.
    #[must_use]
    pub fn any_dirty(&self) -> bool {
        !self.dirty.none_set()
    }

    /// The number of dirty fields.
    #[must_use]
    pub fn dirty_count(&self) -> usize {
        self.dirty.count_set()
    }

    /// The ids of currently-dirty fields, ascending.
    pub fn dirty_field_ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        // field_id fits u16 because the layout is capped at u16::MAX fields.
        self.dirty.iter_set().map(|i| i as FieldId)
    }

    /// Shadow-diff safety net (design §3.2). For every **non-push** field,
    /// compare the object's current value against the shadow; on a difference set
    /// the dirty bit and advance the shadow for that field. This is the mandatory
    /// net for fields whose writes cannot be structurally auto-marked. Cost is
    /// O(non-push fields).
    ///
    /// Run this once per tick **before** [`NetworkPeer::audit_unmarked_changes`]
    /// and the encode.
    pub fn detect_shadow_changes(&mut self, obj: &impl Replicated) {
        for f in self.layout.fields() {
            if f.push_based {
                continue;
            }
            let current = obj.field_value(f.id);
            if self.shadow.get(f.id) != Some(&current) {
                self.dirty.set(f.id as usize);
                self.shadow.set(f.id, current);
            }
        }
    }

    /// Dev/CI pre-encode audit (design §3.2, review findings 1-2). Over **all**
    /// registered fields (including push-model ones), any field whose current
    /// value differs from the shadow but has **no** dirty bit is a bug: a write
    /// bypassed both `mark_dirty` and the shadow net. Returns [`UnmarkedChanges`]
    /// listing the offending fields.
    ///
    /// This is read-only (it does not mutate dirty/shadow) and MUST run **before**
    /// the tick's delta is encoded, so the regression fails the build/test rather
    /// than shipping stale state. Run [`NetworkPeer::detect_shadow_changes`]
    /// first so the mandatory net's diffs are already marked.
    pub fn audit_unmarked_changes(&self, obj: &impl Replicated) -> Result<(), UnmarkedChanges> {
        let mut offenders = Vec::new();
        for f in self.layout.fields() {
            let current = obj.field_value(f.id);
            let changed = self.shadow.get(f.id) != Some(&current);
            if changed && !self.dirty.get(f.id as usize) {
                offenders.push(f.id);
            }
        }
        if offenders.is_empty() {
            Ok(())
        } else {
            Err(UnmarkedChanges(offenders))
        }
    }

    /// Per-tick reset after the delta has been encoded (design §5: dirty/shadow
    /// advance once the change is folded into the pending set). Clears every dirty
    /// bit and re-snapshots the shadow to the object's current state so the next
    /// tick starts clean.
    ///
    /// **Audit-gated (review finding, ).** Advancing is where an unmarked
    /// change would be permanently absorbed into the shadow and lost, so in
    /// dev/CI builds this runs [`NetworkPeer::audit_unmarked_changes`] **first**
    /// and refuses to advance (returns `Err`, leaving dirty/shadow untouched) if
    /// any field changed without a dirty bit — so "advance before audit" cannot
    /// silently drop a change regardless of call order. Run
    /// [`NetworkPeer::detect_shadow_changes`] before this so the mandatory net's
    /// diffs are already marked. Shipping builds skip the audit (design §3.2) and
    /// always reset.
    ///
    /// Phase 2 gates this on per-connection ack bookkeeping; Phase 1 exposes it so
    /// tests can drive a full tick cycle.
    pub fn advance_after_encode(&mut self, obj: &impl Replicated) -> Result<(), UnmarkedChanges> {
        if cfg!(debug_assertions) {
            self.audit_unmarked_changes(obj)?;
        }
        self.dirty.clear();
        self.shadow.resync(self.layout, obj);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::realtime::netpeer::layout::{
        FieldAuthority, FieldBounds, RepCondition, RepLayoutBuilder, TypeTag,
    };
    use citadel_wire::codec::codec_id;
    use std::sync::OnceLock;

    // A representative replicated actor: `health` and `name` are push-model
    // (auto-marked in Unreal via TCitadelReplicated); `team` is a non-push field
    // that relies on the mandatory shadow net.
    const F_HEALTH: FieldId = 0;
    const F_NAME: FieldId = 1;
    const F_TEAM: FieldId = 2;

    struct PlayerState {
        health: i64,
        name: String,
        team: u64,
    }

    fn layout() -> &'static RepLayout {
        static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
        LAYOUT.get_or_init(|| {
            RepLayoutBuilder::new(0xA11CE, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true, // push
                )
                .field(
                    "name",
                    TypeTag::Bytes,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::MaxLen { max_len: 16 },
                    true, // push
                )
                .field(
                    "team",
                    TypeTag::Uint,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 8 },
                    false, // shadow-net
                )
                .build()
                .unwrap()
        })
    }

    impl Replicated for PlayerState {
        fn rep_layout(&self) -> &'static RepLayout {
            layout()
        }
        fn field_value(&self, field_id: FieldId) -> FieldValue {
            match field_id {
                F_HEALTH => FieldValue::Int(self.health),
                F_NAME => FieldValue::Bytes(self.name.clone().into_bytes()),
                F_TEAM => FieldValue::Uint(self.team),
                _ => FieldValue::Bytes(Vec::new()),
            }
        }
    }

    fn player() -> PlayerState {
        PlayerState {
            health: 100,
            name: "hero".to_string(),
            team: 1,
        }
    }

    #[test]
    fn fresh_peer_is_clean() {
        let p = player();
        let peer = NetworkPeer::new(&p);
        assert!(!peer.any_dirty());
        assert_eq!(peer.dirty_count(), 0);
    }

    #[test]
    fn mark_dirty_sets_the_bit() {
        let p = player();
        let mut peer = NetworkPeer::new(&p);
        assert!(peer.mark_dirty(F_HEALTH));
        assert!(peer.is_dirty(F_HEALTH));
        assert!(!peer.is_dirty(F_NAME));
        assert_eq!(peer.dirty_count(), 1);
        let ids: Vec<_> = peer.dirty_field_ids().collect();
        assert_eq!(ids, vec![F_HEALTH]);
    }

    #[test]
    fn mark_dirty_out_of_range_is_rejected() {
        let p = player();
        let mut peer = NetworkPeer::new(&p);
        assert!(!peer.mark_dirty(99));
        assert!(!peer.any_dirty());
    }

    #[test]
    fn shadow_net_detects_an_unmarked_nonpush_change() {
        let mut p = player();
        let mut peer = NetworkPeer::new(&p);
        // A non-push field changes with no mark_dirty call.
        p.team = 4;
        assert!(!peer.is_dirty(F_TEAM));
        peer.detect_shadow_changes(&p);
        // The mandatory net caught it.
        assert!(peer.is_dirty(F_TEAM));
        // And the audit now passes because the net marked it.
        assert!(peer.audit_unmarked_changes(&p).is_ok());
    }

    #[test]
    fn audit_fails_on_a_push_field_changed_without_mark() {
        let mut p = player();
        let mut peer = NetworkPeer::new(&p);
        // A push field mutated but mark_dirty was forgotten.
        p.health = 50;
        peer.detect_shadow_changes(&p); // does not cover push fields
        let err = peer
            .audit_unmarked_changes(&p)
            .expect_err("must fail closed on an unmarked push change");
        assert_eq!(err, UnmarkedChanges(vec![F_HEALTH]));
    }

    #[test]
    fn audit_passes_when_push_field_is_marked() {
        let mut p = player();
        let mut peer = NetworkPeer::new(&p);
        p.health = 50;
        peer.mark_dirty(F_HEALTH); // correctly marked
        peer.detect_shadow_changes(&p);
        assert!(peer.audit_unmarked_changes(&p).is_ok());
    }

    #[test]
    fn advance_after_encode_resets_dirty_and_shadow() {
        let mut p = player();
        let mut peer = NetworkPeer::new(&p);
        p.health = 50;
        peer.mark_dirty(F_HEALTH);
        peer.advance_after_encode(&p).expect("clean advance");
        // Clean again; the new value is the shadow baseline.
        assert!(!peer.any_dirty());
        assert!(peer.audit_unmarked_changes(&p).is_ok());
        // A subsequent unmarked change is still caught next tick.
        p.health = 25;
        peer.detect_shadow_changes(&p);
        assert_eq!(
            peer.audit_unmarked_changes(&p),
            Err(UnmarkedChanges(vec![F_HEALTH]))
        );
    }

    #[test]
    fn advance_refuses_to_absorb_an_unmarked_change() {
        // BLOCKER-2 guard: calling advance without auditing first must not
        // silently swallow an unmarked push change into the shadow. In a debug
        // build advance audits and refuses, leaving the change still detectable.
        let mut p = player();
        let mut peer = NetworkPeer::new(&p);
        p.health = 50; // push field changed, never marked
        let err = peer
            .advance_after_encode(&p)
            .expect_err("advance must fail closed on an unmarked change");
        assert_eq!(err, UnmarkedChanges(vec![F_HEALTH]));
        // State was not advanced: the change is still visible to a later audit.
        assert_eq!(
            peer.audit_unmarked_changes(&p),
            Err(UnmarkedChanges(vec![F_HEALTH]))
        );
    }

    #[test]
    fn nan_float_shadow_is_stable_across_ticks() {
        // A float field whose bits do not change must not be reported as dirty,
        // even for NaN (bit-equality is total).
        #[derive(Clone)]
        struct FloatActor {
            v: f32,
        }
        fn flayout() -> &'static RepLayout {
            static L: OnceLock<RepLayout> = OnceLock::new();
            L.get_or_init(|| {
                RepLayoutBuilder::new(0xF10A7, 1)
                    .field(
                        "v",
                        TypeTag::Scalar,
                        codec_id::SCALAR_QUANT,
                        RepCondition::None,
                        FieldAuthority::ServerOnly,
                        FieldBounds::ScalarRange {
                            min: 0.0,
                            max: 1.0,
                            values_per_unit: 1024,
                        },
                        false,
                    )
                    .build()
                    .unwrap()
            })
        }
        impl Replicated for FloatActor {
            fn rep_layout(&self) -> &'static RepLayout {
                flayout()
            }
            fn field_value(&self, _id: FieldId) -> FieldValue {
                FieldValue::f32(self.v)
            }
        }
        let a = FloatActor { v: f32::NAN };
        let mut peer = NetworkPeer::new(&a);
        peer.detect_shadow_changes(&a);
        assert!(!peer.any_dirty());
        assert!(peer.audit_unmarked_changes(&a).is_ok());
    }
}
