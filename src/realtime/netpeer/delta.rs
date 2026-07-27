//! Per-connection baseline orchestration for `NetworkPeer` deltas (,
//! design §5, §5.0). This is the Rust-side state machine that turns an object's
//! current replicated state into a per-receiver [`DeltaBunch`] diffed against that
//! receiver's **last-acked** baseline, mints the server-issued monotonic tokens,
//! retains the pending change-set until ack, and advances only on a genuine ack.
//!
//! The wire encode/decode + hostile-input hardening live in
//! [`citadel_wire::netpeer`]; this module owns the *orchestration* that the design
//! hardened against review findings 3-9 and the collection findings 13-17:
//!
//! - **Per-receiver encoding (finding 3, §5.0).** Baselines are per connection;
//!   each receiver's delta is built against *its own* last-acked snapshot, never a
//!   single global mask cleared after the first receiver.
//! - **Cumulative against the last *acked* baseline (finding 6).** Consecutive
//!   deltas before an ack all diff against the last acked snapshot, so a dropped
//!   intermediate delta never strands a change.
//! - **Server-issued monotonic nonzero tokens; explicit full (finding 3/8).**
//!   Every emitted bunch carries a nonzero `result_id`; a full snapshot carries
//!   `base_id == 0` and the schema hash. Acks name `result_id`.
//! - **Retain-until-ack + ack-timeout (findings 4-5).** The pending snapshot per
//!   `result_id` is kept until a matching ack; a timeout forces a resend or a full
//!   snapshot. Pending growth is capped so a never-acking peer cannot exhaust
//!   memory (finding 6/E).
//! - **Stale/forged acks never regress (findings 7-9).** All baseline mutation for
//!   an `(object, receiver)` funnels through one [`BaselineTracker`], which only
//!   advances to an outstanding, strictly-newer token.
//! - **Collections (findings 13-17).** The per-receiver baseline stores the item
//!   `id → (key, value)` map; the diff yields removed/added/changed with `gen`-
//!   tagged ids so a reused slot is a distinct id. The map is capped; on overflow
//!   the collection falls back to a full snapshot.
//!
//! The untrusted-inbound validate/apply/rebroadcast pipeline is ; this
//! module is the sender-side baseline mechanics only.

use std::collections::BTreeMap;

use citadel_wire::baseline::{AckField, BaselineAllocator, BaselineId, BaselineTracker};
use citadel_wire::netpeer::{
    CollItem, CollectionDelta, DeltaBunch, FieldDelta, RepId, RepSchema, RepValue,
};

/// A receiver (connection) identity for per-connection baseline state.
pub type ReceiverId = u64;

/// Maximum unacked pending snapshots retained per `(object, receiver)` before the
/// sender gives up on deltas and forces a full snapshot (finding 6). Bounds
/// server memory against a peer that never acks.
pub const MAX_PENDING_PER_RECEIVER: usize = 64;

/// Maximum live collection items tracked per collection field per receiver before
/// the baseline falls back to a full snapshot and resets (finding 17).
pub const MAX_COLLECTION_BASELINE: usize = 4096;

/// The item map of one collection field: `rep_id → (rep_key, value)`.
pub type CollectionState = BTreeMap<RepId, (u64, RepValue)>;

/// An immutable snapshot of one object's replicated state at a tick. Scalars are
/// keyed by `field_id`; collections carry the keyed item map per field.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepSnapshot {
    /// Scalar field values by `field_id`.
    pub scalars: BTreeMap<u16, RepValue>,
    /// Collection field item maps by `field_id`.
    pub collections: BTreeMap<u16, CollectionState>,
}

impl RepSnapshot {
    /// An empty snapshot (the implicit baseline of a never-synced receiver).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a scalar field value.
    pub fn set_scalar(&mut self, field_id: u16, value: RepValue) {
        self.scalars.insert(field_id, value);
    }

    /// Insert or update a collection element.
    pub fn set_item(&mut self, field_id: u16, id: RepId, key: u64, value: RepValue) {
        self.collections
            .entry(field_id)
            .or_default()
            .insert(id, (key, value));
    }

    /// Remove a collection element.
    pub fn remove_item(&mut self, field_id: u16, id: RepId) {
        if let Some(map) = self.collections.get_mut(&field_id) {
            map.remove(&id);
        }
    }

    fn collection_len(&self) -> usize {
        self.collections
            .values()
            .map(BTreeMap::len)
            .max()
            .unwrap_or(0)
    }
}

/// Per-`(object, receiver)` baseline state. All baseline mutation funnels through
/// this one owner so ack/data races cannot interleave (finding 9).
#[derive(Debug)]
struct ReceiverState {
    tracker: BaselineTracker,
    /// The last-acked snapshot the receiver provably holds (`None` => must full).
    acked: Option<RepSnapshot>,
    /// The `result_id` of `acked` (the `base_id` the next delta diffs against).
    acked_id: u64,
    /// Retained per-`result_id` snapshots awaiting an ack.
    pending: BTreeMap<u64, RepSnapshot>,
}

impl ReceiverState {
    fn new() -> Self {
        Self {
            tracker: BaselineTracker::new(),
            acked: None,
            acked_id: 0,
            pending: BTreeMap::new(),
        }
    }
}

/// One replicated object's sender-side baseline orchestration across all its
/// receivers. Holds the object's current state and each receiver's baseline.
#[derive(Debug)]
pub struct ObjectReplicator {
    object_id: u32,
    schema: RepSchema,
    current: RepSnapshot,
    receivers: BTreeMap<ReceiverId, ReceiverState>,
}

/// The outcome of building a delta for a receiver.
#[derive(Debug, Clone, PartialEq)]
pub struct BuiltDelta {
    /// The bunch to send.
    pub bunch: DeltaBunch,
    /// The token it establishes (also the pending key).
    pub result_id: u64,
    /// Whether it is a full snapshot.
    pub is_full: bool,
}

impl ObjectReplicator {
    /// Create an orchestrator for `object_id` with the class `schema`.
    #[must_use]
    pub fn new(object_id: u32, schema: RepSchema) -> Self {
        Self {
            object_id,
            schema,
            current: RepSnapshot::new(),
            receivers: BTreeMap::new(),
        }
    }

    /// The class schema (for encoding produced bunches).
    #[must_use]
    pub fn schema(&self) -> &RepSchema {
        &self.schema
    }

    /// Mutable access to the object's current state (the tick's authoritative
    /// values). The sender diffs this against each receiver's acked baseline.
    pub fn current_mut(&mut self) -> &mut RepSnapshot {
        &mut self.current
    }

    /// Register a receiver (a newly-relevant connection). Its first delta is a
    /// full snapshot.
    pub fn add_receiver(&mut self, receiver: ReceiverId) {
        self.receivers
            .entry(receiver)
            .or_insert_with(ReceiverState::new);
    }

    /// Drop a receiver (relevancy exit / disconnect).
    pub fn remove_receiver(&mut self, receiver: ReceiverId) {
        self.receivers.remove(&receiver);
    }

    /// Force `receiver`'s next delta to be a full snapshot (ack-timeout escalation
    /// or relevancy re-entry): clears its acked baseline and pending set (finding
    /// 5/10).
    pub fn force_full(&mut self, receiver: ReceiverId) {
        if let Some(state) = self.receivers.get_mut(&receiver) {
            state.acked = None;
            state.acked_id = 0;
            state.pending.clear();
        }
    }

    /// Build the delta for `receiver` against its last-acked baseline, minting a
    /// nonzero token from `allocator`. Returns `None` when there is nothing to
    /// send (the receiver's baseline already matches current). A receiver with no
    /// acked baseline (new / forced / timed out / overflowed) gets a full
    /// snapshot.
    pub fn build_delta(
        &mut self,
        receiver: ReceiverId,
        allocator: &mut BaselineAllocator,
    ) -> Option<BuiltDelta> {
        let object_id = self.object_id;
        let current = self.current.clone();
        let state = self.receivers.get_mut(&receiver)?;

        // Decide full vs delta. Fall back to full when there is no acked baseline,
        // when the pending set is saturated (finding 6), or when a collection grew
        // past the baseline cap (finding 17).
        let must_full = state.acked.is_none()
            || state.pending.len() >= MAX_PENDING_PER_RECEIVER
            || current.collection_len() > MAX_COLLECTION_BASELINE;

        let (is_full, base_id, changes) = if must_full {
            // A full snapshot resets pending (findings 5/10): old in-flight deltas
            // are superseded.
            state.pending.clear();
            let empty = RepSnapshot::new();
            (true, 0u64, diff_snapshots(&empty, &current))
        } else {
            // Diff against the last *acked* snapshot (finding 6), not the last
            // unacked one, so a dropped intermediate delta still carries the change.
            let base = state.acked.as_ref().unwrap_or(&self.current);
            let changes = diff_snapshots(base, &current);
            if changes.is_empty() {
                return None;
            }
            (false, state.acked_id, changes)
        };

        let result = allocator.allocate().ok()?;
        let result_id = result.get();
        state.tracker.issue(result);
        state.pending.insert(result_id, current);

        let mut bunch = DeltaBunch::new(object_id, is_full, result_id, base_id);
        for (field_id, delta) in changes {
            bunch.set(field_id, delta);
        }
        Some(BuiltDelta {
            bunch,
            result_id,
            is_full,
        })
    }

    /// Apply an ack window from `receiver`. Advances the receiver's baseline only
    /// to an outstanding, strictly-newer token (stale/forged acks are ignored and
    /// never regress). On advance, the acked snapshot becomes the base for future
    /// deltas. Returns the newly-acked token if it advanced.
    pub fn on_ack(&mut self, receiver: ReceiverId, ack: &AckField) -> Option<BaselineId> {
        let state = self.receivers.get_mut(&receiver)?;
        let advanced = state.tracker.apply_ack(ack)?;
        let acked_id = advanced.get();
        // The snapshot the receiver now provably holds.
        if let Some(snapshot) = state.pending.get(&acked_id).cloned() {
            state.acked = Some(snapshot);
            state.acked_id = acked_id;
        }
        // Prune everything settled at or below the new baseline.
        state.pending = state.pending.split_off(&(acked_id + 1));
        Some(advanced)
    }

    /// The `result_id` of `receiver`'s last-acked baseline (`0` = none), for
    /// tests / diagnostics.
    #[must_use]
    pub fn acked_id(&self, receiver: ReceiverId) -> u64 {
        self.receivers.get(&receiver).map_or(0, |s| s.acked_id)
    }

    /// Number of unacked pending snapshots for `receiver`.
    #[must_use]
    pub fn pending_len(&self, receiver: ReceiverId) -> usize {
        self.receivers.get(&receiver).map_or(0, |s| s.pending.len())
    }
}

/// Diff `current` against `base`, producing the per-field delta. Scalars differ
/// when the value changed; collections yield removed/added/changed by `rep_id`
/// with `gen`-tagged distinctness (design §3.3).
fn diff_snapshots(base: &RepSnapshot, current: &RepSnapshot) -> BTreeMap<u16, FieldDelta> {
    let mut out = BTreeMap::new();

    for (&field_id, value) in &current.scalars {
        if base.scalars.get(&field_id) != Some(value) {
            out.insert(field_id, FieldDelta::Value(value.clone()));
        }
    }

    for (&field_id, cur_map) in &current.collections {
        let empty = CollectionState::new();
        let base_map = base.collections.get(&field_id).unwrap_or(&empty);
        let mut removed = Vec::new();
        let mut added = Vec::new();
        let mut changed = Vec::new();

        // Removed: in base, not in current (a reused slot has a different gen, so
        // it correctly shows here as removed-old + added-new).
        for id in base_map.keys() {
            if !cur_map.contains_key(id) {
                removed.push(*id);
            }
        }
        for (id, (key, value)) in cur_map {
            match base_map.get(id) {
                None => added.push(CollItem {
                    id: *id,
                    key: *key,
                    value: value.clone(),
                }),
                Some((base_key, _)) if base_key != key => changed.push(CollItem {
                    id: *id,
                    key: *key,
                    value: value.clone(),
                }),
                Some(_) => {} // unchanged: never re-sent (finding, FastArray edge)
            }
        }

        if !removed.is_empty() || !added.is_empty() || !changed.is_empty() {
            out.insert(
                field_id,
                FieldDelta::Collection(CollectionDelta {
                    removed,
                    added,
                    changed,
                }),
            );
        }
    }

    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use citadel_wire::netpeer::{MAX_ENVELOPE_ALLOC, RepFieldCodec};
    use citadel_wire::schema::{LayoutField, schema_hash};

    fn hash() -> citadel_wire::schema::SchemaHash {
        schema_hash(
            1,
            &[LayoutField {
                field_id: 0,
                type_tag: 2,
                codec_id: 2,
                cond: 0,
                authority: 1,
                bounds_shape: 0,
            }],
        )
        .unwrap()
    }

    fn schema() -> RepSchema {
        RepSchema::new(
            hash(),
            vec![
                RepFieldCodec::IntRange { min: 0, max: 1000 },
                RepFieldCodec::IntRange { min: 0, max: 100 },
                RepFieldCodec::Collection {
                    item: Box::new(RepFieldCodec::IntRange { min: 0, max: 9999 }),
                    max_items: 256,
                },
            ],
        )
        .unwrap()
    }

    fn repl() -> ObjectReplicator {
        ObjectReplicator::new(1, schema())
    }

    fn ack_of(id: u64) -> AckField {
        let mut a = AckField::new();
        a.ack(id);
        a
    }

    #[test]
    fn first_delta_is_full_snapshot() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(50));
        r.add_receiver(7);
        let built = r.build_delta(7, &mut alloc).unwrap();
        assert!(built.is_full);
        assert_eq!(built.bunch.base_id, 0);
        assert_ne!(built.result_id, 0);
        // Encodes/decodes against the schema.
        let blob = built.bunch.encode(r.schema()).unwrap();
        let mut budget = MAX_ENVELOPE_ALLOC;
        let back = DeltaBunch::decode(&blob, r.schema(), &mut budget).unwrap();
        assert!(back.is_full);
    }

    #[test]
    fn baseline_advances_on_ack() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(50));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        assert_eq!(r.acked_id(7), 0);
        r.on_ack(7, &ack_of(full.result_id));
        assert_eq!(r.acked_id(7), full.result_id);
        assert_eq!(r.pending_len(7), 0);
        // With nothing changed, a subsequent build produces no delta.
        assert!(r.build_delta(7, &mut alloc).is_none());
    }

    #[test]
    fn delta_after_ack_diffs_against_acked() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(50));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));
        r.current_mut().set_scalar(0, RepValue::Int(60));
        let delta = r.build_delta(7, &mut alloc).unwrap();
        assert!(!delta.is_full);
        assert_eq!(delta.bunch.base_id, full.result_id);
        assert_eq!(
            delta.bunch.changes.get(&0),
            Some(&FieldDelta::Value(RepValue::Int(60)))
        );
    }

    #[test]
    fn cumulative_against_last_acked_survives_dropped_delta() {
        // Ack the full, change field 0, build delta A (dropped), change field 1,
        // build delta B: B must still carry field 0's change because both diff
        // against the last acked snapshot.
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(10));
        r.current_mut().set_scalar(1, RepValue::Int(1));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));

        r.current_mut().set_scalar(0, RepValue::Int(20));
        let _a = r.build_delta(7, &mut alloc).unwrap(); // dropped, never acked
        r.current_mut().set_scalar(1, RepValue::Int(2));
        let b = r.build_delta(7, &mut alloc).unwrap();
        // B carries BOTH changes relative to the acked baseline.
        assert_eq!(
            b.bunch.changes.get(&0),
            Some(&FieldDelta::Value(RepValue::Int(20)))
        );
        assert_eq!(
            b.bunch.changes.get(&1),
            Some(&FieldDelta::Value(RepValue::Int(2)))
        );
    }

    #[test]
    fn stale_and_forged_acks_never_regress() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(1));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));
        r.current_mut().set_scalar(0, RepValue::Int(2));
        let d = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(d.result_id));
        assert_eq!(r.acked_id(7), d.result_id);
        // A stale ack for the old full cannot regress.
        r.on_ack(7, &ack_of(full.result_id));
        assert_eq!(r.acked_id(7), d.result_id);
        // A forged ack for a never-issued token is ignored.
        r.on_ack(7, &ack_of(d.result_id + 999));
        assert_eq!(r.acked_id(7), d.result_id);
    }

    #[test]
    fn per_receiver_baselines_diverge() {
        // Two receivers acked at different points must get different base_ids.
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(1));
        r.add_receiver(1);
        r.add_receiver(2);
        let f1 = r.build_delta(1, &mut alloc).unwrap();
        let f2 = r.build_delta(2, &mut alloc).unwrap();
        r.on_ack(1, &ack_of(f1.result_id));
        r.on_ack(2, &ack_of(f2.result_id));
        r.current_mut().set_scalar(0, RepValue::Int(2));
        let d1 = r.build_delta(1, &mut alloc).unwrap();
        // Receiver 2 never gets a second ack; its next delta still diffs against f2.
        let d2 = r.build_delta(2, &mut alloc).unwrap();
        assert_eq!(d1.bunch.base_id, f1.result_id);
        assert_eq!(d2.bunch.base_id, f2.result_id);
        assert_ne!(d1.bunch.base_id, d2.bunch.base_id);
    }

    #[test]
    fn ack_timeout_force_full_resets_baseline() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(1));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));
        // Force full (ack-timeout escalation): next build is a full snapshot again.
        r.force_full(7);
        r.current_mut().set_scalar(0, RepValue::Int(2));
        let again = r.build_delta(7, &mut alloc).unwrap();
        assert!(again.is_full);
        assert_eq!(again.bunch.base_id, 0);
    }

    #[test]
    fn pending_cap_forces_full_snapshot() {
        // A receiver that never acks accumulates pending until the cap forces a
        // full snapshot and clears pending (finding 6).
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_scalar(0, RepValue::Int(0));
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));
        // Build many unacked deltas.
        for i in 1..=MAX_PENDING_PER_RECEIVER as i64 {
            r.current_mut().set_scalar(0, RepValue::Int(i.min(1000)));
            let d = r.build_delta(7, &mut alloc).unwrap();
            if r.pending_len(7) >= MAX_PENDING_PER_RECEIVER {
                // Next build must be a full snapshot with pending reset.
                r.current_mut().set_scalar(0, RepValue::Int(999));
                let forced = r.build_delta(7, &mut alloc).unwrap();
                assert!(forced.is_full, "iteration {i}");
                assert_eq!(r.pending_len(7), 1);
                return;
            }
            assert!(!d.is_full);
        }
        panic!("pending cap never reached");
    }

    #[test]
    fn collection_keyed_delta_diff() {
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_item(
            2,
            RepId {
                index: 0,
                generation: 0,
            },
            1,
            RepValue::Int(10),
        );
        r.current_mut().set_item(
            2,
            RepId {
                index: 1,
                generation: 0,
            },
            1,
            RepValue::Int(20),
        );
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        assert!(full.is_full);
        r.on_ack(7, &ack_of(full.result_id));

        // Remove item 0, change item 1 (bump key), add item 2.
        r.current_mut().remove_item(
            2,
            RepId {
                index: 0,
                generation: 0,
            },
        );
        r.current_mut().set_item(
            2,
            RepId {
                index: 1,
                generation: 0,
            },
            2,
            RepValue::Int(25),
        );
        r.current_mut().set_item(
            2,
            RepId {
                index: 2,
                generation: 0,
            },
            1,
            RepValue::Int(30),
        );
        let d = r.build_delta(7, &mut alloc).unwrap();
        match d.bunch.changes.get(&2).unwrap() {
            FieldDelta::Collection(c) => {
                assert_eq!(
                    c.removed,
                    vec![RepId {
                        index: 0,
                        generation: 0
                    }]
                );
                assert_eq!(c.added.len(), 1);
                assert_eq!(
                    c.added[0].id,
                    RepId {
                        index: 2,
                        generation: 0
                    }
                );
                assert_eq!(c.changed.len(), 1);
                assert_eq!(
                    c.changed[0].id,
                    RepId {
                        index: 1,
                        generation: 0
                    }
                );
                assert_eq!(c.changed[0].value, RepValue::Int(25));
            }
            _ => panic!("expected collection delta"),
        }
    }

    #[test]
    fn rep_id_reuse_new_generation_is_remove_then_add() {
        // Same slot index reused with a bumped generation shows as removed(old gen)
        // + added(new gen), never a silent in-place change.
        let mut r = repl();
        let mut alloc = BaselineAllocator::new();
        r.current_mut().set_item(
            2,
            RepId {
                index: 5,
                generation: 0,
            },
            1,
            RepValue::Int(1),
        );
        r.add_receiver(7);
        let full = r.build_delta(7, &mut alloc).unwrap();
        r.on_ack(7, &ack_of(full.result_id));

        r.current_mut().remove_item(
            2,
            RepId {
                index: 5,
                generation: 0,
            },
        );
        r.current_mut().set_item(
            2,
            RepId {
                index: 5,
                generation: 1,
            },
            1,
            RepValue::Int(2),
        );
        let d = r.build_delta(7, &mut alloc).unwrap();
        match d.bunch.changes.get(&2).unwrap() {
            FieldDelta::Collection(c) => {
                assert_eq!(
                    c.removed,
                    vec![RepId {
                        index: 5,
                        generation: 0
                    }]
                );
                assert_eq!(c.added.len(), 1);
                assert_eq!(
                    c.added[0].id,
                    RepId {
                        index: 5,
                        generation: 1
                    }
                );
            }
            _ => panic!("expected collection delta"),
        }
    }
}
