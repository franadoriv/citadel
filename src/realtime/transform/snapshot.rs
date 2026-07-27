//! `ClientSnapshotState`: builds one client's delta snapshot and tracks its acks.
//!
//! # The per-client ring model (adversarial review, )
//!
//! A naive "diff against the last-acked snapshot id and fill omitted fields from
//! the most-recent applied packet" is **unsafe** over unordered datagrams: a
//! packet ack and an object baseline are different things, and omitted objects /
//! divergent per-object baselines eventually produce a delta the client decodes
//! against the wrong state.
//!
//! Instead this follows the Quake3/Source model. The server keeps, per client, a
//! **ring of reconstructed full states keyed by `snapshot_id`** — the exact state
//! the client will hold *after applying* that snapshot. Every outgoing snapshot
//! is diffed against `ring[confirmed_base]`, where `confirmed_base` is the newest
//! snapshot id the client has acked **and** the server still holds. Because the
//! client acked that id, it provably holds `full[confirmed_base]`, so a delta can
//! never reference a base the client lacks. The server reconstructs `ring[new_id]
//! = ring[base] − removed + written`, which is byte-for-byte what the client
//! reconstructs, so loss/reorder self-heal with no explicit retransmit.
//!
//! Area-of-interest enter/exit is expressed by **set membership**: an object that
//! left is listed in `removed` (relative to the base), and a re-entering object
//! is simply absent from the base and sent full again. `gen_epoch` is reserved
//! for respawn/object-id reuse only.

use std::collections::{BTreeMap, HashMap};

use citadel_wire::baseline::AckField;
use citadel_wire::tsync::{self, ObjectUpdate, Snapshot, TransformFields};

use super::ObjectId;
use super::authority::TransformState;
use super::world::Frame;

/// How many reconstructed full states to retain per client. Must be at least the
/// 32-bit ack window so a freshly-acked id is always still held as a base.
const MAX_RING: usize = 64;

/// The reconstructed full state of one object as the client will hold it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SentObject {
    gen_epoch: u16,
    state: TransformState,
}

type SentState = HashMap<ObjectId, SentObject>;

/// Per-client snapshot construction + ack bookkeeping (design §7.4).
#[derive(Debug)]
pub struct ClientSnapshotState {
    /// Which objects are currently relevant to this client (AOI hysteresis).
    relevance: citadel_wire::interest::RelevanceSet,
    /// Per-object priority accumulator (Gaffer): bumped each tick a relevant
    /// object is unsent, reset when sent, so nothing starves.
    priority_acc: HashMap<ObjectId, f32>,
    /// The client's acked snapshot-id window (observability + recovery).
    ack: AckField,
    /// Next snapshot id to mint (monotonic; `0` reserved for "no base").
    next_snapshot_id: u32,
    /// Newest acked id the server still holds as a diff base (`0` = none).
    confirmed_base: u32,
    /// Reconstructed full states keyed by snapshot id.
    ring: BTreeMap<u32, SentState>,
    /// AOI inner (enter) radius.
    inner: f32,
    /// AOI outer (exit) radius.
    outer: f32,
}

impl ClientSnapshotState {
    /// A fresh builder with the given AOI hysteresis band.
    #[must_use]
    pub fn new(inner: f32, outer: f32) -> Self {
        Self {
            relevance: citadel_wire::interest::RelevanceSet::new(),
            priority_acc: HashMap::new(),
            ack: AckField::new(),
            next_snapshot_id: 1,
            confirmed_base: 0,
            ring: BTreeMap::new(),
            inner,
            outer,
        }
    }

    /// The id the server is currently diffing against (`0` = full baselines).
    #[must_use]
    pub fn confirmed_base(&self) -> u32 {
        self.confirmed_base
    }

    /// The client's newest acked id, if any.
    #[must_use]
    pub fn latest_acked(&self) -> Option<u32> {
        self.ack.latest().map(|v| v as u32)
    }

    /// Build this client's next snapshot from the latched `frame`, as seen from
    /// `viewer_pos`, sending at most `budget` object updates (priority-ordered,
    /// new/full objects first so a newly relevant object is never starved).
    ///
    /// Returns `None` only if there is genuinely nothing to send (no relevant
    /// objects and no removals) — the caller may still choose to emit a keep-alive
    /// but P1 simply skips.
    pub fn build(
        &mut self,
        frame: &Frame,
        viewer_participant: u64,
        viewer_pos: [f32; 3],
        budget: usize,
        send_rate_hz: u8,
    ) -> Option<Snapshot> {
        // 1. Recompute relevancy (hysteresis). Despawned objects auto-exit.
        self.relevance
            .update(frame.grid(), viewer_pos, self.inner, self.outer);

        let base = self.confirmed_base;
        let empty = SentState::new();
        let base_state = self.ring.get(&base).unwrap_or(&empty);

        // 2. The relevant set that still exists in the frame.
        let mut relevant: Vec<&super::world::FrameObject> = self
            .relevance
            .subscribed()
            .iter()
            .filter_map(|&id| frame.object(id as ObjectId))
            .collect();
        relevant.sort_by_key(|o| o.object_id);

        let relevant_ids: std::collections::HashSet<ObjectId> =
            relevant.iter().map(|o| o.object_id).collect();

        // 3. Removals: objects in the base but no longer relevant.
        let mut removed: Vec<ObjectId> = base_state
            .keys()
            .copied()
            .filter(|id| !relevant_ids.contains(id))
            .collect();
        removed.sort_unstable();

        // 4. Classify each relevant object as full/delta/unchanged; accumulate
        //    priority for everything relevant.
        struct Candidate {
            object: super::world::FrameObject,
            fields: TransformFields,
            is_full: bool,
            last_input_seq: Option<u32>,
        }
        let mut candidates: Vec<Candidate> = Vec::new();
        for obj in &relevant {
            *self.priority_acc.entry(obj.object_id).or_insert(0.0) += obj.priority.max(0.0);
            // For an object this client owns (OwnerPredicted + owner == viewer),
            // echo the highest contiguous applied input seq every snapshot so the
            // owner can reconcile even when the transform did not change this tick
            // (design §5.1). Non-owners never receive it.
            let last_input_seq = obj
                .owner_matches(viewer_participant)
                .then_some(obj.last_input_seq);
            let base_obj = base_state.get(&obj.object_id);
            match base_obj {
                Some(b) if b.gen_epoch == obj.gen_epoch => {
                    // Delta: only changed fields.
                    let fields = delta_fields(&b.state, &obj.state, obj.replicate_velocity);
                    // Emit when something changed OR we owe the owner an input ack.
                    if !fields.is_empty() || last_input_seq.is_some() {
                        candidates.push(Candidate {
                            object: **obj,
                            fields,
                            is_full: false,
                            last_input_seq,
                        });
                    }
                    // Empty delta => unchanged => omitted; still stays in the ring.
                }
                _ => {
                    // Full: new object, or generation changed (respawn/reuse).
                    candidates.push(Candidate {
                        object: **obj,
                        fields: full_fields(&obj.state, obj.replicate_velocity),
                        is_full: true,
                        last_input_seq,
                    });
                }
            }
        }

        if candidates.is_empty() && removed.is_empty() {
            return None;
        }

        // 5. Priority order: full/lifecycle objects first (reserved budget so a
        //    newly relevant object cannot be starved), then by accumulated
        //    priority. Take up to `budget` updates.
        candidates.sort_by(|a, b| {
            b.is_full
                .cmp(&a.is_full)
                .then_with(|| {
                    let pa = self
                        .priority_acc
                        .get(&a.object.object_id)
                        .copied()
                        .unwrap_or(0.0);
                    let pb = self
                        .priority_acc
                        .get(&b.object.object_id)
                        .copied()
                        .unwrap_or(0.0);
                    pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| a.object.object_id.cmp(&b.object.object_id))
        });
        if budget > 0 && candidates.len() > budget {
            candidates.truncate(budget);
        }

        // 6. Mint the snapshot id and reconstruct the new ring entry exactly as
        //    the client will: base − removed + written updates.
        let snapshot_id = self.next_snapshot_id;
        self.next_snapshot_id = self.next_snapshot_id.wrapping_add(1).max(1);

        let mut new_state = base_state.clone();
        for id in &removed {
            new_state.remove(id);
        }
        let mut updates = Vec::with_capacity(candidates.len());
        for c in &candidates {
            new_state.insert(
                c.object.object_id,
                SentObject {
                    gen_epoch: c.object.gen_epoch,
                    state: c.object.state,
                },
            );
            self.priority_acc.insert(c.object.object_id, 0.0);
            updates.push(ObjectUpdate {
                object_id: c.object.object_id,
                gen_epoch: c.object.gen_epoch,
                fields: c.fields,
                last_input_seq: c.last_input_seq,
            });
        }
        updates.sort_by_key(|u| u.object_id);

        self.ring.insert(snapshot_id, new_state);
        self.prune_ring();

        Some(Snapshot {
            server_tick: frame.tick,
            snapshot_id,
            base_snapshot_id: base,
            send_rate_hz,
            removed,
            updates,
        })
    }

    /// Apply a client ack. Advances `confirmed_base` to the newest acked id the
    /// server still holds as a ring entry (monotonic; a stale/unknown/forged id
    /// can never regress it), then prunes settled ring entries.
    pub fn apply_ack(&mut self, ack: &tsync::Ack) {
        let Ok(incoming) = AckField::from_wire(u64::from(ack.acked_snapshot_id), ack.history)
        else {
            return;
        };
        let mut best = self.confirmed_base;
        for id in incoming.iter_acked() {
            let id = id as u32;
            if id > best && self.ring.contains_key(&id) {
                best = id;
            }
            self.ack.ack(u64::from(id));
        }
        if best > self.confirmed_base {
            self.confirmed_base = best;
            self.prune_ring();
        }
    }

    /// Whether an object is currently relevant to this client.
    #[must_use]
    pub fn is_relevant(&self, id: ObjectId) -> bool {
        self.relevance.contains(u64::from(id))
    }

    /// Retain the confirmed base and recent ids, bounded to [`MAX_RING`].
    fn prune_ring(&mut self) {
        // Drop everything strictly older than the confirmed base (settled).
        if self.confirmed_base > 0 {
            self.ring = self.ring.split_off(&self.confirmed_base);
        }
        // Cap total size, never dropping the confirmed base entry.
        while self.ring.len() > MAX_RING {
            let Some((&oldest, _)) = self.ring.iter().next() else {
                break;
            };
            if oldest == self.confirmed_base && self.confirmed_base > 0 {
                // The base is the oldest; drop the next one instead.
                let next = self.ring.range((oldest + 1)..).next().map(|(&k, _)| k);
                match next {
                    Some(k) => {
                        self.ring.remove(&k);
                    }
                    None => break,
                }
            } else {
                self.ring.remove(&oldest);
            }
        }
    }
}

/// The fields that differ between `base` and `current` (delta compression). A
/// field is included only when it changed; velocity only when replicated.
fn delta_fields(base: &TransformState, current: &TransformState, vel: bool) -> TransformFields {
    TransformFields {
        position: (base.position != current.position).then_some(current.position),
        rotation: (base.rotation != current.rotation).then_some(current.rotation),
        velocity: (vel && base.velocity != current.velocity).then_some(current.velocity),
    }
}

/// A full baseline: position + rotation always, velocity only when replicated.
fn full_fields(current: &TransformState, vel: bool) -> TransformFields {
    TransformFields {
        position: Some(current.position),
        rotation: Some(current.rotation),
        velocity: vel.then_some(current.velocity),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::realtime::transform::SyncRole;
    use crate::realtime::transform::authority::TransformAuthority;
    use crate::realtime::transform::world::TransformWorld;

    fn world_with(id: ObjectId, pos: [f32; 3]) -> TransformWorld {
        let mut w = TransformWorld::new(10_000.0);
        let mut a = TransformAuthority::new(id, SyncRole::ServerSimulated, TransformState::at(pos));
        a.replicate_velocity = true;
        w.spawn(a);
        w
    }

    #[test]
    fn first_snapshot_is_full_baseline() {
        let w = world_with(1, [100.0, 0.0, 0.0]);
        let frame = w.latch();
        let mut cs = ClientSnapshotState::new(1e6, 1e6);
        let snap = cs
            .build(&frame, 0, [0.0, 0.0, 0.0], 0, 20)
            .expect("snapshot");
        assert_eq!(snap.base_snapshot_id, 0, "no base yet => full");
        assert_eq!(snap.updates.len(), 1);
        let u = snap.updates[0];
        assert!(u.fields.position.is_some());
        assert!(u.fields.rotation.is_some());
        assert!(u.fields.velocity.is_some());
    }

    #[test]
    fn delta_only_after_ack_and_omits_unchanged() {
        let mut w = world_with(1, [100.0, 0.0, 0.0]);
        let mut cs = ClientSnapshotState::new(1e6, 1e6);

        let s1 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        assert_eq!(s1.base_snapshot_id, 0);
        // Ack it => confirmed base advances.
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: s1.snapshot_id,
            history: 0,
        });
        assert_eq!(cs.confirmed_base(), s1.snapshot_id);

        // Move only position; rotation/velocity unchanged.
        w.set_transform(1, {
            let mut s = w.get_transform(1).unwrap();
            s.position[0] = 150.0;
            s
        });
        let s2 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        assert_eq!(s2.base_snapshot_id, s1.snapshot_id, "delta vs acked base");
        assert_eq!(s2.updates.len(), 1);
        let u = s2.updates[0];
        assert!(u.fields.position.is_some(), "position changed");
        assert!(u.fields.rotation.is_none(), "rotation omitted");
        // Velocity here is the default zero on both => unchanged => omitted.
        assert!(u.fields.velocity.is_none());
    }

    #[test]
    fn unacked_client_keeps_getting_full_baselines() {
        let w = world_with(1, [100.0, 0.0, 0.0]);
        let mut cs = ClientSnapshotState::new(1e6, 1e6);
        let s1 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        // No ack: next snapshot still bases off 0 (full), self-healing.
        let s2 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        assert_eq!(s1.base_snapshot_id, 0);
        assert_eq!(s2.base_snapshot_id, 0);
        assert!(s2.snapshot_id > s1.snapshot_id);
    }

    #[test]
    fn exit_produces_a_removal_relative_to_base() {
        // Two objects relevant, ack the baseline, then move one far away so it
        // exits; the next snapshot must carry it in `removed`.
        let mut w = TransformWorld::new(100.0);
        w.spawn_server_simulated(1, TransformState::at([0.0, 0.0, 0.0]));
        w.spawn_server_simulated(2, TransformState::at([10.0, 0.0, 0.0]));
        let mut cs = ClientSnapshotState::new(50.0, 90.0);
        let s1 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        assert_eq!(s1.updates.len(), 2);
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: s1.snapshot_id,
            history: 0,
        });
        // Move object 2 far outside the outer radius.
        w.set_transform(2, TransformState::at([100_000.0, 0.0, 0.0]));
        w.advance(0.0);
        let s2 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        assert!(
            s2.removed.contains(&2),
            "exited object removed: {:?}",
            s2.removed
        );
    }

    #[test]
    fn stale_ack_never_regresses_confirmed_base() {
        let mut w = world_with(1, [0.0, 0.0, 0.0]);
        let mut cs = ClientSnapshotState::new(1e6, 1e6);
        let s1 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: s1.snapshot_id,
            history: 0,
        });
        w.set_transform(1, TransformState::at([1.0, 0.0, 0.0]));
        let s2 = cs.build(&w.latch(), 0, [0.0; 3], 0, 20).unwrap();
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: s2.snapshot_id,
            history: 0,
        });
        assert_eq!(cs.confirmed_base(), s2.snapshot_id);
        // A stale ack for s1 cannot pull the base backward.
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: s1.snapshot_id,
            history: 0,
        });
        assert_eq!(cs.confirmed_base(), s2.snapshot_id);
        // A forged ack for a never-sent id is ignored.
        cs.apply_ack(&tsync::Ack {
            acked_snapshot_id: 99_999,
            history: 0,
        });
        assert_eq!(cs.confirmed_base(), s2.snapshot_id);
    }
}
