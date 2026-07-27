//! `RemoteWorldView`: the reusable client-runtime core (design §2.3, §4).
//!
//! This is the client side of the snapshot protocol — the logic the Unreal
//! `UCitadelTransformSync` component and the future Unity/Godot surfaces wrap
//! behind the C ABI. It:
//!
//! 1. **decodes** a [`Snapshot`] and reconstructs the full world state against
//!    the base it holds (`full[id] = full[base] − removed + updates`), discarding
//!    any snapshot whose base it lacks and applying only strictly-newer ids
//!    (monotonic guard) — the mirror of the server's ring so loss/reorder heal;
//! 2. feeds a per-object **jitter buffer** of timestamped samples and renders
//!    remote objects **in the past**, interpolating with **Hermite** position
//!    (when velocity is replicated) + **slerp** rotation, and **bounded
//!    extrapolation** when the buffer drains (design §4).
//!
//! Client-side prediction/reconciliation is deliberately absent (that is
//! `OwnerPredicted`, P2/); every object here is rendered interpolated.

use std::collections::{BTreeMap, HashMap, VecDeque};

use citadel_wire::baseline::AckField;
use citadel_wire::tsync::{self, Snapshot, TransformCodec};

use super::ObjectId;
use super::authority::TransformState;

/// A reconstructed object as the client holds it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RemoteObject {
    /// Replication generation.
    pub gen_epoch: u16,
    /// Reconstructed transform.
    pub state: TransformState,
}

/// A timestamped interpolation sample.
#[derive(Debug, Clone, Copy)]
struct Sample {
    tick: u32,
    state: TransformState,
    /// Whether velocity was replicated for this object (enables Hermite).
    has_velocity: bool,
}

/// How many reconstructed full states to retain (>= the 32-bit ack window).
const MAX_RING: usize = 64;
/// How many samples to keep per object in the jitter buffer.
const MAX_SAMPLES: usize = 32;

/// The client-side reconstruction + interpolation runtime for one connection.
#[derive(Debug)]
pub struct RemoteWorldView {
    codec: TransformCodec,
    /// Reconstructed full states keyed by snapshot id.
    ring: BTreeMap<u32, HashMap<ObjectId, RemoteObject>>,
    /// Highest snapshot id applied (monotonic guard).
    last_applied_id: u32,
    /// Ack window echoed back to the server.
    ack: AckField,
    /// Per-object jitter buffer of timestamped samples (ascending tick).
    samples: HashMap<ObjectId, VecDeque<Sample>>,
    /// Server sim rate (ticks/sec) for tick<->seconds conversion.
    sim_rate_hz: f64,
    /// Current effective send rate (packets/sec) from the latest snapshot.
    send_rate_hz: f64,
    /// Interpolation buffer size as a multiple of the send interval. When
    /// [`adaptive`](Self::adaptive) is set this is the *current* value, decaying
    /// toward [`buffer_floor`](Self::buffer_floor) on a clean link and growing
    /// back toward [`buffer_ceil`](Self::buffer_ceil) on detected loss.
    buffer_multiplier: f64,
    /// Lower bound the adaptive buffer shrinks to on a loss-free link (lower
    /// latency). The `1.5×` floor keeps at least ~1.5 send-intervals of samples.
    buffer_floor: f64,
    /// Upper bound the adaptive buffer grows to under jitter/loss (safe margin).
    buffer_ceil: f64,
    /// When true, [`buffer_multiplier`](Self::buffer_multiplier) tracks link
    /// quality (shrink on clean delivery, grow on snapshot-id gaps); when false it
    /// stays pinned at the ceiling. On localhost/LAN this converges to the floor.
    adaptive: bool,
    /// Max seconds to extrapolate past the last sample on drain.
    max_extrapolation_secs: f64,
    /// Count of snapshots discarded for a missing base (observability/tests).
    discarded_missing_base: u64,
    /// Count of snapshots discarded as stale/reordered (observability/tests).
    discarded_stale: u64,
    /// Per-owned-object highest contiguous input seq the server has acked
    /// (design §5.1) — the reconciliation input for the prediction layer.
    owner_acks: HashMap<ObjectId, u32>,
}

impl RemoteWorldView {
    /// A new view using `codec` and the negotiated sim/send rates.
    #[must_use]
    pub fn new(codec: TransformCodec, sim_rate_hz: u8, send_rate_hz: u8) -> Self {
        Self {
            codec,
            ring: BTreeMap::new(),
            last_applied_id: 0,
            ack: AckField::new(),
            samples: HashMap::new(),
            sim_rate_hz: f64::from(sim_rate_hz.max(1)),
            send_rate_hz: f64::from(send_rate_hz.max(1)),
            buffer_multiplier: 2.5,
            buffer_floor: 1.5,
            buffer_ceil: 2.5,
            adaptive: true,
            max_extrapolation_secs: 0.25,
            discarded_missing_base: 0,
            discarded_stale: 0,
            owner_acks: HashMap::new(),
        }
    }

    /// Decode and apply a snapshot datagram body. Returns `true` if it was
    /// applied, `false` if discarded (stale, missing base, or malformed).
    pub fn apply_datagram(&mut self, body: &[u8]) -> bool {
        match Snapshot::decode(body, &self.codec) {
            Ok(snap) => self.apply(&snap),
            Err(_) => false,
        }
    }

    /// Apply an already-decoded snapshot. Same discard rules as
    /// [`apply_datagram`](RemoteWorldView::apply_datagram).
    pub fn apply(&mut self, snap: &Snapshot) -> bool {
        // Monotonic guard: never apply an older-or-equal id (reorder/dup).
        if self.last_applied_id != 0 && snap.snapshot_id <= self.last_applied_id {
            self.discarded_stale += 1;
            return false;
        }
        // Base guard: a delta whose base we do not hold is unrecoverable here;
        // drop it and wait for the next snapshot whose base we do hold.
        if snap.base_snapshot_id != 0 && !self.ring.contains_key(&snap.base_snapshot_id) {
            self.discarded_missing_base += 1;
            return false;
        }
        let mut state: HashMap<ObjectId, RemoteObject> = if snap.base_snapshot_id == 0 {
            HashMap::new()
        } else {
            self.ring
                .get(&snap.base_snapshot_id)
                .cloned()
                .unwrap_or_default()
        };
        for id in &snap.removed {
            state.remove(id);
        }
        for u in &snap.updates {
            let prev = state.get(&u.object_id).copied();
            let is_delta = prev.is_some_and(|p| p.gen_epoch == u.gen_epoch);
            let mut ts = if is_delta {
                prev.expect("delta implies prev").state
            } else {
                // Full: require position + rotation, else the object is malformed.
                if u.fields.position.is_none() || u.fields.rotation.is_none() {
                    continue;
                }
                TransformState::default()
            };
            if let Some(p) = u.fields.position {
                ts.position = p;
            }
            if let Some(r) = u.fields.rotation {
                ts.rotation = r;
            }
            if let Some(v) = u.fields.velocity {
                ts.velocity = v;
            }
            state.insert(
                u.object_id,
                RemoteObject {
                    gen_epoch: u.gen_epoch,
                    state: ts,
                },
            );
            // Record the owner input-ack (monotonic) so the prediction layer can
            // reconcile owned objects (design §5.1). Only the owner's snapshot
            // carries this field.
            if let Some(seq) = u.last_input_seq {
                let slot = self.owner_acks.entry(u.object_id).or_insert(0);
                *slot = (*slot).max(seq);
            }
        }

        self.send_rate_hz = f64::from(snap.send_rate_hz.max(1));
        // Adaptive interpolation buffer: on a clean link (consecutive applied
        // snapshot ids) the render delay decays toward the floor for lower latency;
        // on detected loss (a gap in applied ids) it grows back toward the ceiling
        // to restore jitter margin. On localhost/LAN with no loss this converges to
        // ~1.5× send-interval automatically; over a lossy link it holds ~2.5×. No
        // configuration, and it never crosses the floor/ceiling bounds.
        if self.adaptive && self.last_applied_id != 0 {
            let gap = snap.snapshot_id.saturating_sub(self.last_applied_id);
            if gap > 1 {
                let grow = 0.5 * f64::from(gap - 1);
                self.buffer_multiplier = (self.buffer_multiplier + grow).min(self.buffer_ceil);
            } else {
                self.buffer_multiplier = (self.buffer_multiplier - 0.02).max(self.buffer_floor);
            }
        }
        self.ring.insert(snap.snapshot_id, state.clone());
        self.prune_ring();
        self.last_applied_id = snap.snapshot_id;
        self.ack.ack(u64::from(snap.snapshot_id));

        // Feed the jitter buffer: every held object gets a sample at this tick,
        // preserving steady cadence for interpolation timing.
        let vel_objects: std::collections::HashSet<ObjectId> = snap
            .updates
            .iter()
            .filter(|u| u.fields.velocity.is_some())
            .map(|u| u.object_id)
            .collect();
        for (&id, obj) in &state {
            let has_velocity = vel_objects.contains(&id) || obj.state.velocity != [0.0; 3];
            self.push_sample(id, snap.server_tick, obj.state, has_velocity);
        }
        // Drop buffers for objects no longer present.
        self.samples.retain(|id, _| state.contains_key(id));
        true
    }

    /// The ack to send back (newest applied id + 32-bit history).
    #[must_use]
    pub fn ack(&self) -> tsync::Ack {
        let (latest, history) = self.ack.to_wire();
        tsync::Ack {
            acked_snapshot_id: latest as u32,
            history,
        }
    }

    /// The reconstructed object as last applied (no interpolation).
    #[must_use]
    pub fn object(&self, id: ObjectId) -> Option<RemoteObject> {
        self.ring
            .get(&self.last_applied_id)
            .and_then(|m| m.get(&id).copied())
    }

    /// Ids currently present in the reconstructed world.
    #[must_use]
    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.ring
            .get(&self.last_applied_id)
            .map(|m| {
                let mut v: Vec<ObjectId> = m.keys().copied().collect();
                v.sort_unstable();
                v
            })
            .unwrap_or_default()
    }

    /// The highest contiguous input seq the server has acked for an owned object
    /// (design §5.1), or `None` if the client owns no such object yet. The
    /// prediction layer reconciles against this.
    #[must_use]
    pub fn owner_ack(&self, id: ObjectId) -> Option<u32> {
        self.owner_acks.get(&id).copied()
    }

    /// The authoritative post-input state of an owned object (the reconciliation
    /// target): its last reconstructed transform. Distinct from
    /// [`sample`](RemoteWorldView::sample), which renders remote objects in the
    /// past — an owned object is reconciled to the *present* authoritative state.
    #[must_use]
    pub fn authoritative_state(&self, id: ObjectId) -> Option<TransformState> {
        self.object(id).map(|o| o.state)
    }

    /// How many snapshots were discarded for a missing base.
    #[must_use]
    pub fn discarded_missing_base(&self) -> u64 {
        self.discarded_missing_base
    }

    /// How many snapshots were discarded as stale/reordered.
    #[must_use]
    pub fn discarded_stale(&self) -> u64 {
        self.discarded_stale
    }

    /// The newest sample tick across all objects, if any.
    #[must_use]
    pub fn latest_sample_tick(&self) -> Option<u32> {
        self.samples
            .values()
            .filter_map(|b| b.back().map(|s| s.tick))
            .max()
    }

    /// The current adaptive interpolation multiplier (send-interval multiples).
    /// Starts at the ceiling, decays toward the floor on a clean link, grows back
    /// on loss. Exposed for tests/metrics.
    #[must_use]
    pub fn interp_multiplier(&self) -> f64 {
        self.buffer_multiplier
    }

    /// The interpolation delay (in ticks) the buffer renders behind the newest
    /// sample: `multiplier × send_interval`, expressed in sim ticks.
    #[must_use]
    pub fn buffer_delay_ticks(&self) -> f64 {
        let send_interval_secs = 1.0 / self.send_rate_hz;
        let delay_secs =
            (self.buffer_multiplier * send_interval_secs).clamp(1.5 * send_interval_secs, 0.4);
        delay_secs * self.sim_rate_hz
    }

    /// The render tick the buffer should sample at right now: the newest sample
    /// tick minus the interpolation delay. Returns `None` with no samples.
    #[must_use]
    pub fn render_tick(&self) -> Option<f64> {
        let latest = self.latest_sample_tick()? as f64;
        Some(latest - self.buffer_delay_ticks())
    }

    /// Interpolate an object at `render_tick` (in sim-tick units): Hermite
    /// position (when velocity is replicated) + slerp rotation, clamped at the
    /// buffer ends with bounded extrapolation past the last sample.
    #[must_use]
    pub fn sample(&self, id: ObjectId, render_tick: f64) -> Option<TransformState> {
        let buf = self.samples.get(&id)?;
        if buf.is_empty() {
            return None;
        }
        let first = *buf.front().expect("non-empty");
        let last = *buf.back().expect("non-empty");

        // Before the buffer: clamp to the oldest known sample.
        if render_tick <= first.tick as f64 {
            return Some(first.state);
        }
        // Past the newest sample: bounded extrapolation from velocity.
        if render_tick >= last.tick as f64 {
            return Some(self.extrapolate(&last, render_tick));
        }
        // Find the bracketing pair.
        let mut lo = first;
        let mut hi = last;
        for w in buf.iter() {
            if (w.tick as f64) <= render_tick {
                lo = *w;
            }
            if (w.tick as f64) >= render_tick {
                hi = *w;
                break;
            }
        }
        if hi.tick == lo.tick {
            return Some(lo.state);
        }
        let span = (hi.tick - lo.tick) as f64;
        let t = ((render_tick - lo.tick as f64) / span).clamp(0.0, 1.0);
        Some(self.interpolate(&lo, &hi, t))
    }

    fn push_sample(&mut self, id: ObjectId, tick: u32, state: TransformState, has_velocity: bool) {
        let buf = self.samples.entry(id).or_default();
        // Ignore a sample that is not strictly newer than the last (dedup/reorder
        // at the sample level; the ring already guards snapshot ordering).
        if let Some(back) = buf.back()
            && tick <= back.tick
        {
            return;
        }
        buf.push_back(Sample {
            tick,
            state,
            has_velocity,
        });
        while buf.len() > MAX_SAMPLES {
            buf.pop_front();
        }
    }

    fn prune_ring(&mut self) {
        while self.ring.len() > MAX_RING {
            let Some((&oldest, _)) = self.ring.iter().next() else {
                break;
            };
            self.ring.remove(&oldest);
        }
    }

    /// Interpolate between two samples at fraction `t ∈ [0, 1]`.
    fn interpolate(&self, lo: &Sample, hi: &Sample, t: f64) -> TransformState {
        let use_hermite = lo.has_velocity && hi.has_velocity;
        let h_secs = (hi.tick - lo.tick) as f64 / self.sim_rate_hz;
        let position = if use_hermite {
            hermite_vec3(
                lo.state.position,
                lo.state.velocity,
                hi.state.position,
                hi.state.velocity,
                t,
                h_secs,
            )
        } else {
            lerp_vec3(lo.state.position, hi.state.position, t)
        };
        TransformState {
            position,
            rotation: slerp(lo.state.rotation, hi.state.rotation, t),
            velocity: lerp_vec3(lo.state.velocity, hi.state.velocity, t),
        }
    }

    /// Bounded extrapolation from the last sample's velocity.
    fn extrapolate(&self, last: &Sample, render_tick: f64) -> TransformState {
        let ahead_ticks = (render_tick - last.tick as f64).max(0.0);
        let ahead_secs = (ahead_ticks / self.sim_rate_hz).min(self.max_extrapolation_secs);
        let mut state = last.state;
        if last.has_velocity {
            for axis in 0..3 {
                state.position[axis] += last.state.velocity[axis] * ahead_secs as f32;
            }
        }
        state
    }
}

fn lerp_vec3(a: [f32; 3], b: [f32; 3], t: f64) -> [f32; 3] {
    let t = t as f32;
    [
        a[0] + (b[0] - a[0]) * t,
        a[1] + (b[1] - a[1]) * t,
        a[2] + (b[2] - a[2]) * t,
    ]
}

/// Cubic Hermite interpolation of a position with velocity tangents (cm/s), over
/// an interval of `h` seconds (design §4: smoother than linear lerp).
fn hermite_vec3(
    p0: [f32; 3],
    v0: [f32; 3],
    p1: [f32; 3],
    v1: [f32; 3],
    t: f64,
    h: f64,
) -> [f32; 3] {
    let t2 = t * t;
    let t3 = t2 * t;
    let h00 = 2.0 * t3 - 3.0 * t2 + 1.0;
    let h10 = t3 - 2.0 * t2 + t;
    let h01 = -2.0 * t3 + 3.0 * t2;
    let h11 = t3 - t2;
    let mut out = [0.0f32; 3];
    for axis in 0..3 {
        let m0 = f64::from(v0[axis]) * h; // tangent = velocity * interval
        let m1 = f64::from(v1[axis]) * h;
        let val = h00 * f64::from(p0[axis]) + h10 * m0 + h01 * f64::from(p1[axis]) + h11 * m1;
        out[axis] = val as f32;
    }
    out
}

/// Spherical linear interpolation of two quaternions `(x, y, z, w)`, taking the
/// shorter arc and falling back to normalized-lerp for nearly-parallel inputs.
fn slerp(a: [f32; 4], b: [f32; 4], t: f64) -> [f32; 4] {
    let mut b = b;
    let mut dot = f64::from(a[0]) * f64::from(b[0])
        + f64::from(a[1]) * f64::from(b[1])
        + f64::from(a[2]) * f64::from(b[2])
        + f64::from(a[3]) * f64::from(b[3]);
    // Shorter arc.
    if dot < 0.0 {
        for c in &mut b {
            *c = -*c;
        }
        dot = -dot;
    }
    if dot > 0.9995 {
        // Nearly parallel: normalized lerp avoids division by ~0.
        return normalize4([
            lerp1(a[0], b[0], t),
            lerp1(a[1], b[1], t),
            lerp1(a[2], b[2], t),
            lerp1(a[3], b[3], t),
        ]);
    }
    let theta_0 = dot.clamp(-1.0, 1.0).acos();
    let theta = theta_0 * t;
    let sin_theta = theta.sin();
    let sin_theta_0 = theta_0.sin();
    let s0 = (theta_0 - theta).sin() / sin_theta_0;
    let s1 = sin_theta / sin_theta_0;
    normalize4([
        (s0 * f64::from(a[0]) + s1 * f64::from(b[0])) as f32,
        (s0 * f64::from(a[1]) + s1 * f64::from(b[1])) as f32,
        (s0 * f64::from(a[2]) + s1 * f64::from(b[2])) as f32,
        (s0 * f64::from(a[3]) + s1 * f64::from(b[3])) as f32,
    ])
}

fn lerp1(a: f32, b: f32, t: f64) -> f32 {
    a + (b - a) * t as f32
}

fn normalize4(q: [f32; 4]) -> [f32; 4] {
    let n = (f64::from(q[0]) * f64::from(q[0])
        + f64::from(q[1]) * f64::from(q[1])
        + f64::from(q[2]) * f64::from(q[2])
        + f64::from(q[3]) * f64::from(q[3]))
    .sqrt();
    if n < 1e-9 {
        return [0.0, 0.0, 0.0, 1.0];
    }
    [
        (f64::from(q[0]) / n) as f32,
        (f64::from(q[1]) / n) as f32,
        (f64::from(q[2]) / n) as f32,
        (f64::from(q[3]) / n) as f32,
    ]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use citadel_wire::tsync::{Hello, ObjectUpdate, TransformFields};

    fn codec() -> TransformCodec {
        TransformCodec::from_hello(&Hello::default()).unwrap()
    }

    fn full_update(id: ObjectId, pos: [f32; 3], vel: [f32; 3]) -> ObjectUpdate {
        ObjectUpdate {
            object_id: id,
            gen_epoch: 0,
            fields: TransformFields {
                position: Some(pos),
                rotation: Some([0.0, 0.0, 0.0, 1.0]),
                velocity: Some(vel),
            },
            last_input_seq: None,
        }
    }

    #[test]
    fn reconstructs_full_then_delta() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        let s1 = Snapshot {
            server_tick: 1,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [100.0, 0.0, 0.0], [0.0; 3])],
        };
        assert!(v.apply(&s1));
        assert!((v.object(1).unwrap().state.position[0] - 100.0).abs() <= 0.0625);

        // Delta based on s1: only position changes.
        let s2 = Snapshot {
            server_tick: 2,
            snapshot_id: 2,
            base_snapshot_id: 1,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![ObjectUpdate {
                object_id: 1,
                gen_epoch: 0,
                fields: TransformFields {
                    position: Some([150.0, 0.0, 0.0]),
                    rotation: None,
                    velocity: None,
                },
                last_input_seq: None,
            }],
        };
        assert!(v.apply(&s2));
        assert!((v.object(1).unwrap().state.position[0] - 150.0).abs() <= 0.0625);
    }

    fn full_snapshot(id: u32) -> Snapshot {
        Snapshot {
            server_tick: id,
            snapshot_id: id,
            base_snapshot_id: 0,
            send_rate_hz: 60,
            removed: vec![],
            updates: vec![full_update(1, [0.0, 0.0, 0.0], [0.0; 3])],
        }
    }

    #[test]
    fn adaptive_buffer_shrinks_on_clean_link_and_grows_on_loss() {
        let mut v = RemoteWorldView::new(codec(), 60, 60);
        let start = v.interp_multiplier();
        assert!((start - 2.5).abs() < 1e-9, "starts at the ceiling");

        // A run of clean, consecutive snapshots decays the buffer toward the floor.
        for id in 1..=60u32 {
            assert!(v.apply(&full_snapshot(id)));
        }
        let after_clean = v.interp_multiplier();
        assert!(after_clean < start, "clean link shrinks the buffer");
        assert!(after_clean >= 1.5, "never below the floor");

        // A gap in applied ids (lost snapshots) grows it back, capped at the ceiling.
        assert!(v.apply(&full_snapshot(80))); // gap of ~20
        let after_loss = v.interp_multiplier();
        assert!(after_loss > after_clean, "loss grows the buffer");
        assert!(after_loss <= 2.5, "never above the ceiling");
    }

    #[test]
    fn discards_delta_with_missing_base() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        // First a full baseline (snapshot 1).
        let s1 = Snapshot {
            server_tick: 1,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [0.0, 0.0, 0.0], [0.0; 3])],
        };
        assert!(v.apply(&s1));
        // A delta based on snapshot 5, which we never received.
        let s = Snapshot {
            server_tick: 6,
            snapshot_id: 6,
            base_snapshot_id: 5,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![],
        };
        assert!(!v.apply(&s));
        assert_eq!(v.discarded_missing_base(), 1);
    }

    #[test]
    fn discards_stale_reordered_snapshot() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        for id in [1u32, 2] {
            let s = Snapshot {
                server_tick: id,
                snapshot_id: id,
                base_snapshot_id: 0,
                send_rate_hz: 20,
                removed: vec![],
                updates: vec![full_update(1, [id as f32, 0.0, 0.0], [0.0; 3])],
            };
            assert!(v.apply(&s));
        }
        // A reordered old snapshot (id 1) arrives after id 2: discarded.
        let old = Snapshot {
            server_tick: 1,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [1.0, 0.0, 0.0], [0.0; 3])],
        };
        assert!(!v.apply(&old));
        assert_eq!(v.discarded_stale(), 1);
    }

    #[test]
    fn removal_drops_object() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        let s1 = Snapshot {
            server_tick: 1,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![
                full_update(1, [0.0; 3], [0.0; 3]),
                full_update(2, [10.0, 0.0, 0.0], [0.0; 3]),
            ],
        };
        assert!(v.apply(&s1));
        assert_eq!(v.object_ids(), vec![1, 2]);
        let s2 = Snapshot {
            server_tick: 2,
            snapshot_id: 2,
            base_snapshot_id: 1,
            send_rate_hz: 20,
            removed: vec![2],
            updates: vec![],
        };
        assert!(v.apply(&s2));
        assert_eq!(v.object_ids(), vec![1]);
    }

    #[test]
    fn interpolation_renders_between_samples() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        // Two samples: tick 10 at x=0, tick 20 at x=1000, constant velocity.
        let vel = [(1000.0 / (10.0 / 60.0)) as f32, 0.0, 0.0]; // cm/s over 10 ticks
        v.apply(&Snapshot {
            server_tick: 10,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [0.0, 0.0, 0.0], vel)],
        });
        v.apply(&Snapshot {
            server_tick: 20,
            snapshot_id: 2,
            base_snapshot_id: 1,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [1000.0, 0.0, 0.0], vel)],
        });
        // Midway (tick 15): Hermite with matched constant velocity ≈ linear mid.
        let mid = v.sample(1, 15.0).expect("sample");
        assert!(
            (mid.position[0] - 500.0).abs() < 20.0,
            "x={}",
            mid.position[0]
        );
        // Before the buffer clamps to the first sample.
        let before = v.sample(1, 5.0).expect("sample");
        assert!((before.position[0] - 0.0).abs() < 1e-3);
    }

    #[test]
    fn bounded_extrapolation_on_drain() {
        let mut v = RemoteWorldView::new(codec(), 60, 20);
        let vel = [600.0, 0.0, 0.0]; // 600 cm/s => 10 cm per tick at 60 Hz
        v.apply(&Snapshot {
            server_tick: 10,
            snapshot_id: 1,
            base_snapshot_id: 0,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [0.0, 0.0, 0.0], vel)],
        });
        v.apply(&Snapshot {
            server_tick: 20,
            snapshot_id: 2,
            base_snapshot_id: 1,
            send_rate_hz: 20,
            removed: vec![],
            updates: vec![full_update(1, [100.0, 0.0, 0.0], vel)],
        });
        // Far past the last sample: extrapolation is capped at 0.25 s (=15cm here),
        // never unbounded.
        let far = v.sample(1, 1000.0).expect("sample");
        let max_expected = 100.0 + 600.0 * 0.25; // last pos + capped extrapolation
        assert!(
            far.position[0] <= max_expected + 1.0,
            "x={}",
            far.position[0]
        );
        assert!(far.position[0] >= 100.0);
    }
}
