//! Owner-input processing: in-order application, dedup, contiguous-ack tracking,
//! and validated kinematic integration (design §5.1,  P2-a).
//!
//! The server receives a redundant bundle of individually-sequenced owner input
//! frames ([`citadel_wire::tsync::InputFrame`]). This module turns that untrusted
//! stream into an authoritative transform advance while preserving the invariants
//! the client's reconciliation depends on:
//!
//! - **Individual sequences, applied strictly in seq order.** Each frame carries
//!   its own monotonic `input_seq`; the server never coalesces frames into a
//!   "highest seq" (review §13.2). An out-of-order frame is **buffered**
//!   and applied only once the gap before it fills, so the authoritative state
//!   reflects *exactly* the contiguous prefix it acks. Applying an out-of-order
//!   frame early would let the client (which replays inputs `> last_input_seq`)
//!   double-count it.
//! - **Dedup of redundant resends.** Redundant bundling resends recent frames so
//!   a single datagram loss self-heals; a frame already applied/buffered is
//!   ignored.
//! - **Contiguous ack.** The value echoed to the owner
//!   ([`OwnerInputQueue::last_contiguous`]) is the highest *contiguous* applied
//!   seq — acking 12 while 11 is missing is forbidden, so the client can safely
//!   drop `<= last_input_seq` and replay the rest.
//! - **Untrusted input is validated.** Ownership, `ownership_epoch`, per-tick
//!   rate, and speed/position bounds are all checked before anything is applied
//!   (design §2.1, §5.1); the client is *allowed to predict*, not *trusted*.

use std::collections::BTreeMap;

use citadel_wire::codec::WorldBounds;
use citadel_wire::tsync::InputFrame;

use super::authority::TransformAuthority;

/// How far above the contiguous watermark an out-of-order seq may sit before it
/// is dropped. Bounds the pending buffer against a client spamming sparse seqs.
const MAX_AHEAD_GAP: u32 = 256;

/// Per-object in-order queue + contiguous-ack bookkeeping for owner inputs
/// (design §5.1). Buffers out-of-order frames and releases them only in
/// contiguous seq order.
#[derive(Debug, Clone, Default)]
pub struct OwnerInputQueue {
    last_contiguous: u32,
    pending: BTreeMap<u32, InputFrame>,
}

/// Why an offered owner input frame was not accepted (for metrics/tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOutcome {
    /// Buffered and/or applied (its seq is new and in range). Zero or more frames
    /// may have been released to the authority as a result.
    Accepted,
    /// A duplicate/stale/too-far-ahead seq — nothing changed.
    Duplicate,
}

impl OwnerInputQueue {
    /// A fresh queue (nothing applied yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest *contiguous* applied seq — the reconciliation ack (design §5.1).
    #[must_use]
    pub fn last_contiguous(&self) -> u32 {
        self.last_contiguous
    }

    /// Number of buffered out-of-order frames awaiting an earlier gap.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Offer a frame. Returns `(outcome, ready)` where `ready` is the frames now
    /// releasable **in contiguous seq order** (empty if the frame filled no gap
    /// or was a duplicate). The contiguous watermark advances across `ready`.
    pub fn offer(&mut self, frame: InputFrame) -> (InputOutcome, Vec<InputFrame>) {
        let seq = frame.input_seq;
        // Seq 0 reserved for "none"; stale/duplicate/too-far-ahead are ignored.
        if seq == 0
            || seq <= self.last_contiguous
            || self.pending.contains_key(&seq)
            || seq > self.last_contiguous.saturating_add(MAX_AHEAD_GAP)
        {
            return (InputOutcome::Duplicate, Vec::new());
        }
        self.pending.insert(seq, frame);
        // Release the contiguous run starting at last_contiguous + 1.
        let mut ready = Vec::new();
        while let Some(f) = self.pending.remove(&(self.last_contiguous + 1)) {
            self.last_contiguous += 1;
            ready.push(f);
        }
        (InputOutcome::Accepted, ready)
    }
}

/// Server-side validation/clamp limits for owner input (design §2.1, §5.1).
#[derive(Debug, Clone, Copy)]
pub struct InputLimits {
    /// Max movement-intent speed in cm/s; a faster `move_velocity` is clamped.
    pub max_speed: f32,
    /// Max timestep in seconds a single input frame may claim (anti-speedhack);
    /// a larger `dt` is clamped down before integration.
    pub max_dt: f32,
}

impl Default for InputLimits {
    fn default() -> Self {
        // ~20 m/s default cap (2000 cm/s) and a 100 ms max step: generous for
        // avatars/projectiles yet bounded so a forged frame cannot teleport.
        Self {
            max_speed: 2000.0,
            max_dt: 0.1,
        }
    }
}

/// Kinematically integrate one **already-ordered** input frame into `authority`
/// (design §5.1). The movement intent is clamped (`max_speed`, `max_dt`),
/// integrated into the authoritative position (clamped to `bounds`, never
/// wrapped), and recorded as the authority's velocity (so remote
/// interpolation/extrapolation is coherent). The caller has already validated
/// ownership/epoch and released this frame in contiguous order.
pub fn integrate_owner_frame(
    authority: &mut TransformAuthority,
    frame: &InputFrame,
    limits: &InputLimits,
    bounds: &WorldBounds,
) {
    let dt = if frame.dt.is_finite() {
        frame.dt.clamp(0.0, limits.max_dt)
    } else {
        0.0
    };
    let vel = clamp_speed(frame.move_velocity, limits.max_speed);
    authority.current.velocity = vel;
    for (axis, p) in authority.current.position.iter_mut().enumerate() {
        *p = clamp_axis(*p + vel[axis] * dt, bounds.min[axis], bounds.max[axis]);
    }
}

/// Clamp a velocity vector to `max_speed` magnitude, preserving direction.
fn clamp_speed(v: [f32; 3], max_speed: f32) -> [f32; 3] {
    if !max_speed.is_finite() || max_speed <= 0.0 {
        return [0.0; 3];
    }
    let mag_sq = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
    let max_sq = max_speed * max_speed;
    if !mag_sq.is_finite() {
        return [0.0; 3];
    }
    if mag_sq <= max_sq {
        return v;
    }
    let scale = max_speed / mag_sq.sqrt();
    [v[0] * scale, v[1] * scale, v[2] * scale]
}

/// Clamp `x` to `[lo, hi]` (never wraps; NaN falls back to `lo`).
fn clamp_axis(x: f32, lo: f32, hi: f32) -> f32 {
    if !x.is_finite() {
        return lo;
    }
    x.clamp(lo, hi)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::realtime::transform::SyncRole;
    use crate::realtime::transform::authority::TransformState;

    fn owned_authority() -> TransformAuthority {
        let mut a = TransformAuthority::new(7, SyncRole::OwnerPredicted, TransformState::default());
        a.owner = 42;
        a.ownership_epoch = 3;
        a
    }

    fn frame(seq: u32, vel: [f32; 3]) -> InputFrame {
        InputFrame {
            input_seq: seq,
            sim_tick: seq,
            dt: 1.0 / 60.0,
            object_id: 7,
            ownership_epoch: 3,
            move_velocity: vel,
            payload: Vec::new(),
            fire: None,
        }
    }

    #[test]
    fn queue_releases_in_contiguous_order_and_dedups() {
        let mut q = OwnerInputQueue::new();
        // In-order frame 1 releases immediately.
        let (o, ready) = q.offer(frame(1, [0.0; 3]));
        assert_eq!(o, InputOutcome::Accepted);
        assert_eq!(
            ready.iter().map(|f| f.input_seq).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(q.last_contiguous(), 1);

        // Frame 3 arrives before 2: buffered, NOT released (gap at 2).
        let (o, ready) = q.offer(frame(3, [0.0; 3]));
        assert_eq!(o, InputOutcome::Accepted);
        assert!(ready.is_empty(), "held behind the gap");
        assert_eq!(q.last_contiguous(), 1);
        assert_eq!(q.pending_len(), 1);

        // Frame 2 fills the gap: 2 AND 3 release together, in order.
        let (_, ready) = q.offer(frame(2, [0.0; 3]));
        assert_eq!(
            ready.iter().map(|f| f.input_seq).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(q.last_contiguous(), 3);

        // Duplicate/stale are ignored.
        assert_eq!(q.offer(frame(2, [0.0; 3])).0, InputOutcome::Duplicate);
        assert_eq!(q.offer(frame(0, [0.0; 3])).0, InputOutcome::Duplicate);
    }

    #[test]
    fn queue_drops_absurd_forward_jump() {
        let mut q = OwnerInputQueue::new();
        let (o, ready) = q.offer(frame(u32::MAX, [0.0; 3]));
        assert_eq!(o, InputOutcome::Duplicate);
        assert!(ready.is_empty());
        assert_eq!(q.last_contiguous(), 0);
    }

    #[test]
    fn integrate_advances_position_and_clamps() {
        let mut a = owned_authority();
        let limits = InputLimits {
            max_speed: 100.0,
            max_dt: 1.0,
        };
        let bounds = WorldBounds {
            min: [-10.0, -10.0, -10.0],
            max: [10.0, 10.0, 10.0],
            values_per_unit: 8,
        };
        // Huge intent over 1 s: speed clamps to 100 cm/s; position clamps to +10.
        let mut f = frame(1, [1_000_000.0, 0.0, 0.0]);
        f.dt = 1.0;
        integrate_owner_frame(&mut a, &f, &limits, &bounds);
        assert!(
            (a.current.velocity[0] - 100.0).abs() < 1e-3,
            "speed clamped"
        );
        assert!(
            (a.current.position[0] - 10.0).abs() < 1e-3,
            "position clamped to bound"
        );
    }
}
