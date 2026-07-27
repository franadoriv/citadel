//! Client-side prediction + reconciliation for `OwnerPredicted` objects
//! (design §5.1,  P2-b).
//!
//! This is the tested Rust **reference** the Unreal `UCitadelTransformSync`
//! component (and later Unity/Godot) mirror. Interpolating the local avatar in
//! the past would feel laggy, so the owner predicts from local input immediately
//! and reconciles against the server's authoritative correction:
//!
//! 1. Each input is tagged with a monotonic `seq`, applied immediately
//!    (prediction), and buffered in the ring keyed by seq.
//! 2. When a snapshot carries `last_input_seq` (the highest **contiguous** input
//!    the server applied to this object, [`super::RemoteWorldView::owner_ack`]),
//!    the ring **snaps the object to the authoritative post-input state, drops
//!    inputs `<= last_input_seq`, and replays only inputs `> last_input_seq` in
//!    seq order** (rollback + replay).
//! 3. **Error smoothing applies to the rendered visual offset only**, never the
//!    simulation state: the sim state is snapped to authority immediately (so
//!    collision stays correct), while the *rendered* position eases from where it
//!    was toward the corrected state (factor 0.95 for small errors ≤25 cm, 0.85
//!    for large ≥1 m). A teleport-scale divergence hard-snaps instead of easing.
//!
//! Reconciliation is **local-owner-only rollback**; remote objects are never
//! rolled back (they interpolate in [`super::RemoteWorldView`]).

use std::collections::VecDeque;

use super::authority::TransformState;

/// One buffered local input the owner predicted with (design §5.1). Kept until
/// the server acks a `>= seq` contiguous input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PredictedInput {
    /// The input's monotonic sequence number.
    pub seq: u32,
    /// The kinematic movement intent in cm/s applied for this input.
    pub move_velocity: [f32; 3],
    /// The timestep in seconds this input covered.
    pub dt: f32,
}

/// Reconciliation error-smoothing tuning (design §5.1, research §2.3).
#[derive(Debug, Clone, Copy)]
pub struct ReconcileConfig {
    /// Visual-offset decay factor per frame for small errors (≤ `small_cm`).
    pub smoothing_small: f32,
    /// Visual-offset decay factor per frame for large errors (≥ `large_cm`).
    pub smoothing_large: f32,
    /// Error magnitude (cm) at/below which `smoothing_small` is used.
    pub small_cm: f32,
    /// Error magnitude (cm) at/above which `smoothing_large` is used.
    pub large_cm: f32,
    /// Error magnitude (cm) above which the visual offset hard-snaps (teleport).
    pub hard_snap_cm: f32,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            smoothing_small: 0.95,
            smoothing_large: 0.85,
            small_cm: 25.0,
            large_cm: 100.0,
            hard_snap_cm: 500.0,
        }
    }
}

/// The owner's prediction ring: buffers recent inputs + the predicted sim state,
/// and reconciles against server corrections (design §5.1). One per owned object.
#[derive(Debug, Clone)]
pub struct PredictionRing {
    /// Unacked inputs, ascending by seq.
    inputs: VecDeque<PredictedInput>,
    /// The current predicted **simulation** state (authoritative-grade; snapped
    /// to the server on reconcile — never smoothed).
    predicted: TransformState,
    /// Next input seq to mint (monotonic; `0` reserved for "none").
    next_seq: u32,
    /// The rendered visual offset from the sim position (smoothed toward zero so
    /// a correction never snaps the *rendered* avatar). Design §5.1: visual only.
    visual_offset: [f32; 3],
    config: ReconcileConfig,
}

impl PredictionRing {
    /// A new ring seeded with `initial` predicted state.
    #[must_use]
    pub fn new(initial: TransformState, config: ReconcileConfig) -> Self {
        Self {
            inputs: VecDeque::new(),
            predicted: initial,
            next_seq: 1,
            visual_offset: [0.0; 3],
            config,
        }
    }

    /// Predict one input locally: apply it immediately to the predicted state,
    /// buffer it for later replay, and return its minted seq. This is the
    /// input-latency-free owner feel — the avatar moves before the server replies.
    pub fn push_input(&mut self, move_velocity: [f32; 3], dt: f32) -> u32 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1).max(1);
        let input = PredictedInput {
            seq,
            move_velocity,
            dt,
        };
        apply_input(&mut self.predicted, &input);
        self.inputs.push_back(input);
        seq
    }

    /// The predicted **simulation** state (used for collision/gameplay).
    #[must_use]
    pub fn predicted_state(&self) -> TransformState {
        self.predicted
    }

    /// The **rendered** position: predicted sim position plus the smoothed visual
    /// offset (design §5.1 — smoothing lives here, not in the sim state).
    #[must_use]
    pub fn render_position(&self) -> [f32; 3] {
        [
            self.predicted.position[0] + self.visual_offset[0],
            self.predicted.position[1] + self.visual_offset[1],
            self.predicted.position[2] + self.visual_offset[2],
        ]
    }

    /// The current rendered visual offset magnitude (cm) (tests/observability).
    #[must_use]
    pub fn visual_offset(&self) -> [f32; 3] {
        self.visual_offset
    }

    /// Number of unacked inputs still buffered.
    #[must_use]
    pub fn pending_inputs(&self) -> usize {
        self.inputs.len()
    }

    /// Reconcile against the server's authoritative post-input state for this
    /// object and `last_input_seq` (the highest contiguous input it applied,
    /// design §5.1):
    ///
    /// 1. Drop inputs `<= last_input_seq` (the server already accounted for them).
    /// 2. Snap the predicted **sim** state to `authoritative`, then replay the
    ///    remaining inputs (`> last_input_seq`) in seq order (rollback + replay).
    /// 3. Fold the difference between the *old* rendered position and the *new*
    ///    predicted position into the visual offset so the rendered avatar does
    ///    not jump; [`advance_smoothing`](PredictionRing::advance_smoothing) then
    ///    eases it to zero. A teleport-scale error hard-snaps (offset cleared).
    pub fn reconcile(&mut self, authoritative: TransformState, last_input_seq: u32) {
        // The rendered position *before* the correction, so we can preserve it.
        let old_render = self.render_position();

        // 1. Drop acked inputs.
        while let Some(front) = self.inputs.front() {
            if front.seq <= last_input_seq {
                self.inputs.pop_front();
            } else {
                break;
            }
        }

        // 2. Snap sim state to authority, replay the unacked tail in order.
        self.predicted = authoritative;
        // Defensive: ensure ascending seq order for a deterministic replay.
        let mut tail: Vec<PredictedInput> = self.inputs.iter().copied().collect();
        tail.sort_by_key(|i| i.seq);
        for input in &tail {
            apply_input(&mut self.predicted, input);
        }
        self.inputs = tail.into();

        // 3. Preserve the rendered position by folding the correction into the
        //    visual offset (sim state stays authoritative; only the render eases).
        let new_pos = self.predicted.position;
        let mut offset = [
            old_render[0] - new_pos[0],
            old_render[1] - new_pos[1],
            old_render[2] - new_pos[2],
        ];
        let err = magnitude(offset);
        if err >= self.config.hard_snap_cm {
            // Teleport-scale divergence: snap the render to authority immediately.
            offset = [0.0; 3];
        }
        self.visual_offset = offset;
    }

    /// Ease the rendered visual offset toward zero by the smoothing factor for
    /// its current magnitude (design §5.1). Call once per rendered frame. The
    /// simulation state is untouched.
    pub fn advance_smoothing(&mut self) {
        let err = magnitude(self.visual_offset);
        if err <= 1e-3 {
            self.visual_offset = [0.0; 3];
            return;
        }
        if err >= self.config.hard_snap_cm {
            self.visual_offset = [0.0; 3];
            return;
        }
        // Interpolate the factor between small/large error bands.
        let factor = if err <= self.config.small_cm {
            self.config.smoothing_small
        } else if err >= self.config.large_cm {
            self.config.smoothing_large
        } else {
            let t = (err - self.config.small_cm)
                / (self.config.large_cm - self.config.small_cm).max(1e-3);
            self.config.smoothing_small
                + (self.config.smoothing_large - self.config.smoothing_small) * t
        };
        for c in &mut self.visual_offset {
            *c *= factor;
        }
    }
}

/// Kinematically apply one input to a state (must match the server's integration
/// in [`super::input::apply_owner_input`], sans the server's authoritative clamp).
fn apply_input(state: &mut TransformState, input: &PredictedInput) {
    let dt = if input.dt.is_finite() && input.dt > 0.0 {
        input.dt
    } else {
        0.0
    };
    state.velocity = input.move_velocity;
    for axis in 0..3 {
        state.position[axis] += input.move_velocity[axis] * dt;
    }
}

fn magnitude(v: [f32; 3]) -> f32 {
    (v[0] * v[0] + v[1] * v[1] + v[2] * v[2]).sqrt()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn ring() -> PredictionRing {
        PredictionRing::new(TransformState::default(), ReconcileConfig::default())
    }

    #[test]
    fn prediction_moves_immediately() {
        let mut r = ring();
        let s1 = r.push_input([600.0, 0.0, 0.0], 0.1); // +60 cm
        assert_eq!(s1, 1);
        assert!((r.predicted_state().position[0] - 60.0).abs() < 1e-3);
        assert_eq!(r.pending_inputs(), 1);
    }

    #[test]
    fn reconcile_drops_acked_and_replays_unacked() {
        let mut r = ring();
        // Predict three inputs: +10, +10, +10 -> x = 30.
        r.push_input([100.0, 0.0, 0.0], 0.1);
        r.push_input([100.0, 0.0, 0.0], 0.1);
        r.push_input([100.0, 0.0, 0.0], 0.1);
        assert!((r.predicted_state().position[0] - 30.0).abs() < 1e-3);
        assert_eq!(r.pending_inputs(), 3);

        // Server acks seq 1 with authoritative x=10 (exactly our prediction).
        // Inputs 2 and 3 replay from there -> x = 30, no drift, no pending seq 1.
        r.reconcile(TransformState::at([10.0, 0.0, 0.0]), 1);
        assert_eq!(r.pending_inputs(), 2, "seq 1 dropped, 2 and 3 remain");
        assert!((r.predicted_state().position[0] - 30.0).abs() < 1e-3);
        // Perfect agreement => negligible visual offset.
        assert!(magnitude(r.visual_offset()) < 1e-3);
    }

    #[test]
    fn correction_preserves_render_then_smooths_to_zero() {
        let mut r = ring();
        r.push_input([100.0, 0.0, 0.0], 0.1); // predict x=10
        r.push_input([100.0, 0.0, 0.0], 0.1); // predict x=20
        let render_before = r.render_position()[0];
        assert!((render_before - 20.0).abs() < 1e-3);

        // Server disagrees: after input 1 the authoritative x is 5 (we mispredicted).
        // Replaying input 2 -> sim x = 15. The *rendered* position must stay ~20
        // (no snap), then ease toward 15 over frames.
        r.reconcile(TransformState::at([5.0, 0.0, 0.0]), 1);
        assert!(
            (r.predicted_state().position[0] - 15.0).abs() < 1e-3,
            "sim snapped+replayed"
        );
        assert!(
            (r.render_position()[0] - 20.0).abs() < 1e-3,
            "render preserved, no snap-back: {}",
            r.render_position()[0]
        );
        // Smoothing decays the offset toward zero; sim state never moves.
        let mut last = magnitude(r.visual_offset());
        for _ in 0..200 {
            r.advance_smoothing();
            let now = magnitude(r.visual_offset());
            assert!(now <= last + 1e-4, "offset is non-increasing");
            last = now;
        }
        assert!(magnitude(r.visual_offset()) < 0.5, "eased to ~zero");
        assert!(
            (r.predicted_state().position[0] - 15.0).abs() < 1e-3,
            "sim untouched by smoothing"
        );
    }

    #[test]
    fn teleport_scale_error_hard_snaps() {
        let mut r = ring();
        r.push_input([100.0, 0.0, 0.0], 0.1); // predict x=10
        // Server teleported us far away (> hard_snap 500 cm): render must snap,
        // not ooze across the map.
        r.reconcile(TransformState::at([10_000.0, 0.0, 0.0]), 1);
        assert!(
            magnitude(r.visual_offset()) < 1e-3,
            "hard-snap clears the visual offset"
        );
        assert!((r.render_position()[0] - 10_000.0).abs() < 1e-3);
    }
}
