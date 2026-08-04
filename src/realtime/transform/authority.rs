//! The per-object authoritative record and its transform state.

use citadel_physics::PhysicsBody;

use super::{ObjectId, SyncRole};

// `TransformState` moved to the `citadel-transform` leaf crate so the engine SDK
// can link it without the server. Re-exported so the sibling modules that reach
// for `super::authority::TransformState` are unchanged.
pub use citadel_transform::TransformState;

/// The server's authoritative record for one networked object (design §7.1).
///
/// `gen_epoch` is bumped only on **respawn / object-id reuse** so a stale delta
/// for a recycled id is rejected; area-of-interest enter/exit is *not* a
/// generation change (it is handled by snapshot set membership — a re-entering
/// object is simply absent from the client's base and sent full again).
#[derive(Debug, Clone, PartialEq)]
pub struct TransformAuthority {
    /// Match-unique replicated-object id.
    pub object_id: ObjectId,
    /// Who drives this object's transform, and how clients obtain it.
    pub role: SyncRole,
    /// The predicting owner (raw participant id), or `0` for server-owned.
    pub owner: u64,
    /// Monotonic ownership epoch guarding reordered handoffs.
    pub ownership_epoch: u32,
    /// Replication generation guarding object-id reuse/respawn.
    pub gen_epoch: u16,
    /// The authoritative transform.
    pub current: TransformState,
    /// Opt-in kinematic controller state. Only server-simulated authorities may
    /// hold a body; the transform world enforces that invariant.
    pub(super) body: Option<Box<PhysicsBody>>,
    /// Whether velocity is replicated (enables client Hermite + extrapolation).
    pub replicate_velocity: bool,
    /// Base network priority feeding the per-client priority accumulator.
    pub priority: f32,
    /// Highest **contiguous** owner input seq applied to this object; echoed to
    /// the owner in its snapshot so it can reconcile (design §5.1, P2). `0` = none.
    pub last_input_seq: u32,
    /// Whether this object records a [`RewindBuffer`](super::RewindBuffer) so it
    /// is eligible for server-rewind hit tests (opt-in per object, design §7.2).
    pub hit_eligible: bool,
}

impl TransformAuthority {
    /// A new server-owned authority for `object_id` in `role` at `state`.
    #[must_use]
    pub fn new(object_id: ObjectId, role: SyncRole, state: TransformState) -> Self {
        Self {
            object_id,
            role,
            owner: 0,
            ownership_epoch: 0,
            gen_epoch: 0,
            current: state,
            body: None,
            replicate_velocity: false,
            priority: 1.0,
            last_input_seq: 0,
            hit_eligible: false,
        }
    }

    /// Whether `participant` is this object's predicting owner (design §5.1). An
    /// object is owned only when it is [`OwnerPredicted`](SyncRole::OwnerPredicted),
    /// its `owner` is non-zero, and it matches `participant`.
    #[must_use]
    pub fn is_owned_by(&self, participant: u64) -> bool {
        self.role == SyncRole::OwnerPredicted && self.owner != 0 && self.owner == participant
    }

    /// Kinematically integrate the current velocity into position over `dt`
    /// seconds. Zero velocity is a no-op, so static objects never drift.
    pub fn integrate(&mut self, dt: f32) {
        if !dt.is_finite() || dt <= 0.0 {
            return;
        }
        for axis in 0..3 {
            let v = self.current.velocity[axis];
            if v != 0.0 {
                self.current.position[axis] += v * dt;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integrate_moves_by_velocity_and_ignores_zero() {
        let mut a =
            TransformAuthority::new(1, SyncRole::ServerSimulated, TransformState::default());
        a.current.velocity = [100.0, 0.0, -50.0];
        a.integrate(0.5);
        assert_eq!(a.current.position, [50.0, 0.0, -25.0]);
        // A second integrate with a non-positive dt is a no-op.
        a.integrate(0.0);
        assert_eq!(a.current.position, [50.0, 0.0, -25.0]);
    }
}
