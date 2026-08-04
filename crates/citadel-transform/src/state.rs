//! The per-object transform state shared by the server and the engine SDK.

use citadel_wire::codec::IDENTITY_QUAT;

/// A networked object's authoritative transform: position + rotation (+ optional
/// velocity for extrapolation/Hermite). Units are centimeters / cm-per-second,
/// matching the shared codec's canonical unit (design §6.1).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TransformState {
    /// Position `(x, y, z)` in cm.
    pub position: [f32; 3],
    /// Rotation quaternion `(x, y, z, w)`.
    pub rotation: [f32; 4],
    /// Velocity `(x, y, z)` in cm/s.
    pub velocity: [f32; 3],
}

impl Default for TransformState {
    fn default() -> Self {
        Self {
            position: [0.0; 3],
            rotation: IDENTITY_QUAT,
            velocity: [0.0; 3],
        }
    }
}

impl TransformState {
    /// A state at `position` with identity rotation and zero velocity.
    #[must_use]
    pub fn at(position: [f32; 3]) -> Self {
        Self {
            position,
            ..Self::default()
        }
    }
}
