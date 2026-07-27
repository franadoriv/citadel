//! Deterministic, server-oriented kinematic collision against static triangle
//! meshes.
//!
//! All distances are **centimetres (cm)**, velocities are **cm/s**, and
//! accelerations are **cm/s²**. These units match Citadel's transform codec.
//! `step` is deterministic for an identical input sequence, fixed positive
//! timestep, target architecture, and build configuration. Cross-platform
//! bit-identical floating-point results are intentionally not guaranteed.
//!
//! This crate is deliberately small: it has no async runtime, wall-clock,
//! random source, or dependency beyond `citadel-map`.

#![forbid(unsafe_code)]

mod bvh;
mod controller;
mod math;
mod queries;

pub use bvh::{StaticTriBvh, Triangle};
pub use controller::{
    DEPENETRATION_PASSES, GROUND_PROBE_DISTANCE, MAX_SLIDE_ITERATIONS, PhysicsBody, PhysicsConfig,
    Shape, step,
};
pub use queries::{GroundHit, RaycastHit, ground_height, raycast, sphere_overlap};
