//! Fixed-order kinematic integration, collision, slide, and ground probing.

use crate::{
    StaticTriBvh, Triangle,
    math::{
        EPSILON, add, add_assign, closest_points_segment_triangle, cross, dot, length,
        length_squared, normalize_or, scale, sub,
    },
};

/// Number of deterministic sweep-and-slide iterations performed by [`step`].
pub const MAX_SLIDE_ITERATIONS: usize = 4;
/// Number of deterministic static-overlap resolution passes performed by [`step`].
pub const DEPENETRATION_PASSES: usize = 4;
/// Downward distance, in centimetres, used by the post-move ground probe.
pub const GROUND_PROBE_DISTANCE: f32 = 1.0;

const CONTACT_EPSILON: f32 = 1.0e-3;
const PENETRATION_SLOP: f32 = 1.0e-4;
const GROUND_NORMAL_MIN_Y: f32 = 0.5;
const MOVE_INTENT_RESPONSE_PER_SECOND: f32 = 12.0;
const CAPSULE_SWEEP_ITERATIONS: usize = 16;

/// The collision shape of a kinematic body, expressed in centimetres.
///
/// A capsule's `height` is its **total** tip-to-tip height. Its position is
/// the centre of that total height; the cylindrical segment is shortened as
/// needed when `height < radius * 2.0`. An AABB position is its centre.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Shape {
    /// A vertical capsule with radius and total height in centimetres.
    Capsule {
        /// Hemisphere/cylinder radius in centimetres.
        radius: f32,
        /// Total tip-to-tip height in centimetres.
        height: f32,
    },
    /// An axis-aligned bounding box with half extents in centimetres.
    Aabb {
        /// Positive half extents along `x`, `y`, and `z` in centimetres.
        half_extents: [f32; 3],
    },
}

impl Shape {
    /// Return conservative broadphase half extents in centimetres.
    #[must_use]
    pub fn broadphase_half_extents(self) -> [f32; 3] {
        match self {
            Self::Capsule { radius, height } => [
                radius.max(0.0),
                (height.max(0.0) * 0.5).max(radius.max(0.0)),
                radius.max(0.0),
            ],
            Self::Aabb { half_extents } => [
                half_extents[0].abs(),
                half_extents[1].abs(),
                half_extents[2].abs(),
            ],
        }
    }
}

/// Default settings for a [`PhysicsBody`].
///
/// Units are centimetres, cm/s, and cm/s²: `gravity` and `buoyancy` are
/// magnitudes in cm/s², `drag` is a linear coefficient in 1/s, and
/// `max_speed` is in cm/s. Positive gravity pulls down the `-Y` axis; positive
/// buoyancy pushes up `+Y`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhysicsConfig {
    /// Initial collision shape.
    pub shape: Shape,
    /// Downward acceleration magnitude in cm/s².
    pub gravity: f32,
    /// Upward acceleration magnitude in cm/s².
    pub buoyancy: f32,
    /// Linear drag coefficient in 1/s.
    pub drag: f32,
    /// Maximum velocity magnitude in cm/s. Zero immobilizes the body.
    pub max_speed: f32,
}

impl Default for PhysicsConfig {
    fn default() -> Self {
        Self {
            shape: Shape::Capsule {
                radius: 30.0,
                height: 90.0,
            },
            gravity: 980.0,
            buoyancy: 0.0,
            drag: 0.0,
            max_speed: 2_000.0,
        }
    }
}

/// Per-actor kinematic controller state.
///
/// All values use centimetres, cm/s, or cm/s². Position and velocity remain
/// caller-owned transform state and are passed to [`step`] by mutable
/// reference. Non-zero `move_intent` components are desired horizontal
/// velocities in cm/s; zero is neutral steering (use `drag` to brake). Its `Y`
/// component is intentionally ignored so vertical motion remains physics-led.
#[derive(Debug, Clone, PartialEq)]
pub struct PhysicsBody {
    /// Collision shape.
    pub shape: Shape,
    /// Downward acceleration magnitude in cm/s².
    pub gravity: f32,
    /// Upward acceleration magnitude in cm/s².
    pub buoyancy: f32,
    /// Linear drag coefficient in 1/s.
    pub drag: f32,
    /// Maximum velocity magnitude in cm/s. Zero immobilizes the body.
    pub max_speed: f32,
    /// Desired horizontal velocity in cm/s, blended at a fixed response rate.
    /// A zero component leaves the existing component unchanged.
    pub move_intent: [f32; 3],
    /// Whether the latest [`step`] found a walkable surface below the body.
    pub grounded: bool,
}

impl PhysicsBody {
    /// Construct a body with [`PhysicsConfig::default`] tuning and `shape`.
    #[must_use]
    pub fn new(shape: Shape) -> Self {
        Self {
            shape,
            ..Self::default()
        }
    }

    /// Construct a body from an explicit set of defaultable settings.
    #[must_use]
    pub fn from_config(config: PhysicsConfig) -> Self {
        config.into()
    }
}

impl Default for PhysicsBody {
    fn default() -> Self {
        PhysicsConfig::default().into()
    }
}

impl From<PhysicsConfig> for PhysicsBody {
    fn from(config: PhysicsConfig) -> Self {
        Self {
            shape: config.shape,
            gravity: config.gravity,
            buoyancy: config.buoyancy,
            drag: config.drag,
            max_speed: config.max_speed,
            move_intent: [0.0; 3],
            grounded: false,
        }
    }
}

/// Advance one fixed deterministic kinematic physics tick.
///
/// Position is in cm, velocity is in cm/s, and `dt` is in seconds. The
/// controller uses explicit-Euler position integration (`pos += old_vel * dt`)
/// followed by velocity integration. This preserves Citadel's transform-step
/// convention and makes free fall analytically predictable. The velocity update
/// accumulates gravity (`-Y`), buoyancy (`+Y`), and a fixed-rate horizontal
/// blend toward non-zero `move_intent` components, then applies linear drag and
/// a speed clamp.
///
/// With a BVH, movement is swept against static triangles, slides along hit
/// surfaces, performs [`DEPENETRATION_PASSES`] fixed overlap passes, and runs a
/// short downward ground probe. With `None`, it performs free fall and no mesh
/// collision. For deterministic replay, supply the same positive fixed `dt`
/// and call sequence on the same target architecture.
pub fn step(
    body: &mut PhysicsBody,
    pos: &mut [f32; 3],
    vel: &mut [f32; 3],
    dt: f32,
    bvh: Option<&StaticTriBvh>,
) {
    if !dt.is_finite() || dt <= 0.0 {
        return;
    }

    let motion_velocity = *vel;
    integrate_velocity(body, vel, dt);
    body.grounded = false;

    let Some(bvh) = bvh else {
        add_assign(pos, scale(motion_velocity, dt));
        return;
    };

    let mut position = *pos;
    let mut remaining = scale(motion_velocity, dt);
    let half_extents = body.shape.broadphase_half_extents();

    for _ in 0..MAX_SLIDE_ITERATIONS {
        if length_squared(remaining) <= EPSILON * EPSILON {
            continue;
        }

        let Some(hit) = earliest_sweep_hit(body.shape, position, remaining, bvh) else {
            add_assign(&mut position, remaining);
            remaining = [0.0; 3];
            continue;
        };

        let time = hit.time.clamp(0.0, 1.0);
        if time > 0.0 {
            add_assign(&mut position, scale(remaining, time));
        }

        remaining = scale(remaining, 1.0 - time);
        remove_into_surface(&mut remaining, hit.normal);
        remove_into_surface(vel, hit.normal);

        // A fixed outward bias keeps an exact t=0 contact from being re-hit
        // forever while remaining far smaller than the one-centimetre probe.
        add_assign(&mut position, scale(hit.normal, CONTACT_EPSILON));
    }

    for _ in 0..DEPENETRATION_PASSES {
        let candidates = bvh.query_swept_aabb(position, [0.0; 3], half_extents);
        let mut deepest: Option<Contact> = None;
        for triangle_index in candidates {
            let Some(triangle) = bvh.triangle(triangle_index).copied() else {
                continue;
            };
            let Some(contact) = shape_triangle_contact(body.shape, position, triangle) else {
                continue;
            };
            if contact.penetration <= PENETRATION_SLOP {
                continue;
            }
            if deepest.is_none_or(|current| contact.penetration > current.penetration) {
                deepest = Some(contact);
            }
        }
        if let Some(contact) = deepest {
            add_assign(
                &mut position,
                scale(contact.normal, contact.penetration + CONTACT_EPSILON),
            );
            remove_into_surface(vel, contact.normal);
        }
    }

    if let Some(ground) = ground_probe(body.shape, position, bvh) {
        body.grounded = true;
        if vel[1] < 0.0 {
            vel[1] = 0.0;
        }
        remove_into_surface(vel, ground.normal);
    }

    *pos = position;
}

fn integrate_velocity(body: &PhysicsBody, velocity: &mut [f32; 3], dt: f32) {
    let blend = (MOVE_INTENT_RESPONSE_PER_SECOND * dt).clamp(0.0, 1.0);
    // A zero intent is neutral steering, not an implicit braking command.
    // Callers that want frictional stopping use `drag`; preserving existing
    // horizontal velocity also keeps zero-input free fall analytically exact.
    if body.move_intent[0] != 0.0 {
        velocity[0] += (body.move_intent[0] - velocity[0]) * blend;
    }
    if body.move_intent[2] != 0.0 {
        velocity[2] += (body.move_intent[2] - velocity[2]) * blend;
    }
    velocity[1] += (body.buoyancy - body.gravity) * dt;

    let drag_factor = (1.0 - body.drag.max(0.0) * dt).max(0.0);
    velocity[0] *= drag_factor;
    velocity[1] *= drag_factor;
    velocity[2] *= drag_factor;

    let max_speed = body.max_speed.max(0.0);
    let speed_squared = length_squared(*velocity);
    if speed_squared > max_speed * max_speed && speed_squared > EPSILON {
        let scale_factor = max_speed / speed_squared.sqrt();
        velocity[0] *= scale_factor;
        velocity[1] *= scale_factor;
        velocity[2] *= scale_factor;
    }
}

fn earliest_sweep_hit(
    shape: Shape,
    position: [f32; 3],
    displacement: [f32; 3],
    bvh: &StaticTriBvh,
) -> Option<SweepHit> {
    let candidates = bvh.query_swept_aabb(position, displacement, shape.broadphase_half_extents());
    let mut earliest = None;
    for triangle_index in candidates {
        let Some(triangle) = bvh.triangle(triangle_index).copied() else {
            continue;
        };
        let Some(hit) = sweep_shape_triangle(shape, position, displacement, triangle) else {
            continue;
        };
        retain_earliest(&mut earliest, hit);
    }
    earliest
}

fn ground_probe(shape: Shape, position: [f32; 3], bvh: &StaticTriBvh) -> Option<SweepHit> {
    let displacement = [0.0, -GROUND_PROBE_DISTANCE, 0.0];
    let candidates = bvh.query_swept_aabb(position, displacement, shape.broadphase_half_extents());
    let mut earliest = None;
    for triangle_index in candidates {
        let Some(triangle) = bvh.triangle(triangle_index).copied() else {
            continue;
        };
        let Some(hit) = sweep_shape_triangle(shape, position, displacement, triangle) else {
            continue;
        };
        if hit.normal[1] >= GROUND_NORMAL_MIN_Y {
            retain_earliest(&mut earliest, hit);
        }
    }
    earliest
}

fn retain_earliest(current: &mut Option<SweepHit>, candidate: SweepHit) {
    match current {
        Some(existing) if candidate.time >= existing.time - EPSILON => {}
        _ => *current = Some(candidate),
    }
}

fn remove_into_surface(vector: &mut [f32; 3], normal: [f32; 3]) {
    let into_surface = dot(*vector, normal);
    if into_surface < 0.0 {
        vector[0] -= normal[0] * into_surface;
        vector[1] -= normal[1] * into_surface;
        vector[2] -= normal[2] * into_surface;
    }
}

#[derive(Debug, Clone, Copy)]
struct SweepHit {
    time: f32,
    normal: [f32; 3],
}

#[derive(Debug, Clone, Copy)]
struct Contact {
    normal: [f32; 3],
    penetration: f32,
}

fn sweep_shape_triangle(
    shape: Shape,
    position: [f32; 3],
    displacement: [f32; 3],
    triangle: Triangle,
) -> Option<SweepHit> {
    match shape {
        Shape::Capsule { radius, height } => {
            let radius = radius.max(0.0);
            let half_segment = (height.max(0.0) * 0.5 - radius).max(0.0);
            sweep_capsule_triangle(position, displacement, radius, half_segment, triangle)
        }
        Shape::Aabb { half_extents } => {
            sweep_aabb_triangle(position, displacement, half_extents, triangle)
        }
    }
}

fn shape_triangle_contact(shape: Shape, position: [f32; 3], triangle: Triangle) -> Option<Contact> {
    match shape {
        Shape::Capsule { radius, height } => {
            let radius = radius.max(0.0);
            let half_segment = (height.max(0.0) * 0.5 - radius).max(0.0);
            capsule_triangle_contact(position, radius, half_segment, triangle, None)
        }
        Shape::Aabb { half_extents } => aabb_triangle_contact(position, half_extents, triangle),
    }
}

fn sweep_capsule_triangle(
    centre: [f32; 3],
    displacement: [f32; 3],
    radius: f32,
    half_segment: f32,
    triangle: Triangle,
) -> Option<SweepHit> {
    let speed = length(displacement);
    if speed <= EPSILON {
        return None;
    }

    let mut time = 0.0;
    for _ in 0..CAPSULE_SWEEP_ITERATIONS {
        let at_time = add(centre, scale(displacement, time));
        let separation = capsule_triangle_separation(at_time, half_segment, triangle);
        if separation.distance <= radius + EPSILON {
            let contact = capsule_contact_from_separation(
                at_time,
                radius,
                triangle,
                separation,
                Some(displacement),
            );
            if time > 0.0
                || contact.penetration > PENETRATION_SLOP
                || dot(displacement, contact.normal) < -EPSILON
            {
                return Some(SweepHit {
                    time,
                    normal: contact.normal,
                });
            }
            return None;
        }

        // Segment-triangle distance is 1-Lipschitz under translation. This
        // conservative advancement cannot step through a contact.
        time += (separation.distance - radius) / speed;
        if time > 1.0 {
            return None;
        }
    }
    None
}

fn capsule_triangle_contact(
    centre: [f32; 3],
    radius: f32,
    half_segment: f32,
    triangle: Triangle,
    motion_hint: Option<[f32; 3]>,
) -> Option<Contact> {
    let separation = capsule_triangle_separation(centre, half_segment, triangle);
    if separation.distance > radius + EPSILON {
        return None;
    }
    Some(capsule_contact_from_separation(
        centre,
        radius,
        triangle,
        separation,
        motion_hint,
    ))
}

#[derive(Debug, Clone, Copy)]
struct CapsuleSeparation {
    capsule_point: [f32; 3],
    triangle_point: [f32; 3],
    distance: f32,
}

fn capsule_triangle_separation(
    centre: [f32; 3],
    half_segment: f32,
    triangle: Triangle,
) -> CapsuleSeparation {
    let segment_start = [centre[0], centre[1] - half_segment, centre[2]];
    let segment_end = [centre[0], centre[1] + half_segment, centre[2]];
    let [a, b, c] = triangle.vertices();
    let (capsule_point, triangle_point) =
        closest_points_segment_triangle(segment_start, segment_end, a, b, c);
    CapsuleSeparation {
        capsule_point,
        triangle_point,
        distance: length(sub(capsule_point, triangle_point)),
    }
}

fn capsule_contact_from_separation(
    centre: [f32; 3],
    radius: f32,
    triangle: Triangle,
    separation: CapsuleSeparation,
    motion_hint: Option<[f32; 3]>,
) -> Contact {
    let delta = sub(separation.capsule_point, separation.triangle_point);
    let normal = if separation.distance > EPSILON {
        scale(delta, separation.distance.recip())
    } else {
        let [a, b, c] = triangle.vertices();
        let triangle_normal = normalize_or(cross(sub(b, a), sub(c, a)), [0.0, 1.0, 0.0]);
        if let Some(motion) = motion_hint {
            if dot(motion, triangle_normal) > EPSILON {
                scale(triangle_normal, -1.0)
            } else if dot(motion, triangle_normal) < -EPSILON {
                triangle_normal
            } else {
                oriented_triangle_normal(centre, triangle_normal, a, b, c)
            }
        } else {
            oriented_triangle_normal(centre, triangle_normal, a, b, c)
        }
    };
    Contact {
        normal,
        penetration: (radius - separation.distance).max(0.0),
    }
}

fn oriented_triangle_normal(
    centre: [f32; 3],
    triangle_normal: [f32; 3],
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
) -> [f32; 3] {
    let centroid = scale(add(add(a, b), c), 1.0 / 3.0);
    if dot(sub(centre, centroid), triangle_normal) >= 0.0 {
        triangle_normal
    } else {
        scale(triangle_normal, -1.0)
    }
}

fn sweep_aabb_triangle(
    centre: [f32; 3],
    displacement: [f32; 3],
    half_extents: [f32; 3],
    triangle: Triangle,
) -> Option<SweepHit> {
    let half_extents = [
        half_extents[0].abs(),
        half_extents[1].abs(),
        half_extents[2].abs(),
    ];
    if let Some(contact) = aabb_triangle_contact(centre, half_extents, triangle)
        && (contact.penetration > PENETRATION_SLOP || dot(displacement, contact.normal) < -EPSILON)
    {
        return Some(SweepHit {
            time: 0.0,
            normal: contact.normal,
        });
    }

    let [a, b, c] = triangle.vertices();
    let mut entry_time: f32 = 0.0;
    let mut exit_time: f32 = 1.0;
    let mut entry_normal = None;
    for axis in triangle_aabb_axes(a, b, c) {
        let axis_length = length(axis);
        if axis_length <= EPSILON {
            continue;
        }
        let axis = scale(axis, axis_length.recip());
        let (triangle_min, triangle_max) = triangle_projection(axis, a, b, c);
        let radius = half_extents[0] * axis[0].abs()
            + half_extents[1] * axis[1].abs()
            + half_extents[2] * axis[2].abs();
        let lower = triangle_min - radius;
        let upper = triangle_max + radius;
        let start = dot(centre, axis);
        let speed = dot(displacement, axis);
        if speed.abs() <= EPSILON {
            if start < lower - EPSILON || start > upper + EPSILON {
                return None;
            }
            continue;
        }

        let first = (lower - start) / speed;
        let second = (upper - start) / speed;
        let axis_entry = first.min(second);
        let axis_exit = first.max(second);
        if axis_entry > entry_time {
            entry_time = axis_entry;
            entry_normal = Some(if speed > 0.0 { scale(axis, -1.0) } else { axis });
        }
        exit_time = exit_time.min(axis_exit);
        if entry_time > exit_time + EPSILON {
            return None;
        }
    }

    let normal = entry_normal?;
    if (0.0..=1.0).contains(&entry_time) {
        Some(SweepHit {
            time: entry_time,
            normal,
        })
    } else {
        None
    }
}

fn aabb_triangle_contact(
    centre: [f32; 3],
    half_extents: [f32; 3],
    triangle: Triangle,
) -> Option<Contact> {
    let half_extents = [
        half_extents[0].abs(),
        half_extents[1].abs(),
        half_extents[2].abs(),
    ];
    let [a, b, c] = triangle.vertices();
    let triangle_centroid = scale(add(add(a, b), c), 1.0 / 3.0);
    let mut shallowest: Option<Contact> = None;
    for axis in triangle_aabb_axes(a, b, c) {
        let axis_length = length(axis);
        if axis_length <= EPSILON {
            continue;
        }
        let axis = scale(axis, axis_length.recip());
        let (triangle_min, triangle_max) = triangle_projection(axis, a, b, c);
        let radius = half_extents[0] * axis[0].abs()
            + half_extents[1] * axis[1].abs()
            + half_extents[2] * axis[2].abs();
        let centre_projection = dot(centre, axis);
        let box_min = centre_projection - radius;
        let box_max = centre_projection + radius;
        let move_positive = triangle_max - box_min;
        let move_negative = box_max - triangle_min;
        if move_positive < -EPSILON || move_negative < -EPSILON {
            return None;
        }

        let (normal, penetration) = if (move_positive - move_negative).abs() <= EPSILON {
            if dot(sub(centre, triangle_centroid), axis) >= 0.0 {
                (axis, move_positive.max(0.0))
            } else {
                (scale(axis, -1.0), move_negative.max(0.0))
            }
        } else if move_positive < move_negative {
            (axis, move_positive.max(0.0))
        } else {
            (scale(axis, -1.0), move_negative.max(0.0))
        };
        let contact = Contact {
            normal,
            penetration,
        };
        if shallowest.is_none_or(|current| contact.penetration < current.penetration) {
            shallowest = Some(contact);
        }
    }
    shallowest
}

fn triangle_projection(axis: [f32; 3], a: [f32; 3], b: [f32; 3], c: [f32; 3]) -> (f32, f32) {
    let a = dot(axis, a);
    let b = dot(axis, b);
    let c = dot(axis, c);
    (a.min(b).min(c), a.max(b).max(c))
}

fn triangle_aabb_axes(a: [f32; 3], b: [f32; 3], c: [f32; 3]) -> [[f32; 3]; 13] {
    let edge_ab = sub(b, a);
    let edge_bc = sub(c, b);
    let edge_ca = sub(a, c);
    let x_axis = [1.0, 0.0, 0.0];
    let y_axis = [0.0, 1.0, 0.0];
    let z_axis = [0.0, 0.0, 1.0];
    [
        x_axis,
        y_axis,
        z_axis,
        cross(edge_ab, edge_bc),
        cross(edge_ab, x_axis),
        cross(edge_ab, y_axis),
        cross(edge_ab, z_axis),
        cross(edge_bc, x_axis),
        cross(edge_bc, y_axis),
        cross(edge_bc, z_axis),
        cross(edge_ca, x_axis),
        cross(edge_ca, y_axis),
        cross(edge_ca, z_axis),
    ]
}

#[cfg(test)]
mod tests {
    use citadel_map::CollisionMesh;

    use super::{PhysicsBody, Shape, StaticTriBvh, step};
    use crate::math::{dot, normalize_or, sub};

    fn floor_mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [-500.0, 0.0, -500.0],
                [500.0, 0.0, -500.0],
                [500.0, 0.0, 500.0],
                [-500.0, 0.0, 500.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    fn wall_mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [0.0, -100.0, -500.0],
                [0.0, 300.0, -500.0],
                [0.0, 300.0, 500.0],
                [0.0, -100.0, 500.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    fn slope_mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [-200.0, -50.0, -200.0],
                [200.0, 150.0, -200.0],
                [200.0, 150.0, 200.0],
                [-200.0, -50.0, 200.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    fn test_body(shape: Shape) -> PhysicsBody {
        PhysicsBody {
            shape,
            gravity: 0.0,
            buoyancy: 0.0,
            drag: 0.0,
            max_speed: 10_000.0,
            move_intent: [0.0; 3],
            grounded: false,
        }
    }

    fn test_shapes() -> [Shape; 2] {
        [
            Shape::Capsule {
                radius: 10.0,
                height: 40.0,
            },
            Shape::Aabb {
                half_extents: [10.0, 20.0, 10.0],
            },
        ]
    }

    fn resting_height(shape: Shape) -> f32 {
        shape.broadphase_half_extents()[1]
    }

    fn assert_close(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() <= 1.0e-4,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn capsule_and_aabb_rest_on_a_floor_quad() {
        let bvh = StaticTriBvh::new(&floor_mesh());
        for shape in test_shapes() {
            let mut body = test_body(shape);
            body.gravity = 100.0;
            let mut position = [0.0, resting_height(shape), 0.0];
            let mut velocity = [0.0; 3];

            step(&mut body, &mut position, &mut velocity, 0.1, Some(&bvh));

            assert!(body.grounded);
            assert_close(position[1], resting_height(shape));
            assert_close(velocity[1], 0.0);
        }
    }

    #[test]
    fn capsule_and_aabb_slide_along_a_wall() {
        let bvh = StaticTriBvh::new(&wall_mesh());
        for shape in test_shapes() {
            let mut body = test_body(shape);
            let half_extents = shape.broadphase_half_extents();
            let mut position = [-half_extents[0], resting_height(shape), 0.0];
            let mut velocity = [100.0, 0.0, 50.0];

            step(&mut body, &mut position, &mut velocity, 0.1, Some(&bvh));

            assert!(position[0] <= -half_extents[0]);
            assert!(position[2] > 4.9);
            assert_close(velocity[0], 0.0);
            assert_close(velocity[2], 50.0);
        }
    }

    #[test]
    fn capsule_and_aabb_stop_before_a_walkable_slope() {
        let mesh = slope_mesh();
        let bvh = StaticTriBvh::new(&mesh);
        let slope_normal = normalize_or([-200.0, 400.0, 0.0], [0.0, 1.0, 0.0]);
        let plane_point = mesh.vertices[0];

        for shape in test_shapes() {
            let mut body = test_body(shape);
            let mut position = [0.0, 220.0, 0.0];
            let mut velocity = [0.0, -2_000.0, 0.0];

            step(&mut body, &mut position, &mut velocity, 0.1, Some(&bvh));

            let support = match shape {
                Shape::Capsule { radius, height } => {
                    radius + (height * 0.5 - radius).max(0.0) * slope_normal[1].abs()
                }
                Shape::Aabb { half_extents } => {
                    half_extents[0] * slope_normal[0].abs()
                        + half_extents[1] * slope_normal[1].abs()
                        + half_extents[2] * slope_normal[2].abs()
                }
            };
            assert!(dot(sub(position, plane_point), slope_normal) >= support - 0.02);
            assert!(dot(velocity, slope_normal) >= -1.0e-3);
            assert!(body.grounded);
        }
    }

    #[test]
    fn gravity_buoyancy_drag_and_speed_clamp_have_expected_numeric_results() {
        let mut body = test_body(Shape::Aabb {
            half_extents: [1.0; 3],
        });
        body.gravity = 10.0;
        body.buoyancy = 4.0;
        body.drag = 0.5;
        body.max_speed = 1_000.0;
        let mut position = [0.0; 3];
        let mut velocity = [10.0, 20.0, -30.0];

        step(&mut body, &mut position, &mut velocity, 0.2, None);

        assert_eq!(position, [2.0, 4.0, -6.0]);
        assert_close(velocity[0], 9.0);
        assert_close(velocity[1], 16.92);
        assert_close(velocity[2], -27.0);

        body.drag = 0.0;
        body.gravity = 0.0;
        body.buoyancy = 0.0;
        body.max_speed = 10.0;
        velocity = [100.0, 0.0, 0.0];
        step(&mut body, &mut position, &mut velocity, 0.1, None);
        assert_close(velocity[0], 10.0);
    }

    #[test]
    fn horizontal_move_intent_blends_without_affecting_vertical_physics() {
        let mut body = test_body(Shape::Aabb {
            half_extents: [1.0; 3],
        });
        body.gravity = 10.0;
        body.move_intent = [100.0, 900.0, -50.0];
        let mut position = [0.0; 3];
        let mut velocity = [0.0; 3];

        step(&mut body, &mut position, &mut velocity, 1.0 / 60.0, None);

        assert_close(velocity[0], 20.0);
        assert_close(velocity[1], -1.0 / 6.0);
        assert_close(velocity[2], -10.0);
        assert_eq!(position, [0.0; 3]);
    }

    #[test]
    fn ground_probe_toggles_grounded_on_and_off_a_floor() {
        let bvh = StaticTriBvh::new(&floor_mesh());
        let shape = Shape::Capsule {
            radius: 10.0,
            height: 40.0,
        };
        let mut body = test_body(shape);
        let mut position = [0.0, resting_height(shape), 0.0];
        let mut velocity = [0.0; 3];

        step(
            &mut body,
            &mut position,
            &mut velocity,
            1.0 / 60.0,
            Some(&bvh),
        );
        assert!(body.grounded);

        position[1] = 100.0;
        step(
            &mut body,
            &mut position,
            &mut velocity,
            1.0 / 60.0,
            Some(&bvh),
        );
        assert!(!body.grounded);
    }

    #[test]
    fn deterministic_runs_are_bit_identical() {
        let bvh = StaticTriBvh::new(&floor_mesh());
        let run = |bvh: &StaticTriBvh| {
            let mut body = test_body(Shape::Capsule {
                radius: 10.0,
                height: 40.0,
            });
            body.gravity = 980.0;
            body.drag = 0.1;
            body.move_intent = [120.0, 0.0, -45.0];
            let mut position = [0.0, 20.0, 0.0];
            let mut velocity = [0.0; 3];
            for _ in 0..240 {
                step(
                    &mut body,
                    &mut position,
                    &mut velocity,
                    1.0 / 60.0,
                    Some(bvh),
                );
            }
            (position, velocity, body.grounded)
        };

        let first = run(&bvh);
        let second = run(&bvh);
        assert_eq!(first.0.map(f32::to_bits), second.0.map(f32::to_bits));
        assert_eq!(first.1.map(f32::to_bits), second.1.map(f32::to_bits));
        assert_eq!(first.2, second.2);
    }

    #[test]
    fn free_fall_without_a_bvh_matches_explicit_euler_analytic_motion() {
        let mut body = test_body(Shape::Aabb {
            half_extents: [1.0; 3],
        });
        body.gravity = 10.0;
        let mut position = [1.0, 2.0, 3.0];
        let mut velocity = [4.0, 5.0, 6.0];
        let dt = 0.25;
        let expected_position = [
            position[0] + velocity[0] * dt,
            position[1] + velocity[1] * dt,
            position[2] + velocity[2] * dt,
        ];
        let expected_velocity = [velocity[0], velocity[1] - body.gravity * dt, velocity[2]];

        step(&mut body, &mut position, &mut velocity, dt, None);

        assert_eq!(
            position.map(f32::to_bits),
            expected_position.map(f32::to_bits)
        );
        assert_eq!(
            velocity.map(f32::to_bits),
            expected_velocity.map(f32::to_bits)
        );
        assert!(!body.grounded);
    }
}
