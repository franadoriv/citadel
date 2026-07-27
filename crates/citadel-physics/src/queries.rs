//! Exact read-only collision queries over a [`StaticTriBvh`].

use crate::{StaticTriBvh, Triangle};

const EPSILON: f32 = 1.0e-5;

/// The nearest hit of a finite ray segment against a map triangle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RaycastHit {
    /// World-space contact point in centimetres.
    pub point: [f32; 3],
    /// Unit geometric triangle normal.
    pub normal: [f32; 3],
    /// Distance from the supplied origin in centimetres.
    pub distance: f32,
    /// Deterministic collision-triangle index.
    pub triangle_index: usize,
}

/// A downward ground hit, returned by [`ground_height`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GroundHit {
    /// World-space contact point in centimetres.
    pub point: [f32; 3],
    /// Unit geometric triangle normal.
    pub normal: [f32; 3],
    /// Vertical drop from the query point in centimetres.
    pub distance: f32,
    /// Deterministic collision-triangle index.
    pub triangle_index: usize,
}

/// Cast a finite segment from `origin` along `direction` against the collision
/// mesh. `direction` is a displacement in centimetres, not a normalized vector.
#[must_use]
pub fn raycast(bvh: &StaticTriBvh, origin: [f32; 3], direction: [f32; 3]) -> Option<RaycastHit> {
    if !origin.into_iter().chain(direction).all(f32::is_finite) {
        return None;
    }
    let length = length(direction);
    if length <= EPSILON {
        return None;
    }
    let mut nearest: Option<(f32, usize, [f32; 3])> = None;
    for index in bvh.query_ray(origin, direction) {
        let Some(triangle) = bvh.triangle(index).copied() else {
            continue;
        };
        let Some(time) = ray_triangle_time(origin, direction, triangle) else {
            continue;
        };
        if nearest.is_none_or(|(best, best_index, _)| {
            time < best - EPSILON || (time - best).abs() <= EPSILON && index < best_index
        }) {
            nearest = Some((time, index, triangle_normal(triangle)));
        }
    }
    nearest.map(|(time, triangle_index, normal)| RaycastHit {
        point: add(origin, scale(direction, time)),
        normal,
        distance: length * time,
        triangle_index,
    })
}

/// Return `true` when a sphere overlaps any collision triangle.
#[must_use]
pub fn sphere_overlap(bvh: &StaticTriBvh, centre: [f32; 3], radius: f32) -> bool {
    if !centre.into_iter().all(f32::is_finite) || !radius.is_finite() || radius < 0.0 {
        return false;
    }
    let extent = [radius; 3];
    bvh.query_swept_aabb(centre, [0.0; 3], extent)
        .into_iter()
        .any(|index| {
            bvh.triangle(index).is_some_and(|triangle| {
                length_squared(sub(centre, closest_point(centre, *triangle))) <= radius * radius
            })
        })
}

/// Find the nearest walkable hit directly below `origin`, up to `max_distance`.
#[must_use]
pub fn ground_height(bvh: &StaticTriBvh, origin: [f32; 3], max_distance: f32) -> Option<GroundHit> {
    if !max_distance.is_finite() || max_distance < 0.0 {
        return None;
    }
    let hit = raycast(bvh, origin, [0.0, -max_distance, 0.0])?;
    let normal = if hit.normal[1] < 0.0 {
        scale(hit.normal, -1.0)
    } else {
        hit.normal
    };
    (normal[1] > 0.0).then_some(GroundHit {
        point: hit.point,
        normal,
        distance: hit.distance,
        triangle_index: hit.triangle_index,
    })
}

fn ray_triangle_time(origin: [f32; 3], direction: [f32; 3], triangle: Triangle) -> Option<f32> {
    let [a, b, c] = triangle.vertices();
    let ab = sub(b, a);
    let ac = sub(c, a);
    let p = cross(direction, ac);
    let determinant = dot(ab, p);
    if determinant.abs() <= EPSILON {
        return None;
    }
    let inverse = determinant.recip();
    let offset = sub(origin, a);
    let u = dot(offset, p) * inverse;
    if !(0.0..=1.0).contains(&u) {
        return None;
    }
    let v = dot(direction, cross(offset, ab)) * inverse;
    if v < 0.0 || u + v > 1.0 {
        return None;
    }
    let time = dot(ac, cross(offset, ab)) * inverse;
    (0.0..=1.0).contains(&time).then_some(time)
}

fn triangle_normal(triangle: Triangle) -> [f32; 3] {
    let [a, b, c] = triangle.vertices();
    normalize(cross(sub(b, a), sub(c, a)))
}
fn closest_point(point: [f32; 3], triangle: Triangle) -> [f32; 3] {
    let [a, b, c] = triangle.vertices();
    let ab = sub(b, a);
    let ac = sub(c, a);
    let ap = sub(point, a);
    let d1 = dot(ab, ap);
    let d2 = dot(ac, ap);
    if d1 <= 0.0 && d2 <= 0.0 {
        return a;
    }
    let bp = sub(point, b);
    let d3 = dot(ab, bp);
    let d4 = dot(ac, bp);
    if d3 >= 0.0 && d4 <= d3 {
        return b;
    }
    let vc = d1 * d4 - d3 * d2;
    if vc <= 0.0 && d1 >= 0.0 && d3 <= 0.0 {
        return add(a, scale(ab, d1 / (d1 - d3)));
    }
    let cp = sub(point, c);
    let d5 = dot(ab, cp);
    let d6 = dot(ac, cp);
    if d6 >= 0.0 && d5 <= d6 {
        return c;
    }
    let vb = d5 * d2 - d1 * d6;
    if vb <= 0.0 && d2 >= 0.0 && d6 <= 0.0 {
        return add(a, scale(ac, d2 / (d2 - d6)));
    }
    let va = d3 * d6 - d5 * d4;
    if va <= 0.0 && (d4 - d3) >= 0.0 && (d5 - d6) >= 0.0 {
        return add(b, scale(sub(c, b), (d4 - d3) / ((d4 - d3) + (d5 - d6))));
    }
    let denom = va + vb + vc;
    if denom.abs() <= EPSILON {
        a
    } else {
        add(a, add(scale(ab, vb / denom), scale(ac, vc / denom)))
    }
}
fn add(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}
fn sub(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}
fn scale(a: [f32; 3], s: f32) -> [f32; 3] {
    [a[0] * s, a[1] * s, a[2] * s]
}
fn dot(a: [f32; 3], b: [f32; 3]) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn cross(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}
fn length_squared(a: [f32; 3]) -> f32 {
    dot(a, a)
}
fn length(a: [f32; 3]) -> f32 {
    length_squared(a).sqrt()
}
fn normalize(a: [f32; 3]) -> [f32; 3] {
    let l = length(a);
    if l > EPSILON {
        scale(a, l.recip())
    } else {
        [0.0; 3]
    }
}

#[cfg(test)]
mod tests {
    use citadel_map::CollisionMesh;

    use super::{ground_height, raycast, sphere_overlap};
    use crate::StaticTriBvh;

    fn floor() -> StaticTriBvh {
        StaticTriBvh::new(&CollisionMesh {
            vertices: vec![
                [-100.0, 0.0, -100.0],
                [100.0, 0.0, -100.0],
                [-100.0, 0.0, 100.0],
                [100.0, 0.0, 100.0],
            ],
            triangles: vec![[0, 1, 2], [2, 1, 3]],
        })
    }

    #[test]
    fn raycast_returns_nearest_map_hit() {
        let hit = raycast(&floor(), [0.0, 50.0, 0.0], [0.0, -100.0, 0.0]).expect("floor is hit");
        assert_eq!(hit.point, [0.0, 0.0, 0.0]);
        assert_eq!(hit.distance, 50.0);
        assert!(hit.normal[1].abs() > 0.99);
    }

    #[test]
    fn sphere_overlap_and_ground_height_respect_the_mesh() {
        let bvh = floor();
        assert!(sphere_overlap(&bvh, [0.0, 2.0, 0.0], 2.0));
        assert!(!sphere_overlap(&bvh, [0.0, 2.1, 0.0], 2.0));
        let ground = ground_height(&bvh, [0.0, 75.0, 0.0], 100.0).expect("ground hit");
        assert_eq!(ground.point, [0.0, 0.0, 0.0]);
        assert_eq!(ground.distance, 75.0);
    }
}
