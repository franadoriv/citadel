//! Deterministic broadphase over a static collision mesh.

use citadel_map::CollisionMesh;

use crate::math::{component_max, component_min, sub};

const LEAF_TRIANGLE_COUNT: usize = 4;

/// One world-space collision triangle in centimetres.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Triangle {
    vertices: [[f32; 3]; 3],
}

impl Triangle {
    /// Construct a triangle from its three world-space vertices in centimetres.
    #[must_use]
    pub const fn new(vertices: [[f32; 3]; 3]) -> Self {
        Self { vertices }
    }

    /// Return the three world-space vertices in centimetres.
    #[must_use]
    pub const fn vertices(self) -> [[f32; 3]; 3] {
        self.vertices
    }

    pub(crate) fn bounds(self) -> Bounds {
        Bounds::from_points(&self.vertices)
    }

    pub(crate) fn centroid(self) -> [f32; 3] {
        [
            (self.vertices[0][0] + self.vertices[1][0] + self.vertices[2][0]) / 3.0,
            (self.vertices[0][1] + self.vertices[1][1] + self.vertices[2][1]) / 3.0,
            (self.vertices[0][2] + self.vertices[1][2] + self.vertices[2][2]) / 3.0,
        ]
    }
}

/// A static, median-split AABB BVH built once from a collision mesh.
///
/// Construction preserves mesh triangle order as the final tie-breaker, and
/// all query methods return ascending triangle indices. Invalid mesh indices
/// and non-finite vertices are ignored rather than causing a panic.
#[derive(Debug, Clone)]
pub struct StaticTriBvh {
    triangles: Vec<Triangle>,
    ordered_indices: Vec<usize>,
    nodes: Vec<Node>,
    root: Option<usize>,
}

impl StaticTriBvh {
    /// Build a static broadphase from a map collision mesh.
    #[must_use]
    pub fn new(mesh: &CollisionMesh) -> Self {
        let mut triangles = Vec::with_capacity(mesh.triangles.len());
        for triangle in &mesh.triangles {
            let Some(&a) = mesh.vertices.get(triangle[0] as usize) else {
                continue;
            };
            let Some(&b) = mesh.vertices.get(triangle[1] as usize) else {
                continue;
            };
            let Some(&c) = mesh.vertices.get(triangle[2] as usize) else {
                continue;
            };
            if [a, b, c].into_iter().flatten().all(f32::is_finite) {
                triangles.push(Triangle::new([a, b, c]));
            }
        }

        let mut ordered_indices: Vec<_> = (0..triangles.len()).collect();
        let mut nodes = Vec::new();
        let root = if ordered_indices.is_empty() {
            None
        } else {
            Some(build_node(
                &triangles,
                &mut ordered_indices,
                &mut nodes,
                0,
                triangles.len(),
            ))
        };

        Self {
            triangles,
            ordered_indices,
            nodes,
            root,
        }
    }

    /// Alias for [`StaticTriBvh::new`] that makes the source type explicit.
    #[must_use]
    pub fn from_collision_mesh(mesh: &CollisionMesh) -> Self {
        Self::new(mesh)
    }

    /// Return the number of valid triangles in this broadphase.
    #[must_use]
    pub fn triangle_count(&self) -> usize {
        self.triangles.len()
    }

    /// Return whether this broadphase contains no valid triangles.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.triangles.is_empty()
    }

    /// Look up a triangle by its deterministic query index.
    #[must_use]
    pub fn triangle(&self, index: usize) -> Option<&Triangle> {
        self.triangles.get(index)
    }

    /// Return candidate triangle indices whose AABBs overlap the supplied AABB.
    ///
    /// `min` and `max` are in centimetres. Their component order is normalized,
    /// so callers may safely pass endpoints in either order.
    #[must_use]
    pub fn query_aabb(&self, min: [f32; 3], max: [f32; 3]) -> Vec<usize> {
        self.query_bounds(Bounds::new(
            component_min(min, max),
            component_max(min, max),
        ))
    }

    /// Return candidate triangle indices for an AABB swept over one movement.
    ///
    /// `start` is the shape centre in centimetres, `displacement` is its full
    /// movement in centimetres (not a velocity), and `half_extents` describes
    /// the swept box. The method is a broadphase: callers still perform their
    /// narrowphase test against every returned triangle.
    #[must_use]
    pub fn query_swept_aabb(
        &self,
        start: [f32; 3],
        displacement: [f32; 3],
        half_extents: [f32; 3],
    ) -> Vec<usize> {
        let end = [
            start[0] + displacement[0],
            start[1] + displacement[1],
            start[2] + displacement[2],
        ];
        let extents = [
            half_extents[0].abs(),
            half_extents[1].abs(),
            half_extents[2].abs(),
        ];
        let min = [
            start[0].min(end[0]) - extents[0],
            start[1].min(end[1]) - extents[1],
            start[2].min(end[2]) - extents[2],
        ];
        let max = [
            start[0].max(end[0]) + extents[0],
            start[1].max(end[1]) + extents[1],
            start[2].max(end[2]) + extents[2],
        ];
        self.query_bounds(Bounds::new(min, max))
    }

    /// Return broadphase candidates for a finite ray segment.
    ///
    /// `direction` is the segment displacement, so the queried ray is
    /// `origin + direction * t` for `0 <= t <= 1`. This conservative query is
    /// useful for deterministic ray/narrowphase callers without exposing BVH
    /// internals.
    #[must_use]
    pub fn query_ray(&self, origin: [f32; 3], direction: [f32; 3]) -> Vec<usize> {
        self.query_swept_aabb(origin, direction, [0.0; 3])
    }

    /// Return broadphase candidates for a finite segment from `start` to `end`.
    #[must_use]
    pub fn query_ray_segment(&self, start: [f32; 3], end: [f32; 3]) -> Vec<usize> {
        self.query_ray(start, sub(end, start))
    }

    fn query_bounds(&self, query: Bounds) -> Vec<usize> {
        let Some(root) = self.root else {
            return Vec::new();
        };

        let mut candidates = Vec::new();
        let mut stack = vec![root];
        while let Some(node_index) = stack.pop() {
            let Some(node) = self.nodes.get(node_index) else {
                continue;
            };
            if !node.bounds.overlaps(query) {
                continue;
            }
            match node.kind {
                NodeKind::Leaf { start, end } => {
                    for ordered_index in start..end {
                        if let Some(&triangle_index) = self.ordered_indices.get(ordered_index)
                            && self
                                .triangles
                                .get(triangle_index)
                                .is_some_and(|triangle| triangle.bounds().overlaps(query))
                        {
                            candidates.push(triangle_index);
                        }
                    }
                }
                NodeKind::Branch { left, right } => {
                    // LIFO stack: push right first to traverse left before right.
                    stack.push(right);
                    stack.push(left);
                }
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        candidates
    }
}

impl From<&CollisionMesh> for StaticTriBvh {
    fn from(mesh: &CollisionMesh) -> Self {
        Self::new(mesh)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    pub(crate) min: [f32; 3],
    pub(crate) max: [f32; 3],
}

impl Bounds {
    pub(crate) const fn new(min: [f32; 3], max: [f32; 3]) -> Self {
        Self { min, max }
    }

    fn from_points(points: &[[f32; 3]; 3]) -> Self {
        let min = component_min(component_min(points[0], points[1]), points[2]);
        let max = component_max(component_max(points[0], points[1]), points[2]);
        Self { min, max }
    }

    fn union(self, other: Self) -> Self {
        Self {
            min: component_min(self.min, other.min),
            max: component_max(self.max, other.max),
        }
    }

    fn overlaps(self, other: Self) -> bool {
        self.min[0] <= other.max[0]
            && self.max[0] >= other.min[0]
            && self.min[1] <= other.max[1]
            && self.max[1] >= other.min[1]
            && self.min[2] <= other.max[2]
            && self.max[2] >= other.min[2]
    }
}

#[derive(Debug, Clone, Copy)]
struct Node {
    bounds: Bounds,
    kind: NodeKind,
}

#[derive(Debug, Clone, Copy)]
enum NodeKind {
    Leaf { start: usize, end: usize },
    Branch { left: usize, right: usize },
}

fn build_node(
    triangles: &[Triangle],
    ordered_indices: &mut [usize],
    nodes: &mut Vec<Node>,
    start: usize,
    end: usize,
) -> usize {
    let bounds = range_bounds(triangles, ordered_indices, start, end);
    let node_index = nodes.len();
    nodes.push(Node {
        bounds,
        kind: NodeKind::Leaf { start, end },
    });

    if end - start <= LEAF_TRIANGLE_COUNT {
        return node_index;
    }

    let centroid_bounds = range_centroid_bounds(triangles, ordered_indices, start, end);
    let axis = longest_axis(sub(centroid_bounds.max, centroid_bounds.min));
    ordered_indices[start..end].sort_by(|left, right| {
        triangles[*left].centroid()[axis]
            .total_cmp(&triangles[*right].centroid()[axis])
            .then_with(|| left.cmp(right))
    });

    let middle = start + (end - start) / 2;
    let left = build_node(triangles, ordered_indices, nodes, start, middle);
    let right = build_node(triangles, ordered_indices, nodes, middle, end);
    if let Some(node) = nodes.get_mut(node_index) {
        node.kind = NodeKind::Branch { left, right };
    }
    node_index
}

fn range_bounds(
    triangles: &[Triangle],
    ordered_indices: &[usize],
    start: usize,
    end: usize,
) -> Bounds {
    let mut bounds = Bounds::new([f32::INFINITY; 3], [f32::NEG_INFINITY; 3]);
    for ordered_index in start..end {
        let Some(&triangle_index) = ordered_indices.get(ordered_index) else {
            continue;
        };
        let Some(triangle) = triangles.get(triangle_index).copied() else {
            continue;
        };
        bounds = if bounds.min[0].is_infinite() {
            triangle.bounds()
        } else {
            bounds.union(triangle.bounds())
        };
    }
    bounds
}

fn range_centroid_bounds(
    triangles: &[Triangle],
    ordered_indices: &[usize],
    start: usize,
    end: usize,
) -> Bounds {
    let mut bounds = Bounds::new([f32::INFINITY; 3], [f32::NEG_INFINITY; 3]);
    for ordered_index in start..end {
        let Some(&triangle_index) = ordered_indices.get(ordered_index) else {
            continue;
        };
        let Some(triangle) = triangles.get(triangle_index).copied() else {
            continue;
        };
        let centroid = triangle.centroid();
        let centroid_bounds = Bounds::new(centroid, centroid);
        bounds = if bounds.min[0].is_infinite() {
            centroid_bounds
        } else {
            bounds.union(centroid_bounds)
        };
    }
    bounds
}

fn longest_axis(extent: [f32; 3]) -> usize {
    if extent[1] > extent[0] && extent[1] >= extent[2] {
        1
    } else if extent[2] > extent[0] && extent[2] > extent[1] {
        2
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use citadel_map::CollisionMesh;

    use super::{Bounds, StaticTriBvh};

    fn mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [-100.0, 0.0, -100.0],
                [0.0, 0.0, -100.0],
                [0.0, 0.0, 0.0],
                [-100.0, 0.0, 0.0],
                [20.0, 10.0, 20.0],
                [80.0, 10.0, 20.0],
                [80.0, 10.0, 80.0],
                [20.0, 10.0, 80.0],
                [-30.0, 0.0, 20.0],
                [-30.0, 90.0, 20.0],
                [-30.0, 0.0, 90.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3], [4, 5, 6], [4, 6, 7], [8, 9, 10]],
        }
    }

    fn brute_force(bvh: &StaticTriBvh, bounds: Bounds) -> Vec<usize> {
        let mut candidates = Vec::new();
        for index in 0..bvh.triangle_count() {
            let Some(triangle) = bvh.triangle(index) else {
                continue;
            };
            if triangle.bounds().overlaps(bounds) {
                candidates.push(index);
            }
        }
        candidates
    }

    fn next_random(state: &mut u32) -> f32 {
        *state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let normalized = (*state >> 8) as f32 / ((u32::MAX >> 8) as f32);
        normalized * 260.0 - 130.0
    }

    #[test]
    fn swept_aabb_candidates_match_brute_force_for_deterministic_random_inputs() {
        let bvh = StaticTriBvh::new(&mesh());
        let mut state = 0xC17A_DE1D;

        for _ in 0..128 {
            let start = [
                next_random(&mut state),
                next_random(&mut state),
                next_random(&mut state),
            ];
            let displacement = [
                next_random(&mut state),
                next_random(&mut state),
                next_random(&mut state),
            ];
            let extents = [
                next_random(&mut state).abs() * 0.1,
                next_random(&mut state).abs() * 0.1,
                next_random(&mut state).abs() * 0.1,
            ];
            let end = [
                start[0] + displacement[0],
                start[1] + displacement[1],
                start[2] + displacement[2],
            ];
            let bounds = Bounds::new(
                [
                    start[0].min(end[0]) - extents[0],
                    start[1].min(end[1]) - extents[1],
                    start[2].min(end[2]) - extents[2],
                ],
                [
                    start[0].max(end[0]) + extents[0],
                    start[1].max(end[1]) + extents[1],
                    start[2].max(end[2]) + extents[2],
                ],
            );
            assert_eq!(
                bvh.query_swept_aabb(start, displacement, extents),
                brute_force(&bvh, bounds)
            );
        }
    }

    #[test]
    fn ray_candidates_match_brute_force_for_deterministic_random_inputs() {
        let bvh = StaticTriBvh::new(&mesh());
        let mut state = 0xB4D_5EED;

        for _ in 0..128 {
            let origin = [
                next_random(&mut state),
                next_random(&mut state),
                next_random(&mut state),
            ];
            let direction = [
                next_random(&mut state),
                next_random(&mut state),
                next_random(&mut state),
            ];
            let end = [
                origin[0] + direction[0],
                origin[1] + direction[1],
                origin[2] + direction[2],
            ];
            let bounds = Bounds::new(
                [
                    origin[0].min(end[0]),
                    origin[1].min(end[1]),
                    origin[2].min(end[2]),
                ],
                [
                    origin[0].max(end[0]),
                    origin[1].max(end[1]),
                    origin[2].max(end[2]),
                ],
            );
            assert_eq!(bvh.query_ray(origin, direction), brute_force(&bvh, bounds));
        }
    }

    #[test]
    fn invalid_mesh_indices_are_ignored_without_panicking() {
        let mesh = CollisionMesh {
            vertices: vec![[0.0, 0.0, 0.0]],
            triangles: vec![[0, 1, 2]],
        };
        assert!(StaticTriBvh::new(&mesh).is_empty());
    }
}
