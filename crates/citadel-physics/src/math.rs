//! Small, explicit `f32` vector helpers used by the deterministic controller.

pub(crate) const EPSILON: f32 = 1.0e-5;

#[must_use]
pub(crate) fn add(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

#[must_use]
pub(crate) fn sub(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}

#[must_use]
pub(crate) fn scale(value: [f32; 3], scalar: f32) -> [f32; 3] {
    [value[0] * scalar, value[1] * scalar, value[2] * scalar]
}

pub(crate) fn add_assign(target: &mut [f32; 3], delta: [f32; 3]) {
    target[0] += delta[0];
    target[1] += delta[1];
    target[2] += delta[2];
}

#[must_use]
pub(crate) fn dot(a: [f32; 3], b: [f32; 3]) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

#[must_use]
pub(crate) fn cross(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

#[must_use]
pub(crate) fn length_squared(value: [f32; 3]) -> f32 {
    dot(value, value)
}

#[must_use]
pub(crate) fn length(value: [f32; 3]) -> f32 {
    length_squared(value).sqrt()
}

#[must_use]
pub(crate) fn normalize_or(value: [f32; 3], fallback: [f32; 3]) -> [f32; 3] {
    let magnitude = length(value);
    if magnitude > EPSILON {
        scale(value, magnitude.recip())
    } else {
        fallback
    }
}

#[must_use]
pub(crate) fn component_min(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0].min(b[0]), a[1].min(b[1]), a[2].min(b[2])]
}

#[must_use]
pub(crate) fn component_max(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0].max(b[0]), a[1].max(b[1]), a[2].max(b[2])]
}

#[must_use]
pub(crate) fn closest_point_on_segment(point: [f32; 3], a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    let ab = sub(b, a);
    let denominator = length_squared(ab);
    if denominator <= EPSILON {
        return a;
    }

    let factor = (dot(sub(point, a), ab) / denominator).clamp(0.0, 1.0);
    add(a, scale(ab, factor))
}

/// Return the closest point on a triangle, including its edges and vertices.
#[must_use]
pub(crate) fn closest_point_on_triangle(
    point: [f32; 3],
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
) -> [f32; 3] {
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
        let factor = d1 / (d1 - d3);
        return add(a, scale(ab, factor));
    }

    let cp = sub(point, c);
    let d5 = dot(ab, cp);
    let d6 = dot(ac, cp);
    if d6 >= 0.0 && d5 <= d6 {
        return c;
    }

    let vb = d5 * d2 - d1 * d6;
    if vb <= 0.0 && d2 >= 0.0 && d6 <= 0.0 {
        let factor = d2 / (d2 - d6);
        return add(a, scale(ac, factor));
    }

    let va = d3 * d6 - d5 * d4;
    if va <= 0.0 && (d4 - d3) >= 0.0 && (d5 - d6) >= 0.0 {
        let bc = sub(c, b);
        let factor = (d4 - d3) / ((d4 - d3) + (d5 - d6));
        return add(b, scale(bc, factor));
    }

    let denominator = va + vb + vc;
    if denominator.abs() <= EPSILON {
        let ab_point = closest_point_on_segment(point, a, b);
        let bc_point = closest_point_on_segment(point, b, c);
        let ca_point = closest_point_on_segment(point, c, a);
        let ab_distance = length_squared(sub(point, ab_point));
        let bc_distance = length_squared(sub(point, bc_point));
        let ca_distance = length_squared(sub(point, ca_point));
        if ab_distance <= bc_distance && ab_distance <= ca_distance {
            ab_point
        } else if bc_distance <= ca_distance {
            bc_point
        } else {
            ca_point
        }
    } else {
        let inverse = denominator.recip();
        let v = vb * inverse;
        let w = vc * inverse;
        add(a, add(scale(ab, v), scale(ac, w)))
    }
}

/// Return the closest points between a segment and a triangle.
///
/// The returned pair is `(point_on_segment, point_on_triangle)`. It handles
/// face, edge, and vertex features without allocating, and uses a stable
/// candidate order for equal distances.
#[must_use]
pub(crate) fn closest_points_segment_triangle(
    segment_start: [f32; 3],
    segment_end: [f32; 3],
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
) -> ([f32; 3], [f32; 3]) {
    if let Some(intersection) = segment_triangle_intersection(segment_start, segment_end, a, b, c) {
        return (intersection, intersection);
    }

    let first_triangle_point = closest_point_on_triangle(segment_start, a, b, c);
    let mut best_pair = (segment_start, first_triangle_point);
    let mut best_distance = length_squared(sub(segment_start, first_triangle_point));

    let mut consider = |segment_point: [f32; 3], triangle_point: [f32; 3]| {
        let distance = length_squared(sub(segment_point, triangle_point));
        if distance < best_distance {
            best_pair = (segment_point, triangle_point);
            best_distance = distance;
        }
    };

    let end_triangle_point = closest_point_on_triangle(segment_end, a, b, c);
    consider(segment_end, end_triangle_point);

    for vertex in [a, b, c] {
        consider(
            closest_point_on_segment(vertex, segment_start, segment_end),
            vertex,
        );
    }
    for [edge_start, edge_end] in [[a, b], [b, c], [c, a]] {
        let pair = closest_points_on_segments(segment_start, segment_end, edge_start, edge_end);
        consider(pair.0, pair.1);
    }

    best_pair
}

fn segment_triangle_intersection(
    segment_start: [f32; 3],
    segment_end: [f32; 3],
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
) -> Option<[f32; 3]> {
    let direction = sub(segment_end, segment_start);
    let edge_ab = sub(b, a);
    let edge_ac = sub(c, a);
    let p = cross(direction, edge_ac);
    let determinant = dot(edge_ab, p);
    if determinant.abs() <= EPSILON {
        return None;
    }

    let inverse = determinant.recip();
    let offset = sub(segment_start, a);
    let u = dot(offset, p) * inverse;
    if !(0.0..=1.0).contains(&u) {
        return None;
    }
    let q = cross(offset, edge_ab);
    let v = dot(direction, q) * inverse;
    if v < 0.0 || u + v > 1.0 {
        return None;
    }
    let time = dot(edge_ac, q) * inverse;
    if (0.0..=1.0).contains(&time) {
        Some(add(segment_start, scale(direction, time)))
    } else {
        None
    }
}

fn closest_points_on_segments(
    first_start: [f32; 3],
    first_end: [f32; 3],
    second_start: [f32; 3],
    second_end: [f32; 3],
) -> ([f32; 3], [f32; 3]) {
    let first_direction = sub(first_end, first_start);
    let second_direction = sub(second_end, second_start);
    let start_delta = sub(first_start, second_start);
    let first_length = dot(first_direction, first_direction);
    let second_length = dot(second_direction, second_direction);
    let second_offset = dot(second_direction, start_delta);

    let (first_t, second_t) = if first_length <= EPSILON && second_length <= EPSILON {
        (0.0, 0.0)
    } else if first_length <= EPSILON {
        (0.0, (second_offset / second_length).clamp(0.0, 1.0))
    } else {
        let first_offset = dot(first_direction, start_delta);
        if second_length <= EPSILON {
            ((-first_offset / first_length).clamp(0.0, 1.0), 0.0)
        } else {
            let direction_dot = dot(first_direction, second_direction);
            let denominator = first_length * second_length - direction_dot * direction_dot;
            let mut first_t = if denominator.abs() > EPSILON {
                ((direction_dot * second_offset - first_offset * second_length) / denominator)
                    .clamp(0.0, 1.0)
            } else {
                0.0
            };
            let unclamped_second = direction_dot * first_t + second_offset;
            let second_t = if unclamped_second <= 0.0 {
                first_t = (-first_offset / first_length).clamp(0.0, 1.0);
                0.0
            } else if unclamped_second >= second_length {
                first_t = ((direction_dot - first_offset) / first_length).clamp(0.0, 1.0);
                1.0
            } else {
                unclamped_second / second_length
            };
            (first_t, second_t)
        }
    };

    (
        add(first_start, scale(first_direction, first_t)),
        add(second_start, scale(second_direction, second_t)),
    )
}
