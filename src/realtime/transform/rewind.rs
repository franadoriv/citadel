//! Server rewind / lag compensation (design §5.2, §7.2,  P2-a).
//!
//! Because remote players are rendered *in the past* and the shooter leads the
//! server, the server must **rewind** hit-eligible objects to the state the
//! shooter actually saw, run the hit test there, then restore ("favor the
//! shooter"). Two rules make this cheat-resistant (review §13.4):
//!
//! 1. **The server computes and clamps the rewind time from its own
//!    per-connection state** — a measured one-way delay (not naive RTT/2, which
//!    assumes a symmetric path), plus the client's stored effective interpolation
//!    delay, all bounded by a configured max-unlag clamp. The client's
//!    `last_seen_snapshot_id` is only a *hint*; a client-supplied timestamp is
//!    never trusted as authority.
//! 2. **Lag compensation disables above an RTT cutoff** (~200-220 ms): past that,
//!    rewinding too far punishes present-time targets, so the shot resolves at
//!    present state instead.
//!
//! Hit registration samples **server-side kinematic capsules (spheres here)**
//! from the [`RewindBuffer`], not per-bone animated hitboxes (design §5.2 scope,
//! §13.8) — do not market as AAA per-bone hitreg.

use std::collections::VecDeque;

use super::ObjectId;
use super::authority::TransformState;

/// A fixed-duration ring of timestamped transforms for one hit-eligible object
/// (design §7.2). Written every sim tick; sampled during a [`RewindHitTest`].
#[derive(Debug, Clone)]
pub struct RewindBuffer {
    ring: VecDeque<(u32, TransformState)>,
    capacity_ticks: usize,
}

impl RewindBuffer {
    /// A buffer holding `capacity_ticks` samples (e.g. ~1 s at the sim rate).
    #[must_use]
    pub fn new(capacity_ticks: usize) -> Self {
        Self {
            ring: VecDeque::new(),
            capacity_ticks: capacity_ticks.max(1),
        }
    }

    /// Record the object's state at `tick`, evicting the oldest past capacity.
    /// Out-of-order/duplicate ticks (`<= newest`) are ignored so the ring stays
    /// monotonic and interpolation brackets are well-formed.
    pub fn record(&mut self, tick: u32, state: TransformState) {
        if let Some(&(back_tick, _)) = self.ring.back()
            && tick <= back_tick
        {
            return;
        }
        self.ring.push_back((tick, state));
        while self.ring.len() > self.capacity_ticks {
            self.ring.pop_front();
        }
    }

    /// The newest recorded tick, if any.
    #[must_use]
    pub fn newest_tick(&self) -> Option<u32> {
        self.ring.back().map(|&(t, _)| t)
    }

    /// The oldest recorded tick, if any.
    #[must_use]
    pub fn oldest_tick(&self) -> Option<u32> {
        self.ring.front().map(|&(t, _)| t)
    }

    /// Number of recorded samples.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Whether the buffer has no samples.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Interpolate the object's transform at fractional `tick`. Positions lerp
    /// between the bracketing samples; before/after the ring clamps to the ends.
    /// Returns `None` only when the buffer is empty.
    #[must_use]
    pub fn sample_at(&self, tick: f64) -> Option<TransformState> {
        let front = *self.ring.front()?;
        let back = *self.ring.back()?;
        if tick <= front.0 as f64 {
            return Some(front.1);
        }
        if tick >= back.0 as f64 {
            return Some(back.1);
        }
        // Find the bracketing pair (rings are small; a linear scan is fine).
        let mut lo = front;
        let mut hi = back;
        for &(t, s) in &self.ring {
            if (t as f64) <= tick {
                lo = (t, s);
            }
            if (t as f64) >= tick {
                hi = (t, s);
                break;
            }
        }
        if hi.0 == lo.0 {
            return Some(lo.1);
        }
        let span = (hi.0 - lo.0) as f64;
        let f = ((tick - lo.0 as f64) / span).clamp(0.0, 1.0) as f32;
        let mut out = lo.1;
        for axis in 0..3 {
            out.position[axis] =
                lo.1.position[axis] + (hi.1.position[axis] - lo.1.position[axis]) * f;
            out.velocity[axis] =
                lo.1.velocity[axis] + (hi.1.velocity[axis] - lo.1.velocity[axis]) * f;
        }
        // Rotation is nearest-sample: hitboxes are spheres here, so orientation
        // does not affect the test; keep the closer sample's rotation.
        out.rotation = if f < 0.5 {
            lo.1.rotation
        } else {
            hi.1.rotation
        };
        Some(out)
    }
}

/// The measured per-connection latency the **server** uses to compute the rewind
/// time (design §5.2). All values are server-derived; nothing here is taken from
/// a client-supplied timestamp.
#[derive(Debug, Clone, Copy)]
pub struct LagProfile {
    /// Measured one-way delay (server->client), in **sim ticks** (asymmetric —
    /// not RTT/2). The server converts its RTT/OWD measurement to ticks.
    pub owd_ticks: f64,
    /// The client's stored effective interpolation delay, in sim ticks (varies
    /// with the adaptive send rate, §6.5).
    pub interp_delay_ticks: f64,
    /// Measured round-trip time in milliseconds (for the cutoff gate).
    pub rtt_ms: f64,
}

/// Static config for lag compensation (design §5.2).
#[derive(Debug, Clone, Copy)]
pub struct RewindConfig {
    /// Max ticks the server will rewind into the past (the max-unlag clamp).
    pub max_unlag_ticks: f64,
    /// RTT cutoff in ms above which lag compensation is disabled.
    pub rtt_cutoff_ms: f64,
    /// Default hit-sphere radius in cm for kinematic capsules (design §5.2 scope).
    pub hit_radius_cm: f32,
}

impl Default for RewindConfig {
    fn default() -> Self {
        // ~1 s max unlag at 60 Hz, a 220 ms cutoff (directional, §9.4), and a
        // 50 cm capsule radius as a reasonable avatar default.
        Self {
            max_unlag_ticks: 60.0,
            rtt_cutoff_ms: 220.0,
            hit_radius_cm: 50.0,
        }
    }
}

/// Whether lag compensation is enabled for `profile` under `config` (design §5.2:
/// disabled above the RTT cutoff).
#[must_use]
pub fn lag_comp_enabled(profile: &LagProfile, config: &RewindConfig) -> bool {
    profile.rtt_ms.is_finite() && profile.rtt_ms <= config.rtt_cutoff_ms
}

/// Compute the tick to rewind to for a shooter at `current_tick` (design §5.2).
///
/// `rewind = current − (owd + interp_delay)`, clamped so it never goes newer
/// than the present tick nor older than `current − max_unlag_ticks`. Entirely
/// server-derived: the client cannot push this value around.
#[must_use]
pub fn compute_rewind_tick(current_tick: u32, profile: &LagProfile, config: &RewindConfig) -> f64 {
    let current = f64::from(current_tick);
    let lag = profile.owd_ticks.max(0.0) + profile.interp_delay_ticks.max(0.0);
    let target = current - lag;
    let floor = current - config.max_unlag_ticks.max(0.0);
    target.clamp(floor, current)
}

/// A ray for a lag-compensated hit test (world space, cm).
#[derive(Debug, Clone, Copy)]
pub struct HitRay {
    /// Ray origin in cm.
    pub origin: [f32; 3],
    /// Ray direction (need not be normalized; normalized internally).
    pub direction: [f32; 3],
}

/// One hit-eligible target sampled at the rewound tick.
#[derive(Debug, Clone, Copy)]
pub struct HitTarget {
    /// The object id.
    pub object_id: ObjectId,
    /// The kinematic capsule center in cm at the rewound tick.
    pub center: [f32; 3],
    /// The capsule (sphere) radius in cm.
    pub radius: f32,
}

/// The nearest resolved hit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HitOutcome {
    /// The object hit.
    pub object_id: ObjectId,
    /// The impact point in cm.
    pub point: [f32; 3],
    /// Distance along the ray to the impact, in cm.
    pub distance: f32,
}

/// Resolve the nearest ray-vs-sphere hit among `targets` (favor-the-shooter). A
/// target whose sphere the ray does not intersect (or that is behind the origin)
/// is skipped; the closest forward intersection wins. Returns `None` on a miss.
#[must_use]
pub fn resolve_hit<I>(ray: &HitRay, targets: I) -> Option<HitOutcome>
where
    I: IntoIterator<Item = HitTarget>,
{
    let dir = normalize(ray.direction)?;
    let mut best: Option<HitOutcome> = None;
    for t in targets {
        if let Some(dist) = ray_sphere(ray.origin, dir, t.center, t.radius) {
            let closer = best.map(|b| dist < b.distance).unwrap_or(true);
            if closer {
                best = Some(HitOutcome {
                    object_id: t.object_id,
                    point: [
                        ray.origin[0] + dir[0] * dist,
                        ray.origin[1] + dir[1] * dist,
                        ray.origin[2] + dir[2] * dist,
                    ],
                    distance: dist,
                });
            }
        }
    }
    best
}

/// Nearest forward ray-sphere intersection distance, or `None` if the ray misses
/// or the sphere is entirely behind the origin.
fn ray_sphere(origin: [f32; 3], dir: [f32; 3], center: [f32; 3], radius: f32) -> Option<f32> {
    let oc = [
        origin[0] - center[0],
        origin[1] - center[1],
        origin[2] - center[2],
    ];
    let b = oc[0] * dir[0] + oc[1] * dir[1] + oc[2] * dir[2];
    let c = oc[0] * oc[0] + oc[1] * oc[1] + oc[2] * oc[2] - radius * radius;
    // Origin inside the sphere: a point-blank hit at distance 0.
    if c <= 0.0 {
        return Some(0.0);
    }
    let disc = b * b - c;
    if disc < 0.0 {
        return None; // no real intersection
    }
    let sqrt_disc = disc.sqrt();
    let t = -b - sqrt_disc; // nearest root
    if t >= 0.0 {
        Some(t)
    } else {
        None // both roots behind the origin
    }
}

fn normalize(v: [f32; 3]) -> Option<[f32; 3]> {
    let mag_sq = v[0] * v[0] + v[1] * v[1] + v[2] * v[2];
    if !mag_sq.is_finite() || mag_sq <= 1e-12 {
        return None;
    }
    let inv = 1.0 / mag_sq.sqrt();
    Some([v[0] * inv, v[1] * inv, v[2] * inv])
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn state_at(x: f32) -> TransformState {
        TransformState::at([x, 0.0, 0.0])
    }

    #[test]
    fn buffer_bounds_capacity_and_ignores_out_of_order() {
        let mut b = RewindBuffer::new(3);
        for t in 1..=5 {
            b.record(t, state_at(t as f32));
        }
        assert_eq!(b.len(), 3, "capacity bounds the ring");
        assert_eq!(b.oldest_tick(), Some(3));
        assert_eq!(b.newest_tick(), Some(5));
        // An out-of-order (older) tick is ignored.
        b.record(2, state_at(99.0));
        assert_eq!(b.newest_tick(), Some(5));
    }

    #[test]
    fn sample_interpolates_and_clamps() {
        let mut b = RewindBuffer::new(10);
        b.record(10, state_at(0.0));
        b.record(20, state_at(100.0));
        // Midway interpolates linearly.
        let mid = b.sample_at(15.0).unwrap();
        assert!((mid.position[0] - 50.0).abs() < 1e-3);
        // Before/after clamp to the ends.
        assert!((b.sample_at(5.0).unwrap().position[0] - 0.0).abs() < 1e-3);
        assert!((b.sample_at(999.0).unwrap().position[0] - 100.0).abs() < 1e-3);
    }

    #[test]
    fn rewind_tick_is_server_clamped() {
        let config = RewindConfig {
            max_unlag_ticks: 30.0,
            rtt_cutoff_ms: 220.0,
            hit_radius_cm: 50.0,
        };
        // Normal case: current 100, owd 6 + interp 5 -> 89.
        let p = LagProfile {
            owd_ticks: 6.0,
            interp_delay_ticks: 5.0,
            rtt_ms: 100.0,
        };
        assert!((compute_rewind_tick(100, &p, &config) - 89.0).abs() < 1e-6);
        // Excessive lag clamps at the max-unlag floor (100 - 30 = 70).
        let far = LagProfile {
            owd_ticks: 500.0,
            interp_delay_ticks: 500.0,
            rtt_ms: 200.0,
        };
        assert!((compute_rewind_tick(100, &far, &config) - 70.0).abs() < 1e-6);
    }

    #[test]
    fn rtt_cutoff_gates_lag_comp() {
        let config = RewindConfig::default();
        let ok = LagProfile {
            owd_ticks: 3.0,
            interp_delay_ticks: 3.0,
            rtt_ms: 150.0,
        };
        let bad = LagProfile {
            owd_ticks: 20.0,
            interp_delay_ticks: 10.0,
            rtt_ms: 300.0,
        };
        assert!(lag_comp_enabled(&ok, &config));
        assert!(!lag_comp_enabled(&bad, &config));
    }

    #[test]
    fn resolve_hit_picks_nearest_forward_sphere() {
        let ray = HitRay {
            origin: [0.0, 0.0, 0.0],
            direction: [1.0, 0.0, 0.0],
        };
        let targets = vec![
            HitTarget {
                object_id: 1,
                center: [100.0, 0.0, 0.0],
                radius: 10.0,
            },
            HitTarget {
                object_id: 2,
                center: [50.0, 0.0, 0.0],
                radius: 10.0,
            },
            // Behind the origin: must be ignored.
            HitTarget {
                object_id: 3,
                center: [-50.0, 0.0, 0.0],
                radius: 10.0,
            },
        ];
        let hit = resolve_hit(&ray, targets).expect("hit");
        assert_eq!(hit.object_id, 2, "nearest forward target");
        assert!((hit.distance - 40.0).abs() < 1e-3, "front of the sphere");
    }

    #[test]
    fn resolve_hit_misses_when_ray_off_axis() {
        let ray = HitRay {
            origin: [0.0, 0.0, 0.0],
            direction: [0.0, 1.0, 0.0],
        };
        let targets = vec![HitTarget {
            object_id: 1,
            center: [100.0, 0.0, 0.0],
            radius: 10.0,
        }];
        assert!(resolve_hit(&ray, targets).is_none());
    }

    #[test]
    fn favor_the_shooter_uses_rewound_position() {
        // A target moving on +x; the shooter fired when it was at x=100 (tick 10),
        // but by the present tick (20) it has moved to x=200. Rewinding to tick 10
        // must register the hit at the position the shooter saw.
        let mut b = RewindBuffer::new(30);
        b.record(10, state_at(100.0));
        b.record(20, state_at(200.0));
        let rewound = b.sample_at(10.0).unwrap();
        let ray = HitRay {
            origin: [0.0, 0.0, 0.0],
            direction: [1.0, 0.0, 0.0],
        };
        let hit = resolve_hit(
            &ray,
            [HitTarget {
                object_id: 1,
                center: rewound.position,
                radius: 10.0,
            }],
        )
        .expect("hit at rewound pos");
        assert!(
            (hit.distance - 90.0).abs() < 1e-3,
            "hit the past position (100 - r)"
        );
        // At the present position it would be a farther hit (200 - r = 190).
        let present = resolve_hit(
            &ray,
            [HitTarget {
                object_id: 1,
                center: [200.0, 0.0, 0.0],
                radius: 10.0,
            }],
        )
        .unwrap();
        assert!(present.distance > hit.distance);
    }
}
