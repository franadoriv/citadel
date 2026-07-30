//! Deterministic local copy of `../server/game/map.lua`.
//!
//! The bot avoids known walls locally to create realistic traffic, but the Lua
//! server is still authoritative and checks every final position and segment.

#[derive(Debug, Clone, Copy)]
pub struct Obstacle {
    pub x: f32,
    pub z: f32,
    pub hx: f32,
    pub hz: f32,
}

#[derive(Debug, Clone)]
pub struct Map {
    pub half_extent: f32,
    pub player_radius: f32,
    obstacles: Vec<Obstacle>,
}

impl Default for Map {
    fn default() -> Self {
        let mut obstacles = Vec::with_capacity(80);
        for gx in -4_i32..=4 {
            for gz in -4_i32..=4 {
                if gx == 0 && gz == 0 {
                    continue;
                }
                let jitter_x = (((gz + 4) % 3) - 1) as f32 * 24.0;
                let jitter_z = (((gx + 4) % 3) - 1) as f32 * 18.0;
                obstacles.push(Obstacle {
                    x: gx as f32 * 190.0 + jitter_x,
                    z: gz as f32 * 190.0 + jitter_z,
                    hx: 18.0 + ((gx + 4) % 3) as f32 * 6.0,
                    hz: 22.0 + ((gz + 4) % 3) as f32 * 5.0,
                });
            }
        }
        Self {
            half_extent: 1000.0,
            player_radius: 4.0,
            obstacles,
        }
    }
}

impl Map {
    pub fn is_free(&self, x: f32, z: f32) -> bool {
        let r = self.player_radius;
        if x - r < -self.half_extent
            || x + r > self.half_extent
            || z - r < -self.half_extent
            || z + r > self.half_extent
        {
            return false;
        }
        !self.obstacles.iter().any(|o| {
            x >= o.x - o.hx - r && x <= o.x + o.hx + r && z >= o.z - o.hz - r && z <= o.z + o.hz + r
        })
    }

    pub fn segment_is_free(&self, x0: f32, z0: f32, x1: f32, z1: f32) -> bool {
        if !self.is_free(x1, z1) {
            return false;
        }
        let r = self.player_radius;
        !self.obstacles.iter().any(|o| {
            segment_hits_box(
                [x0, z0],
                [x1, z1],
                [
                    o.x - o.hx - r,
                    o.x + o.hx + r,
                    o.z - o.hz - r,
                    o.z + o.hz + r,
                ],
            )
        })
    }
}

fn segment_hits_box(start: [f32; 2], end: [f32; 2], bounds: [f32; 4]) -> bool {
    let [x0, z0] = start;
    let [x1, z1] = end;
    let [min_x, max_x, min_z, max_z] = bounds;
    let mut t_min = 0.0_f32;
    let mut t_max = 1.0_f32;
    for (start, delta, low, high) in [(x0, x1 - x0, min_x, max_x), (z0, z1 - z0, min_z, max_z)] {
        if delta.abs() < 0.000_01 {
            if start < low || start > high {
                return false;
            }
            continue;
        }
        let mut a = (low - start) / delta;
        let mut b = (high - start) / delta;
        if a > b {
            std::mem::swap(&mut a, &mut b);
        }
        t_min = t_min.max(a);
        t_max = t_max.min(b);
        if t_min > t_max {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::Map;

    #[test]
    fn generated_map_has_the_same_eighty_obstacles_as_lua() {
        let map = Map::default();
        assert_eq!(map.obstacles.len(), 80);
    }

    #[test]
    fn centre_is_free_but_an_known_wall_is_not() {
        let map = Map::default();
        assert!(map.is_free(0.0, 0.0));
        assert!(!map.is_free(-784.0, -778.0));
    }

    #[test]
    fn long_moves_cannot_cut_through_a_wall() {
        let map = Map::default();
        assert!(!map.segment_is_free(-900.0, -778.0, -650.0, -778.0));
        assert!(map.segment_is_free(-100.0, -100.0, 100.0, -100.0));
    }
}
