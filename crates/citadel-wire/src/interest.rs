//! `InterestGrid`: the shared area-of-interest primitive both
//! advanced-netcode tracks filter fan-out through (transform-sync §7.3,
//! NetworkPeer §7.3/§9). A uniform 2D spatial hash on the horizontal (X/Y)
//! plane with subscribe-to-3x3-neighbor-cells relevancy, paired with a
//! per-viewer dual-range hysteresis tracker (show ≤ inner, hide > outer) that
//! uses full 3D distance so there is no boundary flicker.
//!
//! This is the primitive + its unit tests only. Gateway wiring and the per-tick
//! snapshot loop are the transform/NetworkPeer feature tasks. Until matches
//! land it is a single global grid; it becomes per-match later.

use std::collections::{HashMap, HashSet};

/// A replicated-object network id (match-scoped in the feature tasks).
pub type ObjectId = u64;

/// A grid cell coordinate on the horizontal (X/Y) plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CellCoord {
    /// Cell index along X.
    pub x: i32,
    /// Cell index along Y.
    pub y: i32,
}

/// A change in a viewer's relevant set produced by the hysteresis tracker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelevanceDelta {
    /// Objects that became relevant this update (send a full baseline).
    pub entered: Vec<ObjectId>,
    /// Objects that stopped being relevant (stop streaming; invalidate base).
    pub exited: Vec<ObjectId>,
}

impl RelevanceDelta {
    /// Whether nothing changed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entered.is_empty() && self.exited.is_empty()
    }
}

/// A uniform spatial-hash grid bucketing objects into square cells by their X/Y
/// position. Vertical (Z) separation is handled by the hysteresis radius, not by
/// the cells.
#[derive(Debug, Clone)]
pub struct InterestGrid {
    cell_size: f32,
    cells: HashMap<CellCoord, HashSet<ObjectId>>,
    positions: HashMap<ObjectId, [f32; 3]>,
}

impl InterestGrid {
    /// A new grid with the given square cell size (in world units). `cell_size`
    /// must be finite and positive; otherwise a default of `1.0` is used to stay
    /// panic-free.
    #[must_use]
    pub fn new(cell_size: f32) -> Self {
        let cell_size = if cell_size.is_finite() && cell_size > 0.0 {
            cell_size
        } else {
            1.0
        };
        Self {
            cell_size,
            cells: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    /// The configured cell size.
    #[must_use]
    pub fn cell_size(&self) -> f32 {
        self.cell_size
    }

    /// Number of tracked objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the grid tracks no objects.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// The cell a position falls into.
    #[must_use]
    pub fn cell_of(&self, pos: [f32; 3]) -> CellCoord {
        CellCoord {
            x: floor_div(pos[0], self.cell_size),
            y: floor_div(pos[1], self.cell_size),
        }
    }

    /// The last recorded position of an object.
    #[must_use]
    pub fn position(&self, id: ObjectId) -> Option<[f32; 3]> {
        self.positions.get(&id).copied()
    }

    /// Insert a new object or move an existing one to `pos`.
    pub fn insert_or_move(&mut self, id: ObjectId, pos: [f32; 3]) {
        let new_cell = self.cell_of(pos);
        if let Some(old_pos) = self.positions.insert(id, pos) {
            let old_cell = self.cell_of(old_pos);
            if old_cell != new_cell {
                if let Some(set) = self.cells.get_mut(&old_cell) {
                    set.remove(&id);
                    if set.is_empty() {
                        self.cells.remove(&old_cell);
                    }
                }
                self.cells.entry(new_cell).or_default().insert(id);
            }
        } else {
            self.cells.entry(new_cell).or_default().insert(id);
        }
    }

    /// Remove an object entirely.
    pub fn remove(&mut self, id: ObjectId) {
        if let Some(pos) = self.positions.remove(&id) {
            let cell = self.cell_of(pos);
            if let Some(set) = self.cells.get_mut(&cell) {
                set.remove(&id);
                if set.is_empty() {
                    self.cells.remove(&cell);
                }
            }
        }
    }

    /// Broad-phase candidates for a viewer: every object in the 3x3 block of
    /// cells centered on the viewer's cell (the subscribe-to-neighbors rule that
    /// kills the border effect). Order is unspecified.
    #[must_use]
    pub fn candidates_for(&self, viewer_pos: [f32; 3]) -> Vec<ObjectId> {
        let center = self.cell_of(viewer_pos);
        let mut out = Vec::new();
        for dy in -1..=1 {
            for dx in -1..=1 {
                let coord = CellCoord {
                    x: center.x.saturating_add(dx),
                    y: center.y.saturating_add(dy),
                };
                if let Some(set) = self.cells.get(&coord) {
                    out.extend(set.iter().copied());
                }
            }
        }
        out
    }
}

/// Per-viewer dual-range relevancy state with hysteresis: an object becomes
/// relevant when it is within `inner` and stops being relevant only once it is
/// beyond `outer`, so an object hovering near the boundary never flickers.
#[derive(Debug, Clone, Default)]
pub struct RelevanceSet {
    subscribed: HashSet<ObjectId>,
}

impl RelevanceSet {
    /// An empty relevance set (nothing subscribed).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The currently relevant objects.
    #[must_use]
    pub fn subscribed(&self) -> &HashSet<ObjectId> {
        &self.subscribed
    }

    /// Whether `id` is currently relevant.
    #[must_use]
    pub fn contains(&self, id: ObjectId) -> bool {
        self.subscribed.contains(&id)
    }

    /// Recompute relevancy for `viewer_pos` against `grid` using the inner/outer
    /// hysteresis band and return what entered/exited. Distances are full 3D.
    ///
    /// `inner <= outer` is required for flicker-free behavior; if misordered the
    /// values are swapped defensively.
    pub fn update(
        &mut self,
        grid: &InterestGrid,
        viewer_pos: [f32; 3],
        inner: f32,
        outer: f32,
    ) -> RelevanceDelta {
        let (inner, outer) = if inner <= outer {
            (inner, outer)
        } else {
            (outer, inner)
        };
        let inner_sq = f64::from(inner) * f64::from(inner);
        let outer_sq = f64::from(outer) * f64::from(outer);

        let mut delta = RelevanceDelta::default();

        // Exit pass: any subscribed object now beyond `outer` (or gone) leaves.
        let mut to_exit = Vec::new();
        for &id in &self.subscribed {
            let leave = match grid.position(id) {
                Some(pos) => dist_sq(pos, viewer_pos) > outer_sq,
                None => true, // despawned / removed
            };
            if leave {
                to_exit.push(id);
            }
        }
        for id in to_exit {
            self.subscribed.remove(&id);
            delta.exited.push(id);
        }

        // Enter pass: broad-phase candidates within `inner` that are not yet in.
        for id in grid.candidates_for(viewer_pos) {
            if self.subscribed.contains(&id) {
                continue;
            }
            if let Some(pos) = grid.position(id)
                && dist_sq(pos, viewer_pos) <= inner_sq
            {
                self.subscribed.insert(id);
                delta.entered.push(id);
            }
        }

        delta
    }
}

/// Floor-divide a coordinate by the cell size into an `i32` cell index,
/// saturating rather than panicking on extreme/non-finite inputs.
fn floor_div(coord: f32, cell_size: f32) -> i32 {
    if !coord.is_finite() {
        return 0;
    }
    let c = (coord / cell_size).floor();
    if c >= i32::MAX as f32 {
        i32::MAX
    } else if c <= i32::MIN as f32 {
        i32::MIN
    } else {
        c as i32
    }
}

/// Squared 3D distance in `f64` (deterministic, overflow-resistant).
fn dist_sq(a: [f32; 3], b: [f32; 3]) -> f64 {
    let dx = f64::from(a[0]) - f64::from(b[0]);
    let dy = f64::from(a[1]) - f64::from(b[1]);
    let dz = f64::from(a[2]) - f64::from(b[2]);
    dx * dx + dy * dy + dz * dz
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_move_remove_maintains_cells() {
        let mut g = InterestGrid::new(10.0);
        g.insert_or_move(1, [5.0, 5.0, 0.0]);
        assert_eq!(g.len(), 1);
        assert_eq!(g.candidates_for([5.0, 5.0, 0.0]), vec![1]);

        // Move into a far cell; the near viewer no longer sees it.
        g.insert_or_move(1, [1000.0, 1000.0, 0.0]);
        assert!(g.candidates_for([5.0, 5.0, 0.0]).is_empty());
        assert_eq!(g.candidates_for([1000.0, 1000.0, 0.0]), vec![1]);

        g.remove(1);
        assert!(g.is_empty());
    }

    #[test]
    fn candidates_span_3x3_neighbor_cells() {
        let mut g = InterestGrid::new(10.0);
        // Object one cell diagonally away from the viewer's cell.
        g.insert_or_move(1, [15.0, 15.0, 0.0]); // cell (1,1)
        // Viewer in cell (0,0) => 3x3 covers (1,1).
        let mut c = g.candidates_for([5.0, 5.0, 0.0]);
        c.sort_unstable();
        assert_eq!(c, vec![1]);
        // Object two cells away is outside the 3x3 block.
        g.insert_or_move(2, [25.0, 25.0, 0.0]); // cell (2,2)
        let mut c = g.candidates_for([5.0, 5.0, 0.0]);
        c.sort_unstable();
        assert_eq!(c, vec![1]);
    }

    #[test]
    fn hysteresis_no_flicker_in_band() {
        let mut g = InterestGrid::new(100.0);
        let mut rel = RelevanceSet::new();
        let inner = 50.0;
        let outer = 90.0;

        // Object at distance 40 (< inner) enters.
        g.insert_or_move(1, [40.0, 0.0, 0.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert_eq!(d.entered, vec![1]);
        assert!(rel.contains(1));

        // Move into the band (distance 70): stays (no flicker).
        g.insert_or_move(1, [70.0, 0.0, 0.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert!(d.is_empty());
        assert!(rel.contains(1));

        // Beyond outer (distance 95 within the same/adjacent cells): exits.
        g.insert_or_move(1, [95.0, 0.0, 0.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert_eq!(d.exited, vec![1]);
        assert!(!rel.contains(1));
    }

    #[test]
    fn hysteresis_reentry_requires_inner_again() {
        let mut g = InterestGrid::new(100.0);
        let mut rel = RelevanceSet::new();
        let (inner, outer) = (50.0, 90.0);
        g.insert_or_move(1, [95.0, 0.0, 0.0]);
        // At distance 95 it never entered.
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert!(d.is_empty());
        // In the band (70) but never subscribed: still does not enter.
        g.insert_or_move(1, [70.0, 0.0, 0.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert!(d.is_empty());
        // Only crossing inner subscribes it.
        g.insert_or_move(1, [30.0, 0.0, 0.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], inner, outer);
        assert_eq!(d.entered, vec![1]);
    }

    #[test]
    fn vertical_separation_excludes_via_3d_distance() {
        let mut g = InterestGrid::new(100.0);
        let mut rel = RelevanceSet::new();
        // Same X/Y cell as the viewer but far above on Z.
        g.insert_or_move(1, [0.0, 0.0, 500.0]);
        let d = rel.update(&g, [0.0, 0.0, 0.0], 50.0, 90.0);
        assert!(d.is_empty(), "Z separation keeps it irrelevant");
    }

    #[test]
    fn despawn_forces_exit() {
        let mut g = InterestGrid::new(100.0);
        let mut rel = RelevanceSet::new();
        g.insert_or_move(1, [10.0, 0.0, 0.0]);
        rel.update(&g, [0.0, 0.0, 0.0], 50.0, 90.0);
        assert!(rel.contains(1));
        g.remove(1);
        let d = rel.update(&g, [0.0, 0.0, 0.0], 50.0, 90.0);
        assert_eq!(d.exited, vec![1]);
    }

    #[test]
    fn non_finite_cell_size_falls_back() {
        let g = InterestGrid::new(f32::NAN);
        assert_eq!(g.cell_size(), 1.0);
        let g = InterestGrid::new(-5.0);
        assert_eq!(g.cell_size(), 1.0);
    }
}
