//! Server-side map catalog: discover and load `.map` (CMAP) level geometry.
//!
//! Mirrors the `scripts_dir` model. A node has a `maps_dir` (default `./maps`)
//! holding cooked `.map` files (see the [`citadel_map`] crate). On startup the
//! server scans that directory once and loads every well-formed `.map`, indexing
//! it by file stem so a room's `map` name (chosen by `on_room_create`) resolves to
//! loaded geometry. A corrupt or unreadable file is logged and skipped — one bad
//! map never stops the server from booting.
//!
//! This is the server half of the map pipeline (, Phase B): the geometry
//! is the input a later navmesh bake (Phase C) will consume. Today the catalog
//! gives a room a validated map and exposes basic info (bounds, triangle count).

use std::collections::BTreeMap;
use std::path::Path;

use citadel_map::MapFile;

/// Read-only summary of a loaded map for trusted game logic.
#[derive(Debug, Clone, PartialEq)]
pub struct MapInfo {
    /// World-space AABB minimum in Unreal world units (cm).
    pub bounds_min: [f32; 3],
    /// World-space AABB maximum in Unreal world units (cm).
    pub bounds_max: [f32; 3],
    /// Number of collision vertices.
    pub vertex_count: usize,
    /// Number of collision triangles.
    pub triangle_count: usize,
}

/// A loaded map: its parsed CMAP contents plus the key it was indexed under.
#[derive(Debug, Clone)]
pub struct LoadedMap {
    /// Index key — the file stem (`Lvl_ThirdPerson` for `Lvl_ThirdPerson.map`).
    pub key: String,
    /// The parsed map geometry and metadata.
    pub file: MapFile,
}

impl LoadedMap {
    /// Number of collision triangles in the map.
    #[must_use]
    pub fn triangle_count(&self) -> usize {
        self.file.collision.triangles.len()
    }

    /// Number of collision vertices in the map.
    #[must_use]
    pub fn vertex_count(&self) -> usize {
        self.file.collision.vertices.len()
    }
}

/// An in-memory catalog of loaded maps, indexed by file stem.
///
/// Construct with [`MapCatalog::load_dir`] (scans a directory) or
/// [`MapCatalog::empty`] (no maps — the default when `maps_dir` is absent).
/// Lookups are by the same name a room's `map` label carries.
#[derive(Debug, Clone, Default)]
pub struct MapCatalog {
    maps: BTreeMap<String, LoadedMap>,
}

impl MapCatalog {
    /// An empty catalog (no maps loaded).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Scan `dir` for `*.map` files and load each well-formed one.
    ///
    /// A missing or unreadable directory yields an empty catalog (not an error): a
    /// node without cooked maps runs exactly as before. A file that fails to read
    /// or decode is logged at `warn` and skipped, so one corrupt map never blocks
    /// startup.
    #[must_use]
    pub fn load_dir(dir: &Path) -> Self {
        let mut maps = BTreeMap::new();
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            // Absent / unreadable directory => empty catalog, same as no maps.
            Err(_) => return Self { maps },
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("map") {
                continue;
            }
            let Some(key) = path.file_stem().and_then(|s| s.to_str()).map(str::to_owned) else {
                continue;
            };
            match std::fs::read(&path) {
                Ok(bytes) => match MapFile::decode(&bytes) {
                    Ok(mut file) => {
                        if let Some(navmesh) = &file.navmesh {
                            if let Err(error) = citadel_nav::validate_abi(navmesh) {
                                tracing::warn!(map = %key, error = ?error, "skipping map with incompatible NAVMESH ABI");
                                continue;
                            }
                        } else {
                            match citadel_nav::bake(&file.collision) {
                                Ok(navmesh) => file.navmesh = Some(navmesh),
                                Err(error) => {
                                    tracing::warn!(map = %key, error = ?error, "loaded map has no usable navigation mesh");
                                }
                            }
                        }
                        tracing::info!(
                            map = %key,
                            verts = file.collision.vertices.len(),
                            tris = file.collision.triangles.len(),
                            "loaded map"
                        );
                        maps.insert(key.clone(), LoadedMap { key, file });
                    }
                    Err(e) => {
                        tracing::warn!(map = %key, error = %e, "skipping malformed .map file")
                    }
                },
                Err(e) => tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "skipping unreadable .map file"
                ),
            }
        }
        Self { maps }
    }

    /// Look up a loaded map by its key (file stem / room map name).
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&LoadedMap> {
        self.maps.get(name)
    }

    /// Number of loaded maps.
    #[must_use]
    pub fn len(&self) -> usize {
        self.maps.len()
    }

    /// Whether the catalog holds no maps.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.maps.is_empty()
    }

    /// The keys of all loaded maps, in sorted order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.maps.keys().map(String::as_str)
    }

    /// Return a copyable, read-only summary for a loaded map.
    #[must_use]
    pub fn info(&self, name: &str) -> Option<MapInfo> {
        self.get(name).map(|map| MapInfo {
            bounds_min: map.file.metadata.bounds_min,
            bounds_max: map.file.metadata.bounds_max,
            vertex_count: map.vertex_count(),
            triangle_count: map.triangle_count(),
        })
    }

    /// Find a server-authoritative navigation corridor over a loaded map's
    /// collision geometry. The native bake/query seam remains isolated in
    /// `citadel-nav`; a missing map has no path.
    pub fn find_path(
        &self,
        name: &str,
        start: [f32; 3],
        goal: [f32; 3],
    ) -> Result<Option<Vec<[f32; 3]>>, citadel_nav::NavError> {
        self.get(name)
            .map(|map| citadel_nav::find_path(&map.file.collision, start, goal))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use citadel_map::{CollisionMesh, MapMetadata};

    fn sample_map(name: &str, tris: u32) -> MapFile {
        // A trivial mesh with `tris` triangles over 3 shared vertices — enough to
        // exercise counts without caring about geometry.
        let mut triangles = Vec::new();
        for _ in 0..tris {
            triangles.push([0u32, 1, 2]);
        }
        MapFile {
            metadata: MapMetadata {
                name: name.to_owned(),
                bounds_min: [-1.0, 0.0, -1.0],
                bounds_max: [1.0, 2.0, 1.0],
            },
            collision: CollisionMesh {
                vertices: vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]],
                triangles,
            },
            navmesh: None,
        }
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        // Process- and tag-unique dir (no external temp-crate dep).
        let base = std::env::temp_dir().join(format!("citadel-maps-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn absent_directory_yields_empty_catalog() {
        let dir = std::env::temp_dir().join("citadel-maps-does-not-exist-xyz");
        let cat = MapCatalog::load_dir(&dir);
        assert!(cat.is_empty());
        assert_eq!(cat.len(), 0);
        assert!(cat.get("anything").is_none());
    }

    #[test]
    fn loads_well_formed_maps_and_indexes_by_stem() {
        let dir = temp_dir("load");
        std::fs::write(dir.join("Arena.map"), sample_map("Arena", 4).encode()).unwrap();
        std::fs::write(dir.join("Lobby.map"), sample_map("Lobby", 7).encode()).unwrap();

        let cat = MapCatalog::load_dir(&dir);
        assert_eq!(cat.len(), 2);
        assert_eq!(cat.get("Arena").unwrap().triangle_count(), 4);
        assert_eq!(cat.get("Lobby").unwrap().triangle_count(), 7);
        assert_eq!(cat.get("Arena").unwrap().vertex_count(), 3);
        let names: Vec<_> = cat.names().collect();
        assert_eq!(names, vec!["Arena", "Lobby"]); // BTreeMap => sorted
        assert_eq!(
            cat.info("Arena"),
            Some(MapInfo {
                bounds_min: [-1.0, 0.0, -1.0],
                bounds_max: [1.0, 2.0, 1.0],
                vertex_count: 3,
                triangle_count: 4,
            })
        );
        assert_eq!(cat.info("Unknown"), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skips_corrupt_and_non_map_files_without_failing() {
        let dir = temp_dir("skip");
        std::fs::write(dir.join("Good.map"), sample_map("Good", 2).encode()).unwrap();
        std::fs::write(dir.join("Bad.map"), b"not a cmap file").unwrap();
        std::fs::write(dir.join("readme.txt"), b"ignored").unwrap();

        let cat = MapCatalog::load_dir(&dir);
        assert_eq!(cat.len(), 1, "only the well-formed .map is loaded");
        assert!(cat.get("Good").is_some());
        assert!(
            cat.get("Bad").is_none(),
            "corrupt map is skipped, not fatal"
        );
        assert!(cat.get("readme").is_none(), "non-.map files are ignored");

        std::fs::remove_dir_all(&dir).ok();
    }
}
