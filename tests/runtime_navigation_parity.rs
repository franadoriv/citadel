//! Parity coverage for core-owned map discovery and Detour path queries.

#![allow(clippy::panic, clippy::unwrap_used)]
#![cfg(all(feature = "runtime-python", feature = "runtime-js"))]

use std::sync::Arc;
use std::time::Duration;

use citadel::maps::MapCatalog;
use citadel::runtime::{JsRuntime, LuaRuntime, PythonRuntime, Runtime};
use citadel_map::{CollisionMesh, MapFile, MapMetadata};

const LUA: &str = r#"
    citadel.on_tick(function()
        local maps = citadel.map_names()
        assert(#maps == 1 and maps[1] == "Arena")
        assert(citadel.map_info("Arena") ~= nil)
        local path = citadel.find_path("Arena", {1, 0, 1}, {9, 0, 9})
        assert(path ~= nil and #path >= 2)
        assert(citadel.find_path("missing", {1, 0, 1}, {9, 0, 9}) == nil)
    end)
"#;

const PYTHON: &str = r#"
import citadel

@citadel.on_tick
def tick(_dt):
    assert citadel.map_names() == ["Arena"]
    assert citadel.map_info("Arena") is not None
    path = citadel.find_path("Arena", (1, 0, 1), (9, 0, 9))
    assert path is not None and len(path) >= 2
    assert citadel.find_path("missing", (1, 0, 1), (9, 0, 9)) is None
"#;

const JAVASCRIPT: &str = r#"
citadel.on_tick(() => {
  const maps = citadel.map_names();
  if (maps.length !== 1 || maps[0] !== "Arena") throw new Error("map discovery failed");
  if (citadel.map_info("Arena") === null) throw new Error("map info missing");
  const path = citadel.find_path("Arena", [1, 0, 1], [9, 0, 9]);
  if (path === null || path.length < 2) throw new Error("path missing");
  if (citadel.find_path("missing", [1, 0, 1], [9, 0, 9]) !== null) throw new Error("unknown map route");
});
"#;

fn catalog() -> (Arc<MapCatalog>, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "citadel-runtime-navigation-parity-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("create map test directory");
    let map = MapFile {
        metadata: MapMetadata {
            name: "Arena".to_owned(),
            bounds_min: [0.0, 0.0, 0.0],
            bounds_max: [10.0, 0.0, 10.0],
        },
        collision: CollisionMesh {
            vertices: vec![
                [0.0, 0.0, 0.0],
                [10.0, 0.0, 0.0],
                [10.0, 0.0, 10.0],
                [0.0, 0.0, 10.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        },
        navmesh: None,
    };
    std::fs::write(directory.join("Arena.map"), map.encode()).expect("write map fixture");
    (Arc::new(MapCatalog::load_dir(&directory)), directory)
}

#[test]
fn lua_python_and_javascript_share_authoritative_navigation_contract() {
    let (maps, directory) = catalog();
    let lua = LuaRuntime::from_source(LUA, "navigation.lua", 100)
        .expect("lua runtime loads")
        .with_maps(Arc::clone(&maps));
    let python = PythonRuntime::from_source(PYTHON, "navigation.py", 100)
        .expect("python runtime loads")
        .with_maps(Arc::clone(&maps));
    let javascript = JsRuntime::from_source(JAVASCRIPT, "navigation.js", 100)
        .expect("javascript runtime loads")
        .with_maps(maps);

    let budget = Duration::from_millis(100);
    assert!(lua.tick(Duration::from_millis(16), budget).is_empty());
    assert!(python.tick(Duration::from_millis(16), budget).is_empty());
    assert!(
        javascript
            .tick(Duration::from_millis(16), budget)
            .is_empty()
    );
    std::fs::remove_dir_all(directory).ok();
}
