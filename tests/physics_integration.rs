//! End-to-end server-simulated physics coverage.

#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use citadel::maps::MapCatalog;
use citadel::observability::NodeMetrics;
use citadel::realtime::transform::{TransformHub, TransformHubConfig};
use citadel::realtime::{Gateway, RoomLabel};
use citadel::runtime::LuaRuntime;
use citadel_map::{CollisionMesh, MapFile, MapMetadata};
use citadel_wire::tsync::Snapshot;

static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempMapDir(PathBuf);

impl TempMapDir {
    fn new() -> Self {
        let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "citadel-physics-integration-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temporary map directory");
        Self(path)
    }
}

impl Drop for TempMapDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn floor_map() -> MapFile {
    MapFile {
        metadata: MapMetadata {
            name: "physics-floor".to_string(),
            bounds_min: [-500.0, -100.0, -500.0],
            bounds_max: [500.0, 500.0, 500.0],
        },
        collision: CollisionMesh {
            vertices: vec![
                [-500.0, 0.0, -500.0],
                [500.0, 0.0, -500.0],
                [500.0, 0.0, 500.0],
                [-500.0, 0.0, 500.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        },
        navmesh: None,
    }
}

const BOT_SCRIPT: &str = r#"
    local phase = 0
    citadel.on_tick(function(_, room_id)
        if room_id == nil then return end
        if phase == 0 then
            phase = 1
            local bot = citadel.spawn_actor({ x = 0, y = 200, z = 0 })
            citadel.set_physics(bot, {
                gravity = 980, buoyancy = 0, drag = 0,
                radius = 10, height = 40, max_speed = 2000, shape = "capsule"
            })
        elseif phase == 1 then
            phase = 2
            citadel.apply_impulse(0x40000000, 0, 500, 0)
        end
    end)
"#;

#[test]
fn server_simulated_bot_falls_jumps_and_replicates_from_a_loaded_map() {
    let map_dir = TempMapDir::new();
    fs::write(map_dir.0.join("physics-floor.map"), floor_map().encode())
        .expect("write in-test CMAP floor");
    let maps = Arc::new(MapCatalog::load_dir(&map_dir.0));
    assert!(
        maps.get("physics-floor").is_some(),
        "catalog loads the CMAP"
    );

    let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
    let runtime = LuaRuntime::from_source(BOT_SCRIPT, "physics-bot.lua", 100)
        .expect("physics bot script loads");
    let gateway =
        Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(Arc::new(runtime)))
            .with_maps(maps)
            .with_transform_hub(Arc::clone(&hub));
    gateway.create_room(RoomLabel::with_map("physics-floor"));

    // The room-scoped runtime tick traverses the canonical command path:
    // spawn_actor/set_physics -> Gateway -> TransformHub, and selects the map BVH.
    gateway.tick(Duration::from_millis(16), Duration::from_millis(100));
    let spawned = hub
        .physics_state(0x4000_0000)
        .expect("physics body attached");
    assert!(!spawned.grounded);

    for _ in 0..180 {
        gateway.transform_sim_step();
    }
    let landed = hub.physics_state(0x4000_0000).expect("bot remains bodied");
    assert!(
        landed.grounded,
        "bot rests on the loaded map floor: {landed:?}"
    );
    assert!(
        (landed.position[1] - 20.0).abs() < 0.5,
        "capsule centre rests one half-height above the floor: {landed:?}"
    );
    assert!(landed.velocity[1].abs() < f32::EPSILON);

    // The next room tick emits apply_impulse through the same gateway command path.
    gateway.tick(Duration::from_millis(16), Duration::from_millis(100));
    gateway.transform_sim_step();
    let airborne = hub.physics_state(0x4000_0000).expect("bot remains bodied");
    assert!(
        airborne.position[1] > landed.position[1],
        "a +Y impulse raises the bot: landed={landed:?}, airborne={airborne:?}"
    );

    hub.register_client(7);
    let outbound = hub.snapshot_tick();
    let snapshot = Snapshot::decode(&outbound[0].body, hub.codec()).expect("snapshot decodes");
    let update = snapshot
        .updates
        .iter()
        .find(|update| update.object_id == 0x4000_0000)
        .expect("physics bot appears in the snapshot frame");
    let position = update.fields.position.expect("snapshot carries position");
    assert!(
        (position[1] - airborne.position[1]).abs() < 1.0,
        "snapshot contains the physics result"
    );
}
