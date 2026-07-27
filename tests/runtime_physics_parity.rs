//! Behavioral parity for the four physics host functions.

#![allow(clippy::panic, clippy::unwrap_used)]
#![cfg(all(feature = "runtime-python", feature = "runtime-js"))]

use std::sync::Arc;
use std::time::Duration;

use citadel::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
use citadel::runtime::{LuaRuntime, OutboundCommand, PhysicsOptions};
use citadel_map::CollisionMesh;

const OBJECT_ID: u32 = 77;

const LUA: &str = r#"
    citadel.on_tick(function()
        assert(citadel.physics_state(77) ~= nil)
        local hit = citadel.raycast({0, 100, 0}, {0, -200, 0})
        assert(hit ~= nil and hit.distance == 100)
        assert(citadel.sphere_overlap({0, 10, 0}, 10))
        assert(citadel.ground_height({0, 100, 0}, 200) ~= nil)
        citadel.set_physics(77, { gravity = 600, buoyancy = 100, drag = 0.25,
            radius = 12, height = 48, max_speed = 900, shape = "capsule" })
        citadel.apply_impulse(77, 15, 300, -20)
        citadel.set_move_intent(77, 80, 999, -40)
    end)
"#;

const PYTHON: &str = r#"
import citadel

@citadel.on_tick
def tick(_dt):
    assert citadel.physics_state(77) is not None
    hit = citadel.raycast((0, 100, 0), (0, -200, 0))
    assert hit is not None and hit["distance"] == 100
    assert citadel.sphere_overlap((0, 10, 0), 10)
    assert citadel.ground_height((0, 100, 0), 200) is not None
    citadel.set_physics(77, {"gravity": 600, "buoyancy": 100, "drag": 0.25,
        "radius": 12, "height": 48, "max_speed": 900, "shape": "capsule"})
    citadel.apply_impulse(77, 15, 300, -20)
    citadel.set_move_intent(77, 80, 999, -40)
"#;

const JAVASCRIPT: &str = r#"
citadel.on_tick(() => {
  if (citadel.physics_state(77) === null) throw new Error("missing physics state");
  const hit = citadel.raycast([0, 100, 0], [0, -200, 0]);
  if (hit === null || hit.distance !== 100) throw new Error("missing raycast hit");
  if (!citadel.sphere_overlap([0, 10, 0], 10)) throw new Error("missing overlap");
  if (citadel.ground_height([0, 100, 0], 200) === null) throw new Error("missing ground");
  citadel.set_physics(77, {gravity: 600, buoyancy: 100, drag: 0.25,
    radius: 12, height: 48, max_speed: 900, shape: "capsule"});
  citadel.apply_impulse(77, 15, 300, -20);
  citadel.set_move_intent(77, 80, 999, -40);
});
"#;

fn probe_hub() -> Arc<TransformHub> {
    let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
    hub.set_physics_map(Some((
        "test-floor",
        &CollisionMesh {
            vertices: vec![
                [-100.0, 0.0, -100.0],
                [100.0, 0.0, -100.0],
                [-100.0, 0.0, 100.0],
                [100.0, 0.0, 100.0],
            ],
            triangles: vec![[0, 1, 2], [2, 1, 3]],
        },
    )));
    hub.spawn_server_simulated(OBJECT_ID, TransformState::at([0.0, 100.0, 0.0]));
    hub.set_physics(OBJECT_ID, Some(PhysicsOptions::default()));
    hub
}

fn simulate(commands: &[OutboundCommand]) -> citadel::realtime::transform::PhysicsState {
    let hub = probe_hub();
    for command in commands {
        match command {
            OutboundCommand::SetPhysics { object_id, opts } => hub.set_physics(*object_id, *opts),
            OutboundCommand::ApplyImpulse { object_id, impulse } => {
                hub.apply_impulse(*object_id, *impulse);
            }
            OutboundCommand::SetMoveIntent { object_id, intent } => {
                hub.set_move_intent(*object_id, *intent);
            }
            _ => panic!("physics parity script emitted an unexpected command"),
        }
    }
    for _ in 0..30 {
        hub.sim_tick();
    }
    hub.physics_state(OBJECT_ID).expect("body remains attached")
}

#[test]
fn lua_python_and_javascript_physics_host_calls_have_identical_behavior() {
    use citadel::runtime::PythonRuntime;

    let lua = LuaRuntime::from_source(LUA, "physics-parity.lua", 100)
        .expect("lua runtime loads")
        .with_transform_hub(probe_hub());
    let python = PythonRuntime::from_source(PYTHON, "physics-parity.py", 100)
        .expect("python runtime loads")
        .with_transform_hub(probe_hub());
    let javascript = citadel::runtime::JsRuntime::from_source(JAVASCRIPT, "physics-parity.js", 100)
        .expect("javascript runtime loads")
        .with_transform_hub(probe_hub());

    let lua_commands = lua.tick(Duration::from_millis(16), Duration::from_millis(100));
    let python_commands = python.tick(Duration::from_millis(16), Duration::from_millis(100));
    let javascript_commands =
        javascript.tick(Duration::from_millis(16), Duration::from_millis(100));
    assert_eq!(
        lua_commands, python_commands,
        "Lua and Python command contracts match"
    );
    assert_eq!(
        lua_commands, javascript_commands,
        "Lua and JS command contracts match"
    );

    let lua_state = simulate(&lua_commands);
    assert_eq!(lua_state, simulate(&python_commands));
    assert_eq!(lua_state, simulate(&javascript_commands));
}
