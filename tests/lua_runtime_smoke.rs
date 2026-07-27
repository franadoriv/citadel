#![allow(clippy::unwrap_used)]

#[path = "support/runtime_smoke.rs"]
mod runtime_smoke;

use std::sync::Arc;

use citadel::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
use citadel::runtime::{LuaRuntime, PhysicsOptions, RpcOutcome};

const FIXTURE: &str = include_str!("fixtures/host_api_smoke.lua");

#[test]
fn lua_host_api_tier_b_smoke_matches_command_shapes() {
    let runtime = LuaRuntime::from_source(FIXTURE, "tests/fixtures/host_api_smoke.lua", 100)
        .expect("lua fixture loads");

    runtime_smoke::assert_host_api_smoke_contract(&runtime);
}

#[test]
fn lua_physics_state_reads_the_live_transform_hub() {
    let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub builds"));
    hub.spawn_server_simulated(42, TransformState::at([1.0, 2.0, 3.0]));
    hub.set_physics(42, Some(PhysicsOptions::default()));
    let runtime = LuaRuntime::from_source(
        r#"
            citadel.on_rpc("state", function()
                local state = citadel.physics_state(42)
                return string.format("%s:%.0f:%.0f", tostring(state.grounded), state.position[1], state.velocity[2])
            end)
        "#,
        "physics_state.lua",
        100,
    )
    .expect("lua fixture loads")
    .with_transform_hub(hub);

    assert_eq!(
        runtime.call_rpc(1, None, "state", b""),
        RpcOutcome::Ok(b"false:1:0".to_vec())
    );
}
