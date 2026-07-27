#![allow(clippy::unwrap_used)]

#[path = "support/runtime_smoke.rs"]
mod runtime_smoke;

#[cfg(feature = "runtime-js")]
mod js_smoke {
    use std::sync::Arc;

    use citadel::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
    use citadel::runtime::{JsRuntime, PhysicsOptions, RpcOutcome};

    use super::runtime_smoke;

    const FIXTURE: &str = include_str!("fixtures/host_api_smoke.js");

    #[test]
    fn js_host_api_tier_b_smoke_matches_command_shapes() {
        let runtime = JsRuntime::from_source(FIXTURE, "tests/fixtures/host_api_smoke.js", 100)
            .expect("javascript fixture loads");

        runtime_smoke::assert_host_api_smoke_contract(&runtime);
    }

    #[test]
    fn js_example_game_loads_from_scripts_dir() {
        let scripts_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("js-game");

        let runtime = JsRuntime::load(&scripts_dir, 100)
            .expect("javascript example load succeeds")
            .expect("examples/js-game/main.js exists");

        assert!(!runtime.has_tick_handler());
    }

    #[test]
    fn js_physics_state_reads_the_live_transform_hub() {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub builds"));
        hub.spawn_server_simulated(42, TransformState::at([1.0, 2.0, 3.0]));
        hub.set_physics(42, Some(PhysicsOptions::default()));
        let runtime = JsRuntime::from_source(
            r#"
citadel.on_rpc("state", () => {
  const state = citadel.physics_state(42);
  return `${state.grounded}:${state.position[0].toFixed(0)}:${state.velocity[2].toFixed(0)}`;
});
"#,
            "physics_state.js",
            100,
        )
        .expect("javascript fixture loads")
        .with_transform_hub(hub);

        assert_eq!(
            runtime.call_rpc(1, None, "state", b""),
            RpcOutcome::Ok(b"false:1:0".to_vec())
        );
    }
}

#[cfg(not(feature = "runtime-js"))]
#[test]
fn js_host_api_tier_b_smoke_skips_without_feature() {
    eprintln!("javascript runtime Tier-B smoke skipped: build lacks runtime-js feature");
}
