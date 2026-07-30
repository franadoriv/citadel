#![allow(clippy::unwrap_used)]

#[cfg(feature = "runtime-python")]
#[path = "support/runtime_smoke.rs"]
mod runtime_smoke;

#[cfg(feature = "runtime-python")]
mod python_smoke {
    use std::sync::Arc;

    use citadel::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
    use citadel::runtime::{PhysicsOptions, PythonRuntime, RpcOutcome};

    use super::runtime_smoke;

    const FIXTURE: &str = include_str!("fixtures/host_api_smoke.py");

    #[test]
    fn python_host_api_tier_b_smoke_matches_command_shapes() {
        let runtime = PythonRuntime::from_source(FIXTURE, "tests/fixtures/host_api_smoke.py", 100)
            .expect("python fixture loads");

        runtime_smoke::assert_host_api_smoke_contract(&runtime);
    }

    #[test]
    fn python_example_game_loads_from_scripts_dir() {
        let scripts_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("python-game");

        let runtime = PythonRuntime::load(&scripts_dir, 100)
            .expect("python example load succeeds")
            .expect("examples/python-game/main.py exists");

        assert!(runtime.has_tick_handler());
    }

    #[test]
    fn python_physics_state_reads_the_live_transform_hub() {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub builds"));
        hub.spawn_server_simulated(42, TransformState::at([1.0, 2.0, 3.0]));
        hub.set_physics(42, Some(PhysicsOptions::default()));
        let runtime = PythonRuntime::from_source(
            r#"
import citadel

@citadel.on_rpc("state")
def state(ctx, body):
    state = citadel.physics_state(42)
    return f"{state['grounded']}:{state['position'][0]:.0f}:{state['velocity'][2]:.0f}"
"#,
            "physics_state.py",
            100,
        )
        .expect("python fixture loads")
        .with_transform_hub(hub);

        assert_eq!(
            runtime.call_rpc(1, None, "state", b""),
            RpcOutcome::Ok(b"False:1:0".to_vec())
        );
    }
}

#[cfg(not(feature = "runtime-python"))]
#[test]
fn python_host_api_tier_b_smoke_skips_without_feature() {
    eprintln!("python runtime Tier-B smoke skipped: build lacks runtime-python feature");
}
