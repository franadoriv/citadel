//! Behavioral parity tests for script-facing custom runtime metrics.

use std::sync::Arc;

use citadel::runtime::{LuaRuntime, RuntimeMetricSnapshot, RuntimeMetrics};

fn expected_metrics() -> Vec<RuntimeMetricSnapshot> {
    vec![
        RuntimeMetricSnapshot::Timer {
            name: "frame_seconds".to_owned(),
            count: 1,
            sum_seconds: 0.25,
        },
        RuntimeMetricSnapshot::Counter {
            name: "kills".to_owned(),
            value: 2,
        },
        RuntimeMetricSnapshot::Gauge {
            name: "players".to_owned(),
            value: 3.5,
        },
    ]
}

#[test]
fn lua_metrics_delegate_to_the_rust_owned_registry() {
    let metrics = Arc::new(RuntimeMetrics::default());
    let runtime = LuaRuntime::from_source(
        r#"
            citadel.on_message(1, function()
                citadel.metrics.counter("kills", 2)
                citadel.metrics.gauge("players", 3.5)
                citadel.metrics.timer("frame_seconds", 0.25)
            end)
        "#,
        "metrics.lua",
        100,
    )
    .expect("runtime loads")
    .with_metrics(Arc::clone(&metrics));

    assert!(runtime.dispatch(1, None, 1, b"").is_empty());
    assert_eq!(metrics.snapshot(), expected_metrics());
}

#[cfg(feature = "runtime-python")]
#[test]
fn python_metrics_delegate_to_the_rust_owned_registry() {
    use citadel::runtime::PythonRuntime;

    let metrics = Arc::new(RuntimeMetrics::default());
    let runtime = PythonRuntime::from_source(
        r#"
import citadel

@citadel.on_message(1)
def message(ctx, body):
    citadel.metrics.counter("kills", 2)
    citadel.metrics.gauge("players", 3.5)
    citadel.metrics.timer("frame_seconds", 0.25)
"#,
        "metrics.py",
        100,
    )
    .expect("runtime loads")
    .with_metrics(Arc::clone(&metrics));

    assert!(runtime.dispatch(1, None, 1, b"").is_empty());
    assert_eq!(metrics.snapshot(), expected_metrics());
}

#[cfg(feature = "runtime-js")]
#[test]
fn javascript_metrics_delegate_to_the_rust_owned_registry() {
    use citadel::runtime::JsRuntime;

    let metrics = Arc::new(RuntimeMetrics::default());
    let runtime = JsRuntime::from_source(
        r#"
            citadel.on_message(1, () => {
              citadel.metrics.counter("kills", 2);
              citadel.metrics.gauge("players", 3.5);
              citadel.metrics.timer("frame_seconds", 0.25);
            });
        "#,
        "metrics.js",
        100,
    )
    .expect("runtime loads")
    .with_metrics(Arc::clone(&metrics));

    assert!(runtime.dispatch(1, None, 1, b"").is_empty());
    assert_eq!(metrics.snapshot(), expected_metrics());
}
