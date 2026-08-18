//! Host-API manifest generator + stale-guard.
//!
//! `src/runtime/host_api_manifest.json` is generated from
//! `citadel::runtime::HOST_API_SURFACE`. Normal `cargo test` runs fail when the
//! checked-in JSON is stale. Regenerate with:
//!
//! ```text
//! CITADEL_REGEN_CONTRACT=1 cargo test --test host_api_manifest
//! ```

use std::path::PathBuf;

use citadel::runtime::HOST_API_SURFACE;
use citadel::runtime::{HostApiCategory, HostApiStatus};

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("runtime")
        .join("host_api_manifest.json")
}

fn render_manifest() -> String {
    let mut rendered =
        serde_json::to_string_pretty(HOST_API_SURFACE).expect("serialize host API manifest");
    rendered.push('\n');
    rendered
}

#[test]
fn host_api_manifest_json_is_in_sync() {
    let expected = render_manifest();
    let path = manifest_path();

    if std::env::var_os("CITADEL_REGEN_CONTRACT").is_some() {
        std::fs::write(&path, &expected).expect("write host_api_manifest.json");
        eprintln!("regenerated {}", path.display());
        return;
    }

    let actual = std::fs::read_to_string(&path).expect(
        "read src/runtime/host_api_manifest.json; regenerate with \
         CITADEL_REGEN_CONTRACT=1 cargo test --test host_api_manifest",
    );

    assert_eq!(
        actual, expected,
        "src/runtime/host_api_manifest.json is stale relative to \
         citadel::runtime::HOST_API_SURFACE. Regenerate with: \
         CITADEL_REGEN_CONTRACT=1 cargo test --test host_api_manifest"
    );
}

#[test]
fn tournament_discovery_is_a_shipped_runtime_contract() {
    let api = HOST_API_SURFACE
        .iter()
        .find(|entry| entry.name == "tournaments.call")
        .expect("tournament discovery is declared");

    assert_eq!(api.category, HostApiCategory::Domain);
    assert_eq!(
        api.params,
        &[
            "actor:string",
            "operation:list|get|results|registration",
            "payload_json:string",
        ]
    );
    assert_eq!(api.returns, "json");
    assert_eq!(api.status, HostApiStatus::Shipped);
    assert_eq!(api.since, "IMPL-20260803-TOURNAMENTS-DISCOVERY");
}

#[test]
fn leaderboard_reset_callback_is_a_shipped_runtime_contract() {
    let callback = HOST_API_SURFACE
        .iter()
        .find(|entry| entry.name == "on_leaderboard_reset")
        .expect("leaderboard reset callback is declared");

    assert_eq!(callback.category, HostApiCategory::LeaderboardHook);
    assert_eq!(
        callback.params,
        &["handler:fn(ctx:{leaderboard_id:string,due_at_unix_ms:u64,fencing_token:u64})"]
    );
    assert_eq!(callback.returns, "void");
    assert_eq!(callback.status, HostApiStatus::Shipped);
    assert_eq!(callback.since, "unreleased");
}

#[test]
fn text_policy_is_a_shipped_cross_runtime_contract() {
    let expected = [
        (
            "text_policy.load_json",
            &["path:relative .json"][..],
            "policy_ref:string",
        ),
        (
            "text_policy.scan",
            &["policy_ref:string", "text:string"][..],
            "{decision:allow|flag|mask|replace|reject,matches:array,text:string}",
        ),
        (
            "text_policy.sanitize",
            &["policy_ref:string", "text:string"][..],
            "{decision:allow|flag|mask|replace|reject,matches:array,text:string}",
        ),
    ];

    for (name, params, returns) in expected {
        let api = HOST_API_SURFACE
            .iter()
            .find(|entry| entry.name == name)
            .expect("text policy API is declared");

        assert_eq!(api.category, HostApiCategory::TextPolicy);
        assert_eq!(api.params, params);
        assert_eq!(api.returns, returns);
        assert_eq!(api.status, HostApiStatus::Shipped);
        assert_eq!(api.since, "unreleased");
    }
}

#[test]
fn custom_metrics_are_a_shipped_cross_runtime_contract() {
    let expected = [
        ("metrics.counter", &["name:string", "value:u64"][..]),
        ("metrics.gauge", &["name:string", "value:f64"][..]),
        ("metrics.timer", &["name:string", "seconds:f64"][..]),
    ];

    for (name, params) in expected {
        let api = HOST_API_SURFACE
            .iter()
            .find(|entry| entry.name == name)
            .expect("custom metrics API is declared");

        assert_eq!(api.category, HostApiCategory::Metrics);
        assert_eq!(api.params, params);
        assert_eq!(api.returns, "void");
        assert_eq!(api.status, HostApiStatus::Shipped);
        assert_eq!(api.since, "IMPL-20260818-RUNTIME-CUSTOM-METRICS");
    }
}
