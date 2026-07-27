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
