//! Behavioral parity for Rust-owned text-policy catalogs in every embedded runtime.

#![allow(clippy::panic, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

struct TempGame {
    root: PathBuf,
}

impl TempGame {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "citadel-text-policy-{label}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(root.join("game")).expect("create script directory");
        std::fs::create_dir_all(root.join("common")).expect("create data directory");
        Self { root }
    }

    fn scripts_dir(&self) -> PathBuf {
        self.root.join("game")
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("common")
    }

    fn write_data(&self, relative: impl AsRef<Path>, bytes: &[u8]) {
        std::fs::write(self.data_dir().join(relative), bytes).expect("write policy data");
    }
}

impl Drop for TempGame {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

const POLICY: &[u8] = br#"{
  "schema_version": 1,
  "rules": [{
    "id": "bad-word",
    "category": "abuse",
    "severity": "high",
    "terms": ["bad"],
    "match": "whole_word",
    "action": "mask"
  }]
}"#;

#[test]
fn lua_text_policy_load_scan_and_sanitize_use_a_sealed_rust_catalog() {
    use citadel::runtime::{LuaRuntime, RpcOutcome};

    let game = TempGame::new("lua");
    game.write_data("policy.json", POLICY);
    std::fs::write(
        game.scripts_dir().join("main.lua"),
        r#"
local policy = citadel.text_policy.load_json("policy.json")
citadel.on_rpc("summary", function()
    local scan = citadel.text_policy.scan(policy, "BAD actor")
    local clean = citadel.text_policy.sanitize(policy, "BAD actor")
    return scan.decision .. ":" .. scan.text .. ":" .. scan.matches[1].rule_id .. ":" .. scan.matches[1].span.start .. ":" .. clean.text
end)
"#,
    )
    .expect("write Lua script");
    let runtime =
        LuaRuntime::load_with_static_data(&game.scripts_dir(), 100, Some(&game.data_dir()), 1024)
            .expect("runtime loads")
            .expect("main.lua exists");

    game.write_data("policy.json", br#"{"schema_version":1,"rules":[{"id":"changed","category":"abuse","terms":["bad"],"match":"whole_word","action":"reject"}]}"#);
    assert_eq!(
        runtime.call_rpc(1, None, "summary", b""),
        RpcOutcome::Ok(b"mask:BAD actor:bad-word:0:*** actor".to_vec())
    );
}

#[cfg(feature = "runtime-python")]
#[test]
fn python_text_policy_load_scan_and_sanitize_use_a_sealed_rust_catalog() {
    use citadel::runtime::{PythonRuntime, RpcOutcome};

    let game = TempGame::new("python");
    game.write_data("policy.json", POLICY);
    std::fs::write(
        game.scripts_dir().join("main.py"),
        r#"
import citadel
policy = citadel.text_policy.load_json("policy.json")
@citadel.on_rpc("summary")
def summary(ctx, body):
    scan = citadel.text_policy.scan(policy, "BAD actor")
    clean = citadel.text_policy.sanitize(policy, "BAD actor")
    return f"{scan['decision']}:{scan['text']}:{scan['matches'][0]['rule_id']}:{scan['matches'][0]['span']['start']}:{clean['text']}"
"#,
    )
    .expect("write Python script");
    let runtime = PythonRuntime::load_with_static_data(
        &game.scripts_dir(),
        100,
        Some(&game.data_dir()),
        1024,
    )
    .expect("runtime loads")
    .expect("main.py exists");

    game.write_data("policy.json", br#"{"schema_version":1,"rules":[{"id":"changed","category":"abuse","terms":["bad"],"match":"whole_word","action":"reject"}]}"#);
    assert_eq!(
        runtime.call_rpc(1, None, "summary", b""),
        RpcOutcome::Ok(b"mask:BAD actor:bad-word:0:*** actor".to_vec())
    );
}

#[cfg(feature = "runtime-js")]
#[test]
fn javascript_text_policy_load_scan_and_sanitize_use_a_sealed_rust_catalog() {
    use citadel::runtime::{JsRuntime, RpcOutcome};

    let game = TempGame::new("javascript");
    game.write_data("policy.json", POLICY);
    std::fs::write(
        game.scripts_dir().join("main.js"),
        r#"
const policy = citadel.text_policy.load_json("policy.json");
citadel.on_rpc("summary", () => {
  let rejectsNonString = false;
  try {
    citadel.text_policy.scan(policy, 123);
  } catch (_) {
    rejectsNonString = true;
  }
  if (!rejectsNonString) throw new Error("text policy must reject non-string text");
  const scan = citadel.text_policy.scan(policy, "BAD actor");
  const clean = citadel.text_policy.sanitize(policy, "BAD actor");
  return `${scan.decision}:${scan.text}:${scan.matches[0].rule_id}:${scan.matches[0].span.start}:${clean.text}`;
});
"#,
    )
    .expect("write JavaScript script");
    let runtime =
        JsRuntime::load_with_static_data(&game.scripts_dir(), 100, Some(&game.data_dir()), 1024)
            .expect("runtime loads")
            .expect("main.js exists");

    game.write_data("policy.json", br#"{"schema_version":1,"rules":[{"id":"changed","category":"abuse","terms":["bad"],"match":"whole_word","action":"reject"}]}"#);
    assert_eq!(
        runtime.call_rpc(1, None, "summary", b""),
        RpcOutcome::Ok(b"mask:BAD actor:bad-word:0:*** actor".to_vec())
    );
}
