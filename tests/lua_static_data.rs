//! Integration coverage for the restricted Lua static-game-data contract.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use citadel::error::ErrorCategory;
use citadel::lifecycle::Supervisor;
use citadel::realtime::LuaReloadService;
use citadel::runtime::{LuaRuntime, ReloadOutcome, RpcOutcome, Runtime};

struct TempGame {
    root: PathBuf,
}

impl TempGame {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "citadel-lua-static-data-{label}-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(root.join("game")).expect("create script dir");
        std::fs::create_dir_all(root.join("common")).expect("create data dir");
        Self { root }
    }

    fn scripts_dir(&self) -> PathBuf {
        self.root.join("game")
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("common")
    }

    fn write_script(&self, source: &str) {
        std::fs::write(self.scripts_dir().join("main.lua"), source).expect("write main.lua");
    }

    fn write_data(&self, relative: impl AsRef<Path>, bytes: &[u8]) {
        let path = self.data_dir().join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create data parent");
        }
        std::fs::write(path, bytes).expect("write static data");
    }

    fn load(&self, max_file_bytes: usize) -> LuaRuntime {
        LuaRuntime::load_with_static_data(
            &self.scripts_dir(),
            100,
            Some(&self.data_dir()),
            max_file_bytes,
        )
        .expect("runtime loads")
        .expect("main.lua exists")
    }
}

impl Drop for TempGame {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn lua_initialization_receives_parsed_cached_json_and_csv_without_filesystem_capabilities() {
    let game = TempGame::new("valid-cache");
    game.write_data(
        "gameplay/collision.json",
        br#"{"character":{"radius":42},"balloon":{"offset_y":18}}"#,
    );
    game.write_data(
        "gameplay/balance.csv",
        b"id,damage,enabled\nslash,12,true\n",
    );
    game.write_script(
        r#"
        local collision = citadel.static_data.load_json("gameplay/collision.json")
        local balance = citadel.static_data.load_csv("gameplay/balance.csv")

        citadel.on_rpc("summary", function()
            return string.format("%d:%d:%d:%s", collision.character.radius,
                collision.balloon.offset_y, balance[1].damage, tostring(balance[1].enabled))
        end)
        citadel.on_rpc("cached", function()
            local again = citadel.static_data.load_json("gameplay/collision.json")
            return tostring(again.character.radius)
        end)
        citadel.on_rpc("filesystem", function()
            return string.format("%s:%s:%s", tostring(io), tostring(os), tostring(package))
        end)
    "#,
    );
    let runtime = game.load(1024);

    assert_eq!(
        runtime.call_rpc(1, None, "summary", b""),
        RpcOutcome::Ok(b"42:18:12:true".to_vec())
    );
    assert_eq!(
        runtime.call_rpc(1, None, "filesystem", b""),
        RpcOutcome::Ok(b"nil:nil:nil".to_vec())
    );

    // An on-disk mutation does not affect the sealed, initialized catalog and
    // the handler cache hit cannot perform filesystem I/O.
    game.write_data("gameplay/collision.json", br#"{"character":{"radius":99}}"#);
    assert_eq!(
        runtime.call_rpc(1, None, "cached", b""),
        RpcOutcome::Ok(b"42".to_vec())
    );
}

#[test]
fn lua_reports_static_data_denials_limits_and_parse_schema_failures() {
    let cases: [(&str, &str, &[u8], usize, &str); 7] = [
        ("missing", "missing.json", b"{}", 1024, "file not found"),
        ("traversal", "../escape.json", b"{}", 1024, "access denied"),
        ("absolute", "/secret.json", b"{}", 1024, "access denied"),
        ("invalid-json", "broken.json", b"{", 1024, "invalid JSON"),
        ("json-schema", "scalar.json", b"42", 1024, "schema invalid"),
        (
            "csv-schema",
            "rows.csv",
            b"id,id\na,b\n",
            1024,
            "schema invalid",
        ),
        (
            "too-large",
            "large.json",
            br#"{"v":"more than sixteen bytes"}"#,
            16,
            "size limit",
        ),
    ];

    for (label, requested, bytes, max_file_bytes, expected) in cases {
        let game = TempGame::new(label);
        if label != "missing" && label != "traversal" && label != "absolute" {
            game.write_data(requested, bytes);
        }
        let loader = if requested.ends_with(".csv") {
            "load_csv"
        } else {
            "load_json"
        };
        game.write_script(&format!(
            "local value = citadel.static_data.{loader}(\"{requested}\")\n             citadel.on_message(1, function() end)"
        ));
        let error = LuaRuntime::load_with_static_data(
            &game.scripts_dir(),
            100,
            Some(&game.data_dir()),
            max_file_bytes,
        )
        .expect_err("invalid static-data initialization must fail");
        assert_eq!(error.category(), ErrorCategory::Runtime, "{label}");
        assert!(
            error.operator_log().contains(expected),
            "{label}: {error:?}"
        );
    }

    let unconfigured = TempGame::new("unconfigured");
    unconfigured.write_script(
        "local value = citadel.static_data.load_json(\"anything.json\")\n         citadel.on_message(1, function() end)",
    );
    let error = LuaRuntime::load(&unconfigured.scripts_dir(), 100)
        .expect_err("unconfigured static data must be denied");
    assert!(error.operator_log().contains("not configured"));
}

#[test]
fn static_data_hot_reload_is_atomic_and_keeps_previous_values_on_failure() {
    let game = TempGame::new("reload");
    game.write_data("gameplay/collision.json", br#"{"character":{"radius":10}}"#);
    game.write_script(
        r#"
        local collision = citadel.static_data.load_json("gameplay/collision.json")
        citadel.on_rpc("radius", function() return tostring(collision.character.radius) end)
    "#,
    );
    let runtime = game.load(1024);
    assert_eq!(
        runtime.call_rpc(1, None, "radius", b""),
        RpcOutcome::Ok(b"10".to_vec())
    );
    assert!(
        runtime
            .reload_watch_paths()
            .iter()
            .any(|path| path.ends_with("collision.json")),
        "a loaded static-data file becomes a reload dependency"
    );

    game.write_data("gameplay/collision.json", br#"{"character":{"radius":25}}"#);
    assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
    assert_eq!(
        runtime.call_rpc(1, None, "radius", b""),
        RpcOutcome::Ok(b"25".to_vec())
    );

    game.write_data("gameplay/collision.json", b"{");
    assert_eq!(runtime.reload(), ReloadOutcome::Rejected);
    assert_eq!(
        runtime.call_rpc(1, None, "radius", b""),
        RpcOutcome::Ok(b"25".to_vec()),
        "a rejected replacement keeps the previous VM and parsed static data"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_reload_watcher_reloads_loaded_data_files_without_message_or_tick_io() {
    let game = TempGame::new("reload-watcher");
    game.write_data("gameplay/collision.json", br#"{"character":{"radius":10}}"#);
    game.write_script(
        r#"
        local collision = citadel.static_data.load_json("gameplay/collision.json")
        citadel.on_rpc("radius", function() return tostring(collision.character.radius) end)
    "#,
    );
    let runtime = Arc::new(game.load(1024));
    let runtime_for_watcher: Arc<dyn Runtime> = runtime.clone();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(LuaReloadService::new(
        runtime_for_watcher,
        game.scripts_dir().join("main.lua"),
        Duration::from_millis(20),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;

    game.write_data("gameplay/collision.json", br#"{"character":{"radius":31}}"#);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if runtime.call_rpc(1, None, "radius", b"") == RpcOutcome::Ok(b"31".to_vec()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "data watcher timed out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    game.write_data("gameplay/collision.json", b"{");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        runtime.call_rpc(1, None, "radius", b""),
        RpcOutcome::Ok(b"31".to_vec()),
        "an invalid data reload must leave the previous catalog live"
    );
    supervisor.shutdown().await.expect("clean watcher shutdown");
}
