//! Integration coverage for Python and JavaScript static-game-data parity.

#![allow(clippy::panic, clippy::unwrap_used)]

#[cfg(feature = "runtime-python")]
mod python {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use citadel::error::ErrorCategory;
    use citadel::runtime::{PythonRuntime, ReloadOutcome, RpcOutcome};

    struct TempGame {
        root: PathBuf,
    }

    impl TempGame {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "citadel-python-static-data-{label}-{}-{n}",
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
            std::fs::write(self.scripts_dir().join("main.py"), source).expect("write main.py");
        }

        fn write_data(&self, relative: impl AsRef<Path>, bytes: &[u8]) {
            let path = self.data_dir().join(relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create data parent");
            }
            std::fs::write(path, bytes).expect("write static data");
        }

        fn load(&self, max_file_bytes: usize) -> PythonRuntime {
            PythonRuntime::load_with_static_data(
                &self.scripts_dir(),
                100,
                Some(&self.data_dir()),
                max_file_bytes,
            )
            .expect("runtime loads")
            .expect("main.py exists")
        }
    }

    impl Drop for TempGame {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn initialization_receives_parsed_cached_json_and_csv() {
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
import citadel

collision = citadel.static_data.load_json("gameplay/collision.json")
balance = citadel.static_data.load_csv("gameplay/balance.csv")

@citadel.on_rpc("summary")
def summary(ctx, body):
    return f"{collision['character']['radius']}:{collision['balloon']['offset_y']}:{balance[0]['damage']}:{balance[0]['enabled']}"

@citadel.on_rpc("cached")
def cached(ctx, body):
    return str(citadel.static_data.load_json("gameplay/collision.json")['character']['radius'])
"#,
        );
        let runtime = game.load(1024);

        assert_eq!(
            runtime.call_rpc(1, None, "summary", b""),
            RpcOutcome::Ok(b"42:18:12:True".to_vec())
        );
        game.write_data("gameplay/collision.json", br#"{"character":{"radius":99}}"#);
        assert_eq!(
            runtime.call_rpc(1, None, "cached", b""),
            RpcOutcome::Ok(b"42".to_vec()),
            "a sealed cache hit must not reread the file"
        );
    }

    #[test]
    fn reports_static_data_denials_limits_and_parse_schema_failures() {
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
            if !matches!(label, "missing" | "traversal" | "absolute") {
                game.write_data(requested, bytes);
            }
            let loader = if requested.ends_with(".csv") {
                "load_csv"
            } else {
                "load_json"
            };
            game.write_script(&format!(
                "import citadel\nvalue = citadel.static_data.{loader}(\"{requested}\")\n@citadel.on_message(1)\ndef handler(ctx, body):\n    pass\n"
            ));
            let error = PythonRuntime::load_with_static_data(
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
            "import citadel\nvalue = citadel.static_data.load_json(\"anything.json\")\n@citadel.on_message(1)\ndef handler(ctx, body):\n    pass\n",
        );
        let error = PythonRuntime::load(&unconfigured.scripts_dir(), 100)
            .expect_err("unconfigured static data must be denied");
        assert!(error.operator_log().contains("not configured"));
    }

    #[test]
    fn static_data_hot_reload_is_atomic() {
        let game = TempGame::new("reload");
        game.write_data("gameplay/collision.json", br#"{"character":{"radius":10}}"#);
        game.write_script(
            r#"
import citadel
collision = citadel.static_data.load_json("gameplay/collision.json")
@citadel.on_rpc("radius")
def radius(ctx, body):
    return str(collision["character"]["radius"])
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
}

#[cfg(feature = "runtime-js")]
mod javascript {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use citadel::error::ErrorCategory;
    use citadel::runtime::{JsRuntime, ReloadOutcome, RpcOutcome};

    struct TempGame {
        root: PathBuf,
    }

    impl TempGame {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "citadel-javascript-static-data-{label}-{}-{n}",
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
            std::fs::write(self.scripts_dir().join("main.js"), source).expect("write main.js");
        }

        fn write_data(&self, relative: impl AsRef<Path>, bytes: &[u8]) {
            let path = self.data_dir().join(relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create data parent");
            }
            std::fs::write(path, bytes).expect("write static data");
        }

        fn load(&self, max_file_bytes: usize) -> JsRuntime {
            JsRuntime::load_with_static_data(
                &self.scripts_dir(),
                100,
                Some(&self.data_dir()),
                max_file_bytes,
            )
            .expect("runtime loads")
            .expect("main.js exists")
        }
    }

    impl Drop for TempGame {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn initialization_receives_parsed_cached_json_and_csv() {
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
const collision = citadel.static_data.load_json("gameplay/collision.json");
const balance = citadel.static_data.load_csv("gameplay/balance.csv");
citadel.on_rpc("summary", () => `${collision.character.radius}:${collision.balloon.offset_y}:${balance[0].damage}:${balance[0].enabled}`);
citadel.on_rpc("cached", () => String(citadel.static_data.load_json("gameplay/collision.json").character.radius));
"#,
        );
        let runtime = game.load(1024);

        assert_eq!(
            runtime.call_rpc(1, None, "summary", b""),
            RpcOutcome::Ok(b"42:18:12:true".to_vec())
        );
        game.write_data("gameplay/collision.json", br#"{"character":{"radius":99}}"#);
        assert_eq!(
            runtime.call_rpc(1, None, "cached", b""),
            RpcOutcome::Ok(b"42".to_vec()),
            "a sealed cache hit must not reread the file"
        );
    }

    #[test]
    fn reports_static_data_denials_limits_and_parse_schema_failures() {
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
            if !matches!(label, "missing" | "traversal" | "absolute") {
                game.write_data(requested, bytes);
            }
            let loader = if requested.ends_with(".csv") {
                "load_csv"
            } else {
                "load_json"
            };
            game.write_script(&format!(
                "const value = citadel.static_data.{loader}(\"{requested}\");\ncitadel.on_message(1, () => {{}});\n"
            ));
            let error = JsRuntime::load_with_static_data(
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
            "const value = citadel.static_data.load_json(\"anything.json\");\ncitadel.on_message(1, () => {});\n",
        );
        let error = JsRuntime::load(&unconfigured.scripts_dir(), 100)
            .expect_err("unconfigured static data must be denied");
        assert!(error.operator_log().contains("not configured"));
    }

    #[test]
    fn static_data_hot_reload_is_atomic() {
        let game = TempGame::new("reload");
        game.write_data("gameplay/collision.json", br#"{"character":{"radius":10}}"#);
        game.write_script(
            r#"
const collision = citadel.static_data.load_json("gameplay/collision.json");
citadel.on_rpc("radius", () => String(collision.character.radius));
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
}

#[cfg(not(any(feature = "runtime-python", feature = "runtime-js")))]
#[test]
fn static_data_parity_skips_without_embedded_runtime_features() {
    eprintln!("static-data parity skipped: build lacks Python and JavaScript runtime features");
}
