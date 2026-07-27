//! Standalone self-bootstrap integration tests.
//!
//! These assert the drop-and-run startup contract exercised by `citadel.exe`
//! when it is unzipped next to an editable `citadel.toml`, an (initially absent)
//! `data.sqlite`, and an (initially absent) `game/` folder:
//!
//! - [`App::bootstrap`] on a fresh SQLite URL creates the database file, applies
//!   the embedded migrations, and yields a backend whose storage repository
//!   round-trips a real object — proving the file is usable with no manual
//!   `db-migrate` step.
//! - [`App::bootstrap`] creates a missing runtime scripts directory (`game/`)
//!   so an operator can drop a `main.lua` in later.
//! - default-config discovery loads `./citadel.toml` when present and falls back
//!   to the built-in defaults otherwise, without changing explicit `--config`.
//!
//! SQLite is embedded, so these run un-gated in `scripts/check.sh` (no Docker,
//! no external database).

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use citadel::App;
use citadel::config::{
    Config, DEFAULT_CONFIG_FILE, DatabaseConfig, RuntimeConfig, discover_config_in,
};
use citadel::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, StorageValue, UserId, WriteRequest,
};
use serde_json::json;

/// A process/time-unique temp directory path (not created).
fn unique_temp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("citadel-it-{tag}-{}-{nanos}", std::process::id()))
}

#[tokio::test]
async fn bootstrap_creates_sqlite_file_game_dir_and_round_trips() {
    let base = unique_temp_dir("bootstrap");
    std::fs::create_dir_all(&base).expect("temp base dir");
    let db_path = base.join("data.sqlite");
    let game_dir = base.join("game");

    // A fresh, non-existent database file and no game/ directory yet.
    assert!(!db_path.exists(), "db file must not exist before bootstrap");
    assert!(
        !game_dir.exists(),
        "game dir must not exist before bootstrap"
    );

    // Use the bare-path form for the DB URL so an absolute Windows path (with a
    // drive letter and backslashes) is handled verbatim by the SQLite backend.
    let config = Config {
        database: DatabaseConfig {
            url: Some(db_path.to_string_lossy().into_owned()),
            ..DatabaseConfig::default()
        },
        runtime: RuntimeConfig {
            scripts_dir: game_dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        },
        ..Config::default()
    };

    let app = App::bootstrap(config)
        .await
        .expect("standalone bootstrap on a fresh SQLite file");

    // The database file was created and the runtime scripts dir was created.
    assert!(db_path.is_file(), "bootstrap creates the SQLite file");
    assert!(game_dir.is_dir(), "bootstrap creates the game/ scripts dir");
    assert_eq!(app.backend_kind(), citadel::repository::BackendKind::Sqlite);

    // Migrations were applied on startup: the storage repository round-trips a
    // real object with no manual migration step.
    let repo = app.backend().storage_repository();
    let alice = UserId::new("alice").expect("user id");
    let object = ObjectId::new(
        Owner::user(alice.clone()),
        Collection::new("saves").expect("collection"),
        Key::new("slot-1").expect("key"),
    );
    let request = WriteRequest::upsert(
        object.clone(),
        StorageValue::new(json!({ "score": 42 })).expect("value"),
        Permissions::owner_private(),
    );
    let written = repo
        .write(&Accessor::User(alice.clone()), request)
        .await
        .expect("write to fresh sqlite storage");
    let read = repo
        .read(&Accessor::User(alice), &object)
        .await
        .expect("read ok")
        .expect("object present after write");
    assert_eq!(read.version, written.version);
    assert_eq!(read.value.as_json(), &json!({ "score": 42 }));

    drop(app);
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn bootstrap_defaults_to_in_memory_and_still_creates_game_dir() {
    // No database URL => the in-memory backend, but the scripts dir is still
    // bootstrapped for the drop-and-run flow.
    let base = unique_temp_dir("bootstrap-mem");
    let game_dir = base.join("game");
    let config = Config {
        runtime: RuntimeConfig {
            scripts_dir: game_dir.to_string_lossy().into_owned(),
            ..RuntimeConfig::default()
        },
        ..Config::default()
    };

    let app = App::bootstrap(config).await.expect("bootstrap in-memory");
    assert_eq!(
        app.backend_kind(),
        citadel::repository::BackendKind::InMemory
    );
    assert!(
        game_dir.is_dir(),
        "game dir created even on the in-memory path"
    );

    drop(app);
    std::fs::remove_dir_all(&base).ok();
}

#[test]
fn default_config_discovery_loads_present_file_and_falls_back() {
    // An empty temp dir discovers nothing.
    let dir = unique_temp_dir("discovery");
    std::fs::create_dir_all(&dir).expect("temp dir");
    assert!(
        discover_config_in(&dir).is_none(),
        "no citadel.toml => fall back to defaults"
    );

    // Write a citadel.toml selecting SQLite; discovery finds it and it loads.
    let cfg_path = dir.join(DEFAULT_CONFIG_FILE);
    std::fs::write(
        &cfg_path,
        r#"
[database]
url = "sqlite://data.sqlite"

[runtime]
enabled = true
hot_reload = true
"#,
    )
    .expect("write citadel.toml");

    let discovered = discover_config_in(&dir).expect("citadel.toml is discovered");
    assert_eq!(discovered, cfg_path);

    let config = Config::from_file(&discovered).expect("discovered config parses");
    assert_eq!(config.database.url.as_deref(), Some("sqlite://data.sqlite"));
    assert!(config.runtime.hot_reload);
    config.validate().expect("discovered config validates");

    std::fs::remove_dir_all(&dir).ok();
}
