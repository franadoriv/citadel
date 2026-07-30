//! CockroachDB compatibility matrix.
//!
//! CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its
//! Postgres backend (`repository::pg`) over CockroachDB unchanged apart from two
//! dialect forks the backend applies automatically for the `cockroach://` flavor:
//! the `migrations-crdb/` DDL (no `COLLATE "C"`) and skipping
//! `pg_advisory_xact_lock` (CockroachDB does not implement it; its default
//! `SERIALIZABLE` isolation plus the primary-key constraint close the same race).
//!
//! This test drives the SAME storage / identity / session repository contracts
//! the in-memory, SQLite, and Postgres suites assert, now against a real
//! CockroachDB instance, to prove the reuse holds end to end (connect, migrate,
//! CRUD, optimistic-version conflicts, cross-owner permission denials, atomic
//! account creation, bulk session revoke).
//!
//! It is **gated**: without `CITADEL_TEST_COCKROACH_URL` the test skips cleanly,
//! so `bash scripts/check.sh` stays green with no database. Run it locally with:
//!
//! ```text
//! docker compose -f docker-compose.crdb.yml up -d
//! docker compose -f docker-compose.crdb.yml exec crdb \
//!   cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS citadel;"
//! CITADEL_TEST_COCKROACH_URL="postgres://root@localhost:26257/citadel?sslmode=disable" \
//!   cargo test --test cockroachdb_compatibility
//! docker compose -f docker-compose.crdb.yml down -v
//! ```
//!
//! Note the URL scheme: the test rewrites whatever `postgres://` URL is provided
//! to the `cockroach://` scheme so the backend selects the CockroachDB flavor
//! (migrations + advisory-lock skip). Pointing a plain `postgres://` URL at
//! CockroachDB would try to run the PostgreSQL `COLLATE "C"` migrations and fail —
//! that is the documented behavior, not a bug.

use citadel::config::{DatabaseBackend, DatabaseConfig, PgFlavor};
use citadel::error::ErrorCategory;
use citadel::identity::{AccountState, AuthCredential, AuthIdentity, DeviceId, User, Username};
use citadel::repository::{Backend, BackendKind, PgDatabase};
use citadel::session::{NodeId, RevocationReason, Session, SessionId, SessionTokenRef};
use citadel::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, Precondition, StorageIndexDefinition,
    StorageIndexField, StorageIndexName, StorageIndexQuery, StorageValue, UserId, WriteRequest,
};
use citadel::time::TimestampMillis;
use serde_json::json;

/// Read the CockroachDB test URL, normalizing its scheme to `cockroach://` so the
/// backend selects the CockroachDB dialect flavor. `None` (unset/blank) skips.
fn test_cockroach_url() -> Option<String> {
    let raw = std::env::var("CITADEL_TEST_COCKROACH_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())?;
    let raw = raw.trim();
    // Accept a plain postgres:// URL for convenience and re-flag it as the
    // CockroachDB flavor; leave an explicit cockroach://-scheme URL untouched.
    let normalized = raw
        .strip_prefix("postgresql://")
        .map(|rest| format!("cockroach://{rest}"))
        .or_else(|| {
            raw.strip_prefix("postgres://")
                .map(|rest| format!("cockroach://{rest}"))
        })
        .unwrap_or_else(|| raw.to_string());
    Some(normalized)
}

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn user_id(id: &str) -> UserId {
    UserId::new(id).expect("valid user id")
}

fn object_id(owner: Owner, collection: &str, key: &str) -> ObjectId {
    ObjectId::new(
        owner,
        Collection::new(collection).expect("collection"),
        Key::new(key).expect("key"),
    )
}

fn value(score: i64) -> StorageValue {
    StorageValue::new(json!({ "score": score })).expect("value")
}

fn sample_user(id: &str, username: &str) -> User {
    User::new(
        user_id(id),
        Username::new(username).expect("username"),
        None,
        None,
        ts(100),
        ts(100),
        AccountState::Active,
    )
    .expect("user")
}

fn device_identity(device: &str, uid: &str) -> AuthIdentity {
    AuthIdentity::new(
        AuthCredential::Device(DeviceId::new(device).expect("device")),
        user_id(uid),
        ts(100),
        ts(100),
    )
    .expect("identity")
}

fn sample_session(id: &str, uid: &str, token: &str) -> Session {
    Session::new(
        SessionId::new(id).expect("sid"),
        user_id(uid),
        NodeId::new("node-crdb").expect("node"),
        ts(100),
        ts(200),
        Some(ts(400)),
        Some(SessionTokenRef::new(token).expect("token ref")),
    )
    .expect("session")
}

async fn connect() -> Option<PgDatabase> {
    let url = test_cockroach_url()?;
    // A cockroach:// URL classifies as the Postgres backend, CockroachDB flavor.
    let config = DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    };
    assert_eq!(
        config.backend().expect("classify"),
        Some(DatabaseBackend::Postgres),
        "a cockroach:// URL selects the Postgres backend"
    );
    assert_eq!(
        config.pg_flavor(),
        PgFlavor::Cockroach,
        "a cockroach:// URL selects the CockroachDB dialect flavor"
    );
    let db = PgDatabase::connect(&config)
        .await
        .expect("connect + migrate against the test CockroachDB");
    Some(db)
}

/// The whole CockroachDB compatibility matrix, driven through ONE connection.
///
/// The sub-contracts run sequentially in a single `#[tokio::test]` (mirroring the
/// SQLite identity/session suite) rather than as separate tests: separate tests
/// would connect and run migrations concurrently, and because CockroachDB
/// migration locking is disabled (it has no advisory locks), concurrent migrators
/// would collide on the `_sqlx_migrations` bookkeeping. One connection also keeps
/// the per-scenario `reset` from racing schema state.
#[tokio::test]
async fn cockroach_backend_compatibility_matrix() {
    let Some(db) = connect().await else {
        eprintln!("skipping CockroachDB compatibility: set CITADEL_TEST_COCKROACH_URL to run it");
        return;
    };

    // Connect + migrate succeeded; the backend reports the CockroachDB kind.
    assert_eq!(
        Backend::kind(&db),
        BackendKind::Cockroach,
        "the CockroachDB flavor reports its own backend kind"
    );
    assert_eq!(BackendKind::Cockroach.as_str(), "cockroach");

    db.reset_storage_for_tests().await.expect("reset");
    cockroach_storage_repository_matches_the_contract(&db).await;

    db.reset_storage_for_tests().await.expect("reset");
    cockroach_storage_index_migration_survives_reconnect(&db).await;

    db.reset_storage_for_tests().await.expect("reset");
    cockroach_identity_and_session_repositories_match_the_contract(&db).await;

    db.reset_storage_for_tests().await.expect("reset");
    cockroach_account_creation_unit_of_work_is_atomic(&db).await;
}

/// Storage-index DDL is part of the Cockroach migration set. Exercise both the
/// ordinary write path (which reads index definitions) and durable projection
/// query, then reconnect so an existing database re-runs migrations idempotently.
async fn cockroach_storage_index_migration_survives_reconnect(db: &PgDatabase) {
    let repo = db.storage_repository();
    let alice = user_id("index-alice");
    let object = object_id(Owner::user(alice.clone()), "profiles", "primary");

    repo.write(
        &Accessor::User(alice.clone()),
        WriteRequest::upsert(object.clone(), value(42), Permissions::public_read()),
    )
    .await
    .expect("storage write reads the migrated index definitions table");

    let index = StorageIndexDefinition::new(
        StorageIndexName::new("profiles_by_score").expect("index name"),
        Collection::new("profiles").expect("collection"),
        None,
        vec![StorageIndexField::new("score").expect("field")],
    )
    .expect("index definition");
    repo.install_index(&index)
        .await
        .expect("install index projection");
    let filters = json!({"score": 42});
    let query = StorageIndexQuery::from_json_filters(
        index.clone(),
        filters.as_object().expect("object filters"),
        10,
    )
    .expect("index query");
    let results = repo
        .query_index(&Accessor::Runtime, &query)
        .await
        .expect("query installed index");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, object);

    let reconnected = connect().await.expect("CockroachDB still configured");
    let results = reconnected
        .storage_repository()
        .query_index(&Accessor::Runtime, &query)
        .await
        .expect("query after idempotent reconnect migration");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, object);
}

/// The storage repository behaves identically on CockroachDB: owner CRUD,
/// optimistic-version conflicts, cross-owner permission denials, and the
/// create-only precondition (which relies on the primary-key/isolation guard the
/// advisory lock is skipped in favor of).
async fn cockroach_storage_repository_matches_the_contract(db: &PgDatabase) {
    let repo = db.storage_repository();

    let alice = user_id("alice");
    let object = object_id(Owner::user(alice.clone()), "saves", "slot-1");

    // Owner write + read round-trip.
    let v1 = repo
        .write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(object.clone(), value(7), Permissions::owner_private()),
        )
        .await
        .expect("owner writes own object");
    let read = repo
        .read(&Accessor::User(alice.clone()), &object)
        .await
        .expect("read ok")
        .expect("present");
    assert_eq!(read.version, v1.version);
    assert_eq!(read.value.as_json(), &json!({ "score": 7 }));

    // Optimistic-version match then stale-version conflict.
    let v2 = repo
        .write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(object.clone(), value(8), Permissions::owner_private())
                .expecting(Precondition::Match(v1.version.clone())),
        )
        .await
        .expect("matching version write");
    assert_ne!(v1.version, v2.version);
    let stale = repo
        .write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(object.clone(), value(9), Permissions::owner_private())
                .expecting(Precondition::Match(v1.version)),
        )
        .await
        .expect_err("stale version conflicts");
    assert_eq!(stale.category(), ErrorCategory::Conflict);

    // Cross-owner create is denied.
    let denied = repo
        .write(
            &Accessor::User(user_id("mallory")),
            WriteRequest::upsert(
                object_id(Owner::user(user_id("bob")), "saves", "slot-x"),
                value(1),
                Permissions::owner_private(),
            ),
        )
        .await
        .expect_err("cross-owner create denied");
    assert_eq!(denied.category(), ErrorCategory::Permission);

    // Create-only precondition rejects a duplicate (advisory lock skipped; the
    // primary key + SERIALIZABLE isolation enforce single-creation on CRDB).
    let cfg = object_id(Owner::System, "config", "global");
    repo.write(
        &Accessor::Runtime,
        WriteRequest::upsert(cfg.clone(), value(1), Permissions::runtime_only())
            .expecting(Precondition::MustNotExist),
    )
    .await
    .expect("first create");
    let dup = repo
        .write(
            &Accessor::Runtime,
            WriteRequest::upsert(cfg.clone(), value(2), Permissions::runtime_only())
                .expecting(Precondition::MustNotExist),
        )
        .await
        .expect_err("duplicate create rejected");
    assert_eq!(dup.category(), ErrorCategory::Conflict);

    // Delete with matching version, then a versioned delete of the now-absent
    // object conflicts.
    let current = repo
        .read(&Accessor::Runtime, &cfg)
        .await
        .expect("read ok")
        .expect("present");
    repo.delete(
        &Accessor::Runtime,
        &cfg,
        Precondition::Match(current.version.clone()),
    )
    .await
    .expect("delete with matching version");
    let missing = repo
        .delete(
            &Accessor::Runtime,
            &cfg,
            Precondition::Match(current.version),
        )
        .await
        .expect_err("versioned delete of missing object conflicts");
    assert_eq!(missing.category(), ErrorCategory::Conflict);
}

/// Identity + session repositories and the duplicate-link conflict behave
/// identically on CockroachDB.
async fn cockroach_identity_and_session_repositories_match_the_contract(db: &PgDatabase) {
    let users = db.user_repository();
    let identities = db.auth_identity_repository();
    let sessions = db.session_repository();

    // Create user, fetch by id and by username.
    users
        .create_user(sample_user("u-crdb-1", "crdb_alice"))
        .await
        .expect("create user");
    assert!(
        users
            .get_user(&user_id("u-crdb-1"))
            .await
            .expect("get")
            .is_some()
    );
    assert!(
        users
            .get_user_by_username(&Username::new("crdb_alice").expect("username"))
            .await
            .expect("get by username")
            .is_some()
    );

    // Link an auth identity; a duplicate link is an idempotent no-op re-link.
    identities
        .link_auth_identity(device_identity("dev-crdb-1", "u-crdb-1"))
        .await
        .expect("link identity");
    identities
        .link_auth_identity(device_identity("dev-crdb-1", "u-crdb-1"))
        .await
        .expect("idempotent re-link");
    assert_eq!(
        identities
            .list_auth_identities(&user_id("u-crdb-1"))
            .await
            .expect("list")
            .len(),
        1
    );

    // Create sessions and bulk-revoke them.
    sessions
        .create_session(sample_session("s-crdb-1", "u-crdb-1", "tok-crdb-1"))
        .await
        .expect("create session 1");
    sessions
        .create_session(sample_session("s-crdb-2", "u-crdb-1", "tok-crdb-2"))
        .await
        .expect("create session 2");
    let revoked = sessions
        .revoke_user_sessions(&user_id("u-crdb-1"), ts(500), RevocationReason::Logout)
        .await
        .expect("bulk revoke");
    assert_eq!(revoked, 2, "both active sessions revoked");
}

/// A create-user-then-link-identity workflow driven through one CockroachDB unit
/// of work is atomic: rolling it back leaves no partial account.
async fn cockroach_account_creation_unit_of_work_is_atomic(db: &PgDatabase) {
    // Commit path: user + identity both persist.
    let uow = db.begin().await.expect("begin");
    uow.user_repository()
        .create_user(sample_user("u-crdb-uow", "crdb_uow"))
        .await
        .expect("create user in tx");
    uow.auth_identity_repository()
        .link_auth_identity(device_identity("dev-crdb-uow", "u-crdb-uow"))
        .await
        .expect("link in tx");
    uow.commit().await.expect("commit");
    assert!(
        db.user_repository()
            .get_user(&user_id("u-crdb-uow"))
            .await
            .expect("get")
            .is_some(),
        "committed account is visible"
    );

    // Rollback path: nothing persists.
    let uow = db.begin().await.expect("begin");
    uow.user_repository()
        .create_user(sample_user("u-crdb-rollback", "crdb_rollback"))
        .await
        .expect("create user in tx");
    uow.rollback().await.expect("rollback");
    assert!(
        db.user_repository()
            .get_user(&user_id("u-crdb-rollback"))
            .await
            .expect("get")
            .is_none(),
        "rolled-back account must not persist"
    );
}
