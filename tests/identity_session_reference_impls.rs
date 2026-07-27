//! Reusable contract tests for the identity/session reference implementations
//!.
//!
//! The `*_contract` functions are written against the trait objects, not the
//! concrete in-memory types, so any future backend (e.g. Postgres) can be held
//! to the same behavior by calling them with a fresh instance. The `#[test]`
//! wrappers run them against the in-memory implementations and also exercise the
//! full authentication -> session -> validation stack end to end.

use std::sync::Arc;

use citadel::identity::{
    AccountState, AuthCredential, AuthIdentity, CustomId, DeviceId, User, Username,
};
use citadel::repository::{
    AuthIdentityRepository, Backend, InMemoryAuthIdentityRepository, InMemoryBackend,
    InMemorySessionRepository, InMemoryUserRepository, SessionRepository, UserRepository,
};
use citadel::services::{
    AuthenticationOptions, AuthenticationService, AuthenticationServiceImpl,
    DeviceAuthenticationRequest, InMemorySessionDirectory, InMemorySessionService,
    RefreshSessionRequest, RevokeSessionRequest, SessionDirectory, SharedSessionService,
    ValidateSessionRequest,
};
use citadel::session::{
    NodeId, OwnershipGeneration, ResolveSessionOwnerRequest, RevocationReason, Session,
    SessionDirectoryEntry, SessionId, SessionInvalidity, SessionOwnerLease, SessionOwnership,
    SessionTokenRef,
};
use citadel::storage::UserId;
use citadel::time::{DurationMillis, TimestampMillis};

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn ms(v: u64) -> DurationMillis {
    DurationMillis::from_millis(v)
}

fn session(id: &str, user: &str, token: &str) -> Session {
    Session::new(
        SessionId::new(id).expect("sid"),
        UserId::new(user).expect("uid"),
        NodeId::new("node-a").expect("node"),
        ts(100),
        ts(200),
        Some(ts(400)),
        Some(SessionTokenRef::new(token).expect("ref")),
    )
    .expect("session")
}

/// Reusable contract for any [`SessionRepository`]: create, CAS update, and
/// scoped/idempotent bulk revoke.
async fn session_repository_contract(repo: &dyn SessionRepository) {
    repo.create_session(session("s-1", "u-1", "t-1"))
        .await
        .expect("create s-1");
    repo.create_session(session("s-2", "u-1", "t-2"))
        .await
        .expect("create s-2");

    // Duplicate id conflicts.
    assert!(
        repo.create_session(session("s-1", "u-1", "t-9"))
            .await
            .is_err(),
        "duplicate id must conflict"
    );

    // Compare-and-set: revoke stored, then a stale refresh must not resurrect it.
    let mut stale = repo
        .get_session(&SessionId::new("s-1").expect("test value"))
        .await
        .expect("test value")
        .expect("test value");
    let mut current = repo
        .get_session(&SessionId::new("s-1").expect("test value"))
        .await
        .expect("test value")
        .expect("test value");
    current
        .revoke_at(ts(150), RevocationReason::Logout)
        .expect("test value");
    repo.update_session(current).await.expect("store revoke");
    stale
        .refresh_at(ts(150), ts(500), Some(ts(800)), None)
        .expect("test value");
    assert!(
        repo.update_session(stale).await.is_err(),
        "stale refresh of a revoked session must conflict"
    );

    // Bulk revoke is scoped to the user and idempotent.
    let revoked = repo
        .revoke_user_sessions(
            &UserId::new("u-1").expect("test value"),
            ts(160),
            RevocationReason::Admin,
        )
        .await
        .expect("revoke user");
    assert_eq!(
        revoked, 1,
        "only the remaining active u-1 session is revoked"
    );
    assert_eq!(
        repo.revoke_user_sessions(
            &UserId::new("u-1").expect("test value"),
            ts(170),
            RevocationReason::Admin
        )
        .await
        .expect("revoke again"),
        0
    );
}

/// Reusable contract for any [`SessionDirectory`] whose local node is `node-a`.
async fn session_directory_contract(dir: &dyn SessionDirectory) {
    let entry = |session: &str, node: &str, generation: u64, expires: u64| SessionDirectoryEntry {
        session_id: SessionId::new(session).expect("test value"),
        owner: SessionOwnerLease {
            node_id: NodeId::new(node).expect("test value"),
            generation: OwnershipGeneration::new(generation),
            expires_at: ts(expires),
        },
    };
    let resolve = |session: &str, now: u64| ResolveSessionOwnerRequest {
        session_id: SessionId::new(session).expect("test value"),
        expected: None,
        now: ts(now),
    };

    dir.bind_session_owner(entry("s-1", "node-a", 1, 1_000))
        .await
        .expect("bind local");
    assert_eq!(
        dir.resolve_session_owner(&resolve("s-1", 500))
            .await
            .expect("test value"),
        SessionOwnership::Local
    );

    dir.bind_session_owner(entry("s-2", "node-b", 1, 1_000))
        .await
        .expect("bind remote");
    assert_eq!(
        dir.resolve_session_owner(&resolve("s-2", 500))
            .await
            .expect("test value"),
        SessionOwnership::Remote(NodeId::new("node-b").expect("test value"))
    );

    // Lower generation cannot roll back a live lease.
    assert!(
        dir.bind_session_owner(entry("s-1", "node-b", 1, 1_000))
            .await
            .is_err(),
        "same generation, different owner must conflict"
    );
    // Higher generation transfers ownership.
    dir.bind_session_owner(entry("s-1", "node-b", 2, 1_000))
        .await
        .expect("higher gen transfer");
    assert_eq!(
        dir.resolve_session_owner(&resolve("s-1", 500))
            .await
            .expect("test value"),
        SessionOwnership::Remote(NodeId::new("node-b").expect("test value"))
    );
}

fn uid(value: &str) -> UserId {
    UserId::new(value).expect("valid user id")
}

fn user(id: &str, username: &str) -> User {
    User::new(
        uid(id),
        Username::new(username).expect("username"),
        None,
        None,
        ts(100),
        ts(100),
        AccountState::Active,
    )
    .expect("user")
}

fn device_identity(device: &str, user_id: &str) -> AuthIdentity {
    AuthIdentity::new(
        AuthCredential::Device(DeviceId::new(device).expect("device")),
        uid(user_id),
        ts(100),
        ts(100),
    )
    .expect("identity")
}

fn custom_identity(custom: &str, user_id: &str) -> AuthIdentity {
    AuthIdentity::new(
        AuthCredential::Custom(CustomId::new(custom).expect("custom")),
        uid(user_id),
        ts(100),
        ts(100),
    )
    .expect("identity")
}

/// Reusable contract for any [`UserRepository`]: create/get round trips, unique
/// id + username, immutable `created_at`, username-uniqueness on rename, and
/// state transitions gated on existence.
async fn user_repository_contract(repo: &dyn UserRepository) {
    repo.create_user(user("u-1", "alice"))
        .await
        .expect("create");
    repo.create_user(user("u-2", "bob")).await.expect("create");

    assert_eq!(
        repo.get_user(&uid("u-1"))
            .await
            .expect("get")
            .expect("present")
            .username
            .as_str(),
        "alice"
    );
    assert_eq!(
        repo.get_user_by_username(&Username::new("bob").expect("username"))
            .await
            .expect("get")
            .expect("present")
            .id
            .as_str(),
        "u-2"
    );

    // Duplicate id and duplicate username both conflict.
    assert!(
        repo.create_user(user("u-1", "carol")).await.is_err(),
        "duplicate id must conflict"
    );
    assert!(
        repo.create_user(user("u-3", "alice")).await.is_err(),
        "duplicate username must conflict"
    );

    // Rename u-2 to a free username; the old handle is released.
    let mut renamed = user("u-2", "charlie");
    renamed.updated_at = ts(200);
    repo.update_user(renamed).await.expect("rename ok");
    assert!(
        repo.get_user_by_username(&Username::new("bob").expect("username"))
            .await
            .expect("get")
            .is_none()
    );

    // Renaming to a taken username conflicts.
    assert!(
        repo.update_user(user("u-2", "alice")).await.is_err(),
        "rename to a taken username must conflict"
    );

    // `created_at` is immutable.
    let mut history = user("u-1", "alice");
    history.created_at = ts(999);
    history.updated_at = ts(999);
    assert!(
        repo.update_user(history).await.is_err(),
        "changing created_at must conflict"
    );

    // Updating a missing account is a not-found.
    assert!(
        repo.update_user(user("ghost", "ghost")).await.is_err(),
        "update of a missing user must fail"
    );

    // State transition updates and requires existence.
    let disabled = repo
        .set_user_state(&uid("u-1"), AccountState::Disabled, ts(300))
        .await
        .expect("set state");
    assert_eq!(disabled.state, AccountState::Disabled);
    assert_eq!(disabled.updated_at, ts(300));
    assert_eq!(disabled.created_at, ts(100));
    assert!(
        repo.set_user_state(&uid("missing"), AccountState::Disabled, ts(300))
            .await
            .is_err(),
        "state transition on a missing user must fail"
    );
}

/// Reusable contract for any [`AuthIdentityRepository`]: one-credential-to-one-
/// account, idempotent re-link, deterministic listing, and idempotent unlink.
async fn auth_identity_repository_contract(repo: &dyn AuthIdentityRepository) {
    let device = AuthCredential::Device(DeviceId::new("d-1").expect("device"));

    repo.link_auth_identity(device_identity("d-1", "u-1"))
        .await
        .expect("link");
    // Re-linking the same pair is idempotent.
    repo.link_auth_identity(device_identity("d-1", "u-1"))
        .await
        .expect("idempotent re-link");
    // Linking the same credential to a different account conflicts.
    assert!(
        repo.link_auth_identity(device_identity("d-1", "u-2"))
            .await
            .is_err(),
        "credential already linked to another account must conflict"
    );

    assert_eq!(
        repo.get_auth_identity(&device)
            .await
            .expect("get")
            .expect("present")
            .user_id
            .as_str(),
        "u-1"
    );

    // A second credential for the same account; the listing is deterministic
    // (custom before device).
    repo.link_auth_identity(custom_identity("c-1", "u-1"))
        .await
        .expect("link custom");
    let list = repo.list_auth_identities(&uid("u-1")).await.expect("list");
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].provider().as_str(), "custom");
    assert_eq!(list[1].provider().as_str(), "device");

    // Unlink is idempotent.
    repo.unlink_auth_identity(&device).await.expect("unlink");
    repo.unlink_auth_identity(&device)
        .await
        .expect("idempotent unlink");
    assert!(
        repo.get_auth_identity(&device)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn in_memory_user_repository_meets_contract() {
    user_repository_contract(&InMemoryUserRepository::new()).await;
}

#[tokio::test]
async fn in_memory_auth_identity_repository_meets_contract() {
    auth_identity_repository_contract(&InMemoryAuthIdentityRepository::new()).await;
}

#[tokio::test]
async fn in_memory_session_repository_meets_contract() {
    session_repository_contract(&InMemorySessionRepository::new()).await;
}

#[tokio::test]
async fn in_memory_session_directory_meets_contract() {
    session_directory_contract(&InMemorySessionDirectory::new(
        NodeId::new("node-a").expect("test value"),
    ))
    .await;
}

/// Compose the identity/session services over `backend` exactly as the node
/// does (`App::build_identity_services`): a session service over the backend's
/// session repository, and the concrete authentication service over the backend.
fn auth_stack_over(
    backend: Arc<dyn Backend>,
) -> (Arc<dyn AuthenticationService>, SharedSessionService) {
    let sessions: SharedSessionService = Arc::new(InMemorySessionService::with_default_issuer(
        backend.session_repository(),
    ));
    let auth: Arc<dyn AuthenticationService> = Arc::new(AuthenticationServiceImpl::new(
        Arc::clone(&backend),
        Arc::clone(&sessions),
    ));
    (auth, sessions)
}

fn device_options_named(create: bool, username: &str) -> AuthenticationOptions {
    AuthenticationOptions {
        create_account: create,
        username: Some(Username::new(username).expect("username")),
        display_name: None,
        metadata: None,
        now: ts(1_000),
        owner_node: NodeId::new("node-a").expect("node"),
        session_ttl: ms(1_000),
        refresh_ttl: Some(ms(5_000)),
    }
}

/// Reusable service-level contract: register via device auth (account created in
/// one unit of work), then validate/refresh/revoke through the session service.
/// `device`/`username` are parameters so the same contract can run repeatedly
/// against one shared (Postgres) backend without id/username collisions.
async fn full_stack_register_validate_refresh_revoke_contract(
    backend: Arc<dyn Backend>,
    device: &str,
    username: &str,
) {
    let (auth, sessions) = auth_stack_over(backend);

    let outcome = auth
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("device"),
            options: device_options_named(true, username),
        })
        .await
        .expect("register");
    assert!(outcome.account_created);
    assert!(outcome.identity_created);

    // The issued access token validates through the session service.
    let validation = sessions
        .validate_session(ValidateSessionRequest {
            access_token: outcome.tokens.access.clone(),
            now: ts(1_500),
        })
        .await
        .expect("validate");
    assert!(validation.is_valid());

    // A second auth with the same device reuses the account (no new account).
    let second = auth
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("device"),
            options: device_options_named(false, username),
        })
        .await
        .expect("login");
    assert!(!second.account_created);
    assert_eq!(second.user.id, outcome.user.id);

    // Refresh via the issued refresh token.
    let refreshed = sessions
        .refresh_session(RefreshSessionRequest {
            refresh_token: outcome.tokens.refresh.clone().expect("refreshable"),
            now: ts(1_500),
            owner_node: NodeId::new("node-a").expect("test value"),
            session_ttl: ms(1_000),
            refresh_ttl: Some(ms(5_000)),
        })
        .await
        .expect("refresh");

    // Revoke and confirm the token no longer validates.
    sessions
        .revoke_session(RevokeSessionRequest {
            session_id: refreshed.session.id.clone(),
            revoked_at: ts(1_700),
            reason: RevocationReason::Logout,
        })
        .await
        .expect("revoke");
    let after = sessions
        .validate_session(ValidateSessionRequest {
            access_token: refreshed.tokens.access,
            now: ts(1_800),
        })
        .await
        .expect("validate revoked");
    assert_eq!(after.invalidity(), Some(SessionInvalidity::Revoked));
}

/// Reusable service-level contract: an unknown credential and a disabled account
/// yield the identical sanitized error (no credential-existence oracle).
async fn disabled_and_unknown_indistinguishable_contract(
    backend: Arc<dyn Backend>,
    device: &str,
    username: &str,
) {
    let (auth, _sessions) = auth_stack_over(Arc::clone(&backend));

    let unknown = auth
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new("ghost-unknown").expect("test value"),
            options: device_options_named(false, username),
        })
        .await
        .expect_err("unknown");

    let registered = auth
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("test value"),
            options: device_options_named(true, username),
        })
        .await
        .expect("register");
    backend
        .user_repository()
        .set_user_state(&registered.user.id, AccountState::Disabled, ts(2_000))
        .await
        .expect("disable");
    let disabled = auth
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("test value"),
            options: device_options_named(false, username),
        })
        .await
        .expect_err("disabled");

    // Same sanitized message: no credential-existence oracle.
    assert_eq!(unknown.to_string(), disabled.to_string());
}

#[tokio::test]
async fn full_stack_register_validate_refresh_revoke() {
    full_stack_register_validate_refresh_revoke_contract(
        Arc::new(InMemoryBackend::new()),
        "device-1",
        "player",
    )
    .await;
}

#[tokio::test]
async fn disabled_account_and_unknown_credential_are_indistinguishable() {
    disabled_and_unknown_indistinguishable_contract(
        Arc::new(InMemoryBackend::new()),
        "device-1",
        "player",
    )
    .await;
}

// --- SQLite run (always; embedded, no server) -------------------------------
//
// Runs the SAME reusable identity/session contracts against a real SQLite
// backend. SQLite is embedded, so — unlike Postgres — this run is UN-gated: it
// exercises a real SQL backend on every `bash scripts/check.sh`, including that a
// create-user-then-link-identity workflow driven through one `SqliteUnitOfWork`
// is atomic (all-or-nothing). An in-memory database keeps it hermetic (the
// provider forces a single connection so every statement sees the same database).
mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    async fn connect() -> SqliteDatabase {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate against an in-memory SQLite database")
    }

    /// One test drives every contract sequentially against a shared in-memory
    /// database. Running as a single `#[tokio::test]` keeps the reset and the
    /// per-contract fixtures from racing over the same tables (and keeps the
    /// single-connection in-memory database alive for the whole run).
    #[tokio::test]
    async fn sqlite_backend_satisfies_identity_session_contracts() {
        let db = connect().await;

        db.reset_storage_for_tests().await.expect("reset");
        user_repository_contract(db.user_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        auth_identity_repository_contract(db.auth_identity_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        session_repository_contract(db.session_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        account_creation_is_atomic(&db).await;

        // Service-level contracts: the SAME contracts the in-memory backend runs,
        // now driven end to end against SQLite through the concrete
        // authentication/session services composed over the backend.
        db.reset_storage_for_tests().await.expect("reset");
        let backend: Arc<dyn Backend> = Arc::new(db);
        full_stack_register_validate_refresh_revoke_contract(
            Arc::clone(&backend),
            "sqlite-device-full",
            "sqlite_full_player",
        )
        .await;
        disabled_and_unknown_indistinguishable_contract(
            Arc::clone(&backend),
            "sqlite-device-disabled",
            "sqlite_disabled_player",
        )
        .await;
    }

    /// Prove that an account created through a `SqliteUnitOfWork` is atomic
    /// (all-or-nothing): a committed create-user-then-link-identity workflow is
    /// durable, and a workflow whose identity link conflicts and rolls back leaves
    /// no partially-created account behind.
    async fn account_creation_is_atomic(db: &SqliteDatabase) {
        // Success: create the user and link its credential in one transaction.
        let uow = db.begin().await.expect("begin");
        uow.user_repository()
            .create_user(user("acct-1", "atomic"))
            .await
            .expect("create user in tx");
        uow.auth_identity_repository()
            .link_auth_identity(device_identity("dev-1", "acct-1"))
            .await
            .expect("link identity in tx");
        uow.commit().await.expect("commit");

        assert!(
            db.user_repository()
                .get_user(&uid("acct-1"))
                .await
                .expect("get")
                .is_some(),
            "committed user is durable"
        );
        assert!(
            db.auth_identity_repository()
                .get_auth_identity(&AuthCredential::Device(
                    DeviceId::new("dev-1").expect("device")
                ))
                .await
                .expect("get")
                .is_some(),
            "committed identity is durable"
        );

        // All-or-nothing: a workflow whose identity link conflicts and then rolls
        // back must leave no partially-created account behind.
        let uow = db.begin().await.expect("begin");
        uow.user_repository()
            .create_user(user("acct-2", "atomic2"))
            .await
            .expect("create user in tx");
        let conflict = uow
            .auth_identity_repository()
            .link_auth_identity(device_identity("dev-1", "acct-2"))
            .await;
        assert!(
            conflict.is_err(),
            "linking an already-owned credential must conflict"
        );
        uow.rollback().await.expect("rollback");

        assert!(
            db.user_repository()
                .get_user(&uid("acct-2"))
                .await
                .expect("get")
                .is_none(),
            "rolled-back user must not persist (account creation is atomic)"
        );
    }
}

// --- Postgres runs (opt-in via DATABASE_URL) --------------------------------
//
// Runs the same reusable contracts against a real Postgres backend when
// `DATABASE_URL` (or `CITADEL_TEST_DATABASE_URL`) is set, and additionally proves
// that a create-user-then-link-identity workflow driven through one
// `PgUnitOfWork` is atomic (all-or-nothing). Skipped when neither variable is
// set, so `bash scripts/check.sh` stays green without a database.
//
// ```text
// make db-up
// DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//   cargo test --test identity_session_reference_impls
// make db-down
// ```
mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
    }

    async fn connect() -> Option<PgDatabase> {
        let url = test_database_url()?;
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        Some(
            PgDatabase::connect(&config)
                .await
                .expect("connect + migrate against the test Postgres"),
        )
    }

    /// One test drives every contract sequentially against the shared database.
    /// Running as a single `#[tokio::test]` keeps the reset (`TRUNCATE`) and the
    /// per-contract fixtures from racing other tests over the same tables.
    #[tokio::test]
    async fn postgres_backend_satisfies_identity_session_contracts() {
        let Some(db) = connect().await else {
            eprintln!(
                "skipping Postgres identity/session contracts: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run them"
            );
            return;
        };

        db.reset_storage_for_tests().await.expect("reset");
        user_repository_contract(db.user_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        auth_identity_repository_contract(db.auth_identity_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        session_repository_contract(db.session_repository().as_ref()).await;

        db.reset_storage_for_tests().await.expect("reset");
        account_creation_is_atomic(&db).await;

        // Service-level contracts: the SAME contracts the in-memory backend runs,
        // now driven end to end against Postgres through the concrete
        // authentication/session services composed over the backend.
        db.reset_storage_for_tests().await.expect("reset");
        let url = test_database_url().expect("url present");
        let backend: Arc<dyn Backend> = Arc::new(db);
        full_stack_register_validate_refresh_revoke_contract(
            Arc::clone(&backend),
            "pg-device-full",
            "pg_full_player",
        )
        .await;
        disabled_and_unknown_indistinguishable_contract(
            Arc::clone(&backend),
            "pg-device-disabled",
            "pg_disabled_player",
        )
        .await;

        // The capstone: an account created by the service PERSISTS across a fresh
        // pool/repository instance (survives a node "restart").
        account_persists_across_restart(&url).await;
    }

    /// Prove that an account the authentication service creates on Postgres is
    /// durable across a brand-new pool/repository instance — i.e. it survives a
    /// process restart, which the in-memory backend cannot do.
    async fn account_persists_across_restart(url: &str) {
        let config = DatabaseConfig {
            url: Some(url.to_string()),
            ..DatabaseConfig::default()
        };

        // First "process": create an account through the service over a fresh
        // backend, then drop everything (simulating shutdown).
        let (user_id, username) = {
            let db = PgDatabase::connect(&config).await.expect("connect (run 1)");
            db.reset_storage_for_tests().await.expect("reset");
            let backend: Arc<dyn Backend> = Arc::new(db);
            let (auth, _sessions) = auth_stack_over(Arc::clone(&backend));
            let outcome = auth
                .authenticate_device(DeviceAuthenticationRequest {
                    device_id: DeviceId::new("pg-device-restart").expect("device"),
                    options: device_options_named(true, "pg_restart_player"),
                })
                .await
                .expect("register");
            assert!(outcome.account_created);
            (outcome.user.id.clone(), outcome.user.username.clone())
        };

        // Second "process": a brand-new pool/repository must still see the account
        // and its credential link.
        let db = PgDatabase::connect(&config).await.expect("connect (run 2)");
        let restored = db
            .user_repository()
            .get_user(&user_id)
            .await
            .expect("get")
            .expect("account persists across a fresh pool/repository");
        assert_eq!(restored.username, username);
        assert!(
            db.auth_identity_repository()
                .get_auth_identity(&AuthCredential::Device(
                    DeviceId::new("pg-device-restart").expect("device")
                ))
                .await
                .expect("get")
                .is_some(),
            "the credential link persists across restart"
        );
    }

    async fn account_creation_is_atomic(db: &PgDatabase) {
        // Success: create the user and link its credential in one transaction.
        let uow = db.begin().await.expect("begin");
        uow.user_repository()
            .create_user(user("acct-1", "atomic"))
            .await
            .expect("create user in tx");
        uow.auth_identity_repository()
            .link_auth_identity(device_identity("dev-1", "acct-1"))
            .await
            .expect("link identity in tx");
        uow.commit().await.expect("commit");

        assert!(
            db.user_repository()
                .get_user(&uid("acct-1"))
                .await
                .expect("get")
                .is_some(),
            "committed user is durable"
        );
        assert!(
            db.auth_identity_repository()
                .get_auth_identity(&AuthCredential::Device(
                    DeviceId::new("dev-1").expect("device")
                ))
                .await
                .expect("get")
                .is_some(),
            "committed identity is durable"
        );

        // All-or-nothing: a workflow whose identity link conflicts and then rolls
        // back must leave no partially-created account behind.
        let uow = db.begin().await.expect("begin");
        uow.user_repository()
            .create_user(user("acct-2", "atomic2"))
            .await
            .expect("create user in tx");
        let conflict = uow
            .auth_identity_repository()
            .link_auth_identity(device_identity("dev-1", "acct-2"))
            .await;
        assert!(
            conflict.is_err(),
            "linking an already-owned credential must conflict"
        );
        uow.rollback().await.expect("rollback");

        assert!(
            db.user_repository()
                .get_user(&uid("acct-2"))
                .await
                .expect("get")
                .is_none(),
            "rolled-back user must not persist (account creation is atomic)"
        );
    }
}
