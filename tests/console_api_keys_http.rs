use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use citadel::config::{Config, DatabaseConfig};
use citadel::repository::{Backend, SqliteDatabase};
use citadel::services::{ApiKeyScope, AuditFilter, CreateApiKeyRequest};
use citadel::time::{Clock, SystemClock};
use citadel::{App, http};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Debug)]
struct Response {
    status: u16,
    body: Option<Value>,
    raw: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.raw
            .split_once("\r\n\r\n")?
            .0
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    }
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let body = body.unwrap_or("");
    let auth = bearer
        .map(|value| format!("Authorization: Bearer {value}\r\n"))
        .unwrap_or_default();
    let content = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}{content}Connection: close\r\n\r\n{body}"
    );
    stream.write_all(wire.as_bytes()).await.expect("write");
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .expect("response timeout")
        .expect("read");
    let raw = String::from_utf8_lossy(&bytes).into_owned();
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    Response { status, body, raw }
}

async fn request_with_authorization_lines(
    addr: SocketAddr,
    path: &str,
    authorization_lines: &str,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization_lines}Connection: close\r\n\r\n"
    );
    stream.write_all(wire.as_bytes()).await.expect("write");
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .expect("response timeout")
        .expect("read");
    let raw = String::from_utf8_lossy(&bytes).into_owned();
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    Response { status, body, raw }
}

async fn spawn_with_app(
    app: App,
) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        http::serve(listener, app, async {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    (addr, tx, task)
}

async fn spawn() -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let mut config = Config::default();
    config.console.username = "ops".into();
    config.console.password = "operator-secret".into();
    config.console.viewer_password = Some("viewer-secret".into());
    config.validate().expect("config");
    spawn_with_app(App::new(config)).await
}

async fn login(addr: SocketAddr, password: &str) -> String {
    let response = request(
        addr,
        "POST",
        "/console/v1/login",
        None,
        Some(&format!(r#"{{"username":"ops","password":"{password}"}}"#)),
    )
    .await;
    assert_eq!(response.status, 200, "{response:?}");
    response.body.expect("body")["token"]
        .as_str()
        .expect("token")
        .to_owned()
}

async fn create_key(addr: SocketAddr, admin: &str, name: &str, scopes: &[&str]) -> Value {
    let body = serde_json::json!({"name": name, "scopes": scopes});
    let response = request(
        addr,
        "POST",
        "/console/v1/api-keys",
        Some(admin),
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(response.status, 201, "{response:?}");
    response.body.expect("create body")
}

#[tokio::test]
async fn human_admin_can_create_and_machine_principal_is_distinct() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let created = create_key(addr, &admin, "telemetry robot", &["telemetry:read"]).await;
    let secret = created["secret"].as_str().expect("one-time secret");
    assert!(secret.starts_with("ctdl_k1_"));

    let me = request(addr, "GET", "/console/v1/me", Some(secret), None).await;
    assert_eq!(me.status, 200, "{me:?}");
    let me = me.body.expect("me");
    assert_eq!(me["actor_type"], "api_key");
    assert_eq!(me["key_name"], "telemetry robot");
    assert!(
        me.get("username").is_none(),
        "machine is not a human username"
    );

    let telemetry = request(addr, "GET", "/console/v1/telemetry", Some(secret), None).await;
    assert_eq!(telemetry.status, 200, "{telemetry:?}");
    let denied = request(addr, "GET", "/console/v1/config", Some(secret), None).await;
    assert_eq!(denied.status, 403, "{denied:?}");

    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn authorized_machine_reads_are_audited_centrally_without_affecting_humans() {
    let mut config = Config::default();
    config.console.username = "ops".into();
    config.console.password = "operator-secret".into();
    config.validate().expect("config");
    let app = App::new(config);
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "audited reader".to_owned(),
                scopes: vec![ApiKeyScope::TelemetryRead, ApiKeyScope::AccountsRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("create key");
    let audit = Arc::clone(app.audit_log());
    let (addr, shutdown, server) = spawn_with_app(app).await;

    assert_eq!(
        request(
            addr,
            "GET",
            "/console/v1/telemetry",
            Some(&issued.secret),
            None,
        )
        .await
        .status,
        200
    );
    assert_eq!(
        request(
            addr,
            "GET",
            "/console/v1/accounts",
            Some(&issued.secret),
            None,
        )
        .await
        .status,
        200
    );

    let human = login(addr, "operator-secret").await;
    assert_eq!(
        request(addr, "GET", "/console/v1/telemetry", Some(&human), None)
            .await
            .status,
        200
    );

    let entries = audit.list(&AuditFilter {
        action: Some("console.read".to_owned()),
        ..AuditFilter::default()
    });
    assert_eq!(
        entries.len(),
        2,
        "human reads retain existing audit behavior"
    );
    for (entry, path) in entries
        .iter()
        .rev()
        .zip(["/console/v1/telemetry", "/console/v1/accounts"])
    {
        assert_eq!(entry.actor_type, "api_key");
        assert_eq!(entry.actor, issued.key.id.as_str());
        assert_eq!(entry.credential_id.as_deref(), Some(issued.key.id.as_str()));
        assert_eq!(entry.key_name.as_deref(), Some("audited reader"));
        assert_eq!(entry.role, "api_key");
        assert_eq!(entry.action, "console.read");
        assert_eq!(entry.target, path);
        assert_eq!(entry.details, format!("method=GET path={path}"));
    }
    let serialized = serde_json::to_string(&entries).expect("serialize audit entries");
    assert!(!serialized.contains(&issued.secret));
    assert!(!serialized.contains("Authorization"));
    assert!(!serialized.contains("secret_verifier"));

    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn graceful_shutdown_flushes_last_used_without_dashboard_read() {
    let database = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect SQLite");
    let database: Arc<dyn Backend> = Arc::new(database);
    let repository = database.api_key_repository();
    let app = App::with_backend(Config::default(), database);
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "shutdown-reader".to_owned(),
                scopes: vec![ApiKeyScope::TelemetryRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("create key");
    let (addr, shutdown, server) = spawn_with_app(app).await;

    assert_eq!(
        request(
            addr,
            "GET",
            "/console/v1/telemetry",
            Some(&issued.secret),
            None
        )
        .await
        .status,
        200
    );
    assert_eq!(
        repository
            .get(&issued.key.id)
            .await
            .expect("read before shutdown")
            .expect("key exists")
            .last_used_at,
        None,
        "observation remains coalesced before shutdown"
    );

    let _ = shutdown.send(());
    server.await.expect("join");
    assert!(
        repository
            .get(&issued.key.id)
            .await
            .expect("read after shutdown")
            .expect("key exists")
            .last_used_at
            .is_some(),
        "graceful shutdown must persist pending last_used_at without a dashboard read"
    );
}

#[tokio::test]
async fn expired_key_is_rejected_with_the_uniform_auth_error() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after epoch")
        .as_millis() as u64
        + 200;
    let body = serde_json::json!({
        "name": "short lived",
        "scopes": ["telemetry:read"],
        "expires_at": expires_at,
    });
    let created = request(
        addr,
        "POST",
        "/console/v1/api-keys",
        Some(&admin),
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(created.status, 201, "{created:?}");
    let secret = created.body.expect("created")["secret"]
        .as_str()
        .expect("secret")
        .to_owned();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let expired = request(addr, "GET", "/console/v1/me", Some(&secret), None).await;
    let malformed = request(addr, "GET", "/console/v1/me", Some("ctdl_k1_bad"), None).await;
    assert_eq!(
        (expired.status, expired.body),
        (malformed.status, malformed.body)
    );
    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn scopes_and_methods_are_explicitly_fail_closed() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let cases = [
        ("telemetry:read", "/console/v1/telemetry"),
        ("config:read", "/console/v1/config"),
        ("audit:read", "/console/v1/audit"),
        ("errors:read", "/console/v1/errors"),
        ("accounts:read", "/console/v1/accounts"),
        ("groups:read", "/console/v1/groups"),
        ("runtime:read", "/console/v1/runtime"),
        ("matches:read", "/console/v1/matches"),
        ("storage:read", "/console/v1/storage"),
        ("database:read", "/console/v1/database"),
        ("chat:read", "/console/v1/chat"),
        ("notifications:read", "/console/v1/notifications"),
        ("leaderboards:read", "/console/v1/leaderboards"),
        ("tournaments:read", "/console/v1/tournaments"),
        ("purchases:read", "/console/v1/purchases"),
        ("subscriptions:read", "/console/v1/subscriptions"),
    ];
    for (scope, path) in cases {
        let created = create_key(addr, &admin, scope, &[scope]).await;
        let secret = created["secret"].as_str().expect("secret");
        let allowed = request(addr, "GET", path, Some(secret), None).await;
        assert_ne!(allowed.status, 401, "auth rejected {scope}: {allowed:?}");
        assert_ne!(allowed.status, 403, "scope rejected {scope}: {allowed:?}");
    }
    let created = create_key(addr, &admin, "safe", &["telemetry:read"]).await;
    let secret = created["secret"].as_str().expect("secret");
    assert_eq!(
        request(addr, "HEAD", "/console/v1/telemetry", Some(secret), None)
            .await
            .status,
        200
    );
    assert_eq!(
        request(addr, "GET", "/console/v1/config", Some(secret), None)
            .await
            .status,
        403
    );
    assert_eq!(
        request(
            addr,
            "POST",
            "/console/v1/database/rows",
            Some(secret),
            Some("{}")
        )
        .await
        .status,
        403
    );
    assert_eq!(
        request(
            addr,
            "PUT",
            "/console/v1/storage/a/b",
            Some(secret),
            Some("{}")
        )
        .await
        .status,
        403
    );
    assert_eq!(
        request(addr, "GET", "/console/v1/api-keys", Some(secret), None)
            .await
            .status,
        403
    );
    for path in [
        "/console/v1/telemetry/",
        "/console/v1/telemetry/unknown",
        "/console/v1/%74elemetry",
    ] {
        assert_ne!(
            request(addr, "GET", path, Some(secret), None).await.status,
            200,
            "{path}"
        );
    }
    let no_scope = request(
        addr,
        "POST",
        "/console/v1/api-keys",
        Some(&admin),
        Some(r#"{"name":"none","scopes":[]}"#),
    )
    .await;
    assert_eq!(no_scope.status, 400);
    let unknown_field = request(
        addr,
        "POST",
        "/console/v1/api-keys",
        Some(&admin),
        Some(r#"{"name":"bad","scopes":["telemetry:read"],"secret":"inject"}"#),
    )
    .await;
    assert_eq!(unknown_field.status, 400);
    let viewer = login(addr, "viewer-secret").await;
    assert_eq!(
        request(addr, "GET", "/console/v1/api-keys", Some(&viewer), None)
            .await
            .status,
        403
    );
    assert_eq!(
        request(addr, "GET", "/console/v1/me", Some(&viewer), None)
            .await
            .body
            .expect("me")["username"],
        "ops"
    );
    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn database_explorer_hides_api_keys_from_machine_credentials_and_viewers() {
    let database = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect SQLite");
    let backend: Arc<dyn Backend> = Arc::new(database);
    let mut config = Config::default();
    config.console.username = "ops".into();
    config.console.password = "operator-secret".into();
    config.console.viewer_password = Some("viewer-secret".into());
    config.validate().expect("config");
    let app = App::with_backend(config, backend);
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: "database explorer".to_owned(),
                scopes: vec![ApiKeyScope::DatabaseRead],
                expires_at: None,
            },
            SystemClock.now(),
        )
        .await
        .expect("create database reader");
    let (addr, shutdown, server) = spawn_with_app(app).await;
    let viewer = login(addr, "viewer-secret").await;
    let admin = login(addr, "operator-secret").await;

    for bearer in [issued.secret.as_str(), viewer.as_str()] {
        let tables = request(addr, "GET", "/console/v1/database", Some(bearer), None).await;
        assert_eq!(tables.status, 200, "database listing remains readable");
        let tables = tables.body.expect("tables body")["tables"]
            .as_array()
            .expect("tables array")
            .clone();
        assert!(
            tables
                .iter()
                .any(|entry| entry["table"]["table"] == "users"),
            "ordinary application data remains visible with database:read"
        );
        assert!(
            tables
                .iter()
                .all(|entry| entry["table"]["table"] != "api_keys"),
            "the internal credential relation must not be enumerable"
        );

        assert_eq!(
            request(
                addr,
                "GET",
                "/console/v1/database/main/api_keys",
                Some(bearer),
                None,
            )
            .await
            .status,
            403,
            "direct API-key table description must be forbidden"
        );
        for (path, body) in [
            (
                "/console/v1/database/rows",
                r#"{"table":{"schema":"main","table":"api_keys"},"filters":[],"sort":{"column":"id","direction":"asc"},"limit":1}"#,
            ),
            (
                "/console/v1/database/row",
                r#"{"table":{"schema":"main","table":"api_keys"},"row_ref":"opaque"}"#,
            ),
        ] {
            assert_eq!(
                request(addr, "POST", path, Some(bearer), Some(body))
                    .await
                    .status,
                403,
                "direct API-key row access must be forbidden at {path}"
            );
        }
    }

    let admin_description = request(
        addr,
        "GET",
        "/console/v1/database/main/api_keys",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(
        admin_description.status, 200,
        "a human admin may inspect the internal relation"
    );
    assert!(
        admin_description.body.expect("admin description")["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .any(|column| column["name"] == "secret_verifier" && column["sensitive"] == true),
        "the verifier must remain classified as sensitive"
    );
    let admin_rows = request(
        addr,
        "POST",
        "/console/v1/database/rows",
        Some(&admin),
        Some(
            r#"{"table":{"schema":"main","table":"api_keys"},"filters":[],"sort":{"column":"id","direction":"asc"},"limit":1}"#,
        ),
    )
    .await;
    assert_eq!(admin_rows.status, 200);
    assert_eq!(
        admin_rows.body.expect("admin rows")["rows"][0]["values"]["secret_verifier"]["kind"],
        "redacted",
        "even human-admin explorer responses must never expose verifier bytes"
    );

    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn database_read_scope_allows_only_the_two_semantic_post_reads() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let database = create_key(addr, &admin, "database", &["database:read"]).await;
    let database_secret = database["secret"].as_str().expect("secret");
    for path in ["/console/v1/database/rows", "/console/v1/database/row"] {
        let response = request(addr, "POST", path, Some(database_secret), Some("{}")).await;
        assert_ne!(response.status, 401, "authentication rejected {path}");
        assert_ne!(response.status, 403, "database:read rejected {path}");
    }

    let telemetry = create_key(addr, &admin, "telemetry", &["telemetry:read"]).await;
    let telemetry_secret = telemetry["secret"].as_str().expect("secret");
    for path in ["/console/v1/database/rows", "/console/v1/database/row"] {
        assert_eq!(
            request(addr, "POST", path, Some(telemetry_secret), Some("{}"))
                .await
                .status,
            403,
            "missing database:read must deny {path}"
        );
    }
    for (method, path) in [
        ("POST", "/console/v1/database"),
        ("POST", "/console/v1/database/main/users"),
        ("POST", "/console/v1/database/rows/extra"),
        ("PUT", "/console/v1/database/rows"),
    ] {
        let status = request(addr, method, path, Some(database_secret), Some("{}"))
            .await
            .status;
        assert!(
            matches!(status, 403..=405),
            "alternate POST/mutation must fail closed: {method} {path} returned {status}"
        );
    }
    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn authorization_header_is_single_bounded_and_canonical() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let created = create_key(addr, &admin, "telemetry", &["telemetry:read"]).await;
    let key = created["secret"].as_str().expect("secret");

    let malformed = [
        format!("Authorization: Bearer {key}\r\nAuthorization: Bearer {key}\r\n"),
        format!("Authorization: bearer {key}\r\n"),
        format!("Authorization: BEARER {key}\r\n"),
        format!("Authorization: Bearer  {key}\r\n"),
        format!("Authorization: Bearer {}\t{}\r\n", &key[..10], &key[10..]),
        format!("Authorization: Bearer {}\r\n", "x".repeat(4097)),
    ];
    for (index, headers) in malformed.into_iter().enumerate() {
        assert_eq!(
            request_with_authorization_lines(addr, "/console/v1/me", &headers)
                .await
                .status,
            401,
            "malformed Authorization case {index} must fail uniformly"
        );
    }

    let prefixed_unknown = "Authorization: Bearer ctdl_k1_not-a-key\r\n";
    assert_eq!(
        request_with_authorization_lines(addr, "/console/v1/me", prefixed_unknown)
            .await
            .status,
        401
    );
    let _ = shutdown.send(());
    server.await.expect("join");
}

#[tokio::test]
async fn rotate_revoke_and_one_time_secret_contract() {
    let (addr, shutdown, server) = spawn().await;
    let admin = login(addr, "operator-secret").await;
    let created_response = request(
        addr,
        "POST",
        "/console/v1/api-keys",
        Some(&admin),
        Some(r#"{"name":"audit-safe","scopes":["telemetry:read"]}"#),
    )
    .await;
    assert_eq!(created_response.status, 201, "{created_response:?}");
    assert_eq!(created_response.header("cache-control"), Some("no-store"));
    let created = created_response.body.expect("create body");
    let first = created["secret"].as_str().expect("secret").to_owned();
    let id = created["key"]["id"].as_str().expect("id").to_owned();
    assert_eq!(created["key"]["status"], "active");
    for path in [
        "/console/v1/api-keys".to_owned(),
        format!("/console/v1/api-keys/{id}"),
    ] {
        let response = request(addr, "GET", &path, Some(&admin), None).await;
        assert_eq!(response.status, 200);
        assert!(!response.raw.contains(&first));
        assert!(!response.raw.contains("\"secret\""));
    }
    let zero_generation = request(
        addr,
        "POST",
        &format!("/console/v1/api-keys/{id}/rotate"),
        Some(&admin),
        Some(r#"{"generation":0}"#),
    )
    .await;
    assert_eq!(zero_generation.status, 400);
    let rotated = request(
        addr,
        "POST",
        &format!("/console/v1/api-keys/{id}/rotate"),
        Some(&admin),
        Some(r#"{"generation":1}"#),
    )
    .await;
    assert_eq!(rotated.status, 200, "{rotated:?}");
    assert_eq!(rotated.header("cache-control"), Some("no-store"));
    let rotated = rotated.body.expect("rotate");
    let second = rotated["secret"].as_str().expect("secret").to_owned();
    assert_ne!(first, second);
    assert_eq!(rotated["key"]["generation"], 2);
    assert_eq!(
        request(addr, "GET", "/console/v1/me", Some(&first), None)
            .await
            .status,
        401
    );
    assert_eq!(
        request(addr, "GET", "/console/v1/me", Some(&second), None)
            .await
            .status,
        200
    );
    let revoked = request(
        addr,
        "POST",
        &format!("/console/v1/api-keys/{id}/revoke"),
        Some(&admin),
        Some(r#"{"generation":2}"#),
    )
    .await;
    assert_eq!(revoked.status, 200);
    assert_eq!(revoked.body.expect("revoke")["status"], "revoked");
    assert_eq!(
        request(addr, "GET", "/console/v1/me", Some(&second), None)
            .await
            .status,
        401
    );
    let malformed = request(addr, "GET", "/console/v1/me", Some("ctdl_k1_bad"), None).await;
    let mut unknown_token = first.clone();
    unknown_token.pop();
    unknown_token.push(if first.ends_with('A') { 'B' } else { 'A' });
    let unknown = request(addr, "GET", "/console/v1/me", Some(&unknown_token), None).await;
    assert_eq!(
        (malformed.status, malformed.body),
        (unknown.status, unknown.body)
    );
    let audit = request(
        addr,
        "GET",
        "/console/v1/audit?action=api_keys",
        Some(&admin),
        None,
    )
    .await;
    assert!(!audit.raw.contains(&first));
    assert!(!audit.raw.contains(&second));
    let entries = audit.body.expect("audit")["entries"]
        .as_array()
        .expect("entries")
        .clone();
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().all(|entry| entry["actor_type"] == "human"));
    assert!(
        entries
            .iter()
            .all(|entry| entry.get("credential_id").is_none())
    );
    let _ = shutdown.send(());
    server.await.expect("join");
}
