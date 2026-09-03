//! Integration tests for the console API foundation.
//!
//! Binds an ephemeral port, runs the real server, and drives the console auth
//! flow with raw HTTP/1.1 requests: login (success, wrong password, viewer
//! role), the bearer guard on `/console/v1/*` (401 without/with a bad token),
//! the `/me` identity route, and the `501` stubs for every section path. The
//! public routes (`/status`, `/dashboard`, `/health`) must stay reachable
//! without console credentials.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// A parsed HTTP response: status code and (optional) JSON body.
struct Response {
    status: u16,
    body: Option<Value>,
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_response(raw: &str) -> Response {
    let status_line = raw.lines().next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("response should carry an HTTP status code");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .filter(|b| !b.trim().is_empty())
        .and_then(|b| serde_json::from_str::<Value>(b.trim()).ok());
    Response { status, body }
}

/// Issue a raw HTTP/1.1 `POST` with a JSON body.
async fn post_json(addr: SocketAddr, path: &str, body: &str) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_response(&raw)
}

/// Issue a raw HTTP/1.1 `GET`, optionally with a bearer token.
async fn get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let auth_header = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth_header}Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_response(&raw)
}

/// Spawn the real server on an ephemeral loopback port for `app`.
async fn spawn_server(app: App) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = rx.await;
    };
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, app, shutdown).await;
    });
    (addr, tx, server)
}

/// A config with non-default operator credentials and a viewer role enabled.
fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.console.viewer_password = Some("viewer-secret".to_string());
    config.validate().expect("test config must validate");
    config
}

async fn login(addr: SocketAddr, username: &str, password: &str) -> Response {
    post_json(
        addr,
        http::LOGIN_PATH,
        &format!(r#"{{"username":"{username}","password":"{password}"}}"#),
    )
    .await
}

/// Password guessing against the operator login must be bounded.
///
/// The console credential is static, unhashed, and grants full read/write over
/// every section, so the endpoint has to fail closed under repeated attempts
/// rather than merely recording them in the audit trail.
#[tokio::test]
async fn console_login_is_rate_limited_after_repeated_failures() {
    let mut config = console_config();
    // Keep the fixture explicit rather than depending on the shipped default.
    config.authentication.limits.console_login = citadel::config::AuthRateLimitRule {
        limit: 3,
        window_ms: 300_000,
    };
    config.validate().expect("test config must validate");
    let (addr, tx, server) = spawn_server(App::new(config)).await;

    // The configured budget is spent on wrong passwords, each a uniform 401.
    for attempt in 0..3 {
        let rejected = login(addr, "ops", "wrong").await;
        assert_eq!(rejected.status, 401, "attempt {attempt} should be a 401");
    }

    // The next attempt is refused by the limiter, not the credential check.
    let throttled = login(addr, "ops", "wrong").await;
    assert_eq!(throttled.status, 429, "further guesses must be throttled");

    // Crucially, the throttle also holds for the *correct* password: an
    // attacker must not be able to distinguish a hit from a miss by spending
    // the budget, and a exhausted window cannot be bypassed by guessing right.
    let correct = login(addr, "ops", "operator-secret").await;
    assert_eq!(
        correct.status, 429,
        "the window applies regardless of credential validity"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn console_login_guard_and_section_stubs() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    // 1. Wrong password: uniform 401 with the stable auth error code.
    let rejected = login(addr, "ops", "wrong").await;
    assert_eq!(rejected.status, 401);
    assert_eq!(
        rejected.body.expect("error body")["code"],
        "authentication_failed"
    );

    // 2. Unknown username: identical 401 (no credential oracle).
    assert_eq!(login(addr, "intruder", "operator-secret").await.status, 401);

    // 3. Malformed body: typed 400.
    let malformed = post_json(addr, http::LOGIN_PATH, r#"{"username":"ops"}"#).await;
    assert_eq!(malformed.status, 400);

    // 4. Admin login succeeds and reports role + expiry.
    let admin = login(addr, "ops", "operator-secret").await;
    assert_eq!(admin.status, 200);
    let admin_body = admin.body.expect("login body");
    let admin_token = admin_body["token"].as_str().expect("token").to_string();
    assert_eq!(admin_body["role"], "admin");
    assert_eq!(admin_body["expires_in_sec"], 3_600);
    assert!(!admin_token.is_empty());

    // 5. Viewer password grants the read-only role.
    let viewer = login(addr, "ops", "viewer-secret").await;
    assert_eq!(viewer.status, 200);
    let viewer_body = viewer.body.expect("viewer body");
    assert_eq!(viewer_body["role"], "viewer");
    let viewer_token = viewer_body["token"].as_str().expect("token").to_string();

    // 6. /me reflects the authenticated identity for both roles.
    let me = get(addr, http::ME_PATH, Some(&admin_token)).await;
    assert_eq!(me.status, 200);
    let me_body = me.body.expect("me body");
    assert_eq!(me_body["username"], "ops");
    assert_eq!(me_body["role"], "admin");
    let viewer_me = get(addr, http::ME_PATH, Some(&viewer_token)).await;
    assert_eq!(viewer_me.body.expect("viewer me")["role"], "viewer");

    // 7. The guard: no token and a garbage token are the uniform 401.
    assert_eq!(get(addr, http::ME_PATH, None).await.status, 401);
    assert_eq!(get(addr, http::ME_PATH, Some("bogus")).await.status, 401);

    // 8. Every section route is registered and guarded: 401 when
    //    unauthenticated; when authenticated, an implemented section serves
    //    (200) and a pending one answers 501 — never a 404 either way.
    for path in http::SECTION_PATHS {
        let unauthenticated = get(addr, path, None).await;
        assert_eq!(unauthenticated.status, 401, "unauthenticated {path}");
        let section = get(addr, path, Some(&admin_token)).await;
        if *path == "/console/v1/database" {
            // This suite intentionally uses the in-memory backend. The route
            // is live and authenticated, but database metadata requires a
            // durable SQL backend (covered separately below with SQLite).
            assert_eq!(section.status, 400, "database explorer without SQL backend");
        } else if http::console_api::IMPLEMENTED_SECTION_PATHS.contains(path) {
            assert_eq!(section.status, 200, "implemented section {path}");
        } else {
            assert_eq!(section.status, 501, "authenticated stub {path}");
            assert_eq!(
                section.body.expect("stub body")["code"],
                "not_implemented",
                "stub body {path}"
            );
        }
    }

    // 9. Operator auth never gates the public surface.
    assert_eq!(get(addr, http::STATUS_PATH, None).await.status, 200);
    assert_eq!(get(addr, http::HEALTH_PATH, None).await.status, 200);
    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn config_browser_returns_grouped_redacted_configuration() {
    let mut config = console_config();
    config.database.url = Some("sqlite::memory:".to_string());
    let (addr, tx, server) = spawn_server(App::new(config)).await;

    let token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login body")["token"]
        .as_str()
        .expect("token")
        .to_string();

    let response = get(addr, "/console/v1/config", Some(&token)).await;
    assert_eq!(response.status, 200);
    let body = response.body.expect("config body");
    assert_eq!(body["node_id"], "dev-1");
    assert_eq!(body["backend"], "in-memory");

    let raw = serde_json::to_string(&body).expect("serialize");
    // The operator and viewer passwords must never reach the response bytes.
    assert!(!raw.contains("operator-secret"));
    assert!(!raw.contains("viewer-secret"));
    assert!(raw.contains("<redacted>"));

    // Groups carry real, browsable values.
    let groups = body["groups"].as_array().expect("groups");
    let server_group = groups
        .iter()
        .find(|g| g["name"] == "server")
        .expect("server group");
    assert!(
        server_group["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|e| e["key"] == "node_id" && e["value"] == "dev-1")
    );

    let _ = tx.send(());
    let _ = server.await;
}

/// Issue a raw HTTP/1.1 request with a method, bearer token, and JSON body.
async fn send_json(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: &str,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let body_text = body.unwrap_or("");
    let content = if body.is_some() {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body_text.len()
        )
    } else {
        String::new()
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {bearer}\r\n{content}Connection: close\r\n\r\n{body_text}"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_response(&raw)
}

#[tokio::test]
async fn storage_browser_supports_browse_inspect_write_delete() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let admin = login(addr, "ops", "operator-secret").await;
    let admin_token = admin.body.expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let viewer = login(addr, "ops", "viewer-secret").await;
    let viewer_token = viewer.body.expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Empty node: no collections yet.
    let empty = get(addr, "/console/v1/storage", Some(&admin_token)).await;
    assert_eq!(empty.status, 200);
    assert_eq!(
        empty.body.expect("body")["collections"]
            .as_array()
            .expect("collections")
            .len(),
        0
    );

    // Write a system object and a user object through the console.
    let wrote = send_json(
        addr,
        "PUT",
        "/console/v1/storage/saves/slot-1",
        &admin_token,
        Some(r#"{"value":{"hp":10},"read_permission":2}"#),
    )
    .await;
    assert_eq!(wrote.status, 200);
    let wrote_body = wrote.body.expect("write body");
    assert_eq!(wrote_body["collection"], "saves");
    assert_eq!(wrote_body["value"]["hp"], 10);
    let version = wrote_body["version"].as_str().expect("version").to_string();

    let user_object = send_json(
        addr,
        "PUT",
        "/console/v1/storage/saves/slot-2?user_id=u-1",
        &admin_token,
        Some(r#"{"value":{"hp":5}}"#),
    )
    .await;
    assert_eq!(user_object.status, 200);
    assert_eq!(user_object.body.expect("body")["user_id"], "u-1");

    // A viewer cannot mutate: 403 forbidden.
    let forbidden = send_json(
        addr,
        "PUT",
        "/console/v1/storage/saves/slot-3",
        &viewer_token,
        Some(r#"{"value":{}}"#),
    )
    .await;
    assert_eq!(forbidden.status, 403);
    assert_eq!(forbidden.body.expect("body")["code"], "forbidden");

    // Collections now report `saves` with both objects.
    let collections = get(addr, "/console/v1/storage", Some(&viewer_token)).await;
    let collections = collections.body.expect("body");
    let entries = collections["collections"].as_array().expect("collections");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["collection"], "saves");
    assert_eq!(entries[0]["objects"], 2);

    // Listing pages the object summaries (no values).
    let listed = get(
        addr,
        "/console/v1/storage/saves?limit=10",
        Some(&admin_token),
    )
    .await;
    assert_eq!(listed.status, 200);
    let listed = listed.body.expect("body");
    assert_eq!(listed["items"].as_array().expect("items").len(), 2);

    // Filter by owner.
    let filtered = get(
        addr,
        "/console/v1/storage/saves?user_id=u-1",
        Some(&admin_token),
    )
    .await;
    let filtered = filtered.body.expect("body");
    let items = filtered["items"].as_array().expect("items");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["key"], "slot-2");

    // Inspect one object.
    let object = get(addr, "/console/v1/storage/saves/slot-1", Some(&admin_token)).await;
    assert_eq!(object.status, 200);
    let object = object.body.expect("body");
    assert_eq!(object["value"]["hp"], 10);
    assert_eq!(object["read_permission"], 2);
    assert_eq!(object["version"].as_str().expect("version"), version);

    // A stale version precondition conflicts.
    let conflict = send_json(
        addr,
        "PUT",
        "/console/v1/storage/saves/slot-1",
        &admin_token,
        Some(r#"{"value":{"hp":11},"version":"not-the-version"}"#),
    )
    .await;
    assert_eq!(conflict.status, 409);

    // Delete and confirm 404 on re-read; deleting again stays idempotent.
    let deleted = send_json(
        addr,
        "DELETE",
        "/console/v1/storage/saves/slot-1",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted.status, 204);
    let missing = get(addr, "/console/v1/storage/saves/slot-1", Some(&admin_token)).await;
    assert_eq!(missing.status, 404);

    // The mutations are in the audit trail; the viewer's rejected write is not.
    let audit = get(addr, "/console/v1/audit?action=storage", Some(&admin_token)).await;
    let audit = audit.body.expect("audit body");
    let actions: Vec<String> = audit["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        actions,
        vec!["storage.delete", "storage.write", "storage.write"],
        "newest-first mutation trail"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn database_explorer_is_read_only_and_available_to_viewers_on_sqlite() {
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, SqliteDatabase};

    let database_config = DatabaseConfig {
        url: Some("sqlite::memory:".to_string()),
        ..DatabaseConfig::default()
    };
    let backend: Arc<dyn Backend> = Arc::new(
        SqliteDatabase::connect(&database_config)
            .await
            .expect("connect and migrate SQLite"),
    );
    let (addr, tx, server) = spawn_server(App::with_backend(console_config(), backend)).await;
    let token = login(addr, "ops", "viewer-secret")
        .await
        .body
        .expect("login body")["token"]
        .as_str()
        .expect("viewer token")
        .to_owned();

    let tables = get(addr, "/console/v1/database", Some(&token)).await;
    assert_eq!(tables.status, 200);
    let tables = tables.body.expect("tables");
    let users = tables["tables"]
        .as_array()
        .expect("table list")
        .iter()
        .find(|table| table["table"]["table"] == "users")
        .expect("migrated users table");
    assert_eq!(users["table"]["schema"], "main");

    let description = get(addr, "/console/v1/database/main/users", Some(&token)).await;
    assert_eq!(description.status, 200);
    let description = description.body.expect("description");
    assert_eq!(
        description["capabilities"]["stable_keyset_pagination"],
        true
    );

    let page = send_json(
        addr,
        "POST",
        "/console/v1/database/rows",
        &token,
        Some(r#"{"table":{"schema":"main","table":"users"},"filters":[],"sort":{"column":"id","direction":"asc"},"limit":1}"#),
    )
    .await;
    assert_eq!(page.status, 200);
    assert!(page.body.expect("page")["rows"].as_array().is_some());

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn matches_section_lists_live_rooms_from_the_gateway() {
    use citadel::realtime::{Gateway, ParticipantId, RoomLabel};

    let app = App::new(console_config());
    let (addr, tx, server) = spawn_server(app.clone()).await;
    let token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Before any gateway is attached the section reports that honestly.
    let detached = get(addr, "/console/v1/matches", Some(&token)).await;
    assert_eq!(detached.status, 200);
    let detached = detached.body.expect("body");
    assert_eq!(detached["realtime_attached"], false);
    assert_eq!(detached["items"].as_array().expect("items").len(), 0);

    // Attach a gateway (the seam transports use) and populate rooms.
    let gateway = std::sync::Arc::new(Gateway::new());
    app.attach_realtime_gateway(std::sync::Arc::clone(&gateway));
    let (lobby, _) = gateway
        .join_or_create_room(ParticipantId::from_raw(1), "lobby", || {
            RoomLabel::with_map("ForestArena")
        })
        .expect("create lobby");
    gateway
        .join_room(ParticipantId::from_raw(2), lobby)
        .expect("second member");
    gateway
        .create_room(RoomLabel::with_map("DesertMap"))
        .expect("relay-compatible gateway creates room");
    // (The id-only room has no members, so it exists but stays listable.)

    let listed = get(addr, "/console/v1/matches", Some(&token)).await;
    assert_eq!(listed.status, 200);
    let listed = listed.body.expect("body");
    assert_eq!(listed["realtime_attached"], true);
    assert_eq!(listed["total"], 2);
    let items = listed["items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["id"], lobby);
    assert_eq!(items[0]["name"], "lobby");
    assert_eq!(items[0]["map"], "ForestArena");
    assert_eq!(items[0]["players"], 2);
    assert_eq!(items[0]["open"], true);

    // Substring filter narrows by map.
    let filtered = get(addr, "/console/v1/matches?filter=Desert", Some(&token)).await;
    let filtered = filtered.body.expect("body");
    assert_eq!(filtered["items"].as_array().expect("items").len(), 1);
    assert_eq!(filtered["items"][0]["map"], "DesertMap");

    // Detail includes the member roll (guest participants: no user id).
    let detail = get(addr, &format!("/console/v1/matches/{lobby}"), Some(&token)).await;
    assert_eq!(detail.status, 200);
    let detail = detail.body.expect("body");
    assert_eq!(detail["map"], "ForestArena");
    let members = detail["members"].as_array().expect("members");
    assert_eq!(members.len(), 2);
    assert_eq!(members[0]["participant"], 1);
    assert!(members[0]["user_id"].is_null());

    // Unknown match id is a 404.
    assert_eq!(
        get(addr, "/console/v1/matches/9999", Some(&token))
            .await
            .status,
        404
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn matches_listing_fails_closed_under_the_script_readiness_gate() {
    use citadel::realtime::Gateway;
    use citadel::runtime::GameScriptReadiness;
    use citadel::time::{Clock, SystemClock};

    let mut config = console_config();
    config.runtime.enabled = true;
    config.runtime.require_script = true;
    config.validate().expect("strict runtime config validates");
    let app = App::new(config);
    let (addr, tx, server) = spawn_server(app.clone()).await;
    let token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // A require_script gateway boots not-ready (NoScript).
    let readiness = std::sync::Arc::new(GameScriptReadiness::new(SystemClock.now()));
    let gateway = std::sync::Arc::new(
        Gateway::new().with_script_readiness(std::sync::Arc::clone(&readiness)),
    );
    app.attach_realtime_gateway(std::sync::Arc::clone(&gateway));

    // Listing and detail both fail closed with the one stable client-safe
    // error; nothing is advertised.
    let gated = get(addr, "/console/v1/matches", Some(&token)).await;
    assert_eq!(gated.status, 503);
    let gated = gated.body.expect("body");
    assert_eq!(gated["code"], "runtime_unavailable");
    assert_eq!(gated["message"], "game script unavailable");
    let detail = get(addr, "/console/v1/matches/1", Some(&token)).await;
    assert_eq!(detail.status, 503);

    // The readiness surface explains the closed gate to the operator.
    let runtime = get(addr, "/console/v1/runtime", Some(&token)).await;
    assert_eq!(runtime.status, 200);
    let runtime = runtime.body.expect("body");
    assert_eq!(runtime["readiness"]["state"], "no_script");
    assert_eq!(runtime["readiness"]["generation"], 0);

    // A successful load opens the gate: matches list again, and the row
    // carries the binding the room was born with.
    readiness.record_loaded("sha256:console-v1", Clock::now(&SystemClock));
    let (lobby, _) = gateway
        .join_or_create_room_bound(
            citadel::realtime::ParticipantId::from_raw(1),
            "lobby",
            gateway
                .script_readiness()
                .and_then(|authority| authority.snapshot().binding()),
            || citadel::realtime::RoomLabel::with_map("ForestArena"),
        )
        .expect("bound room");
    let listed = get(addr, "/console/v1/matches", Some(&token)).await;
    assert_eq!(listed.status, 200);
    let listed = listed.body.expect("body");
    assert_eq!(listed["items"][0]["id"], lobby);
    assert_eq!(listed["items"][0]["script_revision"], "sha256:console-v1");
    assert_eq!(listed["items"][0]["script_generation"], 1);
    let runtime = get(addr, "/console/v1/runtime", Some(&token)).await;
    let runtime = runtime.body.expect("body");
    assert_eq!(runtime["readiness"]["state"], "ready");
    assert_eq!(runtime["readiness"]["revision_id"], "sha256:console-v1");

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn runtime_section_introspects_and_invokes_rpcs() {
    use citadel::realtime::Gateway;
    use citadel::runtime::LuaRuntime;

    let app = App::new(console_config());
    let (addr, tx, server) = spawn_server(app.clone()).await;
    let admin_token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Without an attached runtime the section reports facts, not errors...
    let detached = get(addr, "/console/v1/runtime", Some(&admin_token)).await;
    assert_eq!(detached.status, 200);
    let detached = detached.body.expect("body");
    assert_eq!(detached["attached"], false);
    // ...and the RPC caller has nothing to call.
    let no_runtime = send_json(
        addr,
        "POST",
        "/console/v1/runtime/rpc/ping",
        &admin_token,
        Some("{}"),
    )
    .await;
    assert_eq!(no_runtime.status, 404);

    // Attach a gateway with a script registering RPCs and handlers.
    let runtime = std::sync::Arc::new(
        LuaRuntime::from_source(
            r#"
            citadel.on_rpc("ping", function(ctx, body) return "pong" end)
            citadel.on_rpc("echo", function(ctx, body) return body end)
            citadel.on_message(1, function(ctx, body) end)
            "#,
            "console-test-script",
            100,
        )
        .expect("build runtime"),
    );
    let gateway = std::sync::Arc::new(Gateway::with_metrics_and_runtime(
        std::sync::Arc::clone(app.metrics()),
        Some(runtime),
    ));
    app.attach_realtime_gateway(gateway);

    // Introspection lists the registered surface.
    let info = get(addr, "/console/v1/runtime", Some(&admin_token)).await;
    let info = info.body.expect("body");
    assert_eq!(info["attached"], true);
    assert_eq!(info["script"]["source"], "console-test-script");
    assert_eq!(
        info["script"]["rpcs"]
            .as_array()
            .expect("rpcs")
            .iter()
            .map(|v| v.as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["echo", "ping"]
    );
    assert_eq!(info["script"]["message_kinds"][0], 1);

    // Invoke an RPC end-to-end with a payload.
    let echoed = send_json(
        addr,
        "POST",
        "/console/v1/runtime/rpc/echo",
        &admin_token,
        Some(r#"{"payload":"hello"}"#),
    )
    .await;
    assert_eq!(echoed.status, 200);
    let echoed = echoed.body.expect("body");
    assert_eq!(echoed["ok"], true);
    assert_eq!(echoed["reply"], "hello");

    // Unknown method: ok=false with the generic client-facing message.
    let unknown = send_json(
        addr,
        "POST",
        "/console/v1/runtime/rpc/nope",
        &admin_token,
        Some("{}"),
    )
    .await;
    assert_eq!(unknown.status, 200);
    assert_eq!(unknown.body.expect("body")["ok"], false);

    // Viewer role cannot invoke RPCs.
    let viewer_token = login(addr, "ops", "viewer-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let forbidden = send_json(
        addr,
        "POST",
        "/console/v1/runtime/rpc/ping",
        &viewer_token,
        Some("{}"),
    )
    .await;
    assert_eq!(forbidden.status, 403);

    // The invocation is audited.
    let audit = get(addr, "/console/v1/audit?action=runtime", Some(&admin_token)).await;
    let entries = audit.body.expect("audit")["entries"]
        .as_array()
        .expect("entries")
        .len();
    assert!(entries >= 2, "rpc invocations recorded");

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn audit_trail_records_logins_and_supports_filters() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    // Produce one failure then one admin success.
    assert_eq!(login(addr, "ops", "wrong").await.status, 401);
    let admin = login(addr, "ops", "operator-secret").await;
    let token = admin.body.expect("login body")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Newest-first: the successful login precedes the failure in the page.
    let page = get(addr, "/console/v1/audit", Some(&token)).await;
    assert_eq!(page.status, 200);
    let body = page.body.expect("audit body");
    let entries = body["entries"].as_array().expect("entries");
    assert!(entries.len() >= 2, "both login events recorded");
    assert_eq!(entries[0]["action"], "console.login");
    assert_eq!(entries[0]["actor"], "ops");
    assert_eq!(entries[0]["role"], "admin");
    assert_eq!(entries[1]["action"], "console.login_failed");
    assert_eq!(entries[1]["role"], "-");
    assert!(body["capacity"].as_u64().expect("capacity") >= 2);

    // Action prefix filter narrows to failures only.
    let failures = get(
        addr,
        "/console/v1/audit?action=console.login_failed",
        Some(&token),
    )
    .await;
    let failures = failures.body.expect("filtered body");
    let failure_entries = failures["entries"].as_array().expect("entries");
    assert_eq!(failure_entries.len(), 1);
    assert_eq!(failure_entries[0]["action"], "console.login_failed");
    // The trail never carries the presented password.
    assert!(
        !serde_json::to_string(&failures)
            .expect("serialize")
            .contains("wrong"),
        "audit body must not contain credentials"
    );

    // limit=1 pages down to the newest entry.
    let limited = get(addr, "/console/v1/audit?limit=1", Some(&token)).await;
    assert_eq!(
        limited.body.expect("limited")["entries"]
            .as_array()
            .expect("entries")
            .len(),
        1
    );

    // A typo'd query parameter is a 400, not a silently-ignored filter.
    let typo = get(addr, "/console/v1/audit?actr=ops", Some(&token)).await;
    assert_eq!(typo.status, 400);

    // The trail requires authentication like every section.
    assert_eq!(get(addr, "/console/v1/audit", None).await.status, 401);

    let _ = tx.send(());
    let _ = server.await;
}
