//! Integration tests for the device/custom HTTP authentication routes
//!.
//!
//! Binds an ephemeral port, runs the real server with a controllable graceful
//! shutdown, and issues raw HTTP/1.1 `POST`s (avoiding an HTTP client
//! dependency). The same scenario suite runs against the in-memory backend and,
//! opt-in via `DATABASE_URL`, against a real Postgres backend so the persistent
//! path is exercised too.
//!
//! ```text
//! make db-up
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo test --test http_auth
//! make db-down
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use citadel::repository::Backend;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// A parsed HTTP response: status code and (optional) JSON body.
struct Response {
    status: u16,
    body: Option<Value>,
    retry_after: Option<String>,
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

/// Issue a raw HTTP/1.1 `POST` with a JSON body and parse the response.
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

    let retry_after = raw
        .lines()
        .find_map(|line| {
            line.strip_prefix("retry-after: ")
                .or_else(|| line.strip_prefix("Retry-After: "))
        })
        .map(str::to_string);

    Response {
        status,
        body,
        retry_after,
    }
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

#[tokio::test]
async fn authentication_rate_limit_is_uniform_and_returns_retry_after() {
    let mut config = Config::default();
    config.authentication.limits.source.limit = 2;
    config.authentication.limits.source.window_ms = 2_000;
    let (addr, tx, server) = spawn_server(App::new(config)).await;

    for id in ["limited-device-one", "limited-device-two"] {
        let accepted = post_json(
            addr,
            http::DEVICE_AUTH_PATH,
            &format!(r#"{{"id":"{id}","create":true,"username":"{id}"}}"#),
        )
        .await;
        assert_eq!(accepted.status, 201);
    }

    let rejected = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        r#"{"id":"limited-device-three","create":true,"username":"limited-three"}"#,
    )
    .await;
    assert_eq!(rejected.status, 429);
    // A registration is subject to both its short source window and the
    // hour-long registration window; `Retry-After` conservatively covers the
    // complete multi-key plan without revealing which key was exhausted.
    assert_eq!(rejected.retry_after.as_deref(), Some("3600"));
    assert_eq!(
        rejected
            .body
            .as_ref()
            .and_then(|body| body["code"].as_str()),
        Some("rate_limited")
    );

    let _ = tx.send(());
    let _ = server.await;
}

/// The full scenario suite, run against any assembled `App`/backend.
///
/// `device_id`/`custom_id` are per-backend unique so a shared Postgres database
/// does not collide across runs.
async fn run_auth_scenarios(app: App, device_id: &str, custom_id: &str, username: &str) {
    let (addr, tx, server) = spawn_server(app).await;

    // 1. First device auth with create=true registers and returns a token.
    let created = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        &format!(r#"{{"id":"{device_id}","create":true,"username":"{username}"}}"#),
    )
    .await;
    assert_eq!(created.status, 201, "first device auth should create (201)");
    let created_body = created.body.expect("device create body");
    let token = created_body["token"].as_str().expect("access token");
    assert!(!token.is_empty(), "token must be non-empty");
    assert_eq!(created_body["created"], true);
    assert_eq!(created_body["username"], username);
    let first_user_id = created_body["user_id"]
        .as_str()
        .expect("user_id")
        .to_string();

    // 2. Same device id again is idempotent: same user, created=false.
    let repeat = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        &format!(r#"{{"id":"{device_id}","create":false}}"#),
    )
    .await;
    assert_eq!(repeat.status, 200, "returning device auth should be 200");
    let repeat_body = repeat.body.expect("device repeat body");
    assert_eq!(repeat_body["created"], false);
    assert_eq!(
        repeat_body["user_id"].as_str().expect("user_id"),
        first_user_id,
        "same device id must map to the same account"
    );
    assert!(
        !repeat_body["token"].as_str().expect("token").is_empty(),
        "returning auth still issues a fresh token"
    );

    // 3. create=false for an unknown id fails uniformly without creating.
    let unknown = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        r#"{"id":"totally-unknown-device","create":false}"#,
    )
    .await;
    assert_eq!(unknown.status, 401, "unknown id without create is 401");
    let unknown_body = unknown.body.expect("error body");
    assert_eq!(unknown_body["code"], "authentication_failed");
    assert_eq!(unknown_body["message"], "authentication failed");
    // The error must not carry any account/user field (no existence oracle).
    assert!(unknown_body.get("user_id").is_none());
    assert!(unknown_body.get("token").is_none());

    // 4. Custom auth is analogous: create then idempotent reuse.
    let custom_created = post_json(
        addr,
        http::CUSTOM_AUTH_PATH,
        &format!(r#"{{"id":"{custom_id}","create":true,"username":"{username}-c"}}"#),
    )
    .await;
    assert_eq!(custom_created.status, 201, "custom create should be 201");
    let custom_body = custom_created.body.expect("custom body");
    let custom_user_id = custom_body["user_id"]
        .as_str()
        .expect("user_id")
        .to_string();
    assert_eq!(custom_body["created"], true);

    let custom_repeat = post_json(
        addr,
        http::CUSTOM_AUTH_PATH,
        &format!(r#"{{"id":"{custom_id}"}}"#),
    )
    .await;
    assert_eq!(custom_repeat.status, 200);
    assert_eq!(
        custom_repeat.body.expect("body")["user_id"]
            .as_str()
            .expect("user_id"),
        custom_user_id,
        "same custom id must map to the same account"
    );

    // A device credential and a custom credential are distinct accounts.
    assert_ne!(
        first_user_id, custom_user_id,
        "device and custom credentials map to distinct accounts"
    );

    // 5. Email/password creates once, accepts a normalized returning email,
    // and gives the same credential error for an unknown email and bad password.
    let email_created = post_json(
        addr,
        http::EMAIL_AUTH_PATH,
        &format!(r#"{{"email":"Player-{username}@Example.COM","password":"correct horse battery staple","create":true,"username":"{username}-email"}}"#),
    )
    .await;
    assert_eq!(email_created.status, 201);
    let email_user_id = email_created.body.expect("email create body")["user_id"]
        .as_str()
        .expect("email user id")
        .to_owned();
    let email_repeat = post_json(
        addr,
        http::EMAIL_AUTH_PATH,
        &format!(r#"{{"email":"player-{username}@example.com","password":"correct horse battery staple"}}"#),
    )
    .await;
    assert_eq!(email_repeat.status, 200);
    assert_eq!(
        email_repeat.body.expect("email repeat")["user_id"],
        email_user_id
    );
    for body in [
        format!(r#"{{"email":"player-{username}@example.com","password":"wrong password"}}"#),
        format!(
            r#"{{"email":"unknown-{username}@example.com","password":"correct horse battery staple"}}"#
        ),
    ] {
        let rejected = post_json(addr, http::EMAIL_AUTH_PATH, &body).await;
        assert_eq!(rejected.status, 401);
        assert_eq!(
            rejected.body.expect("auth error")["code"],
            "authentication_failed"
        );
    }

    // 6. Invalid input (empty id, well-formed JSON) is a typed 400.
    let invalid = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        r#"{"id":"","create":true,"username":"whoever"}"#,
    )
    .await;
    assert_eq!(invalid.status, 400, "empty id is a validation error (400)");
    assert_eq!(invalid.body.expect("error body")["code"], "invalid_request");

    // 6. create=true without a username is a validation error, not a 500.
    let missing_username = post_json(
        addr,
        http::DEVICE_AUTH_PATH,
        r#"{"id":"needs-a-name","create":true}"#,
    )
    .await;
    assert_eq!(missing_username.status, 400);

    // 7. Malformed JSON and unknown fields are normalized to the SAME uniform
    //    400 request-shape error (no leaked parser detail), not axum's default.
    for bad in [r#"{"id": "#, r#"{"id":"x","bogus":true}"#] {
        let rejected = post_json(addr, http::DEVICE_AUTH_PATH, bad).await;
        assert_eq!(rejected.status, 400, "bad body {bad:?} should be 400");
        assert_eq!(
            rejected.body.expect("error body")["code"],
            "invalid_request",
            "bad body {bad:?} should map to invalid_request"
        );
    }

    tx.send(()).expect("send shutdown");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server task should stop")
        .expect("server task should not panic");
}

#[tokio::test]
async fn device_and_custom_auth_over_in_memory_backend() {
    let app = App::new(Config::default());
    run_auth_scenarios(app, "mem-device-1", "mem-custom-1", "mem_player").await;
}

// --- Postgres run (opt-in via DATABASE_URL) ---------------------------------
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

    #[tokio::test]
    async fn device_and_custom_auth_over_postgres_backend() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping Postgres HTTP auth: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run it"
            );
            return;
        };
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = PgDatabase::connect(&config)
            .await
            .expect("connect + migrate against the test Postgres");
        db.reset_storage_for_tests()
            .await
            .expect("reset storage before run");

        let backend: Arc<dyn Backend> = Arc::new(db);
        let app = App::with_backend(Config::default(), backend);
        run_auth_scenarios(app, "pg-device-1", "pg-custom-1", "pg_player").await;
    }
}
