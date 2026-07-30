//! Integration tests for the live console SPA shell.
//!
//! Binds an ephemeral port, runs the real server, and verifies the served
//! `/dashboard` document is the fully live console (login wired to
//! `/console/v1/login`, no placeholder affordances left), then smoke-tests
//! that the endpoints the SPA drives (accounts, audit) answer an
//! authenticated operator — proving the UI's data sources exist end to end.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::error::AppError;
use citadel::error_journal::{ErrorJournal, JournalIncident};
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

static NEXT_JOURNAL_PATH: AtomicU64 = AtomicU64::new(0);

/// A parsed HTTP response: status code and the raw (decoded) body text.
struct RawResponse {
    status: u16,
    body: String,
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_raw(raw: &str) -> RawResponse {
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("response should carry an HTTP status code");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    RawResponse { status, body }
}

/// Issue a raw HTTP/1.1 `GET`, optionally with a bearer token.
async fn get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> RawResponse {
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
    parse_raw(&raw)
}

/// Issue a raw HTTP/1.1 `POST` with a JSON body.
async fn post_json(addr: SocketAddr, path: &str, body: &str) -> RawResponse {
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
    parse_raw(&raw)
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

/// A config with non-default operator credentials.
fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.validate().expect("test config must validate");
    config
}

fn test_journal_path() -> PathBuf {
    let nonce = NEXT_JOURNAL_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "citadel-console-errors-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

#[tokio::test]
async fn dashboard_serves_the_fully_live_console_spa() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    // The SPA wires its login view to the console API...
    assert!(
        dashboard.body.contains("/console/v1/login"),
        "console SPA must reference the login endpoint"
    );
    // ...and ships no placeholder affordance anywhere.
    assert!(
        !dashboard.body.contains("Not yet implemented"),
        "console SPA must not contain placeholder sections"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn console_spa_data_sources_answer_an_authenticated_operator() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    // Login exactly like the SPA does.
    let login = post_json(
        addr,
        http::LOGIN_PATH,
        r#"{"username":"ops","password":"operator-secret"}"#,
    )
    .await;
    assert_eq!(login.status, 200);
    let login_body: Value = serde_json::from_str(login.body.trim()).expect("login JSON");
    let token = login_body["token"].as_str().expect("token").to_string();
    assert_eq!(login_body["role"], "admin");

    // A representative subset of the sections the UI renders on load.
    let accounts = get(addr, "/console/v1/accounts?limit=5", Some(&token)).await;
    assert_eq!(accounts.status, 200);
    let accounts_body: Value = serde_json::from_str(accounts.body.trim()).expect("accounts JSON");
    assert!(accounts_body["items"].is_array());

    let audit = get(addr, "/console/v1/audit?limit=10", Some(&token)).await;
    assert_eq!(audit.status, 200);
    let audit_body: Value = serde_json::from_str(audit.body.trim()).expect("audit JSON");
    // The login above is already in the trail the UI renders.
    assert!(
        audit_body["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|e| e["action"] == "console.login"),
        "audit trail should record the SPA-style login"
    );

    let errors = get(addr, "/console/v1/errors?limit=10", Some(&token)).await;
    assert_eq!(errors.status, 200);
    let errors_body: Value = serde_json::from_str(errors.body.trim()).expect("errors JSON");
    assert!(errors_body["entries"].is_array());
    assert!(errors_body["total"].is_u64());

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn error_journal_is_redacted_and_viewer_readable_end_to_end() {
    let mut config = console_config();
    config.console.viewer_password = Some("viewer-secret".to_string());
    let journal_path = test_journal_path();
    let journal = Arc::new(ErrorJournal::new(&journal_path));
    assert!(
        journal
            .append(JournalIncident::from_app_error(
                "repository.pg",
                &AppError::database("postgres://operator:very-secret@db.example/citadel")
                    .with_detail("token=very-secret"),
            ))
            .is_written()
    );
    let app = App::new(config).with_error_journal(Arc::clone(&journal));
    let (addr, tx, server) = spawn_server(app).await;

    let login = post_json(
        addr,
        http::LOGIN_PATH,
        r#"{"username":"ops","password":"viewer-secret"}"#,
    )
    .await;
    assert_eq!(login.status, 200);
    let token =
        serde_json::from_str::<Value>(login.body.trim()).expect("viewer login JSON")["token"]
            .as_str()
            .expect("viewer token")
            .to_string();

    let response = get(addr, "/console/v1/errors?offset=0&limit=9999", Some(&token)).await;
    assert_eq!(response.status, 200);
    assert!(!response.body.contains("very-secret"));
    assert!(!response.body.contains("postgres://"));
    let body: Value = serde_json::from_str(response.body.trim()).expect("errors JSON");
    let entry = body["entries"]
        .as_array()
        .and_then(|entries| entries.first())
        .expect("one redacted incident");
    assert_eq!(entry["category"], "database");
    assert_eq!(entry["component"], "repository.pg");
    assert_eq!(entry["message"], "database failure");

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_file(journal_path);
}
