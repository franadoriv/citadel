//! End-to-end contract for the console tournament operations API.
//!
//! The real HTTP server is driven with an operator bearer token so this covers
//! authentication, role authorization, lifecycle mutation, discovery, entries,
//! results, and audit records at the public console boundary.

use std::net::SocketAddr;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Response {
    status: u16,
    body: Option<Value>,
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_response(raw: &str) -> Response {
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("HTTP status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .filter(|body| !body.trim().is_empty())
        .and_then(|body| serde_json::from_str(body.trim()).ok());
    Response { status, body }
}

async fn send_json(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let body = body.unwrap_or("");
    let authorization = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let content = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}{content}Connection: close\r\n\r\n{body}"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    parse_response(&read_http_response(&mut stream).await)
}

async fn spawn_server(app: App) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, app, async move {
            let _ = stopped.await;
        })
        .await;
    });
    (address, shutdown, server)
}

fn config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.console.viewer_password = Some("viewer-secret".to_string());
    config.validate().expect("config");
    config
}

async fn login(addr: SocketAddr, password: &str) -> String {
    send_json(
        addr,
        "POST",
        http::LOGIN_PATH,
        None,
        Some(&format!(r#"{{"username":"ops","password":"{password}"}}"#)),
    )
    .await
    .body
    .expect("login body")["token"]
        .as_str()
        .expect("token")
        .to_string()
}

#[tokio::test]
async fn console_admin_can_operate_tournament_lifecycle_and_view_results() {
    let (addr, shutdown, server) = spawn_server(App::new(config())).await;
    let admin = login(addr, "operator-secret").await;
    let viewer = login(addr, "viewer-secret").await;

    let created = send_json(
        addr,
        "POST",
        "/console/v1/tournaments",
        Some(&admin),
        Some(r#"{"id":"weekly","leaderboard_id":"points","registration_opens_at_unix_ms":100,"registration_closes_at_unix_ms":200,"starts_at_unix_ms":200,"ends_at_unix_ms":300}"#),
    )
    .await;
    assert_eq!(created.status, 201);
    assert_eq!(created.body.expect("created")["state"], "draft");

    let duplicate = send_json(
        addr,
        "POST",
        "/console/v1/tournaments",
        Some(&viewer),
        Some(r#"{"id":"other","leaderboard_id":"points","registration_opens_at_unix_ms":100,"registration_closes_at_unix_ms":200,"starts_at_unix_ms":200,"ends_at_unix_ms":300}"#),
    )
    .await;
    assert_eq!(duplicate.status, 403);

    for state in ["registration_open", "running"] {
        let transitioned = send_json(
            addr,
            "POST",
            "/console/v1/tournaments/weekly/transition",
            Some(&admin),
            Some(&format!(r#"{{"state":"{state}"}}"#)),
        )
        .await;
        assert_eq!(transitioned.status, 200);
        assert_eq!(transitioned.body.expect("transition")["state"], state);
    }

    let scheduler_owned = send_json(
        addr,
        "POST",
        "/console/v1/tournaments/weekly/transition",
        Some(&admin),
        Some(r#"{"state":"finalizing"}"#),
    )
    .await;
    assert_eq!(scheduler_owned.status, 409);

    let listing = send_json(addr, "GET", "/console/v1/tournaments", Some(&viewer), None).await;
    assert_eq!(listing.status, 200);
    assert_eq!(listing.body.expect("listing")["items"][0]["id"], "weekly");

    let detail = send_json(
        addr,
        "GET",
        "/console/v1/tournaments/weekly",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(detail.status, 200);
    assert_eq!(detail.body.expect("detail")["state"], "running");

    let entries = send_json(
        addr,
        "GET",
        "/console/v1/tournaments/weekly/entries",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(entries.status, 200);
    assert_eq!(
        entries.body.expect("entries")["items"]
            .as_array()
            .expect("items")
            .len(),
        0
    );

    let results = send_json(
        addr,
        "GET",
        "/console/v1/tournaments/weekly/results",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(results.status, 200);
    assert_eq!(
        results.body.expect("results")["items"]
            .as_array()
            .expect("items")
            .len(),
        0
    );

    let audit = send_json(
        addr,
        "GET",
        "/console/v1/audit?action=tournaments",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(audit.status, 200);
    assert!(
        audit.body.expect("audit")["entries"]
            .as_array()
            .expect("items")
            .iter()
            .any(|row| row["action"] == "tournaments.transition")
    );

    let _ = shutdown.send(());
    let _ = server.await;
}
