//! Integration tests for the console Notifications section.
//!
//! Helpers below are copied from `tests/console_api_auth.rs` (not shared via a
//! module, per that file's own "integration helpers" convention) so this file
//! stays a self-contained end-to-end driver: login, send targeted + broadcast,
//! list with a user filter, delete, viewer-role rejection, and audit trail
//! coverage.

use std::net::SocketAddr;
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

#[tokio::test]
async fn notifications_section_supports_send_target_broadcast_and_delete() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let admin_token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let viewer_token = login(addr, "ops", "viewer-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Bare GET with no params must succeed with an empty page.
    let empty = get(addr, "/console/v1/notifications", Some(&admin_token)).await;
    assert_eq!(empty.status, 200);
    let empty = empty.body.expect("body");
    assert_eq!(empty["items"].as_array().expect("items").len(), 0);
    assert_eq!(empty["total"], 0);

    // Send a targeted notification to u-1.
    let targeted = send_json(
        addr,
        "POST",
        "/console/v1/notifications",
        &admin_token,
        Some(r#"{"user_id":"u-1","subject":"welcome","content":{"level":1},"code":7}"#),
    )
    .await;
    assert_eq!(targeted.status, 201);
    let targeted_body = targeted.body.expect("body");
    assert_eq!(targeted_body["user_id"], "u-1");
    assert_eq!(targeted_body["subject"], "welcome");
    assert_eq!(targeted_body["content"]["level"], 1);
    assert_eq!(targeted_body["code"], 7);
    assert_eq!(targeted_body["read"], false);
    let targeted_id = targeted_body["id"].as_u64().expect("id");

    // Send a targeted notification to a different user (u-2).
    let other_user = send_json(
        addr,
        "POST",
        "/console/v1/notifications",
        &admin_token,
        Some(r#"{"user_id":"u-2","subject":"for u2"}"#),
    )
    .await;
    assert_eq!(other_user.status, 201);
    assert_eq!(
        other_user.body.expect("body")["content"],
        serde_json::json!({}),
        "content defaults to an empty object"
    );

    // Send a broadcast (no user_id).
    let broadcast = send_json(
        addr,
        "POST",
        "/console/v1/notifications",
        &admin_token,
        Some(r#"{"subject":"server maintenance"}"#),
    )
    .await;
    assert_eq!(broadcast.status, 201);
    assert!(broadcast.body.expect("body")["user_id"].is_null());

    // Listing everything (no filter) sees all three.
    let all = get(addr, "/console/v1/notifications", Some(&admin_token)).await;
    let all = all.body.expect("body");
    assert_eq!(all["total"], 3);
    let all_items = all["items"].as_array().expect("items");
    assert_eq!(all_items.len(), 3);
    // Newest first.
    assert_eq!(all_items[0]["subject"], "server maintenance");

    // Filtering by u-1 sees its own targeted notification plus the broadcast,
    // but not u-2's targeted notification.
    let filtered = get(
        addr,
        "/console/v1/notifications?user_id=u-1",
        Some(&admin_token),
    )
    .await;
    let filtered = filtered.body.expect("body");
    assert_eq!(filtered["total"], 2);
    let filtered_subjects: Vec<String> = filtered["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|n| n["subject"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        filtered_subjects,
        vec!["server maintenance".to_string(), "welcome".to_string()]
    );
    assert!(
        !filtered_subjects.contains(&"for u2".to_string()),
        "u-1's view must not include u-2's targeted notification"
    );

    // Viewer role cannot send or delete.
    let viewer_send = send_json(
        addr,
        "POST",
        "/console/v1/notifications",
        &viewer_token,
        Some(r#"{"subject":"nope"}"#),
    )
    .await;
    assert_eq!(viewer_send.status, 403);
    let viewer_delete = send_json(
        addr,
        "DELETE",
        &format!("/console/v1/notifications/{targeted_id}"),
        &viewer_token,
        None,
    )
    .await;
    assert_eq!(viewer_delete.status, 403);

    // A viewer can still read the section (any role).
    let viewer_read = get(addr, "/console/v1/notifications", Some(&viewer_token)).await;
    assert_eq!(viewer_read.status, 200);

    // Empty subject is a 400, not silently accepted.
    let blank_subject = send_json(
        addr,
        "POST",
        "/console/v1/notifications",
        &admin_token,
        Some(r#"{"subject":""}"#),
    )
    .await;
    assert_eq!(blank_subject.status, 400);

    // Delete the targeted notification (admin) and confirm it is gone.
    let deleted = send_json(
        addr,
        "DELETE",
        &format!("/console/v1/notifications/{targeted_id}"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted.status, 204);
    let after_delete = get(addr, "/console/v1/notifications", Some(&admin_token)).await;
    assert_eq!(after_delete.body.expect("body")["total"], 2);

    // Deleting an unknown id is a 404.
    let unknown_delete = send_json(
        addr,
        "DELETE",
        "/console/v1/notifications/999999",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(unknown_delete.status, 404);

    // The mutations are in the audit trail.
    let audit = get(
        addr,
        "/console/v1/audit?action=notifications",
        Some(&admin_token),
    )
    .await;
    let audit = audit.body.expect("audit body");
    let actions: Vec<String> = audit["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        actions,
        vec![
            "notifications.delete".to_string(),
            "notifications.send".to_string(),
            "notifications.send".to_string(),
            "notifications.send".to_string(),
        ],
        "newest-first mutation trail"
    );

    let _ = tx.send(());
    let _ = server.await;
}
