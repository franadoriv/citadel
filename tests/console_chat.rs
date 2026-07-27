//! Integration tests for the console Chat section.
//!
//! Binds an ephemeral port, runs the real server, and drives channel/history
//! moderation end-to-end: append messages to two channels, list channels,
//! page history newest-first, tombstone a message, confirm the `viewer` role
//! cannot mutate, confirm mutations are audited, and confirm an unknown query
//! parameter is rejected. Helper functions are copied from
//! `tests/console_api_auth.rs` (that file is left untouched per project
//! convention) rather than shared, to keep integration test files independent.

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
async fn chat_history_and_moderation_end_to_end() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let admin_token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login body")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let viewer_token = login(addr, "ops", "viewer-secret")
        .await
        .body
        .expect("login body")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // Bare GET with no params must 200 (IMPLEMENTED_SECTION_PATHS contract).
    let empty = get(addr, "/console/v1/chat", Some(&admin_token)).await;
    assert_eq!(empty.status, 200);
    let empty_body = empty.body.expect("channels body");
    assert_eq!(empty_body["total"], 0);
    assert_eq!(empty_body["items"].as_array().expect("items").len(), 0);

    // Append messages to two channels through the console producer.
    let append_one = send_json(
        addr,
        "POST",
        "/console/v1/chat/lobby/messages",
        &admin_token,
        Some(r#"{"sender":"alice","content":"hello world"}"#),
    )
    .await;
    assert_eq!(append_one.status, 200);
    let append_one_body = append_one.body.expect("append body");
    assert_eq!(append_one_body["id"], 1);
    assert_eq!(append_one_body["sender"], "alice");
    assert_eq!(append_one_body["content"], "hello world");
    assert_eq!(append_one_body["deleted"], false);

    let append_two = send_json(
        addr,
        "POST",
        "/console/v1/chat/lobby/messages",
        &admin_token,
        Some(r#"{"sender":"bob","content":"hi alice"}"#),
    )
    .await;
    assert_eq!(append_two.status, 200);
    let second_id = append_two.body.expect("append body")["id"]
        .as_u64()
        .expect("id");
    assert_eq!(second_id, 2);

    // Second channel, explicit channel_type.
    let raid_append = send_json(
        addr,
        "POST",
        "/console/v1/chat/raid-1/messages",
        &admin_token,
        Some(r#"{"sender":"carol","content":"pulling boss","channel_type":"group"}"#),
    )
    .await;
    assert_eq!(raid_append.status, 200);

    // Channels list reports both channels with counts and activity.
    let channels = get(addr, "/console/v1/chat", Some(&admin_token)).await;
    assert_eq!(channels.status, 200);
    let channels_body = channels.body.expect("channels body");
    assert_eq!(channels_body["total"], 2);
    let items = channels_body["items"].as_array().expect("items");
    assert_eq!(items.len(), 2);
    let lobby = items
        .iter()
        .find(|c| c["channel"] == "lobby")
        .expect("lobby channel");
    assert_eq!(lobby["channel_type"], "room");
    assert_eq!(lobby["messages"], 2);
    let raid = items
        .iter()
        .find(|c| c["channel"] == "raid-1")
        .expect("raid channel");
    assert_eq!(raid["channel_type"], "group");
    assert_eq!(raid["messages"], 1);

    // Substring filter narrows the listing.
    let filtered = get(addr, "/console/v1/chat?filter=raid", Some(&admin_token)).await;
    let filtered_items = filtered.body.expect("body")["items"]
        .as_array()
        .expect("items")
        .len();
    assert_eq!(filtered_items, 1);

    // Paged history, newest first.
    let page = get(addr, "/console/v1/chat/lobby/messages", Some(&viewer_token)).await;
    assert_eq!(page.status, 200);
    let page_body = page.body.expect("page body");
    assert_eq!(page_body["channel"], "lobby");
    let page_items = page_body["items"].as_array().expect("items");
    assert_eq!(page_items.len(), 2);
    assert_eq!(page_items[0]["id"], 2, "newest first");
    assert_eq!(page_items[0]["sender"], "bob");
    assert_eq!(page_items[1]["id"], 1);

    // A one-item page with `before` resumes correctly.
    let limited = get(
        addr,
        "/console/v1/chat/lobby/messages?limit=1",
        Some(&viewer_token),
    )
    .await;
    let limited_body = limited.body.expect("body");
    assert_eq!(limited_body["items"][0]["id"], 2);
    let next = limited_body["next"].as_u64().expect("next cursor");
    let resumed = get(
        addr,
        &format!("/console/v1/chat/lobby/messages?limit=1&before={next}"),
        Some(&viewer_token),
    )
    .await;
    assert_eq!(
        resumed.body.expect("body")["items"][0]["id"],
        1,
        "before cursor resumes the page"
    );

    // Delete (tombstone) the second message: visible as deleted with empty
    // content, but still counted in the channel's message total.
    let deleted = send_json(
        addr,
        "DELETE",
        &format!("/console/v1/chat/lobby/messages/{second_id}"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted.status, 204);

    let after_delete = get(addr, "/console/v1/chat/lobby/messages", Some(&admin_token)).await;
    let after_items = after_delete.body.expect("body")["items"]
        .as_array()
        .expect("items")
        .clone();
    let tombstoned = after_items
        .iter()
        .find(|m| m["id"] == second_id)
        .expect("tombstoned message still present");
    assert_eq!(tombstoned["deleted"], true);
    assert_eq!(tombstoned["content"], "");

    // Deleting an unknown message id is 404.
    let missing = send_json(
        addr,
        "DELETE",
        "/console/v1/chat/lobby/messages/9999",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(missing.status, 404);

    // The viewer role cannot append or delete.
    let viewer_append = send_json(
        addr,
        "POST",
        "/console/v1/chat/lobby/messages",
        &viewer_token,
        Some(r#"{"sender":"eve","content":"nope"}"#),
    )
    .await;
    assert_eq!(viewer_append.status, 403);

    let viewer_delete = send_json(
        addr,
        "DELETE",
        "/console/v1/chat/lobby/messages/1",
        &viewer_token,
        None,
    )
    .await;
    assert_eq!(viewer_delete.status, 403);

    // The mutations are in the audit trail.
    let audit = get(addr, "/console/v1/audit?action=chat", Some(&admin_token)).await;
    let audit_body = audit.body.expect("audit body");
    let actions: Vec<String> = audit_body["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        actions,
        vec![
            "chat.message.delete",
            "chat.message.append",
            "chat.message.append",
            "chat.message.append"
        ],
        "newest-first mutation trail"
    );

    // A typo'd query parameter is rejected with 400, not silently ignored.
    let typo = get(addr, "/console/v1/chat?fitler=lobby", Some(&admin_token)).await;
    assert_eq!(typo.status, 400);
    let typo_messages = get(
        addr,
        "/console/v1/chat/lobby/messages?limt=5",
        Some(&admin_token),
    )
    .await;
    assert_eq!(typo_messages.status, 400);

    // Chat requires authentication like every other section.
    assert_eq!(get(addr, "/console/v1/chat", None).await.status, 401);

    let _ = tx.send(());
    let _ = server.await;
}
