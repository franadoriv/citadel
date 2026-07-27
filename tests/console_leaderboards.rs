//! Integration tests for the console Leaderboards section.
//!
//! Binds an ephemeral port, runs the real server, and drives the console
//! Leaderboards API end to end with raw HTTP/1.1 requests: board creation
//! under both sort orders, operator semantics via repeated submissions,
//! ranked record listing, record/board deletion, the duplicate-id conflict,
//! the viewer role's mutation guard, and the audit trail.
//!
//! HTTP plumbing (response parsing, `login`, `get`/`send_json` helpers) is
//! copied from `tests/console_api_auth.rs` rather than shared, per that file's
//! own "COPY into a new file, do not edit it" contract.

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
async fn leaderboards_section_supports_full_lifecycle_and_ranking() {
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

    // Empty node: 200 with no boards (also exercised bare by console_api_auth
    // via IMPLEMENTED_SECTION_PATHS).
    let empty = get(addr, "/console/v1/leaderboards", Some(&admin_token)).await;
    assert_eq!(empty.status, 200);
    let empty_body = empty.body.expect("body");
    assert_eq!(empty_body["total"], 0);
    assert_eq!(empty_body["items"].as_array().expect("items").len(), 0);

    // Create a `desc` board (default sort/operator: desc/best) and an `asc`
    // board with an explicit operator and reset schedule.
    let desc_board = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards",
        &admin_token,
        Some(r#"{"id":"points"}"#),
    )
    .await;
    assert_eq!(desc_board.status, 201);
    let desc_body = desc_board.body.expect("body");
    assert_eq!(desc_body["id"], "points");
    assert_eq!(desc_body["sort"], "desc");
    assert_eq!(desc_body["operator"], "best");
    assert_eq!(desc_body["records"], 0);

    let asc_board = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards",
        &admin_token,
        Some(r#"{"id":"race_time","sort":"asc","operator":"best","reset_schedule":"0 0 * * *"}"#),
    )
    .await;
    assert_eq!(asc_board.status, 201);
    assert_eq!(asc_board.body.expect("body")["sort"], "asc");

    // Duplicate id is a 409 conflict.
    let dup = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards",
        &admin_token,
        Some(r#"{"id":"points"}"#),
    )
    .await;
    assert_eq!(dup.status, 409);

    // A viewer cannot create a board.
    let viewer_create = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards",
        &viewer_token,
        Some(r#"{"id":"viewer-board"}"#),
    )
    .await;
    assert_eq!(viewer_create.status, 403);
    assert_eq!(viewer_create.body.expect("body")["code"], "forbidden");

    // Submit scores on the desc/best board: charlie's best score wins,
    // alpha/bravo tie at 50 and rank by user_id (deterministic tie order).
    for (user, score) in [("bravo", 50), ("alpha", 50), ("charlie", 90)] {
        let submitted = send_json(
            addr,
            "POST",
            "/console/v1/leaderboards/points/records",
            &admin_token,
            Some(&format!(r#"{{"user_id":"{user}","score":{score}}}"#)),
        )
        .await;
        assert_eq!(submitted.status, 200, "submit {user}");
    }
    // charlie submits a worse score; `best` keeps the higher one.
    let worse = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards/points/records",
        &admin_token,
        Some(r#"{"user_id":"charlie","score":10,"metadata":{"note":"worse"}}"#),
    )
    .await;
    assert_eq!(worse.status, 200);
    let worse_body = worse.body.expect("body");
    assert_eq!(worse_body["score"], 90, "best keeps the higher score");
    assert_eq!(worse_body["submissions"], 2);

    let ranked = get(
        addr,
        "/console/v1/leaderboards/points/records",
        Some(&viewer_token),
    )
    .await;
    assert_eq!(ranked.status, 200);
    let ranked_body = ranked.body.expect("body");
    assert_eq!(ranked_body["board"], "points");
    assert_eq!(ranked_body["total"], 3);
    let items = ranked_body["items"].as_array().expect("items");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["user_id"], "charlie");
    assert_eq!(items[0]["rank"], 1);
    assert_eq!(items[1]["user_id"], "alpha");
    assert_eq!(items[1]["rank"], 2);
    assert_eq!(items[2]["user_id"], "bravo");
    assert_eq!(items[2]["rank"], 3);

    // Pagination: limit=1&offset=1 returns just the second-ranked row.
    let paged = get(
        addr,
        "/console/v1/leaderboards/points/records?limit=1&offset=1",
        Some(&admin_token),
    )
    .await;
    let paged_body = paged.body.expect("body");
    let paged_items = paged_body["items"].as_array().expect("items");
    assert_eq!(paged_items.len(), 1);
    assert_eq!(paged_items[0]["user_id"], "alpha");
    assert_eq!(paged_items[0]["rank"], 2);

    // A viewer cannot submit either.
    let viewer_submit = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards/points/records",
        &viewer_token,
        Some(r#"{"user_id":"eve","score":1}"#),
    )
    .await;
    assert_eq!(viewer_submit.status, 403);

    // Delete a record, confirm it drops off the ranking.
    let deleted_record = send_json(
        addr,
        "DELETE",
        "/console/v1/leaderboards/points/records/bravo",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted_record.status, 204);
    let after_delete = get(
        addr,
        "/console/v1/leaderboards/points/records",
        Some(&admin_token),
    )
    .await;
    assert_eq!(after_delete.body.expect("body")["total"], 2);

    // Viewer cannot delete records or boards.
    assert_eq!(
        send_json(
            addr,
            "DELETE",
            "/console/v1/leaderboards/points/records/alpha",
            &viewer_token,
            None,
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_json(
            addr,
            "DELETE",
            "/console/v1/leaderboards/points",
            &viewer_token,
            None
        )
        .await
        .status,
        403
    );

    // Delete the board; it disappears from the listing.
    let deleted_board = send_json(
        addr,
        "DELETE",
        "/console/v1/leaderboards/points",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted_board.status, 204);
    let listing = get(addr, "/console/v1/leaderboards", Some(&admin_token)).await;
    let listing_body = listing.body.expect("body");
    let ids: Vec<String> = listing_body["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| item["id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(ids, vec!["race_time"]);

    // Records/deletes against the now-deleted board are 404.
    assert_eq!(
        get(
            addr,
            "/console/v1/leaderboards/points/records",
            Some(&admin_token)
        )
        .await
        .status,
        404
    );

    // The audit trail carries every leaderboards mutation.
    let audit = get(
        addr,
        "/console/v1/audit?action=leaderboards",
        Some(&admin_token),
    )
    .await;
    let audit_body = audit.body.expect("body");
    let actions: Vec<String> = audit_body["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(actions.contains(&"leaderboards.create".to_string()));
    assert!(actions.contains(&"leaderboards.delete".to_string()));
    assert!(actions.contains(&"leaderboards.record.submit".to_string()));
    assert!(actions.contains(&"leaderboards.record.delete".to_string()));
    // Rejected viewer mutations never reach the trail.
    assert!(
        !serde_json::to_string(&audit_body)
            .expect("serialize")
            .contains("viewer-board")
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn incr_operator_accumulates_across_submissions() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;
    let admin_token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    let created = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards",
        &admin_token,
        Some(r#"{"id":"kills","sort":"desc","operator":"incr"}"#),
    )
    .await;
    assert_eq!(created.status, 201);

    send_json(
        addr,
        "POST",
        "/console/v1/leaderboards/kills/records",
        &admin_token,
        Some(r#"{"user_id":"u1","score":3}"#),
    )
    .await;
    let second = send_json(
        addr,
        "POST",
        "/console/v1/leaderboards/kills/records",
        &admin_token,
        Some(r#"{"user_id":"u1","score":4}"#),
    )
    .await;
    assert_eq!(second.status, 200);
    let body = second.body.expect("body");
    assert_eq!(body["score"], 7, "incr adds submissions together");
    assert_eq!(body["submissions"], 2);

    let _ = tx.send(());
    let _ = server.await;
}
