//! Integration tests for console Purchases & Subscriptions.

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
    let mut chunk = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let auth = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
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
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}{content}Connection: close\r\n\r\n{body_text}"
    );
    stream.write_all(request.as_bytes()).await.expect("write");
    let raw = read_http_response(&mut stream).await;
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .filter(|b| !b.trim().is_empty())
        .and_then(|b| serde_json::from_str::<Value>(b.trim()).ok());
    Response { status, body }
}

fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.console.viewer_password = Some("viewer-secret".to_string());
    config.validate().expect("valid config");
    config
}

async fn login(addr: SocketAddr, password: &str) -> String {
    let response = send(
        addr,
        "POST",
        http::LOGIN_PATH,
        None,
        Some(&format!(r#"{{"username":"ops","password":"{password}"}}"#)),
    )
    .await;
    response.body.expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string()
}

#[tokio::test]
async fn purchases_validate_list_detail_and_subscriptions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, App::new(console_config()), async move {
            let _ = rx.await;
        })
        .await;
    });
    let admin = login(addr, "operator-secret").await;
    let viewer = login(addr, "viewer-secret").await;

    // Validate a consumable and a subscription through the dev validator.
    let consumable = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&admin),
        Some(
            r#"{"user_id":"u-1","store":"custom","receipt":"{\"transaction_id\":\"tx-1\",\"product_id\":\"gold-pack\"}"}"#,
        ),
    )
    .await;
    assert_eq!(consumable.status, 201);
    let consumable = consumable.body.expect("purchase");
    assert_eq!(consumable["product_id"], "gold-pack");
    assert_eq!(
        consumable["receipt_sha256"].as_str().expect("hash").len(),
        64
    );

    let subscription = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&admin),
        Some(
            r#"{"user_id":"u-1","store":"custom","receipt":"{\"transaction_id\":\"tx-2\",\"product_id\":\"vip\",\"subscription_expiry_unix_ms\":99999999999999}"}"#,
        ),
    )
    .await;
    assert_eq!(subscription.status, 201);

    // The assembled application uses the composite validator: a JSON-shaped
    // Apple receipt is not mistaken for production validation before the Apple
    // provider adapter is implemented and explicitly enabled.
    let disabled_store = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&admin),
        Some(
            r#"{"user_id":"u-1","store":"apple","receipt":"{\"transaction_id\":\"tx-apple\",\"product_id\":\"vip\"}"}"#,
        ),
    )
    .await;
    assert_eq!(disabled_store.status, 403);
    assert_eq!(disabled_store.body.expect("error")["code"], "forbidden");

    // Replaying the same receipt is a conflict.
    let replay = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&admin),
        Some(
            r#"{"user_id":"u-2","store":"custom","receipt":"{\"transaction_id\":\"tx-1\",\"product_id\":\"gold-pack\"}"}"#,
        ),
    )
    .await;
    assert_eq!(replay.status, 409);

    // A malformed receipt is a validation error.
    let malformed = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&admin),
        Some(r#"{"user_id":"u-1","store":"custom","receipt":"not json"}"#),
    )
    .await;
    assert_eq!(malformed.status, 400);

    // Listing (newest first) + user filter + detail.
    let listed = send(addr, "GET", "/console/v1/purchases", Some(&viewer), None).await;
    assert_eq!(listed.status, 200);
    let items = listed.body.expect("page")["items"]
        .as_array()
        .expect("items")
        .clone();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["transaction_id"], "tx-2");
    let detail = send(
        addr,
        "GET",
        "/console/v1/purchases/tx-1",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(detail.status, 200);
    assert_eq!(
        send(
            addr,
            "GET",
            "/console/v1/purchases/tx-nope",
            Some(&viewer),
            None
        )
        .await
        .status,
        404
    );

    // Subscriptions derive live status; consumables are excluded.
    let subs = send(
        addr,
        "GET",
        "/console/v1/subscriptions",
        Some(&viewer),
        None,
    )
    .await;
    assert_eq!(subs.status, 200);
    let subs = subs.body.expect("subs")["items"]
        .as_array()
        .expect("items")
        .clone();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0]["status"], "active");

    // Viewer cannot validate receipts; the mutation is audited.
    let forbidden = send(
        addr,
        "POST",
        "/console/v1/purchases",
        Some(&viewer),
        Some(r#"{"user_id":"u-1","store":"custom","receipt":"{}"}"#),
    )
    .await;
    assert_eq!(forbidden.status, 403);
    let audit = send(
        addr,
        "GET",
        "/console/v1/audit?action=purchases",
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(
        audit.body.expect("audit")["entries"]
            .as_array()
            .expect("entries")
            .len(),
        2,
        "both successful validations audited"
    );

    let _ = tx.send(());
    let _ = server.await;
}
