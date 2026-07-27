//! Integration test for the node dashboard endpoints (`/status`, `/dashboard`).
//!
//! Binds an ephemeral port, runs the real server with a controllable graceful
//! shutdown, issues raw HTTP/1.1 GETs (avoiding an HTTP client dependency), and
//! asserts the JSON status and HTML dashboard responses.

use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

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

async fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    read_http_response(&mut stream).await
}

#[tokio::test]
async fn status_and_dashboard_endpoints_respond() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    let app = App::new(Config::default());

    let (tx, rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = rx.await;
    };
    let server = tokio::spawn(async move { http::serve(listener, app, shutdown).await });

    // JSON status.
    let status = get(addr, http::STATUS_PATH).await;
    assert!(
        status.starts_with("HTTP/1.1 200"),
        "expected 200 OK for status, got: {status}"
    );
    assert!(
        status.contains("\"status\":\"healthy\""),
        "expected healthy status, got: {status}"
    );
    assert!(
        status.contains("\"node_id\":\"dev-1\""),
        "expected node_id, got: {status}"
    );
    assert!(
        status.contains("\"transports\""),
        "expected transports section, got: {status}"
    );
    assert!(
        status.contains("\"metrics\""),
        "expected metrics section, got: {status}"
    );

    // HTML console (the Nakama-style navy admin console, ).
    let dash = get(addr, http::DASHBOARD_PATH).await;
    assert!(
        dash.starts_with("HTTP/1.1 200"),
        "expected 200 OK for dashboard, got: {dash}"
    );
    assert!(
        dash.contains("text/html"),
        "expected HTML content type, got: {dash}"
    );
    assert!(
        dash.contains("Citadel <span>Console</span>"),
        "expected console brand, got: {dash}"
    );
    // The console must ship the full Nakama-style section navigation.
    for label in [
        "Status",
        "Accounts",
        "Groups",
        "Chat",
        "Notifications",
        "Storage",
        "Leaderboards",
        "Matches",
        "Purchases & Subscriptions",
        "Configuration",
        "API Explorer / Runtime",
        "Audit Logs",
    ] {
        assert!(
            dash.contains(label),
            "expected navigation label {label:?} in console, got: {dash}"
        );
    }
    // The Status section reads the live /status endpoint.
    assert!(
        dash.contains("fetch('/status'"),
        "expected Status section to fetch /status, got: {dash}"
    );

    // The liveness/health endpoint still works unchanged.
    let health = get(addr, http::HEALTH_PATH).await;
    assert!(
        health.starts_with("HTTP/1.1 200"),
        "expected 200 OK for health, got: {health}"
    );
    assert!(
        health.contains("\"status\":\"healthy\""),
        "expected healthy health body, got: {health}"
    );

    tx.send(()).expect("send shutdown");
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server task should stop after shutdown")
        .expect("server task should not panic");
    assert!(result.is_ok(), "serve returned an error: {result:?}");
}
