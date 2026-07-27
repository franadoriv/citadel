//! Integration test for the HTTP health endpoint, startup, and shutdown.
//!
//! Binds an ephemeral port, runs the real server with a controllable graceful
//! shutdown, issues a raw HTTP/1.1 GET to `/health` over TCP (avoiding an HTTP
//! client dependency), asserts the response, then triggers shutdown and
//! confirms the server task completes.

use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

async fn read_http_response(stream: &mut TcpStream) -> String {
    // The client request sends `Connection: close`, so the server closes the
    // socket after the full response. Read to EOF (bounded by a timeout).
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

#[tokio::test]
async fn health_endpoint_responds_and_server_shuts_down() {
    // Bind an ephemeral port on loopback so the test is hermetic.
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

    // Connect and issue a raw HTTP/1.1 GET for the health path.
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        http::HEALTH_PATH,
        addr
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let response = read_http_response(&mut stream).await;

    assert!(
        response.starts_with("HTTP/1.1 200"),
        "expected 200 OK, got: {response}"
    );
    assert!(
        response.contains("\"status\":\"healthy\""),
        "expected healthy status in body, got: {response}"
    );
    assert!(
        response.contains("\"node_id\":\"dev-1\""),
        "expected node_id in body, got: {response}"
    );

    // Trigger graceful shutdown and confirm the server task completes.
    tx.send(()).expect("send shutdown");
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server task should stop after shutdown")
        .expect("server task should not panic");
    assert!(result.is_ok(), "serve returned an error: {result:?}");
}

#[tokio::test]
async fn bind_rejects_invalid_address() {
    let mut config = Config::default();
    config.http.bind = "not-an-address".to_string();
    let app = App::new(config);
    let err = http::bind(&app).await.expect_err("invalid bind must fail");
    assert_eq!(err.category(), citadel::ErrorCategory::Config);
}
