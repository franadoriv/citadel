//! Integration test proving the node dashboard gauges are live.
//!
//! Wires the shared `Arc<NodeMetrics>` from an `App` into a `Gateway`, runs the
//! WebSocket transport against that gateway, connects two real clients, and
//! relays a position between them. It then GETs `/status` on the same `App` and
//! asserts the connection, session, and message gauges moved off zero — i.e. a
//! connect + relay is observable through the dashboard endpoint.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use citadel::lifecycle::Supervisor;
use citadel::realtime::Gateway;
use citadel::transport::codec::Envelope;
use citadel::transport::websocket::WebSocketServer;
use citadel_wire::protocol::KIND_POSITION;
use futures_util::SinkExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::ws_guest_handshake;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
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

async fn get(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    read_http_response(&mut stream).await
}

/// Extract the JSON body from a raw HTTP/1.1 response.
fn body_json(response: &str) -> serde_json::Value {
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or("");
    serde_json::from_str(body).expect("status body is JSON")
}

#[tokio::test]
async fn dashboard_status_reflects_live_transport_gauges() {
    let app = App::new(Config::default());

    // Gateway shares the app's metrics registry, exactly as `start_enabled` does.
    let gateway = Arc::new(Gateway::with_metrics(Arc::clone(app.metrics())));

    // WebSocket transport on an ephemeral port using the shared gateway.
    let ws = WebSocketServer::bind_with_gateway(loopback_any(), Arc::clone(&gateway))
        .await
        .expect("bind ws");
    let ws_addr = ws.local_addr();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(ws);

    // HTTP dashboard on an ephemeral port sharing the same `App` (same metrics).
    let http_listener = TcpListener::bind(loopback_any()).await.expect("bind http");
    let http_addr = http_listener.local_addr().expect("http addr");
    let (tx, rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = rx.await;
    };
    let http_app = app.clone();
    let server = tokio::spawn(async move { http::serve(http_listener, http_app, shutdown).await });

    // Two WebSocket clients join the shared room.
    let url = format!("ws://{ws_addr}/");
    let (mut client_a, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url.clone()),
    )
    .await
    .expect("client a connect did not time out")
    .expect("client a connected");
    let (mut client_b, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("client b connect did not time out")
    .expect("client b connected");

    // Both clients present the guest handshake so they register in the shared
    // gateway (they are guests: no session service is wired into this gateway).
    ws_guest_handshake(&mut client_a).await;
    ws_guest_handshake(&mut client_b).await;

    // Client A sends a position; the gateway relays it to client B.
    let payload = vec![9u8, 8, 7, 6];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    client_a
        .send(Message::Binary(env.encode_framed().to_vec()))
        .await
        .expect("client a sends");

    // Let the relay and gauge updates settle.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let status = get(http_addr, http::STATUS_PATH).await;
    assert!(
        status.starts_with("HTTP/1.1 200"),
        "expected 200 OK for status, got: {status}"
    );
    let json = body_json(&status);
    let metrics = &json["metrics"];

    let connections_active = metrics["connections_active"].as_u64().expect("u64");
    let connections_accepted = metrics["connections_accepted_total"].as_u64().expect("u64");
    let participants_active = metrics["participants_active"].as_u64().expect("u64");
    let sessions_active = metrics["sessions_active"].as_u64().expect("u64");
    let messages_in = metrics["messages_in_total"].as_u64().expect("u64");
    let messages_out = metrics["messages_out_total"].as_u64().expect("u64");

    assert_eq!(connections_active, 2, "both clients counted: {metrics}");
    assert_eq!(connections_accepted, 2, "two accepted: {metrics}");
    assert_eq!(
        participants_active, 2,
        "two guest participants registered: {metrics}"
    );
    assert_eq!(
        sessions_active, 0,
        "guests are not authenticated sessions: {metrics}"
    );
    assert!(messages_in >= 1, "inbound position counted: {metrics}");
    assert!(messages_out >= 1, "relayed to a peer: {metrics}");

    // Close one client and confirm the gauges drop, proving decrement is wired.
    client_a.close(None).await.ok();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let status = get(http_addr, http::STATUS_PATH).await;
    let json = body_json(&status);
    let metrics = &json["metrics"];
    assert_eq!(
        metrics["connections_active"].as_u64().expect("u64"),
        1,
        "one client remains connected: {metrics}"
    );
    assert_eq!(
        metrics["participants_active"].as_u64().expect("u64"),
        1,
        "one guest participant remains: {metrics}"
    );
    assert_eq!(
        metrics["connections_accepted_total"].as_u64().expect("u64"),
        2,
        "accepted total is sticky: {metrics}"
    );

    client_b.close(None).await.ok();
    tx.send(()).expect("send shutdown");
    let result = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server task should stop")
        .expect("server task should not panic");
    assert!(result.is_ok(), "serve returned an error: {result:?}");
    supervisor.shutdown().await.expect("shutdown transports");
}
