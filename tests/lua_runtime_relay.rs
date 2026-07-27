//! End-to-end test that a Lua script drives the realtime relay.
//!
//! Loads the repo's shipped `game/main.lua` into a real [`LuaRuntime`], attaches
//! it to a [`Gateway`], and runs two WebSocket clients through the transport
//! stack. A POSITION sent by client A must be handled by the Lua script and
//! broadcast to client B as a sender-tagged PEER_POSITION — proving the whole
//! "write game logic in a script" loop works over the wire, not just in a unit
//! test. This reuses the same transport harness as `realtime_relay.rs`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use citadel::lifecycle::Supervisor;
use citadel::observability::NodeMetrics;
use citadel::realtime::Gateway;
use citadel::runtime::LuaRuntime;
use citadel::transport::codec::{Envelope, decode_framed};
use citadel::transport::websocket::WebSocketServer;
use citadel_wire::protocol::{KIND_PEER_POSITION, KIND_POSITION, split_sender};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::ws_guest_handshake;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

/// Absolute path to the repo's shipped sample game scripts.
fn game_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("game")
}

#[tokio::test]
async fn shipped_lua_script_relays_position_between_websocket_clients() {
    // Load the real sample script and attach it to the gateway.
    let runtime = LuaRuntime::load(&game_dir(), 100)
        .expect("game/main.lua loads")
        .expect("game/main.lua exists in the repo");
    let gateway = Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::new(runtime)),
    ));
    assert!(gateway.has_runtime(), "gateway is script-driven");

    let ws = WebSocketServer::bind_with_gateway(loopback_any(), Arc::clone(&gateway))
        .await
        .expect("bind ws");
    let ws_addr = ws.local_addr();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(ws);

    // Two WebSocket clients: A sends, B receives the relayed peer position.
    let url = format!("ws://{ws_addr}/");
    let (mut client_a, _ra) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("A connect did not time out")
    .expect("A connected");
    let (mut client_b, _rb) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("B connect did not time out")
    .expect("B connected");

    // Both clients present the guest handshake so they register in the gateway.
    ws_guest_handshake(&mut client_a).await;
    ws_guest_handshake(&mut client_b).await;

    // A sends a position; the Lua handler must relay it to B.
    let payload = vec![4u8, 3, 2, 1];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    client_a
        .send(Message::Binary(env.encode_framed().to_vec()))
        .await
        .expect("A sends");

    // B receives the script-built PEER_POSITION, sender-tagged.
    let relayed = read_one_envelope(&mut client_b).await;
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&relayed.body).expect("tagged body");
    assert_eq!(
        rest,
        &payload[..],
        "Lua script relayed A's position to B intact"
    );

    client_a.close(None).await.ok();
    client_b.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}

/// Read one binary WebSocket message and decode a single framed envelope.
async fn read_one_envelope<S>(client: &mut S) -> Envelope
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("relay did not time out")
            .expect("stream open")
            .expect("message ok");
        if let Message::Binary(data) = msg {
            let mut buf = BytesMut::from(&data[..]);
            if let Some(env) = decode_framed(&mut buf).expect("decode framed") {
                return env;
            }
        }
    }
}
