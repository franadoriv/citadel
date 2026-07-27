//! Cross-transport relay test for the realtime gateway.
//!
//! Binds the QUIC and WebSocket transports sharing a single `Gateway`/room,
//! then verifies a position sent by a WebSocket client is relayed to a QUIC
//! client (as a datagram) — proving the room is shared across transports, not
//! per-transport.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::realtime::Gateway;
use citadel::transport::codec::{Envelope, decode_datagram};
use citadel::transport::quic::tls;
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel::transport::websocket::WebSocketServer;
use citadel_wire::protocol::{KIND_PEER_POSITION, KIND_POSITION, split_sender};
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::{quic_guest_handshake, ws_guest_handshake};

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

#[tokio::test]
async fn websocket_message_relays_to_quic_client_via_shared_gateway() {
    // One shared gateway/room for both transports.
    let gateway = Arc::new(Gateway::new());

    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let quic = QuicServer::bind_with_gateway(loopback_any(), &cert, Arc::clone(&gateway))
        .expect("bind quic");
    let quic_addr = quic.local_addr();

    let ws = WebSocketServer::bind_with_gateway(loopback_any(), Arc::clone(&gateway))
        .await
        .expect("bind ws");
    let ws_addr = ws.local_addr();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(quic);
    supervisor.spawn(ws);

    // QUIC receiver client.
    let client_config = tls::client_config_trusting(&cert).expect("client cfg");
    let mut endpoint = quinn::Endpoint::client(loopback_any()).expect("client endpoint");
    endpoint.set_default_client_config(client_config);
    let quic_conn = tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(quic_addr, "localhost")
            .expect("connect call"),
    )
    .await
    .expect("connect did not time out")
    .expect("quic connected");

    // WebSocket sender client.
    let url = format!("ws://{ws_addr}/");
    let (mut ws_client, _resp) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("ws connect did not time out")
    .expect("ws connected");

    // Both clients present the guest handshake so they register in the shared
    // gateway (the QUIC receiver must be registered before the WS relay fires).
    quic_guest_handshake(&quic_conn).await;
    ws_guest_handshake(&mut ws_client).await;

    // WebSocket client sends a position; it must reach the QUIC client.
    let payload = vec![4u8, 3, 2, 1];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    ws_client
        .send(Message::Binary(env.encode_framed().to_vec()))
        .await
        .expect("ws sends");

    let bytes = tokio::time::timeout(Duration::from_secs(5), quic_conn.read_datagram())
        .await
        .expect("relay did not time out")
        .expect("datagram received");
    let relayed = decode_datagram(&bytes).expect("decode datagram");
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&relayed.body).expect("tagged body");
    assert_eq!(
        rest,
        &payload[..],
        "WebSocket position relayed to QUIC client intact"
    );

    ws_client.close(None).await.ok();
    quic_conn.close(0u32.into(), b"done");
    supervisor.shutdown().await.expect("shutdown");
}
