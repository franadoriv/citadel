//! Integration test for the WebSocket transport relay.
//!
//! Binds a real WebSocket server backed by the realtime gateway, connects two
//! real `tokio-tungstenite` clients, has client A send a position, and asserts
//! that client B receives it relayed (tagged with A's session id) while A does
//! NOT receive its own message. Then shuts down gracefully via the supervisor.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::BytesMut;
use citadel::lifecycle::Supervisor;
use citadel::transport::codec::{Envelope, decode_framed};
use citadel::transport::websocket::WebSocketServer;
use citadel_wire::protocol::{KIND_PEER_POSITION, KIND_POSITION, split_sender};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

mod common;
use common::ws_guest_handshake;

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(addr: SocketAddr) -> Ws {
    let url = format!("ws://{addr}/");
    let (ws, _resp) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect did not time out")
    .expect("websocket connected");
    ws
}

async fn next_binary(ws: &mut Ws) -> Vec<u8> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("recv did not time out")
            .expect("stream not closed")
            .expect("message ok");
        if let Message::Binary(data) = msg {
            return data;
        }
    }
}

#[tokio::test]
async fn websocket_relays_position_to_other_client() {
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = WebSocketServer::bind(bind).await.expect("bind server");
    let addr = server.local_addr();
    let metrics = server.metrics();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let mut client_a = connect(addr).await;
    let mut client_b = connect(addr).await;
    // Each client presents its handshake; registration completes when the
    // guest ack is received (no arbitrary sleep needed).
    ws_guest_handshake(&mut client_a).await;
    ws_guest_handshake(&mut client_b).await;

    // A sends a position; the body is opaque to the server.
    let payload = vec![1u8, 2, 3, 4];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    client_a
        .send(Message::Binary(env.encode_framed().to_vec()))
        .await
        .expect("A sends");

    // B receives a relayed KIND_PEER_POSITION tagged with A's session id.
    let data = next_binary(&mut client_b).await;
    let mut buf = BytesMut::from(&data[..]);
    let relayed = decode_framed(&mut buf)
        .expect("decode ok")
        .expect("one envelope");
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender_id, rest) = split_sender(&relayed.body).expect("tagged body");
    assert_eq!(rest, &payload[..], "original payload relayed intact");

    // A must NOT receive its own message (no echo).
    let self_echo = tokio::time::timeout(Duration::from_millis(300), client_a.next()).await;
    assert!(
        self_echo.is_err(),
        "sender must not receive its own message"
    );

    // Metrics observed two connections and at least one relayed send.
    let snap = metrics.snapshot();
    assert_eq!(snap.connections_accepted, 2);
    assert!(snap.envelopes_received >= 1);
    assert!(snap.envelopes_sent >= 1);

    client_a.close(None).await.ok();
    client_b.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}
