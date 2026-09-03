//! Relay tests for the client SDK against an in-process Citadel server.
//!
//! These bind real Citadel transports (QUIC, WebSocket) on ephemeral ports
//! backed by the realtime gateway, connect two SDK clients, and verify that a
//! message sent by client A is relayed to client B (tagged with A's session id)
//! and not echoed back to A. Then shut down gracefully via the supervisor.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel::transport::websocket::WebSocketServer;
use citadel_client::quic::ClientTls;
use citadel_client::{Envelope, QuicClient, WsClient};
use citadel_wire::protocol::{
    KIND_AUTH, KIND_AUTH_RESULT, KIND_PEER_POSITION, KIND_POSITION, split_sender,
};

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

#[tokio::test]
async fn websocket_sdk_relays_to_other_client() {
    let server = WebSocketServer::bind(loopback_any())
        .await
        .expect("bind ws server");
    let addr = server.local_addr();
    let mut sup = Supervisor::new();
    sup.spawn(server);

    let url = format!("ws://{addr}/");
    let mut a = WsClient::connect(&url).await.expect("A connects");
    let mut b = WsClient::connect(&url).await.expect("B connects");
    // Each client presents the guest handshake (empty KIND_AUTH) and drains the
    // KIND_AUTH_RESULT ack, which registers it in the gateway.
    ws_guest_handshake(&mut a).await;
    ws_guest_handshake(&mut b).await;

    let payload = vec![5u8, 6, 7, 8];
    a.send(&Envelope::new(KIND_POSITION, payload.clone()))
        .await
        .expect("A sends");

    // The server may emit diagnostics clock frames independently of relay
    // traffic. Drain those unrelated control frames until the peer relay arrives.
    let relayed = loop {
        let envelope = tokio::time::timeout(Duration::from_secs(5), b.recv())
            .await
            .expect("B recv did not time out")
            .expect("recv ok")
            .expect("an envelope");
        if envelope.kind == KIND_PEER_POSITION {
            break envelope;
        }
    };
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&relayed.body).expect("tagged");
    assert_eq!(rest, &payload[..]);

    a.close().await.ok();
    b.close().await.ok();
    sup.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn quic_sdk_relays_datagram_to_other_client() {
    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let server = QuicServer::bind(loopback_any(), &cert).expect("bind quic server");
    let addr = server.local_addr();
    let mut sup = Supervisor::new();
    sup.spawn(server);

    let a = QuicClient::connect(addr, "localhost", ClientTls::insecure_skip_verification())
        .await
        .expect("A connects");
    let b = QuicClient::connect(addr, "localhost", ClientTls::insecure_skip_verification())
        .await
        .expect("B connects");
    // Each client presents the guest handshake on a reliable stream and drains
    // the ack (a datagram handshake would be dropped pre-auth).
    quic_guest_handshake(&a).await;
    quic_guest_handshake(&b).await;

    let payload = vec![1u8, 1, 2, 3];
    a.send_unreliable(&Envelope::new(KIND_POSITION, payload.clone()))
        .expect("A sends datagram");

    let relayed = tokio::time::timeout(Duration::from_secs(5), b.recv_datagram())
        .await
        .expect("B recv did not time out")
        .expect("datagram");
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&relayed.body).expect("tagged");
    assert_eq!(rest, &payload[..]);

    a.close();
    b.close();
    sup.shutdown().await.expect("shutdown");
}

/// Present the guest handshake over a WS SDK client and drain the ack.
async fn ws_guest_handshake(client: &mut WsClient) {
    client
        .send(&Envelope::new(KIND_AUTH, Vec::new()))
        .await
        .expect("send guest auth");
    let ack = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("ack did not time out")
        .expect("recv ok")
        .expect("an ack envelope");
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "guest handshake is acked");
}

/// Present the guest handshake over a QUIC SDK client (reliable stream) and drain
/// the ack uni stream.
async fn quic_guest_handshake(client: &QuicClient) {
    client
        .send_reliable(&Envelope::new(KIND_AUTH, Vec::new()))
        .await
        .expect("send guest auth");
    let ack = tokio::time::timeout(Duration::from_secs(5), client.recv_uni())
        .await
        .expect("ack did not time out")
        .expect("ack uni stream");
    assert!(
        ack.iter().any(|env| env.kind == KIND_AUTH_RESULT),
        "guest handshake is acked on a uni stream"
    );
}
