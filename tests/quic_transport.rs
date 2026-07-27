//! Integration test for the QUIC transport relay.
//!
//! Binds a real QUIC server backed by the realtime gateway, connects two real
//! `quinn` clients, has client A send a position datagram, and asserts client B
//! receives it relayed (tagged with A's session id). Then shuts down gracefully.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::transport::codec::{Envelope, decode_datagram};
use citadel::transport::quic::tls;
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel_wire::protocol::{KIND_PEER_POSITION, KIND_POSITION, split_sender};

mod common;
use common::quic_guest_handshake;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn connect(server_addr: SocketAddr, cert: &SelfSignedCert) -> quinn::Connection {
    let client_config = tls::client_config_trusting(cert).expect("client config");
    let mut endpoint = quinn::Endpoint::client(loopback_any()).expect("client endpoint");
    endpoint.set_default_client_config(client_config);
    tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(server_addr, "localhost")
            .expect("connect call"),
    )
    .await
    .expect("connect did not time out")
    .expect("connection established")
}

#[tokio::test]
async fn quic_relays_position_datagram_to_other_client() {
    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("generate cert");
    let server = QuicServer::bind(loopback_any(), &cert).expect("bind server");
    let server_addr = server.local_addr();
    let metrics = server.metrics();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let conn_a = connect(server_addr, &cert).await;
    let conn_b = connect(server_addr, &cert).await;
    // Both connections present the guest handshake (over a reliable stream) so
    // they register; a pre-auth datagram would be dropped.
    quic_guest_handshake(&conn_a).await;
    quic_guest_handshake(&conn_b).await;

    // A sends a position datagram.
    let payload = vec![9u8, 8, 7, 6];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    conn_a
        .send_datagram(env.encode_datagram())
        .expect("A sends datagram");

    // B receives the relayed peer position datagram.
    let bytes = tokio::time::timeout(Duration::from_secs(5), conn_b.read_datagram())
        .await
        .expect("relay did not time out")
        .expect("datagram received");
    let relayed = decode_datagram(&bytes).expect("decode datagram");
    assert_eq!(relayed.kind, KIND_PEER_POSITION);
    let (_sender_id, rest) = split_sender(&relayed.body).expect("tagged body");
    assert_eq!(rest, &payload[..], "original payload relayed intact");

    let snap = metrics.snapshot();
    assert_eq!(snap.connections_accepted, 2);
    assert!(snap.envelopes_received >= 1);
    assert!(snap.envelopes_sent >= 1);

    conn_a.close(0u32.into(), b"done");
    conn_b.close(0u32.into(), b"done");
    let result = tokio::time::timeout(Duration::from_secs(5), supervisor.shutdown())
        .await
        .expect("server shutdown completes promptly");
    result.expect("clean shutdown");
}
