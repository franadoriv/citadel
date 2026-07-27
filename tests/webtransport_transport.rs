//! Integration test for the WebTransport transport relay.
//!
//! Binds a real WebTransport server backed by the realtime gateway, connects two
//! in-process WebTransport clients (pinning the server's dev cert hash, exactly
//! as a browser would via `serverCertificateHashes`), has client A send a
//! position datagram, and asserts client B receives it relayed (tagged with A's
//! session id). Proves the server-side WebTransport path and gateway integration
//! without a browser. Then shuts down gracefully.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::transport::codec::{Envelope, decode_datagram};
use citadel::transport::webtransport::{WebTransportDevCert, WebTransportServer};
use citadel_wire::protocol::{KIND_PEER_POSITION, KIND_POSITION, split_sender};
use web_transport_quinn::{ClientBuilder, Session};

mod common;
use common::wt_guest_handshake;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn connect(port: u16, cert: &WebTransportDevCert) -> Session {
    // Pin the dev cert by SHA-256, exactly like a browser serverCertificateHashes.
    let client = ClientBuilder::new()
        .with_server_certificate_hashes(vec![cert.cert_sha256().to_vec()])
        .expect("client builder");
    // Use the IPv4 literal so the client dials 127.0.0.1 (matching the server
    // bind) rather than resolving `localhost`, which can prefer ::1. Cert-hash
    // pinning verifies by fingerprint, not by SNI/host name.
    let url = url::Url::parse(&format!("https://127.0.0.1:{port}/")).expect("url");
    tokio::time::timeout(Duration::from_secs(5), client.connect(url))
        .await
        .expect("connect did not time out")
        .expect("webtransport connected")
}

#[tokio::test]
async fn webtransport_relays_position_datagram_to_other_client() {
    let cert = WebTransportDevCert::generate(&["localhost".to_string()]).expect("cert");
    let server = WebTransportServer::bind(loopback_any(), &cert).expect("bind server");
    let port = server.local_addr().port();
    let metrics = server.metrics();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let session_a = connect(port, &cert).await;
    let session_b = connect(port, &cert).await;
    // Both sessions present the guest handshake (over a reliable stream) so they
    // register; a pre-auth datagram would be dropped.
    wt_guest_handshake(&session_a).await;
    wt_guest_handshake(&session_b).await;

    // A sends a position datagram.
    let payload = vec![7u8, 7, 8, 8];
    let env = Envelope::new(KIND_POSITION, payload.clone());
    session_a
        .send_datagram(env.encode_datagram())
        .expect("A sends datagram");

    // B receives the relayed peer position datagram.
    let bytes = tokio::time::timeout(Duration::from_secs(5), session_b.read_datagram())
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

    session_a.close(0u32, b"done");
    session_b.close(0u32, b"done");
    supervisor.shutdown().await.expect("shutdown");
}
