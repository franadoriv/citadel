//! Regression coverage for exact-session replacement on QUIC-family transports.
//!
//! A newer authenticated connection for the same account session must actively
//! stop the superseded QUIC/WebTransport receive loop. Fencing gateway work is
//! not enough: an old loop must not continue decoding client-controlled frames.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::identity::{DeviceId, Username};
use citadel::lifecycle::Supervisor;
use citadel::realtime::{Authenticator, Gateway};
use citadel::services::{AuthenticationOptions, DeviceAuthenticationRequest};
use citadel::session::NodeId;
use citadel::time::{Clock, DurationMillis, SystemClock};
use citadel::transport::codec::{Envelope, decode_framed};
use citadel::transport::quic::tls;
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel::transport::websocket::WebSocketServer;
use citadel::transport::webtransport::{WebTransportDevCert, WebTransportServer};
use citadel_wire::protocol::{
    AUTH_STATUS_AUTHENTICATED, KIND_AUTH, KIND_AUTH_RESULT, KIND_POSITION,
};
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;
use web_transport_quinn::{ClientBuilder, Session};

mod common;
use common::{Ws, quic_auth_handshake, ws_auth_handshake};

const ACK_STREAM_BYTES: usize = 64 * 1024;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn mint_token(app: &App, device: &str, username: &str) -> String {
    app.authentication_service()
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("device id"),
            options: AuthenticationOptions {
                create_account: true,
                username: Some(Username::new(username).expect("username")),
                display_name: None,
                metadata: None,
                now: SystemClock.now(),
                owner_node: NodeId::new(app.node_id()).expect("node id"),
                session_ttl: DurationMillis::from_millis(60 * 60 * 1_000),
                refresh_ttl: Some(DurationMillis::from_millis(24 * 60 * 60 * 1_000)),
            },
        })
        .await
        .expect("device authentication")
        .tokens
        .access
        .expose_secret()
        .to_owned()
}

fn authenticated_gateway(app: &App) -> Arc<Gateway> {
    Arc::new(Gateway::with_metrics_runtime_auth(
        Arc::clone(app.metrics()),
        None,
        Authenticator::new(Some(Arc::clone(app.session_service())), true, false),
    ))
}

async fn connect_quic(addr: SocketAddr, cert: &SelfSignedCert) -> quinn::Connection {
    let mut endpoint = quinn::Endpoint::client(loopback_any()).expect("client endpoint");
    endpoint.set_default_client_config(tls::client_config_trusting(cert).expect("client config"));
    tokio::time::timeout(
        Duration::from_secs(5),
        endpoint.connect(addr, "localhost").expect("connect call"),
    )
    .await
    .expect("connect did not time out")
    .expect("connection established")
}

async fn connect_webtransport(port: u16, cert: &WebTransportDevCert) -> Session {
    let client = ClientBuilder::new()
        .with_server_certificate_hashes(vec![cert.cert_sha256().to_vec()])
        .expect("client builder");
    tokio::time::timeout(
        Duration::from_secs(5),
        client.connect(url::Url::parse(&format!("https://127.0.0.1:{port}/")).expect("server URL")),
    )
    .await
    .expect("connect did not time out")
    .expect("webtransport connected")
}

async fn connect_websocket(addr: SocketAddr) -> Ws {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(format!("ws://{addr}/")),
    )
    .await
    .expect("connect did not time out")
    .expect("websocket connected")
    .0
}

async fn wt_auth_handshake(session: &Session, token: &str) -> u8 {
    let mut send = session.open_uni().await.expect("open uni for auth");
    send.write_all(&Envelope::new(KIND_AUTH, token.as_bytes().to_vec()).encode_framed())
        .await
        .expect("write auth");
    send.finish().expect("finish auth stream");

    let mut recv = tokio::time::timeout(Duration::from_secs(5), session.accept_uni())
        .await
        .expect("ack did not time out")
        .expect("ack stream accepted");
    let data = recv
        .read_to_end(ACK_STREAM_BYTES)
        .await
        .expect("read ack stream");
    let mut buf = bytes::BytesMut::from(&data[..]);
    let ack = decode_framed(&mut buf)
        .expect("decode ack")
        .expect("ack frame");
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "handshake is acked");
    let result = citadel_wire::protocol::decode_auth_result(&ack.body).expect("auth result");
    assert!(!result.is_rejected(), "authenticated handshake is accepted");

    let mut recv = tokio::time::timeout(Duration::from_secs(5), session.accept_uni())
        .await
        .expect("server time did not arrive")
        .expect("server-time stream accepted");
    let _ = recv
        .read_to_end(ACK_STREAM_BYTES)
        .await
        .expect("read server-time stream");
    result.status
}

async fn wait_for_active_connections(
    metrics: &citadel::transport::metrics::TransportMetrics,
    expected: u64,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if metrics.snapshot().connections_active == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("superseded transport task exits");
}

#[tokio::test]
async fn quic_same_session_replacement_closes_old_loop_before_later_frames_decode() {
    let app = App::new(Config::default());
    let token = mint_token(&app, "quic-replacement", "quic-replacement-player").await;
    let observer_token = mint_token(&app, "quic-observer", "quic-observer-player").await;
    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let gateway = authenticated_gateway(&app);
    assert!(
        gateway
            .resolve_handshake(&Envelope::new(KIND_AUTH, token.as_bytes().to_vec()))
            .await
            .outcome
            .is_accepted(),
        "the freshly issued token is valid before it reaches QUIC"
    );
    let server =
        QuicServer::bind_with_gateway(loopback_any(), &cert, gateway).expect("bind server");
    let addr = server.local_addr();
    let metrics = server.metrics();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let old = connect_quic(addr, &cert).await;
    assert_eq!(
        quic_auth_handshake(&old, &token).await.status,
        AUTH_STATUS_AUTHENTICATED
    );
    let observer = connect_quic(addr, &cert).await;
    assert_eq!(
        quic_auth_handshake(&observer, &observer_token).await.status,
        AUTH_STATUS_AUTHENTICATED
    );
    let replacement = connect_quic(addr, &cert).await;
    assert_eq!(
        quic_auth_handshake(&replacement, &token).await.status,
        AUTH_STATUS_AUTHENTICATED
    );

    wait_for_active_connections(&metrics, 2).await;
    let received_before = metrics.snapshot().envelopes_received;
    let late = Envelope::new(KIND_POSITION, b"late-old-quic".to_vec()).encode_datagram();
    let _ = old.send_datagram(late.clone());
    let _ = old.send_datagram(late);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        metrics.snapshot().envelopes_received,
        received_before,
        "the closed old QUIC loop must not decode later frames"
    );

    observer.close(0u32.into(), b"done");
    replacement.close(0u32.into(), b"done");
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn webtransport_same_session_replacement_closes_old_loop_before_later_frames_decode() {
    let app = App::new(Config::default());
    let token = mint_token(
        &app,
        "webtransport-replacement",
        "webtransport-replacement-player",
    )
    .await;
    let observer_token = mint_token(
        &app,
        "webtransport-observer",
        "webtransport-observer-player",
    )
    .await;
    let cert = WebTransportDevCert::generate(&["localhost".to_string()]).expect("cert");
    let server =
        WebTransportServer::bind_with_gateway(loopback_any(), &cert, authenticated_gateway(&app))
            .expect("bind server");
    let port = server.local_addr().port();
    let metrics = server.metrics();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let old = connect_webtransport(port, &cert).await;
    assert_eq!(
        wt_auth_handshake(&old, &token).await,
        AUTH_STATUS_AUTHENTICATED
    );
    let observer = connect_webtransport(port, &cert).await;
    assert_eq!(
        wt_auth_handshake(&observer, &observer_token).await,
        AUTH_STATUS_AUTHENTICATED
    );
    let replacement = connect_webtransport(port, &cert).await;
    assert_eq!(
        wt_auth_handshake(&replacement, &token).await,
        AUTH_STATUS_AUTHENTICATED
    );

    wait_for_active_connections(&metrics, 2).await;
    let received_before = metrics.snapshot().envelopes_received;
    let late = Envelope::new(KIND_POSITION, b"late-old-webtransport".to_vec()).encode_datagram();
    let _ = old.send_datagram(late.clone());
    let _ = old.send_datagram(late);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        metrics.snapshot().envelopes_received,
        received_before,
        "the closed old WebTransport loop must not decode later frames"
    );

    observer.close(0, b"done");
    replacement.close(0, b"done");
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn websocket_same_session_replacement_stops_late_frame_decode() {
    let app = App::new(Config::default());
    let token = mint_token(
        &app,
        "websocket-replacement",
        "websocket-replacement-player",
    )
    .await;
    let observer_token = mint_token(&app, "websocket-observer", "websocket-observer-player").await;
    let gateway = authenticated_gateway(&app);
    let server = WebSocketServer::bind_with_gateway(loopback_any(), gateway)
        .await
        .expect("bind server");
    let addr = server.local_addr();
    let metrics = server.metrics();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let mut old = connect_websocket(addr).await;
    assert_eq!(
        ws_auth_handshake(&mut old, &token).await.status,
        AUTH_STATUS_AUTHENTICATED
    );
    let mut observer = connect_websocket(addr).await;
    assert_eq!(
        ws_auth_handshake(&mut observer, &observer_token)
            .await
            .status,
        AUTH_STATUS_AUTHENTICATED
    );
    let mut replacement = connect_websocket(addr).await;
    assert_eq!(
        ws_auth_handshake(&mut replacement, &token).await.status,
        AUTH_STATUS_AUTHENTICATED
    );

    wait_for_active_connections(&metrics, 2).await;
    let received_before = metrics.snapshot().envelopes_received;
    let late = Envelope::new(KIND_POSITION, b"late-old-websocket".to_vec()).encode_framed();
    for _ in 0..8 {
        let _ = old.send(Message::Binary(late.to_vec())).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        metrics.snapshot().envelopes_received,
        received_before,
        "the superseded WebSocket loop must consume cancellation before decoding late frames"
    );

    observer.close(None).await.ok();
    replacement.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}
