//! End-to-end tests for the authenticated realtime handshake.
//!
//! Binds a real WebSocket transport whose gateway is wired to a live
//! `SessionService` (as `transport::start_enabled` does in production), mints a
//! real session token through the authentication service, and drives the
//! `KIND_AUTH` handshake over the wire. Verifies:
//!
//! - a valid token binds the connection to its `user_id` and moves the
//!   authenticated-session gauge;
//! - an invalid token is rejected with a coarse reason and no session state;
//! - a guest connect works when allowed;
//! - an auth-required gateway refuses a guest connect.

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
use citadel::transport::websocket::WebSocketServer;
use citadel_wire::protocol::{
    AUTH_REASON_AUTH_FAILED, AUTH_REASON_AUTH_REQUIRED, AUTH_STATUS_AUTHENTICATED,
    AUTH_STATUS_REJECTED,
};

mod common;
use common::{ws_auth_handshake, ws_guest_handshake};

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

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

/// Register a device account through the app's auth service and return
/// `(access_token, user_id)`.
async fn mint_token(app: &App) -> (String, String) {
    let outcome = app
        .authentication_service()
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new("device-handshake").expect("device id"),
            options: AuthenticationOptions {
                create_account: true,
                username: Some(Username::new("handshake-player").expect("username")),
                display_name: None,
                metadata: None,
                now: SystemClock.now(),
                owner_node: NodeId::new(app.node_id()).expect("node id"),
                session_ttl: DurationMillis::from_millis(60 * 60 * 1_000),
                refresh_ttl: Some(DurationMillis::from_millis(24 * 60 * 60 * 1_000)),
            },
        })
        .await
        .expect("device auth succeeds");
    (
        outcome.tokens.access.expose_secret().to_string(),
        outcome.user.id.as_str().to_string(),
    )
}

/// Bind a WebSocket server whose gateway is wired to `app`'s session service and
/// the given auth stance, sharing `app`'s metrics registry.
async fn serve_ws(app: &App, require_auth: bool, allow_guests: bool) -> (SocketAddr, Supervisor) {
    let authenticator = Authenticator::new(
        Some(Arc::clone(app.session_service())),
        require_auth,
        allow_guests,
    );
    let gateway = Arc::new(Gateway::with_metrics_runtime_auth(
        Arc::clone(app.metrics()),
        None,
        authenticator,
    ));
    let ws = WebSocketServer::bind_with_gateway(loopback_any(), gateway)
        .await
        .expect("bind ws");
    let addr = ws.local_addr();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(ws);
    (addr, supervisor)
}

#[tokio::test]
async fn valid_token_binds_user_id_and_moves_session_gauge() {
    let app = App::new(Config::default());
    let (token, user_id) = mint_token(&app).await;
    let (addr, supervisor) = serve_ws(&app, false, true).await;

    let mut client = connect(addr).await;
    let ack = ws_auth_handshake(&mut client, &token).await;
    assert_eq!(
        ack.status, AUTH_STATUS_AUTHENTICATED,
        "valid token authenticates"
    );
    assert_eq!(ack.user_id, user_id, "the bound user_id is returned");

    // The authenticated-session gauge moved (registration precedes the ack).
    let snap = app.metrics().snapshot();
    assert_eq!(snap.sessions_active, 1, "one authenticated session");
    assert_eq!(snap.participants_active, 1, "one participant");

    client.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn invalid_token_is_rejected_without_session_state() {
    let app = App::new(Config::default());
    let (addr, supervisor) = serve_ws(&app, false, true).await;

    let mut client = connect(addr).await;
    let ack = ws_auth_handshake(&mut client, "not-a-real-token").await;
    assert_eq!(
        ack.status, AUTH_STATUS_REJECTED,
        "invalid token is rejected"
    );
    assert_eq!(
        ack.reason_class, AUTH_REASON_AUTH_FAILED,
        "reason collapses to a coarse auth failure"
    );
    assert!(ack.user_id.is_empty(), "a rejection carries no user_id");

    // No participant or session state was created for the rejected connection.
    let snap = app.metrics().snapshot();
    assert_eq!(snap.participants_active, 0, "no participant registered");
    assert_eq!(snap.sessions_active, 0, "no authenticated session");

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn guest_connect_works_when_allowed() {
    let app = App::new(Config::default());
    let (addr, supervisor) = serve_ws(&app, false, true).await;

    let mut client = connect(addr).await;
    // ws_guest_handshake asserts the guest ack internally.
    ws_guest_handshake(&mut client).await;

    let snap = app.metrics().snapshot();
    assert_eq!(snap.participants_active, 1, "guest is a participant");
    assert_eq!(
        snap.sessions_active, 0,
        "a guest is not an authenticated session"
    );

    client.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn auth_required_refuses_guest() {
    let app = App::new(Config::default());
    // require_auth = true: guests are refused.
    let (addr, supervisor) = serve_ws(&app, true, true).await;

    let mut client = connect(addr).await;
    // Send an empty KIND_AUTH (guest request); expect a rejection.
    let ack = ws_auth_handshake(&mut client, "").await;
    assert_eq!(
        ack.status, AUTH_STATUS_REJECTED,
        "guest refused under auth-required"
    );
    assert_eq!(
        ack.reason_class, AUTH_REASON_AUTH_REQUIRED,
        "reason is auth-required"
    );

    let snap = app.metrics().snapshot();
    assert_eq!(snap.participants_active, 0, "no participant registered");

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn auth_required_accepts_a_valid_token() {
    let app = App::new(Config::default());
    let (token, user_id) = mint_token(&app).await;
    let (addr, supervisor) = serve_ws(&app, true, false).await;

    let mut client = connect(addr).await;
    let ack = ws_auth_handshake(&mut client, &token).await;
    assert_eq!(
        ack.status, AUTH_STATUS_AUTHENTICATED,
        "a valid token authenticates even under auth-required"
    );
    assert_eq!(ack.user_id, user_id);

    client.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}

/// A silent client that connects but never sends a handshake frame is closed by
/// the handshake deadline and never registers. Uses a short deadline for speed.
#[tokio::test]
async fn silent_client_times_out_without_registering() {
    let app = App::new(Config::default());
    let authenticator = Authenticator::new(Some(Arc::clone(app.session_service())), false, true);
    let gateway = Arc::new(Gateway::with_metrics_runtime_auth(
        Arc::clone(app.metrics()),
        None,
        authenticator,
    ));
    let ws = WebSocketServer::bind_with_gateway(loopback_any(), gateway)
        .await
        .expect("bind ws")
        .with_handshake_timeout(Duration::from_millis(200));
    let addr = ws.local_addr();
    let mut supervisor = Supervisor::new();
    supervisor.spawn(ws);

    let mut client = connect(addr).await;
    // Never send a handshake; wait past the deadline.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let snap = app.metrics().snapshot();
    assert_eq!(
        snap.participants_active, 0,
        "a silent client never registers"
    );
    // The server closed the connection; a send may now fail (best-effort).
    let _ = client.send(Message::Close(None)).await;
    supervisor.shutdown().await.expect("shutdown");
}
