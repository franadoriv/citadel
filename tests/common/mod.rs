//! Shared integration-test helpers for the realtime auth handshake.
//!
//! With the authenticated handshake, a realtime connection is not registered in
//! the gateway until it presents its first frame (a `KIND_AUTH` frame, or — for
//! backwards compatibility in the default guest-allowed stance — any first
//! frame). These helpers let transport tests perform the guest handshake and
//! drain the `KIND_AUTH_RESULT` ack so the rest of the test can assume the
//! participant is registered.
#![allow(dead_code)]

use std::time::Duration;

use bytes::BytesMut;
use citadel::transport::codec::{Envelope, decode_framed};
use citadel_wire::diagnostics::ServerTime;
use citadel_wire::protocol::{
    KIND_AUTH, KIND_AUTH_RESULT, KIND_DIAG_SERVER_TIME, decode_auth_result,
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// The concrete `tokio-tungstenite` client stream type used by the WS tests.
pub type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Read the next binary WebSocket message, ignoring non-binary frames.
pub async fn ws_next_binary(ws: &mut Ws) -> Vec<u8> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("recv did not time out")
            .expect("stream open")
            .expect("message ok");
        if let Message::Binary(data) = msg {
            return data;
        }
    }
}

/// Decode a single framed envelope from a WS binary message.
pub fn decode_one(data: &[u8]) -> Envelope {
    let mut buf = BytesMut::from(data);
    decode_framed(&mut buf)
        .expect("decode framed")
        .expect("one frame")
}

/// Perform the guest handshake over a WS client: send an empty `KIND_AUTH` and
/// consume the `KIND_AUTH_RESULT(guest)` ack. Returns once the server has
/// registered the participant (the ack is sent right after registration).
pub async fn ws_guest_handshake(ws: &mut Ws) {
    let auth = Envelope::new(KIND_AUTH, Vec::new());
    ws.send(Message::Binary(auth.encode_framed().to_vec()))
        .await
        .expect("send guest auth");
    let ack = decode_one(&ws_next_binary(ws).await);
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "guest handshake is acked");
    let result = decode_auth_result(&ack.body).expect("auth result decodes");
    assert!(result.is_guest(), "empty KIND_AUTH is accepted as guest");
    assert_server_time(decode_one(&ws_next_binary(ws).await));
}

/// Perform an authenticated handshake over a WS client: present `token` and
/// consume the ack. Returns the ack's decoded outcome so the caller can assert
/// the bound `user_id` (authenticated) or the coarse rejection reason.
pub async fn ws_auth_handshake(ws: &mut Ws, token: &str) -> AuthAck {
    let auth = Envelope::new(KIND_AUTH, token.as_bytes().to_vec());
    ws.send(Message::Binary(auth.encode_framed().to_vec()))
        .await
        .expect("send auth token");
    let ack = decode_one(&ws_next_binary(ws).await);
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "handshake is acked");
    let result = decode_auth_result(&ack.body).expect("auth result decodes");
    if !result.is_rejected() {
        assert_server_time(decode_one(&ws_next_binary(ws).await));
    }
    AuthAck {
        status: result.status,
        user_id: result.user_id.to_string(),
        reason_class: result.reason_class,
    }
}

fn assert_server_time(envelope: Envelope) {
    assert_eq!(
        envelope.kind, KIND_DIAG_SERVER_TIME,
        "accepted handshake is followed by a server-time offer"
    );
    assert!(
        ServerTime::decode(&envelope.body).is_ok(),
        "server-time offer has a strict v1 body"
    );
}

/// An owned copy of a decoded `KIND_AUTH_RESULT`, so it can outlive the frame.
#[derive(Debug, Clone)]
pub struct AuthAck {
    pub status: u8,
    pub user_id: String,
    pub reason_class: u8,
}

/// Bound on reading a single ack/handshake stream in the test clients.
const ACK_STREAM_BYTES: usize = 64 * 1024;

/// Perform the guest handshake over a QUIC client: open a uni stream, send an
/// empty `KIND_AUTH`, finish it, then read the server's ack on a fresh uni
/// stream. Returns once registration is complete (the ack arrives after it).
pub async fn quic_guest_handshake(conn: &quinn::Connection) {
    let mut send = conn.open_uni().await.expect("open uni for auth");
    let auth = Envelope::new(KIND_AUTH, Vec::new());
    send.write_all(&auth.encode_framed())
        .await
        .expect("write auth");
    send.finish().expect("finish auth stream");

    let mut recv = tokio::time::timeout(Duration::from_secs(5), conn.accept_uni())
        .await
        .expect("ack did not time out")
        .expect("ack stream accepted");
    let data = recv
        .read_to_end(ACK_STREAM_BYTES)
        .await
        .expect("read ack stream");
    let ack = decode_one(&data);
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "guest handshake is acked");
    assert!(
        decode_auth_result(&ack.body)
            .expect("auth result")
            .is_guest(),
        "empty KIND_AUTH is accepted as guest"
    );
    let mut recv = tokio::time::timeout(Duration::from_secs(5), conn.accept_uni())
        .await
        .expect("server time did not arrive")
        .expect("server-time stream accepted");
    let data = recv
        .read_to_end(ACK_STREAM_BYTES)
        .await
        .expect("read server-time stream");
    assert_server_time(decode_one(&data));
}

/// Perform the guest handshake over a WebTransport session (same shape as QUIC).
pub async fn wt_guest_handshake(session: &web_transport_quinn::Session) {
    let mut send = session.open_uni().await.expect("open uni for auth");
    let auth = Envelope::new(KIND_AUTH, Vec::new());
    send.write_all(&auth.encode_framed())
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
    let ack = decode_one(&data);
    assert_eq!(ack.kind, KIND_AUTH_RESULT, "guest handshake is acked");
    assert!(
        decode_auth_result(&ack.body)
            .expect("auth result")
            .is_guest(),
        "empty KIND_AUTH is accepted as guest"
    );
    let mut recv = tokio::time::timeout(Duration::from_secs(5), session.accept_uni())
        .await
        .expect("server time did not arrive")
        .expect("server-time stream accepted");
    let data = recv
        .read_to_end(ACK_STREAM_BYTES)
        .await
        .expect("read server-time stream");
    assert_server_time(decode_one(&data));
}
