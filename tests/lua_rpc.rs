//! End-to-end test that a client's RPC request is answered by a Lua handler
//! and correlated back to that one caller over a real transport.
//!
//! Loads the repo's shipped `game/main.lua` (which registers `on_rpc` handlers)
//! into a real [`LuaRuntime`], attaches it to a [`Gateway`], and runs WebSocket
//! clients through the transport stack. A client sends a `KIND_RPC_REQUEST` and
//! must receive exactly one `KIND_RPC_RESPONSE` carrying the same `request_id`
//! and the handler's reply — and a second connected client must receive nothing
//! (an RPC reply is unicast to the caller, never broadcast). Reuses the same
//! transport harness as `lua_runtime_relay.rs`.

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
use citadel_wire::protocol::{
    KIND_RPC_REQUEST, KIND_RPC_RESPONSE, decode_rpc_response, encode_rpc_request,
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

mod common;
use common::ws_guest_handshake;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn game_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("game")
}

#[tokio::test]
async fn shipped_lua_script_answers_rpc_over_the_wire() {
    let runtime = LuaRuntime::load(&game_dir(), 100)
        .expect("game/main.lua loads")
        .expect("game/main.lua exists in the repo");
    let gateway = Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::new(runtime)),
    ));

    let ws = WebSocketServer::bind_with_gateway(loopback_any(), Arc::clone(&gateway))
        .await
        .expect("bind ws");
    let ws_addr = ws.local_addr();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(ws);

    let url = format!("ws://{ws_addr}/");
    let (mut caller, _rc) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("caller connect did not time out")
    .expect("caller connected");
    let (mut bystander, _rb) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .expect("bystander connect did not time out")
    .expect("bystander connected");

    // Both clients present the guest handshake so they register (caller first,
    // so the bystander joins last and receives no join broadcast).
    ws_guest_handshake(&mut caller).await;
    ws_guest_handshake(&mut bystander).await;

    // 1) A `ping` RPC must come back as a correlated `pong`.
    let request = encode_rpc_request(0xCAFE, "ping", b"");
    caller
        .send(Message::Binary(
            Envelope::new(KIND_RPC_REQUEST, request)
                .encode_framed()
                .to_vec(),
        ))
        .await
        .expect("caller sends ping");

    let response = read_rpc_response(&mut caller).await;
    let decoded = decode_rpc_response(&response.body).expect("valid rpc response");
    assert_eq!(decoded.request_id, 0xCAFE, "response is correlated");
    assert!(decoded.is_ok(), "ping succeeds");
    assert_eq!(decoded.payload, b"pong");

    // 2) A typed `add` RPC (two big-endian u32s) returns their sum.
    let mut add_payload = Vec::new();
    add_payload.extend_from_slice(&2u32.to_be_bytes());
    add_payload.extend_from_slice(&40u32.to_be_bytes());
    let request = encode_rpc_request(0xBEEF, "add", &add_payload);
    caller
        .send(Message::Binary(
            Envelope::new(KIND_RPC_REQUEST, request)
                .encode_framed()
                .to_vec(),
        ))
        .await
        .expect("caller sends add");
    let response = read_rpc_response(&mut caller).await;
    let decoded = decode_rpc_response(&response.body).expect("valid rpc response");
    assert_eq!(decoded.request_id, 0xBEEF);
    assert!(decoded.is_ok());
    let sum = u32::from_be_bytes(decoded.payload.try_into().expect("u32 reply"));
    assert_eq!(sum, 42);

    // 3) The bystander must have received nothing: RPC replies are unicast.
    let bystander_got = tokio::time::timeout(Duration::from_millis(300), bystander.next()).await;
    assert!(
        bystander_got.is_err(),
        "an RPC reply must reach only its caller, never a peer"
    );

    caller.close(None).await.ok();
    bystander.close(None).await.ok();
    supervisor.shutdown().await.expect("shutdown");
}

/// Read framed envelopes until the next `KIND_RPC_RESPONSE`, skipping any
/// lifecycle broadcasts (e.g. a peer's `on_join` PLAYER_JOINED) the caller may
/// receive interleaved with its RPC reply.
async fn read_rpc_response<S>(client: &mut S) -> Envelope
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("rpc response did not time out")
            .expect("stream open")
            .expect("message ok");
        if let Message::Binary(data) = msg {
            let mut buf = BytesMut::from(&data[..]);
            if let Some(env) = decode_framed(&mut buf).expect("decode framed")
                && env.kind == KIND_RPC_RESPONSE
            {
                return env;
            }
        }
    }
}
