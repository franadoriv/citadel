//! RPC helper tests for the client SDK against an in-process Citadel server.
//!
//! These stand up a real Citadel transport (WebSocket, QUIC) backed by a gateway
//! with an embedded Lua runtime that registers `on_rpc` handlers, then exercise
//! the SDK's `call_rpc` convenience: it must send a `KIND_RPC_REQUEST` and
//! resolve the correlated `KIND_RPC_RESPONSE` reply (matched by `request_id`),
//! including the error-status path. Mirrors the transport harness in
//! `round_trip.rs` and the server-side coverage in the repo's `tests/lua_rpc.rs`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use citadel::lifecycle::Supervisor;
use citadel::observability::NodeMetrics;
use citadel::realtime::Gateway;
use citadel::runtime::LuaRuntime;
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel::transport::websocket::WebSocketServer;
use citadel_client::quic::ClientTls;
use citadel_client::{ClientError, QuicClient, WsClient};

/// A minimal game script exposing the RPC methods the tests call. Kept inline so
/// the test does not depend on the repo's `game/main.lua` layout: `add` sums two
/// big-endian u32s (a typed request/response) and `boom` always errors.
const RPC_SCRIPT: &str = r#"
    citadel.on_rpc("add", function(ctx, body)
      local a, b = string.unpack(">I4I4", body)
      return string.pack(">I4", (a + b) & 0xFFFFFFFF)
    end)
    citadel.on_rpc("boom", function(ctx, body)
      error("handler blew up")
    end)
"#;

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

fn rpc_gateway() -> Arc<Gateway> {
    let runtime =
        LuaRuntime::from_source(RPC_SCRIPT, "rpc-test", 100).expect("inline RPC script loads");
    Arc::new(Gateway::with_metrics_and_runtime(
        Arc::new(NodeMetrics::new()),
        Some(Arc::new(runtime)),
    ))
}

fn encode_add(a: u32, b: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&a.to_be_bytes());
    payload.extend_from_slice(&b.to_be_bytes());
    payload
}

#[tokio::test]
async fn websocket_call_rpc_resolves_correlated_reply() {
    let server = WebSocketServer::bind_with_gateway(loopback_any(), rpc_gateway())
        .await
        .expect("bind ws server");
    let addr = server.local_addr();
    let mut sup = Supervisor::new();
    sup.spawn(server);

    let url = format!("ws://{addr}/");
    let mut client = WsClient::connect(&url).await.expect("client connects");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Success path: `add(2, 40)` resolves to the correlated `42` reply.
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("add", &encode_add(2, 40), |_| Ok(())),
    )
    .await
    .expect("add rpc did not time out")
    .expect("add rpc succeeds");
    let sum = u32::from_be_bytes(reply.as_slice().try_into().expect("u32 reply"));
    assert_eq!(sum, 42);

    // A second call on the same connection gets its own correlated reply (the
    // helper's request-id counter advances and still matches correctly).
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("add", &encode_add(100, 1), |_| Ok(())),
    )
    .await
    .expect("second add did not time out")
    .expect("second add succeeds");
    let sum = u32::from_be_bytes(reply.as_slice().try_into().expect("u32 reply"));
    assert_eq!(sum, 101);

    // Error path: a handler that raises maps to `ClientError::Rpc` (never a
    // panic), carrying the correlation id and a generic message.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("boom", b"", |_| Ok(())),
    )
    .await
    .expect("boom rpc did not time out")
    .expect_err("boom rpc reports an error");
    // The error is a correlated `Rpc` variant carrying this (third) call's id.
    assert!(
        matches!(err, ClientError::Rpc { request_id: 3, .. }),
        "expected a correlated ClientError::Rpc for the 3rd call, got {err:?}"
    );

    // Unknown method also errors rather than hanging or panicking.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("does-not-exist", b"", |_| Ok(())),
    )
    .await
    .expect("unknown rpc did not time out")
    .expect_err("unknown method reports an error");
    assert!(matches!(err, ClientError::Rpc { .. }));

    client.close().await.ok();
    sup.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn quic_call_rpc_resolves_correlated_reply() {
    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let server = QuicServer::bind_with_gateway(loopback_any(), &cert, rpc_gateway())
        .expect("bind quic server");
    let addr = server.local_addr();
    let mut sup = Supervisor::new();
    sup.spawn(server);

    let mut client =
        QuicClient::connect(addr, "localhost", ClientTls::insecure_skip_verification())
            .await
            .expect("client connects");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("add", &encode_add(20, 22), |_| Ok(())),
    )
    .await
    .expect("add rpc did not time out")
    .expect("add rpc succeeds");
    let sum = u32::from_be_bytes(reply.as_slice().try_into().expect("u32 reply"));
    assert_eq!(sum, 42);

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.call_rpc("boom", b"", |_| Ok(())),
    )
    .await
    .expect("boom rpc did not time out")
    .expect_err("boom rpc reports an error");
    assert!(matches!(err, ClientError::Rpc { .. }));

    client.close();
    sup.shutdown().await.expect("shutdown");
}
