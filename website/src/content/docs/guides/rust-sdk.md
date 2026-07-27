---
title: Use the Rust SDK
description: Build a Citadel client in Rust with the citadel-client crate over WebSocket and QUIC.
---

`citadel-client` (`crates/citadel-client`) is a minimal Rust client SDK for
Citadel's realtime transports. It offers a small `connect / send / recv` surface
over WebSocket and QUIC, reusing the shared [`citadel-wire`](/concepts/envelopes/)
envelope so clients and the server cannot drift apart. It is pure network/state
logic with no rendering, so it is fully testable.

## Add the dependency

The crate is part of the Citadel workspace. From within the workspace, depend on
it by path:

```toml
[dependencies]
citadel-client = { path = "crates/citadel-client" }
citadel-wire = { path = "crates/citadel-wire" }
```

## WebSocket client

Reliable, ordered delivery. Best for browsers' fallback, lobby/control, and
networks that block UDP.

```rust
use citadel_client::WsClient;
use citadel_wire::{Envelope, protocol::KIND_POSITION};

# async fn run -> anyhow::Result<> {
let mut client = WsClient::connect("ws://127.0.0.1:7352/").await?;
client.send(&Envelope::new(KIND_POSITION, vec![0u8; 8])).await?;

// recv yields relayed peer envelopes, or None when the connection closes.
while let Some(env) = client.recv.await {
    // handle env (e.g. KIND_PEER_POSITION) ...
    let _ = env;
}
client.close.await;
# Ok() }
```

## QUIC client

Datagrams (unreliable, hot-path) plus reliable streams.

```rust
use citadel_client::{QuicClient, ClientTls};
use citadel_wire::{Envelope, protocol::KIND_POSITION};

# async fn run -> anyhow::Result<> {
let tls = ClientTls::insecure_skip_verification; // dev only
let mut client = QuicClient::connect("127.0.0.1:7351", "localhost", tls).await?;

client.send_unreliable(&Envelope::new(KIND_POSITION, vec![0u8; 8])).await?; // datagram
client.send_reliable(&Envelope::new(KIND_POSITION, vec![0u8; 8])).await?;   // uni stream

let _peer_datagram = client.recv_datagram.await?;     // relayed peer datagrams
let _peer_reliable = client.recv_uni.await?;          // relayed reliable peer messages
client.close.await;
# Ok() }
```

## Calling an RPC

Both clients expose `call_rpc(method, payload)` — the client half of the RPC
[request/response wire format](/reference/protocol/envelope/#rpc-requestresponse). It
generates a correlation id, sends a `KIND_RPC_REQUEST`, and awaits the matching
`KIND_RPC_RESPONSE`, returning the handler's reply bytes (or
`ClientError::Rpc { request_id, message }` on an error status).

`call_rpc` imposes **no timeout**, so wrap it in `tokio::time::timeout` to bound
the wait:

```rust
use std::time::Duration;
use citadel_client::{QuicClient, ClientTls};

# async fn run -> anyhow::Result<> {
let tls = ClientTls::insecure_skip_verification; // dev only
let client = QuicClient::connect("127.0.0.1:7351", "localhost", tls).await?;

// `add`: two big-endian i32 operands; the handler replies with their i32 sum.
let mut payload = Vec::new;
payload.extend_from_slice(&7i32.to_be_bytes);
payload.extend_from_slice(&35i32.to_be_bytes);

let reply = tokio::time::timeout(
    Duration::from_secs(5),
    client.call_rpc("add", &payload),
).await??; // outer ? = timeout elapsed; inner ? = ClientError

let sum = i32::from_be_bytes(reply[..4].try_into?);
assert_eq!(sum, 42);
client.close;
# Ok() }
```

`WsClient::call_rpc` takes `&mut self`; `QuicClient::call_rpc` takes `&self`. Both
**discard non-RPC envelopes** (e.g. relayed peer positions) while awaiting the
reply, so they are meant for a connection not concurrently consuming the relay
stream. See the [reference notes](/reference/client-sdk/rust-sdk/#rpc-call-helpers) for the
full correlation contract.

## API surface

| Type | Methods |
| --- | --- |
| `WsClient` | `connect(url)`, `send(&Envelope)`, `recv -> Option<Envelope>`, `call_rpc(method, payload) -> Vec<u8>`, `close` |
| `QuicClient` | `connect(addr, server_name, ClientTls)`, `send_reliable(&Envelope)`, `send_unreliable(&Envelope)`, `recv_datagram -> Envelope`, `recv_uni -> Vec<Envelope>`, `call_rpc(method, payload) -> Vec<u8>`, `close` |
| `ClientTls` | `trusting(cert_chain)`, `insecure_skip_verification` |

`Envelope` is re-exported from `citadel-wire`. QUIC requires the ALPN `citadel/0`,
matching the server.

For the full generated API, see the [Rust SDK reference](/reference/client-sdk/rust-sdk/)
and the [generated rustdoc](/reference/operations/generated/).

:::caution[Not implemented yet]
The insecure TLS option exists only for local development against the self-signed
dev cert; there is no production verification path yet. No reconnection/backoff,
auth/session binding (internal ), or message-kind taxonomy yet. Single
global relay room. `call_rpc` has no built-in timeout or retry (wrap it yourself);
streaming RPC is not implemented.
:::
