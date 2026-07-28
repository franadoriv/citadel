---
title: Connect a native client
description: Connect a native Rust client to Citadel over QUIC using the citadel-client SDK.
---

Native clients use **QUIC** for low-latency datagrams plus reliable streams. The
simplest path is the [`citadel-client`](/guides/rust-sdk/) Rust SDK; this guide
shows the QUIC client specifically. The bundled `demo-client` crate is a working
example that renders a 2D window on top of the SDK.

## Run the bundled native demo

Start a server with QUIC enabled, then run the demo:

```bash
cargo run -- --config examples/configs/demo.toml serve
cargo run -p demo-client -- 127.0.0.1:7351
```

The address can also come from the `CITADEL_QUIC_ADDR` environment variable
(default `127.0.0.1:7351`). Run two instances (or `make demo-native2`) to see the
relay: move the square in one window and the peer moves in the other.

## Connect with the SDK

```rust
use citadel_client::{QuicClient, ClientTls};
use citadel_wire::{Envelope, protocol::KIND_POSITION};

# async fn run -> anyhow::Result<> {
// Production: verify a public CA certificate and the server hostname.
let tls = ClientTls::webpki_roots();
let mut client = QuicClient::connect("127.0.0.1:7351", "localhost", tls).await?;

// Send our position as an unreliable datagram (hot-path state).
let mut body = Vec::new;
body.extend_from_slice(&1.0f32.to_le_bytes); // x
body.extend_from_slice(&2.0f32.to_le_bytes); // y
client.send_unreliable(&Envelope::new(KIND_POSITION, body)).await?;

// Receive relayed peer datagrams (KIND_PEER_POSITION). The first 8 bytes of the
// body are the big-endian sender session id.
let peer = client.recv_datagram.await?;
// ... decode peer.body ...

client.close.await;
# Ok() }
```

## QUIC delivery model

- `send_unreliable(&Envelope)` sends a datagram — use it for hot-path game state.
- `send_reliable(&Envelope)` sends fire-and-forget over a fresh uni stream — use
  it for control traffic.
- `recv_datagram` yields relayed peer datagrams.
- `recv_uni` yields relayed reliable peer messages.

QUIC negotiates the ALPN `citadel/0`, matching the server. The gateway relays your
message to *other* sessions; it never echoes it back to you.

## TLS

- `ClientTls::webpki_roots()` verifies public CA certificates and the hostname.
- `ClientTls::trusting(cert_chain)` pins a known certificate, including a local
  development certificate.
- `ClientTls::insecure_skip_verification()` disables verification for local
  development against the self-signed dev cert. It is clearly named and never
  validates identity.

:::caution[Not implemented yet]
There is no session validation yet
(internal ). Endpoints and certificates are parameters; no credentials
are embedded. There is no reconnection/backoff in the SDK yet.
:::
