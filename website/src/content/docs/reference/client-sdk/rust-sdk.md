---
title: Rust SDK reference
description: API surface of the citadel-client crate — WsClient, QuicClient, ClientTls.
---

`citadel-client` (`crates/citadel-client`) is the Rust client SDK. This page
summarizes the public surface; for full signatures and docs, generate the
[rustdoc](/reference/operations/generated/) (`cargo doc --no-deps --workspace`). See the
[Rust SDK guide](/guides/rust-sdk/) for usage.

`Envelope` is re-exported from [`citadel-wire`](/reference/protocol/envelope/).

## `WsClient`

Reliable, ordered WebSocket client.

| Method | Signature (async) | Description |
| --- | --- | --- |
| `connect` | `connect(url) -> WsClient` | Open a WebSocket connection. |
| `send` | `send(&Envelope)` | Send a framed envelope as a binary message. |
| `recv` | `recv -> Option<Envelope>` | Next relayed peer envelope, or `None` when closed. |
| `call_rpc` | `call_rpc(method, payload) -> Vec<u8>` | Invoke a server RPC and await its correlated reply. See [RPC helper notes](#rpc-call-helpers). |
| `close` | `close` | Close the connection. |

## `QuicClient`

QUIC client with datagrams and reliable streams. Requires ALPN `citadel/0`.

| Method | Signature (async) | Description |
| --- | --- | --- |
| `connect` | `connect(addr, server_name, ClientTls) -> QuicClient` | Connect and handshake TLS 1.3. |
| `send_unreliable` | `send_unreliable(&Envelope)` | Send as a datagram (hot-path state). |
| `send_reliable` | `send_reliable(&Envelope)` | Fire-and-forget over a fresh uni stream. |
| `recv_datagram` | `recv_datagram -> Envelope` | Next relayed peer datagram. |
| `recv_uni` | `recv_uni -> Vec<Envelope>` | Relayed reliable peer messages. |
| `call_rpc` | `call_rpc(method, payload) -> Vec<u8>` | Invoke a server RPC and await its correlated reply. See [RPC helper notes](#rpc-call-helpers). |
| `close` | `close` | Close the connection. |

## `ClientTls`

| Constructor | Description |
| --- | --- |
| `ClientTls::trusting(cert_chain)` | Pin a known server certificate. |
| `ClientTls::insecure_skip_verification` | Dev/test only: disable verification (clearly named; never validates identity). |

## RPC call helpers

`WsClient::call_rpc` and `QuicClient::call_rpc` are the client half of the RPC
[request/response wire format](/reference/protocol/envelope/#rpc-requestresponse). Each
call:

- generates a fresh, monotonically increasing `request_id`,
- sends a `KIND_RPC_REQUEST` (kind 3) reliably (`send` for WebSocket,
  `send_reliable` for QUIC), then
- reads envelopes until the matching `KIND_RPC_RESPONSE` (kind 4, correlated by
  `request_id`) arrives, and
- returns the handler's reply bytes on success, or
  `ClientError::Rpc { request_id, message }` when the server answered with an
  error status.

Correlation and usage notes:

- The reply is matched by `request_id`, so a stale or duplicate response for a
  different id is skipped rather than mistaken for this call's reply.
- The helper **discards any non-RPC envelopes** (for example relayed peer
  positions) that arrive while awaiting the reply. It is therefore meant for a
  connection that is not concurrently consuming the relay stream. Apps that also
  need relayed peer messages should poll and dispatch by kind themselves (as the
  Unity sample's `RpcClient` does) instead of calling this helper.
- It imposes **no timeout**. Wrap the call in `tokio::time::timeout` to bound how
  long you wait.

## Behavior notes

- The server runs a [relay gateway](/concepts/gateway/): inbound reliable streams
  and datagrams are routed to the gateway, and relayed peer messages arrive as
  datagrams (`recv_datagram`) or server-opened uni streams (`recv_uni`). The
  server does not echo to the sender.
- Endpoints and certificates are parameters; no credentials are embedded.

:::caution[Not implemented yet]
Dev-only TLS options; no production verification path. No reconnection/backoff,
auth/session binding (internal ), or message-kind taxonomy. Single
global relay room.
:::
