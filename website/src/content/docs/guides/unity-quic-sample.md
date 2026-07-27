---
title: Unity QUIC sample (C#)
description: A Unity C# sample that connects to Citadel over QUIC through the citadel-client-ffi C ABI, sending and relaying positions.
---

`clients/unity/` is the Unity C# SDK — hand-written bindings (`Citadel/`) plus a
minimal sample (`Demo/`) — that connects to a Citadel server through the native
**C ABI** ([`citadel-client-ffi`](/guides/c-abi/), ABI version 1) over **QUIC**.
`clients/` is SDK-only: source bindings and an import README, with the native
plugin built at package time (not committed). The sample runs the
move-and-broadcast loop end to end: a local
cube streams its position to the server as `KIND_POSITION`, and the server relays
it to peers as `KIND_PEER_POSITION`, which the sample renders as one cube per
remote session. It mirrors the native [`demo-client`](/guides/native-client/) but
drives the shared C ABI that Unreal or Godot would also bind.

:::note[Manual verification only]
The C# bindings are hand-written against the C header and there is **no automated
Unity test** (Unity is not in CI). The native plugin build is verified
(`cargo build --release -p citadel-client-ffi`); the in-editor run below is a
manual step.
:::

## Build and install the native plugin

From the repo root, build the cdylib and install it into the SDK:

```bat
:: Windows (cmd)
make unity-plugin
```

```powershell
# Windows (PowerShell)
.\make unity-plugin
```

```bash
# macOS / Linux
make unity-plugin
```

Both run `cargo build --release -p citadel-client-ffi`. On Windows the target
copies `citadel_client_ffi.dll` into `clients/unity/Plugins/x86_64/`; on macOS
it copies `libcitadel_client_ffi.dylib` into `clients/unity/Plugins/macOS/`.
These files are built, not committed. The matching release package contains the
same platform-native library.

## Managed C# API

The scripts under `clients/unity/Citadel/` bind the C ABI 1:1:

- **`CitadelNative`** — raw `[DllImport("citadel_client_ffi")]` (Cdecl) entry
  points plus the `CitadelStatus` enum. Strings are marshaled as NUL-terminated
  UTF-8 `byte[]`, C `bool` as a 1-byte value, and `uintptr_t` as `UIntPtr`, so the
  binding works on Unity's Mono and IL2CPP marshaling. `ExpectedAbiVersion = 1`.
- **`CitadelClient : IDisposable`** — the managed wrapper you use:

  ```csharp
  CitadelClient.CheckAbiVersion;                 // throws on ABI mismatch
  var client = CitadelClient.ConnectQuic("127.0.0.1:7351", "localhost", insecure: true);
  // also: CitadelClient.ConnectWebSocket("ws://127.0.0.1:7352/")
  AuthHandshakeResult auth = client.AuthenticateGuest;
  // or: client.AuthenticateWithToken(sessionToken)

  byte[] body = CitadelProtocol.EncodePosition(x, y);
  client.Send(CitadelProtocol.KindPosition, body, reliable: false);

  var buffer = new byte[256];
  PollResult r = client.Poll(buffer, out ushort kind, out int length, out bool truncated);
  // r is Message, Again, or Disconnected

  string err = client.LastError;                 // native message after a failure
  client.Dispose;                                // frees the native handle
  ```

  `Poll` is non-blocking: `Message` writes an envelope into your caller-owned
  `buffer` (with `kind`/`length`, and `truncated` if it did not fit), `Again`
  means nothing is ready this frame, and `Disconnected` means the connection is
  closed and drained. A finalizer frees the handle if `Dispose` is missed.
- **`CitadelProtocol`** — wire kinds and (de)serialization: `KindPosition = 1`,
  `KindPeerPosition = 2`, `KindRpcRequest = 3`, `KindRpcResponse = 4`,
  `RpcStatusOk`/`RpcStatusError`, `EncodePosition`, `TryDecodePosition`,
  `TryDecodePeerPosition`, and the RPC helpers `EncodeRpcRequest` /
  `TryDecodeRpcResponse`. It handles the mixed endianness explicitly so it is
  correct on any host.

### Wire protocol

Matching [`citadel-wire`](/concepts/envelopes/) and the native demo:

- `KIND_POSITION` = 1 — body: two **little-endian** `f32` `(x, y)`.
- `KIND_PEER_POSITION` = 2 — body: an 8-byte **big-endian** sender session id
  followed by the same two-`f32` position payload.

The sample maps world `(x, y)` to Unity `(x, 0, y)` so cubes slide on the ground
plane.

## Sample components

The MonoBehaviours under `clients/unity/Demo/`:

- **`CitadelConnection`** — owns the client, verifies the ABI version, connects
  over QUIC to `127.0.0.1:7351` (insecure dev cert), performs the guest realtime
  handshake on `Start`, and disposes on `OnDestroy`.
- **`LocalPlayer`** — reads WASD/arrow input, moves its transform on the X/Z
  plane, and sends `KIND_POSITION` **unreliable** at a fixed cadence.
- **`PeerManager`** — the **single owner of the poll loop**. It drains the shared
  native poll queue each frame and dispatches by kind: `KIND_PEER_POSITION` is
  rendered as one cube per sender session id, and `KIND_RPC_RESPONSE` is forwarded
  to the optional `RpcClient.HandleResponse`.
- **`RpcClient`** — issues request/response RPCs and correlates replies by
  `request_id`. See [Calling an RPC](#calling-an-rpc).

## Calling an RPC

`RpcClient` is the client half of the RPC
[request/response wire format](/reference/protocol/envelope/#rpc-requestresponse), built on
the **unchanged** poll-based C ABI — correlation lives entirely in the managed
layer. The flow:

1. `CallRpc(string method, byte[] payload, Action<CitadelRpcResult> onReply)`
   generates a monotonic `request_id`, encodes the body with
   `CitadelProtocol.EncodeRpcRequest`, and sends it as a `KindRpcRequest`
   **reliable** message. Once the send goes out, it registers `onReply` in a
   pending map keyed by `request_id`.
2. Because the native poll queue is shared across kinds, exactly **one component
   drains it** — `PeerManager`. When it polls a `KindRpcResponse`, it forwards the
   body to `RpcClient.HandleResponse`.
3. `HandleResponse` decodes the body with `CitadelProtocol.TryDecodeRpcResponse`,
   looks up the pending callback by `request_id`, and invokes it with a
   `CitadelRpcResult { RequestId, Ok, Payload }`. Unknown or duplicate ids are
   dropped with a warning.

`CitadelRpcResult` also offers `TryReadBeInt32(out int)` and `PayloadAsText`
helpers for common reply shapes.

Press **R** to fire the built-in sample, which calls two handlers defined in
`game/main.lua`:

- `add` — two big-endian `int32` operands; the reply is their `int32` sum.
- `ping` — a liveness check; the reply is the text `pong`.

Both results are logged via `Debug.Log`.

```csharp
// Two big-endian int32 operands -> the handler replies with their int32 sum.
byte[] payload = /* 7, 35 as big-endian int32 */;
rpcClient.CallRpc("add", payload, result =>
{
    if (result.Ok && result.TryReadBeInt32(out int sum))
        Debug.Log($"add = {sum}");
    else
        Debug.LogWarning($"add failed: {result.PayloadAsText}");
});
```

## Run it (manual)

1. Build the plugin: `make unity-plugin` (cmd or macOS/Linux) or `.\make unity-plugin` (PowerShell).
2. Import `clients/unity/Citadel/` and `Demo/` into a Unity project's `Assets/`.
   On Windows copy the DLL into `Assets/Plugins/x86_64/` and set **x86_64 /
   Standalone Windows**. On macOS copy the dylib into `Assets/Plugins/macOS/`
   and enable macOS plus the matching Apple Silicon or Intel CPU.
3. Build a scene with a `CitadelConnection` object, a cube with `LocalPlayer`, and
   a `PeerManager` object; wire the connection reference into `LocalPlayer` and
   `PeerManager`. Add a top-down camera and a light. To try RPC, add an `RpcClient`
   component, wire its `connection` reference, and set the `PeerManager.rpcClient`
   reference so polled responses are dispatched to it.
4. Start the server:

   ```bash
   cargo run -- --config examples/configs/demo.toml serve
   ```

5. Press **Play**, move the cube with WASD/arrows, and open a second client
   (another Unity instance or `cargo run -p demo-client`) to watch its cube track
   in real time. Press **R** to fire the sample `add`/`ping` RPCs and watch the
   replies in the console.

See `clients/unity/README.md` for detailed scene setup and
troubleshooting.

:::caution[Not implemented yet]
Windows x86_64 is released; macOS `.dylib` packages for Apple Silicon and Intel
can be built locally and await their signed/notarized public release path. Linux `.so`,
IL2CPP stripping, a shipped `.unitypackage`, and a richer host API are
follow-ups. The sample uses the insecure dev TLS path (no certificate
verification) for local development only; a pinned/verified path is deferred
(internal ). No credentials are embedded.
RPC correlation lives in the managed layer; a C ABI-level RPC convenience,
client-side RPC timeouts/retries, and streaming RPC are follow-ups.
:::
