# Citadel Unity client plugin

This folder is a drop-in Unity plugin that connects to a Citadel server through
the native **C ABI** (`citadel_client_ffi.dll`). It contains the hand-written C#
bindings, a small sample (move-and-broadcast + RPC), and the prebuilt Windows
native library.

```
clients/unity/
├── Citadel/
│   ├── CitadelNative.cs      P/Invoke bindings + CitadelStatus enum (ABI v1)
│   ├── CitadelClient.cs      Managed IDisposable wrapper (connect/send/poll/free)
│   └── CitadelProtocol.cs    Wire kinds + position/RPC (de)serialization
├── Demo/
│   ├── CitadelConnection.cs  MonoBehaviour: owns the client, connects on Start
│   ├── LocalPlayer.cs        MonoBehaviour: input -> move + send KIND_POSITION
│   ├── PeerManager.cs        MonoBehaviour: single poll loop; dispatch by kind
│   └── RpcClient.cs          MonoBehaviour: CallRpc + correlate replies by id
└── Plugins/
    └── x86_64/
        └── citadel_client_ffi.dll   the native plugin (Windows x86_64)
```

## Requirements

- Unity 2021.3 LTS or newer (Mono or IL2CPP backend).
- Windows x86_64 (the shipped `.dll`). macOS (`.dylib`) / Linux (`.so`) are
  follow-ups.

## Import into a Unity project

1. Copy the `Citadel/`, `Demo/`, and `Plugins/` folders from here into your
   project's `Assets/` folder (e.g. under `Assets/Citadel/`).
2. In the Project view, select `Plugins/x86_64/citadel_client_ffi.dll` and, in
   the Inspector, set the plugin platform to **x86_64 / Standalone Windows**,
   then click Apply. Unity generates the `.meta` files on import.

## Wire up the sample scene

Create one scene with:

1. An empty GameObject `Citadel` with the **CitadelConnection** component
   (defaults: `127.0.0.1:7351`, server name `localhost`, insecure = on — point
   this at your Citadel server's QUIC address).
2. A **Cube** with the **LocalPlayer** component; drag the `Citadel` object into
   its `connection` field.
3. An empty GameObject `Peers` with the **PeerManager** component; drag the
   `Citadel` object into its `connection` field.
4. On the same `Peers` object, add the **RpcClient** component; drag the
   `Citadel` object into its `connection` field, then drag the **RpcClient** into
   the **PeerManager**'s `rpcClient` field.
5. A camera looking down at the ground plane, plus a directional light.

## Play

- Start the Citadel server (run `citadel.exe` in the package root). QUIC listens
  on `127.0.0.1:7351` by default.
- Press **Play** in Unity. The Console prints `native ABI version 1 OK` and
  `connected to 127.0.0.1:7351 (QUIC)`.
- Move your cube with **WASD / arrow keys**. Press **R** to fire the sample RPCs
  (`add`, `ping`) handled by `game/main.lua`.
- Open a second client and watch a second cube appear and track its movement.

## The wire protocol (must match the server)

- `KIND_POSITION` = **1** — client -> server. Body: two **little-endian** `f32`
  `(x, y)`.
- `KIND_PEER_POSITION` = **2** — server -> client. Body: an 8-byte **big-endian**
  sender session id, then the same two-`f32` position payload.
- `KIND_RPC_REQUEST` = **3** — client -> server. Body (big-endian):
  `request_id: u64 | method_len: u16 | method: utf8 | payload`.
- `KIND_RPC_RESPONSE` = **4** — server -> client (unicast). Body:
  `request_id: u64 | status: u8 (0=ok, 1=err) | payload`.

## Troubleshooting

- **`DllNotFoundException: citadel_client_ffi`** — the `.dll` is missing or its
  Unity import platform is not set. Confirm it is in `Plugins/x86_64/` with the
  platform set to Standalone Windows x86_64.
- **ABI mismatch error** — the plugin is stale relative to the bindings; the C#
  binding targets `CITADEL_FFI_ABI_VERSION = 1`.
- **Connect fails** — the server is not running, or QUIC is disabled in
  `citadel.toml` (`[transport.quic] enabled = true`).
- **No peer cube** — a single client has no peers; connect a second client.
