# Citadel Unity client SDK

The Citadel Unity SDK: hand-written C# bindings over the native **C ABI**
(`citadel-client-ffi`) plus a small sample that runs the move-and-broadcast loop.
This is an **SDK, not a Unity project** — you import these scripts into your own
Unity project (see the `clients/` convention below).

The bindings mirror the native `demo-client` (macroquad) but drive the same C ABI
that Unity, Unreal, or Godot would use in a real game.

> **Manual verification only.** These are hand-written C# bindings verified
> against the C header; there is no automated Unity test in CI. The Rust plugin
> build is verified (`cargo build --release -p citadel-client-ffi`). The
> in-editor run below is a manual step.

## `clients/` is SDK-only

Every `clients/<lang-or-engine>/` directory holds **only the client SDK**: the
source bindings and an import README. Build outputs — such as the native plugin
DLL — are produced at package time (or by `make unity-plugin`), **not committed**.
The repo does not track a full engine project. See
`website/src/content/docs/guides/engines.md` for the convention and how it applies to
future SDKs.

## What's here

```
clients/unity/
  Citadel/                  the SDK bindings (import these into your project)
    CitadelNative.cs        P/Invoke bindings + CitadelStatus enum (ABI v1)
    CitadelClient.cs        Managed IDisposable wrapper (connect/send/poll/free)
    CitadelProtocol.cs      Wire kinds + position/RPC (de)serialization
    CitadelRooms.cs         Named-room operations + joined/left lifecycle events
  Editor/
    CitadelCmapExporter.cs  Editor-only static collision + Terrain CMAP exporter
  Demo/                     a usage sample (move-and-broadcast + RPC)
    CitadelConnection.cs    MonoBehaviour: owns the client, connects on Start
    LocalPlayer.cs          MonoBehaviour: input -> move + send KIND_POSITION
    PeerManager.cs          MonoBehaviour: single poll loop; dispatch by kind
    RpcClient.cs            MonoBehaviour: CallRpc + correlate replies by id
  README.md
```

The native plugin is **not** in this tree. You build it and drop the
platform-specific library into the matching `Assets/Plugins/` folder (see below).

## Requirements

- Unity 2021.3 LTS or newer (any recent LTS with the Mono or IL2CPP backend).
- The Rust toolchain, to build the native plugin.
- Windows x86_64 is released today. Native macOS Apple Silicon and Intel
  packages use `Plugins/macOS/libcitadel_client_ffi.dylib`; choose the archive
  that matches the editor/target architecture. Linux (`.so`) and IL2CPP remain
  follow-ups (see the repo root `README.md` roadmap).

## Where the native DLL comes from

The bindings load `citadel_client_ffi` at runtime, so your Unity project needs the
native plugin. Build it from the repo root:

**Windows (cmd or PowerShell):**

From `cmd.exe`:
```cmd
make unity-plugin
```

From PowerShell:
```powershell
.\make unity-plugin
```

**macOS / Linux:**

```bash
make unity-plugin
```

This runs `cargo build --release -p citadel-client-ffi`. On Windows it copies
`target/release/citadel_client_ffi.dll` into `clients/unity/Plugins/x86_64/`; on
macOS it copies `target/release/libcitadel_client_ffi.dylib` into
`clients/unity/Plugins/macOS/` (both are git-ignored build output). The matching
release package contains the same native file beside the bindings and this
README.

## Import into a Unity project

1. Copy `Citadel/` (and, if you want the sample, `Demo/`) into your project's
   `Assets/` folder — e.g. `Assets/Citadel/` and `Assets/Demo/`.
2. On Windows, copy `citadel_client_ffi.dll` into `Assets/Plugins/x86_64/` and
   set its import platform to **x86_64 / Standalone Windows**. On macOS, copy
   `libcitadel_client_ffi.dylib` into `Assets/Plugins/macOS/`, then enable
   **macOS** and the matching Apple Silicon or Intel CPU in the Inspector.

## Export level collision (CMAP)

Copy `Editor/CitadelCmapExporter.cs` to `Assets/Citadel/Editor/`. With a scene
open, use **Tools → Citadel → Export CMAP Map…**. The editor collects static
`MeshCollider` geometry and built-in static `Terrain` heightfields in stable
scene order, emits one world-space CMAP mesh, welds shared vertices at `0.001`
world units, skips degenerate triangles, and honours Terrain holes. The export
fails instead of writing a partial map if a source transform is singular, a
vertex is non-finite, or it would exceed the 10-million-triangle safety cap.
The result belongs in the server `maps_dir`; see the website Maps reference for
CMAP limitations.

This first Unity exporter does not include non-static colliders, trees/details,
runtime terrain deformation, or boolean mesh union. It is editor-only. For the
editor smoke test, create a static `MeshCollider` floor plus a static Terrain
with one painted hole, export it, and run `cargo test --test
unity_cmap_export` in the Citadel checkout to verify the checked-in CMAP layout
and navmesh regression fixture. Open the exported map with a server whose
`maps_dir` contains that file; the server log must load it and bake navigation.

## The wire protocol (must match the server)

- `KIND_POSITION` = **1** — client → server. Body: two **little-endian** `f32`
  `(x, y)`: "my position".
- `KIND_PEER_POSITION` = **2** — server → client. Body: an 8-byte **big-endian**
  sender session id, followed by the same two-`f32` position payload.
- `KIND_RPC_REQUEST` = **3** — client → server. Body (all integers
  **big-endian**): `request_id: u64 | method_len: u16 | method: utf8 | payload`.
- `KIND_RPC_RESPONSE` = **4** — server → client (unicast to the caller). Body:
  `request_id: u64` (echoed for correlation) `| status: u8` (0 = ok, 1 = error)
  `| payload` (the handler's reply on ok, or a short utf8 error message).

`CitadelProtocol.cs` encodes/decodes exactly this. The sample maps world `(x, y)`
to Unity `(x, 0, y)` so cubes slide on the ground plane.

### Rooms

`CitadelRooms` is intentionally fed by your application's single `Poll` owner.
Create it from the connected client, assign it to `PeerManager.rooms` when using
the sample dispatcher, subscribe to `Joined`, load `RoomInfo.Map`, and call
`SendMapReady(RoomInfo.RoomId)` after the scene is ready. `JoinOrCreate(name)`,
`Join(id)`, and `Leave(id)` always send reliable room frames.

### RPC call flow (request/response)

Unlike the fire-and-forget position relay, an RPC expects exactly one correlated
reply. `RpcClient.CallRpc(method, payload, onReply)`:

1. Generates a monotonically increasing `request_id`.
2. Encodes a `KIND_RPC_REQUEST` (`CitadelProtocol.EncodeRpcRequest`) and `Send`s
   it **reliably**.
3. Registers `onReply` in a pending map keyed by `request_id`.

The native poll queue is shared across all kinds, so **exactly one component owns
the poll loop** — here `PeerManager`. It drains envelopes and dispatches by kind:
peer positions are rendered; a `KIND_RPC_RESPONSE` is forwarded to
`RpcClient.HandleResponse`, which decodes it, looks up the pending callback by
`request_id`, invokes it with a `CitadelRpcResult`, and removes it. A response for
an unknown or already-resolved `request_id` is dropped with a warning, so replies
are never mistaken for peer positions (or vice versa). This managed-layer
correlation is why the C ABI stays poll-based and unchanged.

## Quickstart (run the sample)

### 1. Build and install the native plugin

`make unity-plugin` (from cmd or `.\make unity-plugin` from PowerShell) — see
"Where the native DLL comes from" above.

### 2. Import the scripts and set the plugin platform

Copy `Citadel/` and `Demo/` into your project's `Assets/`. On Windows copy the
DLL into `Assets/Plugins/x86_64/` with its import platform set to **x86_64 /
Standalone Windows**; on macOS copy the dylib into `Assets/Plugins/macOS/` and
enable the matching CPU architecture.

### 3. Build the scene

Create one scene with:

1. An empty GameObject `Citadel` with the **CitadelConnection** component
   (defaults: `127.0.0.1:7351`, server name `localhost`, insecure = on).
2. A **Cube** with the **LocalPlayer** component; drag the `Citadel` object into
   its `connection` field.
3. An empty GameObject `Peers` with the **PeerManager** component; drag the
   `Citadel` object into its `connection` field.
4. On the same `Peers` object (or another GameObject), add the **RpcClient**
   component; drag the `Citadel` object into its `connection` field, then drag the
   **RpcClient** into the **PeerManager**'s `rpcClient` field so polled RPC
   responses are dispatched to it.
5. A camera looking down at the ground plane (e.g. position `(0, 12, 0)`,
   rotation `(90, 0, 0)`), plus a directional light.

### 4. Run the server (with game logic)

From the repo root, in a separate terminal:

```bash
cargo run -- --config examples/configs/demo.toml serve
```

The tracked `game/main.lua` relays `KIND_POSITION` → `KIND_PEER_POSITION`. If
`game/` is absent the server uses the identical built-in relay, so the sample
works either way.

### 5. Play

- Press **Play** in Unity. The Console prints `native ABI version 1 OK` and
  `connected to 127.0.0.1:7351 (QUIC), auth=Guest`.
- Move your cube with **WASD / arrow keys**.
- Press **R** to fire the sample RPCs. The Console logs
  `add(7, 35) = 42` (a typed request/response) and `ping RPC -> pong`. The
  handlers live in `game/main.lua` (`add`, `ping`).
- Open a second client — another Unity editor/build, or the native demo
  (`make native` / `cargo run -p demo-client`) — and watch a second cube appear
  and track that client's movement. Move it and your first client shows it too.

## Troubleshooting

- **`DllNotFoundException: citadel_client_ffi`** — the plugin was not built or is
  in the wrong folder. Re-run `.\make.ps1 unity-plugin` and confirm the DLL is in
  your project's `Assets/Plugins/x86_64/` with its Unity import platform set.
- **ABI mismatch error** — the plugin is stale. Rebuild it; the C# binding
  targets `CITADEL_FFI_ABI_VERSION = 1`.
- **Connect fails** — the server is not running, or QUIC is disabled. Use
  `examples/configs/demo.toml`, which enables QUIC on `:7351`.
- **Auth fails after connect** — call `AuthenticateGuest()` or
  `AuthenticateWithToken(token)` immediately after `ConnectQuic` /
  `ConnectWebSocket`, before gameplay sends. The demo uses guest auth.
- **No peer cube** — you need a *second* client connected; a single client has no
  peers to relay to.

## Notes and limitations

- Polling runs on the main thread in `Update()` (the C ABI poll is non-blocking).
  A background poll thread is not required for the sample. Exactly one component
  (`PeerManager`) polls and dispatches by kind; other behaviours (like
  `RpcClient`) receive their envelopes from it rather than polling in parallel.
- Positions are sent **unreliable** (hot-path state); RPC requests are sent
  **reliable** so the request/response pair is not silently dropped.
- `RpcClient` has **no timeout or retry**: a pending callback for a request whose
  reply never arrives stays registered. Client-side timeouts/retries, a C
  ABI-level RPC convenience, and streaming RPC are follow-ups.
- Windows x86_64 is released. macOS Apple Silicon/Intel package builds are
  covered by CI and await their first signed/notarized public release. Linux
  `.so`, IL2CPP stripping, a real `.unitypackage`, and a richer host API remain
  follow-ups.
</content>
</invoke>
