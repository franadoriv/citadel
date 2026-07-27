# Citadel Godot client SDK

Hand-maintained **GDScript** bindings over the Citadel client **C ABI**
(`citadel-client-ffi`, ABI v2). This is an **SDK, not a Godot project** — you
copy these scripts into your own Godot 4 project (see the `clients/` convention
below).

The wire constants and `PackedByteArray` (de)serialization in
`citadel/protocol.gd` are mechanically checked against the canonical contract on
every `scripts/check.sh` run (Tier-A parity). `CitadelClient` uses the shipped
native GDExtension source to call `citadel-client-ffi`, including QUIC/WebSocket
connect, the explicit realtime auth handshake, send, and non-blocking poll.

## `clients/` is SDK-only

Every `clients/<lang-or-engine>/` directory holds **only the client SDK**: the
source bindings and an import README. Build outputs — the native GDExtension
library (`.dll` / `.so` / `.dylib`) — are produced at package time, **not
committed**. The repo does not track a full Godot project (`.godot/`, scenes,
per-developer editor state). See `docs/architecture/client-sdk-layout.md`.

## What's here

```
clients/godot/
  citadel/
    protocol.gd   wire kinds/statuses/byte-counts + ABI version, and the
                  position/RPC (de)serialization. This is the ONE file the
                  Tier-A parity check parses (const NAME := N).
    client.gd     CitadelClient wrapper over the C ABI (connect/send/poll/close)
    rooms.gd      Named-room operations plus joined/left signals
                  plus the CitadelStatus enum. Delegates to the native binding.
  sample/
    peer_sync.gd  a minimal move-and-broadcast usage sample.
  sdk.manifest.json  names citadel/protocol.gd + the canonical keys Godot claims.
  native/            GDExtension source, descriptor, and reproducible SCons build.
  addons/citadel_map_exporter/
                    editor plugin that exports static scene geometry to CMAP.
  README.md
```

The native binary is **not** committed. It is built from `native/` and shipped by
the release packaging step.

## Requirements

- Godot **4.x**.
- A C++17 compiler, SCons, and a Godot 4 `godot-cpp` checkout.
- The Rust toolchain, to build `citadel-client-ffi`.

## Where the native library comes from

The GDScript bindings drive the native `citadel-client-ffi` C ABI. That library
is built from the repo with the Rust toolchain (the same crate the Unity SDK
uses):

```bash
cargo build --release -p citadel-client-ffi
# produces target/release/citadel_client_ffi.{dll,so,dylib}
```

Build the thin C++ GDExtension over that C ABI, then copy the descriptor and the
matching `bin/` output beside your scripts. On Windows SCons also copies
`citadel_client_ffi.dll` into `bin/`; macOS links the FFI static archive into
the GDExtension, so its `.dylib` has no Citadel runtime dylib dependency. The
extension does not reimplement framing or authentication; it delegates them to
Rust:

```bash
git clone --branch 4.3 https://github.com/godotengine/godot-cpp.git ../godot-cpp
cargo build --release -p citadel-client-ffi
cd clients/godot/native
GODOT_CPP_PATH=../../../../godot-cpp \
  CITADEL_FFI_LIB_DIR=../../../target/release \
  scons target=template_release platform=windows api_version=4.6 \
  build_profile=build_profile.json use_static_cpp=no
```

On PowerShell, set the two variables with `$env:GODOT_CPP_PATH` and
`$env:CITADEL_FFI_LIB_DIR` before running `scons`. Use the platform/target that
matches your Godot export. Place `native/citadel.gdextension` at
`addons/citadel/citadel.gdextension` and the generated library where the
descriptor names it (`addons/citadel/bin/`). Both are package artifacts; only the
descriptor source is versioned here.

## Import into a Godot project

:::note
**Using the release download?** The Windows package already contains the
prebuilt GDExtension. macOS Apple Silicon and Intel packages are built in CI and
will appear as `citadel-client-godot-macos-{aarch64|x86_64}-v{version}.zip` with
the first signed/notarized macOS release. Each contains the matching prebuilt
GDExtension under `addons/citadel/bin/`; copy its `addons/` folder into your
project's `res://` root and skip the native build below.
:::

1. Copy `citadel/` (and, if you want the sample, `sample/`) into your Godot 4
   project, e.g. `res://addons/citadel/`.
2. Build `native/` as above. Copy `native/citadel.gdextension` and every generated
   file in `native/bin/` under `addons/citadel/`; on Windows this includes
   `citadel_client_ffi.dll`, while macOS produces architecture-specific
   `libcitadel_godot.macos.template_*.{arm64|x86_64}.dylib` files. Godot registers
   `CitadelClientNative` on project startup. (The release packaging step does the
   same for each supported platform — see the note above.)
3. Use the SDK from your own scripts:

```gdscript
var client := CitadelClient.new()
if not client.check_abi_version():
    push_warning(client.last_error)
var status := client.connect_quic("127.0.0.1:7351", "localhost", true)
if status == CitadelClient.Status.OK:
    var auth := {}
    status = client.authenticate_guest(auth) # or authenticate_with_token(token, auth)
if status == CitadelClient.Status.OK:
    client.send(CitadelProtocol.KIND_POSITION, CitadelProtocol.encode_position(x, y), false)
```

## Export server collision geometry

The SDK also ships an editor-only **Citadel Map Exporter** plugin. Copy
`addons/citadel_map_exporter/` into your project's `res://addons/` directory,
enable **Citadel Map Exporter** in **Project → Project Settings → Plugins**, then
open the level and select **Tools → Citadel → Export CMAP Map…**.

### Terrain providers

Godot does not define a built-in Terrain node. A terrain addon opts into CMAP
collision export by implementing `citadel_cmap_terrain()` on its terrain node.
The method returns a dictionary with `width`, `depth`, `heights` (a row-major
`PackedFloat32Array` of `width * depth` collision heights), `size` (`Vector3`,
with `x` and `z` extents), and optionally `holes` (a row-major
`PackedByteArray` of `(width - 1) * (depth - 1)` cells; non-zero means absent).
The exporter applies the node global transform, combines this heightfield with
opted-in static meshes, welds at `0.001` world units, drops degenerates, and
rejects non-finite data or maps over 10 million triangles. Trees, details,
render displacement, and runtime deformation are intentionally excluded.

It exports each `MeshInstance3D` under a `StaticBody3D` to one world-space
triangle mesh. A mesh outside a static body can opt in with metadata named
`citadel_export_collision` set to `true`; set that metadata to `false` to exclude
one. Put the resulting `.map` file in the server's `runtime.maps_dir`. See the
[map workflow](../../website/src/content/docs/reference/client-sdk/maps.mdx) for
the server setup and limitations.

## The wire protocol (must match the server)

`citadel/protocol.gd` encodes/decodes exactly this — matching
`crates/citadel-wire/src/protocol.rs` and `crates/citadel-wire/contract.json`:

- `KIND_POSITION` = **1** — client → server. Body: two **little-endian** `f32`
  `(x, y)`.
- `KIND_PEER_POSITION` = **2** — server → client. Body: an 8-byte **big-endian**
  sender session id, followed by the two-`f32` position payload.
- `KIND_RPC_REQUEST` = **3** — client → server. Body (integers **big-endian**):
  `request_id: u64 | method_len: u16 | method: utf8 | payload`.
- `KIND_RPC_RESPONSE` = **4** — server → client. Body:
  `request_id: u64 | status: u8` (0 = ok, 1 = error) `| payload`.
- `KIND_AUTH` = **5** — client → server as the first realtime envelope. Empty
  body requests a guest; token bytes authenticate an account session.
- `KIND_AUTH_RESULT` = **6** — server → client auth reply. The wrapper surfaces
  this through `authenticate_guest` / `authenticate_with_token`.

**Endianness is not auto-checkable** — only the constant values are. The Tier-A
parity check guarantees the numbers match the server; the byte order in
`protocol.gd` (position floats LE, sender id + RPC ids BE) is verified by review
and, once the transport is live, an in-editor smoke test.

### Rooms

Create `CitadelRooms.new(client)` after authentication and subscribe to its
`joined(room)` signal before calling `join_or_create(name)` or `join(id)`. Keep
one poll loop and pass every envelope through `rooms.handle_envelope(kind,
payload)`; on `joined`, load `room["map"]` and call
`send_map_ready(room["room_id"])` after the scene is available. Room operations
are reliable.

## Godot Web export (no GDExtension)

For a browser export, install the dedicated distributable WebAssembly package
instead of assembling GDScript files by hand. It requires Godot 4.3 (or the
supported matching version) with Web export templates. From a checkout, run:

```bash
GODOT_BIN=/path/to/godot make package-client-godot-web
# Windows PowerShell
$env:GODOT_BIN = "C:\Path\To\Godot.exe"
.\make.ps1 package-client-godot-web
```

Extract `dist/citadel-client-godot-web-v<version>.zip` into the Godot project's
`res://` root. It creates `res://addons/citadel/` with
`protocol.gd`, `client.gd`, `web_client.gd`, and `rooms.gd`; it deliberately
contains no `.gdextension` or native `bin/` directory. Its `web/` directory is
a runnable verification export with matched `index.html`, `index.js`,
`index.pck`, and `index.wasm` files, plus `citadel-e2e.toml` and `serve_web.py`
for the real-browser verification. Serve that directory over HTTP(S), keeping
the names together and serving `.wasm` as `application/wasm`.

Copy the root `addons/` directory into your own project and export your game as
usual; the included Web build is a distributable verification application, not a
replacement for your game's export. Instantiate `CitadelWebClient` and connect
to a Citadel `wss://` endpoint.
`WebSocketPeer` is non-blocking: call `pump()` from `_process`, wait for
`is_open()`, then retry `authenticate_guest()` or `authenticate_with_token()`
until it returns `Status.OK`. Existing `CitadelRooms` works through the same
`send`/`poll` loop. Authentication is the required first envelope: gameplay
`send` calls return `Status.CONNECT` until its one-time reply arrives.

To prove the released application against a local compatible Citadel server,
run `cargo build --bin citadel`, then from its `web/` directory run
`target/debug/citadel --yes --config citadel-e2e.toml serve` and
`python3 serve_web.py --port 18080`. Open
`http://127.0.0.1:18080/index.html?citadel_ws=ws://127.0.0.1:17532/` in a
WebAssembly/WebGL2-capable browser. The application itself opens two browser
clients, guest-authenticates them, verifies Citadel's relayed position, and
closes both sockets. Use `wss://` instead of `ws://` outside localhost.

```gdscript
var client := CitadelWebClient.new()
var auth := {}

func _ready() -> void:
	assert(client.connect_websocket("wss://game.example.com:7352/") == CitadelClient.Status.OK)

func _process(_delta: float) -> void:
	client.pump()
	if client.is_open() and auth.is_empty():
		client.authenticate_guest(auth) # AGAIN until the server reply arrives.
```

An HTTPS page must use `wss://`: browsers block mixed-content `ws://` traffic.
The endpoint must be reachable from the page and present a browser-trusted TLS
certificate. The client follows the native WebSocket handshake semantics: it
requires `KIND_AUTH_RESULT` as the first binary Citadel reply, rejects malformed
frames, and safely treats an unknown auth status as rejected. Non-Citadel text
WebSocket packets are ignored just like the native client. This is reliable
WebSocket only; browser exports intentionally do not provide QUIC/datagrams,
transform-sync, or native replication codecs.

## Parity check

`scripts/check-sdk-parity.sh` (run by `scripts/check.sh`) discovers this SDK via
`clients/godot/sdk.manifest.json` and diffs every constant Godot claims against
`crates/citadel-wire/contract.json`. Any value mismatch, or a claimed key that
`citadel/protocol.gd` omits, fails the build. Run it standalone:

```bash
bash scripts/check-sdk-parity.sh
```

## Known limitations

- **Browser runtime verification is CI-backed, but a real hosted origin remains
  an operator check.** CI loads the packaged `.wasm` application in Chromium,
  connects it to a running Citadel WebSocket server, and verifies guest auth,
  reliable relay, receive/poll and close. It also keeps the fast RFC 6455 fixture
  and artifact-integrity checks. A manual `wss://` playthrough remains necessary
  to validate the deployed origin, TLS certificate, MIME type, and browser
  console.
- The editor run of `sample/peer_sync.gd`, desktop-native signature correctness,
  and native marshaling endianness remain **manual pre-release** items (see
  `docs/architecture/client-sdk-sync.md` §6).
- Native binaries are release artifacts rather than repository files. The build
  currently publishes the platforms supported by the C ABI package; add a
  descriptor entry only together with its verified package output.
