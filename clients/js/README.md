# @citadel/client — JavaScript / Web SDK

The realtime client SDK for Citadel in the browser and Node, peer to the
Unity, Godot, and Unreal SDKs under `clients/`. It speaks Citadel's wire format
directly over WebSocket — no native library, no build step.

- **Zero-build ESM.** Plain `.js` modules with JSDoc types plus a hand-written
  `index.d.ts`. Import from `file://`, `<script type="module">`, Node (≥ 22), or
  any bundler.
- **Wire-accurate.** `Envelope` framing and all protocol constants are validated
  against the canonical contract (`crates/citadel-wire/contract.json`) by the
  Tier-A parity check (`scripts/check.sh`), exactly like the other SDKs.
- **Pure network/state.** No rendering; the JS peer of the Rust `citadel-client`
  crate. Bring your own renderer. The runnable
  [`examples/threejs-starter/`](examples/threejs-starter/) is the canonical
  browser-game integration.

## Install

Pre-publish, import straight from source (the demo does this):

```js
import { CitadelClient, KIND_POSITION } from "../path/to/clients/js/src/index.js";
```

Once published: `npm install @citadel/client`.

## Quick start

```js
import { CitadelClient, KIND_POSITION } from "@citadel/client";

const client = await CitadelClient.connect("ws://127.0.0.1:7352/");
await client.handshakeGuest();                 // register as a guest

// Fire-and-forget: relay a position; listen for peers.
client.on(KIND_PEER_POSITION, (payload) => { /* render peer */ });
client.send(KIND_POSITION, new Uint8Array([/* your layout */]));

// Request/response RPC (matches citadel.on_rpc in your Lua game logic).
const reply = await client.callRpc("ping");    // Uint8Array
```

## Start a visual browser game with Three.js

The SDK deliberately does not depend on a renderer. For a browser game,
Three.js is Citadel's canonical JavaScript example: it gives you a scene loop,
input, meshes, and interpolation while `@citadel/client` handles only
WebSocket framing and messages.

Run the starter against the tracked local relay:

```bash
# terminal 1, from the repository root
cargo run -- --config examples/configs/demo.toml serve

# terminal 2, serve clients/js/ as the static root
python3 -m http.server 8000 --directory clients/js
```

Open <http://127.0.0.1:8000/examples/threejs-starter/> in two tabs. The starter
keeps the responsibilities intentionally separate:

| Layer | Owns |
| --- | --- |
| `@citadel/client` | WebSocket connection, guest handshake, framed envelopes, message dispatch. |
| Three.js application | Input, local visual prediction, meshes, remote interpolation, and packet layout. |
| Citadel game logic | Validation and authoritative rules when the game needs them. |

Read the starter's [run instructions](examples/threejs-starter/README.md) for
Windows serving commands, endpoint overrides, and the route from this simple
relay to authoritative movement.

## API surface

| Symbol | What it does |
| --- | --- |
| `CitadelClient.connect(url, opts?)` | Open a WebSocket and resolve when ready. |
| `client.handshakeGuest()` | Send `KIND_AUTH` (guest) and await the ack that registers the session. |
| `client.send(kind, body?)` | Send a framed envelope. |
| `client.on(kind, cb)` / `off` / `onAny(cb)` | Dispatch inbound envelopes by kind. |
| `client.callRpc(method, payload?, opts?)` | Correlated RPC; resolves reply bytes or throws `RpcError`. |
| `client.waitForKind(kind, timeoutMs?)` | One-shot await of the next envelope of a kind. |
| `client.joinOrCreateRoom(name)` / `joinRoom(id)` | Enter a named room or an existing room. |
| `client.onRoomJoined(cb)` / `sendMapReady(id)` | Receive the server-chosen map, load it, then acknowledge it. |
| `client.close()` | Close the connection. |
| `Envelope`, `FrameDecoder` | Wire framing (`encodeFramed` / stream decode). |
| `encodeRpcRequest`, `decodeRpcResponse`, `decodeAuthResult`, `splitSender`, `tagWithSender` | Low-level codecs. |
| `KIND_*`, `AUTH_*`, `RPC_*`, `*_BYTES` | Protocol constants (contract-checked). |

Bodies are raw `Uint8Array`; use `DataView` for typed layouts (Citadel is
big-endian on the wire). Pick your own message kinds `>= 100` — kinds `1..25`
are reserved by the core and netcode.

### Rooms

Subscribe before joining so your game receives the server-controlled map. Rooms
are reliable and `currentRoom` is updated before the callback runs:

```js
client.onRoomJoined((room) => {
  loadMap(room.map);
  client.sendMapReady(room.roomId);
});
client.joinOrCreateRoom("lobby");
```

## Tests

```bash
node --test clients/js/test/*.test.js   # codec round-trip + protocol layouts (zero deps)
```

## Combat benchmark

`crates/citadel-client/examples/combat_viz.html` renders 30 independent clients
(30 WebSockets) fighting in a three.js top-down arena, driven entirely by this
SDK. The Lua script owns HP, chat, monsters, monster attacks, death/respawn, and
static obstacles; the browser bot AI prefers monsters, falls back to players, and
uses lightweight obstacle avoidance. A Three.js telemetry panel displays
benchmark-local Lua snapshots for RTT, message rate, estimated payload KB/s,
combat churn, tick cadence, and script error rate. The single viewport also
draws server-relayed peer-position ghosts from one observer client and reports
replica freshness/drift, which verifies realtime replication without spawning 30
separate Three.js renderers. The refreshable benchmark package stages
the server, Lua combat script, HTML client, and local SDK import in
`bin/benchmark/`:

```bash
make benchmark-serve
```

That stages `bin/benchmark/`, starts the packaged server, serves the HTML client,
and opens `http://127.0.0.1:8080/client.html`. Manual equivalent:

```bash
make bin-benchmark
cd bin/benchmark && ./server.exe serve
# from the repo root in another terminal:
python3 -m http.server 8080 --directory bin/benchmark
# open http://127.0.0.1:8080/client.html
```

## Status & limitations

- WebSocket transport only. Browser WebTransport/QUIC is a planned follow-up.
- Not yet published to npm (import from source for now).
- Constant/layout parity is guaranteed by CI; end-to-end behavior is verified by
  the demo and the Rust `citadel-client` integration tests.
