---
title: Connect a web client
description: Build a browser game with Three.js and the @citadel/client browser SDK, using WebTransport datagrams when available and WebSocket fallback everywhere else.
---

The primary Citadel path for a browser game is **Three.js +
`@citadel/client`**. Three.js owns the visible game — scene, input, meshes, and
smoothing — while the SDK owns the WebTransport/WebSocket connection, Citadel
framing, guest handshake, and inbound-message dispatch. It is the JavaScript equivalent of
showing a Unity developer a C# scene integration, rather than a bare socket log.

`@citadel/client` remains renderer-neutral: Three.js is a recommended,
runnable integration, not an SDK dependency. The SDK is the JS peer of the
Unity, Godot, and Unreal SDKs, and its constants are kept in lockstep with the
server by the same Tier-A parity check.

## Build a browser game with Three.js (recommended)

The runnable source starter is
`clients/js/examples/threejs-starter/`. It uses the source SDK and the tracked
position-relay game. Start Citadel with WebSocket enabled:

```bash
# terminal 1, from the repository root (Windows, macOS, Linux)
cargo run -- --config examples/configs/demo.toml serve
```

Serve `clients/js/` as the static root — serving the nested example directory
would prevent its relative SDK import from resolving:

```bash
# macOS/Linux, or Windows with python on PATH
python3 -m http.server 8000 --directory clients/js

# Windows PowerShell with the Python launcher
py -m http.server 8000 --directory clients/js
```

Open <http://127.0.0.1:8000/examples/threejs-starter/> in **two browser tabs**.
The blue cube responds to your input immediately; a green cube represents the
other tab and smoothly moves to each relayed network update. Use
`?endpoint=ws://host:port/` to select another server.

The same starter is included by `make bin-client-js`: serve
`bin/clients/js/` and open `/examples/threejs-starter/`. Once the package is
published, replace the starter's relative source import with
`@citadel/client`; the Three.js game code does not need to change.

### Keep these responsibilities separate

| Layer | Owns | Does not own |
| --- | --- | --- |
| `@citadel/client` | WebTransport/WebSocket connection, guest/token handshake, Citadel envelopes and dispatch. | Scene graph, controls, models, interpolation policy. |
| Your Three.js app | Render loop, input, local visual prediction, remote visual interpolation, game packet layout. | Wire framing or server authority. |
| Citadel game logic | Validation, combat, persistence, and authoritative state. | Browser rendering. |

The starter's relay is deliberately simple: it sends opaque position packets
and relays them to other guests. It teaches the rendering/networking boundary;
it is **not** an authoritative movement implementation. For a game where the
server validates moves and owns monster HP, follow
[Build Knights vs Monsters](/tutorials/knights-vs-monsters/).

## SDK integration at a glance

The starter's important boundary is small. It uses Three.js for data that
becomes a visual, and hands `Uint8Array` bodies to the SDK:

```js
import * as THREE from "https://unpkg.com/three@0.160.0/build/three.module.js";
import {
  CitadelClient, KIND_POSITION, KIND_PEER_POSITION, splitSender,
} from "../../src/index.js"; // path inside clients/js/examples/threejs-starter/

const client = await CitadelClient.connect("ws://127.0.0.1:7352/");
await client.handshakeGuest;
const peers = new Map; // bigint -> { mesh, target: THREE.Vector3 }

function encodePosition(position) {
  const body = new Uint8Array(20); // x/y/z LE f32 + timestamp LE f64
  const view = new DataView(body.buffer);
  view.setFloat32(0, position.x, true);
  view.setFloat32(4, position.y, true);
  view.setFloat32(8, position.z, true);
  view.setFloat64(12, performance.now, true);
  return body;
}

client.on(KIND_PEER_POSITION, (body) => {
  const tagged = splitSender(body);
  if (!tagged || tagged[1].byteLength !== 20) return;
  const [senderId, payload] = tagged;
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const target = new THREE.Vector3(
    view.getFloat32(0, true), view.getFloat32(4, true), view.getFloat32(8, true),
  );
  peers.get(senderId)?.target.copy(target); // create the mesh on first sight
});

// In requestAnimationFrame: update localPlayer from input, then throttle sends.
// Remote meshes lerp toward their `target` in that same render loop.
client.send(KIND_POSITION, encodePosition(localPlayer.position));

// Request/response RPC, matched by correlation id under the hood.
const reply = await client.callRpc("ping"); // Uint8Array; throws RpcError on failure
```

Pick game message kinds `>= 100` — kinds `1..25` are reserved by the core and
netcode. The starter's 20-byte position body is a game-owned layout; version it
before extending it. See the server-side handlers in
[the Lua runtime](/reference/server-sdk/lua-runtime/) (`citadel.on_message`,
`citadel.on_rpc`).

### API surface

| Symbol | What it does |
| --- | --- |
| `CitadelClient.connect(url, opts?)` | Open a WebSocket and resolve when ready. |
| `CitadelClient.connectWebTransport(url, opts?)` | Open Chromium WebTransport. Reliable sends use streams; unreliable sends use datagrams. |
| `CitadelClient.connectAuto({ webTransportUrl, webSocketUrl }, opts?)` | Prefer WebTransport, then use the explicitly supplied WebSocket fallback if it is unavailable or fails before ready. |
| `handshakeGuest` | Present the guest handshake and await the ack. |
| `send(kind, body?, { reliable? })` | Send reliably by default; WebTransport uses a bare datagram when `reliable: false`. WebSocket remains reliable-only. |
| `on(kind, cb)` / `off` / `onAny(cb)` | Dispatch inbound envelopes by kind. |
| `callRpc(method, payload?, opts?)` | Correlated RPC; resolves reply bytes. |
| `webTransportCertificateHash(base64)` | Convert the server's logged development-certificate hash to the native browser pin shape. |
| `Envelope`, `FrameDecoder` | Low-level framing. |
| `KIND_*`, `splitSender`, `encodeRpcRequest`, … | Protocol constants and codecs. |

### Larger Three.js example: combat benchmark

`crates/citadel-client/examples/combat_viz.html` drives **30 independent
clients** (30 WebSockets) in a Three.js top-down arena. Lua decides HP, damage,
death, chat, monsters, monster attacks, and respawns; the browser owns visual
movement and a Three.js telemetry panel. One observer client draws the
server-relayed peer-position ghosts and reports replica freshness/drift.

```bash
make benchmark-serve
```

That stages `bin/benchmark/`, starts the packaged server, serves the HTML
client, and opens <http://127.0.0.1:8080/client.html>. Re-run
`make bin-benchmark` after changing the combat client, Lua script, or JS SDK.
See [Local build & staging targets](/reference/operations/make-targets/) for
the full `bin-*` family.

## Advanced: raw wire path

The SDK is the default for browser games. Work at the raw level only when you
are implementing another client library. The protocol is documented in
[Envelopes & wire protocol](/concepts/envelopes/): WebSocket carries a framed
`u32` big-endian body length, `u16` big-endian kind, then a payload.

### Raw WebSocket

```js
const ws = new WebSocket("ws://127.0.0.1:7352/");
ws.binaryType = "arraybuffer";

function encodeFramed(kind, payload) {
  const buf = new ArrayBuffer(4 + 2 + payload.length);
  const view = new DataView(buf);
  view.setUint32(0, 2 + payload.length); // body length = kind + payload
  view.setUint16(4, kind);
  new Uint8Array(buf, 6).set(payload);
  return buf;
}

ws.onopen = () => ws.send(encodeFramed(5 /* KIND_AUTH */, new Uint8Array(0)));
```

## WebTransport with WebSocket fallback (Chromium)

WebTransport gives Chromium a QUIC-grade datagram path without changing the
SDK's handshake, RPC, rooms, or dispatch APIs. Start the server and copy the
`cert_sha256_base64 = <hash>` value it logs for local development. Then give
the SDK both endpoints; it never guesses one endpoint from the other:

```js
import { CitadelClient, KIND_POSITION } from "./citadel-client.min.mjs";

const certHash = /* base64 from the server log */;
const client = await CitadelClient.connectAuto({
  webTransportUrl: "https://127.0.0.1:7353/",
  webSocketUrl: "ws://127.0.0.1:7352/",
}, {
  webTransport: { serverCertificateHashBase64: certHash },
});
await client.handshakeGuest();

// Position updates may be dropped; the SDK sends a bare u16-kind datagram on
// WebTransport and keeps the message reliable when WebSocket was selected.
client.send(KIND_POSITION, positionBytes, {
  reliable: client.transportKind !== "webtransport",
});
```

The development certificate is short-lived (at most 14 days) and ECDSA P-256,
matching Chrome's `serverCertificateHashes` requirement. For a CA-trusted
production certificate, omit `serverCertificateHashBase64`. `connectAuto`
falls back only before the WebTransport connection is ready; it never silently
migrates an authenticated client after a connection closes. Use
`connectWebTransport` when fallback is not appropriate.

:::caution[Known limitations]
The WebSocket listener is plain `ws://` (no built-in `wss://`); terminate TLS at
a reverse proxy for production. WebTransport support is currently a Chromium
browser path; WebSocket remains the portable browser transport. CI covers the
SDK codec and constant parity; exercise browser scenes manually with two tabs.
:::
