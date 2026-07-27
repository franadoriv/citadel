# Citadel low-level transport demo (Three.js: WebTransport or WebSocket)

This is a local, no-build-step **transport** demo. It manually encodes Citadel
envelopes so it can exercise WebTransport datagrams as well as WebSocket. It
visualizes the realtime gateway relay: move a blue cube, send its position, and
watch a green cube for each OTHER connected player move as the server relays
their positions.

For a browser game built on the JavaScript SDK, start with
[`clients/js/examples/threejs-starter/`](../../clients/js/examples/threejs-starter/)
instead. That is the canonical `@citadel/client` + Three.js example; this demo
intentionally stays below the SDK boundary for transport coverage.

The demo prefers **WebTransport** (Chromium, low-latency datagrams) and falls
back to **WebSocket** when WebTransport is unavailable or no cert hash is
provided.

This is a **local development example**. Do not deploy it to external hosting.
No credentials are embedded; the endpoints are configurable.

## Prerequisites

Enable the transports in the server config. Create a file, e.g.
`web-demo.toml`:

```toml
[transport.webtransport]
enabled = true
bind = "127.0.0.1:7353"

[transport.websocket]
enabled = true
bind = "127.0.0.1:7352"
```

## Run

1. Start the server:

   ```bash
   cargo run -- --config web-demo.toml serve
   ```

   You should see `WebSocket listener accepting connections` and, for
   WebTransport, a `WebTransport listener ... cert_sha256_base64 = <hash>` line.
   Copy that base64 hash for the WebTransport path.

2. Open the demo. Because it is an ES module, serve the folder over HTTP (most
   browsers block module imports from `file://`). Any static server works, e.g.:

   ```bash
   # from the repo root
   python3 -m http.server 8000 --directory examples/web-demo
   ```

   Then open <http://127.0.0.1:8000/> in Chrome (Chromium-based) for
   WebTransport.

3. For **WebTransport** (preferred, Chromium): paste the cert hash from the
   server log into the "WT cert hash" field, confirm the WT URL
   (`https://127.0.0.1:7353/`), and click **Connect**. The transport indicator
   shows "WebTransport (datagrams)". The dev cert is short-lived (<= 14 days,
   ECDSA P-256) — Chrome's requirement for `serverCertificateHashes`.

   For **WebSocket** (fallback): leave the cert hash empty (or use a browser
   without WebTransport). The demo falls back to `ws://127.0.0.1:7352/`.

4. Move with WASD / arrow keys or drag the mouse. To see the relay, open a
   SECOND tab/browser and connect. Moving in one moves a green cube in the other.
   You never see your own cube as a peer — the server relays only to OTHERS.

Query-string overrides: `?endpoint=ws://...`, `?wt=https://...`, `?wtHash=<b64>`.

## Wire format

The demo encodes envelopes exactly like `crates/citadel-wire`:

```
framed = u32 BE body_len | u16 BE kind | payload
body_len = 2 + payload.len()   (covers the u16 kind + payload)
```

We send `KIND_POSITION` (1) with a payload of three little-endian `f32`
coordinates (x, y, z) plus a little-endian `f64` client timestamp. The gateway
relays it to peers as `KIND_PEER_POSITION` (2): an 8-byte big-endian sender
session id followed by the original payload. The demo renders one green cube per
sender id.

## Notes and limitations

- Browsers cannot speak raw QUIC/UDP; this demo uses WebSocket. The native
  `demo-client` crate proves the QUIC datagram and reliable-stream paths.
- The relay room is a single global room; presence/streams/matches are future
  work.
- Web and native demos use different position payloads, so they render each
  other only as raw movement is not interoperable; use two web tabs (or two
  native clients) for a coherent picture.

## Try it against a different runtime (Python or JavaScript)

The web demo is runtime-agnostic — it speaks the wire protocol, so any game
script that relays positions works. To drive it with the **embedded Python**
runtime instead of Lua, point the server at the ready-made config:

```
cargo run --features runtime-python -- --config examples/configs/python-demo.toml
```

That serves `examples/python-game/main.py` (the Python port of the sample game)
behind the same transports. Open this page and move the cube exactly as with
Lua — the Python game relays the positions. Hot-reload works: edit
`examples/python-game/main.py` and save.

To use the capped embedded JavaScript runtime, run:

```
cargo run --features runtime-js -- --config examples/configs/js-game.toml
```

That serves `examples/js-game/main.js`, which implements the same position
relay. This mode is QuickJS: it does not provide Node APIs, npm packages,
worker threads, or TypeScript transpilation. Edit `main.js` and save to test
hot reload.

### Browser-free verification (`relay_smoke.py`)

`relay_smoke.py` is a headless companion to this page: it connects two clients,
authenticates as guests, sends a position from one, and asserts the other
receives the relayed `KIND_PEER_POSITION`. It is runtime-agnostic (the wire
kinds are language-neutral), so the same probe verifies the Lua, Python, or JS
game — whichever the server is running:

```
pip install websockets
python examples/web-demo/relay_smoke.py    # exit 0 = PASS
```

Override the endpoint with `CITADEL_WS=ws://host:port/`. This proves the full
realtime chain (auth handshake -> on_join -> the game's on_message handler ->
broadcast -> wire) without a browser.
