# Three.js multiplayer starter

This is the smallest playable browser scene built with **Three.js** and
`@citadel/client`. The SDK owns WebTransport/WebSocket framing, guest
authentication, and message dispatch. The game owns the render loop, input, packet layout, local
prediction, and peer interpolation.

It runs against Citadel's tracked position relay. That relay intentionally
forwards opaque position bytes; it is useful for learning the client boundary,
not an authoritative movement system. For server-owned rules such as combat,
read the **Build Knights vs Monsters** tutorial in the Citadel documentation
site.

## Run from a checkout

From the repository root, start Citadel with WebSocket enabled:

```bash
cargo run -- --config examples/configs/demo.toml serve
```

Serve `clients/js/`, not this nested directory, so the starter can import the
SDK source:

```bash
# macOS/Linux (or Windows when `python` is on PATH)
python3 -m http.server 8000 --directory clients/js

# Windows PowerShell with the Python launcher
py -m http.server 8000 --directory clients/js
```

Open <http://127.0.0.1:8000/examples/threejs-starter/> in two tabs. Move with
WASD or the arrow keys. The blue cube is immediately predicted local input;
green cubes are remote peers smoothed toward the last relayed position.

Use another WebSocket endpoint when needed:

```text
http://127.0.0.1:8000/examples/threejs-starter/?endpoint=ws://localhost:7352/
```

To prefer the local Chromium WebTransport listener, copy the server's logged
`cert_sha256_base64` value and supply both explicit endpoints. The starter sends
its drop-safe position updates as datagrams when WebTransport wins, and keeps
them reliable on WebSocket fallback:

```text
http://127.0.0.1:8000/examples/threejs-starter/?webtransportEndpoint=https://127.0.0.1:7353/&webtransportCertHash=<base64-server-hash>
```

## Run from the staged SDK

`make bin-client-js` includes this example. Serve `bin/clients/js/` as the
static root and open the same `/examples/threejs-starter/` path. A release ZIP
contains this starter plus `dist/citadel-client.min.mjs`; replace the relative
source import in `app.js` with a relative import to that extracted ESM file.
The Three.js and game-loop code stay unchanged.

## What to change next

- Replace the box meshes with your own Three.js models and keep rendering in
  this layer.
- Version the `encodePosition` payload before adding fields.
- Replace the relay with server-authoritative inputs/snapshots before treating
  movement as competitive gameplay.
