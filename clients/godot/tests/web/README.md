# Citadel Godot Web SDK verification build

This directory is part of `citadel-client-godot-web-v<version>.zip`. It is a
runnable Godot Web export that embeds the same portable `addons/citadel/`
GDScript SDK shipped at the archive root.

Serve this directory over HTTP(S); do not open `index.html` from disk. Keep
`index.html`, `index.js`, `index.pck`, and `index.wasm` together with their
names unchanged. Serve `index.wasm` as `application/wasm` and use HTTPS plus a
trusted `wss://` Citadel endpoint for a real browser deployment.

This is a real Godot Web verification application, not a source-only sample.
When opened with `?citadel_ws=<endpoint>`, it opens two `CitadelWebClient`
connections in the browser, guest-authenticates both, sends a position through
the compatible Citadel listener, requires the second client to receive its
`KIND_PEER_POSITION` relay, then closes both sockets. Success is visible in the
page and as `data-citadel-e2e="pass"` on the HTML element.

For the same loopback proof used in CI, from this `web/` directory start a
Citadel checkout in one terminal and this MIME-correct static server in another:

```bash
cargo build --bin citadel
target/debug/citadel --yes --config citadel-e2e.toml serve
python3 serve_web.py --port 18080
```

Open <http://127.0.0.1:18080/index.html?citadel_ws=ws://127.0.0.1:17532/> in a
WebAssembly/WebGL2-capable browser. `ws://` is only suitable for this localhost
test; use HTTPS and a trusted `wss://` endpoint for a deployment.

Your game should copy the root `addons/` directory into its own `res://` project
and export its own Web build.
