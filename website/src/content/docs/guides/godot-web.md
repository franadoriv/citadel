---
title: Export a Godot game for the web
description: Connect a Godot 4 Web export to Citadel with the pure-GDScript WebSocket SDK.
---

This guide uses Citadel's browser-safe Godot transport. It avoids the desktop
GDExtension completely and works through Godot's built-in `WebSocketPeer`.

1. Enable Citadel's WebSocket listener and publish it at a browser-reachable
   hostname. For an HTTPS game page, terminate TLS and expose a matching
   `wss://game.example.com:7352/` endpoint; browsers reject an insecure
   `ws://` endpoint from HTTPS.
2. Build or download `citadel-client-godot-web-v<version>.zip`, then extract it
   at the game's `res://` root. It installs
   `addons/citadel/{protocol,client,web_client,rooms}.gd` and includes a
   distributable `web/` verification export with matched `.html`, `.js`, `.pck`,
   and `.wasm` files. Do not copy a `.gdextension` or native `bin/` folder into
   the Web-specific export. From a Citadel checkout, build it with
   `GODOT_BIN=/path/to/godot make package-client-godot-web` (or set `GODOT_BIN`
   to `Godot.exe` and run `./make.ps1 package-client-godot-web` on Windows).
3. Instantiate `CitadelWebClient` and drive its non-blocking `pump` method
   from `_process`. Wait for `is_open`, then repeat the authentication helper
   until it returns `OK`.
4. Inspect the returned handshake `status` before sending gameplay messages.
   Then use the normal `send`/`poll` loop and pass room events to
   `CitadelRooms`.
5. In the Godot Export dialog, add a **Web** preset and export the project.
   Serve the `.html`, `.wasm`, `.pck`, and JavaScript files over HTTP(S); do
   not open the HTML directly from disk. Keep their generated names together and
   configure the server to send `.wasm` as `application/wasm`.
6. Verify the distributable application against a real local Citadel server.
   The archive's `web/` directory contains `citadel-e2e.toml` and
   `serve_web.py`. With a Citadel checkout available, run:

   ```bash
   cargo build --bin citadel
   target/debug/citadel --yes --config citadel-e2e.toml serve
   python3 serve_web.py --port 18080
   ```

   Then open
   `http://127.0.0.1:18080/index.html?citadel_ws=ws://127.0.0.1:17532/` in a
   WebAssembly/WebGL2-capable browser. The app opens two browser clients,
   guest-authenticates, checks the Citadel position relay, and closes both
   connections. `ws://` is only valid for this loopback proof; deployed HTTPS
   pages must use a browser-trusted `wss://` endpoint.

The transport supports all reliable framed operations: authentication,
correlated generic RPC, named rooms, notifications, chat events, and relayed
messages. Authentication remains the first Citadel envelope, and gameplay sends
are held until its reply succeeds. It does not support QUIC, unreliable
snapshots, transform-sync, or native NetworkPeer codecs. See the [Godot Web SDK reference](/reference/client-sdk/godot-web/)
for each method and error result.

## Verification checklist

- The browser developer console has no mixed-content or certificate error.
- `is_open` becomes true before `authenticate_*` reports `OK`.
- The auth dictionary contains a non-rejected status.
- A reliable RPC or room join receives its expected envelope in the single
  application-owned poll loop.
- CI verifies both the shared GDScript transport against a deterministic local
  WebSocket fixture and the actual browser-loaded Godot `.wasm` application
  against a running Citadel server; perform the final check from the deployed
  HTTPS origin with its production `wss://` certificate and `application/wasm`
  MIME type.
