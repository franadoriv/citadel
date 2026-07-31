# Citadel — standalone Windows server

This is a self-contained Citadel game server. Unzip it anywhere and run one
executable — it bootstraps its own database and migrations on
first start. No database server, no install step.

```
citadel/
├── citadel.exe        # the server
├── citadel.toml       # editable config (loaded automatically)
├── scripts/main.lua   # Lua game logic (hot-reloads on save)
├── data.sqlite        # auto-created + migrated on first run
└── maps/              # optional cooked level geometry
```

## 1. Run the server

Double-click `citadel.exe`, or from a terminal in this folder:

```powershell
.\citadel.exe
```

With no arguments the server discovers the `citadel.toml` next to it, creates
`data.sqlite`, applies migrations, loads `scripts/main.lua`, and starts
listening. Accounts and sessions persist to the single `data.sqlite` file and
survive restarts.

By default it binds to `127.0.0.1` (local machine only). To accept connections
from other machines, edit `citadel.toml` and change the `[http]` and
`[transport.*]` `bind` addresses to `0.0.0.0:<port>`, then restart.

## 2. Write your game logic

Edit `scripts/main.lua` to add server-authoritative behavior. Scripts handle
inbound messages, players joining/leaving, a server game loop, and RPC:

```lua
citadel.on_message(1, function(ctx, body)          -- kind 1 = position
  citadel.broadcast(2, string.pack(">I8", ctx.sender) .. body, true)
end)

citadel.on_join(function(ctx)
  citadel.log("player joined: " .. ctx.sender)
end)

citadel.on_rpc("ping", function(ctx, body)
  return "pong"
end)
```

With `runtime.hot_reload = true` (the shipped default) the running server
reloads the script live on save — no restart. A broken edit is rejected and the
previous script keeps serving. Delete `scripts/` and the server falls back to the
built-in relay.

## 3. Open the dashboard

With the server running, open a browser at:

- <http://127.0.0.1:7350/dashboard> — admin console (live status + navigable
  sections).
- <http://127.0.0.1:7350/status> — JSON node status (uptime, version, live
  connection/session/message gauges).
- <http://127.0.0.1:7350/health> — liveness check.

## 4. Connect a game client

Server archives intentionally contain no client SDKs. Download the dedicated
Unity, Unreal, Godot, or browser SDK archive from the same Citadel release, then
follow that SDK's import instructions. Point the client at this server's QUIC
address (`127.0.0.1:7351` by default).

## Configuration

Every key in `citadel.toml` is optional and documented inline. Common changes:

- `[database] url` — switch from `sqlite://data.sqlite` to
  `postgres://user:pass@host/db`, or remove the key to run non-durably
  in-memory.
- `[http] bind` / `[transport.*] bind` — bind addresses and ports.
- `[runtime] scripts_dir` / `tick_hz` / `hot_reload` — game runtime behavior.

Pass `--config <path>` to load a different file, or set `CITADEL_*` environment
variables to override individual values.
