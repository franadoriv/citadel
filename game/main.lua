-- Citadel game logic (embedded Lua runtime, ).
--
-- This is the sample "game" that ships with the repo. It reproduces the built-in
-- position relay entirely in script: when a client sends a POSITION update, the
-- server tags it with the sender's participant id and broadcasts it to everyone
-- else as a PEER_POSITION. Delete this file (or the whole `game/` folder) and the
-- node falls back to the identical built-in relay.
--
-- Host API available to scripts:
--   citadel.on_message(kind, function(ctx, body) ... end)
--       Register a handler for an inbound wire kind (a u16). `ctx.sender` is the
--       sending participant id; `ctx.kind` is the message kind. `body` is the raw
--       payload as a Lua (binary-safe) string.
--   citadel.on_join(function(ctx) ... end) / citadel.on_leave(function(ctx) ... end)
--       Run when a participant connects/disconnects. `ctx.sender` is the
--       participant id. Handlers may broadcast/send (e.g. spawn/despawn peers).
--   citadel.on_tick(function(dt) ... end)
--       Server game loop. Runs at `runtime.tick_hz` (0 = disabled). `dt` is the
--       nominal seconds per tick. Use it for authoritative simulation.
--   citadel.broadcast(kind, body [, unreliable])
--       Send `body` (wire `kind`) to every connected participant except the
--       sender. Pass `true` for best-effort/unreliable delivery on transports
--       that support it (QUIC/WebTransport datagrams); WebSocket is reliable.
--   citadel.send(session, kind, body [, unreliable])
--       Send `body` to a single participant id.
--   citadel.on_rpc(method, function(ctx, body) return reply end)
--       Register a request/response RPC handler for `method` (a string). The
--       client sends a KIND_RPC_REQUEST (method + payload + correlation id); this
--       handler runs and MUST `return` a reply string (binary-safe). The server
--       sends exactly one correlated KIND_RPC_RESPONSE back to that caller only.
--       `ctx.sender` is the caller; `ctx.method` is the method name. Raising an
--       error (or an unknown method) yields a status!=0 error response to the
--       caller with a generic message; it never crashes the node.
--   citadel.log(message [, level])
--       Structured server-side log tagged as script output. `level` is an
--       optional string: "trace" | "debug" | "info" (default) | "warn" | "error".
--   require("module") / require("dir.module")
--       Load another Lua file from the scripts directory: dotted names map to
--       subdirectories (`require("systems.roster")` -> `game/systems/roster.lua`).
--       Modules run once and their return value is cached; cycles and paths that
--       escape the scripts directory are rejected. There is no `io`/`os`/`package`.
--
-- Per-game state lives in this script's Lua globals: there is a single shared VM
-- and every handler runs serialized under one lock, so plain globals are a safe,
-- simple place to keep authoritative state. Bigger games split logic across files
-- with `require` (see `systems/roster.lua`, loaded below).

-- Wire kinds shared with the clients/demos (see `crates/citadel-wire`).
local KIND_POSITION = 1      -- client -> server: "my position update"
local KIND_PEER_POSITION = 2 -- server -> client: a peer's position, sender-tagged
local KIND_PLAYER_JOINED = 10 -- server -> client: a peer joined (body: u64 id)
local KIND_PLAYER_LEFT = 11   -- server -> client: a peer left (body: u64 id)

-- Multi-file game logic: the connected-player roster lives in its own module.
local roster = require("systems.roster")

-- Relay: prepend the 8-byte big-endian sender id, then broadcast to peers.
citadel.on_message(KIND_POSITION, function(ctx, body)
  local tagged = string.pack(">I8", ctx.sender) .. body
  citadel.broadcast(KIND_PEER_POSITION, tagged, true)
end)

-- Lifecycle: track the roster (via the required module) and tell existing peers
-- to spawn/despawn.
citadel.on_join(function(ctx)
  roster.add(ctx.sender)
  citadel.log("player joined: " .. ctx.sender)
  citadel.broadcast(KIND_PLAYER_JOINED, string.pack(">I8", ctx.sender), false)
end)

citadel.on_leave(function(ctx)
  roster.remove(ctx.sender)
  citadel.log("player left: " .. ctx.sender)
  citadel.broadcast(KIND_PLAYER_LEFT, string.pack(">I8", ctx.sender), false)
end)

-- RPC handlers (request/response). A client sends a KIND_RPC_REQUEST naming one
-- of these methods; the handler's return value is sent back to that caller only,
-- correlated by the client's request id. These are examples for 's
-- client helpers to exercise.

-- Liveness check: reply "pong" to any "ping".
citadel.on_rpc("ping", function(ctx, body)
  return "pong"
end)

-- Echo: return the request payload unchanged (binary-safe round-trip).
citadel.on_rpc("echo", function(ctx, body)
  return body
end)

-- Add: body is two big-endian u32s; reply is their (wrapping) u32 sum. Shows a
-- typed request/response using string.pack/unpack.
citadel.on_rpc("add", function(ctx, body)
  local a, b = string.unpack(">I4I4", body)
  return string.pack(">I4", (a + b) & 0xFFFFFFFF)
end)

-- Server game loop (only runs when runtime.tick_hz > 0). Trivial demo: it
-- accumulates elapsed time and logs a heartbeat roughly once per second so the
-- authoritative loop is observable without spamming clients every tick.
local elapsed = 0.0
citadel.on_tick(function(dt)
  elapsed = elapsed + dt
  if elapsed >= 1.0 then
    elapsed = elapsed - 1.0
    citadel.log("tick heartbeat: " .. roster.count() .. " player(s) online", "debug")
  end
end)
