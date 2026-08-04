-- main.lua — your Citadel game logic.
--
-- Citadel loads this file on startup (runtime.scripts_dir in citadel.toml) and
-- routes realtime traffic to the handlers you register below. With hot_reload on,
-- just save this file to reload live — no server restart.
-- Docs: https://citadel.dev/reference/embedded-lua-runtime  (or website/src/content/docs/reference/server-sdk/lua-runtime.md)
--
-- Host API (available as the global `citadel`):
--   citadel.log(message, level)                     -- level: "info" | "warn" | "error"
--   citadel.broadcast(kind, body, unreliable)       -- send to every participant
--   citadel.send(session, kind, body, unreliable)   -- send to one participant
-- Handlers receive `ctx`: ctx.sender (participant id), ctx.user_id (may be nil), ctx.kind.
-- Bodies are raw bytes — use string.pack / string.unpack for binary layouts.

-- Wire message kinds. These must match your client. Kinds 1..6 are used by the
-- built-in demos; pick your own numbers for your game.
local KIND_POSITION      = 1
local KIND_PEER_POSITION = 2

citadel.log("game logic loaded", "info")

-- A player connected.
citadel.on_join(function(ctx)
    citadel.log("join " .. tostring(ctx.sender), "info")
end)

-- A player disconnected.
citadel.on_leave(function(ctx)
    citadel.log("leave " .. tostring(ctx.sender), "info")
end)

-- An inbound message. This example relays a player's position to everyone else,
-- tagged with the sender id so clients know who moved.
citadel.on_message(KIND_POSITION, function(ctx, body)
    local tagged = string.pack(">I8", ctx.sender) .. body
    citadel.broadcast(KIND_PEER_POSITION, tagged, true) -- unreliable
end)

-- A request/response RPC, called by name from a client. Return a string reply
-- (may contain binary bytes).
citadel.on_rpc("ping", function(ctx, body)
    return "pong"
end)

-- Fixed-rate server tick (runtime.tick_hz). `dt` is seconds since the last tick.
citadel.on_tick(function(dt)
    -- your simulation step goes here
end)
