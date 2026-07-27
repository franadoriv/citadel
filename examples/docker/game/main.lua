-- This file is read through a bind mount. Save it to exercise Citadel's
-- polling hot reload: a valid edit logs another "loaded" line; an invalid Lua
-- edit leaves the previous runtime serving.
citadel.log("Docker Lua game logic loaded", "info")

local KIND_POSITION = 1
local KIND_PEER_POSITION = 2

citadel.on_message(KIND_POSITION, function(ctx, body)
  citadel.broadcast(KIND_PEER_POSITION, string.pack(">I8", ctx.sender) .. body, true)
end)
