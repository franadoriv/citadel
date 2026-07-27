-- game/systems/roster.lua — example of multi-file game logic via `require`.
--
-- `require("systems.roster")` resolves to this file: dotted module names map to
-- subdirectories under the scripts directory (here `game/`). A module returns a
-- value (this table) which is cached, so `require` runs the body once per VM.
-- Only files inside the scripts directory are reachable — there is no `io`/`os`/
-- `package` and no way to escape the root (see the runtime docs).

local Roster = {}

-- Authoritative set of connected participant ids -> true, plus a live count.
local players = {}
local count = 0

--- Record a participant as present. Returns the new online count.
function Roster.add(id)
  if not players[id] then
    players[id] = true
    count = count + 1
  end
  return count
end

--- Remove a participant. Returns the new online count.
function Roster.remove(id)
  if players[id] then
    players[id] = nil
    count = count - 1
  end
  return count
end

--- How many participants are currently online.
function Roster.count()
  return count
end

return Roster
