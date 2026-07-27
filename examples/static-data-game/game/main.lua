-- Shared collision constants, loaded once while Citadel initializes this VM.
-- The local tables stay in memory and are used by authoritative game logic;
-- message and tick handlers never read the filesystem.
local collision = citadel.static_data.load_json("gameplay/collision.json")

local knight = collision.characters.knight.hitbox
local balloon = collision.characters.balloon.hitbox
local melee_reach_cm = collision.rules.melee_reach_cm
local padding_cm = collision.rules.collision_padding_cm

-- A game would get these positions from its authoritative actor state, never
-- from a client-supplied hit result. This helper is deliberately deterministic
-- so every client can consume the same JSON for cosmetics while the server is
-- the only authority that accepts/rejects a hit.
local function hit_is_valid(distance_cm, target_is_balloon)
  local target = target_is_balloon and balloon or knight
  local allowed = melee_reach_cm + knight.radius_cm + target.radius_cm + padding_cm
  return distance_cm <= allowed
end

citadel.on_rpc("collision_volume", function(_, body)
  -- Clients can use the same versioned JSON for presentation, but this response
  -- only reports the server's already-loaded constants for diagnostics.
  return string.format(
    "knight=%d balloon=%d balloon_offset_y=%d",
    knight.radius_cm,
    balloon.radius_cm,
    balloon.offset_y_cm
  )
end)

citadel.on_message(80, function(ctx, body)
  -- Example placeholder: the real handler would decode an input command,
  -- calculate distance from authoritative transforms, and call hit_is_valid.
  -- A client never gets to declare that its own hit connected.
  local distance_cm = #body
  if hit_is_valid(distance_cm, false) then
    citadel.broadcast(81, "authoritative_hit", false)
  end
end)
