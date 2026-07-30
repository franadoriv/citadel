-- Gameplay autoritativo para el simulador de estrés de bots.
--
-- Cada POSITION lleva: sequence(u32 BE), x(f32 BE), z(f32 BE),
-- sender_unix_ns(u64 BE). El servidor verifica el segmento contra el mapa,
-- confirma el resultado al emisor y difunde solamente posiciones aceptadas.

local map = require("map")

local KIND_POSITION = 200
local KIND_PEER_SNAPSHOT = 204
local KIND_POSITION_ACK = 202
local KIND_PLAYER_ID = 203

local MOVE_BLOCKED = 0
local MOVE_ACCEPTED = 1
local MOVE_CLAMPED = 2

local positions = {}
local participant_count = 0
local bad_packets = 0
local snapshot_elapsed = 0.0
local SNAPSHOT_INTERVAL = 0.25
local SNAPSHOT_ENTRIES_PER_CHUNK = 32

local function pack_ack(sequence, x, z, status)
  return string.pack(">I4ffB", sequence, x, z, status)
end

citadel.on_join(function(ctx)
  participant_count = participant_count + 1
  citadel.send(ctx.sender, KIND_PLAYER_ID, string.pack(">I8", ctx.sender), false)
  citadel.log("stress bot joined: " .. tostring(ctx.sender), "info")
end)

citadel.on_leave(function(ctx)
  participant_count = math.max(0, participant_count - 1)
  positions[ctx.sender] = nil
  citadel.log("stress bot left: " .. tostring(ctx.sender), "info")
end)

citadel.on_message(KIND_POSITION, function(ctx, body)
  if body == nil or #body ~= 20 then
    bad_packets = bad_packets + 1
    return
  end

  local ok, sequence, wanted_x, wanted_z, origin_ns = pcall(
    string.unpack, ">I4ffI8", body
  )
  if not ok or wanted_x ~= wanted_x or wanted_z ~= wanted_z then
    bad_packets = bad_packets + 1
    return
  end

  local x, z = map.clamp(wanted_x, wanted_z, map.player_radius)
  local status = (x == wanted_x and z == wanted_z) and MOVE_ACCEPTED or MOVE_CLAMPED
  local previous = positions[ctx.sender]

  -- No se comprueba un segmento para el spawn inicial: el punto de llegada sí
  -- debe estar libre. Las actualizaciones posteriores nunca pueden atravesar un
  -- bloque aunque intenten saltar varios cientos de unidades de una vez.
  if map.is_blocked(x, z, map.player_radius)
      or (previous ~= nil and map.segment_hits_obstacle(
        previous.x, previous.z, x, z, map.player_radius
      )) then
    local old_x, old_z = 0.0, 0.0
    if previous ~= nil then old_x, old_z = previous.x, previous.z end
    citadel.send(ctx.sender, KIND_POSITION_ACK,
      pack_ack(sequence, old_x, old_z, MOVE_BLOCKED), false)
    return
  end

  positions[ctx.sender] = {
    x = x,
    z = z,
    sequence = sequence,
    origin_ns = origin_ns,
  }
  citadel.send(ctx.sender, KIND_POSITION_ACK,
    pack_ack(sequence, x, z, status), false)

  -- Se reconstruye el cuerpo con la posición validada (no el objetivo que
  -- propuso el cliente) y se mantiene su timestamp para análisis posterior.
end)

local elapsed = 0.0
citadel.on_tick(function(dt)
  local step = dt or 0.0
  elapsed = elapsed + step
  snapshot_elapsed = snapshot_elapsed + step
  if snapshot_elapsed >= SNAPSHOT_INTERVAL then
    snapshot_elapsed = snapshot_elapsed - SNAPSHOT_INTERVAL
    -- Sort once per snapshot so every datagram chunk retains a stable key.
    local ids = {}
    for player_id, _ in pairs(positions) do
      ids[#ids + 1] = player_id
    end
    table.sort(ids)

    local chunk_index = 0
    for start = 1, #ids, SNAPSHOT_ENTRIES_PER_CHUNK do
      local entries = {}
      local finish = math.min(start + SNAPSHOT_ENTRIES_PER_CHUNK - 1, #ids)
      for index = start, finish do
        local player_id = ids[index]
        local position = positions[player_id]
        entries[#entries + 1] = string.pack(
          ">I8I4ffI8",
          player_id,
          position.sequence,
          position.x,
          position.z,
          position.origin_ns
        )
      end
      local body = string.pack(">I2I2", chunk_index, #entries) .. table.concat(entries)
      citadel.broadcast(KIND_PEER_SNAPSHOT, body, true)
      chunk_index = chunk_index + 1
    end
  end
  if elapsed >= 30.0 then
    elapsed = elapsed - 30.0
    citadel.log(
      "stress map online=" .. tostring(participant_count)
        .. " obstacles=" .. tostring(#map.obstacles)
        .. " malformed=" .. tostring(bad_packets),
      "info"
    )
  end
end)
