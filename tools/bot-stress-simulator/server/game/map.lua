-- Mapa cuadrado determinista compartido por server/main.lua y el cliente Rust.
-- Las unidades son arbitrarias de mundo: un área de 2000 x 2000 con 80 bloques
-- deja muchísimo espacio para 1000 jugadores, pero fuerza rutas con colisiones.

local map = {
  half_extent = 1000.0,
  player_radius = 4.0,
  obstacles = {},
}

for gx = -4, 4 do
  for gz = -4, 4 do
    if not (gx == 0 and gz == 0) then
      local jitter_x = ((gz + 4) % 3 - 1) * 24.0
      local jitter_z = ((gx + 4) % 3 - 1) * 18.0
      table.insert(map.obstacles, {
        x = gx * 190.0 + jitter_x,
        z = gz * 190.0 + jitter_z,
        hx = 18.0 + ((gx + 4) % 3) * 6.0,
        hz = 22.0 + ((gz + 4) % 3) * 5.0,
      })
    end
  end
end

function map.clamp(x, z, radius)
  local limit = map.half_extent - radius
  return math.max(-limit, math.min(limit, x)), math.max(-limit, math.min(limit, z))
end

function map.is_blocked(x, z, radius)
  if x - radius < -map.half_extent or x + radius > map.half_extent
      or z - radius < -map.half_extent or z + radius > map.half_extent then
    return true
  end

  for _, obstacle in ipairs(map.obstacles) do
    if x >= obstacle.x - obstacle.hx - radius
        and x <= obstacle.x + obstacle.hx + radius
        and z >= obstacle.z - obstacle.hz - radius
        and z <= obstacle.z + obstacle.hz + radius then
      return true
    end
  end
  return false
end

local function segment_hits_box(x0, z0, x1, z1, min_x, max_x, min_z, max_z)
  local dx = x1 - x0
  local dz = z1 - z0
  local t_min, t_max = 0.0, 1.0

  local function clip(start, delta, lo, hi)
    if math.abs(delta) < 0.00001 then
      return start >= lo and start <= hi, t_min, t_max
    end
    local a = (lo - start) / delta
    local b = (hi - start) / delta
    if a > b then a, b = b, a end
    local next_min = math.max(t_min, a)
    local next_max = math.min(t_max, b)
    return next_min <= next_max, next_min, next_max
  end

  local ok
  ok, t_min, t_max = clip(x0, dx, min_x, max_x)
  if not ok then return false end
  ok, t_min, t_max = clip(z0, dz, min_z, max_z)
  return ok
end

-- Evita que una actualización muy grande atraviese una pared aunque sus dos
-- extremos estén libres. Se prueba contra las cajas expandidas por el radio del
-- jugador, igual que la comprobación de punto del cliente.
function map.segment_hits_obstacle(x0, z0, x1, z1, radius)
  for _, obstacle in ipairs(map.obstacles) do
    if segment_hits_box(
        x0, z0, x1, z1,
        obstacle.x - obstacle.hx - radius,
        obstacle.x + obstacle.hx + radius,
        obstacle.z - obstacle.hz - radius,
        obstacle.z + obstacle.hz + radius
      ) then
      return true
    end
  end
  return false
end

return map
