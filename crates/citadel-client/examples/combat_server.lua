-- combat_server.lua — game logic for the combat benchmark.
--
-- The standalone server loads `<scripts_dir>/main.lua`; the benchmark Makefile
-- stages this tracked copy into `bin/benchmark/scripts/main.lua`.
--
-- Server-authoritative combat: HP, monster state, monster attacks, deaths, and
-- chat relay live on the server. Clients only propose player movement and hits.

-- Kinds 1..6 are core, 7..25 are reserved netcode; benchmark kinds use >= 100.
local KIND_POSITION      = 1    -- client -> server: x,z as f32 BE
local KIND_PEER_POSITION = 2    -- server -> clients: sender-tagged position

local KIND_HIT     = 100  -- client -> server: target(u64) + damage(f32)
local KIND_HEALTH  = 101  -- server -> clients: player hp
local KIND_DEATH   = 102  -- server -> clients: player death
local KIND_RESPAWN = 103  -- server -> clients: player respawn
local KIND_WELCOME = 104  -- server -> joiner: participant id
local KIND_CHAT_SEND = 105
local KIND_CHAT_RECV = 106

local KIND_MONSTER_SPAWN   = 107 -- id(u64) + x,z,hp,max(f32)
local KIND_MONSTER_STATE   = 108 -- id(u64) + x,z,hp,max(f32) + alive(u8)
local KIND_MONSTER_DEATH   = 109 -- monster_id(u64) + killer_id(u64)
local KIND_MONSTER_RESPAWN = 110 -- monster_id(u64) + x,z(f32)
local KIND_MONSTER_ATTACK  = 111 -- monster_id(u64) + target_id(u64) + damage(f32)
local KIND_TELEMETRY       = 112 -- 14 f32 benchmark stress snapshot
local KIND_TELEMETRY_PING  = 113 -- client -> server: opaque timestamp bytes
local KIND_TELEMETRY_PONG  = 114 -- server -> client: echoed timestamp bytes

local MAX_HP = 100.0
local MONSTER_MAX_HP = 180.0
local MAX_CHAT_BYTES = 180
local ARENA = 46.0
local PLAYER_RADIUS = 1.8
local MONSTER_RADIUS = 2.3
local MONSTER_SPEED = 10.0
local MONSTER_ATTACK_RANGE = 5.8
local MONSTER_ATTACK_CD = 1.35
local MONSTER_RESPAWN_SECONDS = 5.0
local MONSTER_RETARGET_SECONDS = 1.4
local STATE_BROADCAST_SECONDS = 0.10
local TELEMETRY_BROADCAST_SECONDS = 0.50

-- Keep this list in sync with combat_viz.html. x/z are center, hx/hz are half
-- extents. The benchmark uses these as a lightweight stand-in until core
-- navmesh pathfinding lands.
local OBSTACLES = {
    { x =  0.0, z =   0.0, hx = 7.0, hz = 4.0 },
    { x = -23.0, z = -12.0, hx = 4.0, hz = 10.0 },
    { x =  23.0, z =  12.0, hx = 4.0, hz = 10.0 },
    { x = -10.0, z =  25.0, hx = 9.0, hz = 3.0 },
    { x =  12.0, z = -26.0, hx = 9.0, hz = 3.0 },
}

local MONSTER_SPAWNS = {
    { id = 1000001, x = -34.0, z = -32.0 },
    { id = 1000002, x =  34.0, z = -32.0 },
    { id = 1000003, x = -34.0, z =  32.0 },
    { id = 1000004, x =  34.0, z =  32.0 },
    { id = 1000005, x =   0.0, z = -38.0 },
    { id = 1000006, x =   0.0, z =  38.0 },
    { id = 1000007, x = -38.0, z =   4.0 },
    { id = 1000008, x =  38.0, z =  -4.0 },
}

local hp = {}          -- player_id -> hp
local player_pos = {}  -- player_id -> {x,z}
local monsters = {}    -- monster_id -> monster
local monster_order = {}
local state_accum = 0.0
local telemetry_accum = 0.0
local participant_count = 0
local tick_ms_ema = 0.0
local metrics = {
    in_msg = 0,
    out_msg = 0,
    in_bytes = 0,
    out_bytes = 0,
    hit = 0,
    monster_attack = 0,
    death = 0,
    chat = 0,
    error = 0,
}
local last_metrics = {
    in_msg = 0,
    out_msg = 0,
    in_bytes = 0,
    out_bytes = 0,
    hit = 0,
    monster_attack = 0,
    death = 0,
    chat = 0,
    error = 0,
}

local function clamp(v, lo, hi)
    if v < lo then return lo end
    if v > hi then return hi end
    return v
end

local function dist2(ax, az, bx, bz)
    local dx = bx - ax
    local dz = bz - az
    return dx * dx + dz * dz
end

local function normalize(x, z)
    local len = math.sqrt(x * x + z * z)
    if len < 0.0001 then return 0.0, 0.0, 0.0 end
    return x / len, z / len, len
end

local function body_len(body)
    if body == nil then return 0 end
    return #body
end

local function note_in(body)
    metrics.in_msg = metrics.in_msg + 1
    metrics.in_bytes = metrics.in_bytes + body_len(body)
end

local function note_error()
    metrics.error = metrics.error + 1
end

local function note_out(count, body)
    if count <= 0 then return end
    metrics.out_msg = metrics.out_msg + count
    metrics.out_bytes = metrics.out_bytes + count * body_len(body)
end

local function send(to, kind, body, reliable)
    citadel.send(to, kind, body, reliable)
    note_out(1, body)
end

local function broadcast(kind, body, reliable)
    citadel.broadcast(kind, body, reliable)
    note_out(participant_count, body)
end

local function alive_player_count()
    local n = 0
    for _, cur in pairs(hp) do
        if cur > 0 then n = n + 1 end
    end
    return n
end

local function alive_monster_count()
    local n = 0
    for _, id in ipairs(monster_order) do
        if monsters[id].alive then n = n + 1 end
    end
    return n
end

local function rate(name, period)
    local current = metrics[name]
    local previous = last_metrics[name] or 0
    last_metrics[name] = current
    return (current - previous) / period
end

local function send_telemetry(period)
    if participant_count <= 0 then return end
    local safe_period = math.max(period, 0.001)
    local in_msg_sec = rate("in_msg", safe_period)
    local out_msg_sec = rate("out_msg", safe_period)
    local in_kb_sec = rate("in_bytes", safe_period) / 1024.0
    local out_kb_sec = rate("out_bytes", safe_period) / 1024.0
    local hit_sec = rate("hit", safe_period)
    local monster_attack_sec = rate("monster_attack", safe_period)
    local death_sec = rate("death", safe_period)
    local chat_sec = rate("chat", safe_period)
    local error_sec = rate("error", safe_period)
    local body = string.pack(
        ">ffffffffffffff",
        participant_count,
        alive_player_count(),
        alive_monster_count(),
        in_msg_sec,
        out_msg_sec,
        in_kb_sec,
        out_kb_sec,
        hit_sec,
        monster_attack_sec,
        death_sec,
        chat_sec,
        metrics.error,
        error_sec,
        tick_ms_ema
    )
    broadcast(KIND_TELEMETRY, body, false)
end

local function inside_obstacle(x, z, radius)
    for _, o in ipairs(OBSTACLES) do
        if x >= o.x - o.hx - radius and x <= o.x + o.hx + radius
            and z >= o.z - o.hz - radius and z <= o.z + o.hz + radius then
            return true, o
        end
    end
    return false, nil
end

local function choose_step(monster, target, dt)
    local tx, tz = target.x, target.z
    local dir_x, dir_z = normalize(tx - monster.x, tz - monster.z)
    if dir_x == 0.0 and dir_z == 0.0 then return monster.x, monster.z, 0.0, 0.0 end

    -- Try cheap tangent alternatives and pick the one that makes the most
    -- progress to the target. This is not a navmesh; each candidate only checks
    -- the next short step against expanded AABBs so 30 browser bots do not push
    -- the Lua tick over budget.
    local candidates = {
        { dir_x, dir_z },
        { -dir_z, dir_x },
        { dir_z, -dir_x },
        { dir_x * 0.45 - dir_z * 0.90, dir_z * 0.45 + dir_x * 0.90 },
        { dir_x * 0.45 + dir_z * 0.90, dir_z * 0.45 - dir_x * 0.90 },
    }

    local best_x, best_z = dir_x, dir_z
    local best_score = math.huge
    local step = MONSTER_SPEED * dt
    for _, c in ipairs(candidates) do
        local cx, cz = normalize(c[1], c[2])
        local nx = clamp(monster.x + cx * step, -ARENA, ARENA)
        local nz = clamp(monster.z + cz * step, -ARENA, ARENA)
        if not inside_obstacle(nx, nz, MONSTER_RADIUS) then
            local score = dist2(nx, nz, tx, tz)
            if score < best_score then
                best_score = score
                best_x, best_z = cx, cz
            end
        end
    end

    local nx = clamp(monster.x + best_x * step, -ARENA, ARENA)
    local nz = clamp(monster.z + best_z * step, -ARENA, ARENA)
    if inside_obstacle(nx, nz, MONSTER_RADIUS) then
        return monster.x, monster.z, 0.0, 0.0
    end
    return nx, nz, best_x * MONSTER_SPEED, best_z * MONSTER_SPEED
end

local function init_monsters()
    for _, spawn in ipairs(MONSTER_SPAWNS) do
        local m = {
            id = spawn.id,
            spawn_x = spawn.x,
            spawn_z = spawn.z,
            x = spawn.x,
            z = spawn.z,
            hp = MONSTER_MAX_HP,
            alive = true,
            target = nil,
            retarget_in = 0.0,
            attack_cd = 0.25,
            respawn_in = 0.0,
        }
        monsters[m.id] = m
        table.insert(monster_order, m.id)
    end
end

local function monster_state_body(m)
    return string.pack(">I8ffffB", m.id, m.x, m.z, m.hp, MONSTER_MAX_HP, m.alive and 1 or 0)
end

local function send_monster_spawn(to, m)
    local body = string.pack(">I8ffff", m.id, m.x, m.z, m.hp, MONSTER_MAX_HP)
    if to then
        send(to, KIND_MONSTER_SPAWN, body, false)
        send(to, KIND_MONSTER_STATE, monster_state_body(m), false)
    else
        broadcast(KIND_MONSTER_SPAWN, body, false)
        broadcast(KIND_MONSTER_STATE, monster_state_body(m), false)
    end
end

local function broadcast_monster_state(m)
    broadcast(KIND_MONSTER_STATE, monster_state_body(m), false)
end

local function send_all_monsters(to)
    for _, id in ipairs(monster_order) do
        send_monster_spawn(to, monsters[id])
    end
end

local function announce_health(who)
    local msg = string.pack(">I8ff", who, hp[who], MAX_HP)
    broadcast(KIND_HEALTH, msg, false)
    send(who, KIND_HEALTH, msg, false)
end

local function damage_player(target, damage, attacker)
    local cur = hp[target]
    if cur == nil or cur <= 0 then return end
    cur = cur - damage
    if cur <= 0 then
        hp[target] = 0
        announce_health(target)

        local dmsg = string.pack(">I8I8", target, attacker)
        metrics.death = metrics.death + 1
        broadcast(KIND_DEATH, dmsg, false)
        send(target, KIND_DEATH, dmsg, false)

        hp[target] = MAX_HP
        local rmsg = string.pack(">I8", target)
        broadcast(KIND_RESPAWN, rmsg, false)
        send(target, KIND_RESPAWN, rmsg, false)
        announce_health(target)
    else
        hp[target] = cur
        announce_health(target)
    end
end

local function damage_monster(monster, damage, attacker)
    if not monster.alive or damage <= 0 then return end
    monster.hp = monster.hp - damage
    if monster.hp <= 0 then
        monster.hp = 0
        monster.alive = false
        monster.target = nil
        monster.respawn_in = MONSTER_RESPAWN_SECONDS
        broadcast_monster_state(monster)
        local body = string.pack(">I8I8", monster.id, attacker)
        metrics.death = metrics.death + 1
        broadcast(KIND_MONSTER_DEATH, body, false)
        citadel.log(tostring(attacker) .. " killed monster " .. tostring(monster.id), "info")
    else
        broadcast_monster_state(monster)
    end
end

local function choose_target(monster)
    if monster.target and hp[monster.target] and hp[monster.target] > 0 and player_pos[monster.target] then
        local p = player_pos[monster.target]
        if dist2(monster.x, monster.z, p.x, p.z) < 70.0 * 70.0 and monster.retarget_in > 0 then
            return monster.target
        end
    end

    local best, best_d = nil, math.huge
    for id, p in pairs(player_pos) do
        if hp[id] and hp[id] > 0 then
            local d = dist2(monster.x, monster.z, p.x, p.z)
            if d < best_d then
                best_d = d
                best = id
            end
        end
    end
    monster.target = best
    monster.retarget_in = MONSTER_RETARGET_SECONDS
    return best
end

init_monsters()
citadel.log("combat benchmark game logic loaded", "info")

citadel.on_join(function(ctx)
    if hp[ctx.sender] == nil then
        participant_count = participant_count + 1
    end
    hp[ctx.sender] = MAX_HP
    player_pos[ctx.sender] = { x = 0.0, z = 0.0 }
    send(ctx.sender, KIND_WELCOME, string.pack(">I8", ctx.sender), false)
    announce_health(ctx.sender)
    send_all_monsters(ctx.sender)
    citadel.log("join " .. tostring(ctx.sender) .. " hp=" .. MAX_HP, "info")
end)

citadel.on_leave(function(ctx)
    if hp[ctx.sender] ~= nil then
        participant_count = math.max(0, participant_count - 1)
    end
    hp[ctx.sender] = nil
    player_pos[ctx.sender] = nil
    citadel.log("leave " .. tostring(ctx.sender), "info")
end)

citadel.on_message(KIND_POSITION, function(ctx, body)
    note_in(body)
    local ok, x, z = pcall(string.unpack, ">ff", body)
    if ok then
        player_pos[ctx.sender] = { x = x, z = z }
    else
        note_error()
        return
    end
    local tagged = string.pack(">I8", ctx.sender) .. body
    broadcast(KIND_PEER_POSITION, tagged, true)
end)

citadel.on_message(KIND_HIT, function(ctx, body)
    note_in(body)
    local ok, target, damage = pcall(string.unpack, ">I8f", body)
    if not ok then note_error(); return end
    if damage <= 0 or damage > 1000 then note_error(); return end
    metrics.hit = metrics.hit + 1

    local monster = monsters[target]
    if monster ~= nil then
        damage_monster(monster, damage, ctx.sender)
        return
    end
    damage_player(target, damage, ctx.sender)
end)

citadel.on_message(KIND_CHAT_SEND, function(ctx, body)
    note_in(body)
    if body == nil or #body == 0 then note_error(); return end
    local text = string.sub(body, 1, MAX_CHAT_BYTES)
    text = string.gsub(text, "[%z\1-\8\11\12\14-\31]", "")
    if #text == 0 then note_error(); return end
    metrics.chat = metrics.chat + 1

    local tagged = string.pack(">I8", ctx.sender) .. text
    broadcast(KIND_CHAT_RECV, tagged, false)
    send(ctx.sender, KIND_CHAT_RECV, tagged, false)
end)

citadel.on_message(KIND_TELEMETRY_PING, function(ctx, body)
    note_in(body)
    if body == nil or #body < 8 then note_error(); return end
    send(ctx.sender, KIND_TELEMETRY_PONG, body, false)
end)

citadel.on_tick(function(dt)
    dt = math.min(dt or 0.05, 0.10)
    tick_ms_ema = tick_ms_ema == 0.0 and (dt * 1000.0) or (tick_ms_ema * 0.88 + dt * 1000.0 * 0.12)
    state_accum = state_accum + dt
    telemetry_accum = telemetry_accum + dt

    for _, id in ipairs(monster_order) do
        local m = monsters[id]
        if not m.alive then
            m.respawn_in = m.respawn_in - dt
            if m.respawn_in <= 0 then
                m.x = m.spawn_x
                m.z = m.spawn_z
                m.hp = MONSTER_MAX_HP
                m.alive = true
                m.attack_cd = 0.5
                m.retarget_in = 0.0
                local body = string.pack(">I8ff", m.id, m.x, m.z)
                broadcast(KIND_MONSTER_RESPAWN, body, false)
                send_monster_spawn(nil, m)
            end
        else
            m.retarget_in = m.retarget_in - dt
            m.attack_cd = math.max(0.0, m.attack_cd - dt)

            local target_id = choose_target(m)
            if target_id and player_pos[target_id] then
                local target = player_pos[target_id]
                local d = math.sqrt(dist2(m.x, m.z, target.x, target.z))
                if d > MONSTER_ATTACK_RANGE then
                    local nx, nz = choose_step(m, target, dt)
                    m.x = nx
                    m.z = nz
                elseif m.attack_cd <= 0.0 then
                    m.attack_cd = MONSTER_ATTACK_CD
                    local damage = 10.0 + math.random() * 8.0
                    local body = string.pack(">I8I8f", m.id, target_id, damage)
                    metrics.monster_attack = metrics.monster_attack + 1
                    broadcast(KIND_MONSTER_ATTACK, body, false)
                    send(target_id, KIND_MONSTER_ATTACK, body, false)
                    damage_player(target_id, damage, m.id)
                end
            end
        end

        if state_accum >= STATE_BROADCAST_SECONDS then
            broadcast_monster_state(m)
        end
    end

    if state_accum >= STATE_BROADCAST_SECONDS then
        state_accum = 0.0
    end
    if telemetry_accum >= TELEMETRY_BROADCAST_SECONDS then
        local period = telemetry_accum
        telemetry_accum = 0.0
        send_telemetry(period)
    end
end)

citadel.on_rpc("ping", function(ctx, body)
    return "pong"
end)
