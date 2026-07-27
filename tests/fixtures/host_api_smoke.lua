local last_actor = nil

citadel.on_message(1, function(ctx, body)
    citadel.broadcast(2, "hello:" .. body, true)
end)

citadel.on_join(function(ctx)
    last_actor = citadel.spawn_actor({ archetype = 7, x = 1, y = 2, z = 3 })
    citadel.send(ctx.sender, 3, "joined")
    citadel.log("joined", "info")
end)

citadel.on_leave(function(ctx)
    if last_actor ~= nil then
        citadel.despawn_actor(last_actor)
    end
end)

citadel.on_tick(function(dt)
    if last_actor ~= nil then
        citadel.move_actor(last_actor, 4, 5, 6, 7, 8, 9)
        citadel.set_physics(last_actor, { gravity = 900, buoyancy = 300, drag = 0.5,
                                          radius = 30, height = 90, max_speed = 400,
                                          shape = "capsule" })
        citadel.apply_impulse(last_actor, 1, 2, 3)
        citadel.set_move_intent(last_actor, 4, 0, -5)
        local state = citadel.physics_state(last_actor)
        if state ~= nil and state.position == nil then error("bad physics state") end
    end
end)

citadel.on_rpc("ping", function(ctx, body)
    return "pong"
end)

citadel.on_room_create(function(ctx, params)
    return { map = "Arena", mode = "duel", max_players = 2, open = true }
end)

citadel.on_room_join(function(ctx, room_id)
    return room_id == 7
end)
