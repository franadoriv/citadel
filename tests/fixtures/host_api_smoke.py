import citadel

last_actor = None


@citadel.on_message(1)
def handle_message(ctx, body):
    citadel.broadcast(2, b"hello:" + body, unreliable=True)


@citadel.on_join
def handle_join(ctx):
    global last_actor
    last_actor = citadel.spawn_actor({"archetype": 7, "x": 1, "y": 2, "z": 3})
    citadel.send(ctx.sender, 3, b"joined")
    citadel.log("joined", "info")


@citadel.on_leave
def handle_leave(ctx):
    if last_actor is not None:
        citadel.despawn_actor(last_actor)


@citadel.on_tick
def handle_tick(dt):
    if last_actor is not None:
        citadel.move_actor(last_actor, 4, 5, 6, 7, 8, 9)
        citadel.set_physics(last_actor, {"gravity": 900, "buoyancy": 300, "drag": 0.5,
                                         "radius": 30, "height": 90, "max_speed": 400,
                                         "shape": "capsule"})
        citadel.apply_impulse(last_actor, 1, 2, 3)
        citadel.set_move_intent(last_actor, 4, 0, -5)
        state = citadel.physics_state(last_actor)
        if state is not None and "position" not in state:
            raise RuntimeError("bad physics state")


@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")


@citadel.on_room_create
def room_create(ctx, params):
    return {"map": "Arena", "mode": "duel", "max_players": 2, "open": True}


@citadel.on_room_join
def room_join(ctx, room_id):
    return room_id == 7
