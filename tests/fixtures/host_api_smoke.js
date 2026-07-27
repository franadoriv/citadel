let lastActor = null;

function ascii(text) {
  const out = new Uint8Array(text.length);
  for (let i = 0; i < text.length; i += 1) {
    out[i] = text.charCodeAt(i);
  }
  return out;
}

function concat(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

citadel.on_message(1, (_ctx, body) => {
  citadel.broadcast(2, concat(ascii("hello:"), body), true);
});

citadel.on_join((ctx) => {
  lastActor = citadel.spawn_actor({ archetype: 7, x: 1, y: 2, z: 3 });
  citadel.send(ctx.sender, 3, "joined");
  citadel.log("joined", "info");
});

citadel.on_leave(() => {
  if (lastActor !== null) {
    citadel.despawn_actor(lastActor);
  }
});

citadel.on_tick(() => {
  if (lastActor !== null) {
    citadel.move_actor(lastActor, 4, 5, 6, 7, 8, 9);
    citadel.set_physics(lastActor, {
      gravity: 900, buoyancy: 300, drag: 0.5, radius: 30, height: 90,
      max_speed: 400, shape: "capsule",
    });
    citadel.apply_impulse(lastActor, 1, 2, 3);
    citadel.set_move_intent(lastActor, 4, 0, -5);
    const state = citadel.physics_state(lastActor);
    if (state !== null && state.position === undefined) throw new Error("bad physics state");
  }
});

citadel.on_rpc("ping", () => citadel.Reply.ok("pong"));

citadel.on_room_create(() => ({
  map: "Arena",
  mode: "duel",
  max_players: 2,
  open: true,
}));

citadel.on_room_join((_ctx, roomId) => roomId === 7n);
