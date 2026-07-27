# Citadel game logic (embedded Python runtime, ).
#
# This is the Python version of the repo's sample game. It reproduces the
# built-in position relay entirely in script: when a client sends a POSITION
# update, the server tags it with the sender's participant id and broadcasts it
# to everyone else as a PEER_POSITION.
#
# This file intentionally lives under examples/python-game instead of game/.
# The default scripts_dir is ./game and currently contains game/main.lua; adding
# main.py there would make runtime autodetection reject the directory until the
# operator chooses a language explicitly.
#
# Host API available to scripts:
#   import citadel
#       Import the embedded host module registered by Citadel.
#   @citadel.on_message(kind) / citadel.on_message(kind, handler)
#       Register a handler for an inbound wire kind (a u16). ctx.sender is the
#       sending participant id; ctx.kind is the message kind. body is raw bytes.
#   @citadel.on_join / @citadel.on_leave
#       Run when a participant connects/disconnects. ctx.sender is the
#       participant id. Handlers may broadcast/send spawn/despawn notifications.
#   @citadel.on_tick
#       Server game loop. Runs at runtime.tick_hz (0 = disabled). dt is the
#       nominal seconds per tick.
#   citadel.broadcast(kind, body, unreliable=False)
#       Send body (bytes or str) to every connected participant except the
#       sender. Pass True for best-effort/unreliable delivery on transports that
#       support it.
#   citadel.send(session, kind, body, unreliable=False)
#       Send body to a single participant id.
#   @citadel.on_rpc(method)
#       Register a request/response RPC handler. Return bytes, str, or
#       citadel.Reply.ok(body). Return citadel.Reply.err(message) for an RPC
#       error response.
#   citadel.log(message, level="info")
#       Structured server-side log tagged as script output.
#   import local modules
#       The scripts directory is added to sys.path, so local modules can be
#       imported normally (see systems/roster.py below).
#
# Per-game state lives in module globals. Citadel calls into one embedded
# Python runtime, and handler execution is serialized through the runtime lock,
# so simple globals are a safe place to keep small authoritative state.

import citadel

from systems import roster


# Wire kinds shared with the clients/demos (see crates/citadel-wire).
KIND_POSITION = 1       # client -> server: "my position update"
KIND_PEER_POSITION = 2  # server -> client: a peer's position, sender-tagged
KIND_PLAYER_JOINED = 10 # server -> client: a peer joined (body: u64 id)
KIND_PLAYER_LEFT = 11   # server -> client: a peer left (body: u64 id)


@citadel.on_message(KIND_POSITION)
def relay_position(ctx, body):
    tagged = int(ctx.sender).to_bytes(8, "big") + body
    citadel.broadcast(KIND_PEER_POSITION, tagged, unreliable=True)


@citadel.on_join
def joined(ctx):
    roster.add(ctx.sender)
    citadel.log(f"player joined: {ctx.sender}")
    citadel.broadcast(
        KIND_PLAYER_JOINED,
        int(ctx.sender).to_bytes(8, "big"),
        unreliable=False,
    )


@citadel.on_leave
def left(ctx):
    roster.remove(ctx.sender)
    citadel.log(f"player left: {ctx.sender}")
    citadel.broadcast(
        KIND_PLAYER_LEFT,
        int(ctx.sender).to_bytes(8, "big"),
        unreliable=False,
    )


@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")


@citadel.on_rpc("echo")
def echo(ctx, body):
    return citadel.Reply.ok(body)


@citadel.on_rpc("add")
def add(ctx, body):
    a = int.from_bytes(body[0:4], "big")
    b = int.from_bytes(body[4:8], "big")
    return citadel.Reply.ok(((a + b) & 0xFFFFFFFF).to_bytes(4, "big"))


elapsed = 0.0


@citadel.on_tick
def tick(dt):
    global elapsed
    elapsed += dt
    if elapsed >= 1.0:
        elapsed -= 1.0
        citadel.log(f"tick heartbeat: {roster.count()} player(s) online", "debug")
