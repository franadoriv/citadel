# main.py - your Citadel game logic.
#
# Citadel loads this file on startup when citadel.toml sets:
#
#   [runtime]
#   language = "python"
#   scripts_dir = "./scripts"
#
# Python support is compiled only in runtime-python builds. Packaged Python
# releases bundle a matching CPython runtime beside citadel.exe so this script
# does not require a globally installed Python.
#
# Host API (available as the imported `citadel` module):
#   citadel.log(message, level="info")                 # "trace" | "debug" | "info" | "warn" | "error"
#   citadel.broadcast(kind, body, unreliable=False)    # send to every participant except sender
#   citadel.send(session, kind, body, unreliable=False) # send to one participant
#   @citadel.on_message(kind)                          # handler(ctx, body)
#   @citadel.on_join / @citadel.on_leave               # handler(ctx)
#   @citadel.on_rpc(method)                            # handler(ctx, body) -> bytes/str/Reply
#   @citadel.on_tick                                   # handler(dt)
# Bodies are raw bytes. Use int.to_bytes / int.from_bytes or struct for binary
# layouts shared with your client.

import citadel


# Wire message kinds. These must match your client. Kinds 1..6 are used by the
# built-in demos; pick your own numbers for your game.
KIND_POSITION = 1
KIND_PEER_POSITION = 2


citadel.log("python game logic loaded", "info")


@citadel.on_join
def joined(ctx):
    citadel.log(f"join {ctx.sender}", "info")


@citadel.on_leave
def left(ctx):
    citadel.log(f"leave {ctx.sender}", "info")


@citadel.on_message(KIND_POSITION)
def relay_position(ctx, body):
    tagged = int(ctx.sender).to_bytes(8, "big") + body
    citadel.broadcast(KIND_PEER_POSITION, tagged, True)


@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")


@citadel.on_tick
def tick(dt):
    # Your simulation step goes here.
    return None
