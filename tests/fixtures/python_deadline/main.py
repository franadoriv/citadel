import citadel


@citadel.on_message(1)
def hung_message(ctx, body):
    citadel.broadcast(99, b"discarded")
    while True:
        pass


@citadel.on_message(2)
def healthy_message(ctx, body):
    citadel.broadcast(3, b"alive:" + body)


@citadel.on_rpc("hang")
def hung_rpc(ctx, body):
    while True:
        pass


@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")
