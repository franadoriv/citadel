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

citadel.on_message(1, () => {
  citadel.broadcast(3, "discarded", false);
  while (true) {}
});

citadel.on_message(2, (_ctx, body) => {
  citadel.broadcast(3, concat(ascii("alive:"), body), false);
});

citadel.on_rpc("hang", () => {
  while (true) {}
});

citadel.on_rpc("ping", () => citadel.Reply.ok("pong"));
