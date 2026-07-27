// QuickJS capped-mode Citadel game script.
//
// Run with:
//   cargo run --features runtime-js -- --config examples/configs/js-game.toml
//
// This mirrors game/main.lua's position relay so examples/web-demo works
// unchanged: inbound POSITION (kind 1) is sender-tagged and rebroadcast as
// PEER_POSITION (kind 2).

import { concat, u64be } from "./systems/binary.js";

const KIND_POSITION = 1;
const KIND_PEER_POSITION = 2;

citadel.on_message(KIND_POSITION, (ctx, body) => {
  const tagged = concat(u64be(ctx.sender), body);
  citadel.broadcast(KIND_PEER_POSITION, tagged, true);
});

citadel.on_rpc("ping", () => citadel.Reply.ok("pong"));
