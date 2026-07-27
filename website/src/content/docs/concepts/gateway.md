---
title: Gateway, rooms & relay
description: How Citadel admits a connection, assigns a realtime participant, scopes players into rooms, and routes accepted messages.
---

The realtime gateway is Citadel's gatekeeper and dispatcher. It admits a
connection only after the first authentication message succeeds, gives that
connection a temporary participant identity, and routes later messages to game
logic or the correct audience.

## The first five seconds of a connection

1. A client opens QUIC, WebTransport, or WebSocket.
2. Its first reliable envelope is `KIND_AUTH`: either a session token or an
   empty body that asks to join as a guest.
3. Citadel validates the token, checks whether guests are allowed, and returns
   one `KIND_AUTH_RESULT`.
4. Only an accepted connection receives a `ParticipantId` and enters the live
   registry.
5. Game messages, RPCs, room requests, and runtime hooks may now run.

An invalid token gets one coarse rejection and a closed connection. Citadel does
not reveal whether a token was unknown, expired, or revoked.

## What the gateway knows

For a live participant, the gateway can answer:

- Which connection and transport should receive bytes?
- Is this participant a guest or bound to an authenticated `user_id`?
- Which named room, if any, is the participant in?
- Which game script or built-in service should handle this message?
- Which peers are allowed to see the result?

The gateway does not draw a player or decide damage. The client draws; your game
logic decides rules.

## Rooms make the audience explicit

A room is the current single-node match/lobby boundary. A client can create or
join a named room, receive its map/mode label, and tell the server when that map
is ready. Membership makes presence and relay scope explicit instead of sending
every gameplay update to every connected player.

One local participant belongs to one room at a time. Leaving, switching rooms,
or disconnecting removes the old membership. Your runtime can accept or reject
room creation/join requests and keep authoritative state keyed by `room_id`.

Cross-node authoritative room ownership and failover are still planned work;
today's room/match state lives on one node.

## Relay, not echo

The quickstart uses a tiny built-in lesson:

- player A sends `KIND_POSITION`;
- Citadel tags the body with A's `ParticipantId`;
- player B receives `KIND_PEER_POSITION`;
- A does not receive its own message back.

That proves two clients can share state. A real game normally sends the request
through server game logic first, then broadcasts only the accepted result to
the room.

## Realtime delivery is not durable history

Live queues are bounded and slow peers can miss best-effort state. Design a game
so a fresh authoritative snapshot can repair transient loss. Put facts that must
survive reconnects — inventory, unlocks, scores, chat history — in Citadel's
storage or domain services.

## Related concepts

- [Player identity & sessions](/concepts/sessions/) separates connection,
  participant, and account identity.
- [Choosing a transport](/concepts/transports/) explains the three network roads.
- [Game logic & server authority](/concepts/game-logic/) explains who validates a request.
- [Room API reference](/reference/client-sdk/rooms/) gives exact client operations.
