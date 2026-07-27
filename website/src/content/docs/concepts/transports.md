---
title: Choosing a transport
description: Pick QUIC, WebTransport, or WebSocket by client type and message needs, with reliable and unreliable delivery explained plainly.
---

A **transport** is the road between a game client and Citadel. All three roads
carry the same Citadel message envelope. They differ in which platforms can use
them and whether they offer a fast, loss-tolerant lane.

![Three anime couriers reach the same Citadel server using two-lane native QUIC, two-lane browser WebTransport, or one dependable WebSocket bridge.](../../../assets/docs/three-couriers-transports.png)

*Three couriers, one destination: choose the road that fits the client and the
kind of message you need to deliver.*

## What you need to know

- Use **QUIC** for a native action game that needs fast state updates.
- Use **WebTransport** when a Chromium browser needs a similar fast lane.
- Use **WebSocket** for the broadest browser reach and reliable-only traffic.

Citadel deliberately does **not** ship a hand-rolled RUDP path: QUIC provides
the reliable streams, unreliable datagrams, congestion control, and TLS that a
native action game needs. The three routes below are the documented newcomer
choices exposed by the current engine and web integration guides.

| Your client | Start with | Why |
| --- | --- | --- |
| Unity, Unreal, Godot, or native Rust action game | QUIC | Reliable commands plus unreliable, latest-wins state |
| Chromium browser that needs datagrams | WebTransport | QUIC-like delivery through the browser's HTTP/3 stack |
| General browser, Node app, or UDP-blocked network | WebSocket | Simple and widely reachable; every message is reliable |

## Reliable and unreliable are different tools

**Reliable** delivery keeps messages ordered and retries missing data. Use it
for actions that must arrive: authentication, room joins, RPCs, purchases,
inventory changes, or chat.

**Unreliable** delivery may lose an update. That is useful for rapidly changing
state such as position snapshots. If snapshot 41 is lost but snapshot 42 arrives
milliseconds later, waiting for 41 would only make the game feel older.

Fast state and reliable commands usually travel together. QUIC and WebTransport
offer both lanes; WebSocket offers the reliable lane only.

## QUIC: the native speedster

Citadel's native QUIC path uses `quinn` and TLS 1.3. It sends loss-tolerant game
state as datagrams and reliable control messages as streams. The transport
negotiates the Citadel protocol name `citadel/0` during connection setup.

QUIC also handles packet protection, address validation, replay defense, and
network-path changes. Citadel still performs its own application handshake
after QUIC connects, because an encrypted socket is not the same thing as an
authenticated player.

## WebTransport: the browser speedster

WebTransport gives Chromium browsers datagrams and reliable streams over
HTTP/3. It runs on its own UDP endpoint, separate from native QUIC.

Local development uses a short-lived self-signed certificate that the browser
pins by SHA-256 hash. The server prints that hash at startup. Production
certificate provisioning remains an operator responsibility.

## WebSocket: the dependable all-rounder

WebSocket carries reliable, ordered binary messages and works in the broadest
set of browser/network environments. It has no unreliable datagram lane, so it
is excellent for commands, social features, RPCs, and prototypes; high-rate
native transform snapshots should use QUIC.

Citadel currently serves plain `ws://` directly. Terminate production TLS at a
fronting proxy when you need `wss://`.

## When a client cannot keep up

Every connection has a bounded outgoing queue, configured in envelopes. Citadel
never blocks gameplay routing on a slow socket: if that queue is full or closed,
the current outbound attempt is dropped. Treat transient realtime updates as
replaceable, repair them with a fresh authoritative snapshot, and keep durable
game facts in storage or a domain service.

## Go deeper only when you need it

- [Gateway, rooms & relay](/concepts/gateway/) explains where messages go.
- [Messages & envelopes](/concepts/envelopes/) explains what every transport carries.
- [Configuration](/reference/operations/configuration/) lists exact endpoints and keys.
- [Protocol reference](/reference/protocol/envelope/) gives byte-level contracts.
