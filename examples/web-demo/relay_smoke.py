#!/usr/bin/env python3
"""Headless relay smoke test for the web-demo wire — no browser required.

This is the browser-free companion to index.html. It proves a running Citadel
server relays player positions live, exercising the whole realtime chain:
auth handshake -> on_join registration -> the game's on_message handler ->
the broadcast host-API -> wire encoding.

It is RUNTIME-AGNOSTIC: KIND_POSITION / KIND_PEER_POSITION are wire constants,
not per-language, so the same probe verifies whichever game the server is
running (Lua, Python, or JS). Point the server at a demo config and run this:

    # terminal 1 — start a server (pick a runtime):
    cargo run -- --config examples/configs/demo.toml                     # Lua
    cargo run --features runtime-python -- --config examples/configs/python-demo.toml
    cargo run --features runtime-js     -- --config examples/configs/js-demo.toml

    # terminal 2 — verify the relay (browser-free):
    pip install websockets
    python examples/web-demo/relay_smoke.py           # exit 0 = PASS

Protocol (see crates/citadel-wire): the FIRST envelope on each connection must
be KIND_AUTH (5); an EMPTY body requests a guest session. The server replies
with KIND_AUTH_RESULT (6); only then is the session registered. Client A then
sends KIND_POSITION (1); client B must receive KIND_PEER_POSITION (2) whose
payload = 8-byte BE sender id + A's original 20-byte payload.

Wire: framed = u32 BE body_len | u16 BE kind | payload ; body_len = 2 + len(payload).
"""
import asyncio
import os
import struct
import sys

import websockets

WS = os.environ.get("CITADEL_WS", "ws://127.0.0.1:7352/")
KIND_POSITION = 1
KIND_PEER_POSITION = 2
KIND_AUTH = 5
KIND_AUTH_RESULT = 6


def frame(kind: int, payload: bytes) -> bytes:
    return struct.pack(">IH", 2 + len(payload), kind) + payload


def parse(buf: bytes):
    out, off = [], 0
    while off + 6 <= len(buf):
        body_len = struct.unpack_from(">I", buf, off)[0]
        kind = struct.unpack_from(">H", buf, off + 4)[0]
        payload = buf[off + 6 : off + 4 + body_len]
        out.append((kind, payload))
        off += 4 + body_len
    return out


async def recv_kind(ws, want_kind, timeout=5.0):
    """Read frames until one of kind `want_kind` arrives; return its payload."""
    while True:
        msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
        raw = msg if isinstance(msg, (bytes, bytearray)) else msg.encode()
        for kind, payload in parse(raw):
            if kind == want_kind:
                return payload


async def authenticate(ws, label):
    await ws.send(frame(KIND_AUTH, b""))  # empty body = guest
    result = await recv_kind(ws, KIND_AUTH_RESULT)
    status = result[0] if result else -1
    names = {0: "authenticated", 1: "guest"}
    print(f"[{label}] KIND_AUTH_RESULT status={status} ({names.get(status, 'rejected/other')})")
    return status


async def main():
    pos = struct.pack("<fffd", 1.5, 2.5, 3.5, 42.0)  # 3 LE f32 + 1 LE f64 = 20 bytes
    async with websockets.connect(WS, max_size=None) as a, \
               websockets.connect(WS, max_size=None) as b:
        if await authenticate(a, "A") not in (0, 1) or await authenticate(b, "B") not in (0, 1):
            print("FAIL: guest auth rejected — server may disallow guests")
            return 1

        await asyncio.sleep(0.3)  # let both memberships settle
        await a.send(frame(KIND_POSITION, pos))
        print("[A] sent KIND_POSITION")

        try:
            payload = await recv_kind(b, KIND_PEER_POSITION, timeout=3.0)
        except asyncio.TimeoutError:
            print("FAIL: client B received no KIND_PEER_POSITION within 3s")
            return 1

        sender_id = int.from_bytes(payload[:8], "big")
        ok = payload[8:] == pos
        print(f"[B] got KIND_PEER_POSITION sender_id={sender_id} "
              f"payload_len={len(payload)} pos_matches={ok}")
        if ok:
            print("PASS: the game relayed A's position to B over the live gateway")
            return 0
        print(f"FAIL: relayed payload {payload[8:].hex()} != sent {pos.hex()}")
        return 1


sys.exit(asyncio.run(main()))
