#!/usr/bin/env python3
"""One-connection Citadel WebSocket fixture for the Godot transport test.

It deliberately implements the small RFC 6455 surface the test needs without a
third-party package. The fixture checks the exact framed guest handshake and a
reliable position send, then coalesces two server envelopes in one binary
WebSocket message to exercise the client's stream buffering.
"""

from __future__ import annotations

import base64
import hashlib
import socket
import struct
import sys
import time


HOST = "127.0.0.1"
PORT = 7352
KIND_POSITION = 1
KIND_PEER_POSITION = 2
KIND_AUTH = 5
KIND_AUTH_RESULT = 6
KIND_NOTIFICATION = 27


def citadel_frame(kind: int, payload: bytes = b"") -> bytes:
    body = kind.to_bytes(2, "big") + payload
    return len(body).to_bytes(4, "big") + body


def read_exact(conn: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise RuntimeError("peer closed before sending the expected WebSocket frame")
        data.extend(chunk)
    return bytes(data)


def read_http_headers(conn: socket.socket) -> dict[str, str]:
    raw = bytearray()
    while b"\r\n\r\n" not in raw:
        chunk = conn.recv(4096)
        if not chunk:
            raise RuntimeError("peer closed during the WebSocket upgrade")
        raw.extend(chunk)
        if len(raw) > 16 * 1024:
            raise RuntimeError("WebSocket upgrade headers are too large")
    headers: dict[str, str] = {}
    for line in raw.decode("ascii").split("\r\n")[1:]:
        if not line:
            break
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return headers


def websocket_payload(conn: socket.socket) -> tuple[int, bytes]:
    first, second = read_exact(conn, 2)
    if first & 0x80 == 0:
        raise RuntimeError("fragmented WebSocket frames are not expected in this fixture")
    opcode = first & 0x0F
    masked = second & 0x80
    length = second & 0x7F
    if length == 126:
        length = int.from_bytes(read_exact(conn, 2), "big")
    elif length == 127:
        length = int.from_bytes(read_exact(conn, 8), "big")
    if not masked:
        raise RuntimeError("browser/client WebSocket frames must be masked")
    mask = read_exact(conn, 4)
    payload = bytearray(read_exact(conn, length))
    for index in range(length):
        payload[index] ^= mask[index % 4]
    return opcode, bytes(payload)


def send_binary(conn: socket.socket, payload: bytes) -> None:
    header = bytearray([0x82])
    if len(payload) < 126:
        header.append(len(payload))
    elif len(payload) <= 0xFFFF:
        header.append(126)
        header.extend(len(payload).to_bytes(2, "big"))
    else:
        header.append(127)
        header.extend(len(payload).to_bytes(8, "big"))
    conn.sendall(header + payload)


def serve() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((HOST, PORT))
        listener.listen(1)
        listener.settimeout(15)
        print(f"Citadel Godot Web mock listening on {HOST}:{PORT}", flush=True)
        conn, _address = listener.accept()
        with conn:
            conn.settimeout(10)
            headers = read_http_headers(conn)
            key = headers.get("sec-websocket-key")
            if not key:
                raise RuntimeError("missing Sec-WebSocket-Key")
            accept = base64.b64encode(
                hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
            ).decode("ascii")
            conn.sendall(
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: Upgrade\r\n"
                + f"Sec-WebSocket-Accept: {accept}\r\n\r\n".encode("ascii")
            )

            opcode, payload = websocket_payload(conn)
            expected_auth = citadel_frame(KIND_AUTH)
            if opcode != 2 or payload != expected_auth:
                raise RuntimeError(f"expected binary guest auth {expected_auth.hex()}, got opcode={opcode} payload={payload.hex()}")
            send_binary(conn, citadel_frame(KIND_AUTH_RESULT, b"\x01"))

            opcode, payload = websocket_payload(conn)
            expected_position = citadel_frame(KIND_POSITION, struct.pack("<ff", 1.25, -2.5))
            if opcode != 2 or payload != expected_position:
                raise RuntimeError("Godot client did not send the expected reliable position envelope")

            peer_position = (42).to_bytes(8, "big") + struct.pack("<ff", 1.25, -2.5)
            send_binary(
                conn,
                citadel_frame(KIND_PEER_POSITION, peer_position)
                + citadel_frame(KIND_NOTIFICATION, b"notice"),
            )
            # Keep the peer alive long enough for Godot's next poll tick to
            # drain both queued envelopes. Closing the raw TCP socket here can
            # otherwise discard the receive queue before the fixture observes
            # it, which is not representative of a Citadel session.
            time.sleep(1)
            print("Citadel Godot Web mock completed handshake and relay fixture", flush=True)


if __name__ == "__main__":
    try:
        serve()
    except Exception as error:  # pragma: no cover - only the CI fixture entry point.
        print(f"Citadel Godot Web mock failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
