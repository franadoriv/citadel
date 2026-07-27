#!/usr/bin/env python3
"""Drive the packaged Godot Web app through Chromium's DevTools Protocol.

The Web export advances its `WebSocketPeer` state machine on animation frames.
Chrome's `--dump-dom` exits while the first WebAssembly frame is still pending,
so the CI gate uses CDP to keep the real page alive and poll the result marker.
This module deliberately uses only the Python standard library so the release
verification does not acquire a browser-driver or container dependency.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen


class CdpError(RuntimeError):
    """A DevTools Protocol or browser execution failure."""


class DevToolsSocket:
    """Small RFC 6455 client for the local Chromium DevTools endpoint."""

    def __init__(self, endpoint: str, timeout_seconds: float) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme != "ws" or not parsed.hostname or not parsed.port:
            raise CdpError(f"unsupported DevTools endpoint: {endpoint}")
        self._socket = socket.create_connection((parsed.hostname, parsed.port), timeout_seconds)
        self._socket.settimeout(timeout_seconds)
        self._buffer = bytearray()
        path = parsed.path or "/"
        if parsed.query:
            path += f"?{parsed.query}"
        key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parsed.hostname}:{parsed.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self._socket.sendall(request.encode("ascii"))
        response = self._read_until_headers()
        headers, _, remainder = response.partition(b"\r\n\r\n")
        if not headers.startswith(b"HTTP/1.1 101"):
            raise CdpError(f"DevTools WebSocket upgrade failed: {headers.decode('utf-8', 'replace')}")
        self._buffer.extend(remainder)

    def close(self) -> None:
        self._socket.close()

    def command(self, message_id: int, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": message_id, "method": method}
        if params:
            payload["params"] = params
        self._send_json(payload)
        while True:
            response = self._receive_json()
            if response.get("id") != message_id:
                continue
            if "error" in response:
                raise CdpError(f"CDP {method} failed: {response['error']}")
            return response

    def _read_until_headers(self) -> bytes:
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise CdpError("DevTools closed the connection during the WebSocket upgrade")
            response.extend(chunk)
        return bytes(response)

    def _read_exact(self, size: int) -> bytes:
        while len(self._buffer) < size:
            chunk = self._socket.recv(max(4096, size - len(self._buffer)))
            if not chunk:
                raise CdpError("DevTools closed the WebSocket connection")
            self._buffer.extend(chunk)
        value = bytes(self._buffer[:size])
        del self._buffer[:size]
        return value

    def _send_json(self, value: dict[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self._send_frame(0x81, payload)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            header = bytes((opcode, 0x80 | size))
        elif size <= 0xFFFF:
            header = bytes((opcode, 0x80 | 126)) + size.to_bytes(2, "big")
        else:
            header = bytes((opcode, 0x80 | 127)) + size.to_bytes(8, "big")
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self._socket.sendall(header + mask + masked)

    def _receive_json(self) -> dict[str, Any]:
        while True:
            first, second = self._read_exact(2)
            opcode = first & 0x0F
            is_masked = (second & 0x80) != 0
            size = second & 0x7F
            if size == 126:
                size = int.from_bytes(self._read_exact(2), "big")
            elif size == 127:
                size = int.from_bytes(self._read_exact(8), "big")
            mask = self._read_exact(4) if is_masked else b""
            payload = self._read_exact(size)
            if is_masked:
                payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
            if opcode == 0x8:
                raise CdpError("DevTools closed the WebSocket connection")
            if opcode == 0x9:
                self._send_frame(0x8A, payload)
                continue
            if opcode != 0x1:
                continue
            return json.loads(payload.decode("utf-8"))


def json_at(url: str, timeout_seconds: float) -> Any:
    with urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_devtools(profile: str, timeout_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    active_port = Path(profile) / "DevToolsActivePort"
    endpoint: str | None = None
    while time.monotonic() < deadline:
        if endpoint is None:
            try:
                port = active_port.read_text(encoding="utf-8").splitlines()[0]
                endpoint = f"http://127.0.0.1:{int(port)}/json/list"
            except (FileNotFoundError, IndexError, ValueError):
                time.sleep(0.1)
                continue
        try:
            targets = json_at(endpoint, min(1.0, timeout_seconds))
        except (OSError, URLError, json.JSONDecodeError):
            time.sleep(0.1)
            continue
        for target in targets:
            if target.get("type") == "page" and target.get("webSocketDebuggerUrl"):
                return target
        time.sleep(0.1)
    if endpoint is None:
        raise CdpError("Chromium did not create DevToolsActivePort in its temporary profile")
    raise CdpError(f"Chromium did not expose a DevTools page target at {endpoint}")


def result_marker(browser: DevToolsSocket, message_id: int) -> str | None:
    response = browser.command(
        message_id,
        "Runtime.evaluate",
        {"expression": "document.documentElement.getAttribute('data-citadel-e2e')", "returnByValue": True},
    )
    value = response.get("result", {}).get("result", {}).get("value")
    return value if isinstance(value, str) else None


def page_text(browser: DevToolsSocket, message_id: int) -> str:
    response = browser.command(
        message_id,
        "Runtime.evaluate",
        {"expression": "document.body ? document.body.innerText : ''", "returnByValue": True},
    )
    value = response.get("result", {}).get("result", {}).get("value")
    return value if isinstance(value, str) else ""


def run(args: argparse.Namespace) -> None:
    profile = tempfile.mkdtemp(prefix="citadel-godot-web-cdp-")
    try:
        command = [
            args.browser,
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--enable-webgl",
            "--ignore-gpu-blocklist",
            "--use-angle=swiftshader",
            "--enable-unsafe-swiftshader",
            "--remote-debugging-address=127.0.0.1",
            # Let Chromium choose a free loopback port. Its documented
            # DevToolsActivePort profile file communicates that port back to
            # this driver, avoiding collisions with another runner process.
            "--remote-debugging-port=0",
            f"--user-data-dir={profile}",
            "about:blank",
        ]
        log_path = Path(args.browser_log)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as browser_log:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=browser_log)
            socket_client: DevToolsSocket | None = None
            try:
                target = wait_for_devtools(profile, min(10.0, args.timeout))
                socket_client = DevToolsSocket(target["webSocketDebuggerUrl"], timeout_seconds=5.0)
                next_id = 1
                socket_client.command(next_id, "Page.enable")
                next_id += 1
                socket_client.command(next_id, "Runtime.enable")
                next_id += 1
                socket_client.command(next_id, "Page.navigate", {"url": args.url})
                next_id += 1
                deadline = time.monotonic() + args.timeout
                latest = None
                while time.monotonic() < deadline:
                    latest = result_marker(socket_client, next_id)
                    next_id += 1
                    if latest == "pass":
                        print("Godot Web real Citadel E2E passed in Chromium")
                        return
                    if latest == "fail":
                        detail = page_text(socket_client, next_id)
                        raise CdpError(f"Godot Web app reported failure: {detail}")
                    time.sleep(0.2)
                raise CdpError(f"timed out waiting for Godot Web result marker (last value: {latest!r})")
            finally:
                if socket_client is not None:
                    try:
                        socket_client.close()
                    except OSError:
                        pass
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    finally:
        # Chromium may let a child flush its profile after the parent exits.
        # This profile is created exclusively for this one CI invocation, and a
        # best-effort cleanup must not turn a passed browser scenario into a
        # false-negative release gate.
        shutil.rmtree(profile, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", required=True, help="path to Google Chrome or Chromium")
    parser.add_argument("--url", required=True, help="Godot Web app URL with its Citadel WebSocket query parameter")
    parser.add_argument("--browser-log", required=True, help="file used to capture Chromium stderr")
    parser.add_argument("--timeout", type=float, default=30.0, help="maximum application wait in seconds")
    return parser.parse_args()


def main() -> int:
    try:
        run(parse_args())
    except (CdpError, OSError, subprocess.SubprocessError) as error:
        print(f"Godot Web real Citadel E2E failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
