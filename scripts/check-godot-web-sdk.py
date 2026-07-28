#!/usr/bin/env python3
"""Static source/vector guard for Godot's browser-only WebSocket transport."""
from pathlib import Path
import re
import sys


def frame(kind: int, body: bytes) -> bytes:
    payload = kind.to_bytes(2, "big") + body
    return len(payload).to_bytes(4, "big") + payload


def decode(data: bytes):
    result, offset = [], 0
    while len(data) - offset >= 4:
        length = int.from_bytes(data[offset:offset + 4], "big")
        if not 2 <= length <= 16 * 1024 * 1024:
            raise ValueError("invalid frame length")
        end = offset + 4 + length
        if end > len(data):
            break
        result.append((int.from_bytes(data[offset + 4:offset + 6], "big"), data[offset + 6:end]))
        offset = end
    return result, data[offset:]


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    protocol_path = root / "clients/godot/citadel/protocol.gd"
    client_path = root / "clients/godot/citadel/web_client.gd"
    makefile_path = root / "Makefile"
    powershell_path = root / "make.ps1"
    artifact_script_path = root / "scripts/package_godot_web_artifact.py"
    ci_workflow_path = root / ".github/workflows/ci.yml"
    release_workflow_path = root / ".github/workflows/release.yml"
    artifact_readme_path = root / "clients/godot/tests/web/README.md"
    smoke_path = root / "clients/godot/tests/web/smoke.gd"
    e2e_config_path = root / "clients/godot/tests/web/citadel-e2e.toml"
    e2e_server_path = root / "clients/godot/tests/web/serve_web.py"
    e2e_browser_path = root / "scripts/verify_godot_web_e2e.py"
    test_path = root / "clients/godot/tests/web/test_web_client.gd"
    integration_test_path = root / "clients/godot/tests/web/test_websocket_integration.gd"
    mock_path = root / "clients/godot/tests/web/mock_citadel_websocket.py"
    protocol = protocol_path.read_text(encoding="utf-8")
    client = client_path.read_text(encoding="utf-8")
    makefile = makefile_path.read_text(encoding="utf-8")
    powershell = powershell_path.read_text(encoding="utf-8")
    artifact_script = artifact_script_path.read_text(encoding="utf-8") if artifact_script_path.is_file() else ""
    ci_workflow = ci_workflow_path.read_text(encoding="utf-8") if ci_workflow_path.is_file() else ""
    release_workflow = release_workflow_path.read_text(encoding="utf-8") if release_workflow_path.is_file() else ""
    artifact_readme = artifact_readme_path.read_text(encoding="utf-8") if artifact_readme_path.is_file() else ""
    smoke = smoke_path.read_text(encoding="utf-8") if smoke_path.is_file() else ""
    e2e_config = e2e_config_path.read_text(encoding="utf-8") if e2e_config_path.is_file() else ""
    e2e_server = e2e_server_path.read_text(encoding="utf-8") if e2e_server_path.is_file() else ""
    e2e_browser = e2e_browser_path.read_text(encoding="utf-8") if e2e_browser_path.is_file() else ""
    test_source = test_path.read_text(encoding="utf-8") if test_path.is_file() else ""
    integration_source = integration_test_path.read_text(encoding="utf-8") if integration_test_path.is_file() else ""
    mock_source = mock_path.read_text(encoding="utf-8") if mock_path.is_file() else ""
    required = [
        (protocol, "encode_websocket_frame"), (protocol, "decode_websocket_frames"),
        (protocol, "decode_auth_result"), (client, "class_name CitadelWebClient"),
        (client, "WebSocketPeer.new()"), (client, "func authenticate_token"),
        (client, "WebProtocol.encode_websocket_frame"),
        (client, "WebProtocol.decode_websocket_frames"),
        (client, "_peer.poll()"), (client, "_peer.inbound_buffer_size"),
        (client, "_auth_accepted"),
        (client, 'extends "res://addons/citadel/client.gd"'),
        (client, 'preload("res://addons/citadel/protocol.gd")'),
        (makefile, "package-client-godot-web:"),
        (makefile, "package_godot_web_artifact.py"),
        (makefile, "bin-client-godot-web:"),
        (powershell, "Invoke-PackageClientGodotWeb"),
        (powershell, "package_godot_web_artifact.py"),
        (powershell, "Invoke-BinClientGodotWeb"),
        (artifact_script, "web/index.wasm"),
        (artifact_script, "web/citadel-e2e.toml"),
        (artifact_script, "web/serve_web.py"),
        (artifact_script, "--verify-package"),
        (ci_workflow, "package_godot_web_artifact.py --verify-package"),
        (ci_workflow, "Run exported Godot Web app against real Citadel"),
        (ci_workflow, "google-chrome"),
        (ci_workflow, "verify_godot_web_e2e.py"),
        (ci_workflow, "citadel-e2e.toml"),
        (release_workflow, "godot-web:"),
        (artifact_readme, "application/wasm"),
        (smoke, "CitadelWebClient.new()"),
        (smoke, "authenticate_guest"),
        (smoke, "KIND_PEER_POSITION"),
        (smoke, "data-citadel-e2e"),
        (e2e_config, "[transport.websocket]"),
        (e2e_config, "127.0.0.1:17532"),
        (e2e_server, '".wasm": "application/wasm"'),
        (e2e_browser, "DevTools Protocol"),
        (e2e_browser, "data-citadel-e2e"),
        (e2e_browser, "--enable-unsafe-swiftshader"),
    ]
    errors = [f"missing {needle}" for source, needle in required if needle not in source]
    # The release publish job must wait for both native packages and the Godot
    # WebAssembly package.  Do not require an exact `needs` list: release
    # version validation is also a legitimate dependency.
    publish_needs = re.search(
        r"^  publish:\s*\n\s*needs:\s*\[([^\]]*)\]",
        release_workflow,
        flags=re.MULTILINE,
    )
    if not publish_needs or not {"package", "godot-web"}.issubset(
        {dependency.strip() for dependency in publish_needs.group(1).split(",")}
    ):
        errors.append("release publish must depend on package and godot-web")
    if "var result := _peer.poll()" in client:
        errors.append("WebSocketPeer.poll is void in Godot 4")
    if "CitadelRooms must accept the browser client contract" not in test_source:
        errors.append("missing Godot room-compatibility regression test")
    if "res://addons/citadel/" not in test_source:
        errors.append("Godot Web tests must load the public addon layout")
    if "Citadel Godot WebSocket integration test passed" not in integration_source:
        errors.append("missing live Godot WebSocket integration test")
    if "expected binary guest auth" not in mock_source or "KIND_PEER_POSITION" not in mock_source:
        errors.append("missing deterministic Citadel WebSocket fixture")
    first, second = frame(5, b""), frame(3, b"rpc")
    if first.hex() != "000000020005":
        errors.append("guest auth frame vector changed")
    if decode(first + second) != ([(5, b""), (3, b"rpc")], b""):
        errors.append("concatenated frame vector failed")
    if decode(second[:-1]) != ([], second[:-1]):
        errors.append("partial frame vector failed")
    try:
        decode(b"\x01\x00\x00\x01")
        errors.append("oversized frame accepted")
    except ValueError:
        pass
    if errors:
        print("check-godot-web-sdk: " + "; ".join(errors), file=sys.stderr)
        return 1
    print("check-godot-web-sdk: source contract, framing vectors, and WebAssembly package gate OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
