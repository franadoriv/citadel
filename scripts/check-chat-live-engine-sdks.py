#!/usr/bin/env python3
"""Adversarial source/fixture gate for Unity and Godot durable chat helpers."""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
PATHS = {
    "fixture": ROOT / "tests/fixtures/chat-live-events-v1.json",
    "unity": ROOT / "clients/unity/Citadel/CitadelChatLive.cs",
    "unity_test": ROOT / "clients/unity/Editor/tests/CitadelChatLiveTests.cs",
    "godot": ROOT / "clients/godot/citadel/chat_live.gd",
    "godot_test": ROOT / "clients/godot/tests/web/test_chat_live.gd",
    "package": ROOT / "scripts/package_godot_web_artifact.py",
    "unity_poll": ROOT / "clients/unity/Demo/PeerManager.cs",
    "unity_rpc": ROOT / "clients/unity/Demo/RpcClient.cs",
    "godot_poll": ROOT / "clients/godot/sample/peer_sync.gd",
    "godot_native": ROOT / "clients/godot/native/src/citadel_client_native.cpp",
}
KINDS = {"presence.join", "presence.leave", "typing", "message.create", "message.update", "message.remove", "access.revoked", "resync_required"}
GODOT_FUNCTIONS = {
    "_init", "decode_event", "_message_event", "_presence", "_valid_message", "_valid_history_message", "_strict_int", "_integer_json_fields", "_unique_json_object_keys", "_nonempty", "_exact_keys", "_valid_content",
    "handle_envelope", "joined_channels", "on_disconnected", "needs_resync", "is_current", "active_typing", "direct_target", "group_target", "room_target", "history_request",
    "join", "leave", "send_message", "history", "begin_reconciliation", "edit", "delete_message", "moderate", "set_typing", "rejoin_tracked_channels", "_join_response", "_history_response", "_newest_first", "_valid_history_request",
}
UNITY_METHODS = {"CitadelChatLive", "HandleEnvelope", "OnDisconnected", "NeedsResync", "IsCurrent", "ActiveTyping", "Join", "Leave", "Send", "History", "BeginReconciliation", "Edit", "Delete", "Moderate", "SetTyping", "RejoinTrackedChannels"}


def class_body(source: str, marker: str) -> str:
    start = source.rfind(marker)
    if start < 0:
        return ""
    opening = source.find("{", start)
    depth = 0
    quoted = False
    escaped = False
    for i in range(opening, len(source)):
        c = source[i]
        if quoted:
            if escaped: escaped = False
            elif c == "\\": escaped = True
            elif c == '"': quoted = False
            continue
        if c == '"': quoted = True
        elif c == "{": depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0: return source[opening + 1:i]
    return ""


def validate(s: dict[str, str], fixture: dict) -> list[str]:
    errors: list[str] = []
    if fixture.get("version") != 1 or {x.get("kind") for x in fixture.get("valid", [])} != KINDS:
        errors.append("fixture is not the closed v1 eight-variant contract")
    invalid_names = {x.get("name") for x in fixture.get("invalid", [])}
    for name in ("malformed_json", "numeric_string_event_id", "fractional_revision", "unknown_variant_field", "duplicate_type", "unknown_hyphenated_field", "typing_false_nonzero_expiry", "duplicate_resync_scope", "remove_has_content", "message_time_reversal"):
        if name not in invalid_names: errors.append(f"fixture missing adversarial vector {name}")
    if len(fixture.get("invalid", [])) < 20: errors.append("fixture fail-closed suite was weakened")

    common = ["access.revoked", "resync_required", "chat.join", "chat.history", "acknowledge_watermark"]
    for key in ("unity", "godot"):
        for token in common:
            if token not in s[key]: errors.append(f"{key} missing contract marker {token}")

    unity_markers = ["LiveEventPending", "ChatLiveEventApplication", "ChatHistorySnapshotApplication", "ChatReconciliationHandle", "HashSet<string> _revoked", "ResetAndRestart", "Staged.AddRange", "ValidateHistoryJson", "ValidateClosedJson", "TryScanJson", "state.PendingLive", "state.Admitted", "request.Floor", "_revoked.Remove(response.channel_id)"]
    godot_markers = ["signal live_event_pending", "class LiveEventApplication", "class HistorySnapshotApplication", "class ReconciliationHandle", "var authority :=", "_integer_json_fields", "_unique_json_object_keys", "request.staged", "request.page_serial", "state.pending_live", "state.admitted", "request.floor", "authority.revoked.erase(channel_id)"]
    for token in unity_markers:
        if token not in s["unity"]: errors.append(f"Unity authority/apply/snapshot marker missing: {token}")
    for token in godot_markers:
        if token not in s["godot"]: errors.append(f"Godot authority/apply/snapshot marker missing: {token}")

    for forbidden in ("func _get_or_create", "func confirm_history_page_applied", "func track_joined_channel", "force_current"):
        if forbidden in s["godot"]: errors.append(f"Godot public authority escape remains: {forbidden}")
    for forbidden in ("ConfirmHistoryPageApplied", "TrackJoinedChannel", "ForceCurrent"):
        if forbidden in s["unity"]: errors.append(f"Unity public authority escape remains: {forbidden}")
    for forbidden in ("var _channels", "var _reconciliations", "var _pending_joins", "func _call("):
        if forbidden in s["godot"]: errors.append(f"Godot externally mutable authority remains: {forbidden}")
    if s["godot"].count("if guard.used") < 2 or s["godot"].count("guard.used = true") < 2:
        errors.append("Godot application completion lost closure-captured exactly-once guard")
    if "if typeof(value) == TYPE_INT" not in s["godot"] or "floor(value) == value" not in s["godot"]:
        errors.append("Godot strict decoder became coercive")
    if not re.search(r"not _newest_first[^\n]*:\n\s*restart_reconciliation\.call\(request\); return", s["godot"]):
        errors.append("Godot malformed continuation no longer transactionally restarts")

    godot_functions = set(re.findall(r"^(?:static )?func\s+([A-Za-z0-9_]+)\s*\(", s["godot"], re.M))
    if godot_functions != GODOT_FUNCTIONS:
        errors.append("Godot top-level callable surface changed: " + ",".join(sorted(godot_functions ^ GODOT_FUNCTIONS)))
    body = class_body(s["unity"], "public sealed class CitadelChatLive")
    unity_methods = set(re.findall(r"^        public\s+(?:[A-Za-z0-9_<>,\[\]]+\s+)?([A-Za-z0-9_]+)\s*\(", body, re.M))
    if unity_methods != UNITY_METHODS:
        errors.append("Unity public authority surface changed: " + ",".join(sorted(unity_methods ^ UNITY_METHODS)))

    test_markers = [
        "revocation tombstone must survive disconnect", "successful live apply alone advances dedup state",
        "malformed continuation must transactionally restart", "history pages must remain internal",
        "reconciliation must preserve the existing generation", "numeric strings must fail strict event decoding",
        "authority state must not be externally mutable", "generic RPC forwarding must not expose ACK construction",
        "snapshot below captured recovery floor", "ACK send failure must remove dead operation",
    ]
    for token in test_markers:
        if token not in s["godot_test"]: errors.append(f"Godot adversarial runtime regression missing: {token}")
    for token in ("ChatHistorySnapshotApplication", "LiveEventPending", "Replace", "SnapshotRestarted"):
        if token not in s["unity_test"]: errors.append(f"Unity NUnit regression marker missing: {token}")

    if '"addons/citadel/chat_live.gd"' not in s["package"]: errors.append("Godot package omits chat_live.gd")
    if "chatLive.HandleEnvelope" not in s["unity_poll"]: errors.append("Unity single poll owner does not route chat envelopes")
    if "_chat_live.handle_envelope" not in s["godot_poll"]: errors.append("Godot single poll owner does not route chat envelopes")
    if "new byte[8192]" in s["unity_poll"] or "8 * 1024 * 1024" not in s["unity_poll"]:
        errors.append("Unity poll owner must use the explicit bounded 8 MiB envelope buffer")
    if 'rpcClient?.FailAllPending($"consumed oversized' not in s["unity_poll"] or "chatLive?.OnDisconnected" not in s["unity_poll"]:
        errors.append("Unity truncation/disconnect path must fail pending RPCs and fence chat authority")
    if "result.Ok ? result.Payload : System.Array.Empty<byte>()" not in s["unity_poll"]:
        errors.append("Unity chat RPC adapter must dispatch asynchronous errors to the SDK callback")
    if "public void FailAllPending" not in s["unity_rpc"]:
        errors.append("Unity RPC transport lacks deterministic pending-callback failure cleanup")
    if 'FailAllPending("malformed RPC response")' not in s["unity_rpc"]:
        errors.append("Unity malformed uncorrelated RPC response silently strands pending callbacks")
    if "callback.call(reply.get(\"payload\", PackedByteArray()) if" not in s["godot_poll"]:
        errors.append("Godot chat RPC adapter must dispatch asynchronous errors to the SDK callback")
    godot_truncation_failure = 'int(envelope.get("required_len", 0)))\n\t\t\t_fail_pending_rpc()'
    if godot_truncation_failure not in s["godot_poll"] or s["godot_poll"].count("_fail_pending_rpc()") < 2 or "_chat_live.on_disconnected()" not in s["godot_poll"]:
        errors.append("Godot malformed/disconnected transport must fail pending RPCs and fence chat authority")
    if "kPollCapacity = 8 * 1024 * 1024" not in s["godot_native"] or "kInitialPollCapacity" in s["godot_native"]:
        errors.append("Godot native poll must use one explicit bounded 8 MiB buffer")
    poll_body = s["godot_native"][s["godot_native"].find("Dictionary CitadelClientNative::poll()") : s["godot_native"].find("Dictionary CitadelClientNative::decode_rep")]
    if poll_body.count("citadel_client_poll(") != 1 or 'result["truncated"]' not in poll_body:
        errors.append("Godot native poll must never repoll a consumed truncated envelope and must report truncation")
    if "public static ChatTarget Group(ulong groupId)" not in s["unity"]: errors.append("Unity group target lost exact u64 contract")
    if "static func group_target(group_id: int)" not in s["godot"]: errors.append("Godot group target lost integer contract")
    return errors


def self_test(sources: dict[str, str], fixture: dict) -> list[str]:
    failures: list[str] = []
    mutations: list[tuple[str, dict[str, str], dict]] = []
    g = dict(sources); g["godot"] += "\nfunc freshly_named_override(channel_id: String) -> void:\n\t_channels[channel_id] = {\"current\":true}\n"
    mutations.append(("Godot force-current wrapper", g, fixture))
    g = dict(sources); g["godot"] += "\nfunc freshly_named_state_leak() -> Dictionary:\n\treturn _channels\n"
    mutations.append(("Godot mutable helper leak", g, fixture))
    u = dict(sources); marker = body_end_marker = "\n    }\n}"
    u["unity"] = u["unity"].replace(marker, "\n        public void FreshlyNamedOverride(string channel) { }" + marker, 1)
    mutations.append(("Unity force-current wrapper", u, fixture))
    weak = json.loads(json.dumps(fixture)); weak["invalid"] = [{"name": "malformed_json", "payload": "{"}]
    mutations.append(("weakened invalid fixtures", sources, weak))
    coercive = dict(sources); coercive["godot"] = coercive["godot"].replace("if typeof(value) == TYPE_INT: return value >= minimum and value <= maximum", "return int(value) >= minimum and int(value) <= maximum", 1)
    mutations.append(("coercive Godot decoder", coercive, fixture))
    continuation = dict(sources); continuation["godot"] = continuation["godot"].replace('or not _newest_first(response.get("items", []), request.before):\n\t\t\t\trestart_reconciliation.call(request); return', 'or not _newest_first(response.get("items", []), request.before):\n\t\t\t\treturn', 1)
    mutations.append(("malformed continuation bare return", continuation, fixture))
    completion = dict(sources); completion["godot"] = completion["godot"].replace("if guard.used or event == null", "if false or event == null", 1)
    mutations.append(("Godot completion guard bypass", completion, fixture))
    small_poll = dict(sources); small_poll["unity_poll"] = small_poll["unity_poll"].replace("8 * 1024 * 1024", "8192", 1)
    mutations.append(("Unity 8 KiB poll buffer", small_poll, fixture))
    success_only = dict(sources); success_only["unity_poll"] = success_only["unity_poll"].replace("callback?.Invoke(result.Ok ? result.Payload : System.Array.Empty<byte>())", "if (result.Ok) callback?.Invoke(result.Payload)", 1)
    mutations.append(("Unity success-only chat RPC callback", success_only, fixture))
    silent_truncation = dict(sources); silent_truncation["unity_poll"] = silent_truncation["unity_poll"].replace('rpcClient?.FailAllPending($"consumed oversized', '_ = rpcClient; // consumed oversized', 1)
    mutations.append(("Unity silent truncation", silent_truncation, fixture))
    godot_small_poll = dict(sources); godot_small_poll["godot_native"] = godot_small_poll["godot_native"].replace("8 * 1024 * 1024", "8192", 1)
    mutations.append(("Godot 8 KiB poll buffer", godot_small_poll, fixture))
    godot_success_only = dict(sources); godot_success_only["godot_poll"] = godot_success_only["godot_poll"].replace('callback.call(reply.get("payload", PackedByteArray()) if int(reply.get("status", -1)) == CitadelProtocol.RPC_STATUS_OK else PackedByteArray())', 'if int(reply.get("status", -1)) == CitadelProtocol.RPC_STATUS_OK: callback.call(reply.get("payload", PackedByteArray()))', 1)
    mutations.append(("Godot success-only chat RPC callback", godot_success_only, fixture))
    godot_silent = dict(sources); godot_silent["godot_poll"] = godot_silent["godot_poll"].replace('int(envelope.get("required_len", 0)))\n\t\t\t_fail_pending_rpc()', 'int(envelope.get("required_len", 0)))\n\t\t\tpass', 1)
    mutations.append(("Godot silent truncation", godot_silent, fixture))
    for name, mutated_sources, mutated_fixture in mutations:
        if not validate(mutated_sources, mutated_fixture): failures.append(f"checker self-test missed {name}")
    return failures


def main() -> int:
    missing = [str(path.relative_to(ROOT)) for path in PATHS.values() if not path.is_file()]
    if missing:
        print("check-chat-live-engine-sdks: missing " + ", ".join(missing), file=sys.stderr); return 1
    sources = {name: path.read_text(encoding="utf-8") for name, path in PATHS.items() if name != "fixture"}
    fixture = json.loads(PATHS["fixture"].read_text(encoding="utf-8"))
    errors = validate(sources, fixture)
    if not errors: errors.extend(self_test(sources, fixture))
    if errors:
        print("check-chat-live-engine-sdks: " + "; ".join(errors), file=sys.stderr); return 1
    print("check-chat-live-engine-sdks: strict authority/apply/snapshot contracts and adversarial self-tests OK")
    return 0


if __name__ == "__main__": raise SystemExit(main())
