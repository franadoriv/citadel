#!/usr/bin/env python3
"""Source/fixture gate for Unreal durable chat live-event completion.

The behavioral contract itself lives in UE automation tests. This gate runs on
hosts without Unreal and fails closed when that smoke or its production seam is
missing; it never claims the Unreal code compiled or executed.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient"
HEADER = BASE / "Public/CitadelChatLive.h"
CPP = BASE / "Private/CitadelChatLive.cpp"
TEST = BASE / "Private/CitadelChatLiveTests.cpp"
BUILD = BASE / "CitadelClient.Build.cs"
SUBSYSTEM_H = BASE / "Public/CitadelClientSubsystem.h"
SUBSYSTEM_CPP = BASE / "Private/CitadelClientSubsystem.cpp"
PUMP_CPP = BASE / "Private/CitadelTransformSync.cpp"
HOOK = ROOT / "clients/unreal/parity-hook.sh"
FIXTURE = ROOT / "tests/fixtures/chat-live-events-v1.json"

errors: list[str] = []

def require_file(path: Path) -> str:
    if not path.is_file():
        errors.append(f"missing {path.relative_to(ROOT)}")
        return ""
    return path.read_text(encoding="utf-8")

def require(text: str, needle: str, source: str) -> None:
    if needle not in text:
        errors.append(f"{source}: missing {needle!r}")

fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
valid_kinds = {entry["kind"] for entry in fixture["valid"]}
expected = {"presence.join", "presence.leave", "typing", "message.create", "message.update", "message.remove", "access.revoked", "resync_required"}
if fixture.get("version") != 1 or valid_kinds != expected:
    errors.append("shared fixture no longer describes the closed v1 eight-variant contract")

header = require_file(HEADER)
cpp = require_file(CPP)
test = require_file(TEST)
build = require_file(BUILD)
subsystem_h = require_file(SUBSYSTEM_H)
subsystem_cpp = require_file(SUBSYSTEM_CPP)
pump_cpp = require_file(PUMP_CPP)
hook = require_file(HOOK)

if "AcknowledgeWatermark" in header or "BuildHistoryAck" in header:
    errors.append("CitadelChatLive.h: history ACK must not be public/Blueprint-authored")
if "ConfirmJoined" in header or "ConfirmRejoined" in header:
    errors.append("CitadelChatLive.h: channel authority setters must not be public or Blueprint-callable")
if "RequestSeal" not in header or "IsSealedForNativeSend" not in cpp:
    errors.append("typed chat requests must reject Blueprint-authored method/JSON structs")
for needle in ("SealedMethod", "SealedPayloadJson", "Method == SealedMethod", "PayloadJson == SealedPayloadJson"):
    if needle not in header:
        errors.append(f"CitadelChatLive.h: sealed request provenance must bind immutable method/JSON ({needle!r})")
request_public = header.partition("struct FCitadelChatRpcRequest")[2].partition("private:")[0]
if "SealForNativeSend" in request_public:
    errors.append("CitadelChatLive.h: native request sealing must not be public")
if "friend class UCitadelClientSubsystem;" not in header:
    errors.append("CitadelChatLive.h: only the subsystem may seal native transport requests")
request_private = header.partition("struct FCitadelChatRpcRequest")[2].partition("};")[0].partition("private:")[2]
request_friends = set(re.findall(r"friend class (\w+);", request_private))
if request_friends != {"UCitadelClientSubsystem", "FCitadelChatRpcRequestTestAccess"}:
    errors.append("CitadelChatLive.h: request sealing friends must be only the subsystem and narrow automation accessor")
if "SealForNativeSend" in cpp or subsystem_cpp.count("SealForNativeSend") != 1:
    errors.append("native private ACK sealing must occur only in the subsystem's correlated ACK branch")

state_body = header.partition("class UCitadelChatLiveState")[2].partition("class UCitadelChatJoinSync")[0]
state_public = state_body.partition("public:")[2].partition("private:")[0]
for authority_method in ("Apply(", "ApplyReconcileSnapshot(", "ConfirmReconcileAcknowledged("):
    if authority_method in state_public:
        errors.append(f"CitadelChatLive.h: {authority_method[:-1]} must remain a private authority transition")
if re.search(r"UFUNCTION\s*\([^)]*BlueprintCallable[^)]*\)\s*ECitadelChatApplyResult\s+Apply", state_body, re.S):
    errors.append("CitadelChatLive.h: Apply must not be Blueprint-callable")

history_body = header.rpartition("class UCitadelChatHistorySync")[2]
history_public = history_body.partition("public:")[2].partition("private:")[0]
if history_public.strip():
    errors.append(
        "CitadelChatLive.h: history sync must expose no public native wrapper over private authority"
    )
for authority_method in (
    "BeginReconcile(", "HandleRpcResponse(", "CancelReconcile(",
    "CancelChannel(", "CancelAll(", "OnDisconnect(",
):
    if authority_method in history_public:
        errors.append(
            f"CitadelChatLive.h: history {authority_method[:-1]} must remain subsystem-private"
        )
history_friends = set(re.findall(r"friend class (\w+);", history_body.partition("private:")[2]))
if history_friends != {"UCitadelClientSubsystem", "FCitadelChatHistorySyncTestAccess"}:
    errors.append(
        "CitadelChatLive.h: history authority friends must be only the subsystem and narrow automation accessor"
    )

for needle in ("UENUM(BlueprintType)", "ECitadelChatEventType", "FCitadelChatPresence", "FCitadelChatMessage", "FCitadelChatLiveEvent", "FCitadelChatJoinResponse", "UCitadelChatLiveEventDispatcher", "UCitadelChatLiveState", "UCitadelChatJoinSync", "UCitadelChatHistorySync", "ECitadelChatHistoryResponseResult", "FCitadelChatRpcRequest", "BuildJoinDirect", "BuildJoinGroup", "BuildJoinCurrentRoom", "BuildLeave", "BuildSend", "BuildHistory", "BuildEdit", "BuildDelete", "BuildModerate", "BuildTyping"):
    require(header, needle, "CitadelChatLive.h")
for needle in ("TryDecode", "KIND_CHAT_EVENT", "OnRawEnvelope.Broadcast", "OnChatEvent.Broadcast", "mismatched last_event_id", "ApplyInitialJoin", "ApplyCorrelatedRejoin", "BeginInitialJoin", "BeginRejoin", "HandleJoinResponse", "ApplyReconcileSnapshot", "ConfirmReconcileAcknowledged", "BuildHistoryAck", "HandleRpcResponse", "SnapshotWatermark", "BeforeMessageId", "ExpireTyping", "AccessRevoked"):
    require(cpp, needle, "CitadelChatLive.cpp")
for needle in ("IMPLEMENT_SIMPLE_AUTOMATION_TEST", "chat-live-events-v1.json", "IPluginManager::Get()", "Valid", "Invalid", "Duplicate", "Gap", "Disconnect", "Rejoin", "Reconcile", "Revoked", "TypingExpiry", "RequestBuilders", "PollOwnerIntegration", "JoinCorrelation", "HistoryStableSnapshotAck", "HistoryMovingSnapshotRestart", "HistoryCorrelation"):
    require(test, needle, "CitadelChatLiveTests.cpp")
for needle in (
    "ConsumedPollTruncationFencesConnection",
    "consumed truncation reports receive failure",
    "consumed truncation fences chat authority like disconnect",
    "consumed truncation fails pending join RPC",
    "consumed truncation fails pending history RPC",
    "consumed truncation makes late history RPC inert",
):
    require(test, needle, "CitadelChatLiveTests.cpp")
for needle in ("class FCitadelChatJoinSyncTestAccess", "class FCitadelChatLiveStateTestAccess", "class FCitadelChatRpcRequestTestAccess", "class FCitadelChatHistorySyncTestAccess", "FCitadelChatJoinSyncTestAccess::BeginInitialJoin", "FCitadelChatJoinSyncTestAccess::HandleJoinResponse", "FCitadelChatLiveStateTestAccess::Apply", "FCitadelChatHistorySyncTestAccess::BeginReconcile", "FCitadelChatHistorySyncTestAccess::HandleRpcResponse", "AutomaticReconcileDriver", "durable gap automatically starts the reconcile driver", "explicit resync automatically starts the reconcile driver", "divergent rejoin automatically starts the reconcile driver", "only one reconcile generation may be in flight per channel", "history reconciliation authority is automation-accessor-only", "typed history factory seals exact transport provenance", "history reconciliation uses the exact chat.history method", "sealed history request builds a transport payload", "mutating sealed history method is rejected", "mutating sealed history payload cannot forge an ACK", "native disconnected send invalidates late join", "explicit disconnect invalidates even after disconnected status", "revocation cancels channel history", "revoked late history response is ignored", "mutating a typed sealed request cannot forge an early ACK"):
    require(test, needle, "CitadelChatLiveTests.cpp")
if not re.search(
    r'TestTrue\s*\(\s*TEXT\("typed history factory seals exact transport provenance"\)\s*,\s*'
    r'FCitadelChatRpcRequestTestAccess::IsSealed\(First\)\s*\)',
    test,
    re.S,
):
    errors.append("CitadelChatLiveTests.cpp: BeginReconcile history request must assert sealed provenance")
if re.search(
    r'TestFalse\s*\([^;]*FCitadelChatRpcRequestTestAccess::IsSealed\(First\)',
    test,
    re.S,
):
    errors.append("CitadelChatLiveTests.cpp: BeginReconcile must not expect an unsealed typed history request")
for private_call in ("BeginInitialJoin", "BeginRejoin", "HandleJoinResponse", "OnAccessRevoked", "OnDisconnect"):
    if re.search(rf"\b(?:Sync|Rejoin)->{private_call}\s*\(", test):
        errors.append(f"CitadelChatLiveTests.cpp: private {private_call} call bypasses FCitadelChatJoinSyncTestAccess")
for private_call in ("Apply", "ApplyReconcileSnapshot", "ConfirmReconcileAcknowledged", "OnDisconnect"):
    if re.search(rf"\bState->{private_call}\s*\(", test):
        errors.append(f"CitadelChatLiveTests.cpp: private state {private_call} call bypasses FCitadelChatLiveStateTestAccess")
for private_call in (
    "BeginReconcile", "HandleRpcResponse", "CancelReconcile",
    "CancelChannel", "CancelAll", "OnDisconnect",
):
    if re.search(rf"\b(?:Sync|HistorySync)->{private_call}\s*\(", test):
        errors.append(
            f"CitadelChatLiveTests.cpp: private history {private_call} call bypasses FCitadelChatHistorySyncTestAccess"
        )
require(build, '"Projects"', "CitadelClient.Build.cs")
require(subsystem_h, "SendChatRequest", "CitadelClientSubsystem.h")
require(subsystem_h, "RejoinChatChannel", "CitadelClientSubsystem.h")
require(subsystem_h, "RouteInboundEnvelope", "CitadelClientSubsystem.h")
require(subsystem_h, "GetChatLiveDispatcher", "CitadelClientSubsystem.h")
require(subsystem_cpp, "BuildRpcPayload(RequestId, Request", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "Send(CitadelWire::KIND_RPC_REQUEST, Payload, true)", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "KIND_RPC_RESPONSE", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "HandleJoinResponse(Payload", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "BeginInitialJoin(RequestId)", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "BeginRejoin(RequestId, ChannelId", "CitadelClientSubsystem.cpp")
require(subsystem_cpp, "GetChatLiveDispatcher()->DispatchEnvelope", "CitadelClientSubsystem.cpp")
if "UFUNCTION(BlueprintCallable, Category=\"Citadel|Chat\") void OnDisconnect" in header:
    errors.append("CitadelChatLive.h: disconnect lifecycle must not be a Blueprint authority control")
if "UFUNCTION(BlueprintCallable, Category = \"Citadel|Chat\")\n    ECitadelStatus BeginChatReconcile" in subsystem_h:
    errors.append("CitadelClientSubsystem.h: reconciliation may only start from authorized internal state")
if re.search(r"UFUNCTION\s*\([^)]*BlueprintCallable[^)]*\)\s*[^;]*BeginChatReconcile", subsystem_h, re.S):
    errors.append("CitadelClientSubsystem.h: BeginChatReconcile must remain internal-only")
join_route = subsystem_cpp.find("HandleJoinResponse(Payload")
history_route = subsystem_cpp.find("HandleRpcResponse(\n        Payload")
if join_route < 0 or history_route < 0 or join_route > history_route:
    errors.append("CitadelClientSubsystem.cpp: correlated join routing must precede history RPC routing")
if "!State->HasChannel(ChannelId) || !State->NeedsReconcile(ChannelId)" not in subsystem_cpp:
    errors.append("CitadelClientSubsystem.cpp: reconciliation must reject unauthorized/non-stale channels")
for needle in ("InvalidateChatConnectionState", "ChatJoinSync->OnDisconnect()", "ChatHistorySync->CancelAll()"):
    if needle not in subsystem_cpp + subsystem_h:
        errors.append(f"Unreal disconnect must centrally invalidate join/history/live state ({needle!r})")
truncation_branch = re.search(
    r"if\s*\(bTruncated\s*\|\|\s*Len\s*>[^)]*PollBuffer\.Num\(\)[^)]*\)\)\s*"
    r"\{(?P<body>.*?)\}",
    subsystem_cpp,
    re.S,
)
if truncation_branch is None:
    errors.append("CitadelClientSubsystem.cpp: missing consumed truncation/oversize branch")
else:
    body = truncation_branch.group("body")
    if "return HandleConsumedPollTruncation(OutPayload);" not in body:
        errors.append("CitadelClientSubsystem.cpp: consumed truncation must use the disconnect-equivalent fencing path")
    if "citadel_client_poll" in body:
        errors.append("CitadelClientSubsystem.cpp: consumed truncation must not repoll")
if subsystem_cpp.count("citadel_client_poll(") != 1:
    errors.append("CitadelClientSubsystem.cpp: Poll must remain one-shot with no truncation repoll")
for needle in (
    "HandleConsumedPollTruncation",
    "InvalidateChatConnectionState();",
    "LastStatus = ECitadelStatus::Receive;",
):
    require(subsystem_cpp, needle, "CitadelClientSubsystem.cpp")
for needle in ("ChatHistorySync->CancelChannel(Event.ChannelId)", "void UCitadelChatHistorySync::CancelChannel"):
    if needle not in subsystem_cpp + cpp:
        errors.append(f"access revocation must cancel channel history before state removal ({needle!r})")
for needle in ("Result == ECitadelChatApplyResult::Gap", "GetRequiredReconcileWatermark(Event.ChannelId", "BeginChatReconcile(Event.ChannelId, RequiredWatermark"):
    if needle not in subsystem_cpp:
        errors.append(f"CitadelClientSubsystem.cpp: durable gaps must automatically start reconciliation ({needle!r})")
for needle in ("HistoryByChannel.Contains(ChannelId)", "IsReconciling(ChannelId)"):
    if needle not in cpp + subsystem_cpp:
        errors.append(f"Unreal reconciliation must allow only one in-flight generation per channel ({needle!r})")
for needle in ("cross-channel rejoin fails closed", "same-watermark rejoin restores current", "same watermark cannot erase stronger reconciliation requirement", "revoked late join cannot revive state", "disconnect generation invalidates late join", "invalid join watermark fails closed", "resync for an unauthorized channel is rejected"):
    require(test, needle, "CitadelChatLiveTests.cpp")
require(pump_cpp, "Client->RouteInboundEnvelope(Kind, Payload)", "CitadelTransformSync.cpp")
require(hook, "tier_b_check.py", "parity-hook.sh")

if errors:
    raise SystemExit("unreal chat live source gate: FAIL\n  - " + "\n  - ".join(errors))
print(f"unreal chat live source gate: OK — fixture valid={len(fixture['valid'])} invalid={len(fixture['invalid'])}; UE automation smoke present (not executed)")
