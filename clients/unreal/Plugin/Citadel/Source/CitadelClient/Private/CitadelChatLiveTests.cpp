#if WITH_DEV_AUTOMATION_TESTS

#include "CitadelChatLive.h"
#include "CitadelClientSubsystem.h"

#include "Interfaces/IPluginManager.h"
#include "Misc/AutomationTest.h"
#include "Misc/FileHelper.h"
#include "Misc/Paths.h"
#include "Serialization/JsonReader.h"
#include "Serialization/JsonSerializer.h"

class FCitadelChatJoinSyncTestAccess
{
public:
    static bool BeginInitialJoin(UCitadelChatJoinSync* Target, int64 RequestId)
    {
        return Target->BeginInitialJoin(RequestId);
    }

    static bool BeginRejoin(
        UCitadelChatJoinSync* Target,
        int64 RequestId,
        const FString& ChannelId,
        UCitadelChatLiveState* State)
    {
        return Target->BeginRejoin(RequestId, ChannelId, State);
    }

    static void OnDisconnect(UCitadelChatJoinSync* Target) { Target->OnDisconnect(); }
    static void OnAccessRevoked(UCitadelChatJoinSync* Target, const FString& ChannelId)
    {
        Target->OnAccessRevoked(ChannelId);
    }

    static ECitadelChatJoinResponseResult HandleJoinResponse(
        UCitadelChatJoinSync* Target,
        const TArray<uint8>& RpcPayload,
        UCitadelChatLiveState* State,
        FString& OutChannelId,
        int64& OutRequiredWatermark,
        FString& OutError)
    {
        return Target->HandleJoinResponse(
            RpcPayload, State, OutChannelId, OutRequiredWatermark, OutError);
    }
};

class FCitadelChatLiveStateTestAccess
{
public:
    static void OnDisconnect(UCitadelChatLiveState* Target) { Target->OnDisconnect(); }
    static ECitadelChatApplyResult Apply(
        UCitadelChatLiveState* Target,
        const FCitadelChatLiveEvent& Event)
    {
        return Target->Apply(Event);
    }
    static bool ApplyReconcileSnapshot(
        UCitadelChatLiveState* Target,
        const FString& ChannelId,
        const TArray<FCitadelChatMessage>& Messages,
        int64 WatermarkEventId)
    {
        return Target->ApplyReconcileSnapshot(ChannelId, Messages, WatermarkEventId);
    }
    static bool ConfirmReconcileAcknowledged(
        UCitadelChatLiveState* Target,
        const FString& ChannelId,
        int64 WatermarkEventId)
    {
        return Target->ConfirmReconcileAcknowledged(ChannelId, WatermarkEventId);
    }
};

class FCitadelChatRpcRequestTestAccess
{
public:
    static bool IsSealed(const FCitadelChatRpcRequest& Request)
    {
        return Request.IsSealedForNativeSend();
    }
};

class FCitadelChatHistorySyncTestAccess
{
public:
    static FCitadelChatRpcRequest BeginReconcile(
        UCitadelChatHistorySync* Target,
        const FString& ChannelId,
        int64 RequiredWatermarkEventId,
        int32 Limit,
        int64 RequestId)
    {
        return Target->BeginReconcile(
            ChannelId, RequiredWatermarkEventId, Limit, RequestId);
    }

    static ECitadelChatHistoryResponseResult HandleRpcResponse(
        UCitadelChatHistorySync* Target,
        const TArray<uint8>& RpcPayload,
        int64 NextRequestId,
        UCitadelChatLiveState* State,
        FCitadelChatRpcRequest& OutNextRequest,
        FString& OutChannelId,
        FString& OutError)
    {
        return Target->HandleRpcResponse(
            RpcPayload, NextRequestId, State, OutNextRequest, OutChannelId, OutError);
    }

    static bool IsReconciling(UCitadelChatHistorySync* Target, const FString& ChannelId)
    {
        return Target->IsReconciling(ChannelId);
    }
};

class FCitadelClientSubsystemChatTestAccess
{
public:
    static void EnsureChatObjects(UCitadelClientSubsystem* Client)
    {
        Client->GetChatLiveState();
        Client->GetChatLiveDispatcher();
        Client->ChatJoinSync = NewObject<UCitadelChatJoinSync>(Client);
        Client->ChatHistorySync = NewObject<UCitadelChatHistorySync>(Client);
    }
    static void ApplyChatLiveEvent(UCitadelClientSubsystem* Client, const FCitadelChatLiveEvent& Event)
    {
        Client->ApplyChatLiveEvent(Event);
    }
    static int64 NextRequestId(const UCitadelClientSubsystem* Client)
    {
        return Client->NextChatRequestId;
    }
    static UCitadelChatJoinSync* JoinSync(UCitadelClientSubsystem* Client)
    {
        return Client->ChatJoinSync;
    }
    static UCitadelChatHistorySync* HistorySync(UCitadelClientSubsystem* Client)
    {
        return Client->ChatHistorySync;
    }
};

namespace
{
FString FixturePath()
{
    const TSharedPtr<IPlugin> Plugin = IPluginManager::Get().FindPlugin(TEXT("Citadel"));
    return Plugin.IsValid()
        ? FPaths::ConvertRelativePathToFull(Plugin->GetBaseDir(), TEXT("../../../../tests/fixtures/chat-live-events-v1.json"))
        : FString();
}

bool LoadFixture(TSharedPtr<FJsonObject>& Out)
{
    FString Text;
    return FFileHelper::LoadFileToString(Text, *FixturePath())
        && FJsonSerializer::Deserialize(TJsonReaderFactory<>::Create(Text), Out)
        && Out.IsValid();
}

FString JsonString(const TSharedPtr<FJsonObject>& Object)
{
    FString Out;
    FJsonSerializer::Serialize(Object.ToSharedRef(), TJsonWriterFactory<>::Create(&Out));
    return Out;
}

TArray<uint8> RpcResponse(int64 RequestId, uint8 Status, const FString& Json)
{
    TArray<uint8> Out;
    const uint64 WireId = static_cast<uint64>(RequestId);
    for (int32 Shift = 56; Shift >= 0; Shift -= 8)
        Out.Add(static_cast<uint8>((WireId >> Shift) & 0xff));
    Out.Add(Status);
    const FTCHARToUTF8 Utf8(*Json);
    Out.Append(reinterpret_cast<const uint8*>(Utf8.Get()), Utf8.Length());
    return Out;
}

bool JoinState(UCitadelChatLiveState* State, const FString& ChannelId, int64 Watermark, int64 RequestId = 1)
{
    UCitadelChatJoinSync* Sync = NewObject<UCitadelChatJoinSync>();
    if (!FCitadelChatJoinSyncTestAccess::BeginInitialJoin(Sync, RequestId)) return false;
    FString JoinedChannel;
    int64 RequiredWatermark = -1;
    FString Error;
    const FString Json = FString::Printf(
        TEXT("{\"channel_id\":\"%s\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":%lld,\"subscription\":\"sub\"}"),
        *ChannelId, Watermark);
    return FCitadelChatJoinSyncTestAccess::HandleJoinResponse(
        Sync, RpcResponse(RequestId, CitadelWire::RPC_STATUS_OK, Json), State,
        JoinedChannel, RequiredWatermark, Error) == ECitadelChatJoinResponseResult::Joined;
}
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatLiveFixtureTest,
    "Citadel.Chat.Live.Fixture.ValidInvalid", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatLiveFixtureTest::RunTest(const FString& Parameters)
{
    TSharedPtr<FJsonObject> Fixture;
    if (!TestTrue(TEXT("shared fixture loads through plugin-relative source path"), LoadFixture(Fixture))) return false;
    const TArray<TSharedPtr<FJsonValue>>* Valid = nullptr;
    const TArray<TSharedPtr<FJsonValue>>* Invalid = nullptr;
    if (!TestTrue(TEXT("Valid fixture array"), Fixture->TryGetArrayField(TEXT("valid"), Valid))
        || !TestTrue(TEXT("Invalid fixture array"), Fixture->TryGetArrayField(TEXT("invalid"), Invalid))) return false;
    TestEqual(TEXT("eight closed valid variants"), Valid->Num(), 8);
    for (const TSharedPtr<FJsonValue>& Item : *Valid)
    {
        const TSharedPtr<FJsonObject> Entry = Item->AsObject();
        FCitadelChatLiveEvent Event;
        FString Error;
        TestTrue(*FString::Printf(TEXT("Valid %s"), *Entry->GetStringField(TEXT("name"))),
            UCitadelChatLiveEventLibrary::TryDecode(JsonString(Entry->GetObjectField(TEXT("event"))), Event, Error));
        TestEqual(TEXT("outer kind matches closed event type"), Event.TypeName(), Entry->GetStringField(TEXT("kind")));
    }
    for (const TSharedPtr<FJsonValue>& Item : *Invalid)
    {
        const TSharedPtr<FJsonObject> Entry = Item->AsObject();
        FString Payload;
        if (!Entry->TryGetStringField(TEXT("payload"), Payload)) Payload = JsonString(Entry->GetObjectField(TEXT("event")));
        FCitadelChatLiveEvent Event;
        FString Error;
        TestFalse(*FString::Printf(TEXT("Invalid %s fails closed"), *Entry->GetStringField(TEXT("name"))),
            UCitadelChatLiveEventLibrary::TryDecode(Payload, Event, Error));
        TestFalse(TEXT("decode failure explains itself"), Error.IsEmpty());
    }
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatLiveStateTest,
    "Citadel.Chat.Live.State.DuplicateGapDisconnectRejoinReconcileRevokedTypingExpiry",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatLiveStateTest::RunTest(const FString& Parameters)
{
    UCitadelChatLiveState* State = NewObject<UCitadelChatLiveState>();
    TestTrue(TEXT("correlated join creates channel state"), JoinState(State, TEXT("ch"), 4));
    FCitadelChatLiveEvent Event;
    Event.Type = ECitadelChatEventType::MessageCreate;
    Event.ChannelId = TEXT("ch");
    Event.EventId = 5;
    Event.Message.Id = 1; Event.Message.Sender = TEXT("alice"); Event.Message.Content = TEXT("x");
    Event.Message.CreatedAtUnixMs = 1; Event.Message.UpdatedAtUnixMs = 1;
    Event.Message.Revision = 1; Event.Message.LastEventId = 5;
    TestEqual(TEXT("first durable event applies"), FCitadelChatLiveStateTestAccess::Apply(State, Event), ECitadelChatApplyResult::Applied);
    TestEqual(TEXT("Duplicate is ignored"), FCitadelChatLiveStateTestAccess::Apply(State, Event), ECitadelChatApplyResult::Duplicate);
    Event.EventId = 7; Event.Message.LastEventId = 7;
    TestEqual(TEXT("Gap requests resync"), FCitadelChatLiveStateTestAccess::Apply(State, Event), ECitadelChatApplyResult::Gap);
    FCitadelChatLiveStateTestAccess::OnDisconnect(State);
    TestFalse(TEXT("Disconnect marks channel not current"), State->IsCurrent(TEXT("ch")));
    FCitadelChatLiveEvent StalePresence;
    StalePresence.Type = ECitadelChatEventType::PresenceJoin;
    StalePresence.ChannelId = TEXT("ch");
    StalePresence.Presence.PresenceId = TEXT("stale");
    StalePresence.Presence.UserId = TEXT("bob");
    TestEqual(TEXT("disconnected delivery fails closed"), FCitadelChatLiveStateTestAccess::Apply(State, StalePresence), ECitadelChatApplyResult::NeedsReconcile);
    UCitadelChatJoinSync* Rejoin = NewObject<UCitadelChatJoinSync>();
    TestTrue(TEXT("rejoin request captures existing channel"), FCitadelChatJoinSyncTestAccess::BeginRejoin(Rejoin, 2, TEXT("ch"), State));
    FString JoinedChannel; int64 RequiredWatermark = -1; FString JoinError;
    const FString RejoinJson = TEXT("{\"channel_id\":\"ch\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":7,\"subscription\":\"sub\"}");
    TestEqual(TEXT("changed watermark requires reconciliation"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Rejoin, RpcResponse(2, 0, RejoinJson), State, JoinedChannel, RequiredWatermark, JoinError),
        ECitadelChatJoinResponseResult::ReconcileRequired);
    TestTrue(TEXT("Rejoin detects authoritative advance"), State->NeedsReconcile(TEXT("ch")));
    const TArray<FCitadelChatMessage> Snapshot = { Event.Message };
    TestTrue(TEXT("stable terminal snapshot applies"), FCitadelChatLiveStateTestAccess::ApplyReconcileSnapshot(State, TEXT("ch"), Snapshot, 7));
    TestFalse(TEXT("applied snapshot waits for ACK response"), State->IsCurrent(TEXT("ch")));
    TestTrue(TEXT("correlated ACK response completes reconcile"), FCitadelChatLiveStateTestAccess::ConfirmReconcileAcknowledged(State, TEXT("ch"), 7));
    Event.Type = ECitadelChatEventType::Typing; Event.EventId = 0;
    Event.Presence.PresenceId = TEXT("p"); Event.Presence.UserId = TEXT("alice");
    Event.bTyping = true; Event.ExpiresAtUnixMs = 10;
    TestEqual(TEXT("typing applies"), FCitadelChatLiveStateTestAccess::Apply(State, Event), ECitadelChatApplyResult::Applied);
    State->ExpireTyping(10);
    TestFalse(TEXT("TypingExpiry clears lost stop"), State->IsTyping(TEXT("ch"), TEXT("p")));
    Event.Type = ECitadelChatEventType::AccessRevoked;
    TestEqual(TEXT("Revoked applies"), FCitadelChatLiveStateTestAccess::Apply(State, Event), ECitadelChatApplyResult::Revoked);
    TestFalse(TEXT("Revoked clears channel helper state"), State->HasChannel(TEXT("ch")));
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatJoinCorrelationTest,
    "Citadel.Chat.Live.JoinCorrelation.StaleCrossChannelRevoked",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatJoinCorrelationTest::RunTest(const FString& Parameters)
{
    UCitadelChatLiveState* State = NewObject<UCitadelChatLiveState>();
    UCitadelChatJoinSync* Sync = NewObject<UCitadelChatJoinSync>();
    FString Channel; int64 Required = -1; FString Error;
    const FString Ch5 = TEXT("{\"channel_id\":\"ch\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":5,\"subscription\":\"sub\"}");
    TestTrue(TEXT("initial request is tracked"), FCitadelChatJoinSyncTestAccess::BeginInitialJoin(Sync, 10));
    TestEqual(TEXT("typed correlated join creates current state"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(10, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Joined);
    TestTrue(TEXT("joined channel is current"), State->IsCurrent(TEXT("ch")));
    TestEqual(TEXT("duplicate response is inert"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(10, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);

    FCitadelChatLiveStateTestAccess::OnDisconnect(State);
    TestTrue(TEXT("rejoin captures existing channel cursor"), FCitadelChatJoinSyncTestAccess::BeginRejoin(Sync, 11, TEXT("ch"), State));
    TestTrue(TEXT("newer rejoin supersedes old request"), FCitadelChatJoinSyncTestAccess::BeginRejoin(Sync, 12, TEXT("ch"), State));
    TestEqual(TEXT("superseded rejoin response is inert"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(11, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);
    const FString Other = TEXT("{\"channel_id\":\"other\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":5,\"subscription\":\"sub\"}");
    TestEqual(TEXT("cross-channel rejoin fails closed"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(12, 0, Other), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Failed);
    TestFalse(TEXT("cross-channel response creates no state"), State->HasChannel(TEXT("other")));

    TestTrue(TEXT("second rejoin is tracked"), FCitadelChatJoinSyncTestAccess::BeginRejoin(Sync, 16, TEXT("ch"), State));
    TestEqual(TEXT("same-watermark rejoin restores current"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(16, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::RejoinedCurrent);
    TestTrue(TEXT("same-watermark state is current"), State->IsCurrent(TEXT("ch")));

    FCitadelChatLiveEvent Stronger;
    Stronger.Type = ECitadelChatEventType::ResyncRequired;
    Stronger.ChannelId = TEXT("ch");
    Stronger.WatermarkEventId = 5;
    TestEqual(TEXT("authorized resync establishes stronger requirement"),
        FCitadelChatLiveStateTestAccess::Apply(State, Stronger), ECitadelChatApplyResult::NeedsReconcile);
    FCitadelChatLiveStateTestAccess::OnDisconnect(State);
    TestTrue(TEXT("rejoin with outstanding reconcile is tracked"), FCitadelChatJoinSyncTestAccess::BeginRejoin(Sync, 15, TEXT("ch"), State));
    TestEqual(TEXT("same watermark cannot erase stronger reconciliation requirement"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(15, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::ReconcileRequired);
    TestFalse(TEXT("stronger requirement remains non-current"), State->IsCurrent(TEXT("ch")));

    TestTrue(TEXT("late initial is pending before revocation"), FCitadelChatJoinSyncTestAccess::BeginInitialJoin(Sync, 13));
    FCitadelChatJoinSyncTestAccess::OnAccessRevoked(Sync, TEXT("ch"));
    FCitadelChatLiveEvent Revoked; Revoked.Type = ECitadelChatEventType::AccessRevoked; Revoked.ChannelId = TEXT("ch");
    FCitadelChatLiveStateTestAccess::Apply(State, Revoked);
    TestEqual(TEXT("revoked late join cannot revive state"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(13, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);
    TestFalse(TEXT("revoked channel remains absent"), State->HasChannel(TEXT("ch")));
    FCitadelChatLiveEvent UnauthorizedResync;
    UnauthorizedResync.Type = ECitadelChatEventType::ResyncRequired;
    UnauthorizedResync.ChannelId = TEXT("unknown");
    UnauthorizedResync.WatermarkEventId = 9;
    TestEqual(TEXT("resync for an unauthorized channel is rejected"),
        FCitadelChatLiveStateTestAccess::Apply(State, UnauthorizedResync), ECitadelChatApplyResult::UnknownChannel);

    const TArray<FString> InvalidJoinResponses = {
        TEXT("{\"channel_id\":\"bad\",\"channel_type\":\"direct\",\"presence\":[],\"subscription\":\"sub\"}"),
        TEXT("{\"channel_id\":\"bad\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":\"5\",\"subscription\":\"sub\"}"),
        TEXT("{\"channel_id\":\"bad\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":1.5,\"subscription\":\"sub\"}"),
        TEXT("{\"channel_id\":\"bad\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":-1,\"subscription\":\"sub\"}"),
        TEXT("{\"channel_id\":\"bad\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":9007199254740992,\"subscription\":\"sub\"}"),
    };
    for (int32 Index = 0; Index < InvalidJoinResponses.Num(); ++Index)
    {
        const int64 RequestId = 20 + Index;
        TestTrue(TEXT("invalid-response request is tracked"), FCitadelChatJoinSyncTestAccess::BeginInitialJoin(Sync, RequestId));
        TestEqual(TEXT("invalid join watermark fails closed"),
            FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(RequestId, 0, InvalidJoinResponses[Index]), State, Channel, Required, Error),
            ECitadelChatJoinResponseResult::Failed);
        TestFalse(TEXT("invalid join creates no state"), State->HasChannel(TEXT("bad")));
    }

    TestTrue(TEXT("old generation initial is tracked"), FCitadelChatJoinSyncTestAccess::BeginInitialJoin(Sync, 14));
    FCitadelChatJoinSyncTestAccess::OnDisconnect(Sync);
    TestEqual(TEXT("disconnect generation invalidates late join"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(Sync, RpcResponse(14, 0, Ch5), State, Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatDisconnectRevocationInvalidationTest,
    "Citadel.Chat.Live.DisconnectRevocationInvalidation",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatDisconnectRevocationInvalidationTest::RunTest(const FString& Parameters)
{
    UCitadelClientSubsystem* Client = NewObject<UCitadelClientSubsystem>();
    FCitadelClientSubsystemChatTestAccess::EnsureChatObjects(Client);
    UCitadelChatLiveState* State = Client->GetChatLiveState();
    UCitadelChatJoinSync* JoinSync = FCitadelClientSubsystemChatTestAccess::JoinSync(Client);
    FString Channel; int64 Required = -1; FString Error;
    const FString Joined = TEXT("{\"channel_id\":\"ch\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":1,\"subscription\":\"sub\"}");

    TestTrue(TEXT("native-disconnect join is pending"),
        FCitadelChatJoinSyncTestAccess::BeginInitialJoin(JoinSync, 601));
    TestEqual(TEXT("send without a native handle reports disconnected"),
        Client->Send(CitadelWire::KIND_RPC_REQUEST, TArray<uint8>(), true),
        ECitadelStatus::Disconnected);
    TestEqual(TEXT("native disconnected send invalidates late join"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(
            JoinSync, RpcResponse(601, CitadelWire::RPC_STATUS_OK, Joined), State,
            Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);

    TestTrue(TEXT("second join is pending after disconnected status"),
        FCitadelChatJoinSyncTestAccess::BeginInitialJoin(JoinSync, 602));
    Client->Disconnect();
    TestEqual(TEXT("explicit disconnect invalidates even after disconnected status"),
        FCitadelChatJoinSyncTestAccess::HandleJoinResponse(
            JoinSync, RpcResponse(602, CitadelWire::RPC_STATUS_OK, Joined), State,
            Channel, Required, Error),
        ECitadelChatJoinResponseResult::Ignored);

    TestTrue(TEXT("authorized channel exists before revocation"), JoinState(State, TEXT("revoked"), 1, 603));
    FCitadelChatLiveEvent Resync;
    Resync.Type = ECitadelChatEventType::ResyncRequired;
    Resync.ChannelId = TEXT("revoked");
    Resync.WatermarkEventId = 2;
    TestEqual(TEXT("authorized channel enters reconciliation"),
        FCitadelChatLiveStateTestAccess::Apply(State, Resync),
        ECitadelChatApplyResult::NeedsReconcile);
    UCitadelChatHistorySync* HistorySync = FCitadelClientSubsystemChatTestAccess::HistorySync(Client);
    TestTrue(TEXT("history operation starts before revocation"),
        FCitadelChatHistorySyncTestAccess::BeginReconcile(
            HistorySync, TEXT("revoked"), 2, 2, 604).bValid);
    TestTrue(TEXT("history bookkeeping exists before revocation"),
        FCitadelChatHistorySyncTestAccess::IsReconciling(HistorySync, TEXT("revoked")));

    FCitadelChatLiveEvent Revoked;
    Revoked.Type = ECitadelChatEventType::AccessRevoked;
    Revoked.ChannelId = TEXT("revoked");
    FCitadelClientSubsystemChatTestAccess::ApplyChatLiveEvent(Client, Revoked);
    TestFalse(TEXT("revocation cancels channel history"),
        FCitadelChatHistorySyncTestAccess::IsReconciling(HistorySync, TEXT("revoked")));
    FCitadelChatRpcRequest Next;
    const FString LatePage = TEXT("{\"items\":[],\"watermark_event_id\":2}");
    TestEqual(TEXT("revoked late history response is ignored"),
        FCitadelChatHistorySyncTestAccess::HandleRpcResponse(
            HistorySync, RpcResponse(604, CitadelWire::RPC_STATUS_OK, LatePage), 605, State,
            Next, Channel, Error),
        ECitadelChatHistoryResponseResult::Ignored);
    TestFalse(TEXT("revoked late history cannot revive channel"), State->HasChannel(TEXT("revoked")));
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatRequestBuildersTest,
    "Citadel.Chat.Live.RequestBuilders", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatRequestBuildersTest::RunTest(const FString& Parameters)
{
    const TArray<FCitadelChatRpcRequest> Requests = {
        UCitadelChatRequestLibrary::BuildJoinDirect(TEXT("bob")),
        UCitadelChatRequestLibrary::BuildJoinGroup(42),
        UCitadelChatRequestLibrary::BuildJoinCurrentRoom(),
        UCitadelChatRequestLibrary::BuildLeave(TEXT("ch")),
        UCitadelChatRequestLibrary::BuildSend(TEXT("ch"), TEXT("hello")),
        UCitadelChatRequestLibrary::BuildHistory(TEXT("ch"), 50, 0),
        UCitadelChatRequestLibrary::BuildEdit(TEXT("ch"), 1, TEXT("edited")),
        UCitadelChatRequestLibrary::BuildDelete(TEXT("ch"), 1),
        UCitadelChatRequestLibrary::BuildModerate(TEXT("ch"), 1),
        UCitadelChatRequestLibrary::BuildTyping(TEXT("ch"), true),
    };
    const TArray<FString> Methods = { TEXT("chat.join"), TEXT("chat.join"), TEXT("chat.join"), TEXT("chat.leave"), TEXT("chat.send"), TEXT("chat.history"), TEXT("chat.edit"), TEXT("chat.delete"), TEXT("chat.moderate"), TEXT("chat.typing") };
    for (int32 Index = 0; Index < Requests.Num(); ++Index)
    {
        TestEqual(TEXT("domain method"), Requests[Index].Method, Methods[Index]);
        TestTrue(TEXT("typed request factory seals transport provenance"),
            FCitadelChatRpcRequestTestAccess::IsSealed(Requests[Index]));
        TSharedPtr<FJsonObject> Json;
        TestTrue(TEXT("builder emits object JSON"), FJsonSerializer::Deserialize(TJsonReaderFactory<>::Create(Requests[Index].PayloadJson), Json) && Json.IsValid());
    }
    TArray<uint8> Frame;
    TestTrue(TEXT("RPC request envelope builds without manual JSON"), UCitadelChatRequestLibrary::BuildRpcPayload(9, Requests[4], Frame));
    FCitadelChatRpcRequest Forged;
    Forged.Method = TEXT("chat.history");
    Forged.PayloadJson = TEXT("{\"acknowledge_watermark\":999}");
    Forged.bValid = true;
    TestFalse(TEXT("Blueprint-authored request structs cannot forge an early ACK"),
        UCitadelChatRequestLibrary::BuildRpcPayload(10, Forged, Frame));
    FCitadelChatRpcRequest MutatedSealed = Requests[5];
    MutatedSealed.PayloadJson = TEXT("{\"channel_id\":\"ch\",\"limit\":1,\"acknowledge_watermark\":999}");
    TestFalse(TEXT("mutating a typed sealed request cannot forge an early ACK"),
        UCitadelChatRequestLibrary::BuildRpcPayload(11, MutatedSealed, Frame));
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatPollOwnerIntegrationTest,
    "Citadel.Chat.Live.PollOwnerIntegration", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatPollOwnerIntegrationTest::RunTest(const FString& Parameters)
{
    UCitadelClientSubsystem* Client = NewObject<UCitadelClientSubsystem>();
    TestNotNull(TEXT("subsystem exposes its live dispatcher"), Client->GetChatLiveDispatcher());
    const FTCHARToUTF8 Utf8(TEXT("{bad"));
    TArray<uint8> Invalid;
    Invalid.Append(reinterpret_cast<const uint8*>(Utf8.Get()), Utf8.Length());
    TestFalse(TEXT("the subsystem routing seam reaches fail-closed chat decoding"),
        Client->RouteInboundEnvelope(CitadelWire::KIND_CHAT_EVENT, Invalid));
    TestTrue(TEXT("non-chat raw envelopes remain routable"),
        Client->RouteInboundEnvelope(CitadelWire::KIND_NOTIFICATION, TArray<uint8>()));
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatAutomaticReconcileDriverTest,
    "Citadel.Chat.Live.AutomaticReconcileDriver.GapResyncAndInflightBounded",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatAutomaticReconcileDriverTest::RunTest(const FString& Parameters)
{
    UCitadelClientSubsystem* Client = NewObject<UCitadelClientSubsystem>();
    FCitadelClientSubsystemChatTestAccess::EnsureChatObjects(Client);
    UCitadelChatLiveState* State = Client->GetChatLiveState();
    TestTrue(TEXT("subsystem state joined"), JoinState(State, TEXT("auto"), 1));

    FCitadelChatLiveEvent Gap;
    Gap.Type = ECitadelChatEventType::MessageCreate;
    Gap.ChannelId = TEXT("auto");
    Gap.EventId = 3;
    Gap.Message.Id = 1;
    Gap.Message.Sender = TEXT("alice");
    Gap.Message.Content = TEXT("gap");
    Gap.Message.CreatedAtUnixMs = 1;
    Gap.Message.UpdatedAtUnixMs = 1;
    Gap.Message.Revision = 1;
    Gap.Message.LastEventId = 3;
    const int64 BeforeGap = FCitadelClientSubsystemChatTestAccess::NextRequestId(Client);
    FCitadelClientSubsystemChatTestAccess::ApplyChatLiveEvent(Client, Gap);
    TestEqual(TEXT("durable gap automatically starts the reconcile driver"),
        FCitadelClientSubsystemChatTestAccess::NextRequestId(Client), BeforeGap + 1);
    TestTrue(TEXT("gap remains stale while history is unavailable"), State->NeedsReconcile(TEXT("auto")));

    FCitadelChatLiveEvent Resync;
    Resync.Type = ECitadelChatEventType::ResyncRequired;
    Resync.ChannelId = TEXT("auto");
    Resync.WatermarkEventId = 4;
    Resync.Scopes.Add(ECitadelChatResyncScope::History);
    const int64 BeforeResync = FCitadelClientSubsystemChatTestAccess::NextRequestId(Client);
    FCitadelClientSubsystemChatTestAccess::ApplyChatLiveEvent(Client, Resync);
    TestEqual(TEXT("explicit resync automatically starts the reconcile driver"),
        FCitadelClientSubsystemChatTestAccess::NextRequestId(Client), BeforeResync + 1);
    TestFalse(TEXT("explicit resync never lowers the authoritative watermark"),
        FCitadelChatLiveStateTestAccess::ApplyReconcileSnapshot(State, TEXT("auto"), TArray<FCitadelChatMessage>(), 3));

    FCitadelChatLiveStateTestAccess::OnDisconnect(State);
    UCitadelChatJoinSync* JoinSync = FCitadelClientSubsystemChatTestAccess::JoinSync(Client);
    TestTrue(TEXT("divergent rejoin is tracked"),
        FCitadelChatJoinSyncTestAccess::BeginRejoin(JoinSync, 500, TEXT("auto"), State));
    const int64 BeforeRejoin = FCitadelClientSubsystemChatTestAccess::NextRequestId(Client);
    const FString Diverged = TEXT("{\"channel_id\":\"auto\",\"channel_type\":\"direct\",\"presence\":[],\"watermark_event_id\":5,\"subscription\":\"sub\"}");
    Client->RouteInboundEnvelope(
        CitadelWire::KIND_RPC_RESPONSE,
        RpcResponse(500, CitadelWire::RPC_STATUS_OK, Diverged));
    TestEqual(TEXT("divergent rejoin automatically starts the reconcile driver"),
        FCitadelClientSubsystemChatTestAccess::NextRequestId(Client), BeforeRejoin + 1);

    UCitadelChatHistorySync* Sync = NewObject<UCitadelChatHistorySync>();
    TestTrue(TEXT("first reconcile for a channel starts"),
        FCitadelChatHistorySyncTestAccess::BeginReconcile(
            Sync, TEXT("auto"), 3, 50, 901).bValid);
    TestFalse(TEXT("only one reconcile generation may be in flight per channel"),
        FCitadelChatHistorySyncTestAccess::BeginReconcile(
            Sync, TEXT("auto"), 3, 50, 902).bValid);
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatHistoryStableSnapshotAckTest,
    "Citadel.Chat.Live.HistoryStableSnapshotAck", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatHistoryStableSnapshotAckTest::RunTest(const FString& Parameters)
{
    UCitadelChatLiveState* State = NewObject<UCitadelChatLiveState>();
    TestTrue(TEXT("state joined"), JoinState(State, TEXT("ch"), 1));
    FCitadelChatLiveEvent Resync;
    Resync.Type = ECitadelChatEventType::ResyncRequired;
    Resync.ChannelId = TEXT("ch");
    Resync.WatermarkEventId = 3;
    TestEqual(TEXT("resync enters reconciliation"), FCitadelChatLiveStateTestAccess::Apply(State, Resync), ECitadelChatApplyResult::NeedsReconcile);

    UCitadelChatHistorySync* Sync = NewObject<UCitadelChatHistorySync>();
    const FCitadelChatRpcRequest First = FCitadelChatHistorySyncTestAccess::BeginReconcile(
        Sync, TEXT("ch"), 3, 2, 101);
    TestTrue(TEXT("history reconciliation authority is automation-accessor-only"), First.bValid);
    TestTrue(TEXT("typed history factory seals exact transport provenance"),
        FCitadelChatRpcRequestTestAccess::IsSealed(First));
    TestEqual(TEXT("history reconciliation uses the exact chat.history method"),
        First.Method, FString(TEXT("chat.history")));
    TSharedPtr<FJsonObject> FirstPayload;
    TestTrue(TEXT("history reconciliation payload is valid JSON"),
        FJsonSerializer::Deserialize(
            TJsonReaderFactory<>::Create(First.PayloadJson), FirstPayload)
            && FirstPayload.IsValid());
    if (FirstPayload.IsValid())
    {
        FString PayloadChannel;
        double PayloadLimit = 0.0;
        TestEqual(TEXT("history reconciliation payload has only channel and limit"),
            FirstPayload->Values.Num(), 2);
        TestTrue(TEXT("history reconciliation payload carries its exact channel"),
            FirstPayload->TryGetStringField(TEXT("channel_id"), PayloadChannel));
        TestEqual(TEXT("history reconciliation payload channel is exact"),
            PayloadChannel, FString(TEXT("ch")));
        TestTrue(TEXT("history reconciliation payload carries its exact limit"),
            FirstPayload->TryGetNumberField(TEXT("limit"), PayloadLimit));
        TestEqual(TEXT("history reconciliation payload limit is exact"), PayloadLimit, 2.0);
    }
    TestFalse(TEXT("ordinary history never carries ACK"), First.PayloadJson.Contains(TEXT("acknowledge_watermark")));
    TArray<uint8> HistoryFrame;
    TestTrue(TEXT("sealed history request builds a transport payload"),
        UCitadelChatRequestLibrary::BuildRpcPayload(101, First, HistoryFrame));
    FCitadelChatRpcRequest MutatedHistoryMethod = First;
    MutatedHistoryMethod.Method = TEXT("chat.send");
    TestFalse(TEXT("mutating sealed history method is rejected"),
        UCitadelChatRequestLibrary::BuildRpcPayload(101, MutatedHistoryMethod, HistoryFrame));
    FCitadelChatRpcRequest MutatedHistoryPayload = First;
    MutatedHistoryPayload.PayloadJson = TEXT("{\"channel_id\":\"ch\",\"limit\":2,\"acknowledge_watermark\":3}");
    TestFalse(TEXT("mutating sealed history payload cannot forge an ACK"),
        UCitadelChatRequestLibrary::BuildRpcPayload(101, MutatedHistoryPayload, HistoryFrame));

    FCitadelChatRpcRequest Next;
    FString Channel;
    FString Error;
    const FString Page1 = TEXT("{\"items\":[{\"id\":3,\"sender\":\"a\",\"content\":\"c\",\"created_at_unix_ms\":3,\"updated_at_unix_ms\":3,\"revision\":1,\"last_event_id\":3,\"deleted\":false},{\"id\":2,\"sender\":\"a\",\"content\":\"b\",\"created_at_unix_ms\":2,\"updated_at_unix_ms\":2,\"revision\":1,\"last_event_id\":2,\"deleted\":false}],\"watermark_event_id\":3}");
    TestEqual(TEXT("full newest-first page requests older page"),
        FCitadelChatHistorySyncTestAccess::HandleRpcResponse(
            Sync, RpcResponse(101, CitadelWire::RPC_STATUS_OK, Page1), 102, State, Next, Channel, Error),
        ECitadelChatHistoryResponseResult::RequestNextPage);
    TestTrue(TEXT("second page uses exclusive oldest cursor"), Next.PayloadJson.Contains(TEXT("\"before_message_id\":2")));
    TestFalse(TEXT("continuation page has no ACK"), Next.PayloadJson.Contains(TEXT("acknowledge_watermark")));

    const FString Page2 = TEXT("{\"items\":[{\"id\":1,\"sender\":\"a\",\"content\":\"a\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":1,\"deleted\":false}],\"watermark_event_id\":3}");
    TestEqual(TEXT("terminal page applies snapshot then creates private ACK request"),
        FCitadelChatHistorySyncTestAccess::HandleRpcResponse(
            Sync, RpcResponse(102, CitadelWire::RPC_STATUS_OK, Page2), 103, State, Next, Channel, Error),
        ECitadelChatHistoryResponseResult::AwaitingAck);
    TestTrue(TEXT("private ACK carries the applied snapshot watermark"), Next.PayloadJson.Contains(TEXT("\"acknowledge_watermark\":3")));
    TestFalse(TEXT("history state machine cannot self-authorize ACK transport"),
        FCitadelChatRpcRequestTestAccess::IsSealed(Next));
    TestFalse(TEXT("channel cannot be current before ACK response"), State->IsCurrent(TEXT("ch")));

    const FString Ack = TEXT("{\"items\":[],\"watermark_event_id\":3}");
    TestEqual(TEXT("correlated successful ACK response makes state current"),
        FCitadelChatHistorySyncTestAccess::HandleRpcResponse(
            Sync, RpcResponse(103, CitadelWire::RPC_STATUS_OK, Ack), 0, State, Next, Channel, Error),
        ECitadelChatHistoryResponseResult::Current);
    TestTrue(TEXT("channel is current only after ACK response"), State->IsCurrent(TEXT("ch")));
    const TArray<FCitadelChatMessage> Messages = State->GetMessages(TEXT("ch"));
    TestEqual(TEXT("snapshot contains all three pages"), Messages.Num(), 3);
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatHistoryMovingSnapshotRestartTest,
    "Citadel.Chat.Live.HistoryMovingSnapshotRestart", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatHistoryMovingSnapshotRestartTest::RunTest(const FString& Parameters)
{
    UCitadelChatLiveState* State = NewObject<UCitadelChatLiveState>();
    TestTrue(TEXT("state joined"), JoinState(State, TEXT("ch"), 1));
    FCitadelChatLiveEvent Resync; Resync.Type = ECitadelChatEventType::ResyncRequired;
    Resync.ChannelId = TEXT("ch"); Resync.WatermarkEventId = 3; FCitadelChatLiveStateTestAccess::Apply(State, Resync);
    UCitadelChatHistorySync* Sync = NewObject<UCitadelChatHistorySync>();
    FCitadelChatHistorySyncTestAccess::BeginReconcile(Sync, TEXT("ch"), 3, 1, 201);
    FCitadelChatRpcRequest Next; FString Channel; FString Error;
    const FString First = TEXT("{\"items\":[{\"id\":2,\"sender\":\"a\",\"content\":\"b\",\"created_at_unix_ms\":2,\"updated_at_unix_ms\":2,\"revision\":1,\"last_event_id\":2,\"deleted\":false}],\"watermark_event_id\":3}");
    TestEqual(TEXT("first full page continues"), FCitadelChatHistorySyncTestAccess::HandleRpcResponse(Sync, RpcResponse(201, 0, First), 202, State, Next, Channel, Error), ECitadelChatHistoryResponseResult::RequestNextPage);
    const FString Moved = TEXT("{\"items\":[{\"id\":1,\"sender\":\"a\",\"content\":\"a\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":1,\"deleted\":false}],\"watermark_event_id\":4}");
    TestEqual(TEXT("moving watermark discards pages and restarts at newest"), FCitadelChatHistorySyncTestAccess::HandleRpcResponse(Sync, RpcResponse(202, 0, Moved), 203, State, Next, Channel, Error), ECitadelChatHistoryResponseResult::Restarted);
    TestFalse(TEXT("restart request has no stale before cursor"), Next.PayloadJson.Contains(TEXT("before_message_id")));
    TestFalse(TEXT("restart never ACKs moving snapshot"), Next.PayloadJson.Contains(TEXT("acknowledge_watermark")));
    TestFalse(TEXT("moving snapshot remains non-current"), State->IsCurrent(TEXT("ch")));
    return true;
}

IMPLEMENT_SIMPLE_AUTOMATION_TEST(FCitadelChatHistoryCorrelationTest,
    "Citadel.Chat.Live.HistoryCorrelation", EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)
bool FCitadelChatHistoryCorrelationTest::RunTest(const FString& Parameters)
{
    UCitadelChatLiveState* State = NewObject<UCitadelChatLiveState>();
    TestTrue(TEXT("state joined"), JoinState(State, TEXT("ch"), 0));
    FCitadelChatLiveEvent Resync; Resync.Type = ECitadelChatEventType::ResyncRequired;
    Resync.ChannelId = TEXT("ch"); Resync.WatermarkEventId = 2; FCitadelChatLiveStateTestAccess::Apply(State, Resync);
    UCitadelChatHistorySync* Sync = NewObject<UCitadelChatHistorySync>();
    FCitadelChatHistorySyncTestAccess::BeginReconcile(Sync, TEXT("ch"), 2, 2, 301);
    FCitadelChatRpcRequest Next; FString Channel; FString Error;
    TestEqual(TEXT("unknown request id is ignored"), FCitadelChatHistorySyncTestAccess::HandleRpcResponse(Sync, RpcResponse(999, 0, TEXT("{}")), 302, State, Next, Channel, Error), ECitadelChatHistoryResponseResult::Ignored);
    const FString Ascending = TEXT("{\"items\":[{\"id\":1,\"sender\":\"a\",\"content\":\"a\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":1,\"deleted\":false},{\"id\":2,\"sender\":\"a\",\"content\":\"b\",\"created_at_unix_ms\":2,\"updated_at_unix_ms\":2,\"revision\":1,\"last_event_id\":2,\"deleted\":false}],\"watermark_event_id\":2}");
    TestEqual(TEXT("non-newest-first page fails closed"), FCitadelChatHistorySyncTestAccess::HandleRpcResponse(Sync, RpcResponse(301, 0, Ascending), 302, State, Next, Channel, Error), ECitadelChatHistoryResponseResult::Failed);
    TestFalse(TEXT("protocol failure explains itself"), Error.IsEmpty());
    TestFalse(TEXT("invalid page never makes state current"), State->IsCurrent(TEXT("ch")));
    return true;
}

#endif
