#include "CitadelChatLive.h"

#include "CitadelWire.h"
#include "Dom/JsonObject.h"
#include "Serialization/JsonReader.h"
#include "Serialization/JsonSerializer.h"

namespace
{
constexpr int64 MaxExactJsonInteger = 9007199254740991LL;

bool RequiredString(const TSharedPtr<FJsonObject>& Json, const TCHAR* Name, FString& Out, FString& Error, bool bAllowEmpty = false)
{
    if (!Json.IsValid() || !Json->TryGetStringField(Name, Out) || (!bAllowEmpty && Out.IsEmpty()))
    {
        Error = FString::Printf(TEXT("missing or empty string field: %s"), Name);
        return false;
    }
    return true;
}

bool RequiredInt64(const TSharedPtr<FJsonObject>& Json, const TCHAR* Name, int64& Out, FString& Error, int64 Minimum)
{
    const TSharedPtr<FJsonValue>* Value = Json.IsValid() ? Json->Values.Find(Name) : nullptr;
    if (Value == nullptr || !Value->IsValid() || !(*Value)->TryGetNumber(Out)
        || Out < Minimum || Out > MaxExactJsonInteger)
    {
        Error = FString::Printf(TEXT("missing or invalid integer field: %s"), Name);
        return false;
    }
    return true;
}

bool RequiredBool(const TSharedPtr<FJsonObject>& Json, const TCHAR* Name, bool& Out, FString& Error)
{
    if (!Json.IsValid() || !Json->TryGetBoolField(Name, Out))
    {
        Error = FString::Printf(TEXT("missing boolean field: %s"), Name);
        return false;
    }
    return true;
}

bool ParsePresence(const TSharedPtr<FJsonObject>& Json, FCitadelChatPresence& Out, FString& Error)
{
    return RequiredString(Json, TEXT("presence_id"), Out.PresenceId, Error)
        && RequiredString(Json, TEXT("user_id"), Out.UserId, Error);
}

bool ParseMessage(const TSharedPtr<FJsonObject>& Json, FCitadelChatMessage& Out, FString& Error)
{
    if (!RequiredInt64(Json, TEXT("id"), Out.Id, Error, 1)
        || !RequiredString(Json, TEXT("sender"), Out.Sender, Error)
        || !RequiredString(Json, TEXT("content"), Out.Content, Error, true)
        || !RequiredInt64(Json, TEXT("created_at_unix_ms"), Out.CreatedAtUnixMs, Error, 0)
        || !RequiredInt64(Json, TEXT("updated_at_unix_ms"), Out.UpdatedAtUnixMs, Error, 0)
        || !RequiredInt64(Json, TEXT("revision"), Out.Revision, Error, 1)
        || !RequiredInt64(Json, TEXT("last_event_id"), Out.LastEventId, Error, 1)
        || !RequiredBool(Json, TEXT("deleted"), Out.bDeleted, Error))
    {
        return false;
    }
    if (Out.UpdatedAtUnixMs < Out.CreatedAtUnixMs)
    {
        Error = TEXT("message updated_at_unix_ms precedes created_at_unix_ms");
        return false;
    }
    return true;
}

FCitadelChatRpcRequest InvalidRequest(const FString& Error)
{
    FCitadelChatRpcRequest Result;
    Result.Error = Error;
    return Result;
}

FCitadelChatRpcRequest MakeRequest(const TCHAR* Method, const TSharedRef<FJsonObject>& Body)
{
    FCitadelChatRpcRequest Result;
    Result.Method = Method;
    Result.bValid = FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Result.PayloadJson));
    if (!Result.bValid) Result.Error = TEXT("could not serialize chat request");
    return Result;
}

TSharedRef<FJsonObject> ChannelBody(const FString& ChannelId)
{
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    Body->SetStringField(TEXT("channel_id"), ChannelId);
    return Body;
}

bool ValidChannel(const FString& ChannelId)
{
    return !ChannelId.IsEmpty();
}

FCitadelChatRpcRequest BuildHistoryAck(const FString& ChannelId, int64 WatermarkEventId)
{
    if (!ValidChannel(ChannelId) || WatermarkEventId < 0 || WatermarkEventId > MaxExactJsonInteger)
        return InvalidRequest(TEXT("valid channel and non-negative ACK watermark are required"));
    // before_message_id=1 is an intentionally terminal page because message ids
    // begin at one. The ACK remains coupled to a real authorized history reply.
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId);
    Body->SetNumberField(TEXT("limit"), 1);
    Body->SetNumberField(TEXT("before_message_id"), 1);
    Body->SetNumberField(TEXT("acknowledge_watermark"), WatermarkEventId);
    return MakeRequest(TEXT("chat.history"), Body);
}

bool DecodeRpcResponse(
    const TArray<uint8>& Payload,
    int64& OutRequestId,
    uint8& OutStatus,
    TSharedPtr<FJsonObject>& OutJson,
    FString& OutError)
{
    if (Payload.Num() < 9)
    {
        OutError = TEXT("truncated RPC response");
        return false;
    }
    uint64 WireId = 0;
    for (int32 Index = 0; Index < 8; ++Index) WireId = (WireId << 8) | Payload[Index];
    if (WireId == 0 || WireId > static_cast<uint64>(TNumericLimits<int64>::Max()))
    {
        OutError = TEXT("invalid RPC response request id");
        return false;
    }
    OutRequestId = static_cast<int64>(WireId);
    OutStatus = Payload[8];
    const int32 JsonLength = Payload.Num() - 9;
    const FUTF8ToTCHAR Converted(
        reinterpret_cast<const ANSICHAR*>(Payload.GetData() + 9), JsonLength);
    const FString JsonText(Converted.Length(), Converted.Get());
    if (OutStatus != CitadelWire::RPC_STATUS_OK)
    {
        OutError = JsonText.IsEmpty() ? TEXT("chat history RPC failed") : JsonText;
        return true;
    }
    if (!FJsonSerializer::Deserialize(TJsonReaderFactory<>::Create(JsonText), OutJson) || !OutJson.IsValid())
    {
        OutError = TEXT("malformed chat history response JSON");
        return false;
    }
    return true;
}

bool ParseHistoryPage(
    const TSharedPtr<FJsonObject>& Json,
    TArray<FCitadelChatMessage>& OutMessages,
    int64& OutWatermark,
    FString& OutError)
{
    if (!RequiredInt64(Json, TEXT("watermark_event_id"), OutWatermark, OutError, 0)) return false;
    const TArray<TSharedPtr<FJsonValue>>* Items = nullptr;
    if (!Json->TryGetArrayField(TEXT("items"), Items) || Items == nullptr)
    {
        OutError = TEXT("missing history items array");
        return false;
    }
    for (const TSharedPtr<FJsonValue>& Item : *Items)
    {
        const TSharedPtr<FJsonObject> MessageJson =
            Item.IsValid() && Item->Type == EJson::Object ? Item->AsObject() : nullptr;
        FCitadelChatMessage Message;
        if (!MessageJson.IsValid() || !ParseMessage(MessageJson, Message, OutError))
        {
            if (OutError.IsEmpty()) OutError = TEXT("invalid history message");
            return false;
        }
        if (Message.LastEventId > OutWatermark)
        {
            OutError = TEXT("history message exceeds response watermark");
            return false;
        }
        OutMessages.Add(Message);
    }
    return true;
}

bool ParseJoinResponse(
    const TSharedPtr<FJsonObject>& Json,
    FCitadelChatJoinResponse& Out,
    FString& OutError)
{
    FString ChannelType;
    if (!RequiredString(Json, TEXT("channel_id"), Out.ChannelId, OutError)
        || !RequiredString(Json, TEXT("channel_type"), ChannelType, OutError)
        || !RequiredInt64(Json, TEXT("watermark_event_id"), Out.WatermarkEventId, OutError, 0)
        || !RequiredString(Json, TEXT("subscription"), Out.SubscriptionId, OutError))
        return false;
    if (ChannelType == TEXT("direct")) Out.ChannelType = ECitadelChatChannelType::Direct;
    else if (ChannelType == TEXT("group")) Out.ChannelType = ECitadelChatChannelType::Group;
    else if (ChannelType == TEXT("room")) Out.ChannelType = ECitadelChatChannelType::Room;
    else { OutError = TEXT("unknown chat.join channel_type"); return false; }

    const TArray<TSharedPtr<FJsonValue>>* Presences = nullptr;
    if (!Json->TryGetArrayField(TEXT("presence"), Presences) || Presences == nullptr)
    {
        OutError = TEXT("missing chat.join presence array");
        return false;
    }
    TSet<FString> PresenceIds;
    for (const TSharedPtr<FJsonValue>& Value : *Presences)
    {
        const TSharedPtr<FJsonObject> PresenceJson =
            Value.IsValid() && Value->Type == EJson::Object ? Value->AsObject() : nullptr;
        FCitadelChatPresence Presence;
        if (!PresenceJson.IsValid() || !ParsePresence(PresenceJson, Presence, OutError)
            || PresenceIds.Contains(Presence.PresenceId))
        {
            if (OutError.IsEmpty()) OutError = TEXT("invalid or duplicate chat.join presence");
            return false;
        }
        PresenceIds.Add(Presence.PresenceId);
        Out.Presences.Add(Presence);
    }
    return true;
}
}

FString FCitadelChatLiveEvent::TypeName() const
{
    switch (Type)
    {
    case ECitadelChatEventType::PresenceJoin: return TEXT("presence.join");
    case ECitadelChatEventType::PresenceLeave: return TEXT("presence.leave");
    case ECitadelChatEventType::Typing: return TEXT("typing");
    case ECitadelChatEventType::MessageCreate: return TEXT("message.create");
    case ECitadelChatEventType::MessageUpdate: return TEXT("message.update");
    case ECitadelChatEventType::MessageRemove: return TEXT("message.remove");
    case ECitadelChatEventType::AccessRevoked: return TEXT("access.revoked");
    case ECitadelChatEventType::ResyncRequired: return TEXT("resync_required");
    }
    return FString();
}

bool FCitadelChatLiveEvent::IsDurable() const
{
    return Type == ECitadelChatEventType::MessageCreate
        || Type == ECitadelChatEventType::MessageUpdate
        || Type == ECitadelChatEventType::MessageRemove;
}

bool UCitadelChatLiveEventLibrary::TryDecode(const TArray<uint8>& Payload, FCitadelChatLiveEvent& OutEvent, FString& OutError)
{
    if (Payload.IsEmpty())
    {
        OutError = TEXT("empty chat event payload");
        return false;
    }
    const FUTF8ToTCHAR Converted(reinterpret_cast<const ANSICHAR*>(Payload.GetData()), Payload.Num());
    return TryDecode(FString(Converted.Length(), Converted.Get()), OutEvent, OutError);
}

bool UCitadelChatLiveEventLibrary::TryDecode(const FString& PayloadJson, FCitadelChatLiveEvent& OutEvent, FString& OutError)
{
    OutEvent = FCitadelChatLiveEvent();
    OutError.Reset();
    TSharedPtr<FJsonObject> Json;
    if (!FJsonSerializer::Deserialize(TJsonReaderFactory<>::Create(PayloadJson), Json) || !Json.IsValid())
    {
        OutError = TEXT("malformed chat event JSON object");
        return false;
    }
    int64 Version = 0;
    FString Type;
    if (!RequiredInt64(Json, TEXT("version"), Version, OutError, 1) || Version != 1)
    {
        if (OutError.IsEmpty()) OutError = TEXT("unsupported chat event version");
        else if (Version != 1) OutError = TEXT("unsupported chat event version");
        return false;
    }
    if (!RequiredString(Json, TEXT("type"), Type, OutError)
        || !RequiredString(Json, TEXT("channel_id"), OutEvent.ChannelId, OutError)) return false;
    OutEvent.Version = 1;

    if (Type == TEXT("presence.join")) OutEvent.Type = ECitadelChatEventType::PresenceJoin;
    else if (Type == TEXT("presence.leave")) OutEvent.Type = ECitadelChatEventType::PresenceLeave;
    else if (Type == TEXT("typing")) OutEvent.Type = ECitadelChatEventType::Typing;
    else if (Type == TEXT("message.create")) OutEvent.Type = ECitadelChatEventType::MessageCreate;
    else if (Type == TEXT("message.update")) OutEvent.Type = ECitadelChatEventType::MessageUpdate;
    else if (Type == TEXT("message.remove")) OutEvent.Type = ECitadelChatEventType::MessageRemove;
    else if (Type == TEXT("access.revoked")) OutEvent.Type = ECitadelChatEventType::AccessRevoked;
    else if (Type == TEXT("resync_required")) OutEvent.Type = ECitadelChatEventType::ResyncRequired;
    else { OutError = TEXT("unknown chat event type"); return false; }

    FString ChannelType;
    if (Json->TryGetStringField(TEXT("channel_type"), ChannelType))
    {
        if (ChannelType == TEXT("direct")) OutEvent.ChannelType = ECitadelChatChannelType::Direct;
        else if (ChannelType == TEXT("group")) OutEvent.ChannelType = ECitadelChatChannelType::Group;
        else if (ChannelType == TEXT("room")) OutEvent.ChannelType = ECitadelChatChannelType::Room;
        else { OutError = TEXT("unknown chat channel_type"); return false; }
    }

    if (OutEvent.Type == ECitadelChatEventType::PresenceJoin
        || OutEvent.Type == ECitadelChatEventType::PresenceLeave
        || OutEvent.Type == ECitadelChatEventType::Typing
        || OutEvent.Type == ECitadelChatEventType::AccessRevoked)
    {
        const TSharedPtr<FJsonObject>* Presence = nullptr;
        if (!Json->TryGetObjectField(TEXT("presence"), Presence) || Presence == nullptr
            || !ParsePresence(*Presence, OutEvent.Presence, OutError))
        {
            if (OutError.IsEmpty()) OutError = TEXT("missing presence object");
            return false;
        }
    }

    if (OutEvent.Type == ECitadelChatEventType::Typing)
    {
        return RequiredBool(Json, TEXT("typing"), OutEvent.bTyping, OutError)
            && RequiredInt64(Json, TEXT("expires_at"), OutEvent.ExpiresAtUnixMs, OutError, 0);
    }

    if (OutEvent.IsDurable())
    {
        const TSharedPtr<FJsonObject>* Message = nullptr;
        if (!RequiredInt64(Json, TEXT("event_id"), OutEvent.EventId, OutError, 1)
            || !Json->TryGetObjectField(TEXT("message"), Message) || Message == nullptr
            || !ParseMessage(*Message, OutEvent.Message, OutError))
        {
            if (OutError.IsEmpty()) OutError = TEXT("missing message object");
            return false;
        }
        if (OutEvent.Message.LastEventId != OutEvent.EventId)
        {
            OutError = TEXT("mismatched last_event_id");
            return false;
        }
        if (OutEvent.Type == ECitadelChatEventType::MessageCreate
            && (OutEvent.Message.Revision != 1 || OutEvent.Message.bDeleted
                || OutEvent.Message.CreatedAtUnixMs != OutEvent.Message.UpdatedAtUnixMs))
        {
            OutError = TEXT("invalid message.create invariants");
            return false;
        }
        if (OutEvent.Type == ECitadelChatEventType::MessageUpdate
            && (OutEvent.Message.Revision < 2 || OutEvent.Message.bDeleted))
        {
            OutError = TEXT("invalid message.update invariants");
            return false;
        }
        if (OutEvent.Type == ECitadelChatEventType::MessageRemove
            && (OutEvent.Message.Revision < 2 || !OutEvent.Message.bDeleted || !OutEvent.Message.Content.IsEmpty()))
        {
            OutError = TEXT("invalid message.remove invariants");
            return false;
        }
    }

    if (OutEvent.Type == ECitadelChatEventType::ResyncRequired)
    {
        if (!RequiredInt64(Json, TEXT("watermark_event_id"), OutEvent.WatermarkEventId, OutError, 0)) return false;
        const TArray<TSharedPtr<FJsonValue>>* Scopes = nullptr;
        if (!Json->TryGetArrayField(TEXT("scopes"), Scopes) || Scopes == nullptr || Scopes->IsEmpty())
        {
            OutError = TEXT("missing resync scopes");
            return false;
        }
        for (const TSharedPtr<FJsonValue>& ScopeValue : *Scopes)
        {
            FString Scope;
            if (!ScopeValue.IsValid() || !ScopeValue->TryGetString(Scope)) { OutError = TEXT("invalid resync scope"); return false; }
            if (Scope == TEXT("history")) OutEvent.Scopes.Add(ECitadelChatResyncScope::History);
            else if (Scope == TEXT("presence")) OutEvent.Scopes.Add(ECitadelChatResyncScope::Presence);
            else { OutError = TEXT("unknown resync scope"); return false; }
        }
    }
    return true;
}

bool UCitadelChatLiveEventDispatcher::DispatchEnvelope(int32 Kind, const TArray<uint8>& Payload)
{
    OnRawEnvelope.Broadcast(Kind, Payload);
    if (Kind != static_cast<int32>(CitadelWire::KIND_CHAT_EVENT)) return true;
    FCitadelChatLiveEvent Event;
    FString Error;
    if (!UCitadelChatLiveEventLibrary::TryDecode(Payload, Event, Error))
    {
        OnChatEventRejected.Broadcast(Error);
        return false;
    }
    OnChatEvent.Broadcast(Event);
    return true;
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildJoinDirect(const FString& OtherUserId)
{
    if (OtherUserId.IsEmpty()) return InvalidRequest(TEXT("other user id is required"));
    const TSharedRef<FJsonObject> Target = MakeShared<FJsonObject>();
    Target->SetStringField(TEXT("kind"), TEXT("direct")); Target->SetStringField(TEXT("other_user_id"), OtherUserId);
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>(); Body->SetObjectField(TEXT("target"), Target);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.join"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildJoinGroup(int64 GroupId)
{
    if (GroupId <= 0 || GroupId > MaxExactJsonInteger)
        return InvalidRequest(TEXT("group id must be a positive exact JSON integer"));
    const TSharedRef<FJsonObject> Target = MakeShared<FJsonObject>();
    Target->SetStringField(TEXT("kind"), TEXT("group")); Target->SetNumberField(TEXT("group_id"), GroupId);
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>(); Body->SetObjectField(TEXT("target"), Target);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.join"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildJoinCurrentRoom()
{
    const TSharedRef<FJsonObject> Target = MakeShared<FJsonObject>(); Target->SetStringField(TEXT("kind"), TEXT("room"));
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>(); Body->SetObjectField(TEXT("target"), Target);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.join"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildLeave(const FString& ChannelId)
{
    return ValidChannel(ChannelId)
        ? SealTypedFactoryResult(MakeRequest(TEXT("chat.leave"), ChannelBody(ChannelId)))
        : InvalidRequest(TEXT("channel id is required"));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildSend(const FString& ChannelId, const FString& Content)
{
    if (!ValidChannel(ChannelId)) return InvalidRequest(TEXT("channel id is required"));
    if (Content.IsEmpty()) return InvalidRequest(TEXT("content is required"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetStringField(TEXT("content"), Content);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.send"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildHistory(const FString& ChannelId, int32 Limit, int64 BeforeMessageId)
{
    if (!ValidChannel(ChannelId)) return InvalidRequest(TEXT("channel id is required"));
    if (Limit < 1 || Limit > 200) return InvalidRequest(TEXT("history limit must be between 1 and 200"));
    if (BeforeMessageId < 0 || BeforeMessageId > MaxExactJsonInteger)
        return InvalidRequest(TEXT("history cursor must be an exact non-negative JSON integer"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetNumberField(TEXT("limit"), Limit);
    if (BeforeMessageId > 0) Body->SetNumberField(TEXT("before_message_id"), BeforeMessageId);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.history"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildEdit(const FString& ChannelId, int64 MessageId, const FString& Content)
{
    if (!ValidChannel(ChannelId) || MessageId <= 0 || MessageId > MaxExactJsonInteger || Content.IsEmpty())
        return InvalidRequest(TEXT("channel, exact positive message id, and content are required"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetNumberField(TEXT("message_id"), MessageId); Body->SetStringField(TEXT("content"), Content);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.edit"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildDelete(const FString& ChannelId, int64 MessageId)
{
    if (!ValidChannel(ChannelId) || MessageId <= 0 || MessageId > MaxExactJsonInteger)
        return InvalidRequest(TEXT("channel and exact positive message id are required"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetNumberField(TEXT("message_id"), MessageId);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.delete"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildModerate(const FString& ChannelId, int64 MessageId)
{
    if (!ValidChannel(ChannelId) || MessageId <= 0 || MessageId > MaxExactJsonInteger)
        return InvalidRequest(TEXT("channel and exact positive message id are required"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetNumberField(TEXT("message_id"), MessageId);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.moderate"), Body));
}

FCitadelChatRpcRequest FCitadelChatRpcRequest::BuildTyping(const FString& ChannelId, bool bTyping)
{
    if (!ValidChannel(ChannelId)) return InvalidRequest(TEXT("channel id is required"));
    const TSharedRef<FJsonObject> Body = ChannelBody(ChannelId); Body->SetBoolField(TEXT("typing"), bTyping);
    return SealTypedFactoryResult(MakeRequest(TEXT("chat.typing"), Body));
}

FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildJoinDirect(const FString& OtherUserId) { return FCitadelChatRpcRequest::BuildJoinDirect(OtherUserId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildJoinGroup(int64 GroupId) { return FCitadelChatRpcRequest::BuildJoinGroup(GroupId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildJoinCurrentRoom() { return FCitadelChatRpcRequest::BuildJoinCurrentRoom(); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildLeave(const FString& ChannelId) { return FCitadelChatRpcRequest::BuildLeave(ChannelId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildSend(const FString& ChannelId, const FString& Content) { return FCitadelChatRpcRequest::BuildSend(ChannelId, Content); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildHistory(const FString& ChannelId, int32 Limit, int64 BeforeMessageId) { return FCitadelChatRpcRequest::BuildHistory(ChannelId, Limit, BeforeMessageId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildEdit(const FString& ChannelId, int64 MessageId, const FString& Content) { return FCitadelChatRpcRequest::BuildEdit(ChannelId, MessageId, Content); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildDelete(const FString& ChannelId, int64 MessageId) { return FCitadelChatRpcRequest::BuildDelete(ChannelId, MessageId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildModerate(const FString& ChannelId, int64 MessageId) { return FCitadelChatRpcRequest::BuildModerate(ChannelId, MessageId); }
FCitadelChatRpcRequest UCitadelChatRequestLibrary::BuildTyping(const FString& ChannelId, bool bTyping) { return FCitadelChatRpcRequest::BuildTyping(ChannelId, bTyping); }

bool UCitadelChatRequestLibrary::BuildRpcPayload(int64 RequestId, const FCitadelChatRpcRequest& Request, TArray<uint8>& OutPayload)
{
    OutPayload.Reset();
    if (RequestId <= 0 || !Request.bValid || !Request.IsSealedForNativeSend() || Request.Method.IsEmpty()) return false;
    const FTCHARToUTF8 Method(*Request.Method);
    const FTCHARToUTF8 Json(*Request.PayloadJson);
    if (Method.Length() <= 0 || Method.Length() > TNumericLimits<uint16>::Max()) return false;
    const uint64 WireId = static_cast<uint64>(RequestId);
    OutPayload.Reserve(8 + 2 + Method.Length() + Json.Length());
    for (int32 Shift = 56; Shift >= 0; Shift -= 8) OutPayload.Add(static_cast<uint8>((WireId >> Shift) & 0xff));
    OutPayload.Add(static_cast<uint8>((Method.Length() >> 8) & 0xff));
    OutPayload.Add(static_cast<uint8>(Method.Length() & 0xff));
    OutPayload.Append(reinterpret_cast<const uint8*>(Method.Get()), Method.Length());
    OutPayload.Append(reinterpret_cast<const uint8*>(Json.Get()), Json.Length());
    return true;
}

bool UCitadelChatLiveState::ApplyInitialJoin(const FCitadelChatJoinResponse& Response)
{
    if (Response.ChannelId.IsEmpty() || Response.WatermarkEventId < 0
        || Channels.Contains(Response.ChannelId)) return false;
    FChannelState State;
    State.Cursor = Response.WatermarkEventId;
    State.ExpectedWatermark = Response.WatermarkEventId;
    for (const FCitadelChatPresence& Presence : Response.Presences)
    {
        if (Presence.PresenceId.IsEmpty() || Presence.UserId.IsEmpty()
            || State.Presences.Contains(Presence.PresenceId)) return false;
        State.Presences.Add(Presence.PresenceId, Presence);
    }
    Channels.Add(Response.ChannelId, MoveTemp(State));
    return true;
}

bool UCitadelChatLiveState::CaptureRejoinCursor(const FString& ChannelId, int64& OutCursor) const
{
    const FChannelState* State = Channels.Find(ChannelId);
    if (State == nullptr || !State->bNeedsRejoin) return false;
    OutCursor = State->Cursor;
    return true;
}

bool UCitadelChatLiveState::GetRequiredReconcileWatermark(
    const FString& ChannelId,
    int64& OutWatermark) const
{
    const FChannelState* State = Channels.Find(ChannelId);
    if (State == nullptr || !State->bNeedsReconcile) return false;
    OutWatermark = State->ExpectedWatermark;
    return true;
}

ECitadelChatJoinResponseResult UCitadelChatLiveState::ApplyCorrelatedRejoin(
    const FCitadelChatJoinResponse& Response,
    int64 CapturedCursor,
    int64& OutRequiredWatermark)
{
    FChannelState* State = Channels.Find(Response.ChannelId);
    if (State == nullptr || !State->bNeedsRejoin || State->Cursor != CapturedCursor)
        return ECitadelChatJoinResponseResult::Failed;

    TMap<FString, FCitadelChatPresence> Presences;
    for (const FCitadelChatPresence& Presence : Response.Presences)
    {
        if (Presence.PresenceId.IsEmpty() || Presence.UserId.IsEmpty()
            || Presences.Contains(Presence.PresenceId))
            return ECitadelChatJoinResponseResult::Failed;
        Presences.Add(Presence.PresenceId, Presence);
    }
    State->Presences = MoveTemp(Presences);
    State->TypingExpiries.Reset();
    State->bNeedsRejoin = false;
    if (Response.WatermarkEventId == CapturedCursor
        && !State->bNeedsReconcile && State->ExpectedWatermark <= CapturedCursor)
    {
        State->bCurrent = true;
        State->ExpectedWatermark = CapturedCursor;
        State->bSnapshotApplied = false;
        OutRequiredWatermark = CapturedCursor;
        return ECitadelChatJoinResponseResult::RejoinedCurrent;
    }

    State->bCurrent = false;
    State->bNeedsReconcile = true;
    State->bSnapshotApplied = false;
    State->ExpectedWatermark = FMath::Max(
        FMath::Max(State->ExpectedWatermark, CapturedCursor), Response.WatermarkEventId);
    OutRequiredWatermark = State->ExpectedWatermark;
    return ECitadelChatJoinResponseResult::ReconcileRequired;
}

void UCitadelChatLiveState::OnDisconnect()
{
    for (TPair<FString, FChannelState>& Pair : Channels)
    {
        Pair.Value.bCurrent = false;
        Pair.Value.bNeedsRejoin = true;
        Pair.Value.Presences.Reset();
        Pair.Value.TypingExpiries.Reset();
    }
}

bool UCitadelChatJoinSync::BeginInitialJoin(int64 RequestId)
{
    if (RequestId <= 0 || PendingJoins.Contains(RequestId)) return false;
    PendingJoins.Add(RequestId, FPendingJoin{Generation, FString(), 0, false});
    return true;
}

bool UCitadelChatJoinSync::BeginRejoin(
    int64 RequestId,
    const FString& ChannelId,
    UCitadelChatLiveState* State)
{
    if (RequestId <= 0 || ChannelId.IsEmpty() || State == nullptr
        || PendingJoins.Contains(RequestId)) return false;
    int64 Cursor = 0;
    if (!State->CaptureRejoinCursor(ChannelId, Cursor)) return false;
    for (auto It = PendingJoins.CreateIterator(); It; ++It)
        if (It.Value().bRejoin && It.Value().ExpectedChannelId == ChannelId)
            It.RemoveCurrent();
    PendingJoins.Add(RequestId, FPendingJoin{Generation, ChannelId, Cursor, true});
    return true;
}

void UCitadelChatJoinSync::CancelRequest(int64 RequestId)
{
    PendingJoins.Remove(RequestId);
}

void UCitadelChatJoinSync::OnDisconnect()
{
    PendingJoins.Reset();
    Generation = Generation == TNumericLimits<int64>::Max() ? 1 : Generation + 1;
}

void UCitadelChatJoinSync::OnAccessRevoked(const FString& ChannelId)
{
    for (auto It = PendingJoins.CreateIterator(); It; ++It)
        if (!It.Value().bRejoin || It.Value().ExpectedChannelId == ChannelId)
            It.RemoveCurrent();
}

ECitadelChatJoinResponseResult UCitadelChatJoinSync::HandleJoinResponse(
    const TArray<uint8>& RpcPayload,
    UCitadelChatLiveState* State,
    FString& OutChannelId,
    int64& OutRequiredWatermark,
    FString& OutError)
{
    OutChannelId.Reset();
    OutRequiredWatermark = -1;
    OutError.Reset();
    int64 RequestId = 0;
    uint8 Status = CitadelWire::RPC_STATUS_ERROR;
    TSharedPtr<FJsonObject> Json;
    if (!DecodeRpcResponse(RpcPayload, RequestId, Status, Json, OutError))
    {
        if (RequestId <= 0 || !PendingJoins.Contains(RequestId))
            return ECitadelChatJoinResponseResult::Ignored;
        PendingJoins.Remove(RequestId);
        return ECitadelChatJoinResponseResult::Failed;
    }
    const FPendingJoin* Found = PendingJoins.Find(RequestId);
    if (Found == nullptr) return ECitadelChatJoinResponseResult::Ignored;
    const FPendingJoin Pending = *Found;
    PendingJoins.Remove(RequestId); // consume before any untrusted payload mutation
    if (Pending.Generation != Generation) return ECitadelChatJoinResponseResult::Ignored;
    if (Status != CitadelWire::RPC_STATUS_OK)
    {
        if (OutError.IsEmpty()) OutError = TEXT("chat.join RPC failed");
        return ECitadelChatJoinResponseResult::Failed;
    }
    if (State == nullptr)
    {
        OutError = TEXT("chat live state is required");
        return ECitadelChatJoinResponseResult::Failed;
    }

    FCitadelChatJoinResponse Response;
    if (!ParseJoinResponse(Json, Response, OutError))
        return ECitadelChatJoinResponseResult::Failed;
    OutChannelId = Response.ChannelId;
    if (!Pending.bRejoin)
    {
        if (!State->ApplyInitialJoin(Response))
        {
            OutError = TEXT("initial join cannot overwrite tracked channel state");
            return ECitadelChatJoinResponseResult::Failed;
        }
        OutRequiredWatermark = Response.WatermarkEventId;
        return ECitadelChatJoinResponseResult::Joined;
    }
    if (Response.ChannelId != Pending.ExpectedChannelId)
    {
        OutError = TEXT("chat.join response channel does not match rejoin request");
        return ECitadelChatJoinResponseResult::Failed;
    }
    const ECitadelChatJoinResponseResult Result = State->ApplyCorrelatedRejoin(
        Response, Pending.CapturedCursor, OutRequiredWatermark);
    if (Result == ECitadelChatJoinResponseResult::Failed)
        OutError = TEXT("stale chat rejoin response rejected by channel state");
    return Result;
}

ECitadelChatApplyResult UCitadelChatLiveState::Apply(const FCitadelChatLiveEvent& Event)
{
    if (Event.Type == ECitadelChatEventType::AccessRevoked)
    {
        Channels.Remove(Event.ChannelId);
        return ECitadelChatApplyResult::Revoked;
    }
    FChannelState* State = Channels.Find(Event.ChannelId);
    if (State == nullptr) return ECitadelChatApplyResult::UnknownChannel;
    if (Event.Type == ECitadelChatEventType::ResyncRequired)
    {
        State->bCurrent = false; State->bNeedsReconcile = true;
        State->ExpectedWatermark = FMath::Max(
            State->ExpectedWatermark, FMath::Max(State->Cursor, Event.WatermarkEventId));
        State->bSnapshotApplied = false;
        return ECitadelChatApplyResult::NeedsReconcile;
    }
    if (!Event.IsDurable() && (!State->bCurrent || State->bNeedsRejoin || State->bNeedsReconcile))
        return ECitadelChatApplyResult::NeedsReconcile;
    if (Event.IsDurable())
    {
        if (Event.EventId <= State->Cursor) return ECitadelChatApplyResult::Duplicate;
        if (!State->bCurrent || State->bNeedsRejoin || State->bNeedsReconcile) return ECitadelChatApplyResult::NeedsReconcile;
        if (Event.EventId != State->Cursor + 1)
        {
            State->bCurrent = false; State->bNeedsReconcile = true; State->ExpectedWatermark = Event.EventId;
            State->bSnapshotApplied = false;
            return ECitadelChatApplyResult::Gap;
        }
        State->Cursor = Event.EventId;
        State->Messages.Add(Event.Message.Id, Event.Message);
    }
    else if (Event.Type == ECitadelChatEventType::PresenceJoin) State->Presences.Add(Event.Presence.PresenceId, Event.Presence);
    else if (Event.Type == ECitadelChatEventType::PresenceLeave)
    {
        State->Presences.Remove(Event.Presence.PresenceId); State->TypingExpiries.Remove(Event.Presence.PresenceId);
    }
    else if (Event.Type == ECitadelChatEventType::Typing)
    {
        if (Event.bTyping) State->TypingExpiries.Add(Event.Presence.PresenceId, Event.ExpiresAtUnixMs);
        else State->TypingExpiries.Remove(Event.Presence.PresenceId);
    }
    return ECitadelChatApplyResult::Applied;
}

bool UCitadelChatLiveState::ApplyReconcileSnapshot(
    const FString& ChannelId,
    const TArray<FCitadelChatMessage>& Messages,
    int64 WatermarkEventId)
{
    FChannelState* State = Channels.Find(ChannelId);
    if (State == nullptr || !State->bNeedsReconcile || State->bNeedsRejoin
        || WatermarkEventId < State->ExpectedWatermark) return false;
    TMap<int64, FCitadelChatMessage> Replacement;
    for (const FCitadelChatMessage& Message : Messages)
    {
        if (Message.Id <= 0 || Message.LastEventId <= 0
            || Message.LastEventId > WatermarkEventId || Replacement.Contains(Message.Id)) return false;
        Replacement.Add(Message.Id, Message);
    }
    State->Messages = MoveTemp(Replacement);
    State->Cursor = WatermarkEventId;
    State->ExpectedWatermark = WatermarkEventId;
    State->bSnapshotApplied = true;
    State->bCurrent = false;
    return true;
}

bool UCitadelChatLiveState::ConfirmReconcileAcknowledged(const FString& ChannelId, int64 WatermarkEventId)
{
    FChannelState* State = Channels.Find(ChannelId);
    if (State == nullptr || !State->bNeedsReconcile || !State->bSnapshotApplied
        || State->bNeedsRejoin || WatermarkEventId != State->ExpectedWatermark) return false;
    State->bSnapshotApplied = false;
    State->bNeedsReconcile = false;
    State->bCurrent = true;
    return true;
}

void UCitadelChatLiveState::ExpireTyping(int64 NowUnixMs)
{
    for (TPair<FString, FChannelState>& Channel : Channels)
    {
        for (auto It = Channel.Value.TypingExpiries.CreateIterator(); It; ++It)
            if (It.Value() <= NowUnixMs) It.RemoveCurrent();
    }
}

bool UCitadelChatLiveState::HasChannel(const FString& ChannelId) const { return Channels.Contains(ChannelId); }
bool UCitadelChatLiveState::IsCurrent(const FString& ChannelId) const { const FChannelState* S = Channels.Find(ChannelId); return S && S->bCurrent; }
bool UCitadelChatLiveState::NeedsReconcile(const FString& ChannelId) const { const FChannelState* S = Channels.Find(ChannelId); return S && S->bNeedsReconcile; }
bool UCitadelChatLiveState::IsTyping(const FString& ChannelId, const FString& PresenceId) const { const FChannelState* S = Channels.Find(ChannelId); return S && S->TypingExpiries.Contains(PresenceId); }

TArray<FCitadelChatPresence> UCitadelChatLiveState::GetPresences(const FString& ChannelId) const
{
    TArray<FCitadelChatPresence> Out;
    if (const FChannelState* State = Channels.Find(ChannelId)) State->Presences.GenerateValueArray(Out);
    return Out;
}

TArray<FCitadelChatMessage> UCitadelChatLiveState::GetMessages(const FString& ChannelId) const
{
    TArray<FCitadelChatMessage> Out;
    if (const FChannelState* State = Channels.Find(ChannelId)) State->Messages.GenerateValueArray(Out);
    Out.Sort([](const FCitadelChatMessage& A, const FCitadelChatMessage& B) { return A.Id > B.Id; });
    return Out;
}

FCitadelChatRpcRequest UCitadelChatHistorySync::BeginReconcile(
    const FString& ChannelId,
    int64 RequiredWatermarkEventId,
    int32 Limit,
    int64 RequestId)
{
    if (!ValidChannel(ChannelId) || RequiredWatermarkEventId < 0
        || RequiredWatermarkEventId > MaxExactJsonInteger
        || Limit < 1 || Limit > 200 || RequestId <= 0 || PendingRequests.Contains(RequestId)
        || HistoryByChannel.Contains(ChannelId))
        return InvalidRequest(TEXT("invalid or colliding history reconciliation request"));

    FHistoryState& History = HistoryByChannel.FindOrAdd(ChannelId);
    History = FHistoryState();
    History.Limit = Limit;
    History.RequiredWatermark = RequiredWatermarkEventId;
    History.Generation = 1;

    FCitadelChatRpcRequest Request = UCitadelChatRequestLibrary::BuildHistory(ChannelId, Limit, 0);
    if (Request.bValid)
        PendingRequests.Add(RequestId, FPendingRequest{ChannelId, History.Generation, false});
    else
        HistoryByChannel.Remove(ChannelId);
    return Request;
}

void UCitadelChatHistorySync::CancelReconcile(const FString& ChannelId)
{
    CancelChannel(ChannelId);
}

void UCitadelChatHistorySync::CancelChannel(const FString& ChannelId)
{
    for (auto It = PendingRequests.CreateIterator(); It; ++It)
        if (It.Value().ChannelId == ChannelId) It.RemoveCurrent();
    HistoryByChannel.Remove(ChannelId);
}

void UCitadelChatHistorySync::CancelAll()
{
    PendingRequests.Reset();
    HistoryByChannel.Reset();
}

bool UCitadelChatHistorySync::IsReconciling(const FString& ChannelId) const
{
    return HistoryByChannel.Contains(ChannelId);
}

ECitadelChatHistoryResponseResult UCitadelChatHistorySync::HandleRpcResponse(
    const TArray<uint8>& RpcPayload,
    int64 NextRequestId,
    UCitadelChatLiveState* State,
    FCitadelChatRpcRequest& OutNextRequest,
    FString& OutChannelId,
    FString& OutError)
{
    OutNextRequest = FCitadelChatRpcRequest();
    OutChannelId.Reset();
    OutError.Reset();

    int64 RequestId = 0;
    uint8 Status = CitadelWire::RPC_STATUS_ERROR;
    TSharedPtr<FJsonObject> Json;
    if (!DecodeRpcResponse(RpcPayload, RequestId, Status, Json, OutError))
    {
        // A trustworthy header still owns the malformed response: consume and
        // cancel only that correlated operation rather than leaving stale state.
        if (RequestId > 0)
        {
            if (const FPendingRequest* Malformed = PendingRequests.Find(RequestId))
            {
                const FPendingRequest Copy = *Malformed;
                OutChannelId = Copy.ChannelId;
                PendingRequests.Remove(RequestId);
                if (const FHistoryState* History = HistoryByChannel.Find(Copy.ChannelId);
                    History != nullptr && History->Generation == Copy.Generation)
                    HistoryByChannel.Remove(Copy.ChannelId);
            }
        }
        return ECitadelChatHistoryResponseResult::Failed;
    }

    const FPendingRequest* FoundPending = PendingRequests.Find(RequestId);
    if (FoundPending == nullptr) return ECitadelChatHistoryResponseResult::Ignored;
    const FPendingRequest Pending = *FoundPending;
    PendingRequests.Remove(RequestId); // consume once; duplicate responses are stale
    OutChannelId = Pending.ChannelId;

    FHistoryState* History = HistoryByChannel.Find(Pending.ChannelId);
    if (History == nullptr || History->Generation != Pending.Generation)
        return ECitadelChatHistoryResponseResult::Ignored;

    const auto Fail = [this, &Pending, &OutError](const FString& Error)
    {
        OutError = Error;
        for (auto It = PendingRequests.CreateIterator(); It; ++It)
            if (It.Value().ChannelId == Pending.ChannelId && It.Value().Generation == Pending.Generation)
                It.RemoveCurrent();
        HistoryByChannel.Remove(Pending.ChannelId);
        return ECitadelChatHistoryResponseResult::Failed;
    };

    if (Status != CitadelWire::RPC_STATUS_OK)
        return Fail(OutError.IsEmpty() ? TEXT("chat history RPC failed") : OutError);
    if (State == nullptr) return Fail(TEXT("chat live state is required"));

    TArray<FCitadelChatMessage> Page;
    int64 Watermark = -1;
    if (!ParseHistoryPage(Json, Page, Watermark, OutError)) return Fail(OutError);

    if (Pending.bAck)
    {
        if (!Page.IsEmpty() || Watermark != History->SnapshotWatermark)
            return Fail(TEXT("ACK response did not confirm the applied terminal snapshot"));
        if (!State->ConfirmReconcileAcknowledged(Pending.ChannelId, Watermark))
            return Fail(TEXT("chat state rejected correlated ACK confirmation"));
        HistoryByChannel.Remove(Pending.ChannelId);
        return ECitadelChatHistoryResponseResult::Current;
    }

    if (Watermark < History->RequiredWatermark)
        return Fail(TEXT("history watermark predates required resync watermark"));
    if (History->SnapshotWatermark < 0)
    {
        History->SnapshotWatermark = Watermark;
    }
    else if (Watermark != History->SnapshotWatermark)
    {
        if (NextRequestId <= 0 || PendingRequests.Contains(NextRequestId))
            return Fail(TEXT("moving snapshot restart request id is invalid or colliding"));
        ++History->Generation;
        History->SnapshotWatermark = -1;
        History->BeforeMessageId = 0;
        History->Messages.Reset();
        OutNextRequest = UCitadelChatRequestLibrary::BuildHistory(Pending.ChannelId, History->Limit, 0);
        if (!OutNextRequest.bValid) return Fail(OutNextRequest.Error);
        PendingRequests.Add(NextRequestId, FPendingRequest{Pending.ChannelId, History->Generation, false});
        return ECitadelChatHistoryResponseResult::Restarted;
    }

    if (Page.Num() > History->Limit)
        return Fail(TEXT("history page exceeds requested limit"));
    int64 PreviousId = TNumericLimits<int64>::Max();
    for (const FCitadelChatMessage& Message : Page)
    {
        if (Message.Id >= PreviousId)
            return Fail(TEXT("history page is not strictly newest-first"));
        if (History->BeforeMessageId > 0 && Message.Id >= History->BeforeMessageId)
            return Fail(TEXT("history page violated exclusive before_message_id"));
        if (History->Messages.ContainsByPredicate(
                [&Message](const FCitadelChatMessage& Existing) { return Existing.Id == Message.Id; }))
            return Fail(TEXT("history page repeated a message id"));
        PreviousId = Message.Id;
        History->Messages.Add(Message);
    }

    if (Page.Num() == History->Limit)
    {
        if (NextRequestId <= 0 || PendingRequests.Contains(NextRequestId))
            return Fail(TEXT("history continuation request id is invalid or colliding"));
        History->BeforeMessageId = Page.Last().Id;
        OutNextRequest = UCitadelChatRequestLibrary::BuildHistory(
            Pending.ChannelId, History->Limit, History->BeforeMessageId);
        if (!OutNextRequest.bValid) return Fail(OutNextRequest.Error);
        PendingRequests.Add(NextRequestId, FPendingRequest{Pending.ChannelId, History->Generation, false});
        return ECitadelChatHistoryResponseResult::RequestNextPage;
    }

    if (!State->ApplyReconcileSnapshot(
            Pending.ChannelId, History->Messages, History->SnapshotWatermark))
        return Fail(TEXT("chat state did not confirm terminal snapshot application"));
    if (NextRequestId <= 0 || PendingRequests.Contains(NextRequestId))
        return Fail(TEXT("private ACK request id is invalid or colliding"));
    OutNextRequest = BuildHistoryAck(Pending.ChannelId, History->SnapshotWatermark);
    if (!OutNextRequest.bValid) return Fail(OutNextRequest.Error);
    PendingRequests.Add(NextRequestId, FPendingRequest{Pending.ChannelId, History->Generation, true});
    return ECitadelChatHistoryResponseResult::AwaitingAck;
}
