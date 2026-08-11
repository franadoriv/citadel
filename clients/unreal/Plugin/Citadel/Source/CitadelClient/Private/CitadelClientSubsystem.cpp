// CitadelClientSubsystem.cpp — implementation of the header-driven UE wrapper.
//
// Every native call here goes straight into the canonical `citadel_client.h`
// (pulled in via the subsystem header). There are NO re-declared prototypes:
// the signatures are exactly whatever the cbindgen header currently exports, so
// a C ABI change surfaces as a compile error in this TU (and in the Tier-B TU
// that CI compiles even without Unreal).
//
// Device/custom account auth is HTTP (`/v1/auth/{device,custom}`) and returns a
// session token. Realtime admission is a separate C ABI handshake on the active
// transport (`AuthenticateRealtimeGuest` / `AuthenticateRealtimeWithSessionToken`).
#include "CitadelClientSubsystem.h"

#include "HttpModule.h"
#include "Interfaces/IHttpResponse.h"
#include "Dom/JsonObject.h"
#include "Serialization/JsonReader.h"
#include "Serialization/JsonSerializer.h"

namespace
{
    // Map the C enum to the Blueprint-facing UE enum. Kept adjacent to the calls
    // so a new CitadelStatus variant is easy to wire through.
    ECitadelStatus ToUe(CitadelStatus Status)
    {
        return static_cast<ECitadelStatus>(static_cast<uint8>(Status));
    }
}

ECitadelStatus UCitadelClientSubsystem::ConnectQuic(const FString& Address, const FString& ServerName, bool bInsecure)
{
    const FTCHARToUTF8 AddrUtf8(*Address);
    const FTCHARToUTF8 NameUtf8(*ServerName);
    const CitadelStatus Status = citadel_client_connect_quic(
        AddrUtf8.Get(), NameUtf8.Get(), bInsecure, &Handle);
    LastStatus = ToUe(Status);
    return LastStatus;
}

ECitadelStatus UCitadelClientSubsystem::ConnectWebSocket(const FString& Url)
{
    const FTCHARToUTF8 UrlUtf8(*Url);
    const CitadelStatus Status = citadel_client_connect_websocket(UrlUtf8.Get(), &Handle);
    LastStatus = ToUe(Status);
    return LastStatus;
}

void UCitadelClientSubsystem::Disconnect()
{
    if (Handle != nullptr)
    {
        citadel_client_free(Handle);
        Handle = nullptr;
    }
    InvalidateChatConnectionState();
    LastStatus = ECitadelStatus::Disconnected;
}

bool UCitadelClientSubsystem::IsConnected() const
{
    return Handle != nullptr;
}

ECitadelStatus UCitadelClientSubsystem::GetLastStatus() const
{
    return LastStatus;
}

FString UCitadelClientSubsystem::GetLastError()
{
    return LastError();
}

ECitadelStatus UCitadelClientSubsystem::Send(uint16 Kind, const TArray<uint8>& Payload, bool bReliable)
{
    if (Handle == nullptr)
    {
        LastStatus = ECitadelStatus::Disconnected;
        InvalidateChatConnectionState();
        return LastStatus;
    }
    const CitadelStatus Status = citadel_client_send(
        Handle,
        Kind,
        Payload.Num() > 0 ? Payload.GetData() : nullptr,
        static_cast<uintptr_t>(Payload.Num()),
        bReliable);
    LastStatus = ToUe(Status);
    if (LastStatus == ECitadelStatus::Disconnected) InvalidateChatConnectionState();
    return LastStatus;
}

void UCitadelClientSubsystem::InvalidateChatConnectionState()
{
    if (ChatLiveState != nullptr) ChatLiveState->OnDisconnect();
    if (ChatJoinSync != nullptr) ChatJoinSync->OnDisconnect();
    if (ChatHistorySync != nullptr) ChatHistorySync->CancelAll();
}

ECitadelStatus UCitadelClientSubsystem::HandleConsumedPollTruncation(TArray<uint8>& OutPayload)
{
    OutPayload.Reset();
    InvalidateChatConnectionState();
    LastStatus = ECitadelStatus::Receive;
    return LastStatus;
}

int64 UCitadelClientSubsystem::AllocateChatRequestId()
{
    if (NextChatRequestId <= 0 || NextChatRequestId == TNumericLimits<int64>::Max())
        NextChatRequestId = 1;
    return NextChatRequestId++;
}

ECitadelStatus UCitadelClientSubsystem::SendChatRequest(
    const FCitadelChatRpcRequest& Request,
    int64& OutRequestId)
{
    const int64 RequestId = AllocateChatRequestId();
    OutRequestId = RequestId;
    const bool bInitialJoin = Request.Method == TEXT("chat.join");
    if (bInitialJoin && ChatJoinSync == nullptr) ChatJoinSync = NewObject<UCitadelChatJoinSync>(this);
    if (bInitialJoin && !ChatJoinSync->BeginInitialJoin(RequestId))
    {
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    TArray<uint8> Payload;
    if (!UCitadelChatRequestLibrary::BuildRpcPayload(RequestId, Request, Payload))
    {
        if (bInitialJoin) ChatJoinSync->CancelRequest(RequestId);
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    const ECitadelStatus Status = Send(CitadelWire::KIND_RPC_REQUEST, Payload, true);
    if (bInitialJoin && Status != ECitadelStatus::Ok) ChatJoinSync->CancelRequest(RequestId);
    return Status;
}

ECitadelStatus UCitadelClientSubsystem::RejoinChatChannel(
    const FString& ChannelId,
    const FCitadelChatRpcRequest& JoinRequest,
    int64& OutRequestId)
{
    const int64 RequestId = AllocateChatRequestId();
    OutRequestId = RequestId;
    if (ChatJoinSync == nullptr) ChatJoinSync = NewObject<UCitadelChatJoinSync>(this);
    if (JoinRequest.Method != TEXT("chat.join")
        || !ChatJoinSync->BeginRejoin(RequestId, ChannelId, GetChatLiveState()))
    {
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    TArray<uint8> Payload;
    if (!UCitadelChatRequestLibrary::BuildRpcPayload(RequestId, JoinRequest, Payload))
    {
        ChatJoinSync->CancelRequest(RequestId);
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    const ECitadelStatus Status = Send(CitadelWire::KIND_RPC_REQUEST, Payload, true);
    if (Status != ECitadelStatus::Ok) ChatJoinSync->CancelRequest(RequestId);
    return Status;
}

UCitadelChatLiveEventDispatcher* UCitadelClientSubsystem::GetChatLiveDispatcher()
{
    if (ChatLiveDispatcher == nullptr)
    {
        ChatLiveDispatcher = NewObject<UCitadelChatLiveEventDispatcher>(this);
        ChatLiveDispatcher->OnChatEvent.AddDynamic(this, &UCitadelClientSubsystem::ApplyChatLiveEvent);
    }
    return ChatLiveDispatcher;
}

UCitadelChatLiveState* UCitadelClientSubsystem::GetChatLiveState()
{
    if (ChatLiveState == nullptr) ChatLiveState = NewObject<UCitadelChatLiveState>(this);
    return ChatLiveState;
}

ECitadelStatus UCitadelClientSubsystem::BeginChatReconcile(
    const FString& ChannelId,
    int64 RequiredWatermarkEventId,
    int32 PageLimit)
{
    UCitadelChatLiveState* State = GetChatLiveState();
    if (!State->HasChannel(ChannelId) || !State->NeedsReconcile(ChannelId))
    {
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    if (ChatHistorySync == nullptr) ChatHistorySync = NewObject<UCitadelChatHistorySync>(this);
    if (ChatHistorySync->IsReconciling(ChannelId))
    {
        LastStatus = ECitadelStatus::Ok;
        return LastStatus;
    }
    const int64 RequestId = AllocateChatRequestId();
    const FCitadelChatRpcRequest Request = ChatHistorySync->BeginReconcile(
        ChannelId, RequiredWatermarkEventId, PageLimit, RequestId);
    TArray<uint8> Payload;
    if (!UCitadelChatRequestLibrary::BuildRpcPayload(RequestId, Request, Payload))
    {
        ChatHistorySync->CancelReconcile(ChannelId);
        LastStatus = ECitadelStatus::InvalidArgument;
        return LastStatus;
    }
    const ECitadelStatus Status = Send(CitadelWire::KIND_RPC_REQUEST, Payload, true);
    if (Status != ECitadelStatus::Ok) ChatHistorySync->CancelReconcile(ChannelId);
    return Status;
}

void UCitadelClientSubsystem::ApplyChatLiveEvent(const FCitadelChatLiveEvent& Event)
{
    if (Event.Type == ECitadelChatEventType::AccessRevoked)
    {
        if (ChatJoinSync != nullptr) ChatJoinSync->OnAccessRevoked(Event.ChannelId);
        if (ChatHistorySync != nullptr) ChatHistorySync->CancelChannel(Event.ChannelId);
    }
    UCitadelChatLiveState* State = GetChatLiveState();
    const ECitadelChatApplyResult Result = State->Apply(Event);
    if (Result == ECitadelChatApplyResult::Gap
        || (Event.Type == ECitadelChatEventType::ResyncRequired
            && Result == ECitadelChatApplyResult::NeedsReconcile))
    {
        // Use the state's monotonic authority fence. The history owner admits
        // only one generation per channel; failed sends are cancelled for retry.
        int64 RequiredWatermark = -1;
        if (State->GetRequiredReconcileWatermark(Event.ChannelId, RequiredWatermark))
            BeginChatReconcile(Event.ChannelId, RequiredWatermark, 50);
    }
}

bool UCitadelClientSubsystem::RouteInboundEnvelope(uint16 Kind, const TArray<uint8>& Payload)
{
    const bool bDispatched = GetChatLiveDispatcher()->DispatchEnvelope(Kind, Payload);
    if (Kind != CitadelWire::KIND_RPC_RESPONSE) return bDispatched;

    if (ChatJoinSync != nullptr)
    {
        FString JoinedChannel;
        int64 RequiredWatermark = -1;
        FString JoinError;
        const ECitadelChatJoinResponseResult JoinResult = ChatJoinSync->HandleJoinResponse(Payload,
            GetChatLiveState(), JoinedChannel, RequiredWatermark, JoinError);
        if (JoinResult != ECitadelChatJoinResponseResult::Ignored)
        {
            if (JoinResult == ECitadelChatJoinResponseResult::ReconcileRequired)
                return BeginChatReconcile(JoinedChannel, RequiredWatermark, 50) == ECitadelStatus::Ok;
            return JoinResult != ECitadelChatJoinResponseResult::Failed && bDispatched;
        }
    }

    if (ChatHistorySync == nullptr) return bDispatched;
    const int64 NextRequestId = AllocateChatRequestId();
    FCitadelChatRpcRequest NextRequest;
    FString ChannelId;
    FString Error;
    const ECitadelChatHistoryResponseResult Result = ChatHistorySync->HandleRpcResponse(
        Payload, NextRequestId, GetChatLiveState(), NextRequest, ChannelId, Error);
    if (Result == ECitadelChatHistoryResponseResult::RequestNextPage
        || Result == ECitadelChatHistoryResponseResult::Restarted
        || Result == ECitadelChatHistoryResponseResult::AwaitingAck)
    {
        // Only this correlated terminal-history branch may seal the private ACK.
        if (Result == ECitadelChatHistoryResponseResult::AwaitingAck)
            NextRequest.SealForNativeSend();
        TArray<uint8> RequestPayload;
        if (!UCitadelChatRequestLibrary::BuildRpcPayload(NextRequestId, NextRequest, RequestPayload))
        {
            ChatHistorySync->CancelReconcile(ChannelId);
            return false;
        }
        const bool bSent = Send(CitadelWire::KIND_RPC_REQUEST, RequestPayload, true) == ECitadelStatus::Ok;
        if (!bSent) ChatHistorySync->CancelReconcile(ChannelId);
        return bSent;
    }
    return Result != ECitadelChatHistoryResponseResult::Failed && bDispatched;
}

ECitadelStatus UCitadelClientSubsystem::Poll(uint16& OutKind, TArray<uint8>& OutPayload)
{
    // citadel_client_poll consumes an envelope even when the caller buffer is
    // short, so retry is impossible. Reuse the canonical 8 MiB reliable-frame
    // bound and reject any impossible truncation before touching the output.
    constexpr int32 MaxReliableFrameBodyBytes = 8 * 1024 * 1024;
    if (PollBuffer.Num() != MaxReliableFrameBodyBytes)
        PollBuffer.SetNumUninitialized(MaxReliableFrameBodyBytes);
    uint16 Kind = 0;
    uintptr_t Len = 0;
    bool bTruncated = false;
    const CitadelStatus Status = citadel_client_poll(
        Handle, &Kind, PollBuffer.GetData(), static_cast<uintptr_t>(PollBuffer.Num()), &Len, &bTruncated);
    if (Status == CITADEL_STATUS_OK)
    {
        if (bTruncated || Len > static_cast<uintptr_t>(PollBuffer.Num()))
        {
            return HandleConsumedPollTruncation(OutPayload);
        }
        OutKind = Kind;
        OutPayload.Reset();
        OutPayload.Append(PollBuffer.GetData(), static_cast<int32>(Len));
    }
    LastStatus = ToUe(Status);
    if (LastStatus == ECitadelStatus::Disconnected) InvalidateChatConnectionState();
    return LastStatus;
}

FString UCitadelClientSubsystem::LastError()
{
    char Buffer[512];
    const uintptr_t Written = citadel_client_last_error(
        Handle, Buffer, static_cast<uintptr_t>(sizeof(Buffer)));
    if (Written == 0)
    {
        return FString();
    }
    return FString(UTF8_TO_TCHAR(Buffer));
}

void UCitadelClientSubsystem::AuthenticateDevice(const FString& BaseUrl, const FString& DeviceId, bool bCreate, const FString& Username)
{
    Authenticate(BaseUrl, TEXT("/v1/auth/device"), DeviceId, bCreate, Username);
}

void UCitadelClientSubsystem::AuthenticateCustom(const FString& BaseUrl, const FString& CustomId, bool bCreate, const FString& Username)
{
    Authenticate(BaseUrl, TEXT("/v1/auth/custom"), CustomId, bCreate, Username);
}

void UCitadelClientSubsystem::AuthenticateEmail(const FString& BaseUrl, const FString& Email, const FString& Password, bool bCreate, const FString& Username)
{
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    Body->SetStringField(TEXT("email"), Email);
    Body->SetStringField(TEXT("password"), Password);
    Body->SetBoolField(TEXT("create"), bCreate);
    if (!Username.IsEmpty()) Body->SetStringField(TEXT("username"), Username);
    FString Payload;
    FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Payload));
    FString Trimmed = BaseUrl;
    while (Trimmed.EndsWith(TEXT("/"))) Trimmed.LeftChopInline(1);
    const TSharedRef<IHttpRequest, ESPMode::ThreadSafe> Request = FHttpModule::Get().CreateRequest();
    Request->SetURL(Trimmed + TEXT("/v1/auth/email"));
    Request->SetVerb(TEXT("POST"));
    Request->SetHeader(TEXT("Content-Type"), TEXT("application/json"));
    Request->SetContentAsString(Payload);
    Request->OnProcessRequestComplete().BindUObject(this, &UCitadelClientSubsystem::OnAuthResponse);
    Request->ProcessRequest();
}

ECitadelStatus UCitadelClientSubsystem::AuthenticateRealtimeGuest(
    ECitadelRealtimeAuthStatus& OutAuthStatus,
    FString& OutUserId,
    uint8& OutReason)
{
    CitadelAuthStatus NativeAuthStatus = CITADEL_AUTH_STATUS_REJECTED;
    char UserBuffer[256];
    uintptr_t UserLen = 0;
    uint8 Reason = 0;
    const CitadelStatus Status = citadel_client_authenticate(
        Handle,
        nullptr,
        0,
        &NativeAuthStatus,
        UserBuffer,
        static_cast<uintptr_t>(sizeof(UserBuffer)),
        &UserLen,
        &Reason);
    LastStatus = ToUe(Status);
    OutAuthStatus = static_cast<ECitadelRealtimeAuthStatus>(static_cast<uint8>(NativeAuthStatus));
    OutUserId = Status == CITADEL_STATUS_OK ? FString(UTF8_TO_TCHAR(UserBuffer)) : FString();
    OutReason = Reason;
    return LastStatus;
}

ECitadelStatus UCitadelClientSubsystem::AuthenticateRealtimeWithSessionToken(
    const FString& Token,
    ECitadelRealtimeAuthStatus& OutAuthStatus,
    FString& OutUserId,
    uint8& OutReason)
{
    const FTCHARToUTF8 TokenUtf8(*Token);
    CitadelAuthStatus NativeAuthStatus = CITADEL_AUTH_STATUS_REJECTED;
    char UserBuffer[256];
    uintptr_t UserLen = 0;
    uint8 Reason = 0;
    const CitadelStatus Status = citadel_client_authenticate(
        Handle,
        reinterpret_cast<const uint8*>(TokenUtf8.Get()),
        static_cast<uintptr_t>(TokenUtf8.Length()),
        &NativeAuthStatus,
        UserBuffer,
        static_cast<uintptr_t>(sizeof(UserBuffer)),
        &UserLen,
        &Reason);
    LastStatus = ToUe(Status);
    OutAuthStatus = static_cast<ECitadelRealtimeAuthStatus>(static_cast<uint8>(NativeAuthStatus));
    OutUserId = Status == CITADEL_STATUS_OK ? FString(UTF8_TO_TCHAR(UserBuffer)) : FString();
    OutReason = Reason;
    return LastStatus;
}

void UCitadelClientSubsystem::Authenticate(const FString& BaseUrl, const FString& Path, const FString& Id, bool bCreate, const FString& Username)
{
    // Build the id-based auth body (see AuthRequest in src/http/auth.rs). A
    // username is only meaningful on the create path; include it when non-empty.
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    Body->SetStringField(TEXT("id"), Id);
    Body->SetBoolField(TEXT("create"), bCreate);
    if (!Username.IsEmpty())
    {
        Body->SetStringField(TEXT("username"), Username);
    }

    FString Payload;
    const TSharedRef<TJsonWriter<>> Writer = TJsonWriterFactory<>::Create(&Payload);
    FJsonSerializer::Serialize(Body, Writer);

    // Join origin + path without a double slash.
    FString Trimmed = BaseUrl;
    while (Trimmed.EndsWith(TEXT("/")))
    {
        Trimmed.LeftChopInline(1);
    }
    const FString Url = Trimmed + Path;

    const TSharedRef<IHttpRequest, ESPMode::ThreadSafe> Request = FHttpModule::Get().CreateRequest();
    Request->SetURL(Url);
    Request->SetVerb(TEXT("POST"));
    Request->SetHeader(TEXT("Content-Type"), TEXT("application/json"));
    Request->SetContentAsString(Payload);
    Request->OnProcessRequestComplete().BindUObject(this, &UCitadelClientSubsystem::OnAuthResponse);
    Request->ProcessRequest();
}

void UCitadelClientSubsystem::OnAuthResponse(FHttpRequestPtr Request, FHttpResponsePtr Response, bool bConnectedOk)
{
    if (!bConnectedOk || !Response.IsValid())
    {
        OnAuthenticationFailed.Broadcast(TEXT("authenticate: HTTP request failed (no response)"));
        return;
    }

    const int32 Code = Response->GetResponseCode();
    const FString ContentBody = Response->GetContentAsString();
    if (Code < 200 || Code >= 300)
    {
        OnAuthenticationFailed.Broadcast(
            FString::Printf(TEXT("authenticate: HTTP %d"), Code));
        return;
    }

    TSharedPtr<FJsonObject> Parsed;
    const TSharedRef<TJsonReader<>> Reader = TJsonReaderFactory<>::Create(ContentBody);
    if (!FJsonSerializer::Deserialize(Reader, Parsed) || !Parsed.IsValid())
    {
        OnAuthenticationFailed.Broadcast(TEXT("authenticate: malformed response body"));
        return;
    }

    FString Token;
    if (!Parsed->TryGetStringField(TEXT("token"), Token) || Token.IsEmpty())
    {
        OnAuthenticationFailed.Broadcast(TEXT("authenticate: response missing token"));
        return;
    }

    FString ParsedUserId;
    Parsed->TryGetStringField(TEXT("user_id"), ParsedUserId);
    FString ParsedUsername;
    Parsed->TryGetStringField(TEXT("username"), ParsedUsername);

    SessionToken = Token;
    UserId = ParsedUserId;
    OnAuthenticated.Broadcast(Token, ParsedUserId, ParsedUsername);
}

void UCitadelClientSubsystem::GetAccount(const FString& BaseUrl, const FString& AccessToken)
{
    StartPlayerRequest(BaseUrl, TEXT("/v1/account"), TEXT("GET"), AccessToken, FString(), EPlayerRequest::GetAccount);
}

void UCitadelClientSubsystem::UpdateAccount(const FString& BaseUrl, const FString& AccessToken, const FString& Username, const FString& DisplayName, bool bClearDisplayName)
{
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    if (!Username.IsEmpty()) Body->SetStringField(TEXT("username"), Username);
    if (bClearDisplayName) Body->SetField(TEXT("display_name"), MakeShared<FJsonValueNull>());
    else if (!DisplayName.IsEmpty()) Body->SetStringField(TEXT("display_name"), DisplayName);
    FString Payload;
    FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Payload));
    StartPlayerRequest(BaseUrl, TEXT("/v1/account"), TEXT("PATCH"), AccessToken, Payload, EPlayerRequest::UpdateAccount);
}

void UCitadelClientSubsystem::LookupUsers(const FString& BaseUrl, const FString& AccessToken, const TArray<FString>& UserIds, const TArray<FString>& Usernames)
{
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    TArray<TSharedPtr<FJsonValue>> IdValues;
    for (const FString& Value : UserIds) IdValues.Add(MakeShared<FJsonValueString>(Value));
    TArray<TSharedPtr<FJsonValue>> UsernameValues;
    for (const FString& Value : Usernames) UsernameValues.Add(MakeShared<FJsonValueString>(Value));
    if (!IdValues.IsEmpty()) Body->SetArrayField(TEXT("user_ids"), IdValues);
    if (!UsernameValues.IsEmpty()) Body->SetArrayField(TEXT("usernames"), UsernameValues);
    FString Payload;
    FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Payload));
    StartPlayerRequest(BaseUrl, TEXT("/v1/users/lookup"), TEXT("POST"), AccessToken, Payload, EPlayerRequest::LookupUsers);
}

void UCitadelClientSubsystem::RefreshSession(const FString& BaseUrl, const FString& RefreshToken)
{
    const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
    Body->SetStringField(TEXT("refresh_token"), RefreshToken);
    FString Payload;
    FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Payload));
    // The backend deliberately accepts the refresh secret only in the body.
    StartPlayerRequest(BaseUrl, TEXT("/v1/session/refresh"), TEXT("POST"), FString(), Payload, EPlayerRequest::Refresh);
}

void UCitadelClientSubsystem::LogoutSession(const FString& BaseUrl, const FString& AccessToken, const FString& RefreshToken)
{
    FString Payload;
    if (!RefreshToken.IsEmpty())
    {
        const TSharedRef<FJsonObject> Body = MakeShared<FJsonObject>();
        Body->SetStringField(TEXT("refresh_token"), RefreshToken);
        FJsonSerializer::Serialize(Body, TJsonWriterFactory<>::Create(&Payload));
    }
    StartPlayerRequest(BaseUrl, TEXT("/v1/session/logout"), TEXT("POST"), AccessToken, Payload, EPlayerRequest::Logout);
}

void UCitadelClientSubsystem::StartPlayerRequest(const FString& BaseUrl, const FString& Path, const FString& Verb, const FString& AccessToken, const FString& JsonBody, EPlayerRequest Kind)
{
    FString Origin = BaseUrl;
    while (Origin.EndsWith(TEXT("/"))) Origin.LeftChopInline(1);
    const TSharedRef<IHttpRequest, ESPMode::ThreadSafe> Request = FHttpModule::Get().CreateRequest();
    Request->SetURL(Origin + Path);
    Request->SetVerb(Verb);
    Request->SetHeader(TEXT("Accept"), TEXT("application/json"));
    if (!AccessToken.IsEmpty()) Request->SetHeader(TEXT("Authorization"), TEXT("Bearer ") + AccessToken);
    if (!JsonBody.IsEmpty())
    {
        Request->SetHeader(TEXT("Content-Type"), TEXT("application/json"));
        Request->SetContentAsString(JsonBody);
    }
    Request->OnProcessRequestComplete().BindWeakLambda(this, [this, Kind](FHttpRequestPtr CompletedRequest, FHttpResponsePtr Response, bool bConnectedOk)
    {
        OnPlayerResponse(CompletedRequest, Response, bConnectedOk, Kind);
    });
    if (!Request->ProcessRequest()) OnPlayerRequestFailed.Broadcast(0, TEXT("transport_error"), TEXT("request failed"));
}

bool UCitadelClientSubsystem::ParseProfile(const TSharedPtr<FJsonObject>& Json, FCitadelPublicProfile& OutProfile)
{
    if (!Json.IsValid()
        || !Json->TryGetStringField(TEXT("user_id"), OutProfile.UserId)
        || !Json->TryGetStringField(TEXT("username"), OutProfile.Username))
    {
        return false;
    }
    Json->TryGetStringField(TEXT("display_name"), OutProfile.DisplayName);
    return true;
}

void UCitadelClientSubsystem::OnPlayerResponse(FHttpRequestPtr Request, FHttpResponsePtr Response, bool bConnectedOk, EPlayerRequest Kind)
{
    if (!bConnectedOk || !Response.IsValid())
    {
        OnPlayerRequestFailed.Broadcast(0, TEXT("transport_error"), TEXT("request failed"));
        return;
    }
    const int32 Status = Response->GetResponseCode();
    if (Status == 204 && Kind == EPlayerRequest::Logout) return;
    TSharedPtr<FJsonObject> Json;
    FJsonSerializer::Deserialize(TJsonReaderFactory<>::Create(Response->GetContentAsString()), Json);
    if (Status < 200 || Status >= 300)
    {
        FString Code = TEXT("http_error"), Message = TEXT("request failed");
        if (Json.IsValid()) { Json->TryGetStringField(TEXT("code"), Code); Json->TryGetStringField(TEXT("message"), Message); }
        OnPlayerRequestFailed.Broadcast(Status, Code, Message);
        return;
    }
    if (!Json.IsValid()) { OnPlayerRequestFailed.Broadcast(Status, TEXT("invalid_response"), TEXT("server returned an invalid response")); return; }
    if (Kind == EPlayerRequest::GetAccount || Kind == EPlayerRequest::UpdateAccount)
    {
        FCitadelPublicProfile Profile;
        if (!ParseProfile(Json, Profile)) { OnPlayerRequestFailed.Broadcast(Status, TEXT("invalid_response"), TEXT("server returned an invalid response")); return; }
        if (Kind == EPlayerRequest::GetAccount) OnAccountReceived.Broadcast(Profile); else OnAccountUpdated.Broadcast(Profile);
    }
    else if (Kind == EPlayerRequest::LookupUsers)
    {
        const TArray<TSharedPtr<FJsonValue>>* Values = nullptr;
        if (!Json->TryGetArrayField(TEXT("users"), Values)) { OnPlayerRequestFailed.Broadcast(Status, TEXT("invalid_response"), TEXT("server returned an invalid response")); return; }
        TArray<FCitadelPublicProfile> Users;
        for (const TSharedPtr<FJsonValue>& Value : *Values) { FCitadelPublicProfile Profile; if (ParseProfile(Value->AsObject(), Profile)) Users.Add(Profile); }
        OnUsersLookupReceived.Broadcast(Users);
    }
    else if (Kind == EPlayerRequest::Refresh)
    {
        FCitadelSessionTokenPair Tokens;
        if (!Json->TryGetStringField(TEXT("token"), Tokens.Token) || !Json->TryGetStringField(TEXT("refresh_token"), Tokens.RefreshToken) || !Json->TryGetStringField(TEXT("user_id"), Tokens.UserId) || !Json->TryGetStringField(TEXT("username"), Tokens.Username))
        { OnPlayerRequestFailed.Broadcast(Status, TEXT("invalid_response"), TEXT("server returned an invalid response")); return; }
        OnSessionRefreshed.Broadcast(Tokens);
    }
}

void UCitadelClientSubsystem::Initialize(FSubsystemCollectionBase& Collection)
{
    Super::Initialize(Collection);
    GetChatLiveState();
    GetChatLiveDispatcher();
    ChatJoinSync = NewObject<UCitadelChatJoinSync>(this);
    ChatHistorySync = NewObject<UCitadelChatHistorySync>(this);
}

void UCitadelClientSubsystem::Deinitialize()
{
    if (Handle != nullptr)
    {
        citadel_client_free(Handle);
        Handle = nullptr;
    }
    ChatLiveDispatcher = nullptr;
    ChatLiveState = nullptr;
    ChatJoinSync = nullptr;
    ChatHistorySync = nullptr;
    PollBuffer.Reset();
    Super::Deinitialize();
}
