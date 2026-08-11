#pragma once

#include "CoreMinimal.h"
#include "Kismet/BlueprintFunctionLibrary.h"
#include "UObject/Object.h"
#include "CitadelChatLive.generated.h"

/** Closed v1 chat live-event variants. Unknown wire types never reach this enum. */
UENUM(BlueprintType)
enum class ECitadelChatEventType : uint8
{
    PresenceJoin,
    PresenceLeave,
    Typing,
    MessageCreate,
    MessageUpdate,
    MessageRemove,
    AccessRevoked,
    ResyncRequired,
};

UENUM(BlueprintType)
enum class ECitadelChatChannelType : uint8
{
    Unspecified,
    Direct,
    Group,
    Room,
};

UENUM(BlueprintType)
enum class ECitadelChatResyncScope : uint8
{
    History,
    Presence,
};

UENUM(BlueprintType)
enum class ECitadelChatApplyResult : uint8
{
    Applied,
    Duplicate,
    Gap,
    NeedsReconcile,
    Revoked,
    UnknownChannel,
};

/** Result of consuming one correlated chat-history RPC response. */
UENUM(BlueprintType)
enum class ECitadelChatHistoryResponseResult : uint8
{
    Ignored,
    RequestNextPage,
    Restarted,
    AwaitingAck,
    Current,
    Failed,
};

/** Result of consuming one typed, correlated chat.join RPC response. */
UENUM(BlueprintType)
enum class ECitadelChatJoinResponseResult : uint8
{
    Ignored,
    Joined,
    RejoinedCurrent,
    ReconcileRequired,
    Failed,
};

USTRUCT(BlueprintType)
struct FCitadelChatPresence
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString PresenceId;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString UserId;
};

/** Complete durable message representation carried by create/update/remove. */
USTRUCT(BlueprintType)
struct FCitadelChatMessage
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 Id = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString Sender;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString Content;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 CreatedAtUnixMs = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 UpdatedAtUnixMs = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 Revision = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 LastEventId = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") bool bDeleted = false;
};

/** Validated, closed v1 event. Variant-irrelevant fields remain at defaults. */
USTRUCT(BlueprintType)
struct FCitadelChatLiveEvent
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int32 Version = 1;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") ECitadelChatEventType Type = ECitadelChatEventType::PresenceJoin;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString ChannelId;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") ECitadelChatChannelType ChannelType = ECitadelChatChannelType::Unspecified;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 EventId = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FCitadelChatPresence Presence;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FCitadelChatMessage Message;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") bool bTyping = false;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 ExpiresAtUnixMs = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") int64 WatermarkEventId = 0;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") TArray<ECitadelChatResyncScope> Scopes;

    FString TypeName() const;
    bool IsDurable() const;
};

/** Strict native chat.join response; never Blueprint-authored. */
struct FCitadelChatJoinResponse
{
    FString ChannelId;
    ECitadelChatChannelType ChannelType = ECitadelChatChannelType::Unspecified;
    TArray<FCitadelChatPresence> Presences;
    int64 WatermarkEventId = 0;
    FString SubscriptionId;
};

DECLARE_DYNAMIC_MULTICAST_DELEGATE_TwoParams(FCitadelRawEnvelopeReceived, int32, Kind, const TArray<uint8>&, Payload);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelChatLiveEventReceived, const FCitadelChatLiveEvent&, Event);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelChatLiveEventRejected, const FString&, Error);

/** Pure decoder; malformed/version/type/invariant violations fail closed. */
UCLASS()
class UCitadelChatLiveEventLibrary : public UBlueprintFunctionLibrary
{
    GENERATED_BODY()
public:
    UFUNCTION(BlueprintCallable, Category="Citadel|Chat")
    static bool TryDecode(const FString& PayloadJson, FCitadelChatLiveEvent& OutEvent, FString& OutError);

    static bool TryDecode(const TArray<uint8>& Payload, FCitadelChatLiveEvent& OutEvent, FString& OutError);
};

/**
 * Feed this dispatcher envelopes obtained by any poll/transport owner. It never
 * polls and always preserves the raw kind+payload route before typed decoding.
 */
UCLASS(BlueprintType)
class UCitadelChatLiveEventDispatcher : public UObject
{
    GENERATED_BODY()
public:
    UPROPERTY(BlueprintAssignable, Category="Citadel|Raw") FCitadelRawEnvelopeReceived OnRawEnvelope;
    UPROPERTY(BlueprintAssignable, Category="Citadel|Chat") FCitadelChatLiveEventReceived OnChatEvent;
    UPROPERTY(BlueprintAssignable, Category="Citadel|Chat") FCitadelChatLiveEventRejected OnChatEventRejected;

    UFUNCTION(BlueprintCallable, Category="Citadel|Chat")
    bool DispatchEnvelope(int32 Kind, const TArray<uint8>& Payload);
};

/** Domain request ready for generic RPC send; callers never author JSON. */
USTRUCT(BlueprintType)
struct FCitadelChatRpcRequest
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString Method;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString PayloadJson;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") bool bValid = false;
    UPROPERTY(BlueprintReadOnly, Category="Citadel|Chat") FString Error;

    /** Native-only typed factories used by the Blueprint library wrappers. */
    static FCitadelChatRpcRequest BuildJoinDirect(const FString& OtherUserId);
    static FCitadelChatRpcRequest BuildJoinGroup(int64 GroupId);
    static FCitadelChatRpcRequest BuildJoinCurrentRoom();
    static FCitadelChatRpcRequest BuildLeave(const FString& ChannelId);
    static FCitadelChatRpcRequest BuildSend(const FString& ChannelId, const FString& Content);
    static FCitadelChatRpcRequest BuildHistory(const FString& ChannelId, int32 Limit, int64 BeforeMessageId);
    static FCitadelChatRpcRequest BuildEdit(const FString& ChannelId, int64 MessageId, const FString& Content);
    static FCitadelChatRpcRequest BuildDelete(const FString& ChannelId, int64 MessageId);
    static FCitadelChatRpcRequest BuildModerate(const FString& ChannelId, int64 MessageId);
    static FCitadelChatRpcRequest BuildTyping(const FString& ChannelId, bool bTyping);
    bool IsSealedForNativeSend() const
    {
        return RequestSeal == 0x43484154u
            && Method == SealedMethod
            && PayloadJson == SealedPayloadJson;
    }

private:
    friend class UCitadelClientSubsystem;
#if WITH_DEV_AUTOMATION_TESTS
    friend class FCitadelChatRpcRequestTestAccess;
#endif
    /** No caller may seal caller-authored method/JSON; only typed factories and the subsystem's private ACK path reach this. */
    void SealForNativeSend()
    {
        SealedMethod = Method;
        SealedPayloadJson = PayloadJson;
        RequestSeal = 0x43484154u;
    }
    static FCitadelChatRpcRequest SealTypedFactoryResult(FCitadelChatRpcRequest Request)
    {
        if (Request.bValid) Request.SealForNativeSend();
        return Request;
    }
    UPROPERTY(meta=(AllowPrivateAccess="true"))
    uint32 RequestSeal = 0;
    UPROPERTY(meta=(AllowPrivateAccess="true"))
    FString SealedMethod;
    UPROPERTY(meta=(AllowPrivateAccess="true"))
    FString SealedPayloadJson;
};

UCLASS()
class UCitadelChatRequestLibrary : public UBlueprintFunctionLibrary
{
    GENERATED_BODY()
public:
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildJoinDirect(const FString& OtherUserId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildJoinGroup(int64 GroupId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildJoinCurrentRoom();
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildLeave(const FString& ChannelId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildSend(const FString& ChannelId, const FString& Content);
    /** Ordinary newest-first history page. ACK is intentionally not public. */
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildHistory(const FString& ChannelId, int32 Limit, int64 BeforeMessageId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildEdit(const FString& ChannelId, int64 MessageId, const FString& Content);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildDelete(const FString& ChannelId, int64 MessageId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildModerate(const FString& ChannelId, int64 MessageId);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") static FCitadelChatRpcRequest BuildTyping(const FString& ChannelId, bool bTyping);

    /** Encode u64 request id + u16 method length + method + UTF-8 JSON. */
    UFUNCTION(BlueprintCallable, Category="Citadel|Chat")
    static bool BuildRpcPayload(int64 RequestId, const FCitadelChatRpcRequest& Request, TArray<uint8>& OutPayload);
};

/**
 * Per-joined-channel state: bounded naturally by active subscriptions, with one
 * authority cursor per channel (no unbounded event-id cache).
 */
UCLASS(BlueprintType)
class UCitadelChatLiveState : public UObject
{
    GENERATED_BODY()
public:
    UFUNCTION(BlueprintCallable, Category="Citadel|Chat") void ExpireTyping(int64 NowUnixMs);
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") bool HasChannel(const FString& ChannelId) const;
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") bool IsCurrent(const FString& ChannelId) const;
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") bool NeedsReconcile(const FString& ChannelId) const;
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") bool IsTyping(const FString& ChannelId, const FString& PresenceId) const;
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") TArray<FCitadelChatPresence> GetPresences(const FString& ChannelId) const;
    UFUNCTION(BlueprintPure, Category="Citadel|Chat") TArray<FCitadelChatMessage> GetMessages(const FString& ChannelId) const;

private:
    friend class UCitadelChatJoinSync;
    friend class UCitadelChatHistorySync;
    friend class UCitadelClientSubsystem;
#if WITH_DEV_AUTOMATION_TESTS
    friend class FCitadelChatLiveStateTestAccess;
#endif
    /** Authority transitions are transport-owned and never reflected/public. */
    void OnDisconnect();
    ECitadelChatApplyResult Apply(const FCitadelChatLiveEvent& Event);
    /** Atomically replace history after a terminal stable page; stays non-current. */
    bool ApplyReconcileSnapshot(const FString& ChannelId, const TArray<FCitadelChatMessage>& Messages, int64 WatermarkEventId);
    /** Mark current only after the server answered the private ACK request. */
    bool ConfirmReconcileAcknowledged(const FString& ChannelId, int64 WatermarkEventId);
    bool ApplyInitialJoin(const FCitadelChatJoinResponse& Response);
    bool CaptureRejoinCursor(const FString& ChannelId, int64& OutCursor) const;
    bool GetRequiredReconcileWatermark(const FString& ChannelId, int64& OutWatermark) const;
    ECitadelChatJoinResponseResult ApplyCorrelatedRejoin(
        const FCitadelChatJoinResponse& Response,
        int64 CapturedCursor,
        int64& OutRequiredWatermark);

    struct FChannelState
    {
        int64 Cursor = 0;
        int64 ExpectedWatermark = 0;
        bool bCurrent = true;
        bool bNeedsRejoin = false;
        bool bNeedsReconcile = false;
        bool bSnapshotApplied = false;
        TMap<FString, FCitadelChatPresence> Presences;
        TMap<int64, FCitadelChatMessage> Messages;
        TMap<FString, int64> TypingExpiries;
    };
    TMap<FString, FChannelState> Channels;
};

/** Opaque request/generation owner for typed chat.join replies. */
UCLASS()
class UCitadelChatJoinSync : public UObject
{
    GENERATED_BODY()
private:
    friend class UCitadelClientSubsystem;
#if WITH_DEV_AUTOMATION_TESTS
    friend class FCitadelChatJoinSyncTestAccess;
#endif
    bool BeginInitialJoin(int64 RequestId);
    bool BeginRejoin(int64 RequestId, const FString& ChannelId, UCitadelChatLiveState* State);
    void CancelRequest(int64 RequestId);
    void OnDisconnect();
    void OnAccessRevoked(const FString& ChannelId);
    ECitadelChatJoinResponseResult HandleJoinResponse(
        const TArray<uint8>& RpcPayload,
        UCitadelChatLiveState* State,
        FString& OutChannelId,
        int64& OutRequiredWatermark,
        FString& OutError);

   struct FPendingJoin
    {
        int64 Generation = 0;
        FString ExpectedChannelId;
        int64 CapturedCursor = 0;
        bool bRejoin = false;
    };
    int64 Generation = 1;
    TMap<int64, FPendingJoin> PendingJoins;
};

/**
 * Correlated fail-closed history reconciliation. It captures one stable server
 * watermark, validates newest-first paging, applies only a terminal snapshot,
 * then emits the sole private ACK and waits for its correlated response.
 */
UCLASS()
class UCitadelChatHistorySync : public UObject
{
    GENERATED_BODY()
private:
    friend class UCitadelClientSubsystem;
#if WITH_DEV_AUTOMATION_TESTS
    friend class FCitadelChatHistorySyncTestAccess;
#endif
    FCitadelChatRpcRequest BeginReconcile(
        const FString& ChannelId,
        int64 RequiredWatermarkEventId,
        int32 Limit,
        int64 RequestId);

    ECitadelChatHistoryResponseResult HandleRpcResponse(
        const TArray<uint8>& RpcPayload,
        int64 NextRequestId,
        UCitadelChatLiveState* State,
        FCitadelChatRpcRequest& OutNextRequest,
        FString& OutChannelId,
        FString& OutError);

    bool IsReconciling(const FString& ChannelId) const;
    void CancelReconcile(const FString& ChannelId);
    void CancelChannel(const FString& ChannelId);
    void CancelAll();
    struct FPendingRequest
    {
        FString ChannelId;
        int64 Generation = 0;
        bool bAck = false;
    };

    struct FHistoryState
    {
        int32 Limit = 0;
        int64 RequiredWatermark = 0;
        int64 SnapshotWatermark = -1;
        int64 BeforeMessageId = 0;
        int64 Generation = 0;
        TArray<FCitadelChatMessage> Messages;
    };

    TMap<int64, FPendingRequest> PendingRequests;
    TMap<FString, FHistoryState> HistoryByChannel;
};
