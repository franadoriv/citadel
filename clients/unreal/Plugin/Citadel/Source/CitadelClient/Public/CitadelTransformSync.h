// CitadelTransformSync.h — the Unreal transform-sync surface (, P1).
//
// `UCitadelTransformSync` is the ergonomic UActorComponent a game attaches to a
// replicated actor; it binds that actor to a server object id and, every frame,
// applies the interpolated authoritative transform the client runtime
// reconstructs from KIND_TSYNC_SNAPSHOT datagrams. P1 renders
// RemoteInterpolated / ServerSimulated / StaticReplicated objects in the past
// (Hermite position when velocity is replicated + slerp rotation, adaptive
// jitter buffer, bounded extrapolation on drain). Owner prediction /
// reconciliation is P2 and deliberately absent here.
//
// The heavy lifting lives in `FCitadelRemoteWorldView` (a faithful C++ port of
// the tested Rust `RemoteWorldView`); `UCitadelTransformSyncSubsystem` owns one
// view per connection, decodes snapshots, and acks. A later task replaces the
// hand-port decode with the shared citadel-client-ffi C ABI so Unity/Godot
// inherit the same core. In-editor two-client behavior is a MANUAL pre-release
// check (C++ is not built in CI) — see clients/unreal/README.md.
#pragma once

#include "CoreMinimal.h"
#include "Components/ActorComponent.h"
#include "Subsystems/GameInstanceSubsystem.h"
#include "Tickable.h" // FTickableGameObject, STATGROUP_Tickables

#include "CitadelTransformWire.h"

#include "CitadelTransformSync.generated.h"

/** Per-object sync role, mirroring the wire (citadel_wire::tsync::SyncRole). */
UENUM(BlueprintType)
enum class ECitadelSyncRole : uint8
{
    OwnerPredicted,     // P2: this client owns + predicts (not handled in P1).
    RemoteInterpolated, // server-owned, rendered in the past (P1 default).
    ServerSimulated,    // server/Lua drives; clients interpolate (P1).
    StaticReplicated    // one-shot + rare updates (P1).
};

/** A reconstructed remote object as the client holds it. */
struct FCitadelRemoteObject
{
    uint16 GenEpoch = 0;
    FVector Position = FVector::ZeroVector; // cm
    FQuat Rotation = FQuat::Identity;
    FVector Velocity = FVector::ZeroVector; // cm/s
    bool bHasVelocity = false;
};

/**
 * The reusable client runtime core: reconstructs full world state from delta
 * snapshots against the base it holds (discarding snapshots whose base it lacks
 * and applying only strictly-newer ids), feeds a per-object jitter buffer, and
 * renders in the past with Hermite/slerp + bounded extrapolation. A C++ port of
 * the tested Rust `RemoteWorldView`; keep the two in lockstep.
 */
class FCitadelRemoteWorldView
{
public:
    void SetCodec(const CitadelTransform::FCodecParams& InParams);
    bool HasCodec() const { return bHaveCodec; }
    const CitadelTransform::FCodecParams& GetCodec() const { return Params; }

    /** Decode + apply a snapshot datagram body. Returns true if applied. */
    bool ApplyDatagram(const uint8* Body, int32 Len);
    /** Decode/apply a v2 wrapper, rejecting a stale/mixed gameplay-clock epoch. */
    bool ApplyV2Datagram(const uint8* Body, int32 Len);
    /** Clear all baselines/samples before admitting a new reconnect epoch. */
    bool ResetV2Epoch(uint64 Epoch);
    /** The accepted v2 epoch/tick for owner input diagnostics (zero = none). */
    uint64 V2Epoch() const { return ClockEpoch; }
    uint64 LastObservedV2Tick() const { return LastObservedClockTick; }

    /** The ack to send back (fills an 8-byte KIND_TSYNC_ACK body). */
    void AckBytes(uint8 Out[8]) const;

    /** The newest applied snapshot id (piggybacked into input bundles). */
    uint32 LastAppliedSnapshotId() const { return LastAppliedId; }

    /** The reconstructed (non-interpolated) object, if present. */
    bool GetObject(uint32 ObjectId, FCitadelRemoteObject& Out) const;

    /**
     * The highest CONTIGUOUS input seq the server has acked for an owned object
     * ( §5.1), or false if none yet. The prediction ring reconciles
     * against this. Only the owner's snapshot carries it.
     */
    bool GetOwnerAck(uint32 ObjectId, uint32& OutSeq) const;

    /**
     * Interpolate an object at the current render time (newest sample minus the
     * adaptive buffer delay): Hermite position (when velocity replicated) + slerp
     * rotation, with bounded extrapolation past the last sample. Returns false
     * when the object has no samples yet.
     */
    bool SampleNow(uint32 ObjectId, bool bHermite, float MaxExtrapolationSeconds, FVector& OutPos, FQuat& OutRot) const;

private:
    struct FSample
    {
        uint32 Tick = 0;
        FVector Position = FVector::ZeroVector;
        FQuat Rotation = FQuat::Identity;
        FVector Velocity = FVector::ZeroVector;
        bool bHasVelocity = false;
    };

    static constexpr int32 MaxRing = 64;
    static constexpr int32 MaxSamples = 32;

    CitadelTransform::FCodecParams Params;
    bool bHaveCodec = false;
    uint64 ClockEpoch = 0;
    uint64 LastObservedClockTick = 0;

    // snapshotId -> (objectId -> reconstructed object).
    TMap<uint32, TMap<uint32, FCitadelRemoteObject>> Ring;
    uint32 LastAppliedId = 0;
    // Ack window (mirror of citadel_wire::baseline::AckField).
    uint32 AckLatest = 0;
    uint32 AckHistory = 0;

    TMap<uint32, TArray<FSample>> Samples;
    // Per-owned-object highest contiguous input ack (design §5.1).
    TMap<uint32, uint32> OwnerAcks;
    float SimRateHz = 60.0f;
    float SendRateHz = 20.0f;
    // Adaptive interpolation buffer (lockstep with Rust RemoteWorldView): the
    // current multiplier starts at the ceiling, decays toward the floor on a clean
    // link (lower latency), and grows back toward the ceiling on detected loss.
    // On localhost/LAN it converges to the floor automatically; no configuration.
    float BufferMultiplier = 2.5f; // current adaptive value
    float BufferFloor = 1.5f;
    float BufferCeil = 2.5f;
    bool bAdaptive = true;

    void ResetState();

    void Ack(uint32 Id);
    void PushSample(uint32 ObjectId, uint32 Tick, const FCitadelRemoteObject& Obj);
    void PruneRing();
    float BufferDelayTicks() const;
    int32 LatestSampleTick() const;
};

/** Reconciliation error-smoothing tuning (design §5.1). Mirror of ReconcileConfig. */
struct FCitadelReconcileConfig
{
    float SmoothingSmall = 0.95f; // <= SmallCm
    float SmoothingLarge = 0.85f; // >= LargeCm
    float SmallCm = 25.0f;
    float LargeCm = 100.0f;
    float HardSnapCm = 500.0f;
};

/** One buffered local input the owner predicted with (design §5.1). */
struct FCitadelPredictedInput
{
    uint32 Seq = 0;
    FVector MoveVelocity = FVector::ZeroVector; // cm/s
    float Dt = 0.0f;
};

/**
 * The owner's prediction ring: predict from local input immediately, reconcile
 * against the server's authoritative correction (rollback + replay of only the
 * unacked inputs), and smooth the RENDERED visual offset ONLY — never the sim
 * state. A faithful C++ port of the tested Rust `PredictionRing`
 * (src/realtime/transform/prediction.rs); keep the two in lockstep.
 */
class FCitadelPredictionRing
{
public:
    void Reset(const FVector& Initial)
    {
        Inputs.Reset();
        Predicted = Initial;
        NextSeq = 1;
        VisualOffset = FVector::ZeroVector;
    }

    /** Predict one input locally: apply immediately, buffer it, return its seq. */
    uint32 PushInput(const FVector& MoveVelocity, float Dt);

    /** The predicted SIMULATION position (collision/gameplay). */
    FVector PredictedPosition() const { return Predicted; }

    /** The RENDERED position: predicted + smoothed visual offset (§5.1). */
    FVector RenderPosition() const { return Predicted + VisualOffset; }

    /** The current visual offset (tests/observability). */
    FVector VisualOffsetVec() const { return VisualOffset; }

    /** Number of unacked inputs still buffered. */
    int32 PendingInputs() const { return Inputs.Num(); }

    /** The most recently predicted input seq (0 = none). */
    uint32 LatestSeq() const { return NextSeq > 1 ? NextSeq - 1 : 0; }

    /**
     * Reconcile against the authoritative post-input state + last_input_seq:
     * drop inputs <= last_input_seq, snap the sim state to authority, replay the
     * remaining inputs in seq order, and fold the correction into the visual
     * offset so the rendered avatar does not snap. Returns true if a correction
     * with a non-trivial error was applied (for the OnReconciled event).
     */
    bool Reconcile(const FVector& Authoritative, uint32 LastInputSeq);

    /** Ease the rendered visual offset toward zero (call once per render frame). */
    void AdvanceSmoothing();

    FCitadelReconcileConfig Config;

private:
    TArray<FCitadelPredictedInput> Inputs; // ascending seq
    FVector Predicted = FVector::ZeroVector;
    uint32 NextSeq = 1;
    FVector VisualOffset = FVector::ZeroVector;

    static void ApplyInput(FVector& State, const FCitadelPredictedInput& In);
};

/** Fired when an owned object is corrected by the server (error in cm). */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FOnCitadelReconciled, float, ErrorCm);
/** Fired when the server assigns/hands off ownership of this object. */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_TwoParams(FOnCitadelOwnership, bool, bIsOwner, int64, OwnershipEpoch);
/** Fired with the authoritative lag-comp hit result for a fire this client made. */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_ThreeParams(FOnCitadelRewindResult, bool, bHit, int64, HitObjectId, FVector, HitPoint);

/**
 * The ergonomic component. Attach to a replicated actor, set `ObjectId` to the
 * server object it mirrors, and it applies the interpolated authoritative
 * transform each frame. The component never owns the socket; it reads from the
 * connection's `UCitadelTransformSyncSubsystem` (authority discipline, §2.3).
 */
UCLASS(ClassGroup = (Citadel), meta = (BlueprintSpawnableComponent))
class UCitadelTransformSync : public UActorComponent
{
    GENERATED_BODY()

public:
    UCitadelTransformSync();

    /** The server object id this component mirrors. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Sync")
    int64 ObjectId = 0;

    /** Role hint for spawn; the server assigns the true role/owner. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Authority")
    ECitadelSyncRole Role = ECitadelSyncRole::RemoteInterpolated;

    /** Interpolation buffer size as a multiple of the send interval (~2.5x). */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Interp")
    float InterpBufferMultiplier = 2.5f;

    /** Shrink on a stable link, grow under jitter/loss (1.5x..~400ms bounds). */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Interp")
    bool bAdaptiveBuffer = true;

    /** Max seconds to extrapolate past the last sample on buffer drain. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Interp")
    float MaxExtrapolationSeconds = 0.25f;

    /** Hermite position (needs replicated velocity) vs linear lerp. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Interp")
    bool bHermitePosition = true;

    /** Feeds the server's per-client priority accumulator (higher = sooner). */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Send")
    float NetPriority = 1.0f;

    /** How many recent inputs to bundle redundantly per packet (single-loss heal). */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Predict")
    int32 InputRedundancy = 4;

    /** Teleport-scale correction (cm) above which the render hard-snaps. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Predict")
    float HardSnapThresholdCm = 500.0f;

    /**
     * The only owner write path (design §2.3, §5.1): predict `MoveVelocity`
     * (cm/s) locally over `Dt`, buffer + bundle the input redundantly, and send
     * KIND_TSYNC_INPUT (or negotiated KIND_TSYNC_V2_INPUT). The component never writes transform to the server
     * directly. No-op unless this client owns the object (OwnerPredicted).
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Predict")
    void SubmitInput(FVector MoveVelocity, float Dt);

    /**
     * Fire a lag-compensated shot along a world-space ray. The command rides the
     * next input bundle and is resolved SERVER-side against the rewound history;
     * the authoritative result arrives via OnRewindResult. The client never
     * resolves the hit. No-op unless this client owns the object.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Predict")
    void RewindHitTest(FVector Origin, FVector Direction);

    /** Whether this client currently owns + predicts this object. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Authority")
    bool IsOwner() const { return bIsOwner; }

    /** Fired on a server correction (rollback+replay) of this owned object. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Predict")
    FOnCitadelReconciled OnReconciled;

    /** Fired when the server assigns/hands off ownership of this object. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Authority")
    FOnCitadelOwnership OnOwnershipChanged;

    /** Fired with the authoritative hit result for a shot this client fired. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Predict")
    FOnCitadelRewindResult OnRewindResult;

    virtual void BeginPlay() override;
    virtual void EndPlay(const EEndPlayReason::Type Reason) override;
    virtual void TickComponent(float DeltaTime, ELevelTick TickType, FActorComponentTickFunction* ThisTickFunction) override;

    // --- Called by the subsystem when routing frames for this object ---
    /** Apply a server ownership/role transition (KIND_TSYNC_ROLE). */
    void OnRoleFrame(bool bNowOwned, uint32 InOwnershipEpoch);
    /** Try to claim + fire a rewind result by its input seq; true if it was ours. */
    bool ClaimRewindResult(const CitadelTransform::FRewindResult& Result);
    /** The object id as u32 (0 if unset). */
    uint32 ObjectIdU32() const { return ObjectId > 0 ? static_cast<uint32>(ObjectId) : 0; }

private:
    /** Whether this component has become relevant (received any state). */
    bool bEverApplied = false;
    /** Whether this client owns + predicts this object (server-assigned). */
    bool bIsOwner = false;
    /** The current server-assigned ownership epoch (guards stale handoffs). */
    uint32 OwnershipEpoch = 0;
    /** The owner prediction ring (only used while bIsOwner). */
    FCitadelPredictionRing Prediction;
    /** Recent input frames kept for redundant bundling (oldest first). */
    TArray<CitadelTransform::FInputFrame> RecentInputs;
    /** A fire queued to ride the next input, if any. */
    bool bPendingFire = false;
    CitadelTransform::FFireCommand PendingFire;
    /** Input seqs of fires we are awaiting an authoritative result for. */
    TSet<uint32> PendingFireSeqs;

    /** Owner tick: predict-render + reconcile against the latest owner ack. */
    void TickOwnerPredicted(float DeltaTime);
    /** Remote tick: apply the interpolated authoritative transform. */
    void TickRemoteInterpolated();
    /** Build + send the current redundant input bundle for this object. */
    void SendInputBundle(uint32 NewSeq, const FVector& MoveVelocity, float Dt);
};

/**
 * Owns one `FCitadelRemoteWorldView` per game instance connection: opts into
 * transform sync (sends KIND_TSYNC_HELLO), drains inbound envelopes, decodes
 * snapshots, and acks. Components register here and query their interpolated
 * transform each frame. For P1 this drains the shared client subsystem's poll
 * queue and dispatches transform frames; other kinds are re-queued to game code
 * via `OnOtherEnvelope`.
 */
UCLASS()
class UCitadelTransformSyncSubsystem : public UGameInstanceSubsystem, public FTickableGameObject
{
    GENERATED_BODY()

public:
    /** Send KIND_TSYNC_HELLO to opt into transform sync on the active connection. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Sync")
    void OptIn();

    /**
     * Tell the subsystem which participant id this client authenticated as, so
     * KIND_TSYNC_ROLE ownership frames can be matched to the local player (design
     * §2.2). Until set, ownership is not assumed and prediction stays off.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Sync")
    void SetLocalParticipantId(int64 ParticipantId) { LocalParticipantId = static_cast<uint64>(ParticipantId); }

    /** Send a raw transform-sync frame on the active connection (used by inputs). */
    void SendFrame(uint16 Kind, const TArray<uint8>& Body, bool bReliable);
    /** True only after the server echoes the exact v2 manifest for this HELLO lifetime. */
    bool IsV2InputNegotiated() const { return bV2Negotiated && WorldView.V2Epoch() != 0; }

    /** Register/unregister a component so it is updated each frame. */
    void RegisterComponent(UCitadelTransformSync* Component);
    void UnregisterComponent(UCitadelTransformSync* Component);

    /** The client runtime view (reconstruction + interpolation). */
    FCitadelRemoteWorldView& View() { return WorldView; }

    // FTickableGameObject: pump inbound frames + update components each frame.
    virtual void Tick(float DeltaTime) override;
    virtual TStatId GetStatId() const override { RETURN_QUICK_DECLARE_CYCLE_STAT(UCitadelTransformSyncSubsystem, STATGROUP_Tickables); }
    // Tick whenever we have a live GameInstance (not only after opt-in): the pump
    // routes inbound room frames, which must flow BEFORE AnnouncePresence (that is
    // what opts in). Gating on the GameInstance keeps the CDO/template — whose
    // GetGameInstance is null — from ticking and dereferencing null in PumpInbound.
    virtual bool IsTickable() const override { return GetGameInstance() != nullptr; }

private:
    FCitadelRemoteWorldView WorldView;
    UPROPERTY()
    TArray<TWeakObjectPtr<UCitadelTransformSync>> Components;
    bool bOptedIn = false;
    bool bV2Negotiated = false;
    /** The local player's participant id (0 = unknown; see SetLocalParticipantId). */
    uint64 LocalParticipantId = 0;

    void PumpInbound();
    void SendV2Hello();
    void SendAck();
    /** Route a decoded KIND_TSYNC_ROLE frame to the matching component. */
    void RouteRoleFrame(const uint8* Body, int32 Len);
    /** Route a decoded KIND_TSYNC_REWIND result to the component that fired it. */
    void RouteRewindResult(const uint8* Body, int32 Len);
    /** Find the component bound to `ObjectId`, if any. */
    UCitadelTransformSync* FindComponent(uint32 ObjectId);
};
