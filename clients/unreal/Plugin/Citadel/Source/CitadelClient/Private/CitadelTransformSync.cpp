// CitadelTransformSync.cpp — implementation of the Unreal transform-sync surface
// (, P1). See CitadelTransformSync.h for the design. C++ is not built in
// CI; the in-editor two-client behavior is a manual pre-release check.

#include "CitadelTransformSync.h"
#include "CitadelNetworkPeer.h"

#include "CitadelClientSubsystem.h"
#include "CitadelNetworkedActor.h"
#include "CitadelRoom.h"
#include "CitadelWire.h"
#include "Engine/GameInstance.h"
#include "GameFramework/Actor.h"

#if WITH_DEV_AUTOMATION_TESTS
#include "Misc/AutomationTest.h"
#endif

// ------------------------- FCitadelRemoteWorldView --------------------------

void FCitadelRemoteWorldView::ResetState()
{
    Ring.Reset(); Samples.Reset(); OwnerAcks.Reset();
    LastAppliedId = 0; AckLatest = 0; AckHistory = 0; ClockEpoch = 0; LastObservedClockTick = 0;
    SimRateHz = 60.0f; SendRateHz = 20.0f;
    BufferMultiplier = BufferCeil;
}

void FCitadelRemoteWorldView::SetCodec(const CitadelTransform::FCodecParams& InParams)
{
    Params = InParams; bHaveCodec = true;
    // A reliable HELLO begins a fresh connection/match lifetime, so no old
    // v1/v2 delta baseline or epoch may cross it.
    ResetState();
}

bool FCitadelRemoteWorldView::ResetV2Epoch(uint64 Epoch)
{
    if (Epoch == 0) { return false; }
    ResetState();
    ClockEpoch = Epoch;
    return true;
}

bool FCitadelRemoteWorldView::ApplyV2Datagram(const uint8* Body, int32 Len)
{
    // epoch:u64 | tick:u64 | tick_hz:u16 | byte-for-byte v1 snapshot
    CitadelTransform::FClockMetadata Clock;
    if (!bHaveCodec || !CitadelTransform::FClockMetadata::Decode(Body, static_cast<size_t>(Len), Clock)
        || (ClockEpoch != 0 && ClockEpoch != Clock.Epoch)) { return false; }
    if (!ApplyDatagram(Body + 18, Len - 18)) { return false; }
    ClockEpoch = Clock.Epoch;
    LastObservedClockTick = Clock.Tick;
    return true;
}

bool FCitadelRemoteWorldView::ApplyDatagram(const uint8* Body, int32 Len)
{
    if (!bHaveCodec) { return false; }
    CitadelTransform::FSnapshot Snap;
    if (!Snap.Decode(Body, static_cast<size_t>(Len), Params)) { return false; }

    // Monotonic guard: never apply an older-or-equal id (reorder/dup).
    if (LastAppliedId != 0 && Snap.SnapshotId <= LastAppliedId) { return false; }
    // Base guard: a delta whose base we do not hold is unrecoverable; drop it.
    if (Snap.BaseSnapshotId != 0 && !Ring.Contains(Snap.BaseSnapshotId)) { return false; }

    TMap<uint32, FCitadelRemoteObject> State;
    if (Snap.BaseSnapshotId != 0)
    {
        State = Ring[Snap.BaseSnapshotId];
    }
    for (uint32 RemovedId : Snap.Removed) { State.Remove(RemovedId); }

    for (const CitadelTransform::FObjectUpdate& U : Snap.Updates)
    {
        FCitadelRemoteObject* Prev = State.Find(U.ObjectId);
        const bool bIsDelta = Prev != nullptr && Prev->GenEpoch == U.GenEpoch;
        FCitadelRemoteObject Obj;
        if (bIsDelta)
        {
            Obj = *Prev;
        }
        else
        {
            // Full: require position + rotation, else malformed; skip object.
            if (!U.bHasPosition || !U.bHasRotation) { continue; }
            Obj = FCitadelRemoteObject();
        }
        Obj.GenEpoch = U.GenEpoch;
        if (U.bHasPosition) { Obj.Position = FVector(U.Position[0], U.Position[1], U.Position[2]); }
        if (U.bHasRotation) { Obj.Rotation = FQuat(U.Rotation[0], U.Rotation[1], U.Rotation[2], U.Rotation[3]); }
        if (U.bHasVelocity) { Obj.Velocity = FVector(U.Velocity[0], U.Velocity[1], U.Velocity[2]); Obj.bHasVelocity = true; }
        State.Add(U.ObjectId, Obj);
        // Record the owner input-ack (monotonic) for the prediction layer (§5.1).
        if (U.bHasInputSeq)
        {
            uint32& Slot = OwnerAcks.FindOrAdd(U.ObjectId);
            Slot = FMath::Max(Slot, U.LastInputSeq);
        }
    }

    SendRateHz = FMath::Max<float>(1.0f, static_cast<float>(Snap.SendRateHz));
    SimRateHz = FMath::Max<float>(1.0f, static_cast<float>(Params.SimRateHz));
    // Adaptive interpolation buffer (lockstep with Rust RemoteWorldView): decay
    // toward the floor on a clean link (consecutive ids), grow back toward the
    // ceiling on detected loss (a gap in applied snapshot ids). Localhost/LAN with
    // no loss converges to ~1.5x automatically; a lossy link holds ~2.5x.
    if (bAdaptive && LastAppliedId != 0 && Snap.SnapshotId > LastAppliedId)
    {
        const uint32 Gap = Snap.SnapshotId - LastAppliedId;
        if (Gap > 1)
        {
            BufferMultiplier = FMath::Min(BufferCeil, BufferMultiplier + 0.5f * static_cast<float>(Gap - 1));
        }
        else
        {
            BufferMultiplier = FMath::Max(BufferFloor, BufferMultiplier - 0.02f);
        }
    }
    Ring.Add(Snap.SnapshotId, State);
    PruneRing();
    LastAppliedId = Snap.SnapshotId;
    Ack(Snap.SnapshotId);

    // Feed the jitter buffer: every held object gets a sample at this tick.
    for (const TPair<uint32, FCitadelRemoteObject>& Pair : State)
    {
        PushSample(Pair.Key, Snap.ServerTick, Pair.Value);
    }
    // Drop buffers for objects no longer present.
    for (auto It = Samples.CreateIterator(); It; ++It)
    {
        if (!State.Contains(It.Key())) { It.RemoveCurrent(); }
    }
    return true;
}

void FCitadelRemoteWorldView::Ack(uint32 Id)
{
    if (Id == 0) { return; }
    if (AckLatest == 0)
    {
        AckLatest = Id;
        AckHistory = 0;
        return;
    }
    if (Id > AckLatest)
    {
        const uint32 Delta = Id - AckLatest;
        if (Delta < 32) { AckHistory = (AckHistory << Delta) | (1u << (Delta - 1)); }
        else if (Delta == 32) { AckHistory = 1u << 31; }
        else { AckHistory = 0; }
        AckLatest = Id;
    }
    else if (Id < AckLatest)
    {
        const uint32 Offset = AckLatest - Id;
        if (Offset <= 32) { AckHistory |= (1u << (Offset - 1)); }
    }
}

void FCitadelRemoteWorldView::AckBytes(uint8 Out[8]) const
{
    CitadelTransform::EncodeAck(AckLatest, AckHistory, Out);
}

bool FCitadelRemoteWorldView::GetObject(uint32 ObjectId, FCitadelRemoteObject& Out) const
{
    const TMap<uint32, FCitadelRemoteObject>* State = Ring.Find(LastAppliedId);
    if (!State) { return false; }
    const FCitadelRemoteObject* Obj = State->Find(ObjectId);
    if (!Obj) { return false; }
    Out = *Obj;
    return true;
}

bool FCitadelRemoteWorldView::GetOwnerAck(uint32 ObjectId, uint32& OutSeq) const
{
    const uint32* Seq = OwnerAcks.Find(ObjectId);
    if (!Seq) { return false; }
    OutSeq = *Seq;
    return true;
}

#if WITH_DEV_AUTOMATION_TESTS
IMPLEMENT_SIMPLE_AUTOMATION_TEST(
    FCitadelTransformV2WrapperParityTest,
    "Citadel.TransformSync.V2WrapperParity",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FCitadelTransformV2WrapperParityTest::RunTest(const FString& Parameters)
{
    // A complete v1 snapshot with no object updates: tick=1, id=1, base=0,
    // send_rate=60, removed=0, updates=0. It exercises the real view's v1
    // decoder while keeping the vector independent of any actor/world setup.
    const uint8 V1Snapshot[] = {
        0,0,0,1, 0,0,0,1, 0,0,0,0, 60, 0,0, 0,0
    };
    const uint8 V2Epoch7[] = {
        0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,99, 0,60,
        0,0,0,1, 0,0,0,1, 0,0,0,0, 60, 0,0, 0,0
    };
    const uint8 V2Epoch8[] = {
        0,0,0,0,0,0,0,8, 0,0,0,0,0,0,0,100, 0,60,
        0,0,0,1, 0,0,0,1, 0,0,0,0, 60, 0,0, 0,0
    };
    FCitadelRemoteWorldView View;
    View.SetCodec(CitadelTransform::FCodecParams());
    TestTrue(TEXT("v2 wrapper applies a valid embedded v1 snapshot"), View.ApplyV2Datagram(V2Epoch7, UE_ARRAY_COUNT(V2Epoch7)));
    uint8 Ack[8] = {};
    View.AckBytes(Ack);
    TestEqual(TEXT("v2 apply advances the managed acknowledgement"), Ack[3], uint8(1));
    TestFalse(TEXT("mixed epoch is rejected before touching the v1 view"), View.ApplyV2Datagram(V2Epoch8, UE_ARRAY_COUNT(V2Epoch8)));
    TestTrue(TEXT("explicit reset admits a fresh nonzero epoch"), View.ResetV2Epoch(8));
    View.AckBytes(Ack);
    TestEqual(TEXT("reset clears managed acknowledgement state"), Ack[3], uint8(0));
    TestTrue(TEXT("reset epoch applies its v2 snapshot"), View.ApplyV2Datagram(V2Epoch8, UE_ARRAY_COUNT(V2Epoch8)));
    View.SetCodec(CitadelTransform::FCodecParams());
    TestTrue(TEXT("v1 fallback remains the real view apply path"), View.ApplyDatagram(V1Snapshot, UE_ARRAY_COUNT(V1Snapshot)));

    const std::vector<uint8_t> V1Input = { 0xaa, 0xbb };
    std::vector<uint8_t> V2Input;
    TestTrue(TEXT("v2 input wrapper encodes accepted epoch/tick diagnostics"),
        CitadelTransform::FInputV2Metadata::Encode(8, 100, V1Input, V2Input));
    const std::vector<uint8_t> ExpectedV2Input = {
        0,0,0,0,0,0,0,8, 0,0,0,0,0,0,0,100, 0, 0xaa,0xbb };
    TestTrue(TEXT("v2 input wrapper is big-endian epoch/tick, zero flags, unchanged v1 bundle"),
        V2Input == ExpectedV2Input);
    TestFalse(TEXT("v2 input wrapper rejects zero epoch"),
        CitadelTransform::FInputV2Metadata::Encode(0, 100, V1Input, V2Input));
    const uint8 AcceptedManifest[] = { 2, 1 };
    const uint8 UnknownManifest[] = { 2, 2 };
    TestTrue(TEXT("only exact HELLO echo accepts v2"),
        CitadelTransform::FV2Manifest::IsClock(AcceptedManifest, UE_ARRAY_COUNT(AcceptedManifest)));
    TestFalse(TEXT("unknown HELLO capability does not select v2"),
        CitadelTransform::FV2Manifest::IsClock(UnknownManifest, UE_ARRAY_COUNT(UnknownManifest)));
    return true;
}
#endif

// -------------------------- FCitadelPredictionRing --------------------------

void FCitadelPredictionRing::ApplyInput(FVector& State, const FCitadelPredictedInput& In)
{
    const float Dt = (FMath::IsFinite(In.Dt) && In.Dt > 0.0f) ? In.Dt : 0.0f;
    State += In.MoveVelocity * Dt;
}

uint32 FCitadelPredictionRing::PushInput(const FVector& MoveVelocity, float Dt)
{
    FCitadelPredictedInput In;
    In.Seq = NextSeq;
    In.MoveVelocity = MoveVelocity;
    In.Dt = Dt;
    NextSeq = FMath::Max<uint32>(1, NextSeq + 1);
    ApplyInput(Predicted, In);
    Inputs.Add(In);
    return In.Seq;
}

bool FCitadelPredictionRing::Reconcile(const FVector& Authoritative, uint32 LastInputSeq)
{
    const FVector OldRender = RenderPosition();

    // 1. Drop acked inputs (seq <= last_input_seq).
    Inputs.RemoveAll([LastInputSeq](const FCitadelPredictedInput& I) { return I.Seq <= LastInputSeq; });

    // 2. Snap sim state to authority, replay the unacked tail in seq order.
    Predicted = Authoritative;
    Inputs.Sort([](const FCitadelPredictedInput& A, const FCitadelPredictedInput& B) { return A.Seq < B.Seq; });
    for (const FCitadelPredictedInput& In : Inputs) { ApplyInput(Predicted, In); }

    // 3. Preserve the rendered position via the visual offset (sim stays exact).
    FVector Offset = OldRender - Predicted;
    const float Err = Offset.Size();
    if (Err >= Config.HardSnapCm) { Offset = FVector::ZeroVector; }
    VisualOffset = Offset;
    return Err > 1e-3f;
}

void FCitadelPredictionRing::AdvanceSmoothing()
{
    const float Err = VisualOffset.Size();
    if (Err <= 1e-3f || Err >= Config.HardSnapCm) { VisualOffset = FVector::ZeroVector; return; }
    float Factor;
    if (Err <= Config.SmallCm) { Factor = Config.SmoothingSmall; }
    else if (Err >= Config.LargeCm) { Factor = Config.SmoothingLarge; }
    else
    {
        const float T = (Err - Config.SmallCm) / FMath::Max(1e-3f, Config.LargeCm - Config.SmallCm);
        Factor = Config.SmoothingSmall + (Config.SmoothingLarge - Config.SmoothingSmall) * T;
    }
    VisualOffset *= Factor;
}

void FCitadelRemoteWorldView::PushSample(uint32 ObjectId, uint32 Tick, const FCitadelRemoteObject& Obj)
{
    TArray<FSample>& Buf = Samples.FindOrAdd(ObjectId);
    if (Buf.Num() > 0 && Tick <= Buf.Last().Tick) { return; } // dedup / reorder
    FSample S;
    S.Tick = Tick;
    S.Position = Obj.Position;
    S.Rotation = Obj.Rotation;
    S.Velocity = Obj.Velocity;
    S.bHasVelocity = Obj.bHasVelocity;
    Buf.Add(S);
    while (Buf.Num() > MaxSamples) { Buf.RemoveAt(0); }
}

void FCitadelRemoteWorldView::PruneRing()
{
    while (Ring.Num() > MaxRing)
    {
        uint32 Oldest = TNumericLimits<uint32>::Max();
        for (const TPair<uint32, TMap<uint32, FCitadelRemoteObject>>& Pair : Ring)
        {
            Oldest = FMath::Min(Oldest, Pair.Key);
        }
        Ring.Remove(Oldest);
    }
}

int32 FCitadelRemoteWorldView::LatestSampleTick() const
{
    int32 Latest = -1;
    for (const TPair<uint32, TArray<FSample>>& Pair : Samples)
    {
        if (Pair.Value.Num() > 0) { Latest = FMath::Max(Latest, static_cast<int32>(Pair.Value.Last().Tick)); }
    }
    return Latest;
}

float FCitadelRemoteWorldView::BufferDelayTicks() const
{
    const float SendInterval = 1.0f / SendRateHz;
    float DelaySecs = BufferMultiplier * SendInterval;
    DelaySecs = FMath::Clamp(DelaySecs, 1.5f * SendInterval, 0.4f);
    return DelaySecs * SimRateHz;
}

bool FCitadelRemoteWorldView::SampleNow(uint32 ObjectId, bool bHermite, float MaxExtrapolationSeconds, FVector& OutPos, FQuat& OutRot) const
{
    const TArray<FSample>* Buf = Samples.Find(ObjectId);
    if (!Buf || Buf->Num() == 0) { return false; }
    const int32 Latest = LatestSampleTick();
    if (Latest < 0) { return false; }
    const double RenderTick = static_cast<double>(Latest) - static_cast<double>(BufferDelayTicks());

    const FSample& First = (*Buf)[0];
    const FSample& Last = (*Buf)[Buf->Num() - 1];

    if (RenderTick <= static_cast<double>(First.Tick))
    {
        OutPos = First.Position; OutRot = First.Rotation; return true;
    }
    if (RenderTick >= static_cast<double>(Last.Tick))
    {
        // Bounded extrapolation from the last sample's velocity.
        double Ahead = RenderTick - static_cast<double>(Last.Tick);
        double AheadSecs = FMath::Min(Ahead / SimRateHz, static_cast<double>(MaxExtrapolationSeconds));
        OutPos = Last.Position;
        if (Last.bHasVelocity) { OutPos += Last.Velocity * static_cast<float>(AheadSecs); }
        OutRot = Last.Rotation;
        return true;
    }

    // Find the bracketing pair.
    FSample Lo = First;
    FSample Hi = Last;
    for (const FSample& S : *Buf)
    {
        if (static_cast<double>(S.Tick) <= RenderTick) { Lo = S; }
        if (static_cast<double>(S.Tick) >= RenderTick) { Hi = S; break; }
    }
    if (Hi.Tick == Lo.Tick) { OutPos = Lo.Position; OutRot = Lo.Rotation; return true; }

    const double Span = static_cast<double>(Hi.Tick - Lo.Tick);
    const double T = FMath::Clamp((RenderTick - static_cast<double>(Lo.Tick)) / Span, 0.0, 1.0);

    const bool bUseHermite = bHermite && Lo.bHasVelocity && Hi.bHasVelocity;
    if (bUseHermite)
    {
        const double H = Span / SimRateHz; // interval seconds
        const double T2 = T * T;
        const double T3 = T2 * T;
        const double H00 = 2*T3 - 3*T2 + 1;
        const double H10 = T3 - 2*T2 + T;
        const double H01 = -2*T3 + 3*T2;
        const double H11 = T3 - T2;
        FVector M0 = Lo.Velocity * static_cast<float>(H);
        FVector M1 = Hi.Velocity * static_cast<float>(H);
        OutPos = Lo.Position * static_cast<float>(H00)
               + M0 * static_cast<float>(H10)
               + Hi.Position * static_cast<float>(H01)
               + M1 * static_cast<float>(H11);
    }
    else
    {
        OutPos = FMath::Lerp(Lo.Position, Hi.Position, static_cast<float>(T));
    }
    OutRot = FQuat::Slerp(Lo.Rotation, Hi.Rotation, static_cast<float>(T));
    OutRot.Normalize();
    return true;
}

// ----------------------------- Component ------------------------------------

UCitadelTransformSync::UCitadelTransformSync()
{
    PrimaryComponentTick.bCanEverTick = true;
}

static UCitadelTransformSyncSubsystem* GetSyncSubsystem(const UActorComponent* Component)
{
    if (const UWorld* World = Component->GetWorld())
    {
        if (UGameInstance* GI = World->GetGameInstance())
        {
            return GI->GetSubsystem<UCitadelTransformSyncSubsystem>();
        }
    }
    return nullptr;
}

void UCitadelTransformSync::BeginPlay()
{
    Super::BeginPlay();
    if (UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this))
    {
        Sub->RegisterComponent(this);
    }
}

void UCitadelTransformSync::EndPlay(const EEndPlayReason::Type Reason)
{
    if (UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this))
    {
        Sub->UnregisterComponent(this);
    }
    Super::EndPlay(Reason);
}

void UCitadelTransformSync::TickComponent(float DeltaTime, ELevelTick TickType, FActorComponentTickFunction* ThisTickFunction)
{
    Super::TickComponent(DeltaTime, TickType, ThisTickFunction);
    UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this);
    AActor* Owner = GetOwner();
    if (!Sub || !Owner || ObjectId <= 0) { return; }

    if (bIsOwner)
    {
        // Owner-predicted: reconcile against the server ack, ease the visual
        // offset, and render the predicted (input-latency-free) transform.
        TickOwnerPredicted(DeltaTime);
    }
    else
    {
        TickRemoteInterpolated();
    }
    // Relevancy exit: when the object disappears from the reconstructed world the
    // component simply stops updating (the game may hide/despawn the actor).
}

void UCitadelTransformSync::TickRemoteInterpolated()
{
    UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this);
    AActor* Owner = GetOwner();
    if (!Sub || !Owner) { return; }
    FVector Pos;
    FQuat Rot;
    if (Sub->View().SampleNow(ObjectIdU32(), bHermitePosition, MaxExtrapolationSeconds, Pos, Rot))
    {
        Owner->SetActorLocationAndRotation(Pos, Rot);
        bEverApplied = true;
    }
}

void UCitadelTransformSync::TickOwnerPredicted(float DeltaTime)
{
    UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this);
    AActor* Owner = GetOwner();
    if (!Sub || !Owner) { return; }

    // Reconcile against the newest authoritative post-input state + ack. The sim
    // state snaps to authority (collision-correct); only the RENDER eases (§5.1).
    uint32 AckSeq = 0;
    FCitadelRemoteObject Auth;
    if (Sub->View().GetOwnerAck(ObjectIdU32(), AckSeq)
        && Sub->View().GetObject(ObjectIdU32(), Auth))
    {
        Prediction.Config.HardSnapCm = HardSnapThresholdCm;
        if (Prediction.Reconcile(Auth.Position, AckSeq))
        {
            const float ErrorCm = Prediction.VisualOffsetVec().Size();
            OnReconciled.Broadcast(ErrorCm);
        }
    }
    Prediction.AdvanceSmoothing();

    // Render the predicted position (+ smoothed offset). Rotation from the local
    // actor is left to gameplay/camera; prediction here is kinematic position.
    Owner->SetActorLocation(Prediction.RenderPosition());
    bEverApplied = true;
}

void UCitadelTransformSync::SubmitInput(FVector MoveVelocity, float Dt)
{
    if (!bIsOwner || ObjectId <= 0) { return; } // only owner write path (§2.3)
    const uint32 Seq = Prediction.PushInput(MoveVelocity, Dt);
    SendInputBundle(Seq, MoveVelocity, Dt);
}

void UCitadelTransformSync::RewindHitTest(FVector Origin, FVector Direction)
{
    if (!bIsOwner || ObjectId <= 0) { return; }
    // The fire rides the NEXT input bundle so it is processed exactly once in seq
    // order; the server resolves it. We never resolve the hit locally (§5.2).
    bPendingFire = true;
    PendingFire.Origin[0] = Origin.X; PendingFire.Origin[1] = Origin.Y; PendingFire.Origin[2] = Origin.Z;
    PendingFire.Direction[0] = Direction.X; PendingFire.Direction[1] = Direction.Y; PendingFire.Direction[2] = Direction.Z;
    // Send a zero-movement input immediately so the fire is not delayed to the
    // next movement tick.
    const uint32 Seq = Prediction.PushInput(FVector::ZeroVector, 0.0f);
    SendInputBundle(Seq, FVector::ZeroVector, 0.0f);
}

void UCitadelTransformSync::SendInputBundle(uint32 NewSeq, const FVector& MoveVelocity, float Dt)
{
    UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this);
    if (!Sub) { return; }

    CitadelTransform::FInputFrame Frame;
    Frame.InputSeq = NewSeq;
    Frame.SimTick = NewSeq; // client sim tick index (monotonic with seq here)
    Frame.Dt = Dt;
    Frame.ObjectId = ObjectIdU32();
    Frame.OwnershipEpoch = OwnershipEpoch;
    Frame.MoveVelocity[0] = MoveVelocity.X;
    Frame.MoveVelocity[1] = MoveVelocity.Y;
    Frame.MoveVelocity[2] = MoveVelocity.Z;
    if (bPendingFire)
    {
        Frame.bHasFire = true;
        Frame.Fire = PendingFire;
        PendingFireSeqs.Add(NewSeq);
        bPendingFire = false;
    }
    RecentInputs.Add(Frame);
    while (RecentInputs.Num() > FMath::Max(1, InputRedundancy)) { RecentInputs.RemoveAt(0); }

    // Redundant bundle of the last N frames + the piggybacked snapshot ack/hint.
    std::vector<CitadelTransform::FInputFrame> Bundle;
    Bundle.reserve(RecentInputs.Num());
    for (const CitadelTransform::FInputFrame& F : RecentInputs) { Bundle.push_back(F); }
    const uint32 AckedId = Sub->View().LastAppliedSnapshotId();
    std::vector<uint8_t> Body = CitadelTransform::EncodeInputBundle(AckedId, AckedId, Bundle);

    TArray<uint8> Payload;
    if (Sub->IsV2InputNegotiated())
    {
        std::vector<uint8_t> V2Body;
        if (CitadelTransform::FInputV2Metadata::Encode(
                Sub->View().V2Epoch(), Sub->View().LastObservedV2Tick(), Body, V2Body))
        {
            Payload.Append(V2Body.data(), static_cast<int32>(V2Body.size()));
            Sub->SendFrame(CitadelWire::KIND_TSYNC_V2_INPUT, Payload, /*bReliable=*/false);
            return;
        }
    }
    // No exact HELLO echo, no accepted nonzero epoch, or an invalid wrapper:
    // retain the unchanged v1 input path instead of guessing a v2 layout.
    Payload.Append(Body.data(), static_cast<int32>(Body.size()));
    Sub->SendFrame(CitadelWire::KIND_TSYNC_INPUT, Payload, /*bReliable=*/false);
}

void UCitadelTransformSync::OnRoleFrame(bool bNowOwned, uint32 InOwnershipEpoch)
{
    // Idempotent + epoch-guarded: ignore a stale/reordered transition (§2.2).
    if (InOwnershipEpoch < OwnershipEpoch) { return; }
    const bool bWasOwner = bIsOwner;
    OwnershipEpoch = InOwnershipEpoch;
    bIsOwner = bNowOwned;
    if (bIsOwner && !bWasOwner)
    {
        // Begin predicting from the last authoritative state (no replay of
        // pre-handoff input, §2.2).
        FCitadelRemoteObject Auth;
        FVector Start = GetOwner() ? GetOwner()->GetActorLocation() : FVector::ZeroVector;
        UCitadelTransformSyncSubsystem* Sub = GetSyncSubsystem(this);
        if (Sub && Sub->View().GetObject(ObjectIdU32(), Auth)) { Start = Auth.Position; }
        Prediction.Reset(Start);
        RecentInputs.Reset();
        PendingFireSeqs.Reset();
        bPendingFire = false;
    }
    if (bWasOwner != bIsOwner)
    {
        OnOwnershipChanged.Broadcast(bIsOwner, static_cast<int64>(OwnershipEpoch));
    }
}

bool UCitadelTransformSync::ClaimRewindResult(const CitadelTransform::FRewindResult& Result)
{
    if (!PendingFireSeqs.Contains(Result.InputSeq)) { return false; }
    PendingFireSeqs.Remove(Result.InputSeq);
    OnRewindResult.Broadcast(
        Result.bHit,
        static_cast<int64>(Result.ObjectId),
        FVector(Result.HitPoint[0], Result.HitPoint[1], Result.HitPoint[2]));
    return true;
}

// ----------------------------- Subsystem ------------------------------------

void UCitadelTransformSyncSubsystem::RegisterComponent(UCitadelTransformSync* Component)
{
    Components.AddUnique(Component);
}

void UCitadelTransformSyncSubsystem::UnregisterComponent(UCitadelTransformSync* Component)
{
    Components.RemoveAll([Component](const TWeakObjectPtr<UCitadelTransformSync>& P)
    {
        return !P.IsValid() || P.Get() == Component;
    });
}

void UCitadelTransformSyncSubsystem::OptIn()
{
    if (UCitadelClientSubsystem* Client = GetGameInstance()->GetSubsystem<UCitadelClientSubsystem>())
    {
        // Empty HELLO body: the server dictates the negotiation and replies.
        Client->Send(CitadelWire::KIND_TSYNC_HELLO, TArray<uint8>(), /*bReliable=*/true);
        // Dedicated v2 negotiation is opt-in; a server that does not reply keeps
        // the existing v1 snapshot path untouched.
        bOptedIn = true;
        SendV2Hello();
    }
}

void UCitadelTransformSyncSubsystem::PumpInbound()
{
    // Guard the GameInstance: now that we tick before opt-in, this can run in a frame
    // where GetGameInstance is null (CDO/teardown) — deref-then-crash otherwise.
    UGameInstance* GI = GetGameInstance();
    if (!GI) { return; }
    UCitadelClientSubsystem* Client = GI->GetSubsystem<UCitadelClientSubsystem>();
    if (!Client) { return; }
    uint16 Kind = 0;
    TArray<uint8> Payload;
    bool bAppliedAny = false;
    // Drain all currently-available envelopes this frame.
    while (Client->Poll(Kind, Payload) == ECitadelStatus::Ok)
    {
        if (Kind == CitadelWire::KIND_TSYNC_HELLO)
        {
            CitadelTransform::FCodecParams Params;
            if (Params.DecodeHello(Payload.GetData(), Payload.Num()))
            {
                WorldView.SetCodec(Params);
                // A reliable HELLO begins a new baseline lifetime. Acceptance
                // must be echoed again; never carry v2 permission across it.
                bV2Negotiated = false;
                SendV2Hello();
            }
        }
        else if (Kind == CitadelWire::KIND_TSYNC_V2_HELLO)
        {
            // Only the exact accepted manifest selects v2. Any other echo is
            // not a downgrade signal and leaves the v1 path active.
            bV2Negotiated = bOptedIn && WorldView.HasCodec()
                && CitadelTransform::FV2Manifest::IsClock(Payload.GetData(), Payload.Num());
        }
        else if (Kind == CitadelWire::KIND_TSYNC_SNAPSHOT)
        {
            if (WorldView.ApplyDatagram(Payload.GetData(), Payload.Num()))
            {
                bAppliedAny = true;
            }
        }
        else if (Kind == CitadelWire::KIND_TSYNC_V2_SNAPSHOT)
        {
            if (bV2Negotiated && WorldView.ApplyV2Datagram(Payload.GetData(), Payload.Num()))
            {
                bAppliedAny = true;
            }
        }
        else if (Kind == CitadelWire::KIND_TSYNC_ROLE)
        {
            RouteRoleFrame(Payload.GetData(), Payload.Num());
        }
        else if (Kind == CitadelWire::KIND_TSYNC_REWIND)
        {
            RouteRewindResult(Payload.GetData(), Payload.Num());
        }
        else if (Kind == CitadelWire::KIND_NA_SPAWN
                 || Kind == CitadelWire::KIND_NA_SPAWN_BATCH
                 || Kind == CitadelWire::KIND_NA_DESPAWN)
        {
            // Networked-Actors frames: this pump is the single reader of the
            // client's envelope queue, so it forwards them to the NA subsystem
            // (which spawns/destroys the proxy actors) rather than polling itself.
            if (UCitadelNetworkedActorSubsystem* NA =
                    GetGameInstance()->GetSubsystem<UCitadelNetworkedActorSubsystem>())
            {
                NA->RouteNaFrame(Kind, Payload.GetData(), Payload.Num());
            }
        }
        else if (Kind == CitadelWire::KIND_ROOM_JOINED || Kind == CitadelWire::KIND_ROOM_LEAVE)
        {
            // Room frames: forward to the room subsystem (fires OnRoomJoined etc.).
            if (UCitadelRoomSubsystem* Rooms =
                    GetGameInstance()->GetSubsystem<UCitadelRoomSubsystem>())
            {
                Rooms->RouteRoomFrame(Kind, Payload.GetData(), Payload.Num());
            }
        }
        else if (Kind == CitadelWire::KIND_NOTIFICATION)
        {
            // The transform subsystem is the single queue reader. Preserve live
            // notification delivery for games using transform sync by routing the
            // exact UTF-8 JSON payload to the client subsystem's Blueprint event.
            FUTF8ToTCHAR NotificationUtf8(
                reinterpret_cast<const ANSICHAR*>(Payload.GetData()), Payload.Num());
            Client->OnNotificationReceived.Broadcast(
                FString(NotificationUtf8.Length(), NotificationUtf8.Get()));
        }
        else if (Kind == CitadelWire::KIND_REP_DELTA)
        {
            // The shared queue has one reader.  NetworkPeer performs its own
            // canonical C-ABI decode, object-identity routing, apply, and ACK.
            UCitadelNetworkPeer::RouteRepDelta(GetGameInstance(), Payload);
        }
        // Other kinds are ignored here; a production subsystem routes them to game.
    }
    if (bAppliedAny) { SendAck(); }
}

void UCitadelTransformSyncSubsystem::SendV2Hello()
{
    if (UCitadelClientSubsystem* Client = GetGameInstance()->GetSubsystem<UCitadelClientSubsystem>())
    {
        TArray<uint8> V2Manifest;
        V2Manifest.Add(2); // TSYNC_V2_VERSION
        V2Manifest.Add(1); // TSYNC_V2_CLOCK_CAPABILITY
        Client->Send(CitadelWire::KIND_TSYNC_V2_HELLO, V2Manifest, /*bReliable=*/true);
    }
}

UCitadelTransformSync* UCitadelTransformSyncSubsystem::FindComponent(uint32 ObjectId)
{
    for (const TWeakObjectPtr<UCitadelTransformSync>& P : Components)
    {
        if (P.IsValid() && P->ObjectIdU32() == ObjectId) { return P.Get(); }
    }
    return nullptr;
}

void UCitadelTransformSyncSubsystem::RouteRoleFrame(const uint8* Body, int32 Len)
{
    CitadelTransform::FRoleFrame Role;
    if (!Role.Decode(Body, static_cast<size_t>(Len))) { return; }
    // An OwnerPredicted (role 0) role frame is only ever sent to the participant
    // that owns the object, so if we don't yet know our own participant id, latch
    // it from this frame. This lets the server drive ownership end to end without
    // a separate "here is your participant id" message (server player-slot mode).
    if (Role.Role == 0 && Role.Owner != 0 && LocalParticipantId == 0)
    {
        LocalParticipantId = Role.Owner;
    }
    UCitadelTransformSync* Component = FindComponent(Role.ObjectId);
    if (!Component) { return; }
    // OwnerPredicted (role 0) AND the owner is this local player => we own it.
    const bool bNowOwned =
        Role.Role == 0 && LocalParticipantId != 0 && Role.Owner == LocalParticipantId;
    Component->OnRoleFrame(bNowOwned, Role.OwnershipEpoch);
}

void UCitadelTransformSyncSubsystem::RouteRewindResult(const uint8* Body, int32 Len)
{
    CitadelTransform::FRewindResult Result;
    if (!Result.Decode(Body, static_cast<size_t>(Len))) { return; }
    // Deliver to whichever owner component fired this seq (the client never
    // resolves the hit itself; it only reports the authoritative result).
    for (const TWeakObjectPtr<UCitadelTransformSync>& P : Components)
    {
        if (P.IsValid() && P->ClaimRewindResult(Result)) { break; }
    }
}

void UCitadelTransformSyncSubsystem::SendFrame(uint16 Kind, const TArray<uint8>& Body, bool bReliable)
{
    if (UCitadelClientSubsystem* Client = GetGameInstance()->GetSubsystem<UCitadelClientSubsystem>())
    {
        Client->Send(Kind, Body, bReliable);
    }
}

void UCitadelTransformSyncSubsystem::SendAck()
{
    UCitadelClientSubsystem* Client = GetGameInstance()->GetSubsystem<UCitadelClientSubsystem>();
    if (!Client) { return; }
    uint8 AckBody[8];
    WorldView.AckBytes(AckBody);
    TArray<uint8> Payload;
    Payload.Append(AckBody, 8);
    Client->Send(CitadelWire::KIND_TSYNC_ACK, Payload, /*bReliable=*/false);
}

void UCitadelTransformSyncSubsystem::Tick(float DeltaTime)
{
    // Always pump the inbound queue, even before opt-in: room frames (ROOM_JOINED
    // etc.) must be routed before AnnouncePresence — which is what opts in. Gating
    // the pump on bOptedIn deadlocked that chain (join -> ROOM_JOINED -> OnRoomJoined
    // -> AnnouncePresence -> opt-in). PumpInbound early-outs until the client exists.
    PumpInbound();
    // Components apply their own interpolated transform in their TickComponent,
    // so nothing more is needed here; the subsystem only owns the shared view.
}
