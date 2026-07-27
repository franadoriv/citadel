// CitadelNetworkedActor.cpp — presence + replicated spawn.

#include "CitadelNetworkedActor.h"

#include "CitadelReplicated.h"
#include "CitadelTransformSync.h"
#include "CitadelTransformWire.h"
#include "CitadelWire.h"

#include "Engine/GameInstance.h"
#include "Engine/World.h"
#include "GameFramework/Actor.h"

#include <vector>

namespace
{
    // Big-endian readers (mirror the server's raw-f32 NA layout). The caller has
    // already bound-checked the buffer, so these only advance `Off`.
    uint16 ReadBeU16(const uint8* B, size_t& Off)
    {
        const uint16 V = (uint16(B[Off]) << 8) | uint16(B[Off + 1]);
        Off += 2;
        return V;
    }

    uint32 ReadBeU32(const uint8* B, size_t& Off)
    {
        uint32 V = 0;
        for (int i = 0; i < 4; ++i)
        {
            V = (V << 8) | uint32(B[Off + i]);
        }
        Off += 4;
        return V;
    }

    uint64 ReadBeU64(const uint8* B, size_t& Off)
    {
        uint64 V = 0;
        for (int i = 0; i < 8; ++i)
        {
            V = (V << 8) | uint64(B[Off + i]);
        }
        Off += 8;
        return V;
    }

    float ReadBeF32(const uint8* B, size_t& Off)
    {
        const uint32 U = ReadBeU32(B, Off);
        float F;
        FMemory::Memcpy(&F, &U, 4);
        return F;
    }

    // Append an actor's world transform in the NA raw-f32 layout:
    // position[3] (cm) · rotation[4] (quat xyzw) · velocity[3] (cm/s). This matches
    // the convention UCitadelTransformSync applies verbatim on the peer side.
    void AppendActorTransform(std::vector<uint8_t>& Body, const AActor* Actor)
    {
        const FVector Pos = Actor->GetActorLocation();
        const FQuat Rot = Actor->GetActorQuat();
        const FVector Vel = Actor->GetVelocity();
        CitadelTransform::PutBeF32(Body, (float)Pos.X);
        CitadelTransform::PutBeF32(Body, (float)Pos.Y);
        CitadelTransform::PutBeF32(Body, (float)Pos.Z);
        CitadelTransform::PutBeF32(Body, (float)Rot.X);
        CitadelTransform::PutBeF32(Body, (float)Rot.Y);
        CitadelTransform::PutBeF32(Body, (float)Rot.Z);
        CitadelTransform::PutBeF32(Body, (float)Rot.W);
        CitadelTransform::PutBeF32(Body, (float)Vel.X);
        CitadelTransform::PutBeF32(Body, (float)Vel.Y);
        CitadelTransform::PutBeF32(Body, (float)Vel.Z);
    }

    // Fixed byte sizes of the NA bodies (raw f32; see citadel_wire::na).
    constexpr int32 NA_TRANSFORM_BYTES = 10 * 4;
    constexpr int32 NA_SPAWN_BYTES = 4 + 2 + 8 + NA_TRANSFORM_BYTES; // 54
    constexpr int32 NA_DESPAWN_BYTES = 4;
}

UCitadelTransformSyncSubsystem* UCitadelNetworkedActorSubsystem::TransformSub() const
{
    const UGameInstance* GI = GetGameInstance();
    return GI ? GI->GetSubsystem<UCitadelTransformSyncSubsystem>() : nullptr;
}

UWorld* UCitadelNetworkedActorSubsystem::GetTickableGameObjectWorld() const
{
    const UGameInstance* GI = GetGameInstance();
    return GI ? GI->GetWorld() : nullptr;
}

TStatId UCitadelNetworkedActorSubsystem::GetStatId() const
{
    RETURN_QUICK_DECLARE_CYCLE_STAT(UCitadelNetworkedActorSubsystem, STATGROUP_Tickables);
}

void UCitadelNetworkedActorSubsystem::RegisterArchetype(int32 ArchetypeId, TSubclassOf<AActor> ActorClass)
{
    Archetypes.Add(ArchetypeId, ActorClass);
}

void UCitadelNetworkedActorSubsystem::SetPredictedAuthoritative(int32 ArchetypeId, bool bEnabled)
{
    if (bEnabled)
    {
        PredictedAuthoritativeArchetypes.Add(ArchetypeId);
    }
    else
    {
        PredictedAuthoritativeArchetypes.Remove(ArchetypeId);
    }
}

void UCitadelNetworkedActorSubsystem::AnnouncePresence(AActor* LocalPawn, int32 ArchetypeId)
{
    if (!LocalPawn)
    {
        return;
    }
    LocalActor = LocalPawn;
    LocalArchetypeId = ArchetypeId;
    bLocalPredictedAuthoritative = PredictedAuthoritativeArchetypes.Contains(ArchetypeId);
    bAwaitingSelfSpawn = true;
    bAnnounced = true;

    UCitadelTransformSyncSubsystem* Sub = TransformSub();
    if (!Sub)
    {
        return;
    }
    // Receive peer snapshots so the spawned proxies animate.
    Sub->OptIn();

    // KIND_NA_PRESENCE body: archetype_id u16 + transform.
    std::vector<uint8_t> Body;
    CitadelTransform::PutBeU16(Body, (uint16)ArchetypeId);
    AppendActorTransform(Body, LocalPawn);
    TArray<uint8> Payload;
    Payload.Append(Body.data(), Body.size());
    Sub->SendFrame(CitadelWire::KIND_NA_PRESENCE, Payload, /*bReliable=*/true);
}

void UCitadelNetworkedActorSubsystem::RouteNaFrame(uint16 Kind, const uint8* Body, int32 Len)
{
    using namespace CitadelWire;
    if (Kind == KIND_NA_SPAWN)
    {
        if (Len < NA_SPAWN_BYTES)
        {
            return;
        }
        size_t Off = 0;
        const uint32 ObjectId = ReadBeU32(Body, Off);
        const uint16 Arch = ReadBeU16(Body, Off);
        const uint64 Owner = ReadBeU64(Body, Off);
        float T[10];
        for (int i = 0; i < 10; ++i)
        {
            T[i] = ReadBeF32(Body, Off);
        }
        HandleSpawn(ObjectId, Arch, Owner, T);
    }
    else if (Kind == KIND_NA_SPAWN_BATCH)
    {
        if (Len < 2)
        {
            return;
        }
        size_t Off = 0;
        const uint16 Count = ReadBeU16(Body, Off);
        for (uint16 i = 0; i < Count; ++i)
        {
            if ((int32)Off + NA_SPAWN_BYTES > Len)
            {
                break; // truncated / malformed batch; stop.
            }
            const uint32 ObjectId = ReadBeU32(Body, Off);
            const uint16 Arch = ReadBeU16(Body, Off);
            const uint64 Owner = ReadBeU64(Body, Off);
            float T[10];
            for (int j = 0; j < 10; ++j)
            {
                T[j] = ReadBeF32(Body, Off);
            }
            HandleSpawn(ObjectId, Arch, Owner, T);
        }
    }
    else if (Kind == KIND_NA_DESPAWN)
    {
        if (Len < NA_DESPAWN_BYTES)
        {
            return;
        }
        size_t Off = 0;
        const uint32 ObjectId = ReadBeU32(Body, Off);
        HandleDespawn(ObjectId);
    }
}

void UCitadelNetworkedActorSubsystem::HandleSpawn(uint32 ObjectId, uint16 ArchetypeId, uint64 Owner, const float Transform[10])
{
    // Is this our own actor? The server sends the owner its own spawn FIRST, so the
    // first spawn after AnnouncePresence is self; thereafter match by participant id.
    bool bSelf = false;
    if (bAwaitingSelfSpawn)
    {
        bAwaitingSelfSpawn = false;
        LocalParticipantId = Owner;
        LocalObjectId = ObjectId;
        bSelf = true;
    }
    else if (LocalParticipantId != 0 && Owner == LocalParticipantId)
    {
        LocalObjectId = ObjectId;
        bSelf = true;
    }

    if (bSelf)
    {
        // The local owner is the native pawn; do NOT spawn a proxy. Notify it so it
        // can possess itself / enable input if it opts into the interface.
        if (AActor* Actor = LocalActor.Get())
        {
            // The predicted path reuses the component and KIND_TSYNC_INPUT built
            // for transform-sync P2. Create/bind it before the server's ordered
            // ROLE frame arrives; Relay deliberately does not create one.
            if (bLocalPredictedAuthoritative)
            {
                UCitadelTransformSync* Sync = NewObject<UCitadelTransformSync>(Actor);
                Sync->ObjectId = (int64)ObjectId;
                Sync->Role = ECitadelSyncRole::OwnerPredicted;
                Sync->RegisterComponent();
            }
            if (Actor->Implements<UCitadelReplicated>())
            {
                FCitadelSpawnInfo Info;
                Info.bIsLocalOwner = true;
                Info.ObjectId = (int64)ObjectId;
                Info.OwnerId = (int64)Owner;
                Info.ArchetypeId = (int32)ArchetypeId;
                ICitadelReplicated::Execute_OnCitadelSpawn(Actor, Info);
            }
        }
        return;
    }

    // Remote proxy — idempotent (a batch can repeat a spawn we already have).
    if (Proxies.Contains((int64)ObjectId))
    {
        return;
    }
    const TSubclassOf<AActor>* ClassPtr = Archetypes.Find((int32)ArchetypeId);
    if (!ClassPtr || !ClassPtr->Get())
    {
        UE_LOG(LogTemp, Warning,
               TEXT("Citadel: no archetype registered for id %d; cannot spawn proxy object %u"),
               (int32)ArchetypeId, ObjectId);
        return;
    }
    UWorld* World = GetTickableGameObjectWorld();
    if (!World)
    {
        return;
    }
    const FVector Pos(Transform[0], Transform[1], Transform[2]);
    const FQuat Rot(Transform[3], Transform[4], Transform[5], Transform[6]);
    FActorSpawnParameters Params;
    Params.SpawnCollisionHandlingOverride = ESpawnActorCollisionHandlingMethod::AlwaysSpawn;
    AActor* Actor = World->SpawnActor<AActor>(ClassPtr->Get(), FTransform(Rot, Pos), Params);
    if (!Actor)
    {
        return;
    }
    // Bind a transform-sync component so the existing snapshot path animates it.
    UCitadelTransformSync* Sync = NewObject<UCitadelTransformSync>(Actor);
    Sync->ObjectId = (int64)ObjectId;
    Sync->Role = ECitadelSyncRole::RemoteInterpolated;
    Sync->RegisterComponent();

    Proxies.Add((int64)ObjectId, Actor);

    if (Actor->Implements<UCitadelReplicated>())
    {
        FCitadelSpawnInfo Info;
        Info.bIsLocalOwner = false;
        Info.ObjectId = (int64)ObjectId;
        Info.OwnerId = (int64)Owner;
        Info.ArchetypeId = (int32)ArchetypeId;
        ICitadelReplicated::Execute_OnCitadelSpawn(Actor, Info);
    }
}

void UCitadelNetworkedActorSubsystem::HandleDespawn(uint32 ObjectId)
{
    TWeakObjectPtr<AActor> Found;
    if (Proxies.RemoveAndCopyValue((int64)ObjectId, Found))
    {
        if (AActor* Actor = Found.Get())
        {
            if (Actor->Implements<UCitadelReplicated>())
            {
                ICitadelReplicated::Execute_OnCitadelDespawn(Actor);
            }
            Actor->Destroy();
        }
    }
}

void UCitadelNetworkedActorSubsystem::SendOwnerState()
{
    AActor* Actor = LocalActor.Get();
    if (!Actor || LocalObjectId == 0)
    {
        return;
    }
    UCitadelTransformSyncSubsystem* Sub = TransformSub();
    if (!Sub)
    {
        return;
    }
    // KIND_NA_STATE body: object_id u32 + transform.
    std::vector<uint8_t> Body;
    CitadelTransform::PutBeU32(Body, LocalObjectId);
    AppendActorTransform(Body, Actor);
    TArray<uint8> Payload;
    Payload.Append(Body.data(), Body.size());
    Sub->SendFrame(CitadelWire::KIND_NA_STATE, Payload, /*bReliable=*/false);
}

void UCitadelNetworkedActorSubsystem::Tick(float DeltaTime)
{
    if (!bAnnounced || bLocalPredictedAuthoritative || LocalObjectId == 0 || StateSendHz <= 0.0f)
    {
        return;
    }
    StateAccum += DeltaTime;
    const float Period = 1.0f / StateSendHz;
    if (StateAccum >= Period)
    {
        StateAccum = 0.0f;
        SendOwnerState();
    }
}
