// CitadelNetworkedActor.h — out-of-the-box presence + replicated spawn.
//
// UCitadelNetworkedActorSubsystem turns "a client connects" into "every other
// client spawns its avatar, and it sees everyone already present", with no
// per-object wiring. It sits ABOVE transform-sync: it spawns/destroys the remote
// proxy actors and binds each to a UCitadelTransformSync, while the existing
// snapshot path animates them. The local owner moves with native Unreal input;
// this subsystem relays its transform to the server each tick (KIND_NA_STATE).
//
// Usage (typically from your GameMode/PlayerController once connected):
//   auto* NA = GetGameInstance->GetSubsystem<UCitadelNetworkedActorSubsystem>;
//   NA->RegisterArchetype(0, BP_ThirdPersonCharacterClass); // archetype 0
//   NA->AnnouncePresence(MyPawn, /*ArchetypeId=*/0);        // relay MyPawn
// Give the spawnable actor(s) the ICitadelReplicated interface to react to
// spawn/despawn (possess the local owner, leave remote proxies interpolated).
#pragma once

#include "CoreMinimal.h"
#include "Subsystems/GameInstanceSubsystem.h"
#include "Tickable.h"
#include "Templates/SubclassOf.h"

#include "CitadelNetworkedActor.generated.h"

class AActor;
class UCitadelTransformSyncSubsystem;

UCLASS()
class UCitadelNetworkedActorSubsystem : public UGameInstanceSubsystem, public FTickableGameObject
{
    GENERATED_BODY()

public:
    /** Map an archetype id (chosen by the server / game logic) to the actor class
     *  to instantiate for a remote proxy. Call before AnnouncePresence. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Replication")
    void RegisterArchetype(int32 ArchetypeId, TSubclassOf<AActor> ActorClass);

    /** Select movement for a local archetype. Relay is the default and preserves
     *  the legacy NA_STATE wire exactly. PredictedAuthoritative makes the native
     *  owner use the existing transform-sync input/reconciliation component; the
     *  server must list the same archetype in
     *  transform_sync.predicted_authoritative_archetypes. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Replication")
    void SetPredictedAuthoritative(int32 ArchetypeId, bool bEnabled);

    /** Announce this client's avatar to the server. `LocalPawn` is the already
     *  existing native pawn whose transform is relayed to peers (Citadel does NOT
     *  spawn it). Opts into transform-sync so peer snapshots arrive, sends
     *  KIND_NA_PRESENCE, and begins relaying `LocalPawn`'s transform each tick. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Replication")
    void AnnouncePresence(AActor* LocalPawn, int32 ArchetypeId);

    /** How many owner-state relay packets to send per second (default 60, matching
     *  the server's 60 Hz sim so the owner's transform reaches the server one sim
     *  tick behind at most). Lower it to trade responsiveness for uplink bandwidth. */
    UPROPERTY(EditAnywhere, BlueprintReadWrite, Category = "Citadel|Replication")
    float StateSendHz = 60.0f;

    /** Route a decoded NA frame here (called by the transform-sync inbound pump,
     *  the single reader of the client's envelope queue). */
    void RouteNaFrame(uint16 Kind, const uint8* Body, int32 Len);

    // FTickableGameObject
    virtual void Tick(float DeltaTime) override;
    virtual bool IsTickable() const override { return bAnnounced; }
    virtual TStatId GetStatId() const override;
    virtual bool IsTickableInEditor() const override { return false; }
    virtual UWorld* GetTickableGameObjectWorld() const override;

private:
    void HandleSpawn(uint32 ObjectId, uint16 ArchetypeId, uint64 Owner, const float Transform[10]);
    void HandleDespawn(uint32 ObjectId);
    void SendOwnerState();
    UCitadelTransformSyncSubsystem* TransformSub() const;

    /** archetype_id -> actor class for remote proxies. */
    UPROPERTY()
    TMap<int32, TSubclassOf<AActor>> Archetypes;

    /** Archetypes whose local native owner uses validated input, not NA_STATE. */
    TSet<int32> PredictedAuthoritativeArchetypes;

    /** object_id -> spawned remote proxy actor. */
    UPROPERTY()
    TMap<int64, TWeakObjectPtr<AActor>> Proxies;

    /** The local pawn whose transform we relay (native-moved, not Citadel-spawned). */
    UPROPERTY()
    TWeakObjectPtr<AActor> LocalActor;

    uint64 LocalParticipantId = 0;
    uint32 LocalObjectId = 0;
    int32 LocalArchetypeId = 0;
    bool bAnnounced = false;
    bool bAwaitingSelfSpawn = false;
    bool bLocalPredictedAuthoritative = false;
    float StateAccum = 0.0f;
};
