// CitadelReplicated.h — the actor-facing hook for Citadel networked actors.
//
// An actor that should participate in the out-of-the-box presence + replicated
// spawn layer implements ICitadelReplicated. The
// UCitadelNetworkedActorSubsystem calls OnCitadelSpawn when the server spawns the
// actor (as a remote proxy, or as the local owner) and OnCitadelDespawn when it is
// removed. Blueprints override these (BlueprintNativeEvent) to wire up possession,
// camera, and input for the local owner, or leave a remote proxy purely
// interpolated.
#pragma once

#include "CoreMinimal.h"
#include "UObject/Interface.h"

#include "CitadelReplicated.generated.h"

/** Context passed to ICitadelReplicated::OnCitadelSpawn. */
USTRUCT(BlueprintType)
struct FCitadelSpawnInfo
{
    GENERATED_BODY()

    /** True when this actor represents THIS client's own player (the local owner):
     *  possess it with the local PlayerController and drive it with input. False
     *  for a remote proxy: it is interpolated from server snapshots and must NOT be
     *  possessed by the local PlayerController. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Replication")
    bool bIsLocalOwner = false;

    /** The transform-sync object id this actor is bound to. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Replication")
    int64 ObjectId = 0;

    /** The owning participant id (server session id). */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Replication")
    int64 OwnerId = 0;

    /** The archetype id the server spawned this actor from. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Replication")
    int32 ArchetypeId = 0;
};

// Blueprintable so actors can implement it (and its BlueprintNativeEvents) in
// Blueprint as well as C++.
UINTERFACE(BlueprintType, Blueprintable)
class UCitadelReplicated : public UInterface
{
    GENERATED_BODY()
};

/** Implement on any actor spawned by the networked-actor subsystem. */
class ICitadelReplicated
{
    GENERATED_BODY()

public:
    /** Called once when the server tells this client to spawn the actor. See
     *  FCitadelSpawnInfo::bIsLocalOwner for the local-owner vs remote-proxy split. */
    UFUNCTION(BlueprintNativeEvent, BlueprintCallable, Category = "Citadel|Replication")
    void OnCitadelSpawn(const FCitadelSpawnInfo& Info);

    /** Called when the server despawns the actor (its owner disconnected). The
     *  subsystem destroys the actor immediately after this returns. */
    UFUNCTION(BlueprintNativeEvent, BlueprintCallable, Category = "Citadel|Replication")
    void OnCitadelDespawn();
};
