// CitadelRoom.h — client-side rooms (match/lobby membership + map load), Phase A.
//
// UCitadelRoomSubsystem is the client half of Citadel rooms. Create or join a room;
// when the server confirms (KIND_ROOM_JOINED) the subsystem fires OnRoomJoined with
// the room's map name so your game logic can open that level, then acknowledge with
// SendMapReady. Room frames are routed here by the transform-sync inbound pump (the
// single reader of the client's envelope queue).
//
// Typical flow (from your GameMode/PlayerController once connected + authenticated):
//   auto* Rooms = GetGameInstance->GetSubsystem<UCitadelRoomSubsystem>;
//   Rooms->OnRoomJoined.AddDynamic(this, &AMyGM::HandleRoomJoined);
//   Rooms->JoinOrCreateRoom(TEXT("lobby"));   // everyone asking "lobby" shares one room
// ...and in HandleRoomJoined: OpenLevel(Room.Map); then Rooms->SendMapReady(Room.RoomId).
#pragma once

#include "CoreMinimal.h"
#include "Subsystems/GameInstanceSubsystem.h"

#include "CitadelRoom.generated.h"

/** Info delivered when this client joins a room (KIND_ROOM_JOINED). */
USTRUCT(BlueprintType)
struct FCitadelRoomInfo
{
    GENERATED_BODY()

    /** The room this client is now in. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Rooms")
    int64 RoomId = 0;

    /** The map/level name the client should have open. Open it (e.g. `Open Level`)
     *  then call SendMapReady(RoomId). */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Rooms")
    FString Map;

    /** The room's game mode (game-defined; may be empty). */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Rooms")
    FString Mode;
};

DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelRoomJoined, const FCitadelRoomInfo&, Room);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelRoomLeft, int64, RoomId);

/**
 * Client-side rooms. Create/join a room and react to the join via OnRoomJoined,
 * which carries the map name to load. Frames arrive through the transform-sync pump.
 */
UCLASS()
class UCitadelRoomSubsystem : public UGameInstanceSubsystem
{
    GENERATED_BODY()

public:
    /** Join the room named `RoomName`, creating it if it does not exist yet. Everyone
     *  who asks for the same name lands in the SAME room (the first caller creates it,
     *  the rest join) — this is the matchmaking entry point. The room's MAP is decided
     *  by the server's `on_room_create` hook, not by this name. OnRoomJoined fires with
     *  the map to load (for the creator and every joiner). */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Rooms")
    void JoinOrCreateRoom(const FString& RoomName);

    /** Ask to join an existing room by id (subject to the server's `on_room_join`). */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Rooms")
    void JoinRoom(int64 RoomId);

    /** Leave the given room. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Rooms")
    void LeaveRoom(int64 RoomId);

    /** Tell the server this client now has the room's map/level open. Call from your
     *  OnRoomJoined handler after the level finished loading. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Rooms")
    void SendMapReady(int64 RoomId);

    /** The room this client is currently in (0 = none). */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Rooms")
    int64 CurrentRoom() const { return CurrentRoomId; }

    /** Fired on KIND_ROOM_JOINED — carries the map to load. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Rooms")
    FCitadelRoomJoined OnRoomJoined;

    /** Fired on KIND_ROOM_LEAVE — a member (or you) left the room. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Rooms")
    FCitadelRoomLeft OnRoomLeft;

    /** Route a decoded room frame here (called by the transform-sync inbound pump). */
    void RouteRoomFrame(uint16 Kind, const uint8* Body, int32 Len);

private:
    void SendRoomFrame(uint16 Kind, const TArray<uint8>& Body);

    int64 CurrentRoomId = 0;
};
