// CitadelRoom.cpp — client-side rooms implementation (Phase A).

#include "CitadelRoom.h"

#include "CitadelTransformSync.h" // UCitadelTransformSyncSubsystem::SendFrame
#include "CitadelWire.h"
#include "Engine/GameInstance.h"

namespace
{
    // Big-endian writers (match the Rust wire: room.rs).
    void WriteU16BE(TArray<uint8>& Out, uint16 V)
    {
        Out.Add(static_cast<uint8>((V >> 8) & 0xFF));
        Out.Add(static_cast<uint8>(V & 0xFF));
    }

    void WriteU64BE(TArray<uint8>& Out, uint64 V)
    {
        for (int32 Shift = 56; Shift >= 0; Shift -= 8)
        {
            Out.Add(static_cast<uint8>((V >> Shift) & 0xFF));
        }
    }

    // u16 length-prefixed UTF-8 string (truncated at 65535 bytes).
    void WriteString(TArray<uint8>& Out, const FString& S)
    {
        FTCHARToUTF8 Utf8(*S);
        const int32 Len = FMath::Min(Utf8.Length(), 0xFFFF);
        WriteU16BE(Out, static_cast<uint16>(Len));
        Out.Append(reinterpret_cast<const uint8*>(Utf8.Get()), Len);
    }

    // Bounds-checked big-endian readers advancing `Off`; return false on underrun.
    bool ReadU16BE(const uint8* Body, int32 Len, int32& Off, uint16& Out)
    {
        if (Off + 2 > Len) { return false; }
        Out = static_cast<uint16>((static_cast<uint16>(Body[Off]) << 8) | Body[Off + 1]);
        Off += 2;
        return true;
    }

    bool ReadU64BE(const uint8* Body, int32 Len, int32& Off, uint64& Out)
    {
        if (Off + 8 > Len) { return false; }
        uint64 V = 0;
        for (int32 i = 0; i < 8; ++i) { V = (V << 8) | Body[Off + i]; }
        Off += 8;
        Out = V;
        return true;
    }

    bool ReadString(const uint8* Body, int32 Len, int32& Off, FString& Out)
    {
        uint16 SLen = 0;
        if (!ReadU16BE(Body, Len, Off, SLen)) { return false; }
        if (Off + SLen > Len) { return false; }
        FUTF8ToTCHAR Conv(reinterpret_cast<const ANSICHAR*>(Body + Off), SLen);
        Out = FString(Conv.Length(), Conv.Get());
        Off += SLen;
        return true;
    }
}

void UCitadelRoomSubsystem::JoinOrCreateRoom(const FString& RoomName)
{
    // ROOM_CREATE body = {u16 len, params}; params = the room's matchmaking NAME
    // (UTF-8). The server joins an existing room with this name or creates one.
    TArray<uint8> Body;
    WriteString(Body, RoomName);
    SendRoomFrame(CitadelWire::KIND_ROOM_CREATE, Body);
}

void UCitadelRoomSubsystem::JoinRoom(int64 RoomId)
{
    TArray<uint8> Body;
    WriteU64BE(Body, static_cast<uint64>(RoomId));
    SendRoomFrame(CitadelWire::KIND_ROOM_JOIN, Body);
}

void UCitadelRoomSubsystem::LeaveRoom(int64 RoomId)
{
    TArray<uint8> Body;
    WriteU64BE(Body, static_cast<uint64>(RoomId));
    SendRoomFrame(CitadelWire::KIND_ROOM_LEAVE, Body);
    if (CurrentRoomId == RoomId) { CurrentRoomId = 0; }
}

void UCitadelRoomSubsystem::SendMapReady(int64 RoomId)
{
    TArray<uint8> Body;
    WriteU64BE(Body, static_cast<uint64>(RoomId));
    SendRoomFrame(CitadelWire::KIND_ROOM_MAP_READY, Body);
}

void UCitadelRoomSubsystem::RouteRoomFrame(uint16 Kind, const uint8* Body, int32 Len)
{
    int32 Off = 0;
    if (Kind == CitadelWire::KIND_ROOM_JOINED)
    {
        FCitadelRoomInfo Info;
        uint64 RoomId = 0;
        if (!ReadU64BE(Body, Len, Off, RoomId)
            || !ReadString(Body, Len, Off, Info.Map)
            || !ReadString(Body, Len, Off, Info.Mode))
        {
            return; // malformed; ignore
        }
        Info.RoomId = static_cast<int64>(RoomId);
        CurrentRoomId = Info.RoomId;
        OnRoomJoined.Broadcast(Info);
    }
    else if (Kind == CitadelWire::KIND_ROOM_LEAVE)
    {
        uint64 RoomId = 0;
        if (!ReadU64BE(Body, Len, Off, RoomId)) { return; }
        OnRoomLeft.Broadcast(static_cast<int64>(RoomId));
    }
}

void UCitadelRoomSubsystem::SendRoomFrame(uint16 Kind, const TArray<uint8>& Body)
{
    if (UCitadelTransformSyncSubsystem* Sub =
            GetGameInstance()->GetSubsystem<UCitadelTransformSyncSubsystem>())
    {
        // Room frames are reliable; delivery order matters (join before map-ready).
        Sub->SendFrame(Kind, Body, /*bReliable=*/true);
    }
}
