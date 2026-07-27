using System;

namespace Citadel
{
    /// <summary>Server-authoritative room metadata delivered by <c>ROOM_JOINED</c>.</summary>
    public readonly struct RoomInfo
    {
        public ulong RoomId { get; }
        public string Map { get; }
        public string Mode { get; }

        public RoomInfo(ulong roomId, string map, string mode)
        {
            RoomId = roomId;
            Map = map ?? string.Empty;
            Mode = mode ?? string.Empty;
        }
    }

    /// <summary>
    /// Room operations over a connected <see cref="CitadelClient"/>. Feed every
    /// polled envelope to <see cref="HandleEnvelope"/> from the application's one
    /// poll loop; this object then raises room lifecycle events on that same thread.
    /// </summary>
    public sealed class CitadelRooms
    {
        private readonly CitadelClient _client;
        /// <summary>The current room, or null before joining/after leaving.</summary>
        public RoomInfo? CurrentRoom { get; private set; }
        public event Action<RoomInfo> Joined;
        public event Action<ulong> Left;

        public CitadelRooms(CitadelClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <summary>Create or join the named room. The server chooses its map.</summary>
        public CitadelStatus JoinOrCreate(string name) =>
            _client.Send(CitadelProtocol.KindRoomCreate, CitadelProtocol.EncodeRoomCreate(name), true);

        /// <summary>Request admission to a room by id.</summary>
        public CitadelStatus Join(ulong roomId) =>
            _client.Send(CitadelProtocol.KindRoomJoin, CitadelProtocol.EncodeRoomId(roomId), true);

        /// <summary>Leave a room.</summary>
        public CitadelStatus Leave(ulong roomId) =>
            _client.Send(CitadelProtocol.KindRoomLeave, CitadelProtocol.EncodeRoomId(roomId), true);

        /// <summary>Acknowledge that the server-selected map is now open.</summary>
        public CitadelStatus SendMapReady(ulong roomId) =>
            _client.Send(CitadelProtocol.KindRoomMapReady, CitadelProtocol.EncodeRoomId(roomId), true);

        /// <summary>Consume one inbound envelope. Returns true for room frames, including malformed ones.</summary>
        public bool HandleEnvelope(ushort kind, byte[] body, int length)
        {
            if (kind == CitadelProtocol.KindRoomJoined)
            {
                if (CitadelProtocol.TryDecodeRoomJoined(body, length, out RoomInfo room))
                {
                    CurrentRoom = room;
                    Joined?.Invoke(room);
                }
                return true;
            }
            if (kind == CitadelProtocol.KindRoomLeave)
            {
                if (CitadelProtocol.TryDecodeRoomId(body, length, out ulong roomId))
                {
                    if (CurrentRoom.HasValue && CurrentRoom.Value.RoomId == roomId) CurrentRoom = null;
                    Left?.Invoke(roomId);
                }
                return true;
            }
            return false;
        }
    }
}
