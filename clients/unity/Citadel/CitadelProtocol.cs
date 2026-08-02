// Wire protocol constants and (de)serialization for the Citadel relay demo.
//
// This mirrors `citadel-wire::protocol` and `crates/demo-client/src/state.rs`:
//
//   KIND_POSITION       = 1  client -> server. Body: two little-endian f32
//                            (x, y): "my position".
//   KIND_PEER_POSITION  = 2  server -> client. Body: 8-byte BIG-endian sender
//                            session id, followed by the two-f32 position
//                            payload above. Rendered per sender id.
//
// Endianness matters: the position floats are LITTLE-endian, but the relayed
// sender id prefix is BIG-endian. BitConverter is host-endian (little-endian on
// x86_64), so we read/write the id bytes explicitly to stay correct regardless
// of host.
//
// RPC (request/response) mirrors `citadel-wire::protocol` (/0150):
//
//   KIND_RPC_REQUEST  = 3  client -> server. Body (all integers BIG-endian):
//                            request_id: u64 | method_len: u16 |
//                            method: utf8 (method_len bytes) | payload.
//   KIND_RPC_RESPONSE = 4  server -> client (unicast to the caller). Body:
//                            request_id: u64 (echoed) | status: u8
//                            (0 = ok, 1 = error) | payload (reply bytes on ok,
//                            or a short utf8 error message on error).

using System;
using System.Text;

namespace Citadel
{
    /// <summary>
    /// Envelope kinds and body encoding shared with the Citadel server gateway
    /// and the native demo client.
    /// </summary>
    public static class CitadelProtocol
    {
        /// <summary>Transform-sync negotiation (reliable, C↔S).</summary>
        public const ushort KindTsyncHello = 7;
        /// <summary>Transform snapshot (unreliable, S→C).</summary>
        public const ushort KindTsyncSnapshot = 8;
        /// <summary>Sequenced owner input (unreliable, C→S).</summary>
        public const ushort KindTsyncInput = 9;
        /// <summary>Snapshot acknowledgement (unreliable, C→S).</summary>
        public const ushort KindTsyncAck = 10;
        /// <summary>Ownership/role transition (reliable, S→C).</summary>
        public const ushort KindTsyncRole = 11;
        /// <summary>Authoritative rewind result (reliable, S→C).</summary>
        public const ushort KindTsyncRewind = 12;
        /// <summary>Negotiated v2 transform manifest (reliable, C↔S).</summary>
        public const ushort KindTsyncV2Hello = 29;
        /// <summary>Epoch-bearing v2 transform snapshot (unreliable, S→C).</summary>
        public const ushort KindTsyncV2Snapshot = 30;
        /// <summary>Epoch-fenced v2 transform input (unreliable, C→S).</summary>
        public const ushort KindTsyncV2Input = 31;

        /// <summary>Client-&gt;server: create or join a named room.</summary>
        public const ushort KindRoomCreate = 21;
        /// <summary>Client-&gt;server: join an existing room by id.</summary>
        public const ushort KindRoomJoin = 22;
        /// <summary>Server-&gt;client: room membership and its server-chosen map.</summary>
        public const ushort KindRoomJoined = 23;
        /// <summary>Either direction: leave or removal from a room.</summary>
        public const ushort KindRoomLeave = 24;
        /// <summary>Client-&gt;server: the room map has finished loading.</summary>
        public const ushort KindRoomMapReady = 25;

        /// <summary>Client-&gt;server: "my position" (body: two LE f32 x, y).</summary>
        public const ushort KindPosition = 1;

        /// <summary>
        /// Server-&gt;client: a relayed peer position (body: 8-byte BE sender id
        /// + the two-f32 position payload).
        /// </summary>
        public const ushort KindPeerPosition = 2;

        /// <summary>Client-&gt;server: invoke a server-side RPC (request/response).</summary>
        public const ushort KindRpcRequest = 3;

        /// <summary>Server-&gt;client: the correlated reply to a <see cref="KindRpcRequest"/>.</summary>
        public const ushort KindRpcResponse = 4;

        /// <summary>
        /// Client-&gt;server: the auth handshake. MUST be the first frame on a new
        /// connection. Body: the session token bytes, or empty for explicit guest.
        /// </summary>
        public const ushort KindAuth = 5;

        /// <summary>
        /// Server-&gt;client: the reply to a <see cref="KindAuth"/> handshake. Body:
        /// a status byte (<c>AuthStatus*</c>) plus, on the authenticated path, the
        /// resolved user_id (utf8).
        /// </summary>
        public const ushort KindAuthResult = 6;

        /// <summary>
        /// Server-&gt;client: ticket matchmaker handoff JSON. The client presents
        /// its opaque token through the generic <c>matchmaker.accept</c> RPC.
        /// </summary>
        public const ushort KindMatchmakerMatched = 26;

        /// <summary>
        /// Server-&gt;client: durable player-notification live delivery. The body is
        /// UTF-8 JSON for the persisted notification. Delivery is at-least-once;
        /// deduplicate by notification <c>id</c> and reconcile through the
        /// <c>notifications.list</c> RPC.
        /// </summary>
        public const ushort KindNotification = 27;
        /// <summary>Server-to-client authorized chat presence, ephemeral typing, or durable mutation event (UTF-8 JSON).</summary>
        public const ushort KindChatEvent = 28;

        /// <summary>
        /// Auth result status: the token validated; the connection is bound to the
        /// user_id that follows in the body.
        /// </summary>
        public const byte AuthStatusAuthenticated = 0;

        /// <summary>
        /// Auth result status: accepted as an anonymous guest (no account bound).
        /// Only possible when the server allows guests.
        /// </summary>
        public const byte AuthStatusGuest = 1;

        /// <summary>
        /// Auth result status: the handshake was refused; the body carries a coarse
        /// <c>AuthReason*</c> class and the connection closes immediately after.
        /// </summary>
        public const byte AuthStatusRejected = 2;

        /// <summary>
        /// Rejected reason class: authentication failed (bad/expired/revoked token).
        /// Intentionally coarse so it cannot aid account enumeration.
        /// </summary>
        public const byte AuthReasonAuthFailed = 0;

        /// <summary>
        /// Rejected reason class: a token was required but none was presented
        /// (guests disallowed on this connection).
        /// </summary>
        public const byte AuthReasonAuthRequired = 1;

        /// <summary>
        /// Rejected reason class: the handshake broke protocol (first frame was not
        /// <see cref="KindAuth"/>, a duplicate auth, an oversized token, or auth on
        /// an unreliable path).
        /// </summary>
        public const byte AuthReasonProtocol = 2;

        /// <summary>RPC response status: the handler ran and payload is its reply.</summary>
        public const byte RpcStatusOk = 0;

        /// <summary>RPC response status: the call failed; payload is a utf8 message.</summary>
        public const byte RpcStatusError = 1;

        /// <summary>Bytes used to prefix a relayed message with the sender id.</summary>
        public const int SenderIdBytes = 8;

        /// <summary>Bytes in a position payload: two little-endian f32.</summary>
        public const int PositionBytes = 8;

        /// <summary>Bytes of the big-endian request_id correlation prefix (RPC).</summary>
        public const int RpcRequestIdBytes = 8;

        /// <summary>Bytes of the big-endian method_len prefix in an RPC request.</summary>
        public const int RpcMethodLenBytes = 2;

        /// <summary>Minimum RPC response body: request_id (8) + status (1).</summary>
        public const int RpcResponseMinBytes = RpcRequestIdBytes + 1;

        /// <summary>Bytes in a room id carried by join, leave, and map-ready.</summary>
        public const int RoomIdBytes = 8;

        /// <summary>
        /// Encode a 2D position as a <see cref="KindPosition"/> body: two
        /// little-endian f32 (x, y).
        /// </summary>
        public static byte[] EncodePosition(float x, float y)
        {
            var buf = new byte[PositionBytes];
            WriteLeFloat(buf, 0, x);
            WriteLeFloat(buf, 4, y);
            return buf;
        }

        /// <summary>
        /// Decode a <see cref="KindPosition"/> payload (two little-endian f32).
        /// Returns false if the payload is not exactly 8 bytes.
        /// </summary>
        public static bool TryDecodePosition(byte[] payload, int offset, int length, out float x, out float y)
        {
            x = 0f;
            y = 0f;
            if (payload == null || length != PositionBytes || offset + length > payload.Length)
            {
                return false;
            }

            x = ReadLeFloat(payload, offset);
            y = ReadLeFloat(payload, offset + 4);
            return true;
        }

        /// <summary>
        /// Split a relayed <see cref="KindPeerPosition"/> body into its sender id
        /// and the two-f32 position it carries. Returns false if the body is
        /// malformed (too short, or the trailing payload is not 8 bytes).
        /// </summary>
        public static bool TryDecodePeerPosition(
            byte[] body,
            int length,
            out ulong senderId,
            out float x,
            out float y)
        {
            senderId = 0;
            x = 0f;
            y = 0f;

            if (body == null || length < SenderIdBytes + PositionBytes)
            {
                return false;
            }

            senderId = ReadBeUInt64(body, 0);
            return TryDecodePosition(body, SenderIdBytes, PositionBytes, out x, out y);
        }

        /// <summary>
        /// Encode a <see cref="KindRpcRequest"/> body:
        /// <c>request_id (u64 BE) | method_len (u16 BE) | method (utf8) | payload</c>.
        /// </summary>
        /// <exception cref="ArgumentNullException">If <paramref name="method"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// If the utf8-encoded method exceeds <see cref="ushort.MaxValue"/> bytes
        /// (RPC methods are short identifiers; an over-long name is a caller bug).
        /// </exception>
        public static byte[] EncodeRpcRequest(ulong requestId, string method, byte[] payload)
        {
            if (method == null)
            {
                throw new ArgumentNullException(nameof(method));
            }

            byte[] methodBytes = Encoding.UTF8.GetBytes(method);
            if (methodBytes.Length > ushort.MaxValue)
            {
                throw new ArgumentException(
                    $"RPC method is {methodBytes.Length} bytes; the wire method_len is a u16 " +
                    $"(max {ushort.MaxValue}).",
                    nameof(method));
            }

            byte[] body = payload ?? Array.Empty<byte>();
            var buf = new byte[RpcRequestIdBytes + RpcMethodLenBytes + methodBytes.Length + body.Length];

            WriteBeUInt64(buf, 0, requestId);
            WriteBeUInt16(buf, RpcRequestIdBytes, (ushort)methodBytes.Length);
            int offset = RpcRequestIdBytes + RpcMethodLenBytes;
            Array.Copy(methodBytes, 0, buf, offset, methodBytes.Length);
            offset += methodBytes.Length;
            Array.Copy(body, 0, buf, offset, body.Length);

            return buf;
        }

        /// <summary>
        /// Decode a <see cref="KindRpcResponse"/> body into its correlation id,
        /// status, and reply payload. Returns false if the body is too short to
        /// hold the <c>request_id (8) + status (1)</c> header. <paramref name="payload"/>
        /// is a fresh copy of exactly the reply bytes (may be empty).
        /// </summary>
        public static bool TryDecodeRpcResponse(
            byte[] body,
            int length,
            out ulong requestId,
            out byte status,
            out byte[] payload)
        {
            requestId = 0;
            status = 0;
            payload = Array.Empty<byte>();

            if (body == null || length < RpcResponseMinBytes || length > body.Length)
            {
                return false;
            }

            requestId = ReadBeUInt64(body, 0);
            status = body[RpcRequestIdBytes];

            int payloadLen = length - RpcResponseMinBytes;
            if (payloadLen > 0)
            {
                payload = new byte[payloadLen];
                Array.Copy(body, RpcResponseMinBytes, payload, 0, payloadLen);
            }

            return true;
        }

        /// <summary>Encode a named room create request: UTF-8 name prefixed by u16 BE.</summary>
        public static byte[] EncodeRoomCreate(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));
            byte[] value = Encoding.UTF8.GetBytes(name);
            if (value.Length > ushort.MaxValue)
                throw new ArgumentException("Room name exceeds the u16 wire limit.", nameof(name));
            var body = new byte[2 + value.Length];
            WriteBeUInt16(body, 0, (ushort)value.Length);
            Array.Copy(value, 0, body, 2, value.Length);
            return body;
        }

        /// <summary>Encode a room id for join, leave, or map-ready.</summary>
        public static byte[] EncodeRoomId(ulong roomId)
        {
            var body = new byte[RoomIdBytes];
            WriteBeUInt64(body, 0, roomId);
            return body;
        }

        /// <summary>Decode a ROOM_JOINED body into the server-selected room metadata.</summary>
        public static bool TryDecodeRoomJoined(byte[] body, int length, out RoomInfo room)
        {
            room = default(RoomInfo);
            if (body == null || length < RoomIdBytes + 4 || length > body.Length) return false;
            int offset = 0;
            ulong roomId = ReadBeUInt64(body, offset);
            offset += RoomIdBytes;
            if (!TryReadRoomString(body, length, ref offset, out string map) ||
                !TryReadRoomString(body, length, ref offset, out string mode) || offset != length) return false;
            room = new RoomInfo(roomId, map, mode);
            return true;
        }

        /// <summary>Decode a room id body, rejecting trailing bytes.</summary>
        public static bool TryDecodeRoomId(byte[] body, int length, out ulong roomId)
        {
            roomId = 0;
            if (body == null || length != RoomIdBytes || length > body.Length) return false;
            roomId = ReadBeUInt64(body, 0);
            return true;
        }

        private static bool TryReadRoomString(byte[] body, int length, ref int offset, out string value)
        {
            value = string.Empty;
            if (offset + 2 > length) return false;
            int stringLength = (body[offset] << 8) | body[offset + 1];
            offset += 2;
            if (offset + stringLength > length) return false;
            try { value = Encoding.UTF8.GetString(body, offset, stringLength); }
            catch (DecoderFallbackException) { return false; }
            offset += stringLength;
            return true;
        }

        private static void WriteBeUInt64(byte[] buf, int offset, ulong value)
        {
            for (int i = 0; i < 8; i++)
            {
                buf[offset + i] = (byte)(value >> (8 * (7 - i)));
            }
        }

        private static void WriteBeUInt16(byte[] buf, int offset, ushort value)
        {
            buf[offset] = (byte)(value >> 8);
            buf[offset + 1] = (byte)value;
        }

        private static void WriteLeFloat(byte[] buf, int offset, float value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            buf[offset] = bytes[0];
            buf[offset + 1] = bytes[1];
            buf[offset + 2] = bytes[2];
            buf[offset + 3] = bytes[3];
        }

        private static float ReadLeFloat(byte[] buf, int offset)
        {
            if (BitConverter.IsLittleEndian)
            {
                return BitConverter.ToSingle(buf, offset);
            }

            var tmp = new byte[4];
            tmp[0] = buf[offset];
            tmp[1] = buf[offset + 1];
            tmp[2] = buf[offset + 2];
            tmp[3] = buf[offset + 3];
            Array.Reverse(tmp);
            return BitConverter.ToSingle(tmp, 0);
        }

        private static ulong ReadBeUInt64(byte[] buf, int offset)
        {
            ulong value = 0;
            for (int i = 0; i < SenderIdBytes; i++)
            {
                value = (value << 8) | buf[offset + i];
            }

            return value;
        }
    }
}
