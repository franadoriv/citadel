using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using UnityEngine;

namespace Citadel
{
    public delegate bool ChatRpcCall(string method, byte[] payload, Action<byte[]> onReply);

    [Serializable]
    public sealed class ChatPresence
    {
        public string presence_id;
        public string user_id;
        internal bool IsValid => !string.IsNullOrEmpty(presence_id) && !string.IsNullOrEmpty(user_id);
    }

    [Serializable]
    public sealed class ChatMessage
    {
        public long id;
        public string sender;
        public string content;
        public long created_at_unix_ms;
        public long updated_at_unix_ms;
        public long revision;
        public long last_event_id;
        public bool deleted;

        internal bool IsValid => id > 0 && !string.IsNullOrEmpty(sender) && content != null &&
            created_at_unix_ms >= 0 && updated_at_unix_ms >= created_at_unix_ms &&
            revision > 0 && last_event_id > 0;
    }

    /// <summary>JsonUtility wire DTO. Use ChatLiveEventCodec to obtain validated closed variants.</summary>
    [Serializable]
    public sealed class ChatEventWire
    {
        public int version;
        public string type;
        public string channel_id;
        public string channel_type;
        public long event_id;
        public ChatPresence presence;
        public ChatMessage message;
        public bool typing;
        public long expires_at;
        public long watermark_event_id;
        public string[] scopes;
    }

    public abstract class ChatLiveEvent
    {
        public const int ContractVersion = 1;
        public string Type { get; }
        public string ChannelId { get; }
        protected ChatLiveEvent(string type, string channelId) { Type = type; ChannelId = channelId; }
    }

    public sealed class ChatPresenceJoined : ChatLiveEvent
    {
        public string ChannelType { get; }
        public ChatPresence Presence { get; }
        internal ChatPresenceJoined(string channelId, string channelType, ChatPresence presence) : base("presence.join", channelId) { ChannelType = channelType; Presence = presence; }
    }
    public sealed class ChatPresenceLeft : ChatLiveEvent
    {
        public ChatPresence Presence { get; }
        internal ChatPresenceLeft(string channelId, ChatPresence presence) : base("presence.leave", channelId) { Presence = presence; }
    }
    public sealed class ChatTyping : ChatLiveEvent
    {
        public ChatPresence Presence { get; }
        public bool IsTyping { get; }
        public long ExpiresAtUnixMs { get; }
        internal ChatTyping(string channelId, ChatPresence presence, bool typing, long expiresAt) : base("typing", channelId) { Presence = presence; IsTyping = typing; ExpiresAtUnixMs = expiresAt; }
    }
    public abstract class ChatMessageEvent : ChatLiveEvent
    {
        public long EventId { get; }
        public ChatMessage Message { get; }
        protected ChatMessageEvent(string type, string channelId, long eventId, ChatMessage message) : base(type, channelId) { EventId = eventId; Message = message; }
    }
    public sealed class ChatMessageCreated : ChatMessageEvent { internal ChatMessageCreated(string channel, long id, ChatMessage message) : base("message.create", channel, id, message) { } }
    public sealed class ChatMessageUpdated : ChatMessageEvent { internal ChatMessageUpdated(string channel, long id, ChatMessage message) : base("message.update", channel, id, message) { } }
    public sealed class ChatMessageRemoved : ChatMessageEvent { internal ChatMessageRemoved(string channel, long id, ChatMessage message) : base("message.remove", channel, id, message) { } }
    public sealed class ChatAccessRevoked : ChatLiveEvent
    {
        public ChatPresence Presence { get; }
        internal ChatAccessRevoked(string channel, ChatPresence presence) : base("access.revoked", channel) { Presence = presence; }
    }
    public sealed class ChatResyncRequired : ChatLiveEvent
    {
        public long WatermarkEventId { get; }
        public IReadOnlyList<string> Scopes { get; }
        internal ChatResyncRequired(string channel, long watermark, string[] scopes) : base("resync_required", channel) { WatermarkEventId = watermark; Scopes = scopes ?? Array.Empty<string>(); }
    }

    public static class ChatLiveEventCodec
    {
        public static bool TryDecode(ushort kind, byte[] payload, out ChatLiveEvent result)
        {
            if (kind != CitadelProtocol.KindChatEvent) { result = null; return false; }
            return TryDecodePayload(payload, out result);
        }

        // The explicit expected type is used by fixture tests to prove kind/type parity.
        public static bool TryDecode(string expectedType, byte[] payload, out ChatLiveEvent result)
        {
            if (!TryDecodePayload(payload, out result)) return false;
            if (result.Type == expectedType) return true;
            result = null;
            return false;
        }

        private static bool TryDecodePayload(byte[] payload, out ChatLiveEvent result)
        {
            result = null;
            if (payload == null || payload.Length == 0) return false;
            ChatEventWire wire;
            try
            {
                string json = new UTF8Encoding(false, true).GetString(payload);
                if (!TryScanJson(json, out List<JsonProperty> properties)) return false;
                wire = JsonUtility.FromJson<ChatEventWire>(json);
                if (wire == null || !ValidateClosedJson(properties, wire.type)) return false;
            }
            catch { return false; }
            if (wire == null || wire.version != ChatLiveEvent.ContractVersion || string.IsNullOrEmpty(wire.channel_id) || string.IsNullOrEmpty(wire.type)) return false;
            switch (wire.type)
            {
                case "presence.join":
                    if (wire.presence == null || !wire.presence.IsValid || !IsChannelType(wire.channel_type)) return false;
                    result = new ChatPresenceJoined(wire.channel_id, wire.channel_type, wire.presence); return true;
                case "presence.leave":
                    if (wire.presence == null || !wire.presence.IsValid) return false;
                    result = new ChatPresenceLeft(wire.channel_id, wire.presence); return true;
                case "typing":
                    if (wire.presence == null || !wire.presence.IsValid || (wire.typing ? wire.expires_at <= 0 : wire.expires_at != 0)) return false;
                    result = new ChatTyping(wire.channel_id, wire.presence, wire.typing, wire.expires_at); return true;
                case "message.create": return TryMessage(wire, 1, false, true, out result);
                case "message.update": return TryMessage(wire, 2, false, false, out result);
                case "message.remove": return TryMessage(wire, 2, true, false, out result);
                case "access.revoked":
                    if (wire.presence == null || !wire.presence.IsValid) return false;
                    result = new ChatAccessRevoked(wire.channel_id, wire.presence); return true;
                case "resync_required":
                    if (wire.watermark_event_id <= 0 || !AreScopesValid(wire.scopes)) return false;
                    result = new ChatResyncRequired(wire.channel_id, wire.watermark_event_id, wire.scopes); return true;
                default: return false;
            }
        }

        private static bool TryMessage(ChatEventWire wire, long minimumRevision, bool mustBeDeleted, bool isCreate, out ChatLiveEvent result)
        {
            result = null;
            ChatMessage message = wire.message;
            if (wire.event_id <= 0 || message == null || !message.IsValid || message.last_event_id != wire.event_id || message.revision < minimumRevision) return false;
            if (isCreate && (message.revision != 1 || message.deleted || message.created_at_unix_ms != message.updated_at_unix_ms)) return false;
            if (!isCreate && message.revision <= 1) return false;
            if (mustBeDeleted != message.deleted) return false;
            if (mustBeDeleted && message.content.Length != 0) return false;
            if (!mustBeDeleted && !IsValidContent(message.content)) return false;
            if (wire.type == "message.create") result = new ChatMessageCreated(wire.channel_id, wire.event_id, message);
            else if (wire.type == "message.update") result = new ChatMessageUpdated(wire.channel_id, wire.event_id, message);
            else result = new ChatMessageRemoved(wire.channel_id, wire.event_id, message);
            return true;
        }

        internal static bool IsValidContent(string content)
        {
            if (string.IsNullOrEmpty(content) || Encoding.UTF8.GetByteCount(content) > 2048) return false;
            foreach (char value in content) if (char.IsControl(value) && value != '\n' && value != '\r') return false;
            return true;
        }
        private static bool IsChannelType(string value) => value == "direct" || value == "group" || value == "room";
        private static bool AreScopesValid(string[] scopes)
        {
            if (scopes == null) return true;
            var unique = new HashSet<string>();
            foreach (string scope in scopes) if ((scope != "history" && scope != "presence") || !unique.Add(scope)) return false;
            return true;
        }
        internal sealed class JsonProperty
        {
            internal string Name;
            internal bool ExactInteger;
            internal bool Boolean;
        }

        // Full bounded lexical parse before JsonUtility. This preserves escaped
        // property names, rejects duplicate keys in every object, and records
        // the exact token kind for closed-contract validation.
        internal static bool TryScanJson(string json, out List<JsonProperty> properties)
        {
            properties = new List<JsonProperty>();
            if (json == null || Encoding.UTF8.GetByteCount(json) > 262144) return false;
            var parser = new JsonLexicalParser(json, properties);
            return parser.Parse();
        }

        private static bool ValidateClosedJson(List<JsonProperty> properties, string type)
        {
            var expected = new List<string> { "version", "type", "channel_id" };
            var integers = new HashSet<string> { "version" };
            var booleans = new HashSet<string>();
            switch (type)
            {
                case "presence.join": expected.AddRange(new[] { "channel_type", "presence", "presence_id", "user_id" }); break;
                case "presence.leave": case "access.revoked": expected.AddRange(new[] { "presence", "presence_id", "user_id" }); break;
                case "typing": expected.AddRange(new[] { "presence", "presence_id", "user_id", "typing", "expires_at" }); integers.Add("expires_at"); booleans.Add("typing"); break;
                case "message.create": case "message.update": case "message.remove":
                    expected.AddRange(new[] { "event_id", "message", "id", "sender", "content", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id", "deleted" });
                    foreach (string name in new[] { "event_id", "id", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id" }) integers.Add(name);
                    booleans.Add("deleted"); break;
                case "resync_required": expected.AddRange(new[] { "watermark_event_id", "scopes" }); integers.Add("watermark_event_id"); break;
                default: return false;
            }
            if (properties.Count != expected.Count) return false;
            var counts = new Dictionary<string, int>();
            foreach (JsonProperty property in properties)
            {
                counts[property.Name] = counts.TryGetValue(property.Name, out int count) ? count + 1 : 1;
                if (integers.Contains(property.Name) && !property.ExactInteger) return false;
                if (booleans.Contains(property.Name) && !property.Boolean) return false;
            }
            foreach (string name in expected) if (!counts.TryGetValue(name, out int count) || count != 1) return false;
            return true;
        }

        private sealed class JsonLexicalParser
        {
            private readonly string _json;
            private readonly List<JsonProperty> _properties;
            private int _index;
            internal JsonLexicalParser(string json, List<JsonProperty> properties) { _json = json; _properties = properties; }
            internal bool Parse()
            {
                Skip();
                if (!Value(0, out _, out _)) return false;
                Skip();
                return _index == _json.Length;
            }
            private bool Value(int depth, out bool exactInteger, out bool boolean)
            {
                exactInteger = false; boolean = false;
                if (depth > 64) return false;
                Skip(); if (_index >= _json.Length) return false;
                char c = _json[_index];
                if (c == '{') return Object(depth + 1);
                if (c == '[') return Array(depth + 1);
                if (c == '"') return String(out _);
                if (c == 't') { boolean = true; return Literal("true"); }
                if (c == 'f') { boolean = true; return Literal("false"); }
                if (c == 'n') return Literal("null");
                return Number(out exactInteger);
            }
            private bool Object(int depth)
            {
                _index++; Skip(); var keys = new HashSet<string>();
                if (Take('}')) return true;
                while (true)
                {
                    if (!String(out string key) || !keys.Add(key)) return false;
                    Skip(); if (!Take(':')) return false;
                    if (!Value(depth, out bool integer, out bool boolean)) return false;
                    _properties.Add(new JsonProperty { Name = key, ExactInteger = integer, Boolean = boolean });
                    Skip(); if (Take('}')) return true; if (!Take(',')) return false; Skip();
                }
            }
            private bool Array(int depth)
            {
                _index++; Skip(); if (Take(']')) return true;
                while (true) { if (!Value(depth, out _, out _)) return false; Skip(); if (Take(']')) return true; if (!Take(',')) return false; Skip(); }
            }
            private bool String(out string value)
            {
                value = null; if (!Take('"')) return false; var result = new StringBuilder();
                while (_index < _json.Length)
                {
                    char c = _json[_index++];
                    if (c == '"') { value = result.ToString(); return true; }
                    if (c < 0x20) return false;
                    if (c != '\\') { result.Append(c); continue; }
                    if (_index >= _json.Length) return false; char escape = _json[_index++];
                    switch (escape)
                    {
                        case '"': case '\\': case '/': result.Append(escape); break;
                        case 'b': result.Append('\b'); break; case 'f': result.Append('\f'); break;
                        case 'n': result.Append('\n'); break; case 'r': result.Append('\r'); break; case 't': result.Append('\t'); break;
                        case 'u':
                            if (!Hex(out char unit)) return false;
                            if (char.IsHighSurrogate(unit)) { if (_index + 1 >= _json.Length || _json[_index++] != '\\' || _json[_index++] != 'u' || !Hex(out char low) || !char.IsLowSurrogate(low)) return false; result.Append(unit); result.Append(low); }
                            else { if (char.IsLowSurrogate(unit)) return false; result.Append(unit); }
                            break;
                        default: return false;
                    }
                }
                return false;
            }
            private bool Hex(out char value)
            {
                value = '\0'; if (_index + 4 > _json.Length) return false; int result = 0;
                for (int i = 0; i < 4; i++) { char c = _json[_index++]; int digit = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1; if (digit < 0) return false; result = result * 16 + digit; }
                value = (char)result; return true;
            }
            private bool Number(out bool exactInteger)
            {
                exactInteger = false; int start = _index; if (Take('-') && _index >= _json.Length) return false;
                if (Take('0')) { if (_index < _json.Length && char.IsDigit(_json[_index])) return false; }
                else { if (_index >= _json.Length || _json[_index] < '1' || _json[_index] > '9') return false; while (_index < _json.Length && char.IsDigit(_json[_index])) _index++; }
                bool fraction = false, exponent = false;
                if (Take('.')) { fraction = true; int digits = _index; while (_index < _json.Length && char.IsDigit(_json[_index])) _index++; if (_index == digits) return false; }
                if (_index < _json.Length && (_json[_index] == 'e' || _json[_index] == 'E')) { exponent = true; _index++; if (_index < _json.Length && (_json[_index] == '+' || _json[_index] == '-')) _index++; int digits = _index; while (_index < _json.Length && char.IsDigit(_json[_index])) _index++; if (_index == digits) return false; }
                exactInteger = _index > start && !fraction && !exponent; return true;
            }
            private bool Literal(string literal) { if (_index + literal.Length > _json.Length || string.CompareOrdinal(_json, _index, literal, 0, literal.Length) != 0) return false; _index += literal.Length; return true; }
            private void Skip() { while (_index < _json.Length && (_json[_index] == ' ' || _json[_index] == '\t' || _json[_index] == '\r' || _json[_index] == '\n')) _index++; }
            private bool Take(char c) { if (_index < _json.Length && _json[_index] == c) { _index++; return true; } return false; }
        }
    }

    [Serializable]
    public sealed class ChatTarget
    {
        public string kind;
        public string other_user_id;
        public ulong group_id;
        public long room_id;
        private ChatTarget() { }
        public static ChatTarget Direct(string userId) { Required(userId, "userId"); return new ChatTarget { kind = "direct", other_user_id = userId }; }
        public static ChatTarget Group(ulong groupId) { if (groupId == 0) throw new ArgumentOutOfRangeException(nameof(groupId)); return new ChatTarget { kind = "group", group_id = groupId }; }
        public static ChatTarget Room(long roomId) { if (roomId <= 0) throw new ArgumentOutOfRangeException(nameof(roomId)); return new ChatTarget { kind = "room", room_id = roomId }; }
        internal static ChatTarget CopyOf(ChatTarget value) => new ChatTarget { kind = value.kind, other_user_id = value.other_user_id, group_id = value.group_id, room_id = value.room_id };
        private static string Required(string value, string name) { if (string.IsNullOrEmpty(value)) throw new ArgumentException(name + " must not be empty", name); return value; }
    }

    /// <summary>Opaque identity for one correlated join request.</summary>
    public sealed class ChatJoinHandle
    {
        internal ulong Id { get; }
        internal ulong Generation { get; }
        internal ChatJoinHandle(ulong id, ulong generation) { Id = id; Generation = generation; }
    }

    public sealed class ChatJoinResponse
    {
        public string ChannelId { get; }
        public long WatermarkEventId { get; }
        public bool RequiresHistory { get; }
        public bool IsCurrent { get; }
        internal ChatJoinResponse(string channel, long watermark, bool requiresHistory, bool current)
        { ChannelId = channel; WatermarkEventId = watermark; RequiresHistory = requiresHistory; IsCurrent = current; }
    }

    [Serializable] internal sealed class JoinRequest { public ChatTarget target; }
    [Serializable] internal sealed class ChannelRequest { public string channel_id; }
    [Serializable] internal sealed class ContentRequest { public string channel_id; public string content; }
    [Serializable] internal sealed class MessageMutationRequest { public string channel_id; public long message_id; public string content; }
    [Serializable] internal sealed class MessageIdRequest { public string channel_id; public long message_id; }
    [Serializable] internal sealed class TypingRequest { public string channel_id; public bool typing; }
    [Serializable] internal sealed class JoinWireResponse { public string channel_id; public long watermark_event_id; }
    [Serializable] internal sealed class LeaveResponse { public bool left; }
    [Serializable] internal sealed class HistoryResponse { public ChatMessage[] items; public long watermark_event_id; }
    [Serializable]
    public sealed class ChatHistoryRequest
    {
        public string channel_id;
        public int limit;
        public long before_message_id;
        public static ChatHistoryRequest Page(string channelId, int limit, long beforeMessageId = 0) => Create(channelId, limit, beforeMessageId);
        private static ChatHistoryRequest Create(string channelId, int limit, long before)
        {
            if (string.IsNullOrEmpty(channelId)) throw new ArgumentException("channelId must not be empty", nameof(channelId));
            if (limit <= 0 || before < 0) throw new ArgumentOutOfRangeException();
            return new ChatHistoryRequest { channel_id = channelId, limit = limit, before_message_id = before };
        }

        internal object WireValue()
        {
            if (before_message_id > 0) return new HistoryBefore { channel_id = channel_id, limit = limit, before_message_id = before_message_id };
            return new HistoryPage { channel_id = channel_id, limit = limit };
        }
        [Serializable] private sealed class HistoryPage { public string channel_id; public int limit; }
        [Serializable] private sealed class HistoryBefore { public string channel_id; public int limit; public long before_message_id; }
    }

    public sealed class ChatHistoryPage
    {
        public string ChannelId { get; }
        public IReadOnlyList<ChatMessage> Items { get; }
        public long SnapshotWatermark { get; }
        internal ChatHistoryPage(string channelId, ChatMessage[] items, long watermark)
        { ChannelId = channelId; Items = items; SnapshotWatermark = watermark; }
    }

    /// <summary>Opaque exactly-once local application for one durable live event.</summary>
    public sealed class ChatLiveEventApplication
    {
        private Action<ChatLiveEventApplication, bool> _completion;
        public ChatMessageEvent Event { get; }
        internal ChatLiveEventApplication(ChatMessageEvent value, Action<ChatLiveEventApplication, bool> completion) { Event = value; _completion = completion; }
        public bool Apply(Func<ChatMessageEvent, bool> applier)
        {
            Action<ChatLiveEventApplication, bool> completion = _completion;
            if (completion == null || applier == null) return false;
            _completion = null;
            bool applied;
            try { applied = applier(Event); } catch { applied = false; }
            completion(this, applied);
            return applied;
        }
    }

    public sealed class ChatReconciliationHandle
    {
        internal ulong Id { get; }
        internal ChatReconciliationHandle(ulong id) { Id = id; }
    }

    /// <summary>One complete transactional history replacement, never a page.</summary>
    public sealed class ChatHistorySnapshotApplication
    {
        private Action<ChatHistorySnapshotApplication, bool> _completion;
        public string ChannelId { get; }
        public IReadOnlyList<ChatMessage> Messages { get; }
        public long SnapshotWatermark { get; }
        public bool Replace => true;
        public ulong Generation { get; }
        public bool SnapshotRestarted { get; }
        internal ChatHistorySnapshotApplication(string channel, ChatMessage[] messages, long watermark, ulong generation, bool restarted, Action<ChatHistorySnapshotApplication, bool> completion)
        { ChannelId = channel; Messages = messages; SnapshotWatermark = watermark; Generation = generation; SnapshotRestarted = restarted; _completion = completion; }
        public bool Apply(Func<ChatHistorySnapshotApplication, bool> applier)
        {
            Action<ChatHistorySnapshotApplication, bool> completion = _completion;
            if (completion == null || applier == null) return false;
            _completion = null;
            bool applied;
            try { applied = applier(this); } catch { applied = false; }
            completion(this, applied);
            return applied;
        }
    }

    /// <summary>Poll-loop-neutral typed chat dispatcher, lifecycle state, and RPC builders.</summary>
    public sealed class CitadelChatLive
    {
        private sealed class ChannelState { public long Cursor; public long RequiredWatermark; public bool Current; public bool Admitted; public ulong Epoch; public ChatTarget Target; public ChatLiveEventApplication PendingLive; }
        private sealed class Reconciliation { public ChatReconciliationHandle Handle; public string Channel; public int Limit; public long Floor; public long Snapshot = -1; public long Before; public ulong AdmissionEpoch; public ulong Generation = 1; public ulong PageSerial; public bool AwaitingPage; public bool AwaitingApply; public bool AwaitingAck; public bool Restarted; public readonly List<ChatMessage> Staged = new List<ChatMessage>(); public Action<ChatHistorySnapshotApplication> Callback; }
        private sealed class PendingJoin { public ChatJoinHandle Handle; public ulong Generation; public ChatTarget Target; public string ExpectedChannel; public long ExpectedWatermark; public Action<ChatJoinResponse> Callback; }
        private readonly ChatRpcCall _rpc;
        private readonly int _maxTrackedChannels;
        private readonly Dictionary<string, ChannelState> _channels = new Dictionary<string, ChannelState>();
        private readonly Dictionary<string, Dictionary<string, long>> _typing = new Dictionary<string, Dictionary<string, long>>();
        private readonly Dictionary<string, Reconciliation> _reconciliations = new Dictionary<string, Reconciliation>();
        private readonly Dictionary<ulong, PendingJoin> _pendingJoins = new Dictionary<ulong, PendingJoin>();
        private readonly HashSet<string> _revoked = new HashSet<string>();
        private ulong _nextReconciliationId = 1;
        private ulong _nextJoinId = 1;
        private ulong _joinGeneration = 1;

        public event Action<ushort, byte[]> RawEnvelopeReceived;
        public event Action<ChatLiveEvent> EventReceived;
        public event Action<ChatLiveEventApplication> LiveEventPending;
        public event Action<string, long> ResyncNeeded;
        public IReadOnlyCollection<string> JoinedChannels => _channels.Keys;

        public CitadelChatLive(ChatRpcCall rpc, int maxTrackedChannels)
        {
            _rpc = rpc ?? throw new ArgumentNullException(nameof(rpc));
            if (maxTrackedChannels <= 0) throw new ArgumentOutOfRangeException(nameof(maxTrackedChannels));
            _maxTrackedChannels = maxTrackedChannels;
        }

        public bool HandleEnvelope(ushort kind, byte[] payload)
        {
            if (kind != CitadelProtocol.KindChatEvent) return false;
            byte[] raw = payload == null ? Array.Empty<byte>() : (byte[])payload.Clone();
            RawEnvelopeReceived?.Invoke(kind, raw);
            if (!ChatLiveEventCodec.TryDecode(kind, raw, out ChatLiveEvent evt)) return true;
            if (evt is ChatAccessRevoked)
            {
                InvalidatePendingJoins(); _revoked.Add(evt.ChannelId); ClearChannel(evt.ChannelId); EventReceived?.Invoke(evt); return true;
            }
            if (_revoked.Contains(evt.ChannelId)) return true;
            if (evt is ChatResyncRequired requested)
            {
                if (!_channels.TryGetValue(evt.ChannelId, out ChannelState state)) return true;
                state.Current = false; state.RequiredWatermark = Math.Max(state.RequiredWatermark, requested.WatermarkEventId);
                ResyncNeeded?.Invoke(evt.ChannelId, requested.WatermarkEventId); EventReceived?.Invoke(evt); return true;
            }
            if (evt is ChatMessageEvent durable)
            {
                if (!_channels.TryGetValue(evt.ChannelId, out ChannelState state) || !state.Admitted || durable.EventId <= state.Cursor || state.PendingLive != null) return true;
                if (state.Cursor > 0 && durable.EventId != state.Cursor + 1)
                {
                    state.Current = false; state.RequiredWatermark = Math.Max(state.RequiredWatermark, durable.EventId);
                    ResyncNeeded?.Invoke(evt.ChannelId, durable.EventId); return true;
                }
                ulong epoch = state.Epoch;
                ChatLiveEventApplication application = null;
                application = new ChatLiveEventApplication(durable, (candidate, applied) =>
                {
                    if (!_channels.TryGetValue(evt.ChannelId, out ChannelState active) || !active.Admitted || active.Epoch != epoch || !ReferenceEquals(active.PendingLive, candidate)) return;
                    active.PendingLive = null;
                    if (!applied || _revoked.Contains(evt.ChannelId)) return;
                    active.Cursor = durable.EventId;
                    if (active.RequiredWatermark <= active.Cursor) active.RequiredWatermark = 0;
                    EventReceived?.Invoke(evt);
                });
                state.PendingLive = application;
                LiveEventPending?.Invoke(application);
                return true;
            }
            if (evt is ChatTyping typing && _channels.ContainsKey(evt.ChannelId)) ApplyTyping(typing);
            EventReceived?.Invoke(evt);
            return true;
        }

        public void OnDisconnected()
        {
            InvalidatePendingJoins();
            foreach (ChannelState state in _channels.Values) { state.Current = false; state.RequiredWatermark = Math.Max(state.RequiredWatermark, state.Cursor); state.PendingLive = null; state.Admitted = false; state.Epoch++; }
            _typing.Clear(); _reconciliations.Clear();
        }
        public bool NeedsResync(string channelId) => _channels.TryGetValue(channelId, out ChannelState state) && state.RequiredWatermark > 0;
        public bool IsCurrent(string channelId) => _channels.TryGetValue(channelId, out ChannelState state) && state.Current && state.RequiredWatermark == 0;

        public IReadOnlyCollection<ChatPresence> ActiveTyping(string channelId, long nowUnixMs)
        {
            var active = new List<ChatPresence>();
            if (!_typing.TryGetValue(channelId, out Dictionary<string, long> entries)) return active;
            var expired = new List<string>();
            foreach (KeyValuePair<string, long> item in entries) { if (item.Value <= nowUnixMs) expired.Add(item.Key); else { string[] ids = item.Key.Split('\n'); active.Add(new ChatPresence { presence_id = ids[0], user_id = ids[1] }); } }
            foreach (string key in expired) entries.Remove(key);
            return active;
        }

        public ChatJoinHandle Join(ChatTarget target, Action<ChatJoinResponse> reply)
        {
            if (target == null) throw new ArgumentNullException(nameof(target));
            return BeginJoin(ChatTarget.CopyOf(target), null, 0, reply);
        }
        public bool Leave(string channelId, Action<byte[]> reply)
        {
            string channel = Required(channelId);
            return Call("chat.leave", new ChannelRequest { channel_id = channel }, bytes =>
            {
                LeaveResponse response = TryResponse<LeaveResponse>(bytes);
                if (response != null && response.left) ClearChannel(channel);
                reply?.Invoke(bytes);
            });
        }
        public bool Send(string channelId, string content, Action<byte[]> reply)
        {
            if (!ChatLiveEventCodec.IsValidContent(content)) throw new ArgumentException("content violates the chat text contract", nameof(content));
            return Call("chat.send", new ContentRequest { channel_id = Required(channelId), content = content }, reply);
        }
        public bool History(ChatHistoryRequest request, Action<ChatHistoryPage> reply)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            return Call("chat.history", request.WireValue(), bytes => { if (TryHistory(bytes, request.limit, out HistoryResponse response)) reply?.Invoke(new ChatHistoryPage(request.channel_id, response.items, response.watermark_event_id)); });
        }
        public ChatReconciliationHandle BeginReconciliation(string channelId, int limit, Action<ChatHistorySnapshotApplication> onSnapshot)
        {
            string channel = Required(channelId); if (limit <= 0 || limit > 100) throw new ArgumentOutOfRangeException(nameof(limit));
            if (!_channels.TryGetValue(channel, out ChannelState channelState) || _revoked.Contains(channel)) throw new InvalidOperationException("channel is not tracked or is revoked");
            if (!channelState.Admitted) return null;
            foreach (PendingJoin pending in _pendingJoins.Values) if (pending.ExpectedChannel == channel) return null;
            foreach (Reconciliation active in _reconciliations.Values) if (active.Channel == channel) return active.Handle;
            var request = new Reconciliation { Handle = new ChatReconciliationHandle(_nextReconciliationId++), Channel = channel, Limit = limit, Floor = Math.Max(channelState.Cursor, channelState.RequiredWatermark), AdmissionEpoch = channelState.Epoch, Callback = onSnapshot };
            _reconciliations[request.Handle.Id.ToString("x")] = request;
            if (!RequestReconciliationPage(request)) { _reconciliations.Remove(request.Handle.Id.ToString("x")); return null; }
            return request.Handle;
        }
        public bool Edit(string channelId, long messageId, string content, Action<byte[]> reply) => Mutate("chat.edit", channelId, messageId, content, reply);
        public bool Delete(string channelId, long messageId, Action<byte[]> reply) => MessageIdCall("chat.delete", channelId, messageId, reply);
        public bool Moderate(string channelId, long messageId, Action<byte[]> reply) => MessageIdCall("chat.moderate", channelId, messageId, reply);
        public bool SetTyping(string channelId, bool typing, Action<byte[]> reply) => Call("chat.typing", new TypingRequest { channel_id = Required(channelId), typing = typing }, reply);
        public IReadOnlyList<ChatJoinHandle> RejoinTrackedChannels(Action<ChatJoinResponse> reply)
        {
            var handles = new List<ChatJoinHandle>();
            foreach (KeyValuePair<string, ChannelState> item in _channels)
            {
                ChannelState state = item.Value;
                if (state.Target == null) continue;
                bool reconciling = false;
                foreach (Reconciliation active in _reconciliations.Values) if (active.Channel == item.Key) { reconciling = true; break; }
                if (reconciling) continue;
                PendingJoin existing = null;
                foreach (PendingJoin pending in _pendingJoins.Values) if (pending.ExpectedChannel == item.Key) { existing = pending; break; }
                if (existing != null) { handles.Add(existing.Handle); continue; }
                ChatJoinHandle handle = BeginJoin(ChatTarget.CopyOf(state.Target), item.Key, Math.Max(state.Cursor, state.RequiredWatermark), reply);
                if (handle != null) handles.Add(handle);
            }
            return handles;
        }

        private ChatJoinHandle BeginJoin(ChatTarget target, string expectedChannel, long expectedWatermark, Action<ChatJoinResponse> reply)
        {
            var handle = new ChatJoinHandle(_nextJoinId++, _joinGeneration);
            var request = new PendingJoin { Handle = handle, Generation = _joinGeneration, Target = target, ExpectedChannel = expectedChannel, ExpectedWatermark = expectedWatermark, Callback = reply };
            _pendingJoins[handle.Id] = request;
            bool sent = Call("chat.join", new JoinRequest { target = target }, bytes => CompleteJoin(request, bytes));
            if (!sent) { _pendingJoins.Remove(handle.Id); return null; }
            return handle;
        }

        private void CompleteJoin(PendingJoin request, byte[] bytes)
        {
            if (request.Generation != _joinGeneration || !_pendingJoins.TryGetValue(request.Handle.Id, out PendingJoin active) || !ReferenceEquals(active, request)) return;
            _pendingJoins.Remove(request.Handle.Id);
            JoinWireResponse response = TryJoinResponse(bytes);
            if (response == null || string.IsNullOrEmpty(response.channel_id) || response.watermark_event_id < 0) return;
            bool requiresHistory = false;
            bool current = false;
            if (request.ExpectedChannel == null)
            {
                if (_channels.ContainsKey(response.channel_id)) return;
                _revoked.Remove(response.channel_id);
                ChannelState state = GetOrCreate(response.channel_id); if (state == null) return;
                state.Cursor = response.watermark_event_id;
                state.RequiredWatermark = 0;
                state.Current = true;
                state.Admitted = true;
                state.Epoch++;
                state.Target = request.Target;
                current = true;
            }
            else
            {
                if (response.channel_id != request.ExpectedChannel || _revoked.Contains(response.channel_id) || !_channels.TryGetValue(request.ExpectedChannel, out ChannelState state)) return;
                state.Admitted = true;
                state.Epoch++;
                if (response.watermark_event_id == request.ExpectedWatermark && state.Cursor == request.ExpectedWatermark && state.RequiredWatermark <= state.Cursor)
                {
                    state.RequiredWatermark = 0;
                    state.Current = true;
                    current = true;
                }
                else
                {
                    state.Current = false;
                    state.RequiredWatermark = Math.Max(state.RequiredWatermark, Math.Max(state.Cursor, response.watermark_event_id));
                    requiresHistory = true;
                    ResyncNeeded?.Invoke(request.ExpectedChannel, response.watermark_event_id);
                }
            }
            request.Callback?.Invoke(new ChatJoinResponse(response.channel_id, response.watermark_event_id, requiresHistory, current));
        }

        private void CancelPendingRejoin(string channel)
        {
            foreach (ulong id in new List<ulong>(_pendingJoins.Keys)) if (_pendingJoins[id].ExpectedChannel == channel) _pendingJoins.Remove(id);
        }
        private void InvalidatePendingJoins() { _joinGeneration++; _pendingJoins.Clear(); }

        private static JoinWireResponse TryJoinResponse(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0) return null;
            try
            {
                string json = new UTF8Encoding(false, true).GetString(bytes);
                if (!ChatLiveEventCodec.TryScanJson(json, out List<ChatLiveEventCodec.JsonProperty> properties) || properties.Count != 2) return null;
                bool channel = false, watermark = false;
                foreach (ChatLiveEventCodec.JsonProperty property in properties)
                {
                    if (property.Name == "channel_id") channel = true;
                    else if (property.Name == "watermark_event_id" && property.ExactInteger) watermark = true;
                    else return null;
                }
                if (!channel || !watermark) return null;
                return JsonUtility.FromJson<JoinWireResponse>(json);
            }
            catch { return null; }
        }

        private bool Mutate(string method, string channel, long message, string content, Action<byte[]> reply)
        {
            if (message <= 0) throw new ArgumentOutOfRangeException(nameof(message));
            if (!ChatLiveEventCodec.IsValidContent(content)) throw new ArgumentException("content violates the chat text contract", nameof(content));
            return Call(method, new MessageMutationRequest { channel_id = Required(channel), message_id = message, content = content }, reply);
        }
        private bool MessageIdCall(string method, string channel, long message, Action<byte[]> reply)
        {
            if (message <= 0) throw new ArgumentOutOfRangeException(nameof(message));
            return Call(method, new MessageIdRequest { channel_id = Required(channel), message_id = message }, reply);
        }
        private bool Call(string method, object request, Action<byte[]> reply) => _rpc(method, Encoding.UTF8.GetBytes(JsonUtility.ToJson(request)), reply);
        private bool RequestReconciliationPage(Reconciliation request)
        {
            if (!IsActiveAndAdmitted(request) || request.AwaitingPage || request.AwaitingApply || request.AwaitingAck) { CancelReconciliation(request); return false; }
            object wire = request.Before > 0 ? ChatHistoryRequest.Page(request.Channel, request.Limit, request.Before).WireValue() : ChatHistoryRequest.Page(request.Channel, request.Limit).WireValue();
            ulong serial = ++request.PageSerial;
            request.AwaitingPage = true;
            bool sent = Call("chat.history", wire, bytes =>
            {
                if (!IsActiveAndAdmitted(request) || !request.AwaitingPage || request.PageSerial != serial) return;
                request.AwaitingPage = false;
                if (bytes == null || bytes.Length == 0) { CancelReconciliation(request); return; }
                if (!TryHistory(bytes, request.Limit, out HistoryResponse response)) { ResetAndRestart(request); return; }
                if (request.Snapshot >= 0 && response.watermark_event_id != request.Snapshot) { ResetAndRestart(request); return; }
                if (!IsNewestFirst(response.items, request.Before)) { ResetAndRestart(request); return; }
                request.Snapshot = response.watermark_event_id;
                request.Staged.AddRange(response.items);
                if (response.items.Length == request.Limit)
                {
                    request.Before = response.items[response.items.Length - 1].id;
                    RequestReconciliationPage(request);
                    return;
                }
                if (request.Snapshot < request.Floor) { ResetAndRestart(request); return; }
                request.AwaitingApply = true;
                ulong generation = request.Generation;
                ChatHistorySnapshotApplication application = null;
                application = new ChatHistorySnapshotApplication(request.Channel, request.Staged.ToArray(), request.Snapshot, generation, request.Restarted, (candidate, applied) =>
                {
                    if (!IsActiveAndAdmitted(request) || request.Generation != generation || !request.AwaitingApply) return;
                    request.AwaitingApply = false;
                    if (!applied) { ResetAndRestart(request); return; }
                    RequestReconciliationAck(request);
                });
                try
                {
                    if (request.Callback == null) application.Apply(_ => false);
                    else request.Callback(application);
                }
                catch { application.Apply(_ => false); }
            });
            if (!sent) { request.AwaitingPage = false; _reconciliations.Remove(request.Handle.Id.ToString("x")); }
            return sent;
        }
        private bool RequestReconciliationAck(Reconciliation request)
        {
            if (!IsActiveAndAdmitted(request) || request.AwaitingAck) { CancelReconciliation(request); return false; }
            if (request.Snapshot < request.Floor) { ResetAndRestart(request); return false; }
            request.AwaitingAck = true;
            ulong generation = request.Generation;
            var ack = new HistoryAckRequest { channel_id = request.Channel, limit = 1, acknowledge_watermark = request.Snapshot };
            bool sent = Call("chat.history", ack, bytes =>
            {
                if (!IsActiveAndAdmitted(request) || request.Generation != generation || !request.AwaitingAck) return;
                request.AwaitingAck = false;
                if (bytes == null || bytes.Length == 0) { CancelReconciliation(request); return; }
                if (!TryHistory(bytes, 1, out HistoryResponse response) || response.watermark_event_id != request.Snapshot || response.watermark_event_id < request.Floor) { ResetAndRestart(request); return; }
                if (_channels.TryGetValue(request.Channel, out ChannelState state) && state.Admitted && !_revoked.Contains(request.Channel) && state.RequiredWatermark <= request.Snapshot)
                { state.Cursor = Math.Max(state.Cursor, request.Snapshot); state.RequiredWatermark = 0; state.Current = true; }
                _reconciliations.Remove(request.Handle.Id.ToString("x"));
            });
            if (!sent) { request.AwaitingAck = false; _reconciliations.Remove(request.Handle.Id.ToString("x")); }
            return sent;
        }
        private void ResetAndRestart(Reconciliation request)
        {
            if (!IsActiveAndAdmitted(request)) { CancelReconciliation(request); return; }
            request.Generation++; request.PageSerial++; request.Snapshot = -1; request.Before = 0;
            request.AwaitingPage = false; request.AwaitingApply = false; request.AwaitingAck = false; request.Restarted = true; request.Staged.Clear();
            RequestReconciliationPage(request);
        }
        [Serializable] private sealed class HistoryAckRequest { public string channel_id; public int limit; public long acknowledge_watermark; }
        private static bool TryHistory(byte[] bytes, int requestedLimit, out HistoryResponse response)
        {
            response = null;
            if (bytes == null || bytes.Length == 0) return false;
            string json;
            List<ChatLiveEventCodec.JsonProperty> properties;
            try
            {
                json = new UTF8Encoding(false, true).GetString(bytes);
                if (!ChatLiveEventCodec.TryScanJson(json, out properties)) return false;
                response = JsonUtility.FromJson<HistoryResponse>(json);
            }
            catch { return false; }
            if (response == null || response.watermark_event_id < 0) return false;
            response.items = response.items ?? Array.Empty<ChatMessage>();
            if (response.items.Length > requestedLimit || !ValidateHistoryJson(properties, response.items.Length)) return false;
            foreach (ChatMessage item in response.items) if (!IsValidHistoryMessage(item, response.watermark_event_id)) return false;
            return true;
        }
        private static bool ValidateHistoryJson(List<ChatLiveEventCodec.JsonProperty> properties, int itemCount)
        {
            var expected = new Dictionary<string, int> { ["items"] = 1, ["watermark_event_id"] = 1 };
            foreach (string name in new[] { "id", "sender", "content", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id", "deleted" }) expected[name] = itemCount;
            if (properties.Count != 2 + itemCount * 8) return false;
            var actual = new Dictionary<string, int>();
            var integers = new HashSet<string>(new[] { "watermark_event_id", "id", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id" });
            foreach (ChatLiveEventCodec.JsonProperty property in properties)
            {
                actual[property.Name] = actual.TryGetValue(property.Name, out int count) ? count + 1 : 1;
                if (integers.Contains(property.Name) && !property.ExactInteger) return false;
                if (property.Name == "deleted" && !property.Boolean) return false;
            }
            foreach (KeyValuePair<string, int> pair in expected) if (!actual.TryGetValue(pair.Key, out int count) || count != pair.Value) return false;
            return true;
        }
        private static bool IsValidHistoryMessage(ChatMessage item, long watermark)
        {
            if (item == null || !item.IsValid || item.last_event_id > watermark) return false;
            if (item.revision == 1) return !item.deleted && item.created_at_unix_ms == item.updated_at_unix_ms && ChatLiveEventCodec.IsValidContent(item.content);
            return item.deleted ? item.content.Length == 0 : ChatLiveEventCodec.IsValidContent(item.content);
        }
        private static bool IsNewestFirst(ChatMessage[] items, long before)
        {
            long previous = before > 0 ? before : long.MaxValue;
            foreach (ChatMessage item in items) { if (item.id >= previous) return false; previous = item.id; }
            return true;
        }
        private bool IsActive(Reconciliation request) => _reconciliations.TryGetValue(request.Handle.Id.ToString("x"), out Reconciliation active) && ReferenceEquals(active, request);
        private bool IsActiveAndAdmitted(Reconciliation request) => IsActive(request) && !_revoked.Contains(request.Channel) && _channels.TryGetValue(request.Channel, out ChannelState state) && state.Admitted && state.Epoch == request.AdmissionEpoch;
        private void CancelReconciliation(Reconciliation request) { if (IsActive(request)) _reconciliations.Remove(request.Handle.Id.ToString("x")); }
        private static T TryResponse<T>(byte[] bytes) where T : class
        {
            if (bytes == null || bytes.Length == 0) return null;
            try { return JsonUtility.FromJson<T>(new UTF8Encoding(false, true).GetString(bytes)); }
            catch { return null; }
        }
        private ChannelState GetOrCreate(string channel)
        {
            if (_channels.TryGetValue(channel, out ChannelState state)) return state;
            if (_channels.Count >= _maxTrackedChannels) return null;
            state = new ChannelState(); _channels[channel] = state; return state;
        }
        private void ApplyTyping(ChatTyping value)
        {
            if (!_typing.TryGetValue(value.ChannelId, out Dictionary<string, long> entries)) { entries = new Dictionary<string, long>(); _typing[value.ChannelId] = entries; }
            string key = value.Presence.presence_id + "\n" + value.Presence.user_id;
            if (value.IsTyping) entries[key] = value.ExpiresAtUnixMs; else entries.Remove(key);
        }
        private void ClearChannel(string channel) { _channels.Remove(channel); _typing.Remove(channel); foreach (string id in new List<string>(_reconciliations.Keys)) if (_reconciliations[id].Channel == channel) _reconciliations.Remove(id); }
        private static string Required(string value) { if (string.IsNullOrEmpty(value)) throw new ArgumentException("value must not be empty"); return value; }
    }
}
