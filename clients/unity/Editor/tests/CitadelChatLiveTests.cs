#if UNITY_EDITOR
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using UnityEngine;

namespace Citadel.Tests
{
    public sealed class CitadelChatLiveTests
    {
        [Serializable] private sealed class Fixture { public FixtureCase[] valid; public InvalidCase[] invalid; }
        [Serializable] private sealed class FixtureCase { public string kind; public ChatEventWire event_data; }
        [Serializable] private sealed class InvalidCase { public string name; public string payload; public ChatEventWire event_data; }
        [Serializable] private sealed class FixtureRoot { public FixtureCaseRaw[] valid; public InvalidCaseRaw[] invalid; }
        [Serializable] private sealed class FixtureCaseRaw { public string name; public string kind; public ChatEventWire @event; }
        [Serializable] private sealed class InvalidCaseRaw { public string name; public string payload; public ChatEventWire @event; }
        private sealed class RpcCall { public string Method; public string Json; public Action<byte[]> Callback; }

        private static string FixturePath => Path.GetFullPath(Path.Combine(Application.dataPath, "../../tests/fixtures/chat-live-events-v1.json"));

        [Test]
        public void CanonicalFixtureDecodesEightClosedVariantsAndRejectsInvalidCases()
        {
            string json = File.ReadAllText(FixturePath);
            FixtureRoot fixture = JsonUtility.FromJson<FixtureRoot>(json);
            Assert.That(fixture.valid, Has.Length.EqualTo(8));
            foreach (FixtureCaseRaw item in fixture.valid)
            {
                string payload = ExtractEventObject(json, item.name);
                Assert.That(ChatLiveEventCodec.TryDecode(item.kind, Encoding.UTF8.GetBytes(payload), out ChatLiveEvent decoded), Is.True, item.name);
                Assert.That(decoded.Type, Is.EqualTo(item.kind));
            }
            foreach (InvalidCaseRaw item in fixture.invalid)
            {
                string payload = item.payload ?? ExtractEventObject(json, item.name);
                Assert.That(ChatLiveEventCodec.TryDecode(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(payload), out _), Is.False, item.name);
            }
        }

        [Test]
        public void DecoderRejectsDuplicateEscapedAndUnknownPropertyNamesAndSemanticDuplicates()
        {
            string[] invalid = {
                "{\"version\":1,\"type\":\"presence.leave\",\"type\":\"presence.leave\",\"channel_id\":\"c\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"}}",
                "{\"version\":1,\"type\":\"presence.leave\",\"channel_id\":\"c\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"},\"force-current\":true}",
                "{\"channel_id\":\"c\",\"watermark_event_id\":1,\"\\u0066orce-current\":true}",
                "{\"version\":1,\"type\":\"typing\",\"channel_id\":\"c\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"},\"typing\":false,\"expires_at\":1}",
                "{\"version\":1,\"type\":\"resync_required\",\"channel_id\":\"c\",\"watermark_event_id\":1,\"scopes\":[\"history\",\"history\"]}"
            };
            foreach (string json in invalid)
                Assert.That(ChatLiveEventCodec.TryDecode(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(json), out _), Is.False, json);
        }

        private static string ExtractEventObject(string fixture, string name)
        {
            int start = fixture.IndexOf("\"" + name + "\"", StringComparison.Ordinal);
            Assert.That(start, Is.GreaterThanOrEqualTo(0), name);
            start = fixture.IndexOf("\"event\":", start, StringComparison.Ordinal);
            Assert.That(start, Is.GreaterThanOrEqualTo(0), name);
            start = fixture.IndexOf('{', start);
            int depth = 0; bool quoted = false; bool escaped = false;
            for (int index = start; index < fixture.Length; index++)
            {
                char value = fixture[index];
                if (quoted) { if (escaped) escaped = false; else if (value == '\\') escaped = true; else if (value == '"') quoted = false; continue; }
                if (value == '"') { quoted = true; continue; }
                if (value == '{') depth++;
                else if (value == '}' && --depth == 0) return fixture.Substring(start, index - start + 1);
            }
            Assert.Fail("unterminated event object: " + name); return null;
        }

        [Test]
        public void DispatcherLifecycleDeduplicatesDetectsGapsExpiresTypingAndRevokesPrivateState()
        {
            var callbacks = new List<Action<byte[]>>();
            var chat = new CitadelChatLive((method, payload, callback) => { callbacks.Add(callback); return true; }, 4);
            var seen = new List<string>();
            chat.EventReceived += evt => seen.Add(evt.Type);
            chat.LiveEventPending += application => application.Apply(_ => true);
            Assert.That(chat.Join(ChatTarget.Direct("bob"), _ => { }), Is.Not.Null);
            callbacks[0](Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":4}"));

            Assert.That(chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"message.create\",\"channel_id\":\"ch_demo\",\"event_id\":5,\"message\":{\"id\":1,\"sender\":\"alice\",\"content\":\"hello\",\"created_at_unix_ms\":1000,\"updated_at_unix_ms\":1000,\"revision\":1,\"last_event_id\":5,\"deleted\":false}}")), Is.True);
            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"message.create\",\"channel_id\":\"ch_demo\",\"event_id\":5,\"message\":{\"id\":1,\"sender\":\"alice\",\"content\":\"hello\",\"created_at_unix_ms\":1000,\"updated_at_unix_ms\":1000,\"revision\":1,\"last_event_id\":5,\"deleted\":false}}"));
            Assert.That(seen, Has.Count.EqualTo(1));
            Assert.That(chat.NeedsResync("ch_demo"), Is.False);

            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"message.update\",\"channel_id\":\"ch_demo\",\"event_id\":7,\"message\":{\"id\":1,\"sender\":\"alice\",\"content\":\"later\",\"created_at_unix_ms\":1000,\"updated_at_unix_ms\":1100,\"revision\":2,\"last_event_id\":7,\"deleted\":false}}"));
            Assert.That(chat.NeedsResync("ch_demo"), Is.True);
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);

            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"typing\",\"channel_id\":\"ch_demo\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"},\"typing\":true,\"expires_at\":10}"));
            Assert.That(chat.ActiveTyping("ch_demo", 9), Has.Count.EqualTo(1));
            Assert.That(chat.ActiveTyping("ch_demo", 10), Is.Empty);

            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"access.revoked\",\"channel_id\":\"ch_demo\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"}}"));
            Assert.That(chat.JoinedChannels, Is.Empty);
        }

        [Test]
        public void DomainOperationsBuildTypedRpcRequestsAndReconnectRequiresReconcile()
        {
            var methods = new List<string>(); var callbacks = new List<Action<byte[]>>();
            var chat = new CitadelChatLive((method, payload, callback) => { methods.Add(method); callbacks.Add(callback); return true; }, 4);
            Assert.That(chat.Join(ChatTarget.Direct("bob"), _ => { }), Is.Not.Null);
            callbacks[0](Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":9}"));
            chat.OnDisconnected();
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            Assert.That(chat.RejoinTrackedChannels(_ => { }), Has.Count.EqualTo(1));
            Assert.That(chat.History(ChatHistoryRequest.Page("ch_demo", 50), _ => { }), Is.True);
            Assert.That(chat.Send("ch_demo", "hello", _ => { }), Is.True);
            Assert.That(chat.Edit("ch_demo", 1, "edited", _ => { }), Is.True);
            Assert.That(chat.Delete("ch_demo", 1, _ => { }), Is.True);
            Assert.That(chat.Moderate("ch_demo", 1, _ => { }), Is.True);
            Assert.That(chat.SetTyping("ch_demo", true, _ => { }), Is.True);
            Assert.That(chat.Leave("ch_demo", _ => { }), Is.True);
            Assert.That(methods, Is.EqualTo(new[] {
                "chat.join", "chat.join", "chat.history", "chat.send", "chat.edit",
                "chat.delete", "chat.moderate", "chat.typing", "chat.leave"
            }));
        }

        [Test]
        public void JoinCallbacksAreTypedCorrelatedAndCannotReviveRevokedOrStaleState()
        {
            var calls = new List<RpcCall>();
            var replies = new List<ChatJoinResponse>();
            var chat = new CitadelChatLive((method, payload, callback) => {
                calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true;
            }, 4);
            ChatJoinHandle first = chat.Join(ChatTarget.Direct("bob"), response => replies.Add(response));
            Assert.That(first, Is.Not.Null);
            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, Encoding.UTF8.GetBytes(
                "{\"version\":1,\"type\":\"access.revoked\",\"channel_id\":\"ch_revoked\",\"presence\":{\"presence_id\":\"p\",\"user_id\":\"u\"}}"));
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_revoked\",\"watermark_event_id\":4}"));
            Assert.That(chat.JoinedChannels, Is.Empty);
            Assert.That(replies, Is.Empty);

            Assert.That(chat.Join(ChatTarget.Direct("bob"), response => replies.Add(response)), Is.Not.Null);
            calls[1].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":4}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.True);
            Assert.That(replies[0].ChannelId, Is.EqualTo("ch_demo"));
            chat.OnDisconnected();
            chat.RejoinTrackedChannels(_ => { });
            Action<byte[]> stale = calls[2].Callback;
            chat.OnDisconnected();
            chat.RejoinTrackedChannels(_ => { });
            stale(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":4}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            calls[3].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_other\",\"watermark_event_id\":4}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            Assert.That(chat.JoinedChannels, Does.Not.Contain("ch_other"));
            chat.RejoinTrackedChannels(_ => { });
            calls[4].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":4}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.True);
            chat.OnDisconnected();
            chat.RejoinTrackedChannels(_ => { });
            calls[5].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":8}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            Assert.That(chat.NeedsResync("ch_demo"), Is.True);
            Assert.That(replies[replies.Count - 1].RequiresHistory, Is.True);

            calls.Clear(); replies.Clear();
            var malformed = new CitadelChatLive((method, payload, callback) => {
                calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true;
            }, 2);
            malformed.Join(ChatTarget.Direct("mallory"), response => replies.Add(response));
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_string\",\"watermark_event_id\":\"4\"}"));
            Assert.That(malformed.JoinedChannels, Is.Empty);
            Assert.That(replies, Is.Empty);
        }

        [Test]
        public void ReconciliationUsesStableNewestFirstPagesAndWaitsForAppliedPageAndAckReply()
        {
            var calls = new List<RpcCall>();
            var snapshots = new List<ChatHistorySnapshotApplication>();
            var chat = new CitadelChatLive((method, payload, callback) => {
                calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true;
            }, 4);
            chat.Join(ChatTarget.Direct("bob"), _ => { });
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":4}"));
            calls.Clear();
            chat.OnDisconnected();
            chat.RejoinTrackedChannels(_ => { });
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch_demo\",\"watermark_event_id\":9}"));
            calls.Clear();

            ChatReconciliationHandle request = chat.BeginReconciliation("ch_demo", 2, snapshot => snapshots.Add(snapshot));
            Assert.That(request, Is.Not.Null);
            Assert.That(calls[0].Json, Does.Not.Contain("acknowledge_watermark"));
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[{\"id\":9,\"sender\":\"a\",\"content\":\"x\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":9,\"deleted\":false},{\"id\":8,\"sender\":\"a\",\"content\":\"y\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":8,\"deleted\":false}],\"watermark_event_id\":9}"));
            Assert.That(snapshots, Is.Empty);
            Assert.That(calls, Has.Count.EqualTo(2));
            Assert.That(calls[1].Json, Does.Contain("\"before_message_id\":8"));
            calls[1].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            Assert.That(snapshots, Has.Count.EqualTo(1));
            Assert.That(snapshots[0].Replace, Is.True);
            Assert.That(snapshots[0].Messages, Has.Count.EqualTo(2));
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            Assert.That(snapshots[0].Apply(_ => true), Is.True);
            Assert.That(calls[2].Json, Does.Contain("\"acknowledge_watermark\":9"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.False);
            calls[2].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            Assert.That(chat.IsCurrent("ch_demo"), Is.True);
        }

        [Test]
        public void DisconnectConsumesLiveAuthorityUntilCorrelatedRejoin()
        {
            var calls = new List<RpcCall>(); var pending = new List<ChatLiveEventApplication>(); var applied = new List<string>();
            var chat = new CitadelChatLive((method, payload, callback) => { calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true; }, 2);
            chat.LiveEventPending += value => pending.Add(value); chat.EventReceived += value => applied.Add(value.Type);
            chat.Join(ChatTarget.Direct("bob"), _ => { }); calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"c\",\"watermark_event_id\":4}"));
            byte[] live = Encoding.UTF8.GetBytes("{\"version\":1,\"type\":\"message.create\",\"channel_id\":\"c\",\"event_id\":5,\"message\":{\"id\":1,\"sender\":\"a\",\"content\":\"x\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":5,\"deleted\":false}}");
            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, live); Assert.That(pending, Has.Count.EqualTo(1));
            chat.OnDisconnected(); Assert.That(pending[0].Apply(_ => true), Is.True); Assert.That(applied, Is.Empty);
            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, live); Assert.That(pending, Has.Count.EqualTo(1));
            chat.RejoinTrackedChannels(_ => { }); calls[calls.Count - 1].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"c\",\"watermark_event_id\":4}"));
            chat.HandleEnvelope(CitadelProtocol.KindChatEvent, live); Assert.That(pending, Has.Count.EqualTo(2));
        }

        [Test]
        public void DisconnectFencesEveryReconciliationBoundaryUntilCorrelatedRejoin()
        {
            var calls = new List<RpcCall>(); var snapshots = new List<ChatHistorySnapshotApplication>();
            var chat = new CitadelChatLive((method, payload, callback) => { calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true; }, 2);
            chat.Join(ChatTarget.Direct("bob"), _ => { }); calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"disconnect\",\"watermark_event_id\":4}")); calls.Clear();
            chat.OnDisconnected();
            Assert.That(chat.BeginReconciliation("disconnect", 2, value => snapshots.Add(value)), Is.Null, "disconnect must reject reconciliation until a fresh correlated rejoin");
            Assert.That(calls, Is.Empty, "rejected reconciliation must issue no history RPC");
            chat.RejoinTrackedChannels(_ => { }); calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"disconnect\",\"watermark_event_id\":9}")); calls.Clear();

            Assert.That(chat.BeginReconciliation("disconnect", 2, value => snapshots.Add(value)), Is.Not.Null);
            Action<byte[]> stalePage = calls[0].Callback; chat.OnDisconnected();
            stalePage(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            Assert.That(snapshots, Is.Empty, "disconnect at page boundary must expose no snapshot");
            Assert.That(calls, Has.Count.EqualTo(1), "disconnect at page boundary must issue no ACK");

            chat.RejoinTrackedChannels(_ => { }); calls[calls.Count - 1].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"disconnect\",\"watermark_event_id\":9}")); calls.Clear();
            chat.BeginReconciliation("disconnect", 2, value => snapshots.Add(value)); calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            Assert.That(snapshots, Has.Count.EqualTo(1)); chat.OnDisconnected(); snapshots[0].Apply(_ => true);
            Assert.That(calls, Has.Count.EqualTo(1), "disconnect at application boundary must issue no ACK");

            chat.RejoinTrackedChannels(_ => { }); calls[calls.Count - 1].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"disconnect\",\"watermark_event_id\":9}")); calls.Clear(); snapshots.Clear();
            chat.BeginReconciliation("disconnect", 2, value => snapshots.Add(value)); calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            snapshots[0].Apply(_ => { chat.OnDisconnected(); return true; });
            Assert.That(calls, Has.Count.EqualTo(1), "disconnect between local apply and ACK must issue no ACK");

            chat.RejoinTrackedChannels(_ => { }); calls[calls.Count - 1].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"disconnect\",\"watermark_event_id\":9}")); calls.Clear(); snapshots.Clear();
            chat.BeginReconciliation("disconnect", 2, value => snapshots.Add(value)); calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}")); snapshots[0].Apply(_ => true);
            Action<byte[]> staleAck = calls[1].Callback; chat.OnDisconnected(); staleAck(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}"));
            Assert.That(chat.IsCurrent("disconnect"), Is.False, "disconnect at ACK boundary must make late ACK inert");
        }

        [Test]
        public void RecoveryIsMutuallySingleflightEnforcesFloorAndDoesNotStrandSendFailures()
        {
            var calls = new List<RpcCall>(); var snapshots = new List<ChatHistorySnapshotApplication>(); bool failNext = false;
            var chat = new CitadelChatLive((method, payload, callback) => { if (failNext) { failNext = false; return false; } calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true; }, 2);
            chat.Join(ChatTarget.Direct("bob"), _ => { }); calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"c\",\"watermark_event_id\":4}")); calls.Clear();
            chat.OnDisconnected(); chat.RejoinTrackedChannels(_ => { });
            Assert.That(chat.BeginReconciliation("c", 2, _ => { }), Is.Null);
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"c\",\"watermark_event_id\":9}")); calls.Clear();
            failNext = true; Assert.That(chat.BeginReconciliation("c", 2, _ => { }), Is.Null);
            ChatReconciliationHandle recovery = chat.BeginReconciliation("c", 2, snapshot => snapshots.Add(snapshot)); Assert.That(recovery, Is.Not.Null);
            Assert.That(chat.RejoinTrackedChannels(_ => { }), Is.Empty);
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":8}"));
            Assert.That(snapshots, Is.Empty); Assert.That(calls[1].Json, Does.Not.Contain("before_message_id"));
            calls[1].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":9}")); Assert.That(snapshots, Has.Count.EqualTo(1));
            failNext = true; Assert.That(snapshots[0].Apply(_ => true), Is.True);
            Assert.That(chat.BeginReconciliation("c", 2, _ => { }), Is.Not.Null);
        }

        [Test]
        public void AsynchronousRpcErrorsCancelConsumedPageAndAckOperations()
        {
            var calls = new List<RpcCall>(); var snapshots = new List<ChatHistorySnapshotApplication>();
            var chat = new CitadelChatLive((method, payload, callback) => { calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true; }, 2);
            chat.Join(ChatTarget.Direct("bob"), _ => { }); calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"errors\",\"watermark_event_id\":4}")); calls.Clear();
            ChatReconciliationHandle failedPage = chat.BeginReconciliation("errors", 2, _ => { }); calls[0].Callback(Array.Empty<byte>());
            ChatReconciliationHandle afterPageError = chat.BeginReconciliation("errors", 2, value => snapshots.Add(value));
            Assert.That(afterPageError, Is.Not.Null); Assert.That(afterPageError, Is.Not.SameAs(failedPage));
            calls[calls.Count - 1].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":4}")); snapshots[0].Apply(_ => true);
            calls[calls.Count - 1].Callback(Array.Empty<byte>());
            ChatReconciliationHandle afterAckError = chat.BeginReconciliation("errors", 2, _ => { });
            Assert.That(afterAckError, Is.Not.Null); Assert.That(afterAckError, Is.Not.SameAs(afterPageError));
        }

        [Test]
        public void ReconciliationRestartsWhenSnapshotWatermarkMoves()
        {
            var calls = new List<RpcCall>(); var snapshots = new List<ChatHistorySnapshotApplication>();
            var chat = new CitadelChatLive((method, payload, callback) => { calls.Add(new RpcCall { Method = method, Json = Encoding.UTF8.GetString(payload), Callback = callback }); return true; }, 2);
            chat.Join(ChatTarget.Direct("bob"), _ => { });
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch\",\"watermark_event_id\":1}"));
            calls.Clear(); chat.OnDisconnected();
            chat.RejoinTrackedChannels(_ => { });
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"channel_id\":\"ch\",\"watermark_event_id\":9}"));
            calls.Clear();
            chat.BeginReconciliation("ch", 1, snapshot => snapshots.Add(snapshot));
            calls[0].Callback(Encoding.UTF8.GetBytes("{\"items\":[{\"id\":9,\"sender\":\"a\",\"content\":\"x\",\"created_at_unix_ms\":1,\"updated_at_unix_ms\":1,\"revision\":1,\"last_event_id\":9,\"deleted\":false}],\"watermark_event_id\":9}"));
            calls[1].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":10}"));
            Assert.That(calls[2].Json, Does.Not.Contain("before_message_id"));
            calls[2].Callback(Encoding.UTF8.GetBytes("{\"items\":[],\"watermark_event_id\":10}"));
            Assert.That(snapshots, Has.Count.EqualTo(1));
            Assert.That(snapshots[0].SnapshotRestarted, Is.True);
            Assert.That(snapshots[0].SnapshotWatermark, Is.EqualTo(10));
        }
    }
}
#endif
