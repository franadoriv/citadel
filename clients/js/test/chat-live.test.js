import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { parse, stringify } from "lossless-json";

import {
  ChatEventCursor,
  CitadelClient,
  Envelope,
  KIND_CHAT_EVENT,
  decodeChatEvent,
} from "../src/index.js";

const fixture = JSON.parse(await readFile(
  new URL("../../../tests/fixtures/chat-live-events-v1.json", import.meta.url),
  "utf8",
));
const encoder = new TextEncoder();
const U64_MAX = 18_446_744_073_709_551_615n;

test("chat package subpath publishes a TypeScript declaration entry", async () => {
  const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(packageJson.exports["./chat"], {
    types: "./chat.d.ts",
    import: "./src/chat.js",
  });
  assert.match(await readFile(new URL("../chat.d.ts", import.meta.url), "utf8"), /ChatEventCursor/);
});

test("browser release stages the chat subpath declaration", async () => {
  const buildScript = await readFile(new URL("../scripts/build-release.mjs", import.meta.url), "utf8");
  assert.match(buildScript, /resolve\(sdkDir, "chat\.d\.ts"\)/);
  assert.match(buildScript, /"chat\.d\.ts"/);
});

function parseNumber(source) {
  if (/^-?\d+$/.test(source)) {
    const integer = BigInt(source);
    return integer >= Number.MIN_SAFE_INTEGER && integer <= Number.MAX_SAFE_INTEGER
      ? Number(integer) : integer;
  }
  return Number(source);
}

function parseJson(text) {
  return parse(text, null, { parseNumber });
}

function bytes(value) {
  return encoder.encode(typeof value === "string" ? value : JSON.stringify(value));
}

test("decodeChatEvent accepts all eight canonical v1 variants", () => {
  assert.equal(fixture.version, 1);
  assert.equal(fixture.valid.length, 8);
  for (const entry of fixture.valid) {
    const event = decodeChatEvent(bytes(entry.event));
    assert.equal(event?.type, entry.kind, entry.name);
    assert.equal(event?.channel_id, "ch_demo", entry.name);
  }
});

test("decodeChatEvent rejects every canonical invalid payload", () => {
  for (const entry of fixture.invalid) {
    const payload = entry.payload ?? entry.event;
    assert.equal(decodeChatEvent(bytes(payload)), null, entry.name);
  }
});

test("decodeChatEvent rejects invalid u64 identifiers", () => {
  const base = structuredClone(fixture.valid.find(({ name }) => name === "message_create").event);
  for (const mutate of [
    (event) => { event.event_id = -1; event.message.last_event_id = event.event_id; },
    (event) => { event.message.id = -1; },
    (event) => { event.message.revision = 1.5; },
    (event) => { event.message.created_at_unix_ms = -1; },
  ]) {
    const event = structuredClone(base);
    mutate(event);
    assert.equal(decodeChatEvent(bytes(event)), null);
  }
  const tooLarge = U64_MAX + 1n;
  const payload = `{"version":1,"type":"message.create","channel_id":"ch_big","event_id":${tooLarge},"message":{"id":1,"sender":"alice","content":"invalid","created_at_unix_ms":1,"updated_at_unix_ms":1,"revision":1,"last_event_id":${tooLarge},"deleted":false}}`;
  assert.equal(decodeChatEvent(payload), null);
});

test("decodeChatEvent and cursor preserve the full u64 range with bigint", () => {
  const max = U64_MAX;
  const previous = max - 1n;
  const payload = `{"version":1,"type":"message.create","channel_id":"ch_big","event_id":${max},"message":{"id":${max},"sender":"alice","content":"full u64","created_at_unix_ms":1000,"updated_at_unix_ms":1000,"revision":1,"last_event_id":${max},"deleted":false}}`;
  const event = decodeChatEvent(new TextEncoder().encode(payload));
  assert.equal(event.event_id, max);
  assert.equal(event.message.id, max);
  assert.equal(event.message.last_event_id, max);

  const cursor = new ChatEventCursor("ch_big", previous);
  assert.deepEqual(cursor.observe(event), { type: "apply", event_id: max });
  assert.equal(cursor.watermark, max);
});

test("decodeChatEvent rejects incomplete presence.join metadata", () => {
  const joined = structuredClone(fixture.valid.find(({ name }) => name === "presence_join").event);
  delete joined.channel_type;
  assert.equal(decodeChatEvent(bytes(joined)), null);
  joined.channel_type = "future";
  assert.equal(decodeChatEvent(bytes(joined)), null);
});

test("onChatEvent emits only typed events while on(kind) preserves raw delivery", () => {
  const client = new CitadelClient(new FakeWebSocket());
  const typed = [];
  const raw = [];
  client.onChatEvent((event) => typed.push(event));
  client.on(KIND_CHAT_EVENT, (payload) => raw.push(new TextDecoder().decode(payload)));

  client._dispatch(new Envelope(KIND_CHAT_EVENT, bytes(fixture.valid[0].event)));
  client._dispatch(new Envelope(KIND_CHAT_EVENT, bytes(fixture.invalid[0].payload)));

  assert.equal(typed.length, 1);
  assert.equal(typed[0].type, "presence.join");
  assert.equal(raw.length, 2);
});

test("ChatEventCursor is channel-bound and classifies duplicate, gap, and resync", () => {
  const events = Object.fromEntries(fixture.valid.map((entry) => [entry.name, decodeChatEvent(bytes(entry.event))]));
  const cursor = new ChatEventCursor("ch_demo", 4);

  assert.deepEqual(cursor.observe(events.message_create), { type: "apply", event_id: 5 });
  assert.deepEqual(cursor.observe(events.message_create), { type: "duplicate", event_id: 5 });
  assert.deepEqual(cursor.observe(events.message_remove), {
    type: "reconcile_gap", current_watermark: 5, observed_event_id: 7,
  });
  assert.equal(cursor.watermark, 5, "a gap cannot advance the durable watermark");
  assert.equal(cursor.state, "reconcile_required");
  assert.deepEqual(cursor.observe(events.resync_required), {
    type: "resync_required", watermark_event_id: 9,
  });
  assert.equal(cursor.watermark, 5, "resync cannot advance before history acknowledgement");
  assert.throws(() => cursor.observe({ ...events.message_create, channel_id: "other" }), /channel/);
});

test("ChatEventCursor requires rejoin and exposes no manual reconciliation or ACK bypass", () => {
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.disconnected();
  assert.equal(cursor.state, "rejoin_required");

  cursor.rejoined({ channel_id: "ch_demo", watermark_event_id: 9 });
  assert.equal(cursor.state, "reconcile_required");
  assert.equal(cursor.watermark, 5);
  assert.equal(cursor.reconciliationComplete, undefined);
  assert.equal(cursor.acknowledgeReconciliation, undefined);
  assert.deepEqual(
    Object.getOwnPropertySymbols(Object.getPrototypeOf(cursor)),
    [],
    "privileged reconciliation methods must not be discoverable on the public prototype",
  );
});

test("ChatEventCursor expires typing and access.revoked terminates channel state", () => {
  const typing = decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "typing").event));
  const revoked = decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "access_revoked").event));
  const cursor = new ChatEventCursor("ch_demo", 0);

  assert.deepEqual(cursor.observe(typing, typing.expires_at - 1), {
    type: "typing", presence: typing.presence, typing: true, expires_at: typing.expires_at,
  });
  assert.deepEqual(cursor.expireTyping(typing.expires_at), [{ ...typing.presence, typing: false }]);
  assert.deepEqual(cursor.observe(revoked), { type: "access_revoked", presence: revoked.presence });
  assert.equal(cursor.state, "revoked");
  assert.throws(() => cursor.observe(typing), /revoked/);
});

function decodeRpcFrame(frame) {
  const envelopeBody = frame.subarray(6);
  const view = new DataView(envelopeBody.buffer, envelopeBody.byteOffset, envelopeBody.byteLength);
  const requestId = view.getBigUint64(0, false);
  const methodLength = view.getUint16(8, false);
  return {
    requestId,
    method: new TextDecoder().decode(envelopeBody.subarray(10, 10 + methodLength)),
    payload: parseJson(new TextDecoder().decode(envelopeBody.subarray(10 + methodLength))),
    payloadText: new TextDecoder().decode(envelopeBody.subarray(10 + methodLength)),
  };
}

function replyJson(client, requestId, value) {
  const payload = encoder.encode(stringify(value));
  const body = new Uint8Array(9 + payload.length);
  new DataView(body.buffer).setBigUint64(0, requestId, false);
  body[8] = 0;
  body.set(payload, 9);
  client._dispatch(new Envelope(4, body));
}

async function expectJsonRpc(client, ws, promise, method, payload, response) {
  const request = decodeRpcFrame(ws.sent.at(-1));
  assert.equal(request.method, method);
  assert.deepEqual(request.payload, payload);
  replyJson(client, request.requestId, response);
  assert.deepEqual(await promise, response);
}

async function waitForSent(ws, count) {
  while (ws.sent.length < count) await new Promise((resolve) => setImmediate(resolve));
  return decodeRpcFrame(ws.sent[count - 1]);
}

function historyMessage(id, lastEventId = 5) {
  const message = structuredClone(fixture.valid.find(({ name }) => name === "message_create").event.message);
  message.id = id;
  message.last_event_id = lastEventId;
  return message;
}

test("chat domain methods encode canonical RPC payloads without caller JSON", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const message = fixture.valid.find(({ name }) => name === "message_create").event.message;
  const editedMessage = fixture.valid.find(({ name }) => name === "message_update").event.message;
  const presence = fixture.valid.find(({ name }) => name === "presence_join").event.presence;

  await expectJsonRpc(client, ws,
    client.joinChat({ kind: "direct", other_user_id: "bob" }),
    "chat.join", { target: { kind: "direct", other_user_id: "bob" } },
    { channel_id: "ch_demo", channel_type: "direct", presence: [presence], watermark_event_id: 4, subscription: "s1" });
  await expectJsonRpc(client, ws, client.leaveChat("ch_demo"),
    "chat.leave", { channel_id: "ch_demo" }, { left: true });
  await expectJsonRpc(client, ws, client.sendChatMessage("ch_demo", "hello"),
    "chat.send", { channel_id: "ch_demo", content: "hello" }, { message, event_id: 5 });
  await expectJsonRpc(client, ws, client.getChatHistory("ch_demo", {
    limit: 50, beforeMessageId: 123,
  }), "chat.history", {
    channel_id: "ch_demo", limit: 50, before_message_id: 123,
  }, { items: [message], watermark_event_id: 9 });
  await expectJsonRpc(client, ws, client.editChatMessage("ch_demo", 1, "edited"),
    "chat.edit", { channel_id: "ch_demo", message_id: 1, content: "edited" }, { message: editedMessage, event_id: 6 });
  await expectJsonRpc(client, ws, client.deleteChatMessage("ch_demo", 1),
    "chat.delete", { channel_id: "ch_demo", message_id: 1 }, { message_id: 1, deleted: true, event_id: 7 });
  await expectJsonRpc(client, ws, client.moderateChatMessage("ch_demo", 1),
    "chat.moderate", { channel_id: "ch_demo", message_id: 1 }, { message_id: 1, deleted: true, event_id: 7 });
  await expectJsonRpc(client, ws, client.setChatTyping("ch_demo", true),
    "chat.typing", { channel_id: "ch_demo", typing: true }, { typing: true, expires_at: 1234 });
});

test("chat RPCs preserve u64::MAX as exact numeric JSON in requests and responses", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const presence = fixture.valid.find(({ name }) => name === "presence_join").event.presence;
  const created = structuredClone(fixture.valid.find(({ name }) => name === "message_create").event.message);
  created.id = U64_MAX;
  created.last_event_id = U64_MAX;
  const edited = structuredClone(fixture.valid.find(({ name }) => name === "message_update").event.message);
  edited.id = U64_MAX;
  edited.last_event_id = U64_MAX;
  const historyItem = { ...created, id: U64_MAX - 1n };

  await expectJsonRpc(client, ws, client.joinChat({ kind: "group", group_id: U64_MAX }),
    "chat.join", { target: { kind: "group", group_id: U64_MAX } },
    { channel_id: "ch_big", channel_type: "group", presence: [presence], watermark_event_id: U64_MAX, subscription: "s-max" });
  assert.match(decodeRpcFrame(ws.sent.at(-1)).payloadText, /"group_id":18446744073709551615/);

  await expectJsonRpc(client, ws, client.sendChatMessage("ch_big", "max"),
    "chat.send", { channel_id: "ch_big", content: "max" }, { message: created, event_id: U64_MAX });
  await expectJsonRpc(client, ws, client.getChatHistory("ch_big", { beforeMessageId: U64_MAX }),
    "chat.history", { channel_id: "ch_big", before_message_id: U64_MAX },
    { items: [historyItem], watermark_event_id: U64_MAX });
  assert.match(decodeRpcFrame(ws.sent.at(-1)).payloadText, /"before_message_id":18446744073709551615/);

  await expectJsonRpc(client, ws, client.editChatMessage("ch_big", U64_MAX, "edited"),
    "chat.edit", { channel_id: "ch_big", message_id: U64_MAX, content: "edited" },
    { message: edited, event_id: U64_MAX });
  await expectJsonRpc(client, ws, client.deleteChatMessage("ch_big", U64_MAX),
    "chat.delete", { channel_id: "ch_big", message_id: U64_MAX },
    { message_id: U64_MAX, deleted: true, event_id: U64_MAX });
  await expectJsonRpc(client, ws, client.moderateChatMessage("ch_big", U64_MAX),
    "chat.moderate", { channel_id: "ch_big", message_id: U64_MAX },
    { message_id: U64_MAX, deleted: true, event_id: U64_MAX });
  assert.match(decodeRpcFrame(ws.sent.at(-1)).payloadText, /"message_id":18446744073709551615/);

  await expectJsonRpc(client, ws, client.editChatMessage("ch_big", 7n, "safe bigint"),
    "chat.edit", { channel_id: "ch_big", message_id: 7, content: "safe bigint" },
    { message: { ...edited, id: 7, last_event_id: 8 }, event_id: 8 });
  await expectJsonRpc(client, ws, client.deleteChatMessage("ch_big", 7n),
    "chat.delete", { channel_id: "ch_big", message_id: 7 },
    { message_id: 7, deleted: true, event_id: 8 });
});

test("chat RPCs reject negative, fractional, and overflowing u64 inputs before network I/O", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  for (const invalid of [-1, 1.5, U64_MAX + 1n]) {
    await assert.rejects(client.deleteChatMessage("ch_big", invalid), /64-bit|integer/);
    await assert.rejects(client.joinChat({ kind: "group", group_id: invalid }), /chat target/);
  }
  assert.equal(ws.sent.length, 0);
});

test("ordinary history cannot emit a reconciliation acknowledgement", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);

  await assert.rejects(
    client.getChatHistory("ch_demo", { acknowledgeWatermark: 9 }),
    /acknowledgeWatermark.*not supported/i,
  );
  assert.equal(ws.sent.length, 0);
});

test("reconciliation retry budget is bounded before network I/O", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));

  await assert.rejects(
    client.reconcileChat(cursor, () => {}, { maxAttempts: 11, timeoutMs: 1 }),
    /maxAttempts.*no greater than 10/i,
  );
  assert.equal(ws.sent.length, 0);
});

test("chat RPC results reject overflowing identifiers before they reach cursor state", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const message = structuredClone(fixture.valid.find(({ name }) => name === "message_create").event.message);
  const overflowing = U64_MAX + 1n;
  message.id = overflowing;
  message.last_event_id = overflowing;

  const pending = client.sendChatMessage("ch_demo", "hello");
  const rejected = assert.rejects(pending, /malformed chat\.send response/);
  const request = decodeRpcFrame(ws.sent.at(-1));
  replyJson(client, request.requestId, { message, event_id: overflowing });
  await rejected;
  await assert.rejects(client.deleteChatMessage("ch_demo", overflowing), /64-bit/);
});

test("reconcile paginates newest-first, applies a terminal snapshot, then privately ACKs", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));
  const applied = [];

  const reconcile = client.reconcileChat(cursor, ({ messages }) => applied.push(...messages), { limit: 2 });
  const first = await waitForSent(ws, 1);
  assert.deepEqual(first.payload, { channel_id: "ch_demo", limit: 2 });
  replyJson(client, first.requestId, { items: [historyMessage(30), historyMessage(20)], watermark_event_id: 9 });

  const second = await waitForSent(ws, 2);
  assert.deepEqual(second.payload, { channel_id: "ch_demo", limit: 2, before_message_id: 20 });
  replyJson(client, second.requestId, { items: [historyMessage(10)], watermark_event_id: 9 });

  const ack = await waitForSent(ws, 3);
  assert.deepEqual(applied.map(({ id }) => id), [30, 20, 10]);
  assert.equal(cursor.state, "ack_required");
  assert.equal(cursor.watermark, 5, "applied history is not current before the correlated ACK response");
  assert.deepEqual(ack.payload, { channel_id: "ch_demo", limit: 1, acknowledge_watermark: 9 });
  replyJson(client, ack.requestId, { items: [], watermark_event_id: 9 });

  assert.deepEqual((await reconcile).items.map(({ id }) => id), [30, 20, 10]);
  assert.equal(cursor.state, "live");
  assert.equal(cursor.watermark, 9);
});

test("reconcile preserves a u64::MAX watermark through history application and ACK", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_big", U64_MAX - 1n);
  cursor.observe(decodeChatEvent(`{"version":1,"type":"resync_required","channel_id":"ch_big","watermark_event_id":${U64_MAX}}`));
  let appliedWatermark;

  const reconcile = client.reconcileChat(cursor, ({ watermark_event_id }) => {
    appliedWatermark = watermark_event_id;
  });
  const history = await waitForSent(ws, 1);
  replyJson(client, history.requestId, { items: [], watermark_event_id: U64_MAX });
  const ack = await waitForSent(ws, 2);
  assert.deepEqual(ack.payload, { channel_id: "ch_big", limit: 1, acknowledge_watermark: U64_MAX });
  assert.match(ack.payloadText, /"acknowledge_watermark":18446744073709551615/);
  replyJson(client, ack.requestId, { items: [], watermark_event_id: U64_MAX });

  assert.equal((await reconcile).watermark_event_id, U64_MAX);
  assert.equal(appliedWatermark, U64_MAX);
  assert.equal(cursor.watermark, U64_MAX);
});

test("reconcile rejects unordered pages without applying or ACKing", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));
  let applied = false;

  const reconcile = client.reconcileChat(cursor, () => { applied = true; }, { limit: 2 });
  const request = await waitForSent(ws, 1);
  replyJson(client, request.requestId, {
    items: [historyMessage(20), historyMessage(30)], watermark_event_id: 9,
  });

  await assert.rejects(reconcile, /newest-first/);
  assert.equal(applied, false);
  assert.equal(ws.sent.length, 1);
  assert.equal(cursor.state, "reconcile_required");
  assert.equal(cursor.watermark, 5);
});

test("a malformed correlated response releases its cursor-bound request handle", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));

  const malformed = client.reconcileChat(cursor, () => {}, { limit: 2 });
  const first = await waitForSent(ws, 1);
  replyJson(client, first.requestId, { items: "not-an-array", watermark_event_id: 9 });
  await assert.rejects(malformed, /malformed chat\.history response/);

  const retried = client.reconcileChat(cursor, () => {}, { limit: 2 });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ws.sent.length, 2, "failed request handle must not permanently lock the cursor");
  const page = decodeRpcFrame(ws.sent[1]);
  replyJson(client, page.requestId, { items: [], watermark_event_id: 9 });
  const ack = await waitForSent(ws, 3);
  replyJson(client, ack.requestId, { items: [], watermark_event_id: 9 });
  await retried;
});

test("a malformed continuation aborts its partial snapshot and retries from newest", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));

  const malformed = client.reconcileChat(cursor, () => {}, { limit: 2 });
  const first = await waitForSent(ws, 1);
  replyJson(client, first.requestId, {
    items: [historyMessage(30), historyMessage(20)], watermark_event_id: 9,
  });
  const continuation = await waitForSent(ws, 2);
  assert.equal(continuation.payload.before_message_id, 20);
  replyJson(client, continuation.requestId, {
    items: [historyMessage(20), historyMessage(10)], watermark_event_id: 9,
  });

  await assert.rejects(malformed, /does not follow its request cursor/);
  assert.equal(cursor.state, "reconcile_required");
  assert.equal(ws.sent.length, 2, "malformed history must not be acknowledged");

  const retried = client.reconcileChat(cursor, () => {}, { limit: 2 });
  const restarted = await waitForSent(ws, 3);
  assert.deepEqual(restarted.payload, {
    channel_id: "ch_demo", limit: 2,
  }, "a retry must discard the partial snapshot and start from newest");
  replyJson(client, restarted.requestId, { items: [], watermark_event_id: 9 });
  const ack = await waitForSent(ws, 4);
  replyJson(client, ack.requestId, { items: [], watermark_event_id: 9 });
  await retried;
});

test("history handles are invalidated when their cursor disconnects and rejoins", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));
  let applied = false;

  const stale = client.reconcileChat(cursor, () => { applied = true; }, { limit: 2 });
  const staleRequest = await waitForSent(ws, 1);
  cursor.disconnected();
  cursor.rejoined({ channel_id: "ch_demo", watermark_event_id: 9 });
  replyJson(client, staleRequest.requestId, { items: [], watermark_event_id: 9 });

  await assert.rejects(stale, /does not match|out-of-sequence/);
  assert.equal(applied, false, "a response for the pre-disconnect request must not reach application state");
  assert.equal(cursor.state, "reconcile_required");
});

test("reconcile restarts from the newest page when the snapshot watermark changes", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));
  const applied = [];

  const reconcile = client.reconcileChat(cursor, ({ messages }) => applied.push(...messages), { limit: 2, maxAttempts: 3 });
  const first = await waitForSent(ws, 1);
  replyJson(client, first.requestId, { items: [historyMessage(30), historyMessage(20)], watermark_event_id: 9 });
  const raced = await waitForSent(ws, 2);
  replyJson(client, raced.requestId, { items: [historyMessage(10)], watermark_event_id: 10 });

  const restarted = await waitForSent(ws, 3);
  assert.deepEqual(restarted.payload, { channel_id: "ch_demo", limit: 2 });
  replyJson(client, restarted.requestId, { items: [historyMessage(30, 10)], watermark_event_id: 10 });
  const ack = await waitForSent(ws, 4);
  assert.deepEqual(applied.map(({ id }) => id), [30], "unstable pages are never applied");
  replyJson(client, ack.requestId, { items: [], watermark_event_id: 10 });

  await reconcile;
  assert.equal(cursor.watermark, 10);
});

test("ACK watermark movement publishes an explicit replacement snapshot before becoming current", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));
  const snapshots = [];

  const reconcile = client.reconcileChat(cursor, (snapshot) => {
    snapshots.push(snapshot);
  }, { limit: 2, maxAttempts: 3 });

  const history9 = await waitForSent(ws, 1);
  replyJson(client, history9.requestId, {
    items: [historyMessage(30), historyMessage(20)], watermark_event_id: 9,
  });
  const history9Terminal = await waitForSent(ws, 2);
  replyJson(client, history9Terminal.requestId, { items: [], watermark_event_id: 9 });

  const ack9 = await waitForSent(ws, 3);
  assert.equal(cursor.state, "ack_required");
  assert.equal(cursor.watermark, 5);
  assert.deepEqual(snapshots, [{
    messages: [historyMessage(30), historyMessage(20)],
    watermark_event_id: 9,
    replace: true,
    generation: 1,
  }]);
  replyJson(client, ack9.requestId, { items: [], watermark_event_id: 10 });

  const history10 = await waitForSent(ws, 4);
  assert.deepEqual(history10.payload, { channel_id: "ch_demo", limit: 2 });
  replyJson(client, history10.requestId, {
    items: [historyMessage(40, 10)], watermark_event_id: 10,
  });

  const ack10 = await waitForSent(ws, 5);
  assert.equal(cursor.state, "ack_required");
  assert.equal(cursor.watermark, 5, "replacement application is not current before ACK 10");
  assert.deepEqual(snapshots, [
    {
      messages: [historyMessage(30), historyMessage(20)],
      watermark_event_id: 9,
      replace: true,
      generation: 1,
    },
    {
      messages: [historyMessage(40, 10)],
      watermark_event_id: 10,
      replace: true,
      generation: 2,
    },
  ]);
  replyJson(client, ack10.requestId, { items: [], watermark_event_id: 10 });

  assert.equal((await reconcile).watermark_event_id, 10);
  assert.equal(cursor.state, "live");
  assert.equal(cursor.watermark, 10);
});

test("failed snapshot application aborts the transaction and permits a fresh replacement", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  const cursor = new ChatEventCursor("ch_demo", 5);
  cursor.observe(decodeChatEvent(bytes(fixture.valid.find(({ name }) => name === "resync_required").event)));

  const failed = client.reconcileChat(cursor, () => {
    throw new Error("application transaction failed");
  });
  const failedHistory = await waitForSent(ws, 1);
  replyJson(client, failedHistory.requestId, { items: [], watermark_event_id: 9 });
  await assert.rejects(failed, /application transaction failed/);
  assert.equal(cursor.state, "reconcile_required");
  assert.equal(cursor.watermark, 5);

  let replacement;
  const retried = client.reconcileChat(cursor, (snapshot) => { replacement = snapshot; });
  const retryHistory = await waitForSent(ws, 2);
  replyJson(client, retryHistory.requestId, {
    items: [historyMessage(30)], watermark_event_id: 9,
  });
  const ack = await waitForSent(ws, 3);
  assert.equal(replacement.replace, true);
  assert.equal(replacement.watermark_event_id, 9);
  assert.equal(cursor.watermark, 5);
  replyJson(client, ack.requestId, { items: [], watermark_event_id: 9 });
  await retried;
  assert.equal(cursor.watermark, 9);
});

class FakeWebSocket {
  constructor() {
    this.readyState = 1;
    this.listeners = new Map();
    this.sent = [];
  }
  addEventListener(kind, handler) { this.listeners.set(kind, handler); }
  send(data) { this.sent.push(new Uint8Array(data)); }
  close() { this.readyState = 3; }
}
