import { test } from "node:test";
import assert from "node:assert/strict";

import { CitadelClient } from "../src/client.js";
import { Envelope, FrameDecoder } from "../src/envelope.js";
import {
  LagRecorder,
  DEFAULT_LAG_RECORD_BYTES,
  DIAG_DELIVERY_RELIABLE,
  DIAG_DIRECTION_INBOUND,
  DIAG_DIRECTION_OUTBOUND,
  LAG_HEADER_BYTES,
  LAG_RECORD_BYTES,
  decodeDiagFlush,
  decodeDiagStart,
} from "../src/lag-recorder.js";
import {
  AUTH_STATUS_GUEST,
  KIND_AUTH_RESULT,
  KIND_DIAG_CAPABILITIES,
  KIND_DIAG_FLUSH,
  KIND_DIAG_SERVER_TIME,
  KIND_DIAG_START,
  KIND_DIAG_STATUS,
  KIND_TSYNC_SNAPSHOT,
  KIND_TSYNC_V2_SNAPSHOT,
} from "../src/protocol.js";
import { WebSocketTransport } from "../src/transport.js";

function u16(out, offset, value) { out[offset] = value >>> 8; out[offset + 1] = value; }
function u32(out, offset, value) {
  out[offset] = value >>> 24; out[offset + 1] = value >>> 16; out[offset + 2] = value >>> 8; out[offset + 3] = value;
}
function u64(out, offset, value) {
  let current = BigInt(value);
  for (let index = 7; index >= 0; index -= 1) { out[offset + index] = Number(current & 0xffn); current >>= 8n; }
}
function readU32(bytes, offset) { return (((bytes[offset] * 0x1000000) + (bytes[offset + 1] << 16) + (bytes[offset + 2] << 8) + bytes[offset + 3]) >>> 0); }
function id(value = 7) { return new Uint8Array(16).fill(value); }

function serverTime(offer = 1n, ms = 2_000n) {
  const body = new Uint8Array(17); body[0] = 1; u64(body, 1, offer); u64(body, 9, ms); return body;
}

function startBody({ captureId = id(), generation = 2n, deadline = 3_800n, maxRecordBytes = 96, filters = [{ kind: KIND_TSYNC_SNAPSHOT, direction: 0 }] } = {}) {
  const body = new Uint8Array(38 + (filters.length * 12));
  body[0] = 1; body.set(captureId, 1); u64(body, 17, generation); u64(body, 25, deadline); u32(body, 33, maxRecordBytes); body[37] = filters.length;
  let offset = 38;
  for (const filter of filters) {
    u16(body, offset, filter.kind); body[offset + 2] = filter.direction; body[offset + 3] = filter.entityId ? 1 : 0; u64(body, offset + 4, filter.entityId || 0n); offset += 12;
  }
  return body;
}

function flushBody({ captureId = id(), generation = 2n, attempt = 3n, deadline = 3_900n, max = 1024 * 1024, token = "signed-token.1" } = {}) {
  const path = new TextEncoder().encode("/v1/diagnostics/captures/upload");
  const tokenBytes = new TextEncoder().encode(token);
  const body = new Uint8Array(51 + path.length + tokenBytes.length);
  body[0] = 1; body.set(captureId, 1); u64(body, 17, generation); u64(body, 25, attempt); u64(body, 33, deadline); u32(body, 41, max);
  body[45] = 1; body[46] = 1; u16(body, 47, path.length); u16(body, 49, tokenBytes.length); body.set(path, 51); body.set(tokenBytes, 51 + path.length);
  return body;
}

function snapshot(packet = 1, base = 0, tick = 99) {
  const body = new Uint8Array(13); u32(body, 0, tick); u32(body, 4, packet); u32(body, 8, base); body[12] = 20; return body;
}

function makeRecorder(nowRef, options = {}) {
  const status = [];
  const clocks = [];
  const recorder = new LagRecorder({
    now: () => nowRef.value,
    sendStatus: (body) => status.push(body),
    sendClockSync: (body) => clocks.push(body),
    uploadOrigin: "https://citadel.test:7353",
    ...options,
  });
  recorder.setAuthenticated(true);
  assert.ok(recorder.acceptServerTime({ offerId: 1n, serverUtcMs: 2_000n }));
  return { recorder, status, clocks };
}

async function bytesFrom(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value); total += value.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { out.set(chunk, offset); offset += chunk.length; }
  return out;
}

class FakeWebSocket {
  constructor() { this.readyState = 1; this.listeners = new Map(); this.sent = []; }
  addEventListener(name, callback) { this.listeners.set(name, callback); }
  send(value) { this.sent.push(value); }
  close() { this.readyState = 3; this.listeners.get("close")?.({}); }
  receive(value) { this.listeners.get("message")?.({ data: value }); }
}

test("disabled recorder stays silent; enabled recorder waits for a valid post-auth SERVER_TIME", () => {
  const disabledSocket = new FakeWebSocket();
  const disabled = new CitadelClient(disabledSocket);
  disabled._dispatch(new Envelope(KIND_AUTH_RESULT, new Uint8Array([AUTH_STATUS_GUEST])));
  disabled._dispatch(new Envelope(KIND_DIAG_SERVER_TIME, serverTime()));
  assert.equal(disabledSocket.sent.length, 0);

  const enabledSocket = new FakeWebSocket();
  const enabled = new CitadelClient(enabledSocket, {
    diagnostics: { lagRecorder: { enabled: true } },
    _diagnosticUploadOrigin: "https://citadel.test",
  });
  enabled._dispatch(new Envelope(KIND_DIAG_SERVER_TIME, serverTime()));
  assert.equal(enabledSocket.sent.length, 0, "pre-auth offer is ignored");
  enabled._dispatch(new Envelope(KIND_AUTH_RESULT, new Uint8Array([AUTH_STATUS_GUEST])));
  enabled._dispatch(new Envelope(KIND_DIAG_SERVER_TIME, serverTime()));
  const capability = new FrameDecoder().push(enabledSocket.sent.at(-1))[0];
  assert.equal(capability.kind, KIND_DIAG_CAPABILITIES);
});

test("legacy or disabled clients consume every reserved diagnostic control without handlers or uploads", () => {
  const socket = new FakeWebSocket();
  const legacy = new CitadelClient(socket);
  let gameplayHandlerCalled = false;
  legacy.on(KIND_DIAG_START, () => { gameplayHandlerCalled = true; });

  legacy._dispatch(new Envelope(KIND_AUTH_RESULT, new Uint8Array([AUTH_STATUS_GUEST])));
  legacy._dispatch(new Envelope(KIND_DIAG_SERVER_TIME, serverTime()));
  legacy._dispatch(new Envelope(KIND_DIAG_START, startBody()));
  legacy._dispatch(new Envelope(KIND_DIAG_FLUSH, flushBody()));

  assert.equal(socket.sent.length, 0, "a legacy client never advertises or uploads diagnostics");
  assert.equal(gameplayHandlerCalled, false, "reserved controls never become application frames");
});

test("START creates a bounded 48-byte ring and emits chronological CLAG rows after wrap", async () => {
  const now = { value: 1_000_000 };
  const { recorder, status } = makeRecorder(now);
  const started = decodeDiagStart(startBody());
  assert.ok(started);
  assert.equal(recorder.start(started), true);
  assert.equal(status.length, 1);
  assert.equal(status[0][25], 1);
  assert.equal(readU32(status[0], 42), LAG_HEADER_BYTES);

  now.value += 10;
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(1, 0, 10), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  now.value += 10;
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(2, 1, 11), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  now.value += 10;
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(3, 2, 12), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const flush = decodeDiagFlush(flushBody());
  assert.ok(flush);
  const frozen = recorder.freeze(flush);
  assert.ok(frozen);
  recorder._slots = 0; // A close/reset must not change the immutable snapshot order.
  const raw = await bytesFrom(recorder._rawStream(frozen));
  assert.equal(raw.length, LAG_HEADER_BYTES + (2 * LAG_RECORD_BYTES));
  assert.deepEqual([...raw.slice(0, 4)], [...new TextEncoder().encode("CLAG")]);
  assert.equal(readU32(raw, 12), 2);
  assert.equal(readU32(raw, LAG_HEADER_BYTES + 12), 2);
  assert.equal(readU32(raw, LAG_HEADER_BYTES + LAG_RECORD_BYTES + 12), 3);
  assert.equal(readU32(raw, 24), 0, "u64 high word of overwritten count");
  assert.equal(readU32(raw, 28), 1, "one deterministic overwrite");
});

test("local policy rejects entity filters and a conflicting START without replacing evidence", () => {
  const now = { value: 1_000_000 };
  const { recorder, status } = makeRecorder(now);
  const first = decodeDiagStart(startBody({ captureId: id(3) }));
  assert.equal(recorder.start(first), true);
  assert.equal(recorder.start(first), true, "the exact START is idempotent");
  const conflict = decodeDiagStart(startBody({ captureId: id(4) }));
  assert.equal(recorder.start(conflict), false);
  assert.equal(recorder.isRecording, true);
  assert.equal(status.at(-1)[25], 4);
  const altered = decodeDiagStart(startBody({ captureId: id(3), maxRecordBytes: 48 }));
  assert.equal(recorder.start(altered), false, "same identity with altered policy is not a duplicate START");
  const entity = decodeDiagStart(startBody({ captureId: id(5), filters: [{ kind: KIND_TSYNC_SNAPSHOT, direction: 0, entityId: 9n }] }));
  assert.equal(recorder.start(entity), false);
  assert.equal(recorder.isRecording, true);
});

test("v2 snapshot metadata is copied from its fixed prefix and malformed data never creates a row", async () => {
  const now = { value: 1_000_000 };
  const { recorder } = makeRecorder(now);
  const request = decodeDiagStart(startBody({ filters: [{ kind: KIND_TSYNC_V2_SNAPSHOT, direction: 0 }] }));
  assert.equal(recorder.start(request), true);
  recorder.record(KIND_TSYNC_V2_SNAPSHOT, new Uint8Array(3), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const body = new Uint8Array(31);
  u64(body, 0, 12n); u64(body, 8, 34n); u16(body, 16, 60); u32(body, 18, 55); u32(body, 22, 70); u32(body, 26, 69); body[30] = 20;
  now.value += 10;
  recorder.record(KIND_TSYNC_V2_SNAPSHOT, body, DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const frozen = recorder.freeze(decodeDiagFlush(flushBody()));
  const raw = await bytesFrom(recorder._rawStream(frozen));
  assert.equal(readU32(raw, 12), 1);
  assert.equal(readU32(raw, 40), 0);
  assert.equal(readU32(raw, 44), 1, "one malformed candidate");
  const row = raw.subarray(LAG_HEADER_BYTES);
  assert.equal(readU32(row, 12), 70);
  assert.equal(readU32(row, 16), 69);
  assert.equal(readU32(row, 24), 55);
  assert.equal((row[28] << 8) | row[29], 60);
  assert.deepEqual([...row.slice(32, 40)], [...body.slice(0, 8)]);
});

test("FLUSH grant is strict and upload is gzip, tokenized, bounded, and safely retryable", async () => {
  const now = { value: 1_000_000 };
  const requests = [];
  const { recorder, status } = makeRecorder(now, {
    fetch: async (request) => {
      requests.push(request);
      const raw = await new Response(request.body.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer();
      assert.deepEqual([...new Uint8Array(raw).slice(0, 4)], [...new TextEncoder().encode("CLAG")]);
      return { ok: requests.length > 1 };
    },
  });
  const start = decodeDiagStart(startBody({ maxRecordBytes: DEFAULT_LAG_RECORD_BYTES }));
  assert.equal(recorder.start(start), true);
  now.value += 10;
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const first = decodeDiagFlush(flushBody({ attempt: 3n }));
  assert.equal(await recorder.upload(first), false);
  assert.equal(recorder.isFrozen, true, "ambiguous/non-2xx retains bytes for a fresh grant");
  assert.equal(first.token, null, "attempt token is disposed after use");
  assert.equal(requests[0].headers.get("authorization"), "Bearer signed-token.1");
  assert.equal(requests[0].headers.get("content-type"), "application/vnd.citadel.lag-capture");
  assert.equal(requests[0].headers.get("content-encoding"), "gzip");
  assert.equal(requests[0].credentials, "omit");
  assert.equal(requests[0].redirect, "error");
  const second = decodeDiagFlush(flushBody({ attempt: 4n, token: "signed-token.2" }));
  assert.equal(await recorder.upload(second), true);
  assert.equal(recorder.isFrozen, false);
  assert.deepEqual(status.map((body) => body[25]), [1, 2, 4, 2, 3]);
});

test("FLUSH rejects malformed grants and a reused upload attempt without replacing frozen bytes", async () => {
  const now = { value: 1_000_000 };
  const { recorder } = makeRecorder(now, { fetch: async () => ({ ok: false }) });
  assert.equal(recorder.start(decodeDiagStart(startBody())), true);
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const malformed = flushBody();
  malformed[45] = 9;
  assert.equal(decodeDiagFlush(malformed), null);
  const first = decodeDiagFlush(flushBody({ attempt: 8n }));
  assert.equal(await recorder.upload(first), false);
  const replay = decodeDiagFlush(flushBody({ attempt: 8n, token: "fresh-but-replayed-id" }));
  assert.equal(await recorder.upload(replay), false);
  assert.equal(replay.token, null);
  assert.equal(recorder.isFrozen, true);
});

test("cancelling a pending upload aborts it and never publishes a late terminal status", async () => {
  const now = { value: 1_000_000 };
  let began;
  const begun = new Promise((resolve) => { began = resolve; });
  let release;
  const pendingResponse = new Promise((resolve) => { release = resolve; });
  const { recorder, status } = makeRecorder(now, {
    fetch: async () => {
      began();
      await pendingResponse;
      return { ok: true };
    },
  });
  assert.equal(recorder.start(decodeDiagStart(startBody())), true);
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const flush = decodeDiagFlush(flushBody({ attempt: 11n }));
  const upload = recorder.upload(flush);
  await begun;
  recorder.cancel();
  release();
  assert.equal(await upload, false);
  assert.equal(flush.token, null);
  assert.deepEqual(status.map((body) => body[25]), [1, 2]);
  assert.equal(recorder.isFrozen, false);
});

test("a late cancelled upload cannot clear the attempt owned by a newer capture", async () => {
  const now = { value: 1_000_000 };
  const began = [];
  const begin = [];
  const pending = [];
  const responses = [];
  const { recorder, status } = makeRecorder(now, {
    fetch: async () => {
      const index = began.length;
      began.push(index);
      begin[index]();
      return pending[index];
    },
  });
  const beginOne = new Promise((resolve) => { begin[0] = resolve; });
  pending[0] = new Promise((resolve) => { responses[0] = resolve; });
  assert.equal(recorder.start(decodeDiagStart(startBody({ captureId: id(12), generation: 2n }))), true);
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const firstUpload = recorder.upload(decodeDiagFlush(flushBody({ captureId: id(12), generation: 2n, attempt: 20n })));
  await beginOne;
  recorder.cancel();

  const beginTwo = new Promise((resolve) => { begin[1] = resolve; });
  pending[1] = new Promise((resolve) => { responses[1] = resolve; });
  assert.equal(recorder.start(decodeDiagStart(startBody({ captureId: id(13), generation: 3n }))), true);
  recorder.record(KIND_TSYNC_SNAPSHOT, snapshot(2), DIAG_DIRECTION_INBOUND | DIAG_DELIVERY_RELIABLE);
  const secondUpload = recorder.upload(decodeDiagFlush(flushBody({ captureId: id(13), generation: 3n, attempt: 21n, token: "second-token" })));
  await beginTwo;
  responses[1]({ ok: true });
  assert.equal(await secondUpload, true);
  responses[0]({ ok: true });
  assert.equal(await firstUpload, false);
  assert.deepEqual(status.map((body) => body[25]), [1, 2, 1, 2, 3]);
});

test("transport diagnostic hook uses a packed primitive and runs before normal dispatch", () => {
  const socket = new FakeWebSocket();
  const transport = new WebSocketTransport(socket);
  const events = [];
  transport.setHandlers({
    onDiagnosticEnvelope: (env, flags) => events.push(["diag", env.kind, flags]),
    onEnvelope: (env) => events.push(["dispatch", env.kind]),
    onClose: () => {},
  });
  socket.receive(new Envelope(99, new Uint8Array([1])).encodeFramed());
  assert.deepEqual(events, [["diag", 99, DIAG_DELIVERY_RELIABLE], ["dispatch", 99]]);
});

test("diagnostic controls never reach ordinary client handlers", () => {
  const socket = new FakeWebSocket();
  const client = new CitadelClient(socket, {
    diagnostics: { lagRecorder: { enabled: true } }, _diagnosticUploadOrigin: "https://citadel.test",
  });
  let invoked = false;
  client.on(KIND_DIAG_START, () => { invoked = true; });
  client._dispatch(new Envelope(KIND_AUTH_RESULT, new Uint8Array([AUTH_STATUS_GUEST])));
  client._dispatch(new Envelope(KIND_DIAG_SERVER_TIME, serverTime()));
  client._dispatch(new Envelope(KIND_DIAG_START, startBody()));
  assert.equal(invoked, false);
  const sent = socket.sent.map((frame) => new FrameDecoder().push(frame)[0]);
  assert.ok(sent.some((envelope) => envelope.kind === KIND_DIAG_STATUS));
});
