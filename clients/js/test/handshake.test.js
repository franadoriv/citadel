// Realtime auth handshake tests for the token variant (`handshakeToken`),
// mirroring the guest handshake's send/await/decode shape. The load-bearing
// assertion is wire framing: KIND_AUTH must carry the UTF-8 bytes of the token
// string (an empty body is the separate guest handshake).

import { test } from "node:test";
import assert from "node:assert/strict";

import { CitadelClient } from "../src/client.js";
import { Envelope, FrameDecoder } from "../src/envelope.js";
import {
  KIND_AUTH,
  KIND_AUTH_RESULT,
  AUTH_STATUS_AUTHENTICATED,
} from "../src/protocol.js";

class FakeWebSocket {
  constructor() { this.readyState = 1; this.listeners = new Map(); this.sent = []; }
  addEventListener(kind, handler) { this.listeners.set(kind, handler); }
  send(data) { this.sent.push(data); }
  close() { this.readyState = 3; }
}

/** Decode the single framed envelope a fake socket captured. */
function onlySentEnvelope(ws) {
  assert.equal(ws.sent.length, 1);
  const envelopes = new FrameDecoder().push(ws.sent[0]);
  assert.equal(envelopes.length, 1);
  return envelopes[0];
}

test("handshakeToken frames KIND_AUTH with the UTF-8 token bytes and resolves the auth result", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);

  // A non-ASCII char proves UTF-8 encoding (é => 0xC3 0xA9), not char codes.
  const token = "session-token-é";
  const pending = client.handshakeToken(token);

  const sent = onlySentEnvelope(ws);
  assert.equal(sent.kind, KIND_AUTH);
  assert.deepEqual(sent.body, new TextEncoder().encode(token));

  const ackBody = new Uint8Array([AUTH_STATUS_AUTHENTICATED, ...new TextEncoder().encode("user-1")]);
  client._dispatch(new Envelope(KIND_AUTH_RESULT, ackBody));

  const result = await pending;
  assert.equal(result.status, AUTH_STATUS_AUTHENTICATED);
  assert.equal(result.userId, "user-1");
  assert.equal(result.reasonClass, 0);
});

test("handshakeToken rejects an empty token instead of sending a guest handshake", async () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  await assert.rejects(() => client.handshakeToken(""), TypeError);
  assert.equal(ws.sent.length, 0);
});
