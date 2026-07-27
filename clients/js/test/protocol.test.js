// Protocol (de)serialization tests: RPC request/response, auth result, and the
// sender-id prefix. Byte layouts must match crates/citadel-wire/src/protocol.rs.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  AUTH_STATUS_AUTHENTICATED,
  AUTH_STATUS_GUEST,
  AUTH_STATUS_REJECTED,
  AUTH_REASON_PROTOCOL,
  RPC_STATUS_OK,
  RPC_STATUS_ERROR,
  decodeAuthResult,
  decodeRpcResponse,
  encodeRpcRequest,
  splitSender,
  tagWithSender,
  encodeRoomCreate,
  encodeRoomId,
  decodeRoomJoined,
  decodeRoomId,
} from "../src/protocol.js";

test("encodeRpcRequest layout: [id u64][len u16][method][payload]", () => {
  const payload = new Uint8Array([9, 9]);
  const buf = encodeRpcRequest(258n, "add", payload);
  const dv = new DataView(buf.buffer);
  assert.equal(dv.getBigUint64(0, false), 258n);
  assert.equal(dv.getUint16(8, false), 3); // "add"
  assert.equal(new TextDecoder().decode(buf.slice(10, 13)), "add");
  assert.deepEqual([...buf.slice(13)], [9, 9]);
});

test("encodeRpcRequest accepts a number id", () => {
  const buf = encodeRpcRequest(1, "ping");
  assert.equal(new DataView(buf.buffer).getBigUint64(0, false), 1n);
});

test("decodeRpcResponse ok + error", () => {
  const ok = new Uint8Array(8 + 1 + 2);
  const okDv = new DataView(ok.buffer);
  okDv.setBigUint64(0, 7n, false);
  ok[8] = RPC_STATUS_OK;
  ok[9] = 42; ok[10] = 43;
  const okRes = decodeRpcResponse(ok);
  assert.equal(okRes.requestId, 7n);
  assert.equal(okRes.status, RPC_STATUS_OK);
  assert.deepEqual([...okRes.payload], [42, 43]);

  const err = new Uint8Array(9);
  new DataView(err.buffer).setBigUint64(0, 8n, false);
  err[8] = RPC_STATUS_ERROR;
  assert.equal(decodeRpcResponse(err).status, RPC_STATUS_ERROR);
});

test("decodeRpcResponse rejects a too-short body", () => {
  assert.equal(decodeRpcResponse(new Uint8Array(5)), null);
});

test("decodeAuthResult: guest, authenticated, rejected", () => {
  assert.equal(decodeAuthResult(new Uint8Array([AUTH_STATUS_GUEST])).status, AUTH_STATUS_GUEST);

  const auth = new Uint8Array([AUTH_STATUS_AUTHENTICATED, ...new TextEncoder().encode("user-1")]);
  const authRes = decodeAuthResult(auth);
  assert.equal(authRes.status, AUTH_STATUS_AUTHENTICATED);
  assert.equal(authRes.userId, "user-1");

  const rej = new Uint8Array([AUTH_STATUS_REJECTED, AUTH_REASON_PROTOCOL]);
  assert.equal(decodeAuthResult(rej).reasonClass, AUTH_REASON_PROTOCOL);

  assert.equal(decodeAuthResult(new Uint8Array(0)), null);
});

test("tagWithSender / splitSender round-trip", () => {
  const payload = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
  const tagged = tagWithSender(42n, payload);
  const split = splitSender(tagged);
  assert.notEqual(split, null);
  assert.equal(split[0], 42n);
  assert.deepEqual([...split[1]], [...payload]);
});

test("splitSender rejects a short body", () => {
  assert.equal(splitSender(new Uint8Array(4)), null);
});

test("room codecs use big-endian ids and u16 UTF-8 strings", () => {
  const create = encodeRoomCreate("lobby");
  assert.deepEqual([...create.slice(0, 2)], [0, 5]);
  assert.equal(new TextDecoder().decode(create.slice(2)), "lobby");
  assert.equal(decodeRoomId(encodeRoomId(42n)), 42n);
  assert.equal(decodeRoomId(new Uint8Array(7)), null);

  const map = new TextEncoder().encode("arena");
  const mode = new TextEncoder().encode("duel");
  const joined = new Uint8Array(8 + 2 + map.length + 2 + mode.length);
  const view = new DataView(joined.buffer);
  view.setBigUint64(0, 42n, false);
  view.setUint16(8, map.length, false);
  joined.set(map, 10);
  view.setUint16(10 + map.length, mode.length, false);
  joined.set(mode, 12 + map.length);
  assert.deepEqual(decodeRoomJoined(joined), { roomId: 42n, map: "arena", mode: "duel" });
  assert.equal(decodeRoomJoined(joined.slice(0, -1)), null);
});
