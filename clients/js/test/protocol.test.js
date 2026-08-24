// Protocol (de)serialization tests: RPC request/response, auth result, and the
// sender-id prefix. Byte layouts must match crates/citadel-wire/src/protocol.rs.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as protocolExports from "../src/protocol.js";
import * as rootExports from "../src/index.js";

const V1_AUTHORITATIVE_INPUT_EXPORTS = [
  "KIND_INPUT_STREAM_CONTROL",
  "INPUT_STREAM_CONTROL_VERSION",
  "INPUT_STREAM_CONTROL_ADVERTISE",
  "INPUT_STREAM_CONTROL_REVOKE",
  "INPUT_STREAM_TOKEN_BYTES",
  "KIND_AUTHORITATIVE_INPUT",
  "AUTHORITATIVE_INPUT_VERSION",
  "KIND_CAPABILITY_OFFER",
  "KIND_CAPABILITY_ACCEPTANCE",
  "CAPABILITY_NEGOTIATION_VERSION",
  "CAPABILITY_AUTHORITATIVE_INPUT",
  "CAPABILITY_CHALLENGE_BYTES",
  "encodeCapabilityAcceptance",
  "decodeCapabilityOffer",
  "MAX_SEQUENCED_INPUT_BODY_BYTES",
  "decodeInputStreamControl",
];

const authoritativeInputFixtures = JSON.parse(readFileSync(
  new URL("../../authoritative-input-fixtures.json", import.meta.url),
  "utf8",
));

import {
  EXPECTED_ABI_VERSION,
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
  decodeTsyncV2Snapshot,
  TsyncV2EpochFence,
  TSYNC_V2_VERSION,
  TSYNC_V2_CLOCK_CAPABILITY,
  encodeTsyncV2Manifest,
  decodeTsyncV2Manifest,
  KIND_CAPABILITY_OFFER,
  KIND_CAPABILITY_ACCEPTANCE,
  encodeCapabilityAcceptance,
  decodeCapabilityOffer,
  decodeInputStreamControl,
  INPUT_STREAM_CONTROL_ADVERTISE,
  INPUT_STREAM_CONTROL_REVOKE,
  KIND_AUTHORITATIVE_INPUT,
  encodeSequencedInput,
  decodeSequencedInput,
  encodeInputReceipt,
  decodeInputReceipt,
} from "../src/protocol.js";

test("protocol bindings target ABI v3", () => {
  assert.equal(EXPECTED_ABI_VERSION, 3);
});

test("V1 authoritative input/control protocol exports match the public declaration surface", () => {
  const declarations = readFileSync(new URL("../index.d.ts", import.meta.url), "utf8");

  for (const name of V1_AUTHORITATIVE_INPUT_EXPORTS) {
    assert.ok(name in protocolExports, `${name} must be exported by protocol.js`);
    assert.ok(name in rootExports, `${name} must be re-exported by the package root`);
    assert.match(
      declarations,
      new RegExp(`export (?:const|function) ${name}\\b`),
      `${name} must be declared by index.d.ts`,
    );
  }
});

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

test("input-stream control codec accepts canonical values and rejects malformed values", () => {
  const advertise = new Uint8Array(34);
  advertise.set([1, INPUT_STREAM_CONTROL_ADVERTISE], 0);
  new DataView(advertise.buffer).setBigUint64(2, 7n, false);
  new DataView(advertise.buffer).setBigUint64(10, 9n, false);
  advertise.fill(0xA5, 18);
  const decoded = decodeInputStreamControl(advertise);
  assert.equal(decoded.opcode, INPUT_STREAM_CONTROL_ADVERTISE);
  assert.equal(decoded.matchId, 7n);
  assert.equal(decoded.streamId, 9n);
  assert.deepEqual([...decoded.token], Array(16).fill(0xA5));
  const revoke = advertise.slice(0, 18);
  revoke[1] = INPUT_STREAM_CONTROL_REVOKE;
  assert.deepEqual(decodeInputStreamControl(revoke), {
    opcode: INPUT_STREAM_CONTROL_REVOKE, matchId: 7n, streamId: 9n,
  });
  for (const malformed of [new Uint8Array(17), new Uint8Array([2, 1]), advertise.slice(0, 33), new Uint8Array(34)]) {
    assert.equal(decodeInputStreamControl(malformed), null);
  }
});

test("sequenced authoritative input is canonical and separate from legacy generic input", () => {
  assert.equal(KIND_AUTHORITATIVE_INPUT, 41);
  const token = new Uint8Array(16).fill(0xA5);
  const body = encodeSequencedInput(token, 0x0102030405060708n, 0xbeef, new Uint8Array([1, 2, 3]));
  assert.deepEqual([...body], [1, ...Array(16).fill(0xA5), 1, 2, 3, 4, 5, 6, 7, 8, 0xbe, 0xef, 0, 0, 0, 3, 1, 2, 3]);
  assert.deepEqual(decodeSequencedInput(body), { streamToken: token, sequence: 0x0102030405060708n, originalCustomKind: 0xbeef, body: new Uint8Array([1, 2, 3]) });
  assert.equal(decodeSequencedInput(body.slice(0, -1)), null);
});

test("sequenced authoritative input rejects lossy and out-of-range u64 sequences before encoding", () => {
  const token = new Uint8Array(16).fill(0xA5);
  for (const sequence of [Number.MAX_SAFE_INTEGER + 1, -1, 1.5, 1n << 64n]) {
    assert.throws(
      () => encodeSequencedInput(token, sequence, 1),
      /sequence must be an unsigned u64/,
    );
  }
  assert.equal(
    new DataView(encodeSequencedInput(token, (1n << 64n) - 1n, 1).buffer).getBigUint64(17, false),
    (1n << 64n) - 1n,
  );
});

test("receipts preserve unsigned u64 correlation and opaque corrections exactly", () => {
  const token = new Uint8Array([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30]);
  const max = (1n << 64n) - 1n;
  const receipt = {
    matchId: max,
    streamId: 0x1112131415161718n,
    streamToken: token,
    acknowledgedSequence: 0n,
    decidedSequence: max,
    disposition: 1,
    authoritativeTick: 0x5152535455565758n,
    correction: new Uint8Array([0x00, 0xff, 0x80, 0x41]),
  };
  const encoded = encodeInputReceipt(receipt);
  assert.equal(
    Buffer.from(encoded).toString("hex"),
    authoritativeInputFixtures.input_receipt.hex,
    "the JS codec must emit the shared canonical cross-language receipt fixture",
  );
  assert.deepEqual(decodeInputReceipt(encoded), receipt);
  assert.equal(decodeInputReceipt(encoded.slice(0, -1)), null);
  assert.equal(decodeInputReceipt(new Uint8Array([...encoded, 0xa5])), null);
  const invalidDisposition = encoded.slice(); invalidDisposition[49] = 2;
  assert.equal(decodeInputReceipt(invalidDisposition), null);
  const absentCorrection = encodeInputReceipt({ ...receipt, correction: null });
  absentCorrection[59] = 0; absentCorrection[60] = 0; absentCorrection[61] = 0; absentCorrection[62] = 1;
  assert.equal(decodeInputReceipt(absentCorrection), null);
  assert.throws(
    () => encodeInputReceipt({ ...receipt, decidedSequence: Number.MAX_SAFE_INTEGER + 1 }),
    /decidedSequence must be an unsigned u64/,
  );
});

test("standalone V1 capability offer has one canonical non-bearer acceptance echo", () => {
  const offer = new Uint8Array([1, 1, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f]);
  assert.equal(KIND_CAPABILITY_OFFER, 42);
  assert.equal(KIND_CAPABILITY_ACCEPTANCE, 43);
  assert.deepEqual(decodeCapabilityOffer(offer), { capability: 1, challenge: offer.slice(2) });
  assert.deepEqual(encodeCapabilityAcceptance(offer), offer);
  assert.equal(decodeCapabilityOffer(new Uint8Array([...offer, 0])), null);
  assert.equal(decodeCapabilityOffer(new Uint8Array(18)), null);
});

test("TSYNC manifest only negotiates its clock layout", () => {
  assert.deepEqual(encodeTsyncV2Manifest(TSYNC_V2_CLOCK_CAPABILITY), new Uint8Array([TSYNC_V2_VERSION, TSYNC_V2_CLOCK_CAPABILITY]));
  assert.deepEqual(decodeTsyncV2Manifest(new Uint8Array([TSYNC_V2_VERSION, TSYNC_V2_CLOCK_CAPABILITY])), {
    capabilities: TSYNC_V2_CLOCK_CAPABILITY,
  });
  assert.equal(decodeTsyncV2Manifest(new Uint8Array([TSYNC_V2_VERSION, 0])), null);
  assert.equal(decodeTsyncV2Manifest(new Uint8Array([TSYNC_V2_VERSION, TSYNC_V2_CLOCK_CAPABILITY | 0x80])), null);
});

test("v2 transform wrapper decodes, fences stale epochs, and resets", () => {
  const body = new Uint8Array(18 + 2);
  const view = new DataView(body.buffer);
  view.setBigUint64(0, 7n, false);
  view.setBigUint64(8, 99n, false);
  view.setUint16(16, 60, false);
  body.set([0xaa, 0xbb], 18);
  assert.deepEqual(decodeTsyncV2Snapshot(body), {
    epoch: 7n, tick: 99n, tickHz: 60, snapshotBody: new Uint8Array([0xaa, 0xbb]),
  });
  const fence = new TsyncV2EpochFence();
  assert.deepEqual(fence.apply(body, (snapshot) => snapshot), {
    clock: { epoch: 7n, tick: 99n, tickHz: 60 }, snapshot: new Uint8Array([0xaa, 0xbb]),
  });
  view.setBigUint64(0, 6n, false);
  assert.equal(fence.apply(body, (snapshot) => snapshot), null, "old epoch is rejected");
  assert.equal(fence.reset(8n), true);
  view.setBigUint64(0, 8n, false);
  assert.notEqual(fence.apply(body, (snapshot) => snapshot), null, "reset admits new epoch");
  assert.equal(decodeTsyncV2Snapshot(new Uint8Array(17)), null);
});
