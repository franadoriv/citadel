// Codec round-trip tests: mirror crates/citadel-wire/src/lib.rs unit tests to
// guarantee the JS SDK frames bytes identically to the server.

import { test } from "node:test";
import assert from "node:assert/strict";

import { Envelope, FrameDecoder, decodeDatagram } from "../src/envelope.js";

test("framed round-trip of a single envelope", () => {
  const env = new Envelope(7, new TextEncoder().encode("hello world"));
  const frame = env.encodeFramed();
  // [u32 len][u16 kind][body]; len = 2 + body.length
  assert.equal(frame.length, 4 + 2 + 11);
  const dv = new DataView(frame.buffer);
  assert.equal(dv.getUint32(0, false), 2 + 11);
  assert.equal(dv.getUint16(4, false), 7);

  const [decoded] = new FrameDecoder().push(frame);
  assert.equal(decoded.kind, 7);
  assert.equal(new TextDecoder().decode(decoded.body), "hello world");
});

test("multiple envelopes in one buffer decode in order", () => {
  const a = new Envelope(1, new TextEncoder().encode("aaa"));
  const b = new Envelope(2, new TextEncoder().encode("bbbb"));
  const merged = new Uint8Array(a.framedLen() + b.framedLen());
  merged.set(a.encodeFramed(), 0);
  merged.set(b.encodeFramed(), a.framedLen());

  const out = new FrameDecoder().push(merged);
  assert.equal(out.length, 2);
  assert.equal(out[0].kind, 1);
  assert.equal(out[1].kind, 2);
  assert.equal(new TextDecoder().decode(out[1].body), "bbbb");
});

test("partial frame stays buffered until the rest arrives", () => {
  const env = new Envelope(9, new TextEncoder().encode("abcdef"));
  const frame = env.encodeFramed();
  const dec = new FrameDecoder();

  const first = dec.push(frame.slice(0, frame.length - 3));
  assert.equal(first.length, 0, "incomplete frame yields nothing");
  assert.ok(dec.buffered > 0);

  const rest = dec.push(frame.slice(frame.length - 3));
  assert.equal(rest.length, 1);
  assert.equal(rest[0].kind, 9);
  assert.equal(dec.buffered, 0, "all bytes consumed");
});

test("partial length prefix is retained", () => {
  const dec = new FrameDecoder();
  assert.equal(dec.push(new Uint8Array([0, 0])).length, 0);
  assert.equal(dec.buffered, 2);
});

test("oversized length prefix throws", () => {
  const dec = new FrameDecoder();
  const buf = new Uint8Array(4);
  new DataView(buf.buffer).setUint32(0, 0xffffffff, false);
  assert.throws(() => dec.push(buf), RangeError);
});

test("undersized body length throws", () => {
  const dec = new FrameDecoder();
  const buf = new Uint8Array(4);
  new DataView(buf.buffer).setUint32(0, 1, false);
  assert.throws(() => dec.push(buf), RangeError);
});

test("datagram round-trip", () => {
  const env = new Envelope(42, new Uint8Array([1, 2, 3, 4]));
  const dgram = env.encodeDatagram();
  assert.equal(dgram.length, 2 + 4);
  const decoded = decodeDatagram(dgram);
  assert.equal(decoded.kind, 42);
  assert.deepEqual([...decoded.body], [1, 2, 3, 4]);
});
