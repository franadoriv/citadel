// Schema-bound NetworkPeer DeltaBunch codec for the browser SDK.
//
// This mirrors the canonical MSB-first wire format. It is intentionally a
// transport helper: callers send returned bodies through KIND_REP_DELTA and
// apply the decoded values to their own game objects.

import { KIND_REP_ACK, KIND_REP_DELTA } from "./protocol.js";

const MAX_COLLECTION_OPS = 65_536;
const MAX_BYTES = 1 << 20;
const MAX_BUNCHES = 4096;
const MAX_ACKS = 8192;

function fail(message) { throw new Error(`NetworkPeer: ${message}`); }
function asBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  fail("bytes value must be Uint8Array or ArrayBuffer");
}
function integer(value, name, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max) fail(`${name} is out of range`);
  return value;
}
function token(value, name) {
  if (typeof value === "bigint") { if (value < 0n || value > 0xffff_ffff_ffff_ffffn) fail(`${name} is out of range`); return value; }
  if (!Number.isSafeInteger(value) || value < 0) fail(`${name} is out of range`);
  return BigInt(value);
}

class Bits {
  constructor(bytes) { this.bytes = bytes ? Array.from(bytes) : []; this.pos = 0; this.reading = !!bytes; }
  write(value, count) {
    let v = BigInt(value); if (count < 0 || count > 64 || v < 0n || v >= (1n << BigInt(count || 1))) fail("invalid bit value");
    for (let i = count - 1; i >= 0; i--) { const at = this.pos >> 3; if (this.pos % 8 === 0) this.bytes[at] = 0; this.bytes[at] |= Number((v >> BigInt(i)) & 1n) << (7 - (this.pos % 8)); this.pos++; }
  }
  read(count) {
    if (count < 0 || count > 64 || this.pos + count > this.bytes.length * 8) fail("truncated bit stream");
    let v = 0n; for (let i = 0; i < count; i++) { v = (v << 1n) | BigInt((this.bytes[this.pos >> 3] >> (7 - (this.pos % 8))) & 1); this.pos++; } return v;
  }
  bool() { return this.read(1) === 1n; }
  finish() { while (this.pos % 8) this.write(0n, 1); return Uint8Array.from(this.bytes); }
  assertPadding() { while (this.pos < this.bytes.length * 8) if (this.read(1) !== 0n) fail("non-canonical trailing padding"); }
}
function writeVarint(w, value) { let v = token(value, "varint"); do { const payload = v & 0x7fn; v >>= 7n; w.write((v ? 0x80n : 0n) | payload, 8); } while (v); }
function readVarint(r) { let out = 0n; for (let i = 0; i < 10; i++) { const byte = r.read(8); out |= (byte & 0x7fn) << BigInt(i * 7); if (!(byte & 0x80n)) { if (i && byte === 0n) fail("non-canonical varint"); return out; } } fail("varint too long"); }
/** Canonical RepId index/generation fields are u32, not arbitrary JS numbers. */
function readU32Varint(r, name) { const value = readVarint(r); if (value > 0xffffffffn) fail(`${name} must fit u32`); return Number(value); }
function byteVarint(out, value) { let v = token(value, "varint"); do { const b = Number(v & 0x7fn); v >>= 7n; out.push(v ? b | 0x80 : b); } while (v); }
function readByteVarint(bytes, state) { let out = 0n; for (let i = 0; i < 10; i++) { if (state.pos >= bytes.length) fail("truncated byte varint"); const b = bytes[state.pos++]; out |= BigInt(b & 0x7f) << BigInt(i * 7); if (!(b & 0x80)) { if (i && b === 0) fail("non-canonical byte varint"); return out; } } fail("byte varint too long"); }
function bitWidth(n) { let width = 0; for (let v = BigInt(n); v > 1n; v = (v + 1n) >> 1n) width++; return width; }
function scalar(codec, value, decode) {
  const { min, max, valuesPerUnit } = codec;
  if (!Number.isFinite(min) || !Number.isFinite(max) || !Number.isSafeInteger(valuesPerUnit) || max <= min || valuesPerUnit < 1) fail("invalid scalar codec");
  const steps = Math.max(1, Math.floor((max - min) * valuesPerUnit + .5)); const bits = bitWidth(steps + 1);
  if (decode) { const code = Number(decode.read(bits)); if (code > steps) fail("scalar code out of range"); return min + code / valuesPerUnit; }
  if (Number.isNaN(value)) fail("scalar must not be NaN"); return { bits, code: Math.min(steps, Math.max(0, Math.floor((Math.min(max, Math.max(min, value)) - min) * valuesPerUnit + .5))) };
}
function valueCodec(w, codec, value, decode = false) {
  if (!codec || typeof codec.type !== "string") fail("invalid field codec");
  switch (codec.type) {
    case "bool": return decode ? w.bool() : (typeof value !== "boolean" ? fail("bool value required") : w.write(value ? 1n : 0n, 1));
    case "int": { const min = integer(codec.min, "int min", -Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER); const max = integer(codec.max, "int max", min, Number.MAX_SAFE_INTEGER); const bits = bitWidth(BigInt(max) - BigInt(min) + 1n); if (decode) { const code = Number(w.read(bits)); if (code > max - min) fail("int code out of range"); return min + code; } if (!Number.isSafeInteger(value)) fail("int value is out of range"); return w.write(BigInt(Math.min(max, Math.max(min, value)) - min), bits); }
    case "scalar": { if (decode) return scalar(codec, 0, w); const q = scalar(codec, value); return w.write(BigInt(q.code), q.bits); }
    case "vector3": { if (decode) return [scalar({ ...codec, type: "scalar" }, 0, w), scalar({ ...codec, type: "scalar" }, 0, w), scalar({ ...codec, type: "scalar" }, 0, w)]; if (!Array.isArray(value) || value.length !== 3) fail("vector3 value required"); for (const x of value) valueCodec(w, { ...codec, type: "scalar" }, x); return; }
    case "quat": { const bits = codec.bits; if (![9, 10, 15].includes(bits)) fail("quat bits must be 9, 10, or 15"); if (decode) { const dropped = Number(w.read(2)); const levels = 2 ** bits; const kept = [Number(w.read(bits)), Number(w.read(bits)), Number(w.read(bits))].map(c => (c / (levels - 1)) * Math.SQRT2 - Math.SQRT1_2); const q = []; let k = 0, sum = 0; for (const x of kept) sum += x * x; for (let i = 0; i < 4; i++) q[i] = i === dropped ? Math.sqrt(Math.max(0, 1 - sum)) : kept[k++]; const norm = Math.hypot(...q); return norm < 1e-6 ? [0, 0, 0, 1] : q.map(x => x / norm); } if (!Array.isArray(value) || value.length !== 4 || value.some(x => !Number.isFinite(x))) fail("quaternion value required"); let q = value.slice(); const norm = Math.hypot(...q); q = norm < 1e-6 ? [0, 0, 0, 1] : q.map(x => x / norm); let dropped = 0; for (let i = 1; i < 4; i++) if (Math.abs(q[i]) > Math.abs(q[dropped])) dropped = i; if (q[dropped] < 0) q = q.map(x => -x); w.write(BigInt(dropped), 2); const levels = 2 ** bits; for (let i = 0; i < 4; i++) if (i !== dropped) w.write(BigInt(Math.min(levels - 1, Math.max(0, Math.floor(((q[i] + Math.SQRT1_2) / Math.SQRT2) * (levels - 1) + .5)))), bits); return; }
    case "bytes": { const cap = Math.min(integer(codec.maxLen, "bytes maxLen", 0), MAX_BYTES); if (decode) { const n = Number(readVarint(w)); if (n > cap) fail("byte field exceeds cap"); const out = new Uint8Array(n); for (let i = 0; i < n; i++) out[i] = Number(w.read(8)); return out; } const bytes = asBytes(value); if (bytes.length > cap) fail("byte field exceeds cap"); writeVarint(w, bytes.length); for (const b of bytes) w.write(BigInt(b), 8); return; }
    default: fail(`unsupported field codec ${codec.type}`);
  }
}
function collection(w, codec, value, decode = false) {
  if (codec.type !== "collection" || !codec.item || codec.item.type === "collection") fail("invalid collection codec"); const cap = Math.min(integer(codec.maxItems, "collection maxItems", 0), MAX_COLLECTION_OPS);
  const id = (x) => { const index = integer(x.index, "rep id index", 0, 0xffffffff); const generation = integer(x.generation, "rep id generation", 0, 0xffffffff); return { index, generation }; };
  const list = (items, withValue) => { if (items.length > cap) fail("collection exceeds cap"); writeVarint(w, items.length); for (const item of items) { const rep = id(withValue ? item.id : item); writeVarint(w, rep.index); writeVarint(w, rep.generation); if (withValue) { writeVarint(w, token(item.key, "rep key")); valueCodec(w, codec.item, item.value); } } };
  const read = (withValue) => { const n = Number(readVarint(w)); if (n > cap) fail("collection exceeds cap"); const items = []; for (let i = 0; i < n; i++) { const rep = { index: readU32Varint(w, "rep id index"), generation: readU32Varint(w, "rep id generation") }; if (withValue) items.push({ id: rep, key: readVarint(w), value: valueCodec(w, codec.item, undefined, true) }); else items.push(rep); } return items; };
  if (decode) { const out = { removed: read(false), added: read(true), changed: read(true) }; unique(out); return out; }
  if (!value || !Array.isArray(value.removed) || !Array.isArray(value.added) || !Array.isArray(value.changed)) fail("collection delta required"); if (value.removed.length + value.added.length + value.changed.length > MAX_COLLECTION_OPS) fail("collection total exceeds cap"); unique(value); list(value.removed, false); list(value.added, true); list(value.changed, true);
}
function unique(value) { const seen = new Set(); for (const item of [...value.removed, ...value.added.map(x => x.id), ...value.changed.map(x => x.id)]) { const key = `${item.index}/${item.generation}`; if (seen.has(key)) fail("duplicate collection rep id"); seen.add(key); } }
function validateSchema(schema) { if (!schema || !(schema.hash instanceof Uint8Array) || schema.hash.length !== 16 || !integer(schema.layoutVersion, "layoutVersion", 0, 0xffffffff) && schema.layoutVersion !== 0 || !Array.isArray(schema.fields) || schema.fields.length > 0xffff) fail("invalid schema"); for (const f of schema.fields) { if (f.type === "collection" && f.item?.type === "collection") fail("nested collection"); if (!f?.type) fail("invalid field codec"); } return schema; }

/** Encode one standalone schema-bound DeltaBunch body. */
export function encodeDeltaBunch(schema, bunch) {
  validateSchema(schema); const { objectId, isFull, resultId } = bunch || {}; integer(objectId, "objectId", 0, 0xffffffff); const result = token(resultId, "resultId"); if (!result) fail("resultId must be nonzero"); const base = isFull ? 0n : token(bunch.baseId, "baseId"); if (!isFull && !base) fail("baseId must be nonzero for a delta"); const changes = bunch.changes || new Map(); const entries = changes instanceof Map ? [...changes.entries()] : Object.entries(changes).map(([k, v]) => [Number(k), v]); entries.sort((a, b) => a[0] - b[0]); const fieldValues = new Map(); for (const [field, value] of entries) { integer(field, "fieldId", 0, schema.fields.length - 1); if (fieldValues.has(field)) fail("duplicate field"); fieldValues.set(field, value); }
  const w = new Bits(); w.write(BigInt(objectId), 32); w.write(isFull ? 1n : 0n, 1); writeVarint(w, result); if (isFull) { for (const byte of schema.hash) w.write(BigInt(byte), 8); w.write(BigInt(schema.layoutVersion), 32); } else writeVarint(w, base); for (let i = 0; i < schema.fields.length; i++) w.write(fieldValues.has(i) ? 1n : 0n, 1); for (const [field, value] of fieldValues) schema.fields[field].type === "collection" ? collection(w, schema.fields[field], value) : valueCodec(w, schema.fields[field], value); return w.finish();
}
/** Decode one standalone DeltaBunch body and reject malformed/trailing bits. */
export function decodeDeltaBunch(schema, body) {
  validateSchema(schema); const r = new Bits(asBytes(body)); const objectId = Number(r.read(32)); const isFull = r.bool(); const resultId = readVarint(r); if (!resultId) fail("resultId must be nonzero"); let baseId = 0n; if (isFull) { const hash = new Uint8Array(16); for (let i = 0; i < 16; i++) hash[i] = Number(r.read(8)); const layoutVersion = Number(r.read(32)); if (layoutVersion !== schema.layoutVersion || !hash.every((x, i) => x === schema.hash[i])) fail("schema mismatch"); } else { baseId = readVarint(r); if (!baseId) fail("baseId must be nonzero for a delta"); } const changed = []; for (let i = 0; i < schema.fields.length; i++) if (r.bool()) changed.push(i); const changes = new Map(); for (const field of changed) changes.set(field, schema.fields[field].type === "collection" ? collection(r, schema.fields[field], undefined, true) : valueCodec(r, schema.fields[field], undefined, true)); r.assertPadding(); return { objectId, isFull, resultId, baseId, changes };
}
/** Coalesce standalone bunches into a KIND_REP_DELTA body. */
export function encodeDeltaBunches(schema, bunches) { if (!Array.isArray(bunches) || bunches.length > MAX_BUNCHES) fail("too many bunches"); const out = []; byteVarint(out, bunches.length); for (const bunch of bunches) { const body = encodeDeltaBunch(schema, bunch); byteVarint(out, body.length); out.push(...body); } return Uint8Array.from(out); }
/** Decode an all-or-nothing KIND_REP_DELTA body. */
export function decodeDeltaBunches(schema, body) { const bytes = asBytes(body), state = { pos: 0 }, count = Number(readByteVarint(bytes, state)); if (count > MAX_BUNCHES) fail("too many bunches"); const out = []; for (let i = 0; i < count; i++) { const n = Number(readByteVarint(bytes, state)); if (n > bytes.length - state.pos) fail("truncated coalesced bunch"); out.push(decodeDeltaBunch(schema, bytes.slice(state.pos, state.pos += n))); } if (state.pos !== bytes.length) fail("trailing coalesced bytes"); return out; }
export function encodeRepAck(entries) { if (!Array.isArray(entries) || entries.length > MAX_ACKS) fail("too many ack entries"); const w = new Bits(); writeVarint(w, entries.length); for (const e of entries) { w.write(BigInt(integer(e.objectId, "objectId", 0, 0xffffffff)), 32); writeVarint(w, token(e.ackedResultId, "ackedResultId")); w.write(BigInt(integer(e.history, "history", 0, 0xffffffff)), 32); } return w.finish(); }
export function decodeRepAck(body) { const r = new Bits(asBytes(body)), n = Number(readVarint(r)); if (n > MAX_ACKS) fail("too many ack entries"); const entries = []; for (let i = 0; i < n; i++) entries.push({ objectId: Number(r.read(32)), ackedResultId: readVarint(r), history: Number(r.read(32)) }); r.assertPadding(); return entries; }

/** Browser receive state. Missing-base packets are not acknowledged. */
export class NetworkPeerSession {
  constructor(schema) { this.schema = validateSchema(schema); this._baselines = new Map(); this._acks = new Map(); }
  apply(body) { const bunch = decodeDeltaBunch(this.schema, body), current = this._baselines.get(bunch.objectId); if (current !== undefined && bunch.resultId <= current) return { status: "stale" }; if (!bunch.isFull && current !== bunch.baseId) return { status: "needs_full", objectId: bunch.objectId, expectedBase: current }; this._baselines.set(bunch.objectId, bunch.resultId); const ack = this._acks.get(bunch.objectId); if (!ack) this._acks.set(bunch.objectId, { ackedResultId: bunch.resultId, history: 0 }); else if (bunch.resultId > ack.ackedResultId) { const distance = bunch.resultId - ack.ackedResultId; ack.history = distance > 32n ? 0 : (((ack.history << Number(distance)) | (1 << (Number(distance) - 1))) >>> 0); ack.ackedResultId = bunch.resultId; } return { status: "applied", bunch }; }
  baseline(objectId) { return this._baselines.get(objectId); }
  ackBody() { return encodeRepAck([...this._acks].map(([objectId, ack]) => ({ objectId, ...ack }))); }
  ackEnvelope() { return { kind: KIND_REP_ACK, body: this.ackBody() }; }
  applyEnvelope(envelope) { if (!envelope || envelope.kind !== KIND_REP_DELTA) fail("expected KIND_REP_DELTA"); return this.apply(envelope.body); }
}

/** Schema-bound transactional authoring helper. */
export class NetworkPeerAuthor {
  constructor(schema) { this.schema = validateSchema(schema); }
  full(objectId, resultId, changes = new Map()) { return encodeDeltaBunch(this.schema, { objectId, isFull: true, resultId, changes }); }
  delta(objectId, resultId, baseId, changes = new Map()) { return encodeDeltaBunch(this.schema, { objectId, isFull: false, resultId, baseId, changes }); }
}
