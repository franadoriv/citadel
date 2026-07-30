import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  NetworkPeerAuthor, NetworkPeerSession, decodeDeltaBunch, decodeDeltaBunches,
  decodeRepAck, encodeDeltaBunches,
} from "../src/networkpeer.js";

const fixture = JSON.parse(readFileSync(new URL("../../../tests/fixtures/networkpeer-cross-engine-v1.json", import.meta.url)));
const schema = {
  hash: Uint8Array.from(Buffer.from(fixture.schema.hash_hex, "hex")), layoutVersion: fixture.schema.layout_version,
  fields: [
    { type: "bool" },
    { type: "int", min: -10, max: 10 },
    { type: "scalar", min: -100, max: 100, valuesPerUnit: 10 },
    { type: "vector3", min: -100, max: 100, valuesPerUnit: 10 },
    { type: "quat", bits: 10 },
    { type: "bytes", maxLen: 32 },
    { type: "collection", item: { type: "int", min: 0, max: 100 }, maxItems: 4 },
  ],
};

const changes = new Map([
  [0, true], [1, 4], [2, 1.2], [3, [1, -2, 3]], [4, [0, 0, 0, 1]],
  [5, new TextEncoder().encode("citadel")],
  [6, { removed: [{ index: 1, generation: 2 }], added: [{ id: { index: 3, generation: 1 }, key: 8n, value: 7 }], changed: [{ id: { index: 4, generation: 1 }, key: 9n, value: 8 }] }],
]);

function assertClose(actual, expected, tolerance = 0.002) {
  assert.ok(Math.abs(actual - expected) <= tolerance, `${actual} is not within ${tolerance} of ${expected}`);
}

function assertGoldenChanges(actual, expected) {
  for (const [field, value] of Object.entries(expected)) {
    const decoded = actual.get(Number(field));
    if (field === "2") assertClose(decoded, value, 1e-9);
    else if (field === "4") decoded.forEach((component, i) => assertClose(component, value[i]));
    else if (field === "5") assert.equal(Buffer.from(decoded).toString("hex"), value.bytes_hex);
    else if (field === "6") {
      const normalizeItems = (items) => items.map(({ id, key, value: itemValue }) => ({ id, key: key?.toString(), value: itemValue }));
      assert.deepEqual(decoded.removed, value.removed);
      assert.deepEqual(normalizeItems(decoded.added), value.added);
      assert.deepEqual(normalizeItems(decoded.changed), value.changed);
    } else assert.deepEqual(decoded, value);
  }
}

test("NetworkPeer decodes canonical Rust semantic golden vectors", () => {
  assert.equal(fixture.fixture_version, 1);
  const vectors = fixture.golden_vectors;
  assert.equal(vectors.length, 4);
  for (const vector of vectors.filter(({ expected }) => expected)) {
    const decoded = decodeDeltaBunch(schema, Buffer.from(vector.encoded_hex, "hex"));
    assert.equal(decoded.objectId, vector.expected.object_id);
    assert.equal(decoded.isFull, vector.expected.is_full);
    assert.equal(decoded.resultId.toString(), vector.expected.result_id);
    assert.equal(decoded.baseId.toString(), vector.expected.base_id);
    assertGoldenChanges(decoded.changes, vector.expected.changes);
  }
  const overflow = vectors.find(({ id }) => id === "reject_rep_id_index_above_u32");
  assert.throws(() => decodeDeltaBunch(schema, Buffer.from(overflow.encoded_hex, "hex")), /rep id index must fit u32/);
  const generationOverflow = vectors.find(({ id }) => id === "reject_rep_id_generation_above_u32");
  assert.throws(() => decodeDeltaBunch(schema, Buffer.from(generationOverflow.encoded_hex, "hex")), /rep id generation must fit u32/);
});

test("NetworkPeer authors and decodes all browser value families from fixture v1", () => {
  const author = new NetworkPeerAuthor(schema);
  const body = author.full(9, 3n, changes);
  const decoded = decodeDeltaBunch(schema, body);
  assert.equal(decoded.objectId, 9);
  assert.equal(decoded.resultId, 3n);
  assert.equal(decoded.changes.get(1), 4);
  assert.deepEqual(decoded.changes.get(3), [1, -2, 3]);
  assert.deepEqual([...decoded.changes.get(5)], [...new TextEncoder().encode("citadel")]);
  assert.equal(decoded.changes.get(6).added[0].key, 8n);
});

test("NetworkPeer session acks accepted fulls and rejects stale/missing base", () => {
  const author = new NetworkPeerAuthor(schema), session = new NetworkPeerSession(schema);
  const full = author.full(9, 3n, changes);
  assert.equal(session.apply(full).status, "applied");
  assert.equal(session.apply(full).status, "stale");
  const missing = author.delta(9, 5n, 4n, new Map([[0, false]]));
  assert.deepEqual(session.apply(missing), { status: "needs_full", objectId: 9, expectedBase: 3n });
  const ack = decodeRepAck(session.ackBody());
  assert.equal(ack[0].objectId, 9);
  assert.equal(ack[0].ackedResultId, 3n);
});

test("NetworkPeer coalescing and malformed data fail closed", () => {
  const author = new NetworkPeerAuthor(schema);
  const body = encodeDeltaBunches(schema, [
    { objectId: 1, isFull: true, resultId: 1n, changes: new Map([[0, true]]) },
    { objectId: 2, isFull: true, resultId: 1n, changes: new Map([[1, 2]]) },
  ]);
  assert.equal(decodeDeltaBunches(schema, body).length, 2);
  assert.throws(() => author.full(1, 0n));
  assert.throws(() => author.full(1, 1n, new Map([[6, { removed: [{ index: 1, generation: 1 }], added: [{ id: { index: 1, generation: 1 }, key: 1n, value: 1 }], changed: [] }]])));
  assert.throws(() => decodeDeltaBunch(schema, new Uint8Array([1, 2])));
});
