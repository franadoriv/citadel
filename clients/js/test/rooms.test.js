import { test } from "node:test";
import assert from "node:assert/strict";

import { CitadelClient } from "../src/client.js";
import { Envelope } from "../src/envelope.js";
import {
  KIND_ROOM_CREATE,
  KIND_ROOM_JOIN,
  KIND_ROOM_JOINED,
  KIND_ROOM_LEAVE,
  KIND_ROOM_MAP_READY,
  encodeRoomId,
} from "../src/protocol.js";

class FakeWebSocket {
  constructor() { this.readyState = 1; this.listeners = new Map(); this.sent = []; }
  addEventListener(kind, handler) { this.listeners.set(kind, handler); }
  send(data) { this.sent.push(data); }
  close() { this.readyState = 3; }
}

function joined(roomId, map, mode) {
  const enc = new TextEncoder(); const mapBytes = enc.encode(map); const modeBytes = enc.encode(mode);
  const body = new Uint8Array(8 + 2 + mapBytes.length + 2 + modeBytes.length);
  const view = new DataView(body.buffer);
  view.setBigUint64(0, roomId, false); view.setUint16(8, mapBytes.length, false);
  body.set(mapBytes, 10); view.setUint16(10 + mapBytes.length, modeBytes.length, false);
  body.set(modeBytes, 12 + mapBytes.length);
  return body;
}

test("room methods frame reliable requests and update lifecycle state", () => {
  const ws = new FakeWebSocket();
  const client = new CitadelClient(ws);
  client.joinOrCreateRoom("lobby");
  client.joinRoom(4n);
  client.leaveRoom(4n);
  client.sendMapReady(4n);
  const kinds = ws.sent.map((frame) => new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint16(4, false));
  assert.deepEqual(kinds, [KIND_ROOM_CREATE, KIND_ROOM_JOIN, KIND_ROOM_LEAVE, KIND_ROOM_MAP_READY]);

  const events = [];
  client.onRoomJoined((room) => events.push(room));
  client._dispatch(new Envelope(KIND_ROOM_JOINED, joined(4n, "arena", "duel")));
  assert.deepEqual(client.currentRoom, { roomId: 4n, map: "arena", mode: "duel" });
  assert.equal(events.length, 1);
  client._dispatch(new Envelope(KIND_ROOM_LEAVE, encodeRoomId(4n)));
  assert.equal(client.currentRoom, null);
});
