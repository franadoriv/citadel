// Citadel web demo: move a cube, send its position to the Citadel WebSocket
// transport as a framed binary envelope. The realtime gateway relays it to
// OTHER connected clients, so with two browser tabs open each sees the other
// player's cube move (no self-echo).
//
// Wire format must match `crates/citadel-wire`:
//   framed = u32 BE body_len | u16 BE kind | payload
//   body_len counts (kind + payload), i.e. 2 + payload.len()
// We send KIND_POSITION (1). The gateway relays it to peers as
// KIND_PEER_POSITION (2) with the body prefixed by an 8-byte big-endian sender
// session id, followed by our original payload.
//
// The position payload is 3 little-endian f32 (x, y, z) + 1 little-endian f64
// timestamp (used for round-trip timing of our own relayed messages). Other web
// clients share this payload; native clients use a different payload, so cross
// web/native rendering is out of scope for this demo.
//
// No credentials are embedded; the endpoint is read from the input field or a
// `?endpoint=` URL query parameter. This demo is local-only.

import * as THREE from "https://unpkg.com/three@0.160.0/build/three.module.js";

const KIND_POSITION = 1;
const KIND_PEER_POSITION = 2;
const SENDER_ID_BYTES = 8;

// --- Wire codec (mirrors citadel-wire framed encoding) ---------------------

function encodeFramed(kind, payload) {
  const bodyLen = 2 + payload.length; // u16 kind + payload
  const buf = new ArrayBuffer(4 + bodyLen);
  const view = new DataView(buf);
  view.setUint32(0, bodyLen, false); // big-endian length prefix
  view.setUint16(4, kind, false); // big-endian kind
  new Uint8Array(buf, 6).set(payload);
  return buf;
}

// Bare datagram encoding (no length prefix): u16 BE kind | payload.
// Matches citadel-wire's `encode_datagram`; one envelope per datagram.
function encodeDatagram(kind, payload) {
  const buf = new ArrayBuffer(2 + payload.length);
  const view = new DataView(buf);
  view.setUint16(0, kind, false);
  new Uint8Array(buf, 2).set(payload);
  return new Uint8Array(buf);
}

// Decode all complete framed envelopes from a growing byte buffer.
// Returns { envelopes: [{kind, payload}], rest: Uint8Array }.
function decodeFramed(bytes) {
  const envelopes = [];
  let offset = 0;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  while (bytes.length - offset >= 4) {
    const bodyLen = view.getUint32(offset, false);
    if (bytes.length - offset < 4 + bodyLen) break; // incomplete frame
    const kind = view.getUint16(offset + 4, false);
    const payload = bytes.slice(offset + 6, offset + 4 + bodyLen);
    envelopes.push({ kind, payload });
    offset += 4 + bodyLen;
  }
  return { envelopes, rest: bytes.slice(offset) };
}

function encodePosition(x, y, z, t) {
  // 3 f32 coords + 1 f64 timestamp for round-trip measurement.
  const buf = new ArrayBuffer(3 * 4 + 8);
  const view = new DataView(buf);
  view.setFloat32(0, x, true);
  view.setFloat32(4, y, true);
  view.setFloat32(8, z, true);
  view.setFloat64(12, t, true);
  return new Uint8Array(buf);
}

function decodePosition(payload) {
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  return {
    x: view.getFloat32(0, true),
    y: view.getFloat32(4, true),
    z: view.getFloat32(8, true),
    t: view.getFloat64(12, true),
  };
}

// --- Scene -----------------------------------------------------------------

const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 100);
camera.position.set(0, 4, 9);
camera.lookAt(0, 0, 0);

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setSize(window.innerWidth, window.innerHeight);
renderer.setPixelRatio(window.devicePixelRatio);
document.body.appendChild(renderer.domElement);

scene.add(new THREE.GridHelper(20, 20, 0x2a3140, 0x1a1f29));
const light = new THREE.DirectionalLight(0xffffff, 1.1);
light.position.set(5, 10, 7);
scene.add(light);
scene.add(new THREE.AmbientLight(0x404040));

const localCube = new THREE.Mesh(
  new THREE.BoxGeometry(1, 1, 1),
  new THREE.MeshStandardMaterial({ color: 0x2a6df4 }),
);
localCube.position.set(0, 0.5, 0);
scene.add(localCube);

// One green cube per other player, keyed by relayed sender session id.
const peerCubes = new Map();

function peerCube(senderId) {
  let cube = peerCubes.get(senderId);
  if (!cube) {
    cube = new THREE.Mesh(
      new THREE.BoxGeometry(1, 1, 1),
      new THREE.MeshStandardMaterial({ color: 0x5bd66f, transparent: true, opacity: 0.7 }),
    );
    cube.position.set(0, 0.5, 0);
    scene.add(cube);
    peerCubes.set(senderId, cube);
  }
  return cube;
}

window.addEventListener("resize", () => {
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
});

// --- Input -----------------------------------------------------------------

const keys = new Set();
window.addEventListener("keydown", (e) => keys.add(e.key.toLowerCase()));
window.addEventListener("keyup", (e) => keys.delete(e.key.toLowerCase()));

let dragging = false;
renderer.domElement.addEventListener("mousedown", () => (dragging = true));
window.addEventListener("mouseup", () => (dragging = false));
window.addEventListener("mousemove", (e) => {
  if (!dragging) return;
  localCube.position.x += e.movementX * 0.02;
  localCube.position.z += e.movementY * 0.02;
});

function applyKeyboard(dt) {
  const speed = 5 * dt;
  if (keys.has("w") || keys.has("arrowup")) localCube.position.z -= speed;
  if (keys.has("s") || keys.has("arrowdown")) localCube.position.z += speed;
  if (keys.has("a") || keys.has("arrowleft")) localCube.position.x -= speed;
  if (keys.has("d") || keys.has("arrowright")) localCube.position.x += speed;
  const clamp = (v) => Math.max(-9, Math.min(9, v));
  localCube.position.x = clamp(localCube.position.x);
  localCube.position.z = clamp(localCube.position.z);
}

// --- Networking ------------------------------------------------------------

const statusEl = document.getElementById("status");
const sentEl = document.getElementById("sent");
const echoedEl = document.getElementById("echoed");
const rttEl = document.getElementById("rtt");
const endpointEl = document.getElementById("endpoint");
const wtUrlEl = document.getElementById("wtUrl");
const wtHashEl = document.getElementById("wtHash");
const transportEl = document.getElementById("transport");
const connectBtn = document.getElementById("connect");

const query = new URLSearchParams(window.location.search);
if (query.get("endpoint")) endpointEl.value = query.get("endpoint");
if (query.get("wt")) wtUrlEl.value = query.get("wt");
if (query.get("wtHash")) wtHashEl.value = query.get("wtHash");

let conn = null; // active transport: { send(bytes), close() }
let sentCount = 0;
let peerMsgCount = 0;

function setStatus(text, ok) {
  statusEl.textContent = text;
  statusEl.className = ok ? "ok" : "bad";
}

// Read the 8-byte big-endian sender id prefix from a relayed body.
function splitSender(payload) {
  if (payload.length < SENDER_ID_BYTES) return null;
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  // Session ids fit comfortably in 53-bit JS numbers for a demo.
  const hi = view.getUint32(0, false);
  const lo = view.getUint32(4, false);
  const senderId = hi * 0x100000000 + lo;
  return { senderId, rest: payload.slice(SENDER_ID_BYTES) };
}

// Apply one inbound relayed peer-position envelope to the scene.
function applyEnvelope(env) {
  if (env.kind !== KIND_PEER_POSITION) return;
  const split = splitSender(env.payload);
  if (!split) return;
  const p = decodePosition(split.rest);
  peerCube(split.senderId).position.set(p.x, 0.5, p.z);
  peerMsgCount += 1;
  echoedEl.textContent = String(peerMsgCount);
  rttEl.textContent = String(peerCubes.size);
}

// Decode base64 (browser cert hash) to a Uint8Array.
function b64ToBytes(b64) {
  const bin = atob(b64.trim());
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) out[i] = bin.charCodeAt(i);
  return out;
}

// --- WebTransport (preferred) ----------------------------------------------

async function connectWebTransport() {
  if (typeof WebTransport === "undefined") return null; // not supported
  const url = wtUrlEl.value.trim();
  const hashB64 = wtHashEl.value.trim();
  if (!url || !hashB64) return null; // need a cert hash for the dev cert
  let wt;
  try {
    wt = new WebTransport(url, {
      serverCertificateHashes: [{ algorithm: "sha-256", value: b64ToBytes(hashB64) }],
    });
    await wt.ready;
  } catch (err) {
    return null; // fall back to WebSocket
  }

  // Read relayed datagrams (the gateway relays positions as datagrams).
  (async () => {
    try {
      const reader = wt.datagrams.readable.getReader();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        // Datagram payload is one envelope: u16 kind + payload.
        const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
        const kind = view.getUint16(0, false);
        const payload = value.slice(2);
        applyEnvelope({ kind, payload });
      }
    } catch (_e) {
      setStatus("disconnected", false);
    }
  })();

  const writer = wt.datagrams.writable.getWriter();
  setStatus("connected", true);
  transportEl.textContent = "WebTransport (datagrams)";
  return {
    send: (bytes) => {
      // WebTransport datagrams carry one bare envelope (u16 kind + payload),
      // not the length-framed form used by streams/WebSocket.
      writer.write(bytes).catch(() => {});
    },
    close: () => wt.close(),
    framed: false,
  };
}

// --- WebSocket (fallback) --------------------------------------------------

function connectWebSocket() {
  let recvBuf = new Uint8Array(0);
  let ws;
  try {
    ws = new WebSocket(endpointEl.value);
  } catch (err) {
    setStatus("invalid endpoint", false);
    return null;
  }
  ws.binaryType = "arraybuffer";
  ws.onopen = () => {
    setStatus("connected", true);
    transportEl.textContent = "WebSocket (fallback)";
  };
  ws.onclose = () => setStatus("disconnected", false);
  ws.onerror = () => setStatus("error", false);
  ws.onmessage = (ev) => {
    const incoming = new Uint8Array(ev.data);
    const merged = new Uint8Array(recvBuf.length + incoming.length);
    merged.set(recvBuf);
    merged.set(incoming, recvBuf.length);
    const { envelopes, rest } = decodeFramed(merged);
    recvBuf = rest;
    for (const env of envelopes) applyEnvelope(env);
  };
  return {
    send: (bytes) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(bytes);
    },
    close: () => ws.close(),
    framed: true,
  };
}

async function connect() {
  if (conn) conn.close();
  conn = null;
  setStatus("connecting ...", false);
  transportEl.textContent = "-";
  // Try WebTransport first; fall back to WebSocket.
  conn = await connectWebTransport();
  if (!conn) conn = connectWebSocket();
}

connectBtn.addEventListener("click", () => {
  connect();
});

let lastSend = 0;
function maybeSend(now) {
  if (!conn) return;
  // Throttle to ~30 Hz.
  if (now - lastSend < 33) return;
  lastSend = now;
  const payload = encodePosition(localCube.position.x, 0.5, localCube.position.z, now);
  // Streams/WebSocket use the length-framed encoding; WebTransport datagrams
  // carry the bare datagram encoding (one envelope per datagram).
  const bytes = conn.framed
    ? new Uint8Array(encodeFramed(KIND_POSITION, payload))
    : encodeDatagram(KIND_POSITION, payload);
  conn.send(bytes);
  sentCount += 1;
  sentEl.textContent = String(sentCount);
}

// --- Loop ------------------------------------------------------------------

let prev = performance.now();
function frame(now) {
  const dt = Math.min(0.05, (now - prev) / 1000);
  prev = now;
  applyKeyboard(dt);
  maybeSend(now);
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
