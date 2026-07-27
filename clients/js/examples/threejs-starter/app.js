// A minimal browser-game integration: @citadel/client owns WebSocket framing
// and dispatch; Three.js owns the scene, input, and visual interpolation.
//
// Serve `clients/js/` as the static root so this source import works both in a
// checkout and after `make bin-client-js` stages the SDK. See README.md.

import * as THREE from "https://unpkg.com/three@0.160.0/build/three.module.js";
import {
  CitadelClient,
  KIND_PEER_POSITION,
  KIND_POSITION,
  splitSender,
} from "../../src/index.js";

const ENDPOINT = new URLSearchParams(window.location.search).get("endpoint")
  ?? "ws://127.0.0.1:7352/";
const SEND_INTERVAL_MS = 50;
const PEER_STALE_MS = 5_000;
const POSITION_BYTES = 20; // x/y/z LE f32 + send time LE f64; game-owned layout.
const status = document.querySelector("#status");

const scene = new THREE.Scene();
scene.background = new THREE.Color(0x0b0e14);

const camera = new THREE.PerspectiveCamera(60, innerWidth / innerHeight, 0.1, 100);
camera.position.set(0, 12, 14);
camera.lookAt(0, 0, 0);

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setSize(innerWidth, innerHeight);
renderer.setPixelRatio(devicePixelRatio);
document.body.append(renderer.domElement);

scene.add(new THREE.GridHelper(20, 20, 0x44506c, 0x1d2638));
scene.add(new THREE.HemisphereLight(0xc9dcff, 0x172033, 2.5));
const keyLight = new THREE.DirectionalLight(0xffffff, 2);
keyLight.position.set(5, 10, 5);
scene.add(keyLight);

function avatar(color) {
  const mesh = new THREE.Mesh(
    new THREE.BoxGeometry(1, 1, 1),
    new THREE.MeshStandardMaterial({ color }),
  );
  mesh.position.y = 0.5;
  scene.add(mesh);
  return mesh;
}

// This is local prediction: input moves the blue visual immediately. A real
// authoritative game later reconciles it against server state (see the
// Knights vs Monsters tutorial); the default relay only forwards bytes.
const localPlayer = avatar(0x3788ff);
const keys = new Set();
const peers = new Map(); // bigint -> { mesh, target, lastUpdate }
let client;
let lastSentAt = 0;
let previousFrameAt = performance.now();

function encodePosition(position, sentAt) {
  const body = new Uint8Array(POSITION_BYTES);
  const view = new DataView(body.buffer);
  view.setFloat32(0, position.x, true);
  view.setFloat32(4, position.y, true);
  view.setFloat32(8, position.z, true);
  view.setFloat64(12, sentAt, true);
  return body;
}

function decodePosition(body) {
  if (body.byteLength !== POSITION_BYTES) return null;
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  return new THREE.Vector3(
    view.getFloat32(0, true),
    view.getFloat32(4, true),
    view.getFloat32(8, true),
  );
}

function receivePeerPosition(body) {
  const tagged = splitSender(body);
  if (!tagged) return;
  const [senderId, payload] = tagged;
  const position = decodePosition(payload);
  if (!position) return;

  let peer = peers.get(senderId);
  if (!peer) {
    peer = { mesh: avatar(0x54d67b), target: position.clone(), lastUpdate: 0 };
    peer.mesh.position.copy(position);
    peers.set(senderId, peer);
  }
  peer.target.copy(position);
  peer.lastUpdate = performance.now();
}

function updateLocalPrediction(dt) {
  const input = new THREE.Vector3(
    Number(keys.has("d") || keys.has("arrowright")) - Number(keys.has("a") || keys.has("arrowleft")),
    0,
    Number(keys.has("s") || keys.has("arrowdown")) - Number(keys.has("w") || keys.has("arrowup")),
  );
  if (input.lengthSq() === 0) return;
  input.normalize().multiplyScalar(6 * dt);
  localPlayer.position.add(input);
  localPlayer.position.x = THREE.MathUtils.clamp(localPlayer.position.x, -9.5, 9.5);
  localPlayer.position.z = THREE.MathUtils.clamp(localPlayer.position.z, -9.5, 9.5);
}

function updateRemoteVisuals(now, dt) {
  for (const [senderId, peer] of peers) {
    // Render smoothing stays entirely visual; it never changes the packet data.
    peer.mesh.position.lerp(peer.target, Math.min(1, dt * 12));
    if (now - peer.lastUpdate > PEER_STALE_MS) {
      scene.remove(peer.mesh);
      peers.delete(senderId);
    }
  }
}

function maybeSendPosition(now) {
  if (!client?.isOpen || now - lastSentAt < SEND_INTERVAL_MS) return;
  lastSentAt = now;
  client.send(KIND_POSITION, encodePosition(localPlayer.position, now));
}

function frame(now) {
  const dt = Math.min(0.05, (now - previousFrameAt) / 1_000);
  previousFrameAt = now;
  updateLocalPrediction(dt);
  maybeSendPosition(now);
  updateRemoteVisuals(now, dt);
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}

window.addEventListener("keydown", (event) => keys.add(event.key.toLowerCase()));
window.addEventListener("keyup", (event) => keys.delete(event.key.toLowerCase()));
window.addEventListener("resize", () => {
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(innerWidth, innerHeight);
});

async function connect() {
  try {
    client = await CitadelClient.connect(ENDPOINT);
    client.on(KIND_PEER_POSITION, receivePeerPosition);
    await client.handshakeGuest();
    status.textContent = `connected to ${ENDPOINT}`;
    status.classList.add("connected");
  } catch (error) {
    status.textContent = `connection failed: ${error instanceof Error ? error.message : String(error)}`;
  }
}

await connect();
requestAnimationFrame(frame);
