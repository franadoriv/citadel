// Citadel JS/Web client SDK — public entry point.
//
// Zero-build ESM: import directly from source in the browser, Node (>= 22), or
// any bundler. Wire types stay in lockstep with the server via the Tier-A
// parity check over `src/protocol.js`.
//
// Quick start:
//
//   import { CitadelClient, KIND_POSITION } from "@citadel/client";
//   const client = await CitadelClient.connect("ws://127.0.0.1:7352/");
//   await client.handshakeGuest();
//   client.on(KIND_POSITION, (payload) => { ... });
//   const reply = await client.callRpc("ping");

export { CitadelClient, RpcError } from "./client.js";
export { webTransportCertificateHash } from "./transport.js";
export {
  NetworkPeerAuthor,
  NetworkPeerSession,
  encodeDeltaBunch,
  decodeDeltaBunch,
  encodeDeltaBunches,
  decodeDeltaBunches,
  encodeRepAck,
  decodeRepAck,
} from "./networkpeer.js";
export { CitadelHttpClient, HttpApiError } from "./http.js";
export {
  Envelope,
  FrameDecoder,
  decodeDatagram,
  LENGTH_PREFIX_BYTES,
  KIND_BYTES,
  MAX_FRAME_BODY_BYTES,
} from "./envelope.js";
export * from "./protocol.js";
