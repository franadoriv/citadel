// TypeScript declarations for @citadel/client (hand-written; no build step).

// --- Envelope + framing ------------------------------------------------------

export const LENGTH_PREFIX_BYTES: number;
export const KIND_BYTES: number;
export const MAX_FRAME_BODY_BYTES: number;

/** A wire-agnostic realtime envelope: a numeric kind and an opaque body. */
export class Envelope {
  kind: number;
  body: Uint8Array;
  constructor(kind: number, body?: Uint8Array);
  datagramLen(): number;
  framedLen(): number;
  encodeFramed(): Uint8Array;
  encodeDatagram(): Uint8Array;
}

/** Decode a bare datagram body into an {@link Envelope}. */
export function decodeDatagram(data: Uint8Array): Envelope;

/** Stateful decoder for a stream of length-delimited frames. */
export class FrameDecoder {
  push(chunk: ArrayBuffer | Uint8Array): Envelope[];
  readonly buffered: number;
}

// --- Client ------------------------------------------------------------------

export interface ConnectOptions {
  WebSocket?: typeof WebSocket;
  timeoutMs?: number;
}

export interface AuthResult {
  status: number;
  userId: string;
  reasonClass: number;
}

export type EnvelopeHandler = (payload: Uint8Array, env: Envelope) => void;
export type AnyHandler = (env: Envelope) => void;

/** Decoded JSON body of a `KIND_CHAT_EVENT` envelope. */
export interface ChatEvent {
  version: number;
  type: string;
  channel_id: string;
  event_id?: number;
  watermark_event_id?: number;
  [field: string]: unknown;
}

/** A connected Citadel WebSocket client (reliable, ordered delivery). */
export class CitadelClient {
  readonly closed: boolean;
  readonly isOpen: boolean;
  currentRoom: RoomInfo | null;
  static connect(url: string, opts?: ConnectOptions): Promise<CitadelClient>;
  handshakeGuest(opts?: { timeoutMs?: number }): Promise<AuthResult>;
  send(kind: number, body?: Uint8Array): void;
  sendEnvelope(env: Envelope): void;
  on(kind: number, handler: EnvelopeHandler): () => void;
  off(kind: number, handler: EnvelopeHandler): void;
  onAny(handler: AnyHandler): () => void;
  waitForKind(kind: number, timeoutMs?: number): Promise<Uint8Array>;
  joinOrCreateRoom(name: string): void;
  joinRoom(roomId: bigint | number): void;
  leaveRoom(roomId: bigint | number): void;
  sendMapReady(roomId: bigint | number): void;
  onRoomJoined(handler: (room: RoomInfo) => void): () => void;
  onRoomLeft(handler: (roomId: bigint) => void): () => void;
  onChatEvent(handler: (event: ChatEvent) => void): () => void;
  callRpc(method: string, payload?: Uint8Array, opts?: { timeoutMs?: number }): Promise<Uint8Array>;
  close(code?: number, reason?: string): void;
}

/** A sanitized error returned by a Citadel player HTTP endpoint. */
export class HttpApiError extends Error {
  status: number;
  code: string;
}

export interface PublicProfile {
  user_id: string;
  username: string;
  display_name?: string;
}

export interface SessionTokenPair {
  token: string;
  refresh_token?: string;
  user_id: string;
  username: string;
  created: boolean;
}

/** Email/password credentials for explicit account registration or sign-in. Do not log this value. */
export interface EmailAuthenticationRequest {
  email: string;
  password: string;
  create?: boolean;
  username?: string;
}

/** Typed HTTP wrapper for account, known-player, refresh, and logout routes. */
export class CitadelHttpClient {
  constructor(baseUrl: string, opts?: { fetch?: typeof fetch });
  authenticateEmail(request: EmailAuthenticationRequest): Promise<SessionTokenPair>;
  getAccount(accessToken: string): Promise<PublicProfile>;
  updateAccount(accessToken: string, patch: { username?: string; display_name?: string | null }): Promise<PublicProfile>;
  lookupUsers(accessToken: string, query: { user_ids?: string[]; usernames?: string[] }): Promise<{ users: PublicProfile[] }>;
  refreshSession(refreshToken: string): Promise<SessionTokenPair>;
  logoutSession(tokens?: { accessToken?: string; refreshToken?: string }): Promise<void>;
}

export interface RoomInfo {
  roomId: bigint;
  map: string;
  mode: string;
}

/** Error thrown when a server RPC handler answers with an error status. */
export class RpcError extends Error {
  requestId: bigint;
  serverMessage: string;
}

// --- Protocol constants + codecs ---------------------------------------------

export const EXPECTED_ABI_VERSION: number;

export const KIND_POSITION: number;
export const KIND_PEER_POSITION: number;
export const KIND_RPC_REQUEST: number;
export const KIND_RPC_RESPONSE: number;
export const KIND_AUTH: number;
export const KIND_AUTH_RESULT: number;
export const KIND_TSYNC_HELLO: number;
export const KIND_TSYNC_SNAPSHOT: number;
export const KIND_TSYNC_INPUT: number;
export const KIND_TSYNC_ACK: number;
export const KIND_TSYNC_ROLE: number;
export const KIND_TSYNC_REWIND: number;
export const KIND_REP_DELTA: number;
export const KIND_REP_ACK: number;
export const KIND_REP_SCHEMA: number;
export const KIND_NA_PRESENCE: number;
export const KIND_NA_SPAWN: number;
export const KIND_NA_SPAWN_BATCH: number;
export const KIND_NA_DESPAWN: number;
export const KIND_NA_STATE: number;
export const KIND_ROOM_CREATE: number;
export const KIND_ROOM_JOIN: number;
export const KIND_ROOM_JOINED: number;
export const KIND_ROOM_LEAVE: number;
export const KIND_ROOM_MAP_READY: number;
export const KIND_MATCHMAKER_MATCHED: number;
export const KIND_NOTIFICATION: number;
export const KIND_CHAT_EVENT: number;

export const TSYNC_KIND_MIN: number;
export const TSYNC_KIND_MAX: number;
export const REP_KIND_MIN: number;
export const REP_KIND_MAX: number;
export const NA_KIND_MIN: number;
export const NA_KIND_MAX: number;
export const ROOM_KIND_MIN: number;
export const ROOM_KIND_MAX: number;
export const MATCHMAKER_KIND_MIN: number;
export const MATCHMAKER_KIND_MAX: number;
export const NOTIFICATION_KIND_MIN: number;
export const NOTIFICATION_KIND_MAX: number;
export const CHAT_KIND_MIN: number;
export const CHAT_KIND_MAX: number;

export const AUTH_STATUS_AUTHENTICATED: number;
export const AUTH_STATUS_GUEST: number;
export const AUTH_STATUS_REJECTED: number;
export const AUTH_REASON_AUTH_FAILED: number;
export const AUTH_REASON_AUTH_REQUIRED: number;
export const AUTH_REASON_PROTOCOL: number;

export const RPC_STATUS_OK: number;
export const RPC_STATUS_ERROR: number;
export const RPC_REQUEST_ID_BYTES: number;
export const RPC_METHOD_LEN_BYTES: number;

export const SENDER_ID_BYTES: number;
export const POSITION_BYTES: number;
export const SCHEMA_HASH_BYTES: number;
export const ACK_HISTORY_BITS: number;

export const EMPTY: Uint8Array;

export function encodeRpcRequest(
  requestId: bigint | number,
  method: string,
  payload?: Uint8Array,
): Uint8Array;

export function decodeRpcResponse(
  body: Uint8Array,
): { requestId: bigint; status: number; payload: Uint8Array } | null;

export function decodeAuthResult(
  body: Uint8Array,
): { status: number; userId: string; reasonClass: number } | null;

export function splitSender(body: Uint8Array): [bigint, Uint8Array] | null;

export function tagWithSender(senderId: bigint | number, payload?: Uint8Array): Uint8Array;
export function encodeRoomCreate(name: string): Uint8Array;
export function encodeRoomId(roomId: bigint | number): Uint8Array;
export function decodeRoomJoined(body: Uint8Array): RoomInfo | null;
export function decodeRoomId(body: Uint8Array): bigint | null;
