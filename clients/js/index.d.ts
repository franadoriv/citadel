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
  /** Explicit source-code opt-in for bounded, metadata-only lag diagnostics. */
  diagnostics?: DiagnosticsOptions;
}

/** Debug-only recorder setting. It is never read from URL, storage, or server controls. */
export interface LagRecorderDebugOptions {
  enabled?: boolean;
}

export interface DiagnosticsOptions {
  lagRecorder?: LagRecorderDebugOptions;
}

/** Request delivery over a reliable stream or (where available) a datagram. */
export interface SendOptions {
  reliable?: boolean;
}

/** Browser WebTransport certificate descriptor for a SHA-256 certificate pin. */
export interface WebTransportCertificateHash {
  algorithm: "sha-256";
  value: Uint8Array | ArrayBuffer;
}

export interface WebTransportConnectOptions {
  /** Injectable browser constructor for tests or non-global runtimes. */
  WebTransport?: typeof WebTransport;
  timeoutMs?: number;
  /** Native browser certificate descriptors. Omit for a CA-trusted production certificate. */
  serverCertificateHashes?: WebTransportCertificateHash[];
  /** Base64 SHA-256 development-certificate hash printed by Citadel at startup. */
  serverCertificateHashBase64?: string;
  /** Explicit source-code opt-in for bounded, metadata-only lag diagnostics. */
  diagnostics?: DiagnosticsOptions;
}

export interface AutoConnectOptions {
  /** Default true: use WebSocket only if WebTransport is unavailable or fails before ready. */
  fallbackToWebSocket?: boolean;
  /** Applied to both transports unless that transport has its own diagnostics option. */
  diagnostics?: DiagnosticsOptions;
  webTransport?: WebTransportConnectOptions;
  webSocket?: ConnectOptions;
}

export interface AutoConnectEndpoints {
  webTransportUrl?: string;
  webSocketUrl?: string;
}

/** Convert Citadel's logged base64 development hash into a WebTransport pin. */
export function webTransportCertificateHash(base64: string): WebTransportCertificateHash;

export interface AuthResult {
  status: number;
  userId: string;
  reasonClass: number;
}

export type EnvelopeHandler = (payload: Uint8Array, env: Envelope) => void;
export type AnyHandler = (env: Envelope) => void;

export type ChatChannelType = "direct" | "group" | "room";
/** Unsigned 64-bit JSON integer. Safe values remain numbers; larger values decode as bigint. */
export type ChatU64 = number | bigint;

export interface ChatPresence {
  presence_id: string;
  user_id: string;
}

export interface ChatMessage {
  id: ChatU64;
  sender: string;
  content: string;
  created_at_unix_ms: ChatU64;
  updated_at_unix_ms: ChatU64;
  revision: ChatU64;
  last_event_id: ChatU64;
  deleted: boolean;
}

interface ChatEventBase {
  readonly version: 1;
  channel_id: string;
  [field: string]: unknown;
}

export interface ChatPresenceJoinEvent extends ChatEventBase {
  type: "presence.join";
  channel_type: ChatChannelType;
  presence: ChatPresence;
}
export interface ChatPresenceLeaveEvent extends ChatEventBase {
  type: "presence.leave";
  presence: ChatPresence;
}
export interface ChatTypingEvent extends ChatEventBase {
  type: "typing";
  presence: ChatPresence;
  typing: boolean;
  expires_at: ChatU64;
}
export interface ChatMessageCreateEvent extends ChatEventBase {
  type: "message.create";
  event_id: ChatU64;
  message: ChatMessage;
}
export interface ChatMessageUpdateEvent extends ChatEventBase {
  type: "message.update";
  event_id: ChatU64;
  message: ChatMessage;
}
export interface ChatMessageRemoveEvent extends ChatEventBase {
  type: "message.remove";
  event_id: ChatU64;
  message: ChatMessage;
}
export interface ChatAccessRevokedEvent extends ChatEventBase {
  type: "access.revoked";
  presence: ChatPresence;
}
export interface ChatResyncRequiredEvent extends ChatEventBase {
  type: "resync_required";
  watermark_event_id: ChatU64;
  scopes?: string[];
}

/** Closed, version-1 discriminated union decoded from KIND_CHAT_EVENT. */
export type ChatEvent =
  | ChatPresenceJoinEvent
  | ChatPresenceLeaveEvent
  | ChatTypingEvent
  | ChatMessageCreateEvent
  | ChatMessageUpdateEvent
  | ChatMessageRemoveEvent
  | ChatAccessRevokedEvent
  | ChatResyncRequiredEvent;

export function decodeChatEvent(body: Uint8Array | ArrayBuffer | string): ChatEvent | null;

export type ChatCursorState = "live" | "rejoin_required" | "reconcile_required" | "ack_required" | "revoked";
export type ChatEventDisposition =
  | { type: "apply"; event_id: ChatU64 }
  | { type: "duplicate"; event_id: ChatU64 }
  | { type: "reconcile_gap"; current_watermark: ChatU64; observed_event_id: ChatU64 }
  | { type: "resync_required"; watermark_event_id: ChatU64 }
  | { type: "typing"; presence: ChatPresence; typing: boolean; expires_at: ChatU64 }
  | { type: "access_revoked"; presence: ChatPresence }
  | { type: "ephemeral" };

export class ChatEventCursor {
  constructor(channelId: string, watermark?: ChatU64);
  readonly channelId: string;
  readonly watermark: ChatU64;
  readonly state: ChatCursorState;
  readonly requiredWatermark: ChatU64 | null;
  disconnected(): void;
  rejoined(joinResult: Pick<ChatJoinResult, "channel_id" | "watermark_event_id">): void;
  observe(event: ChatEvent, now?: ChatU64): ChatEventDisposition;
  expireTyping(now?: ChatU64): Array<ChatPresence & { typing: false }>;
}

export type ChatTarget =
  | { kind: "direct"; other_user_id: string }
  | { kind: "group"; group_id: ChatU64 }
  | { kind: "room" };

export interface ChatJoinResult {
  channel_id: string;
  channel_type: ChatChannelType;
  presence: ChatPresence[];
  watermark_event_id: ChatU64;
  subscription: string;
}
export interface ChatTypingResult {
  typing: boolean;
  expires_at: ChatU64;
}
export interface ChatMutationResult { message: ChatMessage; event_id: ChatU64; }
export interface ChatRemoveResult { message_id: ChatU64; deleted: true; event_id: ChatU64; }
export interface ChatHistoryResult { items: ChatMessage[]; watermark_event_id: ChatU64; }
/** Complete reconciliation snapshot. Replace application state atomically on every callback. */
export interface ChatHistorySnapshot {
  readonly messages: readonly ChatMessage[];
  readonly watermark_event_id: ChatU64;
  readonly replace: true;
  /** Monotonically increasing within one reconcileChat call, including watermark retries. */
  readonly generation: number;
}
export interface ChatHistoryOptions {
  limit?: number;
  beforeMessageId?: ChatU64;
  timeoutMs?: number;
}
export interface ChatReconcileOptions extends Omit<ChatHistoryOptions, "beforeMessageId"> {
  maxAttempts?: number;
}

/** A connected Citadel realtime client. WebSocket is reliable-only; WebTransport adds datagrams. */
export class CitadelClient {
  readonly closed: boolean;
  readonly isOpen: boolean;
  readonly transportKind: "websocket" | "webtransport";
  currentRoom: RoomInfo | null;
  static connect(url: string, opts?: ConnectOptions): Promise<CitadelClient>;
  static connectWebTransport(url: string, opts?: WebTransportConnectOptions): Promise<CitadelClient>;
  static connectAuto(endpoints: AutoConnectEndpoints, opts?: AutoConnectOptions): Promise<CitadelClient>;
  handshakeGuest(opts?: { timeoutMs?: number }): Promise<AuthResult>;
  handshakeToken(token: string, opts?: { timeoutMs?: number }): Promise<AuthResult>;
  send(kind: number, body?: Uint8Array, opts?: SendOptions): void;
  sendEnvelope(env: Envelope, opts?: SendOptions): void;
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
  joinChat(target: ChatTarget, opts?: { timeoutMs?: number }): Promise<ChatJoinResult>;
  leaveChat(channelId: string, opts?: { timeoutMs?: number }): Promise<{ left: boolean }>;
  sendChatMessage(channelId: string, content: string, opts?: { timeoutMs?: number }): Promise<ChatMutationResult>;
  getChatHistory(channelId: string, options?: ChatHistoryOptions): Promise<ChatHistoryResult>;
  editChatMessage(channelId: string, messageId: ChatU64, content: string, opts?: { timeoutMs?: number }): Promise<ChatMutationResult>;
  deleteChatMessage(channelId: string, messageId: ChatU64, opts?: { timeoutMs?: number }): Promise<ChatRemoveResult>;
  moderateChatMessage(channelId: string, messageId: ChatU64, opts?: { timeoutMs?: number }): Promise<ChatRemoveResult>;
  setChatTyping(channelId: string, typing: boolean, opts?: { timeoutMs?: number }): Promise<ChatTypingResult>;
  rejoinChat(target: ChatTarget, cursor: ChatEventCursor, opts?: { timeoutMs?: number }): Promise<ChatJoinResult>;
  reconcileChat(
    cursor: ChatEventCursor,
    applyHistory: (snapshot: ChatHistorySnapshot) => void | Promise<void>,
    options?: ChatReconcileOptions,
  ): Promise<ChatHistoryResult>;
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

// --- NetworkPeer browser helpers ---------------------------------------------

export type NetworkPeerFieldCodec =
  | { type: "bool" }
  | { type: "int"; min: number; max: number }
  | { type: "scalar"; min: number; max: number; valuesPerUnit: number }
  | { type: "vector3"; min: number; max: number; valuesPerUnit: number }
  | { type: "quat"; bits: 9 | 10 | 15 }
  | { type: "bytes"; maxLen: number }
  | { type: "collection"; item: Exclude<NetworkPeerFieldCodec, { type: "collection" }>; maxItems: number };
export interface NetworkPeerSchema { hash: Uint8Array; layoutVersion: number; fields: NetworkPeerFieldCodec[]; }
export interface RepId { index: number; generation: number; }
export interface CollectionDelta { removed: RepId[]; added: Array<{ id: RepId; key: bigint; value: unknown }>; changed: Array<{ id: RepId; key: bigint; value: unknown }>; }
export interface DeltaBunch { objectId: number; isFull: boolean; resultId: bigint; baseId?: bigint; changes?: Map<number, unknown> | Record<number, unknown>; }
export function encodeDeltaBunch(schema: NetworkPeerSchema, bunch: DeltaBunch): Uint8Array;
export function decodeDeltaBunch(schema: NetworkPeerSchema, body: Uint8Array): DeltaBunch & { baseId: bigint; changes: Map<number, unknown> };
export function encodeDeltaBunches(schema: NetworkPeerSchema, bunches: DeltaBunch[]): Uint8Array;
export function decodeDeltaBunches(schema: NetworkPeerSchema, body: Uint8Array): Array<ReturnType<typeof decodeDeltaBunch>>;
export function encodeRepAck(entries: Array<{ objectId: number; ackedResultId: bigint; history: number }>): Uint8Array;
export function decodeRepAck(body: Uint8Array): Array<{ objectId: number; ackedResultId: bigint; history: number }>;
export class NetworkPeerAuthor {
  constructor(schema: NetworkPeerSchema);
  full(objectId: number, resultId: bigint, changes?: DeltaBunch["changes"]): Uint8Array;
  delta(objectId: number, resultId: bigint, baseId: bigint, changes?: DeltaBunch["changes"]): Uint8Array;
}
export class NetworkPeerSession {
  constructor(schema: NetworkPeerSchema);
  apply(body: Uint8Array): { status: "applied"; bunch: ReturnType<typeof decodeDeltaBunch> } | { status: "stale" } | { status: "needs_full"; objectId: number; expectedBase?: bigint };
  applyEnvelope(envelope: Envelope): ReturnType<NetworkPeerSession["apply"]>;
  baseline(objectId: number): bigint | undefined;
  ackBody(): Uint8Array;
  ackEnvelope(): Envelope;
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
export const KIND_TSYNC_V2_HELLO: number;
export const KIND_TSYNC_V2_SNAPSHOT: number;
export const KIND_TSYNC_V2_INPUT: number;
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
export const KIND_DIAG_SERVER_TIME: number;
export const KIND_DIAG_CAPABILITIES: number;
export const KIND_DIAG_CLOCK_SYNC: number;
export const KIND_DIAG_START: number;
export const KIND_DIAG_FLUSH: number;
export const KIND_DIAG_STATUS: number;

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
export const TSYNC_V2_CLOCK_BYTES: number;

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
export function decodeTsyncV2Snapshot(body: Uint8Array): {
  epoch: bigint; tick: bigint; tickHz: number; snapshotBody: Uint8Array;
} | null;
export class TsyncV2EpochFence {
  readonly epoch: bigint | null;
  apply<T>(body: Uint8Array, decodeV1Snapshot: (body: Uint8Array) => T | null): {
    clock: { epoch: bigint; tick: bigint; tickHz: number }; snapshot: T;
  } | null;
  reset(epoch: bigint | number): boolean;
}
