// Browser/Node realtime client for Citadel's WebSocket and WebTransport paths.
//
// Pure network/state logic (no rendering), the JS peer of the Rust
// `citadel-client` crate's transport client: connect, present the guest
// handshake, send/receive envelopes, dispatch by kind, and await correlated
// RPC replies. WebSocket works in browsers and Node >= 22; WebTransport is a
// Chromium browser path with an injectable implementation for tests.

import { Envelope } from "./envelope.js";
import { ChatEventCursor, decodeChatEvent } from "./chat.js";
import { equalU64, isU64, parseChatJsonText, stringifyChatJson } from "./chat-json.js";
import {
  abortChatHistoryApply,
  abortChatRequest,
  acceptChatAck,
  acceptChatHistory,
  beginChatAck,
  beginChatHistory,
  completeChatHistoryApply,
} from "./chat-internal.js";
import {
  WebSocketTransport,
  WebTransportTransport,
  webTransportCertificateHash,
} from "./transport.js";
import {
  LagRecorder,
  DIAG_DELIVERY_DATAGRAM,
  DIAG_DELIVERY_RELIABLE,
  DIAG_DIRECTION_OUTBOUND,
  decodeDiagClockSyncResponse,
  decodeDiagFlush,
  decodeDiagServerTime,
  decodeDiagStart,
} from "./lag-recorder.js";
import {
  KIND_AUTH,
  KIND_AUTH_RESULT,
  KIND_CHAT_EVENT,
  KIND_DIAG_CAPABILITIES,
  KIND_DIAG_CLOCK_SYNC,
  KIND_DIAG_FLUSH,
  KIND_DIAG_SERVER_TIME,
  KIND_DIAG_START,
  KIND_DIAG_STATUS,
  KIND_ROOM_CREATE,
  KIND_ROOM_JOIN,
  KIND_ROOM_JOINED,
  KIND_ROOM_LEAVE,
  KIND_ROOM_MAP_READY,
  KIND_RPC_REQUEST,
  KIND_RPC_RESPONSE,
  RPC_STATUS_OK,
  decodeAuthResult,
  decodeRpcResponse,
  encodeRpcRequest,
  encodeRoomCreate,
  encodeRoomId,
  decodeRoomJoined,
  decodeRoomId,
  EMPTY,
} from "./protocol.js";

/** Default timeout (ms) for handshake / RPC awaits. */
const DEFAULT_TIMEOUT_MS = 10_000;
const CHAT_ENCODER = new TextEncoder();
const CHAT_DECODER = new TextDecoder("utf-8", { fatal: true });

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function isSafeUint(value, positive = false) {
  return Number.isSafeInteger(value) && value >= (positive ? 1 : 0);
}

function diagnosticUploadOrigin(realtimeUrl) {
  try {
    const url = new URL(realtimeUrl);
    if (url.protocol === "ws:") url.protocol = "http:";
    else if (url.protocol === "wss:") url.protocol = "https:";
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return url.origin;
  } catch { return null; }
}

function lagRecorderEnabled(options) {
  return options?.diagnostics?.lagRecorder?.enabled === true;
}

function assertChannelId(channelId) {
  if (!isNonEmptyString(channelId)) throw new TypeError("channelId must be a non-empty string");
}

function assertMessageId(messageId) {
  if (!isU64(messageId, true)) throw new TypeError("messageId must be a positive unsigned 64-bit integer");
}

function validPresence(value) {
  return isObject(value) && isNonEmptyString(value.presence_id) && isNonEmptyString(value.user_id);
}

function validMessage(value) {
  return isObject(value)
    && isU64(value.id, true)
    && isNonEmptyString(value.sender)
    && typeof value.content === "string"
    && isU64(value.created_at_unix_ms)
    && isU64(value.updated_at_unix_ms)
    && value.updated_at_unix_ms >= value.created_at_unix_ms
    && isU64(value.revision, true)
    && isU64(value.last_event_id, true)
    && typeof value.deleted === "boolean"
    && (!value.deleted || value.content.length === 0);
}

function validHistoryResult(value) {
  return Array.isArray(value.items) && value.items.every(validMessage)
    && isU64(value.watermark_event_id);
}

function parseChatJson(body, method) {
  try {
    const value = parseChatJsonText(CHAT_DECODER.decode(body));
    if (!isObject(value)) throw new Error();
    return value;
  } catch {
    throw new Error(`malformed ${method} response`);
  }
}

function validateChatTarget(target) {
  if (!isObject(target)) throw new TypeError("chat target must be an object");
  if (target.kind === "direct" && isNonEmptyString(target.other_user_id)) return;
  if (target.kind === "group" && isU64(target.group_id, true)) return;
  if (target.kind === "room") return;
  throw new TypeError("invalid chat target");
}

/** @param {Promise<unknown>} promise @param {number} timeoutMs @param {() => void} onTimeout */
function waitWithTimeout(promise, timeoutMs, onTimeout) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      onTimeout();
      reject(new Error(`connect timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    Promise.resolve(promise).then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });
}

/**
 * A connected Citadel realtime client. WebSocket delivery is always reliable;
 * WebTransport callers can choose reliable streams or unreliable datagrams per
 * message.
 *
 * Construct with the async {@link CitadelClient.connect} factory rather than
 * `new`.
 */
export class CitadelClient {
  /**
   * @param {WebSocket | WebSocketTransport | WebTransportTransport} transport
   * @private
   */
  constructor(transport, options = {}) {
    /** @type {WebSocketTransport | WebTransportTransport} @private */
    this._transport = transport?.kind && typeof transport.setHandlers === "function"
      ? transport
      : new WebSocketTransport(transport);
    /** @type {"websocket" | "webtransport"} */
    this.transportKind = this._transport.kind;
    /** @type {Map<number, Set<Function>>} @private */
    this._handlers = new Map();
    /** @type {Set<Function>} @private */
    this._anyHandlers = new Set();
    /** @type {Map<string, { resolve: Function, reject: Function, timer: any }>} @private */
    this._pendingRpc = new Map();
    /** @type {Array<{ kind: number, resolve: Function, reject: Function, timer: any }>} @private */
    this._waiters = [];
    /** @type {bigint} @private */
    this._nextRequestId = 1n;
    /** @type {boolean} */
    this.closed = false;
    /** @type {{ roomId: bigint, map: string, mode: string } | null} */
    this.currentRoom = null;
    /** @type {Set<(room: { roomId: bigint, map: string, mode: string }) => void>} @private */
    this._roomJoinedHandlers = new Set();
    /** @type {Set<(roomId: bigint) => void>} @private */
    this._roomLeftHandlers = new Set();
    /** @type {boolean} @private */
    this._diagnosticsAuthenticated = false;
    /** @type {LagRecorder | null} @private */
    this._lagRecorder = lagRecorderEnabled(options) && typeof options._diagnosticUploadOrigin === "string"
      ? new LagRecorder({
        sendStatus: (body) => this.send(KIND_DIAG_STATUS, body),
        sendClockSync: (body) => this.send(KIND_DIAG_CLOCK_SYNC, body),
        uploadOrigin: options._diagnosticUploadOrigin,
      })
      : null;

    this._transport.setHandlers({
      onEnvelope: (env) => this._dispatch(env),
      onDiagnosticEnvelope: (env, flags) => this._recordDiagnosticEnvelope(env, flags),
      onClose: (error) => {
        this.closed = true;
        this._lagRecorder?.cancel();
        this._failAll(error);
      },
    });
  }

  /**
   * Connect to a Citadel WebSocket endpoint, e.g. `ws://127.0.0.1:7352/`.
   *
   * @param {string} url
   * @param {{ WebSocket?: typeof WebSocket, timeoutMs?: number, diagnostics?: { lagRecorder?: { enabled?: boolean } } }} [opts]
   * @returns {Promise<CitadelClient>}
   */
  static connect(url, opts = {}) {
    const WS = opts.WebSocket || globalThis.WebSocket;
    if (!WS) {
      return Promise.reject(new Error("no WebSocket implementation; pass opts.WebSocket"));
    }
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    return new Promise((resolve, reject) => {
      let ws;
      try {
        ws = new WS(url);
      } catch (err) {
        reject(err);
        return;
      }
      ws.binaryType = "arraybuffer";
      const timer = setTimeout(() => {
        try { ws.close(); } catch { /* noop */ }
        reject(new Error(`connect timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      ws.addEventListener("open", () => {
        clearTimeout(timer);
        resolve(new CitadelClient(ws, { diagnostics: opts.diagnostics, _diagnosticUploadOrigin: diagnosticUploadOrigin(url) }));
      }, { once: true });
      ws.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new Error(`failed to connect to ${url}`));
      }, { once: true });
    });
  }

  /**
   * Connect to a Chromium WebTransport endpoint. Reliable Citadel envelopes
   * use fresh unidirectional streams; callers opt into bare unreliable
   * datagrams with `send(kind, body, { reliable: false })`.
   *
   * `serverCertificateHashBase64` is the development hash logged by Citadel's
   * WebTransport listener. Omit it for a CA-trusted production certificate.
   *
   * @param {string} url
   * @param {{ WebTransport?: typeof WebTransport, timeoutMs?: number, serverCertificateHashes?: Array<{ algorithm: "sha-256", value: BufferSource }>, serverCertificateHashBase64?: string, diagnostics?: { lagRecorder?: { enabled?: boolean } }, [key: string]: unknown }} [opts]
   * @returns {Promise<CitadelClient>}
   */
  static async connectWebTransport(url, opts = {}) {
    const {
      WebTransport: WebTransportImpl = globalThis.WebTransport,
      timeoutMs = DEFAULT_TIMEOUT_MS,
      serverCertificateHashBase64,
      serverCertificateHashes,
      ...init
    } = opts;
    if (!WebTransportImpl) {
      throw new Error("WebTransport is unavailable in this runtime");
    }
    if (serverCertificateHashBase64 && serverCertificateHashes) {
      throw new TypeError("provide either serverCertificateHashBase64 or serverCertificateHashes, not both");
    }
    if (serverCertificateHashBase64) {
      init.serverCertificateHashes = [webTransportCertificateHash(serverCertificateHashBase64)];
    } else if (serverCertificateHashes) {
      init.serverCertificateHashes = serverCertificateHashes;
    }

    let webTransport;
    try {
      webTransport = new WebTransportImpl(url, init);
    } catch (error) {
      throw error instanceof Error ? error : new Error("failed to construct WebTransport");
    }
    try {
      await waitWithTimeout(Promise.resolve(webTransport.ready), timeoutMs, () => {
        try { webTransport.close(); } catch { /* noop */ }
      });
    } catch (error) {
      try { webTransport.close(); } catch { /* noop */ }
      throw error;
    }
    return new CitadelClient(new WebTransportTransport(webTransport), {
      diagnostics: opts.diagnostics,
      _diagnosticUploadOrigin: diagnosticUploadOrigin(url),
    });
  }

  /**
   * Prefer WebTransport and fall back to an explicitly supplied WebSocket URL
   * if capability detection or the pre-ready WebTransport connection fails.
   * The endpoints are separate because production proxies need not map one URL
   * to the other. The SDK never migrates a connected/authenticated client.
   *
   * @param {{ webTransportUrl?: string, webSocketUrl?: string }} endpoints
   * @param {{ fallbackToWebSocket?: boolean, diagnostics?: { lagRecorder?: { enabled?: boolean } }, webTransport?: object, webSocket?: ConnectOptions }} [opts]
   * @returns {Promise<CitadelClient>}
   */
  static async connectAuto(endpoints, opts = {}) {
    const { webTransportUrl, webSocketUrl } = endpoints || {};
    const {
      fallbackToWebSocket = true,
      diagnostics,
      webTransport: webTransportOptions = {},
      webSocket: webSocketOptions = {},
    } = opts;

    let webTransportError;
    if (webTransportUrl) {
      try {
        return await CitadelClient.connectWebTransport(webTransportUrl, {
          ...webTransportOptions,
          diagnostics: webTransportOptions.diagnostics ?? diagnostics,
        });
      } catch (error) {
        webTransportError = error;
      }
    }
    if (webSocketUrl && (!webTransportUrl || fallbackToWebSocket)) {
      return CitadelClient.connect(webSocketUrl, {
        ...webSocketOptions,
        diagnostics: webSocketOptions.diagnostics ?? diagnostics,
      });
    }
    if (webTransportError) throw webTransportError;
    throw new TypeError("connectAuto requires webTransportUrl, webSocketUrl, or both");
  }

  /** Whether the selected realtime transport is open. */
  get isOpen() {
    return !this.closed && this._transport.isOpen;
  }

  /**
   * Present the guest handshake (empty `KIND_AUTH`) and await the
   * `KIND_AUTH_RESULT` ack that registers this connection server-side.
   *
   * @param {{ timeoutMs?: number }} [opts]
   * @returns {Promise<{ status: number, userId: string, reasonClass: number }>}
   */
  async handshakeGuest(opts = {}) {
    this.send(KIND_AUTH, EMPTY);
    const ack = await this.waitForKind(KIND_AUTH_RESULT, opts.timeoutMs);
    const result = decodeAuthResult(ack);
    if (!result) throw new Error("malformed auth result");
    return result;
  }

  /**
   * Present the token handshake and await the `KIND_AUTH_RESULT` ack that binds
   * this connection to the token's account. The account/session peer of
   * {@link handshakeGuest}: the `KIND_AUTH` body is the UTF-8 encoding of
   * `token` (an empty body is the guest handshake, so pass a non-empty token
   * and use {@link handshakeGuest} for anonymous sessions).
   *
   * @param {string} token Opaque session-token string (UTF-8 encoded on the wire).
   * @param {{ timeoutMs?: number }} [opts]
   * @returns {Promise<{ status: number, userId: string, reasonClass: number }>}
   */
  async handshakeToken(token, opts = {}) {
    if (typeof token !== "string" || token.length === 0) {
      throw new TypeError("handshakeToken requires a non-empty token string; use handshakeGuest for anonymous sessions");
    }
    this.send(KIND_AUTH, new TextEncoder().encode(token));
    const ack = await this.waitForKind(KIND_AUTH_RESULT, opts.timeoutMs);
    const result = decodeAuthResult(ack);
    if (!result) throw new Error("malformed auth result");
    return result;
  }

  /**
   * Send a raw envelope of `kind` carrying `body`.
   * @param {number} kind
   * @param {Uint8Array} [body]
   */
  send(kind, body = EMPTY, opts = {}) {
    this.sendEnvelope(new Envelope(kind, body), opts);
  }

  /**
   * Send a pre-built {@link Envelope}.
   * @param {Envelope} env
   */
  sendEnvelope(env, opts = {}) {
    if (this.closed) throw new Error("cannot send on a closed client");
    const reliable = opts.reliable ?? true;
    this._lagRecorder?.record(env.kind, env.body,
      DIAG_DIRECTION_OUTBOUND | (reliable ? DIAG_DELIVERY_RELIABLE : DIAG_DELIVERY_DATAGRAM));
    const pending = this._transport.send(env, reliable);
    if (pending && typeof pending.then === "function") {
      void pending.catch((error) => this._transport.fail(error));
    }
  }

  /**
   * Register a handler for envelopes of `kind`. The handler receives the raw
   * payload bytes and the full envelope. Returns an unsubscribe function.
   *
   * @param {number} kind
   * @param {(payload: Uint8Array, env: Envelope) => void} handler
   * @returns {() => void}
   */
  on(kind, handler) {
    let set = this._handlers.get(kind);
    if (!set) { set = new Set(); this._handlers.set(kind, set); }
    set.add(handler);
    return () => this.off(kind, handler);
  }

  /**
   * Remove a handler previously registered with {@link on}.
   * @param {number} kind
   * @param {Function} handler
   */
  off(kind, handler) {
    this._handlers.get(kind)?.delete(handler);
  }

  /**
   * Register a handler that receives EVERY inbound envelope (except correlated
   * RPC responses, which resolve their {@link callRpc} promise). Returns an
   * unsubscribe function.
   *
   * @param {(env: Envelope) => void} handler
   * @returns {() => void}
   */
  onAny(handler) {
    this._anyHandlers.add(handler);
    return () => this._anyHandlers.delete(handler);
  }

  /**
   * Resolve with the payload of the next envelope of `kind`.
   * @param {number} kind
   * @param {number} [timeoutMs]
   * @returns {Promise<Uint8Array>}
   */
  waitForKind(kind, timeoutMs = DEFAULT_TIMEOUT_MS) {
    return new Promise((resolve, reject) => {
      const waiter = { kind, resolve, reject, timer: null };
      waiter.timer = setTimeout(() => {
        const i = this._waiters.indexOf(waiter);
        if (i >= 0) this._waiters.splice(i, 1);
        reject(new Error(`timed out waiting for kind ${kind} after ${timeoutMs}ms`));
      }, timeoutMs);
      this._waiters.push(waiter);
    });
  }

  /** Create or join a named room. The server later supplies the map in `onRoomJoined`. */
  joinOrCreateRoom(name) { this.send(KIND_ROOM_CREATE, encodeRoomCreate(name)); }

  /** Request admission to an existing room. */
  joinRoom(roomId) { this.send(KIND_ROOM_JOIN, encodeRoomId(roomId)); }

  /** Leave a room. */
  leaveRoom(roomId) { this.send(KIND_ROOM_LEAVE, encodeRoomId(roomId)); }

  /** Acknowledge that the server-selected map has finished loading. */
  sendMapReady(roomId) { this.send(KIND_ROOM_MAP_READY, encodeRoomId(roomId)); }

  /** Subscribe to authoritative room membership/map events. */
  onRoomJoined(handler) {
    this._roomJoinedHandlers.add(handler);
    return () => this._roomJoinedHandlers.delete(handler);
  }

  /** Subscribe to room leave/removal notifications. */
  onRoomLeft(handler) {
    this._roomLeftHandlers.add(handler);
    return () => this._roomLeftHandlers.delete(handler);
  }

  /**
   * Subscribe to decoded local chat presence, typing, and durable-mutation
   * events. Events are at-least-once; callers deduplicate durable mutations by
   * `(channel_id, event_id)`, expire `typing` at `expires_at`, and reconcile
   * `resync_required` with history.
   * Malformed server payloads are ignored rather than crashing the socket loop.
   *
   * @param {(event: object) => void} handler
   * @returns {() => void}
   */
  onChatEvent(handler) {
    return this.on(KIND_CHAT_EVENT, (payload) => {
      const event = decodeChatEvent(payload);
      if (event !== null) handler(event);
    });
  }

  async joinChat(target, opts = {}) {
    validateChatTarget(target);
    return this._callChatJson("chat.join", { target }, (value) => {
      if (!isNonEmptyString(value.channel_id)
        || !["direct", "group", "room"].includes(value.channel_type)
        || !Array.isArray(value.presence) || value.presence.some((entry) => !validPresence(entry))
        || !isU64(value.watermark_event_id)
        || !isNonEmptyString(value.subscription)) return false;
      return true;
    }, opts);
  }

  async leaveChat(channelId, opts = {}) {
    assertChannelId(channelId);
    return this._callChatJson("chat.leave", { channel_id: channelId },
      (value) => typeof value.left === "boolean", opts);
  }

  async sendChatMessage(channelId, content, opts = {}) {
    assertChannelId(channelId);
    if (typeof content !== "string") throw new TypeError("content must be a string");
    return this._callChatJson("chat.send", { channel_id: channelId, content },
      (value) => isU64(value.event_id, true) && validMessage(value.message)
        && value.message.last_event_id === value.event_id, opts);
  }

  async getChatHistory(channelId, options = {}) {
    assertChannelId(channelId);
    const { limit, beforeMessageId, timeoutMs } = options;
    if (Object.hasOwn(options, "acknowledgeWatermark")) {
      throw new TypeError("acknowledgeWatermark is not supported by ordinary history");
    }
    const request = { channel_id: channelId };
    if (limit !== undefined) {
      if (!isSafeUint(limit, true) || limit > 200) {
        throw new TypeError("limit must be a safe positive integer no greater than 200");
      }
      request.limit = limit;
    }
    if (beforeMessageId !== undefined) {
      assertMessageId(beforeMessageId);
      request.before_message_id = beforeMessageId;
    }

    return this._callChatJson("chat.history", request, (value) => {
      if (!validHistoryResult(value) || value.items.length > (limit ?? 50)) return false;
      if (!value.items.every((message, index) => index === 0 || value.items[index - 1].id > message.id)) {
        return false;
      }
      return beforeMessageId === undefined || value.items.every(({ id }) => id < beforeMessageId);
    }, { timeoutMs });
  }

  async editChatMessage(channelId, messageId, content, opts = {}) {
    assertChannelId(channelId);
    assertMessageId(messageId);
    if (typeof content !== "string") throw new TypeError("content must be a string");
    return this._callChatJson("chat.edit", {
      channel_id: channelId, message_id: messageId, content,
    }, (value) => isU64(value.event_id, true) && validMessage(value.message)
      && equalU64(value.message.id, messageId) && value.message.last_event_id === value.event_id, opts);
  }

  async deleteChatMessage(channelId, messageId, opts = {}) {
    return this._chatRemove("chat.delete", channelId, messageId, opts);
  }

  async moderateChatMessage(channelId, messageId, opts = {}) {
    return this._chatRemove("chat.moderate", channelId, messageId, opts);
  }

  /**
   * Set this connection's ephemeral typing state for an already-joined channel.
   */
  async setChatTyping(channelId, typing, opts = {}) {
    assertChannelId(channelId);
    if (typeof typing !== "boolean") throw new TypeError("typing must be a boolean");
    return this._callChatJson("chat.typing", { channel_id: channelId, typing },
      (value) => typeof value.typing === "boolean" && isU64(value.expires_at), opts);
  }

  /** Rejoin a cursor's channel after reconnect without advancing its watermark. */
  async rejoinChat(target, cursor, opts = {}) {
    if (!(cursor instanceof ChatEventCursor)) throw new TypeError("cursor must be a ChatEventCursor");
    const joined = await this.joinChat(target, opts);
    cursor.rejoined(joined);
    return joined;
  }

  /**
   * Reconcile and acknowledge a cursor. If the watermark races forward while
   * history is loading, retry a bounded number of times rather than falsely
   * treating an older acknowledgement as complete.
   */
  async reconcileChat(cursor, applyHistory, options = {}) {
    if (!(cursor instanceof ChatEventCursor)) throw new TypeError("cursor must be a ChatEventCursor");
    if (cursor.state !== "reconcile_required") throw new Error("chat cursor does not require reconciliation");
    if (typeof applyHistory !== "function") throw new TypeError("applyHistory callback is required");
    const { limit = 50, maxAttempts = 3, timeoutMs } = options;
    if (!isSafeUint(limit, true) || limit > 200) {
      throw new TypeError("limit must be a safe positive integer no greater than 200");
    }
    if (!isSafeUint(maxAttempts, true) || maxAttempts > 10) {
      throw new TypeError("maxAttempts must be a safe positive integer no greater than 10");
    }

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const items = [];
      let terminalHandle;
      let snapshotWatermark;
      let restart = false;

      while (!terminalHandle) {
        const handle = beginChatHistory(cursor, limit);
        const request = { channel_id: cursor.channelId, limit };
        if (handle.beforeMessageId !== null) request.before_message_id = handle.beforeMessageId;
        let page;
        let accepted;
        try {
          page = await this._callChatJson("chat.history", request, validHistoryResult, { timeoutMs });
          accepted = acceptChatHistory(cursor, handle, page);
        } catch (error) {
          abortChatRequest(cursor, handle);
          throw error;
        }
        if (accepted.restart) {
          restart = true;
          break;
        }
        items.push(...page.items);
        snapshotWatermark = accepted.watermark;
        if (accepted.terminal) terminalHandle = handle;
      }
      if (restart) continue;

      try {
        await applyHistory(Object.freeze({
          messages: Object.freeze([...items]),
          watermark_event_id: snapshotWatermark,
          replace: true,
          generation: attempt + 1,
        }));
        completeChatHistoryApply(cursor, terminalHandle);
      } catch (error) {
        abortChatHistoryApply(cursor, terminalHandle);
        throw error;
      }

      const ackHandle = beginChatAck(cursor);
      let acknowledgement;
      let acceptedAck;
      try {
        acknowledgement = await this._callChatJson("chat.history", {
          channel_id: cursor.channelId,
          limit: 1,
          acknowledge_watermark: ackHandle.watermark,
        }, validHistoryResult, { timeoutMs });
        acceptedAck = acceptChatAck(cursor, ackHandle, acknowledgement);
      } catch (error) {
        abortChatRequest(cursor, ackHandle);
        throw error;
      }
      if (acceptedAck.restart) continue;
      return { items, watermark_event_id: acknowledgement.watermark_event_id };
    }
    throw new Error("chat reconciliation watermark did not stabilize");
  }

  async _chatRemove(method, channelId, messageId, opts) {
    assertChannelId(channelId);
    assertMessageId(messageId);
    return this._callChatJson(method, { channel_id: channelId, message_id: messageId },
      (value) => equalU64(value.message_id, messageId) && value.deleted === true
        && isU64(value.event_id, true), opts);
  }

  async _callChatJson(method, request, validate, opts = {}) {
    const response = await this.callRpc(method, CHAT_ENCODER.encode(stringifyChatJson(request)), opts);
    const value = parseChatJson(response, method);
    if (!validate(value)) throw new Error(`malformed ${method} response`);
    return value;
  }

  /**
   * Call a server-side RPC method and await its correlated reply.
   *
   * Sends a `KIND_RPC_REQUEST` with a fresh `request_id`, then resolves with the
   * handler's reply bytes when the matching `KIND_RPC_RESPONSE` arrives, or
   * rejects with an {@link RpcError} if the server answered with an error status.
   *
   * @param {string} method
   * @param {Uint8Array} [payload]
   * @param {{ timeoutMs?: number }} [opts]
   * @returns {Promise<Uint8Array>}
   */
  callRpc(method, payload = EMPTY, opts = {}) {
    const requestId = this._nextRequestId;
    this._nextRequestId += 1n;
    const key = requestId.toString();
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingRpc.delete(key);
        reject(new Error(`rpc "${method}" (${key}) timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this._pendingRpc.set(key, { resolve, reject, timer });
      try {
        this.send(KIND_RPC_REQUEST, encodeRpcRequest(requestId, method, payload));
      } catch (err) {
        clearTimeout(timer);
        this._pendingRpc.delete(key);
        reject(err);
      }
    });
  }

  /** Close the connection. */
  close(code, reason) {
    this._lagRecorder?.cancel();
    this._transport.close(code, reason);
  }

  /**
   * @param {Envelope} env
   * @private
   */
  _dispatch(env) {
    if (env.kind === KIND_AUTH_RESULT) {
      const authResult = decodeAuthResult(env.body);
      this._diagnosticsAuthenticated = authResult !== null && authResult.status !== 2;
      this._lagRecorder?.setAuthenticated(this._diagnosticsAuthenticated);
    }
    if (this._handleDiagnosticsControl(env)) return;
    if (env.kind === KIND_ROOM_JOINED) {
      const room = decodeRoomJoined(env.body);
      if (room) {
        this.currentRoom = room;
        for (const cb of this._roomJoinedHandlers) cb(room);
      }
    } else if (env.kind === KIND_ROOM_LEAVE) {
      const roomId = decodeRoomId(env.body);
      if (roomId !== null) {
        if (this.currentRoom?.roomId === roomId) this.currentRoom = null;
        for (const cb of this._roomLeftHandlers) cb(roomId);
      }
    }
    if (env.kind === KIND_RPC_RESPONSE) {
      const res = decodeRpcResponse(env.body);
      if (res) {
        const key = res.requestId.toString();
        const pending = this._pendingRpc.get(key);
        if (pending) {
          this._pendingRpc.delete(key);
          clearTimeout(pending.timer);
          if (res.status === RPC_STATUS_OK) {
            pending.resolve(res.payload);
          } else {
            pending.reject(new RpcError(res.requestId, res.payload));
          }
          return;
        }
      }
      // Unknown/duplicate correlation id: fall through to generic handlers.
    }

    // One-shot waiters for this kind.
    if (this._waiters.length) {
      for (let i = this._waiters.length - 1; i >= 0; i--) {
        const w = this._waiters[i];
        if (w.kind === env.kind) {
          this._waiters.splice(i, 1);
          clearTimeout(w.timer);
          w.resolve(env.body);
        }
      }
    }

    const set = this._handlers.get(env.kind);
    if (set) for (const cb of set) cb(env.body, env);
    for (const cb of this._anyHandlers) cb(env);
  }

  /** @param {Envelope} env @param {number} flags @private */
  _recordDiagnosticEnvelope(env, flags) {
    this._lagRecorder?.record(env.kind, env.body, flags);
  }

  /** Keep reserved diagnostics frames out of gameplay handlers. @param {Envelope} env @private */
  _handleDiagnosticsControl(env) {
    if (env.kind < KIND_DIAG_SERVER_TIME || env.kind > KIND_DIAG_STATUS) return false;
    if (!this._lagRecorder || !this._diagnosticsAuthenticated) return true;
    if (env.kind === KIND_DIAG_SERVER_TIME) {
      const offer = decodeDiagServerTime(env.body);
      const capabilities = offer && this._lagRecorder.acceptServerTime(offer);
      if (capabilities) {
        try { this.send(KIND_DIAG_CAPABILITIES, capabilities); } catch { /* transport closed */ }
      }
      return true;
    }
    if (env.kind === KIND_DIAG_CLOCK_SYNC) {
      const response = decodeDiagClockSyncResponse(env.body);
      if (response) this._lagRecorder.acceptClockSync(response);
      return true;
    }
    if (env.kind === KIND_DIAG_START) {
      const start = decodeDiagStart(env.body);
      if (start) this._lagRecorder.start(start);
      return true;
    }
    if (env.kind === KIND_DIAG_FLUSH) {
      const flush = decodeDiagFlush(env.body);
      if (flush) void this._lagRecorder.upload(flush);
      return true;
    }
    return true;
  }

  /**
   * Reject every pending RPC / waiter with `err` (on close or fatal decode).
   * @param {Error} err
   * @private
   */
  _failAll(err) {
    for (const { reject, timer } of this._pendingRpc.values()) {
      clearTimeout(timer);
      reject(err);
    }
    this._pendingRpc.clear();
    for (const w of this._waiters) {
      clearTimeout(w.timer);
      w.reject(err);
    }
    this._waiters = [];
  }
}

/** Error thrown when a server RPC handler answers with an error status. */
export class RpcError extends Error {
  /**
   * @param {bigint} requestId
   * @param {Uint8Array} payload utf8 error message bytes.
   */
  constructor(requestId, payload) {
    const message = new TextDecoder().decode(payload);
    super(`rpc call ${requestId} failed: ${message}`);
    this.name = "RpcError";
    /** @type {bigint} */
    this.requestId = requestId;
    /** @type {string} */
    this.serverMessage = message;
  }
}
