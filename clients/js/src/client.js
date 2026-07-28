// Browser/Node WebSocket client for Citadel's realtime transport.
//
// Pure network/state logic (no rendering), the JS peer of the Rust
// `citadel-client` crate's `WsClient`: connect, present the guest handshake,
// send/receive framed envelopes, dispatch by kind, and await correlated RPC
// replies. Works with the global `WebSocket` (browsers, Node >= 22) or an
// injected implementation via `opts.WebSocket`.

import { Envelope, FrameDecoder } from "./envelope.js";
import {
  KIND_AUTH,
  KIND_AUTH_RESULT,
  KIND_CHAT_EVENT,
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

/**
 * A connected Citadel WebSocket client. Reliable, ordered delivery only.
 *
 * Construct with the async {@link CitadelClient.connect} factory rather than
 * `new`.
 */
export class CitadelClient {
  /**
   * @param {WebSocket} ws An OPEN WebSocket with `binaryType = "arraybuffer"`.
   * @private
   */
  constructor(ws) {
    /** @type {WebSocket} @private */
    this._ws = ws;
    /** @private */
    this._decoder = new FrameDecoder();
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

    ws.binaryType = "arraybuffer";
    ws.addEventListener("message", (ev) => {
      let envelopes;
      try {
        envelopes = this._decoder.push(ev.data);
      } catch (err) {
        this._failAll(err);
        try { ws.close(); } catch { /* already closing */ }
        return;
      }
      for (const env of envelopes) this._dispatch(env);
    });
    ws.addEventListener("close", () => {
      this.closed = true;
      this._failAll(new Error("connection closed"));
    });
  }

  /**
   * Connect to a Citadel WebSocket endpoint, e.g. `ws://127.0.0.1:7352/`.
   *
   * @param {string} url
   * @param {{ WebSocket?: typeof WebSocket, timeoutMs?: number }} [opts]
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
        resolve(new CitadelClient(ws));
      }, { once: true });
      ws.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new Error(`failed to connect to ${url}`));
      }, { once: true });
    });
  }

  /** Whether the socket is open. */
  get isOpen() {
    return !this.closed && this._ws.readyState === 1 /* OPEN */;
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
   * Send a raw envelope of `kind` carrying `body`.
   * @param {number} kind
   * @param {Uint8Array} [body]
   */
  send(kind, body = EMPTY) {
    this.sendEnvelope(new Envelope(kind, body));
  }

  /**
   * Send a pre-built {@link Envelope}.
   * @param {Envelope} env
   */
  sendEnvelope(env) {
    if (this.closed) throw new Error("cannot send on a closed client");
    this._ws.send(env.encodeFramed());
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
    const decoder = new TextDecoder();
    return this.on(KIND_CHAT_EVENT, (payload) => {
      try {
        const event = JSON.parse(decoder.decode(payload));
        if (event && typeof event === "object") handler(event);
      } catch {
        // The generic `on(KIND_CHAT_EVENT, ...)` route remains available for
        // diagnostics when a peer violates the JSON event contract.
      }
    });
  }

  /**
   * Set this connection's ephemeral typing state for an already-joined chat
   * channel. Receivers clear a true indication at the server-provided
   * `expires_at` timestamp; call with `false` when input is abandoned.
   *
   * @param {string} channelId
   * @param {boolean} typing
   * @param {{ timeoutMs?: number }} [opts]
   * @returns {Promise<{typing: boolean, expires_at: number}>}
   */
  async setChatTyping(channelId, typing, opts = {}) {
    const payload = new TextEncoder().encode(JSON.stringify({ channel_id: channelId, typing }));
    const response = await this.callRpc("chat.typing", payload, opts);
    return JSON.parse(new TextDecoder().decode(response));
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
    try { this._ws.close(code, reason); } catch { /* already closing */ }
  }

  /**
   * @param {Envelope} env
   * @private
   */
  _dispatch(env) {
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
