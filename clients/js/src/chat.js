// Versioned, fail-closed client contract for KIND_CHAT_EVENT (28).

import { registerChatCursor } from "./chat-internal.js";
import { isNextU64, isU64, maxU64, parseChatJsonText } from "./chat-json.js";

const decoder = new TextDecoder("utf-8", { fatal: true });
const encoder = new TextEncoder();
const MAX_CHAT_CONTENT_BYTES = 2_048;
// Keep kind-28 decoding within the authenticated DeliverChat control payload
// boundary (server matchmaker_transport::MAX_FRAME_BYTES / 2). Generic realtime
// envelopes remain larger; chat rejects before UTF-8 decode or lexical scanning.
const MAX_CHAT_EVENT_BYTES = 32 * 1024;
const EVENT_TYPES = new Set([
  "presence.join",
  "presence.leave",
  "typing",
  "message.create",
  "message.update",
  "message.remove",
  "access.revoked",
  "resync_required",
]);
const DURABLE_TYPES = new Set(["message.create", "message.update", "message.remove"]);
const PRESENCE_TYPES = new Set(["presence.join", "presence.leave", "typing", "access.revoked"]);

function object(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function validChatContent(value) {
  return nonEmptyString(value)
    && encoder.encode(value).byteLength <= MAX_CHAT_CONTENT_BYTES
    && ![...value].some((character) => character !== "\n" && character !== "\r"
      && /\p{Cc}/u.test(character));
}

function exactKeys(value, keys) {
  if (!object(value)) return false;
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

// lossless-json preserves u64 values, but—like JSON.parse—accepts duplicate
// object names. Scan the lexical JSON first so duplicate names, including
// escaped aliases such as "type" and "t\u0079pe", fail closed at any depth.
function hasUniqueJsonObjectKeys(text) {
  let offset = 0;
  const skipWhitespace = () => {
    while (offset < text.length && /\s/u.test(text[offset])) offset += 1;
  };
  const parseString = () => {
    const start = offset;
    if (text[offset++] !== "\"") throw new Error("expected string");
    while (offset < text.length) {
      const current = text[offset++];
      if (current === "\"") return JSON.parse(text.slice(start, offset));
      if (current === "\\") {
        if (offset >= text.length) throw new Error("truncated escape");
        if (text[offset] === "u") offset += 5;
        else offset += 1;
      } else if (current.charCodeAt(0) < 0x20) {
        throw new Error("control character");
      }
    }
    throw new Error("unterminated string");
  };
  const parseValue = (depth) => {
    if (depth > 64) throw new Error("JSON nesting limit");
    skipWhitespace();
    if (text[offset] === "{") {
      offset += 1;
      skipWhitespace();
      const keys = new Set();
      if (text[offset] === "}") { offset += 1; return; }
      while (true) {
        skipWhitespace();
        const key = parseString();
        if (keys.has(key)) throw new Error("duplicate key");
        keys.add(key);
        skipWhitespace();
        if (text[offset++] !== ":") throw new Error("expected colon");
        parseValue(depth + 1);
        skipWhitespace();
        const separator = text[offset++];
        if (separator === "}") return;
        if (separator !== ",") throw new Error("expected comma");
      }
    }
    if (text[offset] === "[") {
      offset += 1;
      skipWhitespace();
      if (text[offset] === "]") { offset += 1; return; }
      while (true) {
        parseValue(depth + 1);
        skipWhitespace();
        const separator = text[offset++];
        if (separator === "]") return;
        if (separator !== ",") throw new Error("expected comma");
      }
    }
    if (text[offset] === "\"") { parseString(); return; }
    const token = text.slice(offset).match(/^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/u);
    if (!token) throw new Error("invalid JSON value");
    offset += token[0].length;
  };
  try {
    parseValue(0);
    skipWhitespace();
    return offset === text.length;
  } catch {
    return false;
  }
}

function validPresence(value) {
  return exactKeys(value, ["presence_id", "user_id"])
    && nonEmptyString(value.presence_id)
    && nonEmptyString(value.user_id);
}

function validMessage(value, type, eventId) {
  if (!exactKeys(value, ["id", "sender", "content", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id", "deleted"])
    || !isU64(value.id, true)
    || !nonEmptyString(value.sender)
    || typeof value.content !== "string"
    || !isU64(value.created_at_unix_ms)
    || !isU64(value.updated_at_unix_ms)
    || value.updated_at_unix_ms < value.created_at_unix_ms
    || !isU64(value.revision, true)
    || !isU64(value.last_event_id, true)
    || value.last_event_id !== eventId
    || typeof value.deleted !== "boolean") return false;

  if (type === "message.create") {
    return validChatContent(value.content) && !value.deleted && value.revision === 1
      && value.created_at_unix_ms === value.updated_at_unix_ms;
  }
  if (type === "message.update") {
    return validChatContent(value.content) && !value.deleted && value.revision > 1;
  }
  return value.deleted && value.content.length === 0 && value.revision > 1;
}

/**
 * Decode and validate one UTF-8 JSON body from KIND_CHAT_EVENT.
 * Unknown versions/types and incomplete payloads return null.
 *
 * @param {Uint8Array | ArrayBuffer | string} body
 * @returns {object | null}
 */
export function decodeChatEvent(body) {
  let value;
  try {
    const bytes = typeof body === "string"
      ? encoder.encode(body)
      : (body instanceof Uint8Array ? body : new Uint8Array(body));
    if (bytes.byteLength > MAX_CHAT_EVENT_BYTES) return null;
    const text = typeof body === "string" ? body : decoder.decode(bytes);
    if (!hasUniqueJsonObjectKeys(text)) return null;
    value = parseChatJsonText(text);
  } catch {
    return null;
  }
  if (!object(value)
    || value.version !== 1
    || !EVENT_TYPES.has(value.type)
    || !nonEmptyString(value.channel_id)) return null;

  const fields = {
    "presence.join": ["version", "type", "channel_id", "channel_type", "presence"],
    "presence.leave": ["version", "type", "channel_id", "presence"],
    typing: ["version", "type", "channel_id", "presence", "typing", "expires_at"],
    "message.create": ["version", "type", "channel_id", "event_id", "message"],
    "message.update": ["version", "type", "channel_id", "event_id", "message"],
    "message.remove": ["version", "type", "channel_id", "event_id", "message"],
    "access.revoked": ["version", "type", "channel_id", "presence"],
  };
  if (value.type === "resync_required") {
    const expected = value.scopes === undefined
      ? ["version", "type", "channel_id", "watermark_event_id"]
      : ["version", "type", "channel_id", "watermark_event_id", "scopes"];
    if (!exactKeys(value, expected)) return null;
  } else if (!exactKeys(value, fields[value.type])) return null;

  if (PRESENCE_TYPES.has(value.type) && !validPresence(value.presence)) return null;
  if (value.type === "presence.join"
    && !["direct", "group", "room"].includes(value.channel_type)) return null;

  if (DURABLE_TYPES.has(value.type)) {
    if (!isU64(value.event_id, true) || !validMessage(value.message, value.type, value.event_id)) {
      return null;
    }
  }

  if (value.type === "typing"
    && (typeof value.typing !== "boolean"
      || !isU64(value.expires_at)
      || (!value.typing && value.expires_at !== 0))) return null;

  if (value.type === "resync_required") {
    if (!isU64(value.watermark_event_id)) return null;
    if (value.scopes !== undefined
      && (!Array.isArray(value.scopes)
        || value.scopes.some((scope) => !nonEmptyString(scope))
        || new Set(value.scopes).size !== value.scopes.length)) return null;
  }

  return value;
}

/**
 * Bounded durable cursor and recovery state for exactly one joined channel.
 * A gap, resync, or reconnect never advances the committed watermark until a
 * complete reconciliation has been explicitly acknowledged.
 */
export class ChatEventCursor {
  #channelId;
  #watermark;
  #state = "live";
  #requiredWatermark = null;
  #completedWatermark = null;
  #reconciliationGeneration = 0;
  #nextRequestSequence = 1;
  #nextBeforeMessageId = null;
  #snapshotWatermark = null;
  #pendingRequest = null;
  #pendingApplication = null;
  #typing = new Map();

  constructor(channelId, watermark = 0) {
    if (!nonEmptyString(channelId)) throw new TypeError("chat cursor channel_id must not be empty");
    if (!isU64(watermark)) throw new TypeError("chat cursor watermark must be a safe unsigned integer");
    this.#channelId = channelId;
    this.#watermark = watermark;
    registerChatCursor(this, {
      beginHistory: (limit) => this.#beginChatHistory(limit),
      acceptHistory: (handle, response) => this.#acceptChatHistory(handle, response),
      completeHistoryApply: (handle) => this.#completeChatHistoryApply(handle),
      abortHistoryApply: (handle) => this.#abortChatHistoryApply(handle),
      beginAck: () => this.#beginChatAck(),
      acceptAck: (handle, response) => this.#acceptChatAck(handle, response),
      abortRequest: (handle) => this.#abortChatRequest(handle),
    });
  }

  get channelId() { return this.#channelId; }
  get watermark() { return this.#watermark; }
  get state() { return this.#state; }
  get requiredWatermark() { return this.#requiredWatermark; }

  disconnected() {
    if (this.#state === "revoked") return;
    this.#state = "rejoin_required";
    this.#requiredWatermark = null;
    this.#completedWatermark = null;
    this.#reconciliationGeneration += 1;
    this.#nextBeforeMessageId = null;
    this.#snapshotWatermark = null;
    this.#pendingRequest = null;
    this.#pendingApplication = null;
    this.#typing.clear();
  }

  rejoined(joinResult) {
    if (this.#state !== "rejoin_required") throw new Error("chat cursor is not awaiting rejoin");
    if (!object(joinResult) || joinResult.channel_id !== this.#channelId
      || !isU64(joinResult.watermark_event_id)) {
      throw new TypeError("rejoin result does not match the cursor channel");
    }
    this.#requireReconciliation(maxU64(this.#watermark, joinResult.watermark_event_id));
  }

  #beginChatHistory(limit) {
    if (this.#state !== "reconcile_required" || this.#pendingRequest || this.#pendingApplication) {
      throw new Error("chat cursor cannot start concurrent or out-of-sequence history");
    }
    const handle = Object.freeze({
      cursor: this,
      generation: this.#reconciliationGeneration,
      sequence: this.#nextRequestSequence++,
      kind: "history",
      limit,
      beforeMessageId: this.#nextBeforeMessageId,
    });
    this.#pendingRequest = handle;
    return handle;
  }

  #acceptChatHistory(handle, response) {
    this.#assertPending(handle, "history");
    const { items, watermark_event_id: responseWatermark } = response;
    if (items.length > handle.limit) throw new Error("chat history page exceeds its requested limit");
    if (!items.every((message, index) => index === 0 || items[index - 1].id > message.id)) {
      throw new Error("chat history items must be ordered newest-first");
    }
    if (handle.beforeMessageId !== null && items.some(({ id }) => id >= handle.beforeMessageId)) {
      throw new Error("chat history page does not follow its request cursor");
    }
    let snapshotWatermark = this.#snapshotWatermark;
    if (snapshotWatermark === null) {
      if (responseWatermark < this.#requiredWatermark) {
        throw new Error("chat history watermark is older than the required snapshot");
      }
      snapshotWatermark = responseWatermark;
    }
    if (items.some(({ last_event_id: eventId }) => eventId > responseWatermark)) {
      throw new Error("chat history item is newer than the response watermark");
    }
    if (responseWatermark !== snapshotWatermark) {
      this.#requireReconciliation(maxU64(this.#requiredWatermark, responseWatermark));
      return { restart: true };
    }

    // Commit only after every semantic check succeeds. If validation throws,
    // the correlated handle remains pending so the caller can abort the whole
    // partial snapshot and reset its continuation cursor transactionally.
    this.#pendingRequest = null;
    this.#snapshotWatermark = snapshotWatermark;
    if (items.length < handle.limit) {
      this.#pendingApplication = handle;
      this.#completedWatermark = snapshotWatermark;
      return { restart: false, terminal: true, watermark: snapshotWatermark };
    }
    this.#nextBeforeMessageId = items.at(-1).id;
    return { restart: false, terminal: false, watermark: snapshotWatermark };
  }

  #completeChatHistoryApply(handle) {
    if (this.#pendingApplication !== handle || handle.cursor !== this
      || handle.generation !== this.#reconciliationGeneration) {
      throw new Error("chat history application does not match this cursor request");
    }
    this.#pendingApplication = null;
    this.#state = "ack_required";
  }

  #abortChatHistoryApply(handle) {
    if (this.#pendingApplication === handle) this.#requireReconciliation(this.#requiredWatermark);
  }

  #beginChatAck() {
    if (this.#state !== "ack_required" || this.#pendingRequest || this.#completedWatermark === null) {
      throw new Error("chat cursor is not ready to acknowledge history");
    }
    const handle = Object.freeze({
      cursor: this,
      generation: this.#reconciliationGeneration,
      sequence: this.#nextRequestSequence++,
      kind: "ack",
      watermark: this.#completedWatermark,
    });
    this.#pendingRequest = handle;
    return handle;
  }

  #acceptChatAck(handle, response) {
    this.#assertPending(handle, "ack");
    this.#pendingRequest = null;
    if (response.watermark_event_id !== handle.watermark) {
      this.#requireReconciliation(maxU64(handle.watermark, response.watermark_event_id));
      return { restart: true };
    }
    this.#watermark = handle.watermark;
    this.#requiredWatermark = null;
    this.#completedWatermark = null;
    this.#state = "live";
    return { restart: false };
  }

  #abortChatRequest(handle) {
    if (this.#pendingRequest === handle && handle?.cursor === this
      && handle.generation === this.#reconciliationGeneration) {
      this.#requireReconciliation(maxU64(
        this.#requiredWatermark ?? this.#watermark,
        this.#completedWatermark ?? this.#watermark,
      ));
    }
  }

  observe(event, now = Date.now()) {
    if (this.#state === "revoked") throw new Error("chat cursor access is revoked");
    if (!object(event) || event.version !== 1 || !EVENT_TYPES.has(event.type)) {
      throw new TypeError("chat cursor requires a decoded chat event");
    }
    if (event.channel_id !== this.#channelId) throw new Error("chat event channel does not match cursor channel");
    if (!isU64(now)) throw new TypeError("now must be a safe Unix-millisecond timestamp");

    if (event.type === "access.revoked") {
      this.#typing.clear();
      this.#requiredWatermark = null;
      this.#completedWatermark = null;
      this.#reconciliationGeneration += 1;
      this.#pendingRequest = null;
      this.#pendingApplication = null;
      this.#state = "revoked";
      return { type: "access_revoked", presence: event.presence };
    }
    if (event.type === "typing") {
      const active = event.typing && event.expires_at > now;
      if (active) {
        this.#typing.set(event.presence.presence_id, {
          ...event.presence, expires_at: event.expires_at,
        });
      } else {
        this.#typing.delete(event.presence.presence_id);
      }
      return {
        type: "typing", presence: event.presence, typing: active, expires_at: event.expires_at,
      };
    }
    if (event.type === "resync_required") {
      this.#requireReconciliation(event.watermark_event_id);
      return { type: "resync_required", watermark_event_id: event.watermark_event_id };
    }
    if (!DURABLE_TYPES.has(event.type)) return { type: "ephemeral" };
    if (!isU64(event.event_id, true)) throw new TypeError("chat event_id must be a safe positive integer");
    if (event.event_id <= this.#watermark) return { type: "duplicate", event_id: event.event_id };
    if (this.#state === "live" && isNextU64(this.#watermark, event.event_id)) {
      this.#watermark = event.event_id;
      return { type: "apply", event_id: event.event_id };
    }
    this.#requireReconciliation(event.event_id);
    return {
      type: "reconcile_gap",
      current_watermark: this.#watermark,
      observed_event_id: event.event_id,
    };
  }

  expireTyping(now = Date.now()) {
    if (!isU64(now)) throw new TypeError("now must be a safe Unix-millisecond timestamp");
    const expired = [];
    for (const [presenceId, presence] of this.#typing) {
      if (presence.expires_at <= now) {
        this.#typing.delete(presenceId);
        expired.push({ presence_id: presence.presence_id, user_id: presence.user_id, typing: false });
      }
    }
    return expired;
  }

  #requireReconciliation(watermark) {
    if (!isU64(watermark)) throw new TypeError("reconciliation watermark must be a safe unsigned integer");
    this.#requiredWatermark = maxU64(this.#requiredWatermark ?? this.#watermark, watermark);
    this.#completedWatermark = null;
    this.#reconciliationGeneration += 1;
    this.#nextBeforeMessageId = null;
    this.#snapshotWatermark = null;
    this.#pendingRequest = null;
    this.#pendingApplication = null;
    this.#state = "reconcile_required";
  }

  #assertPending(handle, kind) {
    if (this.#pendingRequest !== handle || handle?.cursor !== this || handle.kind !== kind
      || handle.generation !== this.#reconciliationGeneration) {
      throw new Error(`chat ${kind} response does not match this cursor request`);
    }
  }
}
