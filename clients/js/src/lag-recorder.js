// Opt-in, metadata-only lag diagnostics recorder. This module deliberately has
// no URL/configuration discovery: CitadelClient creates it only for an explicit
// application-code `diagnostics.lagRecorder.enabled === true` setting.

import {
  KIND_TSYNC_ACK,
  KIND_TSYNC_INPUT,
  KIND_TSYNC_SNAPSHOT,
  KIND_TSYNC_V2_INPUT,
  KIND_TSYNC_V2_SNAPSHOT,
} from "./protocol.js";

export const DIAGNOSTICS_VERSION = 1;
export const LAG_RECORD_BYTES = 48;
export const LAG_HEADER_BYTES = 128;
export const DEFAULT_LAG_RECORD_BYTES = 4 * 1024 * 1024;
export const DEFAULT_LAG_UPLOAD_BYTES = 5 * 1024 * 1024;
export const MAX_LAG_FILTERS = 16;
export const MAX_LAG_CAPTURE_DURATION_US = 30 * 60 * 1_000_000;
export const MAX_WIRE_CAPTURE_BYTES = 64 * 1024 * 1024;
const MAX_UPLOAD_ATTEMPTS = 8;

// Packed hook flags. The transport passes one primitive rather than an
// allocation-prone direction/delivery object for every decoded envelope.
export const DIAG_DIRECTION_INBOUND = 0;
export const DIAG_DIRECTION_OUTBOUND = 1;
export const DIAG_DELIVERY_RELIABLE = 1 << 1;
export const DIAG_DELIVERY_DATAGRAM = 1 << 2;

const CLAG_FLAG_METADATA_ONLY = 1;
const CLAG_FLAG_TRUNCATED = 1 << 1;
const CLAG_FLAG_SERVER_CLOCK = 1 << 2;
const METADATA_V1_SNAPSHOT = 1;
const METADATA_V2_SNAPSHOT = 1 << 1;
const UINT32_MAX = 0xffff_ffff;

function monotonicNowUs() {
  const performanceNow = globalThis.performance?.now;
  if (typeof performanceNow !== "function") return null;
  const value = performanceNow.call(globalThis.performance);
  if (!Number.isFinite(value) || value < 0) return null;
  return Math.round(value * 1000);
}

function readU16(bytes, offset) {
  return (bytes[offset] * 256) + bytes[offset + 1];
}

function readU32(bytes, offset) {
  return (((bytes[offset] * 0x1000000) + (bytes[offset + 1] << 16)
    + (bytes[offset + 2] << 8) + bytes[offset + 3]) >>> 0);
}

function readU64(bytes, offset) {
  let value = 0n;
  for (let index = 0; index < 8; index += 1) value = (value << 8n) | BigInt(bytes[offset + index]);
  return value;
}

function writeU16(bytes, offset, value) {
  bytes[offset] = (value >>> 8) & 0xff;
  bytes[offset + 1] = value & 0xff;
}

function writeU32(bytes, offset, value) {
  bytes[offset] = (value >>> 24) & 0xff;
  bytes[offset + 1] = (value >>> 16) & 0xff;
  bytes[offset + 2] = (value >>> 8) & 0xff;
  bytes[offset + 3] = value & 0xff;
}

function writeU64(bytes, offset, value) {
  let current = BigInt(value);
  for (let index = 7; index >= 0; index -= 1) {
    bytes[offset + index] = Number(current & 0xffn);
    current >>= 8n;
  }
}

function copyU64(source, sourceOffset, target, targetOffset) {
  for (let index = 0; index < 8; index += 1) target[targetOffset + index] = source[sourceOffset + index];
}

function nonZeroBytes(bytes, start, length) {
  for (let index = 0; index < length; index += 1) if (bytes[start + index] !== 0) return true;
  return false;
}

function isNonZero(value) { return value !== 0n; }

function isLocalhost(hostname) {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]" || hostname === "::1";
}

function asU32(value) {
  return Math.min(UINT32_MAX, Math.max(0, Math.floor(value)));
}

function sameCapture(left, right) {
  if (!left || !right || left.generation !== right.generation) return false;
  for (let index = 0; index < 16; index += 1) if (left.captureId[index] !== right.captureId[index]) return false;
  return true;
}

function sameStartRequest(left, right) {
  if (!sameCapture(left, right) || left.deadlineServerUtcMs !== right.deadlineServerUtcMs
    || left.maxRecordBytes !== right.maxRecordBytes || left.filters.length !== right.filters.length) return false;
  for (let index = 0; index < left.filters.length; index += 1) {
    const first = left.filters[index];
    const second = right.filters[index];
    if (first.kind !== second.kind || first.direction !== second.direction || first.entityId !== second.entityId) return false;
  }
  return true;
}

function isAllowedLocal(kind, direction) {
  if (direction === DIAG_DIRECTION_INBOUND) {
    return kind === KIND_TSYNC_SNAPSHOT || kind === KIND_TSYNC_V2_SNAPSHOT;
  }
  return direction === DIAG_DIRECTION_OUTBOUND
    && (kind === KIND_TSYNC_INPUT || kind === KIND_TSYNC_V2_INPUT || kind === KIND_TSYNC_ACK);
}

/** Decode an exact v1 `KIND_DIAG_SERVER_TIME` body, or return null. */
export function decodeDiagServerTime(body) {
  if (!(body instanceof Uint8Array) || body.length !== 17 || body[0] !== DIAGNOSTICS_VERSION) return null;
  const offerId = readU64(body, 1);
  const serverUtcMs = readU64(body, 9);
  if (!isNonZero(offerId) || !isNonZero(serverUtcMs)) return null;
  return { offerId, serverUtcMs };
}

/** Encode the one local recorder capability bit after a valid server-time offer. */
export function encodeDiagCapabilities(offerId) {
  if (typeof offerId !== "bigint" || !isNonZero(offerId)) return null;
  const out = new Uint8Array(11);
  out[0] = DIAGNOSTICS_VERSION;
  writeU64(out, 1, offerId);
  writeU16(out, 9, 1);
  return out;
}

/** Decode an exact v1 `KIND_DIAG_CLOCK_SYNC` response, or return null. */
export function decodeDiagClockSyncResponse(body) {
  if (!(body instanceof Uint8Array) || body.length !== 30 || body[0] !== DIAGNOSTICS_VERSION || body[1] !== 1) return null;
  const sequence = readU32(body, 2);
  const clientSentMonoUs = readU64(body, 6);
  const serverReceivedUtcUs = readU64(body, 14);
  const serverSentUtcUs = readU64(body, 22);
  if (!isNonZero(serverReceivedUtcUs) || !isNonZero(serverSentUtcUs) || serverSentUtcUs < serverReceivedUtcUs) return null;
  return { sequence, clientSentMonoUs, serverReceivedUtcUs, serverSentUtcUs };
}

/** Encode the client half of the bounded v1 clock correlation exchange. */
export function encodeDiagClockSyncRequest(sequence, clientSentMonoUs) {
  if (!Number.isInteger(sequence) || sequence < 0 || sequence > UINT32_MAX || typeof clientSentMonoUs !== "bigint") return null;
  const out = new Uint8Array(14);
  out[0] = DIAGNOSTICS_VERSION;
  out[1] = 0;
  writeU32(out, 2, sequence);
  writeU64(out, 6, clientSentMonoUs);
  return out;
}

/** Decode an exact/bounded v1 `KIND_DIAG_START` body, or return null. */
export function decodeDiagStart(body) {
  const fixedBytes = 1 + 16 + 8 + 8 + 4 + 1;
  if (!(body instanceof Uint8Array) || body.length < fixedBytes || body[0] !== DIAGNOSTICS_VERSION) return null;
  if (!nonZeroBytes(body, 1, 16)) return null;
  const generation = readU64(body, 17);
  const deadlineServerUtcMs = readU64(body, 25);
  const maxRecordBytes = readU32(body, 33);
  const count = body[37];
  if (!isNonZero(generation) || !isNonZero(deadlineServerUtcMs) || maxRecordBytes === 0 || maxRecordBytes > MAX_WIRE_CAPTURE_BYTES || count === 0 || count > MAX_LAG_FILTERS
    || body.length !== fixedBytes + (count * 12)) return null;
  const filters = new Array(count);
  let offset = fixedBytes;
  for (let index = 0; index < count; index += 1) {
    const kind = readU16(body, offset);
    const direction = body[offset + 2];
    const entityTag = body[offset + 3];
    const entityId = readU64(body, offset + 4);
    if (kind === 0 || (direction !== DIAG_DIRECTION_INBOUND && direction !== DIAG_DIRECTION_OUTBOUND)
      || (entityTag !== 0 && entityTag !== 1) || (entityTag === 0 && entityId !== 0n)
      || (entityTag === 1 && entityId === 0n)) return null;
    filters[index] = { kind, direction, entityId: entityTag === 1 ? entityId : null };
    offset += 12;
  }
  return { captureId: body.slice(1, 17), generation, deadlineServerUtcMs, maxRecordBytes, filters };
}

export function decodeDiagFlush(body) {
  const fixedBytes = 51;
  if (!(body instanceof Uint8Array) || body.length < fixedBytes || body[0] !== DIAGNOSTICS_VERSION || !nonZeroBytes(body, 1, 16)) return null;
  const generation = readU64(body, 17);
  const attemptId = readU64(body, 25);
  const uploadDeadlineServerUtcMs = readU64(body, 33);
  const maxCompressedBytes = readU32(body, 41);
  const contentType = body[45];
  const contentEncoding = body[46];
  const pathLength = readU16(body, 47);
  const tokenLength = readU16(body, 49);
  if (!isNonZero(generation) || !isNonZero(attemptId) || !isNonZero(uploadDeadlineServerUtcMs) || maxCompressedBytes === 0 || maxCompressedBytes > MAX_WIRE_CAPTURE_BYTES
    || contentType !== 1 || contentEncoding !== 1 || pathLength === 0 || pathLength > 128 || tokenLength === 0 || tokenLength > 2048
    || body.length !== fixedBytes + pathLength + tokenLength) return null;
  let uploadPath;
  let uploadToken;
  try {
    const decoder = new TextDecoder("utf-8", { fatal: true });
    uploadPath = decoder.decode(body.subarray(fixedBytes, fixedBytes + pathLength));
    uploadToken = decoder.decode(body.subarray(fixedBytes + pathLength));
  } catch { return null; }
  if (uploadPath !== "/v1/diagnostics/captures/upload" || !/^[A-Za-z0-9._-]+$/.test(uploadToken)) return null;
  return {
    captureId: body.slice(1, 17), generation, attemptId, uploadDeadlineServerUtcMs,
    uploadPath, token: uploadToken, maxCompressedBytes,
    mime: "application/vnd.citadel.lag-capture", encoding: "gzip",
  };
}

/** Encode one exact v1 `KIND_DIAG_STATUS` body. */
export function encodeDiagStatus(capture, code, attemptId, recordedPackets, droppedPackets, recordedBytes) {
  if (!capture || !nonZeroBytes(capture.captureId, 0, 16) || typeof capture.generation !== "bigint" || !isNonZero(capture.generation)
    || !Number.isInteger(code) || code < 1 || code > 4 || typeof attemptId !== "bigint") return null;
  if ((code === 1 || code === 4) ? attemptId !== 0n : attemptId === 0n) return null;
  const out = new Uint8Array(46);
  out[0] = DIAGNOSTICS_VERSION;
  out.set(capture.captureId, 1);
  writeU64(out, 17, capture.generation);
  out[25] = code;
  writeU64(out, 26, attemptId);
  writeU32(out, 34, asU32(recordedPackets));
  writeU32(out, 38, asU32(droppedPackets));
  writeU32(out, 42, asU32(recordedBytes));
  return out;
}

/**
 * A bounded, fixed-row recorder. It never retains payload bytes; all per-event
 * writes are indexed scalar writes into the preallocated backing array.
 */
export class LagRecorder {
  /** @param {{ sendStatus: (body: Uint8Array) => void, sendClockSync: (body: Uint8Array) => void, uploadOrigin: string, now?: () => number | null, fetch?: typeof fetch }} options */
  constructor(options) {
    this._sendStatus = options.sendStatus;
    this._sendClockSync = options.sendClockSync;
    this._uploadOrigin = options.uploadOrigin;
    this._now = options.now || monotonicNowUs;
    this._fetch = options.fetch || globalThis.fetch;
    this._serverOffer = null;
    this._serverOffsetUs = null;
    this._serverUncertaintyUs = UINT32_MAX;
    this._capabilitiesSent = false;
    this._authenticated = false;
    this._state = "idle";
    this._capture = null;
    this._startRequest = null;
    this._buffer = null;
    this._slots = 0;
    this._write = 0;
    this._count = 0;
    this._accepted = 0;
    this._overwritten = 0;
    this._skippedFilter = 0;
    this._skippedMalformed = 0;
    this._filterKinds = null;
    this._filterDirections = null;
    this._captureStartMonoUs = 0;
    this._deadlineMonoUs = 0;
    this._serverUtcAtStartUs = 0n;
    this._syncSamples = new Uint8Array(48);
    this._syncSampleCount = 0;
    this._nextSyncMonoUs = 0;
    this._clockSequence = 0;
    this._clockPendingSequence = null;
    this._clockPendingSentMonoUs = 0;
    this._frozen = null;
    this._uploadingAttempt = null;
    this._attemptIds = null;
    this._activeUploadController = null;
    this._activeUploadTimer = null;
  }

  get isRecording() { return this._state === "recording"; }
  get isFrozen() { return this._state === "frozen"; }

  setAuthenticated(value) {
    this._authenticated = value === true;
  }

  /** Accept one valid post-auth SERVER_TIME offer and return capabilities bytes once. */
  acceptServerTime(serverTime) {
    if (!this._authenticated || !serverTime || this._serverOffer !== null) return null;
    const now = this._now();
    if (!Number.isSafeInteger(now) || now < 0) return null;
    this._serverOffer = serverTime.offerId;
    this._serverOffsetUs = (serverTime.serverUtcMs * 1000n) - BigInt(now);
    this._serverUncertaintyUs = UINT32_MAX;
    if (this._capabilitiesSent) return null;
    const body = encodeDiagCapabilities(serverTime.offerId);
    if (body !== null) this._capabilitiesSent = true;
    return body;
  }

  /**
   * Accept a START only after the local authenticated capability flow. Remote
   * filters can only narrow immutable local policy; entity selectors fail
   * closed because v1 has no allocation-safe exact entity decoder.
   */
  start(request) {
    if (!this._authenticated || this._serverOffer === null || !request || this._isExpiredNow()) return false;
    if (this._state !== "idle") {
      if (this._startRequest && sameStartRequest(this._startRequest, request)) return true;
      this._emitStatusFor(request, 4, 0n);
      return false;
    }
    const now = this._now();
    if (!Number.isSafeInteger(now) || now < 0 || this._serverOffsetUs === null) return false;
    const requestedDurationUs = Number((request.deadlineServerUtcMs * 1000n) - (BigInt(now) + this._serverOffsetUs));
    if (!Number.isSafeInteger(requestedDurationUs) || requestedDurationUs <= 0 || requestedDurationUs > MAX_LAG_CAPTURE_DURATION_US) {
      this._emitStatusFor(request, 4, 0n);
      return false;
    }
    let supported = 0;
    for (let index = 0; index < request.filters.length; index += 1) {
      const filter = request.filters[index];
      if (filter.entityId !== null) {
        this._emitStatusFor(request, 4, 0n);
        return false;
      }
      if (isAllowedLocal(filter.kind, filter.direction)) supported += 1;
    }
    if (supported === 0) {
      this._emitStatusFor(request, 4, 0n);
      return false;
    }
    const bytes = Math.floor(Math.min(DEFAULT_LAG_RECORD_BYTES, request.maxRecordBytes) / LAG_RECORD_BYTES) * LAG_RECORD_BYTES;
    if (bytes < LAG_RECORD_BYTES) {
      this._emitStatusFor(request, 4, 0n);
      return false;
    }
    this._buffer = new Uint8Array(bytes);
    this._slots = bytes / LAG_RECORD_BYTES;
    this._filterKinds = new Uint16Array(supported);
    this._filterDirections = new Uint8Array(supported);
    let writeFilter = 0;
    for (let index = 0; index < request.filters.length; index += 1) {
      const filter = request.filters[index];
      if (isAllowedLocal(filter.kind, filter.direction)) {
        this._filterKinds[writeFilter] = filter.kind;
        this._filterDirections[writeFilter] = filter.direction;
        writeFilter += 1;
      }
    }
    this._capture = { captureId: request.captureId, generation: request.generation };
    this._startRequest = request;
    this._state = "recording";
    this._write = 0;
    this._count = 0;
    this._accepted = 0;
    this._overwritten = 0;
    this._skippedFilter = 0;
    this._skippedMalformed = 0;
    this._syncSamples.fill(0);
    this._syncSampleCount = 0;
    this._captureStartMonoUs = now;
    this._deadlineMonoUs = now + requestedDurationUs;
    this._serverUtcAtStartUs = BigInt(now) + this._serverOffsetUs;
    this._nextSyncMonoUs = now;
    this._clockPendingSequence = null;
    this._attemptIds = new Set();
    this._emitStatus(1, 0n);
    this._maybeRequestClockSync(now);
    return true;
  }

  /** Record one envelope at the adapter/client boundary without copying its body. */
  record(kind, body, flags) {
    if (this._state !== "recording") return;
    const now = this._now();
    if (!Number.isSafeInteger(now) || now < this._captureStartMonoUs || now >= this._deadlineMonoUs) {
      this._expire();
      return;
    }
    const direction = flags & 1;
    let matches = false;
    for (let index = 0; index < this._filterKinds.length; index += 1) {
      if (this._filterKinds[index] === kind && this._filterDirections[index] === direction) { matches = true; break; }
    }
    if (!matches) { this._skippedFilter = asU32(this._skippedFilter + 1); return; }
    const base = this._write * LAG_RECORD_BYTES;
    if (!this._writeRow(base, kind, body, flags, now - this._captureStartMonoUs)) {
      this._skippedMalformed = asU32(this._skippedMalformed + 1);
      return;
    }
    this._accepted = asU32(this._accepted + 1);
    if (this._count === this._slots) {
      this._overwritten = asU32(this._overwritten + 1);
    } else {
      this._count += 1;
    }
    this._write += 1;
    if (this._write === this._slots) this._write = 0;
    this._maybeRequestClockSync(now);
  }

  /** Apply an NTP response only when it matches this recorder's outstanding probe. */
  acceptClockSync(response) {
    if (!this._authenticated || !response || this._clockPendingSequence === null || response.sequence !== this._clockPendingSequence
      || response.clientSentMonoUs !== BigInt(this._clockPendingSentMonoUs)) return false;
    const now = this._now();
    this._clockPendingSequence = null;
    if (!Number.isSafeInteger(now) || now < this._clockPendingSentMonoUs || response.serverSentUtcUs < response.serverReceivedUtcUs) return false;
    const clientElapsed = BigInt(now - this._clockPendingSentMonoUs);
    const serverElapsed = response.serverSentUtcUs - response.serverReceivedUtcUs;
    if (clientElapsed < serverElapsed) return false;
    const delay = clientElapsed - serverElapsed;
    const offset = ((response.serverReceivedUtcUs - response.clientSentMonoUs) + (response.serverSentUtcUs - BigInt(now))) / 2n;
    this._serverOffsetUs = offset;
    this._serverUncertaintyUs = Number(delay > BigInt(UINT32_MAX) ? UINT32_MAX : delay / 2n);
    if (this._state === "recording" && this._syncSampleCount < 3) {
      const elapsed = now - this._captureStartMonoUs;
      if (elapsed >= 0 && elapsed <= UINT32_MAX) {
        const offsetBytes = this._syncSampleCount * 16;
        writeU32(this._syncSamples, offsetBytes, elapsed);
        writeU64(this._syncSamples, offsetBytes + 4, BigInt(now) + offset);
        writeU32(this._syncSamples, offsetBytes + 12, this._serverUncertaintyUs);
        this._syncSampleCount += 1;
      }
    }
    return true;
  }

  /** Freeze a matching capture; upload is intentionally handled separately. */
  freeze(flush) {
    if (!this._authenticated || !flush || !this._capture || !sameCapture(this._capture, flush)) return null;
    if (this._state === "recording") {
      const now = this._now();
      if (!Number.isSafeInteger(now) || now >= this._deadlineMonoUs) { this._expire(); return null; }
      this._frozen = this._makeFrozen();
      this._state = "frozen";
    }
    if (this._state !== "frozen" || !this._frozen || this._uploadingAttempt !== null) return null;
    return this._frozen;
  }

  /** Run one server-issued one-use upload attempt against immutable frozen bytes. */
  async upload(flush) {
    const frozen = this.freeze(flush);
    if (!frozen || !this._validGrant(flush)) {
      if (frozen) {
        if (typeof flush?.token === "string") flush.token = null;
        this._emitStatus(4, 0n);
      }
      return false;
    }
    if (this._attemptIds.has(flush.attemptId) || this._attemptIds.size >= MAX_UPLOAD_ATTEMPTS) {
      flush.token = null;
      this._emitStatus(4, 0n);
      return false;
    }
    if (typeof globalThis.AbortController !== "function") {
      flush.token = null;
      this._emitStatus(4, 0n);
      return false;
    }
    this._attemptIds.add(flush.attemptId);
    this._uploadingAttempt = flush.attemptId;
    this._emitStatus(2, flush.attemptId);
    let accepted = false;
    let prepared = null;
    const controller = new AbortController();
    this._activeUploadController = controller;
    try {
      prepared = await this._buildUploadRequest(flush, frozen, controller);
      if (!prepared || typeof this._fetch !== "function") throw new Error("lag upload unavailable");
      if (!this._ownsUpload(flush)) return false;
      const response = await this._fetch(prepared.request);
      accepted = response?.ok === true;
    } catch {
      accepted = false;
    } finally {
      if (prepared) clearTimeout(prepared.timer);
      if (prepared && this._activeUploadTimer === prepared.timer) this._activeUploadTimer = null;
      // A one-use token must never survive either a success or ambiguous failure.
      if (this._activeUploadController === controller) {
        this._activeUploadController = null;
        this._uploadingAttempt = null;
      }
      flush.token = null;
    }
    if (!this._capture || !sameCapture(this._capture, flush)) return false;
    if (accepted) {
      this._emitStatus(3, flush.attemptId);
      this._clear();
      return true;
    }
    // Preserve the immutable frozen capture for a distinct server-issued retry.
    this._emitStatus(4, 0n);
    return false;
  }

  cancel() { this._clear(); }

  _writeRow(base, kind, body, flags, elapsed) {
    if (!(body instanceof Uint8Array) || body.length > UINT32_MAX || elapsed < 0 || elapsed > UINT32_MAX) return false;
    // Validate fixed transform prefixes without decoding or retaining variable
    // payloads. V1: server_tick, snapshot_id, base_snapshot_id, send_rate.
    let packetId = 0;
    let basePacketId = 0;
    let serverTick = 0;
    let tickHz = 0;
    let metadataFlags = 0;
    let v2 = false;
    if (kind === KIND_TSYNC_SNAPSHOT) {
      if (body.length < 13) return false;
      serverTick = readU32(body, 0);
      packetId = readU32(body, 4);
      basePacketId = readU32(body, 8);
      metadataFlags = METADATA_V1_SNAPSHOT;
    } else if (kind === KIND_TSYNC_V2_SNAPSHOT) {
      if (body.length < 31 || readU16(body, 16) === 0) return false;
      serverTick = readU32(body, 18);
      packetId = readU32(body, 22);
      basePacketId = readU32(body, 26);
      tickHz = readU16(body, 16);
      metadataFlags = METADATA_V1_SNAPSHOT | METADATA_V2_SNAPSHOT;
      v2 = true;
    } else if (kind === KIND_TSYNC_INPUT) {
      if (body.length < 9) return false;
      packetId = readU32(body, 0);
      basePacketId = readU32(body, 4);
    } else if (kind === KIND_TSYNC_V2_INPUT) {
      // v2 input carries `epoch:u64, last_observed_tick:u64, flags:u8`
      // followed by the v1 input bundle. It deliberately has no tick rate.
      if (body.length < 26 || body[16] !== 0) return false;
      packetId = readU32(body, 17);
      basePacketId = readU32(body, 21);
      v2 = true;
      metadataFlags = METADATA_V2_SNAPSHOT;
    } else if (kind === KIND_TSYNC_ACK) {
      if (body.length < 8) return false;
      packetId = readU32(body, 0);
    } else {
      return false;
    }
    writeU32(this._buffer, base, elapsed);
    writeU16(this._buffer, base + 4, kind);
    this._buffer[base + 6] = flags & 1;
    this._buffer[base + 7] = flags & 0xfe;
    writeU32(this._buffer, base + 8, body.length);
    writeU32(this._buffer, base + 12, packetId);
    writeU32(this._buffer, base + 16, basePacketId);
    writeU32(this._buffer, base + 20, 0); // v1 has no exact entity metadata.
    writeU32(this._buffer, base + 24, serverTick);
    writeU16(this._buffer, base + 28, tickHz);
    writeU16(this._buffer, base + 30, metadataFlags);
    if (v2) {
      copyU64(body, 0, this._buffer, base + 32);
      copyU64(body, 8, this._buffer, base + 40);
    } else {
      for (let index = 32; index < 48; index += 1) this._buffer[base + index] = 0;
    }
    return true;
  }

  _maybeRequestClockSync(now) {
    if (this._state !== "recording" || this._clockPendingSequence !== null || this._syncSampleCount >= 3 || now < this._nextSyncMonoUs) return;
    const sequence = this._clockSequence;
    this._clockSequence = (this._clockSequence + 1) >>> 0;
    const body = encodeDiagClockSyncRequest(sequence, BigInt(now));
    if (body === null) return;
    this._clockPendingSequence = sequence;
    this._clockPendingSentMonoUs = now;
    this._nextSyncMonoUs = now + (10 * 60 * 1_000_000);
    try { this._sendClockSync(body); } catch { this._clockPendingSequence = null; }
  }

  _makeFrozen() {
    const header = new Uint8Array(LAG_HEADER_BYTES);
    header.set([0x43, 0x4c, 0x41, 0x47], 0);
    writeU16(header, 4, 1);
    writeU16(header, 6, LAG_HEADER_BYTES);
    writeU16(header, 8, LAG_RECORD_BYTES);
    let flags = CLAG_FLAG_METADATA_ONLY;
    if (this._overwritten > 0) flags |= CLAG_FLAG_TRUNCATED;
    if (this._serverOffsetUs !== null) flags |= CLAG_FLAG_SERVER_CLOCK;
    writeU16(header, 10, flags);
    writeU32(header, 12, this._count);
    writeU64(header, 16, BigInt(this._accepted));
    writeU64(header, 24, BigInt(this._overwritten));
    writeU64(header, 32, BigInt(this._skippedFilter));
    writeU64(header, 40, BigInt(this._skippedMalformed));
    header.set(this._capture.captureId, 48);
    writeU32(header, 64, Number(this._capture.generation & 0xffff_ffffn));
    writeU32(header, 68, this._serverUncertaintyUs);
    writeU64(header, 72, this._serverUtcAtStartUs);
    header.set(this._syncSamples, 80);
    return {
      header,
      write: this._write,
      count: this._count,
      slots: this._slots,
      buffer: this._buffer,
    };
  }

  _validGrant(flush) {
    if (!flush || typeof flush.uploadPath !== "string" || flush.uploadPath.length === 0 || typeof flush.token !== "string" || flush.token.length === 0
      || !Number.isInteger(flush.maxCompressedBytes) || flush.maxCompressedBytes <= 0
      || flush.mime !== "application/vnd.citadel.lag-capture" || flush.encoding !== "gzip") return false;
    return this._uploadUrl(flush.uploadPath) !== null;
  }

  _uploadUrl(path) {
    try {
      if (!path.startsWith("/") || path.startsWith("//")) return null;
      const origin = new URL(this._uploadOrigin);
      const url = new URL(path, origin);
      if (url.origin !== origin.origin) return null;
      if (url.protocol !== "https:" && !(url.protocol === "http:" && isLocalhost(url.hostname))) return null;
      return url;
    } catch { return null; }
  }

  async _buildUploadRequest(flush, frozen, controller) {
    const url = this._uploadUrl(flush.uploadPath);
    if (!url || typeof globalThis.CompressionStream !== "function" || typeof globalThis.ReadableStream !== "function"
      || typeof globalThis.TransformStream !== "function") return null;
    const now = this._now();
    if (!Number.isSafeInteger(now) || this._serverOffsetUs === null) return null;
    const deadlineMono = Number((flush.uploadDeadlineServerUtcMs * 1000n) - this._serverOffsetUs);
    if (!Number.isSafeInteger(deadlineMono) || deadlineMono <= now) return null;
    const timer = setTimeout(() => controller.abort(), Math.ceil((deadlineMono - now) / 1000));
    if (controller.signal.aborted) {
      clearTimeout(timer);
      return null;
    }
    this._activeUploadTimer = timer;
    const compressedLimit = Math.min(flush.maxCompressedBytes, DEFAULT_LAG_UPLOAD_BYTES);
    const init = {
      method: "POST",
      headers: {
        Authorization: `Bearer ${flush.token}`,
        "Content-Type": flush.mime,
        "Content-Encoding": flush.encoding,
      },
      body: this._compressedStream(frozen, compressedLimit),
      credentials: "omit",
      redirect: "error",
      referrerPolicy: "no-referrer",
      signal: controller.signal,
      duplex: "half",
    };
    try {
      return { request: new Request(url, init), timer };
    } catch {
      // Streaming request bodies are not universal. The raw input is already
      // bounded; materialising a bounded compressed body keeps the token flow
      // safe on implementations without `duplex: "half"` support.
      try {
        const compressed = await new Response(this._compressedStream(frozen, compressedLimit)).arrayBuffer();
        if (compressed.byteLength > compressedLimit) throw new RangeError("lag upload exceeds configured limit");
        const fallback = { ...init, body: new Uint8Array(compressed) };
        delete fallback.duplex;
        return { request: new Request(url, fallback), timer };
      } catch {
        clearTimeout(timer);
        if (this._activeUploadTimer === timer) this._activeUploadTimer = null;
        return null;
      }
    }
  }

  _compressedStream(frozen, limit) {
    let size = 0;
    const bounded = new TransformStream({
      transform(chunk, controllerOut) {
        size += chunk.byteLength;
        if (size > limit) throw new RangeError("lag upload exceeds configured limit");
        controllerOut.enqueue(chunk);
      },
    });
    return this._rawStream(frozen).pipeThrough(new CompressionStream("gzip")).pipeThrough(bounded);
  }

  _rawStream(frozen) {
    const rowBytes = frozen.count * LAG_RECORD_BYTES;
    const start = frozen.count === frozen.slots ? frozen.write * LAG_RECORD_BYTES : 0;
    const firstLength = Math.min(rowBytes, frozen.buffer.length - start);
    const secondLength = rowBytes - firstLength;
    return new ReadableStream({
      start(controller) {
        controller.enqueue(frozen.header);
        if (firstLength > 0) controller.enqueue(frozen.buffer.subarray(start, start + firstLength));
        if (secondLength > 0) controller.enqueue(frozen.buffer.subarray(0, secondLength));
        controller.close();
      },
    });
  }

  _emitStatus(code, attemptId) {
    this._emitStatusFor(this._capture, code, attemptId);
  }

  _emitStatusFor(capture, code, attemptId) {
    const body = encodeDiagStatus(capture, code, attemptId, this._count, this._overwritten, this._recordedBytes());
    if (body === null) return;
    try { this._sendStatus(body); } catch { /* local transport is already closing */ }
  }

  _isExpiredNow() {
    if (this._state === "recording") {
      const now = this._now();
      return Number.isSafeInteger(now) && now >= this._deadlineMonoUs;
    }
    return false;
  }

  _expire() {
    if (this._capture) this._emitStatus(4, 0n);
    this._clear();
  }

  _clear() {
    this._activeUploadController?.abort();
    this._activeUploadController = null;
    if (this._activeUploadTimer !== null) clearTimeout(this._activeUploadTimer);
    this._activeUploadTimer = null;
    this._state = "idle";
    this._capture = null;
    this._startRequest = null;
    this._buffer = null;
    this._slots = 0;
    this._filterKinds = null;
    this._filterDirections = null;
    this._frozen = null;
    this._uploadingAttempt = null;
    this._clockPendingSequence = null;
    this._attemptIds = null;
  }

  _ownsUpload(flush) {
    return this._capture !== null && sameCapture(this._capture, flush)
      && this._uploadingAttempt === flush.attemptId && this._state === "frozen";
  }

  _recordedBytes() {
    return LAG_HEADER_BYTES + (this._count * LAG_RECORD_BYTES);
  }
}
