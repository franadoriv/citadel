// Wire-agnostic realtime envelope + framing for the Citadel JS/Web SDK.
//
// Byte-for-byte identical to `citadel_wire::Envelope` (Rust). Framed layout for
// stream transports (WebSocket): `u32` big-endian body length (kind + payload),
// then `u16` big-endian kind, then the payload. Datagram layout (no prefix):
// `u16` big-endian kind, then the payload.

/** Bytes of the big-endian length prefix on a framed stream envelope. */
export const LENGTH_PREFIX_BYTES = 4;
/** Bytes of the `kind` header inside a datagram/frame body. */
export const KIND_BYTES = 2;
/** Maximum framed body length accepted, matching the server codec (16 MiB). */
export const MAX_FRAME_BODY_BYTES = 16 * 1024 * 1024;

/**
 * A realtime envelope: a numeric `kind` discriminant and an opaque byte `body`.
 */
export class Envelope {
  /**
   * @param {number} kind Message-family discriminant (`u16`).
   * @param {Uint8Array} [body] Opaque payload bytes.
   */
  constructor(kind, body = new Uint8Array(0)) {
    /** @type {number} */
    this.kind = kind;
    /** @type {Uint8Array} */
    this.body = body;
  }

  /** Encoded datagram size (header + body), without a length prefix. */
  datagramLen() {
    return KIND_BYTES + this.body.length;
  }

  /** Total framed size (prefix + header + body). */
  framedLen() {
    return LENGTH_PREFIX_BYTES + this.datagramLen();
  }

  /**
   * Encode as a length-delimited frame for a stream transport (WebSocket).
   * @returns {Uint8Array}
   */
  encodeFramed() {
    const bodyLen = this.datagramLen();
    const out = new Uint8Array(LENGTH_PREFIX_BYTES + bodyLen);
    const dv = new DataView(out.buffer);
    dv.setUint32(0, bodyLen, false);
    dv.setUint16(LENGTH_PREFIX_BYTES, this.kind, false);
    out.set(this.body, LENGTH_PREFIX_BYTES + KIND_BYTES);
    return out;
  }

  /**
   * Encode as a bare datagram body (no length prefix); the datagram boundary
   * provides framing.
   * @returns {Uint8Array}
   */
  encodeDatagram() {
    const out = new Uint8Array(this.datagramLen());
    new DataView(out.buffer).setUint16(0, this.kind, false);
    out.set(this.body, KIND_BYTES);
    return out;
  }
}

/**
 * Decode a single datagram body into an {@link Envelope}.
 * @param {Uint8Array} data
 * @returns {Envelope}
 */
export function decodeDatagram(data) {
  if (data.length < KIND_BYTES) {
    throw new RangeError(`datagram length ${data.length} too small for a header`);
  }
  const kind = new DataView(data.buffer, data.byteOffset, data.length).getUint16(0, false);
  return new Envelope(kind, data.slice(KIND_BYTES));
}

/**
 * Stateful decoder for a stream of length-delimited frames.
 *
 * WebSocket binary messages may carry several concatenated frames, or split one
 * frame across messages. Push raw bytes as they arrive and drain complete
 * envelopes; partial frames stay buffered until the rest arrives — the exact
 * behavior of the server's `decode_framed`.
 */
export class FrameDecoder {
  constructor() {
    /** @type {Uint8Array} */
    this._buf = new Uint8Array(0);
  }

  /**
   * Append incoming bytes and return every complete {@link Envelope} now
   * available (possibly none).
   *
   * @param {ArrayBuffer | Uint8Array} chunk
   * @returns {Envelope[]}
   */
  push(chunk) {
    const incoming = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    if (this._buf.length === 0) {
      this._buf = incoming;
    } else {
      const merged = new Uint8Array(this._buf.length + incoming.length);
      merged.set(this._buf, 0);
      merged.set(incoming, this._buf.length);
      this._buf = merged;
    }

    const out = [];
    while (this._buf.length >= LENGTH_PREFIX_BYTES) {
      const dv = new DataView(this._buf.buffer, this._buf.byteOffset, this._buf.length);
      const bodyLen = dv.getUint32(0, false);
      if (bodyLen > MAX_FRAME_BODY_BYTES) {
        throw new RangeError(`frame body length ${bodyLen} exceeds max ${MAX_FRAME_BODY_BYTES}`);
      }
      if (bodyLen < KIND_BYTES) {
        throw new RangeError(`frame body length ${bodyLen} too small`);
      }
      if (this._buf.length < LENGTH_PREFIX_BYTES + bodyLen) break; // incomplete
      const kind = dv.getUint16(LENGTH_PREFIX_BYTES, false);
      const body = this._buf.slice(LENGTH_PREFIX_BYTES + KIND_BYTES, LENGTH_PREFIX_BYTES + bodyLen);
      out.push(new Envelope(kind, body));
      this._buf = this._buf.slice(LENGTH_PREFIX_BYTES + bodyLen);
    }
    return out;
  }

  /** Bytes currently buffered awaiting a complete frame. */
  get buffered() {
    return this._buf.length;
  }
}
