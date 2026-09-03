// Transport adapters for the Citadel JS/Web client. They only translate bytes
// to and from the wire; CitadelClient owns protocol dispatch and state.

import { FrameDecoder, decodeDatagram } from "./envelope.js";

// `lag-recorder.js` owns the public meaning. Keep these packed primitives here
// so an inbound hook does not allocate a direction/delivery metadata object.
const DIAGNOSTIC_RELIABLE = 1 << 1;
const DIAGNOSTIC_DATAGRAM = 1 << 2;

/** @param {ArrayBuffer | ArrayBufferView} value */
function bytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new TypeError("transport received non-binary data");
}

/**
 * Convert Citadel's logged base64 SHA-256 development-certificate hash to the
 * shape accepted by the browser WebTransport constructor.
 *
 * Trusted production certificates do not need this helper. A hash pin is for
 * Citadel's short-lived local-development certificate only.
 *
 * @param {string} base64
 * @returns {{ algorithm: "sha-256", value: Uint8Array }}
 */
export function webTransportCertificateHash(base64) {
  if (typeof base64 !== "string" || base64.length === 0) {
    throw new TypeError("WebTransport certificate hash must be a non-empty base64 string");
  }
  if (typeof globalThis.atob !== "function") {
    throw new Error("base64 decoding is unavailable in this runtime");
  }
  let binary;
  try {
    binary = globalThis.atob(base64);
  } catch {
    throw new TypeError("WebTransport certificate hash is not valid base64");
  }
  const value = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  if (value.length !== 32) {
    throw new RangeError("WebTransport SHA-256 certificate hash must decode to 32 bytes");
  }
  return { algorithm: "sha-256", value };
}

/** WebSocket adapter: all Citadel envelopes are framed and reliable. */
export class WebSocketTransport {
  /** @param {WebSocket} ws */
  constructor(ws) {
    this.kind = "websocket";
    this._ws = ws;
    this._closed = false;
    this._closeError = null;
    this._decoder = new FrameDecoder();
    this._onEnvelope = null;
    this._onDiagnosticEnvelope = null;
    this._onClose = null;

    ws.binaryType = "arraybuffer";
    ws.addEventListener("message", (event) => {
      try {
        for (const envelope of this._decoder.push(bytes(event.data))) {
          this._onDiagnosticEnvelope?.(envelope, DIAGNOSTIC_RELIABLE);
          this._onEnvelope?.(envelope);
        }
      } catch (error) {
        this.fail(error);
      }
    });
    ws.addEventListener("close", () => this._finish(new Error("connection closed")));
  }

  /** @param {{ onEnvelope: (env: import("./envelope.js").Envelope) => void, onDiagnosticEnvelope?: (env: import("./envelope.js").Envelope, flags: number) => void, onClose: (error: Error) => void }} handlers */
  setHandlers(handlers) {
    this._onEnvelope = handlers.onEnvelope;
    this._onDiagnosticEnvelope = handlers.onDiagnosticEnvelope || null;
    this._onClose = handlers.onClose;
    if (this._closed) this._onClose(this._closeError || new Error("connection closed"));
  }

  get isOpen() {
    return !this._closed && this._ws.readyState === 1; // WebSocket.OPEN
  }

  /** @param {import("./envelope.js").Envelope} envelope */
  send(envelope) {
    if (!this.isOpen) throw new Error("cannot send on a closed client");
    this._ws.send(envelope.encodeFramed());
  }

  close(code, reason) {
    try { this._ws.close(code, reason); } catch { /* already closing */ }
  }

  /** @param {unknown} error */
  fail(error) {
    const err = error instanceof Error ? error : new Error(String(error));
    this.close();
    this._finish(err);
  }

  /** @param {Error} error */
  _finish(error) {
    if (this._closed) return;
    this._closed = true;
    this._closeError = error;
    this._onClose?.(error);
  }
}

/** WebTransport adapter: streams are reliable; datagrams are unreliable. */
export class WebTransportTransport {
  /** @param {WebTransport} webTransport */
  constructor(webTransport) {
    this.kind = "webtransport";
    this._webTransport = webTransport;
    this._closed = false;
    this._closeError = null;
    this._onEnvelope = null;
    this._onDiagnosticEnvelope = null;
    this._onClose = null;

    if (!webTransport.incomingUnidirectionalStreams || !webTransport.datagrams?.readable
      || !webTransport.datagrams?.writable) {
      throw new Error("WebTransport implementation does not expose streams and datagrams");
    }

    void this._readUnidirectionalStreams().catch((error) => this.fail(error));
    void this._readDatagrams().catch((error) => this.fail(error));
    void Promise.resolve(webTransport.closed).then(
      () => this._finish(new Error("connection closed")),
      (error) => this._finish(error instanceof Error ? error : new Error("connection closed")),
    );
  }

  /** @param {{ onEnvelope: (env: import("./envelope.js").Envelope) => void, onDiagnosticEnvelope?: (env: import("./envelope.js").Envelope, flags: number) => void, onClose: (error: Error) => void }} handlers */
  setHandlers(handlers) {
    this._onEnvelope = handlers.onEnvelope;
    this._onDiagnosticEnvelope = handlers.onDiagnosticEnvelope || null;
    this._onClose = handlers.onClose;
    if (this._closed) this._onClose(this._closeError || new Error("connection closed"));
  }

  get isOpen() {
    return !this._closed;
  }

  /**
   * @param {import("./envelope.js").Envelope} envelope
   * @param {boolean} reliable
   * @returns {Promise<void>}
   */
  send(envelope, reliable) {
    if (!this.isOpen) return Promise.reject(new Error("cannot send on a closed client"));
    return reliable ? this._sendReliable(envelope) : this._sendUnreliable(envelope);
  }

  close(code, reason) {
    try {
      this._webTransport.close({ closeCode: code ?? 0, reason: reason ?? "" });
    } catch { /* already closing */ }
  }

  /** @param {unknown} error */
  fail(error) {
    const err = error instanceof Error ? error : new Error(String(error));
    this.close();
    this._finish(err);
  }

  /** @param {import("./envelope.js").Envelope} envelope */
  async _sendReliable(envelope) {
    const stream = await this._webTransport.createUnidirectionalStream();
    const writer = stream.getWriter();
    try {
      await writer.write(envelope.encodeFramed());
      await writer.close();
    } finally {
      writer.releaseLock?.();
    }
  }

  /** @param {import("./envelope.js").Envelope} envelope */
  async _sendUnreliable(envelope) {
    const writer = this._webTransport.datagrams.writable.getWriter();
    try {
      await writer.write(envelope.encodeDatagram());
    } finally {
      writer.releaseLock?.();
    }
  }

  async _readUnidirectionalStreams() {
    const reader = this._webTransport.incomingUnidirectionalStreams.getReader();
    try {
      for (;;) {
        const { done, value: stream } = await reader.read();
        if (done) return;
        // Citadel sends one reliable envelope per unidirectional stream. QUIC
        // does not preserve ordering between streams, so drain each stream
        // before accepting the next one to retain reliable event ordering.
        await this._readStream(stream);
      }
    } finally {
      reader.releaseLock?.();
    }
  }

  /** @param {ReadableStream<Uint8Array>} stream */
  async _readStream(stream) {
    const decoder = new FrameDecoder();
    const reader = stream.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        for (const envelope of decoder.push(bytes(value))) {
          this._onDiagnosticEnvelope?.(envelope, DIAGNOSTIC_RELIABLE);
          this._onEnvelope?.(envelope);
        }
      }
    } finally {
      reader.releaseLock?.();
    }
  }

  async _readDatagrams() {
    const reader = this._webTransport.datagrams.readable.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        const envelope = decodeDatagram(bytes(value));
        this._onDiagnosticEnvelope?.(envelope, DIAGNOSTIC_DATAGRAM);
        this._onEnvelope?.(envelope);
      }
    } finally {
      reader.releaseLock?.();
    }
  }

  /** @param {Error} error */
  _finish(error) {
    if (this._closed) return;
    this._closed = true;
    this._closeError = error;
    this._onClose?.(error);
  }
}
