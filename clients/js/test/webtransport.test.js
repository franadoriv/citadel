import { test } from "node:test";
import assert from "node:assert/strict";

import { CitadelClient } from "../src/client.js";
import { Envelope, decodeDatagram } from "../src/envelope.js";
import { webTransportCertificateHash } from "../src/transport.js";

function deferred() {
  /** @type {(value?: unknown) => void} */
  let resolve;
  /** @type {(reason?: unknown) => void} */
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

class FakeWebTransport {
  static instances = [];

  constructor(url, init) {
    this.url = url;
    this.init = init;
    this.reliableWrites = [];
    this.datagramWrites = [];
    this.ready = Promise.resolve();
    this._closed = deferred();
    this.closed = this._closed.promise;
    this._isClosed = false;
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => { this._incomingController = controller; },
    });
    this.datagrams = {
      readable: new ReadableStream({
        start: (controller) => { this._datagramController = controller; },
      }),
      writable: new WritableStream({
        write: (chunk) => this.datagramWrites.push(new Uint8Array(chunk)),
      }),
    };
    FakeWebTransport.instances.push(this);
  }

  async createUnidirectionalStream() {
    return new WritableStream({
      write: (chunk) => this.reliableWrites.push(new Uint8Array(chunk)),
    });
  }

  receiveReliable(frame) {
    this.receiveReliableStream(new ReadableStream({
      start(controller) {
        controller.enqueue(frame);
        controller.close();
      },
    }));
  }

  receiveReliableStream(stream) {
    this._incomingController.enqueue(stream);
  }

  receiveDatagram(datagram) {
    this._datagramController.enqueue(datagram);
  }

  close() {
    if (this._isClosed) return;
    this._isClosed = true;
    this._incomingController.close();
    this._datagramController.close();
    this._closed.resolve();
  }
}

class RejectingWebTransport extends FakeWebTransport {
  constructor(url, init) {
    super(url, init);
    this.ready = Promise.reject(new Error("certificate rejected"));
  }
}

class FakeConnectWebSocket {
  constructor() {
    this.readyState = 0;
    this.listeners = new Map();
    queueMicrotask(() => {
      this.readyState = 1;
      this.listeners.get("open")?.({});
    });
  }

  addEventListener(kind, handler) {
    this.listeners.set(kind, handler);
  }

  send() {}
  close() { this.readyState = 3; this.listeners.get("close")?.({}); }
}

test("WebTransport sends framed reliable envelopes and bare unreliable datagrams", async () => {
  const certHash = btoa(String.fromCharCode(...Uint8Array.from({ length: 32 }, (_, index) => index)));
  const client = await CitadelClient.connectWebTransport("https://citadel.test:7353/", {
    WebTransport: FakeWebTransport,
    serverCertificateHashBase64: certHash,
  });
  const transport = FakeWebTransport.instances.at(-1);
  assert.equal(client.transportKind, "webtransport");
  assert.deepEqual([...transport.init.serverCertificateHashes[0].value], [...Uint8Array.from({ length: 32 }, (_, index) => index)]);

  client.send(41, new Uint8Array([1, 2]));
  client.send(42, new Uint8Array([3, 4]), { reliable: false });
  await settle();

  const framed = transport.reliableWrites[0];
  assert.equal(new DataView(framed.buffer, framed.byteOffset, framed.byteLength).getUint16(4, false), 41);
  assert.deepEqual([...framed.slice(6)], [1, 2]);
  const datagram = decodeDatagram(transport.datagramWrites[0]);
  assert.equal(datagram.kind, 42);
  assert.deepEqual([...datagram.body], [3, 4]);
  client.close();
});

test("WebTransport dispatches inbound uni-stream frames and datagrams through one client surface", async () => {
  const client = await CitadelClient.connectWebTransport("https://citadel.test:7353/", {
    WebTransport: FakeWebTransport,
  });
  const transport = FakeWebTransport.instances.at(-1);
  const received = [];
  client.onAny((envelope) => received.push(envelope));

  transport.receiveReliable(new Envelope(8, new Uint8Array([9])).encodeFramed());
  transport.receiveDatagram(new Envelope(10, new Uint8Array([11])).encodeDatagram());
  await settle();

  // Streams and datagrams are separate asynchronous browser queues; their
  // relative arrival order is not a reliability guarantee.
  assert.deepEqual(received.map((envelope) => [envelope.kind, [...envelope.body]]).sort((a, b) => a[0] - b[0]), [
    [8, [9]],
    [10, [11]],
  ]);
  client.close();
});

test("WebTransport preserves reliable event order across unidirectional streams", async () => {
  const client = await CitadelClient.connectWebTransport("https://citadel.test:7353/", {
    WebTransport: FakeWebTransport,
  });
  const transport = FakeWebTransport.instances.at(-1);
  const received = [];
  const firstStream = deferred();
  client.onAny((envelope) => received.push(envelope.kind));

  transport.receiveReliableStream(new ReadableStream({
    async start(controller) {
      await firstStream.promise;
      controller.enqueue(new Envelope(12).encodeFramed());
      controller.close();
    },
  }));
  transport.receiveReliable(new Envelope(13).encodeFramed());
  await settle();
  assert.deepEqual(received, []);

  firstStream.resolve();
  await settle();
  assert.deepEqual(received, [12, 13]);
  client.close();
});

test("WebTransport development certificate helper validates and converts the server hash", () => {
  const source = Uint8Array.from({ length: 32 }, (_, index) => index);
  const base64 = btoa(String.fromCharCode(...source));
  const hash = webTransportCertificateHash(base64);
  assert.equal(hash.algorithm, "sha-256");
  assert.deepEqual([...hash.value], [...source]);
  assert.throws(() => webTransportCertificateHash("not-base64"), TypeError);
  assert.throws(() => webTransportCertificateHash(btoa("short")), RangeError);
});

test("connectAuto falls back to the supplied WebSocket endpoint before authentication", async () => {
  const client = await CitadelClient.connectAuto({
    webTransportUrl: "https://citadel.test:7353/",
    webSocketUrl: "ws://citadel.test:7352/",
  }, {
    webTransport: { WebTransport: RejectingWebTransport },
    webSocket: { WebSocket: FakeConnectWebSocket },
  });
  assert.equal(client.transportKind, "websocket");
  client.close();
});
