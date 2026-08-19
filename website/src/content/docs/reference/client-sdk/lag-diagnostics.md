---
title: Lag diagnostics (JavaScript)
description: Explicit, bounded client-side recording for diagnosing movement jitter.
---

Lag diagnostics is an **opt-in debug tool**, not a gameplay or telemetry
feature. It records a small, bounded metadata stream when an operator asks for
one. It is designed to help compare movement delivery behaviour across clients;
it does not measure a player's one-way latency, RTT, path asymmetry, or packet
loss.

## Enable it in application source

The JavaScript SDK is off by default. Enable the recorder only in the code that
creates the client:

```js
const client = await CitadelClient.connect("wss://game.example/realtime", {
  diagnostics: { lagRecorder: { enabled: true } },
});
```

There is intentionally no URL parameter, browser-storage value, or remote
configuration bit that can enable it. When the option is missing or `false`,
the SDK allocates no ring buffer, advertises no diagnostics capability, ignores
all diagnostics control frames, and never uploads an artifact. This makes a
new server safe with an older or disabled client, and a new client safe with a
server that does not support diagnostics.

## Capture lifecycle

After authentication, an opted-in client waits for the server's `SERVER_TIME`
offer and advertises its supported diagnostics capability. A trusted server
then controls the lifecycle:

1. It sends `START` with a capture id, generation, deadline, fixed byte cap,
   and an allowed movement-metadata filter.
2. The SDK records only that locally allowed metadata in a fixed-size ring. A
   full ring overwrites its oldest row and marks the artifact as truncated.
3. The server sends a per-client `FLUSH` grant when it is ready to collect.
   Ending a match is the usual time to flush, but the game developer chooses
   when to start and stop a capture.
4. The SDK freezes the snapshot, gzip-compresses its `CLAG` v1 bytes, uploads
   it once, and clears the snapshot only after a successful response.

The server's clock offer is correlation metadata. The SDK derives observed
arrival timestamps from that anchor and local elapsed time, including its clock
uncertainty. It must not present those timestamps as a network timing proof.

The SDK never records application payloads, entity identifiers, player-private
identifiers, or arbitrary packet bodies. The server cannot weaken that local
metadata-only policy through `START`.

## Upload rules

`FLUSH` carries a same-origin relative `upload_path`, a short-lived signed
bearer, the permitted MIME/content encoding, and a compressed-byte cap. The
SDK uses:

```text
POST <same origin><upload_path>
Authorization: Bearer <one-use grant>
Content-Type: application/vnd.citadel.lag-capture
Content-Encoding: gzip
```

The grant belongs to one capture, session/player, tenant, match, attempt, and
deadline. It is not exposed through normal status frames. Do not log it or put
it in crash reports. A failed or ambiguous upload consumes no reusable client
credential: retain the frozen bytes and wait for the server to issue a **new**
`FLUSH` attempt. The SDK deliberately does not retry a bearer automatically.

## Compatibility and troubleshooting

| Situation | Expected result |
| --- | --- |
| Debug option is disabled | No capability, recording, or upload. |
| Server has no diagnostics support | Normal connection and gameplay; no capture begins. |
| Legacy client | It never advertises the capability, so it is ineligible for a capture. |
| `START` is malformed or outside local policy | It is ignored and current evidence is preserved. |
| Upload rejects or expires | The server can request a fresh attempt; never reuse a token. |
| Capture ring fills | The result stays bounded and reports truncation/overwrite quality. |

For the native lifecycle that produces `START` and `FLUSH`, see
[Lag diagnostics native API](/reference/server-sdk/lag-diagnostics/). For
configuration and metric interpretation, see
[Lag diagnostics operations](/reference/operations/lag-diagnostics/).
