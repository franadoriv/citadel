---
title: C ABI reference
description: The stable C ABI exposed by citadel-client-ffi, mirrored from the committed cbindgen header.
---

The source of truth for the C ABI is the cbindgen-generated header
`crates/citadel-client-ffi/include/citadel_client.h`, committed for consumers.
This page mirrors it. If the two ever disagree, the header wins; regenerate this
page from it (see [generated docs](/reference/operations/generated/)). See the
[C ABI guide](/guides/c-abi/) for usage and the ownership model.

## Version

```c
#define CITADEL_FFI_ABI_VERSION 3

uint32_t citadel_client_abi_version(void);
```

Check `citadel_client_abi_version` against `CITADEL_FFI_ABI_VERSION` to guard
against an ABI mismatch.

## Status codes

`CitadelStatus` is a stable `#[repr(C)]` enum:

| Constant | Value | Meaning |
| --- | --- | --- |
| `CITADEL_STATUS_OK` | 0 | Succeeded (for poll, an envelope was written). |
| `CITADEL_STATUS_AGAIN` | 1 | Nothing available to poll right now; try later. |
| `CITADEL_STATUS_DISCONNECTED` | 2 | Connection closed; no more envelopes. |
| `CITADEL_STATUS_INVALID_ARGUMENT` | 3 | A pointer was null or an argument invalid. |
| `CITADEL_STATUS_CONNECT` | 4 | Connecting or handshaking failed. |
| `CITADEL_STATUS_SEND` | 5 | Sending failed. |
| `CITADEL_STATUS_RECEIVE` | 6 | Receiving/decoding failed. |
| `CITADEL_STATUS_INTERNAL` | 7 | Unexpected internal error (incl. a caught panic). |

`CitadelAuthStatus` mirrors the wire `AUTH_STATUS_*` values returned by the
realtime auth handshake:

| Constant | Value | Meaning |
| --- | --- | --- |
| `CITADEL_AUTH_STATUS_AUTHENTICATED` | 0 | Token accepted; user id returned. |
| `CITADEL_AUTH_STATUS_GUEST` | 1 | Empty token accepted as a guest. |
| `CITADEL_AUTH_STATUS_REJECTED` | 2 | Token/guest refused; reason returned. |

## Handle

```c
typedef struct CitadelClient CitadelClient;
```

Opaque handle. A `connect_*` call allocates it; the caller owns it and must call
`citadel_client_free` exactly once.

## Functions

```c
CitadelStatus citadel_client_connect_quic(const char *addr,
                                          const char *server_name,
                                          bool insecure,
                                          CitadelClient **out_handle);

CitadelStatus citadel_client_connect_websocket(const char *url,
                                               CitadelClient **out_handle);

CitadelStatus citadel_client_authenticate(CitadelClient *handle,
                                          const uint8_t *token,
                                          uintptr_t len,
                                          CitadelAuthStatus *out_status,
                                          char *user_buf,
                                          uintptr_t user_cap,
                                          uintptr_t *out_user_len,
                                          uint8_t *out_reason);

CitadelStatus citadel_client_send(CitadelClient *handle,
                                  uint16_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  bool reliable);

CitadelStatus citadel_client_poll(CitadelClient *handle,
                                  uint16_t *out_kind,
                                  uint8_t *buf,
                                  uintptr_t cap,
                                  uintptr_t *out_len,
                                  bool *out_truncated);

uintptr_t citadel_client_last_error(CitadelClient *handle,
                                    char *buf,
                                    uintptr_t cap);

void citadel_client_free(CitadelClient *handle);
```

### `citadel_client_connect_quic`

Connect to a QUIC endpoint. `addr` and `server_name` are NUL-terminated C
strings. `insecure = false` verifies a public CA certificate and the supplied
hostname; `insecure = true` selects dev TLS without verification. On success, writes a heap-allocated
handle to `*out_handle`.

### `citadel_client_connect_websocket`

Connect to a WebSocket endpoint (e.g. `ws://127.0.0.1:7352/`). On success, writes
the handle to `*out_handle`.

### `citadel_client_authenticate`

Perform the realtime auth handshake on a freshly connected handle, before any
gameplay send. Pass `token = NULL, len = 0` to request an explicit guest session,
or pass the HTTP-issued session token bytes from
[`/v1/auth/device|custom`](/reference/client-sdk/authentication/). On `OK`,
`*out_status` is `AUTHENTICATED`, `GUEST`, or `REJECTED`; authenticated results
copy the resolved user id into `user_buf` as UTF-8 with `*out_user_len` set to
the full length, while rejected results set `*out_reason` to a coarse
`AUTH_REASON_*`.

### `citadel_client_send`

Send an envelope. `reliable` chooses a reliable stream vs an unreliable datagram
on QUIC; WebSocket is always reliable. The `data`/`len` bytes are copied; the
caller keeps ownership of its buffer (`data` may be null iff `len == 0`).

### `citadel_client_poll`

Non-blocking receive. On `OK`, writes the envelope `kind` to `*out_kind`, copies
the payload into `buf` (capacity `cap`), writes the full payload length to
`*out_len`, and sets `*out_truncated` to true if it did not fit (only `cap` bytes
were written — retry with a larger buffer). Returns `AGAIN` if nothing is ready,
or `DISCONNECTED` if closed and drained.

### `citadel_client_last_error`

Copy the last error message for `handle` into `buf` as a NUL-terminated string
(truncated to `cap`). Returns the number of bytes written including the NUL, or 0
on invalid arguments.

### `citadel_client_free`

Free a handle. After this the pointer is invalid and must not be reused. Passing
null is a no-op.

## NetworkPeer codec and typed authoring (ABI v3)

ABI v3 exposes the canonical NetworkPeer codec independently of a connection.
`citadel_schema_hash` derives the required 16-byte class identity. The legacy
v2 `citadel_rep_decode` entrypoint and its 40-byte `CitadelRepCodec` array are
frozen for scalar-only callers. Typed Vector3/quaternion and keyed-collection
decode uses the additive v3 `citadel_rep_decode_with_collections` entrypoint
with `CitadelRepDecodeFieldCodecV3` / `CitadelRepCodecV3`. The opaque decoded
handle exposes header/field access and must be freed with
`citadel_rep_decoded_free`.

Collection iteration takes a sparse changed-field index. Before applying its
operations, call `citadel_rep_decoded_collection_field_id` to obtain the source
schema `field_id`; never treat that changed-field ordinal as a reflected property
identifier. The accessor rejects scalar and out-of-range indexes.

For authoring, create a `CitadelRepEncoder` with `citadel_rep_encoder_new`, call
`citadel_rep_encoder_set_schema` for full snapshots, add each changed field, then
call `citadel_rep_encoder_finish` and `citadel_rep_encoder_free`. A non-full
packet requires a nonzero base token; a full packet requires the schema identity.
The transaction fails closed: an invalid field, duplicate field id, mismatched
codec, invalid collection operation, or cap violation makes `finish` return
`CITADEL_STATUS_INVALID_ARGUMENT` without emitting a partial bunch.

| Function family | ABI v3 support |
| --- | --- |
| `citadel_rep_encoder_add_bool`, `_int`, `_scalar`, `_bytes` | Bool, bounded integer, fixed-point scalar, and capped byte fields. |
| `citadel_rep_encoder_add_vector3` | Three finite world-unit floats; `bounds = 0` selects protocol defaults. |
| `citadel_rep_encoder_add_quat` | Smallest-three quaternion with 9, 10, or 15 bits/component. |
| `citadel_rep_encoder_add_collection` | Keyed remove/add/change operations; collection items may use the preceding scalar, vector, or quaternion codecs. Bytes are copied before the call returns. |

The frozen v2 `CitadelRepCodec` supports `0=bool`, `1=int range`,
`2=scalar`, and `3=bytes` only. The distinct v3 `CitadelRepCodecV3` adds
`vector_bounds` and `quat_bits`, with `4=Vector3` and
`5=smallest-three quaternion`; never pass a v3 descriptor array to the legacy
decode entrypoint. `CitadelRepCollectionOp` carries the keyed collection
operation, generation, `rep_key`, typed value slots, and optional bytes.

This ABI encodes/decodes bytes only. It does **not** connect or register a client,
register a class/object, send an envelope, enable `[transport.network_peer]`, or
perform an engine-runtime integration. Unity has a source-level managed v3 wrapper;
Unreal and Godot have source bindings, but engine runtime verification is deferred
because those engines were unavailable for this pass.

## Building

```bash
cargo build -p citadel-client-ffi --release
# target/release/libcitadel_client_ffi.{dylib,so} / .dll  (cdylib)
# target/release/libcitadel_client_ffi.a / .lib           (staticlib)
```

:::caution[Not implemented yet]
QUIC verifies public CA certificates and the supplied hostname when
`insecure = false`; the insecure path is dev-only. Desktop host targets only; mobile/consoles and per-engine
packaging are later phases.
:::
