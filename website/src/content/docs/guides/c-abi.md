---
title: Use the C ABI (FFI)
description: Consume Citadel from C through the citadel-client-ffi stable C ABI — connect, send, poll, free.
---

`citadel-client-ffi` (`crates/citadel-client-ffi`) exposes a small, stable **C
ABI** over the Rust [`citadel-client`](/guides/rust-sdk/) SDK so native code
(including game engines) can consume Citadel without reimplementing the protocol.
The header `crates/citadel-client-ffi/include/citadel_client.h` is the source of
truth and is committed for consumers.

For the full surface, see the [C ABI reference](/reference/client-sdk/c-abi/).

## Build the library

```bash
cargo build -p citadel-client-ffi --release
# Produces, under target/release/:
#   libcitadel_client_ffi.{dylib,so} or citadel_client_ffi.dll  (cdylib)
#   libcitadel_client_ffi.a or .lib                              (staticlib)
```

The header is regenerated on build into the `include/` directory (a committed copy
is provided).

## Minimal C example

```c
#include "citadel_client.h"
#include <stdio.h>

int main(void) {
    CitadelClient *c = NULL;
    if (citadel_client_connect_websocket("ws://127.0.0.1:7352/", &c) != CITADEL_STATUS_OK)
        return 1;

    const unsigned char body[] = {0, 0, 0, 0, 0, 0, 0, 0}; // e.g. a position
    citadel_client_send(c, /*kind*/ 1, body, sizeof(body), /*reliable*/ true);

    unsigned short kind; unsigned char buf[256]; size_t len; bool trunc;
    for (;;) {
        CitadelStatus s = citadel_client_poll(c, &kind, buf, sizeof(buf), &len, &trunc);
        if (s == CITADEL_STATUS_OK) {
            /* handle a relayed peer message of `len` bytes (kind == 2) */
        } else if (s == CITADEL_STATUS_AGAIN) {
            /* nothing ready: sleep briefly, then continue */
        } else {
            break; /* DISCONNECTED or an error */
        }
    }
    citadel_client_free(c);
    return 0;
}
```

For QUIC, use `citadel_client_connect_quic(addr, server_name, insecure, &c)` with
`insecure = true` for the dev self-signed cert.

## Model

- **Poll-based receive.** No callbacks cross the FFI boundary. Call
  `citadel_client_poll` from your main loop. It returns `AGAIN` when nothing is
  ready and `DISCONNECTED` when closed and drained.
- **Caller-owned buffers.** Every byte/string transfer is pointer + length into
  caller-provided buffers. The FFI never returns Rust-allocated buffers you must
  separately free. `send` copies your bytes; `poll` copies the payload into your
  `buf` and sets `out_truncated` if it did not fit (retry with a larger buffer).
  `out_len` is always the full payload length.
- **One owned handle.** The handle from a `connect_*` call is the only
  Rust-allocated object crossing the boundary. Call `citadel_client_free` exactly
  once; passing `null` is a no-op.
- **Panic-safe.** Every entrypoint catches panics and maps them to
  `CITADEL_STATUS_INTERNAL`, so no Rust panic unwinds across C.
- **ABI versioned.** Check `citadel_client_abi_version` against
  `CITADEL_FFI_ABI_VERSION` to guard against mismatch.

## Error details

After a non-OK status, call `citadel_client_last_error(handle, buf, cap)` to copy
a NUL-terminated message into your buffer. It returns the number of bytes written
including the NUL.

:::caution[Not implemented yet]
QUIC currently only wires the dev insecure-TLS path (`insecure = true`); a
pinned/verified path is a follow-up. Desktop host targets only here; mobile and
consoles are later phases. Session validation is deferred (internal ).
No credentials are embedded.
:::
