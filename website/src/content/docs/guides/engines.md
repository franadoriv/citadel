---
title: Engine integration (Unity / Unreal / Godot)
description: Integrate Unity, Unreal, or Godot with Citadel through the shared C ABI.
---

All engine integrations bind to the same maintained client core: the stable C ABI
in `crates/citadel-client-ffi`. There is one protocol implementation; engines call
`connect`, `send`, `poll`, and `free`. See the [C ABI reference](/reference/client-sdk/c-abi/)
for the exact surface.

:::tip[Just want to install it?]
Every release ships a ready-to-use, prebuilt client SDK per engine on the GitHub
Releases page. For step-by-step download-and-drop-in instructions, see
[Install a client SDK](/guides/install-client-sdk/). This page explains how the
integrations work under the hood.
:::

## Shared approach

1. Build the native library (`cargo build -p citadel-client-ffi --release`).
2. Ship the per-platform native libs alongside the engine project.
3. Declare the C functions for your engine's FFI mechanism.
4. Poll from the main loop (or a worker) — receive is poll-based, no callbacks
   cross the boundary.
5. Use the [`citadel-wire`](/concepts/envelopes/) layout for message bodies.

## Unity (C#) — working sample

There is a working Unity sample that uses P/Invoke over the C header to connect
over QUIC and run the move-and-broadcast loop. See the
[Unity QUIC sample](/guides/unity-quic-sample/) for the managed API, wire layout,
and setup. In outline:

```csharp
[DllImport("citadel_client_ffi")]
static extern int citadel_client_connect_websocket(string url, out IntPtr handle);
```

Windows x86_64 is released. Apple Silicon and Intel macOS `.dylib` archives can
be built locally and will be offered after their signing/notarization release
path is enabled;
packaging a UPM `.unitypackage` and IL2CPP support remain follow-ups.

## Unreal (C++) — drop-in plugin

The Unreal SDK ships as a standard **drop-in plugin**: copy the `Citadel/` folder
(`clients/unreal/Plugin/Citadel/` in the repo — a `Citadel.uplugin` plus
`Source/CitadelClient/`) into your project's `Plugins/` directory, provide the
`citadel_client.h` header and the built `citadel-client-ffi` native library, and
regenerate project files. The plugin wraps the C API in UObject-friendly types.

The connection subsystem (`UCitadelClientSubsystem`) is **Blueprint-callable**, so
a designer can drive the whole flow no-code:

- `Connect Quic` / `Connect Web Socket` → `ECitadelStatus`.
- `Authenticate Device` / `Authenticate Custom` (Base URL, id, create, username)
  — an async HTTP call to the node's `/v1/auth/device|custom` route; the resulting
  session token arrives on the `On Authenticated` event (and `Session Token`), or
  `On Authentication Failed` on error.
- `Disconnect`, `Is Connected`, `Get Last Status`, `Get Last Error`.

The gameplay components `UCitadelTransformSync` and `UCitadelNetworkPeer` are
already Blueprint-friendly, so connecting, authenticating, and replicating can all
be wired in Blueprint.

## Godot (GDExtension and Web export)

The Godot SDK ships `CitadelClient` GDScript bindings plus `CitadelClientNative`,
a Godot 4 GDExtension that owns the shared C-ABI client handle. The **release
download** includes the prebuilt Windows GDExtension; locally built macOS
archives contain matching arm64/x86_64 dylibs once the signed release path is
enabled. Both use a drop-in
`addons/citadel/` folder, so most users just copy it in — see
[Install a client SDK → Godot](/guides/install-client-sdk/). Call
`authenticate_guest` or `authenticate_with_token` immediately after connecting
and before gameplay sends.

To build the extension yourself (Linux, a macOS architecture without a published
archive, or to modify it), use the matching `godot-cpp` checkout and
`citadel-client-ffi` library; the packaging step runs this for Windows and macOS
automatically. See the
[Godot SDK README](../../../../../../clients/godot/README.md)
for the exact build command. The in-editor sample run remains a manual pre-release
check; CI checks the declared protocol constants and builds/tests the shared Rust
ABI layer.

Godot Web exports use `CitadelWebClient`, a pure-GDScript `WebSocketPeer`
transport that needs no GDExtension. Call `connect_websocket("wss://…")`, drive
`pump` in `_process`, wait for `is_open`, and retry authentication until it
returns `OK`. It shares the normal Godot protocol and room helpers. An HTTPS
game page must use `wss://`; browser exports intentionally have reliable
WebSocket only, with no QUIC datagrams, transform sync, or native replication
codec APIs. The [Godot SDK README](../../../../../../clients/godot/README.md)
has the runnable lifecycle snippet.

## Why a single C core

One maintained protocol implementation behind a tiny C ABI avoids reimplementing
framing, transports, and the relay protocol per engine, and keeps every client in
lockstep with the server via `citadel-wire`.
