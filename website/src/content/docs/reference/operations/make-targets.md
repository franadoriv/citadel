---
title: Local build & staging targets
description: The bin-* staging targets, benchmark-serve, and Windows/macOS release packages — what each produces, where, and how to run them.
---

Citadel's repo-root `Makefile` wraps local build, staging, and packaging
workflows so you do not have to remember `cargo build` flags, file layouts, or
per-engine copy steps. This page documents the **staging and packaging**
targets. For the full target list (demos, docs, database), run `make help`
from the repo root.

## `bin/` vs `dist/` vs `packaging/`

- **`bin/`** — git-ignored local runnable staging. Each `bin-*` target copies a
  freshly built binary plus its supporting files into a subfolder here so you
  can `cd` in and run it directly, with no Cargo workspace nearby.
- **`dist/`** — git-ignored **versioned release packages**, produced by the
  `package-windows`, `package-linux`, and native-architecture `package-macos` targets.
- **`packaging/`** — tracked **templates** consumed by staging (e.g. the
  hello-world `scripts/main.lua`, release `README` templates). You do not run
  anything from `packaging/` directly; the `bin-*` and `package-*` targets copy
  from it.

## Running `make` on Windows

Windows ships a `make.bat` shim, so there is no more `.\make.ps1 <target>`
friction:

```bat
:: cmd.exe
make bin-server
```

```powershell
# PowerShell
.\make bin-server
```

macOS and Linux are unchanged — use `make <target>` from a POSIX shell.

For the full canonical gate on Windows, run `make check` from cmd or
`.\make check` from PowerShell. The Windows runner invokes the repository's
same `scripts/check.sh` through Git Bash, so it includes documentation, SDK,
runtime-parity, backlog, and agent-parity checks rather than a partial
PowerShell-only subset. Git for Windows is therefore required; the runner gives
a direct install message if Git Bash is unavailable.

## Targets

| Target | Stages / produces | Location |
| --- | --- | --- |
| `bin-server` | A runnable server: `citadel(.exe)` + `citadel.toml` + a hello-world `scripts/main.lua` + an empty `maps/`. | `bin/server/` |
| `bin-server-python` | A Python-enabled runnable server: `citadel.exe` built with `runtime-python` + `scripts/main.py` + bundled CPython `python/` assets. | `bin/server-python/` |
| `bin-benchmark` | The combat benchmark: `server.exe`/`citadel` + the combat Lua game script + the `client.html` bot client + a copy of the JS SDK. | `bin/benchmark/` |
| `benchmark-serve` | Runs `bin-benchmark`, then starts the staged server, serves `client.html` over HTTP, and opens it in your browser. | `bin/benchmark/` (staging), local HTTP server (runtime) |
| `bin-client-unity` | A copy-into-project Unity C# SDK. | `bin/clients/unity/` |
| `bin-client-godot` | A copy-into-project native Godot GDScript SDK. | `bin/clients/godot/` |
| `bin-client-godot-web` | A portable, source-only Godot Web addon (`addons/citadel/`), with no GDExtension or native library. | `bin/clients/godot-web/` |
| `bin-client-unreal` | A copy-into-project Unreal C++ SDK. | `bin/clients/unreal/` |
| `bin-client-js` | A copy-into-project JavaScript (`@citadel/client`) SDK, including the Three.js starter. | `bin/clients/js/` |
| `bin-client-rust` | A copy-into-project Rust (`citadel-client`) SDK. | `bin/clients/rust/` |
| `bin-clients` | Runs every `bin-client-<engine>` target above. | `bin/clients/` |
| `bin-all` | Runs `bin-server`, `bin-benchmark`, and `bin-clients` together — everything staged in one pass. | `bin/` |
| `package-windows` | Builds the server and the Unity native plugin, then stages and zips a versioned Windows release. | `dist/citadel-windows-x86_64-v<version>.zip` |
| `package-linux` | Builds a statically linked Linux server for the selected architecture, stages the standalone layout, and zips it. | `dist/citadel-linux-<x86_64-musl\|aarch64-musl>-v<version>.zip` |
| `package-windows-python` | Builds the Python-enabled server, stages bundled CPython and `scripts/main.py`, smokes `citadel.exe check`, then zips it. | `dist/citadel-windows-x86_64-python-v<version>.zip` |
| `package-client-unity` | Builds the native FFI, then stages and zips the ready-to-use Unity SDK. | `dist/citadel-client-unity-windows-x86_64-v<version>.zip` |
| `package-client-unreal` | Builds the native FFI, then stages and zips the ready-to-use Unreal drop-in plugin (source + Win64 FFI). | `dist/citadel-client-unreal-windows-x86_64-v<version>.zip` |
| `package-client-godot` | Builds the native GDExtension (via SCons + godot-cpp) over the FFI, then stages and zips a drop-in Godot addon with the prebuilt Windows libraries. | `dist/citadel-client-godot-windows-x86_64-v<version>.zip` |
| `package-client-godot-web` | Stages and zips the portable, source-only Godot Web addon. It does not build a GDExtension. | `dist/citadel-client-godot-web-v<version>.zip` |
| `package-client-js` | Bundles, minifies, verifies, and zips the portable browser ESM SDK with TypeScript declarations, Three.js starter, source map, gzip/Brotli sidecars, and internal checksums. | `dist/citadel-client-js-v<version>.zip` |
| `package-clients-windows` | Runs the three native `package-client-<engine>` targets plus the portable Godot Web package. | Three `dist/citadel-client-<engine>-windows-x86_64-v<version>.zip` archives and `dist/citadel-client-godot-web-v<version>.zip` |
| `package-macos` | Builds the standalone server and Unity native library for the active native macOS architecture. | `dist/citadel-macos-<aarch64\|x86_64>-v<version>.zip` |
| `package-client-unity-macos` | Builds the macOS FFI dylib and stages a Unity SDK for the active native architecture. | `dist/citadel-client-unity-macos-<aarch64\|x86_64>-v<version>.zip` |
| `package-client-unreal-macos` | Builds the macOS FFI static archive and stages the Unreal drop-in plugin. | `dist/citadel-client-unreal-macos-<aarch64\|x86_64>-v<version>.zip` |
| `package-client-godot-macos` | Builds the Godot macOS GDExtension dylibs (editor and release) and stages the addon. | `dist/citadel-client-godot-macos-<aarch64\|x86_64>-v<version>.zip` |
| `package-clients-macos` | Runs the three macOS client-package targets for the active native architecture. | Three `dist/citadel-client-*-macos-<arch>-v<version>.zip` archives |

### `bin-server`

```bash
make bin-server
cd bin/server && ./citadel serve
```

Produces a self-contained server directory: `citadel` (or `citadel.exe` on
Windows), the config with `scripts_dir` pointed at the staged `scripts/`
folder, a starter `scripts/main.lua` (position relay + a `ping` RPC), and an
empty `maps/` for cooked level geometry. No Cargo workspace is required to run
it.

### `bin-server-python`

```bash
make bin-server-python
cd bin/server-python && ./citadel.exe serve
```

Produces a Windows Python runtime package for local testing. The target builds
`citadel.exe` with `--features runtime-python`, writes a config with
`runtime.language = "python"` and `scripts_dir = "./scripts"`, copies the
packaged `scripts/main.py`, and stages CPython beside the executable:

```text
bin/server-python/
├── citadel.exe
├── python313.dll
├── python/
│   ├── Lib/
│   └── DLLs/
├── citadel.toml
├── README.txt
├── maps/
└── scripts/main.py
```

After staging, the target runs `scripts/smoke-python-bundle.sh`, which launches
the staged `citadel.exe check` with `PYO3_PYTHON` and global Python path hints
removed and `PYTHONHOME` pointed at the bundle.

### `bin-benchmark` and `benchmark-serve`

```bash
make bin-benchmark
cd bin/benchmark && ./server.exe serve
# from the repo root, in another terminal:
python3 -m http.server 8080 --directory bin/benchmark
```

`benchmark-serve` does all three steps for you — stage, start the server,
serve the HTML — and opens `http://127.0.0.1:8080/client.html`:

```bash
make benchmark-serve
```

Re-run `make bin-benchmark` (or `make benchmark-serve`) after editing the
source demo, the Lua script, or the JS SDK to refresh the staged files. See
[Connect a web client](/guides/web-client/) for what the demo shows.

### `bin-client-<engine>` and `bin-clients`

Each `bin-client-<engine>` target stages a copy-into-project SDK for that
engine at `bin/clients/<engine>/` — a plain folder you can drop into a game
project without pulling the rest of the Citadel repo. `bin-clients` runs all
five in one pass:

```bash
make bin-client-unity
make bin-client-godot
make bin-client-godot-web
make bin-client-unreal
make bin-client-js
make bin-client-rust

# or all at once:
make bin-clients
```

`bin-client-js` also stages `examples/threejs-starter/`. Serve
`bin/clients/js/` with a static HTTP server and open
`/examples/threejs-starter/` to run the WebSocket SDK with a small Three.js
multiplayer scene. See [Connect a web client](/guides/web-client/) for the
server command and the networking/rendering boundary.

### `bin-all`

Stages the server, the benchmark, and every client SDK in one command — the
fastest way to get a complete, runnable local set under `bin/`:

```bash
make bin-all
```

### `package-windows`

Unlike the `bin-*` targets (local, unversioned, git-ignored staging),
`package-windows` produces a **versioned, zipped release** for distribution:
it builds the server and the Unity native plugin in release mode, stages the
release layout, and zips it as
`dist/citadel-windows-x86_64-v<version>.zip`. The version comes from the
workspace `Cargo.toml`.

```bash
make package-windows
```

The optional Python artifact is separate so the default package stays lean:

```bash
make package-windows-python
```

It produces `dist/citadel-windows-x86_64-python-v<version>.zip`, using the same
bundled CPython layout as `bin-server-python`.

### `package-linux`

`package-linux` produces the downloadable Linux server archive:

```bash
make package-linux
```

It cross-compiles the default server for `x86_64-unknown-linux-musl`, stages
`citadel`, its configuration, starter Lua script, and `maps/`, then writes
`dist/citadel-linux-x86_64-musl-v<version>.zip`. The static musl binary does
not depend on the host distribution's glibc, so an x86_64 Linux user can unzip
the GitHub Release asset and run `./citadel` without installing Rust or cloning
the repository. Release CI also produces `aarch64-unknown-linux-musl` as
`dist/citadel-linux-aarch64-musl-v<version>.zip` for 64-bit ARM hosts.

Release CI builds this target through
[Cross](https://github.com/cross-rs/cross), which provides the complete musl
C/C++ toolchain required by Citadel's native navigation dependency. For a local
musl package, install Cross and run `make package-linux LINUX_CARGO=cross` with
Docker available.

To package ARM64 locally, select both the Rust target and release filename:

```bash
make package-linux LINUX_CARGO=cross \
  LINUX_TARGET=aarch64-unknown-linux-musl \
  LINUX_PACKAGE_ARCH=aarch64-musl
```

### `package-client-<engine>` and `package-clients-windows`

Where `package-windows` produces the **server** release, these targets produce
the **ready-to-use client SDK** releases — one versioned, zipped archive per
engine, so a game developer can download just the client for their engine:

```bash
make package-client-unity
make package-client-unreal
make package-client-godot
make package-client-godot-web
make package-client-js

# or all native engine packages plus the portable Web addon:
make package-clients-windows
```

Each zip carries the same copy-into-project layout as the matching
`bin-client-<engine>` staging target, but named and versioned for distribution:

- `dist/citadel-client-unity-windows-x86_64-v<version>.zip` — C# bindings,
  demo, and the built `citadel_client_ffi.dll` under `Plugins/x86_64/`.
- `dist/citadel-client-unreal-windows-x86_64-v<version>.zip` — the drop-in
  `Plugins/Citadel` plugin source with the Win64 FFI `.lib` + header staged
  under `Source/CitadelClient/ThirdParty/`.
- `dist/citadel-client-godot-windows-x86_64-v<version>.zip` — a drop-in
  `addons/citadel/` folder: the GDScript bindings, the `citadel.gdextension`
  descriptor, and the **prebuilt** Windows GDExtension libraries under `bin/`
  (`citadel_godot.windows.template_{debug,release}.x86_64.dll` plus the companion
  `citadel_client_ffi.dll`). The native Godot SDK's GDScript delegates to a native
  GDExtension, so the package builds it — it is not GDScript-only.

- `package-client-godot-web` contains only `addons/citadel/`, the SDK README,
  and its manifest. Extract it at the Godot project's `res://` root. It uses
  `WebSocketPeer`, so it is portable across browser platforms and deliberately
  contains no `.gdextension`, `.dll`, `.dylib`, or `.so` artifact.

- `package-client-js` creates the platform-independent
  `dist/citadel-client-js-v<version>.zip`. Extract it into a browser game's
  static files and import `dist/citadel-client.min.mjs` from a module script.
  Its `.gz` and `.br` siblings are precompressed variants for a static server
  that sets `Content-Encoding`; browser code imports only the `.mjs` file. The
  external source map omits `sourcesContent`, and minification is for compact
  delivery rather than source-code secrecy.

The version comes from the workspace `Cargo.toml`, so all native client zips,
the portable Godot Web zip, and
the server zip share the same `v<version>` for a release. The release CI reuses
`package-clients-windows`, so these same archives are attached to every
published GitHub Release alongside the server package.

:::note
`package-client-godot` builds the Godot GDExtension with **SCons** and a pinned
**godot-cpp** checkout (branch `4.3`, cloned into `target/godot-cpp` on first
run) over `citadel-client-ffi`. The target provisions SCons via `pip` and clones
godot-cpp automatically, and needs a C++ toolchain (MSVC on Windows). Building
both the editor `template_debug` and exported `template_release` libraries adds
noticeably to the packaging time versus the pure-copy Unity/Godot-source path.
The portable `package-client-godot-web` target does not need SCons, godot-cpp,
Rust, or a C++ toolchain.
:::

### `package-macos` and `package-clients-macos`

Run these on the architecture you are packaging; the release workflow does this
once on Apple Silicon and once on Intel rather than cross-compiling a Godot
GDExtension with a mismatched SDK:

```bash
make package-macos
make package-clients-macos
```

The server package contains `citadel`, `citadel.toml`, and a Unity `.dylib` in
`clients/unity/Plugins/macOS/`. The dedicated engine archives contain:

- Unity: `libcitadel_client_ffi.dylib` under `Plugins/macOS/`.
- Unreal: `Plugins/Citadel` plus
  `ThirdParty/Mac/libcitadel_client_ffi.a` and the C header.
- Godot: `addons/citadel/` plus architecture-specific
  `libcitadel_godot.macos.template_{debug,release}.{arm64|x86_64}.dylib` files.

Local invocations produce unsigned developer archives. Public macOS release
publication is temporarily disabled until the required Apple signing and
notarization credentials are configured; the local targets remain available for
developer verification. First-public-release and in-editor engine smokes remain
explicit release checks.

:::note
`bin/` and `dist/` are both git-ignored — they are local build output, not
tracked release artifacts. `packaging/` holds the tracked templates the
targets above copy from; you do not run it directly.
:::
