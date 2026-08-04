# Citadel Unreal C++ SDK plugin

A thin, **header-driven** Unreal Engine C++ SDK over the Citadel client C ABI.
Unreal is C++, so it consumes the canonical, cbindgen-generated
`citadel_client.h` **verbatim** and hand-writes only a UE-idiomatic wrapper. The
compiler binding against that header *is* the drift check (Tier-B), which makes
this the strongest-verified of the three engine SDKs. See
`crates/citadel-wire/contract.json`, `scripts/check_sdk_parity.py`, and
`website/src/content/docs/guides/engines.md`.

The shippable plugin is the self-contained **drop-in folder**
`clients/unreal/Plugin/Citadel/` — copy it into a game's `Plugins/` and you have
`<Project>/Plugins/Citadel/`. The repo tooling (this README,
`sdk.manifest.json`, the parity/build scripts, `tier_b/`, `.gitignore`) stays in
`clients/unreal/`, **outside** the plugin. The plugin is **compile-verified
against UE 5.8** (see [Compile verification](#compile-verification-against-ue-58)).
It is **SDK source only** (per `website/src/content/docs/guides/engines.md`): no
committed engine project, `Binaries/`, `Intermediate/`, or native libraries —
those are build artifacts (git-ignored). The native `citadel_client_ffi` library
is built at package time.

The plugin file is `Citadel.uplugin`; its runtime module is `CitadelClient` (the
drop-in plugin name and the module name differ on purpose). A second, **editor-only**
module `CitadelEditor` hosts in-editor authoring tools — today the **map cook tool**
(`Tools → Citadel → Cook Map Data`), which exports static-mesh collision and the
Landscape collision heightfield (including visibility holes and alternate cell
diagonals) to a Citadel `.map` file for the server. It reads collision data, not
Landscape render topology or Merge Actors output. It is `Type: "Editor"`, so it is
never compiled into a packaged client and has no dependency on the runtime module or
the native FFI.

## Layout

```
clients/unreal/                     # repo working dir (tooling; NOT shipped)
  Plugin/
    Citadel/                        # <-- THE DROP-IN PLUGIN (= Project/Plugins/Citadel/)
      Citadel.uplugin               # plugin descriptor (Runtime: CitadelClient; Editor: CitadelEditor)
      Source/
        CitadelClient/
          CitadelClient.Build.cs    # module rules (deps, C ABI include, FFI wiring)
          Public/
            CitadelWire.h           # constexpr wire/ABI constants (Tier-A source)
            CitadelClientSubsystem.h# UE GameInstance subsystem wrapper (declares)
            CitadelTransformWire.h  # transform-sync snapshot decoder
            CitadelTransformSync.h  # UCitadelTransformSync component + subsystem
            CitadelNetworkPeer.h    # NetworkPeer property table + dirty tracking
          Private/
            CitadelClientSubsystem.cpp # wrapper impl; C ABI + HTTP auth
            CitadelTransformSync.cpp   # transform-sync runtime + component impl
            CitadelNetworkPeer.cpp     # rep-layout reflection walk + shadow net
            CitadelFfiStub.cpp         # link-only C ABI stub (CITADEL_FFI_STUB builds)
        CitadelEditor/              # editor-only tooling module (never shipped)
          CitadelEditor.Build.cs    # editor deps (UnrealEd, ToolMenus, DesktopPlatform, PhysicsCore)
          Public/
            CitadelMapCooker.h      # level geometry -> .map cook API
          Private/
            CitadelEditorModule.cpp # Tools → Citadel → Cook Map Data menu entry
            CitadelMapCooker.cpp    # gather static collision -> world-space mesh
            CitadelCmapWriter.h     # big-endian CMAP encoder (mirror of crates/citadel-map)
  tier_b/
    citadel_parity_tu.cpp           # Tier-B compile-against-header TU (UE-free)
  parity-hook.sh                    # Tier-B hook run by check-sdk-parity.sh
  bundle-ffi.sh                     # build + bundle the native lib/header (drop-in)
  ue-plugin-build.sh                # gated UE 5.8 plugin compile (opt-in)
  sdk.manifest.json                 # Tier-A manifest + non-null tier_b_check
  README.md
```

The module must live under `Source/` — UnrealBuildTool only discovers plugin
modules under `<PluginRoot>/Source/`. `CitadelWire.h` and the subsystem
`#include "citadel_client.h"` directly and **never re-declare** the C prototypes.

## Importing into an Unreal project

This plugin drops straight into a game's `Plugins/` folder and compiles with **no
env vars**. The plugin needs two generated inputs — the C ABI header and the
native `citadel_client_ffi` lib — and `bundle-ffi.sh` stages both *inside* the
plugin so it is self-contained.

1. **Bundle the native lib + header into the plugin** (once, from a repo checkout):
   ```bash
   bash clients/unreal/bundle-ffi.sh
   ```
   This runs `cargo build -p citadel-client-ffi --release` and copies the built
   static library → `Plugin/Citadel/Source/CitadelClient/ThirdParty/<Platform>/`
   (`citadel_client_ffi.lib` on Win64; `libcitadel_client_ffi.a` on macOS/Linux)
   and the canonical `citadel_client.h` → `.../ThirdParty/include/`. Both are
   **git-ignored** (generated, never committed). *In a release package these are
   already bundled by CI, so a downloaded package skips this step.*
2. **Copy the plugin** — the whole `Plugin/Citadel/` folder (now including its
   `ThirdParty/`) — into `YourGame/Plugins/Citadel/`. That is the entire drop-in;
   the repo tooling in `clients/unreal/` is not shipped.
3. **Build/open the project.** `CitadelClient.Build.cs` **auto-detects** the
   bundled header (`ThirdParty/include`) and lib (`ThirdParty/<Platform>/…`) and
   links the real client plus the required Win64 system libs — no configuration.
   `CITADEL_WITH_UNREAL=1` is set so `CitadelWire.h` uses Unreal's own
   `uint8`/`uint16`/`uint32` aliases.

**Resolution order** (in `Build.cs`): a `ThirdParty/include` + `ThirdParty/<Platform>`
bundle (the drop-in path above) → else an in-repo build via the repo-relative
probe → else a consumer-supplied `CITADEL_FFI_LIB` env var. If none is found the
build warns loudly and the `citadel_*` calls fail to link (run `bundle-ffi.sh`).
The gated compile-verification (`ue-plugin-build.sh`) instead sets
`CITADEL_FFI_STUB=1` to link a signatures-only stub without any native lib.

## Compile verification against UE 5.8

The plugin's UE C++ (UObject reflection via `GENERATED_BODY`/`UCLASS`/`UENUM`,
the `FTickableGameObject` subsystem, the `FProperty`/`CPF_Net` reflection walk in
`CitadelNetworkPeer`, and the bit-reader/quantizer ports) is **compiled against
real UE 5.8 headers** by `clients/unreal/ue-plugin-build.sh`. This is the "real"
Tier-B — the UE-free object-only TU (below) cannot exercise the UE-dependent
code.

```bash
# Uses $CITADEL_UE_ROOT, else D:/Games/UE_5.8; SKIPs cleanly if neither exists.
bash clients/unreal/ue-plugin-build.sh
# or drive it through the parity hook:
CITADEL_UE_BUILD=1 bash scripts/check-sdk-parity.sh
```

The script generates a throwaway host project (git-ignored `.uebuild/`) that
references this plugin in place and builds its editor target, so UnrealBuildTool
+ UnrealHeaderTool compile and link the plugin (`UnrealEditor-CitadelClient.dll`)
against the engine. To link without the real native lib present, it sets
`CITADEL_FFI_STUB=1`, which compiles the in-tree `CitadelFfiStub.cpp` — a
link-only stub of the C ABI with matching signatures. This verifies the C++
**compiles and links**; it does not exercise real runtime behavior.

**Not in the fast path.** A UE build is far too slow for `bash scripts/check.sh`,
so it is **opt-in only** and never runs there. The default check keeps the
UE-free Tier-A/Tier-B (below). Real runtime behavior (linking the actual
`citadel_client_ffi` and the in-editor two-client demo) stays a **manual**
pre-release step.

### Requirements

- UE 5.8 installed (default probe `D:/Games/UE_5.8`; override `CITADEL_UE_ROOT`).
- A Visual Studio C++ toolchain UBT can find (the build uses the engine's bundled
  .NET). If UBT/UAT fails for an **environment** reason (missing toolchain, UE
  install problem, EULA prompt), that is not an SDK error — fix the environment.

## Usage sketch

```cpp
UCitadelClientSubsystem* Client =
    GetGameInstance()->GetSubsystem<UCitadelClientSubsystem>();

if (Client->ConnectQuic(TEXT("127.0.0.1:7351"), TEXT("localhost"), /*insecure*/ true)
        == ECitadelStatus::Ok)
{
    TArray<uint8> Body; // two little-endian f32 (x, y)
    Client->Send(CitadelWire::KIND_POSITION, Body, /*reliable*/ false);

    uint16 Kind = 0;
    TArray<uint8> Payload;
    while (Client->Poll(Kind, Payload) == ECitadelStatus::Ok)
    {
        // handle inbound envelope by Kind (CitadelWire::KIND_PEER_POSITION, ...)
    }
}
```

## Blueprint connection API (no-code)

`UCitadelClientSubsystem` exposes the connection surface to Blueprint, so a
designer can connect, authenticate, and drive `UCitadelTransformSync` /
`UCitadelNetworkPeer` without writing C++. Get the subsystem from the game
instance (`Get Game Instance` → `Get Subsystem (Citadel Client Subsystem)`), then:

- **Connect Quic** (Address, Server Name, Insecure) → `ECitadelStatus`.
- **Connect Web Socket** (Url) → `ECitadelStatus`.
- **Authenticate Device** / **Authenticate Custom** / **Authenticate Email**
  (Base URL, Id/email, Create,
  Username). `Base URL` is the node's HTTP origin (e.g. `http://127.0.0.1:7350`);
  these POST to `/v1/auth/device` / `/v1/auth/custom` / `/v1/auth/email` and are **async**. `Authenticate Email` also takes a password, which the subsystem only serializes into the request. Bind the
  results:
  - **On Authenticated** (Session Token, User Id, Username) — also stored on the
    read-only `Session Token` / `User Id` properties.
  - **On Authentication Failed** (Error Message).
- **Authenticate Realtime Guest** — sends the empty guest handshake on the active
  QUIC/WS connection. Call this immediately after connect for guest-only games.
- **Authenticate Realtime With Session Token** — presents a token from
  Authenticate Device/Custom on the active realtime connection.
- **Disconnect**, **Is Connected**, **Get Last Status** (`ECitadelStatus`),
  **Get Last Error** (`FString`).

The `Category` for every node is `Citadel|Connection`. The C++ API above is
unchanged; the Blueprint entry points are thin wrappers over it. `Send`/`Poll`
stay C++-only (their `uint16` envelope kind is not a Blueprint type); designers
drive traffic through the higher-level components.

> **Auth vs. transport.** Device/custom authentication is an HTTP call, so it
> needs the node's HTTP origin, not the QUIC/WS endpoint. The realtime handshake
> is a separate call on the active transport: use guest auth for anonymous play,
> or pass the HTTP session token to `Authenticate Realtime With Session Token`.

## Transform sync (, P1)

`UCitadelTransformSync` is a `UActorComponent` that mirrors a server object's
authoritative transform onto a replicated actor, rendered interpolated in the
past (Hermite position when velocity is replicated + slerp rotation, adaptive
jitter buffer, bounded extrapolation on drain). It targets the P1 roles
`RemoteInterpolated` / `ServerSimulated` / `StaticReplicated`; owner prediction
is P2. The reusable core is `FCitadelRemoteWorldView` (a faithful C++ port of the
tested Rust `RemoteWorldView`) plus `CitadelTransformWire.h` (a hand-port of the
`citadel_wire` bit reader + quantized/smallest-three codecs). A later task
replaces the hand-port decode with the shared `citadel-client-ffi` C ABI so every
engine inherits one implementation.

Component setup:

```cpp
// Once per connection, after the client subsystem has connected:
GetGameInstance()->GetSubsystem<UCitadelTransformSyncSubsystem>()->OptIn();

// On each replicated actor that mirrors a server object:
UCitadelTransformSync* Sync = Actor->CreateDefaultSubobject<UCitadelTransformSync>(TEXT("Sync"));
Sync->ObjectId = 1;                                   // the server object id
Sync->Role = ECitadelSyncRole::ServerSimulated;
Sync->bHermitePosition = true;                        // needs replicated velocity
```

The subsystem sends `KIND_TSYNC_HELLO`, drains inbound snapshot datagrams into the
shared view, and acks with `KIND_TSYNC_ACK`; each component samples its object's
interpolated transform every frame.

## Networked actors — presence + spawn

`UCitadelNetworkedActorSubsystem` is the out-of-the-box, drop-in player
replication built on top of transform sync (full reference:
[networked actors](../../website/src/content/docs/reference/client-sdk/networked-actors.md)).
Instead of pre-placing actors with fixed object ids, you register an actor class
per archetype and announce your pawn; the server spawns your avatar on every peer,
hands you everyone already present, and despawns on disconnect. Movement is
**relay** — your pawn moves with native `CharacterMovement`, and the subsystem
relays its transform to peers (who see it interpolated). Citadel spawns only the
**remote proxies**; your local pawn stays the native one.

```cpp
// Once connected (e.g. in your PlayerController/GameMode):
auto* NA = GetGameInstance()->GetSubsystem<UCitadelNetworkedActorSubsystem>();
NA->RegisterArchetype(0, BP_ThirdPersonCharacterClass); // archetype id -> class
NA->AnnouncePresence(MyPawn, /*ArchetypeId=*/0);        // relay MyPawn to peers
```

Give a spawnable actor the `ICitadelReplicated` interface (C++ or Blueprint) to
react to spawn/despawn:

```cpp
void OnCitadelSpawn(const FCitadelSpawnInfo& Info); // bIsLocalOwner, ObjectId, OwnerId, ArchetypeId
void OnCitadelDespawn();
```

- `Info.bIsLocalOwner == true` → your own player: possess it with the local
  `PlayerController`, enable input/camera.
- `Info.bIsLocalOwner == false` → a remote proxy: leave it interpolated, do **not**
  possess it with the local `PlayerController`.

No `citadel.toml` change is needed — presence is client-driven (just
`transport.transform_sync.enabled = true` on the server). This replaces the
cube-style `demo_movers` / `player_slots` demos for real gameplay actors.

### Manual two-client demo (MANUAL — C++ is not built in CI)

The palpable acceptance slice ("two Unreal clients see server-owned avatars
interpolate smoothly at 20-30 pps under injected loss + latency"):

1. **Run the server with transform sync + demo movers.** In `citadel.toml`:
   ```toml
   [transport.transform_sync]
   enabled = true
   send_rate_hz = 20
   sim_hz = 60
   demo_movers = 2
   ```
   `cargo run` — the server spawns two server-simulated avatars (object ids 1 and
   2) moving on opposing paths, and streams per-client delta snapshots.
2. **Build the SDK into a UE project** (steps above), place two actors with a
   `UCitadelTransformSync` (ObjectId 1 and 2), call `OptIn()` after connecting.
3. **Launch two clients** (two PIE windows or two packaged instances) pointed at
   the server. Each shows both avatars interpolating smoothly.
4. **Inject loss/latency** with a network conditioner (e.g. `clumsy` on Windows,
   `netem` on Linux) at ~10-30% loss and ~50-100 ms latency on the QUIC UDP port;
   the avatars must keep moving smoothly (the absolute-id baseline loop self-heals
   and the jitter buffer + bounded extrapolation absorb the gaps). The deterministic
   equivalent of this is covered in CI by `tests/transform_sync.rs`.

## Verification

- **Tier-A (constant parity):** `bash scripts/check-sdk-parity.sh` diffs
  `CitadelWire.h`'s `constexpr` values against `crates/citadel-wire/contract.json`.
  The transform-sync kinds (`KIND_TSYNC_HELLO/SNAPSHOT/ACK/ROLE`) are now claimed
  and parity-guarded.
- **Tier-B (native signature parity, UE-free):** the same script runs
  `parity-hook.sh`, which compiles `tier_b/citadel_parity_tu.cpp` against
  `citadel_client.h` (object-only, no link/native lib needed). A C ABI signature
  change breaks the compile. If no C/C++ compiler is on the runner, the hook
  reports a clean **SKIP** and does not fail the build. This is in the fast
  `scripts/check.sh` path.
- **Tier-B (real UE compile, gated/opt-in):** `ue-plugin-build.sh` compiles the
  plugin against real UE 5.8 headers — the only check that exercises the
  UObject/UHT reflection and UE-dependent code. Run it directly or via
  `CITADEL_UE_BUILD=1 bash scripts/check-sdk-parity.sh`. It is **not** in the
  default `check.sh` (too slow) and SKIPs cleanly when no UE root is available.
  See [Compile verification](#compile-verification-against-ue-58).
- **Runtime-load guard (fast path):** `parity-hook.sh` statically asserts the
  module contains an `IMPLEMENT_MODULE(...)`. Without it the DLL compiles + links
  (the gated UE build passes) but the editor fails at load with "module
  CitadelClient could not be initialized successfully after it was loaded" — the
  compile-verify never loads the module, so this cheap check guards the regression.
- **Manual (not automatable):** linking the real `citadel_client_ffi` native lib
  and an in-editor PIE run of the sample (behavioral correctness of the wrapper).
  This stays a manual pre-release checklist item.
