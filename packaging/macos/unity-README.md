# Citadel Unity client plugin for macOS

This folder is a drop-in Unity plugin. It contains the C# bindings, demo
components, and a `libcitadel_client_ffi.dylib` built for the architecture named
in the archive.

```text
clients/unity/
├── Citadel/                 # managed C ABI bindings and protocol helpers
├── Demo/                    # connection, peer, movement, and RPC components
└── Plugins/macOS/
    └── libcitadel_client_ffi.dylib
```

## Import

1. Copy `Citadel/`, `Demo/`, and `Plugins/` under your project's `Assets/`
   directory.
2. Select `Plugins/macOS/libcitadel_client_ffi.dylib` in Unity's Inspector and
   enable **macOS** plus the matching CPU architecture (**Apple Silicon** or
   **Intel 64-bit**). Let Unity generate the `.meta` file on import.
3. Start the Citadel server, add `CitadelConnection`, `LocalPlayer`, and
   `PeerManager` as described in the repository's Unity guide, then press Play.

The C# binding loads `citadel_client_ffi` without a suffix, so Unity selects the
native `.dylib`. Do not place both architecture-specific archives in the same
`Plugins/macOS/` directory: download the package that matches the Unity editor or
target architecture.

For a publicly downloaded package, retain the signed/notarized native library;
replacing it with an unsigned locally-built dylib changes its Gatekeeper status.
