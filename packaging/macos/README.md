# Citadel — standalone macOS server

This is a self-contained Citadel game server for the architecture named in the
archive. Unzip it anywhere, then run one executable — on first start it creates
its SQLite database, game folder, and migrations. No database server or install
step is required.

```text
citadel/
├── citadel             # the server (make executable if the unzipper cleared it)
├── citadel.toml        # editable config, discovered automatically
├── game/               # Lua game logic, auto-created and hot-reloaded
├── data.sqlite         # auto-created and migrated on first run
└── clients/unity/      # Unity C# bindings + native macOS library
```

## 1. Run the server

In Terminal, from this folder:

```bash
chmod +x ./citadel  # harmless if the executable bit was preserved
./citadel
```

With no arguments the server discovers `citadel.toml`, creates `data.sqlite`,
applies migrations, creates `game/`, and starts listening. Accounts and sessions
survive restarts in the local SQLite database.

The default binds are loopback-only. To accept connections from another machine,
edit the `[http]` and `[transport.*]` `bind` values in `citadel.toml` to
`0.0.0.0:<port>` and restart.

## 2. Gatekeeper and signed releases

Developer-built archives are intentionally unsigned. A published macOS release
is expected to be Developer-ID signed and notarized; if Gatekeeper says the
archive was not notarized, use a release that says it was notarized rather than
disabling Gatekeeper globally. Inspect a downloaded executable with:

```bash
codesign --verify --deep --strict --verbose=2 ./citadel
spctl --assess --type execute --verbose ./citadel
```

## 3. Open the dashboard and write game logic

Open <http://127.0.0.1:7350/dashboard> after starting the server. Edit
`game/main.lua` for server-authoritative messages, lifecycle handlers, ticks,
and RPCs. With `runtime.hot_reload = true`, a valid edit reloads without a
restart; a broken edit leaves the previous script serving.

See `clients/unity/README.md` to import the included Unity bindings, or download
the dedicated Unity, Unreal, or Godot package matching this macOS architecture.
