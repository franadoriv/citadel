Citadel server — standalone package
===================================

This folder is a ready-to-run Citadel game server. Layout:

  citadel.exe        The server binary.
  citadel.toml       Configuration (edit and restart to change behavior).
  scripts/
    main.lua         Your game logic. Edit it; with hot-reload on it reloads
                     live on save (no restart). See the comments inside.

Run it
------

  Windows:   .\citadel.exe serve
  macOS/Linux: ./citadel serve

On first run the server creates its SQLite database, applies migrations, loads
scripts/main.lua, and starts listening (HTTP + realtime transports per
citadel.toml). Point a Citadel client SDK at it, or open the admin console at
the dashboard URL printed on startup.

Configuration
-------------

Everything is in citadel.toml. Common knobs:
  [runtime] scripts_dir  -> where your Lua lives (defaults to ./scripts here)
  [runtime] tick_hz      -> server tick rate (0 disables citadel.on_tick)
  [runtime] hot_reload   -> live-reload scripts/main.lua on save
  [database]             -> SQLite by default; point at Postgres for production

Docs: https://citadel.dev  (or the docs/ folder in the Citadel repo).
