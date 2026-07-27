Citadel Python server - standalone package
==========================================

This folder is a ready-to-run Citadel game server with embedded CPython enabled.
Layout:

  citadel.exe        The Python-enabled server binary.
  python313.dll      The bundled CPython dynamic library (version may vary).
  python/
    Lib/             Bundled Python standard library.
    DLLs/            Bundled native Python extension modules.
  citadel.toml       Configuration. runtime.language = "python" and
                     runtime.scripts_dir = "./scripts" are already set.
  scripts/
    main.py          Your game logic. Edit it; with hot-reload on it reloads
                     live on save (no restart). See the comments inside.

Run it
------

  Windows: .\citadel.exe serve

On first run the server creates its SQLite database, applies migrations, loads
scripts/main.py through the bundled CPython runtime, and starts listening
(HTTP + realtime transports per citadel.toml).

Configuration
-------------

Everything is in citadel.toml. Common knobs:
  [runtime] scripts_dir  -> where your Python game logic lives
  [runtime] tick_hz      -> server tick rate (0 disables citadel.on_tick)
  [runtime] hot_reload   -> live-reload scripts/main.py on save
  [database]             -> SQLite by default; point at Postgres for production

The default Lua package stays lean and does not include CPython. Use the
Python-specific package target only when you want runtime.language = "python".

Docs: https://citadel.dev  (or the docs/ folder in the Citadel repo).
