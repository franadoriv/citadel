# Citadel — standalone Linux server

This archive is ready to run on the architecture named in its filename. It contains a statically
linked `musl` server binary, so you do not need Rust, Cargo, or a particular
glibc version.

```text
citadel-linux-<arch>-v<version>/
├── citadel          # server binary
├── citadel.toml     # editable configuration
├── scripts/main.lua # starter game logic (hot reload enabled)
└── maps/            # put cooked level geometry here
```

## Run it

Extract the archive, open a terminal in the extracted directory, and run:

```bash
./citadel
```

On the first start Citadel creates `data.sqlite`, applies migrations, and
starts the HTTP and realtime listeners configured in `citadel.toml`. Open the
dashboard URL printed on startup (normally `http://127.0.0.1:7350/dashboard`).

To accept connections from other machines, change the `[http]` and
`[transport.*]` bind addresses in `citadel.toml` from `127.0.0.1` to
`0.0.0.0`, then restart.

Choose the archive matching your host: `x86_64-musl` for AMD64/x86_64 and
`aarch64-musl` for 64-bit ARM (AWS Graviton, Oracle ARM, and Raspberry Pi).
Source builds remain available for unsupported architectures.
