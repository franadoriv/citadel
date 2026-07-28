# Citadel — standalone Linux server (x86_64)

This archive is ready to run on **64-bit x86 Linux**. It contains a statically
linked `musl` server binary, so you do not need Rust, Cargo, or a particular
glibc version.

```text
citadel-linux-x86_64-musl-v<version>/
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

This download is for x86_64/AMD64 Linux only. ARM64 Linux hosts need an ARM64
release binary; source builds remain available for unsupported architectures.
