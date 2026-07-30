---
title: CLI reference
description: The citadel command-line interface — serve, check, and global flags.
---

The `citadel` binary has two subcommands and a small set of global flags. Run it
with `cargo run -- <flags> <subcommand>` from the workspace.

```
citadel [GLOBAL FLAGS] [COMMAND]
```

The subcommand is **optional**. The default command is `serve`, so a bare
invocation starts the server — `citadel` is equivalent to `citadel serve`. This
is the "unzip and run" story: with the shipped `citadel.toml` at the repo root,
`cargo run` alone stands up a standalone server.

## Commands

### `serve` (default)

Load and validate configuration, then start the server. It builds a Tokio
runtime, binds the HTTP listener, and starts every realtime transport that the
config enables, all sharing one cancellation token so they stop together on
shutdown. Running the binary with no subcommand runs `serve`.

```bash
# All three are equivalent when serve is the intended command:
cargo run
cargo run -- serve
cargo run -- --config examples/configs/demo.toml serve
```

Once the server is ready — after the HTTP listener is bound and every enabled
transport has started — it prints a boxed startup banner to stdout (see
[Startup banner](#startup-banner)) and serves until `Ctrl-C`.

### `check`

Load and validate configuration **without** starting any listener. Unlike the
default `serve`, `check` never binds a socket, never starts the first-run
wizard, and never prints the startup banner. On success it prints a concise,
non-secret summary and exits. Useful in CI and pre-flight checks.

```bash
cargo run -- --config examples/configs/demo.toml check
```

Example output:

```
config ok: node_id=dev-1 bind=127.0.0.1:7350 log_level=info log_format=pretty
```

Validation failures surface as configuration errors and name the offending field
without echoing secrets.

## Global flags

These flags apply to both subcommands. They are narrow, high-signal overrides;
most settings belong in the [config file](/reference/operations/configuration/) or
environment variables.

| Flag | Value | Overrides | Description |
| --- | --- | --- | --- |
| `--config` | `PATH` | — | Path to a TOML config file. |
| `--log-level` | `LEVEL` | `logging.level` | Log level directive, e.g. `info`, `debug`. |
| `--bind` | `ADDR` | `http.bind` | HTTP bind address, e.g. `127.0.0.1:7350`. |
| `--node-id` | `ID` | `server.node_id` | Node identifier. |
| `--yes` | — | — | Assume "yes" to first-run prompts and run non-interactively. Aliased as `--non-interactive`. |
| `--version` | — | — | Print the version. |
| `--help` | — | — | Print help. |

`--yes` (alias `--non-interactive`) suppresses the [first-run wizard](#first-run-wizard):
the server takes the existing silent auto-defaults instead of prompting. It is
useful for scripted or headless starts on an interactive terminal, where the
wizard would otherwise open.

## Configuration precedence

Configuration is resolved in this order, each layer overriding the previous:

1. Built-in defaults.
2. The config file (`--config`).
3. `CITADEL_`-prefixed environment variables.
4. CLI flag overrides.

Supported environment variables: `CITADEL_LOG_LEVEL`, `CITADEL_HTTP_BIND`,
`CITADEL_NODE_ID`, `CITADEL_PUBLIC_ADDR`. Unknown `CITADEL_` variables are
ignored. See the [configuration reference](/reference/operations/configuration/).

## Startup banner

When `serve` finishes coming up — the HTTP listener is bound and every enabled
transport has started — the server prints a single boxed ASCII banner to stdout,
right before it begins accepting connections. It is written to stdout regardless
of the configured log level or format, so it is the prominent, readable output
on a normal run.

The banner shows:

- a `CITADEL` wordmark,
- a summary line: the version, node id, and the selected database backend
  (`in-memory`, `sqlite`, `postgres`, `cockroach`, or `mongodb` — never the
  database URL), and
- an aligned list of links.

The links always include the HTTP endpoints, followed by one line per **enabled**
realtime transport with its bind address. Disabled transports do not appear.

| Label | Value |
| --- | --- |
| `Dashboard` | `http://<http.bind>/dashboard` |
| `Status` | `http://<http.bind>/status` |
| `Health` | `http://<http.bind>/health` |
| `QUIC` | `udp://<transport.quic.bind>` (only if QUIC is enabled) |
| `WebSocket` | `ws://<transport.websocket.bind>` (only if WebSocket is enabled) |
| `WebTransport` | `https://<transport.webtransport.bind>` (only if WebTransport is enabled) |

Detailed startup diagnostics (listener bind, per-transport init) are logged at
`debug` so the banner stays the focus of a normal run. To see the full startup
trace, raise the log level with `--log-level debug`, `logging.level = "debug"`,
or `CITADEL_LOG_LEVEL=debug`.

## First-run wizard

On an **interactive terminal**, `serve` runs a short first-run wizard before it
starts the runtime. It runs only when **all** of these hold:

- stdin is a real TTY,
- no `--config` was given, and
- neither `--yes` nor `--non-interactive` was passed.

When it runs, it offers two setup steps, and only for things that are not already
present:

1. **Gameplay script** — if the runtime is enabled and no `game/main.*`
   entrypoint exists, it offers to scaffold a minimal starter Lua script (or
   Python when the binary is built with `runtime-python`) with a position relay
   plus a `ping` RPC.
2. **Database** — if no database is configured, it offers a choice:
   - **SQLite** (default) — writes `sqlite://data.sqlite`, an embedded
     single-file database with no external server.
   - **PostgreSQL** — you supply a connection URL.
   The choice is persisted into `citadel.toml` so the **next** run is
   non-interactive.

The wizard is idempotent: it never re-prompts for a script or database that
already exists. In every non-interactive case — stdin is not a terminal
(CI/headless), `--config` is given, or `--yes`/`--non-interactive` is passed — it
is skipped entirely and the server falls back to its silent auto-defaults.

:::note
The repository already ships a `citadel.toml` and a `game/main.lua`, so cloning
the repo and running `cargo run` never triggers the wizard. The wizard is for
starting from an empty directory.
:::
