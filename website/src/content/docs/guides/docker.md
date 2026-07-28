---
title: Run Citadel with Docker
description: Build and run Citadel locally in Docker while editing configuration and Lua game logic on the host with hot reload.
---

Citadel's Docker workflow builds a local Linux OCI image from the Citadel
checkout. You do **not** need Rust, CMake, or a native Citadel binary on your
host. Your configuration, game logic, and maps remain editable files in your
game project; the server binary and its runtime dependencies stay inside the
image.

Release CI does not build, test, or publish OCI images. Build the image from the
revision you intend to run; historical GHCR images are not updated by releases.

## 1. Build the local image and copy the sample into your game project

From the Citadel checkout, build a local image tagged `citadel:local`:

```bash
docker build --tag citadel:local .
```

The repository ships a complete Compose folder at `examples/docker/`.

```bash
mkdir -p ~/games/my-citadel-server
cp -R examples/docker/. ~/games/my-citadel-server/
cd ~/games/my-citadel-server
cp .env.example .env
```

```powershell
New-Item -ItemType Directory -Force C:\Games\my-citadel-server | Out-Null
Copy-Item -Recurse examples\docker\* C:\Games\my-citadel-server
Copy-Item examples\docker\.env.example C:\Games\my-citadel-server\.env
Set-Location C:\Games\my-citadel-server
```

Set a long unique console password in `.env`. The Compose sample defaults to the
local image; set `CITADEL_IMAGE` only when you intentionally built a different
local tag:

```dotenv
CITADEL_CONSOLE_PASSWORD=use-a-long-unique-password-here
# CITADEL_IMAGE=citadel:local
```

:::caution

Never expose the dashboard using Citadel's default `admin` / `password`
credentials. The Compose sample refuses to start until
`CITADEL_CONSOLE_PASSWORD` is set.

:::

## 2. Start the server

```bash
docker compose up
```

The server becomes healthy at [http://127.0.0.1:7350/health](http://127.0.0.1:7350/health).
The dashboard is at [http://127.0.0.1:7350/dashboard](http://127.0.0.1:7350/dashboard).

The sample mounts only these project-owned inputs:

| Host path | Container path | Access | Purpose |
| --- | --- | --- | --- |
| `citadel.toml` | `/citadel/config/citadel.toml` | read-only | Operator configuration. |
| `game/` | `/citadel/game` | read-only | Lua game logic. |
| `maps/` | `/citadel/maps` | read-only | Cooked `.map` geometry. |
| Docker volume `citadel-data` | `/citadel/data` | read-write | SQLite database and server-owned state. |

Your game assets are never copied into the local image. SQLite survives
`docker compose down` and a later `up`; add `-v` only when you intentionally
want to erase local state.

## 3. Edit game logic with hot reload

The sample enables Lua hot reload with a 500 ms poll interval. Edit
`game/main.lua` while Compose is running, for example change the string in:

```lua
citadel.log("Docker Lua game logic loaded", "info")
```

Save the file and watch the Compose logs: Citadel loads a fresh Lua runtime. To
verify failure safety, temporarily make an invalid Lua edit; Citadel logs the
rejection and continues serving the last valid game logic. Restore a valid file
to reload again.

This uses polling rather than host filesystem events, so it works with Linux
bind mounts and Docker Desktop file sharing on macOS and Windows.

## Ports and public addresses

The image listens on all container interfaces. Compose publishes the sample
ports only on host loopback:

| Surface | Port | Transport |
| --- | ---: | --- |
| HTTP, health, status, dashboard | 7350 | TCP |
| Native QUIC | 7351 | UDP |
| WebSocket | 7352 | TCP |
| WebTransport / HTTP/3 | 7353 | UDP |

For a LAN or internet-facing deployment, deliberately change the Compose port
bindings and set `server.public_addr` in `citadel.toml` to the real public DNS
name/IP and port. Do not use `0.0.0.0` as `public_addr`: it is a bind address,
not an address clients can reach. Put TLS termination, firewalling, secrets, and
database credentials under your normal deployment controls.

## Stop and upgrade

```bash
docker compose down

# Rebuild the local image from the desired Citadel revision, then:
docker build --tag citadel:local .
docker compose up -d
```

Citadel receives Docker's `SIGTERM`, stops its HTTP and realtime transports
through the normal graceful cancellation path, and Compose allows 30 seconds for
that shutdown. The named `citadel-data` volume stays intact across upgrades.

## Maintainer smoke test

From a Git Bash/Linux shell with Docker running, maintainers can execute the
full local image smoke when changing or validating the container path:

```bash
bash scripts/smoke-container.sh
```

It builds `citadel:smoke`, then uses its own temporary mounts, loopback port,
and named volume to check health, valid and rejected Lua reloads, SQLite across
a restart, and a clean Docker `SIGTERM` stop. It removes only those temporary
resources. To test an already-built image without rebuilding it:

```bash
CITADEL_IMAGE=citadel:smoke CITADEL_SMOKE_SKIP_BUILD=1 bash scripts/smoke-container.sh
```

## Limits

- The default image contains the shipped **Lua** runtime only. Python and
  JavaScript/TypeScript images are future versioned artifacts, not aliases for
  this tag.
- The image is a Linux container: it runs on Linux directly and through Docker
  Desktop on macOS (Intel or Apple Silicon) and Windows. It is not a native
  macOS server archive; Citadel ships standalone Windows and x86_64 Linux ZIPs.
- A local Docker engine smoke test is optional and does not run in release CI.
  If Docker Desktop is stopped, static repository checks still validate the
  image and Compose contract but cannot prove a container started.
