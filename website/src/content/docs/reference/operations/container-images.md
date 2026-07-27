---
title: Container images
description: Local Citadel Docker image contract and support boundary.
---

Citadel does not publish OCI images from its release workflow. Current releases
provide Windows ZIP assets; Docker is a local development and operator-managed
build path. Historical container images are not updated
or verified by later releases.

## Build a local image

Build from the Citadel checkout at the revision you intend to run:

```bash
docker build --tag citadel:local .
```

The supplied Compose sample uses that tag by default. Copy
`examples/docker/` to your game project, set a long
`CITADEL_CONSOLE_PASSWORD` in `.env`, then start it with:

```bash
docker compose up -d
```

Set `CITADEL_IMAGE` only if you built and want to use a different local tag. The
[Docker guide](/guides/docker/) documents the full setup, safe mount layout,
loopback-only ports, Lua polling hot reload, and the named SQLite volume.

## Runtime contract

The image is intentionally generic and contains no game project. It runs as a
non-root `citadel` user and expects mutable operator state to be mounted at:

| Container path | Purpose |
| --- | --- |
| `/citadel/config/citadel.toml` | Operator configuration. |
| `/citadel/game` | Lua game logic. |
| `/citadel/maps` | Cooked map files. |
| `/citadel/data` | SQLite database and Citadel-owned persistent data. |

The default image build includes the shipped Lua runtime only. Feature-gated
Python and JavaScript runtime builds are separate, operator-managed images; they
are not enabled by the default local tag.

## Verification and support boundary

Repository checks statically validate the Dockerfile, Compose/config paths,
ports, non-root user, signal contract, and build context without requiring a
Docker daemon. A maintainer may run the end-to-end local smoke test:

```bash
bash scripts/smoke-container.sh
```

That test builds a temporary local image and verifies health, Lua reload,
persistence, and graceful `SIGTERM` shutdown. It is not a release-CI gate and
does not create, attest, or publish a registry image.
