#!/usr/bin/env bash
set -euo pipefail

# Container assets are intentionally validated without a Docker daemon so the
# regular repository gate catches accidental drift on every platform. A real
# Docker engine can run the optional local build/Compose smoke, but release CI
# deliberately does not build, test, or publish container images.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
  echo "check-container-assets: $*" >&2
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "missing required file: $1"
}

require_text() {
  local file="$1"
  local text="$2"
  grep -Fq -- "$text" "$file" || fail "$file is missing required text: $text"
}

reject_text() {
  local file="$1"
  local text="$2"
  if grep -Fq -- "$text" "$file"; then
    fail "$file contains forbidden text: $text"
  fi
}

require_file Dockerfile
require_file .dockerignore
require_file examples/docker/compose.yaml
require_file examples/docker/.env.example
require_file examples/docker/citadel.toml
require_file examples/docker/game/main.lua
require_file examples/docker/maps/.gitkeep
require_file scripts/smoke-container.sh
require_file .github/workflows/release.yml
require_file website/src/content/docs/guides/docker.md
require_file website/src/content/docs/reference/operations/container-images.md

for allowed in '**' '!Cargo.toml' '!Cargo.lock' '!rust-toolchain.toml' '!src/**' '!crates/**' '!migrations/**' '!migrations-crdb/**' '!migrations-sqlite/**' '!examples/docker/citadel.toml'; do
  require_text .dockerignore "$allowed"
done

require_text Dockerfile 'FROM --platform=$TARGETPLATFORM rust:${RUST_VERSION}-bookworm AS builder'
require_text Dockerfile 'ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}'
require_text Dockerfile 'build-essential cmake libclang-dev'
require_text Dockerfile 'cargo build --locked --release --bin citadel'
require_text Dockerfile 'USER citadel'
require_text Dockerfile 'EXPOSE 7350/tcp 7351/udp 7352/tcp 7353/udp'
require_text Dockerfile 'http://127.0.0.1:7350/health'
require_text Dockerfile 'ENTRYPOINT ["/usr/bin/tini", "--", "/citadel/citadel"]'

require_text examples/docker/compose.yaml 'CITADEL_CONSOLE_PASSWORD:'
require_text examples/docker/compose.yaml './citadel.toml:/citadel/config/citadel.toml:ro'
require_text examples/docker/compose.yaml './game:/citadel/game:ro'
require_text examples/docker/compose.yaml './maps:/citadel/maps:ro'
require_text examples/docker/compose.yaml 'citadel-data:/citadel/data'
require_text examples/docker/compose.yaml '127.0.0.1:7351:7351/udp'
require_text examples/docker/compose.yaml 'stop_grace_period: 30s'

for bind in '0.0.0.0:7350' '0.0.0.0:7351' '0.0.0.0:7352' '0.0.0.0:7353'; do
  require_text examples/docker/citadel.toml "$bind"
done
for path in '/citadel/data/data.sqlite' '/citadel/game' '/citadel/maps'; do
  require_text examples/docker/citadel.toml "$path"
done
require_text examples/docker/citadel.toml 'hot_reload = true'
require_text examples/docker/citadel.toml 'language = "lua"'
require_text src/http/mod.rs 'tokio::signal::unix::SignalKind::terminate()'

require_text .github/workflows/release.yml 'needs: [package, godot-web]'
# QEMU user-mode references are allowed exclusively for ARM smoke tests, never
# to build, test, or publish containers.
for forbidden in 'packages: write' 'Docker' 'docker' 'Buildx' 'buildx' \
  'container' 'GHCR' 'ghcr' 'OCI' 'oci'; do
  reject_text .github/workflows/release.yml "$forbidden"
done
require_text scripts/smoke-container.sh 'CITADEL_SMOKE_PULL_IMAGE'

bash -n scripts/smoke-container.sh

echo "check-container-assets: OK"
