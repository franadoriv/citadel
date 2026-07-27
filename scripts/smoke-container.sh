#!/usr/bin/env bash
set -euo pipefail

# End-to-end Docker smoke for the release-image contract. It deliberately uses
# a temporary game/config directory, dynamically allocated loopback HTTP ports,
# and one uniquely named volume; cleanup never prunes shared Docker resources.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

image="${CITADEL_IMAGE:-citadel:smoke}"
skip_build="${CITADEL_SMOKE_SKIP_BUILD:-0}"
pull_image="${CITADEL_SMOKE_PULL_IMAGE:-0}"
prefix="citadel-container-smoke-$$"
first_container="${prefix}-first"
second_container="${prefix}-second"
volume="${prefix}-data"
workspace=""

fail() {
  echo "smoke-container: $*" >&2
  exit 1
}

cleanup() {
  set +e
  docker rm -f "$first_container" "$second_container" >/dev/null 2>&1
  docker volume rm "$volume" >/dev/null 2>&1
  [[ -z "$workspace" ]] || rm -rf "$workspace"
}
trap cleanup EXIT

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

require_command docker
require_command curl
require_command mktemp

# Docker Desktop is invoked through Git Bash on Windows. Convert the host-side
# bind source once, then suppress MSYS rewriting so `/citadel/...` stays a
# container path instead of becoming a host path. Other Unix shells retain their
# normal Docker invocation unchanged.
docker_mount_source() {
  case "$(uname -s)" in
    MINGW*|MSYS*) cygpath -w "$1" ;;
    *) printf '%s' "$1" ;;
  esac
}

docker_run() {
  case "$(uname -s)" in
    MINGW*|MSYS*) MSYS_NO_PATHCONV=1 docker run "$@" ;;
    *) docker run "$@" ;;
  esac
}

if [[ "$skip_build" != "1" ]]; then
  docker build --tag "$image" .
else
  if [[ "$pull_image" == "1" ]]; then
    docker pull "$image"
  fi
  docker image inspect "$image" >/dev/null || fail "image is unavailable: $image"
fi

workspace="$(mktemp -d "${TMPDIR:-/tmp}/citadel-container-smoke.XXXXXX")"
cp examples/docker/citadel.toml "$workspace/citadel.toml"
cp -R examples/docker/game "$workspace/game"
mkdir -p "$workspace/maps"
docker volume create "$volume" >/dev/null

start_container() {
  local name="$1"
  docker_run --detach --name "$name" \
    --env 'CITADEL_CONSOLE_PASSWORD=container-smoke-password-not-for-production' \
    --publish '127.0.0.1::7350/tcp' \
    --volume "$(docker_mount_source "$workspace/citadel.toml"):/citadel/config/citadel.toml:ro" \
    --volume "$(docker_mount_source "$workspace/game"):/citadel/game:ro" \
    --volume "$(docker_mount_source "$workspace/maps"):/citadel/maps:ro" \
    --volume "$volume:/citadel/data" \
    "$image" >/dev/null
}

endpoint_for() {
  local name="$1"
  local mapping
  mapping="$(docker port "$name" 7350/tcp | head -n 1)"
  [[ -n "$mapping" ]] || fail "Docker did not expose HTTP for $name"
  printf 'http://127.0.0.1:%s' "${mapping##*:}"
}

wait_for_health() {
  local name="$1"
  local endpoint
  endpoint="$(endpoint_for "$name")"
  for _ in $(seq 1 60); do
    if curl --fail --silent --show-error --connect-timeout 2 "$endpoint/health" >/dev/null; then
      echo "$endpoint"
      return 0
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  fail "health endpoint did not respond for $name"
}

log_count() {
  local name="$1"
  local text="$2"
  docker logs "$name" 2>&1 | grep --fixed-strings --count "$text" || true
}

wait_for_log_count() {
  local name="$1"
  local text="$2"
  local expected="$3"
  local count
  for _ in $(seq 1 30); do
    count="$(log_count "$name" "$text")"
    if [[ "$count" -ge "$expected" ]]; then
      return 0
    fi
    sleep 0.5
  done
  docker logs "$name" >&2 || true
  fail "did not observe $expected occurrence(s) of '$text' for $name"
}

stop_cleanly() {
  local name="$1"
  local exit_code
  docker stop --time 30 "$name" >/dev/null
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$name")"
  [[ "$exit_code" == "0" ]] || fail "$name exited with code $exit_code after Docker SIGTERM"
}

start_container "$first_container"
first_endpoint="$(wait_for_health "$first_container")"
curl --fail --silent --show-error "$first_endpoint/health" >/dev/null
# Git Bash otherwise rewrites `/bin/sh` as a host path before Docker receives
# it. Disable that rewriting only for the process executed inside the container.
MSYS_NO_PATHCONV=1 docker exec "$first_container" /bin/sh -c 'test -e /citadel/data/data.sqlite'

# A valid edit reloads the mounted Lua file. The runtime emits this exact event
# only when it swaps in a newly constructed VM.
printf '\n-- container smoke valid reload marker\n' >> "$workspace/game/main.lua"
wait_for_log_count "$first_container" 'hot-reload: swapped in the updated script' 1

# A broken mounted edit is rejected without killing the current process. The
# following restore must successfully reload again, proving the watcher recovers.
cp "$workspace/game/main.lua" "$workspace/main.lua.valid"
printf 'this is not valid lua ==\n' > "$workspace/game/main.lua"
wait_for_log_count "$first_container" 'hot-reload: new script rejected (parse/registration error); keeping the current script' 1
curl --fail --silent --show-error "$first_endpoint/health" >/dev/null
cp "$workspace/main.lua.valid" "$workspace/game/main.lua"
wait_for_log_count "$first_container" 'hot-reload: swapped in the updated script' 2

# Docker stop sends SIGTERM. Start a second process over the same named volume
# to prove the SQLite state survives the graceful shutdown and restart.
stop_cleanly "$first_container"
start_container "$second_container"
second_endpoint="$(wait_for_health "$second_container")"
curl --fail --silent --show-error "$second_endpoint/health" >/dev/null
MSYS_NO_PATHCONV=1 docker exec "$second_container" /bin/sh -c 'test -e /citadel/data/data.sqlite'
stop_cleanly "$second_container"

echo "smoke-container: OK ($image)"
