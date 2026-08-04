#!/usr/bin/env bash
# Run the MongoDB-backed repository contracts against a real, disposable,
# authenticated single-node replica set. Linux Docker is required because the
# advertised replica-set endpoint is a host-network loopback address.
set -euo pipefail

image="${CITADEL_MONGODB_TEST_IMAGE:-mongo:8.0}"
port="${CITADEL_MONGODB_TEST_PORT:-27017}"
container="citadel-mongodb-test-$$"
temp_dir="$(mktemp -d)"
password_file="$temp_dir/password"

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$temp_dir"
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "Docker is required" >&2; exit 1; }
docker info >/dev/null || { echo "Docker daemon is unavailable" >&2; exit 1; }
if ! docker image inspect "$image" >/dev/null 2>&1; then
  docker pull "$image"
fi
if [[ ! "$port" =~ ^[0-9]{1,5}$ ]] || ((10#$port == 0 || 10#$port > 65535)); then
  echo "CITADEL_MONGODB_TEST_PORT must be a TCP port from 1 through 65535" >&2
  exit 1
fi
if ss -ltn "sport = :$port" | grep -q LISTEN; then
  echo "MongoDB test port $port is already in use; set CITADEL_MONGODB_TEST_PORT" >&2
  exit 1
fi

umask 077
# MongoDB URI userinfo reserves characters such as +, /, and =.  Generate the
# disposable password as hexadecimal so it is safe both in the URI and in the
# JavaScript createUser argument without ever printing it.
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$password_file"
password="$(cat "$password_file")"

docker run -d --rm --name "$container" --network host \
  --tmpfs /data/db:rw,size=1g \
  --entrypoint bash "$image" -ceu '
    umask 077
    head -c 756 /dev/urandom | base64 | tr -d "\n" > /run/citadel-keyfile
    chown mongodb:mongodb /run/citadel-keyfile
    chmod 400 /run/citadel-keyfile
    chown mongodb:mongodb /data/db
    exec gosu mongodb mongod --replSet rs0 --bind_ip 127.0.0.1 --port "'$port'" --setParameter enableTestCommands=1 \
      --auth --keyFile /run/citadel-keyfile
  ' >/dev/null

ready=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet --port "$port" --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null | grep -qx 1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$container" >&2 || true
  echo "MongoDB did not become ready" >&2
  exit 1
fi
docker exec "$container" mongosh --quiet --port "$port" --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:'"$port"'"}]})' >/dev/null
primary=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet --port "$port" --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -qx true; then
    primary=true
    break
  fi
  sleep 1
done
if [[ "$primary" != true ]]; then
  docker logs "$container" >&2 || true
  echo "MongoDB replica set did not elect a primary" >&2
  exit 1
fi
docker exec "$container" mongosh --quiet --port "$port" --eval \
  'db.getSiblingDB("admin").createUser({user:"citadel_test",pwd:"'"$password"'",roles:[{role:"root",db:"admin"}]})' >/dev/null

export CITADEL_TEST_MONGODB_URL="mongodb://citadel_test:${password}@127.0.0.1:${port}/citadel_test?authSource=admin&replicaSet=rs0"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"

# Run every contract target that has a MongoDB branch while the authenticated
# rs0 URL is set. The un-gated reference cases in those targets are intentional:
# the Mongo-specific case must still execute rather than silently skip.
cargo test --test mongodb_foundation -- --nocapture
for target in \
  storage_repository_contract \
  identity_session_reference_impls \
  mongodb_identity_unlink_repository \
  friends_repository_contract \
  groups_repository_contract \
  leaderboards_repository_contract \
  notifications_repository_contract \
  wallet_repository_contract \
  chat_repository_contract \
  database_explorer_contract \
  http_auth \
  http_player \
  multi_node_chat; do
  cargo test --test "$target" -- --nocapture
done

# Backup/restore is an executable operational contract, not prose.  Keep its
# fixture separate from Citadel collections and restore it under a different
# database name so source data is never overwritten during verification.
backup_collection="citadel_backup_restore_probe"
backup_source_db="citadel_test"
backup_restore_db="citadel_restore_verify"
backup_archive="/tmp/citadel-backup-restore-$$.archive"
docker exec -e CITADEL_TEST_PASSWORD="$password" "$container" mongosh --quiet --port "$port" --eval '
  const admin = db.getSiblingDB("admin");
  admin.auth("citadel_test", process.env.CITADEL_TEST_PASSWORD);
  const source = db.getSiblingDB("citadel_test");
  source.getCollection("citadel_backup_restore_probe").deleteMany({});
  source.getCollection("citadel_backup_restore_probe").insertMany([
    {_id: "backup-1", payload: {kind: "integrity", value: 17}},
    {_id: "backup-2", payload: {kind: "integrity", value: 29}}
  ]);
' >/dev/null
docker exec -e CITADEL_TEST_PASSWORD="$password" "$container" bash -ceu '
  uri="mongodb://citadel_test:${CITADEL_TEST_PASSWORD}@127.0.0.1:'"$port"'/citadel_test?authSource=admin&replicaSet=rs0"
  # Keep tool output out of CI logs: the disposable URI carries credentials.
  mongodump --uri="$uri" --db="'"$backup_source_db"'" --collection="'"$backup_collection"'" \
    --archive="'"$backup_archive"'" >/dev/null 2>&1
  mongorestore --uri="$uri" --archive="'"$backup_archive"'" \
    --nsFrom="'"$backup_source_db"'.*" --nsTo="'"$backup_restore_db"'.*" --drop >/dev/null 2>&1
  rm -f "'"$backup_archive"'"
' >/dev/null
integrity="$(docker exec -e CITADEL_TEST_PASSWORD="$password" "$container" mongosh --quiet --port "$port" --eval '
  const admin = db.getSiblingDB("admin");
  admin.auth("citadel_test", process.env.CITADEL_TEST_PASSWORD);
  const docs = db.getSiblingDB("citadel_restore_verify").getCollection("citadel_backup_restore_probe")
    .find({}, {_id: 1, payload: 1}).sort({_id: 1}).toArray();
  print(JSON.stringify(docs));
')"
[[ "$integrity" == '[{"_id":"backup-1","payload":{"kind":"integrity","value":17}},{"_id":"backup-2","payload":{"kind":"integrity","value":29}}]' ]] || {
  echo "MongoDB backup/restore integrity verification failed" >&2
  exit 1
}
echo "MongoDB backup/restore integrity verification passed"
