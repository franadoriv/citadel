---
title: Run a two-node matchmaker
description: Configure durable leases and mutual TLS so tickets can form and redeem across Citadel nodes.
---

This guide runs the same `matchmaker.add`, `matchmaker.status`,
`matchmaker.cancel`, and `matchmaker.accept` client workflow across two Citadel
nodes. The client remains connected to its original **session node**; Citadel
forwards only typed matchmaker commands over a bounded mutually-authenticated
control connection. It never proxies realtime frames or moves an open socket.

## Before you start

- Use a durable database on **both nodes**. SQLite is suitable when both local
  processes can safely use the same file; use PostgreSQL or CockroachDB for
  independently deployed nodes.
- Issue a private cluster CA certificate and one leaf certificate/key pair per
  node. Every leaf must support both TLS **server** and **client** authentication
  (mutual TLS) and contain the `server_name` you configure below as a DNS Subject
  Alternative Name.
- Put the CA, this node's chain/key, and every peer's **leaf certificate** on
  the local node. The CA establishes TLS trust; the peer leaf file pins a
  `node_id`, so a valid certificate for a different node is still rejected.

For a throwaway local test, generate a private CA and two certificates with
your normal PKI tooling. Keep private keys outside source control.

## 1. Create the first node configuration

Save this as `node-a.toml`. Both nodes point to the same durable database in
this example; their control ports and identities differ.

```toml
[server]
node_id = "node-a"
public_addr = "127.0.0.1:7350"

[database]
url = "sqlite:./cluster.sqlite"

[cluster]
enabled = true
control_bind = "127.0.0.1:7390"
matchmaker_shard = 0
lease_ttl_ms = 5000
handoff_ttl_ms = 30000
command_timeout_ms = 2000

[cluster.tls]
ca_certificate_file = "./certs/cluster-ca.pem"
certificate_file = "./certs/node-a.pem"
private_key_file = "./certs/node-a-key.pem"

[[cluster.peers]]
node_id = "node-b"
control_addr = "127.0.0.1:7391"
server_name = "node-b.local"
certificate_file = "./certs/node-b.pem"
```

## 2. Create the second node configuration

Copy the first file to `node-b.toml`, then change the node-local values and the
peer entry:

```toml
[server]
node_id = "node-b"
public_addr = "127.0.0.1:7354"

[database]
url = "sqlite:./cluster.sqlite"

[cluster]
enabled = true
control_bind = "127.0.0.1:7391"
matchmaker_shard = 0
lease_ttl_ms = 5000
handoff_ttl_ms = 30000
command_timeout_ms = 2000

[cluster.tls]
ca_certificate_file = "./certs/cluster-ca.pem"
certificate_file = "./certs/node-b.pem"
private_key_file = "./certs/node-b-key.pem"

[[cluster.peers]]
node_id = "node-a"
control_addr = "127.0.0.1:7390"
server_name = "node-a.local"
certificate_file = "./certs/node-a.pem"
```

`matchmaker_shard` is the queue partition in this first cluster MVP. Set the
same value on both nodes. The durable lease decides its active owner; the other
node forwards instead of evaluating a local copy.

## 3. Validate and start both nodes

Run the validation command for each config before opening client transports:

```bash
cargo run -- check --config node-a.toml
cargo run -- check --config node-b.toml
```

Start each node in a separate terminal:

```bash
cargo run -- serve --config node-a.toml
cargo run -- serve --config node-b.toml
```

On Windows PowerShell, the same commands work; use `cargo run -- ...` rather
than a Unix-only shell wrapper.

## 4. Submit and redeem tickets normally

Connect one player to each realtime node. Each client uses the regular
[matchmaker RPCs](/reference/client-sdk/matchmaker/):

1. Send `matchmaker.add` from both authenticated clients.
2. Save the reliable `KIND_MATCHMAKER_MATCHED` handoff on each client.
3. Send `matchmaker.accept` with that client's `ticket_id` and `join_token`.
4. Wait for `ROOM_JOINED` before loading the map.

The session node forwards a remote ticket to the current shard owner. The owner
claims the entire cohort under its durable lease, creates the closed room, and
delivers each handoff back to the session-owning node. `matchmaker.accept` is
validated at the match owner; the client cannot use a raw room id or redeem the
same handoff twice.

## 5. Verify failure behavior

Stop the current shard-owning node or let its lease expire, then start a node
with a higher-generation durable lease. A handoff formed by the old owner must
be rejected rather than admitted. The two-node integration test exercises this
stale-owner case and duplicate admission:

```bash
cargo test live_matchmaker_forwards_remote_tickets_over_mtls_and_fences_stale_admission --lib
```

## Current boundaries

- Endpoint registration is explicit `[cluster.peers]`; mDNS discovery and
  gateway placement/redirection are separate work.
- The control plane is matchmaker-only: it carries ticket submit/cancel/status,
  handoff delivery, and admission. It is not a general socket or match-state
  proxy.
- Queue working state remains memory-resident at its active owner. Durable
  leases, formation claims, and one-time admissions survive restart and fence a
  stale owner; automatic match-state migration is not part of this feature.
