---
title: Engine & platform support
description: Per-engine, per-runtime, and per-platform status of every Citadel capability, generated from the canonical capability matrix.
---

<!-- Generated from manifests/capability-matrix.json by scripts/generate_docs_capability_matrix.py. Do not edit by hand; run `python scripts/generate_docs_capability_matrix.py --write`. -->

Every row here comes straight from the [canonical capability matrix](https://github.com/franadoriv/citadel/blob/develop/manifests/capability-matrix.json),
the single source of truth for what Citadel ships. A capability is not marked
shipped here until its documentation is updated, so this page and the code stay in
lockstep.

**Legend:** ✅ Shipped · 🟡 Partial · ⬜ Planned · — Not applicable

## Client SDKs by engine

What each engine and browser client SDK can do today. This is the first thing to
check when picking an engine.

### Connection, authentication, and generic API

| Capability | Unity | Unreal | Godot | Web / JS | Rust |
| --- | :---: | :---: | :---: | :---: | :---: |
| Connect and authenticated realtime handshake | ✅ | ✅ | ✅ | ✅ | ✅ |
| Guest realtime handshake | ✅ | ✅ | ✅ | ✅ | ✅ |
| Email/password authentication | ✅ | ✅ | ✅ | ✅ | ✅ |
| Player profile, exact lookup, session refresh, and logout | ✅ | ✅ | ✅ | ✅ | ✅ |
| Correlated generic RPC | ✅ | ✅ | ✅ | ✅ | ✅ |
| Relayed position/message traffic | ✅ | ✅ | ✅ | ✅ | ✅ |
| Opt-in lag diagnostics capture and upload | ⬜ | ⬜ | ⬜ | ✅ | ⬜ |
| Durable notification inbox and local live stream | ✅ | ✅ | ✅ | ✅ | ✅ |
| Durable chat live events | ✅ | ✅ | ✅ | ✅ | ✅ |
| Friends, groups, leaderboards, chat, wallet RPC | ✅ | ✅ | ✅ | ✅ | ✅ |
| Purchases, subscriptions, and external store validation | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Connect and authenticated realtime handshake** — All SDKs ship connect plus dedicated guest and token realtime handshake helpers; native engines use QUIC/C ABI, and the Web SDK uses WebSocket (handshakeGuest / handshakeToken).
- **Guest realtime handshake** — All clients can connect as a guest where server policy permits.
- **Email/password authentication** — First-class HTTP registration/sign-in uses POST /v1/auth/email and returns caller-owned session tokens; durable hashed multi-key admission limits protect the public boundary. Email verification, recovery/change-password, and linking remain pending.
- **Player profile, exact lookup, session refresh, and logout** — First-class HTTP lifecycle APIs preserve the sanitized backend error contract; the completion manifest checks their bindings and web anchors across all released SDKs. Refreshed token pairs stay caller-owned for atomic secure storage.
- **Correlated generic RPC** — The common route for domain, party, and matchmaker operations.
- **Relayed position/message traffic** — All SDKs expose the base framed protocol; helpers vary by SDK.
- **Opt-in lag diagnostics capture and upload** — The Web / JavaScript SDK has a code-only opt-in recorder. When the server requests it, the SDK records the selected diagnostic packets in a bounded CLAG ring with server-clock correlation and uploads a compressed snapshot through an opaque signed one-use grant. Unity, Unreal, Godot, and Rust client SDKs do not yet expose this recorder or upload API.
- **Durable notification inbox and local live stream** — Read/ack by RPC and consume KIND_NOTIFICATION with client-side deduplication.
- **Durable chat live events** — All released SDKs provide typed closed-schema KIND_CHAT_EVENT lifecycle, deduplication, reconnect/revocation fencing, transactional history application, and private correlated acknowledgement. Durable delivery uses a local-first transactional cluster outbox and remains at-least-once with history reconciliation.
- **Friends, groups, leaderboards, chat, wallet RPC** — Authenticated generic RPC works across all current client targets.
- **Purchases, subscriptions, and external store validation** — No player-facing purchase surface yet.

</details>

### Rooms, matchmaking, parties, and multiplayer

| Capability | Unity | Unreal | Godot | Web / JS | Rust |
| --- | :---: | :---: | :---: | :---: | :---: |
| Named room component and map-ready event | ✅ | ✅ | ✅ | ✅ | 🟡 |
| Local ticket matchmaker RPC workflow | 🟡 | ✅ | 🟡 | 🟡 | 🟡 |
| Local party management and party tickets | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 |
| Transform sync snapshots and interpolation | ✅ | ✅ | ✅ | 🟡 | 🟡 |
| Owner prediction, reconciliation, and rewind | 🟡 | ✅ | 🟡 | ⬜ | 🟡 |
| NetworkPeer property replication authoring | 🟡 | 🟡 | 🟡 | 🟡 | ✅ |
| Networked-actor presence/spawn integration | 🟡 | ✅ | 🟡 | ⬜ | 🟡 |
| Authoritative server physics replication | 🟡 | ✅ | 🟡 | ⬜ | 🟡 |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Named room component and map-ready event** — Unity, Unreal, Godot, and JS/Web expose named-room join/create, leave, map-ready, and joined/left lifecycle events; Unity/Godot editor smoke remains manual.
- **Local ticket matchmaker RPC workflow** — All can use generic RPC; dedicated matchmaker event ergonomics differ.
- **Local party management and party tickets** — All use generic RPC; feature itself remains local-node only.
- **Transform sync snapshots and interpolation** — Unity (via the shared C ABI) and Unreal (a faithful C++ port) run the full interpolation runtime with Hermite/slerp and an adaptive buffer; Godot's GDExtension also binds and runs the transform runtime (its 7 transform methods are headless-smoke verified in Godot 4.7); a full in-editor gameplay pass stays a manual pre-release check. The browser SDK ships v2 snapshot decode primitives and an epoch fence over WebTransport unreliable datagrams (WebSocket stays reliable-only), without the interpolation runtime.
- **Owner prediction, reconciliation, and rewind** — Unreal is the fully documented owner integration; other surfaces are bounded.
- **NetworkPeer property replication authoring** — Rust ships canonical typed authoring. C ABI v3 encodes and iterates decoded typed keyed-collection operations; Unity has a managed v3 wrapper, while Unreal/Godot bindings are source-level only. Engine runtime verification is deferred because those engines are unavailable.
- **Networked-actor presence/spawn integration** — Unreal is end-to-end; Unity/Godot have transform layers but not full spawn integration.
- **Authoritative server physics replication** — Replicates through transform/actor layers; no WebSocket binary gameplay helper.

</details>

### Engine tools and platform-sensitive features

| Capability | Unity | Unreal | Godot | Web / JS | Rust |
| --- | :---: | :---: | :---: | :---: | :---: |
| Unity CMAP map exporter | ✅ | — | — | — | — |
| Godot CMAP map exporter | — | — | ✅ | — | — |
| Distributable Godot WebAssembly SDK package | — | — | ✅ | — | — |
| Unreal CMAP map exporter | — | 🟡 | — | — | — |
| Browser-native binary netcode helpers | — | — | — | 🟡 | — |
| Published npm package | — | — | — | 🟡 | — |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Unity CMAP map exporter** — Static MeshCollider and built-in Terrain extraction with deterministic fixture coverage.
- **Godot CMAP map exporter** — Static-body mesh extraction plus explicit terrain-provider interface.
- **Distributable Godot WebAssembly SDK package** — The ZIP installs the public addons/citadel WebSocketPeer client with no GDExtension and includes a matched Godot Web .html/.js/.pck/.wasm verification export; CI opens that real WebAssembly app in Chromium against a running Citadel listener and validates guest auth, relay, receive/poll, close and payload integrity.
- **Unreal CMAP map exporter** — Static mesh and Landscape source ship; UE 5.8 editor compile/terrain smoke is pending.
- **Browser-native binary netcode helpers** — Browser JS ships schema-bound reliable NetworkPeer DeltaBunch author/decode/ack helpers over WebSocket/WebTransport with deterministic structural fixture/binding validation. Browser and native engine two-client gameplay runs remain deferred external-environment verification.
- **Published npm package** — Source package exists; registry publication is still tracked work.

</details>

## Packages by platform

Which prebuilt download exists per operating system. Where a native package is not
yet published, the SDK still builds from source.

| Capability | Windows | macOS | Linux |
| --- | :---: | :---: | :---: |
| Standalone server package | ✅ | 🟡 | ✅ |
| Unity SDK package | ✅ | 🟡 | ⬜ |
| Unreal plugin package | ✅ | 🟡 | ⬜ |
| Godot SDK package | ✅ | 🟡 | ⬜ |
| Web / JavaScript SDK | ✅ | ✅ | ✅ |
| Rust client crate and C ABI source | ✅ | 🟡 | 🟡 |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Standalone server package** — Windows, Linux x86_64 musl, and Linux ARM64 musl release archives are published and CI-validated. Native Apple Silicon and Intel macOS packages exist only as a local Makefile target (package-macos) and are not built in CI: the macOS release matrix is intentionally disabled pending Apple Developer ID and notarization credentials, and no macOS package is published yet.
- **Unity SDK package** — Windows native FFI package is released. Native Apple Silicon and Intel macOS .dylib packages exist only as a local Makefile target (package-client-unity-macos) and are not built in CI: the macOS matrix is disabled pending Apple Developer ID and notarization credentials, and none is published yet.
- **Unreal plugin package** — Windows drop-in package is released. Native macOS staticlib packages exist only as a local Makefile target (package-client-unreal-macos) and are not built in CI: the macOS matrix is disabled pending Apple Developer ID and notarization credentials; an Unreal Editor macOS smoke and the first signed public release remain manual gates, and none is published yet.
- **Godot SDK package** — Windows GDExtension package is released; the portable Godot Web ZIP ships the reliable WebSocketPeer addon plus a verified .html/.js/.pck/.wasm export without native artifacts. Linux CI loads it in Chromium against a real Citadel WebSocket server for guest auth, relay, receive/poll and close; deployed-browser TLS/origin smoke remains manual. Native macOS .dylib packages exist only as a local Makefile target (package-client-godot-macos) and are not built in CI: the macOS matrix is disabled pending Apple Developer ID and notarization credentials; a Godot Editor macOS smoke and the first signed public release remain manual gates, and none is published yet.
- **Web / JavaScript SDK** — WebSocket client runs in supported browsers; this is not a native engine package.
- **Rust client crate and C ABI source** — Source integration is portable; the Windows C ABI FFI archives ship bundled inside the engine client packages, while native macOS FFI archives exist only as local Makefile targets (package-client-*-macos) and are not built in CI: the macOS matrix is disabled pending Apple Developer ID and notarization credentials, and none is published yet.

</details>

## Server & game-logic capabilities by runtime

Server-side features. **Common** means the capability is available server-wide; the
language columns show which embedded game-logic runtimes expose it.

### Core, identity, and sessions

| Capability | Common | Lua | Python | JavaScript | TypeScript | Rust |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Server bootstrap, CLI, TOML config, and first-run setup | ✅ | — | — | — | — | — |
| Portable server releases and Linux deployment | ✅ | — | — | — | — | — |
| Dockerfile and editable Docker workflow | 🟡 | ✅ | ⬜ | ⬜ | ⬜ | — |
| Health, live status, observability, audit logs | ✅ | — | — | — | — | — |
| Device authentication | ✅ | — | — | — | — | — |
| Custom-id authentication | ✅ | — | — | — | — | — |
| Email/password authentication | ✅ | — | — | — | — | — |
| Apple sign-in | ⬜ | — | — | — | — | — |
| Facebook and Facebook Instant sign-in | ⬜ | — | — | — | — | — |
| Game Center sign-in | ⬜ | — | — | — | — | — |
| Google sign-in | ⬜ | — | — | — | — | — |
| Steam sign-in | ⬜ | — | — | — | — | — |
| Account linking and unlinking | ⬜ | — | — | — | — | — |
| Player account profile and user discovery | ✅ | — | — | — | — | — |
| Session tokens, realtime handshake, revocation | ✅ | — | — | — | — | — |
| Public session refresh and logout API | ✅ | — | — | — | — | — |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Server bootstrap, CLI, TOML config, and first-run setup** — Run a standalone node with generated config, game directory, and SQLite defaults.
- **Portable server releases and Linux deployment** — Versioned Windows, Linux x86_64 musl, and Linux ARM64 musl archives ship with SHA-256 checksums, CI package validation, and a systemd deployment template.
- **Dockerfile and editable Docker workflow** — Dockerfile and Compose development assets remain available, but release CI/CD no longer builds, tests, attests, or publishes OCI images. Historical GHCR images are not updated by releases.
- **Health, live status, observability, audit logs** — Health/status endpoints, structured logs, bounded process-local authoritative-decision telemetry with opaque correlations, generic outcomes, trusted runtime-controlled slices, and private closed-report console views, redacted local incident journaling, optional Sentry telemetry (including Bugsink), tracing seams, and operator audit records.
- **Device authentication** — Creates or authenticates a device identity and issues a session.
- **Custom-id authentication** — Application-owned identifiers map to accounts and sessions.
- **Email/password authentication** — Transactional email/password registration and sign-in at /v1/auth/email; Argon2id PHC verifiers, durable hashed multi-key admission limits, and existing session tokens ship. Email verification, recovery/change-password, and linking remain pending.
- **Apple sign-in** — Provider adapter planned.
- **Facebook and Facebook Instant sign-in** — Provider adapters planned.
- **Game Center sign-in** — Provider adapter planned.
- **Google sign-in** — Provider adapter planned.
- **Steam sign-in** — Provider adapter planned.
- **Account linking and unlinking** — Identity seams allow future providers but no link/unlink API ships.
- **Player account profile and user discovery** — All released client SDKs expose typed profile read/update and exact known-user lookup; the completion manifest mechanically verifies those bindings and their web reference anchors. There is intentionally no directory, fuzzy search, presence, or recommendations.
- **Session tokens, realtime handshake, revocation** — Opaque bearer tokens, ownership, realtime auth, guest admission, expiry/revocation validation, and durable session seams.
- **Public session refresh and logout API** — All released client SDKs rotate caller-owned opaque token pairs and idempotently revoke one session; the completion manifest mechanically verifies every released-SDK binding and reference anchor.

</details>

### Storage, databases, economy, and progression

| Capability | Common | Lua | Python | JavaScript | TypeScript | Rust |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Versioned JSON storage read/write/delete/list | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Storage indexes and query filters | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Atomic multi-resource account/storage/wallet updates | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| SQLite backend | ✅ | — | — | — | — | — |
| PostgreSQL backend | ✅ | — | — | — | — | — |
| CockroachDB backend | ✅ | — | — | — | — | — |
| MongoDB backend | ✅ | — | — | — | — | — |
| Read-only console database explorer | ✅ | — | — | — | — | — |
| Wallet balances and ledger | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Purchase record persistence and replay rejection | ✅ | — | — | — | — | — |
| Production store receipt validation | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Subscriptions and provider lifecycle | 🟡 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Event and telemetry ingestion | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Versioned JSON storage read/write/delete/list** — Permissions, cursors, create-only, compare-and-swap, and runtime access ship.
- **Storage indexes and query filters** — Operator-declared SQLite/PostgreSQL/CockroachDB/MongoDB indexes provide bounded equality filters plus durable include/exclude callbacks in Lua/Python/JS; index search is trusted game-logic work, not a generic client endpoint.
- **Atomic multi-resource account/storage/wallet updates** — Repository boundaries exist but no public multi-update unit-of-work API.
- **SQLite backend** — Single-file durable default for self-hosted nodes.
- **PostgreSQL backend** — Durable production backend with migrations.
- **CockroachDB backend** — Postgres-wire backend with the shipped domain tables.
- **MongoDB backend** — Durable backend for transaction-capable replica sets or sharded clusters; standalone MongoDB is rejected. Single-object storage mutations are transactional; portable atomic multi-object storage batches are explicitly unsupported pending replayable multi-key retry support. CI validates an authenticated disposable rs0 plus backup/restore integrity.
- **Read-only console database explorer** — Viewer/admin dashboard browsing for the configured SQLite, PostgreSQL, CockroachDB, or MongoDB database: allowlisted metadata, structured bound filters, opaque keyset/row handles, server-side redaction, audit records, deadlines and per-operator node limits. No SQL text, MongoDB commands, mutation, export, or system schemas.
- **Wallet balances and ledger** — Clients read balances/ledger; trusted logic adjusts under invariants.
- **Purchase record persistence and replay rejection** — Durable receipts are hashed; transaction ids cannot be replayed.
- **Production store receipt validation** — An asynchronous server-owned validation foundation, redacted configuration, bounded provider egress, and disabled-provider guard ship; Apple, Google, and Huawei adapters remain pending. Only custom deterministic development receipts are enabled.
- **Subscriptions and provider lifecycle** — Admin view derives active/expired state; provider renewal/refund events are pending.
- **Event and telemetry ingestion** — No player event ingestion or telemetry pipeline. Runtime-local best-effort callbacks are available separately to trusted Lua, Python, and JavaScript.

</details>

### Social, groups, chat, and notifications

| Capability | Common | Lua | Python | JavaScript | TypeScript | Rust |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Friends: invite, accept, block, remove, list | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Social-provider friend import and friends-of-friends | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Groups/clans: CRUD, role-safe membership, and admission workflows | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Group invitations and join requests | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Authorized durable direct, group, and room chat history | 🟡 | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Chat presence, typing, and live fan-out | 🟡 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Chat moderation and history administration | ✅ | — | — | — | — | — |
| Durable player notification inbox | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Local live notification delivery | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Cross-node notifications, campaigns, retention, push | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Status follow/unfollow and online presence graph | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Friends: invite, accept, block, remove, list** — Durable social graph with game-client RPC and parity host API.
- **Social-provider friend import and friends-of-friends** — Requires provider identity integrations and graph traversal.
- **Groups/clans: CRUD, role-safe membership, and admission workflows** — Open self-join, closed-group requests, invitations, approval/accept/cancel flows, and superadmin ownership transfer are durable and exposed to trusted game logic through the groups.call host API; client RPC covers CRUD and role-safe membership.
- **Group invitations and join requests** — Persisted request and invitation state supports idempotent cancellation, role-safe approval, and invitation acceptance.
- **Authorized durable direct, group, and room chat history** — Send, history, author edit/delete, group-admin moderation, revisions, tombstones, redacted audit records, and multi-key durable limits derive targets server-side and fence friendship, membership, and room access; live delivery is available after chat.join on current authenticated cluster leases.
- **Chat presence, typing, and live fan-out** — Chat.join/leave, authorized ephemeral typing with receiver-side expiry, presence, committed reliable KIND_CHAT_EVENT fan-out, bounded resync, revocation cleanup, and typed mTLS cross-node durable delivery with leased fenced advertisements ship. Typing is local-node only.
- **Chat moderation and history administration** — Operator console can inspect and tombstone durable history with an atomic redacted audit record and independent retention.
- **Durable player notification inbox** — List/read APIs, idempotent producers, and persisted inbox records.
- **Local live notification delivery** — Committed notifications attempt reliable KIND_NOTIFICATION delivery on the local node.
- **Cross-node notifications, campaigns, retention, push** — No distributed forwarding, external push, or campaign scheduler.
- **Status follow/unfollow and online presence graph** — Presence is scoped to rooms, not a social follow graph.

</details>

### Realtime multiplayer, rooms, matchmaking, maps, and physics

| Capability | Common | Lua | Python | JavaScript | TypeScript | Rust |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| QUIC, WebTransport, and WebSocket | ✅ | — | — | — | — | — |
| Authenticated realtime connection and generic RPC | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Relayed realtime messages | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Named rooms, membership, labels, and map-ready | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Player match listing and query filters | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Single-node authoritative matches and presence | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Multi-node match ownership, migration, and failover | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ |
| Local ticket matchmaker and reconnect handoff | ✅ | — | — | — | — | — |
| Cross-node matchmaker routing and durable leases | ✅ | — | — | — | — | — |
| Local realtime parties | 🟡 | — | — | — | — | — |
| Distributed party ownership, presence, and failover | 🟡 | — | — | — | — | — |
| Transform sync, prediction, reconciliation, rewind | ✅ | — | — | — | — | — |
| NetworkPeer property replication | 🟡 | — | — | — | — | — |
| CMAP static collision, server navmesh, and map queries | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Server-simulated kinematic physics | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **QUIC, WebTransport, and WebSocket** — Native low-latency transport, browser datagram path, and reliable browser/fallback path. QUIC/WebTransport accept production PEM TLS; native clients verify public CA certificates and hostnames, while WebSocket uses WSS through a reverse proxy. Hand-rolled RUDP is deliberately not shipped.
- **Authenticated realtime connection and generic RPC** — Account/guest handshake and correlated request-response messages.
- **Relayed realtime messages** — Game logic can validate, broadcast, or unicast relay messages.
- **Named rooms, membership, labels, and map-ready** — All shipped runtimes expose room creation/admission hooks; the common room boundary scopes membership.
- **Player match listing and query filters** — Operators can inspect matches; no player match-list API.
- **Single-node authoritative matches and presence** — Server rooms, lifecycle, tick, presence, and scoped relay are usable on one node.
- **Multi-node match ownership, migration, and failover** — No end-to-end distributed match runtime.
- **Local ticket matchmaker and reconnect handoff** — Typed mutual queries, TTL, cancellation, atomic cohorts, and account-bound join tokens.
- **Cross-node matchmaker routing and durable leases** — mTLS node-control transport forwards tickets, handoffs, cancellation/status, and admission; durable fenced leases/claims protect PostgreSQL and CockroachDB. Clustered SQLite and MongoDB are rejected at startup.
- **Local realtime parties** — Invite/accept/leader/remove and atomic party tickets ship; all clients use generic RPC and client-specific convenience ergonomics remain partial.
- **Distributed party ownership, presence, and failover** — PostgreSQL/CockroachDB clusters provide durably fenced owner routing, restart-safe membership snapshots, privacy-scoped presence, one recovery resync per owner generation, and atomic whole-party tickets. Party data messages and dedicated client SDK ergonomics remain unshipped; SQLite/MongoDB clusters are rejected.
- **Transform sync, prediction, reconciliation, rewind** — Authoritative snapshots and owner modes; browser WebSocket cannot use the unreliable hot path.
- **NetworkPeer property replication** — Opt-in gateway authority, trusted schema/object lifecycle seam, shared-grid relevance, ABI v3 typed scalar/vector/quaternion and keyed-collection authoring, C ABI decoded keyed-collection iteration, and Rust authoring ship. Unreal receive/apply/ACK/full-recovery and match/room AOI remain separate.
- **CMAP static collision, server navmesh, and map queries** — Static cooked collision feeds navmesh, map_info, raycasts, overlap, and ground queries.
- **Server-simulated kinematic physics** — Deterministic static-map collision, gravity, impulse, movement intent, and state; no dynamic rigid bodies.

</details>

### Game logic, automation, and operator tooling

| Capability | Common | Lua | Python | JavaScript | TypeScript | Rust |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Embedded Lua game logic | — | ✅ | — | — | — | — |
| Embedded Python game logic | — | — | ✅ | — | — | — |
| Embedded JavaScript game logic | — | — | — | ✅ | ⬜ | — |
| Read-only static JSON/CSV gameplay data | — | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Rust game logic as a crate | — | — | — | — | — | ⬜ |
| Hardened WASM game logic | — | — | — | — | — | ⬜ |
| Message/lifecycle/tick/RPC/room hooks | — | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Broadcast/send, actors, maps, physics, storage, log | — | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Friends, groups, leaderboards, chat, wallet, notifications host APIs | — | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Before/after API and realtime interception hooks | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Matchmaker callbacks, leaderboard/tournament reset callbacks | 🟡 | 🟡 | 🟡 | 🟡 | ⬜ | ⬜ |
| Runtime outbound HTTP, custom HTTP endpoints, events, shared cache | ✅ | ✅ | ✅ | ✅ | ⬜ | ⬜ |
| Dashboard and authenticated operator API | ✅ | — | — | — | — | — |
| Lag diagnostics capture, analysis, and Console reports | ✅ | — | — | — | — | — |
| Console MFA, user lifecycle, password reset, ACL templates | ⬜ | — | — | — | — | — |
| Cluster discovery, load balancing, generalized node routing | ⬜ | — | — | — | — | — |

<details>
<summary>Row-by-row notes &amp; caveats</summary>

- **Embedded Lua game logic** — Default runtime with module loading and failure-safe hot reload.
- **Embedded Python game logic** — Feature-gated trusted runtime with parity checks and starter game.
- **Embedded JavaScript game logic** — Feature-gated QuickJS adapter with scoped local ESM modules and dependency-aware hot reload; no Node APIs, npm, workers, native modules, or TypeScript transpilation.
- **Read-only static JSON/CSV gameplay data** — Lua, Python, and JavaScript load bounded, parsed gameplay constants from an operator-owned root at initialization, cache them in memory, and atomically replace them with a successful hot reload.
- **Rust game logic as a crate** — Designed builder/scaffold path; native dynamic plugins remain rejected.
- **Hardened WASM game logic** — Capability-gated multi-tenant runtime is designed, not shipped.
- **Message/lifecycle/tick/RPC/room hooks** — Manifest-enforced parity for on_message, join/leave, tick, RPC, and room hooks.
- **Broadcast/send, actors, maps, physics, storage, log** — Current language-neutral host surface is mechanically checked.
- **Friends, groups, leaderboards, chat, wallet, notifications host APIs** — Friends/notifications have direct functions; remaining domain calls use validated bridges.
- **Before/after API and realtime interception hooks** — Post-handshake before hooks can veto eligible envelopes; after hooks observe the synchronous local delivery outcome without mutation or side effects.
- **Matchmaker callbacks, leaderboard/tournament reset callbacks** — A durable, supervised leaderboard-reset scheduler delivers on_leaderboard_reset to Lua, Python, and JavaScript under fenced backend leases; matchmaker matched callbacks and tournament-reset callbacks are not shipped.
- **Runtime outbound HTTP, custom HTTP endpoints, events, shared cache** — Trusted Lua, Python, and JavaScript expose Rust-owned asynchronous http.start/poll/cancel with explicit egress policy, DNS rebinding defenses, and shared rate/concurrency limits. They can also register bounded endpoints under /ext when enabled, use opt-in best-effort events, and share an opt-in non-durable cache with fenced cluster fan-out.
- **Dashboard and authenticated operator API** — Accounts, storage, groups, chat, notifications, leaderboards, matches, runtime, config, purchases, audit, and the error journal.
- **Lag diagnostics capture, analysis, and Console reports** — Native server controls request opt-in client capture, issue signed one-use upload grants, retain raw artifacts privately, and optionally persist SQL-backed analysis reports for the Console. SQLite, PostgreSQL, and CockroachDB support reports; MongoDB accepts raw capture only with analysis disabled.
- **Console MFA, user lifecycle, password reset, ACL templates** — Operator authentication roles ship; these advanced controls do not.
- **Cluster discovery, load balancing, generalized node routing** — Ownership and fencing groundwork is not a deployable cluster product.

</details>
