---
title: Lua runtime reference
description: Per-function reference for the embedded Lua game-logic runtime — the citadel host API, ctx shapes, execution budgets, hot reload, and console introspection.
---

Citadel can embed a Lua VM in the node itself and route inbound realtime
traffic to a script's handlers. When `[runtime] enabled = true` (the default)
and runtime selection resolves to `<scripts_dir>/main.lua`, the node loads it
once at startup and calls into it for every message, RPC, lifecycle event, room
decision, and (optionally) every game-loop tick. If no selected script is
present, the node falls back to the built-in relay unchanged.

This page documents every function the script sees: the global `citadel`
table, `require` module loading, the `ctx` object each hook receives,
execution budgets/limits, hot-reload semantics, and the two console endpoints
that expose the runtime to operators. Source of truth: `src/runtime/lua.rs`.

:::note[This is server-side scripting]
Lua runs **inside the node process**, not on the client. Game clients never
call these functions directly — they send realtime envelopes or RPC requests
over the wire, and the script running on the server reacts by calling
`broadcast`/`send`/`spawn_actor`/etc. to push state back out. See
[Envelope format reference](/reference/protocol/envelope/) for the wire format these
handlers exchange bytes over, and the chat feature reference (when published)
for a worked client-relay example built on `on_message`/`broadcast`.
:::

For the `[runtime]` config keys (`language`, `adapter`, `tier`, `scripts_dir`,
`deadline_ms`, `tick_hz`, `hot_reload`, ...) see
[Configuration reference](/reference/operations/configuration/#runtime).
This page focuses on the Lua-visible API and behavior those settings drive.

## The `citadel` table

`citadel` is a global table installed into the script's Lua state before the
script body runs. Every function below hangs off it (`citadel.on_message`,
`citadel.broadcast`, ...). The standard library available to scripts is
deliberately narrow: `string`, `table`, and `math` only — no `io`, `os`,
`package`, `coroutine`, or `debug`, so a script cannot touch the filesystem,
spawn processes, escape the deadline hook, or load native code. With explicit
`runtime.lua_execution_mode = "trusted"`, Lua also receives `io`, `os`,
`package`/unrestricted `require`, and `coroutine`; use it only for game code the
operator owns. The trusted VM has no handler deadline. `debug` and native
C-module loading remain unavailable in both modes because they require an
unsafe Rust VM constructor.

## `citadel.http.start`, `poll`, and `cancel`

This is a **trusted-server-only** capability: Lua exposes it only with
`runtime.lua_execution_mode = "trusted"`, and it is for game code the operator
owns—not client input or a browser. Citadel owns the Rust HTTP client, DNS, TLS,
sockets, timeout, and cancellation; a script never receives a network handle.
It is unavailable in either realtime interceptor: `fetch`, `start`, `poll`, and
`cancel` fail with `interceptor_forbidden` there.

```lua
local handle = citadel.http.start(url, opts)
```

`url` must be an `http` or `https` DNS-hostname URL. `opts` is optional and may
contain `method` (string, default `"GET"`), `headers` (string-to-string table),
and `body` (a Lua string). `start` validates this request and the operator
policy before allocating an opaque runtime-local `u64` handle, then schedules
network I/O without waiting for it. Rust policy rejections raise their stable
code and do not produce a handle. Local Lua argument or option validation can
instead raise a Lua-visible validation error; it is not an `error_code` contract.

`poll(handle)` never waits and returns one of these tables:

| `state` | Other fields | Meaning |
| --- | --- | --- |
| `"pending"` | — | Work is still running; poll again from a later tick. |
| `"success"` | `status` (`u16`), `body` (binary-safe string) | HTTP response completed. |
| `"error"` | `error_code` (string) | A stable, redacted code for a network/runtime request result. |
| `"timeout"` | — | The five-second request deadline elapsed. |
| `"cancelled"` | — | The request was cancelled. |

`cancel(handle)` returns the same state table. It aborts a pending request and
returns `cancelled`; it is idempotent for an already-terminal known handle
(including a second cancel). Unknown, malformed, evicted, or reload-invalidated
handles raise an error instead. Terminal handles remain pollable only until the
bounded per-runtime table needs to evict one; reload and shutdown cancel and
forget all handles.

The `error_code` contract is stable and deliberately redacted: never parse a
human-readable error message. The codes are `request_too_large`,
`response_too_large`, `invalid_method`, `invalid_header`, `headers_too_large`,
`authority_header_forbidden`, `capability_disabled`, `invalid_scheme`,
`invalid_url`, `url_credentials_forbidden`, `ip_literal_forbidden`,
`host_forbidden`, `port_forbidden`, `private_address_forbidden`,
`resolution_failed`, `concurrent_limit_reached`, `rate_limit_reached`,
`handle_limit_reached`, `unknown_handle`, and `request_failed`. Policy and
handle failures raise their code directly; a completed request with `state =
"error"` returns its code in `error_code`.

### `citadel.http.fetch` compatibility

`fetch(url, opts?)` remains available in trusted Lua for backward compatibility.
It uses the same request shape and policy, but is synchronous and returns
`{ status = u16, body = string }`; it is not replaced by `start`. New gameplay
paths should use `start` + `poll` to avoid blocking a tick or handler.

```lua
local pending_inventory

citadel.on_tick(function(_dt)
  if not pending_inventory then
    pending_inventory = citadel.http.start("https://inventory.example/v1/stock", {
      method = "GET", headers = { ["authorization"] = "Bearer " .. token },
    })
    return -- start never waits for the response
  end

  local result = citadel.http.poll(pending_inventory)
  if result.state == "pending" then return end
  pending_inventory = nil
  if result.state == "success" and result.status == 200 then
    print(result.body)
  elseif result.state == "error" then
    print("inventory request failed: " .. result.error_code)
  elseif result.state == "timeout" or result.state == "cancelled" then
    print("inventory request did not complete")
  end
end)
```

### Migration limits

There is no native `await`, coroutine scheduling, callback, or automatic fetch
rewrite: retain the runtime-local handle yourself and poll it from a later tick
or handler. A handle cannot survive a successful reload or runtime shutdown;
those lifecycle transitions cancel and forget outstanding work, so callers must
discard stored handles and start a fresh request when the replacement runtime is
ready.

The same operator policy applies to `fetch`, `start`, `poll`, and `cancel`:
hostname/port allowlists, public-address and DNS-rebinding checks, proxy and
redirect denial, a 64 KiB request body cap, 1 MiB response cap, 64 headers/
16 KiB aggregate header cap, five-second timeout, and configured concurrency
and rate quotas. See [outbound HTTP configuration](/reference/operations/configuration/#runtimecapabilitiesoutbound_http).

## citadel.http.register

Register one externally reachable script endpoint during `main.lua` startup.
Citadel owns the router: every declared path is served only below the reserved
`/ext` prefix, so a runtime cannot replace `/health`, `/v1`, `/console`, or any
other Citadel route. The capability is disabled by default and is available
only when `[runtime.capabilities.custom_http_endpoints] enabled = true`.

```lua
citadel.http.register(method, path [, options], handler)
```

`method` must be `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`. `path` is a
canonical relative path such as `/webhooks/inventory`: it must begin with `/`,
cannot end in `/`, and may contain only ASCII letters, digits, `.`, `_`, and
`-` in non-empty segments. Duplicate method/path registrations and invalid
paths reject the whole startup or hot reload, keeping the prior runtime live.

`options.auth` is either `"public"` (the default) or `"session"`. A
session endpoint requires a valid Citadel player bearer and receives
`request.user_id`; public endpoints receive no user id. Citadel consumes the
credential itself and never passes authorization or cookie headers to the
script.

```lua
citadel.http.register("POST", "/webhooks/inventory", { auth = "session" }, function(request)
  -- request.method, request.path, request.headers, request.body, request.user_id
  return {
    status = 201,
    headers = { ["content-type"] = "application/json" },
    body = "{\"accepted\":true}",
  }
end)
```

The handler returns a table with optional `status` (default `200`), `headers`,
and binary-safe `body`. Request and response bodies, request headers, and
requests per minute use the configured capability limits. Citadel rejects
hop-by-hop response headers, enforces the normal runtime deadline, isolates a
handler failure as HTTP `500`, records a sanitized audit outcome, and swaps the
complete endpoint registry atomically on a successful reload. Python and
JavaScript expose the same `citadel.http.register` contract with their native
mapping/object and callback syntax.

---

## citadel.events

Publish and subscribe to bounded, node-local runtime events. This v1 surface is
best-effort only: events are never durable, retried, replicated to another
node, or replayed after restart. Enable it with
`[runtime.capabilities.events] enabled = true`.

```lua
citadel.events.subscribe("match.score", "updated", function(event)
  -- event.namespace, event.type, event.payload (binary-safe Lua string)
  citadel.broadcast(41, event.payload)
end)

local queued = citadel.events.emit("match.score", "updated", "42")
if not queued then
  -- coalesce or safely discard best-effort work
end
```

`namespace` and `type` use 1–80 ASCII alphanumeric, `.`, `_`, or `-`
characters. `emit` returns `false` when the capability is disabled or the
payload is oversized, rate-limited, or the fixed queue is full. Accepted events
are FIFO within this node; each namespace/type pair accepts at most 64
subscribers. Citadel delivers a snapshot after a normal message,
lifecycle, or tick dispatch: the snapshot holds at most 64 events and shares
that invocation's deadline, while remaining FIFO events wait for the next such
dispatch. Remaining subscribers of one event receive a fair share of the
available time. An event emitted by a subscriber also waits for the next
dispatch, avoiding recursive delivery. RPC, room-admission, and
`/ext` endpoint calls can enqueue events but do not drain them themselves,
because their command side effects are intentionally discarded. One subscriber
failure is logged and does not prevent the remaining subscribers. Python and
JavaScript expose the same API with byte payloads and native callback syntax.

---

## citadel.cache

Use the opt-in, bounded cache for transient runtime coordination. It is
node-local unless `[cluster]` is enabled; in a cluster, local mutations are
offered to a single durable writer lease for best-effort fenced fan-out. A
successful call is local, not a cluster-wide commit. Values are still
non-durable and are never replayed after a restart. Enable it with
`[runtime.capabilities.shared_cache]` `enabled = true`. Entries are isolated by
namespace and survive a successful script hot reload because the cache is
node-owned.

```lua
local current = citadel.cache.get("match.score", "player-42")
local next = citadel.cache.cas(
  "match.score", "player-42",
  current and current.version or nil,
  "43",
  30000
)
if next == nil then
  -- another callback changed the value; read and retry if appropriate
end
```

`get(namespace, key)` returns `nil` or `{ value, version, expires_in_ms }`.
`set(namespace, key, value, ttl_ms)` returns that entry, `delete(namespace,
key)` returns whether an entry was removed, and `cas(namespace, key,
expected_version, value, ttl_ms)` atomically writes only when the current
version matches (use `nil` to create an absent key). Values are binary-safe Lua
strings. Namespaces and keys are 1–80 ASCII alphanumeric, `.`, `_`, or `-`
characters. Configured entry, value-size, and TTL limits are enforced; inserting
a new key at capacity evicts the entry nearest expiry. Python and JavaScript
expose the same operations with bytes/`Uint8Array` values and native mappings.

---

## citadel.text_policy

Load an operator-owned text-policy JSON file during `main.lua` initialization,
then scan or sanitize text with its opaque reference.

```
policy_ref = citadel.text_policy.load_json(path) -- string
result = citadel.text_policy.scan(policy_ref, text) -- table
result = citadel.text_policy.sanitize(policy_ref, text) -- table
```

`load_json` accepts one non-empty relative `.json` path under `[runtime]
static_data_dir` (for example, `"policy.json"`) and returns a reference such
as `"text-policy:policy.json"`. The file must be a schema-version-1 policy:

```json
{"schema_version":1,"rules":[{"id":"bad-word","category":"abuse","severity":"high","terms":["bad"],"match":"whole_word","action":"mask"}]}
```

For that policy, `scan(policy_ref, "BAD actor")` returns exactly
`{ decision = "mask", matches = {{ rule_id = "bad-word", category = "abuse", severity = "high", span = { start = 0, end = 3 }, action = "mask" }}, text = "BAD actor" }`.
`sanitize(policy_ref, "BAD actor")` returns the same decision and matches with
`text = "*** actor"`. Every result has `decision`, `matches`, and `text`.
Each match has `rule_id`, `category`, `severity` (or `nil`), `span`, and
`action`; `span.start` and `span.end` are zero-based **UTF-8 byte offsets** in
the input text, with an exclusive end.

Matching folds ASCII letters only (`BAD` matches `bad`); it performs no Unicode
normalization or Unicode case folding. Rules use `whole_word` or `phrase`
matching. Actions and aggregate decisions are `allow`, `flag`, `mask`,
`replace`, and `reject`, in that order of precedence. `sanitize` masks matched
text with one `*` per character and applies a rule's replacement for `replace`;
`allow`, `flag`, and `reject` retain the matched input text. Use `decision` to
enforce a flag or rejection—the API never silently permits an invalid policy.

Policies are compiled and cached by path during top-level initialization. A
repeat load returns the cached reference. Citadel seals the catalog before
handlers run: a cached reference remains usable, but a new path is denied, so
message/tick handlers cannot cause policy-file I/O. A successful hot reload
builds a new runtime and sealed catalog; a failed replacement leaves the prior
runtime active.

**Errors:** all access, parse, validation, unknown-reference, and late-load
failures are fail-closed and raise a Lua error. Static-data access failures are
prefixed `text policy static data error`; invalid JSON/schema/rules/actions are
prefixed `text policy is invalid`; a post-seal cache miss says `text policy was
not loaded during script initialization`; an invalid or foreign reference says
`unknown text policy reference`.

```lua
local chat_policy = citadel.text_policy.load_json("policy.json")

citadel.on_rpc("moderate", function(ctx, body)
  return citadel.text_policy.sanitize(chat_policy, body)
end)
```

---

## citadel.static_data.load_json

Load one preconfigured gameplay JSON document during script initialization.

```
citadel.static_data.load_json(path) -> table
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `path` | string | yes | A non-empty relative `.json` path below `[runtime] static_data_dir`, such as `"gameplay/collision.json"`. Use `/` separators only. Absolute/drive-qualified paths, `.`/`..`, backslashes, escaped symlinks, missing files, and other extensions are rejected. |

**Returns:** a fresh Lua table decoded from a JSON object or array. JSON objects
use string keys; arrays use one-based numeric indexes; JSON `null` becomes Lua
`nil`. The file itself is parsed once into Citadel's in-memory catalog, so
repeated loads of the same path do not read the filesystem.

**Errors:** raises a Lua error beginning with `static data access denied`,
`static data file not found`, `static data file exceeds configured size limit`,
`invalid JSON static data`, or `static data schema invalid`. JSON roots must be
an object or array. The API is always present, but it denies calls when
`runtime.static_data_dir` is unset. A cache miss after top-level script
initialization is denied, so a message/tick handler can never trigger I/O.

```lua
-- Runs while main.lua initializes. Keep these constants in memory for the
-- authoritative handler below; do not load on every attack message.
local collision = citadel.static_data.load_json("gameplay/collision.json")
local knight_radius = collision.characters.knight.hitbox.radius_cm

citadel.on_message(80, function(ctx, body)
  -- Validate with server-owned state plus knight_radius, not a client hit claim.
end)
```

---

## citadel.static_data.load_csv

Load one preconfigured gameplay CSV table during script initialization.

```
citadel.static_data.load_csv(path) -> table
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `path` | string | yes | A non-empty relative `.csv` path below `[runtime] static_data_dir`, such as `"gameplay/balance.csv"`. Use `/` separators only; it has the same containment and size rules as `load_json`. |

**Returns:** a one-based Lua array of row tables keyed by the CSV header. The
header must exist, contain non-empty unique names, and every row must have the
same column count. Cells `true`/`false` become booleans; integer/finite decimal
cells become numbers; all other cells remain strings. Whitespace around cells
is trimmed.

**Errors:** the same root/path/size errors as `load_json`; malformed quoting,
UTF-8, or uneven rows raise `invalid CSV static data`; invalid/missing/duplicate
headers raise `static data schema invalid`. It also may only create a cache entry
during top-level initialization.

```lua
local attacks = citadel.static_data.load_csv("gameplay/attacks.csv")
-- attacks[1] might be { id = "slash", damage = 12, enabled = true }
```

The server does not publish these files to game clients. Package the same
versioned `common/` data tree with your clients for visuals/UI, but treat the
Lua copy as the authority for collision and balance validation.

---

## citadel.on_message

Register a handler for inbound realtime messages of a given wire `kind`.

```
citadel.on_message(kind, handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `kind` | integer (`u16`) | yes | The wire message kind to handle. One handler per kind; re-registering the same `kind` replaces the previous handler. |
| `handler` | function `(ctx, body)` | yes | Called with the [message `ctx`](#ctx-shapes) and the raw message body as a Lua string (binary-safe). Return value is ignored. |

**Returns:** nothing.

**Errors:** none at registration time — `on_message` only stores the handler.
Errors raised *inside* `handler` at dispatch time are isolated: they are
logged server-side, any `broadcast`/`send` calls made before the error are
discarded, and the participant sees no reply (message dispatch has no
response channel).

```lua
-- kind 1 = chat message; echo it back to everyone else.
citadel.on_message(1, function(ctx, body)
  citadel.log("chat from " .. ctx.sender)
  citadel.broadcast(1, body)
end)
```

---

## citadel.before_realtime / citadel.after_realtime

Observe the post-handshake realtime pipeline around every eligible inbound
envelope, including game messages, RPCs, rooms, replication, transform, and
networked-actor traffic. Authentication frames are never exposed.

```lua
citadel.before_realtime(function(ctx, body) ... end)
citadel.after_realtime(function(ctx, body) ... end)
```

`before_realtime` runs before Citadel routes the envelope. Return `false` to
veto it; return `true` or nothing to continue. `ctx` has `sender`, `user_id`,
`room_id`, `kind`, and binary-safe `body` (the second argument is the same
immutable payload). A handler error, invalid return, timeout, or panic fails
closed and vetoes the envelope.

Both interception hooks are restricted to observation and logging: domain,
storage, and outbound HTTP APIs are unavailable while either hook runs.

`after_realtime` runs once after the synchronous routing result, including a
veto. Its `ctx` additionally has `dropped` and `delivered`, the number of local
outbound deliveries queued by that call. It is an observer: return values and
any `broadcast`/`send` calls are discarded. Domain, storage, and outbound HTTP
APIs are also unavailable. Errors are isolated and cannot
change the completed result.

```lua
citadel.before_realtime(function(ctx, body)
  return ctx.kind ~= 77 -- drop a prohibited client kind
end)

citadel.after_realtime(function(ctx, body)
  citadel.log("kind=" .. ctx.kind .. " delivered=" .. ctx.delivered)
end)
```

---

## citadel.on_rpc

Register a handler for a named request/response RPC method.

```
citadel.on_rpc(method, handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `method` | string | yes | The RPC method name clients (or the console) invoke. Re-registering the same name replaces the previous handler. |
| `handler` | function `(ctx, body) -> string` | yes | Called with the [RPC `ctx`](#ctx-shapes) and the request body (Lua string). **Must** `return` a string reply; any other return type is a handler error. |

**Returns:** nothing.

**Errors:** an unknown method, a Lua error thrown by the handler, a
non-string return, a blown deadline, or an isolated Rust-side panic all
produce the same short, generic error back to the caller (`"unknown RPC
method"`, `"RPC handler timed out"`, or `"RPC handler error"`) — the internal
reason is logged server-side but never leaked to the caller. Any
`broadcast`/`send` an RPC handler attempts is silently discarded: an RPC
handler communicates only through its return value.

```lua
citadel.on_rpc("ping", function(ctx, body)
  return "pong from " .. ctx.method
end)
```

---

## citadel.on_join

Register the handler run when a participant connects (registers with the
gateway).

```
citadel.on_join(handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `handler` | function `(ctx)` | yes | Called with the [lifecycle `ctx`](#ctx-shapes). Only one `on_join` handler exists at a time; re-registering replaces it. |

**Returns:** nothing.

**Errors:** isolated exactly like `on_message` — a handler error, panic, or
timeout is logged and discards any queued commands; the join itself is never
blocked or failed because of a script error.

```lua
citadel.on_join(function(ctx)
  citadel.log("participant " .. ctx.sender .. " joined")
  citadel.send(ctx.sender, 1, "welcome")
end)
```

---

## citadel.on_leave

Register the handler run when a participant disconnects (unregisters from
the gateway).

```
citadel.on_leave(handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `handler` | function `(ctx)` | yes | Called with the [lifecycle `ctx`](#ctx-shapes). Only one `on_leave` handler exists at a time. |

**Returns:** nothing.

**Errors:** isolated the same way as `on_join`.

```lua
citadel.on_leave(function(ctx)
  citadel.broadcast(2, ctx.sender .. " left")
end)
```

---

## citadel.on_tick

Register the periodic game-loop handler.

```
citadel.on_tick(handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `handler` | function `(dt [, room_id])` | yes | Called once for the server-wide tick with `dt`, then once per live room with that room's `room_id` as an optional second argument. `dt` is elapsed time **in seconds** (a float). Only one handler exists at a time. |

**Returns:** nothing.

**Errors:** isolated the same way as `on_message` — a hung or erroring tick
yields no commands for that tick and never blocks the next message dispatch.

The tick loop only runs when **both** `[runtime] tick_hz > 0` **and** the
script registered an `on_tick` handler; the bootstrap layer checks
`has_tick_handler` before spawning the periodic task at all, so a script
with no game loop costs nothing.

```lua
local elapsed = 0
citadel.on_tick(function(dt)
  elapsed = elapsed + dt
  if elapsed > 5 then
    citadel.broadcast(3, "tick")
    elapsed = 0
  end
end)
```

For per-match state, key state by the optional room id. Commands emitted from a
per-room invocation broadcast only to that room's current presences.

```lua
local rounds = {}
citadel.on_tick(function(dt, room_id)
  if not room_id then return end -- the server-wide invocation
  rounds[room_id] = (rounds[room_id] or 0) + dt
  if rounds[room_id] >= 10 then
    citadel.broadcast(3, "round complete")
    rounds[room_id] = 0
  end
end)
```

---

## citadel.on_room_create

Register the handler that decides a room's label (map/mode/capacity/open)
when a client asks to create one.

```
citadel.on_room_create(handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `handler` | function `(ctx, params) -> string \| table \| nil` | yes | Called with the [RPC-shaped `ctx`](#ctx-shapes) (`ctx.method == "room.create"`) and the raw create-request params (Lua string). |

**Return value**, one of:

| Return shape | Meaning |
| --- | --- |
| a plain string | Used as the room's `map` name; `mode` empty, `max_players` unlimited (`0`), `open = true`. |
| a table `{ map, mode?, max_players?, open? }` | `map` (string), `mode` (string, default `""`), `max_players` (integer, default `0` = unlimited), `open` (bool, default `true`). Missing fields fall back to these defaults. |
| `nil` / anything else | The gateway uses its default (empty) room label. |

**Errors:** if no `on_room_create` handler is registered, is registered but
errors, panics, or times out, the caller gets `None` and the gateway falls
back to the default label — a broken handler never blocks room creation.
Any `broadcast`/`send` attempted inside the handler is discarded.

```lua
citadel.on_room_create(function(ctx, params)
  return { map = "arena_01", mode = "ffa", max_players = 8, open = true }
end)
```

---

## citadel.on_room_join

Register the admission gate run when a client asks to join an existing room.

```
citadel.on_room_join(handler)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `handler` | function `(ctx, room_id) -> boolean` | yes | Called with the [RPC-shaped `ctx`](#ctx-shapes) (`ctx.method == "room.join"`) and the numeric `room_id`. Must return `true` to admit, `false` to reject. |

**Returns:** nothing (registration).

**Errors and defaults:** if no handler is registered, the join is **admitted
by default** (`true`). If the handler errors or panics, the join is
**rejected** (fail-closed) and the error is logged. A blown deadline is
treated the same as any other handler error — rejected.

```lua
local banned = { [42] = true }
citadel.on_room_join(function(ctx, room_id)
  return not banned[ctx.sender]
end)
```

---

## citadel.broadcast

Send a message to every connected participant except the sender of the
message currently being handled.

```
citadel.broadcast(kind, body [, unreliable])
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `kind` | integer (`u16`) | yes | Wire kind of the outbound envelope. |
| `body` | string | yes | Opaque payload bytes (binary-safe Lua string). Capped at 64 KiB per call (`MAX_OUTBOUND_BODY_BYTES`) — a larger body raises a Lua error. |
| `unreliable` | boolean | no (default `false`) | Requests best-effort delivery on transports that support it. WebSocket is reliable-only and always delivers regardless of this flag. |

**Returns:** nothing.

**Errors:** raises a Lua `RuntimeError` if `body` exceeds 64 KiB. Silently
drops the command (and marks the invocation as overflowed, logged once) if
the per-invocation command cap (1024) or aggregate outbound-byte cap (1 MiB)
is exceeded — it does not raise in that case, so a runaway script degrades
rather than crashing the handler.

```lua
citadel.on_join(function(ctx)
  citadel.broadcast(2, "a new player joined", true)
end)
```

---

## citadel.send

Send a message to a single participant by session id.

```
citadel.send(session, kind, body [, unreliable])
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `session` | integer (`u64`) | yes | Target participant id (raw transport-level id, matches `ctx.sender`). |
| `kind` | integer (`u16`) | yes | Wire kind of the outbound envelope. |
| `body` | string | yes | Opaque payload bytes. Same 64 KiB per-body cap as `broadcast`. |
| `unreliable` | boolean | no (default `false`) | Best-effort delivery hint; see `broadcast`. |

**Returns:** nothing.

**Errors:** same body-size error and command/byte-cap overflow behavior as
`citadel.broadcast`.

```lua
citadel.on_message(1, function(ctx, body)
  citadel.send(ctx.sender, 1, "ack:" .. body)
end)
```

---

## citadel.spawn_actor

Spawn a server-owned networked actor (an NPC) and return its object id
synchronously so the script can move or despawn it later.

```
citadel.spawn_actor{ archetype = <int>, x = <float>, y = <float>, z = <float> } -> object_id
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `archetype` | integer (`u16`) | no (default `0`) | Client archetype id to instantiate for the proxy actor. |
| `x`, `y`, `z` | float | no (default `0.0` each) | Initial world position in centimeters. |

**Returns:** `object_id` (integer, `u32`) — a script-assigned, server-owned
id. Server-owned ids start at `0x4000_0000` (`NPC_ID_BASE`) so they never
collide with player/presence ids, which grow from `1`. The gateway places the
actor in the transform world and fans out an `NA_SPAWN` so every client
instantiates the proxy for `archetype`.

**Errors:** none under normal operation; the id counter wraps back to
`NPC_ID_BASE` on overflow rather than erroring. Actor-command calls are also
subject to the per-invocation command cap (1024) but have no body-size
concept (there is no payload to size-check).

```lua
citadel.on_join(function(ctx)
  local npc = citadel.spawn_actor{ archetype = 7, x = 100, y = 0, z = 100 }
  citadel.log("spawned npc " .. npc)
end)
```

---

## citadel.move_actor

Update a server-owned actor's authoritative transform (the per-tick move
path for an NPC created with `spawn_actor`).

```
citadel.move_actor(object_id, x, y, z [, vx, vy, vz])
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `object_id` | integer (`u32`) | yes | The actor's id, as returned by `spawn_actor`. |
| `x`, `y`, `z` | float | yes | New world position in centimeters. |
| `vx`, `vy`, `vz` | float | no (default `0.0` each) | Linear velocity in cm/s, used by clients to interpolate. |

**Returns:** nothing.

**Errors:** none under normal operation beyond the shared command-cap
overflow (silently dropped, logged once per invocation if exceeded). Facing
is fixed to the identity quaternion `[0, 0, 0, 1]` for this MVP — clients
orient the proxy from velocity rather than an explicit rotation.

```lua
citadel.on_tick(function(dt)
  citadel.move_actor(npc_id, 105, 0, 100, 1, 0, 0)
end)
```

---

## citadel.despawn_actor

Remove a server-owned actor.

```
citadel.despawn_actor(object_id)
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `object_id` | integer (`u32`) | yes | The actor's id, as returned by `spawn_actor`. |

**Returns:** nothing. The gateway fans out an `NA_DESPAWN` so every client
removes the proxy.

**Errors:** none beyond the shared command-cap overflow behavior.

```lua
citadel.despawn_actor(npc_id)
```

## Physics host API

Physics is opt-in for `spawn_actor` actors while transform sync is enabled. All
dimensions use centimetres, velocity uses cm/s, and acceleration uses cm/s².
The write calls queue commands for the gateway; `physics_state` reads the live
authoritative transform hub synchronously.

### citadel.set_physics

```lua
citadel.set_physics(object_id, opts) -- opts may be nil
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `object_id` | integer (`u32`) | Server-simulated actor to configure. |
| `opts` | table or `nil` | Optional `gravity`, `buoyancy`, `drag`, `radius`, `height`, `max_speed`, `shape` (`"capsule"` or `"aabb"`), and `enabled`. |

**Returns:** nothing. `nil` or `{ enabled = false }` detaches the body.

**Errors:** a non-table options value, invalid field type, or an unknown shape
raises a Lua error; the handler's queued side effects are discarded. Calls for
non-server-simulated actors are ignored by the authoritative hub.

### citadel.apply_impulse

```lua
citadel.apply_impulse(object_id, ix, iy, iz)
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `object_id` | integer (`u32`) | Bodied server actor. |
| `ix`, `iy`, `iz` | number | Instantaneous velocity delta in cm/s. Positive Y jumps/flaps. |

**Returns:** nothing.

**Errors:** numeric conversion errors raise Lua errors. An actor without a
physics body is a no-op.

### citadel.set_move_intent

```lua
citadel.set_move_intent(object_id, vx, vy, vz)
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `object_id` | integer (`u32`) | Bodied server actor. |
| `vx`, `vy`, `vz` | number | Desired velocity in cm/s. Physics blends the horizontal X/Z intent; Y remains physics-led. |

**Returns:** nothing.

**Errors:** numeric conversion errors raise Lua errors. It is a no-op when no
body is attached.

### citadel.physics_state

```lua
citadel.physics_state(object_id) -> { grounded: boolean, position: number[3], velocity: number[3] } | nil
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `object_id` | integer (`u32`) | Actor whose live body state to inspect. |

**Returns:** a table with `grounded`, `position = { x, y, z }`, and
`velocity = { x, y, z }`, or `nil` when transform sync is disabled, no hub is
attached, or the actor has no physics body.

**Errors:** object-id conversion errors raise Lua errors; the read itself has
no queued side effect.

### Balloon-Fight bot

This complete tick loop composes jump/flap physics with horizontal AI intent:

```lua
local bot = citadel.spawn_actor({ x = 0, y = 200, z = 0 })
citadel.set_physics(bot, { gravity = 900, buoyancy = 300, drag = 0.5,
                           radius = 30, height = 90, shape = "capsule" })

citadel.on_tick(function(dt)
  local st = citadel.physics_state(bot)
  if st and st.grounded then
    citadel.apply_impulse(bot, 0, 600, 0)
  elseif st and st.velocity[2] < 0 then
    citadel.apply_impulse(bot, 0, 120, 0)
  end
  citadel.set_move_intent(bot, 180, 0, 0)
end)
```

---

## citadel.map_info

```lua
citadel.map_info(name) -> info | nil
```

Returns a read-only summary of a cooked map loaded from `runtime.maps_dir`, or
`nil` when the name is not loaded. `bounds_min` and `bounds_max` use Unreal
world units (cm); `vertex_count` and `triangle_count` describe the exported
collision mesh.

```lua
local level = citadel.map_info("Lvl_ThirdPerson")
if level then
  citadel.log(("map has %d collision triangles"):format(level.triangle_count))
end
```

---

## citadel.map_names and citadel.find_path

```lua
citadel.map_names() -> string[]
citadel.find_path(name, start, goal) -> number[3][] | nil
```

`map_names` returns loaded map keys in deterministic order. `find_path` asks
the Rust core to query the map's authoritative Detour navigation data and
returns a corridor ending at `goal`; it returns `nil` for an unknown map or an
unroutable endpoint. Lua sees neither collision geometry nor a pathfinding
implementation.

```lua
local path = citadel.find_path("Lvl_ThirdPerson", {0, 0, 0}, {900, 0, 300})
if path then
  for _, point in ipairs(path) do citadel.move_actor(bot, point[1], point[2], point[3]) end
end
```

---

## citadel.raycast

```lua
citadel.raycast(origin, direction) -> hit | nil
```

Casts the finite segment `origin + direction` against the active room map. Both
arguments are `{ x, y, z }`-style numeric arrays (`{0, 200, 0}`), in cm.
Returns `nil` when transform sync has no active map or the segment misses.

The hit table contains `point`, unit `normal`, `distance` in cm, and
`triangle_index`. Coordinates must be finite; malformed vectors raise an error.

```lua
local hit = citadel.raycast({0, 200, 0}, {0, -500, 0})
if hit then citadel.log(("floor at %.1f cm"):format(hit.point[2])) end
```

## citadel.sphere_overlap

```lua
citadel.sphere_overlap(centre, radius) -> boolean
```

Returns whether a sphere in cm overlaps any triangle in the active room map.
`centre` is a three-number array and `radius` is a finite, non-negative number.
It returns `false` when no map is active; invalid arguments raise an error.

```lua
if citadel.sphere_overlap({100, 50, 100}, 30) then
  citadel.log("spawn position is blocked", "warn")
end
```

## citadel.ground_height

```lua
citadel.ground_height(origin, max_distance) -> hit | nil
```

Finds the nearest upward-facing map surface below `origin`, up to
`max_distance` cm. It returns the same `point`, `normal`, `distance`, and
`triangle_index` fields as `citadel.raycast`, or `nil` for no walkable hit.
`max_distance` must be finite and non-negative.

```lua
local ground = citadel.ground_height({0, 500, 0}, 1000)
if ground then citadel.log(("ground y = %.1f"):format(ground.point[2])) end
```

---

## citadel.log

Emit a structured log line tagged as script output.

```
citadel.log(message [, level])
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `message` | string | yes | The log message. |
| `level` | string | no (default `"info"`) | Case-insensitive: `"trace"`, `"debug"`, `"warn"`, or `"error"`. Any other value (including omission) falls back to `info`. |

**Returns:** nothing.

**Errors:** none — `citadel.log` cannot fail.

```lua
citadel.log("starting round", "debug")
citadel.log("player disconnected unexpectedly", "warn")
```

---

## require — scoped module loading

A restricted, sandboxed `require` is installed as a Lua global (not under
`citadel.*`) so game logic can be split across multiple files without
exposing the standard `package`/`io`/`os` loader surface.

```
require(name) -> any
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | yes | A dotted module path. `require("systems.combat")` resolves to `<scripts_dir>/systems/combat.lua` (each dot-separated segment becomes a subdirectory). Segments must be non-empty and contain only `[A-Za-z0-9_]` — `..`, absolute paths, path separators, and empty segments are all rejected outright (not silently clamped). |

**Returns:** the value the module's top-level chunk `return`s. A module that
returns nothing caches as `true` (standard Lua `require` convention) rather
than `nil`, so it is not re-run on a later `require` of the same name.

**Caching:** a module runs **once per VM**; the registry caches its returned
value keyed by name, and every subsequent `require` of the same name returns
the cached value without re-executing the file. The cache is per-VM, so a
hot-reload (which builds a brand-new VM) clears it and every module reloads
fresh.

**Reload trigger:** the development watcher observes `main.lua`, not each
required module. A changed module is re-resolved when the runtime reloads, but
editing that module alone does not currently initiate a reload; touch the
entrypoint, use the operator reload control, or restart the server.

**Cycle guard:** a module that is still mid-load is tracked in a separate
in-flight table; if it (transitively) `require`s itself again before
finishing, the call fails with a `"cyclic require detected"` error instead of
recursing forever.

**No module root:** a `LuaRuntime` built from an in-memory source (used by
tests and embedders, not by `LuaRuntime::load`) has no `scripts_dir` to
resolve against. `require` is still installed as a global in that case, but
every call raises `"require(...) is unavailable: this runtime has no script
directory"` — an explicit error rather than a silently missing global.

**Errors:** a malformed name, a path that would escape the script root, a
missing file, a cyclic require, or a syntax/runtime error inside the module
body all raise a Lua error naming the module and the reason. A `require`'d
module body runs under the caller's already-armed deadline — it shares the
budget of the handler invocation that (transitively) required it.

```lua
-- <scripts_dir>/main.lua
local combat = require("systems.combat")

citadel.on_message(1, function(ctx, body)
  combat.apply_damage(ctx.sender, 10)
end)
```

```lua
-- <scripts_dir>/systems/combat.lua
local M = {}
function M.apply_damage(session, amount)
  citadel.log("dealt " .. amount .. " damage to " .. session)
end
return M
```

For a complete game-directory layout and the equivalent Python approach, see
[organize multi-file game logic](/guides/organize-game-server-logic/).

---

## ctx shapes

Every hook receives a `ctx` table, but its fields depend on which hook is
running:

| Hook | `ctx` fields |
| --- | --- |
| `on_message` handler | `ctx.sender` (`u64`), `ctx.kind` (`u16`), `ctx.user_id` (string, present only if the participant is authenticated — absent/`nil` for a guest), `ctx.room_id` (`u64`, present when the sender belongs to a room) |
| `before_realtime` | Message fields plus immutable `ctx.body`; return `false` to veto before routing. Authentication envelopes are excluded. |
| `after_realtime` | Before fields plus `ctx.dropped` and `ctx.delivered`; observer-only after synchronous routing. |
| `on_join` / `on_leave` | `ctx.sender` (`u64`), `ctx.user_id` (string or absent) |
| `on_rpc` handler | `ctx.sender` (`u64`), `ctx.method` (string), `ctx.user_id` (string or absent) |
| `on_room_create` handler | same shape as `on_rpc`, with `ctx.method == "room.create"` |
| `on_room_join` handler | same shape as `on_rpc`, with `ctx.method == "room.join"` |
| `on_tick` handler | **no `ctx`** — receives `dt` (seconds, float) and, for a per-room invocation, `room_id` (`u64`) as its optional second argument |

`ctx.user_id` is set only for an authenticated participant; it is left
absent (Lua `nil`) rather than an empty string for a guest, so `ctx.user_id
or "guest"`-style idioms work naturally. `ctx.sender` is always the
transport-level participant id and is stable for the lifetime of the
connection; `ctx.user_id` is the resolved domain account id from the session
service at connect time.

The console's synthetic RPC caller (see [console runtime
endpoints](#console-runtime-endpoints) below) invokes handlers with
`ctx.sender = 0` and `ctx.user_id = nil` — there is no real participant
behind an operator-triggered call.

---

## citadel.storage_read

Read one JSON-object storage value for a user.

```
citadel.storage_read(user, collection, key) -> object | nil
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `user` | string | Owner account id. Trusted-tier scripts are authoritative and may select the account they act for. |
| `collection`, `key` | string | Non-empty storage namespace and key (each at most 128 bytes). |

**Returns:** `nil` if absent; otherwise `{ value_json, version, read_permission,
write_permission }`. `value_json` is a JSON-object string and `version` is the
opaque token for an optimistic update/delete.

**Errors:** validation failures raise `storage validation: ...`; backend failures
raise `storage operation failed` without database details.

## citadel.storage_write

Create or update one user-owned JSON-object value.

```
citadel.storage_write(user, collection, key, value_json [, expected_version [, read_permission [, write_permission]]]) -> object
```

`value_json` must encode a JSON object. Omit `expected_version` for an upsert;
pass `""` for create-only; pass a returned version for compare-and-set. Read
permission defaults to `1` (owner) and write permission defaults to `1`
(owner); accepted codes are read `0|1|2` and write `0|1`.

**Returns:** the same versioned object shape as `storage_read`.

**Errors:** a failed create-only or version match raises `storage conflict`;
invalid values/permission codes raise `storage validation: ...`; backend errors
remain the generic `storage operation failed`.

```lua
citadel.on_rpc("save", function(ctx, body)
  local saved = citadel.storage_write(ctx.user_id, "profiles", "main",
    '{"level":2}', "", 1, 1)
  local current = citadel.storage_read(ctx.user_id, "profiles", "main")
  return current.value_json .. " @ " .. saved.version
end)
```

## citadel.storage_delete

Delete one user-owned object.

```
citadel.storage_delete(user, collection, key [, expected_version])
```

Omit `expected_version` for an idempotent delete, or pass a version returned by
`storage_read`/`storage_write` to require a match. `""` means the object must
not exist and therefore normally reports `storage conflict`.

**Returns:** nothing. Errors use the same clean validation/conflict/backend
mapping as `storage_write`.

## citadel.storage_index_query

Query an operator-configured storage index with equality filters over its
declared top-level JSON fields.

```
citadel.storage_index_query(index_name, filters_json, limit) -> objects
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `index_name` | string | Name from a `[[storage.indexes]]` server configuration entry. |
| `filters_json` | string | JSON object of equality filters. Every key must be one of that index's declared fields; values may be strings, numbers, or booleans. |
| `limit` | integer | Result cap from `1` through `100`. Results are identity-ordered. |

**Returns:** an array of objects with `user_id` (`nil` for a system object),
`collection`, `key`, `value_json`, `version`, `read_permission`, and
`write_permission`. The trusted runtime can query all matching objects; it is
not a player-facing storage endpoint.

**Errors:** an unknown index, a non-object filter payload, an undeclared field,
non-scalar filter value, or an out-of-range limit raises `storage validation:
...`. Backend failures raise `storage operation failed` without database details.

```lua
citadel.on_rpc("find_players_at_score", function(ctx, body)
  local players = citadel.storage_index_query(
    "profiles_by_score", '{"score":1200}', 25)
  return players[1] and players[1].user_id or "none"
end)
```

The index must be declared by the operator before the server starts; scripts
cannot create indexes or run arbitrary queries. See [storage indexes in
configuration](/reference/operations/configuration/#storage).

## citadel.register_storage_index_filter

Register one write-time filter for an operator-configured storage index.

```
citadel.register_storage_index_filter(index_name, callback)
```

| Parameter | Type | Meaning |
| --- | --- | --- |
| `index_name` | string | Configured `[[storage.indexes]]` name. Register it once during script initialization. |
| `callback` | function | Receives a candidate table and returns exactly `true` (include) or `false` (exclude). |

The candidate table has `index_name`, `user_id`, `collection`, `key`,
`value_json`, `expected_version`, `read_permission`, and `write_permission`.
The callback runs only for `citadel.storage_write` calls whose object matches
the configured collection/key. Returning `false` removes any previous index
membership but does **not** delete the storage object.

**Returns:** nothing.

**Errors:** duplicate/invalid registration, a callback error, deadline expiry,
or a non-boolean callback return rejects that write. The previous storage object
and index membership remain unchanged. Citadel never automatically retries a
callback; script code explicitly retries the write if that is safe.

```lua
citadel.register_storage_index_filter("profiles_by_score", function(candidate)
  local profile = candidate.value_json
  return candidate.key ~= "draft" and profile:find('"published":true', 1, true) ~= nil
end)
```

## Storage concurrency

Storage calls synchronously wait on Citadel's centralized async host-service
bridge while the current runtime VM is serialized. The server uses Tokio's
multi-threaded runtime and `block_in_place`, so other worker tasks continue and
a storage call cannot deadlock the node. A slow storage backend does serialize
this one script VM, however; keep gameplay storage calls small and bounded.
Every handler still has its normal deadline and error-isolation behavior.

## Execution budgets and limits

All limits below are enforced by `src/runtime/lua.rs` and are the same for
every hook unless noted.

| Limit | Value | Configurable | Notes |
| --- | --- | --- | --- |
| Per-invocation deadline | 100 ms default (`DEFAULT_DEADLINE_MS`) | Yes — `[runtime] deadline_ms` (must be `>= 1`) | Applies to `on_message`, `on_join`/`on_leave`, `on_rpc`, `on_room_create`, `on_room_join`. Checked by an instruction-count hook (every 10,000 VM instructions), so it fires promptly on a tight loop without adding meaningful overhead to normal handlers. |
| Tick deadline | derived: `min(50 ms, tick period / 2)`, at least 1 ms | Yes — `[runtime] tick_deadline_ms` (optional; explicit `0` is a config error) | Independent SLO from the message deadline, so a slow tick doesn't inherit the message budget. |
| Script load / hot-reload deadline | 5,000 ms (`LOAD_DEADLINE_MS`) | No (fixed) | Bounds the script's one-time top-level body (its registrations) at initial load and every hot-reload, so an accidental top-level infinite loop can't hang the loader/watcher thread. Enforced by the same instruction hook as handler deadlines. |
| Max outbound commands per invocation | 1024 (`MAX_OUTBOUND_COMMANDS`) | No (fixed) | Shared across `broadcast`/`send`/`spawn_actor`/`move_actor`/`despawn_actor` calls made during one handler invocation. Extra commands are dropped, not raised as an error; the overflow is logged once per invocation. |
| Max body size per `broadcast`/`send` call | 64 KiB (`MAX_OUTBOUND_BODY_BYTES`) | No (fixed) | Exceeding this **does** raise a Lua `RuntimeError` from the call itself (unlike the caps below, which drop silently). |
| Max total outbound bytes per invocation | 1 MiB (`MAX_TOTAL_OUTBOUND_BYTES`) | No (fixed) | Aggregate across every `broadcast`/`send` body in one invocation, bounding a script that queues many full-size bodies (which would otherwise multiply at broadcast fan-out time). Commands past this aggregate are dropped, not raised; logged once per invocation. |

**Isolation guarantee:** every hook invocation runs under a single VM lock, a
panic guard (`catch_unwind`), and the deadline hook described above. A Lua
error, a blown deadline, or an isolated Rust-side panic inside a handler
**never** crashes the node and never wedges the shared VM lock — it is
logged with the script's source label and handler name, any queued outbound
commands from that invocation are discarded, and the next invocation starts
clean. `on_rpc`, `on_room_create`, and `on_room_join` additionally discard
any `broadcast`/`send` a handler attempts — those hooks communicate only
through their return value, never as a side channel.

The standard library exposed to scripts is intentionally narrow —
`string`, `table`, `math` only. There is no `io`, `os`, `package`,
`coroutine`, or `debug`: `coroutine.create` would let a handler spawn work
outside the main VM state the deadline hook watches, and `debug.sethook`
could remove the hook outright — both would let a script evade its time
budget.

:::caution[Cooperative yielding required]
Handlers and ticks must **yield by returning**. The per-invocation deadline
is enforced by an instruction hook, so a pure-Lua `while true do end` is
interrupted — but the platform cannot safely terminate non-cooperative code
in-thread, and a handler wedged inside a blocking native call is beyond the
hook's reach. A match whose script stops yielding is closed server-side: its
members receive a server-error close and are prompted to requeue. If the
stuck thread cannot be reclaimed, the hosting worker process is restarted,
which closes every match it was hosting. Write handlers that return quickly
and never busy-wait for game state.
:::

---

## Hot reload

Hot reload is **opt-in** (`[runtime] hot_reload = false` by default) — a
development convenience, not intended for production. When enabled, the node
polls `<scripts_dir>/main.lua`'s modification time and size every
`[runtime] hot_reload_poll_ms` (default 500 ms) and reloads on change.

Reload is two-phase and failure-safe:

1. **Build off-lock.** The node reads the file and builds a brand-new Lua VM
   (re-running the script's top-level registrations) *without* touching the
   live VM. A missing file, a parse error, or a registration error at this
   stage fails here — the currently-loaded script keeps serving, the failure
   is logged, and the reload outcome is `Rejected`.
2. **Reject an empty/handlerless script.** Even if the new script parses and
   loads cleanly, if it registered **zero** handlers of any kind (message,
   RPC, or lifecycle) the reload is still rejected and the previous script
   keeps serving — this guards against an editor's transient zero-byte save
   silently leaving the node with no handlers.
3. **Swap under the lock.** Only once the new VM is fully built and verified
   to have at least one handler does the node acquire the same lock that
   serializes message dispatch, lifecycle hooks, and ticks, and swap the new
   VM in atomically. A reload can never interleave with an in-flight
   handler invocation.

**In-VM state resets on every reload.** Lua globals a script uses for
in-memory game state are **not** preserved across a hot-reload — the fresh
VM starts clean, `require`'s module cache is rebuilt from scratch, and the
server-owned-actor id counter restarts at `NPC_ID_BASE`. This is expected for
a dev-loop hot-reload; cross-reload state preservation is out of scope.

A `LuaRuntime` built from an in-memory source (not from `<scripts_dir>/main.lua`
on disk) has no backing file to watch, so a reload attempt on it is always a
no-op (`NotReloadable`).

---

## Console runtime endpoints

The operator console exposes the loaded script's registered surface and lets
an operator invoke any registered RPC directly, without a game client, for
debugging. See [Admin console & console API](/reference/admin-api/console/)
for console authentication and roles in general; both routes below require a
valid console session (`Authorization: Bearer <token>`), and the RPC-invoke
route requires the `admin` role.

### GET /console/v1/runtime

Returns runtime facts and, when a script is attached, its introspected
surface.

```
GET /console/v1/runtime
Authorization: Bearer <token>

200 OK
{
  "enabled": true,
  "configured_language": "lua",
  "selected_language": "lua",
  "selection_source": "explicit",
  "entrypoint": "./game/main.lua",
  "adapter": "embedded",
  "tier": "trusted",
  "attached": true,
  "tick_hz": 20,
  "script": {
    "source": "./game/main.lua",
    "reloadable": false,
    "deadline_ms": 100,
    "rpcs": ["add", "ping"],
    "message_kinds": [1],
    "hooks": ["on_join", "on_tick"]
  }
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `enabled` | bool | Whether `[runtime]` is enabled in configuration. |
| `configured_language` | string, optional | Explicit `[runtime] language` when set. Omitted means autodetect. |
| `selected_language` | string, optional | Language selected from explicit config or filesystem autodetection when an entrypoint exists. |
| `selection_source` | string, optional | `"explicit"` or `"autodetected"` for `selected_language`. |
| `entrypoint` | string, optional | Selected entrypoint path, when present. |
| `adapter` | string | Runtime adapter. Currently `"embedded"` is the only implemented adapter. |
| `tier` | string | Runtime tier. Currently `"trusted"` is the only implemented tier. |
| `attached` | bool | Whether a script runtime is actually attached to the realtime gateway (a script was loaded). `false` before transports start or when no selected entrypoint is present. |
| `tick_hz` | integer | Configured `citadel.on_tick` rate; `0` means no game loop. |
| `script` | object or omitted | Present only when `attached`. `source` is the script's path/label; `reloadable` is whether it is backed by an on-disk file the watcher can hot-reload; `deadline_ms` is the per-invocation budget in effect; `rpcs` and `message_kinds` are sorted lists of registered method names / message kinds; `hooks` lists which of `on_join`/`on_leave`/`on_tick`/`on_room_create`/`on_room_join` are registered. |

**Errors:** requires a valid console session; an invalid/missing/expired
token returns `401 authentication_failed` (see the console reference for the
uniform auth-failure behavior).

```bash
curl -s http://localhost:8080/console/v1/runtime \
  -H "Authorization: Bearer $TOKEN"
```

### POST /console/v1/runtime/rpc/:method

Invokes a registered `on_rpc` handler directly from the console, with an
operator-supplied payload, and returns its outcome. The call runs through the
exact same isolated, deadline-bounded `call_rpc` path real game traffic uses,
with no real participant behind it: the handler sees `ctx.sender = 0` and
`ctx.user_id = nil`.

```
POST /console/v1/runtime/rpc/ping
Authorization: Bearer <token>
Content-Type: application/json

{ "payload": "hello" }

200 OK
{ "ok": true, "reply": "pong from ping" }
```

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `:method` (path) | string | yes | The RPC method name to invoke, as registered via `citadel.on_rpc`. |
| `payload` (body) | string | no (default `""`) | Raw UTF-8 payload passed to the handler as its `body` argument. |

**Response:**

| Field | Type | Meaning |
| --- | --- | --- |
| `ok` | bool | Whether the handler ran and returned a reply. |
| `reply` | string, present only when `ok` | The handler's return value, rendered as UTF-8 (lossy). |
| `error` | string, present only when not `ok` | The same short, generic error message a game client would see for this failure (unknown method, handler error, or timeout) — the console never widens the runtime's error surface. |

**Errors:** `403 forbidden` for a `viewer`-role token (mutating call, admin
only); `404 not_found` with `"no script runtime is attached"` if no script
runtime is currently attached; a malformed JSON body returns `400
validation` with the rejection detail. Every invocation (success or failure)
is recorded in the audit log as a `runtime.rpc` entry.

```bash
curl -s -X POST http://localhost:8080/console/v1/runtime/rpc/ping \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"payload":"hello"}'
```

---

## Player notifications

Trusted Lua game logic can create and inspect the durable player inbox. A
successful `notifications_send` commits before attempting local realtime
delivery; the recipient deduplicates the `KIND_NOTIFICATION` stream by `id`.

| Function | Signature | Returns / errors |
| --- | --- | --- |
| `citadel.notifications_send` | `(recipient, code, subject, content_json, sender?, delivery_key?)` | notification table; `content_json` must be JSON and service validation errors raise Lua errors. |
| `citadel.notifications_list` | `(recipient, limit?, cursor?)` | `{ items = {...}, next_cursor = string|nil }`, newest first. |
| `citadel.notifications_mark_read` | `(recipient, ids)` | array of changed IDs; recipient scope and retries are idempotent. |

```lua
local n = citadel.notifications_send("player-42", 7, "Reward", '{"coins":10}', "server", "reward:round-1")
local page = citadel.notifications_list("player-42", 50)
local changed = citadel.notifications_mark_read("player-42", { n.id })
```

## Domain feature calls

### Secure `citadel.chat_call`

`citadel.chat_call(actor, operation, payload_json)` uses the same secure chat
schema as player RPCs. `actor` is an explicit trusted-runtime identity and is
still checked against current friendship, group membership, or room presence;
it is never accepted as a `sender` field inside JSON. For `send`, pass
`{"target":{"kind":"direct","other_user_id":"player-b"},"content":"hi"}`.
For `history`, pass the same target with optional `limit` and `before_id`.
Group targets use `group_id`; room targets are unavailable to this bridge because
only the realtime gateway owns current room presence. Raw `channel` and
`channel_type` payload fields fail with `CHAT_PROTOCOL_UPGRADE_REQUIRED`.
`operation` may be `edit` (`id` plus `content`) or `delete` (`id`): both retain
the explicit actor, canonical target fence, author time window, revision/event
semantics, and durable shared rate limits used by player RPCs. `moderate` accepts
a group target and `id` only; it applies the same group role hierarchy as player
RPCs, fences membership, and writes a redacted durable audit record. Direct and
room moderation targets are rejected.

`citadel.groups_call(actor, operation, payload_json)`,
`citadel.leaderboards_call(actor, operation, payload_json)`,
`citadel.chat_call(actor, operation, payload_json)`, and
`citadel.wallet_call(actor, operation, payload_json)` return JSON strings or
raise Lua errors. Their operation/payload schemas match the corresponding game
client RPCs. Trusted game logic may use wallet `adjust`; clients cannot.

### `citadel.groups_call`

**Signature:** `citadel.groups_call(actor, operation, payload_json) -> string`.
The payload and successful return are JSON strings. Invalid JSON, unknown
operations, capacity, role, and pending-state violations raise Lua errors.

| Operation | Payload JSON | Return JSON |
| --- | --- | --- |
| `join` | `{ "group_id": number }` | `{ "state":"joined", "group":… }` for open groups, or `{ "state":"requested", "admission":… }` for closed groups. |
| `invite` | `{ "group_id": number, "user_id": string }` | `{ "state":"invited", "admission":… }`; requires admin/superadmin. |
| `approve_request` | `{ "group_id": number, "user_id": string }` | Updated group; requires admin/superadmin and a pending request. |
| `accept_invitation` | `{ "group_id": number }` | Updated group for the invited actor. |
| `cancel_admission` | `{ "group_id": number }` | `{}`; repeating cancellation is safe. |
| `transfer_ownership` | `{ "group_id": number, "user_id": string }` | Updated group; only current superadmin may transfer to a member. |

```lua
local pending = citadel.groups_call("owner", "invite", '{"group_id":7,"user_id":"player-42"}')
local group = citadel.groups_call("player-42", "accept_invitation", '{"group_id":7}')
```
