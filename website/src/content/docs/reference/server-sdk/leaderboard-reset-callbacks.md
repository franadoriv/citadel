---
title: Leaderboard reset callbacks
description: Register durable, at-least-once callbacks for fenced leaderboard reset epochs.
---

`on_leaderboard_reset` runs after Citadel commits a leaderboard reset epoch. The reset transaction snapshots the pre-reset records, clears the live leaderboard, persists the epoch, and places a callback item in a durable outbox. A callback is acknowledged only after it returns successfully, so handlers **must be idempotent**.

The scheduler holds a durable lease and carries a monotonic fencing token. Do not treat this hook as exactly-once delivery; use `(leaderboard_id, due_at_unix_ms)` as your idempotency key.

## Context

Every implementation receives one context object:

| Field | Type | Meaning |
| --- | --- | --- |
| `leaderboard_id` | string | Reset leaderboard identifier. |
| `due_at_unix_ms` | integer | UTC scheduled epoch timestamp in Unix milliseconds. |
| `fencing_token` | integer | Scheduler authority token for this committed epoch. |

## Lua

```lua
citadel.on_leaderboard_reset(function(ctx)
  -- Persist/reward exactly once using ctx.leaderboard_id .. ":" .. ctx.due_at_unix_ms.
  citadel.logger_info("reset " .. ctx.leaderboard_id)
end)
```

## Python

```python
@citadel.on_leaderboard_reset
def on_leaderboard_reset(ctx):
    key = f"{ctx['leaderboard_id']}:{ctx['due_at_unix_ms']}"
    # Perform an idempotent post-reset action keyed by `key`.
```

## JavaScript

```javascript
citadel.on_leaderboard_reset((ctx) => {
  const key = `${ctx.leaderboard_id}:${ctx.due_at_unix_ms}`;
  // Perform an idempotent post-reset action keyed by key.
});
```

## Failure and retry

Throwing/raising from the handler leaves the outbox item pending. Citadel retries it later; the reset itself is not rolled back. Keep handlers short, avoid non-idempotent external effects, and store completed epoch keys before issuing rewards or notifications.
