---
title: Tournaments API reference
description: Console operations, lifecycle, discovery, entries, and immutable tournament results.
---

Tournaments are **operator-managed competitions** bound to one leaderboard.
They are created and advanced through the console API; players use the
player-facing discovery and registration API documented separately. A completed
tournament's standings come only from the leaderboard scheduler's immutable
pre-reset snapshot.

:::caution[Do not settle manually]
There is intentionally no HTTP endpoint to mark a tournament `completed` or to
write results. Completion is performed by the durable leaderboard-reset
scheduler after it commits the matching epoch and snapshot. This prevents an
operator retry, a late score, or a foreign leaderboard epoch from changing final
standings.
:::

## Authorization

All routes require a console bearer token from `POST /console/v1/login`.
`viewer` can read tournament discovery, entries, and results. `admin` is
required to create a tournament or perform a lifecycle transition. Mutations
are recorded in the console audit log as `tournaments.create` and
`tournaments.transition`.

## Lifecycle

The only legal transitions are:

```text
Draft -> RegistrationOpen -> Running -> Finalizing -> Completed
Draft | RegistrationOpen | Running -> Cancelled
```

`Finalizing -> Completed` is reserved for scheduler settlement; console
operators must not use it. Every schedule timestamp is Unix epoch milliseconds
and must satisfy:

```text
registration_opens_at <= registration_closes_at <= starts_at <= ends_at
```

## Routes

### Discover tournaments

```
GET /console/v1/tournaments
```

Returns `200` with `{ "items": [...], "total": n }`, ordered by start time
then id. Each item has `id`, `leaderboard_id`, `state`, all schedule timestamps,
`settled_epoch_due_at_unix_ms` (or `null`), and creation/update timestamps.

### Create a tournament

```
POST /console/v1/tournaments
```

**Role:** `admin`.

```json
{
  "id": "weekly-points",
  "leaderboard_id": "points",
  "registration_opens_at_unix_ms": 1760000000000,
  "registration_closes_at_unix_ms": 1760086400000,
  "starts_at_unix_ms": 1760086400000,
  "ends_at_unix_ms": 1760691200000
}
```

Returns `201 Created` with the new `draft` tournament. Duplicate ids and invalid
schedules return `409` and `400` respectively.

### Read a tournament

```
GET /console/v1/tournaments/{id}
```

Returns the full tournament representation or `404`.

### Advance lifecycle

```
POST /console/v1/tournaments/{id}/transition
```

**Role:** `admin`.

```json
{ "state": "registration_open" }
```

Returns the updated tournament. Illegal lifecycle edges return `409`; unknown
or misspelled fields and state tokens return `400`.

### Inspect entrants

```
GET /console/v1/tournaments/{id}/entries
```

Returns `{ "items": [{ "tournament_id", "user_id", "registered_at" }],
"total": n }`. The route verifies the tournament exists before returning an
empty page, so a missing id is `404`, not an indistinguishable empty list.

### Inspect immutable results

```
GET /console/v1/tournaments/{id}/results
```

Returns `{ "items": [{ "tournament_id", "user_id", "rank", "score",
"subscore" }], "total": n }`. Before scheduler settlement, the list is empty.
After settlement, rank order is immutable and is copied from the committed
leaderboard snapshot. A reset retry for the same epoch is idempotent and cannot
create duplicate results.

## Operational checklist

1. Create the bound leaderboard and configure its reset schedule before creating
   a tournament.
2. Create the tournament as `draft`, then open registration and transition to
   `running` at the planned times (or automate these calls from trusted
   operations tooling).
3. Monitor the reset scheduler; it owns finalization, snapshotting, settlement,
   rewards, and player notifications.
4. Verify `completed`, its settled epoch, and `/results`; do not edit rankings
   after completion.
5. Cancel before settlement if the event must stop. Cancellation is terminal.
