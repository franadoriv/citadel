---
title: Manage a player session
description: Read a player profile, rotate an expiring session, and log out safely through Citadel's player HTTP API.
---

Use this guide after device or custom authentication has returned an access
token and a refresh token. It covers a normal game lifecycle, not player search
or social discovery.

## Step 1 — Store the token pair safely

Treat both strings as passwords. Keep them in platform-appropriate secure
storage, never in logs, analytics, crash reports, URLs, or gameplay messages.
The access token authenticates ordinary account requests and the realtime
handshake. The refresh token exists only to get a replacement pair.

## Step 2 — Read or update the signed-in player

Call `GET /v1/account` with `Authorization: Bearer <access-token>` to obtain
the current player's safe profile. It contains only id, username, and optional
display name.

To let a player edit their visible identity, call `PATCH /v1/account` with the
same bearer. They may change their username or display name; `null` clears the
display name. Metadata, credentials, account state, and creation data stay on
the server.

## Step 3 — Resolve a player you already know

When the game already has a player id or exact username — for example, from a
friend invitation or a leaderboard record — call `POST /v1/users/lookup` with
the access bearer and up to 100 exact ids/usernames. The result only contains
active public profiles. An absent result deliberately does not say whether the
name never existed, was disabled, or was removed.

Do not use it as a directory: substring search, listing accounts, presence, and
recommendations are intentionally not part of this API.

## Step 4 — Refresh before the access token stops working

When an authenticated request returns `401 authentication_failed`, call
`POST /v1/session/refresh` with the stored refresh token. On `200`, replace
**both** stored values before sending another authenticated or realtime request.
The old refresh token is immediately invalid, so two devices must not race to
refresh the same stored pair.

If refresh returns `401`, discard the token pair and use device/custom
authentication again. The response never tells you whether it was expired,
revoked, malformed, or already used.

## Step 5 — Log out

Call `POST /v1/session/logout` when the player chooses to sign out. Send the
access bearer, the refresh token in the JSON body, or both tokens from the same
session. A successful response is always `204 No Content`, including retries
and already-invalid credentials. Then erase the locally stored pair.

Logout revokes only the named session; it does not sign a player out of another
device. If both supplied tokens do not belong together, Citadel performs no
revocation and still returns `204` to avoid leaking session information.

## Next step

Use the replacement access token in the first `KIND_AUTH` envelope when opening
or reconnecting a realtime connection. See the full request/response shapes and
error contract in the [authentication reference](/reference/client-sdk/authentication/).
