---
title: Use secure durable chat
description: Join an authorized local chat channel, consume durable live events, and reconcile safely.
---

Citadel chat is durable first and has live delivery across authenticated cluster
nodes. A mutation and its bounded delivery-outbox row commit together before
Citadel attempts reliable `KIND_CHAT_EVENT` fan-out. The event stream is
at-least-once, not a replacement for history: deduplicate it by
`(channel_id, event_id)` and use history after reconnect or reconciliation.

## Runnable order

1. Authenticate the player normally. The RPC actor is derived from that session;
   never send a user or sender id in chat JSON.
2. Call `chat.join` with a server-derived target. It returns an opaque
   `channel_id`, local presence snapshot, and `watermark_event_id`. Joining the
   same channel again from the same connection is idempotent.
3. Register the released SDK's inbound handler for `KIND_CHAT_EVENT` (28) before
   rendering live activity. Its UTF-8 JSON body has a `type`: `presence.join`,
   `presence.leave`, `message.create`, `message.update`, `message.remove`,
   `access.revoked`, or `resync_required`.
4. Use only that returned `channel_id` with `chat.send`, `chat.history`,
   `chat.edit`, `chat.delete`, and `chat.moderate`. A copied id grants no access:
   Citadel reauthorizes the connection on every operation.
5. If an event is duplicated, retain only the larger durable event for its
   `(channel_id, event_id)` key. If `resync_required` arrives, call
   `chat.history` and pass the returned `watermark_event_id` as
   `acknowledge_watermark`; do not clear the local warning based only on a
   partial older page.
6. Call `chat.leave` when the chat view no longer needs delivery. Disconnects,
   blocks, group kicks/leaves/deletes, and room-access revocation remove local
   subscriptions and emit the appropriate presence/revocation event. Remote
   delivery retries only within its 30-second bounded window; afterward history
   is the recovery path.

**Expected result:** `chat.join` returns one opaque `channel_id`; a permitted
`chat.send` commits a message and each joined recipient on a current cluster
lease receives a
`KIND_CHAT_EVENT`. If a private join returns `CHAT_UNAVAILABLE`, treat it as a
normal unavailable target—do not reveal whether friendship, membership, or a
block caused it. On `resync_required`, fetch history and acknowledge its returned
watermark before trusting the live view again.

```json
{
  "target": { "kind": "direct", "other_user_id": "player-b" }
}
```

`chat.join` supports `{ "kind":"direct", "other_user_id":"player-b" }`,
`{ "kind":"group", "group_id": 42 }`, and `{ "kind":"room" }` (the
caller’s current gateway room). Direct chat needs mutual friendship and no block
in either direction. Group chat needs current membership; room chat needs
current room presence. Citadel returns `CHAT_UNAVAILABLE` for every unavailable
private target, so clients must not infer which condition failed.

The old `{ "channel", "channel_type" }` payload is rejected with
`CHAT_PROTOCOL_UPGRADE_REQUIRED`. After joining, `target` is also rejected from
all subsequent chat calls; use the opaque `channel_id` instead. See the
[chat RPC reference](/reference/client-sdk/domain-features/#chat) and
[wire envelope reference](/reference/protocol/envelope/#kind_chat_event-28--local-chat-presence-and-durable-live-delivery)
for request, response, and event fields.

Content must be non-empty valid UTF-8 text no longer than 2,048 bytes. Sends,
edits, deletes, and history reads use durable multi-key limits. Author edits are
allowed for five minutes; author deletes for 24 hours. Group moderation is
limited to eligible group admins and superadmins; it writes a redacted durable
audit record. Typing is not part of this delivery feature.
