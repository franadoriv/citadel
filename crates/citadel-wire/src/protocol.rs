//! Shared realtime relay protocol constants and helpers (Step 1 gateway).
//!
//! These are the message kinds and body conventions used by the gateway's
//! single-room relay and the demos. Keeping them in `citadel-wire` means the
//! server gateway, the Rust client SDK, and the demos all agree without
//! duplicating constants. A richer, typed message taxonomy is future work.

/// Envelope kind a client sends to report its own position.
///
/// Body convention (used by the demos): two little-endian `f32` coordinates.
pub const KIND_POSITION: u16 = 1;

/// Envelope kind the server relays to peers, carrying the sender's session id.
///
/// Body: an 8-byte big-endian sender session id followed by the original
/// position payload.
pub const KIND_PEER_POSITION: u16 = 2;

/// Envelope kind a client sends to invoke a server-side RPC (request/response).
///
/// Unlike the fire-and-forget position relay, an RPC expects exactly one
/// correlated [`KIND_RPC_RESPONSE`] back to the caller only. Body layout (see
/// [`encode_rpc_request`]): big-endian `request_id: u64`, `method_len: u16`,
/// `method: utf8` (`method_len` bytes), then the opaque request `payload`.
pub const KIND_RPC_REQUEST: u16 = 3;

/// Envelope kind the server sends back to the caller of a [`KIND_RPC_REQUEST`].
///
/// Body layout (see [`encode_rpc_response`]): big-endian `request_id: u64`
/// (echoing the request for correlation), `status: u8`
/// ([`RPC_STATUS_OK`]/[`RPC_STATUS_ERROR`]), then the `payload` — the handler's
/// reply bytes on success, or a short utf8 error message on failure. The message
/// is deliberately generic and never carries a Lua stack trace or server
/// internals.
pub const KIND_RPC_RESPONSE: u16 = 4;

/// Envelope kind a client sends as the FIRST frame on a realtime connection to
/// present its identity.
///
/// Body convention: the raw bytes of the client's HTTP-issued session access
/// token (utf-8). An **empty** body is an explicit request to connect as a guest
/// (anonymous participant). The server validates a presented token via its
/// session service and binds the connection to the resolved account before any
/// other message kind is processed; see [`KIND_AUTH_RESULT`].
///
/// The handshake is uniform across every transport: the client sends this same
/// envelope first over the reliable path (a WebSocket binary message, or a QUIC/
/// WebTransport stream), and receives exactly one [`KIND_AUTH_RESULT`] back.
pub const KIND_AUTH: u16 = 5;

/// Envelope kind the server sends back to answer a [`KIND_AUTH`] handshake
///. Always delivered reliably.
///
/// Body layout (see [`encode_auth_result`]): a single `status` byte
/// ([`AUTH_STATUS_AUTHENTICATED`] / [`AUTH_STATUS_GUEST`] /
/// [`AUTH_STATUS_REJECTED`]) followed by the resolved account's `user_id` (utf-8)
/// on the authenticated path, and nothing else on the guest/rejected paths. The
/// rejected status is deliberately opaque: it never distinguishes an
/// unknown/expired/revoked token, and never carries a server-internal reason.
pub const KIND_AUTH_RESULT: u16 = 6;

// --- Reserved advanced-netcode kind ranges ------------------------
//
// Both finalized designs independently proposed kinds starting at 7, which would
// collide. To keep the transform-sync and NetworkPeer
// tracks parallel-safe on this file, the disjoint ranges are reserved up front:
// transform-sync owns kinds 7..=12, NetworkPeer owns kinds 13..=15.
// defines NO frame bodies for these kinds; the feature tasks (+/0178+)
// implement the bodies. The constants exist here so the two tracks never contend
// for the same discriminant, and so the client contract records the reservation.

/// First kind (inclusive) reserved for transform-sync frames.
pub const TSYNC_KIND_MIN: u16 = 7;
/// Last kind (inclusive) reserved for transform-sync frames.
pub const TSYNC_KIND_MAX: u16 = 12;
/// First kind (inclusive) reserved for NetworkPeer replication frames.
pub const REP_KIND_MIN: u16 = 13;
/// Last kind (inclusive) reserved for NetworkPeer replication frames.
pub const REP_KIND_MAX: u16 = 15;
/// First kind (inclusive) reserved for Networked-Actors (presence + spawn) frames.
pub const NA_KIND_MIN: u16 = 16;
/// Last kind (inclusive) reserved for Networked-Actors (presence + spawn) frames.
pub const NA_KIND_MAX: u16 = 20;
/// First kind (inclusive) reserved for room (match/lobby) frames.
pub const ROOM_KIND_MIN: u16 = 21;
/// Last kind (inclusive) reserved for room (match/lobby) frames.
pub const ROOM_KIND_MAX: u16 = 25;
/// First kind (inclusive) reserved for ticket-matchmaker notifications.
pub const MATCHMAKER_KIND_MIN: u16 = 26;
/// Last kind (inclusive) reserved for ticket-matchmaker notifications.
pub const MATCHMAKER_KIND_MAX: u16 = 26;
/// First kind (inclusive) reserved for player-notification stream envelopes.
pub const NOTIFICATION_KIND_MIN: u16 = 27;
/// Last kind (inclusive) reserved for player-notification stream envelopes.
pub const NOTIFICATION_KIND_MAX: u16 = 27;
/// First kind (inclusive) reserved for chat presence and durable live events.
pub const CHAT_KIND_MIN: u16 = 28;
/// Last kind (inclusive) reserved for chat presence and durable live events.
pub const CHAT_KIND_MAX: u16 = 28;

/// Reserved: transform-sync connect negotiation (world bounds, precision, rates).
pub const KIND_TSYNC_HELLO: u16 = 7;
/// Reserved: transform-sync per-client delta snapshot (unreliable hot path).
pub const KIND_TSYNC_SNAPSHOT: u16 = 8;
/// Reserved: transform-sync owner input bundle (unreliable, redundant).
pub const KIND_TSYNC_INPUT: u16 = 9;
/// Reserved: transform-sync snapshot ack (absolute id + 32-bit ack bitfield).
pub const KIND_TSYNC_ACK: u16 = 10;
/// Reserved: transform-sync ownership/role transition (reliable, idempotent).
pub const KIND_TSYNC_ROLE: u16 = 11;
/// Reserved: transform-sync authoritative rewind hit result (reliable).
pub const KIND_TSYNC_REWIND: u16 = 12;

/// Reserved: NetworkPeer property `DeltaBunch` (reliable by default).
pub const KIND_REP_DELTA: u16 = 13;
/// Reserved: NetworkPeer baseline ack (`[(object_id, baseline_id)…]`).
pub const KIND_REP_ACK: u16 = 14;
/// Reserved: NetworkPeer schema table (`class_id → schema_hash`) on join.
pub const KIND_REP_SCHEMA: u16 = 15;

/// Reserved: Networked-Actors presence announce (`C→S`, reliable): a connecting
/// client's avatar `{archetype_id, initial transform}` — triggers the spawn
/// fan-out.
pub const KIND_NA_PRESENCE: u16 = 16;
/// Reserved: Networked-Actors spawn one (`S→C`, reliable):
/// `{object_id, archetype_id, owner, transform}`.
pub const KIND_NA_SPAWN: u16 = 17;
/// Reserved: Networked-Actors batch spawn (`S→C`, reliable): all currently-present
/// actors, sent to a newly-joined client.
pub const KIND_NA_SPAWN_BATCH: u16 = 18;
/// Reserved: Networked-Actors despawn (`S→C`, reliable): `{object_id}`.
pub const KIND_NA_DESPAWN: u16 = 19;
/// Reserved: Networked-Actors owner state report (`C→S`, unreliable): the owner's
/// authoritative `{object_id, transform}` in relay mode; the server applies it and
/// the normal transform-sync snapshots replicate it to observers.
pub const KIND_NA_STATE: u16 = 20;

/// Reserved: room create request (`C→S`, reliable): opaque `{params}` the game's Lua
/// `on_room_create` interprets (e.g. desired map). The server assigns a room id and
/// auto-joins the creator, replying `KIND_ROOM_JOINED`.
pub const KIND_ROOM_CREATE: u16 = 21;
/// Reserved: room join request (`C→S`, reliable): `{room_id}`. Runs the Lua admission
/// gate `on_room_join`; on accept the server replies `KIND_ROOM_JOINED`.
pub const KIND_ROOM_JOIN: u16 = 22;
/// Reserved: room joined (`S→C`, reliable): `{room_id, map, mode}` — the "you are in
/// room R, load this map" signal. Map/mode come from the Lua-set room label.
pub const KIND_ROOM_JOINED: u16 = 23;
/// Reserved: room leave (`C→S` request or `S→C` notify, reliable): `{room_id}`.
pub const KIND_ROOM_LEAVE: u16 = 24;
/// Reserved: room map-ready ack (`C→S`, reliable): `{room_id}` — the client has the
/// room's map/level open; the server may now include it in room fan-out.
pub const KIND_ROOM_MAP_READY: u16 = 25;
/// Reliable matchmaker handoff notification (`S→C`): UTF-8 JSON
/// `{ticket_id, match_id, join_token, expires_at}`. The client must present the
/// opaque token to `matchmaker.accept`; a raw `match_id` never authorizes room
/// admission.
pub const KIND_MATCHMAKER_MATCHED: u16 = 26;
/// Durable player-notification live delivery (`S→C`, reliable): UTF-8 JSON of
/// the persisted notification. Delivery is best effort and at-least-once; a
/// client deduplicates by `id` and reconciles with `notifications.list`.
pub const KIND_NOTIFICATION: u16 = 27;
/// Authorized chat presence, ephemeral typing, and durable mutation delivery
/// (`S→C`, reliable).
///
/// The UTF-8 JSON body is an at-least-once event. Clients deduplicate durable
/// events by `(channel_id, event_id)` and reconcile with `chat.history` after a
/// `resync_required` event. A `typing` event is non-durable and carries a
/// receiver-side `expires_at` timestamp instead of an event id.
pub const KIND_CHAT_EVENT: u16 = 28;

/// Transform-sync v2 negotiation manifest (reliable, C↔S).  This is a
/// separate kind rather than an appended v1 `HELLO`, so a v1 decoder can never
/// accidentally interpret epoch-bearing metadata.
pub const KIND_TSYNC_V2_HELLO: u16 = 29;
/// Transform-sync v2 snapshot (unreliable, S→C), carrying gameplay-clock
/// epoch/tick/rate metadata followed by the unchanged v1 snapshot body.
pub const KIND_TSYNC_V2_SNAPSHOT: u16 = 30;
/// Transform-sync v2 owner input (unreliable, C→S), carrying an opaque epoch
/// fence and bounded diagnostics followed by the unchanged v1 input body.
pub const KIND_TSYNC_V2_INPUT: u16 = 31;

// Compile-time guarantees that the reserved ranges are disjoint and sit above the
// legacy kinds (1..=6). A future edit that overlaps them fails to build rather
// than silently colliding on the wire.
const _: () = assert!(TSYNC_KIND_MAX < REP_KIND_MIN);
const _: () = assert!(REP_KIND_MAX < NA_KIND_MIN);
const _: () = assert!(NA_KIND_MAX < ROOM_KIND_MIN);
const _: () = assert!(ROOM_KIND_MAX < MATCHMAKER_KIND_MIN);
const _: () = assert!(TSYNC_KIND_MIN > KIND_AUTH_RESULT);
const _: () = assert!(KIND_TSYNC_HELLO == TSYNC_KIND_MIN && KIND_TSYNC_REWIND == TSYNC_KIND_MAX);
const _: () = assert!(KIND_REP_DELTA == REP_KIND_MIN && KIND_REP_SCHEMA == REP_KIND_MAX);
const _: () = assert!(KIND_NA_PRESENCE == NA_KIND_MIN && KIND_NA_STATE == NA_KIND_MAX);
const _: () = assert!(KIND_ROOM_CREATE == ROOM_KIND_MIN && KIND_ROOM_MAP_READY == ROOM_KIND_MAX);
const _: () = assert!(KIND_MATCHMAKER_MATCHED == MATCHMAKER_KIND_MIN);
const _: () = assert!(MATCHMAKER_KIND_MAX < NOTIFICATION_KIND_MIN);
const _: () = assert!(
    KIND_NOTIFICATION == NOTIFICATION_KIND_MIN && NOTIFICATION_KIND_MIN == NOTIFICATION_KIND_MAX
);
const _: () = assert!(NOTIFICATION_KIND_MAX < CHAT_KIND_MIN);
const _: () = assert!(KIND_CHAT_EVENT == CHAT_KIND_MIN && CHAT_KIND_MIN == CHAT_KIND_MAX);

/// [`KIND_AUTH_RESULT`] status: the token validated; the connection is bound to
/// the `user_id` that follows in the body.
pub const AUTH_STATUS_AUTHENTICATED: u8 = 0;

/// [`KIND_AUTH_RESULT`] status: the connection was accepted as an anonymous
/// guest (no account bound). Only possible when the server allows guests.
pub const AUTH_STATUS_GUEST: u8 = 1;

/// [`KIND_AUTH_RESULT`] status: the handshake was refused. The body carries a
/// coarse [`reason class`](AUTH_REASON_AUTH_FAILED) byte and the connection is
/// closed immediately after. The reason is intentionally coarse: an
/// invalid/expired/revoked/malformed token all collapse to
/// [`AUTH_REASON_AUTH_FAILED`], so the client learns *that* it was refused but
/// never *why* at a level that could aid enumeration.
pub const AUTH_STATUS_REJECTED: u8 = 2;

/// Rejection reason class: the presented token failed validation. Deliberately
/// collapses unknown/expired/revoked/malformed so no enumeration oracle exists.
pub const AUTH_REASON_AUTH_FAILED: u8 = 0;

/// Rejection reason class: a guest/token-less connect was refused because the
/// server requires authentication.
pub const AUTH_REASON_AUTH_REQUIRED: u8 = 1;

/// Rejection reason class: the handshake violated the protocol (the first frame
/// was not [`KIND_AUTH`], a duplicate auth, an oversized token, or auth on an
/// unreliable path).
pub const AUTH_REASON_PROTOCOL: u8 = 2;

/// Encode an authenticated [`KIND_AUTH_RESULT`] body: `[AUTH_STATUS_AUTHENTICATED,
/// user_id (utf-8)…]`.
#[must_use]
pub fn encode_auth_authenticated(user_id: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + user_id.len());
    buf.push(AUTH_STATUS_AUTHENTICATED);
    buf.extend_from_slice(user_id.as_bytes());
    buf
}

/// Encode a guest [`KIND_AUTH_RESULT`] body: just `[AUTH_STATUS_GUEST]`. Carries
/// no account information.
#[must_use]
pub fn encode_auth_guest() -> Vec<u8> {
    vec![AUTH_STATUS_GUEST]
}

/// Encode a rejected [`KIND_AUTH_RESULT`] body: `[AUTH_STATUS_REJECTED,
/// reason_class]`. The reason class is coarse by design (see
/// [`AUTH_REASON_AUTH_FAILED`]) and never carries a free-form string.
#[must_use]
pub fn encode_auth_rejected(reason_class: u8) -> Vec<u8> {
    vec![AUTH_STATUS_REJECTED, reason_class]
}

/// A decoded [`KIND_AUTH_RESULT`], borrowing the `user_id` from the source body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthResult<'a> {
    /// One of [`AUTH_STATUS_AUTHENTICATED`] / [`AUTH_STATUS_GUEST`] /
    /// [`AUTH_STATUS_REJECTED`].
    pub status: u8,
    /// The resolved account id on the authenticated path; empty otherwise.
    pub user_id: &'a str,
    /// The coarse rejection reason class on the rejected path; `0` otherwise.
    pub reason_class: u8,
}

impl AuthResult<'_> {
    /// Whether the handshake authenticated the connection to an account.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.status == AUTH_STATUS_AUTHENTICATED
    }

    /// Whether the connection was accepted as a guest.
    #[must_use]
    pub fn is_guest(&self) -> bool {
        self.status == AUTH_STATUS_GUEST
    }

    /// Whether the handshake was refused.
    #[must_use]
    pub fn is_rejected(&self) -> bool {
        self.status == AUTH_STATUS_REJECTED
    }
}

/// Decode a [`KIND_AUTH_RESULT`] body.
///
/// Returns `None` for an empty body (no status byte). On the authenticated path
/// the remaining bytes are the utf-8 `user_id` (non-utf8 is rejected as `None`).
/// On the rejected path the second byte, if present, is the reason class. Guest
/// carries neither.
#[must_use]
pub fn decode_auth_result(body: &[u8]) -> Option<AuthResult<'_>> {
    let (status, rest) = body.split_first()?;
    match *status {
        AUTH_STATUS_AUTHENTICATED => {
            let user_id = std::str::from_utf8(rest).ok()?;
            Some(AuthResult {
                status: *status,
                user_id,
                reason_class: 0,
            })
        }
        AUTH_STATUS_REJECTED => Some(AuthResult {
            status: *status,
            user_id: "",
            reason_class: rest.first().copied().unwrap_or(AUTH_REASON_AUTH_FAILED),
        }),
        _ => Some(AuthResult {
            status: *status,
            user_id: "",
            reason_class: 0,
        }),
    }
}

/// RPC response status: the handler ran and `payload` is its reply.
pub const RPC_STATUS_OK: u8 = 0;

/// RPC response status: the request failed (unknown method, handler error, or a
/// blown deadline); `payload` is a short utf8 error message.
pub const RPC_STATUS_ERROR: u8 = 1;

/// Bytes used for the big-endian `request_id` correlation prefix in both the RPC
/// request and response bodies.
pub const RPC_REQUEST_ID_BYTES: usize = 8;

/// Bytes used for the `method_len` prefix in an RPC request body.
///
/// Public because client SDKs depend on this byte count to encode/decode RPC
/// request bodies; it is a value the client-contract manifest
/// (`crates/citadel-wire/contract.json`) and the Tier-A SDK parity check compare
/// against (see `docs/architecture/client-sdk-sync.md`).
pub const RPC_METHOD_LEN_BYTES: usize = 2;

/// Bytes in a `KIND_POSITION` / `KIND_PEER_POSITION` position payload: two
/// little-endian `f32` coordinates `(x, y)`.
///
/// This is a shared wire convention (not demo-local): every client SDK encodes a
/// position as exactly these bytes, so it is part of the canonical client
/// contract compared by the Tier-A SDK parity check
/// (see `docs/architecture/client-sdk-sync.md`).
pub const POSITION_BYTES: usize = 8;

/// Minimum RPC request body: `request_id` (8) + `method_len` (2), zero-length
/// method and empty payload.
const RPC_REQUEST_MIN_BYTES: usize = RPC_REQUEST_ID_BYTES + RPC_METHOD_LEN_BYTES;

/// Minimum RPC response body: `request_id` (8) + `status` (1), empty payload.
const RPC_RESPONSE_MIN_BYTES: usize = RPC_REQUEST_ID_BYTES + 1;

/// A decoded RPC request, borrowing the method/payload from the source body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcRequest<'a> {
    /// Client-chosen correlation id, echoed back in the response.
    pub request_id: u64,
    /// The RPC method name (validated utf8).
    pub method: &'a str,
    /// Opaque request payload bytes.
    pub payload: &'a [u8],
}

/// A decoded RPC response, borrowing the payload from the source body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcResponse<'a> {
    /// The correlation id echoed from the request.
    pub request_id: u64,
    /// [`RPC_STATUS_OK`] or [`RPC_STATUS_ERROR`].
    pub status: u8,
    /// Reply bytes on success, or a short utf8 error message on failure.
    pub payload: &'a [u8],
}

impl RpcResponse<'_> {
    /// Whether this response reports success ([`RPC_STATUS_OK`]).
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.status == RPC_STATUS_OK
    }
}

/// Encode an RPC request body: `request_id | method_len | method | payload`.
///
/// RPC methods are short identifiers. A pathological method longer than 64 KiB
/// (`u16::MAX`) — a caller bug — is clamped at a UTF-8 char boundary so the
/// emitted `method_len` always matches the method bytes actually written: the
/// encoder can never produce a body that mis-parses the payload as part of the
/// method (or vice versa). A clamped method simply resolves to a different name
/// and the server answers with a well-formed "unknown method" error.
#[must_use]
pub fn encode_rpc_request(request_id: u64, method: &str, payload: &[u8]) -> Vec<u8> {
    let method_bytes = clamp_method_bytes(method);
    // Length always fits u16 after clamping, so it matches the bytes written.
    let method_len = method_bytes.len() as u16;
    let mut buf = Vec::with_capacity(RPC_REQUEST_MIN_BYTES + method_bytes.len() + payload.len());
    buf.extend_from_slice(&request_id.to_be_bytes());
    buf.extend_from_slice(&method_len.to_be_bytes());
    buf.extend_from_slice(method_bytes);
    buf.extend_from_slice(payload);
    buf
}

/// Clamp a method name to at most `u16::MAX` bytes on a UTF-8 char boundary.
///
/// Returns the whole method for the normal (short) case; only a >64 KiB method is
/// truncated, keeping the length prefix and the emitted bytes in agreement.
fn clamp_method_bytes(method: &str) -> &[u8] {
    const MAX: usize = u16::MAX as usize;
    if method.len() <= MAX {
        return method.as_bytes();
    }
    let mut end = MAX;
    while end > 0 && !method.is_char_boundary(end) {
        end -= 1;
    }
    &method.as_bytes()[..end]
}

/// Decode an RPC request body produced by [`encode_rpc_request`].
///
/// Returns `None` for a body too short to hold the header, a `method_len` that
/// overruns the buffer, or a method that is not valid utf8. The gateway drops a
/// malformed request (it cannot be correlated without a trustworthy header).
#[must_use]
pub fn decode_rpc_request(body: &[u8]) -> Option<RpcRequest<'_>> {
    if body.len() < RPC_REQUEST_MIN_BYTES {
        return None;
    }
    let request_id = u64::from_be_bytes(body[..RPC_REQUEST_ID_BYTES].try_into().ok()?);
    let method_len = u16::from_be_bytes(
        body[RPC_REQUEST_ID_BYTES..RPC_REQUEST_MIN_BYTES]
            .try_into()
            .ok()?,
    ) as usize;
    let method_start = RPC_REQUEST_MIN_BYTES;
    let method_end = method_start.checked_add(method_len)?;
    if body.len() < method_end {
        return None;
    }
    let method = std::str::from_utf8(&body[method_start..method_end]).ok()?;
    let payload = &body[method_end..];
    Some(RpcRequest {
        request_id,
        method,
        payload,
    })
}

/// Encode an RPC response body: `request_id | status | payload`.
#[must_use]
pub fn encode_rpc_response(request_id: u64, status: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(RPC_RESPONSE_MIN_BYTES + payload.len());
    buf.extend_from_slice(&request_id.to_be_bytes());
    buf.push(status);
    buf.extend_from_slice(payload);
    buf
}

/// Decode an RPC response body produced by [`encode_rpc_response`].
///
/// Returns `None` for a body too short to hold the `request_id` + `status`
/// header.
#[must_use]
pub fn decode_rpc_response(body: &[u8]) -> Option<RpcResponse<'_>> {
    if body.len() < RPC_RESPONSE_MIN_BYTES {
        return None;
    }
    let request_id = u64::from_be_bytes(body[..RPC_REQUEST_ID_BYTES].try_into().ok()?);
    let status = body[RPC_REQUEST_ID_BYTES];
    let payload = &body[RPC_RESPONSE_MIN_BYTES..];
    Some(RpcResponse {
        request_id,
        status,
        payload,
    })
}

/// Number of bytes used to prefix a relayed message with the sender session id.
pub const SENDER_ID_BYTES: usize = 8;

/// Prefix `payload` with the big-endian `u64` sender session id.
#[must_use]
pub fn tag_with_sender(sender_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SENDER_ID_BYTES + payload.len());
    buf.extend_from_slice(&sender_id.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Split a relayed peer-position body into `(sender_id, payload)`.
///
/// Returns `None` if the body is too short to contain the sender id.
#[must_use]
pub fn split_sender(body: &[u8]) -> Option<(u64, &[u8])> {
    if body.len() < SENDER_ID_BYTES {
        return None;
    }
    let id = u64::from_be_bytes(
        body[..SENDER_ID_BYTES]
            .try_into()
            .expect("checked length above"),
    );
    Some((id, &body[SENDER_ID_BYTES..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_and_split_round_trip() {
        let payload = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let tagged = tag_with_sender(42, &payload);
        let (id, rest) = split_sender(&tagged).expect("split");
        assert_eq!(id, 42);
        assert_eq!(rest, &payload);
    }

    #[test]
    fn split_rejects_short_body() {
        assert!(split_sender(&[0u8; 4]).is_none());
    }

    #[test]
    fn kinds_are_stable() {
        assert_eq!(KIND_POSITION, 1);
        assert_eq!(KIND_PEER_POSITION, 2);
        assert_eq!(KIND_RPC_REQUEST, 3);
        assert_eq!(KIND_RPC_RESPONSE, 4);
        assert_eq!(KIND_AUTH, 5);
        assert_eq!(KIND_AUTH_RESULT, 6);
    }

    #[test]
    fn reserved_netcode_kinds_are_stable_and_disjoint() {
        // Transform-sync 7..=12.
        assert_eq!(KIND_TSYNC_HELLO, 7);
        assert_eq!(KIND_TSYNC_SNAPSHOT, 8);
        assert_eq!(KIND_TSYNC_INPUT, 9);
        assert_eq!(KIND_TSYNC_ACK, 10);
        assert_eq!(KIND_TSYNC_ROLE, 11);
        assert_eq!(KIND_TSYNC_REWIND, 12);
        // NetworkPeer 13..=15.
        assert_eq!(KIND_REP_DELTA, 13);
        assert_eq!(KIND_REP_ACK, 14);
        assert_eq!(KIND_REP_SCHEMA, 15);
        // Networked-Actors 16..=20.
        assert_eq!(KIND_NA_PRESENCE, 16);
        assert_eq!(KIND_NA_STATE, 20);
        // Rooms 21..=25.
        assert_eq!(KIND_ROOM_CREATE, 21);
        assert_eq!(KIND_ROOM_JOIN, 22);
        assert_eq!(KIND_ROOM_JOINED, 23);
        assert_eq!(KIND_ROOM_LEAVE, 24);
        assert_eq!(KIND_ROOM_MAP_READY, 25);
        assert_eq!(KIND_MATCHMAKER_MATCHED, 26);
        assert_eq!(KIND_NOTIFICATION, 27);
        assert_eq!(KIND_CHAT_EVENT, 28);
        // Range bounds.
        assert_eq!((TSYNC_KIND_MIN, TSYNC_KIND_MAX), (7, 12));
        assert_eq!((REP_KIND_MIN, REP_KIND_MAX), (13, 15));
        assert_eq!((NA_KIND_MIN, NA_KIND_MAX), (16, 20));
        assert_eq!((ROOM_KIND_MIN, ROOM_KIND_MAX), (21, 25));
        assert_eq!((MATCHMAKER_KIND_MIN, MATCHMAKER_KIND_MAX), (26, 26));
        assert_eq!((NOTIFICATION_KIND_MIN, NOTIFICATION_KIND_MAX), (27, 27));
        assert_eq!((CHAT_KIND_MIN, CHAT_KIND_MAX), (28, 28));
    }

    #[test]
    fn auth_status_codes_are_stable() {
        // Part of the client contract (contract.json) mirrored by every SDK.
        assert_eq!(AUTH_STATUS_AUTHENTICATED, 0);
        assert_eq!(AUTH_STATUS_GUEST, 1);
        assert_eq!(AUTH_STATUS_REJECTED, 2);
    }

    #[test]
    fn auth_reason_classes_are_stable() {
        assert_eq!(AUTH_REASON_AUTH_FAILED, 0);
        assert_eq!(AUTH_REASON_AUTH_REQUIRED, 1);
        assert_eq!(AUTH_REASON_PROTOCOL, 2);
    }

    #[test]
    fn auth_result_authenticated_round_trips_user_id() {
        let encoded = encode_auth_authenticated("user-abc");
        let decoded = decode_auth_result(&encoded).expect("decodes");
        assert!(decoded.is_authenticated());
        assert_eq!(decoded.user_id, "user-abc");
    }

    #[test]
    fn auth_result_guest_carries_no_payload() {
        let encoded = encode_auth_guest();
        assert_eq!(encoded, vec![AUTH_STATUS_GUEST]);
        let decoded = decode_auth_result(&encoded).expect("decodes");
        assert!(decoded.is_guest());
        assert_eq!(decoded.user_id, "");
    }

    #[test]
    fn auth_result_rejected_carries_only_a_coarse_reason() {
        // A rejection must never carry a user id or free-form reason: just the
        // status and a coarse reason class.
        let encoded = encode_auth_rejected(AUTH_REASON_AUTH_FAILED);
        assert_eq!(encoded, vec![AUTH_STATUS_REJECTED, AUTH_REASON_AUTH_FAILED]);
        let decoded = decode_auth_result(&encoded).expect("decodes");
        assert!(decoded.is_rejected());
        assert_eq!(decoded.user_id, "");
        assert_eq!(decoded.reason_class, AUTH_REASON_AUTH_FAILED);

        let required_body = encode_auth_rejected(AUTH_REASON_AUTH_REQUIRED);
        let required = decode_auth_result(&required_body).expect("decodes");
        assert_eq!(required.reason_class, AUTH_REASON_AUTH_REQUIRED);
    }

    #[test]
    fn auth_result_rejects_empty_body() {
        assert!(decode_auth_result(&[]).is_none());
    }

    #[test]
    fn auth_result_rejected_without_reason_defaults_to_auth_failed() {
        // A bare rejected status byte (no reason) decodes to the safe default.
        let decoded = decode_auth_result(&[AUTH_STATUS_REJECTED]).expect("decodes");
        assert_eq!(decoded.reason_class, AUTH_REASON_AUTH_FAILED);
    }

    #[test]
    fn auth_result_authenticated_rejects_non_utf8_user_id() {
        let mut body = vec![AUTH_STATUS_AUTHENTICATED];
        body.extend_from_slice(&[0xFF, 0xFE]);
        assert!(decode_auth_result(&body).is_none());
    }

    #[test]
    fn contract_byte_counts_are_stable() {
        // These are part of the canonical client contract (contract.json) and are
        // mirrored by every client SDK; drift here must be a deliberate, reviewed
        // change that also regenerates contract.json.
        assert_eq!(RPC_STATUS_OK, 0);
        assert_eq!(RPC_STATUS_ERROR, 1);
        assert_eq!(RPC_REQUEST_ID_BYTES, 8);
        assert_eq!(RPC_METHOD_LEN_BYTES, 2);
        assert_eq!(SENDER_ID_BYTES, 8);
        assert_eq!(POSITION_BYTES, 8);
    }

    #[test]
    fn rpc_request_round_trip() {
        let encoded = encode_rpc_request(0xDEAD_BEEF, "ping", b"payload-bytes");
        let req = decode_rpc_request(&encoded).expect("decodes");
        assert_eq!(req.request_id, 0xDEAD_BEEF);
        assert_eq!(req.method, "ping");
        assert_eq!(req.payload, b"payload-bytes");
    }

    #[test]
    fn rpc_request_round_trip_empty_method_and_payload() {
        // A zero-length method and empty payload are structurally valid (the
        // gateway treats an empty method as an unknown method at dispatch).
        let encoded = encode_rpc_request(7, "", &[]);
        let req = decode_rpc_request(&encoded).expect("decodes");
        assert_eq!(req.request_id, 7);
        assert_eq!(req.method, "");
        assert!(req.payload.is_empty());
    }

    #[test]
    fn rpc_request_rejects_truncated_header() {
        // Fewer than request_id(8) + method_len(2) bytes cannot hold a header.
        assert!(decode_rpc_request(&[0u8; 9]).is_none());
    }

    #[test]
    fn rpc_request_rejects_method_len_overrun() {
        // request_id = 0, method_len = 50 but no method bytes follow.
        let mut body = 0u64.to_be_bytes().to_vec();
        body.extend_from_slice(&50u16.to_be_bytes());
        assert!(decode_rpc_request(&body).is_none());
    }

    #[test]
    fn rpc_request_rejects_non_utf8_method() {
        let mut body = 1u64.to_be_bytes().to_vec();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // invalid utf8 method bytes
        assert!(decode_rpc_request(&body).is_none());
    }

    #[test]
    fn rpc_response_round_trip_ok_and_error() {
        let ok = encode_rpc_response(11, RPC_STATUS_OK, b"pong");
        let decoded = decode_rpc_response(&ok).expect("decodes");
        assert_eq!(decoded.request_id, 11);
        assert!(decoded.is_ok());
        assert_eq!(decoded.payload, b"pong");

        let err = encode_rpc_response(11, RPC_STATUS_ERROR, b"unknown method");
        let decoded = decode_rpc_response(&err).expect("decodes");
        assert!(!decoded.is_ok());
        assert_eq!(decoded.status, RPC_STATUS_ERROR);
        assert_eq!(decoded.payload, b"unknown method");
    }

    #[test]
    fn rpc_response_rejects_truncated_header() {
        assert!(decode_rpc_response(&[0u8; 8]).is_none());
    }

    #[test]
    fn rpc_request_over_long_method_never_misparses() {
        // A pathological >64 KiB method must not corrupt the payload boundary: the
        // encoded method_len always matches the emitted method bytes, so decode
        // round-trips with the payload intact (the method is merely clamped).
        let long_method = "m".repeat(70_000);
        let encoded = encode_rpc_request(1, &long_method, b"real-payload");
        let req = decode_rpc_request(&encoded).expect("decodes");
        assert_eq!(req.request_id, 1);
        assert_eq!(req.method.len(), u16::MAX as usize, "clamped to u16::MAX");
        assert!(long_method.starts_with(req.method));
        assert_eq!(req.payload, b"real-payload", "payload boundary is intact");
    }

    #[test]
    fn rpc_response_round_trip_empty_payload() {
        let encoded = encode_rpc_response(3, RPC_STATUS_OK, &[]);
        let decoded = decode_rpc_response(&encoded).expect("decodes");
        assert_eq!(decoded.request_id, 3);
        assert!(decoded.payload.is_empty());
    }
}
