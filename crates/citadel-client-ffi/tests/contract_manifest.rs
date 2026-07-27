//! Client-contract manifest generator + stale-guard.
//!
//! `crates/citadel-wire/contract.json` is a flattened, machine-readable manifest
//! of Citadel's client-facing contract: the wire envelope kinds / RPC status
//! codes / body byte-counts (from `citadel-wire`), the C ABI version (from this
//! crate's [`CITADEL_FFI_ABI_VERSION`]), and the HTTP auth block (routes + JSON
//! field names, from `citadel::http::auth`). SDK authors and the Tier-A parity
//! check (`scripts/check-sdk-parity.sh`) read it without a Rust toolchain.
//!
//! This file is BOTH the generator and the stale-guard, mirroring the pattern
//! cbindgen uses for `citadel_client.h`:
//!
//! - Normal `cargo test` run: [`contract_json_is_in_sync`] renders the manifest
//!   from the canonical consts and asserts the checked-in file matches — a stale
//!   `contract.json` (someone changed a const without regenerating) fails CI.
//! - Regenerate: run with `CITADEL_REGEN_CONTRACT=1` to rewrite the file:
//!
//!   ```text
//!   CITADEL_REGEN_CONTRACT=1 cargo test -p citadel-client-ffi --test contract_manifest
//!   ```

// The `serde_json::json!` manifest literal is large enough to exceed the default
// macro recursion limit once the Networked-Actors kinds were added.
#![recursion_limit = "256"]

use std::path::PathBuf;

use citadel::http::auth::{CUSTOM_AUTH_PATH, DEVICE_AUTH_PATH, EMAIL_AUTH_PATH};
use citadel_client_ffi::CITADEL_FFI_ABI_VERSION;
use citadel_wire::baseline::ACK_HISTORY_BITS;
use citadel_wire::codec::{DEFAULT_WORLD_BOUNDS, QuatMode, codec_id};
use citadel_wire::netpeer::{
    LAYOUT_VERSION_BITS, MAX_ACK_ENTRIES, MAX_BUNCHES_PER_ENVELOPE, MAX_BYTES_FIELD_LEN,
    MAX_COLLECTION_OPS, MAX_ENVELOPE_ALLOC, MAX_SCHEMA_ENTRIES, OBJECT_ID_BITS, VARINT_MAX_GROUPS,
};
use citadel_wire::protocol::{
    AUTH_REASON_AUTH_FAILED, AUTH_REASON_AUTH_REQUIRED, AUTH_REASON_PROTOCOL,
    AUTH_STATUS_AUTHENTICATED, AUTH_STATUS_GUEST, AUTH_STATUS_REJECTED, CHAT_KIND_MAX,
    CHAT_KIND_MIN, KIND_AUTH, KIND_AUTH_RESULT, KIND_CHAT_EVENT, KIND_MATCHMAKER_MATCHED,
    KIND_NA_DESPAWN, KIND_NA_PRESENCE, KIND_NA_SPAWN, KIND_NA_SPAWN_BATCH, KIND_NA_STATE,
    KIND_NOTIFICATION, KIND_PEER_POSITION, KIND_POSITION, KIND_REP_ACK, KIND_REP_DELTA,
    KIND_REP_SCHEMA, KIND_ROOM_CREATE, KIND_ROOM_JOIN, KIND_ROOM_JOINED, KIND_ROOM_LEAVE,
    KIND_ROOM_MAP_READY, KIND_RPC_REQUEST, KIND_RPC_RESPONSE, KIND_TSYNC_ACK, KIND_TSYNC_HELLO,
    KIND_TSYNC_INPUT, KIND_TSYNC_REWIND, KIND_TSYNC_ROLE, KIND_TSYNC_SNAPSHOT, MATCHMAKER_KIND_MAX,
    MATCHMAKER_KIND_MIN, NA_KIND_MAX, NA_KIND_MIN, NOTIFICATION_KIND_MAX, NOTIFICATION_KIND_MIN,
    POSITION_BYTES, REP_KIND_MAX, REP_KIND_MIN, ROOM_KIND_MAX, ROOM_KIND_MIN, RPC_METHOD_LEN_BYTES,
    RPC_REQUEST_ID_BYTES, RPC_STATUS_ERROR, RPC_STATUS_OK, SENDER_ID_BYTES, TSYNC_KIND_MAX,
    TSYNC_KIND_MIN,
};
use citadel_wire::schema::{SCHEMA_HASH_ALGORITHM, SCHEMA_HASH_BYTES};

/// Path to the checked-in manifest: `crates/citadel-wire/contract.json`.
fn contract_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("citadel-wire")
        .join("contract.json")
}

/// Render the canonical manifest JSON (pretty-printed, trailing newline).
///
/// Every value is read from a canonical Rust const/route — nothing is
/// hand-written — so the file can never disagree with the sources. `serde_json`
/// (without `preserve_order`) sorts object keys deterministically, which is all
/// the stale-guard requires.
fn render_manifest() -> String {
    // Field-name lists mirror `citadel::http::auth::AuthRequest` /
    // `AuthResponse`. `AuthRequest` is `Deserialize`-only (no `Serialize`) so it
    // cannot be reflected by serializing an instance; these are kept in step with
    // the serde structs by review + the HTTP integration tests. The routes are
    // sourced from the real consts.
    let auth_request_fields = ["id", "create", "username", "display_name", "metadata"];
    let email_auth_request_fields = [
        "email",
        "password",
        "create",
        "username",
        "display_name",
        "metadata",
    ];
    let auth_response_fields = ["token", "refresh_token", "user_id", "username", "created"];

    let manifest = serde_json::json!({
        "abi_version": CITADEL_FFI_ABI_VERSION,
        "wire": {
            "KIND_POSITION": KIND_POSITION,
            "KIND_PEER_POSITION": KIND_PEER_POSITION,
            "KIND_RPC_REQUEST": KIND_RPC_REQUEST,
            "KIND_RPC_RESPONSE": KIND_RPC_RESPONSE,
            "KIND_AUTH": KIND_AUTH,
            "KIND_AUTH_RESULT": KIND_AUTH_RESULT,
            "AUTH_STATUS_AUTHENTICATED": AUTH_STATUS_AUTHENTICATED,
            "AUTH_STATUS_GUEST": AUTH_STATUS_GUEST,
            "AUTH_STATUS_REJECTED": AUTH_STATUS_REJECTED,
            "AUTH_REASON_AUTH_FAILED": AUTH_REASON_AUTH_FAILED,
            "AUTH_REASON_AUTH_REQUIRED": AUTH_REASON_AUTH_REQUIRED,
            "AUTH_REASON_PROTOCOL": AUTH_REASON_PROTOCOL,
            "RPC_STATUS_OK": RPC_STATUS_OK,
            "RPC_STATUS_ERROR": RPC_STATUS_ERROR,
            "RPC_REQUEST_ID_BYTES": RPC_REQUEST_ID_BYTES,
            "RPC_METHOD_LEN_BYTES": RPC_METHOD_LEN_BYTES,
            "SENDER_ID_BYTES": SENDER_ID_BYTES,
            "POSITION_BYTES": POSITION_BYTES,
            // Reserved advanced-netcode kind ranges. No bodies are
            // defined yet; the feature tasks (+/0178+) implement them.
            // The discriminants are pinned here so SDKs and the two tracks never
            // contend for a kind.
            "KIND_TSYNC_HELLO": KIND_TSYNC_HELLO,
            "KIND_TSYNC_SNAPSHOT": KIND_TSYNC_SNAPSHOT,
            "KIND_TSYNC_INPUT": KIND_TSYNC_INPUT,
            "KIND_TSYNC_ACK": KIND_TSYNC_ACK,
            "KIND_TSYNC_ROLE": KIND_TSYNC_ROLE,
            "KIND_TSYNC_REWIND": KIND_TSYNC_REWIND,
            "KIND_REP_DELTA": KIND_REP_DELTA,
            "KIND_REP_ACK": KIND_REP_ACK,
            "KIND_REP_SCHEMA": KIND_REP_SCHEMA,
            // Networked-Actors presence + replicated spawn (, kinds 16-20).
            "KIND_NA_PRESENCE": KIND_NA_PRESENCE,
            "KIND_NA_SPAWN": KIND_NA_SPAWN,
            "KIND_NA_SPAWN_BATCH": KIND_NA_SPAWN_BATCH,
            "KIND_NA_DESPAWN": KIND_NA_DESPAWN,
            "KIND_NA_STATE": KIND_NA_STATE,
            // Rooms: match/lobby membership + map load (Phase A, kinds 21-25).
            "KIND_ROOM_CREATE": KIND_ROOM_CREATE,
            "KIND_ROOM_JOIN": KIND_ROOM_JOIN,
            "KIND_ROOM_JOINED": KIND_ROOM_JOINED,
            "KIND_ROOM_LEAVE": KIND_ROOM_LEAVE,
            "KIND_ROOM_MAP_READY": KIND_ROOM_MAP_READY,
            "KIND_MATCHMAKER_MATCHED": KIND_MATCHMAKER_MATCHED,
            "KIND_NOTIFICATION": KIND_NOTIFICATION,
            "KIND_CHAT_EVENT": KIND_CHAT_EVENT,
            "TSYNC_KIND_MIN": TSYNC_KIND_MIN,
            "TSYNC_KIND_MAX": TSYNC_KIND_MAX,
            "REP_KIND_MIN": REP_KIND_MIN,
            "REP_KIND_MAX": REP_KIND_MAX,
            "NA_KIND_MIN": NA_KIND_MIN,
            "NA_KIND_MAX": NA_KIND_MAX,
            "ROOM_KIND_MIN": ROOM_KIND_MIN,
            "ROOM_KIND_MAX": ROOM_KIND_MAX,
            "MATCHMAKER_KIND_MIN": MATCHMAKER_KIND_MIN,
            "MATCHMAKER_KIND_MAX": MATCHMAKER_KIND_MAX,
            "NOTIFICATION_KIND_MIN": NOTIFICATION_KIND_MIN,
            "NOTIFICATION_KIND_MAX": NOTIFICATION_KIND_MAX,
            "CHAT_KIND_MIN": CHAT_KIND_MIN,
            "CHAT_KIND_MAX": CHAT_KIND_MAX,
            "ACK_HISTORY_BITS": ACK_HISTORY_BITS,
            "SCHEMA_HASH_BYTES": SCHEMA_HASH_BYTES,
        },
        // The shared netcode codec/quantization/baseline contract.
        // These pin the codec-id table, quantization defaults, the schema_hash
        // algorithm, and the baseline model so every SDK encodes identical bits.
        // The byte-exact codec round-trip ground truth lives in
        // crates/citadel-wire/tests/wire_vectors.json.
        "netcode": {
            "bit_order": "msb-first-within-byte",
            "codec_ids": {
                "BOOL": codec_id::BOOL,
                "SCALAR_QUANT": codec_id::SCALAR_QUANT,
                "VECTOR3_QUANT": codec_id::VECTOR3_QUANT,
                "QUAT_SMALLEST3_9": codec_id::QUAT_SMALLEST3_9,
                "QUAT_SMALLEST3_10": codec_id::QUAT_SMALLEST3_10,
                "QUAT_SMALLEST3_15": codec_id::QUAT_SMALLEST3_15,
            },
            "quantization": {
                "canonical_unit": "cm",
                "formula": "levels = round((max-min)*values_per_unit); bits = ceil_log2(levels+1); inclusive endpoints",
                "rounding": "floor(x + 0.5) in f64",
                "saturation": "encode clamps to bounds (never wraps); decode rejects out-of-range codes",
                "default_world_bounds": {
                    "min": DEFAULT_WORLD_BOUNDS.min,
                    "max": DEFAULT_WORLD_BOUNDS.max,
                    "values_per_unit": DEFAULT_WORLD_BOUNDS.values_per_unit,
                },
                "quat_modes": {
                    "Bits9": QuatMode::Bits9.bits_per_component(),
                    "Bits10": QuatMode::Bits10.bits_per_component(),
                    "Bits15": QuatMode::Bits15.bits_per_component(),
                },
            },
            "schema_hash": {
                "algorithm": SCHEMA_HASH_ALGORITHM,
                "bytes": SCHEMA_HASH_BYTES,
            },
            "baseline": {
                "token": "server-issued monotonic nonzero u64 (0 reserved for none/is_full)",
                "ack_history_bits": ACK_HISTORY_BITS,
            },
            // NetworkPeer DeltaBunch wire layout (, kinds 13-15). Pins the
            // bit-packed bunch framing + hostile-input decoder caps so every SDK
            // encodes/validates identically. Bunch layout: object_id (fixed bits) ·
            // is_full (1 bit) · result_id (bit-varint, nonzero) · base_id (bit-varint,
            // absent on full) · schema_hash+layout_version (full only) · changed_mask
            // (num_fields bits) · per set field a value or keyed-collection block.
            "netpeer": {
                "object_id_bits": OBJECT_ID_BITS,
                "layout_version_bits": LAYOUT_VERSION_BITS,
                "varint": "bit-packed LEB128, [continuation:1][data:7] groups, least-significant first, canonical (overlong rejected)",
                "varint_max_groups": VARINT_MAX_GROUPS,
                "result_id": "server-issued nonzero token every bunch establishes; acks name it",
                "base_id": "token diffed against; 0 iff is_full (no base)",
                "collections": "FastArray-style keyed delta: rep_id {index:u32, gen:u32} + rep_key u64; removed/added/changed; duplicate rep_ids rejected",
                "caps": {
                    "max_bunches_per_envelope": MAX_BUNCHES_PER_ENVELOPE,
                    "max_collection_ops": MAX_COLLECTION_OPS,
                    "max_bytes_field_len": MAX_BYTES_FIELD_LEN,
                    "max_envelope_alloc": MAX_ENVELOPE_ALLOC,
                    "max_ack_entries": MAX_ACK_ENTRIES,
                    "max_schema_entries": MAX_SCHEMA_ENTRIES,
                },
            },
            "test_vectors": "crates/citadel-wire/tests/wire_vectors.json",
        },
        "http": {
            "device_auth_path": DEVICE_AUTH_PATH,
            "custom_auth_path": CUSTOM_AUTH_PATH,
            "email_auth_path": EMAIL_AUTH_PATH,
            "auth_request_fields": auth_request_fields,
            "email_auth_request_fields": email_auth_request_fields,
            "auth_response_fields": auth_response_fields,
        },
    });

    let mut rendered = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
    rendered.push('\n');
    rendered
}

#[test]
fn contract_json_is_in_sync() {
    let expected = render_manifest();
    let path = contract_path();

    if std::env::var_os("CITADEL_REGEN_CONTRACT").is_some() {
        std::fs::write(&path, &expected).expect("write contract.json");
        eprintln!("regenerated {}", path.display());
        return;
    }

    let actual = std::fs::read_to_string(&path).expect(
        "read crates/citadel-wire/contract.json; regenerate with \
         CITADEL_REGEN_CONTRACT=1 cargo test -p citadel-client-ffi --test contract_manifest",
    );

    assert_eq!(
        actual, expected,
        "crates/citadel-wire/contract.json is stale relative to the canonical \
         Rust consts. Regenerate with: \
         CITADEL_REGEN_CONTRACT=1 cargo test -p citadel-client-ffi --test contract_manifest"
    );
}
