//! Canonical host-API surface for embedded game-logic runtimes.
//!
//! This table is the Rust source of truth for the generated
//! `host_api_manifest.json`. Lua and feature-gated Python adapters use this same
//! table to keep their host functions in parity; future adapters must do the
//! same.

/// One language-neutral host API function or hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct HostApiFn {
    /// Stable function name exposed by each runtime adapter.
    pub name: &'static str,
    /// Broad host-API category.
    pub category: HostApiCategory,
    /// Language-neutral parameter descriptors.
    pub params: &'static [&'static str],
    /// Language-neutral return descriptor.
    pub returns: &'static str,
    /// Whether the surface is shipped today or planned.
    pub status: HostApiStatus,
    /// Task/version that introduced the surface, or `-` for planned entries.
    pub since: &'static str,
}

/// Host-API category used by the generated manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostApiCategory {
    /// Message handler registration.
    MessageHook,
    /// Before/after realtime envelope interception registration.
    RealtimeHook,
    /// Join/leave lifecycle hook registration.
    LifecycleHook,
    /// Periodic tick hook registration.
    TickHook,
    /// Leaderboard reset callback registration.
    LeaderboardHook,
    /// Request/response RPC hook registration.
    RpcHook,
    /// Room creation/join hook registration.
    RoomHook,
    /// Outbound world or transport action.
    Action,
    /// Structured logging.
    Log,
    /// Persistent storage host call.
    Storage,
    /// Read-only, cached static gameplay data.
    StaticData,
    /// Read-only, cached text-content policy evaluation.
    TextPolicy,
    /// Outbound HTTP host call.
    Http,
    /// Local best-effort runtime event publication and subscription.
    Event,
    /// Read-only loaded-map query.
    Map,
    /// Server-authoritative navigation query over a loaded map's baked mesh.
    Navigation,
    /// Server-authoritative physics control and state query.
    Physics,
    /// Persisted domain-feature host call (friends, groups, …).
    Domain,
}

/// Whether a host API entry is available in shipped runtimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostApiStatus {
    /// Available in the shipped Lua runtime today.
    Shipped,
    /// Named as future surface but not implemented today.
    Planned,
}

/// Canonical host-API surface for shipped and planned runtime adapters.
pub const HOST_API_SURFACE: &[HostApiFn] = &[
    HostApiFn {
        name: "on_message",
        category: HostApiCategory::MessageHook,
        params: &["kind:u16", "handler:fn(ctx,body)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "before_realtime",
        category: HostApiCategory::RealtimeHook,
        params: &["handler:fn(ctx,body)->bool?"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "after_realtime",
        category: HostApiCategory::RealtimeHook,
        params: &["handler:fn(ctx,body)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_input",
        category: HostApiCategory::RealtimeHook,
        params: &["handler:fn(event:normalized_v2)->outcome"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "IMPL-20260820-AUTH-MATCH-SCOPED-DELIVERY",
    },
    HostApiFn {
        name: "on_join",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(ctx)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "on_leave",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(ctx)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "on_match_created",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_match_started",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_match_ended",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_match_join",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_match_leave",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_match_tick",
        category: HostApiCategory::LifecycleHook,
        params: &["handler:fn(context)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_tick",
        category: HostApiCategory::TickHook,
        params: &["handler:fn(dt:f64)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "on_leaderboard_reset",
        category: HostApiCategory::LeaderboardHook,
        params: &["handler:fn(ctx:{leaderboard_id:string,due_at_unix_ms:u64,fencing_token:u64})"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "on_rpc",
        category: HostApiCategory::RpcHook,
        params: &["method:string", "handler:fn(ctx,body)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "on_room_create",
        category: HostApiCategory::RoomHook,
        params: &["handler:fn(ctx,params)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "on_room_join",
        category: HostApiCategory::RoomHook,
        params: &["handler:fn(ctx,room_id)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "broadcast",
        category: HostApiCategory::Action,
        params: &["kind:u16", "body:bytes", "unreliable:bool?"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "send",
        category: HostApiCategory::Action,
        params: &["session:u64", "kind:u16", "body:bytes", "unreliable:bool?"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "spawn_actor",
        category: HostApiCategory::Action,
        params: &["opts:{archetype:u16?,x:f32?,y:f32?,z:f32?}"],
        returns: "object_id:u32",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "move_actor",
        category: HostApiCategory::Action,
        params: &[
            "object_id:u32",
            "x:f32",
            "y:f32",
            "z:f32",
            "vx:f32?",
            "vy:f32?",
            "vz:f32?",
        ],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "despawn_actor",
        category: HostApiCategory::Action,
        params: &["object_id:u32"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "set_physics",
        category: HostApiCategory::Physics,
        params: &[
            "object_id:u32",
            "opts:{gravity:f32?,buoyancy:f32?,drag:f32?,radius:f32?,height:f32?,max_speed:f32?,shape:string?,enabled:bool?}",
        ],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "apply_impulse",
        category: HostApiCategory::Physics,
        params: &["object_id:u32", "ix:f32", "iy:f32", "iz:f32"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "set_move_intent",
        category: HostApiCategory::Physics,
        params: &["object_id:u32", "vx:f32", "vy:f32", "vz:f32"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "physics_state",
        category: HostApiCategory::Physics,
        params: &["object_id:u32"],
        returns: "{grounded:bool,position:[f32;3],velocity:[f32;3]}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "rewind_query",
        category: HostApiCategory::Action,
        params: &[
            "shooter:u64",
            "origin:[f32;3]",
            "direction:[f32;3]",
            "tick:u64",
        ],
        returns: "{hits:[{object_id:u32,participant:u64,point:[f32;3],distance:f32}]}",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "map_info",
        category: HostApiCategory::Map,
        params: &["name:string"],
        returns: "{bounds_min:[f32;3],bounds_max:[f32;3],vertex_count:usize,triangle_count:usize}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "map_names",
        category: HostApiCategory::Map,
        params: &[],
        returns: "string[]",
        status: HostApiStatus::Shipped,
        since: "IMPL-20260805-AUTHORITATIVE-NAV-HOST-API",
    },
    HostApiFn {
        name: "find_path",
        category: HostApiCategory::Navigation,
        params: &["map:string", "start:[f32;3]", "goal:[f32;3]"],
        returns: "[f32;3][]?",
        status: HostApiStatus::Shipped,
        since: "IMPL-20260805-AUTHORITATIVE-NAV-HOST-API",
    },
    HostApiFn {
        name: "raycast",
        category: HostApiCategory::Map,
        params: &["origin:[f32;3]", "direction:[f32;3]"],
        returns: "{point:[f32;3],normal:[f32;3],distance:f32,triangle_index:usize}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "sphere_overlap",
        category: HostApiCategory::Map,
        params: &["centre:[f32;3]", "radius:f32"],
        returns: "bool",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "ground_height",
        category: HostApiCategory::Map,
        params: &["origin:[f32;3]", "max_distance:f32"],
        returns: "{point:[f32;3],normal:[f32;3],distance:f32,triangle_index:usize}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "log",
        category: HostApiCategory::Log,
        params: &["message:string", "level:string?"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    //  completes the Lua-originated catalog in Python and JavaScript,
    // making this a mechanically enforced, shipped cross-runtime surface.
    HostApiFn {
        name: "static_data.load_json",
        category: HostApiCategory::StaticData,
        params: &["path:relative .json"],
        returns: "object|array",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "static_data.load_csv",
        category: HostApiCategory::StaticData,
        params: &["path:relative .csv"],
        returns: "array<object>",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "text_policy.load_json",
        category: HostApiCategory::TextPolicy,
        params: &["path:relative .json"],
        returns: "policy_ref:string",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "text_policy.scan",
        category: HostApiCategory::TextPolicy,
        params: &["policy_ref:string", "text:string"],
        returns: "{decision:allow|flag|mask|replace|reject,matches:array,text:string}",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "text_policy.sanitize",
        category: HostApiCategory::TextPolicy,
        params: &["policy_ref:string", "text:string"],
        returns: "{decision:allow|flag|mask|replace|reject,matches:array,text:string}",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "friends.add",
        category: HostApiCategory::Domain,
        params: &["user:string", "other:string"],
        returns: "state:string",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "friends.remove",
        category: HostApiCategory::Domain,
        params: &["user:string", "other:string"],
        returns: "removed:bool",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "friends.block",
        category: HostApiCategory::Domain,
        params: &["user:string", "other:string"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "friends.list",
        category: HostApiCategory::Domain,
        params: &["user:string"],
        returns: "rows:list<{user_id:string,state:string,updated_unix_ms:u64}>",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "notifications.send",
        category: HostApiCategory::Domain,
        params: &[
            "recipient:string",
            "code:i32",
            "subject:string",
            "content_json:string",
            "sender:string?",
            "delivery_key:string?",
        ],
        returns: "{id:string,code:i32,subject:string,content:json,sender:string?,created_at_unix_ms:u64,read_at_unix_ms:u64?}",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "notifications.list",
        category: HostApiCategory::Domain,
        params: &["recipient:string", "limit:usize?", "cursor:string?"],
        returns: "{items:list<notification>,next_cursor:string?}",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "notifications.mark_read",
        category: HostApiCategory::Domain,
        params: &["recipient:string", "ids:list<string>"],
        returns: "read_ids:list<string>",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "groups.call",
        category: HostApiCategory::Domain,
        params: &[
            "actor:string",
            "operation:create|list|get|update|delete|add_member|leave|kick|promote|demote|join|invite|approve_request|accept_invitation|cancel_admission|transfer_ownership",
            "payload_json:string",
        ],
        returns: "json",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "leaderboards.call",
        category: HostApiCategory::Domain,
        params: &[
            "actor:string",
            "operation:list|records|submit",
            "payload_json:string",
        ],
        returns: "json",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "tournaments.call",
        category: HostApiCategory::Domain,
        params: &[
            "actor:string",
            "operation:list|get|results|registration",
            "payload_json:string",
        ],
        returns: "json",
        status: HostApiStatus::Shipped,
        since: "IMPL-20260803-TOURNAMENTS-DISCOVERY",
    },
    HostApiFn {
        name: "chat.call",
        category: HostApiCategory::Domain,
        params: &[
            "actor:string",
            "operation:send|history|edit|delete|moderate",
            "payload_json:string",
        ],
        returns: "json",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "wallet.call",
        category: HostApiCategory::Domain,
        params: &[
            "actor:string",
            "operation:balances|ledger|adjust",
            "payload_json:string",
        ],
        returns: "json",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "storage.read",
        category: HostApiCategory::Storage,
        params: &["user:string", "collection:string", "key:string"],
        returns: "{value_json:string,version:string,read_permission:u8,write_permission:u8}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "storage.write",
        category: HostApiCategory::Storage,
        params: &[
            "user:string",
            "collection:string",
            "key:string",
            "value_json:string",
            "expected_version:string?",
            "read_permission:u8?",
            "write_permission:u8?",
        ],
        returns: "{value_json:string,version:string,read_permission:u8,write_permission:u8}",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "storage.delete",
        category: HostApiCategory::Storage,
        params: &[
            "user:string",
            "collection:string",
            "key:string",
            "expected_version:string?",
        ],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "storage.index_query",
        category: HostApiCategory::Storage,
        params: &["index_name:string", "filters_json:string", "limit:usize"],
        returns: "[{user_id:string?,collection:string,key:string,value_json:string,version:string,read_permission:u8,write_permission:u8}]",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "storage.register_index_filter",
        category: HostApiCategory::Storage,
        params: &["index_name:string", "callback:function"],
        returns: "nil",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "http.fetch",
        category: HostApiCategory::Http,
        params: &["url:string", "opts:table?"],
        returns: "{status:u16,body:bytes}",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
    },
    HostApiFn {
        name: "http.start",
        category: HostApiCategory::Http,
        params: &["url:string", "opts:table?"],
        returns: "handle:opaque (Lua/Python:u64; JavaScript:string)",
        status: HostApiStatus::Shipped,
        since: "TASK-0437",
    },
    HostApiFn {
        name: "http.poll",
        category: HostApiCategory::Http,
        params: &["handle:opaque (Lua/Python:u64; JavaScript:string)"],
        returns: "state:table",
        status: HostApiStatus::Shipped,
        since: "TASK-0437",
    },
    HostApiFn {
        name: "http.cancel",
        category: HostApiCategory::Http,
        params: &["handle:opaque (Lua/Python:u64; JavaScript:string)"],
        returns: "state:table",
        status: HostApiStatus::Shipped,
        since: "TASK-0437",
    },
    HostApiFn {
        name: "http.register",
        category: HostApiCategory::Http,
        params: &[
            "method:string",
            "path:string",
            "options:table?",
            "handler:function",
        ],
        returns: "handler:function",
        status: HostApiStatus::Shipped,
        since: "TASK-0416",
    },
    HostApiFn {
        name: "events.emit",
        category: HostApiCategory::Event,
        params: &["namespace:string", "type:string", "payload:bytes"],
        returns: "queued:bool",
        status: HostApiStatus::Shipped,
        since: "TASK-0417",
    },
    HostApiFn {
        name: "events.subscribe",
        category: HostApiCategory::Event,
        params: &["namespace:string", "type:string", "handler:function"],
        returns: "handler:function",
        status: HostApiStatus::Shipped,
        since: "TASK-0417",
    },
    HostApiFn {
        name: "cache.get",
        category: HostApiCategory::Storage,
        params: &["namespace:string", "key:string"],
        returns: "{value:bytes,version:u64,expires_in_ms:u64}?",
        status: HostApiStatus::Shipped,
        since: "TASK-0418",
    },
    HostApiFn {
        name: "cache.set",
        category: HostApiCategory::Storage,
        params: &[
            "namespace:string",
            "key:string",
            "value:bytes",
            "ttl_ms:u64",
        ],
        returns: "{value:bytes,version:u64,expires_in_ms:u64}",
        status: HostApiStatus::Shipped,
        since: "TASK-0418",
    },
    HostApiFn {
        name: "cache.delete",
        category: HostApiCategory::Storage,
        params: &["namespace:string", "key:string"],
        returns: "deleted:bool",
        status: HostApiStatus::Shipped,
        since: "TASK-0418",
    },
    HostApiFn {
        name: "telemetry.begin",
        category: HostApiCategory::Log,
        params: &[],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "telemetry.mark",
        category: HostApiCategory::Log,
        params: &["marker:string"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "telemetry.finish",
        category: HostApiCategory::Log,
        params: &[],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "cache.cas",
        category: HostApiCategory::Storage,
        params: &[
            "namespace:string",
            "key:string",
            "expected_version:u64?",
            "value:bytes",
            "ttl_ms:u64",
        ],
        returns: "{value:bytes,version:u64,expires_in_ms:u64}?",
        status: HostApiStatus::Shipped,
        since: "TASK-0418",
    },
    HostApiFn {
        name: "log.write",
        category: HostApiCategory::Log,
        params: &[
            "level:string",
            "tag:string",
            "message:string",
            "payload_json:string?",
        ],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
    HostApiFn {
        name: "match.set_result",
        category: HostApiCategory::Log,
        params: &["result_json:string"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "unreleased",
    },
];

/// Widest `tag` a script may attach to one durable log line.
///
/// Mirrors the `match_logs.tag` CHECK constraint, so a line the adapter accepts
/// can never be rejected by the database after it has been acknowledged.
pub const MAX_LOG_TAG_BYTES: usize = 64;

/// Widest `message`, mirroring the `match_logs.message` CHECK constraint.
pub const MAX_LOG_MESSAGE_BYTES: usize = 1_024;

/// Widest `payload_json` one line may carry.
///
/// The operator knob is `logs.match_logs.max_payload_bytes`; the runtime
/// adapters hold no configuration handle, so this constant is what every
/// adapter enforces and the test below locks it to the configured default.
pub const MAX_LOG_PAYLOAD_BYTES: usize = 8_192;

/// Validate `citadel.log.write`'s `level` argument.
///
/// Strict about the vocabulary and lenient only about ASCII case, matching the
/// volatile `citadel.log`. It deliberately does not fall back to `info`: this
/// line is persisted, and a stored row that says `info` when the author wrote
/// `eror` is a lie no operator can detect later.
///
/// # Errors
/// Returns the script-facing message for any name outside the five levels.
pub fn validate_log_level(value: &str) -> Result<crate::repository::LogLevel, String> {
    crate::repository::LogLevel::parse(&value.to_ascii_lowercase())
        .map_err(|_| "log level must be one of trace, debug, info, warn, error".to_owned())
}

/// Validate `citadel.log.write`'s `tag` argument.
///
/// The tag is the console's prefix filter, so it is restricted to a shape that
/// stays index-usable and unambiguous: lowercase, no whitespace, and no empty
/// dotted segment.
///
/// # Errors
/// Returns the script-facing message when the tag is empty, too long, carries a
/// character outside `[a-z0-9_.-]`, or has a leading, trailing, or repeated `.`.
pub fn validate_log_tag(value: &str) -> Result<&str, String> {
    const REQUIREMENT: &str =
        "log tag must be 1-64 bytes of [a-z0-9_.-] with no leading, trailing, or repeated '.'";
    if value.is_empty() || value.len() > MAX_LOG_TAG_BYTES {
        return Err(REQUIREMENT.to_owned());
    }
    let shaped = value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
    });
    if !shaped || value.starts_with('.') || value.ends_with('.') || value.contains("..") {
        return Err(REQUIREMENT.to_owned());
    }
    Ok(value)
}

/// Validate `citadel.log.write`'s `message` argument, returning what is stored.
///
/// The trimmed slice is the return value because trimming happens before the
/// length check: a message that is only whitespace is empty, not 1 byte long.
///
/// # Errors
/// Returns the script-facing message when the trimmed text is empty, exceeds
/// [`MAX_LOG_MESSAGE_BYTES`], or contains an ASCII control character.
pub fn validate_log_message(value: &str) -> Result<&str, String> {
    let message = value.trim();
    if message.is_empty() || message.len() > MAX_LOG_MESSAGE_BYTES {
        return Err("log message must be 1-1024 bytes after trimming".to_owned());
    }
    if message.chars().any(|ch| ch.is_ascii_control()) {
        return Err("log message must not contain ASCII control characters".to_owned());
    }
    Ok(message)
}

/// Validate `citadel.log.write`'s optional `payload_json` argument.
///
/// The payload is stored verbatim and is never inspected again, so this is the
/// only place its shape is checked. The size test runs first so an oversized
/// document is refused without parsing it.
///
/// # Errors
/// Returns the script-facing message when the payload exceeds
/// [`MAX_LOG_PAYLOAD_BYTES`] or is not a JSON object or array.
pub fn validate_log_payload(value: &str) -> Result<&str, String> {
    if value.len() > MAX_LOG_PAYLOAD_BYTES {
        return Err(format!(
            "log payload_json must be at most {MAX_LOG_PAYLOAD_BYTES} bytes"
        ));
    }
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(serde_json::Value::Object(_) | serde_json::Value::Array(_)) => Ok(value),
        _ => Err("log payload_json must be a JSON object or array".to_owned()),
    }
}

/// Validate `citadel.match.set_result`'s `result_json` argument.
///
/// Narrower than a log payload on purpose: a match result is one document
/// describing one match, so an array or a bare scalar is a mistake rather than
/// a style choice.
///
/// # Errors
/// Returns the script-facing message when the document exceeds
/// [`crate::match_recorder::MAX_RESULT_JSON_BYTES`] or is not a JSON object.
pub fn validate_match_result(value: &str) -> Result<&str, String> {
    let max = crate::match_recorder::MAX_RESULT_JSON_BYTES;
    if value.len() > max {
        return Err(format!("match result_json must be at most {max} bytes"));
    }
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(serde_json::Value::Object(_)) => Ok(value),
        _ => Err("match result_json must be a JSON object".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::LogLevel;

    #[test]
    fn the_adapter_payload_bound_tracks_the_configured_default() {
        // The adapters hold no config handle, so this constant is the only
        // enforcement point. Drift between the two would silently accept a
        // payload the operator disabled.
        assert_eq!(
            MAX_LOG_PAYLOAD_BYTES,
            crate::config::MatchLogsConfig::default().max_payload_bytes
        );
    }

    #[test]
    fn levels_are_case_insensitive_but_never_default() {
        assert_eq!(validate_log_level("WARN"), Ok(LogLevel::Warn));
        assert_eq!(validate_log_level("info"), Ok(LogLevel::Info));
        assert!(validate_log_level("eror").is_err());
        assert!(validate_log_level("").is_err());
    }

    #[test]
    fn tags_keep_a_prefix_filterable_shape() {
        assert_eq!(validate_log_tag("combat.hit-1_a"), Ok("combat.hit-1_a"));
        for rejected in ["", ".lead", "trail.", "double..dot", "Upper", "has space"] {
            assert!(validate_log_tag(rejected).is_err(), "{rejected}");
        }
        assert!(validate_log_tag(&"a".repeat(MAX_LOG_TAG_BYTES)).is_ok());
        assert!(validate_log_tag(&"a".repeat(MAX_LOG_TAG_BYTES + 1)).is_err());
    }

    #[test]
    fn messages_are_trimmed_before_they_are_measured() {
        assert_eq!(validate_log_message("  round over  "), Ok("round over"));
        assert!(validate_log_message("   ").is_err());
        assert!(validate_log_message("line\nbreak").is_err());
        assert!(validate_log_message(&"m".repeat(MAX_LOG_MESSAGE_BYTES + 1)).is_err());
    }

    #[test]
    fn payloads_must_be_json_containers_and_results_must_be_objects() {
        assert!(validate_log_payload(r#"{"dmg":3}"#).is_ok());
        assert!(validate_log_payload("[1,2,3]").is_ok());
        assert!(validate_log_payload("3").is_err());
        assert!(validate_log_payload("not json").is_err());
        assert!(validate_log_payload(&"x".repeat(MAX_LOG_PAYLOAD_BYTES + 1)).is_err());

        assert!(validate_match_result(r#"{"winner":"kitsune"}"#).is_ok());
        assert!(validate_match_result("[1]").is_err());
        assert!(
            validate_match_result(&"x".repeat(crate::match_recorder::MAX_RESULT_JSON_BYTES + 1))
                .is_err()
        );
    }
}
