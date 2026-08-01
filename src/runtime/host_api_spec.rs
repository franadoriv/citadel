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
    /// Outbound HTTP host call.
    Http,
    /// Local best-effort runtime event publication and subscription.
    Event,
    /// Read-only loaded-map query.
    Map,
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
        name: "on_tick",
        category: HostApiCategory::TickHook,
        params: &["handler:fn(dt:f64)"],
        returns: "void",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
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
        name: "map_info",
        category: HostApiCategory::Map,
        params: &["name:string"],
        returns: "{bounds_min:[f32;3],bounds_max:[f32;3],vertex_count:usize,triangle_count:usize}?",
        status: HostApiStatus::Shipped,
        since: "pre-1.0",
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
];
