//! The embedded Lua VM host: loading, the host API, dispatch, and isolation.
//!
//! A [`LuaRuntime`] wraps a single `mlua` [`Lua`] state behind a `Mutex`. It
//! exposes a tiny host API to scripts and turns inbound messages into a bounded
//! list of [`OutboundCommand`] values that the gateway applies. See the module
//! docs in [`crate::runtime`] for the concurrency and safety model.

use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use citadel_physics::{PhysicsConfig, Shape};
use mlua::{Function, HookTriggers, Lua, LuaOptions, StdLib, Table, Value, VmState};

use crate::authoritative_telemetry_slices::{
    TelemetrySliceService, active_runtime_context, set_active_runtime_scope,
};
use crate::config::LuaExecutionMode;
use crate::error::{AppError, AppResult, ErrorCategory};
use crate::maps::MapCatalog;
use crate::realtime::TransformHub;
use crate::runtime::host_services::{DomainHost, StorageWriteInput};
use crate::runtime::outbound_http::{
    AsyncOutboundHttp, OutboundHttpPolicy, OutboundHttpRequest, OutboundHttpRequestState,
    TrustedHttpClient,
};
use crate::runtime::static_data::StaticDataCatalog;
use crate::runtime::text_policy::TextPolicyCatalog;
use crate::runtime::{
    BridgeCommandSink, BridgeTransform, Correction, Decision, InputOutcome,
    MAX_RUNTIME_EVENTS_PER_INVOCATION, NormalizedEvent, NormalizedEventBatch, NormalizedPayload,
    RealtimeAfterOutcome, RealtimeInterception, Runtime, RuntimeEvent, RuntimeEventBus,
    RuntimeEventBusHandle, RuntimeEventEmitOutcome, RuntimeHttpAuth, RuntimeHttpEndpoint,
    RuntimeHttpEndpointPolicy, RuntimeHttpMethod, RuntimeHttpOutcome, RuntimeHttpRequest,
    RuntimeHttpResponse, RuntimeSharedCache, RuntimeSharedCacheHandle, ScriptCommandBatch,
    append_runtime_event_commands, disabled_runtime_event_bus_handle,
    disabled_runtime_shared_cache_handle, runtime_event_bus, runtime_shared_cache,
    script_command_from_outbound, set_runtime_event_bus, set_runtime_shared_cache,
};
use crate::services::PlayerNotification;
use crate::storage::StorageIndexName;
use crate::time::{Clock, SystemClock};

/// Default per-invocation time budget for a script handler, in milliseconds.
pub const DEFAULT_DEADLINE_MS: u64 = 100;

/// Time budget for running a script's top-level body (its registrations) at
/// load and hot-reload, in milliseconds.
///
/// Generous compared to a per-message deadline because one-time setup may build
/// tables, but still bounds an accidental top-level infinite loop
/// (`while true do end` outside any handler) so a bad edit cannot hang the
/// loader/watcher thread. Enforced by the same instruction-count hook as
/// handlers.
const LOAD_DEADLINE_MS: u64 = 5_000;

/// How often (in VM instructions) the deadline hook checks the time budget.
///
/// Small enough to abort a tight `while true do end` promptly, large enough that
/// the hook itself is negligible overhead on normal handlers.
const HOOK_INSTRUCTION_INTERVAL: u32 = 10_000;

/// Maximum number of outbound commands a single handler invocation may enqueue.
///
/// A runaway script that spams `broadcast`/`send` is capped here; extra commands
/// are dropped and the overflow is logged once per invocation.
const MAX_OUTBOUND_COMMANDS: usize = 1024;

/// Maximum body size (bytes) accepted for a single outbound command.
const MAX_OUTBOUND_BODY_BYTES: usize = 64 * 1024; // 64 KiB per message

/// Maximum total outbound body bytes a single handler invocation may enqueue.
///
/// Bounds a buggy script that would otherwise queue `MAX_OUTBOUND_COMMANDS`
/// full-size bodies (and multiply that by every recipient at fan-out time),
/// which could OOM the node. Commands past this aggregate are dropped and the
/// overflow is logged once per invocation.
const MAX_TOTAL_OUTBOUND_BYTES: usize = 1 << 20; // 1 MiB per invocation

/// Standard libraries exposed to scripts.
///
/// Sandboxed Lua deliberately omits `coroutine` and `debug`: the deadline hook
/// is installed on the main Lua state and would not cover `coroutine.create`,
/// while `debug.sethook` could remove the hook. It also omits `io`, `os`, and
/// `package`. Trusted Lua uses mlua's complete *safe* standard-library set,
/// including those machine-access libraries. `debug` and native C-module
/// loading remain unavailable in both modes: mlua exposes them only through an
/// unsafe Rust constructor, which Citadel deliberately does not permit.
fn script_stdlib(execution_mode: LuaExecutionMode) -> StdLib {
    match execution_mode {
        LuaExecutionMode::Sandboxed => StdLib::STRING | StdLib::TABLE | StdLib::MATH,
        LuaExecutionMode::Trusted => StdLib::ALL_SAFE,
    }
}

/// Registry key under which the per-kind handler table is stored in the Lua state.
const HANDLERS_KEY: &str = "citadel.handlers";

/// Registry key under which the per-method RPC handler table is stored.
const RPC_HANDLERS_KEY: &str = "citadel.rpc_handlers";

/// Registry key for runtime HTTP endpoint declarations. Each value is a table
/// containing the validated method/path/auth data and its Lua callback.
const HTTP_ENDPOINT_HANDLERS_KEY: &str = "citadel.http_endpoint_handlers";

/// Registry key for per-namespace/type ordered runtime event subscribers.
const EVENT_HANDLERS_KEY: &str = "citadel.event_handlers";

/// Bound per-key callback fan-out for a single local event snapshot.
const MAX_RUNTIME_EVENT_SUBSCRIBERS: usize = 64;

/// Registry key for the module cache (`require`d module name -> returned value).
///
/// Populated by the scoped [`install_require`] loader; one entry per module,
/// loaded once per VM (a re-`require` returns the cached value). Reset with the
/// whole VM on hot-reload.
const MODULES_KEY: &str = "citadel.modules";

/// Registry key for the set of modules currently mid-load, used to detect and
/// reject cyclic `require` chains before they recurse forever.
const MODULES_LOADING_KEY: &str = "citadel.modules_loading";

/// Client-visible message when no `citadel.on_rpc` handler matches the method.
///
/// Deliberately generic: it names the client-supplied method (safe to echo) but
/// never leaks server internals.
const RPC_ERR_UNKNOWN_METHOD: &str = "unknown RPC method";

/// Client-visible message when a handler exceeds its per-invocation deadline.
const RPC_ERR_TIMEOUT: &str = "RPC handler timed out";

/// Client-visible message for any other handler failure (Lua error, bad return
/// type, or an isolated Rust panic). Never carries a stack trace or internals.
const RPC_ERR_HANDLER: &str = "RPC handler error";

/// Registry key holding the `on_join` lifecycle handler (a single function).
const ON_JOIN_KEY: &str = "citadel.on_join";

/// Registry key holding the `on_leave` lifecycle handler (a single function).
const ON_LEAVE_KEY: &str = "citadel.on_leave";

/// Registry key holding the `on_tick` game-loop handler (a single function).
const ON_TICK_KEY: &str = "citadel.on_tick";

/// Registry key holding the `on_leaderboard_reset` callback (a single function).
const ON_LEADERBOARD_RESET_KEY: &str = "citadel.on_leaderboard_reset";

/// Registry key holding the `on_room_create` handler (returns a room label spec).
const ON_ROOM_CREATE_KEY: &str = "citadel.on_room_create";

/// Registry key holding the `on_room_join` handler (admission gate; returns bool).
const ON_ROOM_JOIN_KEY: &str = "citadel.on_room_join";

/// Registry key holding the `before_realtime` interceptor.
const BEFORE_REALTIME_KEY: &str = "citadel.before_realtime";

/// Registry key holding the authoritative-bridge per-input handler
/// (`citadel.on_input`). Invoked once per normalized event in a delivered
/// batch; its return value becomes the event's [`InputOutcome`].
const ON_INPUT_KEY: &str = "citadel.on_input";

/// Registry key holding the `after_realtime` observer.
const AFTER_REALTIME_KEY: &str = "citadel.after_realtime";

/// A participant lifecycle transition dispatched to a script handler.
///
/// The gateway invokes the matching handler when a participant registers
/// ([`Join`](LifecycleHook::Join)) or unregisters
/// ([`Leave`](LifecycleHook::Leave)). Both receive a `ctx` table carrying at
/// least `ctx.sender` (the participant id) and may `broadcast`/`send`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleHook {
    /// A participant just registered; `citadel.on_join(ctx)` runs.
    Join,
    /// A participant is about to unregister; `citadel.on_leave(ctx)` runs.
    Leave,
}

impl LifecycleHook {
    /// Registry key of the handler this hook dispatches to.
    const fn registry_key(self) -> &'static str {
        match self {
            Self::Join => ON_JOIN_KEY,
            Self::Leave => ON_LEAVE_KEY,
        }
    }

    /// Stable label for logs and overflow diagnostics.
    const fn label(self) -> &'static str {
        match self {
            Self::Join => "on_join",
            Self::Leave => "on_leave",
        }
    }
}

/// Settings for attaching a kinematic physics body to a server-simulated actor.
///
/// Host-language adapters translate their language-specific optional fields into
/// this fully specified command payload before it reaches the gateway.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhysicsOptions {
    /// Whether a body is attached. `false` detaches any existing body.
    pub enabled: bool,
    /// Shape and movement tuning for an enabled body.
    pub config: PhysicsConfig,
}

impl Default for PhysicsOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            config: PhysicsConfig::default(),
        }
    }
}

/// A side effect a script requested during one handler invocation.
///
/// The runtime never performs I/O itself; it returns these for the gateway to
/// apply against its session registry. `unreliable` maps to the transport's
/// best-effort delivery when the transport supports it (WebSocket is
/// reliable-only and delivers either way).
#[derive(Debug, Clone, PartialEq)]
pub enum OutboundCommand {
    /// Send `body` (kind `kind`) to every session except the original sender.
    Broadcast {
        /// Wire kind of the outbound envelope.
        kind: u16,
        /// Opaque payload bytes.
        body: Vec<u8>,
        /// Whether best-effort delivery was requested.
        unreliable: bool,
    },
    /// Send `body` (kind `kind`) to a single participant by raw id.
    Send {
        /// Target participant id (raw value).
        session: u64,
        /// Wire kind of the outbound envelope.
        kind: u16,
        /// Opaque payload bytes.
        body: Vec<u8>,
        /// Whether best-effort delivery was requested.
        unreliable: bool,
    },
    /// Spawn a server-owned networked actor (an NPC) with a script-assigned
    /// `object_id`. The gateway places it in the transform world and fans out an
    /// `NA_SPAWN` so every client instantiates the proxy for `archetype`. Movement
    /// is driven by the script via [`MoveActor`](OutboundCommand::MoveActor).
    SpawnActor {
        /// Script-assigned server-owned object id (high range, never a player id).
        object_id: u32,
        /// Client archetype id to instantiate for the proxy.
        archetype: u16,
        /// Initial world position `[x, y, z]` (cm).
        position: [f32; 3],
    },
    /// Update a server-owned actor's authoritative transform (the per-tick move
    /// path). Snapshots carry it to clients; `velocity` lets them interpolate.
    MoveActor {
        /// The actor's object id (from [`SpawnActor`](OutboundCommand::SpawnActor)).
        object_id: u32,
        /// New world position `[x, y, z]` (cm).
        position: [f32; 3],
        /// Facing quaternion `[x, y, z, w]`.
        rotation: [f32; 4],
        /// Linear velocity `[x, y, z]` (cm/s).
        velocity: [f32; 3],
    },
    /// Attach, reconfigure, or detach an opt-in kinematic body on a
    /// server-simulated actor. `None` and `enabled = false` detach.
    SetPhysics {
        /// The server-owned actor to configure.
        object_id: u32,
        /// Physics settings, or `None` to detach the body.
        opts: Option<PhysicsOptions>,
    },
    /// Add an instantaneous velocity change to a bodied server-simulated actor.
    ApplyImpulse {
        /// The server-owned actor to change.
        object_id: u32,
        /// Velocity delta in cm/s.
        impulse: [f32; 3],
    },
    /// Set a bodied server-simulated actor's desired control velocity.
    SetMoveIntent {
        /// The server-owned actor to steer.
        object_id: u32,
        /// Desired velocity in cm/s; vertical control is ignored by physics.
        intent: [f32; 3],
    },
    /// Despawn a server-owned actor and fan out an `NA_DESPAWN`.
    DespawnActor {
        /// The actor's object id.
        object_id: u32,
    },
}

/// Command buffer stored as Lua app data and drained after each invocation.
///
/// A distinct app-data type from [`Deadline`] so the deadline hook can read the
/// deadline while a `broadcast`/`send` callback mutably borrows the sink without
/// a borrow conflict.
#[derive(Default)]
struct CommandSink {
    commands: Vec<OutboundCommand>,
    total_bytes: usize,
    overflowed: bool,
}

impl CommandSink {
    fn reset(&mut self) {
        self.commands.clear();
        self.total_bytes = 0;
        self.overflowed = false;
    }
}

/// The current invocation deadline, stored as Lua app data and read by the hook.
#[derive(Clone, Copy)]
struct Deadline(Option<Instant>);

/// Marks invocations that may inspect realtime envelopes but must not use host
/// capabilities with direct, external effects.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InvocationMode {
    Normal,
    RealtimeInterceptor,
}

/// Base object id for server-owned actors (NPCs). Player/presence ids grow from 1,
/// so NPCs live in a high range and the two id spaces never collide.
const NPC_ID_BASE: u32 = 0x4000_0000;

/// Monotonic allocator for server-owned actor ids, kept as Lua app data so
/// `spawn_actor` can return the id synchronously. Unlike the command sink it is not
/// reset per invocation; a hot-reload rebuilds the VM and restarts the counter
/// (acceptable — the gateway just replaces any reused id).
struct NpcIdCounter(u32);

/// Server-owned patrol state declared through `citadel.spawn_actor`.
struct NpcPatrol {
    object_id: u32,
    map: String,
    position: [f32; 3],
    waypoints: Vec<[f32; 3]>,
    next_waypoint: usize,
    speed: f32,
}

#[derive(Default)]
struct NpcPatrols(Vec<NpcPatrol>);

/// The persisted-domain-services seam (friends, …) made available to host
/// functions via VM app-data. Present only when the runtime was
/// built with [`LuaRuntime::with_domain_host`]; absent for service-less runtimes
/// (most tests), where the `citadel.friends_*` functions error cleanly.
struct DomainHostHandle(Arc<dyn DomainHost>);

/// Loaded map catalog made available to read-only script host calls.
struct MapCatalogHandle(Arc<MapCatalog>);

/// Authoritative transform hub made available to synchronous physics reads.
struct TransformHubHandle(Arc<TransformHub>);

/// Private trusted-runtime bridge; scripts receive no recorder or report handles.
struct TelemetrySlicesHandle(Arc<TelemetrySliceService>);

/// Install (or refresh) the domain-host seam on a VM's app-data.
///
/// Called after each VM build (initial + hot-reload) so `citadel.friends_*` can
/// reach the services across a reload. A no-op when no host is attached.
fn apply_domain_host(lua: &Lua, domain: &Option<Arc<dyn DomainHost>>) {
    if let Some(host) = domain {
        lua.set_app_data(DomainHostHandle(Arc::clone(host)));
    }
}

fn apply_map_catalog(lua: &Lua, maps: &Option<Arc<MapCatalog>>) {
    if let Some(maps) = maps {
        lua.set_app_data(MapCatalogHandle(Arc::clone(maps)));
    }
}

fn apply_transform_hub(lua: &Lua, hub: &Option<Arc<TransformHub>>) {
    if let Some(hub) = hub {
        lua.set_app_data(TransformHubHandle(Arc::clone(hub)));
    }
}

fn apply_telemetry_slices(lua: &Lua, slices: &Option<Arc<TelemetrySliceService>>) {
    if let Some(slices) = slices {
        lua.set_app_data(TelemetrySlicesHandle(Arc::clone(slices)));
    }
}

/// The swappable VM state guarded by the runtime lock.
///
/// A hot-reload replaces this whole value atomically under the mutex, so a fresh
/// `Lua` (with its re-run registrations) and its label always move together and
/// stay serialized with any in-flight dispatch/lifecycle/tick invocation.
struct LuaVm {
    lua: Lua,
    /// Human-readable label for the loaded script (path or test label), for logs.
    source_label: String,
    /// Parsed static gameplay data initialized with this VM. Replaced atomically
    /// with the VM on hot reload so a bad data edit cannot partially publish.
    static_data: StaticDataCatalog,
}

/// The outcome of a [`LuaRuntime::call_rpc`] invocation.
///
/// Unlike the fire-and-forget [`dispatch`](LuaRuntime::dispatch) path (which
/// returns commands and swallows every failure into an empty list), an RPC must
/// return a value to the caller. Success carries the handler's reply bytes;
/// every failure mode (unknown method, Lua error, blown deadline, isolated
/// panic) collapses to a short, client-safe [`Err`](RpcOutcome::Err) message —
/// the real error is logged server-side but never returned to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcOutcome {
    /// The handler ran and returned these reply bytes (binary-safe).
    Ok(Vec<u8>),
    /// The request failed; the string is a short, generic client-facing message.
    Err(String),
}

/// Whether a locked RPC handler invocation found a handler (and its reply) or
/// none was registered for the method.
enum RpcInner {
    /// A handler ran and produced these reply bytes.
    Reply(Vec<u8>),
    /// No handler was registered for the requested method.
    NoHandler,
}

/// A room label produced by the Lua `on_room_create` handler. The
/// gateway maps this onto its own `RoomLabel`; keeping it here avoids a runtime →
/// realtime dependency. A handler may return a bare string (the map name) or a
/// table `{ map, mode?, max_players?, open? }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomSpec {
    /// The map/level name clients in the room load.
    pub map: String,
    /// Free-form game mode tag (empty if unset).
    pub mode: String,
    /// Member cap (`0` = unlimited).
    pub max_players: u16,
    /// Whether new joins are accepted.
    pub open: bool,
}

impl Default for RoomSpec {
    fn default() -> Self {
        Self {
            map: String::new(),
            mode: String::new(),
            max_players: 0,
            open: true,
        }
    }
}

/// The result of a [`LuaRuntime::reload`] attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// A fresh VM was built from disk and swapped in; new handlers are live.
    Reloaded,
    /// The new script was rejected (read/parse/register error); the previously
    /// loaded script keeps serving and the error was logged.
    Rejected,
    /// Nothing to reload: this runtime was built from an in-memory source (no
    /// backing file), so there is no on-disk script to watch.
    NotReloadable,
}

/// What a loaded script registered, for operator introspection.
///
/// Produced by [`LuaRuntime::introspect`] and rendered by the console's API
/// Explorer section.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RuntimeIntrospection {
    /// Human-readable script source label (path or inline label).
    pub source: String,
    /// Whether the script is backed by an on-disk file (hot-reloadable).
    pub reloadable: bool,
    /// Per-invocation handler budget, in milliseconds.
    pub deadline_ms: u64,
    /// Registered RPC method names, sorted.
    pub rpcs: Vec<String>,
    /// Message kinds with a registered `on_message` handler, sorted.
    pub message_kinds: Vec<u32>,
    /// Registered singleton hooks, in declaration order.
    pub hooks: Vec<String>,
}

/// An embedded Lua runtime that dispatches inbound messages to script handlers.
pub struct LuaRuntime {
    vm: Mutex<LuaVm>,
    budget: Duration,
    /// Path to the backing `main.lua` for hot-reload, or `None` for a runtime
    /// built from an in-memory source (tests/embedders). Only present when
    /// created via [`LuaRuntime::load`].
    reload_path: Option<PathBuf>,
    /// Root directory that scoped `require` resolves modules within (the
    /// `scripts_dir`), or `None` for an in-memory runtime with no module root
    /// (its `require` errors). Threaded into every VM build so a hot-reload
    /// re-resolves the module graph from the same root.
    module_root: Option<PathBuf>,
    /// Optional operator-owned static-data directory, distinct from scripts.
    /// Retained so a hot reload builds a fresh catalog from the same root.
    static_data_dir: Option<PathBuf>,
    /// Per-file static-data read bound retained across reloads.
    static_data_max_file_bytes: usize,
    /// Persisted-domain-services seam exposed to `citadel.friends_*` host calls
    ///, or `None` when no services are attached. Retained so a
    /// hot-reload re-applies it to the fresh VM.
    domain: Option<Arc<dyn DomainHost>>,
    /// Read-only map catalog retained across hot-reload.
    maps: Option<Arc<MapCatalog>>,
    /// Authoritative transform hub retained for synchronous physics reads.
    transform_hub: Option<Arc<TransformHub>>,
    /// Private trusted host bridge for context-derived telemetry slices.
    telemetry_slices: Option<Arc<TelemetrySliceService>>,
    /// The capability mode used when constructing this VM. Retained so a reload
    /// cannot accidentally change the script's authority.
    execution_mode: LuaExecutionMode,
    outbound_http_policy: OutboundHttpPolicy,
    http_endpoint_policy: RuntimeHttpEndpointPolicy,
    event_bus_handle: RuntimeEventBusHandle,
    shared_cache_handle: RuntimeSharedCacheHandle,
    /// Where this runtime's authoritative-bridge answers land (the gateway),
    /// held weakly to avoid an `Arc` cycle. Lives on the runtime rather than in
    /// VM app data so it survives a hot-reload's whole-VM swap. `None` until the
    /// gateway attaches it; a runtime with no sink evaluates batches but its
    /// answers reach no one (a no-op, which is fail-closed).
    bridge_sink: Mutex<Option<Weak<dyn BridgeCommandSink>>>,
}

impl std::fmt::Debug for LuaRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LuaRuntime")
            .field("budget", &self.budget)
            .field("reload_path", &self.reload_path)
            .finish_non_exhaustive()
    }
}

impl LuaRuntime {
    /// Load `main.lua` from `scripts_dir`, or `Ok(None)` if it is absent.
    ///
    /// A missing scripts directory or missing `main.lua` is not an error: the
    /// caller falls back to the built-in relay. A present-but-broken script (I/O
    /// or syntax/runtime error at load) is a [`Runtime`](ErrorCategory::Runtime)
    /// error so operators notice a real misconfiguration.
    pub fn load(scripts_dir: &Path, deadline_ms: u64) -> AppResult<Option<Self>> {
        Self::load_with_static_data(
            scripts_dir,
            deadline_ms,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
        )
    }

    /// Load `main.lua` with an optional, separately configured static-data root.
    ///
    /// The root is never made visible to Lua. The script can only request
    /// validated relative JSON/CSV paths through `citadel.static_data` while its
    /// top-level initialization body runs.
    pub fn load_with_static_data(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
    ) -> AppResult<Option<Self>> {
        Self::load_with_static_data_and_mode(
            scripts_dir,
            deadline_ms,
            static_data_dir,
            static_data_max_file_bytes,
            LuaExecutionMode::Sandboxed,
        )
    }

    /// Load `main.lua` with static data and an explicit Lua capability mode.
    pub fn load_with_static_data_and_mode(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
        execution_mode: LuaExecutionMode,
    ) -> AppResult<Option<Self>> {
        Self::load_with_static_data_and_mode_and_http_policy(
            scripts_dir,
            deadline_ms,
            static_data_dir,
            static_data_max_file_bytes,
            execution_mode,
            OutboundHttpPolicy::default(),
        )
    }

    /// Load Lua with an explicit outbound HTTP policy retained across reloads.
    pub fn load_with_static_data_and_mode_and_http_policy(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
        execution_mode: LuaExecutionMode,
        outbound_http_policy: OutboundHttpPolicy,
    ) -> AppResult<Option<Self>> {
        Self::load_with_static_data_and_mode_and_capability_policies(
            scripts_dir,
            deadline_ms,
            static_data_dir,
            static_data_max_file_bytes,
            execution_mode,
            outbound_http_policy,
            RuntimeHttpEndpointPolicy::default(),
        )
    }

    /// Load Lua with the complete operator-owned runtime extension policy.
    pub fn load_with_static_data_and_mode_and_capability_policies(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
        execution_mode: LuaExecutionMode,
        outbound_http_policy: OutboundHttpPolicy,
        http_endpoint_policy: RuntimeHttpEndpointPolicy,
    ) -> AppResult<Option<Self>> {
        let main = scripts_dir.join("main.lua");
        if !main.is_file() {
            return Ok(None);
        }
        let source = read_script(&main)?;
        // Modules resolve within the scripts directory (dotted paths -> subdirs).
        let module_root = scripts_dir.to_path_buf();
        let source_label = main.display().to_string();
        let static_data = StaticDataCatalog::new(static_data_dir, static_data_max_file_bytes)?;
        let event_bus_handle = disabled_runtime_event_bus_handle();
        let shared_cache_handle = disabled_runtime_shared_cache_handle();
        let lua = build_lua(
            &source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            Some(&module_root),
            static_data.clone(),
            execution_mode,
            LuaCapabilityPolicies {
                outbound_http: outbound_http_policy.clone(),
                http_endpoints: http_endpoint_policy,
                event_bus_handle: Arc::clone(&event_bus_handle),
                shared_cache_handle: Arc::clone(&shared_cache_handle),
            },
        )?;
        let budget = Duration::from_millis(deadline_ms.max(1));
        Ok(Some(Self {
            vm: Mutex::new(LuaVm {
                lua,
                source_label,
                static_data,
            }),
            budget,
            // Remember the backing file so the watcher can hot-reload it in place.
            reload_path: Some(main),
            module_root: Some(module_root),
            static_data_dir: static_data_dir.map(Path::to_path_buf),
            static_data_max_file_bytes,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
            execution_mode,
            outbound_http_policy,
            http_endpoint_policy,
            event_bus_handle,
            shared_cache_handle,
            bridge_sink: Mutex::new(None),
        }))
    }

    /// Build a runtime from inline `source` (used by tests and [`load`]).
    ///
    /// The resulting runtime has no backing file, so [`reload`](LuaRuntime::reload)
    /// is a no-op ([`ReloadOutcome::NotReloadable`]); [`load`] sets the reload
    /// path.
    ///
    /// [`load`]: LuaRuntime::load
    pub fn from_source(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let capability_policies = LuaCapabilityPolicies::default();
        let event_bus_handle = Arc::clone(&capability_policies.event_bus_handle);
        let shared_cache_handle = Arc::clone(&capability_policies.shared_cache_handle);
        let lua = build_lua(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            None,
            static_data.clone(),
            LuaExecutionMode::Sandboxed,
            capability_policies,
        )?;
        let budget = Duration::from_millis(deadline_ms.max(1));
        Ok(Self {
            vm: Mutex::new(LuaVm {
                lua,
                source_label,
                static_data,
            }),
            budget,
            reload_path: None,
            module_root: None,
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
            execution_mode: LuaExecutionMode::Sandboxed,
            outbound_http_policy: OutboundHttpPolicy::default(),
            http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
            event_bus_handle,
            shared_cache_handle,
            bridge_sink: Mutex::new(None),
        })
    }

    /// Build a runtime from inline `source` with a scoped-`require` module root.
    ///
    /// Like [`from_source`](LuaRuntime::from_source) but resolves `require`d
    /// modules within `module_root` (dotted paths -> subdirectories). Used by
    /// tests that exercise multi-file scripts without a backing `main.lua`.
    pub fn from_source_with_root(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
        module_root: &Path,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let capability_policies = LuaCapabilityPolicies::default();
        let event_bus_handle = Arc::clone(&capability_policies.event_bus_handle);
        let shared_cache_handle = Arc::clone(&capability_policies.shared_cache_handle);
        let lua = build_lua(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            Some(module_root),
            static_data.clone(),
            LuaExecutionMode::Sandboxed,
            capability_policies,
        )?;
        let budget = Duration::from_millis(deadline_ms.max(1));
        Ok(Self {
            vm: Mutex::new(LuaVm {
                lua,
                source_label,
                static_data,
            }),
            budget,
            reload_path: None,
            module_root: Some(module_root.to_path_buf()),
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
            execution_mode: LuaExecutionMode::Sandboxed,
            outbound_http_policy: OutboundHttpPolicy::default(),
            http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
            event_bus_handle,
            shared_cache_handle,
            bridge_sink: Mutex::new(None),
        })
    }

    /// Attach the persisted-domain-services seam, consuming and returning `self`
    /// (builder style, before the runtime is shared as `Arc<dyn Runtime>`).
    ///
    /// Enables the `citadel.friends_*` host functions. The handle is
    /// applied to the current VM's app-data and retained so a hot-reload
    /// re-applies it to the rebuilt VM.
    #[must_use]
    pub fn with_domain_host(mut self, host: Arc<dyn DomainHost>) -> Self {
        self.domain = Some(host);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_domain_host(&guard.lua, &self.domain);
        }
        self
    }

    /// Attach private context-derived telemetry slices to trusted script calls.
    #[must_use]
    pub fn with_telemetry_slices(mut self, slices: Arc<TelemetrySliceService>) -> Self {
        self.telemetry_slices = Some(slices);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_telemetry_slices(&guard.lua, &self.telemetry_slices);
        }
        self
    }

    /// Attach the node-owned local event bus. The indirection is retained
    /// across hot reloads, while the bus itself remains process-local and
    /// best-effort.
    #[must_use]
    pub fn with_event_bus(self, bus: Arc<RuntimeEventBus>) -> Self {
        set_runtime_event_bus(&self.event_bus_handle, bus);
        self
    }

    #[must_use]
    pub fn with_shared_cache(self, cache: Arc<RuntimeSharedCache>) -> Self {
        set_runtime_shared_cache(&self.shared_cache_handle, cache);
        self
    }

    /// Attach the loaded-map catalog for read-only `citadel.map_info` queries.
    #[must_use]
    pub fn with_maps(mut self, maps: Arc<MapCatalog>) -> Self {
        self.maps = Some(maps);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_map_catalog(&guard.lua, &self.maps);
        }
        self
    }

    /// Attach the transform hub for synchronous `citadel.physics_state` reads.
    #[must_use]
    pub fn with_transform_hub(mut self, hub: Arc<TransformHub>) -> Self {
        self.transform_hub = Some(hub);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_transform_hub(&guard.lua, &self.transform_hub);
        }
        self
    }

    /// Whether this runtime is backed by an on-disk script that can be reloaded.
    #[must_use]
    pub fn is_reloadable(&self) -> bool {
        self.reload_path.is_some()
    }

    /// A point-in-time description of what the loaded script registered
    ///: RPC method names, handled message kinds, and lifecycle
    /// hooks. Read under the same VM lock dispatch uses, so it reflects the
    /// live VM (including a just-hot-reloaded one); a registry probe failure
    /// simply yields an empty list rather than an error.
    #[must_use]
    pub fn introspect(&self) -> RuntimeIntrospection {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let mut rpcs: Vec<String> = lua
            .named_registry_value::<Table>(RPC_HANDLERS_KEY)
            .map(|table| {
                table
                    .pairs::<String, mlua::Value>()
                    .flatten()
                    .map(|(method, _)| method)
                    .collect()
            })
            .unwrap_or_default();
        rpcs.sort_unstable();
        let mut message_kinds: Vec<u32> = lua
            .named_registry_value::<Table>(HANDLERS_KEY)
            .map(|table| {
                table
                    .pairs::<u32, mlua::Value>()
                    .flatten()
                    .map(|(kind, _)| kind)
                    .collect()
            })
            .unwrap_or_default();
        message_kinds.sort_unstable();
        let hooks = [
            ("on_join", ON_JOIN_KEY),
            ("on_leave", ON_LEAVE_KEY),
            ("on_tick", ON_TICK_KEY),
            ("on_leaderboard_reset", ON_LEADERBOARD_RESET_KEY),
            ("on_room_create", ON_ROOM_CREATE_KEY),
            ("on_room_join", ON_ROOM_JOIN_KEY),
            ("before_realtime", BEFORE_REALTIME_KEY),
            ("after_realtime", AFTER_REALTIME_KEY),
            ("on_input", ON_INPUT_KEY),
        ]
        .iter()
        .filter(|(_, key)| {
            lua.named_registry_value::<Option<Function>>(key)
                .ok()
                .flatten()
                .is_some()
        })
        .map(|(name, _)| (*name).to_string())
        .collect();
        RuntimeIntrospection {
            source: guard.source_label.clone(),
            reloadable: self.reload_path.is_some(),
            deadline_ms: u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
            rpcs,
            message_kinds,
            hooks,
        }
    }

    /// Snapshot the atomically live set of Lua endpoint declarations.
    #[must_use]
    pub fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        let Ok(handlers) = guard
            .lua
            .named_registry_value::<Table>(HTTP_ENDPOINT_HANDLERS_KEY)
        else {
            return Vec::new();
        };
        let mut endpoints: Vec<_> = handlers
            .pairs::<String, Table>()
            .flatten()
            .filter_map(|(_, entry)| {
                let method = entry
                    .get::<String>("method")
                    .ok()
                    .and_then(|value| RuntimeHttpMethod::parse(&value))?;
                let path = entry.get::<String>("path").ok()?;
                let auth = entry
                    .get::<String>("auth")
                    .ok()
                    .and_then(|value| RuntimeHttpAuth::parse(&value))?;
                RuntimeHttpEndpoint::new(method, path, auth).ok()
            })
            .collect();
        endpoints.sort_unstable();
        endpoints
    }

    /// Invoke an endpoint handler under the same lock, deadline, panic
    /// isolation, and command-sink discard policy as RPC calls.
    pub fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        let lua = &guard.lua;
        let budget = self.budget;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + budget));
            let result = call_http_endpoint_handler(lua, request);
            set_deadline(lua, None);
            result
        }));
        clear_sink(lua);
        match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %error,
                    "lua runtime HTTP endpoint handler failed; isolated"
                );
                RuntimeHttpOutcome::Failed
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "lua runtime HTTP endpoint handler panicked; isolated"
                );
                set_deadline(lua, None);
                RuntimeHttpOutcome::Failed
            }
        }
    }

    /// Rebuild the VM from the backing script on disk and swap it in, failure-safe.
    ///
    /// The reload is deliberately two-phase so a broken edit can never take the
    /// node down:
    ///
    /// 1. Read the file and build a **brand-new** `Lua` VM (re-running the
    ///    script's registrations) *without* holding the runtime lock. A missing
    ///    file, syntax error, or registration error fails here — before the live
    ///    VM is touched — so the currently-loaded script keeps serving. The error
    ///    is logged and [`ReloadOutcome::Rejected`] is returned.
    /// 2. On success, acquire the runtime lock and swap the fresh VM (and its
    ///    label) in atomically. Because this is the same lock that serializes
    ///    [`dispatch`](LuaRuntime::dispatch), the lifecycle hooks, and
    ///    [`tick`](LuaRuntime::tick), a reload can never interleave with an
    ///    in-flight handler: the swap waits for any running handler to finish and
    ///    the next handler runs on the new VM. Building off-lock also keeps the
    ///    critical section to a single move.
    ///
    /// In-VM Lua globals (per-game state) are reset on reload — the fresh VM
    /// starts clean. This is expected for a dev hot-reload; cross-reload state
    /// preservation is out of scope.
    ///
    /// Never panics and never propagates: a runtime with no backing file returns
    /// [`ReloadOutcome::NotReloadable`].
    pub fn reload(&self) -> ReloadOutcome {
        let Some(path) = self.reload_path.as_deref() else {
            return ReloadOutcome::NotReloadable;
        };
        let label = path.display().to_string();
        // Phase 1: build the replacement VM off-lock. Any failure here leaves the
        // live VM untouched.
        let source = match read_script(path) {
            Ok(source) => source,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "hot-reload: cannot read script; keeping the current script"
                );
                return ReloadOutcome::Rejected;
            }
        };
        let fresh_static_data = match StaticDataCatalog::new(
            self.static_data_dir.as_deref(),
            self.static_data_max_file_bytes,
        ) {
            Ok(catalog) => catalog,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "hot-reload: cannot initialize static-data catalog; keeping the current script and data"
                );
                return ReloadOutcome::Rejected;
            }
        };
        let fresh = match build_lua(
            &source,
            &label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            self.module_root.as_deref(),
            fresh_static_data.clone(),
            self.execution_mode,
            LuaCapabilityPolicies {
                outbound_http: self.outbound_http_policy.clone(),
                http_endpoints: self.http_endpoint_policy,
                event_bus_handle: Arc::clone(&self.event_bus_handle),
                shared_cache_handle: Arc::clone(&self.shared_cache_handle),
            },
        ) {
            Ok(lua) => lua,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "hot-reload: new script rejected (parse/registration error); keeping the current script"
                );
                return ReloadOutcome::Rejected;
            }
        };
        // Re-apply the domain-services seam so `citadel.friends_*` keeps working
        // after the swap (the rebuilt VM starts with fresh app-data).
        apply_domain_host(&fresh, &self.domain);
        apply_map_catalog(&fresh, &self.maps);
        apply_transform_hub(&fresh, &self.transform_hub);
        apply_telemetry_slices(&fresh, &self.telemetry_slices);
        // Guard against an accidental empty/handlerless save (e.g. an editor's
        // transient zero-byte write caught mid-save): swapping it in would leave
        // the node with no handlers. Reject and keep the working script.
        if !has_any_handler(&fresh) {
            tracing::warn!(
                script = %label,
                "hot-reload: new script registered no handlers; keeping the current script"
            );
            return ReloadOutcome::Rejected;
        }
        // Phase 2: swap under the runtime lock (serialized with dispatch/tick).
        {
            let mut guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            guard.lua = fresh;
            guard.source_label = label;
            guard.static_data = fresh_static_data;
        }
        tracing::info!(
            script = %path.display(),
            "hot-reload: swapped in the updated script and static data (in-VM Lua state reset)"
        );
        ReloadOutcome::Reloaded
    }

    /// Entry script plus the static-data files initialized by the live VM.
    ///
    /// The returned list is consumed by the development hot-reload watcher only;
    /// it never participates in runtime dispatch or tick execution.
    #[must_use]
    pub fn reload_watch_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.reload_path.iter().cloned().collect::<Vec<_>>();
        let guard = self
            .vm
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        paths.extend(guard.static_data.loaded_paths());
        paths.sort();
        paths.dedup();
        paths
    }

    /// The per-invocation time budget enforced on handlers.
    #[must_use]
    pub fn budget(&self) -> Duration {
        self.budget
    }

    /// Run the registered message handler for `kind` and return its commands.
    ///
    /// Total isolation: a missing handler, a Lua error, a blown deadline, or a
    /// Rust panic inside a callback all yield an empty command list and are
    /// logged. This function never panics and never propagates an error.
    pub fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        self.run_locked("message", self.budget, |lua| {
            let handlers: Table = lua.named_registry_value(HANDLERS_KEY)?;
            let Some(handler) = handlers.get::<Option<Function>>(kind)? else {
                // No handler registered for this kind: not an error.
                tracing::trace!(kind, "no lua handler for kind");
                return Ok(false);
            };
            let ctx = build_ctx(lua, sender, user_id, kind, None)?;
            let body_value = lua.create_string(body)?;
            handler.call::<()>((ctx, body_value))?;
            Ok(true)
        })
    }

    /// Dispatch a message with its authoritative room id in `ctx.room_id`.
    pub fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        self.run_locked("match_message", self.budget, |lua| {
            let handlers: Table = lua.named_registry_value(HANDLERS_KEY)?;
            let Some(handler) = handlers.get::<Option<Function>>(kind)? else {
                return Ok(false);
            };
            let ctx = build_ctx(lua, sender, user_id, kind, Some(room_id))?;
            let body_value = lua.create_string(body)?;
            handler.call::<()>((ctx, body_value))?;
            Ok(true)
        })
    }

    /// Attach the gateway's authoritative-bridge sink (weakly).
    pub fn attach_bridge_sink(&self, sink: Weak<dyn BridgeCommandSink>) {
        *self.bridge_sink.lock().unwrap_or_else(|e| e.into_inner()) = Some(sink);
    }

    fn bridge_sink(&self) -> Option<Arc<dyn BridgeCommandSink>> {
        self.bridge_sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Evaluate one delivered batch inline and deliver the fenced answer to the
    /// attached sink. A batch that produces no answer (no `on_input` handler, or
    /// a script fault) delivers nothing — the fail-closed failure policy.
    pub fn deliver_event_batch(&self, batch: NormalizedEventBatch) {
        let Some(answer) = self.evaluate_event_batch(&batch) else {
            return;
        };
        if let Some(sink) = self.bridge_sink() {
            sink.deliver_command_batch(answer);
        }
    }

    /// Evaluate a normalized-event batch and build the script's fenced answer.
    ///
    /// Runs `citadel.on_input` once per event under the same serialized lock,
    /// per-invocation deadline, and error/panic isolation as every other
    /// handler, then maps the drained command sink to the batch-level
    /// [`ScriptCommand`]s. Returns `None` — no answer, fail-closed — when no
    /// `on_input` handler is registered or the invocation errors, deadlines, or
    /// panics. The validator is the sole authority on whether the returned
    /// answer materializes; this method only builds it.
    pub fn evaluate_event_batch(&self, batch: &NormalizedEventBatch) -> Option<ScriptCommandBatch> {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let budget = self.budget;
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_invocation_mode(lua, InvocationMode::Normal);
            set_deadline(lua, Some(Instant::now() + budget));
            let outcomes = (|| -> mlua::Result<Option<Vec<InputOutcome>>> {
                let Some(handler) = lua.named_registry_value::<Option<Function>>(ON_INPUT_KEY)?
                else {
                    return Ok(None);
                };
                let mut outcomes = Vec::with_capacity(batch.events.len());
                for event in &batch.events {
                    let ev = build_event_table(lua, batch, event)?;
                    let ret: Value = handler.call(ev)?;
                    let (decision, reply) = parse_input_decision(ret)?;
                    outcomes.push(InputOutcome {
                        event_id: event.event_id,
                        decision,
                        reply,
                    });
                }
                Ok(Some(outcomes))
            })();
            set_deadline(lua, None);
            set_invocation_mode(lua, InvocationMode::Normal);
            outcomes
        }));
        match result {
            Ok(Ok(Some(outcomes))) => {
                let commands = take_commands(lua, &guard.source_label, "on_input")
                    .into_iter()
                    .map(script_command_from_outbound)
                    .collect();
                let mut answer = ScriptCommandBatch::answering(batch);
                answer.input_outcomes = outcomes;
                answer.commands = commands;
                Some(answer)
            }
            Ok(Ok(None)) => {
                clear_sink(lua);
                None
            }
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "on_input",
                    error = %error,
                    "lua on_input failed; batch fails closed (no answer)"
                );
                clear_sink(lua);
                None
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "on_input",
                    "lua on_input panicked; batch fails closed (no answer)"
                );
                set_deadline(lua, None);
                set_invocation_mode(lua, InvocationMode::Normal);
                clear_sink(lua);
                None
            }
        }
    }

    /// Run the optional before-realtime interceptor.
    ///
    /// Returning `false` vetoes the envelope. A missing hook continues, while an
    /// invalid result, runtime error, deadline, or panic fails closed. Commands
    /// emitted by the interceptor are discarded so interception cannot create
    /// recursive outbound side effects.
    pub fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        self.run_before_realtime(|lua| {
            let Some(handler) =
                lua.named_registry_value::<Option<Function>>(BEFORE_REALTIME_KEY)?
            else {
                return Ok(None);
            };
            let ctx = build_ctx(lua, sender, user_id, kind, room_id)?;
            let body = lua.create_string(body)?;
            ctx.set("body", body.clone())?;
            let decision: Value = handler.call((ctx, body))?;
            match decision {
                Value::Nil | Value::Boolean(true) => Ok(Some(RealtimeInterception::Continue)),
                Value::Boolean(false) => Ok(Some(RealtimeInterception::Drop)),
                _ => Err(mlua::Error::RuntimeError(
                    "before_realtime must return false, true, or nil".to_string(),
                )),
            }
        })
    }

    /// Run the optional after-realtime observer after gateway routing.
    ///
    /// The same deadline and error isolation as a message handler apply, but any
    /// commands it enqueues are intentionally discarded because the gateway has
    /// already committed the envelope's routing result.
    pub fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        let _ = self.run_locked_with_event_drain("after_realtime", self.budget, false, |lua| {
            let Some(handler) = lua.named_registry_value::<Option<Function>>(AFTER_REALTIME_KEY)?
            else {
                return Ok(false);
            };
            let ctx = build_ctx(lua, sender, user_id, kind, room_id)?;
            let body = lua.create_string(body)?;
            ctx.set("body", body.clone())?;
            ctx.set("dropped", outcome.dropped)?;
            ctx.set("delivered", outcome.delivered)?;
            set_invocation_mode(lua, InvocationMode::RealtimeInterceptor);
            let result = handler.call::<()>((ctx, body));
            set_invocation_mode(lua, InvocationMode::Normal);
            result?;
            Ok(true)
        });
    }

    /// Run the `on_join`/`on_leave` handler for `sender` and return its commands.
    ///
    /// Shares the exact isolation, per-invocation deadline, and command-sink
    /// machinery as [`dispatch`](LuaRuntime::dispatch): a slow or erroring
    /// lifecycle handler cannot wedge the node. When no handler is registered
    /// this is a no-op returning no commands.
    pub fn on_leaderboard_reset(
        &self,
        epoch: &crate::leaderboard_scheduler::ResetEpoch,
        fencing_token: crate::leaderboard_scheduler::SchedulerFencingToken,
    ) -> AppResult<()> {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        let lua = &guard.lua;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + self.budget));
            let result = (|| -> mlua::Result<()> {
                let Some(handler) =
                    lua.named_registry_value::<Option<Function>>(ON_LEADERBOARD_RESET_KEY)?
                else {
                    return Ok(());
                };
                let ctx = lua.create_table()?;
                ctx.set("leaderboard_id", epoch.leaderboard_id.clone())?;
                ctx.set("due_at_unix_ms", epoch.due_at.unix_millis())?;
                ctx.set("fencing_token", fencing_token.get())?;
                handler.call::<()>(ctx)
            })();
            set_deadline(lua, None);
            result
        }));
        clear_sink(lua);
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(AppError::internal("leaderboard reset callback failed")
                .with_detail(error.to_string())),
            Err(_) => {
                set_deadline(lua, None);
                Err(AppError::internal("leaderboard reset callback panicked"))
            }
        }
    }

    /// Run the `on_join`/`on_leave` handler for `sender` and return its commands.
    ///
    /// Shares the exact isolation, per-invocation deadline, and command-sink
    /// machinery as [`dispatch`](LuaRuntime::dispatch): a slow or erroring
    /// lifecycle handler cannot wedge the node. When no handler is registered
    /// this is a no-op returning no commands.
    pub fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        self.run_locked(hook.label(), self.budget, |lua| {
            let Some(handler) =
                lua.named_registry_value::<Option<Function>>(hook.registry_key())?
            else {
                return Ok(false);
            };
            let ctx = build_lifecycle_ctx(lua, sender, user_id)?;
            handler.call::<()>(ctx)?;
            Ok(true)
        })
    }

    /// Run the `on_tick` game-loop handler with elapsed `dt` and return commands.
    ///
    /// `dt` is passed to the script in seconds. Runs under the same serialized
    /// Lua lock as message dispatch, bounded by its own `budget`, with the same
    /// error isolation: a hung or erroring tick yields no commands and never
    /// wedges inbound dispatch. A no-op when no `on_tick` handler is registered.
    pub fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        set_active_runtime_scope(None);
        let dt_secs = dt.as_secs_f64();
        self.run_locked("on_tick", budget, |lua| {
            if let Some(handler) = lua.named_registry_value::<Option<Function>>(ON_TICK_KEY)? {
                handler.call::<()>(dt_secs)?;
            }
            advance_patrols(lua, dt.as_secs_f32())?;
            Ok(true)
        })
    }

    /// Run `on_tick(dt, room_id)` for one authoritative match. Lua accepts the
    /// optional second argument without breaking existing one-argument handlers;
    /// scripts can keep mutable state keyed by that stable room id.
    pub fn tick_in_room(
        &self,
        room_id: u64,
        dt: Duration,
        budget: Duration,
    ) -> Vec<OutboundCommand> {
        set_active_runtime_scope(Some(room_id));
        let dt_secs = dt.as_secs_f64();
        self.run_locked("match_tick", budget, |lua| {
            if let Some(handler) = lua.named_registry_value::<Option<Function>>(ON_TICK_KEY)? {
                handler.call::<()>((dt_secs, room_id))?;
            }
            Ok(true)
        })
    }

    /// Run the registered `citadel.on_rpc` handler for `method` and return its
    /// reply, correlated by the caller upstream.
    ///
    /// This is the request/response sibling of [`dispatch`](LuaRuntime::dispatch):
    /// it runs under the **same** runtime lock, the same per-invocation deadline,
    /// and the same panic/error isolation, but instead of emitting broadcast
    /// commands it threads the handler's return value back out as an
    /// [`RpcOutcome`]. The Lua handler receives `(ctx, body)` where `ctx.sender`
    /// is the caller and `ctx.method` is the method name, and must `return` a
    /// string reply (binary-safe) or raise an error.
    ///
    /// Failure modes never crash the node and never leak internals: an unknown
    /// method, a Lua error, a non-string return, a blown deadline, or an isolated
    /// Rust panic all yield [`RpcOutcome::Err`] with a short, generic message; the
    /// underlying error is logged server-side. Any `broadcast`/`send` a handler
    /// attempts is discarded — an RPC handler communicates only through its
    /// return value.
    pub fn call_rpc(
        &self,
        sender: u64,
        user_id: Option<&str>,
        method: &str,
        body: &[u8],
    ) -> RpcOutcome {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let budget = self.budget;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            // Clear the sink so a stray broadcast/send inside an RPC handler is
            // dropped rather than leaking into the fan-out path.
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + budget));
            let result = call_rpc_handler(lua, sender, user_id, method, body);
            set_deadline(lua, None);
            result
        }));
        // Always leave the sink clean for the next invocation regardless of path.
        let result = match outcome {
            Ok(Ok(RpcInner::Reply(bytes))) => RpcOutcome::Ok(bytes),
            Ok(Ok(RpcInner::NoHandler)) => {
                tracing::debug!(
                    script = %guard.source_label,
                    method,
                    "no lua rpc handler for method"
                );
                RpcOutcome::Err(format!("{RPC_ERR_UNKNOWN_METHOD}: {method}"))
            }
            Ok(Err(e)) => {
                let timed_out = is_deadline_error(&e);
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    error = %e,
                    "lua rpc handler error; isolated, generic error returned to caller"
                );
                if timed_out {
                    RpcOutcome::Err(RPC_ERR_TIMEOUT.to_string())
                } else {
                    RpcOutcome::Err(RPC_ERR_HANDLER.to_string())
                }
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    "lua rpc handler panicked; isolated and dropped"
                );
                set_deadline(lua, None);
                RpcOutcome::Err(RPC_ERR_HANDLER.to_string())
            }
        };
        clear_sink(lua);
        result
    }

    /// Run the `citadel.on_room_create` handler for a client room-create request,
    /// returning the room label spec it produced (map/mode/caps), or `None` when no
    /// handler is registered or it fails/returns an invalid value (the gateway then
    /// uses a default label). Same lock/deadline/panic isolation as
    /// [`call_rpc`](Self::call_rpc); any broadcast/send the handler attempts is
    /// discarded.
    pub fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec> {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let budget = self.budget;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + budget));
            let r = call_room_create_handler(lua, sender, user_id, params);
            set_deadline(lua, None);
            r
        }));
        clear_sink(lua);
        match outcome {
            Ok(Ok(spec)) => spec,
            Ok(Err(e)) => {
                tracing::error!(script = %guard.source_label, error = %e, "lua on_room_create error; isolated, using default label");
                None
            }
            Err(_) => {
                tracing::error!(script = %guard.source_label, "lua on_room_create panicked; isolated");
                set_deadline(lua, None);
                None
            }
        }
    }

    /// Run the `citadel.on_room_join` admission gate for a client join request.
    /// Returns `true` to admit (the default when no handler is registered) or
    /// `false` to reject. A handler error/panic rejects (fail-closed) and is logged.
    pub fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let budget = self.budget;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + budget));
            let r = call_room_join_handler(lua, sender, user_id, room_id);
            set_deadline(lua, None);
            r
        }));
        clear_sink(lua);
        match outcome {
            Ok(Ok(decision)) => decision.unwrap_or(true),
            Ok(Err(e)) => {
                tracing::error!(script = %guard.source_label, error = %e, "lua on_room_join error; isolated, rejecting");
                false
            }
            Err(_) => {
                tracing::error!(script = %guard.source_label, "lua on_room_join panicked; isolated, rejecting");
                set_deadline(lua, None);
                false
            }
        }
    }

    /// Whether the script registered an `on_tick` handler.
    ///
    /// The bootstrap layer uses this to avoid spawning a periodic tick task for a
    /// script that has no game loop.
    #[must_use]
    pub fn has_tick_handler(&self) -> bool {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let has_handler = guard
            .lua
            .named_registry_value::<Option<Function>>(ON_TICK_KEY)
            .map(|h| h.is_some())
            .unwrap_or(false);
        has_handler
            || guard
                .lua
                .app_data_ref::<NpcPatrols>()
                .map(|patrols| !patrols.0.is_empty())
                .unwrap_or(false)
    }

    /// Acquire the Lua lock, run one bounded, isolated handler invocation, and
    /// return the commands it enqueued.
    ///
    /// The single choke point shared by message dispatch, lifecycle hooks, and
    /// the tick. `call` looks up and invokes the handler, returning `Ok(true)`
    /// when a handler ran and `Ok(false)` when none was registered (a no-op). Any
    /// Lua error, blown deadline, or Rust panic inside `call` is caught, logged,
    /// and turned into an empty command list; the runtime is always left clean
    /// for the next invocation. Never panics, never propagates.
    fn run_locked<F>(&self, what: &str, budget: Duration, call: F) -> Vec<OutboundCommand>
    where
        F: FnOnce(&Lua) -> mlua::Result<bool>,
    {
        self.run_locked_with_event_drain(what, budget, true, call)
    }

    fn run_locked_with_event_drain<F>(
        &self,
        what: &str,
        budget: Duration,
        drain_events: bool,
        call: F,
    ) -> Vec<OutboundCommand>
    where
        F: FnOnce(&Lua) -> mlua::Result<bool>,
    {
        // Recover a poisoned lock rather than propagate: a prior panic must not
        // wedge the whole runtime (state is only ever mutated under this lock and
        // is left consistent below). Holding the guard for the whole invocation
        // is exactly what serializes a concurrent hot-reload swap behind it.
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_invocation_mode(lua, InvocationMode::Normal);
            set_deadline(lua, Some(Instant::now() + budget));
            let ran = call(lua);
            set_deadline(lua, None);
            set_invocation_mode(lua, InvocationMode::Normal);
            ran
        }));
        match outcome {
            Ok(Ok(true)) => {
                let mut commands = take_commands(lua, &guard.source_label, what);
                if drain_events {
                    set_deadline(lua, Some(Instant::now() + budget));
                    append_runtime_event_commands(
                        &mut commands,
                        dispatch_pending_runtime_events(lua, &guard.source_label, budget),
                        &guard.source_label,
                    );
                    set_deadline(lua, None);
                }
                commands
            }
            Ok(Ok(false)) => {
                clear_sink(lua);
                let mut commands = Vec::new();
                if drain_events {
                    set_deadline(lua, Some(Instant::now() + budget));
                    append_runtime_event_commands(
                        &mut commands,
                        dispatch_pending_runtime_events(lua, &guard.source_label, budget),
                        &guard.source_label,
                    );
                    set_deadline(lua, None);
                }
                commands
            }
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    error = %e,
                    "lua handler error; isolated, side effects discarded"
                );
                clear_sink(lua);
                Vec::new()
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    "lua handler panicked; isolated and dropped"
                );
                // A panic inside `call` skips the in-closure `set_deadline(None)`;
                // clear it and the sink so the next invocation starts clean.
                set_deadline(lua, None);
                set_invocation_mode(lua, InvocationMode::Normal);
                clear_sink(lua);
                Vec::new()
            }
        }
    }

    /// Run a before-realtime decision under the usual lock, deadline, and panic
    /// guard. Unlike ordinary handlers, all side effects are discarded and a
    /// failure deliberately becomes a veto.
    fn run_before_realtime<F>(&self, call: F) -> RealtimeInterception
    where
        F: FnOnce(&Lua) -> mlua::Result<Option<RealtimeInterception>>,
    {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let lua = &guard.lua;
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            clear_sink(lua);
            set_invocation_mode(lua, InvocationMode::RealtimeInterceptor);
            set_deadline(lua, Some(Instant::now() + self.budget));
            let decision = call(lua);
            set_deadline(lua, None);
            set_invocation_mode(lua, InvocationMode::Normal);
            decision
        }));
        match outcome {
            Ok(Ok(Some(decision))) => {
                clear_sink(lua);
                decision
            }
            Ok(Ok(None)) => {
                clear_sink(lua);
                RealtimeInterception::Continue
            }
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    error = %error,
                    "lua realtime interceptor failed; vetoing envelope"
                );
                clear_sink(lua);
                RealtimeInterception::Drop
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    "lua realtime interceptor panicked; vetoing envelope"
                );
                set_deadline(lua, None);
                set_invocation_mode(lua, InvocationMode::Normal);
                clear_sink(lua);
                RealtimeInterception::Drop
            }
        }
    }
}

impl Runtime for LuaRuntime {
    fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        LuaRuntime::before_realtime(self, sender, user_id, room_id, kind, body)
    }

    fn attach_bridge_sink(&self, sink: Weak<dyn BridgeCommandSink>) {
        LuaRuntime::attach_bridge_sink(self, sink);
    }

    fn deliver_event_batch(&self, batch: NormalizedEventBatch) {
        LuaRuntime::deliver_event_batch(self, batch);
    }

    fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        LuaRuntime::after_realtime(self, sender, user_id, room_id, kind, body, outcome);
    }

    fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        LuaRuntime::dispatch(self, sender, user_id, kind, body)
    }

    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        LuaRuntime::dispatch_in_room(self, sender, user_id, room_id, kind, body)
    }

    fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        LuaRuntime::dispatch_lifecycle(self, hook, sender, user_id)
    }

    fn on_leaderboard_reset(
        &self,
        epoch: &crate::leaderboard_scheduler::ResetEpoch,
        fencing_token: crate::leaderboard_scheduler::SchedulerFencingToken,
    ) -> AppResult<()> {
        LuaRuntime::on_leaderboard_reset(self, epoch, fencing_token)
    }

    fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        LuaRuntime::tick(self, dt, budget)
    }

    fn tick_in_room(&self, room_id: u64, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        LuaRuntime::tick_in_room(self, room_id, dt, budget)
    }

    fn call_rpc(
        &self,
        sender: u64,
        user_id: Option<&str>,
        method: &str,
        body: &[u8],
    ) -> RpcOutcome {
        LuaRuntime::call_rpc(self, sender, user_id, method, body)
    }

    fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec> {
        LuaRuntime::call_room_create(self, sender, user_id, params)
    }

    fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        LuaRuntime::call_room_join(self, sender, user_id, room_id)
    }

    fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        LuaRuntime::http_endpoints(self)
    }

    fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        LuaRuntime::call_http_endpoint(self, request)
    }

    fn has_tick_handler(&self) -> bool {
        LuaRuntime::has_tick_handler(self)
    }

    fn budget(&self) -> Duration {
        LuaRuntime::budget(self)
    }

    fn introspect(&self) -> RuntimeIntrospection {
        LuaRuntime::introspect(self)
    }

    fn is_reloadable(&self) -> bool {
        LuaRuntime::is_reloadable(self)
    }

    fn reload(&self) -> ReloadOutcome {
        LuaRuntime::reload(self)
    }

    fn reload_watch_paths(&self) -> Vec<PathBuf> {
        LuaRuntime::reload_watch_paths(self)
    }
}

/// Reset the command sink to empty (short borrow).
fn clear_sink(lua: &Lua) {
    if let Some(mut sink) = lua.app_data_mut::<CommandSink>() {
        sink.reset();
    }
}

/// Set (or clear) the current invocation deadline (short borrow).
fn set_deadline(lua: &Lua, deadline: Option<Instant>) {
    if let Some(mut d) = lua.app_data_mut::<Deadline>() {
        *d = Deadline(deadline);
    }
}

fn set_invocation_mode(lua: &Lua, mode: InvocationMode) {
    if let Some(mut current) = lua.app_data_mut::<InvocationMode>() {
        *current = mode;
    }
}

/// Take the accumulated commands after a successful handler run (short borrow).
fn take_commands(lua: &Lua, label: &str, handler: &str) -> Vec<OutboundCommand> {
    if let Some(mut sink) = lua.app_data_mut::<CommandSink>() {
        if sink.overflowed {
            tracing::warn!(
                script = %label,
                handler,
                cap = MAX_OUTBOUND_COMMANDS,
                "lua handler exceeded outbound command cap; extra commands dropped"
            );
        }
        std::mem::take(&mut sink.commands)
    } else {
        Vec::new()
    }
}

/// Set `ctx.user_id` to the authenticated account id, or leave it `nil` for a
/// guest participant.
///
/// `ctx.sender` remains the transport-level participant id; `ctx.user_id` is the
/// domain account resolved by the session service at connect, present only for
/// authenticated participants. Game logic distinguishes an account-bound player
/// from an anonymous one by testing `ctx.user_id`.
fn set_user_id(ctx: &Table, user_id: Option<&str>) -> mlua::Result<()> {
    // Leave the field absent (nil) for guests rather than setting an empty
    // string, so `ctx.user_id or ...` idioms work naturally.
    if let Some(id) = user_id {
        ctx.set("user_id", id)?;
    }
    Ok(())
}

/// Build the `ctx` table handed to a message handler: `sender`/`kind`, plus
/// `user_id` for an authenticated participant.
fn build_ctx(
    lua: &Lua,
    sender: u64,
    user_id: Option<&str>,
    kind: u16,
    room_id: Option<u64>,
) -> mlua::Result<Table> {
    set_active_runtime_scope(room_id);
    let ctx = lua.create_table()?;
    ctx.set("sender", sender)?;
    ctx.set("kind", kind)?;
    if let Some(room_id) = room_id {
        ctx.set("room_id", room_id)?;
    }
    set_user_id(&ctx, user_id)?;
    Ok(ctx)
}

/// A `{x, y, z}` table for a 3-vector.
fn vec3_table(lua: &Lua, v: [f32; 3]) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set("x", v[0])?;
    t.set("y", v[1])?;
    t.set("z", v[2])?;
    Ok(t)
}

/// A `{position, rotation = {x,y,z,w}, velocity}` table for a transform.
fn transform_table(lua: &Lua, tr: BridgeTransform) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set("position", vec3_table(lua, tr.position)?)?;
    let rot = lua.create_table()?;
    rot.set("x", tr.rotation[0])?;
    rot.set("y", tr.rotation[1])?;
    rot.set("z", tr.rotation[2])?;
    rot.set("w", tr.rotation[3])?;
    t.set("rotation", rot)?;
    t.set("velocity", vec3_table(lua, tr.velocity)?)?;
    Ok(t)
}

/// Marshal one normalized event into the table handed to `citadel.on_input`.
///
/// `kind` is a stable string tag the handler switches on; the remaining fields
/// are the decoded, ownership-verified intent. Replicated-field marshaling is
/// intentionally shallow in v1 (count only): rich per-field access is a
/// follow-up once script-owned rep values land.
fn build_event_table(
    lua: &Lua,
    batch: &NormalizedEventBatch,
    event: &NormalizedEvent,
) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set("event_id", event.event_id)?;
    t.set("participant", event.participant)?;
    if let Some(user_id) = &event.user_id {
        t.set("user_id", user_id.clone())?;
    }
    t.set("match_id", batch.match_id)?;
    t.set("tick", batch.tick)?;
    match &event.payload {
        NormalizedPayload::TransformInput {
            object_id,
            ownership_epoch,
            input_seq,
            sim_tick,
            dt,
            move_velocity,
            payload,
            fire,
        } => {
            t.set("kind", "transform_input")?;
            t.set("object_id", *object_id)?;
            t.set("ownership_epoch", *ownership_epoch)?;
            t.set("input_seq", *input_seq)?;
            t.set("sim_tick", *sim_tick)?;
            t.set("dt", *dt)?;
            t.set("move_velocity", vec3_table(lua, *move_velocity)?)?;
            t.set("payload", lua.create_string(payload)?)?;
            t.set("has_fire", fire.is_some())?;
            if let Some(fire) = fire {
                let ft = lua.create_table()?;
                ft.set("origin", vec3_table(lua, fire.origin)?)?;
                ft.set("direction", vec3_table(lua, fire.direction)?)?;
                ft.set("weapon", fire.weapon)?;
                t.set("fire", ft)?;
            }
        }
        NormalizedPayload::ActorStateReport {
            object_id,
            transform,
        } => {
            t.set("kind", "actor_state")?;
            t.set("object_id", *object_id)?;
            t.set("transform", transform_table(lua, *transform)?)?;
        }
        NormalizedPayload::ReplicatedVarWrite {
            object_id,
            class_id,
            result_id,
            fields,
            ..
        } => {
            t.set("kind", "replicated_var")?;
            t.set("object_id", *object_id)?;
            t.set("class_id", *class_id)?;
            t.set("result_id", *result_id)?;
            t.set("field_count", fields.len() as u64)?;
        }
        NormalizedPayload::SpawnRequest {
            archetype_id,
            transform,
        } => {
            t.set("kind", "spawn_request")?;
            t.set("archetype_id", *archetype_id)?;
            t.set("transform", transform_table(lua, *transform)?)?;
        }
        NormalizedPayload::MatchMessage { kind, body } => {
            t.set("kind", "message")?;
            t.set("message_kind", *kind)?;
            t.set("body", lua.create_string(body)?)?;
        }
        NormalizedPayload::ParticipantJoined => {
            t.set("kind", "join")?;
        }
        NormalizedPayload::ParticipantLeft => {
            t.set("kind", "leave")?;
        }
    }
    Ok(t)
}

/// Parse the value a `citadel.on_input` handler returned into a decision plus an
/// optional bounded reply.
///
/// Accepted shapes: `nil`/`true`/`"accept"` (Accept), `false`/`"reject"`
/// (Reject, code 0), or a table `{ decision = "accept"|"reject"|"correct",
/// reason_code?, reply?, transform? }`. Anything else is a script error — which
/// fails the whole batch closed, never a silent accept.
fn parse_input_decision(value: Value) -> mlua::Result<(Decision, Option<Vec<u8>>)> {
    match value {
        Value::Nil | Value::Boolean(true) => Ok((Decision::Accept, None)),
        Value::Boolean(false) => Ok((Decision::Reject { reason_code: 0 }, None)),
        Value::String(s) => {
            let s = s.to_string_lossy();
            match s.as_ref() {
                "accept" => Ok((Decision::Accept, None)),
                "reject" => Ok((Decision::Reject { reason_code: 0 }, None)),
                other => Err(mlua::Error::RuntimeError(format!(
                    "on_input returned an unknown decision string {other:?}"
                ))),
            }
        }
        Value::Table(t) => {
            let reply = t
                .get::<Option<mlua::String>>("reply")?
                .map(|r| r.as_bytes().to_vec());
            let decision = t
                .get::<Option<mlua::String>>("decision")?
                .map(|d| d.to_string_lossy());
            match decision.as_deref() {
                None | Some("accept") => Ok((Decision::Accept, reply)),
                Some("reject") => {
                    let reason_code = t.get::<Option<u16>>("reason_code")?.unwrap_or(0);
                    Ok((Decision::Reject { reason_code }, reply))
                }
                Some("correct") => Ok((
                    Decision::Correct {
                        correction: parse_correction(&t)?,
                    },
                    reply,
                )),
                Some(other) => Err(mlua::Error::RuntimeError(format!(
                    "on_input returned an unknown decision {other:?}"
                ))),
            }
        }
        _ => Err(mlua::Error::RuntimeError(
            "on_input must return nil, a boolean, a string, or a table".to_string(),
        )),
    }
}

/// Parse a `Correct` decision's substituted value. v1 supports transform
/// corrections (the movement path); rep/spawn corrections error until their
/// Lua shape lands, so an unsupported correction fails the batch closed rather
/// than materializing a wrong value.
fn parse_correction(t: &Table) -> mlua::Result<Correction> {
    if let Some(transform) = t.get::<Option<Table>>("transform")? {
        return Ok(Correction::Transform(parse_transform(&transform)?));
    }
    Err(mlua::Error::RuntimeError(
        "a correct decision requires a transform table (rep/spawn corrections are not yet supported in Lua)"
            .to_string(),
    ))
}

fn parse_vec3(t: &Table) -> mlua::Result<[f32; 3]> {
    Ok([t.get("x")?, t.get("y")?, t.get("z")?])
}

fn parse_transform(t: &Table) -> mlua::Result<BridgeTransform> {
    let position = parse_vec3(&t.get::<Table>("position")?)?;
    let rotation = {
        let r = t.get::<Table>("rotation")?;
        [r.get("x")?, r.get("y")?, r.get("z")?, r.get("w")?]
    };
    let velocity = match t.get::<Option<Table>>("velocity")? {
        Some(v) => parse_vec3(&v)?,
        None => [0.0; 3],
    };
    Ok(BridgeTransform {
        position,
        rotation,
        velocity,
    })
}

/// Build the `ctx` table handed to a lifecycle handler: `sender` plus `user_id`.
fn build_lifecycle_ctx(lua: &Lua, sender: u64, user_id: Option<&str>) -> mlua::Result<Table> {
    let ctx = lua.create_table()?;
    ctx.set("sender", sender)?;
    set_user_id(&ctx, user_id)?;
    Ok(ctx)
}

/// Build the `ctx` table handed to an RPC handler: `sender`, `method`, plus
/// `user_id` for an authenticated caller.
fn build_rpc_ctx(
    lua: &Lua,
    sender: u64,
    user_id: Option<&str>,
    method: &str,
) -> mlua::Result<Table> {
    let ctx = lua.create_table()?;
    ctx.set("sender", sender)?;
    ctx.set("method", method)?;
    set_user_id(&ctx, user_id)?;
    Ok(ctx)
}

/// Look up and invoke the RPC handler for `method`, returning its reply bytes.
///
/// Runs inside the locked, deadline-armed, panic-guarded closure in
/// [`LuaRuntime::call_rpc`]. A missing handler is [`RpcInner::NoHandler`] (not an
/// error); any Lua error (including a non-string return, which fails the
/// `mlua::String` conversion) propagates as `Err` for the caller to classify.
fn call_rpc_handler(
    lua: &Lua,
    sender: u64,
    user_id: Option<&str>,
    method: &str,
    body: &[u8],
) -> mlua::Result<RpcInner> {
    let handlers: Table = lua.named_registry_value(RPC_HANDLERS_KEY)?;
    let Some(handler) = handlers.get::<Option<Function>>(method)? else {
        return Ok(RpcInner::NoHandler);
    };
    let ctx = build_rpc_ctx(lua, sender, user_id, method)?;
    let body_value = lua.create_string(body)?;
    // The handler must return a string; a nil/other return fails this conversion
    // and is reported to the caller as a generic handler error.
    let reply: mlua::String = handler.call((ctx, body_value))?;
    Ok(RpcInner::Reply(reply.as_bytes().to_vec()))
}

/// Invoke `on_room_create` (if registered), mapping its return — a bare string
/// (the map name) or a `{ map, mode?, max_players?, open? }` table — to a
/// [`RoomSpec`]. `Ok(None)` means no handler is registered.
fn call_room_create_handler(
    lua: &Lua,
    sender: u64,
    user_id: Option<&str>,
    params: &[u8],
) -> mlua::Result<Option<RoomSpec>> {
    let Some(handler) = lua.named_registry_value::<Option<Function>>(ON_ROOM_CREATE_KEY)? else {
        return Ok(None);
    };
    let ctx = build_rpc_ctx(lua, sender, user_id, "room.create")?;
    let params_value = lua.create_string(params)?;
    let spec = match handler.call::<mlua::Value>((ctx, params_value))? {
        mlua::Value::String(s) => RoomSpec {
            map: s.to_string_lossy(),
            ..RoomSpec::default()
        },
        mlua::Value::Table(t) => RoomSpec {
            map: t.get::<Option<String>>("map")?.unwrap_or_default(),
            mode: t.get::<Option<String>>("mode")?.unwrap_or_default(),
            max_players: t.get::<Option<u16>>("max_players")?.unwrap_or(0),
            open: t.get::<Option<bool>>("open")?.unwrap_or(true),
        },
        // A nil/other return means "use the default label" (empty map).
        _ => RoomSpec::default(),
    };
    Ok(Some(spec))
}

/// Invoke `on_room_join` (if registered), returning its admission decision.
/// `Ok(None)` means no handler is registered (the caller admits by default).
fn call_room_join_handler(
    lua: &Lua,
    sender: u64,
    user_id: Option<&str>,
    room_id: u64,
) -> mlua::Result<Option<bool>> {
    let Some(handler) = lua.named_registry_value::<Option<Function>>(ON_ROOM_JOIN_KEY)? else {
        return Ok(None);
    };
    let ctx = build_rpc_ctx(lua, sender, user_id, "room.join")?;
    let allow: bool = handler.call((ctx, room_id))?;
    Ok(Some(allow))
}

fn call_http_endpoint_handler(
    lua: &Lua,
    request: RuntimeHttpRequest,
) -> mlua::Result<RuntimeHttpOutcome> {
    let handlers: Table = match lua.named_registry_value(HTTP_ENDPOINT_HANDLERS_KEY) {
        Ok(handlers) => handlers,
        Err(_) => return Ok(RuntimeHttpOutcome::NotFound),
    };
    let key = format!("{} {}", request.method.as_str(), request.path);
    let Some(entry) = handlers.get::<Option<Table>>(key)? else {
        return Ok(RuntimeHttpOutcome::NotFound);
    };
    let handler: Function = entry.get("handler")?;
    let value = lua.create_table()?;
    value.set("method", request.method.as_str())?;
    value.set("path", request.path)?;
    value.set("body", lua.create_string(request.body)?)?;
    if let Some(user_id) = request.user_id {
        value.set("user_id", user_id)?;
    }
    let headers = lua.create_table()?;
    for (name, header) in request.headers {
        headers.set(name, header)?;
    }
    value.set("headers", headers)?;
    let response: Table = handler.call(value)?;
    let status = response.get::<Option<u16>>("status")?.unwrap_or(200);
    if !(100..=599).contains(&status) {
        return Err(mlua::Error::RuntimeError(
            "runtime HTTP endpoint response status is invalid".to_string(),
        ));
    }
    let body = response
        .get::<Option<mlua::String>>("body")?
        .map(|body| body.as_bytes().to_vec())
        .unwrap_or_default();
    let mut response_headers = std::collections::BTreeMap::new();
    if let Some(headers) = response.get::<Option<Table>>("headers")? {
        for pair in headers.pairs::<mlua::String, mlua::String>() {
            let (name, value) = pair?;
            response_headers.insert(name.to_string_lossy(), value.to_string_lossy());
        }
    }
    Ok(RuntimeHttpOutcome::Response(RuntimeHttpResponse {
        status,
        headers: response_headers,
        body,
    }))
}

fn runtime_event_key(namespace: &str, event_type: &str) -> String {
    format!("{namespace}\0{event_type}")
}

fn cache_value_table(
    lua: &Lua,
    value: crate::runtime::RuntimeSharedCacheValue,
) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("value", lua.create_string(&value.value)?)?;
    table.set("version", value.version)?;
    table.set("expires_in_ms", value.expires_in_ms)?;
    Ok(table)
}

/// Deliver the queue snapshot that existed after an outer Lua invocation. A
/// subscriber error drops only that subscriber's queued commands and does not
/// stop later callbacks; any event it emits remains queued for the next outer
/// invocation, so delivery is never recursive.
fn dispatch_pending_runtime_events(
    lua: &Lua,
    label: &str,
    budget: Duration,
) -> Vec<OutboundCommand> {
    let Some(handle) = lua.app_data_ref::<RuntimeEventBusHandle>() else {
        return Vec::new();
    };
    let event_bus = runtime_event_bus(&handle);
    let mut events = event_bus
        .drain_snapshot_limit(MAX_RUNTIME_EVENTS_PER_INVOCATION)
        .into_iter()
        .peekable();
    let Ok(handlers) = lua.named_registry_value::<Table>(EVENT_HANDLERS_KEY) else {
        return Vec::new();
    };
    let mut commands = Vec::new();
    let delivery_deadline = Instant::now() + budget;
    'events: while let Some(event) = events.next() {
        let key = runtime_event_key(&event.namespace, &event.event_type);
        let Ok(Some(callbacks)) = handlers.get::<Option<Table>>(key.as_str()) else {
            continue;
        };
        let callback_count = callbacks.raw_len();
        for (callback_index, callback) in callbacks.sequence_values::<Function>().enumerate() {
            let Ok(callback) = callback else {
                continue;
            };
            let Some(remaining) = delivery_deadline.checked_duration_since(Instant::now()) else {
                tracing::warn!(script = %label, "runtime event delivery budget exhausted; pending events deferred");
                event_bus.requeue_front(events.collect());
                break 'events;
            };
            let subscribers_remaining = callback_count.saturating_sub(callback_index);
            let subscriber_budget = remaining / subscribers_remaining.max(1) as u32;
            if subscriber_budget.is_zero() {
                tracing::warn!(script = %label, "runtime event delivery budget exhausted; pending events deferred");
                event_bus.requeue_front(events.collect());
                break 'events;
            }
            clear_sink(lua);
            set_deadline(lua, Some(Instant::now() + subscriber_budget));
            let result = (|| -> mlua::Result<()> {
                let value = lua.create_table()?;
                value.set("namespace", event.namespace.as_str())?;
                value.set("type", event.event_type.as_str())?;
                value.set("payload", lua.create_string(&event.payload)?)?;
                callback.call(value)
            })();
            match result {
                Ok(()) => append_runtime_event_commands(
                    &mut commands,
                    take_commands(lua, label, "runtime_event"),
                    label,
                ),
                Err(error) => {
                    tracing::error!(
                        script = %label,
                        namespace = %event.namespace,
                        event_type = %event.event_type,
                        error = %error,
                        "lua runtime event subscriber failed; isolated"
                    );
                    clear_sink(lua);
                    if is_deadline_error(&error) && Instant::now() >= delivery_deadline {
                        event_bus.requeue_front(events.collect());
                        break 'events;
                    }
                }
            }
        }
    }
    commands
}

/// Whether an `mlua` error is the deadline-hook abort (a blown time budget).
///
/// The deadline hook raises a `RuntimeError` with a fixed message; matching it
/// lets [`LuaRuntime::call_rpc`] report a distinct "timed out" reason without
/// leaking the message verbatim.
fn is_deadline_error(err: &mlua::Error) -> bool {
    err.to_string().contains("time budget")
}

/// Install the `citadel` global table (message, lifecycle, tick, and log API).
fn install_host_api(
    lua: &Lua,
    source_label: &str,
    static_data: StaticDataCatalog,
    text_policy: TextPolicyCatalog,
    execution_mode: LuaExecutionMode,
    outbound_http_policy: OutboundHttpPolicy,
    http_endpoint_policy: RuntimeHttpEndpointPolicy,
) -> mlua::Result<()> {
    let citadel = lua.create_table()?;
    let handlers = lua.create_table()?;
    lua.set_named_registry_value(HANDLERS_KEY, handlers)?;

    let on_message = lua.create_function(|lua, (kind, handler): (u16, Function)| {
        let handlers: Table = lua.named_registry_value(HANDLERS_KEY)?;
        handlers.set(kind, handler)?;
        Ok(())
    })?;
    citadel.set("on_message", on_message)?;

    // RPC handlers are keyed by method name in their own registry table.
    let rpc_handlers = lua.create_table()?;
    lua.set_named_registry_value(RPC_HANDLERS_KEY, rpc_handlers)?;
    let on_leaderboard_reset = lua.create_function(|lua, handler: Function| {
        lua.set_named_registry_value(ON_LEADERBOARD_RESET_KEY, handler)
    })?;
    citadel.set("on_leaderboard_reset", on_leaderboard_reset)?;

    let on_rpc = lua.create_function(|lua, (method, handler): (String, Function)| {
        let handlers: Table = lua.named_registry_value(RPC_HANDLERS_KEY)?;
        handlers.set(method, handler)?;
        Ok(())
    })?;
    citadel.set("on_rpc", on_rpc)?;

    // Runtime events are an explicitly local, best-effort queue. Subscribe
    // registrations are VM-local and reload atomically with the VM; emission
    // resolves the node-owned bus through app-data at call time.
    let event_handlers = lua.create_table()?;
    lua.set_named_registry_value(EVENT_HANDLERS_KEY, event_handlers)?;
    let events = lua.create_table()?;
    let subscribe = lua.create_function(
        |lua, (namespace, event_type, handler): (String, String, Function)| {
            let event = RuntimeEvent::new(namespace, event_type, Vec::new())
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let key = runtime_event_key(&event.namespace, &event.event_type);
            let handlers: Table = lua.named_registry_value(EVENT_HANDLERS_KEY)?;
            let callbacks = match handlers.get::<Option<Table>>(key.as_str())? {
                Some(callbacks) => callbacks,
                None => {
                    let callbacks = lua.create_table()?;
                    handlers.set(key.as_str(), callbacks.clone())?;
                    callbacks
                }
            };
            if callbacks.raw_len() >= MAX_RUNTIME_EVENT_SUBSCRIBERS {
                return Err(mlua::Error::RuntimeError(
                    "runtime event subscriber limit exceeded".to_string(),
                ));
            }
            callbacks.set(callbacks.raw_len() + 1, handler.clone())?;
            Ok(handler)
        },
    )?;
    events.set("subscribe", subscribe)?;
    let emit = lua.create_function(
        |lua, (namespace, event_type, payload): (String, String, mlua::String)| {
            ensure_realtime_effects_allowed(lua)?;
            let event = RuntimeEvent::new(namespace, event_type, payload.as_bytes().to_vec())
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let Some(handle) = lua.app_data_ref::<RuntimeEventBusHandle>() else {
                return Err(mlua::Error::RuntimeError(
                    "runtime event bus is unavailable".to_string(),
                ));
            };
            Ok(matches!(
                runtime_event_bus(&handle).emit(event),
                RuntimeEventEmitOutcome::Queued
            ))
        },
    )?;
    events.set("emit", emit)?;
    citadel.set("events", events)?;

    // Scope comes only from the server-owned active runtime invocation. Scripts
    // cannot provide a match/session/account/report correlation to this API.
    let telemetry = lua.create_table()?;
    let begin = lua.create_function(|lua, ()| {
        let context = active_runtime_context().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices require a match-scoped context".to_string())
        })?;
        let handle = lua.app_data_ref::<TelemetrySlicesHandle>().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices are unavailable".to_string())
        })?;
        handle
            .0
            .begin(context, SystemClock.now().unix_millis())
            .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
    })?;
    telemetry.set("begin", begin)?;
    let mark = lua.create_function(|lua, marker: String| {
        let context = active_runtime_context().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices require a match-scoped context".to_string())
        })?;
        let handle = lua.app_data_ref::<TelemetrySlicesHandle>().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices are unavailable".to_string())
        })?;
        handle
            .0
            .mark(context, &marker, SystemClock.now().unix_millis())
            .map(|_| ())
            .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
    })?;
    telemetry.set("mark", mark)?;
    let finish = lua.create_function(|lua, ()| {
        let context = active_runtime_context().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices require a match-scoped context".to_string())
        })?;
        let handle = lua.app_data_ref::<TelemetrySlicesHandle>().ok_or_else(|| {
            mlua::Error::RuntimeError("telemetry slices are unavailable".to_string())
        })?;
        handle
            .0
            .finish(context, SystemClock.now().unix_millis())
            .map(|_| ())
            .map_err(|e| mlua::Error::RuntimeError(e.to_string()))
    })?;
    telemetry.set("finish", finish)?;
    citadel.set("telemetry", telemetry)?;

    let cache = lua.create_table()?;
    let get = lua.create_function(|lua, (namespace, key): (String, String)| {
        ensure_realtime_effects_allowed(lua)?;
        let Some(handle) = lua.app_data_ref::<RuntimeSharedCacheHandle>() else {
            return Err(mlua::Error::RuntimeError(
                "runtime shared cache is unavailable".to_string(),
            ));
        };
        runtime_shared_cache(&handle)
            .get(&namespace, &key)
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
            .map(|value| cache_value_table(lua, value))
            .transpose()
    })?;
    cache.set("get", get)?;
    let set = lua.create_function(
        |lua, (namespace, key, value, ttl_ms): (String, String, mlua::String, u64)| {
            ensure_realtime_effects_allowed(lua)?;
            let Some(handle) = lua.app_data_ref::<RuntimeSharedCacheHandle>() else {
                return Err(mlua::Error::RuntimeError(
                    "runtime shared cache is unavailable".to_string(),
                ));
            };
            let value = runtime_shared_cache(&handle)
                .set(
                    &namespace,
                    &key,
                    value.as_bytes().to_vec(),
                    Duration::from_millis(ttl_ms),
                )
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            cache_value_table(lua, value)
        },
    )?;
    cache.set("set", set)?;
    let delete = lua.create_function(|lua, (namespace, key): (String, String)| {
        ensure_realtime_effects_allowed(lua)?;
        let Some(handle) = lua.app_data_ref::<RuntimeSharedCacheHandle>() else {
            return Err(mlua::Error::RuntimeError(
                "runtime shared cache is unavailable".to_string(),
            ));
        };
        runtime_shared_cache(&handle)
            .delete(&namespace, &key)
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))
    })?;
    cache.set("delete", delete)?;
    let cas = lua.create_function(
        |lua,
         (namespace, key, expected, value, ttl_ms): (
            String,
            String,
            Option<u64>,
            mlua::String,
            u64,
        )| {
            ensure_realtime_effects_allowed(lua)?;
            let Some(handle) = lua.app_data_ref::<RuntimeSharedCacheHandle>() else {
                return Err(mlua::Error::RuntimeError(
                    "runtime shared cache is unavailable".to_string(),
                ));
            };
            runtime_shared_cache(&handle)
                .compare_and_swap(
                    &namespace,
                    &key,
                    expected,
                    value.as_bytes().to_vec(),
                    Duration::from_millis(ttl_ms),
                )
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
                .map(|value| cache_value_table(lua, value))
                .transpose()
        },
    )?;
    cache.set("cas", cas)?;
    citadel.set("cache", cache)?;

    // Lifecycle + tick handlers are single functions stored under fixed registry
    // keys (re-registering replaces the prior handler).
    for (name, key) in [
        ("on_join", ON_JOIN_KEY),
        ("on_leave", ON_LEAVE_KEY),
        ("on_tick", ON_TICK_KEY),
        ("on_room_create", ON_ROOM_CREATE_KEY),
        ("on_room_join", ON_ROOM_JOIN_KEY),
        ("before_realtime", BEFORE_REALTIME_KEY),
        ("after_realtime", AFTER_REALTIME_KEY),
        ("on_input", ON_INPUT_KEY),
    ] {
        let register = lua.create_function(move |lua, handler: Function| {
            lua.set_named_registry_value(key, handler)?;
            Ok(())
        })?;
        citadel.set(name, register)?;
    }

    // `citadel.log(message [, level])`: structured logging tagged as script
    // output. `level` is an optional case-insensitive string
    // (trace/debug/info/warn/error); anything else falls back to info.
    let label = source_label.to_string();
    let log = lua.create_function(
        move |_, (message, level): (mlua::String, Option<mlua::String>)| {
            let message = message.to_string_lossy();
            let level = level.map(|l| l.to_string_lossy().to_ascii_lowercase());
            match level.as_deref() {
                Some("trace") => {
                    tracing::trace!(target: "citadel::script", script = %label, "{message}")
                }
                Some("debug") => {
                    tracing::debug!(target: "citadel::script", script = %label, "{message}")
                }
                Some("warn") => {
                    tracing::warn!(target: "citadel::script", script = %label, "{message}")
                }
                Some("error") => {
                    tracing::error!(target: "citadel::script", script = %label, "{message}")
                }
                _ => tracing::info!(target: "citadel::script", script = %label, "{message}"),
            }
            Ok(())
        },
    )?;
    citadel.set("log", log)?;

    let http_api = lua.create_table()?;
    if http_endpoint_policy.enabled {
        let endpoint_handlers = lua.create_table()?;
        lua.set_named_registry_value(HTTP_ENDPOINT_HANDLERS_KEY, endpoint_handlers)?;
        let register = lua.create_function(
            move |lua,
                  (method, path, options_or_handler, supplied_handler): (
                String,
                String,
                mlua::Value,
                Option<Function>,
            )| {
                let (options, handler) = match options_or_handler {
                    mlua::Value::Function(handler) if supplied_handler.is_none() => (None, handler),
                    mlua::Value::Nil => (
                        None,
                        supplied_handler.ok_or_else(|| {
                            mlua::Error::RuntimeError(
                                "runtime HTTP endpoint handler must be a function".to_string(),
                            )
                        })?,
                    ),
                    mlua::Value::Table(options) => (
                        Some(options),
                        supplied_handler.ok_or_else(|| {
                            mlua::Error::RuntimeError(
                                "runtime HTTP endpoint handler must be a function".to_string(),
                            )
                        })?,
                    ),
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "runtime HTTP endpoint options must be a table".to_string(),
                        ));
                    }
                };
                let method = RuntimeHttpMethod::parse(&method).ok_or_else(|| {
                    mlua::Error::RuntimeError("runtime HTTP endpoint method is invalid".to_string())
                })?;
                let auth = match options.as_ref() {
                    Some(options) => match options.get::<Option<mlua::Value>>("auth")? {
                        Some(mlua::Value::String(auth)) => {
                            let auth = auth.to_string_lossy();
                            RuntimeHttpAuth::parse(&auth).ok_or_else(|| {
                                mlua::Error::RuntimeError(
                                    "runtime HTTP endpoint auth must be 'public' or 'session'"
                                        .to_string(),
                                )
                            })?
                        }
                        Some(_) => {
                            return Err(mlua::Error::RuntimeError(
                                "runtime HTTP endpoint auth must be 'public' or 'session'"
                                    .to_string(),
                            ));
                        }
                        None => RuntimeHttpAuth::Public,
                    },
                    None => RuntimeHttpAuth::Public,
                };
                let endpoint = RuntimeHttpEndpoint::new(method, path, auth)
                    .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
                let key = format!("{} {}", endpoint.method.as_str(), endpoint.path);
                let handlers: Table = lua.named_registry_value(HTTP_ENDPOINT_HANDLERS_KEY)?;
                if handlers.contains_key(key.as_str())? {
                    return Err(mlua::Error::RuntimeError(
                        "runtime HTTP endpoint is already registered".to_string(),
                    ));
                }
                let entry = lua.create_table()?;
                entry.set("method", endpoint.method.as_str())?;
                entry.set("path", endpoint.path)?;
                entry.set("auth", endpoint.auth.as_str())?;
                entry.set("handler", &handler)?;
                handlers.set(key, entry)?;
                Ok(handler)
            },
        )?;
        http_api.set("register", register)?;
    }

    if execution_mode == LuaExecutionMode::Trusted {
        let http = TrustedHttpClient::new_with_policy(outbound_http_policy).map_err(|error| {
            mlua::Error::RuntimeError(format!("cannot initialize outbound HTTP client: {error}"))
        })?;
        let async_http = AsyncOutboundHttp::new(http.clone());
        let fetch_http = http;
        let fetch = lua.create_function(move |lua, (url, options): (String, Option<Table>)| {
            ensure_outbound_http_allowed(lua)?;
            let options = options.unwrap_or(lua.create_table()?);
            let method = options
                .get::<Option<String>>("method")?
                .unwrap_or_else(|| "GET".to_string());
            let body = options
                .get::<Option<mlua::String>>("body")?
                .map(|value| value.as_bytes().to_vec())
                .unwrap_or_default();
            let headers = options
                .get::<Option<Table>>("headers")?
                .map(|headers| {
                    headers
                        .pairs::<mlua::String, mlua::String>()
                        .map(|pair| {
                            pair.map(|(name, value)| {
                                (
                                    name.to_string_lossy().to_string(),
                                    value.to_string_lossy().to_string(),
                                )
                            })
                        })
                        .collect::<mlua::Result<_>>()
                })
                .transpose()?
                .unwrap_or_default();
            let response = fetch_http
                .execute_blocking(OutboundHttpRequest {
                    method,
                    url,
                    headers,
                    body,
                })
                .map_err(|error| mlua::Error::RuntimeError(error.error_code().to_string()))?;
            let result = lua.create_table()?;
            result.set("status", response.status)?;
            result.set("body", lua.create_string(response.body)?)?;
            Ok(result)
        })?;
        let start_http = async_http.clone();
        let start = lua.create_function(move |lua, (url, options): (String, Option<Table>)| {
            ensure_outbound_http_allowed(lua)?;
            let options = options.unwrap_or(lua.create_table()?);
            let method = options
                .get::<Option<String>>("method")?
                .unwrap_or_else(|| "GET".into());
            let body = options
                .get::<Option<mlua::String>>("body")?
                .map(|value| value.as_bytes().to_vec())
                .unwrap_or_default();
            let mut headers = std::collections::BTreeMap::new();
            if let Some(header_table) = options.get::<Option<Table>>("headers")? {
                for entry in header_table.pairs::<mlua::String, mlua::String>() {
                    let (name, value) = entry?;
                    headers.insert(name.to_string_lossy(), value.to_string_lossy());
                }
            }
            let handle = start_http
                .start(OutboundHttpRequest {
                    method,
                    url,
                    headers,
                    body,
                })
                .map_err(|error| mlua::Error::RuntimeError(error.error_code().to_string()))?;
            Ok(handle)
        })?;
        let poll_http = async_http.clone();
        let poll = lua.create_function(move |lua, handle: u64| {
            ensure_outbound_http_allowed(lua)?;
            outbound_http_state_to_lua(
                lua,
                poll_http
                    .poll(handle)
                    .map_err(|e| mlua::Error::RuntimeError(e.error_code().to_string()))?,
            )
        })?;
        let cancel = lua.create_function(move |lua, handle: u64| {
            ensure_outbound_http_allowed(lua)?;
            outbound_http_state_to_lua(
                lua,
                async_http
                    .cancel(handle)
                    .map_err(|e| mlua::Error::RuntimeError(e.error_code().to_string()))?,
            )
        })?;
        http_api.set("start", start)?;
        http_api.set("poll", poll)?;
        http_api.set("cancel", cancel)?;
        http_api.set("fetch", fetch)?;
    }
    if !http_api.is_empty() {
        citadel.set("http", http_api)?;
    }

    // Static game data is intentionally a tiny, capability-free subtable. The
    // catalog has already been rooted and bounded by the host; scripts receive
    // only parsed Lua values and never the path, file, or directory handles.
    let static_data_api = lua.create_table()?;
    let json_catalog = static_data.clone();
    let load_json = lua.create_function(move |lua, path: mlua::String| {
        let path = path.to_str().map_err(|_| {
            mlua::Error::RuntimeError("static data path must be valid UTF-8".to_string())
        })?;
        let value = json_catalog
            .load_json(&path)
            .map_err(static_data_lua_error)?;
        static_data_value_to_lua(lua, &value)
    })?;
    static_data_api.set("load_json", load_json)?;
    let csv_catalog = static_data;
    let load_csv = lua.create_function(move |lua, path: mlua::String| {
        let path = path.to_str().map_err(|_| {
            mlua::Error::RuntimeError("static data path must be valid UTF-8".to_string())
        })?;
        let value = csv_catalog.load_csv(&path).map_err(static_data_lua_error)?;
        static_data_value_to_lua(lua, &value)
    })?;
    static_data_api.set("load_csv", load_csv)?;
    citadel.set("static_data", static_data_api)?;

    let text_policy_api = lua.create_table()?;
    let load_catalog = text_policy.clone();
    let load_policy = lua.create_function(move |_, path: mlua::String| {
        let path = path.to_str().map_err(|_| {
            mlua::Error::RuntimeError("text policy path must be valid UTF-8".to_string())
        })?;
        load_catalog
            .load_json(&path)
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))
    })?;
    text_policy_api.set("load_json", load_policy)?;
    let scan_catalog = text_policy.clone();
    let scan_policy = lua.create_function(
        move |lua, (reference, text): (mlua::String, mlua::String)| {
            let reference = reference.to_str()?;
            let text = text.to_str()?;
            let value = scan_catalog
                .scan_value(&reference, &text)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            static_data_value_to_lua(lua, &value)
        },
    )?;
    text_policy_api.set("scan", scan_policy)?;
    let sanitize_policy = lua.create_function(
        move |lua, (reference, text): (mlua::String, mlua::String)| {
            let reference = reference.to_str()?;
            let text = text.to_str()?;
            let value = text_policy
                .sanitize_value(&reference, &text)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            static_data_value_to_lua(lua, &value)
        },
    )?;
    text_policy_api.set("sanitize", sanitize_policy)?;
    citadel.set("text_policy", text_policy_api)?;

    let broadcast = lua.create_function(
        |lua, (kind, body, unreliable): (u16, mlua::String, Option<bool>)| {
            push_command(lua, &body, |bytes| OutboundCommand::Broadcast {
                kind,
                body: bytes,
                unreliable: unreliable.unwrap_or(false),
            })
        },
    )?;
    citadel.set("broadcast", broadcast)?;

    let send = lua.create_function(
        |lua, (session, kind, body, unreliable): (u64, u16, mlua::String, Option<bool>)| {
            push_command(lua, &body, |bytes| OutboundCommand::Send {
                session,
                kind,
                body: bytes,
                unreliable: unreliable.unwrap_or(false),
            })
        },
    )?;
    citadel.set("send", send)?;

    // spawn_actor{ archetype = , x = , y = , z = } -> object_id. Spawns a server-owned
    // NPC and returns its id synchronously so the script can move/despawn it.
    let spawn_actor = lua.create_function(|lua, opts: mlua::Table| {
        let archetype: u16 = opts.get("archetype").unwrap_or(0);
        let x: f32 = opts.get("x").unwrap_or(0.0);
        let y: f32 = opts.get("y").unwrap_or(0.0);
        let z: f32 = opts.get("z").unwrap_or(0.0);
        let object_id = {
            let mut ctr = lua
                .app_data_mut::<NpcIdCounter>()
                .ok_or_else(|| mlua::Error::RuntimeError("npc id counter missing".into()))?;
            let id = ctr.0;
            ctr.0 = ctr.0.checked_add(1).unwrap_or(NPC_ID_BASE);
            id
        };
        push_actor_command(
            lua,
            OutboundCommand::SpawnActor {
                object_id,
                archetype,
                position: [x, y, z],
            },
        )?;
        if opts.get::<Option<String>>("ai")?.as_deref() == Some("patrol") {
            let map: String = opts.get("map")?;
            let points: mlua::Table = opts.get("waypoints")?;
            let mut waypoints = Vec::new();
            for point in points.sequence_values::<mlua::Table>() {
                let point = point?;
                waypoints.push([point.get("x")?, point.get("y")?, point.get("z")?]);
            }
            if waypoints.is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "patrol requires one or more waypoints".into(),
                ));
            }
            let speed = opts.get::<Option<f32>>("speed")?.unwrap_or(300.0).max(0.0);
            let mut patrols = lua
                .app_data_mut::<NpcPatrols>()
                .ok_or_else(|| mlua::Error::RuntimeError("npc patrol registry missing".into()))?;
            patrols.0.push(NpcPatrol {
                object_id,
                map,
                position: [x, y, z],
                waypoints,
                next_waypoint: 0,
                speed,
            });
        }
        Ok(object_id)
    })?;
    citadel.set("spawn_actor", spawn_actor)?;

    // move_actor(object_id, x, y, z, [vx, vy, vz]) -> update a server-owned actor.
    let move_actor = lua.create_function(
        |lua,
         (object_id, x, y, z, vx, vy, vz): (
            u32,
            f32,
            f32,
            f32,
            Option<f32>,
            Option<f32>,
            Option<f32>,
        )| {
            push_actor_command(
                lua,
                OutboundCommand::MoveActor {
                    object_id,
                    position: [x, y, z],
                    // Identity facing for the MVP; the client can orient by velocity.
                    rotation: [0.0, 0.0, 0.0, 1.0],
                    velocity: [vx.unwrap_or(0.0), vy.unwrap_or(0.0), vz.unwrap_or(0.0)],
                },
            )
        },
    )?;
    citadel.set("move_actor", move_actor)?;

    // despawn_actor(object_id) -> remove a server-owned actor.
    let despawn_actor = lua.create_function(|lua, object_id: u32| {
        push_actor_command(lua, OutboundCommand::DespawnActor { object_id })
    })?;
    citadel.set("despawn_actor", despawn_actor)?;

    // set_physics(object_id, opts) attaches or configures a kinematic body.
    // Passing nil, or `{ enabled = false }`, detaches the body.
    let set_physics = lua.create_function(|lua, (object_id, opts): (u32, Option<Table>)| {
        let opts = opts.map(physics_options_from_lua).transpose()?;
        push_actor_command(lua, OutboundCommand::SetPhysics { object_id, opts })
    })?;
    citadel.set("set_physics", set_physics)?;

    let apply_impulse =
        lua.create_function(|lua, (object_id, ix, iy, iz): (u32, f32, f32, f32)| {
            push_actor_command(
                lua,
                OutboundCommand::ApplyImpulse {
                    object_id,
                    impulse: [ix, iy, iz],
                },
            )
        })?;
    citadel.set("apply_impulse", apply_impulse)?;

    let set_move_intent =
        lua.create_function(|lua, (object_id, vx, vy, vz): (u32, f32, f32, f32)| {
            push_actor_command(
                lua,
                OutboundCommand::SetMoveIntent {
                    object_id,
                    intent: [vx, vy, vz],
                },
            )
        })?;
    citadel.set("set_move_intent", set_move_intent)?;

    // physics_state(object_id) reads the authoritative hub without queuing a
    // command. It is nil when transform sync is disabled or the actor has no body.
    let physics_state = lua.create_function(|lua, object_id: u32| {
        let Some(hub) = lua.app_data_ref::<TransformHubHandle>() else {
            return Ok(mlua::Value::Nil);
        };
        let Some(state) = hub.0.physics_state(object_id) else {
            return Ok(mlua::Value::Nil);
        };
        let value = lua.create_table()?;
        value.set("grounded", state.grounded)?;
        value.set("position", state.position)?;
        value.set("velocity", state.velocity)?;
        Ok(mlua::Value::Table(value))
    })?;
    citadel.set("physics_state", physics_state)?;

    // rewind_query(shooter, origin, direction, tick) is the bounded
    // lag-compensated hit query (owner decision 1). Rust owns the rewind
    // geometry; the script decides the consequence. Returns { hits = { ... } }.
    let rewind_query = lua.create_function(
        |lua, (shooter, origin, direction, tick): (u64, Table, Table, u64)| {
            let value = lua.create_table()?;
            let hits_table = lua.create_table()?;
            if let Some(hub) = lua.app_data_ref::<TransformHubHandle>() {
                let origin = lua_vector3(origin)?;
                let direction = lua_vector3(direction)?;
                for (index, hit) in hub
                    .0
                    .rewind_query(shooter, origin, direction, tick)
                    .into_iter()
                    .enumerate()
                {
                    let entry = lua.create_table()?;
                    entry.set("object_id", hit.object_id)?;
                    entry.set("participant", hit.participant)?;
                    entry.set("point", hit.point)?;
                    entry.set("distance", hit.distance)?;
                    hits_table.set(index + 1, entry)?;
                }
            }
            value.set("hits", hits_table)?;
            Ok(mlua::Value::Table(value))
        },
    )?;
    citadel.set("rewind_query", rewind_query)?;

    // map_info(name) returns the loaded CMAP's read-only geometry summary, or
    // nil when the map is not loaded. No catalog mutation is exposed to scripts.
    let map_info = lua.create_function(|lua, name: mlua::String| {
        let Some(maps) = lua.app_data_ref::<MapCatalogHandle>() else {
            return Ok(mlua::Value::Nil);
        };
        let Some(info) = maps.0.info(&name.to_string_lossy()) else {
            return Ok(mlua::Value::Nil);
        };
        let value = lua.create_table()?;
        value.set("bounds_min", info.bounds_min)?;
        value.set("bounds_max", info.bounds_max)?;
        value.set("vertex_count", info.vertex_count)?;
        value.set("triangle_count", info.triangle_count)?;
        Ok(mlua::Value::Table(value))
    })?;
    citadel.set("map_info", map_info)?;

    // map_names() lists only already-loaded catalog keys in stable order. It
    // does not disclose filesystem paths or permit catalog mutation.
    let map_names = lua.create_function(|lua, ()| {
        let value = lua.create_table()?;
        if let Some(maps) = lua.app_data_ref::<MapCatalogHandle>() {
            for (index, name) in maps.0.names().enumerate() {
                value.set(index + 1, name)?;
            }
        }
        Ok(value)
    })?;
    citadel.set("map_names", map_names)?;

    // find_path(map, start, goal) invokes Detour in the Rust core. Scripts get
    // only the resulting corridor; missing maps and unreachable endpoints are
    // represented as nil so they cannot inspect native navigation failures.
    let find_path =
        lua.create_function(|lua, (name, start, goal): (mlua::String, Table, Table)| {
            let start = lua_vector3(start)?;
            let goal = lua_vector3(goal)?;
            let Some(maps) = lua.app_data_ref::<MapCatalogHandle>() else {
                return Ok(mlua::Value::Nil);
            };
            let Ok(Some(path)) = maps.0.find_path(&name.to_string_lossy(), start, goal) else {
                return Ok(mlua::Value::Nil);
            };
            let value = lua.create_table()?;
            for (index, point) in path.into_iter().enumerate() {
                value.set(index + 1, point)?;
            }
            Ok(mlua::Value::Table(value))
        })?;
    citadel.set("find_path", find_path)?;

    let raycast = lua.create_function(|lua, (origin, direction): (Table, Table)| {
        let origin = lua_vector3(origin)?;
        let direction = lua_vector3(direction)?;
        let Some(hub) = lua.app_data_ref::<TransformHubHandle>() else {
            return Ok(mlua::Value::Nil);
        };
        let Some(hit) = hub.0.raycast(origin, direction) else {
            return Ok(mlua::Value::Nil);
        };
        let value = lua.create_table()?;
        value.set("point", hit.point)?;
        value.set("normal", hit.normal)?;
        value.set("distance", hit.distance)?;
        value.set("triangle_index", hit.triangle_index)?;
        Ok(mlua::Value::Table(value))
    })?;
    citadel.set("raycast", raycast)?;

    let sphere_overlap = lua.create_function(|lua, (centre, radius): (Table, f32)| {
        if !radius.is_finite() || radius < 0.0 {
            return Err(mlua::Error::RuntimeError(
                "radius must be a finite non-negative number".into(),
            ));
        }
        let centre = lua_vector3(centre)?;
        Ok(lua
            .app_data_ref::<TransformHubHandle>()
            .is_some_and(|hub| hub.0.sphere_overlap(centre, radius)))
    })?;
    citadel.set("sphere_overlap", sphere_overlap)?;

    let ground_height = lua.create_function(|lua, (origin, max_distance): (Table, f32)| {
        if !max_distance.is_finite() || max_distance < 0.0 {
            return Err(mlua::Error::RuntimeError(
                "max_distance must be a finite non-negative number".into(),
            ));
        }
        let origin = lua_vector3(origin)?;
        let Some(hub) = lua.app_data_ref::<TransformHubHandle>() else {
            return Ok(mlua::Value::Nil);
        };
        let Some(hit) = hub.0.ground_height(origin, max_distance) else {
            return Ok(mlua::Value::Nil);
        };
        let value = lua.create_table()?;
        value.set("point", hit.point)?;
        value.set("normal", hit.normal)?;
        value.set("distance", hit.distance)?;
        value.set("triangle_index", hit.triangle_index)?;
        Ok(mlua::Value::Table(value))
    })?;
    citadel.set("ground_height", ground_height)?;

    // Persisted friends host API. Each acts as the given `user`
    // (the script is authoritative in the trusted tier). Values are returned
    // synchronously; the async service is bridged behind the DomainHost seam.
    // These functions require a runtime built with `with_domain_host`; without
    // one they error "friends host not available".
    let friends_add = lua.create_function(|lua, (user, other): (mlua::String, mlua::String)| {
        let host = domain_host(lua)?;
        host.friends_add(&user.to_string_lossy(), &other.to_string_lossy())
            .map_err(mlua::Error::RuntimeError)
    })?;
    citadel.set("friends_add", friends_add)?;

    let friends_remove =
        lua.create_function(|lua, (user, other): (mlua::String, mlua::String)| {
            let host = domain_host(lua)?;
            host.friends_remove(&user.to_string_lossy(), &other.to_string_lossy())
                .map_err(mlua::Error::RuntimeError)
        })?;
    citadel.set("friends_remove", friends_remove)?;

    let friends_block =
        lua.create_function(|lua, (user, other): (mlua::String, mlua::String)| {
            let host = domain_host(lua)?;
            host.friends_block(&user.to_string_lossy(), &other.to_string_lossy())
                .map_err(mlua::Error::RuntimeError)
        })?;
    citadel.set("friends_block", friends_block)?;

    let friends_list = lua.create_function(|lua, user: mlua::String| {
        let host = domain_host(lua)?;
        let rows = host
            .friends_list(&user.to_string_lossy())
            .map_err(mlua::Error::RuntimeError)?;
        let arr = lua.create_table()?;
        for (i, row) in rows.into_iter().enumerate() {
            let entry = lua.create_table()?;
            entry.set("user_id", row.user_id)?;
            entry.set("state", row.state)?;
            entry.set("updated_unix_ms", row.updated_unix_ms)?;
            arr.set(i + 1, entry)?;
        }
        Ok(arr)
    })?;
    citadel.set("friends_list", friends_list)?;

    // Durable player notifications. `content_json` keeps the Lua
    // surface dependency-free; list/send return ordinary Lua tables.
    let notifications_send = lua.create_function(
        |lua,
         (recipient, code, subject, content_json, sender, delivery_key): (
            mlua::String,
            i32,
            mlua::String,
            mlua::String,
            Option<mlua::String>,
            Option<mlua::String>,
        )| {
            let host = domain_host(lua)?;
            let sender = sender.map(|value| value.to_string_lossy().to_string());
            let delivery_key = delivery_key.map(|value| value.to_string_lossy().to_string());
            let notification = host
                .notifications_send(
                    &recipient.to_string_lossy(),
                    code,
                    &subject.to_string_lossy(),
                    &content_json.to_string_lossy(),
                    sender.as_deref(),
                    delivery_key.as_deref(),
                )
                .map_err(mlua::Error::RuntimeError)?;
            notification_lua_table(lua, notification)
        },
    )?;
    citadel.set("notifications_send", notifications_send)?;

    let notifications_list = lua.create_function(
        |lua, (recipient, limit, cursor): (mlua::String, Option<usize>, Option<mlua::String>)| {
            let host = domain_host(lua)?;
            let cursor = cursor.map(|value| value.to_string_lossy().to_string());
            let page = host
                .notifications_list(
                    &recipient.to_string_lossy(),
                    limit.unwrap_or(50),
                    cursor.as_deref(),
                )
                .map_err(mlua::Error::RuntimeError)?;
            let out = lua.create_table()?;
            let items = lua.create_table()?;
            for (index, notification) in page.items.into_iter().enumerate() {
                items.set(index + 1, notification_lua_table(lua, notification)?)?;
            }
            out.set("items", items)?;
            out.set("next_cursor", page.next_cursor)?;
            Ok(out)
        },
    )?;
    citadel.set("notifications_list", notifications_list)?;

    let notifications_mark_read =
        lua.create_function(|lua, (recipient, ids): (mlua::String, mlua::Table)| {
            let host = domain_host(lua)?;
            let ids = ids
                .sequence_values::<mlua::String>()
                .map(|value| value.map(|value| value.to_string_lossy().to_string()))
                .collect::<mlua::Result<Vec<_>>>()?;
            let changed = host
                .notifications_mark_read(&recipient.to_string_lossy(), &ids)
                .map_err(mlua::Error::RuntimeError)?;
            let out = lua.create_table()?;
            for (index, id) in changed.into_iter().enumerate() {
                out.set(index + 1, id)?;
            }
            Ok(out)
        })?;
    citadel.set("notifications_mark_read", notifications_mark_read)?;

    // Groups uses the same JSON request/response schema as the built-in
    // `groups.*` client RPC. The trusted script supplies its authoritative
    // actor id explicitly.
    let groups_call = lua.create_function(
        |lua, (actor, operation, payload_json): (mlua::String, mlua::String, mlua::String)| {
            let host = domain_host(lua)?;
            host.groups_call(
                &actor.to_string_lossy(),
                &operation.to_string_lossy(),
                &payload_json.to_string_lossy(),
            )
            .map_err(mlua::Error::RuntimeError)
        },
    )?;
    citadel.set("groups_call", groups_call)?;

    for (name, domain) in [
        ("leaderboards_call", "leaderboards"),
        ("tournaments_call", "tournaments"),
        ("chat_call", "chat"),
        ("wallet_call", "wallet"),
    ] {
        let function =
            lua.create_function(
                move |lua,
                      (actor, operation, payload_json): (
                    mlua::String,
                    mlua::String,
                    mlua::String,
                )| {
                    let host = domain_host(lua)?;
                    let call = match domain {
                        "leaderboards" => host.leaderboards_call(
                            &actor.to_string_lossy(),
                            &operation.to_string_lossy(),
                            &payload_json.to_string_lossy(),
                        ),
                        "tournaments" => host.tournaments_call(
                            &actor.to_string_lossy(),
                            &operation.to_string_lossy(),
                            &payload_json.to_string_lossy(),
                        ),
                        "chat" => host.chat_call(
                            &actor.to_string_lossy(),
                            &operation.to_string_lossy(),
                            &payload_json.to_string_lossy(),
                        ),
                        _ => host.wallet_call(
                            &actor.to_string_lossy(),
                            &operation.to_string_lossy(),
                            &payload_json.to_string_lossy(),
                        ),
                    };
                    call.map_err(mlua::Error::RuntimeError)
                },
            )?;
        citadel.set(name, function)?;
    }

    // Persistent storage host API. Scripts pass the owner id in
    // the trusted tier; values remain typed JSON objects encoded as strings so
    // the narrow Lua standard library needs no JSON dependency.
    let storage_read = lua.create_function(
        |lua, (user, collection, key): (mlua::String, mlua::String, mlua::String)| {
            let host = domain_host(lua)?;
            match host
                .storage_read(
                    &user.to_string_lossy(),
                    &collection.to_string_lossy(),
                    &key.to_string_lossy(),
                )
                .map_err(mlua::Error::RuntimeError)?
            {
                Some(object) => storage_object_table(lua, object),
                None => Ok(mlua::Value::Nil),
            }
        },
    )?;
    citadel.set("storage_read", storage_read)?;

    let storage_index_filters = lua.create_table()?;
    let register_storage_index_filter_registry = storage_index_filters.clone();
    let register_storage_index_filter = lua.create_function(
        move |lua, (index_name, callback): (mlua::String, Function)| {
            ensure_realtime_effects_allowed(lua)?;
            let index_name = StorageIndexName::new(index_name.to_string_lossy())
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let existing: Value =
                register_storage_index_filter_registry.get(index_name.as_str())?;
            if !matches!(existing, Value::Nil) {
                return Err(mlua::Error::RuntimeError(format!(
                    "storage index filter already registered for `{index_name}`"
                )));
            }
            register_storage_index_filter_registry.set(index_name.as_str(), callback)?;
            Ok(())
        },
    )?;
    citadel.set(
        "register_storage_index_filter",
        register_storage_index_filter,
    )?;

    let storage_write_filters = storage_index_filters.clone();
    let storage_write = lua.create_function(
        move |lua,
              (
            user,
            collection,
            key,
            value_json,
            expected_version,
            read_permission,
            write_permission,
        ): (
            mlua::String,
            mlua::String,
            mlua::String,
            mlua::String,
            Option<mlua::String>,
            Option<u8>,
            Option<u8>,
        )| {
            let host = domain_host(lua)?;
            let user = user.to_string_lossy().to_string();
            let collection = collection.to_string_lossy().to_string();
            let key = key.to_string_lossy().to_string();
            let value_json = value_json.to_string_lossy().to_string();
            let candidates = host
                .storage_index_candidates(&user, &collection, &key)
                .map_err(mlua::Error::RuntimeError)?;
            let candidate = lua.create_table()?;
            candidate.set("user_id", user.as_str())?;
            candidate.set("collection", collection.as_str())?;
            candidate.set("key", key.as_str())?;
            candidate.set("value_json", value_json.as_str())?;
            candidate.set(
                "expected_version",
                expected_version
                    .as_ref()
                    .map(|value| value.to_string_lossy()),
            )?;
            candidate.set("read_permission", read_permission)?;
            candidate.set("write_permission", write_permission)?;
            let mut included = Vec::with_capacity(candidates.len());
            for index_name in candidates {
                candidate.set("index_name", index_name.as_str())?;
                let registered: Value = storage_write_filters.get(index_name.as_str())?;
                match registered {
                    Value::Nil => included.push(index_name),
                    Value::Function(callback) => match callback.call::<Value>(candidate.clone())? {
                        Value::Boolean(true) => included.push(index_name),
                        Value::Boolean(false) => {}
                        _ => {
                            return Err(mlua::Error::RuntimeError(
                                "storage index filter must return a boolean".to_string(),
                            ));
                        }
                    },
                    _ => {
                        return Err(mlua::Error::RuntimeError(
                            "storage index filter registration is invalid".to_string(),
                        ));
                    }
                }
            }
            let included_json = serde_json::to_string(&included)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let object = host
                .storage_write(
                    StorageWriteInput::new(&user, &collection, &key, &value_json)
                        .expecting(
                            expected_version
                                .as_ref()
                                .map(|value| value.to_string_lossy())
                                .as_deref(),
                        )
                        .with_permissions(read_permission, write_permission)
                        .with_included_index_names_json(Some(&included_json)),
                )
                .map_err(mlua::Error::RuntimeError)?;
            storage_object_table(lua, object)
        },
    )?;
    citadel.set("storage_write", storage_write)?;

    let storage_delete = lua.create_function(
        |lua,
         (user, collection, key, expected_version): (
            mlua::String,
            mlua::String,
            mlua::String,
            Option<mlua::String>,
        )| {
            let host = domain_host(lua)?;
            host.storage_delete(
                &user.to_string_lossy(),
                &collection.to_string_lossy(),
                &key.to_string_lossy(),
                expected_version
                    .as_ref()
                    .map(|value| value.to_string_lossy())
                    .as_deref(),
            )
            .map_err(mlua::Error::RuntimeError)
        },
    )?;
    citadel.set("storage_delete", storage_delete)?;

    let storage_index_query = lua.create_function(
        |lua, (index_name, filters_json, limit): (mlua::String, mlua::String, usize)| {
            let host = domain_host(lua)?;
            let objects = host
                .storage_index_query(
                    &index_name.to_string_lossy(),
                    &filters_json.to_string_lossy(),
                    limit,
                )
                .map_err(mlua::Error::RuntimeError)?;
            let result = lua.create_table()?;
            for (position, object) in objects.into_iter().enumerate() {
                result.set(position + 1, storage_index_object_table(lua, object)?)?;
            }
            Ok(result)
        },
    )?;
    citadel.set("storage_index_query", storage_index_query)?;

    lua.globals().set("citadel", citadel)?;
    Ok(())
}

fn outbound_http_state_to_lua(lua: &Lua, state: OutboundHttpRequestState) -> mlua::Result<Table> {
    let result = lua.create_table()?;
    result.set("state", state.status())?;
    match state {
        OutboundHttpRequestState::Success(response) => {
            result.set("status", response.status)?;
            result.set("body", lua.create_string(response.body)?)?;
        }
        OutboundHttpRequestState::Error(error) => result.set("error_code", error)?,
        _ => {}
    }
    Ok(result)
}

/// Convert a static-data catalog error to a normal Lua runtime error without
/// attaching a Rust stack trace or host-path detail.
fn static_data_lua_error(error: crate::runtime::static_data::StaticDataError) -> mlua::Error {
    mlua::Error::RuntimeError(error.to_string())
}

/// Convert parsed JSON/CSV values to ordinary Lua data. JSON arrays use Lua's
/// conventional one-based indices; objects use string keys; JSON `null` is Lua
/// `nil`. The catalog is the cache, not the returned table, so scripts may shape
/// their in-memory copy without mutating later reads or the replacement catalog.
fn static_data_value_to_lua(lua: &Lua, value: &serde_json::Value) -> mlua::Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Nil),
        serde_json::Value::Bool(value) => Ok(Value::Boolean(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Integer(value))
            } else if let Some(value) = value.as_u64() {
                if let Ok(value) = i64::try_from(value) {
                    Ok(Value::Integer(value))
                } else {
                    Ok(Value::Number(value as f64))
                }
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Number(value))
            } else {
                Err(mlua::Error::RuntimeError(
                    "static data contains an unsupported number".to_string(),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(lua.create_string(value)?)),
        serde_json::Value::Array(values) => {
            let table = lua.create_table()?;
            for (index, value) in values.iter().enumerate() {
                table.set(index + 1, static_data_value_to_lua(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
        serde_json::Value::Object(values) => {
            let table = lua.create_table()?;
            for (key, value) in values {
                table.set(key.as_str(), static_data_value_to_lua(lua, value)?)?;
            }
            Ok(Value::Table(table))
        }
    }
}

/// Fetch the domain-services seam from VM app-data for a `citadel.friends_*`
/// host call, or a clean Lua error when the runtime has no services attached.
///
/// The `Arc` is cloned out so the app-data borrow is released before the
/// (potentially blocking) service call runs.
fn domain_host(lua: &Lua) -> mlua::Result<Arc<dyn DomainHost>> {
    ensure_realtime_effects_allowed(lua)?;
    lua.app_data_ref::<DomainHostHandle>()
        .map(|handle| Arc::clone(&handle.0))
        .ok_or_else(|| mlua::Error::RuntimeError("friends host not available".into()))
}

fn ensure_outbound_http_allowed(lua: &Lua) -> mlua::Result<()> {
    if lua
        .app_data_ref::<InvocationMode>()
        .is_some_and(|mode| *mode == InvocationMode::RealtimeInterceptor)
    {
        return Err(mlua::Error::RuntimeError("interceptor_forbidden".into()));
    }
    Ok(())
}

fn ensure_realtime_effects_allowed(lua: &Lua) -> mlua::Result<()> {
    if lua
        .app_data_ref::<InvocationMode>()
        .is_some_and(|mode| *mode == InvocationMode::RealtimeInterceptor)
    {
        return Err(mlua::Error::RuntimeError(
            "domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors"
                .into(),
        ));
    }
    Ok(())
}

fn notification_lua_table(lua: &Lua, notification: PlayerNotification) -> mlua::Result<Table> {
    let out = lua.create_table()?;
    out.set("id", notification.id)?;
    out.set("code", notification.code)?;
    out.set("subject", notification.subject)?;
    out.set("content_json", notification.content.to_string())?;
    out.set("sender", notification.sender)?;
    out.set("created_at_unix_ms", notification.created_at_unix_ms)?;
    out.set("read_at_unix_ms", notification.read_at_unix_ms)?;
    Ok(out)
}

fn storage_object_table(
    lua: &Lua,
    object: crate::runtime::StorageObjectDto,
) -> mlua::Result<mlua::Value> {
    let result = lua.create_table()?;
    result.set("value_json", object.value_json)?;
    result.set("version", object.version)?;
    result.set("read_permission", object.read_permission)?;
    result.set("write_permission", object.write_permission)?;
    Ok(mlua::Value::Table(result))
}

fn storage_index_object_table(
    lua: &Lua,
    object: crate::runtime::StorageIndexObjectDto,
) -> mlua::Result<Table> {
    let result = lua.create_table()?;
    match object.user_id {
        Some(user_id) => result.set("user_id", user_id)?,
        None => result.set("user_id", mlua::Value::Nil)?,
    }
    result.set("collection", object.collection)?;
    result.set("key", object.key)?;
    result.set("value_json", object.object.value_json)?;
    result.set("version", object.object.version)?;
    result.set("read_permission", object.object.read_permission)?;
    result.set("write_permission", object.object.write_permission)?;
    Ok(result)
}

/// Push a bodyless actor command into the sink (respecting the command cap). Unlike
/// [`push_command`] there is no payload to size-check.
fn push_actor_command(lua: &Lua, command: OutboundCommand) -> mlua::Result<()> {
    if let Some(mut sink) = lua.app_data_mut::<CommandSink>() {
        if sink.commands.len() >= MAX_OUTBOUND_COMMANDS {
            sink.overflowed = true;
        } else {
            sink.commands.push(command);
        }
    }
    Ok(())
}

fn lua_vector3(value: Table) -> mlua::Result<[f32; 3]> {
    let vector = [value.get(1)?, value.get(2)?, value.get(3)?];
    if vector.into_iter().all(f32::is_finite) {
        Ok(vector)
    } else {
        Err(mlua::Error::RuntimeError(
            "vector coordinates must be finite".into(),
        ))
    }
}

fn physics_options_from_lua(opts: Table) -> mlua::Result<PhysicsOptions> {
    let mut config = PhysicsConfig::default();
    config.gravity = opts
        .get::<Option<f32>>("gravity")?
        .unwrap_or(config.gravity);
    config.buoyancy = opts
        .get::<Option<f32>>("buoyancy")?
        .unwrap_or(config.buoyancy);
    config.drag = opts.get::<Option<f32>>("drag")?.unwrap_or(config.drag);
    config.max_speed = opts
        .get::<Option<f32>>("max_speed")?
        .unwrap_or(config.max_speed);
    let (default_radius, default_height) = match config.shape {
        Shape::Capsule { radius, height } => (radius, height),
        Shape::Aabb { half_extents } => (half_extents[0], half_extents[1] * 2.0),
    };
    let radius = opts.get::<Option<f32>>("radius")?.unwrap_or(default_radius);
    let height = opts.get::<Option<f32>>("height")?.unwrap_or(default_height);
    config.shape = match opts.get::<Option<String>>("shape")?.as_deref() {
        None | Some("capsule") => Shape::Capsule { radius, height },
        Some("aabb") => Shape::Aabb {
            half_extents: [radius, height * 0.5, radius],
        },
        Some(shape) => {
            return Err(mlua::Error::RuntimeError(format!(
                "unsupported physics shape '{shape}' (expected 'capsule' or 'aabb')"
            )));
        }
    };
    Ok(PhysicsOptions {
        enabled: opts.get::<Option<bool>>("enabled")?.unwrap_or(true),
        config,
    })
}

/// Advance each Lua-declared patrol through a Detour corridor. The path query is
/// server-side; only ordinary `MoveActor` commands reach the gateway/snapshot path.
fn advance_patrols(lua: &Lua, dt: f32) -> mlua::Result<()> {
    let Some(maps) = lua
        .app_data_ref::<MapCatalogHandle>()
        .map(|maps| Arc::clone(&maps.0))
    else {
        return Ok(());
    };
    let Some(mut patrols) = lua.app_data_mut::<NpcPatrols>() else {
        return Ok(());
    };
    for patrol in &mut patrols.0 {
        let target = patrol.waypoints[patrol.next_waypoint];
        let path = match maps.find_path(&patrol.map, patrol.position, target) {
            Ok(Some(path)) => path,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(map = %patrol.map, error = ?error, "NPC patrol path query failed");
                continue;
            }
        };
        let next = path.first().copied().unwrap_or(target);
        let delta = [
            next[0] - patrol.position[0],
            next[1] - patrol.position[1],
            next[2] - patrol.position[2],
        ];
        let distance = (delta[0] * delta[0] + delta[1] * delta[1] + delta[2] * delta[2]).sqrt();
        if distance <= 1.0 {
            patrol.position = next;
            if (target[0] - next[0]).abs() <= 1.0
                && (target[1] - next[1]).abs() <= 1.0
                && (target[2] - next[2]).abs() <= 1.0
            {
                patrol.next_waypoint = (patrol.next_waypoint + 1) % patrol.waypoints.len();
            }
            continue;
        }
        let step = (patrol.speed * dt).min(distance);
        let factor = step / distance;
        patrol.position = [
            patrol.position[0] + delta[0] * factor,
            patrol.position[1] + delta[1] * factor,
            patrol.position[2] + delta[2] * factor,
        ];
        push_actor_command(
            lua,
            OutboundCommand::MoveActor {
                object_id: patrol.object_id,
                position: patrol.position,
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [
                    delta[0] / distance * patrol.speed,
                    delta[1] / distance * patrol.speed,
                    delta[2] / distance * patrol.speed,
                ],
            },
        )?;
    }
    Ok(())
}

/// Validate `body`, build a command via `make`, and push it into the sink.
///
/// Borrows the command sink only for the push and drops the guard immediately,
/// so no app-data borrow is ever held across further Lua execution.
fn push_command(
    lua: &Lua,
    body: &mlua::String,
    make: impl FnOnce(Vec<u8>) -> OutboundCommand,
) -> mlua::Result<()> {
    let bytes = body.as_bytes();
    if bytes.len() > MAX_OUTBOUND_BODY_BYTES {
        return Err(mlua::Error::RuntimeError(format!(
            "outbound body too large: {} bytes (max {MAX_OUTBOUND_BODY_BYTES})",
            bytes.len()
        )));
    }
    let command = make(bytes.to_vec());
    if let Some(mut sink) = lua.app_data_mut::<CommandSink>() {
        let would_be_bytes = sink.total_bytes.saturating_add(bytes.len());
        if sink.commands.len() >= MAX_OUTBOUND_COMMANDS || would_be_bytes > MAX_TOTAL_OUTBOUND_BYTES
        {
            sink.overflowed = true;
        } else {
            sink.total_bytes = would_be_bytes;
            sink.commands.push(command);
        }
    }
    Ok(())
}

/// Install a scoped `require` that loads multi-file game logic from within the
/// module root (the `scripts_dir`), without exposing `io`/`os`/`package`.
///
/// `require("systems.combat")` resolves to `<root>/systems/combat.lua` (dotted
/// segments -> subdirectories). A module runs once per VM and its returned value
/// is cached (a re-`require` returns the cached value); cycles are detected and
/// rejected. Paths that would escape the root (`..`, absolute, separators,
/// empty segments) are refused, and no `package.path`/C-loader surface exists.
/// When there is no module root (an in-memory runtime), `require` is still
/// installed but errors on use, so the failure is explicit rather than a `nil`
/// global.
fn install_require(lua: &Lua, module_root: Option<&Path>) -> mlua::Result<()> {
    // Per-VM module cache + in-flight set (reset with the whole VM on reload).
    lua.set_named_registry_value(MODULES_KEY, lua.create_table()?)?;
    lua.set_named_registry_value(MODULES_LOADING_KEY, lua.create_table()?)?;

    let root = module_root.map(Path::to_path_buf);
    let require = lua.create_function(move |lua, name: mlua::String| {
        let name = name.to_str()?.to_string();
        let modules: Table = lua.named_registry_value(MODULES_KEY)?;

        // Cache hit: return the already-loaded module value.
        if let Some(cached) = modules.get::<Option<mlua::Value>>(name.clone())? {
            return Ok(cached);
        }

        let Some(root) = root.as_deref() else {
            return Err(mlua::Error::RuntimeError(format!(
                "require(\"{name}\") is unavailable: this runtime has no script directory"
            )));
        };

        // Cycle guard: a module mid-load must not require itself (transitively).
        let loading: Table = lua.named_registry_value(MODULES_LOADING_KEY)?;
        if loading.get::<Option<bool>>(name.clone())?.unwrap_or(false) {
            return Err(mlua::Error::RuntimeError(format!(
                "cyclic require detected while loading module \"{name}\""
            )));
        }

        let path = resolve_module_path(root, &name).map_err(|reason| {
            mlua::Error::RuntimeError(format!("require(\"{name}\"): {reason}"))
        })?;
        let source = std::fs::read_to_string(&path).map_err(|_| {
            mlua::Error::RuntimeError(format!("require(\"{name}\"): module not found"))
        })?;

        loading.set(name.clone(), true)?;
        // The module body runs under the caller's already-armed deadline; `eval`
        // returns the module's `return`ed value (or nil for a bare chunk).
        let result = lua
            .load(&source)
            .set_name(format!("@{name}"))
            .eval::<mlua::Value>();
        loading.set(name.clone(), mlua::Value::Nil)?;
        let value = result?;

        // Cache the returned value; a module that returns nothing caches `true`
        // (standard `require` convention) so it is not re-run on the next call.
        let cached = if value == mlua::Value::Nil {
            mlua::Value::Boolean(true)
        } else {
            value
        };
        modules.set(name, cached.clone())?;
        Ok(cached)
    })?;
    lua.globals().set("require", require)?;
    Ok(())
}

/// Resolve a dotted module name to a `.lua` file within `root`, or return a
/// short reason string if the name is malformed or would escape the root.
///
/// Rules (all failures reject rather than silently clamp): non-empty; each
/// dot-separated segment is non-empty and made only of `[A-Za-z0-9_]` (so `..`,
/// absolute paths, path separators, and empty segments are all refused). The
/// resolved path is confirmed to stay under `root`.
fn resolve_module_path(root: &Path, name: &str) -> Result<PathBuf, String> {
    if name.is_empty() {
        return Err("empty module name".to_string());
    }
    // A non-empty name always yields >=1 segment; an empty segment (from a
    // leading/trailing/double dot) is rejected below.
    let mut path = root.to_path_buf();
    for segment in name.split('.') {
        if segment.is_empty() {
            return Err("module name has an empty path segment".to_string());
        }
        if !segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err("module name may only contain letters, digits, '_', and '.'".to_string());
        }
        path.push(segment);
    }
    path.set_extension("lua");

    // Defense in depth: even though the character allowlist forbids `..` and
    // separators, confirm the resolved file stays under the (canonical) root.
    if let Ok(canon_root) = root.canonicalize()
        && let Ok(canon_path) = path.canonicalize()
        && !canon_path.starts_with(&canon_root)
    {
        return Err("module path escapes the script directory".to_string());
    }
    Ok(path)
}

/// Install the instruction-count hook that enforces the per-invocation deadline.
fn install_deadline_hook(lua: &Lua) -> mlua::Result<()> {
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(HOOK_INSTRUCTION_INTERVAL),
        |lua, _debug| {
            if let Some(deadline) = lua.app_data_ref::<Deadline>()
                && let Some(at) = deadline.0
                && Instant::now() >= at
            {
                return Err(mlua::Error::RuntimeError(
                    "handler exceeded its time budget".to_string(),
                ));
            }
            Ok(VmState::Continue)
        },
    );
    Ok(())
}

/// Build a fresh, fully-wired Lua VM from `source`, labelled `source_label`.
///
/// Creates the restricted-stdlib state, installs the command sink, deadline
/// app-data, host API, and deadline hook, then executes `source` to run its
/// registrations. Any step failing yields a [`Runtime`](ErrorCategory::Runtime)
/// error and leaves nothing behind (the VM is local until returned), which is
/// what makes [`LuaRuntime::reload`] failure-safe. Shared by initial load and
/// hot-reload.
#[derive(Clone)]
struct LuaCapabilityPolicies {
    outbound_http: OutboundHttpPolicy,
    http_endpoints: RuntimeHttpEndpointPolicy,
    event_bus_handle: RuntimeEventBusHandle,
    shared_cache_handle: RuntimeSharedCacheHandle,
}

impl Default for LuaCapabilityPolicies {
    fn default() -> Self {
        Self {
            outbound_http: OutboundHttpPolicy::default(),
            http_endpoints: RuntimeHttpEndpointPolicy::default(),
            event_bus_handle: disabled_runtime_event_bus_handle(),
            shared_cache_handle: disabled_runtime_shared_cache_handle(),
        }
    }
}

fn build_lua(
    source: &str,
    source_label: &str,
    load_budget: Duration,
    module_root: Option<&Path>,
    static_data: StaticDataCatalog,
    execution_mode: LuaExecutionMode,
    capability_policies: LuaCapabilityPolicies,
) -> AppResult<Lua> {
    let lua = Lua::new_with(script_stdlib(execution_mode), LuaOptions::default())
        .map_err(|e| script_error("failed to initialize Lua state", &e))?;
    lua.set_app_data(CommandSink::default());
    lua.set_app_data(Deadline(None));
    lua.set_app_data(InvocationMode::Normal);
    lua.set_app_data(NpcIdCounter(NPC_ID_BASE));
    lua.set_app_data(NpcPatrols::default());
    lua.set_app_data(Arc::clone(&capability_policies.event_bus_handle));
    lua.set_app_data(Arc::clone(&capability_policies.shared_cache_handle));
    let text_policy = TextPolicyCatalog::new(static_data.clone());
    install_host_api(
        &lua,
        source_label,
        static_data.clone(),
        text_policy.clone(),
        execution_mode,
        capability_policies.outbound_http,
        capability_policies.http_endpoints,
    )
    .map_err(|e| script_error("failed to install host API", &e))?;
    if execution_mode == LuaExecutionMode::Sandboxed {
        install_require(&lua, module_root)
            .map_err(|e| script_error("failed to install require", &e))?;
        install_deadline_hook(&lua)
            .map_err(|e| script_error("failed to install deadline hook", &e))?;
    }
    // Arm the deadline around the top-level exec so an infinite loop in the
    // script body (outside any handler) is aborted instead of hanging the
    // loader/watcher thread. Cleared afterwards; per-invocation handlers arm
    // their own deadline in `run_locked`.
    if execution_mode == LuaExecutionMode::Sandboxed {
        set_deadline(&lua, Some(Instant::now() + load_budget));
    }
    let exec = lua.load(source).set_name(source_label.to_string()).exec();
    if execution_mode == LuaExecutionMode::Sandboxed {
        set_deadline(&lua, None);
    }
    exec.map_err(|e| script_error(&format!("failed to load {source_label}"), &e))?;
    // From now on a cache miss is denied, rather than opening a file from a
    // message or tick handler. Successfully initialized values remain cache
    // hits and are converted to Lua tables without I/O.
    static_data.seal();
    text_policy.seal();
    Ok(lua)
}

/// Whether a freshly-built VM registered at least one handler of any kind.
///
/// Used to reject a hot-reload of an empty or handlerless script (an accidental
/// truncation, an editor's transient zero-byte save, or a stray empty file):
/// such a swap would silently leave the node with no handlers, so the reload is
/// rejected and the previous script keeps serving. An error probing the registry
/// is treated as "no handlers" so a broken VM is never swapped in.
fn has_any_handler(lua: &Lua) -> bool {
    let has_message = lua
        .named_registry_value::<Table>(HANDLERS_KEY)
        .map(|handlers| {
            handlers
                .pairs::<mlua::Value, mlua::Value>()
                .next()
                .is_some()
        })
        .unwrap_or(false);
    if has_message {
        return true;
    }
    let has_rpc = lua
        .named_registry_value::<Table>(RPC_HANDLERS_KEY)
        .map(|handlers| {
            handlers
                .pairs::<mlua::Value, mlua::Value>()
                .next()
                .is_some()
        })
        .unwrap_or(false);
    if has_rpc {
        return true;
    }
    let has_http_endpoint = lua
        .named_registry_value::<Table>(HTTP_ENDPOINT_HANDLERS_KEY)
        .map(|handlers| {
            handlers
                .pairs::<mlua::Value, mlua::Value>()
                .next()
                .is_some()
        })
        .unwrap_or(false);
    if has_http_endpoint {
        return true;
    }
    let has_event_subscriber = lua
        .named_registry_value::<Table>(EVENT_HANDLERS_KEY)
        .map(|handlers| {
            handlers
                .pairs::<mlua::Value, mlua::Value>()
                .next()
                .is_some()
        })
        .unwrap_or(false);
    if has_event_subscriber {
        return true;
    }
    [ON_JOIN_KEY, ON_LEAVE_KEY, ON_TICK_KEY].iter().any(|key| {
        lua.named_registry_value::<Option<Function>>(key)
            .map(|h| h.is_some())
            .unwrap_or(false)
    })
}

/// Read a script file, mapping I/O failure to a [`Runtime`](ErrorCategory::Runtime)
/// error naming the path.
fn read_script(path: &Path) -> AppResult<String> {
    std::fs::read_to_string(path).map_err(|e| {
        AppError::new(
            ErrorCategory::Runtime,
            format!("cannot read game script: {}", path.display()),
        )
        .with_detail(e.to_string())
    })
}

/// Map an `mlua` error to a [`Runtime`](ErrorCategory::Runtime) [`AppError`].
fn script_error(context: &str, err: &mlua::Error) -> AppError {
    AppError::new(ErrorCategory::Runtime, context.to_string()).with_detail(err.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    use super::*;

    const RELAY_SCRIPT: &str = r#"
        citadel.on_message(1, function(ctx, body)
            citadel.broadcast(2, string.pack(">I8", ctx.sender) .. body, true)
        end)
    "#;

    fn runtime(src: &str) -> LuaRuntime {
        LuaRuntime::from_source(src, "test", DEFAULT_DEADLINE_MS).expect("runtime loads")
    }

    #[test]
    fn telemetry_slice_builder_enables_match_scoped_lua_reports() {
        let slices = Arc::new(TelemetrySliceService::new(
            Arc::new(
                crate::authoritative_decision_telemetry::AuthoritativeDecisionRecorder::new(16),
            ),
            crate::authoritative_telemetry_slices::TelemetrySlicePolicy::default(),
        ));
        let runtime = runtime(
            r#"
                citadel.on_message(1, function()
                    citadel.telemetry.begin()
                    citadel.telemetry.mark("match.round")
                    citadel.telemetry.finish()
                end)
            "#,
        )
        .with_telemetry_slices(Arc::clone(&slices));

        assert!(
            runtime
                .dispatch_in_room(7, Some("user-7"), 42, 1, b"")
                .is_empty()
        );
        let reports = slices.list_closed(1);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].context_kind, "match");
        assert_eq!(reports[0].close_reason, "finished");
        assert_eq!(reports[0].marker_total, 1);
    }

    // ---- authoritative bridge: citadel.on_input ----

    #[derive(Default)]
    struct RecordingBridgeSink(Mutex<Vec<ScriptCommandBatch>>);

    impl BridgeCommandSink for RecordingBridgeSink {
        fn deliver_command_batch(&self, answer: ScriptCommandBatch) {
            self.0.lock().unwrap().push(answer);
        }
    }

    fn input_event(event_id: u64, object_id: u32) -> NormalizedEvent {
        NormalizedEvent {
            event_id,
            participant: 1001,
            user_id: None,
            payload: NormalizedPayload::TransformInput {
                object_id,
                ownership_epoch: 1,
                input_seq: 1,
                sim_tick: 1,
                dt: 0.016,
                move_velocity: [1.0, 0.0, 0.0],
                payload: Vec::new(),
                fire: None,
            },
        }
    }

    fn batch_with(events: Vec<NormalizedEvent>) -> NormalizedEventBatch {
        let mut batch = NormalizedEventBatch::new(5, 42, 9, 100, 1);
        batch.events = events;
        batch
    }

    #[test]
    fn on_input_nil_return_accepts_the_event() {
        let rt = runtime("citadel.on_input(function(e) return nil end)");
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(answer.input_outcomes.len(), 1);
        assert_eq!(answer.input_outcomes[0].event_id, 1);
        assert_eq!(answer.input_outcomes[0].decision, Decision::Accept);
        // Fencing echoes the delivered batch exactly.
        assert_eq!(answer.generation, 5);
        assert_eq!(answer.match_id, 42);
        assert_eq!(answer.batch_id, 1);
    }

    #[test]
    fn on_input_reject_table_carries_reason_and_reply() {
        let rt = runtime(
            r#"citadel.on_input(function(e)
                return { decision = "reject", reason_code = 7, reply = "no" }
            end)"#,
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(
            answer.input_outcomes[0].decision,
            Decision::Reject { reason_code: 7 }
        );
        assert_eq!(
            answer.input_outcomes[0].reply.as_deref(),
            Some(b"no".as_ref())
        );
    }

    #[test]
    fn on_input_correct_returns_a_transform_correction() {
        let rt = runtime(
            r#"citadel.on_input(function(e)
                return {
                    decision = "correct",
                    transform = {
                        position = { x = 1, y = 2, z = 3 },
                        rotation = { x = 0, y = 0, z = 0, w = 1 },
                        velocity = { x = 4, y = 5, z = 6 },
                    },
                }
            end)"#,
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        match &answer.input_outcomes[0].decision {
            Decision::Correct {
                correction: Correction::Transform(t),
            } => {
                assert_eq!(t.position, [1.0, 2.0, 3.0]);
                assert_eq!(t.rotation, [0.0, 0.0, 0.0, 1.0]);
                assert_eq!(t.velocity, [4.0, 5.0, 6.0]);
            }
            other => panic!("expected a transform correction, got {other:?}"),
        }
    }

    #[test]
    fn on_input_broadcast_maps_to_a_match_broadcast_command() {
        let rt = runtime(
            r#"citadel.on_input(function(e)
                citadel.broadcast(100, "hi", true)
                return nil
            end)"#,
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(answer.commands.len(), 1);
        assert_eq!(
            answer.commands[0],
            crate::runtime::ScriptCommand::BroadcastMatch {
                kind: 100,
                body: b"hi".to_vec(),
                unreliable: true,
                exclude: None,
            }
        );
    }

    #[test]
    fn no_on_input_handler_fails_closed_with_no_answer() {
        // A script that never registered on_input is not a bridge script: a
        // delivered batch produces no answer, so nothing materializes.
        let rt = runtime("citadel.on_message(1, function(ctx, body) end)");
        let batch = batch_with(vec![input_event(1, 7)]);
        assert!(rt.evaluate_event_batch(&batch).is_none());
    }

    #[test]
    fn on_input_error_fails_the_whole_batch_closed() {
        let rt = runtime(r#"citadel.on_input(function(e) error("boom") end)"#);
        let batch = batch_with(vec![input_event(1, 7), input_event(2, 8)]);
        assert!(
            rt.evaluate_event_batch(&batch).is_none(),
            "a script fault must not produce a partial or default-accept answer"
        );
    }

    #[test]
    fn deliver_event_batch_reaches_the_attached_sink() {
        let rt = runtime("citadel.on_input(function(e) return nil end)");
        let sink = Arc::new(RecordingBridgeSink::default());
        rt.attach_bridge_sink(Arc::downgrade(&sink) as Weak<dyn BridgeCommandSink>);
        rt.deliver_event_batch(batch_with(vec![input_event(1, 7)]));
        let got = sink.0.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].batch_id, 1);
        assert_eq!(got[0].input_outcomes[0].decision, Decision::Accept);
    }

    #[test]
    fn deliver_event_batch_without_a_handler_delivers_nothing() {
        let rt = runtime("citadel.on_message(1, function(ctx, body) end)");
        let sink = Arc::new(RecordingBridgeSink::default());
        rt.attach_bridge_sink(Arc::downgrade(&sink) as Weak<dyn BridgeCommandSink>);
        rt.deliver_event_batch(batch_with(vec![input_event(1, 7)]));
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[test]
    fn on_input_can_call_rewind_query() {
        // The fire/hit host API is callable from on_input (owner decision 1).
        // Without a hub attached it returns an empty hit list, proving the API
        // is wired and the batch still answers.
        let rt = runtime(
            r#"citadel.on_input(function(e)
                local r = citadel.rewind_query(e.participant, {x=0,y=0,z=0}, {x=1,y=0,z=0}, e.tick)
                assert(type(r.hits) == "table")
                return nil
            end)"#,
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(answer.input_outcomes[0].decision, Decision::Accept);
    }

    #[test]
    fn trusted_lua_http_policy_is_applied_at_the_host_boundary() {
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)
                .expect("disabled static-data catalog");
        let error = build_lua(
            "citadel.http.start('https://api.example.test/')",
            "disabled-http.lua",
            Duration::from_millis(50),
            None,
            static_data,
            LuaExecutionMode::Trusted,
            LuaCapabilityPolicies {
                outbound_http: OutboundHttpPolicy {
                    enabled: false,
                    ..OutboundHttpPolicy::default()
                },
                http_endpoints: RuntimeHttpEndpointPolicy::default(),
                event_bus_handle: disabled_runtime_event_bus_handle(),
                shared_cache_handle: disabled_runtime_shared_cache_handle(),
            },
        )
        .expect_err("disabled HTTP policy must reject Lua async start at the Rust host boundary");
        let detail = error.log_detail().unwrap_or_default();
        assert!(
            detail.contains("capability_disabled"),
            "the script-visible failure must stay a stable redacted error code: {detail}"
        );
        assert!(
            !detail.contains("api.example.test"),
            "host-policy errors must not expose request target details: {detail}"
        );
    }

    #[test]
    fn async_http_state_contract_is_stable_for_lua() {
        let lua = Lua::new();
        for state in [
            OutboundHttpRequestState::Pending,
            OutboundHttpRequestState::Timeout,
            OutboundHttpRequestState::Cancelled,
        ] {
            let table = outbound_http_state_to_lua(&lua, state).expect("state maps to Lua");
            assert!(matches!(
                table.get::<Option<mlua::String>>("error_code"),
                Ok(None)
            ));
        }
        assert_eq!(
            outbound_http_state_to_lua(&lua, OutboundHttpRequestState::Timeout)
                .expect("timeout maps")
                .get::<String>("state")
                .expect("timeout state"),
            "timeout"
        );
        assert_eq!(
            outbound_http_state_to_lua(&lua, OutboundHttpRequestState::Cancelled)
                .expect("cancelled maps")
                .get::<String>("state")
                .expect("cancelled state"),
            "cancelled"
        );
        let success = outbound_http_state_to_lua(
            &lua,
            OutboundHttpRequestState::Success(
                crate::runtime::outbound_http::OutboundHttpResponse {
                    status: 201,
                    body: vec![0, 255],
                },
            ),
        )
        .expect("success maps");
        assert_eq!(success.get::<u16>("status").expect("status"), 201);
        assert_eq!(
            success
                .get::<mlua::String>("body")
                .expect("body")
                .as_bytes(),
            &[0, 255]
        );
        let error = outbound_http_state_to_lua(
            &lua,
            OutboundHttpRequestState::Error("request_failed".to_string()),
        )
        .expect("error maps");
        assert_eq!(error.get::<String>("state").expect("error state"), "error");
        assert_eq!(
            error.get::<String>("error_code").expect("error code"),
            "request_failed"
        );
    }

    #[test]
    fn trusted_lua_async_http_rejects_oversized_body_before_network_io() {
        let dir = TempDir::new("lua-async-http-oversized");
        dir.write_main(&format!(
            r#"
            citadel.on_message(1, function()
                local ok, err = pcall(function()
                    citadel.http.start("https://example.test/", {{ body = string.rep("x", {}) }})
                end)
                citadel.broadcast(2, (not ok and string.find(tostring(err), "request_too_large", 1, true)) and "request_too_large" or "unexpected")
            end)
            "#,
            crate::runtime::outbound_http::MAX_OUTBOUND_HTTP_REQUEST_BYTES + 1
        ));
        let runtime = LuaRuntime::load_with_static_data_and_mode_and_http_policy(
            &dir.0,
            DEFAULT_DEADLINE_MS,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            LuaExecutionMode::Trusted,
            OutboundHttpPolicy::default(),
        )
        .expect("load file runtime")
        .expect("main.lua present");
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"request_too_large".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_http_handles_are_pending_until_a_delayed_lua_response_arrives() {
        let listener = TcpListener::bind(("localhost", 0)).expect("bind test HTTP server");
        let port = listener.local_addr().expect("test server address").port();
        let (served, served_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept test HTTP request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).expect("read test HTTP request");
            served.send(()).expect("notify request read");
            release_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("release delayed response");
            stream
                .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 3\r\nConnection: close\r\n\r\n\0\xffA")
                .expect("write test HTTP response");
        });
        let dir = TempDir::new("lua-async-http-delayed");
        dir.write_main(&format!(
            r#"
            local handle = nil
            citadel.on_message(1, function()
                handle = citadel.http.start("http://localhost:{port}/", {{ body = "request" }})
                citadel.broadcast(9, type(handle))
            end)
            citadel.on_message(2, function()
                local result = citadel.http.poll(handle)
                if result.state == "success" then
                    citadel.broadcast(9, "success:" .. result.status .. ":" .. #result.body)
                else
                    citadel.broadcast(9, result.state)
                end
            end)
            "#
        ));
        let runtime = LuaRuntime::load_with_static_data_and_mode_and_http_policy(
            &dir.0,
            DEFAULT_DEADLINE_MS,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            LuaExecutionMode::Trusted,
            OutboundHttpPolicy {
                allowed_hosts: vec!["localhost".to_owned()],
                allowed_ports: vec![port],
                allow_private_networks: true,
                ..OutboundHttpPolicy::default()
            },
        )
        .expect("load file runtime")
        .expect("main.lua present");
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"number".to_vec(),
                unreliable: false,
            }]
        );
        served_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("async request reaches test server");
        assert_eq!(
            runtime.dispatch(1, None, 2, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"pending".to_vec(),
                unreliable: false,
            }],
            "poll returns immediately while the server holds its response"
        );
        release.send(()).expect("release response");
        let response = (0..100)
            .map(|_| {
                let commands = runtime.dispatch(1, None, 2, b"");
                let OutboundCommand::Broadcast { body, .. } = &commands[0] else {
                    panic!("expected HTTP state broadcast");
                };
                if body.starts_with(b"success:") {
                    Some(body.clone())
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                    None
                }
            })
            .find_map(std::convert::identity)
            .expect("completed async response");
        assert_eq!(response, b"success:201:3");
    }

    #[test]
    fn trusted_lua_http_policy_survives_reload() {
        let dir = TempDir::new("lua-http-policy");
        let source = r#"
            citadel.on_message(1, function()
                local failures = {}
                for _, operation in ipairs({
                    function() citadel.http.fetch("https://api.example.test/") end,
                    function() citadel.http.start("https://api.example.test/") end,
                    function() citadel.http.poll(7) end,
                    function() citadel.http.cancel(7) end,
                }) do
                    local ok, err = pcall(operation)
                    if not ok then
                        table.insert(failures, string.find(tostring(err), "capability_disabled", 1, true) and "capability_disabled" or "unexpected")
                    end
                end
                citadel.broadcast(2, table.concat(failures, ","))
            end)
        "#;
        dir.write_main(source);
        let runtime = LuaRuntime::load_with_static_data_and_mode_and_http_policy(
            &dir.0,
            DEFAULT_DEADLINE_MS,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            LuaExecutionMode::Trusted,
            OutboundHttpPolicy {
                enabled: false,
                ..OutboundHttpPolicy::default()
            },
        )
        .expect("load file runtime")
        .expect("main.lua present");
        let assert_disabled = |runtime: &LuaRuntime| {
            assert_eq!(
                runtime.dispatch(1, None, 1, b""),
                vec![OutboundCommand::Broadcast {
                    kind: 2,
                    body: b"capability_disabled,capability_disabled,capability_disabled,capability_disabled".to_vec(),
                    unreliable: false,
                }]
            );
        };
        assert_disabled(&runtime);
        dir.write_main(source);
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
        assert_disabled(&runtime);
    }

    #[test]
    fn custom_http_endpoint_registration_dispatch_and_reload_are_atomic() {
        let dir = TempDir::new("custom-http-endpoint");
        let source = r#"
            citadel.http.register("POST", "/echo", { auth = "session" }, function(request)
                return {
                    status = 201,
                    headers = { ["content-type"] = "text/plain" },
                    body = request.user_id .. ":" .. request.body,
                }
            end)
        "#;
        dir.write_main(source);
        let policy = RuntimeHttpEndpointPolicy {
            enabled: true,
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            max_requests_per_minute: 10,
        };
        let runtime = LuaRuntime::load_with_static_data_and_mode_and_capability_policies(
            &dir.0,
            100,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            LuaExecutionMode::Sandboxed,
            OutboundHttpPolicy::default(),
            policy,
        )
        .expect("load endpoint runtime")
        .expect("main.lua present");
        assert_eq!(
            runtime.http_endpoints(),
            vec![
                RuntimeHttpEndpoint::new(
                    RuntimeHttpMethod::Post,
                    "/echo",
                    RuntimeHttpAuth::Session,
                )
                .expect("valid endpoint")
            ]
        );
        assert_eq!(
            runtime.call_http_endpoint(RuntimeHttpRequest {
                method: RuntimeHttpMethod::Post,
                path: "/echo".to_string(),
                headers: Default::default(),
                body: b"hello".to_vec(),
                user_id: Some("user-7".to_string()),
            }),
            RuntimeHttpOutcome::Response(RuntimeHttpResponse {
                status: 201,
                headers: [("content-type".to_string(), "text/plain".to_string())]
                    .into_iter()
                    .collect(),
                body: b"user-7:hello".to_vec(),
            })
        );

        dir.write_main(
            r#"
                local callback = citadel.http.register("GET", "/next", function(request)
                    return { body = "next" }
                end)
                assert(type(callback) == "function")
            "#,
        );
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
        assert_eq!(
            runtime.http_endpoints(),
            vec![
                RuntimeHttpEndpoint::new(RuntimeHttpMethod::Get, "/next", RuntimeHttpAuth::Public)
                    .expect("valid endpoint")
            ],
            "an endpoint-only runtime reloads atomically"
        );
        assert_eq!(
            runtime.call_http_endpoint(RuntimeHttpRequest {
                method: RuntimeHttpMethod::Get,
                path: "/next".to_string(),
                headers: Default::default(),
                body: Vec::new(),
                user_id: None,
            }),
            RuntimeHttpOutcome::Response(RuntimeHttpResponse {
                status: 200,
                headers: Default::default(),
                body: b"next".to_vec(),
            })
        );

        dir.write_main(
            r#"
                citadel.http.register("GET", "/invalid", { auth = 1 }, function() return {} end)
            "#,
        );
        assert_eq!(runtime.reload(), ReloadOutcome::Rejected);
        assert_eq!(
            runtime.http_endpoints().len(),
            1,
            "old registry remains live"
        );

        dir.write_main(
            r#"
                citadel.http.register("GET", "/dup", { auth = "public" }, function() return {} end)
                citadel.http.register("GET", "/dup", { auth = "session" }, function() return {} end)
            "#,
        );
        assert_eq!(runtime.reload(), ReloadOutcome::Rejected);
        assert_eq!(
            runtime.http_endpoints().len(),
            1,
            "old registry remains live"
        );
    }

    #[test]
    fn runtime_events_are_local_fifo_and_non_reentrant() {
        let bus = Arc::new(RuntimeEventBus::new(
            crate::runtime::RuntimeEventPolicy {
                enabled: true,
                queue_capacity: 8,
                max_event_bytes: 64,
                max_events_per_minute: 10,
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = runtime(
            r#"
                citadel.events.subscribe("match", "first", function(event)
                    citadel.broadcast(7, event.payload)
                    assert(citadel.events.emit("match", "second", "two"))
                end)
                citadel.events.subscribe("match", "second", function(event)
                    citadel.broadcast(8, event.payload)
                end)
                citadel.on_message(1, function()
                    assert(citadel.events.emit("match", "first", "one"))
                end)
            "#,
        )
        .with_event_bus(bus);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"one".to_vec(),
                unreliable: false,
            }],
            "the event emitted by a subscriber waits for the next outer invocation"
        );
        assert_eq!(
            runtime.tick(Duration::ZERO, Duration::from_millis(100)),
            vec![OutboundCommand::Broadcast {
                kind: 8,
                body: b"two".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn shared_cache_is_namespaced_and_supports_cas() {
        let cache = Arc::new(RuntimeSharedCache::new(
            crate::runtime::RuntimeSharedCachePolicy {
                enabled: true,
                max_entries: 8,
                max_value_bytes: 64,
                max_ttl: Duration::from_secs(1),
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = runtime(
            r#"
                citadel.on_message(1, function()
                    local first = citadel.cache.set("match.one", "score", "one", 1000)
                    assert(citadel.cache.get("match.two", "score") == nil)
                    local second = citadel.cache.cas("match.one", "score", first.version, "two", 1000)
                    assert(second ~= nil)
                    assert(citadel.cache.cas("match.one", "score", first.version, "bad", 1000) == nil)
                    citadel.broadcast(7, citadel.cache.get("match.one", "score").value)
                end)
            "#,
        )
        .with_shared_cache(cache);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"two".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn shared_cache_survives_successful_hot_reload() {
        let dir = TempDir::new("shared-cache-reload");
        dir.write_main(
            r#"
                citadel.on_message(1, function()
                    citadel.cache.set("match", "round", "persisted", 1000)
                    citadel.broadcast(7, "v1")
                end)
            "#,
        );
        let cache = Arc::new(RuntimeSharedCache::new(
            crate::runtime::RuntimeSharedCachePolicy {
                enabled: true,
                max_entries: 8,
                max_value_bytes: 64,
                max_ttl: Duration::from_secs(1),
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present")
            .with_shared_cache(cache);
        assert!(!runtime.dispatch(1, None, 1, b"").is_empty());
        dir.write_main(
            r#"
                citadel.on_message(1, function()
                    local entry = citadel.cache.get("match", "round")
                    citadel.broadcast(7, entry and entry.value or "missing")
                end)
            "#,
        );
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"persisted".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn shared_cache_is_unavailable_in_realtime_interceptors() {
        let cache = Arc::new(RuntimeSharedCache::new(
            crate::runtime::RuntimeSharedCachePolicy {
                enabled: true,
                max_entries: 8,
                max_value_bytes: 64,
                max_ttl: Duration::from_secs(1),
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = runtime(
            r#"
                local before_blocked = false
                local after_blocked = false
                citadel.before_realtime(function()
                    before_blocked = not pcall(citadel.cache.set, "match", "key", "bad", 1000)
                    return false
                end)
                citadel.after_realtime(function()
                    after_blocked = not pcall(citadel.cache.get, "match", "key")
                end)
                citadel.on_message(1, function()
                    assert(citadel.cache.get("match", "key") == nil)
                    citadel.broadcast(7, before_blocked and after_blocked and "ok" or "failed")
                end)
            "#,
        )
        .with_shared_cache(cache);
        assert_eq!(
            runtime.before_realtime(1, None, None, 1, b""),
            RealtimeInterception::Drop
        );
        runtime.after_realtime(
            1,
            None,
            None,
            1,
            b"",
            RealtimeAfterOutcome {
                dropped: true,
                delivered: 0,
            },
        );
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"ok".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn realtime_interceptors_veto_and_observe_without_command_side_effects() {
        let rt = runtime(
            r#"
            local seen = "unset"
            citadel.before_realtime(function(ctx, body)
                citadel.broadcast(99, "must-discard")
                return false
            end)
            citadel.after_realtime(function(ctx, body)
                citadel.broadcast(98, "must-discard")
                seen = (ctx.dropped and "drop" or "pass") .. ":" .. tostring(ctx.delivered) .. ":" .. ctx.body
            end)
            citadel.on_message(8, function(ctx, body)
                citadel.broadcast(9, seen)
            end)
        "#,
        );

        assert_eq!(
            rt.before_realtime(7, Some("user-7"), Some(42), 1, b"blocked"),
            RealtimeInterception::Drop
        );
        // The before-hook broadcast is discarded: only the message handler emits.
        assert_eq!(
            rt.dispatch(7, Some("user-7"), 8, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"unset".to_vec(),
                unreliable: false,
            }]
        );

        rt.after_realtime(
            7,
            Some("user-7"),
            Some(42),
            1,
            b"blocked",
            RealtimeAfterOutcome {
                dropped: true,
                delivered: 0,
            },
        );
        // The after-hook broadcast is likewise discarded, but its immutable
        // context is visible to script state used by the next ordinary handler.
        assert_eq!(
            rt.dispatch(7, Some("user-7"), 8, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"drop:0:blocked".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn invalid_before_realtime_result_fails_closed_without_wedging_dispatch() {
        let rt = runtime(
            r#"
            citadel.before_realtime(function(ctx, body) return "invalid" end)
            citadel.on_message(2, function(ctx, body) citadel.broadcast(3, body) end)
        "#,
        );
        assert_eq!(
            rt.before_realtime(1, None, None, 2, b"x"),
            RealtimeInterception::Drop
        );
        assert_eq!(
            rt.dispatch(1, None, 2, b"still-works"),
            vec![OutboundCommand::Broadcast {
                kind: 3,
                body: b"still-works".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn realtime_interceptors_reject_domain_storage_side_effects() {
        let rt = runtime(
            r#"
            local seen = "unset"
            citadel.before_realtime(function(ctx, body)
                citadel.register_storage_index_filter("profiles_by_score", function(candidate)
                    seen = "filter-mutated"
                    return true
                end)
                citadel.storage_write("user", "profiles", "before", "{}")
                return true
            end)
            citadel.after_realtime(function(ctx, body)
                citadel.register_storage_index_filter("profiles_by_score", function(candidate)
                    seen = "filter-mutated"
                    return true
                end)
                citadel.storage_write("user", "profiles", "after", "{}")
                seen = "mutated"
            end)
            citadel.on_message(8, function(ctx, body)
                citadel.storage_write("user", "profiles", "normal", "{\"score\":1}")
                citadel.broadcast(9, seen)
            end)
        "#,
        )
        .with_domain_host(friends_host());

        assert_eq!(
            rt.before_realtime(7, Some("user"), None, 1, b"input"),
            RealtimeInterception::Drop,
            "a rejected storage write makes the before hook fail closed"
        );
        rt.after_realtime(
            7,
            Some("user"),
            None,
            1,
            b"input",
            RealtimeAfterOutcome {
                dropped: false,
                delivered: 0,
            },
        );
        assert_eq!(
            rt.dispatch(7, Some("user"), 8, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"unset".to_vec(),
                unreliable: false,
            }],
            "the after hook stops at the rejected direct host call"
        );
    }

    #[test]
    fn match_message_context_and_tick_are_room_scoped() {
        let rt = runtime(
            r#"
            citadel.on_message(9, function(ctx, body)
                citadel.broadcast(10, tostring(ctx.room_id) .. ":" .. body)
            end)
            citadel.on_tick(function(dt, room_id)
                citadel.broadcast(11, tostring(room_id))
            end)
        "#,
        );
        assert_eq!(
            rt.dispatch_in_room(7, Some("user-7"), 42, 9, b"payload"),
            vec![OutboundCommand::Broadcast {
                kind: 10,
                body: b"42:payload".to_vec(),
                unreliable: false,
            }]
        );
        assert_eq!(
            rt.tick_in_room(42, Duration::from_millis(16), Duration::from_millis(100)),
            vec![OutboundCommand::Broadcast {
                kind: 11,
                body: b"42".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn host_api_surface_matches_manifest_lua() {
        let registered: std::collections::HashSet<&'static str> = [
            "on_message",
            "on_join",
            "on_leave",
            "on_tick",
            "on_leaderboard_reset",
            "on_rpc",
            "on_room_create",
            "on_room_join",
            "before_realtime",
            "after_realtime",
            "on_input",
            "broadcast",
            "send",
            "spawn_actor",
            "move_actor",
            "despawn_actor",
            "set_physics",
            "apply_impulse",
            "set_move_intent",
            "physics_state",
            "rewind_query",
            "map_info",
            "map_names",
            "find_path",
            "raycast",
            "sphere_overlap",
            "ground_height",
            "log",
            "static_data.load_json",
            "static_data.load_csv",
            "text_policy.load_json",
            "text_policy.scan",
            "text_policy.sanitize",
            "friends.add",
            "friends.remove",
            "friends.block",
            "friends.list",
            "notifications.send",
            "notifications.list",
            "notifications.mark_read",
            "groups.call",
            "leaderboards.call",
            "tournaments.call",
            "chat.call",
            "wallet.call",
            "storage.read",
            "storage.write",
            "storage.delete",
            "storage.index_query",
            "storage.register_index_filter",
            "http.fetch",
            "http.start",
            "http.poll",
            "http.cancel",
            "http.register",
            "events.emit",
            "events.subscribe",
            "cache.get",
            "cache.set",
            "cache.delete",
            "cache.cas",
        ]
        .into_iter()
        .collect();
        let shipped: std::collections::HashSet<&'static str> = crate::runtime::HOST_API_SURFACE
            .iter()
            .filter(|entry| entry.status == crate::runtime::HostApiStatus::Shipped)
            .map(|entry| entry.name)
            .collect();
        assert_eq!(registered, shipped);
    }

    fn friends_host() -> Arc<dyn DomainHost> {
        use crate::repository::{
            InMemoryBackend, InMemoryChatRepository, InMemoryFriendsRepository,
            InMemoryGroupsRepository, InMemoryLeaderboardsRepository, InMemoryStorageRepository,
            InMemoryTournamentsRepository, InMemoryWalletRepository,
        };
        use crate::runtime::ServiceDomainHost;
        use crate::services::{
            ChatChannelAuthorizer, ChatService, FriendsService, GroupsService, LeaderboardService,
            PlayerNotificationService, TournamentDiscoveryService, WalletService,
        };
        use crate::storage::{
            Collection, StorageIndexDefinition, StorageIndexField, StorageIndexName,
        };
        let friends = Arc::new(FriendsService::new(Arc::new(
            InMemoryFriendsRepository::new(),
        )));
        let groups = Arc::new(GroupsService::new(
            Arc::new(InMemoryGroupsRepository::new()),
        ));
        let chat = Arc::new(ChatService::new(Arc::new(InMemoryChatRepository::new())));
        let authorizer = Arc::new(ChatChannelAuthorizer::new(
            Arc::clone(&friends),
            Arc::clone(&groups),
        ));
        Arc::new(
            ServiceDomainHost::new(friends, Arc::new(InMemoryStorageRepository::new()))
                .with_storage_indexes(vec![
                    StorageIndexDefinition::new(
                        StorageIndexName::new("profiles_by_score").expect("index name"),
                        Collection::new("profiles").expect("collection"),
                        None,
                        vec![StorageIndexField::new("score").expect("field")],
                    )
                    .expect("index definition"),
                ])
                .with_player_notifications(Arc::new(PlayerNotificationService::new(Arc::new(
                    InMemoryBackend::new(),
                ))))
                .with_groups(groups)
                .with_leaderboards(Arc::new(LeaderboardService::new(Arc::new(
                    InMemoryLeaderboardsRepository::new(),
                ))))
                .with_tournaments(Arc::new(TournamentDiscoveryService::new(Arc::new(
                    InMemoryTournamentsRepository::new(),
                ))))
                .with_chat(chat)
                .with_chat_authorizer(authorizer)
                .with_wallet(Arc::new(WalletService::new(Arc::new(
                    InMemoryWalletRepository::new(),
                )))),
        )
    }

    /// Names of the shipped `Domain`-category host functions from the canonical
    /// manifest. The behavioral gate below must exercise exactly this set, so a
    /// newly-shipped `Domain` function cannot pass parity as a name-claim stub.
    fn shipped_domain_host_api_names() -> std::collections::HashSet<&'static str> {
        crate::runtime::HOST_API_SURFACE
            .iter()
            .filter(|entry| {
                entry.category == crate::runtime::HostApiCategory::Domain
                    && entry.status == crate::runtime::HostApiStatus::Shipped
            })
            .map(|entry| entry.name)
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn domain_host_api_behaviorally_covers_manifest() {
        // Exercise EVERY shipped Domain function against a real (in-memory) host
        // and assert real effects — a name-claim stub throws here and fails.
        let rt = runtime(
            r#"
            citadel.on_rpc("exercise", function(ctx, body)
                local u, o = "prober", "target"
                local added = citadel.friends_add(u, o)
                citadel.friends_add(o, u)
                local chat = citadel.chat_call(u, "send", '{"target":{"kind":"direct","other_user_id":"target"},"content":"hi"}')
                local n1 = #citadel.friends_list(u)
                citadel.friends_block(u, o)
                local blocked = citadel.friends_list(u)[1].state
                local removed = citadel.friends_remove(u, o)
                local n2 = #citadel.friends_list(u)
                local notification = citadel.notifications_send(u, 7, "hello", "{}", "server", "probe")
                local page = citadel.notifications_list(u)
                local read = citadel.notifications_mark_read(u, { notification.id })
                local group = citadel.groups_call(u, "create", '{"name":"probers"}')
                local boards = citadel.leaderboards_call(u, "list", "{}")
                local tournaments = citadel.tournaments_call(u, "list", "{}")
                local wallet = citadel.wallet_call(u, "balances", "{}")
                return added .. "|" .. n1 .. "|" .. blocked .. "|" .. tostring(removed) .. "|" .. n2 .. "|" .. #page.items .. "|" .. #read .. "|" .. tostring(string.find(group, "probers") ~= nil) .. "|" .. tostring(boards == "[]") .. "|" .. tostring(tournaments == "[]") .. "|" .. tostring(string.find(chat, '"id":1') ~= nil) .. "|" .. tostring(wallet == "{}")
            end)
        "#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("prober"), "exercise", b"") else {
            panic!("domain host functions must be wired, not stubbed");
        };
        assert_eq!(
            reply,
            b"invited_sent|1|blocked|true|0|1|1|true|true|true|true|true"
        );

        // Forcing function: the script above must cover the whole shipped Domain
        // surface. Update both when a new Domain function ships.
        let exercised: std::collections::HashSet<&str> = [
            "friends.add",
            "friends.remove",
            "friends.block",
            "friends.list",
            "notifications.send",
            "notifications.list",
            "notifications.mark_read",
            "groups.call",
            "leaderboards.call",
            "tournaments.call",
            "chat.call",
            "wallet.call",
        ]
        .into_iter()
        .collect();
        assert_eq!(
            exercised,
            shipped_domain_host_api_names(),
            "every shipped Domain host-API function needs a behavioral smoke here"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn friends_host_api_errors_without_a_domain_host() {
        // A runtime built without `with_domain_host` exposes the functions but
        // they error cleanly rather than panicking.
        let rt = runtime(
            r#"
            citadel.on_rpc("befriend", function(ctx, body)
                citadel.friends_add(ctx.user_id, body)
                return "unreachable"
            end)
        "#,
        );
        let RpcOutcome::Err(msg) = rt.call_rpc(1, Some("alice"), "befriend", b"bob") else {
            panic!("expected error");
        };
        assert!(msg.contains("friends host not available") || !msg.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_host_api_writes_reads_and_isolates_a_conflict() {
        let rt = runtime(
            r#"
            citadel.on_rpc("save", function(ctx, body)
                local created = citadel.storage_write(ctx.user_id, "saves", "slot", "{\"level\":1}", "", 2, 1)
                local read = citadel.storage_read(ctx.user_id, "saves", "slot")
                local ok = pcall(function()
                    citadel.storage_write(ctx.user_id, "saves", "slot", "{\"level\":2}", "")
                end)
                return read.value_json .. "|" .. created.version .. "|" .. tostring(ok)
            end)
        "#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("alice"), "save", b"") else {
            panic!("storage host must return a reply");
        };
        let text = String::from_utf8(reply).expect("utf8 reply");
        assert!(text.starts_with("{\"level\":1}|"), "got: {text}");
        assert!(text.ends_with("|false"), "got: {text}");
        // The VM remains usable after the caught storage conflict.
        assert!(matches!(
            rt.call_rpc(1, Some("alice"), "save", b""),
            RpcOutcome::Err(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_index_query_is_wired_to_the_lua_host() {
        let rt = runtime(
            r#"
            citadel.on_rpc("search", function(ctx, body)
                citadel.register_storage_index_filter("profiles_by_score", function(candidate)
                    if candidate.key == "boom" then error("filter failed") end
                    return candidate.key == "main"
                end)
                citadel.storage_write(ctx.user_id, "profiles", "skip", "{\"score\":7}")
                citadel.storage_write(ctx.user_id, "profiles", "main", "{\"score\":7}")
                local ok = pcall(function()
                    citadel.storage_write(ctx.user_id, "profiles", "boom", "{\"score\":7}")
                end)
                local missing = citadel.storage_read(ctx.user_id, "profiles", "boom") == nil
                local found = citadel.storage_index_query("profiles_by_score", "{\"score\":7}", 10)
                return tostring(ok) .. "|" .. tostring(missing) .. "|" .. tostring(#found) .. "|" .. found[1].user_id .. "|" .. found[1].key
            end)
        "#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("alice"), "search", b"") else {
            panic!("storage index host must return a reply");
        };
        assert_eq!(reply, b"false|true|1|alice|main");
    }

    #[test]
    fn on_room_create_returns_label_spec_from_table() {
        let rt = runtime(
            r#"
            citadel.on_room_create(function(ctx, params)
                return { map = "ForestArena", mode = "ffa", max_players = 8, open = true }
            end)
        "#,
        );
        let spec = rt.call_room_create(1, None, b"ignored").expect("spec");
        assert_eq!(spec.map, "ForestArena");
        assert_eq!(spec.mode, "ffa");
        assert_eq!(spec.max_players, 8);
        assert!(spec.open);
    }

    #[test]
    fn on_room_create_accepts_bare_string_map() {
        let rt = runtime(r#"citadel.on_room_create(function(ctx, params) return "Lobby" end)"#);
        assert_eq!(
            rt.call_room_create(1, None, b"").expect("spec").map,
            "Lobby"
        );
    }

    #[test]
    fn call_room_create_without_handler_is_none() {
        let rt = runtime("-- no room handler");
        assert!(rt.call_room_create(1, None, b"x").is_none());
    }

    #[test]
    fn on_room_join_gate_admits_and_rejects() {
        let rt = runtime(r#"citadel.on_room_join(function(ctx, room_id) return room_id == 1 end)"#);
        assert!(rt.call_room_join(1, None, 1), "handler admits room 1");
        assert!(!rt.call_room_join(1, None, 2), "handler rejects room 2");
    }

    #[test]
    fn call_room_join_without_handler_admits() {
        let rt = runtime("-- none");
        assert!(rt.call_room_join(1, None, 5), "default is admit");
    }

    #[test]
    fn on_room_join_error_fails_closed() {
        let rt = runtime(r#"citadel.on_room_join(function(ctx, id) error("boom") end)"#);
        assert!(!rt.call_room_join(1, None, 1), "a handler error rejects");
    }

    #[test]
    fn spawn_actor_returns_high_id_and_queues_actor_commands() {
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                local id = citadel.spawn_actor{ archetype = 7, x = 10, y = 0, z = 0 }
                citadel.move_actor(id, 20, 0, 0, 5, 0, 0)
                citadel.despawn_actor(id)
            end)
        "#,
        );
        let cmds = rt.dispatch(1, None, 1, b"go");
        assert_eq!(cmds.len(), 3, "spawn + move + despawn queued");
        let id = match &cmds[0] {
            OutboundCommand::SpawnActor {
                object_id,
                archetype,
                position,
            } => {
                assert!(*object_id >= NPC_ID_BASE, "npc id is in the high range");
                assert_eq!(*archetype, 7);
                assert_eq!(*position, [10.0, 0.0, 0.0]);
                *object_id
            }
            other => panic!("expected SpawnActor, got {other:?}"),
        };
        match &cmds[1] {
            OutboundCommand::MoveActor {
                object_id,
                position,
                velocity,
                ..
            } => {
                assert_eq!(*object_id, id, "move targets the spawned id");
                assert_eq!(*position, [20.0, 0.0, 0.0]);
                assert_eq!(*velocity, [5.0, 0.0, 0.0]);
            }
            other => panic!("expected MoveActor, got {other:?}"),
        }
        assert_eq!(cmds[2], OutboundCommand::DespawnActor { object_id: id });
    }

    #[test]
    fn map_info_returns_loaded_summary_and_nil_for_unknown_map() {
        use citadel_map::{CollisionMesh, MapFile, MapMetadata};
        let dir = TempDir::new("lua-map-info");
        let map = MapFile {
            metadata: MapMetadata {
                name: "Arena".into(),
                bounds_min: [-10.0, 0.0, -20.0],
                bounds_max: [10.0, 5.0, 20.0],
            },
            collision: CollisionMesh {
                vertices: vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]],
                triangles: vec![[0, 1, 2]],
            },
            navmesh: None,
        };
        std::fs::write(dir.0.join("Arena.map"), map.encode()).expect("write map");
        let maps = Arc::new(MapCatalog::load_dir(&dir.0));
        let rt = runtime(r#"
            citadel.on_rpc("map", function()
                local known = citadel.map_info("Arena")
                local missing = citadel.map_info("Missing")
                if known == nil or missing ~= nil then error("bad map lookup") end
                return string.format("%.0f|%.0f|%d|%d", known.bounds_min[1], known.bounds_max[3], known.vertex_count, known.triangle_count)
            end)
        "#).with_maps(maps);
        assert_eq!(
            rt.call_rpc(1, None, "map", b""),
            RpcOutcome::Ok(b"-10|20|3|1".to_vec())
        );
    }

    #[test]
    fn patrol_actor_uses_map_path_and_emits_replicated_move_command() {
        use citadel_map::{CollisionMesh, MapFile, MapMetadata};
        let dir = TempDir::new("lua-patrol-nav");
        let map = MapFile {
            metadata: MapMetadata {
                name: "Arena".into(),
                bounds_min: [0.0; 3],
                bounds_max: [10.0, 0.0, 10.0],
            },
            collision: CollisionMesh {
                vertices: vec![
                    [0.0, 0.0, 0.0],
                    [10.0, 0.0, 0.0],
                    [10.0, 0.0, 10.0],
                    [0.0, 0.0, 10.0],
                ],
                triangles: vec![[0, 1, 2], [0, 2, 3]],
            },
            navmesh: None,
        };
        std::fs::write(dir.0.join("Arena.map"), map.encode()).expect("write map");
        let rt = runtime(r#"
            citadel.on_message(1, function() end)
            citadel.spawn_actor{ archetype = 1, x = 1, y = 0, z = 1, ai = "patrol", map = "Arena", speed = 10, waypoints = {{x = 9, y = 0, z = 9}} }
        "#).with_maps(Arc::new(MapCatalog::load_dir(&dir.0)));
        assert!(
            rt.has_tick_handler(),
            "patrols drive the server tick without a Lua on_tick hook"
        );
        let commands = rt.tick(Duration::from_millis(100), Duration::from_millis(100));
        assert!(commands.iter().any(|command| matches!(
            command,
            OutboundCommand::MoveActor {
                object_id: 0x4000_0000,
                ..
            }
        )));
    }

    #[test]
    fn load_missing_dir_falls_back_to_none() {
        let dir = std::path::Path::new("/nonexistent/citadel/game-does-not-exist");
        let rt = LuaRuntime::load(dir, DEFAULT_DEADLINE_MS).expect("missing dir is not an error");
        assert!(
            rt.is_none(),
            "absent main.lua => fall back to built-in relay"
        );
    }

    #[test]
    fn syntax_error_is_a_runtime_error() {
        let err = LuaRuntime::from_source("this is not lua ==", "bad", DEFAULT_DEADLINE_MS)
            .expect_err("syntax error must fail to load");
        assert_eq!(err.category(), ErrorCategory::Runtime);
    }

    #[test]
    fn dispatch_relay_handler_produces_tagged_broadcast() {
        let rt = runtime(RELAY_SCRIPT);
        let commands = rt.dispatch(42, None, 1, &[9, 8, 7]);
        assert_eq!(commands.len(), 1);
        let OutboundCommand::Broadcast {
            kind,
            body,
            unreliable,
        } = &commands[0]
        else {
            unreachable!("expected a broadcast command");
        };
        assert_eq!(*kind, 2);
        assert!(*unreliable);
        // string.pack(">I8", 42) is the 8-byte big-endian sender id.
        assert_eq!(&body[..8], &42u64.to_be_bytes());
        assert_eq!(&body[8..], &[9, 8, 7]);
    }

    #[test]
    fn dispatch_unknown_kind_is_a_noop() {
        let rt = runtime(RELAY_SCRIPT);
        let commands = rt.dispatch(1, None, 9999, b"x");
        assert!(commands.is_empty(), "no handler for kind => no commands");
    }

    #[test]
    fn send_command_is_captured_with_target() {
        let rt = runtime(
            r#"
            citadel.on_message(7, function(ctx, body)
                citadel.send(123, 8, body)
            end)
        "#,
        );
        let commands = rt.dispatch(1, None, 7, b"hi");
        assert_eq!(
            commands,
            vec![OutboundCommand::Send {
                session: 123,
                kind: 8,
                body: b"hi".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn handler_error_is_isolated_and_discards_side_effects() {
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "partial", false)
                error("boom")
            end)
        "#,
        );
        // Must not panic; a prior broadcast in the same invocation is discarded.
        let commands = rt.dispatch(1, None, 1, b"x");
        assert!(commands.is_empty(), "errored handler yields no commands");
        // The runtime remains usable for subsequent, valid dispatches.
        let again = rt.dispatch(1, None, 1, b"y");
        assert!(again.is_empty());
    }

    #[test]
    fn hung_handler_is_aborted_by_the_deadline() {
        let rt = LuaRuntime::from_source(
            r#"
            citadel.on_message(1, function(ctx, body)
                while true do end
            end)
        "#,
            "hang",
            50,
        )
        .expect("loads");
        let start = Instant::now();
        let commands = rt.dispatch(1, None, 1, b"x");
        let elapsed = start.elapsed();
        assert!(commands.is_empty(), "aborted handler yields no commands");
        assert!(
            elapsed < Duration::from_secs(5),
            "deadline must abort the loop promptly, took {elapsed:?}"
        );
    }

    #[test]
    fn oversized_outbound_body_is_rejected_without_crashing() {
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.rep("a", 2 * 1024 * 1024), false)
            end)
        "#,
        );
        // The host API errors on the oversized body; the handler error is
        // isolated and produces no commands.
        let commands = rt.dispatch(1, None, 1, b"x");
        assert!(commands.is_empty());
    }

    #[test]
    fn coroutine_library_is_unavailable_so_the_deadline_cannot_be_bypassed() {
        // `coroutine` is omitted from the script stdlib because coroutines would
        // run without the main-thread deadline hook. A handler that reaches for
        // it hits a nil global, errors, and is isolated (no hang, no commands).
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                coroutine.resume(coroutine.create(function() while true do end end))
            end)
        "#,
        );
        let start = Instant::now();
        let commands = rt.dispatch(1, None, 1, b"x");
        assert!(commands.is_empty(), "coroutine bypass attempt is isolated");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "must not hang: coroutine is unavailable"
        );
    }

    #[test]
    fn debug_library_is_unavailable() {
        // `debug` is omitted so a script cannot `debug.sethook(nil)` to remove
        // the deadline hook.
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                debug.sethook()
            end)
        "#,
        );
        let commands = rt.dispatch(1, None, 1, b"x");
        assert!(commands.is_empty(), "debug access is isolated");
    }

    #[test]
    fn sandboxed_mode_never_exposes_machine_libraries() {
        // This is a load-time assertion so any accidental stdlib expansion is a
        // hard regression rather than merely an isolated handler error.
        let rt = runtime(
            r#"
            assert(os == nil)
            assert(io == nil)
            assert(package == nil)
            assert(coroutine == nil)
            assert(debug == nil)
            assert(citadel.http == nil)
            citadel.on_message(1, function(ctx, body) end)
        "#,
        );
        assert!(rt.dispatch(1, None, 1, b"x").is_empty());
    }

    #[test]
    fn aggregate_outbound_bytes_are_capped() {
        // Enqueue many max-size bodies; the per-invocation aggregate cap
        // (1 MiB) bounds how many are actually retained.
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                local chunk = string.rep("a", 64 * 1024)
                for _ = 1, 100 do
                    citadel.broadcast(2, chunk, false)
                end
            end)
        "#,
        );
        let commands = rt.dispatch(1, None, 1, b"x");
        let total: usize = commands
            .iter()
            .map(|c| match c {
                OutboundCommand::Broadcast { body, .. } | OutboundCommand::Send { body, .. } => {
                    body.len()
                }
                // Actor commands (spawn/move/despawn) carry no payload body.
                _ => 0,
            })
            .sum();
        assert!(
            total <= MAX_TOTAL_OUTBOUND_BYTES,
            "aggregate outbound bytes {total} exceeded cap {MAX_TOTAL_OUTBOUND_BYTES}"
        );
        assert!(!commands.is_empty(), "some commands still get through");
    }

    #[test]
    fn on_join_and_on_leave_fire_and_can_broadcast() {
        let rt = runtime(
            r#"
            citadel.on_join(function(ctx)
                citadel.broadcast(10, string.pack(">I8", ctx.sender), false)
            end)
            citadel.on_leave(function(ctx)
                citadel.broadcast(11, string.pack(">I8", ctx.sender), false)
            end)
        "#,
        );
        let join = rt.dispatch_lifecycle(LifecycleHook::Join, 55, None);
        assert_eq!(
            join,
            vec![OutboundCommand::Broadcast {
                kind: 10,
                body: 55u64.to_be_bytes().to_vec(),
                unreliable: false,
            }]
        );
        let leave = rt.dispatch_lifecycle(LifecycleHook::Leave, 55, None);
        assert_eq!(
            leave,
            vec![OutboundCommand::Broadcast {
                kind: 11,
                body: 55u64.to_be_bytes().to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn missing_lifecycle_handler_is_a_noop() {
        // A script with no on_join/on_leave: dispatching the hook is harmless.
        let rt = runtime(RELAY_SCRIPT);
        assert!(
            rt.dispatch_lifecycle(LifecycleHook::Join, 1, None)
                .is_empty()
        );
        assert!(
            rt.dispatch_lifecycle(LifecycleHook::Leave, 1, None)
                .is_empty()
        );
        assert!(!rt.has_tick_handler());
    }

    #[test]
    fn on_tick_receives_dt_and_can_broadcast() {
        let rt = runtime(
            r#"
            citadel.on_tick(function(dt)
                -- Encode dt (seconds) so the test can read it back.
                citadel.broadcast(20, string.pack(">d", dt), true)
            end)
        "#,
        );
        assert!(rt.has_tick_handler());
        let commands = rt.tick(Duration::from_millis(50), Duration::from_millis(100));
        let OutboundCommand::Broadcast { kind, body, .. } = &commands[0] else {
            unreachable!("expected a broadcast command");
        };
        assert_eq!(*kind, 20);
        let dt = f64::from_be_bytes(body[..8].try_into().expect("8 bytes"));
        assert!((dt - 0.05).abs() < 1e-9, "dt should be 0.05s, got {dt}");
    }

    #[test]
    fn tick_without_handler_is_a_noop() {
        let rt = runtime(RELAY_SCRIPT);
        assert!(
            rt.tick(Duration::from_millis(16), Duration::from_millis(50))
                .is_empty()
        );
    }

    #[test]
    fn log_runs_without_error_and_does_not_block_side_effects() {
        // `citadel.log` emits a tracing event and returns; a handler that logs
        // then broadcasts still produces its command.
        let rt = runtime(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.log("hello from lua")
                citadel.log("noisy", "warn")
                citadel.broadcast(2, "ok", false)
            end)
        "#,
        );
        let commands = rt.dispatch(1, None, 1, b"x");
        assert_eq!(
            commands,
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"ok".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn erroring_lifecycle_handler_is_isolated() {
        let rt = runtime(
            r#"
            citadel.on_join(function(ctx)
                citadel.broadcast(10, "partial", false)
                error("boom")
            end)
        "#,
        );
        // No commands (partial side effects discarded), no panic, still usable.
        assert!(
            rt.dispatch_lifecycle(LifecycleHook::Join, 1, None)
                .is_empty()
        );
        assert!(rt.dispatch(1, None, 9999, b"y").is_empty());
    }

    #[test]
    fn hung_tick_is_aborted_by_its_own_deadline() {
        let rt = LuaRuntime::from_source(
            r#"
            citadel.on_tick(function(dt)
                while true do end
            end)
        "#,
            "hang-tick",
            100,
        )
        .expect("loads");
        let start = Instant::now();
        let commands = rt.tick(Duration::from_millis(16), Duration::from_millis(50));
        let elapsed = start.elapsed();
        assert!(commands.is_empty(), "aborted tick yields no commands");
        assert!(
            elapsed < Duration::from_secs(5),
            "tick deadline must abort the loop promptly, took {elapsed:?}"
        );
        // The runtime is not wedged: a subsequent dispatch still works.
        let rt2 = runtime(RELAY_SCRIPT);
        assert!(!rt2.dispatch(1, None, 1, b"x").is_empty());
    }

    /// A throwaway temp directory for the file-backed reload tests.
    ///
    /// Avoids a `tempfile` dev-dependency: a unique per-test subdir under the
    /// system temp dir, removed on drop.
    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "citadel-reload-{}-{}-{tag}-{n}",
                std::process::id(),
                Instant::now().elapsed().as_nanos()
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn main_lua(&self) -> std::path::PathBuf {
            self.0.join("main.lua")
        }

        fn write_main(&self, src: &str) {
            std::fs::write(self.main_lua(), src).expect("write main.lua");
        }

        /// Write a script at a path relative to the temp dir, creating parent
        /// directories (e.g. `write("systems/combat.lua", ...)`).
        fn write(&self, rel: &str, src: &str) {
            let path = self.0.join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create module subdir");
            }
            std::fs::write(path, src).expect("write module file");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn from_source_runtime_is_not_reloadable() {
        let rt = runtime(RELAY_SCRIPT);
        assert!(!rt.is_reloadable());
        assert_eq!(rt.reload(), ReloadOutcome::NotReloadable);
    }

    #[test]
    fn editing_the_script_reloads_new_handlers() {
        let dir = TempDir::new("edit");
        // Initial script handles kind 1 -> broadcast kind 2.
        dir.write_main(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "v1", false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("main.lua present");
        assert!(rt.is_reloadable());
        let before = rt.dispatch(1, None, 1, b"");
        assert_eq!(
            before,
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"v1".to_vec(),
                unreliable: false,
            }]
        );

        // Edit: same kind now broadcasts a different kind/body, and a brand-new
        // handler for kind 3 appears.
        dir.write_main(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(9, "v2", true)
            end)
            citadel.on_message(3, function(ctx, body)
                citadel.send(1, 4, "new")
            end)
        "#,
        );
        assert_eq!(rt.reload(), ReloadOutcome::Reloaded);

        // The reloaded handler takes effect.
        let after = rt.dispatch(1, None, 1, b"");
        assert_eq!(
            after,
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"v2".to_vec(),
                unreliable: true,
            }]
        );
        // The newly-added handler for kind 3 is live too.
        let added = rt.dispatch(1, None, 3, b"");
        assert_eq!(
            added,
            vec![OutboundCommand::Send {
                session: 1,
                kind: 4,
                body: b"new".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn broken_edit_is_rejected_and_previous_script_keeps_serving() {
        let dir = TempDir::new("broken");
        dir.write_main(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "good", false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");

        // A syntax-broken edit must be rejected, not swapped in.
        dir.write_main("this is not lua ==");
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);

        // The previously-loaded, valid handler is still serving.
        let commands = rt.dispatch(1, None, 1, b"");
        assert_eq!(
            commands,
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"good".to_vec(),
                unreliable: false,
            }]
        );

        // A handler that errors at registration time (runs at load) is also
        // rejected, keeping the good script.
        dir.write_main(r#"error("registration blows up")"#);
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        assert!(!rt.dispatch(1, None, 1, b"").is_empty(), "still serving v1");
    }

    #[test]
    fn reload_resets_in_vm_global_state() {
        let dir = TempDir::new("state");
        // A counter kept in a Lua global; each dispatch increments and reports it.
        dir.write_main(
            r#"
            count = 0
            citadel.on_message(1, function(ctx, body)
                count = count + 1
                citadel.broadcast(2, string.pack(">I8", count), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        let first = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &first[0] else {
            unreachable!();
        };
        assert_eq!(&body[..8], &1u64.to_be_bytes(), "count starts at 1");
        // Advance the global.
        let _ = rt.dispatch(1, None, 1, b"");
        // Reload the same source: the fresh VM resets globals, so count restarts.
        dir.write_main(
            r#"
            count = 0
            citadel.on_message(1, function(ctx, body)
                count = count + 1
                citadel.broadcast(2, string.pack(">I8", count), false)
            end)
        "#,
        );
        assert_eq!(rt.reload(), ReloadOutcome::Reloaded);
        let after = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &after[0] else {
            unreachable!();
        };
        assert_eq!(
            &body[..8],
            &1u64.to_be_bytes(),
            "in-VM globals reset on reload"
        );
    }

    #[test]
    fn handlerless_reload_is_rejected_and_keeps_serving() {
        let dir = TempDir::new("handlerless");
        dir.write_main(RELAY_SCRIPT);
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        // An empty file is valid Lua but registers nothing: must be rejected so
        // the node never silently loses its handlers (truncate-race guard).
        dir.write_main("");
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        assert!(
            !rt.dispatch(42, None, 1, &[1, 2, 3]).is_empty(),
            "previous handler still serves after an empty save"
        );
        // A comment-only (valid, handlerless) save is likewise rejected.
        dir.write_main("-- just a comment, no handlers\n");
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        assert!(!rt.dispatch(42, None, 1, &[1]).is_empty(), "still serving");
    }

    #[test]
    fn top_level_infinite_loop_is_bounded_at_load() {
        // A script whose top-level body loops forever must be aborted by the load
        // deadline rather than hang the loader thread. Use a short budget so the
        // test is fast.
        let start = Instant::now();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)
                .expect("disabled static-data catalog");
        let err = build_lua(
            "while true do end",
            "loop",
            Duration::from_millis(50),
            None,
            static_data,
            LuaExecutionMode::Sandboxed,
            LuaCapabilityPolicies::default(),
        )
        .expect_err("top-level infinite loop must be aborted, not hang");
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "load deadline must abort promptly, took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn trusted_mode_exposes_machine_libraries() {
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)
                .expect("disabled static-data catalog");
        build_lua(
            "assert(type(os) == 'table'); assert(type(io) == 'table'); assert(type(package) == 'table'); assert(type(coroutine) == 'table'); assert(debug == nil); assert(type(citadel.http) == 'table'); assert(type(citadel.http.fetch) == 'function'); assert(type(citadel.http.start) == 'function'); assert(type(citadel.http.poll) == 'function'); assert(type(citadel.http.cancel) == 'function')",
            "trusted-libraries",
            Duration::from_millis(50),
            None,
            static_data,
            LuaExecutionMode::Trusted,
            LuaCapabilityPolicies::default(),
        )
        .expect("trusted Lua mode exposes machine libraries but not unsafe native loading");
    }

    #[test]
    fn reload_of_deleted_file_is_rejected_and_keeps_serving() {
        let dir = TempDir::new("deleted");
        dir.write_main(RELAY_SCRIPT);
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        std::fs::remove_file(dir.main_lua()).expect("remove main.lua");
        // A vanished file cannot be read: reject, keep the loaded script.
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        assert!(!rt.dispatch(42, None, 1, &[1]).is_empty(), "still serving");
    }

    const RPC_SCRIPT: &str = r#"
        citadel.on_rpc("ping", function(ctx, body)
            return "pong"
        end)
        citadel.on_rpc("echo", function(ctx, body)
            return body
        end)
        citadel.on_rpc("whoami", function(ctx, body)
            return string.pack(">I8", ctx.sender)
        end)
    "#;

    #[test]
    fn call_rpc_returns_handler_reply() {
        let rt = runtime(RPC_SCRIPT);
        assert_eq!(
            rt.call_rpc(1, None, "ping", b""),
            RpcOutcome::Ok(b"pong".to_vec())
        );
        // Binary-safe echo round-trips arbitrary bytes.
        assert_eq!(
            rt.call_rpc(1, None, "echo", &[0, 1, 2, 255]),
            RpcOutcome::Ok(vec![0, 1, 2, 255])
        );
    }

    #[test]
    fn call_rpc_exposes_sender_and_method_on_ctx() {
        let rt = runtime(RPC_SCRIPT);
        let RpcOutcome::Ok(reply) = rt.call_rpc(77, None, "whoami", b"") else {
            unreachable!("whoami replies ok");
        };
        assert_eq!(&reply[..8], &77u64.to_be_bytes());
    }

    #[test]
    fn call_rpc_unknown_method_is_a_generic_error() {
        let rt = runtime(RPC_SCRIPT);
        let RpcOutcome::Err(msg) = rt.call_rpc(1, None, "nope", b"") else {
            unreachable!("unknown method must error");
        };
        assert!(msg.contains("unknown RPC method"), "generic message: {msg}");
        assert!(msg.contains("nope"), "echoes the method name: {msg}");
    }

    #[test]
    fn call_rpc_handler_error_is_isolated_and_generic() {
        let rt = runtime(
            r#"
            citadel.on_rpc("boom", function(ctx, body)
                error("secret internal detail")
            end)
        "#,
        );
        let RpcOutcome::Err(msg) = rt.call_rpc(1, None, "boom", b"") else {
            unreachable!("erroring handler must error");
        };
        assert_eq!(msg, "RPC handler error", "no internal detail leaks");
        assert!(
            !msg.contains("secret"),
            "handler error text must not leak to the caller"
        );
        // The runtime remains usable after an isolated RPC error.
        let rt2 = runtime(RPC_SCRIPT);
        assert_eq!(
            rt2.call_rpc(1, None, "ping", b""),
            RpcOutcome::Ok(b"pong".to_vec())
        );
    }

    #[test]
    fn call_rpc_non_string_return_is_an_error() {
        let rt = runtime(
            r#"
            citadel.on_rpc("nilreply", function(ctx, body)
                return nil
            end)
        "#,
        );
        assert!(matches!(
            rt.call_rpc(1, None, "nilreply", b""),
            RpcOutcome::Err(_)
        ));
    }

    #[test]
    fn call_rpc_slow_handler_is_aborted_by_the_deadline() {
        let rt = LuaRuntime::from_source(
            r#"
            citadel.on_rpc("hang", function(ctx, body)
                while true do end
            end)
        "#,
            "rpc-hang",
            50,
        )
        .expect("loads");
        let start = Instant::now();
        let RpcOutcome::Err(msg) = rt.call_rpc(1, None, "hang", b"") else {
            unreachable!("hung handler must error");
        };
        assert_eq!(msg, "RPC handler timed out");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "deadline must abort the RPC handler promptly"
        );
    }

    #[test]
    fn call_rpc_side_effects_are_discarded() {
        // An RPC handler that also tries to broadcast: the broadcast is dropped
        // (RPC communicates only via its return value) and the reply still works.
        let rt = runtime(
            r#"
            citadel.on_rpc("noisy", function(ctx, body)
                citadel.broadcast(2, "leak", false)
                return "ok"
            end)
        "#,
        );
        assert_eq!(
            rt.call_rpc(1, None, "noisy", b""),
            RpcOutcome::Ok(b"ok".to_vec())
        );
        // A subsequent message dispatch is unaffected by the discarded command.
        assert!(rt.dispatch(1, None, 9999, b"").is_empty());
    }

    #[test]
    fn rpc_only_script_reloads_cleanly() {
        let dir = TempDir::new("rpc-only");
        dir.write_main(
            r#"
            citadel.on_rpc("ping", function(ctx, body) return "pong" end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        // An RPC-only script counts as having handlers, so a reload of an
        // equivalent script is accepted (not rejected as handlerless).
        dir.write_main(
            r#"
            citadel.on_rpc("ping", function(ctx, body) return "pong2" end)
        "#,
        );
        assert_eq!(rt.reload(), ReloadOutcome::Reloaded);
        assert_eq!(
            rt.call_rpc(1, None, "ping", b""),
            RpcOutcome::Ok(b"pong2".to_vec())
        );
    }

    #[test]
    fn ctx_exposes_sender_and_kind() {
        let rt = runtime(
            r#"
            citadel.on_message(5, function(ctx, body)
                citadel.broadcast(6, string.pack(">I8I8", ctx.sender, ctx.kind), false)
            end)
        "#,
        );
        let commands = rt.dispatch(77, None, 5, b"");
        let OutboundCommand::Broadcast { body, .. } = &commands[0] else {
            unreachable!("expected a broadcast command");
        };
        assert_eq!(&body[..8], &77u64.to_be_bytes());
        assert_eq!(&body[8..16], &5u64.to_be_bytes());
    }

    // ------------------------------- require ------------------------------- //

    #[test]
    fn require_loads_a_module_and_its_return_value() {
        let dir = TempDir::new("require");
        dir.write("config.lua", r#"return { start_hp = 100, name = "hero" }"#);
        dir.write_main(
            r#"
            local cfg = require("config")
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.pack(">I8", cfg.start_hp), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        let commands = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &commands[0] else {
            unreachable!("expected a broadcast");
        };
        assert_eq!(&body[..8], &100u64.to_be_bytes(), "module value is visible");
    }

    #[test]
    fn require_resolves_dotted_paths_to_subdirectories() {
        let dir = TempDir::new("require-subdir");
        dir.write(
            "systems/combat.lua",
            r#"return { damage = function(a, b) return a - b end }"#,
        );
        dir.write_main(
            r#"
            local combat = require("systems.combat")
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.pack(">I8", combat.damage(50, 8)), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        let commands = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &commands[0] else {
            unreachable!("expected a broadcast");
        };
        assert_eq!(
            &body[..8],
            &42u64.to_be_bytes(),
            "systems.combat.damage ran"
        );
    }

    #[test]
    fn require_caches_modules_so_the_body_runs_once() {
        let dir = TempDir::new("require-cache");
        // The module increments a global side-effect counter each time its body
        // runs; caching means that happens exactly once across many requires.
        dir.write(
            "counter.lua",
            r#"
            _G.__load_count = (_G.__load_count or 0) + 1
            return _G.__load_count
        "#,
        );
        dir.write_main(
            r#"
            local a = require("counter")
            local b = require("counter")
            local c = require("counter")
            citadel.on_message(1, function(ctx, body)
                -- a, b, c must all equal 1: the body ran once, value cached.
                citadel.broadcast(2, string.pack(">I8I8I8", a, b, c), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        let commands = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &commands[0] else {
            unreachable!("expected a broadcast");
        };
        assert_eq!(&body[0..8], &1u64.to_be_bytes());
        assert_eq!(&body[8..16], &1u64.to_be_bytes());
        assert_eq!(&body[16..24], &1u64.to_be_bytes());
    }

    #[test]
    fn require_rejects_paths_that_escape_the_script_directory() {
        let dir = TempDir::new("require-escape");
        // A secret file one level above the script root must be unreachable.
        std::fs::write(dir.0.join("secret.lua"), "return 'leaked'").ok();
        let sub = dir.0.join("game");
        std::fs::create_dir_all(&sub).expect("subdir");
        for attempt in [
            r#"require("..secret")"#,  // empty segment via ".."
            r#"require("../secret")"#, // path separator
            r#"require(".secret")"#,   // leading dot -> empty segment
            r#"require("secret.")"#,   // trailing dot -> empty segment
            r#"require("")"#,          // empty name
        ] {
            let src = format!("local x = {attempt}");
            let err = LuaRuntime::from_source_with_root(&src, "escape", DEFAULT_DEADLINE_MS, &sub)
                .expect_err("escaping/malformed require must fail at load");
            assert_eq!(err.category(), ErrorCategory::Runtime, "attempt: {attempt}");
        }
    }

    #[test]
    fn require_of_missing_module_is_a_load_error() {
        let dir = TempDir::new("require-missing");
        let err = LuaRuntime::from_source_with_root(
            r#"require("does_not_exist")"#,
            "missing",
            DEFAULT_DEADLINE_MS,
            &dir.0,
        )
        .expect_err("missing module must fail");
        assert_eq!(err.category(), ErrorCategory::Runtime);
    }

    #[test]
    fn require_rejects_cyclic_dependencies() {
        let dir = TempDir::new("require-cycle");
        dir.write("a.lua", r#"local b = require("b"); return {}"#);
        dir.write("b.lua", r#"local a = require("a"); return {}"#);
        let err = LuaRuntime::from_source_with_root(
            r#"require("a")"#,
            "cycle",
            DEFAULT_DEADLINE_MS,
            &dir.0,
        )
        .expect_err("a cyclic require chain must be rejected");
        assert_eq!(err.category(), ErrorCategory::Runtime);
    }

    #[test]
    fn a_broken_required_module_is_isolated_on_hot_reload() {
        let dir = TempDir::new("require-broken");
        dir.write("mod.lua", r#"return { v = 1 }"#);
        dir.write_main(
            r#"
            local m = require("mod")
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.pack(">I8", m.v), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        assert!(!rt.dispatch(1, None, 1, b"").is_empty(), "v1 serves");

        // Break the required module: the reload must be rejected (build fails
        // while running main.lua's top-level require), keeping the good VM.
        dir.write("mod.lua", "this is not lua ==");
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        let still = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &still[0] else {
            unreachable!("still serving v1");
        };
        assert_eq!(&body[..8], &1u64.to_be_bytes(), "previous module kept");
    }

    #[test]
    fn hot_reload_picks_up_a_changed_required_module() {
        let dir = TempDir::new("require-reload");
        dir.write("mod.lua", r#"return { v = 1 }"#);
        dir.write_main(
            r#"
            local m = require("mod")
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.pack(">I8", m.v), false)
            end)
        "#,
        );
        let rt = LuaRuntime::load(&dir.0, DEFAULT_DEADLINE_MS)
            .expect("loads")
            .expect("present");
        let first = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &first[0] else {
            unreachable!();
        };
        assert_eq!(&body[..8], &1u64.to_be_bytes());

        // Edit only the required module (not main.lua) and reload: the module
        // graph is re-resolved, so the new value is picked up.
        dir.write("mod.lua", r#"return { v = 99 }"#);
        assert_eq!(rt.reload(), ReloadOutcome::Reloaded);
        let after = rt.dispatch(1, None, 1, b"");
        let OutboundCommand::Broadcast { body, .. } = &after[0] else {
            unreachable!();
        };
        assert_eq!(&body[..8], &99u64.to_be_bytes(), "changed module reloaded");
    }

    #[test]
    fn require_without_a_module_root_errors_clearly() {
        // A `from_source` runtime has no script directory; require is present but
        // must fail explicitly rather than resolve or be a nil global.
        let err = LuaRuntime::from_source(r#"require("anything")"#, "no-root", DEFAULT_DEADLINE_MS)
            .expect_err("require with no module root must error");
        assert_eq!(err.category(), ErrorCategory::Runtime);
    }

    #[test]
    fn require_runs_under_the_deadline() {
        // A required module whose body loops forever is aborted by the load
        // deadline (armed around the top-level exec), not left to hang.
        let dir = TempDir::new("require-deadline");
        dir.write("slow.lua", "while true do end");
        let start = Instant::now();
        let err = LuaRuntime::from_source_with_root(
            r#"require("slow")"#,
            "slow-require",
            DEFAULT_DEADLINE_MS,
            &dir.0,
        )
        .expect_err("a hung required module must be aborted");
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert!(
            start.elapsed() < Duration::from_secs(6),
            "require deadline must abort promptly, took {:?}",
            start.elapsed()
        );
    }
}
