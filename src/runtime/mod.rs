//! Embedded game-logic runtime for Citadel (, MVP Phase 1).
//!
//! This module hosts an embedded Lua virtual machine that lets a `game/` scripts
//! folder handle inbound realtime messages instead of (or on top of) the built-in
//! relay. It is the smallest real "write your game logic in a script" slice.
//!
//! Design (see the task's decision log and `docs/features/embedded-lua-runtime.md`):
//!
//! - The runtime is a **serialized, bounded, fallible command generator**. It owns
//!   no transport or registry state. [`LuaRuntime::dispatch`] runs the registered
//!   Lua handler for a message kind and returns a `Vec<`[`OutboundCommand`]`>`; the
//!   [`Gateway`](crate::realtime::Gateway) applies those commands to its
//!   `SessionRegistry` outside the Lua lock.
//! - Concurrency: `mlua`'s `Lua` is `!Sync`, so the VM lives behind a `Mutex` and
//!   every handler runs single-threaded under that lock. With the synchronous
//!   `Gateway::handle_inbound(..) -> usize` contract this is simpler and no more
//!   blocking than a dedicated actor thread would be for the MVP.
//! - Safety: a script error, a blown time budget, or even a Rust panic inside a
//!   callback is caught, logged, and turned into "no outbound commands". A bad
//!   script can never crash the node. A per-invocation deadline (an instruction
//!   hook) aborts a hung handler such as `while true do end`.
//!
//! Beyond message dispatch the host API exposes participant lifecycle hooks
//! (`citadel.on_join`/`on_leave`, invoked by the gateway on
//! register/unregister), a server game loop (`citadel.on_tick(dt)`, driven by a
//! periodic task at `runtime.tick_hz`), request/response RPC handlers
//! (`citadel.on_rpc(method, fn)`, invoked by the gateway on a `KIND_RPC_REQUEST`
//! via [`LuaRuntime::call_rpc`], which — unlike the fire-and-forget command
//! paths — RETURNS the handler's reply for the gateway to send back to the
//! caller only, correlated by request id), and `citadel.log`. Lifecycle, tick,
//! and RPC invocations reuse the same serialized lock, per-invocation deadline,
//! and error isolation as message dispatch, so a slow or erroring hook can never
//! wedge the node. Per-game state lives in Lua globals (a single shared VM).
//!
//! Language-neutral trajectory: this is Lua-only today, but the host API
//! (`on_message`/`on_join`/`on_leave`/`on_tick`/`broadcast`/`send`/`log`) and
//! the command model line up with the runtime contract
//! (`docs/architecture/runtime-contract.md`) so other languages can adopt the
//! same shape later.

pub mod cache_lease;
pub mod cluster;
pub mod event_bus;
pub mod host_api_spec;
pub mod host_services;
pub mod http_endpoint;
#[cfg(feature = "runtime-js")]
pub mod js;
pub mod lua;
pub mod outbound_http;
#[cfg(feature = "runtime-python")]
pub mod python;
#[cfg(feature = "runtime-python")]
mod python_bundle;
pub mod shared_cache;
pub(crate) mod static_data;

use std::time::Duration;

use crate::error::AppResult;
use crate::leaderboard_scheduler::{ResetEpoch, SchedulerFencingToken};

pub use event_bus::{
    MAX_RUNTIME_EVENTS_PER_INVOCATION, RuntimeEvent, RuntimeEventBus, RuntimeEventBusHandle,
    RuntimeEventEmitOutcome, RuntimeEventError, RuntimeEventPolicy, RuntimeEventPublisher,
    disabled_runtime_event_bus_handle, is_valid_runtime_event_name, runtime_event_bus,
    set_runtime_event_bus,
};
pub use host_api_spec::{HOST_API_SURFACE, HostApiCategory, HostApiFn, HostApiStatus};
pub use host_services::{
    DomainHost, FriendRowDto, ServiceDomainHost, StorageIndexObjectDto, StorageObjectDto,
    StorageWriteInput,
};
pub use http_endpoint::{
    RUNTIME_HTTP_ENDPOINT_PREFIX, RuntimeHttpAuth, RuntimeHttpEndpoint, RuntimeHttpEndpointError,
    RuntimeHttpEndpointPolicy, RuntimeHttpEndpointRateLimiter, RuntimeHttpMethod,
    RuntimeHttpOutcome, RuntimeHttpRequest, RuntimeHttpResponse,
};
#[cfg(feature = "runtime-js")]
pub use js::{JS_ENTRYPOINT, JsRuntime};
pub use lua::{
    DEFAULT_DEADLINE_MS, LifecycleHook, LuaRuntime, OutboundCommand, PhysicsOptions, ReloadOutcome,
    RoomSpec, RpcOutcome, RuntimeIntrospection,
};
#[cfg(feature = "runtime-python")]
pub use python::PythonRuntime;
#[cfg(feature = "runtime-python")]
pub use python_bundle::{
    BundledPythonEnv, configure_bundled_python_runtime, detect_bundled_python_runtime,
};
pub use shared_cache::{
    RuntimeCachePublisher, RuntimeSharedCache, RuntimeSharedCacheError, RuntimeSharedCacheHandle,
    RuntimeSharedCachePolicy, RuntimeSharedCacheValue, disabled_runtime_shared_cache_handle,
    runtime_shared_cache, set_runtime_shared_cache,
};
pub(crate) use static_data::DEFAULT_STATIC_DATA_MAX_FILE_BYTES;

/// Keep event-snapshot delivery within the same aggregate fan-out envelope as
/// one ordinary runtime invocation. Each subscriber has its own bounded sink,
/// but a full queue must not multiply that bound by the queue capacity.
pub(crate) fn append_runtime_event_commands(
    commands: &mut Vec<OutboundCommand>,
    event_commands: Vec<OutboundCommand>,
    source_label: &str,
) {
    const MAX_COMMANDS: usize = 1_024;
    const MAX_BODY_BYTES: usize = 1 << 20;

    let mut body_bytes = commands
        .iter()
        .map(outbound_command_body_bytes)
        .sum::<usize>();
    for command in event_commands {
        let command_bytes = outbound_command_body_bytes(&command);
        if commands.len() >= MAX_COMMANDS
            || body_bytes.saturating_add(command_bytes) > MAX_BODY_BYTES
        {
            tracing::warn!(
                script = %source_label,
                "runtime event commands exceeded the per-invocation aggregate limit; dropping remainder"
            );
            break;
        }
        body_bytes = body_bytes.saturating_add(command_bytes);
        commands.push(command);
    }
}

fn outbound_command_body_bytes(command: &OutboundCommand) -> usize {
    match command {
        OutboundCommand::Broadcast { body, .. } | OutboundCommand::Send { body, .. } => body.len(),
        _ => 0,
    }
}

/// Decision returned by a runtime's pre-dispatch realtime interceptor.
///
/// The gateway fails closed when an embedded runtime cannot produce a valid
/// decision, so a runtime implementation must return [`Drop`](Self::Drop) for
/// an error, timeout, or panic in its before hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeInterception {
    /// Continue through the gateway's normal router.
    Continue,
    /// Stop routing this envelope. No outbound deliveries are queued.
    Drop,
}

/// Result made visible to an observer after one realtime gateway dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeAfterOutcome {
    /// Whether the before hook vetoed this envelope.
    pub dropped: bool,
    /// Number of local outbound deliveries synchronously queued by the gateway.
    pub delivered: usize,
}

/// A language runtime that turns inbound game events into outbound commands.
///
/// Implemented today by [`LuaRuntime`]. Future adapters (Python, JS/TS, native
/// Rust, WASM) implement the same language-neutral surface so the gateway can
/// hold an `Arc<dyn Runtime>` and avoid naming a concrete scripting engine.
pub trait Runtime: Send + Sync + 'static {
    /// Inspect one post-registration, non-auth realtime envelope before routing.
    ///
    /// The default preserves existing runtime adapters. Implementations expose
    /// this as their `before_realtime` host hook and must fail closed if the hook
    /// cannot produce a valid decision.
    fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        let _ = (sender, user_id, room_id, kind, body);
        RealtimeInterception::Continue
    }

    /// Observe the outcome of one eligible realtime envelope after routing.
    ///
    /// Implementations must isolate errors and discard any outbound commands the
    /// observer attempts to enqueue; this callback cannot alter a completed
    /// gateway result.
    fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        let _ = (sender, user_id, room_id, kind, body, outcome);
    }

    /// Run the registered message handler for `kind` and return its commands.
    fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand>;

    /// Run a message handler in the scope of one authoritative match.
    ///
    /// The default preserves adapters that do not need a room-aware context.
    /// Shipped adapters override it so `ctx.room_id` identifies the match; the
    /// gateway still scopes resulting broadcasts independently of the adapter.
    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        let _ = room_id;
        self.dispatch(sender, user_id, kind, body)
    }

    /// Run the `on_join`/`on_leave` lifecycle handler for `sender`.
    fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand>;

    /// Invoke the registered leaderboard-reset hook for one durable epoch.
    ///
    /// Scheduler delivery uses this result to decide whether to acknowledge the
    /// durable outbox record; implementations must surface handler failures so
    /// the occurrence can be retried.
    fn on_leaderboard_reset(
        &self,
        epoch: &ResetEpoch,
        fencing_token: SchedulerFencingToken,
    ) -> AppResult<()> {
        let _ = (epoch, fencing_token);
        Ok(())
    }

    /// Run the server game-loop handler with elapsed `dt`, bounded by `budget`.
    fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand>;

    /// Advance one authoritative match. Adapters that expose a match-aware tick
    /// override this; the default keeps the global tick behavior unchanged.
    fn tick_in_room(&self, room_id: u64, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        let _ = room_id;
        self.tick(dt, budget)
    }

    /// Run the RPC handler for `method`.
    fn call_rpc(&self, sender: u64, user_id: Option<&str>, method: &str, body: &[u8])
    -> RpcOutcome;

    /// Run the room-create handler.
    fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec>;

    /// Run the room-join admission gate.
    fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool;

    /// Snapshot the endpoint declarations registered while this runtime was
    /// initialized. The HTTP transport uses this only to route under its
    /// reserved prefix; it never permits a runtime to replace Citadel routes.
    fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        Vec::new()
    }

    /// Invoke one endpoint handler selected by the transport from
    /// [`Self::http_endpoints`]. Implementations must apply the same error
    /// isolation and invocation deadline as other synchronous host calls.
    fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        let _ = request;
        RuntimeHttpOutcome::NotFound
    }

    /// Whether a game-loop (`on_tick`) handler is registered.
    fn has_tick_handler(&self) -> bool;

    /// The per-invocation handler time budget.
    fn budget(&self) -> Duration;

    /// A point-in-time description of registered handlers.
    fn introspect(&self) -> RuntimeIntrospection;

    /// Whether this runtime is backed by an on-disk source that can be reloaded.
    fn is_reloadable(&self) -> bool {
        false
    }

    /// Rebuild from the backing source and swap in, failure-safe.
    fn reload(&self) -> ReloadOutcome {
        ReloadOutcome::NotReloadable
    }

    /// Files whose changes should cause this on-disk runtime to reload.
    ///
    /// The default has no dependencies. File-backed adapters return their
    /// entrypoint plus any data dependencies discovered during initialization.
    /// This is intentionally separate from dispatch so polling metadata never
    /// introduces filesystem I/O on a message or tick path.
    fn reload_watch_paths(&self) -> Vec<std::path::PathBuf> {
        Vec::new()
    }
}
