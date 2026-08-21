//! Embedded CPython runtime host for trusted-tier Python game logic.
//!
//! `PythonRuntime` mirrors the Lua adapter's command-return model behind the
//! language-neutral [`Runtime`] trait. The base Citadel build does not compile
//! this module; it is available only with `--features runtime-python`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pyo3::Py;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyModule, PyTuple};

use crate::authoritative_telemetry_slices::{
    RuntimeScopeGuard, TelemetrySliceService, active_runtime_context, set_active_runtime_scope,
};
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
    BridgeCommandSink, LifecycleHook, MAX_RUNTIME_EVENTS_PER_INVOCATION, NativeMatchContext,
    NativeMatchLifecycleHook, NormalizedEventBatch, OutboundCommand, PhysicsOptions,
    RealtimeAfterOutcome, RealtimeInterception, ReloadOutcome, RoomSpec, RpcOutcome, Runtime,
    RuntimeEvent, RuntimeEventBus, RuntimeEventBusHandle, RuntimeEventEmitOutcome, RuntimeHttpAuth,
    RuntimeHttpEndpoint, RuntimeHttpEndpointPolicy, RuntimeHttpMethod, RuntimeHttpOutcome,
    RuntimeHttpRequest, RuntimeHttpResponse, RuntimeIntrospection, RuntimeSharedCache,
    RuntimeSharedCacheHandle, ScriptCommandBatch, append_runtime_event_commands, bridge_event_json,
    bridge_input_outcome_from_json, disabled_runtime_event_bus_handle,
    disabled_runtime_shared_cache_handle, runtime_event_bus, runtime_shared_cache,
    script_command_from_outbound, set_runtime_event_bus, set_runtime_shared_cache,
};
use crate::services::PlayerNotification;
use crate::time::{Clock, SystemClock};
use citadel_physics::{PhysicsConfig, Shape};

static PYTHON_BUILD_LOCK: Mutex<()> = Mutex::new(());
static PYTHON_MODULE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn notification_py_dict(py: Python<'_>, notification: PlayerNotification) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("id", notification.id)?;
    dict.set_item("code", notification.code)?;
    dict.set_item("subject", notification.subject)?;
    dict.set_item("content_json", notification.content.to_string())?;
    dict.set_item("sender", notification.sender)?;
    dict.set_item("created_at_unix_ms", notification.created_at_unix_ms)?;
    dict.set_item("read_at_unix_ms", notification.read_at_unix_ms)?;
    Ok(dict.unbind())
}

/// Default Python script entrypoint under `runtime.scripts_dir`.
pub const PYTHON_ENTRYPOINT: &str = "main.py";

/// Time budget for running top-level Python registrations at load/reload.
const LOAD_DEADLINE_MS: u64 = 5_000;

/// Maximum number of outbound commands a single handler invocation may enqueue.
const MAX_OUTBOUND_COMMANDS: usize = 1024;

/// Bound callback fan-out for one event key and one snapshot delivery.
const MAX_RUNTIME_EVENT_SUBSCRIBERS: usize = 64;

const RPC_ERR_UNKNOWN_METHOD: &str = "unknown RPC method";
const RPC_ERR_TIMEOUT: &str = "RPC handler timed out";
const RPC_ERR_HANDLER: &str = "RPC handler error";

const PYTHON_HOST_API_NAMES: &[&str] = &[
    "on_message",
    "on_join",
    "on_leave",
    "on_match_created",
    "on_match_started",
    "on_match_ended",
    "on_match_join",
    "on_match_leave",
    "on_match_tick",
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
    "telemetry.begin",
    "telemetry.mark",
    "telemetry.finish",
];

/// The Python-side `citadel` module. Keeping this in Python avoids Rust-side
/// PyO3 proc macros in this crate, which has `unsafe_code = "forbid"`.
const PYTHON_HOST_PRELUDE: &str = r#"
import json
import logging
import os
import sys
import time

_message_handlers = {}
_rpc_handlers = {}
_http_endpoint_handlers = {}
_event_handlers = {}
_MAX_EVENT_SUBSCRIBERS = 64
_storage_index_filters = {}
_on_join = None
_on_leave = None
_on_match_created = None
_on_match_started = None
_on_match_ended = None
_on_match_join = None
_on_match_leave = None
_on_match_tick = None
_on_tick = None
_on_leaderboard_reset = None
_on_room_create = None
_on_room_join = None
_on_before_realtime = None
_on_after_realtime = None
_on_input = None
_commands = []
_total_bytes = 0
_overflowed = False
_next_npc_id = 0x40000000
_deadline_at = None

MAX_OUTBOUND_COMMANDS = 1024
MAX_OUTBOUND_BODY_BYTES = 64 * 1024
MAX_TOTAL_OUTBOUND_BYTES = 1 << 20

class Ctx:
    __slots__ = ("sender", "user_id", "kind", "method", "room_id", "body", "dropped", "delivered")

    def __init__(self, sender, user_id=None, kind=None, method=None, room_id=None):
        self.sender = int(sender)
        self.user_id = user_id
        self.kind = kind
        self.method = method
        self.room_id = room_id
        self.body = None
        self.dropped = None
        self.delivered = None

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

class Reply:
    __slots__ = ("is_ok", "body", "error")

    def __init__(self, ok, body=b"", error=""):
        self.is_ok = bool(ok)
        self.body = _bytes(body)
        self.error = str(error)

    @classmethod
    def ok(cls, body=b""):
        return cls(True, body, "")

    @classmethod
    def err(cls, message):
        return cls(False, b"", message)

def _bytes(value):
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError("expected bytes-like value or str")

def _register(kind, store, key, handler):
    if handler is None:
        def decorator(fn):
            if not callable(fn):
                raise TypeError("handler must be callable")
            store[key] = fn
            return fn
        return decorator
    if not callable(handler):
        raise TypeError("handler must be callable")
    store[key] = handler
    return handler

def on_message(kind, handler=None):
    return _register("message", _message_handlers, int(kind), handler)

def on_rpc(method, handler=None):
    return _register("rpc", _rpc_handlers, str(method), handler)

def _single(name, handler):
    def decorator(fn):
        if not callable(fn):
            raise TypeError("handler must be callable")
        globals()[name] = fn
        return fn
    return decorator(handler) if handler is not None else decorator

def on_join(handler=None):
    return _single("_on_join", handler)

def on_leave(handler=None):
    return _single("_on_leave", handler)

def on_match_created(handler=None):
    return _single("_on_match_created", handler)

def on_match_started(handler=None):
    return _single("_on_match_started", handler)

def on_match_ended(handler=None):
    return _single("_on_match_ended", handler)

def on_match_join(handler=None):
    return _single("_on_match_join", handler)

def on_match_leave(handler=None):
    return _single("_on_match_leave", handler)

def on_match_tick(handler=None):
    return _single("_on_match_tick", handler)

def on_tick(handler=None):
    return _single("_on_tick", handler)

def on_leaderboard_reset(handler=None):
    return _single("_on_leaderboard_reset", handler)

def on_room_create(handler=None):
    return _single("_on_room_create", handler)

def on_room_join(handler=None):
    return _single("_on_room_join", handler)

def before_realtime(handler=None):
    return _single("_on_before_realtime", handler)

def after_realtime(handler=None):
    return _single("_on_after_realtime", handler)

def on_input(handler=None):
    return _single("_on_input", handler)

def log(message, level="info"):
    logger = logging.getLogger("citadel.script")
    level = str(level or "info").lower()
    if level == "trace":
        logger.debug(str(message))
    elif level == "debug":
        logger.debug(str(message))
    elif level == "warn":
        logger.warning(str(message))
    elif level == "error":
        logger.error(str(message))
    else:
        logger.info(str(message))

class _Http:
    def fetch(self, url, opts=None):
        """Perform one bounded Rust-owned HTTP request in the trusted runtime."""
        if "_http_bridge" not in globals():
            raise RuntimeError("outbound HTTP host not available")
        return _http_bridge.fetch(str(url), {} if opts is None else dict(opts))

    def start(self, url, opts=None):
        """Schedule one bounded Rust-owned HTTP request without blocking."""
        if "_http_bridge" not in globals():
            raise RuntimeError("outbound HTTP host not available")
        return _http_bridge.start(str(url), {} if opts is None else dict(opts))

    def poll(self, handle):
        return _http_bridge.poll(int(handle))

    def cancel(self, handle):
        return _http_bridge.cancel(int(handle))

    def register(self, method, path, options=None, handler=None):
        """Register one bounded endpoint under Citadel's reserved /ext prefix."""
        if handler is None and callable(options):
            handler, options = options, None

        def decorator(fn):
            if not callable(fn):
                raise TypeError("runtime HTTP endpoint handler must be callable")
            if "_http_endpoint_registry" not in globals():
                raise RuntimeError("runtime HTTP endpoint capability is disabled by runtime policy")
            key = _http_endpoint_registry.register(
                str(method), str(path), {} if options is None else dict(options))
            _http_endpoint_handlers[key] = fn
            return fn

        return decorator(handler) if handler is not None else decorator

http = _Http()

class _Events:
    def subscribe(self, namespace, event_type, handler=None):
        def decorator(fn):
            if not callable(fn):
                raise TypeError("runtime event subscriber must be callable")
            if "_event_bus_bridge" not in globals():
                raise RuntimeError("runtime event host not available")
            key = _event_bus_bridge.subscribe(str(namespace), str(event_type))
            callbacks = _event_handlers.setdefault(key, [])
            if len(callbacks) >= _MAX_EVENT_SUBSCRIBERS:
                raise RuntimeError("runtime event subscriber limit exceeded")
            callbacks.append(fn)
            return fn
        return decorator(handler) if handler is not None else decorator

    def emit(self, namespace, event_type, payload=b""):
        if "_event_bus_bridge" not in globals():
            raise RuntimeError("runtime event host not available")
        return bool(_event_bus_bridge.emit(str(namespace), str(event_type), _bytes(payload)))

events = _Events()

class _Cache:
    def _entry(self, value):
        if value is None:
            return None
        body, version, expires_in_ms = value
        return {"value": bytes(body), "version": int(version), "expires_in_ms": int(expires_in_ms)}

    def get(self, namespace, key):
        if "_shared_cache_bridge" not in globals():
            raise RuntimeError("runtime shared cache host not available")
        return self._entry(_shared_cache_bridge.get(str(namespace), str(key)))

    def set(self, namespace, key, value, ttl_ms):
        return self._entry(_shared_cache_bridge.set(
            str(namespace), str(key), _bytes(value), int(ttl_ms)))

    def delete(self, namespace, key):
        return bool(_shared_cache_bridge.delete(str(namespace), str(key)))

    def cas(self, namespace, key, expected_version, value, ttl_ms):
        version = None if expected_version is None else int(expected_version)
        return self._entry(_shared_cache_bridge.cas(
            str(namespace), str(key), version, _bytes(value), int(ttl_ms)))

cache = _Cache()

class _Telemetry:
    def _bridge(self):
        if "_telemetry_slices_bridge" not in globals():
            raise RuntimeError("telemetry slices are unavailable")
        return _telemetry_slices_bridge

    def begin(self):
        return self._bridge().begin()

    def mark(self, marker):
        return self._bridge().mark(str(marker))

    def finish(self):
        return self._bridge().finish()

telemetry = _Telemetry()

def _push(command, body_len=0):
    global _total_bytes, _overflowed
    if len(_commands) >= MAX_OUTBOUND_COMMANDS:
        _overflowed = True
        return
    if _total_bytes + body_len > MAX_TOTAL_OUTBOUND_BYTES:
        _overflowed = True
        return
    _commands.append(command)
    _total_bytes += body_len

def broadcast(kind, body, unreliable=False):
    body = _bytes(body)
    if len(body) > MAX_OUTBOUND_BODY_BYTES:
        raise RuntimeError("outbound body too large")
    _push(("broadcast", int(kind), body, bool(unreliable)), len(body))

def send(session, kind, body, unreliable=False):
    body = _bytes(body)
    if len(body) > MAX_OUTBOUND_BODY_BYTES:
        raise RuntimeError("outbound body too large")
    _push(("send", int(session), int(kind), body, bool(unreliable)), len(body))

def spawn_actor(opts=None, **kwargs):
    global _next_npc_id
    merged = {}
    if opts:
        merged.update(dict(opts))
    merged.update(kwargs)
    object_id = _next_npc_id
    _next_npc_id = _next_npc_id + 1
    if _next_npc_id > 0xFFFFFFFF:
        _next_npc_id = 0x40000000
    archetype = int(merged.get("archetype", 0))
    x = float(merged.get("x", 0.0))
    y = float(merged.get("y", 0.0))
    z = float(merged.get("z", 0.0))
    _push(("spawn_actor", object_id, archetype, x, y, z))
    return object_id

def move_actor(object_id, x, y, z, vx=0.0, vy=0.0, vz=0.0):
    _push(("move_actor", int(object_id), float(x), float(y), float(z),
           float(vx), float(vy), float(vz)))

def despawn_actor(object_id):
    _push(("despawn_actor", int(object_id)))

def set_physics(object_id, opts=None):
    """Attach/configure physics, or detach when opts is None/disabled."""
    encoded = None if opts is None else json.dumps(dict(opts))
    _push(("set_physics", int(object_id), encoded))

def apply_impulse(object_id, ix, iy, iz):
    _push(("apply_impulse", int(object_id), float(ix), float(iy), float(iz)))

def set_move_intent(object_id, vx, vy, vz):
    _push(("set_move_intent", int(object_id), float(vx), float(vy), float(vz)))

def physics_state(object_id):
    """Return grounded/position/velocity, or None without a bodied actor."""
    if "_transform_hub_bridge" not in globals():
        return None
    return _transform_hub_bridge.physics_state(int(object_id))

def rewind_query(shooter, origin, direction, tick=0):
    """Bounded lag-compensated hit query (Rust owns geometry; script decides)."""
    if "_transform_hub_bridge" not in globals():
        return {"hits": []}
    return _transform_hub_bridge.rewind_query(int(shooter), tuple(origin), tuple(direction), int(tick))

def map_info(name):
    """Return a loaded map's bounds and collision counts, or None when absent."""
    if "_map_catalog_bridge" not in globals():
        return None
    return _map_catalog_bridge.map_info(str(name))

def map_names():
    """Return loaded map keys in deterministic order."""
    if "_map_catalog_bridge" not in globals():
        return []
    return _map_catalog_bridge.map_names()

def find_path(name, start, goal):
    """Return Rust/Detour navigation points, or None when no route exists."""
    if "_map_catalog_bridge" not in globals():
        return None
    return _map_catalog_bridge.find_path(str(name), tuple(start), tuple(goal))

def raycast(origin, direction):
    """Return the nearest active-map hit for a finite ray segment, or None."""
    if "_transform_hub_bridge" not in globals():
        return None
    return _transform_hub_bridge.raycast(tuple(origin), tuple(direction))

def sphere_overlap(centre, radius):
    """Return whether a sphere overlaps the active map collision mesh."""
    if "_transform_hub_bridge" not in globals():
        return False
    return _transform_hub_bridge.sphere_overlap(tuple(centre), float(radius))

def ground_height(origin, max_distance):
    """Return the nearest walkable surface below origin, or None."""
    if "_transform_hub_bridge" not in globals():
        return None
    return _transform_hub_bridge.ground_height(tuple(origin), float(max_distance))

def _reset_commands():
    global _total_bytes, _overflowed
    _commands.clear()
    _total_bytes = 0
    _overflowed = False

def _take_commands():
    out = list(_commands)
    overflowed = _overflowed
    _reset_commands()
    return out, overflowed

def _make_ctx(sender, user_id=None, kind=None, method=None, room_id=None):
    return Ctx(sender, user_id, kind, method, room_id)

def _dispatch_message(kind, ctx, body):
    handler = _message_handlers.get(int(kind))
    if handler is None:
        return False
    handler(ctx, _bytes(body))
    return True

def _dispatch_before_realtime(ctx, body):
    if _on_before_realtime is None:
        return True
    decision = _on_before_realtime(ctx, _bytes(body))
    if decision is None or decision is True:
        return True
    if decision is False:
        return False
    raise TypeError("before_realtime must return False, True, or None")

def _dispatch_after_realtime(ctx, body):
    if _on_after_realtime is None:
        return False
    _on_after_realtime(ctx, _bytes(body))
    return True

def _normalize_input_decision(ret):
    if ret is None or ret is True or ret == "accept":
        return {"decision": "accept"}
    if ret is False or ret == "reject":
        return {"decision": "reject", "reason_code": 0}
    if isinstance(ret, dict):
        decision = str(ret.get("decision", "accept"))
        out = {"decision": decision}
        if "reason_code" in ret:
            out["reason_code"] = int(ret["reason_code"])
        if ret.get("reply") is not None:
            reply = ret["reply"]
            out["reply"] = reply.decode("utf-8") if isinstance(reply, (bytes, bytearray)) else str(reply)
        if decision == "correct":
            if not ret.get("transform"):
                raise TypeError("a correct decision requires a transform")
            out["transform"] = ret["transform"]
        return out
    raise TypeError("on_input must return None, a bool, a str, or a dict")

def _dispatch_input(event_json):
    if _on_input is None:
        return None
    decision = _normalize_input_decision(_on_input(json.loads(event_json)))
    return json.dumps(decision, separators=(",", ":"))

def _has_on_input():
    return _on_input is not None

def _dispatch_lifecycle(hook, ctx):
    handler = _on_join if hook == "on_join" else _on_leave
    if handler is None:
        return False
    handler(ctx)
    return True

def _dispatch_match_lifecycle(hook, context):
    handler = globals().get("_" + str(hook))
    if handler is None:
        return False
    handler(dict(context))
    return True

def _dispatch_tick(dt):
    if _on_tick is None:
        return False
    _on_tick(float(dt))
    return True

def _call_rpc(method, ctx, body):
    handler = _rpc_handlers.get(str(method))
    if handler is None:
        return None
    reply = handler(ctx, _bytes(body))
    if isinstance(reply, Reply):
        return (reply.is_ok, reply.body, reply.error)
    return (True, _bytes(reply), "")

def _call_room_create(ctx, params):
    if _on_room_create is None:
        return None
    spec = _on_room_create(ctx, _bytes(params))
    if spec is None:
        return None
    if isinstance(spec, str):
        return (spec, "", 0, True)
    data = dict(spec)
    return (
        str(data.get("map", "")),
        str(data.get("mode", "")),
        int(data.get("max_players", 0)),
        bool(data.get("open", True)),
    )

def _call_room_join(ctx, room_id):
    if _on_room_join is None:
        return None
    return bool(_on_room_join(ctx, int(room_id)))

def _call_http_endpoint(key, request):
    handler = _http_endpoint_handlers.get(str(key))
    if handler is None:
        return None
    response = handler(dict(request))
    if response is None:
        response = {}
    if not isinstance(response, dict):
        raise TypeError("runtime HTTP endpoint handler must return a mapping")
    status = int(response.get("status", 200))
    if status < 100 or status > 599:
        raise ValueError("runtime HTTP endpoint response status is invalid")
    headers = response.get("headers", {})
    if not isinstance(headers, dict):
        raise TypeError("runtime HTTP endpoint response headers must be a mapping")
    return (status, _bytes(response.get("body", b"")),
            json.dumps({str(name): str(value) for name, value in headers.items()},
                       separators=(",", ":")))

def _runtime_event_subscriber_count(key):
    callbacks = _event_handlers.get(str(key), [])
    return len(callbacks)

def _call_runtime_event_subscriber(key, index, event):
    callbacks = _event_handlers.get(str(key), [])
    if index < 0 or index >= len(callbacks):
        return False
    callbacks[index](dict(event))
    return True

def _has_tick_handler():
    return _on_tick is not None

def _call_leaderboard_reset(ctx):
    if _on_leaderboard_reset is None:
        return False
    _on_leaderboard_reset(dict(ctx))
    return True

def _has_any_handler():
    return (
        bool(_message_handlers)
        or bool(_rpc_handlers)
        or _on_join is not None
        or _on_leave is not None
        or _on_match_created is not None
        or _on_match_started is not None
        or _on_match_ended is not None
        or _on_match_join is not None
        or _on_match_leave is not None
        or _on_match_tick is not None
        or _on_tick is not None
        or _on_leaderboard_reset is not None
        or _on_room_create is not None
        or _on_room_join is not None
        or _on_before_realtime is not None
        or _on_after_realtime is not None
        or _on_input is not None
        or bool(_http_endpoint_handlers)
        or bool(_event_handlers)
    )

def _introspect():
    hooks = []
    if _on_join is not None:
        hooks.append("on_join")
    if _on_leave is not None:
        hooks.append("on_leave")
    if _on_match_created is not None:
        hooks.append("on_match_created")
    if _on_match_started is not None:
        hooks.append("on_match_started")
    if _on_match_ended is not None:
        hooks.append("on_match_ended")
    if _on_match_join is not None:
        hooks.append("on_match_join")
    if _on_match_leave is not None:
        hooks.append("on_match_leave")
    if _on_match_tick is not None:
        hooks.append("on_match_tick")
    if _on_tick is not None:
        hooks.append("on_tick")
    if _on_leaderboard_reset is not None:
        hooks.append("on_leaderboard_reset")
    if _on_room_create is not None:
        hooks.append("on_room_create")
    if _on_room_join is not None:
        hooks.append("on_room_join")
    if _on_before_realtime is not None:
        hooks.append("before_realtime")
    if _on_after_realtime is not None:
        hooks.append("after_realtime")
    if _on_input is not None:
        hooks.append("on_input")
    return (
        sorted(str(name) for name in _rpc_handlers.keys()),
        sorted(int(kind) for kind in _message_handlers.keys()),
        hooks,
    )

def _deadline_trace(frame, event, arg):
    if _deadline_at is not None and time.monotonic() >= _deadline_at:
        raise TimeoutError("handler exceeded its time budget")
    return _deadline_trace

def _arm_deadline(seconds):
    global _deadline_at
    _deadline_at = time.monotonic() + max(float(seconds), 0.001)
    sys.settrace(_deadline_trace)

def _clear_deadline():
    global _deadline_at
    _deadline_at = None
    sys.settrace(None)

def _prepare_imports(root):
    if not root:
        return
    root = os.path.abspath(str(root))
    if root not in sys.path:
        sys.path.insert(0, root)
    prefix = root + os.sep
    for name, module in list(sys.modules.items()):
        filename = getattr(module, "__file__", None)
        if not filename:
            continue
        try:
            filename = os.path.abspath(filename)
        except (TypeError, ValueError):
            continue
        if filename == root or filename.startswith(prefix):
            sys.modules.pop(name, None)

def friends_add(user, other):
    """Invite other to user, or accept their pending invite. Returns the new state token."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("friends host not available")
    return _domain_host_bridge.friends_add(str(user), str(other))

def friends_remove(user, other):
    """Remove any relation between user and other. Returns whether anything was removed."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("friends host not available")
    return _domain_host_bridge.friends_remove(str(user), str(other))

def friends_block(user, other):
    """Block other from user's side."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("friends host not available")
    return _domain_host_bridge.friends_block(str(user), str(other))

def friends_list(user):
    """List user's relations, other-id-ordered."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("friends host not available")
    return _domain_host_bridge.friends_list(str(user))

def notifications_send(recipient, code, subject, content_json, sender=None, delivery_key=None):
    """Persist one notification and attempt local realtime delivery after commit."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("notifications host not available")
    return _domain_host_bridge.notifications_send(str(recipient), int(code), str(subject),
        str(content_json), sender, delivery_key)

def notifications_list(recipient, limit=50, cursor=None):
    """Return recipient's durable notification page, newest first."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("notifications host not available")
    return _domain_host_bridge.notifications_list(str(recipient), int(limit), cursor)

def notifications_mark_read(recipient, ids):
    """Idempotently mark this recipient's notification ids read."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("notifications host not available")
    return _domain_host_bridge.notifications_mark_read(str(recipient), list(ids))

def groups_call(actor, operation, payload_json):
    """Run a groups operation with the JSON schema used by groups.* client RPCs."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("groups host not available")
    return json.loads(_domain_host_bridge.groups_call(str(actor), str(operation), str(payload_json)))

def leaderboards_call(actor, operation, payload_json):
    return json.loads(_domain_host_bridge.leaderboards_call(str(actor), str(operation), str(payload_json)))

def tournaments_call(actor, operation, payload_json):
    return json.loads(_domain_host_bridge.tournaments_call(str(actor), str(operation), str(payload_json)))

def chat_call(actor, operation, payload_json):
    return json.loads(_domain_host_bridge.chat_call(str(actor), str(operation), str(payload_json)))

def wallet_call(actor, operation, payload_json):
    return json.loads(_domain_host_bridge.wallet_call(str(actor), str(operation), str(payload_json)))

def storage_read(user, collection, key):
    """Return one user-owned storage object, or None when absent."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("storage host not available")
    return _domain_host_bridge.storage_read(str(user), str(collection), str(key))

def _make_register_storage_index_filter(guard):
    filters = _storage_index_filters

    def register_storage_index_filter(index_name, callback):
        """Register one synchronous include/exclude callback for a configured index."""
        guard.ensure_realtime_effects_allowed()
        if not isinstance(index_name, str) or not index_name or len(index_name) > 40 \
           or not index_name.isascii() or not (index_name[0].isalpha() or index_name[0] == "_") \
           or not all(char.isalnum() or char == "_" for char in index_name):
            raise ValueError("storage index name must be an ASCII identifier of at most 40 characters")
        if not callable(callback):
            raise TypeError("storage index filter must be callable")
        if index_name in filters:
            raise ValueError("storage index filter already registered for %r" % index_name)
        filters[index_name] = callback
        return callback

    return register_storage_index_filter

def storage_write(user, collection, key, value_json, expected_version=None,
                  read_permission=None, write_permission=None):
    """Write a JSON-object storage value and return its versioned object."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("storage host not available")
    user, collection, key, value_json = str(user), str(collection), str(key), str(value_json)
    candidates = _domain_host_bridge.storage_index_candidates(user, collection, key)
    candidate = {
        "user_id": user, "collection": collection, "key": key,
        "value_json": value_json, "expected_version": expected_version,
        "read_permission": read_permission, "write_permission": write_permission,
    }
    included = []
    for index_name in candidates:
        candidate["index_name"] = index_name
        callback = _storage_index_filters.get(index_name)
        if callback is None:
            included.append(index_name)
            continue
        decision = callback(dict(candidate))
        if type(decision) is not bool:
            raise TypeError("storage index filter must return a boolean")
        if decision:
            included.append(index_name)
    return _domain_host_bridge.storage_write(
        user, collection, key, value_json, expected_version,
        read_permission, write_permission, json.dumps(included, separators=(",", ":")))

def storage_delete(user, collection, key, expected_version=None):
    """Delete one user-owned storage object."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("storage host not available")
    return _domain_host_bridge.storage_delete(
        str(user), str(collection), str(key), expected_version)

def storage_index_query(index_name, filters_json, limit=50):
    """Query one configured storage index with JSON-object equality filters."""
    if "_domain_host_bridge" not in globals():
        raise RuntimeError("storage host not available")
    return _domain_host_bridge.storage_index_query(
        str(index_name), str(filters_json), int(limit))
"#;

/// PyO3 wrapper exposing the domain host to Python scripts.
/// Provides friends_add, friends_remove, friends_block, friends_list methods.
#[pyclass]
struct DomainHostBridge {
    host: Arc<dyn DomainHost>,
    interceptor_mode: Arc<AtomicBool>,
}

/// PyO3 wrapper exposing context-derived telemetry slices to Python scripts.
#[pyclass]
struct TelemetrySlicesHandle {
    slices: Arc<TelemetrySliceService>,
}

#[pymethods]
impl TelemetrySlicesHandle {
    fn begin(&self) -> PyResult<()> {
        let context = active_runtime_context().ok_or_else(|| {
            PyRuntimeError::new_err("telemetry slices require a match-scoped context")
        })?;
        self.slices
            .begin(context, SystemClock.now().unix_millis())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
    fn mark(&self, marker: String) -> PyResult<()> {
        let context = active_runtime_context().ok_or_else(|| {
            PyRuntimeError::new_err("telemetry slices require a match-scoped context")
        })?;
        self.slices
            .mark(context, &marker, SystemClock.now().unix_millis())
            .map(|_| ())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
    fn finish(&self) -> PyResult<()> {
        let context = active_runtime_context().ok_or_else(|| {
            PyRuntimeError::new_err("telemetry slices require a match-scoped context")
        })?;
        self.slices
            .finish(context, SystemClock.now().unix_millis())
            .map(|_| ())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[pyclass]
struct MapCatalogBridge {
    maps: Arc<MapCatalog>,
}

/// PyO3 wrapper exposing the bounded, parsed static-data catalog to Python.
///
/// The bridge has no file or directory handles. Its catalog is sealed after the
/// top-level script finishes, so later cache misses cannot initiate filesystem
/// I/O from a handler or game tick.
#[pyclass]
struct StaticDataBridge {
    catalog: StaticDataCatalog,
}

#[pyclass]
struct TextPolicyBridge {
    catalog: TextPolicyCatalog,
}

/// Rust-owned HTTP bridge. Python receives this narrow request facade, never a
/// socket, an HTTP client, or proxy configuration.
#[pyclass]
struct OutboundHttpBridge {
    client: AsyncOutboundHttp,
    fetch_client: TrustedHttpClient,
    interceptor_mode: Arc<AtomicBool>,
}

/// Narrow Python bridge for the node-local runtime event bus. Python owns
/// callback lists; Rust owns validation, capacity, and rate limits.
#[pyclass]
struct RuntimeEventBusBridge {
    event_bus_handle: RuntimeEventBusHandle,
    interceptor_mode: Arc<AtomicBool>,
}

/// Narrow Python bridge for the process-local, non-durable shared runtime cache.
#[pyclass]
struct RuntimeSharedCacheBridge {
    shared_cache_handle: RuntimeSharedCacheHandle,
    interceptor_mode: Arc<AtomicBool>,
}

/// Registration bridge that validates script declarations while keeping the
/// authoritative endpoint snapshot outside the Python heap.
#[pyclass]
struct RuntimeHttpEndpointRegistry {
    endpoints: Arc<Mutex<BTreeSet<RuntimeHttpEndpoint>>>,
}

#[pyclass]
struct RuntimeModeBridge {
    interceptor_mode: Arc<AtomicBool>,
}

/// PyO3 bridge exposing synchronous transform-physics reads to Python scripts.
#[pyclass]
struct TransformHubHandle {
    hub: Arc<TransformHub>,
}

#[pymethods]
impl StaticDataBridge {
    fn load_json(&self, path: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = self
            .catalog
            .load_json(path)
            .map_err(static_data_python_error)?;
        static_data_value_to_python(py, &value)
    }

    fn load_csv(&self, path: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = self
            .catalog
            .load_csv(path)
            .map_err(static_data_python_error)?;
        static_data_value_to_python(py, &value)
    }
}

#[pymethods]
impl TextPolicyBridge {
    fn load_json(&self, path: &str) -> PyResult<String> {
        self.catalog
            .load_json(path)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn scan(&self, policy_ref: &str, text: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = self
            .catalog
            .scan_value(policy_ref, text)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        static_data_value_to_python(py, &value)
    }

    fn sanitize(&self, policy_ref: &str, text: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value = self
            .catalog
            .sanitize_value(policy_ref, text)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        static_data_value_to_python(py, &value)
    }
}

#[pymethods]
impl OutboundHttpBridge {
    fn fetch(&self, url: &str, opts: &Bound<'_, PyDict>, py: Python<'_>) -> PyResult<Py<PyDict>> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err("interceptor_forbidden"));
        }
        let response = self
            .fetch_client
            .execute_blocking(OutboundHttpRequest {
                method: opts
                    .get_item("method")?
                    .and_then(|value| value.extract::<String>().ok())
                    .unwrap_or_else(|| "GET".to_string()),
                url: url.to_string(),
                headers: opts
                    .get_item("headers")?
                    .map(|value| value.extract::<BTreeMap<String, String>>())
                    .transpose()?
                    .unwrap_or_default(),
                body: opts
                    .get_item("body")?
                    .map(|value| value.extract::<Vec<u8>>())
                    .transpose()?
                    .unwrap_or_default(),
            })
            .map_err(|error| PyRuntimeError::new_err(error.error_code().to_string()))?;
        let result = PyDict::new(py);
        result.set_item("status", response.status)?;
        result.set_item("body", PyBytes::new(py, &response.body))?;
        Ok(result.unbind())
    }
    fn start(&self, url: &str, opts: &Bound<'_, PyDict>) -> PyResult<u64> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err("interceptor_forbidden"));
        }
        let method = opts
            .get_item("method")?
            .map(|value| value.extract::<String>())
            .transpose()?
            .unwrap_or_else(|| "GET".to_string());
        let body = match opts.get_item("body")? {
            Some(value) if value.is_instance_of::<PyBytes>() => {
                value.cast::<PyBytes>()?.as_bytes().to_vec()
            }
            Some(value) => value.extract::<String>()?.into_bytes(),
            None => Vec::new(),
        };
        let headers = match opts.get_item("headers")? {
            Some(value) => value.extract::<BTreeMap<String, String>>()?,
            None => BTreeMap::new(),
        };
        self.client
            .start(OutboundHttpRequest {
                method,
                url: url.to_string(),
                headers,
                body,
            })
            .map_err(|error| PyRuntimeError::new_err(error.error_code().to_string()))
    }

    fn poll(&self, handle: u64, py: Python<'_>) -> PyResult<Py<PyDict>> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err("interceptor_forbidden"));
        }
        outbound_http_state_to_python(
            py,
            self.client
                .poll(handle)
                .map_err(|e| PyRuntimeError::new_err(e.error_code().to_string()))?,
        )
    }

    fn cancel(&self, handle: u64, py: Python<'_>) -> PyResult<Py<PyDict>> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err("interceptor_forbidden"));
        }
        outbound_http_state_to_python(
            py,
            self.client
                .cancel(handle)
                .map_err(|e| PyRuntimeError::new_err(e.error_code().to_string()))?,
        )
    }
}

fn outbound_http_state_to_python(
    py: Python<'_>,
    state: OutboundHttpRequestState,
) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("state", state.status())?;
    match state {
        OutboundHttpRequestState::Success(response) => {
            result.set_item("status", response.status)?;
            result.set_item("body", PyBytes::new(py, &response.body))?;
        }
        OutboundHttpRequestState::Error(error) => result.set_item("error_code", error)?,
        _ => {}
    }
    Ok(result.unbind())
}

#[pymethods]
impl RuntimeEventBusBridge {
    fn subscribe(&self, namespace: &str, event_type: &str) -> PyResult<String> {
        let event = RuntimeEvent::new(namespace, event_type, Vec::new())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(runtime_event_key(&event.namespace, &event.event_type))
    }

    fn emit(&self, namespace: &str, event_type: &str, payload: &[u8]) -> PyResult<bool> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err(
                "domain, storage, outbound HTTP, and runtime events are unavailable in realtime interceptors",
            ));
        }
        let event = RuntimeEvent::new(namespace, event_type, payload.to_vec())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(matches!(
            runtime_event_bus(&self.event_bus_handle).emit(event),
            RuntimeEventEmitOutcome::Queued
        ))
    }
}

#[pymethods]
impl RuntimeSharedCacheBridge {
    fn get(&self, namespace: &str, key: &str) -> PyResult<Option<(Vec<u8>, u64, u64)>> {
        self.ensure_realtime_effects_allowed()?;
        runtime_shared_cache(&self.shared_cache_handle)
            .get(namespace, key)
            .map(|value| value.map(|entry| (entry.value, entry.version, entry.expires_in_ms)))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn set(
        &self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> PyResult<(Vec<u8>, u64, u64)> {
        self.ensure_realtime_effects_allowed()?;
        runtime_shared_cache(&self.shared_cache_handle)
            .set(namespace, key, value, Duration::from_millis(ttl_ms))
            .map(|entry| (entry.value, entry.version, entry.expires_in_ms))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn delete(&self, namespace: &str, key: &str) -> PyResult<bool> {
        self.ensure_realtime_effects_allowed()?;
        runtime_shared_cache(&self.shared_cache_handle)
            .delete(namespace, key)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn cas(
        &self,
        namespace: &str,
        key: &str,
        expected_version: Option<u64>,
        value: Vec<u8>,
        ttl_ms: u64,
    ) -> PyResult<Option<(Vec<u8>, u64, u64)>> {
        self.ensure_realtime_effects_allowed()?;
        runtime_shared_cache(&self.shared_cache_handle)
            .compare_and_swap(
                namespace,
                key,
                expected_version,
                value,
                Duration::from_millis(ttl_ms),
            )
            .map(|value| value.map(|entry| (entry.value, entry.version, entry.expires_in_ms)))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}

impl RuntimeSharedCacheBridge {
    fn ensure_realtime_effects_allowed(&self) -> PyResult<()> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err(
                "domain, storage, outbound HTTP, runtime events, and shared cache APIs are unavailable in realtime interceptors",
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl RuntimeHttpEndpointRegistry {
    fn register(&self, method: &str, path: &str, options: &Bound<'_, PyDict>) -> PyResult<String> {
        let method = RuntimeHttpMethod::parse(method)
            .ok_or_else(|| PyRuntimeError::new_err("runtime HTTP endpoint method is invalid"))?;
        let auth = match options.get_item("auth")? {
            Some(value) => {
                let auth = value.extract::<String>().map_err(|_| {
                    PyRuntimeError::new_err(
                        "runtime HTTP endpoint auth must be 'public' or 'session'",
                    )
                })?;
                RuntimeHttpAuth::parse(&auth).ok_or_else(|| {
                    PyRuntimeError::new_err(
                        "runtime HTTP endpoint auth must be 'public' or 'session'",
                    )
                })?
            }
            None => RuntimeHttpAuth::Public,
        };
        let endpoint = RuntimeHttpEndpoint::new(method, path, auth)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let mut endpoints = self
            .endpoints
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Python callbacks use a method/path key as well. `auth` must not make
        // a second registration distinct, or it could overwrite the callback
        // selected by an earlier public/session declaration.
        if endpoints
            .iter()
            .any(|existing| existing.method == endpoint.method && existing.path == endpoint.path)
        {
            return Err(PyRuntimeError::new_err(
                "runtime HTTP endpoint is already registered",
            ));
        }
        endpoints.insert(endpoint.clone());
        Ok(format!("{} {}", endpoint.method.as_str(), endpoint.path))
    }
}

impl DomainHostBridge {
    fn ensure_realtime_effects_allowed(&self) -> PyResult<()> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err(
                "domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors",
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl RuntimeModeBridge {
    fn ensure_realtime_effects_allowed(&self) -> PyResult<()> {
        if self.interceptor_mode.load(Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err(
                "domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors",
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl MapCatalogBridge {
    #[pyo3(name = "map_info")]
    fn map_info(&self, name: &str, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let Some(info) = self.maps.info(name) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("bounds_min", info.bounds_min)?;
        dict.set_item("bounds_max", info.bounds_max)?;
        dict.set_item("vertex_count", info.vertex_count)?;
        dict.set_item("triangle_count", info.triangle_count)?;
        Ok(Some(dict.unbind()))
    }

    #[pyo3(name = "map_names")]
    fn map_names(&self) -> Vec<String> {
        self.maps.names().map(str::to_owned).collect()
    }

    #[pyo3(name = "find_path")]
    fn find_path(
        &self,
        name: &str,
        start: (f32, f32, f32),
        goal: (f32, f32, f32),
    ) -> PyResult<Option<Vec<(f32, f32, f32)>>> {
        let start = [start.0, start.1, start.2];
        let goal = [goal.0, goal.1, goal.2];
        if !start.into_iter().chain(goal).all(f32::is_finite) {
            return Err(PyRuntimeError::new_err("navigation points must be finite"));
        }
        Ok(self
            .maps
            .find_path(name, start, goal)
            .ok()
            .flatten()
            .map(|path| {
                path.into_iter()
                    .map(|point| (point[0], point[1], point[2]))
                    .collect()
            }))
    }
}

#[pymethods]
impl TransformHubHandle {
    #[pyo3(name = "physics_state")]
    fn physics_state(&self, object_id: u32, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let Some(state) = self.hub.physics_state(object_id) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("grounded", state.grounded)?;
        dict.set_item("position", state.position)?;
        dict.set_item("velocity", state.velocity)?;
        Ok(Some(dict.unbind()))
    }

    fn rewind_query(
        &self,
        shooter: u64,
        origin: (f32, f32, f32),
        direction: (f32, f32, f32),
        tick: u64,
        py: Python<'_>,
    ) -> PyResult<Py<PyDict>> {
        let hits = self.hub.rewind_query(
            shooter,
            [origin.0, origin.1, origin.2],
            [direction.0, direction.1, direction.2],
            tick,
        );
        let list = PyList::empty(py);
        for hit in hits {
            let dict = PyDict::new(py);
            dict.set_item("object_id", hit.object_id)?;
            dict.set_item("participant", hit.participant)?;
            dict.set_item("point", hit.point)?;
            dict.set_item("distance", hit.distance)?;
            list.append(dict)?;
        }
        let out = PyDict::new(py);
        out.set_item("hits", list)?;
        Ok(out.unbind())
    }

    fn raycast(
        &self,
        origin: (f32, f32, f32),
        direction: (f32, f32, f32),
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let Some(hit) = self.hub.raycast(
            [origin.0, origin.1, origin.2],
            [direction.0, direction.1, direction.2],
        ) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("point", hit.point)?;
        dict.set_item("normal", hit.normal)?;
        dict.set_item("distance", hit.distance)?;
        dict.set_item("triangle_index", hit.triangle_index)?;
        Ok(Some(dict.unbind()))
    }

    fn sphere_overlap(&self, centre: (f32, f32, f32), radius: f32) -> PyResult<bool> {
        if !radius.is_finite() || radius < 0.0 {
            return Err(PyRuntimeError::new_err(
                "radius must be a finite non-negative number",
            ));
        }
        Ok(self
            .hub
            .sphere_overlap([centre.0, centre.1, centre.2], radius))
    }

    fn ground_height(
        &self,
        origin: (f32, f32, f32),
        max_distance: f32,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyDict>>> {
        if !max_distance.is_finite() || max_distance < 0.0 {
            return Err(PyRuntimeError::new_err(
                "max_distance must be a finite non-negative number",
            ));
        }
        let Some(hit) = self
            .hub
            .ground_height([origin.0, origin.1, origin.2], max_distance)
        else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("point", hit.point)?;
        dict.set_item("normal", hit.normal)?;
        dict.set_item("distance", hit.distance)?;
        dict.set_item("triangle_index", hit.triangle_index)?;
        Ok(Some(dict.unbind()))
    }
}

#[pymethods]
impl DomainHostBridge {
    /// Invite other to user, or accept their pending invite.
    /// Returns the new state token or raises an exception.
    #[pyo3(name = "friends_add")]
    fn friends_add(&self, user: &str, other: &str) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .friends_add(user, other)
            .map_err(PyRuntimeError::new_err)
    }

    /// Remove any relation between the two.
    /// Returns whether anything was removed or raises an exception.
    #[pyo3(name = "friends_remove")]
    fn friends_remove(&self, user: &str, other: &str) -> PyResult<bool> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .friends_remove(user, other)
            .map_err(PyRuntimeError::new_err)
    }

    /// Block other from user's side.
    /// Raises an exception on error.
    #[pyo3(name = "friends_block")]
    fn friends_block(&self, user: &str, other: &str) -> PyResult<()> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .friends_block(user, other)
            .map_err(PyRuntimeError::new_err)
    }

    /// List user's relations.
    /// Returns a list of dicts or raises an exception.
    #[pyo3(name = "friends_list")]
    fn friends_list(&self, user: &str, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.ensure_realtime_effects_allowed()?;
        let rows = self
            .host
            .friends_list(user)
            .map_err(PyRuntimeError::new_err)?;

        let list = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            dict.set_item("user_id", &row.user_id)?;
            dict.set_item("state", &row.state)?;
            dict.set_item("updated_unix_ms", row.updated_unix_ms)?;
            list.append(dict)?;
        }
        Ok(list.into())
    }

    #[pyo3(name = "notifications_send")]
    #[allow(
        clippy::too_many_arguments,
        reason = "The Python host API deliberately mirrors the documented positional notification signature."
    )]
    fn notifications_send(
        &self,
        recipient: &str,
        code: i32,
        subject: &str,
        content_json: &str,
        sender: Option<&str>,
        delivery_key: Option<&str>,
        py: Python<'_>,
    ) -> PyResult<Py<PyDict>> {
        self.ensure_realtime_effects_allowed()?;
        let notification = self
            .host
            .notifications_send(recipient, code, subject, content_json, sender, delivery_key)
            .map_err(PyRuntimeError::new_err)?;
        notification_py_dict(py, notification)
    }

    #[pyo3(name = "notifications_list")]
    fn notifications_list(
        &self,
        recipient: &str,
        limit: usize,
        cursor: Option<&str>,
        py: Python<'_>,
    ) -> PyResult<Py<PyDict>> {
        self.ensure_realtime_effects_allowed()?;
        let page = self
            .host
            .notifications_list(recipient, limit, cursor)
            .map_err(PyRuntimeError::new_err)?;
        let out = PyDict::new(py);
        let items = PyList::empty(py);
        for notification in page.items {
            items.append(notification_py_dict(py, notification)?)?;
        }
        out.set_item("items", items)?;
        out.set_item("next_cursor", page.next_cursor)?;
        Ok(out.unbind())
    }

    #[pyo3(name = "notifications_mark_read")]
    fn notifications_mark_read(&self, recipient: &str, ids: Vec<String>) -> PyResult<Vec<String>> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .notifications_mark_read(recipient, &ids)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "groups_call")]
    fn groups_call(&self, actor: &str, operation: &str, payload_json: &str) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .groups_call(actor, operation, payload_json)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "leaderboards_call")]
    fn leaderboards_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .leaderboards_call(actor, operation, payload_json)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "tournaments_call")]
    fn tournaments_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .tournaments_call(actor, operation, payload_json)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "chat_call")]
    fn chat_call(&self, actor: &str, operation: &str, payload_json: &str) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .chat_call(actor, operation, payload_json)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "wallet_call")]
    fn wallet_call(&self, actor: &str, operation: &str, payload_json: &str) -> PyResult<String> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .wallet_call(actor, operation, payload_json)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "storage_read")]
    fn storage_read(
        &self,
        user: &str,
        collection: &str,
        key: &str,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyDict>>> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .storage_read(user, collection, key)
            .map_err(PyRuntimeError::new_err)?
            .map(|object| storage_object_dict(object, py))
            .transpose()
    }

    #[pyo3(name = "storage_write")]
    #[allow(clippy::too_many_arguments)]
    fn storage_write(
        &self,
        user: &str,
        collection: &str,
        key: &str,
        value_json: &str,
        expected_version: Option<&str>,
        read_permission: Option<u8>,
        write_permission: Option<u8>,
        included_index_names_json: Option<&str>,
        py: Python<'_>,
    ) -> PyResult<Py<PyDict>> {
        self.ensure_realtime_effects_allowed()?;
        let object = self
            .host
            .storage_write(
                StorageWriteInput::new(user, collection, key, value_json)
                    .expecting(expected_version)
                    .with_permissions(read_permission, write_permission)
                    .with_included_index_names_json(included_index_names_json),
            )
            .map_err(PyRuntimeError::new_err)?;
        storage_object_dict(object, py)
    }

    #[pyo3(name = "storage_index_candidates")]
    fn storage_index_candidates(
        &self,
        user: &str,
        collection: &str,
        key: &str,
    ) -> PyResult<Vec<String>> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .storage_index_candidates(user, collection, key)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "storage_delete")]
    fn storage_delete(
        &self,
        user: &str,
        collection: &str,
        key: &str,
        expected_version: Option<&str>,
    ) -> PyResult<()> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .storage_delete(user, collection, key, expected_version)
            .map_err(PyRuntimeError::new_err)
    }

    #[pyo3(name = "storage_index_query")]
    fn storage_index_query(
        &self,
        index_name: &str,
        filters_json: &str,
        limit: usize,
        py: Python<'_>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        self.ensure_realtime_effects_allowed()?;
        self.host
            .storage_index_query(index_name, filters_json, limit)
            .map_err(PyRuntimeError::new_err)?
            .into_iter()
            .map(|object| storage_index_object_dict(object, py))
            .collect()
    }
}

fn storage_object_dict(
    object: crate::runtime::StorageObjectDto,
    py: Python<'_>,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("value_json", object.value_json)?;
    dict.set_item("version", object.version)?;
    dict.set_item("read_permission", object.read_permission)?;
    dict.set_item("write_permission", object.write_permission)?;
    Ok(dict.into())
}

fn storage_index_object_dict(
    object: crate::runtime::StorageIndexObjectDto,
    py: Python<'_>,
) -> PyResult<Py<PyDict>> {
    let dict = storage_object_dict(object.object, py)?;
    let dict = dict.bind(py);
    dict.set_item("user_id", object.user_id)?;
    dict.set_item("collection", object.collection)?;
    dict.set_item("key", object.key)?;
    Ok(dict.clone().unbind())
}

struct PythonVm {
    citadel: Py<PyModule>,
    source_label: String,
    python_version: String,
    interceptor_mode: Arc<AtomicBool>,
    /// Parsed static gameplay data initialized with this VM. Replaced atomically
    /// with the VM on hot reload so a bad data edit cannot partially publish.
    static_data: StaticDataCatalog,
    /// Validated endpoint declarations built with this VM. A fresh registry is
    /// published only after a complete successful reload.
    http_endpoints: Arc<Mutex<BTreeSet<RuntimeHttpEndpoint>>>,
}

/// Embedded CPython runtime that implements the language-neutral runtime trait.
pub struct PythonRuntime {
    vm: Mutex<PythonVm>,
    budget: Duration,
    reload_path: Option<PathBuf>,
    module_root: Option<PathBuf>,
    /// Optional operator-owned static-data directory, distinct from scripts.
    /// Retained so a hot reload builds a fresh catalog from the same root.
    static_data_dir: Option<PathBuf>,
    /// Per-file static-data read bound retained across reloads.
    static_data_max_file_bytes: usize,
    /// Outbound HTTP policy captured at load time and retained across a
    /// source-only hot reload.
    outbound_http_policy: OutboundHttpPolicy,
    http_endpoint_policy: RuntimeHttpEndpointPolicy,
    event_bus_handle: RuntimeEventBusHandle,
    shared_cache_handle: RuntimeSharedCacheHandle,
    /// Persisted-domain-services seam exposed to `citadel.friends_*` host calls
    ///, or `None` when no services are attached. Retained so a
    /// hot-reload re-applies it to the fresh VM.
    domain: Option<Arc<dyn DomainHost>>,
    maps: Option<Arc<MapCatalog>>,
    transform_hub: Option<Arc<TransformHub>>,
    telemetry_slices: Option<Arc<TelemetrySliceService>>,
    /// Where this runtime's authoritative-bridge answers land (the gateway),
    /// held weakly. Lives on the runtime so it survives a hot-reload swap.
    bridge_sink: Mutex<Option<Weak<dyn BridgeCommandSink>>>,
}

impl std::fmt::Debug for PythonRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonRuntime")
            .field("budget", &self.budget)
            .field("reload_path", &self.reload_path)
            .finish_non_exhaustive()
    }
}

impl PythonRuntime {
    /// Load `main.py` from `scripts_dir`, or `Ok(None)` if it is absent.
    pub fn load(scripts_dir: &Path, deadline_ms: u64) -> AppResult<Option<Self>> {
        Self::load_with_static_data(
            scripts_dir,
            deadline_ms,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
        )
    }

    /// Load `main.py` with an optional, separately configured static-data root.
    ///
    /// The root is never made visible to Python. The script can only request
    /// validated relative JSON/CSV paths through `citadel.static_data` while its
    /// top-level initialization body runs.
    pub fn load_with_static_data(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
    ) -> AppResult<Option<Self>> {
        Self::load_with_static_data_and_http_policy(
            scripts_dir,
            deadline_ms,
            static_data_dir,
            static_data_max_file_bytes,
            OutboundHttpPolicy::default(),
        )
    }

    /// Load `main.py` with an explicit operator-owned outbound HTTP policy.
    pub fn load_with_static_data_and_http_policy(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
        outbound_http_policy: OutboundHttpPolicy,
    ) -> AppResult<Option<Self>> {
        Self::load_with_static_data_and_capability_policies(
            scripts_dir,
            deadline_ms,
            static_data_dir,
            static_data_max_file_bytes,
            outbound_http_policy,
            RuntimeHttpEndpointPolicy::default(),
        )
    }

    /// Load `main.py` with all operator-owned runtime extension policies.
    pub fn load_with_static_data_and_capability_policies(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
        outbound_http_policy: OutboundHttpPolicy,
        http_endpoint_policy: RuntimeHttpEndpointPolicy,
    ) -> AppResult<Option<Self>> {
        let main = scripts_dir.join(PYTHON_ENTRYPOINT);
        if !main.is_file() {
            return Ok(None);
        }
        let source = read_script(&main)?;
        let module_root = scripts_dir.to_path_buf();
        let source_label = main.display().to_string();
        let static_data = StaticDataCatalog::new(static_data_dir, static_data_max_file_bytes)?;
        let event_bus_handle = disabled_runtime_event_bus_handle();
        let shared_cache_handle = disabled_runtime_shared_cache_handle();
        let vm = build_python(
            &source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            PythonBuildOptions {
                module_root: Some(&module_root),
                domain: &None,
                maps: &None,
                transform_hub: &None,
                static_data,
                outbound_http_policy: outbound_http_policy.clone(),
                http_endpoint_policy,
                event_bus_handle: Arc::clone(&event_bus_handle),
                shared_cache_handle: Arc::clone(&shared_cache_handle),
            },
        )?;
        Ok(Some(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: Some(main),
            module_root: Some(module_root),
            static_data_dir: static_data_dir.map(Path::to_path_buf),
            static_data_max_file_bytes,
            outbound_http_policy,
            http_endpoint_policy,
            event_bus_handle,
            shared_cache_handle,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
            bridge_sink: Mutex::new(None),
        }))
    }

    /// Build a runtime from inline Python source.
    pub fn from_source(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let event_bus_handle = disabled_runtime_event_bus_handle();
        let shared_cache_handle = disabled_runtime_shared_cache_handle();
        let vm = build_python(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            PythonBuildOptions {
                module_root: None,
                domain: &None,
                maps: &None,
                transform_hub: &None,
                static_data,
                outbound_http_policy: OutboundHttpPolicy::default(),
                http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
                event_bus_handle: Arc::clone(&event_bus_handle),
                shared_cache_handle: Arc::clone(&shared_cache_handle),
            },
        )?;
        Ok(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: None,
            module_root: None,
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            outbound_http_policy: OutboundHttpPolicy::default(),
            http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
            event_bus_handle,
            shared_cache_handle,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
            bridge_sink: Mutex::new(None),
        })
    }

    /// Build a runtime from inline source with a root added to `sys.path`.
    pub fn from_source_with_root(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
        module_root: &Path,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let event_bus_handle = disabled_runtime_event_bus_handle();
        let shared_cache_handle = disabled_runtime_shared_cache_handle();
        let vm = build_python(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            PythonBuildOptions {
                module_root: Some(module_root),
                domain: &None,
                maps: &None,
                transform_hub: &None,
                static_data,
                outbound_http_policy: OutboundHttpPolicy::default(),
                http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
                event_bus_handle: Arc::clone(&event_bus_handle),
                shared_cache_handle: Arc::clone(&shared_cache_handle),
            },
        )?;
        Ok(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: None,
            module_root: Some(module_root.to_path_buf()),
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            outbound_http_policy: OutboundHttpPolicy::default(),
            http_endpoint_policy: RuntimeHttpEndpointPolicy::default(),
            event_bus_handle,
            shared_cache_handle,
            domain: None,
            maps: None,
            transform_hub: None,
            telemetry_slices: None,
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
            apply_domain_host(
                &guard.citadel,
                &self.domain,
                Arc::clone(&guard.interceptor_mode),
            );
        }
        self
    }

    /// Attach private context-derived telemetry slices to trusted script calls.
    #[must_use]
    pub fn with_telemetry_slices(mut self, slices: Arc<TelemetrySliceService>) -> Self {
        self.telemetry_slices = Some(slices);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_telemetry_slices(&guard.citadel, &self.telemetry_slices);
        }
        self
    }

    /// Attach the node-owned best-effort event bus and retain it through a
    /// source-only Python hot reload.
    #[must_use]
    pub fn with_event_bus(self, bus: Arc<RuntimeEventBus>) -> Self {
        set_runtime_event_bus(&self.event_bus_handle, bus);
        self
    }

    /// Attach the node-local shared cache and retain it through a source-only reload.
    #[must_use]
    pub fn with_shared_cache(self, cache: Arc<RuntimeSharedCache>) -> Self {
        set_runtime_shared_cache(&self.shared_cache_handle, cache);
        self
    }

    /// Attach the loaded-map catalog for read-only `citadel.map_info` calls.
    #[must_use]
    pub fn with_maps(mut self, maps: Arc<MapCatalog>) -> Self {
        self.maps = Some(maps);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_map_catalog(&guard.citadel, &self.maps);
        }
        self
    }

    /// Attach the transform hub for synchronous `citadel.physics_state` reads.
    #[must_use]
    pub fn with_transform_hub(mut self, hub: Arc<TransformHub>) -> Self {
        self.transform_hub = Some(hub);
        {
            let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            apply_transform_hub(&guard.citadel, &self.transform_hub);
        }
        self
    }

    /// Names registered into Python's `citadel` module by this adapter.
    #[must_use]
    pub fn registered_host_api_names() -> HashSet<&'static str> {
        PYTHON_HOST_API_NAMES.iter().copied().collect()
    }

    /// Whether this runtime is backed by an on-disk script.
    #[must_use]
    pub fn is_reloadable(&self) -> bool {
        self.reload_path.is_some()
    }

    /// Rebuild from the backing `main.py`, rejecting broken or handlerless edits.
    pub fn reload(&self) -> ReloadOutcome {
        let Some(path) = self.reload_path.as_deref() else {
            return ReloadOutcome::NotReloadable;
        };
        let label = path.display().to_string();
        let source = match read_script(path) {
            Ok(source) => source,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "python hot-reload: cannot read script; keeping current runtime"
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
                    "python hot-reload: cannot initialize static-data catalog; keeping the current script and data"
                );
                return ReloadOutcome::Rejected;
            }
        };
        let fresh = match build_python(
            &source,
            &label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            PythonBuildOptions {
                module_root: self.module_root.as_deref(),
                domain: &self.domain,
                maps: &self.maps,
                transform_hub: &self.transform_hub,
                static_data: fresh_static_data,
                outbound_http_policy: self.outbound_http_policy.clone(),
                http_endpoint_policy: self.http_endpoint_policy,
                event_bus_handle: Arc::clone(&self.event_bus_handle),
                shared_cache_handle: Arc::clone(&self.shared_cache_handle),
            },
        ) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "python hot-reload: new script rejected; keeping current runtime"
                );
                return ReloadOutcome::Rejected;
            }
        };
        apply_telemetry_slices(&fresh.citadel, &self.telemetry_slices);
        if !vm_has_any_handler(&fresh) {
            tracing::warn!(
                script = %label,
                "python hot-reload: new script registered no handlers; keeping current runtime"
            );
            return ReloadOutcome::Rejected;
        }
        {
            let mut guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
            *guard = fresh;
        }
        tracing::info!(
            script = %path.display(),
            "python hot-reload: swapped in updated script"
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

    /// Per-invocation budget used for non-tick handlers.
    #[must_use]
    pub fn budget(&self) -> Duration {
        self.budget
    }

    /// Dispatch a message handler.
    pub fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        self.run_commands("message", self.budget, |py, module| {
            let ctx = make_ctx(module, sender, user_id, Some(kind), None, None)?;
            let body = PyBytes::new(py, body);
            module
                .getattr("_dispatch_message")?
                .call1((kind, ctx, body))?
                .extract::<bool>()
        })
    }

    /// Dispatch a message with the authoritative room id exposed as
    /// `ctx.room_id`.
    pub fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        self.run_commands("match_message", self.budget, |py, module| {
            let ctx = make_ctx(module, sender, user_id, Some(kind), None, Some(room_id))?;
            let body = PyBytes::new(py, body);
            module
                .getattr("_dispatch_message")?
                .call1((kind, ctx, body))?
                .extract::<bool>()
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
    /// attached sink. No answer (no `on_input` handler, or a fault) delivers
    /// nothing — the fail-closed failure policy.
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
    /// Runs `citadel.on_input` once per event (via the shared JSON bridge) under
    /// the same VM lock, deadline, and error/panic isolation as every other
    /// handler, then maps the drained command sink to the batch-level
    /// [`ScriptCommand`]s. Returns `None` — no answer, fail-closed — when no
    /// `on_input` handler is registered or the invocation errors, times out, or
    /// panics.
    pub fn evaluate_event_batch(&self, batch: &NormalizedEventBatch) -> Option<ScriptCommandBatch> {
        let _runtime_scope = RuntimeScopeGuard::enter(Some(batch.match_id));
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(
                |_py| -> PyResult<Option<(Vec<crate::runtime::InputOutcome>, Vec<OutboundCommand>)>> {
                    let module = guard.citadel.bind(_py);
                    clear_commands(module);
                    run_with_python_deadline(module, self.budget, || {
                        let present: bool =
                            module.getattr("_has_on_input")?.call0()?.extract()?;
                        if !present {
                            clear_commands(module);
                            return Ok(None);
                        }
                        let dispatch = module.getattr("_dispatch_input")?;
                        let mut outcomes = Vec::with_capacity(batch.events.len());
                        for event in &batch.events {
                            let event_json = bridge_event_json(batch, event);
                            let decision = dispatch.call1((event_json,))?;
                            if decision.is_none() {
                                clear_commands(module);
                                return Ok(None);
                            }
                            let decision_json: String = decision.extract()?;
                            let outcome =
                                bridge_input_outcome_from_json(event.event_id, &decision_json)
                                    .map_err(PyRuntimeError::new_err)?;
                            outcomes.push(outcome);
                        }
                        let commands = take_commands(module, &guard.source_label, "on_input")?;
                        Ok(Some((outcomes, commands)))
                    })
                },
            )
        }));
        match outcome {
            Ok(Ok(Some((outcomes, commands)))) => {
                let mut answer = ScriptCommandBatch::answering(batch);
                answer.input_outcomes = outcomes;
                answer.commands = commands
                    .into_iter()
                    .map(script_command_from_outbound)
                    .collect();
                Some(answer)
            }
            Ok(Ok(None)) => None,
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "on_input",
                    error = %error,
                    "python on_input failed; batch fails closed (no answer)"
                );
                clear_vm_commands(&guard);
                None
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "on_input",
                    "python on_input panicked; batch fails closed (no answer)"
                );
                clear_vm_commands(&guard);
                None
            }
        }
    }

    /// Run the optional before-realtime interceptor. A `False` result vetoes the
    /// envelope; errors and deadline failures are isolated and fail closed.
    pub fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        self.run_before_realtime(|py, module| {
            let ctx = make_ctx(module, sender, user_id, Some(kind), None, room_id)?;
            ctx.setattr("body", PyBytes::new(py, body))?;
            module
                .getattr("_dispatch_before_realtime")?
                .call1((ctx, PyBytes::new(py, body)))?
                .extract::<bool>()
        })
    }

    /// Run the optional after-realtime observer. Commands it creates are drained
    /// and discarded because the gateway result is already fixed.
    pub fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        let _ = self.run_restricted_commands("after_realtime", self.budget, |py, module| {
            let ctx = make_ctx(module, sender, user_id, Some(kind), None, room_id)?;
            ctx.setattr("body", PyBytes::new(py, body))?;
            ctx.setattr("dropped", outcome.dropped)?;
            ctx.setattr("delivered", outcome.delivered)?;
            module
                .getattr("_dispatch_after_realtime")?
                .call1((ctx, PyBytes::new(py, body)))?
                .extract::<bool>()
        });
    }

    /// Dispatch `on_join` or `on_leave`.
    pub fn on_leaderboard_reset(
        &self,
        epoch: &crate::leaderboard_scheduler::ResetEpoch,
        fencing_token: crate::leaderboard_scheduler::SchedulerFencingToken,
    ) -> AppResult<()> {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| -> PyResult<()> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let ctx = PyDict::new(py);
                ctx.set_item("leaderboard_id", &epoch.leaderboard_id)?;
                ctx.set_item("due_at_unix_ms", epoch.due_at.unix_millis())?;
                ctx.set_item("fencing_token", fencing_token.get())?;
                run_with_python_deadline(module, self.budget, || {
                    module.getattr("_call_leaderboard_reset")?.call1((ctx,))?;
                    Ok(())
                })?;
                clear_commands(module);
                Ok(())
            })
        }));
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(AppError::internal("leaderboard reset callback failed")
                .with_detail(error.to_string())),
            Err(_) => Err(AppError::internal("leaderboard reset callback panicked")),
        }
    }

    /// Dispatch `on_join` or `on_leave`.
    pub fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        let hook_name = match hook {
            LifecycleHook::Join => "on_join",
            LifecycleHook::Leave => "on_leave",
        };
        self.run_commands(hook_name, self.budget, |_, module| {
            let ctx = make_ctx(module, sender, user_id, None, None, None)?;
            module
                .getattr("_dispatch_lifecycle")?
                .call1((hook_name, ctx))?
                .extract::<bool>()
        })
    }

    /// Dispatch a server-owned authoritative-match lifecycle callback.
    pub fn dispatch_match_lifecycle(
        &self,
        hook: NativeMatchLifecycleHook,
        context: NativeMatchContext,
        budget: Duration,
    ) -> Vec<OutboundCommand> {
        // Match lifecycle telemetry must use this server-owned context, never a
        // previous invocation's thread-local scope.
        let _runtime_scope = RuntimeScopeGuard::enter(Some(context.match_id));
        self.run_commands(hook.name(), budget, |py, module| {
            let callback_context = native_match_context_dict(py, &context)?;
            module
                .getattr("_dispatch_match_lifecycle")?
                .call1((hook.name(), callback_context))?
                .extract::<bool>()
        })
    }

    /// Dispatch the periodic tick handler with `dt` in seconds.
    pub fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        set_active_runtime_scope(None);
        let dt_secs = dt.as_secs_f64();
        self.run_commands("on_tick", budget, |_, module| {
            module
                .getattr("_dispatch_tick")?
                .call1((dt_secs,))?
                .extract::<bool>()
        })
    }

    /// Dispatch an RPC handler.
    pub fn call_rpc(
        &self,
        sender: u64,
        user_id: Option<&str>,
        method: &str,
        body: &[u8],
    ) -> RpcOutcome {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| -> PyResult<PythonRpcInner> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let result = run_with_python_deadline(module, self.budget, || {
                    let ctx = make_ctx(module, sender, user_id, None, Some(method), None)?;
                    let body = PyBytes::new(py, body);
                    let reply = module.getattr("_call_rpc")?.call1((method, ctx, body))?;
                    parse_rpc_reply(reply)
                });
                clear_commands(module);
                result
            })
        }));
        match outcome {
            Ok(Ok(PythonRpcInner::Reply(bytes))) => RpcOutcome::Ok(bytes),
            Ok(Ok(PythonRpcInner::HandlerErr(msg))) => RpcOutcome::Err(msg),
            Ok(Ok(PythonRpcInner::NoHandler)) => {
                tracing::debug!(
                    script = %guard.source_label,
                    method,
                    "no python rpc handler for method"
                );
                RpcOutcome::Err(format!("{RPC_ERR_UNKNOWN_METHOD}: {method}"))
            }
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    error = %e,
                    "python rpc handler error; isolated"
                );
                if is_timeout_error(&e) {
                    RpcOutcome::Err(RPC_ERR_TIMEOUT.to_string())
                } else {
                    RpcOutcome::Err(RPC_ERR_HANDLER.to_string())
                }
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    "python rpc handler panicked; isolated"
                );
                clear_vm_commands(&guard);
                RpcOutcome::Err(RPC_ERR_HANDLER.to_string())
            }
        }
    }

    /// Dispatch room-create hook.
    pub fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec> {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| -> PyResult<Option<RoomSpec>> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let result = run_with_python_deadline(module, self.budget, || {
                    let ctx = make_ctx(module, sender, user_id, None, Some("room.create"), None)?;
                    let params = PyBytes::new(py, params);
                    let spec = module.getattr("_call_room_create")?.call1((ctx, params))?;
                    parse_room_spec(spec)
                });
                clear_commands(module);
                result
            })
        }));
        match outcome {
            Ok(Ok(spec)) => spec,
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %e,
                    "python on_room_create error; isolated, using default label"
                );
                None
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "python on_room_create panicked; isolated"
                );
                clear_vm_commands(&guard);
                None
            }
        }
    }

    /// Dispatch room-join admission gate.
    pub fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| -> PyResult<Option<bool>> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let result = run_with_python_deadline(module, self.budget, || {
                    let ctx = make_ctx(
                        module,
                        sender,
                        user_id,
                        None,
                        Some("room.join"),
                        Some(room_id),
                    )?;
                    module
                        .getattr("_call_room_join")?
                        .call1((ctx, room_id))?
                        .extract::<Option<bool>>()
                });
                clear_commands(module);
                result
            })
        }));
        match outcome {
            Ok(Ok(decision)) => decision.unwrap_or(true),
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %e,
                    "python on_room_join error; isolated, rejecting"
                );
                false
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "python on_room_join panicked; isolated, rejecting"
                );
                clear_vm_commands(&guard);
                false
            }
        }
    }

    /// Whether an `on_tick` handler is registered.
    #[must_use]
    pub fn has_tick_handler(&self) -> bool {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        Python::attach(|py| -> PyResult<bool> {
            guard
                .citadel
                .bind(py)
                .getattr("_has_tick_handler")?
                .call0()?
                .extract()
        })
        .unwrap_or(false)
    }

    /// Point-in-time handler introspection for console/API surfaces.
    #[must_use]
    pub fn introspect(&self) -> RuntimeIntrospection {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let (rpcs, message_kinds, hooks) = Python::attach(|py| {
            guard
                .citadel
                .bind(py)
                .getattr("_introspect")?
                .call0()?
                .extract::<(Vec<String>, Vec<u32>, Vec<String>)>()
        })
        .unwrap_or_default();
        RuntimeIntrospection {
            source: format!("{} ({})", guard.source_label, guard.python_version),
            reloadable: self.reload_path.is_some(),
            deadline_ms: u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
            rpcs,
            message_kinds,
            hooks,
        }
    }

    /// Snapshot the endpoint declarations installed in the live Python VM.
    #[must_use]
    pub fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        guard
            .http_endpoints
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Invoke one script-defined endpoint with the normal Python deadline and
    /// panic-isolation boundary. Handler side-effect commands are discarded.
    pub fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        let guard = self.vm.lock().unwrap_or_else(|error| error.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Python::attach(|py| -> PyResult<RuntimeHttpOutcome> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let key = format!("{} {}", request.method.as_str(), request.path);
                let result = run_with_python_deadline(module, self.budget, || {
                    let value = PyDict::new(py);
                    value.set_item("method", request.method.as_str())?;
                    value.set_item("path", request.path)?;
                    value.set_item("body", PyBytes::new(py, &request.body))?;
                    value.set_item("user_id", request.user_id)?;
                    let headers = PyDict::new(py);
                    for (name, header) in request.headers {
                        headers.set_item(name, header)?;
                    }
                    value.set_item("headers", headers)?;
                    let result = module.getattr("_call_http_endpoint")?.call1((key, value))?;
                    if result.is_none() {
                        return Ok(RuntimeHttpOutcome::NotFound);
                    }
                    let (status, body, headers_json): (u16, Vec<u8>, String) = result.extract()?;
                    let headers = serde_json::from_str(&headers_json).map_err(|_| {
                        PyRuntimeError::new_err(
                            "runtime HTTP endpoint response headers are invalid",
                        )
                    })?;
                    Ok(RuntimeHttpOutcome::Response(RuntimeHttpResponse {
                        status,
                        headers,
                        body,
                    }))
                });
                clear_commands(module);
                result
            })
        }));
        match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %error,
                    "python runtime HTTP endpoint handler failed; isolated"
                );
                RuntimeHttpOutcome::Failed
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "python runtime HTTP endpoint handler panicked; isolated"
                );
                clear_vm_commands(&guard);
                RuntimeHttpOutcome::Failed
            }
        }
    }

    fn run_commands<F>(&self, what: &str, budget: Duration, call: F) -> Vec<OutboundCommand>
    where
        F: FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<bool>,
    {
        self.run_commands_with_mode(what, budget, false, call)
    }

    fn run_restricted_commands<F>(
        &self,
        what: &str,
        budget: Duration,
        call: F,
    ) -> Vec<OutboundCommand>
    where
        F: FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<bool>,
    {
        self.run_commands_with_mode(what, budget, true, call)
    }

    fn run_commands_with_mode<F>(
        &self,
        what: &str,
        budget: Duration,
        restricted: bool,
        call: F,
    ) -> Vec<OutboundCommand>
    where
        F: FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<bool>,
    {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let event_bus_handle = Arc::clone(&self.event_bus_handle);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            guard.interceptor_mode.store(restricted, Ordering::Relaxed);
            Python::attach(|py| -> PyResult<Vec<OutboundCommand>> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let ran = run_with_python_deadline(module, budget, || call(py, module));
                match ran {
                    Ok(true) => {
                        let mut commands = take_commands(module, &guard.source_label, what)?;
                        if !restricted {
                            append_runtime_event_commands(
                                &mut commands,
                                dispatch_pending_runtime_events(
                                    py,
                                    module,
                                    budget,
                                    &event_bus_handle,
                                    &guard.source_label,
                                ),
                                &guard.source_label,
                            );
                        }
                        Ok(commands)
                    }
                    Ok(false) => {
                        clear_commands(module);
                        if restricted {
                            Ok(Vec::new())
                        } else {
                            Ok(dispatch_pending_runtime_events(
                                py,
                                module,
                                budget,
                                &event_bus_handle,
                                &guard.source_label,
                            ))
                        }
                    }
                    Err(e) => {
                        clear_commands(module);
                        Err(e)
                    }
                }
            })
        }));
        guard.interceptor_mode.store(false, Ordering::Relaxed);
        match outcome {
            Ok(Ok(commands)) => commands,
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    error = %e,
                    "python handler error; isolated, side effects discarded"
                );
                Vec::new()
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    "python handler panicked; isolated and dropped"
                );
                clear_vm_commands(&guard);
                Vec::new()
            }
        }
    }

    /// Run one pre-routing decision under the normal lock/deadline boundary.
    /// Commands are cleared regardless of the return value, and every failure
    /// becomes a veto so a partially broken interceptor cannot admit traffic.
    fn run_before_realtime<F>(&self, call: F) -> RealtimeInterception
    where
        F: FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<bool>,
    {
        let guard = self.vm.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            guard.interceptor_mode.store(true, Ordering::Relaxed);
            Python::attach(|py| -> PyResult<bool> {
                let module = guard.citadel.bind(py);
                clear_commands(module);
                let decision = run_with_python_deadline(module, self.budget, || call(py, module));
                clear_commands(module);
                decision
            })
        }));
        guard.interceptor_mode.store(false, Ordering::Relaxed);
        match outcome {
            Ok(Ok(true)) => RealtimeInterception::Continue,
            Ok(Ok(false)) => RealtimeInterception::Drop,
            Ok(Err(error)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    error = %error,
                    "python realtime interceptor failed; vetoing envelope"
                );
                clear_vm_commands(&guard);
                RealtimeInterception::Drop
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    "python realtime interceptor panicked; vetoing envelope"
                );
                clear_vm_commands(&guard);
                RealtimeInterception::Drop
            }
        }
    }
}

/// Apply the read-only map catalog bridge to a freshly-built Python VM.
fn apply_map_catalog(citadel: &Py<PyModule>, maps: &Option<Arc<MapCatalog>>) {
    if let Some(maps) = maps {
        Python::attach(|py| {
            if let Err(e) = citadel.bind(py).setattr(
                "_map_catalog_bridge",
                MapCatalogBridge {
                    maps: Arc::clone(maps),
                },
            ) {
                tracing::warn!(error = %e, "failed to set map catalog bridge on python module");
            }
        });
    }
}

/// Apply the synchronous transform-physics read bridge to a freshly-built VM.
fn apply_telemetry_slices(citadel: &Py<PyModule>, slices: &Option<Arc<TelemetrySliceService>>) {
    if let Some(slices) = slices {
        Python::attach(|py| {
            if let Err(e) = citadel.bind(py).setattr(
                "_telemetry_slices_bridge",
                TelemetrySlicesHandle {
                    slices: Arc::clone(slices),
                },
            ) {
                tracing::warn!(error = %e, "failed to set telemetry slices bridge on python module");
            }
        });
    }
}

fn apply_transform_hub(citadel: &Py<PyModule>, hub: &Option<Arc<TransformHub>>) {
    if let Some(hub) = hub {
        Python::attach(|py| {
            if let Err(e) = citadel.bind(py).setattr(
                "_transform_hub_bridge",
                TransformHubHandle {
                    hub: Arc::clone(hub),
                },
            ) {
                tracing::warn!(error = %e, "failed to set transform hub bridge on python module");
            }
        });
    }
}

fn native_match_context_dict(py: Python<'_>, context: &NativeMatchContext) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("match_id", context.match_id)?;
    result.set_item("lifecycle_generation", context.lifecycle_generation)?;
    result.set_item("clock_epoch", context.clock_epoch)?;
    result.set_item("tick", context.tick)?;
    result.set_item("participants", &context.participants)?;
    result.set_item("map", &context.map)?;
    result.set_item("mode", &context.mode)?;
    result.set_item("max_players", context.max_players)?;
    result.set_item("open", context.open)?;
    result.set_item("termination_reason", &context.termination_reason)?;
    Ok(result.unbind())
}

impl Runtime for PythonRuntime {
    fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        PythonRuntime::before_realtime(self, sender, user_id, room_id, kind, body)
    }

    fn attach_bridge_sink(&self, sink: Weak<dyn BridgeCommandSink>) {
        PythonRuntime::attach_bridge_sink(self, sink);
    }

    fn deliver_event_batch(&self, batch: NormalizedEventBatch) {
        PythonRuntime::deliver_event_batch(self, batch);
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
        PythonRuntime::after_realtime(self, sender, user_id, room_id, kind, body, outcome);
    }

    fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        PythonRuntime::dispatch(self, sender, user_id, kind, body)
    }

    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        PythonRuntime::dispatch_in_room(self, sender, user_id, room_id, kind, body)
    }

    fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        PythonRuntime::dispatch_lifecycle(self, hook, sender, user_id)
    }

    fn dispatch_match_lifecycle(
        &self,
        hook: NativeMatchLifecycleHook,
        context: NativeMatchContext,
        budget: Duration,
    ) -> Vec<OutboundCommand> {
        PythonRuntime::dispatch_match_lifecycle(self, hook, context, budget)
    }

    fn supports_native_match_lifecycle(&self) -> bool {
        true
    }

    fn on_leaderboard_reset(
        &self,
        epoch: &crate::leaderboard_scheduler::ResetEpoch,
        fencing_token: crate::leaderboard_scheduler::SchedulerFencingToken,
    ) -> AppResult<()> {
        PythonRuntime::on_leaderboard_reset(self, epoch, fencing_token)
    }

    fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        PythonRuntime::tick(self, dt, budget)
    }

    fn call_rpc(
        &self,
        sender: u64,
        user_id: Option<&str>,
        method: &str,
        body: &[u8],
    ) -> RpcOutcome {
        PythonRuntime::call_rpc(self, sender, user_id, method, body)
    }

    fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec> {
        PythonRuntime::call_room_create(self, sender, user_id, params)
    }

    fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        PythonRuntime::call_room_join(self, sender, user_id, room_id)
    }

    fn http_endpoints(&self) -> Vec<RuntimeHttpEndpoint> {
        PythonRuntime::http_endpoints(self)
    }

    fn call_http_endpoint(&self, request: RuntimeHttpRequest) -> RuntimeHttpOutcome {
        PythonRuntime::call_http_endpoint(self, request)
    }

    fn has_tick_handler(&self) -> bool {
        PythonRuntime::has_tick_handler(self)
    }

    fn budget(&self) -> Duration {
        PythonRuntime::budget(self)
    }

    fn introspect(&self) -> RuntimeIntrospection {
        PythonRuntime::introspect(self)
    }

    fn is_reloadable(&self) -> bool {
        PythonRuntime::is_reloadable(self)
    }

    fn reload(&self) -> ReloadOutcome {
        PythonRuntime::reload(self)
    }

    fn reload_watch_paths(&self) -> Vec<PathBuf> {
        PythonRuntime::reload_watch_paths(self)
    }
}

enum PythonRpcInner {
    Reply(Vec<u8>),
    HandlerErr(String),
    NoHandler,
}

/// Apply the domain-host seam to a freshly-built VM's citadel module,
/// registering the friends host functions if a domain is provided.
/// Called after each VM build (initial + hot-reload).
fn apply_domain_host(
    citadel: &Py<PyModule>,
    domain: &Option<Arc<dyn DomainHost>>,
    interceptor_mode: Arc<AtomicBool>,
) {
    if let Some(host) = domain {
        Python::attach(|py| {
            let module = citadel.bind(py);
            // Create and set the bridge object so friends functions can call through to Rust
            let bridge = DomainHostBridge {
                host: Arc::clone(host),
                interceptor_mode,
            };
            if let Err(e) = module.setattr("_domain_host_bridge", bridge) {
                tracing::warn!(error = %e, "failed to set domain host bridge on python module");
            }
        });
    }
}

/// Install the small static-data capability before game-script initialization.
/// The Python script receives only a bridge returning parsed values; it never
/// receives a filesystem path, file descriptor, or directory handle.
fn install_static_data(
    citadel: &Bound<'_, PyModule>,
    static_data: StaticDataCatalog,
) -> PyResult<()> {
    citadel.setattr(
        "static_data",
        StaticDataBridge {
            catalog: static_data,
        },
    )
}

fn install_text_policy(
    citadel: &Bound<'_, PyModule>,
    text_policy: TextPolicyCatalog,
) -> PyResult<()> {
    citadel.setattr(
        "text_policy",
        TextPolicyBridge {
            catalog: text_policy,
        },
    )
}

fn install_outbound_http(
    citadel: &Bound<'_, PyModule>,
    interceptor_mode: Arc<AtomicBool>,
    policy: OutboundHttpPolicy,
) -> PyResult<()> {
    let client = TrustedHttpClient::new_with_policy(policy)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let async_client = AsyncOutboundHttp::new(client.clone());
    citadel.setattr(
        "_http_bridge",
        OutboundHttpBridge {
            client: async_client,
            fetch_client: client,
            interceptor_mode,
        },
    )
}

fn install_http_endpoint_registration(
    citadel: &Bound<'_, PyModule>,
    policy: RuntimeHttpEndpointPolicy,
    endpoints: Arc<Mutex<BTreeSet<RuntimeHttpEndpoint>>>,
) -> PyResult<()> {
    if !policy.enabled {
        return Ok(());
    }
    citadel.setattr(
        "_http_endpoint_registry",
        RuntimeHttpEndpointRegistry { endpoints },
    )
}

fn install_runtime_events(
    citadel: &Bound<'_, PyModule>,
    event_bus_handle: RuntimeEventBusHandle,
    interceptor_mode: Arc<AtomicBool>,
) -> PyResult<()> {
    citadel.setattr(
        "_event_bus_bridge",
        RuntimeEventBusBridge {
            event_bus_handle,
            interceptor_mode,
        },
    )
}

fn install_runtime_shared_cache(
    citadel: &Bound<'_, PyModule>,
    shared_cache_handle: RuntimeSharedCacheHandle,
    interceptor_mode: Arc<AtomicBool>,
) -> PyResult<()> {
    citadel.setattr(
        "_shared_cache_bridge",
        RuntimeSharedCacheBridge {
            shared_cache_handle,
            interceptor_mode,
        },
    )
}

fn install_realtime_interceptor_guard(
    citadel: &Bound<'_, PyModule>,
    interceptor_mode: Arc<AtomicBool>,
) -> PyResult<()> {
    let guard = Py::new(citadel.py(), RuntimeModeBridge { interceptor_mode })?;
    let register = citadel
        .getattr("_make_register_storage_index_filter")?
        .call1((guard,))?;
    citadel.setattr("register_storage_index_filter", register)
}

fn static_data_python_error(error: crate::runtime::static_data::StaticDataError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// Convert parsed catalog JSON into ordinary, mutable Python dictionaries and
/// lists. The catalog remains the immutable cached source, so a script can
/// reshape its returned value without mutating cache entries or future reloads.
fn static_data_value_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    py.import("json")?
        .call_method1("loads", (value.to_string(),))
        .map(Bound::unbind)
}

struct PythonBuildOptions<'a> {
    module_root: Option<&'a Path>,
    domain: &'a Option<Arc<dyn DomainHost>>,
    maps: &'a Option<Arc<MapCatalog>>,
    transform_hub: &'a Option<Arc<TransformHub>>,
    static_data: StaticDataCatalog,
    outbound_http_policy: OutboundHttpPolicy,
    http_endpoint_policy: RuntimeHttpEndpointPolicy,
    event_bus_handle: RuntimeEventBusHandle,
    shared_cache_handle: RuntimeSharedCacheHandle,
}

fn build_python(
    source: &str,
    source_label: &str,
    load_budget: Duration,
    options: PythonBuildOptions<'_>,
) -> AppResult<PythonVm> {
    let PythonBuildOptions {
        module_root,
        domain,
        maps,
        transform_hub,
        static_data,
        outbound_http_policy,
        http_endpoint_policy,
        event_bus_handle,
        shared_cache_handle,
    } = options;
    let prelude = cstring(PYTHON_HOST_PRELUDE, "python host prelude")?;
    let source = cstring(source, "python source")?;
    let filename = cstring(source_label, "python source label")?;
    let module_id = PYTHON_MODULE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let main_module_name = cstring(
        &format!("citadel_game_main_{module_id}"),
        "python main module name",
    )?;
    let host_module_name = cstring(
        &format!("citadel_host_{module_id}"),
        "python host module name",
    )?;
    let interceptor_mode = Arc::new(AtomicBool::new(false));
    let text_policy = TextPolicyCatalog::new(static_data.clone());
    let http_endpoints = Arc::new(Mutex::new(BTreeSet::new()));
    let _build_guard = PYTHON_BUILD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    Python::attach(|py| -> PyResult<PythonVm> {
        let citadel = PyModule::from_code(
            py,
            prelude.as_c_str(),
            c"citadel.py",
            host_module_name.as_c_str(),
        )?;
        let sys = py.import("sys")?;
        let modules: Bound<'_, PyDict> = sys.getattr("modules")?.cast_into()?;
        modules.set_item("citadel", &citadel)?;
        install_static_data(&citadel, static_data.clone())?;
        install_text_policy(&citadel, text_policy.clone())?;
        install_outbound_http(
            &citadel,
            Arc::clone(&interceptor_mode),
            outbound_http_policy,
        )?;
        install_runtime_events(
            &citadel,
            Arc::clone(&event_bus_handle),
            Arc::clone(&interceptor_mode),
        )?;
        install_runtime_shared_cache(
            &citadel,
            Arc::clone(&shared_cache_handle),
            Arc::clone(&interceptor_mode),
        )?;
        install_http_endpoint_registration(
            &citadel,
            http_endpoint_policy,
            Arc::clone(&http_endpoints),
        )?;
        install_realtime_interceptor_guard(&citadel, Arc::clone(&interceptor_mode))?;
        if let Some(root) = module_root {
            let root = root.to_string_lossy().to_string();
            citadel.getattr("_prepare_imports")?.call1((root,))?;
        }
        run_with_python_deadline(&citadel, load_budget, || {
            PyModule::from_code(
                py,
                source.as_c_str(),
                filename.as_c_str(),
                main_module_name.as_c_str(),
            )?;
            Ok(())
        })?;
        static_data.seal();
        text_policy.seal();
        let version = sys.getattr("version")?.extract::<String>()?;
        let vm = PythonVm {
            citadel: citadel.unbind(),
            source_label: source_label.to_string(),
            python_version: version,
            interceptor_mode,
            static_data,
            http_endpoints,
        };
        Ok(vm)
    })
    .map_err(|e| script_error(&format!("failed to load {source_label}"), &e))
    .inspect(|vm| {
        apply_domain_host(&vm.citadel, domain, Arc::clone(&vm.interceptor_mode));
        apply_map_catalog(&vm.citadel, maps);
        apply_transform_hub(&vm.citadel, transform_hub);
    })
}

fn run_with_python_deadline<T>(
    module: &Bound<'_, PyModule>,
    budget: Duration,
    call: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    let watchdog = WatchdogGuard::new(budget);
    module
        .getattr("_arm_deadline")?
        .call1((budget.as_secs_f64(),))?;
    let result = call();
    let clear_result = module.getattr("_clear_deadline")?.call0();
    match (result, clear_result) {
        (Ok(_), Ok(_)) if watchdog.expired() => {
            Err(PyRuntimeError::new_err("handler exceeded its time budget"))
        }
        (Ok(value), Ok(_)) => Ok(value),
        (Err(e), _) => Err(e),
        (Ok(_), Err(e)) => Err(e),
    }
}

fn make_ctx<'py>(
    module: &Bound<'py, PyModule>,
    sender: u64,
    user_id: Option<&str>,
    kind: Option<u16>,
    method: Option<&str>,
    room_id: Option<u64>,
) -> PyResult<Bound<'py, PyAny>> {
    set_active_runtime_scope(room_id);
    module
        .getattr("_make_ctx")?
        .call1((sender, user_id, kind, method, room_id))
}

fn clear_commands(module: &Bound<'_, PyModule>) {
    if let Err(e) = module.getattr("_reset_commands").and_then(|f| f.call0()) {
        tracing::warn!(error = %e, "failed to clear python command sink");
    }
}

fn clear_vm_commands(vm: &PythonVm) {
    let _ = Python::attach(|py| -> PyResult<()> {
        clear_commands(vm.citadel.bind(py));
        Ok(())
    });
}

fn take_commands(
    module: &Bound<'_, PyModule>,
    label: &str,
    handler: &str,
) -> PyResult<Vec<OutboundCommand>> {
    let taken = module.getattr("_take_commands")?.call0()?;
    let tuple: Bound<'_, PyTuple> = taken.cast_into()?;
    let commands_obj = tuple.get_item(0)?;
    let overflowed = tuple.get_item(1)?.extract::<bool>()?;
    if overflowed {
        tracing::warn!(
            script = %label,
            handler,
            cap = MAX_OUTBOUND_COMMANDS,
            "python handler exceeded outbound command cap; extra commands dropped"
        );
    }
    parse_commands(commands_obj)
}

fn runtime_event_key(namespace: &str, event_type: &str) -> String {
    format!("{namespace}\0{event_type}")
}

/// Deliver the event snapshot that existed after an outer Python handler. A
/// callback error is isolated and events emitted by callbacks stay queued for
/// the following outer invocation.
fn dispatch_pending_runtime_events(
    py: Python<'_>,
    module: &Bound<'_, PyModule>,
    budget: Duration,
    event_bus_handle: &RuntimeEventBusHandle,
    label: &str,
) -> Vec<OutboundCommand> {
    let count = match module.getattr("_runtime_event_subscriber_count") {
        Ok(count) => count,
        Err(error) => {
            tracing::error!(script = %label, error = %error, "python runtime event bridge unavailable");
            return Vec::new();
        }
    };
    let call = match module.getattr("_call_runtime_event_subscriber") {
        Ok(call) => call,
        Err(error) => {
            tracing::error!(script = %label, error = %error, "python runtime event bridge unavailable");
            return Vec::new();
        }
    };
    let event_bus = runtime_event_bus(event_bus_handle);
    let mut commands = Vec::new();
    let delivery_deadline = Instant::now() + budget;
    let mut events = event_bus
        .drain_snapshot_limit(MAX_RUNTIME_EVENTS_PER_INVOCATION)
        .into_iter()
        .peekable();
    while events.peek().is_some() {
        let Some(remaining) = delivery_deadline.checked_duration_since(Instant::now()) else {
            tracing::warn!(script = %label, "runtime event delivery budget exhausted; pending events deferred");
            event_bus.requeue_front(events.collect());
            break;
        };
        let event = events.next().expect("peeked event exists");
        let key = runtime_event_key(&event.namespace, &event.event_type);
        let subscriber_count = match run_with_python_deadline(module, remaining, || {
            count
                .call1((key.as_str(),))
                .and_then(|value| value.extract::<usize>())
        }) {
            Ok(count) => count.min(MAX_RUNTIME_EVENT_SUBSCRIBERS),
            Err(error) => {
                tracing::error!(script = %label, error = %error, "python runtime event subscriber lookup failed; isolated");
                continue;
            }
        };
        for subscriber_index in 0..subscriber_count {
            let Some(remaining) = delivery_deadline.checked_duration_since(Instant::now()) else {
                tracing::warn!(script = %label, "runtime event delivery budget exhausted; pending events deferred");
                event_bus.requeue_front(events.collect());
                return commands;
            };
            let subscribers_remaining = subscriber_count - subscriber_index;
            let subscriber_budget = remaining / subscribers_remaining as u32;
            if subscriber_budget.is_zero() {
                tracing::warn!(script = %label, "runtime event delivery budget exhausted; pending events deferred");
                event_bus.requeue_front(events.collect());
                return commands;
            }
            clear_commands(module);
            let result = run_with_python_deadline(module, subscriber_budget, || -> PyResult<()> {
                let value = PyDict::new(py);
                value.set_item("namespace", &event.namespace)?;
                value.set_item("type", &event.event_type)?;
                value.set_item("payload", PyBytes::new(py, &event.payload))?;
                call.call1((key.as_str(), subscriber_index, value))?;
                Ok(())
            });
            match result {
                Ok(()) => match take_commands(module, label, "runtime_event") {
                    Ok(event_commands) => {
                        append_runtime_event_commands(&mut commands, event_commands, label)
                    }
                    Err(error) => {
                        tracing::error!(script = %label, error = %error, "python runtime event commands could not be drained");
                        clear_commands(module);
                    }
                },
                Err(error) => {
                    tracing::error!(
                        script = %label,
                        namespace = %event.namespace,
                        event_type = %event.event_type,
                        subscriber_index,
                        error = %error,
                        "python runtime event subscriber failed; isolated"
                    );
                    clear_commands(module);
                }
            }
        }
    }
    commands
}

fn parse_commands(commands: Bound<'_, PyAny>) -> PyResult<Vec<OutboundCommand>> {
    let list: Bound<'_, PyList> = commands.cast_into()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let tuple: Bound<'_, PyTuple> = item.cast_into()?;
        let tag = tuple.get_item(0)?.extract::<String>()?;
        let command = match tag.as_str() {
            "broadcast" => OutboundCommand::Broadcast {
                kind: tuple.get_item(1)?.extract()?,
                body: tuple.get_item(2)?.extract()?,
                unreliable: tuple.get_item(3)?.extract()?,
            },
            "send" => OutboundCommand::Send {
                session: tuple.get_item(1)?.extract()?,
                kind: tuple.get_item(2)?.extract()?,
                body: tuple.get_item(3)?.extract()?,
                unreliable: tuple.get_item(4)?.extract()?,
            },
            "spawn_actor" => OutboundCommand::SpawnActor {
                object_id: tuple.get_item(1)?.extract()?,
                archetype: tuple.get_item(2)?.extract()?,
                position: [
                    tuple.get_item(3)?.extract()?,
                    tuple.get_item(4)?.extract()?,
                    tuple.get_item(5)?.extract()?,
                ],
            },
            "move_actor" => OutboundCommand::MoveActor {
                object_id: tuple.get_item(1)?.extract()?,
                position: [
                    tuple.get_item(2)?.extract()?,
                    tuple.get_item(3)?.extract()?,
                    tuple.get_item(4)?.extract()?,
                ],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [
                    tuple.get_item(5)?.extract()?,
                    tuple.get_item(6)?.extract()?,
                    tuple.get_item(7)?.extract()?,
                ],
            },
            "despawn_actor" => OutboundCommand::DespawnActor {
                object_id: tuple.get_item(1)?.extract()?,
            },
            "set_physics" => {
                let options_json: Option<String> = tuple.get_item(2)?.extract()?;
                let opts = options_json
                    .as_deref()
                    .map(physics_options_from_json)
                    .transpose()
                    .map_err(PyRuntimeError::new_err)?;
                OutboundCommand::SetPhysics {
                    object_id: tuple.get_item(1)?.extract()?,
                    opts,
                }
            }
            "apply_impulse" => OutboundCommand::ApplyImpulse {
                object_id: tuple.get_item(1)?.extract()?,
                impulse: [
                    tuple.get_item(2)?.extract()?,
                    tuple.get_item(3)?.extract()?,
                    tuple.get_item(4)?.extract()?,
                ],
            },
            "set_move_intent" => OutboundCommand::SetMoveIntent {
                object_id: tuple.get_item(1)?.extract()?,
                intent: [
                    tuple.get_item(2)?.extract()?,
                    tuple.get_item(3)?.extract()?,
                    tuple.get_item(4)?.extract()?,
                ],
            },
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown outbound command tag: {other}"
                )));
            }
        };
        out.push(command);
    }
    Ok(out)
}

fn physics_options_from_json(input: &str) -> Result<PhysicsOptions, String> {
    #[derive(serde::Deserialize)]
    struct Input {
        gravity: Option<f32>,
        buoyancy: Option<f32>,
        drag: Option<f32>,
        radius: Option<f32>,
        height: Option<f32>,
        max_speed: Option<f32>,
        shape: Option<String>,
        enabled: Option<bool>,
    }

    let input: Input = serde_json::from_str(input)
        .map_err(|e| format!("physics options must be an object: {e}"))?;
    let mut config = PhysicsConfig::default();
    config.gravity = input.gravity.unwrap_or(config.gravity);
    config.buoyancy = input.buoyancy.unwrap_or(config.buoyancy);
    config.drag = input.drag.unwrap_or(config.drag);
    config.max_speed = input.max_speed.unwrap_or(config.max_speed);
    let (default_radius, default_height) = match config.shape {
        Shape::Capsule { radius, height } => (radius, height),
        Shape::Aabb { half_extents } => (half_extents[0], half_extents[1] * 2.0),
    };
    let radius = input.radius.unwrap_or(default_radius);
    let height = input.height.unwrap_or(default_height);
    config.shape = match input.shape.as_deref() {
        None | Some("capsule") => Shape::Capsule { radius, height },
        Some("aabb") => Shape::Aabb {
            half_extents: [radius, height * 0.5, radius],
        },
        Some(shape) => {
            return Err(format!(
                "unsupported physics shape '{shape}' (expected 'capsule' or 'aabb')"
            ));
        }
    };
    Ok(PhysicsOptions {
        enabled: input.enabled.unwrap_or(true),
        config,
    })
}

fn parse_rpc_reply(reply: Bound<'_, PyAny>) -> PyResult<PythonRpcInner> {
    if reply.is_none() {
        return Ok(PythonRpcInner::NoHandler);
    }
    let tuple: Bound<'_, PyTuple> = reply.cast_into()?;
    let ok = tuple.get_item(0)?.extract::<bool>()?;
    if ok {
        Ok(PythonRpcInner::Reply(tuple.get_item(1)?.extract()?))
    } else {
        Ok(PythonRpcInner::HandlerErr(
            tuple.get_item(2)?.extract::<String>()?,
        ))
    }
}

fn parse_room_spec(spec: Bound<'_, PyAny>) -> PyResult<Option<RoomSpec>> {
    if spec.is_none() {
        return Ok(None);
    }
    let tuple: Bound<'_, PyTuple> = spec.cast_into()?;
    Ok(Some(RoomSpec {
        map: tuple.get_item(0)?.extract()?,
        mode: tuple.get_item(1)?.extract()?,
        max_players: tuple.get_item(2)?.extract()?,
        open: tuple.get_item(3)?.extract()?,
    }))
}

fn vm_has_any_handler(vm: &PythonVm) -> bool {
    Python::attach(|py| -> PyResult<bool> {
        vm.citadel
            .bind(py)
            .getattr("_has_any_handler")?
            .call0()?
            .extract()
    })
    .unwrap_or(false)
}

fn is_timeout_error(err: &PyErr) -> bool {
    let text = err.to_string();
    text.contains("time budget")
        || text.contains("TimeoutError")
        || text.contains("KeyboardInterrupt")
}

fn read_script(path: &Path) -> AppResult<String> {
    std::fs::read_to_string(path).map_err(|e| {
        AppError::new(
            ErrorCategory::Runtime,
            format!("cannot read Python game script: {}", path.display()),
        )
        .with_detail(e.to_string())
    })
}

fn cstring(value: &str, label: &str) -> AppResult<CString> {
    CString::new(value).map_err(|_| {
        AppError::new(
            ErrorCategory::Runtime,
            format!("{label} contains an interior NUL byte"),
        )
    })
}

fn script_error(context: &str, err: &PyErr) -> AppError {
    AppError::new(ErrorCategory::Runtime, context.to_string()).with_detail(err.to_string())
}

struct WatchdogState {
    done: Mutex<bool>,
    done_changed: Condvar,
    expired: AtomicBool,
}

struct WatchdogGuard {
    state: Arc<WatchdogState>,
    handle: Option<JoinHandle<()>>,
}

impl WatchdogGuard {
    fn new(budget: Duration) -> Self {
        let state = Arc::new(WatchdogState {
            done: Mutex::new(false),
            done_changed: Condvar::new(),
            expired: AtomicBool::new(false),
        });
        let thread_state = Arc::clone(&state);
        let handle = match std::thread::Builder::new()
            .name("citadel-python-watchdog".to_string())
            .spawn(move || watchdog_thread(thread_state, budget))
        {
            Ok(handle) => handle,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to spawn python deadline watchdog; timeout overrun will rely on the trace deadline"
                );
                return Self {
                    state,
                    handle: None,
                };
            }
        };
        Self {
            state,
            handle: Some(handle),
        }
    }

    fn expired(&self) -> bool {
        self.state.expired.load(Ordering::Acquire)
    }
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        match self.state.done.lock() {
            Ok(mut done) => {
                *done = true;
                self.state.done_changed.notify_all();
            }
            Err(poisoned) => {
                let mut done = poisoned.into_inner();
                *done = true;
                self.state.done_changed.notify_all();
            }
        }
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            tracing::warn!("python deadline watchdog thread panicked; ignored");
        }
    }
}

fn watchdog_thread(state: Arc<WatchdogState>, budget: Duration) {
    let done = state.done.lock().unwrap_or_else(|e| e.into_inner());
    let timed_out = match state
        .done_changed
        .wait_timeout_while(done, budget, |done| !*done)
    {
        Ok((done, timeout)) => !*done && timeout.timed_out(),
        Err(poisoned) => {
            let (done, timeout) = poisoned.into_inner();
            !*done && timeout.timed_out()
        }
    };
    if !timed_out {
        return;
    }
    // The Python trace hook runs in the handler's own interpreter thread and
    // raises `TimeoutError` at the next bytecode boundary. Do not attempt to
    // attach from this thread: while a handler holds the GIL, that blocks here
    // and `WatchdogGuard::drop` would then deadlock joining this worker.
    state.expired.store(true, Ordering::Release);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::collections::HashSet;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Instant;

    use super::*;
    use crate::runtime::HOST_API_SURFACE;
    use crate::runtime::host_api_spec::HostApiStatus;

    const NPC_ID_BASE: u32 = 0x4000_0000;

    fn runtime(src: &str) -> PythonRuntime {
        PythonRuntime::from_source(src, "test.py", 100).expect("python runtime loads")
    }

    #[test]
    fn native_match_lifecycle_telemetry_uses_its_context_and_restores_prior_scope() {
        let slices = Arc::new(TelemetrySliceService::new(
            Arc::new(
                crate::authoritative_decision_telemetry::AuthoritativeDecisionRecorder::new(16),
            ),
            crate::authoritative_telemetry_slices::TelemetrySlicePolicy::default(),
        ));
        let runtime = runtime(
            r#"
import citadel

@citadel.on_match_tick
def lifecycle(context):
    citadel.telemetry.begin()
    citadel.telemetry.mark("match.lifecycle")
    citadel.telemetry.finish()
"#,
        )
        .with_telemetry_slices(Arc::clone(&slices));

        set_active_runtime_scope(Some(41));
        assert!(
            runtime
                .dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Tick,
                    NativeMatchContext {
                        match_id: 42,
                        lifecycle_generation: 1,
                        clock_epoch: 0,
                        tick: 7,
                        participants: Vec::new(),
                        map: "arena".to_owned(),
                        mode: "duel".to_owned(),
                        max_players: 2,
                        open: true,
                        termination_reason: None,
                    },
                    Duration::from_millis(100),
                )
                .is_empty()
        );
        assert_eq!(
            active_runtime_context(),
            Some(crate::authoritative_telemetry_slices::TelemetrySliceContext::match_context(41))
        );
        set_active_runtime_scope(None);

        let reports = slices.list_closed(1);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].context_kind, "match");
        assert_eq!(reports[0].marker_total, 1);
    }

    // ---- authoritative bridge: citadel.on_input parity ----

    #[derive(Default)]
    struct RecordingBridgeSink(Mutex<Vec<ScriptCommandBatch>>);

    impl BridgeCommandSink for RecordingBridgeSink {
        fn deliver_command_batch(&self, answer: ScriptCommandBatch) {
            self.0.lock().unwrap().push(answer);
        }
    }

    fn input_event(event_id: u64, object_id: u32) -> crate::runtime::NormalizedEvent {
        crate::runtime::NormalizedEvent {
            event_id,
            participant: 1001,
            user_id: None,
            payload: crate::runtime::NormalizedPayload::TransformInput {
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

    fn batch_with(events: Vec<crate::runtime::NormalizedEvent>) -> NormalizedEventBatch {
        let mut batch = NormalizedEventBatch::new(5, 42, 9, 100, 1);
        batch.events = events;
        batch
    }

    #[test]
    fn on_input_telemetry_uses_batch_match_and_restores_prior_scope_after_error() {
        let slices = Arc::new(TelemetrySliceService::new(
            Arc::new(
                crate::authoritative_decision_telemetry::AuthoritativeDecisionRecorder::new(16),
            ),
            crate::authoritative_telemetry_slices::TelemetrySlicePolicy::default(),
        ));
        let runtime = runtime(
            "import citadel\ndef h(e):\n    citadel.telemetry.begin()\n    citadel.telemetry.mark(\"match.input\")\n    citadel.telemetry.finish()\n    raise RuntimeError(\"boom\")\ncitadel.on_input(h)\n",
        )
        .with_telemetry_slices(Arc::clone(&slices));
        slices
            .begin(
                crate::authoritative_telemetry_slices::TelemetrySliceContext::match_context(41),
                SystemClock.now().unix_millis(),
            )
            .expect("prior match slice begins");
        let _prior_scope = RuntimeScopeGuard::enter(Some(41));

        assert!(
            runtime
                .evaluate_event_batch(&batch_with(vec![input_event(1, 7)]))
                .is_none()
        );
        assert_eq!(
            active_runtime_context(),
            Some(crate::authoritative_telemetry_slices::TelemetrySliceContext::match_context(41))
        );
        assert!(
            slices
                .finish(
                    crate::authoritative_telemetry_slices::TelemetrySliceContext::match_context(41),
                    SystemClock.now().unix_millis(),
                )
                .is_ok(),
            "the batch must not close the pre-existing match's telemetry slice"
        );
        assert!(
            slices
                .list_closed(2)
                .iter()
                .any(|report| report.marker_total == 1),
            "the failing batch still closes its own telemetry slice"
        );
    }

    #[test]
    fn on_input_accepts_on_none_return() {
        let rt = runtime("import citadel\ndef h(e):\n    return None\ncitadel.on_input(h)\n");
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(answer.input_outcomes.len(), 1);
        assert_eq!(
            answer.input_outcomes[0].decision,
            crate::runtime::Decision::Accept
        );
        assert_eq!(answer.generation, 5);
        assert_eq!(answer.batch_id, 1);
    }

    #[test]
    fn on_input_reject_dict_carries_reason_and_reply() {
        let rt = runtime(
            "import citadel\ndef h(e):\n    return {\"decision\": \"reject\", \"reason_code\": 7, \"reply\": \"no\"}\ncitadel.on_input(h)\n",
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(
            answer.input_outcomes[0].decision,
            crate::runtime::Decision::Reject { reason_code: 7 }
        );
        assert_eq!(
            answer.input_outcomes[0].reply.as_deref(),
            Some(b"no".as_ref())
        );
    }

    #[test]
    fn on_input_correct_returns_a_transform_correction() {
        let rt = runtime(
            "import citadel\ndef h(e):\n    return {\"decision\": \"correct\", \"transform\": {\"position\": [1, 2, 3], \"rotation\": [0, 0, 0, 1], \"velocity\": [4, 5, 6]}}\ncitadel.on_input(h)\n",
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        match &answer.input_outcomes[0].decision {
            crate::runtime::Decision::Correct {
                correction: crate::runtime::Correction::Transform(t),
            } => {
                assert_eq!(t.position, [1.0, 2.0, 3.0]);
                assert_eq!(t.velocity, [4.0, 5.0, 6.0]);
            }
            other => panic!("expected a transform correction, got {other:?}"),
        }
    }

    #[test]
    fn on_input_broadcast_maps_to_a_match_broadcast_command() {
        let rt = runtime(
            "import citadel\ndef h(e):\n    citadel.broadcast(100, \"hi\", True)\ncitadel.on_input(h)\n",
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
    fn no_on_input_handler_fails_closed() {
        let rt = runtime("import citadel\ndef h(ctx, body):\n    pass\ncitadel.on_message(1, h)\n");
        let batch = batch_with(vec![input_event(1, 7)]);
        assert!(rt.evaluate_event_batch(&batch).is_none());
    }

    #[test]
    fn on_input_error_fails_the_whole_batch_closed() {
        let rt = runtime(
            "import citadel\ndef h(e):\n    raise RuntimeError(\"boom\")\ncitadel.on_input(h)\n",
        );
        let batch = batch_with(vec![input_event(1, 7), input_event(2, 8)]);
        assert!(rt.evaluate_event_batch(&batch).is_none());
    }

    #[test]
    fn deliver_event_batch_reaches_the_attached_sink() {
        let rt = runtime("import citadel\ndef h(e):\n    return None\ncitadel.on_input(h)\n");
        let sink = Arc::new(RecordingBridgeSink::default());
        rt.attach_bridge_sink(Arc::downgrade(&sink) as Weak<dyn BridgeCommandSink>);
        rt.deliver_event_batch(batch_with(vec![input_event(1, 7)]));
        let got = sink.0.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].input_outcomes[0].decision,
            crate::runtime::Decision::Accept
        );
    }

    #[test]
    fn on_input_can_call_rewind_query() {
        let rt = runtime(
            "import citadel\ndef h(e):\n    r = citadel.rewind_query(e[\"participant\"], (0, 0, 0), (1, 0, 0), e[\"tick\"])\n    assert isinstance(r[\"hits\"], list)\n    return None\ncitadel.on_input(h)\n",
        );
        let batch = batch_with(vec![input_event(1, 7)]);
        let answer = rt.evaluate_event_batch(&batch).expect("answer built");
        assert_eq!(
            answer.input_outcomes[0].decision,
            crate::runtime::Decision::Accept
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
import citadel

@citadel.events.subscribe("match", "first")
def first(event):
    citadel.broadcast(7, event["payload"])
    assert citadel.events.emit("match", "second", b"two")

@citadel.events.subscribe("match", "second")
def second(event):
    citadel.broadcast(8, event["payload"])

@citadel.on_message(1)
def message(ctx, body):
    assert citadel.events.emit("match", "first", b"one")
"#,
        )
        .with_event_bus(bus);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"one".to_vec(),
                unreliable: false,
            }]
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
import citadel

@citadel.on_message(1)
def message(ctx, body):
    first = citadel.cache.set("match.one", "score", b"one", 1000)
    assert citadel.cache.get("match.two", "score") is None
    second = citadel.cache.cas("match.one", "score", first["version"], b"two", 1000)
    assert second is not None
    assert citadel.cache.cas("match.one", "score", first["version"], b"bad", 1000) is None
    citadel.broadcast(7, citadel.cache.get("match.one", "score")["value"])
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
import citadel

before_blocked = False
after_blocked = False

@citadel.before_realtime
def before(ctx, body):
    global before_blocked
    try:
        citadel.cache.set("match", "key", b"bad", 1000)
    except RuntimeError:
        before_blocked = True
    return False

@citadel.after_realtime
def after(ctx, body):
    global after_blocked
    try:
        citadel.cache.get("match", "key")
    except RuntimeError:
        after_blocked = True

@citadel.on_message(1)
def message(ctx, body):
    assert citadel.cache.get("match", "key") is None
    citadel.broadcast(7, b"ok" if before_blocked and after_blocked else b"failed")
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

    // The slow subscriber must be the only one: the delivery budget is shared,
    // and the timeout overrun grows arbitrarily under CPU/GIL contention, so an
    // assertion about a second subscriber would depend on the scheduler.
    #[test]
    fn runtime_event_subscriber_timeout_preserves_outer_commands() {
        let bus = Arc::new(RuntimeEventBus::new(
            crate::runtime::RuntimeEventPolicy {
                enabled: true,
                queue_capacity: 8,
                max_event_bytes: 64,
                max_events_per_minute: 10,
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = PythonRuntime::from_source(
            r#"
import citadel

@citadel.events.subscribe("match", "slow")
def slow(event):
    while True:
        pass

@citadel.on_message(1)
def message(ctx, body):
    citadel.broadcast(7, b"outer")
    citadel.events.emit("match", "slow", b"x")
"#,
            "timeout.py",
            10,
        )
        .expect("runtime loads")
        .with_event_bus(bus);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 7,
                body: b"outer".to_vec(),
                unreliable: false,
            }]
        );
    }

    // Timing-independent twin of the timeout test above: the failing subscriber
    // returns instantly, so the generous budget is never consumed and the later
    // subscriber's share cannot be starved by a timeout overrun.
    #[test]
    fn runtime_event_subscriber_failure_keeps_later_subscribers() {
        let bus = Arc::new(RuntimeEventBus::new(
            crate::runtime::RuntimeEventPolicy {
                enabled: true,
                queue_capacity: 8,
                max_event_bytes: 64,
                max_events_per_minute: 10,
            },
            Arc::new(crate::observability::NodeMetrics::new()),
        ));
        let runtime = PythonRuntime::from_source(
            r#"
import citadel

@citadel.events.subscribe("match", "boom")
def failing(event):
    raise RuntimeError("boom")

@citadel.events.subscribe("match", "boom")
def next_subscriber(event):
    citadel.broadcast(8, b"next")

@citadel.on_message(1)
def message(ctx, body):
    citadel.broadcast(7, b"outer")
    citadel.events.emit("match", "boom", b"x")
"#,
            "failure.py",
            1_000,
        )
        .expect("runtime loads")
        .with_event_bus(bus);
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![
                OutboundCommand::Broadcast {
                    kind: 7,
                    body: b"outer".to_vec(),
                    unreliable: false,
                },
                OutboundCommand::Broadcast {
                    kind: 8,
                    body: b"next".to_vec(),
                    unreliable: false,
                },
            ]
        );
    }

    #[test]
    fn host_api_surface_matches_manifest_python() {
        let shipped: HashSet<&'static str> = HOST_API_SURFACE
            .iter()
            .filter(|entry| entry.status == HostApiStatus::Shipped)
            .map(|entry| entry.name)
            .collect();
        assert_eq!(PythonRuntime::registered_host_api_names(), shipped);
    }

    #[test]
    fn realtime_interceptors_veto_and_observe_without_command_side_effects() {
        let rt = runtime(
            r#"
import citadel

seen = "unset"

@citadel.before_realtime
def before(ctx, body):
    citadel.broadcast(99, b"must-discard")
    return False

@citadel.after_realtime
def after(ctx, body):
    global seen
    citadel.broadcast(98, b"must-discard")
    seen = f"{'drop' if ctx.dropped else 'pass'}:{ctx.delivered}:{ctx.body.hex()}"

@citadel.on_message(8)
def message(ctx, body):
    citadel.broadcast(9, seen)
"#,
        );

        assert_eq!(
            rt.before_realtime(7, Some("user-7"), Some(42), 1, &[4, 5]),
            RealtimeInterception::Drop
        );
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
            &[4, 5],
            RealtimeAfterOutcome {
                dropped: true,
                delivered: 0,
            },
        );
        assert_eq!(
            rt.dispatch(7, Some("user-7"), 8, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"drop:0:0405".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn realtime_interceptors_reject_all_async_http_operations() {
        let rt = runtime(
            r#"
import citadel

errors = []

@citadel.before_realtime
def before(ctx, body):
    for operation in (
        lambda: citadel.http.start("https://api.example.test/"),
        lambda: citadel.http.poll(1),
        lambda: citadel.http.cancel(1),
    ):
        try:
            operation()
        except RuntimeError as error:
            errors.append(str(error))
    return True

@citadel.on_message(8)
def message(ctx, body):
    citadel.broadcast(9, ",".join(errors).encode())
"#,
        );
        assert_eq!(
            rt.before_realtime(7, None, None, 1, b"input"),
            RealtimeInterception::Continue
        );
        assert_eq!(
            rt.dispatch(7, None, 8, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"interceptor_forbidden,interceptor_forbidden,interceptor_forbidden".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn invalid_before_realtime_result_fails_closed() {
        let rt = runtime(
            r#"
import citadel

@citadel.before_realtime
def before(ctx, body):
    return "invalid"
"#,
        );
        assert_eq!(
            rt.before_realtime(7, None, None, 1, b"input"),
            RealtimeInterception::Drop
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn realtime_interceptors_reject_domain_storage_side_effects() {
        let rt = runtime(
            r#"
import citadel

seen = "unset"

@citadel.before_realtime
def before(ctx, body):
    def before_filter(candidate):
        global seen
        seen = "filter-mutated"
        return True
    citadel.register_storage_index_filter("profiles_by_score", before_filter)
    citadel.storage_write("user", "profiles", "before", "{}")
    return True

@citadel.after_realtime
def after(ctx, body):
    global seen
    def after_filter(candidate):
        global seen
        seen = "filter-mutated"
        return True
    citadel.register_storage_index_filter("profiles_by_score", after_filter)
    citadel.storage_write("user", "profiles", "after", "{}")
    seen = "mutated"

@citadel.on_message(8)
def message(ctx, body):
    citadel.storage_write("user", "profiles", "normal", '{"score":1}')
    citadel.broadcast(9, seen)
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
    fn message_handler_broadcasts() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_message(1)
def handle(ctx, body):
    citadel.broadcast(2, ctx.sender.to_bytes(8, "big") + body, unreliable=True)
"#,
        );
        assert_eq!(
            rt.dispatch(42, None, 1, b"abc"),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: [42u64.to_be_bytes().as_slice(), b"abc"].concat(),
                unreliable: true,
            }]
        );
    }

    #[test]
    fn file_runtime_http_policy_survives_reload() {
        let dir = TempDir::new("python-http-policy");
        let source = r#"
import citadel

@citadel.on_message(1)
def handle(ctx, body):
    failures = []
    for operation in (
        lambda: citadel.http.fetch("https://api.example.test/"),
        lambda: citadel.http.start("https://api.example.test/"),
        lambda: citadel.http.poll(7),
        lambda: citadel.http.cancel(7),
    ):
        try:
            operation()
        except RuntimeError as error:
            failures.append(str(error))
    citadel.broadcast(2, ",".join(failures).encode())
"#;
        dir.write_main(source);
        let runtime = PythonRuntime::load_with_static_data_and_http_policy(
            &dir.0,
            100,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            OutboundHttpPolicy {
                enabled: false,
                ..OutboundHttpPolicy::default()
            },
        )
        .expect("load file runtime")
        .expect("main.py present");
        let assert_disabled = |runtime: &PythonRuntime| {
            let commands = runtime.dispatch(1, None, 1, b"");
            let OutboundCommand::Broadcast { body, .. } = commands
                .into_iter()
                .next()
                .expect("expected error broadcast")
            else {
                panic!("expected broadcast");
            };
            assert_eq!(
                body,
                b"capability_disabled,capability_disabled,capability_disabled,capability_disabled",
                "fetch compatibility and every async operation must enforce the same host policy"
            );
        };
        assert_disabled(&runtime);
        dir.write_main(source);
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
        assert_disabled(&runtime);
    }

    #[test]
    fn async_http_state_contract_is_stable_for_python() {
        Python::attach(|py| -> PyResult<()> {
            for state in [
                OutboundHttpRequestState::Pending,
                OutboundHttpRequestState::Timeout,
                OutboundHttpRequestState::Cancelled,
            ] {
                let result = outbound_http_state_to_python(py, state)?;
                assert!(result.bind(py).get_item("error_code")?.is_none());
            }
            let timeout = outbound_http_state_to_python(py, OutboundHttpRequestState::Timeout)?;
            assert_eq!(
                timeout
                    .bind(py)
                    .get_item("state")?
                    .expect("timeout state")
                    .extract::<String>()?,
                "timeout"
            );
            let cancelled = outbound_http_state_to_python(py, OutboundHttpRequestState::Cancelled)?;
            assert_eq!(
                cancelled
                    .bind(py)
                    .get_item("state")?
                    .expect("cancelled state")
                    .extract::<String>()?,
                "cancelled"
            );
            let success = outbound_http_state_to_python(
                py,
                OutboundHttpRequestState::Success(
                    crate::runtime::outbound_http::OutboundHttpResponse {
                        status: 201,
                        body: vec![0, 255],
                    },
                ),
            )?;
            let success = success.bind(py);
            assert_eq!(
                success
                    .get_item("status")?
                    .expect("success status")
                    .extract::<u16>()?,
                201
            );
            assert_eq!(
                success
                    .get_item("body")?
                    .expect("success body")
                    .extract::<Vec<u8>>()?,
                vec![0, 255]
            );
            let error = outbound_http_state_to_python(
                py,
                OutboundHttpRequestState::Error("request_failed".to_string()),
            )?;
            let error = error.bind(py);
            assert_eq!(
                error
                    .get_item("state")?
                    .expect("error state")
                    .extract::<String>()?,
                "error"
            );
            assert_eq!(
                error
                    .get_item("error_code")?
                    .expect("error code")
                    .extract::<String>()?,
                "request_failed"
            );
            Ok(())
        })
        .expect("all async HTTP states map to the documented Python contract");
    }

    #[test]
    fn async_http_rejects_oversized_python_bodies_before_network_io() {
        let runtime = runtime(&format!(
            r#"
import citadel

@citadel.on_message(1)
def handle(ctx, body):
    try:
        citadel.http.start("https://example.test/", {{"body": b"x" * {}}})
    except RuntimeError as error:
        citadel.broadcast(2, str(error).encode())
"#,
            crate::runtime::outbound_http::MAX_OUTBOUND_HTTP_REQUEST_BYTES + 1
        ));
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
    async fn async_http_handles_return_bytes_without_blocking_python() {
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
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("release delayed response");
            stream
                .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 3\r\nConnection: close\r\n\r\n\0\xffA")
                .expect("write test HTTP response");
        });
        let dir = TempDir::new("python-async-http-bytes");
        dir.write_main(&format!(
            r#"
import citadel

handle = None

@citadel.on_message(1)
def start(ctx, body):
    global handle
    handle = citadel.http.start("http://localhost:{port}/", {{
        "method": "POST", "headers": {{"x-test": "yes"}}, "body": b"request"
    }})
    citadel.broadcast(9, type(handle).__name__.encode())

@citadel.on_message(2)
def poll(ctx, body):
    result = citadel.http.poll(handle)
    if result["state"] == "success":
        citadel.broadcast(9, f"success:{{result['status']}}:{{result['body'].hex()}}".encode())
    else:
        citadel.broadcast(9, result["state"].encode())
"#
        ));
        let runtime = PythonRuntime::load_with_static_data_and_http_policy(
            &dir.0,
            100,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            OutboundHttpPolicy {
                allowed_hosts: vec!["localhost".to_owned()],
                allowed_ports: vec![port],
                allow_private_networks: true,
                ..OutboundHttpPolicy::default()
            },
        )
        .expect("load file runtime")
        .expect("main.py present");
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 9,
                body: b"int".to_vec(),
                unreliable: false,
            }]
        );
        served_rx
            .recv_timeout(std::time::Duration::from_secs(1))
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
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    None
                }
            })
            .find_map(std::convert::identity)
            .expect("completed async response");
        assert_eq!(response, b"success:201:00ff41");
    }

    #[test]
    fn custom_http_endpoint_registration_dispatch_and_reload_are_atomic() {
        let dir = TempDir::new("custom-http-endpoint");
        let source = r#"
import citadel

@citadel.http.register("POST", "/echo", {"auth": "session"})
def echo(request):
    return {
        "status": 201,
        "headers": {"content-type": "text/plain"},
        "body": request["user_id"].encode() + b":" + request["body"],
    }
"#;
        dir.write_main(source);
        let policy = RuntimeHttpEndpointPolicy {
            enabled: true,
            max_request_bytes: 1024,
            max_response_bytes: 1024,
            max_requests_per_minute: 10,
        };
        let runtime = PythonRuntime::load_with_static_data_and_capability_policies(
            &dir.0,
            100,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            OutboundHttpPolicy::default(),
            policy,
        )
        .expect("load endpoint runtime")
        .expect("main.py present");
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
import citadel

@citadel.http.register("GET", "/next")
def next_endpoint(request):
    return {"body": b"next"}
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
import citadel

citadel.http.register("GET", "/dup", {"auth": "public"}, lambda request: {})
citadel.http.register("GET", "/dup", {"auth": "session"}, lambda request: {})
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
    fn imperative_registration_and_lifecycle_work() {
        let rt = runtime(
            r#"
import citadel

def joined(ctx):
    citadel.send(ctx.sender, 7, b"joined")

citadel.on_join(joined)
"#,
        );
        assert_eq!(
            rt.dispatch_lifecycle(LifecycleHook::Join, 9, None),
            vec![OutboundCommand::Send {
                session: 9,
                kind: 7,
                body: b"joined".to_vec(),
                unreliable: false,
            }]
        );
        assert!(
            rt.dispatch_lifecycle(LifecycleHook::Leave, 9, None)
                .is_empty()
        );
    }

    #[test]
    fn leaderboard_reset_handler_receives_epoch_context_and_surfaces_failures() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_leaderboard_reset
def reset(ctx):
    assert ctx["leaderboard_id"] == "weekly"
    assert ctx["due_at_unix_ms"] == 60_000
    assert ctx["fencing_token"] == 7
    raise RuntimeError("leaderboard reset reached")
"#,
        );
        let epoch = crate::leaderboard_scheduler::ResetEpoch::new(
            "weekly".to_owned(),
            crate::time::TimestampMillis::from_unix_millis(60_000),
        );

        let error = rt
            .on_leaderboard_reset(
                &epoch,
                crate::leaderboard_scheduler::SchedulerFencingToken::new(7),
            )
            .expect_err("registered leaderboard reset hook must run");

        assert!(
            error
                .message()
                .contains("leaderboard reset callback failed")
        );
        assert!(
            error
                .log_detail()
                .is_some_and(|detail| detail.contains("leaderboard reset reached"))
        );
    }

    #[test]
    fn tick_handler_receives_dt() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_tick
def tick(dt):
    citadel.broadcast(5, str(round(dt, 2)).encode())
"#,
        );
        assert!(rt.has_tick_handler());
        assert_eq!(
            rt.tick(Duration::from_millis(125), Duration::from_millis(100)),
            vec![OutboundCommand::Broadcast {
                kind: 5,
                body: b"0.12".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn exceptions_are_isolated() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_message(1)
def boom(ctx, body):
    citadel.broadcast(2, b"lost")
    raise RuntimeError("secret")
"#,
        );
        assert!(rt.dispatch(1, None, 1, b"").is_empty());
    }

    #[test]
    fn rpc_success_error_and_unknown_method() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")

@citadel.on_rpc("bad")
def bad(ctx, body):
    return citadel.Reply.err("invalid")
"#,
        );
        assert_eq!(
            rt.call_rpc(1, None, "ping", b""),
            RpcOutcome::Ok(b"pong".to_vec())
        );
        assert_eq!(
            rt.call_rpc(1, None, "bad", b""),
            RpcOutcome::Err("invalid".to_string())
        );
        assert!(matches!(
            rt.call_rpc(1, None, "missing", b""),
            RpcOutcome::Err(msg) if msg.contains("unknown RPC method")
        ));
    }

    #[test]
    fn room_hooks_work_and_fail_closed() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_room_create
def create(ctx, params):
    return {"map": "Arena", "mode": "duel", "max_players": 2, "open": True}

@citadel.on_room_join
def join(ctx, room_id):
    return room_id == 7
"#,
        );
        let spec = rt.call_room_create(1, None, b"").expect("room spec");
        assert_eq!(spec.map, "Arena");
        assert_eq!(spec.mode, "duel");
        assert_eq!(spec.max_players, 2);
        assert!(spec.open);
        assert!(rt.call_room_join(1, None, 7));
        assert!(!rt.call_room_join(1, None, 8));

        let broken = runtime(
            r#"
import citadel

@citadel.on_room_join
def join(ctx, room_id):
    raise RuntimeError("nope")
"#,
        );
        assert!(!broken.call_room_join(1, None, 7));
    }

    #[test]
    fn actor_commands_match_lua_shapes() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_message(1)
def actor(ctx, body):
    actor_id = citadel.spawn_actor({"archetype": 3, "x": 1, "y": 2, "z": 3})
    citadel.move_actor(actor_id, 4, 5, 6, 7, 8, 9)
    citadel.despawn_actor(actor_id)
"#,
        );
        let commands = rt.dispatch(1, None, 1, b"");
        assert_eq!(commands.len(), 3);
        let object_id = match &commands[0] {
            OutboundCommand::SpawnActor {
                object_id,
                archetype,
                position,
            } => {
                assert!(*object_id >= NPC_ID_BASE);
                assert_eq!(*archetype, 3);
                assert_eq!(*position, [1.0, 2.0, 3.0]);
                *object_id
            }
            other => panic!("expected spawn, got {other:?}"),
        };
        assert_eq!(
            commands[1],
            OutboundCommand::MoveActor {
                object_id,
                position: [4.0, 5.0, 6.0],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [7.0, 8.0, 9.0],
            }
        );
        assert_eq!(commands[2], OutboundCommand::DespawnActor { object_id });
    }

    #[test]
    fn reload_swaps_success_and_rejects_broken_script() {
        let dir = TempDir::new("py-reload");
        dir.write_main(
            r#"
import citadel

@citadel.on_message(1)
def handle(ctx, body):
    citadel.broadcast(2, b"v1")
"#,
        );
        let rt = PythonRuntime::load(&dir.0, 100)
            .expect("loads")
            .expect("present");
        assert_eq!(
            rt.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"v1".to_vec(),
                unreliable: false,
            }]
        );

        dir.write_main(
            r#"
import citadel

@citadel.on_message(1)
def handle(ctx, body):
    citadel.broadcast(2, b"v2")
"#,
        );
        assert_eq!(rt.reload(), ReloadOutcome::Reloaded);
        assert_eq!(
            rt.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"v2".to_vec(),
                unreliable: false,
            }]
        );

        dir.write_main("this is not python ==");
        assert_eq!(rt.reload(), ReloadOutcome::Rejected);
        assert_eq!(
            rt.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"v2".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn hung_handler_is_bounded_by_deadline_trace() {
        let rt = PythonRuntime::from_source(
            r#"
import citadel

@citadel.on_message(1)
def hang(ctx, body):
    while True:
        pass
"#,
            "hang.py",
            50,
        )
        .expect("loads");
        let start = Instant::now();
        assert!(rt.dispatch(1, None, 1, b"").is_empty());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "deadline should interrupt pure-Python loop promptly"
        );
        assert!(rt.dispatch(1, None, 999, b"").is_empty());
    }

    #[test]
    fn introspection_reports_registered_handlers() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_message(9)
def message(ctx, body): pass

@citadel.on_rpc("ping")
def ping(ctx, body): return b"pong"

@citadel.on_leave
def leave(ctx): pass
"#,
        );
        let info = rt.introspect();
        assert!(info.source.contains("test.py"));
        assert_eq!(info.rpcs, vec!["ping"]);
        assert_eq!(info.message_kinds, vec![9]);
        assert_eq!(info.hooks, vec!["on_leave"]);
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
    /// manifest — the behavioral gate below must exercise exactly this set.
    fn shipped_domain_host_api_names() -> HashSet<&'static str> {
        HOST_API_SURFACE
            .iter()
            .filter(|entry| {
                entry.category == crate::runtime::HostApiCategory::Domain
                    && entry.status == HostApiStatus::Shipped
            })
            .map(|entry| entry.name)
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn domain_host_api_behaviorally_covers_manifest() {
        // Exercise EVERY shipped Domain function against a real (in-memory) host
        // and assert real effects — a name-claim stub raises here and fails.
        let rt = runtime(
            r#"
import citadel

@citadel.on_rpc("exercise")
def exercise(ctx, body):
    u, o = "prober", "target"
    added = citadel.friends_add(u, o)
    citadel.friends_add(o, u)
    chat = citadel.chat_call(u, "send", '{"target":{"kind":"direct","other_user_id":"target"},"content":"hi"}')
    n1 = len(citadel.friends_list(u))
    citadel.friends_block(u, o)
    blocked = citadel.friends_list(u)[0]["state"]
    removed = citadel.friends_remove(u, o)
    n2 = len(citadel.friends_list(u))
    notification = citadel.notifications_send(u, 7, "hello", "{}", "server", "probe")
    page = citadel.notifications_list(u)
    read = citadel.notifications_mark_read(u, [notification["id"]])
    group = citadel.groups_call(u, "create", '{"name":"probers"}')
    boards = citadel.leaderboards_call(u, "list", "{}")
    tournaments = citadel.tournaments_call(u, "list", "{}")
    wallet = citadel.wallet_call(u, "balances", "{}")
    return (added + "|" + str(n1) + "|" + blocked + "|" + str(removed) + "|" + str(n2) + "|" + str(len(page["items"])) + "|" + str(len(read)) + "|" + group["name"] + "|" + str(len(boards)) + "|" + str(len(tournaments)) + "|" + str(chat["id"]) + "|" + str(len(wallet))).encode()
"#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("prober"), "exercise", b"") else {
            panic!("domain host functions must be wired, not stubbed");
        };
        // Python `str(True)` is "True".
        assert_eq!(reply, b"invited_sent|1|blocked|True|0|1|1|probers|0|0|1|0");

        let exercised: HashSet<&str> = [
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

    #[test]
    fn friends_host_api_errors_without_a_domain_host() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_rpc("befriend")
def befriend(ctx, body):
    citadel.friends_add(ctx.user_id, body.decode())
    return b"unreachable"
"#,
        );
        let RpcOutcome::Err(msg) = rt.call_rpc(1, Some("alice"), "befriend", b"bob") else {
            panic!("expected error");
        };
        assert!(!msg.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_index_query_is_wired_to_the_python_host() {
        let rt = runtime(
            r#"
import citadel

@citadel.on_rpc("search")
def search(ctx, body):
    def filter(candidate):
        if candidate["key"] == "boom":
            raise RuntimeError("filter failed")
        return candidate["key"] == "main"
    citadel.register_storage_index_filter("profiles_by_score", filter)
    citadel.storage_write(ctx.user_id, "profiles", "skip", '{"score":7}')
    citadel.storage_write(ctx.user_id, "profiles", "main", '{"score":7}')
    try:
        citadel.storage_write(ctx.user_id, "profiles", "boom", '{"score":7}')
        errored = False
    except RuntimeError:
        errored = True
    missing = citadel.storage_read(ctx.user_id, "profiles", "boom") is None
    found = citadel.storage_index_query("profiles_by_score", '{"score":7}', 10)
    return (str(errored).lower() + "|" + str(missing).lower() + "|" + str(len(found)) + "|" + found[0]["user_id"] + "|" + found[0]["key"]).encode()
"#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("alice"), "search", b"") else {
            panic!("storage index host must return a reply");
        };
        assert_eq!(reply, b"true|true|1|alice|main");
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(prefix: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("citadel-{prefix}-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn main_py(&self) -> PathBuf {
            self.0.join(PYTHON_ENTRYPOINT)
        }

        fn write_main(&self, src: &str) {
            std::fs::write(self.main_py(), src).expect("write main.py");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
