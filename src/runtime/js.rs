//! Embedded QuickJS runtime host for capped JavaScript game logic.
//!
//! `JsRuntime` mirrors the Lua and Python adapters: one VM behind a lock, a
//! bounded command-return host API, and fail-closed invocation wrappers. It is
//! compiled only with `--features runtime-js`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rquickjs::context::EvalOptions;
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use rquickjs::{
    Array, BigInt, CatchResultExt, Context, Ctx, Function, Module, Object,
    Runtime as QuickJsRuntime, TypedArray, Value,
};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::maps::MapCatalog;
use crate::realtime::TransformHub;
use crate::runtime::outbound_http::{OutboundHttpRequest, TrustedHttpClient};
use crate::runtime::static_data::StaticDataCatalog;
use crate::runtime::{
    DomainHost, LifecycleHook, OutboundCommand, PhysicsOptions, RealtimeAfterOutcome,
    RealtimeInterception, ReloadOutcome, RoomSpec, RpcOutcome, Runtime, RuntimeIntrospection,
    StorageWriteInput,
};
use citadel_physics::{PhysicsConfig, Shape};

/// Default JavaScript script entrypoint under `runtime.scripts_dir`.
pub const JS_ENTRYPOINT: &str = "main.js";

/// Time budget for running top-level JavaScript registrations at load/reload.
const LOAD_DEADLINE_MS: u64 = 5_000;

/// Maximum number of outbound commands a single handler invocation may enqueue.
const MAX_OUTBOUND_COMMANDS: usize = 1024;

/// Per-runtime heap cap for capped QuickJS mode.
const JS_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Per-runtime stack cap for capped QuickJS mode.
const JS_STACK_LIMIT_BYTES: usize = 512 * 1024;

const RPC_ERR_UNKNOWN_METHOD: &str = "unknown RPC method";
const RPC_ERR_TIMEOUT: &str = "RPC handler timed out";
const RPC_ERR_HANDLER: &str = "RPC handler error";

const JS_HOST_API_NAMES: &[&str] = &[
    "on_message",
    "on_join",
    "on_leave",
    "on_tick",
    "on_rpc",
    "on_room_create",
    "on_room_join",
    "before_realtime",
    "after_realtime",
    "broadcast",
    "send",
    "spawn_actor",
    "move_actor",
    "despawn_actor",
    "set_physics",
    "apply_impulse",
    "set_move_intent",
    "physics_state",
    "map_info",
    "raycast",
    "sphere_overlap",
    "ground_height",
    "log",
    "static_data.load_json",
    "static_data.load_csv",
    "friends.add",
    "friends.remove",
    "friends.block",
    "friends.list",
    "notifications.send",
    "notifications.list",
    "notifications.mark_read",
    "groups.call",
    "leaderboards.call",
    "chat.call",
    "wallet.call",
    "storage.read",
    "storage.write",
    "storage.delete",
    "storage.index_query",
    "storage.register_index_filter",
    "http.fetch",
];

const JS_HOST_PRELUDE: &str = r#"
(function () {
  "use strict";

  const messageHandlers = new Map();
  const rpcHandlers = new Map();
  const storageIndexFilters = new Map();
  let onJoin = null;
  let onLeave = null;
  let onTick = null;
  let onRoomCreate = null;
  let onRoomJoin = null;
  let beforeRealtime = null;
  let afterRealtime = null;
  let commands = [];
  let logs = [];
  let totalBytes = 0;
  let overflowed = false;
  let nextNpcId = 0x40000000;
  let __citadel_domain_host = null;
  const ensureRealtimeEffectsAllowed = globalThis.__citadel_realtime_effects_allowed;

  if (typeof ensureRealtimeEffectsAllowed !== "function") {
    throw new Error("realtime interceptor guard is unavailable");
  }

  const MAX_OUTBOUND_COMMANDS = 1024;
  const MAX_OUTBOUND_BODY_BYTES = 64 * 1024;
  const MAX_TOTAL_OUTBOUND_BYTES = 1 << 20;

  function utf8Bytes(value) {
    const text = String(value);
    const out = [];
    for (let i = 0; i < text.length; i += 1) {
      let code = text.charCodeAt(i);
      if (code >= 0xd800 && code <= 0xdbff && i + 1 < text.length) {
        const low = text.charCodeAt(i + 1);
        if (low >= 0xdc00 && low <= 0xdfff) {
          code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
          i += 1;
        }
      }
      if (code <= 0x7f) {
        out.push(code);
      } else if (code <= 0x7ff) {
        out.push(0xc0 | (code >> 6), 0x80 | (code & 0x3f));
      } else if (code <= 0xffff) {
        out.push(0xe0 | (code >> 12), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
      } else {
        out.push(0xf0 | (code >> 18), 0x80 | ((code >> 12) & 0x3f), 0x80 | ((code >> 6) & 0x3f), 0x80 | (code & 0x3f));
      }
    }
    return new Uint8Array(out);
  }

  function bytes(value) {
    if (value === null || value === undefined) {
      return new Uint8Array(0);
    }
    if (value instanceof Uint8Array) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value.slice(0));
    }
    if (Array.isArray(value)) {
      return new Uint8Array(value.map((item) => Number(item) & 0xff));
    }
    if (typeof value === "string") {
      return utf8Bytes(value);
    }
    throw new TypeError("expected Uint8Array, ArrayBuffer, byte array, or string");
  }

  function handlerRegistration(store, key, handler) {
    const register = (fn) => {
      if (typeof fn !== "function") {
        throw new TypeError("handler must be a function");
      }
      store.set(key, fn);
      return fn;
    };
    return handler === undefined ? register : register(handler);
  }

  function singleRegistration(setter, handler) {
    const register = (fn) => {
      if (typeof fn !== "function") {
        throw new TypeError("handler must be a function");
      }
      setter(fn);
      return fn;
    };
    return handler === undefined ? register : register(handler);
  }

  function push(command, bodyLength) {
    if (commands.length >= MAX_OUTBOUND_COMMANDS) {
      overflowed = true;
      return;
    }
    if (totalBytes + bodyLength > MAX_TOTAL_OUTBOUND_BYTES) {
      overflowed = true;
      return;
    }
    commands.push(command);
    totalBytes += bodyLength;
  }

  function bodyCommand(tag, args, bodyIndex) {
    const body = bytes(args[bodyIndex]);
    if (body.length > MAX_OUTBOUND_BODY_BYTES) {
      throw new Error("outbound body too large");
    }
    args[bodyIndex] = body;
    push([tag].concat(args), body.length);
  }

  function toSessionString(value) {
    if (typeof value === "bigint" || typeof value === "number" || typeof value === "string") {
      return String(value);
    }
    throw new TypeError("session id must be a number, bigint, or string");
  }

  function toRoomId(value) {
    if (typeof value === "bigint") {
      return value;
    }
    return BigInt(String(value));
  }

  function roomSpec(value) {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === "string") {
      return [value, "", 0, true];
    }
    return [
      String(value.map || ""),
      String(value.mode || ""),
      Number(value.max_players || value.maxPlayers || 0),
      value.open === undefined ? true : Boolean(value.open),
    ];
  }

  class Reply {
    constructor(ok, body, error) {
      this.ok = Boolean(ok);
      this.body = bytes(body);
      this.error = String(error || "");
    }

    static ok(body) {
      return new Reply(true, body === undefined ? new Uint8Array(0) : body, "");
    }

    static err(message) {
      return new Reply(false, new Uint8Array(0), message);
    }
  }

  function log(message, level) {
    logs.push([String(level || "info").toLowerCase(), String(message)]);
  }

  const citadel = {
    Reply,
    on_message(kind, handler) {
      return handlerRegistration(messageHandlers, Number(kind), handler);
    },
    on_join(handler) {
      return singleRegistration((fn) => { onJoin = fn; }, handler);
    },
    on_leave(handler) {
      return singleRegistration((fn) => { onLeave = fn; }, handler);
    },
    on_tick(handler) {
      return singleRegistration((fn) => { onTick = fn; }, handler);
    },
    on_rpc(method, handler) {
      return handlerRegistration(rpcHandlers, String(method), handler);
    },
    on_room_create(handler) {
      return singleRegistration((fn) => { onRoomCreate = fn; }, handler);
    },
    on_room_join(handler) {
      return singleRegistration((fn) => { onRoomJoin = fn; }, handler);
    },
    before_realtime(handler) {
      return singleRegistration((fn) => { beforeRealtime = fn; }, handler);
    },
    after_realtime(handler) {
      return singleRegistration((fn) => { afterRealtime = fn; }, handler);
    },
    broadcast(kind, body, unreliable) {
      bodyCommand("broadcast", [Number(kind), body, Boolean(unreliable)], 1);
    },
    send(session, kind, body, unreliable) {
      bodyCommand("send", [toSessionString(session), Number(kind), body, Boolean(unreliable)], 2);
    },
    log,
    http: {
      fetch(url, opts) {
        if (globalThis.__citadel_realtime_interceptor) {
          throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
        }
        if (!globalThis.__citadel_http_fetch) {
          throw new Error("outbound HTTP host not available");
        }
        const response = JSON.parse(globalThis.__citadel_http_fetch(String(url), JSON.stringify(opts || {})));
        return { status: response.status, body: Uint8Array.from(response.body) };
      },
    },
    static_data: {
      load_json(path) {
        if (!globalThis.__citadel_static_data) {
          throw new Error("static data host not available");
        }
        return JSON.parse(globalThis.__citadel_static_data("json", String(path)));
      },
      load_csv(path) {
        if (!globalThis.__citadel_static_data) {
          throw new Error("static data host not available");
        }
        return JSON.parse(globalThis.__citadel_static_data("csv", String(path)));
      },
    },
    spawn_actor(opts) {
      const data = opts || {};
      const objectId = nextNpcId;
      nextNpcId += 1;
      if (nextNpcId > 0xffffffff) {
        nextNpcId = 0x40000000;
      }
      push([
        "spawn_actor",
        objectId,
        Number(data.archetype || 0),
        Number(data.x || 0),
        Number(data.y || 0),
        Number(data.z || 0),
      ], 0);
      return objectId;
    },
    move_actor(objectId, x, y, z, vx, vy, vz) {
      push([
        "move_actor",
        Number(objectId),
        Number(x || 0),
        Number(y || 0),
        Number(z || 0),
        Number(vx || 0),
        Number(vy || 0),
        Number(vz || 0),
      ], 0);
    },
    despawn_actor(objectId) {
      push(["despawn_actor", Number(objectId)], 0);
    },
    set_physics(objectId, opts) {
      push([
        "set_physics",
        Number(objectId),
        opts === null || opts === undefined ? null : JSON.stringify(opts),
      ], 0);
    },
    apply_impulse(objectId, ix, iy, iz) {
      push(["apply_impulse", Number(objectId), Number(ix), Number(iy), Number(iz)], 0);
    },
    set_move_intent(objectId, vx, vy, vz) {
      push(["set_move_intent", Number(objectId), Number(vx), Number(vy), Number(vz)], 0);
    },
    physics_state(objectId) {
      if (!globalThis.__citadel_physics_state) return null;
      return JSON.parse(globalThis.__citadel_physics_state(Number(objectId)));
    },
    map_info(name) {
      if (!globalThis.__citadel_map_info) return null;
      return JSON.parse(globalThis.__citadel_map_info(String(name)));
    },
    raycast(origin, direction) {
      if (!globalThis.__citadel_raycast) return null;
      return JSON.parse(globalThis.__citadel_raycast(origin, direction));
    },
    sphere_overlap(centre, radius) {
      if (!globalThis.__citadel_sphere_overlap) return false;
      return globalThis.__citadel_sphere_overlap(centre, Number(radius));
    },
    ground_height(origin, maxDistance) {
      if (!globalThis.__citadel_ground_height) return null;
      return JSON.parse(globalThis.__citadel_ground_height(origin, Number(maxDistance)));
    },
    // Persisted friends host API. Each delegates to the native
    // `__citadel_friends` bridge (installed only when the runtime is built with
    // `with_domain_host`) which returns a JSON-encoded result or throws. The
    // script passes the acting `user` explicitly (trusted tier).
    friends_add(user, other) {
      return __citadel_friends_call("add", String(user), String(other));
    },
    friends_remove(user, other) {
      return __citadel_friends_call("remove", String(user), String(other));
    },
    friends_block(user, other) {
      __citadel_friends_call("block", String(user), String(other));
    },
    friends_list(user) {
      return __citadel_friends_call("list", String(user), "");
    },
    notifications_send(recipient, code, subject, contentJson, sender, deliveryKey) {
      return __citadel_notifications_call("send", {
        recipient: String(recipient), code: Number(code), subject: String(subject),
        content_json: String(contentJson),
        sender: sender === undefined || sender === null ? null : String(sender),
        delivery_key: deliveryKey === undefined || deliveryKey === null ? null : String(deliveryKey),
      });
    },
    notifications_list(recipient, limit, cursor) {
      return __citadel_notifications_call("list", {
        recipient: String(recipient), limit: limit === undefined ? 50 : Number(limit),
        cursor: cursor === undefined || cursor === null ? null : String(cursor),
      });
    },
    notifications_mark_read(recipient, ids) {
      return __citadel_notifications_call("mark_read", {
        recipient: String(recipient), ids: Array.from(ids, String),
      });
    },
    groups_call(actor, operation, payload) {
      if (globalThis.__citadel_realtime_interceptor) {
        throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
      }
      if (!globalThis.__citadel_groups) throw new Error("groups host not available");
      return JSON.parse(globalThis.__citadel_groups(String(actor), String(operation), JSON.stringify(payload)));
    },
    leaderboards_call(actor, operation, payload) { return __citadel_domain_call("leaderboards", actor, operation, payload); },
    chat_call(actor, operation, payload) { return __citadel_domain_call("chat", actor, operation, payload); },
    wallet_call(actor, operation, payload) { return __citadel_domain_call("wallet", actor, operation, payload); },
    storage_read(user, collection, key) {
      return __citadel_storage_call("read", String(user), String(collection), String(key));
    },
    storage_write(user, collection, key, valueJson, expectedVersion, readPermission, writePermission) {
      const userId = String(user);
      const collectionName = String(collection);
      const objectKey = String(key);
      const encodedValue = String(valueJson);
      const candidates = __citadel_storage_call("index_candidates", userId, collectionName, objectKey);
      const candidate = {
        user_id: userId, collection: collectionName, key: objectKey,
        value_json: encodedValue, expected_version: expectedVersion === undefined ? null : String(expectedVersion),
        read_permission: readPermission === undefined ? null : Number(readPermission),
        write_permission: writePermission === undefined ? null : Number(writePermission),
      };
      const included = [];
      for (const indexName of candidates) {
        candidate.index_name = indexName;
        const callback = storageIndexFilters.get(indexName);
        if (!callback) {
          included.push(indexName);
          continue;
        }
        const decision = callback({ ...candidate });
        if (typeof decision !== "boolean") {
          throw new TypeError("storage index filter must return a boolean");
        }
        if (decision) included.push(indexName);
      }
      return __citadel_storage_call("write", userId, collectionName, objectKey,
        encodedValue, expectedVersion, readPermission, writePermission, undefined, included);
    },
    register_storage_index_filter(indexName, callback) {
      ensureRealtimeEffectsAllowed();
      const name = String(indexName);
      if (!/^[A-Za-z_][A-Za-z0-9_]{0,39}$/.test(name)) {
        throw new TypeError("storage index name must be an ASCII identifier of at most 40 characters");
      }
      if (typeof callback !== "function") throw new TypeError("storage index filter must be callable");
      if (storageIndexFilters.has(name)) {
        throw new Error("storage index filter already registered for `" + name + "`");
      }
      storageIndexFilters.set(name, callback);
      return callback;
    },
    storage_delete(user, collection, key, expectedVersion) {
      __citadel_storage_call("delete", String(user), String(collection), String(key),
        "", expectedVersion);
    },
    storage_index_query(indexName, filtersJson, limit) {
      return __citadel_storage_call("index_query", String(indexName), "", "",
        String(filtersJson), undefined, undefined, undefined,
        limit === undefined ? 50 : Number(limit));
    },
  };

  function __citadel_friends_call(op, user, other) {
    if (globalThis.__citadel_realtime_interceptor) {
      throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
    }
    if (!globalThis.__citadel_friends) {
      throw new Error("friends host not available");
    }
    return JSON.parse(globalThis.__citadel_friends(op, user, other));
  }

  function __citadel_notifications_call(op, payload) {
    if (globalThis.__citadel_realtime_interceptor) {
      throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
    }
    if (!globalThis.__citadel_notifications) {
      throw new Error("notifications host not available");
    }
    return JSON.parse(globalThis.__citadel_notifications(op, JSON.stringify(payload)));
  }

  function __citadel_domain_call(domain, actor, operation, payload) {
    if (globalThis.__citadel_realtime_interceptor) {
      throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
    }
    if (!globalThis.__citadel_domain) throw new Error(domain + " host not available");
    return JSON.parse(globalThis.__citadel_domain(String(domain), String(actor), String(operation), JSON.stringify(payload)));
  }

  function __citadel_storage_call(op, user, collection, key, valueJson, expectedVersion, readPermission, writePermission, limit, includedIndexNames) {
    if (globalThis.__citadel_realtime_interceptor) {
      throw new Error("domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors");
    }
    if (!globalThis.__citadel_storage) {
      throw new Error("storage host not available");
    }
    return JSON.parse(globalThis.__citadel_storage(JSON.stringify({
      op, user, collection, key,
      value_json: valueJson === undefined ? null : String(valueJson),
      expected_version: expectedVersion === undefined ? null : String(expectedVersion),
      read_permission: readPermission === undefined ? null : Number(readPermission),
      write_permission: writePermission === undefined ? null : Number(writePermission),
      limit: limit === undefined ? null : Number(limit),
      included_index_names: includedIndexNames === undefined ? null : includedIndexNames,
    })));
  }

  globalThis.citadel = citadel;
  globalThis.__citadel_realtime_interceptor = false;
  globalThis.console = {
    log: (message) => log(message, "info"),
    info: (message) => log(message, "info"),
    warn: (message) => log(message, "warn"),
    error: (message) => log(message, "error"),
    debug: (message) => log(message, "debug"),
  };

  globalThis.__citadel_reset_commands = function () {
    commands = [];
    logs = [];
    totalBytes = 0;
    overflowed = false;
  };

  globalThis.__citadel_take_commands = function () {
    const out = commands.slice();
    const outLogs = logs.slice();
    const wasOverflowed = overflowed;
    globalThis.__citadel_reset_commands();
    return [out, wasOverflowed, outLogs];
  };

  globalThis.__citadel_dispatch_message = function (kind, ctx, body) {
    const handler = messageHandlers.get(Number(kind));
    if (!handler) {
      return false;
    }
    handler(ctx, bytes(body));
    return true;
  };

  globalThis.__citadel_before_realtime = function (ctx, body) {
    if (!beforeRealtime) {
      return true;
    }
    const decision = beforeRealtime(ctx, bytes(body));
    if (decision === undefined || decision === null || decision === true) {
      return true;
    }
    if (decision === false) {
      return false;
    }
    throw new TypeError("before_realtime must return false, true, null, or undefined");
  };

  globalThis.__citadel_after_realtime = function (ctx, body) {
    if (!afterRealtime) {
      return false;
    }
    afterRealtime(ctx, bytes(body));
    return true;
  };

  globalThis.__citadel_dispatch_lifecycle = function (hook, ctx) {
    const handler = hook === "on_join" ? onJoin : onLeave;
    if (!handler) {
      return false;
    }
    handler(ctx);
    return true;
  };

  globalThis.__citadel_dispatch_tick = function (dt) {
    if (!onTick) {
      return false;
    }
    onTick(Number(dt));
    return true;
  };

  globalThis.__citadel_call_rpc = function (method, ctx, body) {
    const handler = rpcHandlers.get(String(method));
    if (!handler) {
      return null;
    }
    const reply = handler(ctx, bytes(body));
    if (reply instanceof Reply) {
      return [reply.ok, reply.body, reply.error];
    }
    return [true, bytes(reply), ""];
  };

  globalThis.__citadel_call_room_create = function (ctx, params) {
    if (!onRoomCreate) {
      return null;
    }
    return roomSpec(onRoomCreate(ctx, bytes(params)));
  };

  globalThis.__citadel_call_room_join = function (ctx, roomId) {
    if (!onRoomJoin) {
      return null;
    }
    return Boolean(onRoomJoin(ctx, toRoomId(roomId)));
  };

  globalThis.__citadel_has_tick_handler = function () {
    return onTick !== null;
  };

  globalThis.__citadel_has_any_handler = function () {
    return messageHandlers.size > 0
      || rpcHandlers.size > 0
      || onJoin !== null
      || onLeave !== null
      || onTick !== null
      || onRoomCreate !== null
      || onRoomJoin !== null
      || beforeRealtime !== null
      || afterRealtime !== null;
  };

  globalThis.__citadel_introspect = function () {
    const hooks = [];
    if (onJoin) hooks.push("on_join");
    if (onLeave) hooks.push("on_leave");
    if (onTick) hooks.push("on_tick");
    if (onRoomCreate) hooks.push("on_room_create");
    if (onRoomJoin) hooks.push("on_room_join");
    if (beforeRealtime) hooks.push("before_realtime");
    if (afterRealtime) hooks.push("after_realtime");
    return [
      Array.from(rpcHandlers.keys()).sort(),
      Array.from(messageHandlers.keys()).sort((a, b) => a - b),
      hooks,
    ];
  };
}());
"#;

/// Resolve ESM specifiers into root-relative virtual module names. QuickJS sees
/// names such as `systems/combat.js`, never host filesystem paths.
#[derive(Clone)]
struct ScopedEsmResolver {
    root: PathBuf,
}

/// Load only script-root-contained ESM source and remember successfully parsed
/// dependencies for the development reload watcher.
#[derive(Clone)]
struct ScopedEsmLoader {
    root: PathBuf,
    loaded_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
}

impl Resolver for ScopedEsmResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &Ctx<'js>,
        base: &str,
        name: &str,
        attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        if attributes.is_some() {
            return Err(rquickjs::Error::new_resolving_message(
                base,
                name,
                "import attributes are not supported",
            ));
        }
        let path = resolve_esm_module_path(&self.root, base, name)
            .map_err(|reason| rquickjs::Error::new_resolving_message(base, name, reason))?;
        esm_module_id(&self.root, &path)
            .map_err(|reason| rquickjs::Error::new_resolving_message(base, name, reason))
    }
}

impl Loader for ScopedEsmLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<Module<'js>> {
        if attributes.is_some() {
            return Err(rquickjs::Error::new_loading_message(
                name,
                "import attributes are not supported",
            ));
        }
        let path = esm_module_path_from_id(&self.root, name)
            .and_then(|path| canonical_esm_module_path(&self.root, &path))
            .map_err(|reason| rquickjs::Error::new_loading_message(name, reason))?;
        let source = std::fs::read_to_string(&path)
            .map_err(|_| rquickjs::Error::new_loading_message(name, "module not found"))?;
        let module = Module::declare(ctx.clone(), name, source)?;
        lock_mutex(&self.loaded_paths).insert(path);
        Ok(module)
    }
}

struct JsVm {
    runtime: QuickJsRuntime,
    context: Context,
    source_label: String,
    interceptor_mode: Arc<AtomicBool>,
    /// Parsed static gameplay data initialized with this VM. Replaced atomically
    /// with the VM on hot reload so a bad data edit cannot partially publish.
    static_data: StaticDataCatalog,
    /// Canonical local ESM source paths successfully declared by this VM. The
    /// runtime uses this only to watch dependency edits during development.
    module_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
}

/// Authoritative transform hub captured by the synchronous QuickJS read bridge.
#[derive(Clone)]
struct TransformHubHandle(Arc<TransformHub>);

/// Embedded QuickJS runtime for capped JavaScript game logic.
pub struct JsRuntime {
    vm: Mutex<JsVm>,
    budget: Duration,
    reload_path: Option<PathBuf>,
    module_root: Option<PathBuf>,
    /// Optional operator-owned static-data directory, distinct from scripts.
    /// Retained so a hot reload builds a fresh catalog from the same root.
    static_data_dir: Option<PathBuf>,
    /// Per-file static-data read bound retained across reloads.
    static_data_max_file_bytes: usize,
    /// Persisted-domain-services seam exposed to `citadel.friends_*` host calls
    ///, or `None` when no services are attached. Retained so a
    /// reload re-applies it to the fresh context.
    domain: Option<Arc<dyn DomainHost>>,
    /// Loaded map catalog exposed read-only through `citadel.map_info`.
    maps: Option<Arc<MapCatalog>>,
    /// Transform hub retained across hot reload for `citadel.physics_state`.
    transform_hub: Option<Arc<TransformHub>>,
}

impl fmt::Debug for JsRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsRuntime")
            .field("budget", &self.budget)
            .field("reload_path", &self.reload_path)
            .finish_non_exhaustive()
    }
}

impl JsRuntime {
    /// Load `main.js` from `scripts_dir`, or `Ok(None)` if it is absent.
    pub fn load(scripts_dir: &Path, deadline_ms: u64) -> AppResult<Option<Self>> {
        Self::load_with_static_data(
            scripts_dir,
            deadline_ms,
            None,
            crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
        )
    }

    /// Load `main.js` with an optional, separately configured static-data root.
    ///
    /// The root is never made visible to JavaScript. The script can only request
    /// validated relative JSON/CSV paths through `citadel.static_data` while its
    /// top-level initialization body runs.
    pub fn load_with_static_data(
        scripts_dir: &Path,
        deadline_ms: u64,
        static_data_dir: Option<&Path>,
        static_data_max_file_bytes: usize,
    ) -> AppResult<Option<Self>> {
        let main = scripts_dir.join(JS_ENTRYPOINT);
        if !main.is_file() {
            return Ok(None);
        }
        let source = read_script(&main)?;
        let module_root = scripts_dir.to_path_buf();
        let source_label = main.display().to_string();
        let static_data = StaticDataCatalog::new(static_data_dir, static_data_max_file_bytes)?;
        let vm = build_js(
            &source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            Some(&module_root),
            static_data,
        )?;
        Ok(Some(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: Some(main),
            module_root: Some(module_root),
            static_data_dir: static_data_dir.map(Path::to_path_buf),
            static_data_max_file_bytes,
            domain: None,
            maps: None,
            transform_hub: None,
        }))
    }

    /// Build a runtime from inline JavaScript source.
    pub fn from_source(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let vm = build_js(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            None,
            static_data,
        )?;
        Ok(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: None,
            module_root: None,
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            domain: None,
            maps: None,
            transform_hub: None,
        })
    }

    /// Build a runtime from inline source with a root recorded for reload parity.
    pub fn from_source_with_root(
        source: &str,
        label: impl Into<String>,
        deadline_ms: u64,
        module_root: &Path,
    ) -> AppResult<Self> {
        let source_label = label.into();
        let static_data =
            StaticDataCatalog::new(None, crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES)?;
        let vm = build_js(
            source,
            &source_label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            Some(module_root),
            static_data,
        )?;
        Ok(Self {
            vm: Mutex::new(vm),
            budget: Duration::from_millis(deadline_ms.max(1)),
            reload_path: None,
            module_root: Some(module_root.to_path_buf()),
            static_data_dir: None,
            static_data_max_file_bytes: crate::runtime::DEFAULT_STATIC_DATA_MAX_FILE_BYTES,
            domain: None,
            maps: None,
            transform_hub: None,
        })
    }

    /// Attach domain-services to this runtime for host calls. Retained across
    /// reload so friends functions keep working after script updates.
    #[must_use]
    pub fn with_domain_host(mut self, host: Arc<dyn DomainHost>) -> Self {
        self.domain = Some(host);
        {
            let guard = lock_mutex(&self.vm);
            apply_domain_host(
                &guard.context,
                &self.domain,
                Arc::clone(&guard.interceptor_mode),
            );
        }
        self
    }

    /// Attach the loaded-map catalog for read-only `citadel.map_info` calls.
    #[must_use]
    pub fn with_maps(mut self, maps: Arc<MapCatalog>) -> Self {
        self.maps = Some(maps);
        {
            let guard = lock_mutex(&self.vm);
            apply_map_catalog(&guard.context, &self.maps);
        }
        self
    }

    /// Attach the transform hub for synchronous `citadel.physics_state` reads.
    #[must_use]
    pub fn with_transform_hub(mut self, hub: Arc<TransformHub>) -> Self {
        self.transform_hub = Some(hub);
        {
            let guard = lock_mutex(&self.vm);
            apply_transform_hub(&guard.context, &self.transform_hub);
        }
        self
    }

    /// Names registered into the JavaScript `citadel` global by this adapter.
    #[must_use]
    pub fn registered_host_api_names() -> HashSet<&'static str> {
        JS_HOST_API_NAMES.iter().copied().collect()
    }

    /// Whether this runtime is backed by an on-disk script.
    #[must_use]
    pub fn is_reloadable(&self) -> bool {
        self.reload_path.is_some()
    }

    /// Rebuild from the backing `main.js`, rejecting broken or handlerless edits.
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
                    "javascript hot-reload: cannot read script; keeping current runtime"
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
                    "javascript hot-reload: cannot initialize static-data catalog; keeping the current script and data"
                );
                return ReloadOutcome::Rejected;
            }
        };
        let fresh = match build_js(
            &source,
            &label,
            Duration::from_millis(LOAD_DEADLINE_MS),
            self.module_root.as_deref(),
            fresh_static_data,
        ) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::error!(
                    script = %label,
                    error = %e,
                    "javascript hot-reload: new script rejected; keeping current runtime"
                );
                return ReloadOutcome::Rejected;
            }
        };
        if !vm_has_any_handler(&fresh) {
            tracing::warn!(
                script = %label,
                "javascript hot-reload: new script registered no handlers; keeping current runtime"
            );
            return ReloadOutcome::Rejected;
        }
        // Re-apply the domain-services seam so `citadel.friends_*` keeps working
        // after the swap (the rebuilt context starts with no domain host attached).
        apply_domain_host(
            &fresh.context,
            &self.domain,
            Arc::clone(&fresh.interceptor_mode),
        );
        apply_map_catalog(&fresh.context, &self.maps);
        apply_transform_hub(&fresh.context, &self.transform_hub);
        {
            let mut guard = lock_mutex(&self.vm);
            *guard = fresh;
        }
        tracing::info!(
            script = %path.display(),
            "javascript hot-reload: swapped in updated script"
        );
        ReloadOutcome::Reloaded
    }

    /// Entry script plus the local ESM and static-data files initialized by the
    /// live VM.
    ///
    /// The returned list is consumed by the development hot-reload watcher only;
    /// it never participates in runtime dispatch or tick execution.
    #[must_use]
    pub fn reload_watch_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.reload_path.iter().cloned().collect::<Vec<_>>();
        let guard = lock_mutex(&self.vm);
        paths.extend(lock_mutex(&guard.module_paths).iter().cloned());
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
        self.run_commands("message", self.budget, |ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_dispatch_message"))?;
            let js_ctx = make_ctx(&ctx, sender, user_id, Some(kind), None, None)?;
            let body = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, func.call((u32::from(kind), js_ctx, body)))
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
        self.run_commands("match_message", self.budget, |ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_dispatch_message"))?;
            let js_ctx = make_ctx(&ctx, sender, user_id, Some(kind), None, Some(room_id))?;
            let body = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, func.call((u32::from(kind), js_ctx, body)))
        })
    }

    /// Run the optional before-realtime interceptor. A `false` result vetoes the
    /// envelope; any script failure is isolated and fails closed. Commands from
    /// this phase are discarded.
    pub fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        self.run_before_realtime(|ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_before_realtime"))?;
            let js_ctx = make_ctx(&ctx, sender, user_id, Some(kind), None, room_id)?;
            let body_for_ctx = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, js_ctx.set("body", body_for_ctx))?;
            let body_for_handler = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, func.call((js_ctx, body_for_handler)))
        })
    }

    /// Run the optional after-realtime observer. It shares the normal runtime
    /// isolation, but its command sink is discarded because routing is complete.
    pub fn after_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
        outcome: RealtimeAfterOutcome,
    ) {
        let _ = self.run_restricted_commands("after_realtime", self.budget, |ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_after_realtime"))?;
            let js_ctx = make_ctx(&ctx, sender, user_id, Some(kind), None, room_id)?;
            let body_for_ctx = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, js_ctx.set("body", body_for_ctx))?;
            caught(&ctx, js_ctx.set("dropped", outcome.dropped))?;
            caught(&ctx, js_ctx.set("delivered", outcome.delivered))?;
            let body_for_handler = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
            caught(&ctx, func.call((js_ctx, body_for_handler)))
        });
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
        self.run_commands(hook_name, self.budget, |ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_dispatch_lifecycle"))?;
            let js_ctx = make_ctx(&ctx, sender, user_id, None, None, None)?;
            caught(&ctx, func.call((hook_name, js_ctx)))
        })
    }

    /// Dispatch the periodic tick handler with `dt` in seconds.
    pub fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        let dt_secs = dt.as_secs_f64();
        self.run_commands("on_tick", budget, |ctx| {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_dispatch_tick"))?;
            caught(&ctx, func.call((dt_secs,)))
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
        let guard = self.lock_vm();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_with_js_deadline(&guard.runtime, self.budget, || {
                guard.context.with(|ctx| {
                    clear_commands(&ctx);
                    let globals = ctx.globals();
                    let func: Function = caught(&ctx, globals.get("__citadel_call_rpc"))?;
                    let js_ctx = make_ctx(&ctx, sender, user_id, None, Some(method), None)?;
                    let body = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), body))?;
                    let reply: Value = caught(&ctx, func.call((method, js_ctx, body)))?;
                    discard_sink(&ctx, &guard.source_label, "rpc");
                    parse_rpc_reply(reply)
                })
            })
        }));
        match outcome {
            Ok(Ok(JsRpcInner::Reply(bytes))) => RpcOutcome::Ok(bytes),
            Ok(Ok(JsRpcInner::HandlerErr(msg))) => RpcOutcome::Err(msg),
            Ok(Ok(JsRpcInner::NoHandler)) => {
                tracing::debug!(
                    script = %guard.source_label,
                    method,
                    "no javascript rpc handler for method"
                );
                RpcOutcome::Err(format!("{RPC_ERR_UNKNOWN_METHOD}: {method}"))
            }
            Ok(Err(JsInvocationError::Timeout)) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    "javascript rpc handler timed out; isolated"
                );
                clear_vm_commands(&guard);
                RpcOutcome::Err(RPC_ERR_TIMEOUT.to_string())
            }
            Ok(Err(JsInvocationError::Error(e))) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    error = %e,
                    "javascript rpc handler error; isolated"
                );
                clear_vm_commands(&guard);
                RpcOutcome::Err(RPC_ERR_HANDLER.to_string())
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    method,
                    "javascript rpc handler panicked; isolated"
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
        let guard = self.lock_vm();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_with_js_deadline(&guard.runtime, self.budget, || {
                guard.context.with(|ctx| {
                    clear_commands(&ctx);
                    let globals = ctx.globals();
                    let func: Function = caught(&ctx, globals.get("__citadel_call_room_create"))?;
                    let js_ctx = make_ctx(&ctx, sender, user_id, None, Some("room.create"), None)?;
                    let params = caught(&ctx, TypedArray::<u8>::new_copy(ctx.clone(), params))?;
                    let spec: Value = caught(&ctx, func.call((js_ctx, params)))?;
                    discard_sink(&ctx, &guard.source_label, "on_room_create");
                    parse_room_spec(spec)
                })
            })
        }));
        match outcome {
            Ok(Ok(spec)) => spec,
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %e,
                    "javascript on_room_create error; isolated, using default label"
                );
                clear_vm_commands(&guard);
                None
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "javascript on_room_create panicked; isolated"
                );
                clear_vm_commands(&guard);
                None
            }
        }
    }

    /// Dispatch room-join admission gate.
    pub fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        let guard = self.lock_vm();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_with_js_deadline(&guard.runtime, self.budget, || {
                guard.context.with(|ctx| {
                    clear_commands(&ctx);
                    let globals = ctx.globals();
                    let func: Function = caught(&ctx, globals.get("__citadel_call_room_join"))?;
                    let js_ctx = make_ctx(
                        &ctx,
                        sender,
                        user_id,
                        None,
                        Some("room.join"),
                        Some(room_id),
                    )?;
                    let decision: Option<bool> =
                        caught(&ctx, func.call((js_ctx, room_id.to_string())))?;
                    discard_sink(&ctx, &guard.source_label, "on_room_join");
                    Ok(decision)
                })
            })
        }));
        match outcome {
            Ok(Ok(decision)) => decision.unwrap_or(true),
            Ok(Err(e)) => {
                tracing::error!(
                    script = %guard.source_label,
                    error = %e,
                    "javascript on_room_join error; isolated, rejecting"
                );
                clear_vm_commands(&guard);
                false
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    "javascript on_room_join panicked; isolated, rejecting"
                );
                clear_vm_commands(&guard);
                false
            }
        }
    }

    /// Whether an `on_tick` handler is registered.
    #[must_use]
    pub fn has_tick_handler(&self) -> bool {
        let guard = self.lock_vm();
        guard
            .context
            .with(|ctx| -> JsHostResult<bool> {
                let globals = ctx.globals();
                let func: Function = caught(&ctx, globals.get("__citadel_has_tick_handler"))?;
                caught(&ctx, func.call(()))
            })
            .unwrap_or(false)
    }

    /// Point-in-time handler introspection for console/API surfaces.
    #[must_use]
    pub fn introspect(&self) -> RuntimeIntrospection {
        let guard = self.lock_vm();
        let (rpcs, message_kinds, hooks) = guard
            .context
            .with(
                |ctx| -> JsHostResult<(Vec<String>, Vec<u32>, Vec<String>)> {
                    let globals = ctx.globals();
                    let func: Function = caught(&ctx, globals.get("__citadel_introspect"))?;
                    let value: Array = caught(&ctx, func.call(()))?;
                    let rpcs = js_array_strings(value.get(0).map_err(|e| e.to_string())?)?;
                    let message_kinds = js_array_u32(value.get(1).map_err(|e| e.to_string())?)?;
                    let hooks = js_array_strings(value.get(2).map_err(|e| e.to_string())?)?;
                    Ok((rpcs, message_kinds, hooks))
                },
            )
            .unwrap_or_default();
        RuntimeIntrospection {
            source: format!("{} (QuickJS)", guard.source_label),
            reloadable: self.reload_path.is_some(),
            deadline_ms: u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
            rpcs,
            message_kinds,
            hooks,
        }
    }

    fn lock_vm(&self) -> MutexGuard<'_, JsVm> {
        lock_mutex(&self.vm)
    }

    fn run_commands<F>(&self, what: &str, budget: Duration, call: F) -> Vec<OutboundCommand>
    where
        F: FnOnce(Ctx<'_>) -> JsHostResult<bool>,
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
        F: FnOnce(Ctx<'_>) -> JsHostResult<bool>,
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
        F: FnOnce(Ctx<'_>) -> JsHostResult<bool>,
    {
        let guard = self.lock_vm();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            guard.interceptor_mode.store(restricted, Ordering::Relaxed);
            run_with_js_deadline(&guard.runtime, budget, || {
                guard.context.with(|ctx| {
                    clear_commands(&ctx);
                    set_realtime_interceptor_mode(&ctx, restricted)?;
                    let ran = call(ctx.clone());
                    match ran {
                        Ok(true) => take_commands(&ctx, &guard.source_label, what),
                        Ok(false) => {
                            clear_commands(&ctx);
                            Ok(Vec::new())
                        }
                        Err(e) => {
                            clear_commands(&ctx);
                            Err(e)
                        }
                    }
                })
            })
        }));
        guard.interceptor_mode.store(false, Ordering::Relaxed);
        clear_vm_realtime_interceptor_mode(&guard);
        match outcome {
            Ok(Ok(commands)) => commands,
            Ok(Err(JsInvocationError::Timeout)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    "javascript handler timed out; isolated, side effects discarded"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                Vec::new()
            }
            Ok(Err(JsInvocationError::Error(e))) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    error = %e,
                    "javascript handler error; isolated, side effects discarded"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                Vec::new()
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = what,
                    "javascript handler panicked; isolated and dropped"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                Vec::new()
            }
        }
    }

    /// Run a pre-routing decision with the usual lock and deadline, but always
    /// clear commands afterwards. Errors, timeouts, and panics intentionally veto
    /// the envelope rather than exposing a partially executed interceptor.
    fn run_before_realtime<F>(&self, call: F) -> RealtimeInterception
    where
        F: FnOnce(Ctx<'_>) -> JsHostResult<bool>,
    {
        let guard = self.lock_vm();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            guard.interceptor_mode.store(true, Ordering::Relaxed);
            run_with_js_deadline(&guard.runtime, self.budget, || {
                guard.context.with(|ctx| {
                    clear_commands(&ctx);
                    set_realtime_interceptor_mode(&ctx, true)?;
                    let decision = call(ctx.clone());
                    set_realtime_interceptor_mode(&ctx, false)?;
                    clear_commands(&ctx);
                    decision
                })
            })
        }));
        guard.interceptor_mode.store(false, Ordering::Relaxed);
        clear_vm_realtime_interceptor_mode(&guard);
        match outcome {
            Ok(Ok(true)) => RealtimeInterception::Continue,
            Ok(Ok(false)) => RealtimeInterception::Drop,
            Ok(Err(JsInvocationError::Timeout)) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    "javascript realtime interceptor timed out; vetoing envelope"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                RealtimeInterception::Drop
            }
            Ok(Err(JsInvocationError::Error(error))) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    error = %error,
                    "javascript realtime interceptor failed; vetoing envelope"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                RealtimeInterception::Drop
            }
            Err(_) => {
                tracing::error!(
                    script = %guard.source_label,
                    handler = "before_realtime",
                    "javascript realtime interceptor panicked; vetoing envelope"
                );
                clear_vm_commands(&guard);
                clear_vm_realtime_interceptor_mode(&guard);
                RealtimeInterception::Drop
            }
        }
    }
}

impl Runtime for JsRuntime {
    fn before_realtime(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: Option<u64>,
        kind: u16,
        body: &[u8],
    ) -> RealtimeInterception {
        JsRuntime::before_realtime(self, sender, user_id, room_id, kind, body)
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
        JsRuntime::after_realtime(self, sender, user_id, room_id, kind, body, outcome);
    }

    fn dispatch(
        &self,
        sender: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        JsRuntime::dispatch(self, sender, user_id, kind, body)
    }

    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        JsRuntime::dispatch_in_room(self, sender, user_id, room_id, kind, body)
    }

    fn dispatch_lifecycle(
        &self,
        hook: LifecycleHook,
        sender: u64,
        user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        JsRuntime::dispatch_lifecycle(self, hook, sender, user_id)
    }

    fn tick(&self, dt: Duration, budget: Duration) -> Vec<OutboundCommand> {
        JsRuntime::tick(self, dt, budget)
    }

    fn call_rpc(
        &self,
        sender: u64,
        user_id: Option<&str>,
        method: &str,
        body: &[u8],
    ) -> RpcOutcome {
        JsRuntime::call_rpc(self, sender, user_id, method, body)
    }

    fn call_room_create(
        &self,
        sender: u64,
        user_id: Option<&str>,
        params: &[u8],
    ) -> Option<RoomSpec> {
        JsRuntime::call_room_create(self, sender, user_id, params)
    }

    fn call_room_join(&self, sender: u64, user_id: Option<&str>, room_id: u64) -> bool {
        JsRuntime::call_room_join(self, sender, user_id, room_id)
    }

    fn has_tick_handler(&self) -> bool {
        JsRuntime::has_tick_handler(self)
    }

    fn budget(&self) -> Duration {
        JsRuntime::budget(self)
    }

    fn introspect(&self) -> RuntimeIntrospection {
        JsRuntime::introspect(self)
    }

    fn is_reloadable(&self) -> bool {
        JsRuntime::is_reloadable(self)
    }

    fn reload(&self) -> ReloadOutcome {
        JsRuntime::reload(self)
    }

    fn reload_watch_paths(&self) -> Vec<PathBuf> {
        JsRuntime::reload_watch_paths(self)
    }
}

type JsHostResult<T> = Result<T, String>;

enum JsRpcInner {
    Reply(Vec<u8>),
    HandlerErr(String),
    NoHandler,
}

#[derive(Debug)]
enum JsInvocationError {
    Timeout,
    Error(String),
}

impl fmt::Display for JsInvocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("handler exceeded its time budget"),
            Self::Error(error) => f.write_str(error),
        }
    }
}

/// Install the small static-data capability before game-script initialization.
/// The JavaScript script receives only parsed JSON text through a native bridge;
/// it never receives a filesystem path, file descriptor, or directory handle.
fn install_static_data(ctx: &Ctx<'_>, static_data: StaticDataCatalog) -> JsHostResult<()> {
    let loader = caught(
        ctx,
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'_>, format: String, path: String| -> rquickjs::Result<String> {
                let result = match format.as_str() {
                    "json" => static_data.load_json(&path),
                    "csv" => static_data.load_csv(&path),
                    _ => return throw_js(&ctx, "invalid static data format".to_string()),
                };
                match result {
                    Ok(value) => Ok(value.to_string()),
                    Err(error) => throw_js(&ctx, error.to_string()),
                }
            },
        ),
    )?;
    caught(ctx, ctx.globals().set("__citadel_static_data", loader))?;
    Ok(())
}

/// Install the bounded Rust-owned HTTP bridge. The JavaScript prelude converts
/// its JSON result into a `Uint8Array`, so scripts never see a socket or client.
fn install_outbound_http(ctx: &Ctx<'_>, interceptor_mode: Arc<AtomicBool>) -> JsHostResult<()> {
    let client = TrustedHttpClient::new().map_err(|error| error.to_string())?;
    let fetch = caught(
        ctx,
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'_>, url: String, opts_json: String| -> rquickjs::Result<String> {
                ensure_realtime_effects_allowed(&ctx, &interceptor_mode)?;
                let opts: serde_json::Value = match serde_json::from_str(&opts_json) {
                    Ok(value) => value,
                    Err(_) => return throw_js(&ctx, "invalid HTTP options".to_string()),
                };
                let method = opts
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("GET")
                    .to_string();
                let body = opts
                    .get("body")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec();
                let headers: BTreeMap<String, String> = match opts
                    .get("headers")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                {
                    Ok(headers) => headers.unwrap_or_default(),
                    Err(_) => {
                        return throw_js(&ctx, "HTTP headers must be a string map".to_string());
                    }
                };
                match client.execute_blocking(OutboundHttpRequest {
                    method,
                    url,
                    headers,
                    body,
                }) {
                    Ok(response) => Ok(serde_json::json!({
                        "status": response.status,
                        "body": response.body,
                    })
                    .to_string()),
                    Err(error) => throw_js(&ctx, error.to_string()),
                }
            },
        ),
    )?;
    caught(ctx, ctx.globals().set("__citadel_http_fetch", fetch))?;
    Ok(())
}

fn install_realtime_interceptor_guard(
    ctx: &Ctx<'_>,
    interceptor_mode: Arc<AtomicBool>,
) -> JsHostResult<()> {
    let guard = caught(
        ctx,
        Function::new(ctx.clone(), move |ctx: Ctx<'_>| -> rquickjs::Result<()> {
            ensure_realtime_effects_allowed(&ctx, &interceptor_mode)
        }),
    )?;
    caught(
        ctx,
        ctx.globals()
            .set("__citadel_realtime_effects_allowed", guard),
    )?;
    Ok(())
}

/// Canonicalize the operator-owned scripts directory once for each VM build.
/// The ESM resolver never returns a host path to QuickJS; it uses this root only
/// to validate and read local source files.
fn canonical_esm_root(module_root: &Path) -> AppResult<PathBuf> {
    module_root.canonicalize().map_err(|_| {
        AppError::new(
            ErrorCategory::Runtime,
            "cannot resolve JavaScript module root",
        )
    })
}

/// Resolve a static ESM specifier without granting a general filesystem
/// capability. Only `./` and `../` imports ending in `.js` are meaningful;
/// `..` may move between directories but can never escape the canonical root.
fn resolve_esm_module_path(root: &Path, base: &str, specifier: &str) -> Result<PathBuf, String> {
    if specifier.is_empty()
        || specifier.contains('\\')
        || Path::new(specifier).is_absolute()
        || !(specifier.starts_with("./") || specifier.starts_with("../"))
    {
        return Err("only relative local JavaScript modules are allowed".to_owned());
    }
    if Path::new(specifier)
        .extension()
        .and_then(|value| value.to_str())
        != Some("js")
    {
        return Err("only .js modules are allowed".to_owned());
    }
    let base_path = esm_module_path_from_id(root, base)?;
    let parent = base_path
        .parent()
        .ok_or_else(|| "invalid JavaScript module base".to_owned())?;
    canonical_esm_module_path(root, &parent.join(specifier))
}

/// Convert one virtual module name into an untrusted candidate path. A virtual
/// name comes from this resolver, but re-validating it in the loader makes the
/// filesystem boundary explicit at both rquickjs callbacks.
fn esm_module_path_from_id(root: &Path, module_id: &str) -> Result<PathBuf, String> {
    if module_id.is_empty() || module_id.contains('\\') || Path::new(module_id).is_absolute() {
        return Err("invalid JavaScript module name".to_owned());
    }
    let mut path = root.to_path_buf();
    for component in Path::new(module_id).components() {
        match component {
            Component::Normal(segment) => path.push(segment),
            _ => return Err("invalid JavaScript module name".to_owned()),
        }
    }
    if path.extension().and_then(|value| value.to_str()) != Some("js") {
        return Err("only .js modules are allowed".to_owned());
    }
    Ok(path)
}

/// Follow the candidate to its canonical target and reject missing files,
/// directories, and symlinks that point outside the game scripts root.
fn canonical_esm_module_path(root: &Path, candidate: &Path) -> Result<PathBuf, String> {
    let path = candidate
        .canonicalize()
        .map_err(|_| "module not found".to_owned())?;
    if !path.is_file() {
        return Err("module not found".to_owned());
    }
    if !path.starts_with(root) {
        return Err("module access denied".to_owned());
    }
    Ok(path)
}

/// Return a normalized `/`-separated virtual name for QuickJS. This keeps
/// source errors and nested import bases portable and never exposes root paths.
fn esm_module_id(root: &Path, path: &Path) -> Result<String, String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| "module access denied".to_owned())?;
    let mut segments = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(segment) => segments.push(segment.to_string_lossy().into_owned()),
            _ => return Err("invalid JavaScript module name".to_owned()),
        }
    }
    if segments.is_empty() {
        return Err("invalid JavaScript module name".to_owned());
    }
    Ok(segments.join("/"))
}

fn build_js(
    source: &str,
    source_label: &str,
    load_budget: Duration,
    module_root: Option<&Path>,
    static_data: StaticDataCatalog,
) -> AppResult<JsVm> {
    let module_root = module_root.map(canonical_esm_root).transpose()?;
    let module_paths = Arc::new(Mutex::new(BTreeSet::new()));
    let interceptor_mode = Arc::new(AtomicBool::new(false));
    let runtime = QuickJsRuntime::new().map_err(|e| {
        script_error(
            &format!("failed to create QuickJS runtime for {source_label}"),
            e,
        )
    })?;
    runtime.set_memory_limit(JS_MEMORY_LIMIT_BYTES);
    runtime.set_max_stack_size(JS_STACK_LIMIT_BYTES);
    if let Some(root) = &module_root {
        runtime.set_loader(
            ScopedEsmResolver { root: root.clone() },
            ScopedEsmLoader {
                root: root.clone(),
                loaded_paths: Arc::clone(&module_paths),
            },
        );
    }
    let context = Context::full(&runtime).map_err(|e| {
        script_error(
            &format!("failed to create QuickJS context for {source_label}"),
            e,
        )
    })?;
    run_with_js_deadline(&runtime, load_budget, || {
        context.with(|ctx| {
            install_realtime_interceptor_guard(&ctx, Arc::clone(&interceptor_mode))?;
            let prelude_opts = eval_options("citadel_host.js");
            caught(
                &ctx,
                ctx.eval_with_options::<(), _>(JS_HOST_PRELUDE.as_bytes().to_vec(), prelude_opts),
            )?;
            install_static_data(&ctx, static_data.clone())?;
            install_outbound_http(&ctx, Arc::clone(&interceptor_mode))?;
            if let Some(root) = &module_root {
                let entry_name = esm_module_id(root, &root.join(JS_ENTRYPOINT))?;
                let promise = caught(
                    &ctx,
                    Module::evaluate(ctx.clone(), entry_name, source.as_bytes().to_vec()),
                )?;
                caught(&ctx, promise.finish::<()>())
            } else {
                let source_opts = eval_options(source_label);
                caught(
                    &ctx,
                    ctx.eval_with_options::<(), _>(source.as_bytes().to_vec(), source_opts),
                )
            }
        })
    })
    .map_err(|e| {
        AppError::new(
            ErrorCategory::Runtime,
            format!("failed to load JavaScript game script: {source_label}"),
        )
        .with_detail(e.to_string())
    })?;
    static_data.seal();
    Ok(JsVm {
        runtime,
        context,
        source_label: source_label.to_string(),
        interceptor_mode,
        static_data,
        module_paths,
    })
}

fn eval_options(filename: &str) -> EvalOptions {
    let mut options = EvalOptions::default();
    options.filename = Some(filename.to_string());
    options
}

fn run_with_js_deadline<T>(
    runtime: &QuickJsRuntime,
    budget: Duration,
    call: impl FnOnce() -> JsHostResult<T>,
) -> Result<T, JsInvocationError> {
    let timed_out = Arc::new(AtomicBool::new(false));
    let now = Instant::now();
    let deadline = match now.checked_add(budget.max(Duration::from_millis(1))) {
        Some(deadline) => deadline,
        None => now,
    };
    let _guard = InterruptGuard::new(runtime, deadline, Arc::clone(&timed_out));
    let result = call();
    if timed_out.load(Ordering::Relaxed) {
        Err(JsInvocationError::Timeout)
    } else {
        result.map_err(JsInvocationError::Error)
    }
}

struct InterruptGuard<'a> {
    runtime: &'a QuickJsRuntime,
}

impl<'a> InterruptGuard<'a> {
    fn new(runtime: &'a QuickJsRuntime, deadline: Instant, timed_out: Arc<AtomicBool>) -> Self {
        runtime.set_interrupt_handler(Some(Box::new(move || {
            if Instant::now() >= deadline {
                timed_out.store(true, Ordering::Relaxed);
                true
            } else {
                false
            }
        })));
        Self { runtime }
    }
}

impl Drop for InterruptGuard<'_> {
    fn drop(&mut self) {
        self.runtime.set_interrupt_handler(None);
    }
}

fn caught<'js, T>(ctx: &Ctx<'js>, result: rquickjs::Result<T>) -> JsHostResult<T> {
    result.catch(ctx).map_err(|e| e.to_string())
}

fn make_ctx<'js>(
    ctx: &Ctx<'js>,
    sender: u64,
    user_id: Option<&str>,
    kind: Option<u16>,
    method: Option<&str>,
    room_id: Option<u64>,
) -> JsHostResult<Object<'js>> {
    let obj = caught(ctx, Object::new(ctx.clone()))?;
    let sender_bigint = caught(ctx, BigInt::from_u64(ctx.clone(), sender))?;
    caught(ctx, obj.set("sender", sender_bigint))?;
    caught(ctx, obj.set("sender_id", sender.to_string()))?;
    caught(ctx, obj.set("sender_number", sender as f64))?;
    match user_id {
        Some(user_id) => caught(ctx, obj.set("user_id", user_id))?,
        None => caught(ctx, obj.set("user_id", Value::new_null(ctx.clone())))?,
    }
    match kind {
        Some(kind) => caught(ctx, obj.set("kind", u32::from(kind)))?,
        None => caught(ctx, obj.set("kind", Value::new_null(ctx.clone())))?,
    }
    match method {
        Some(method) => caught(ctx, obj.set("method", method))?,
        None => caught(ctx, obj.set("method", Value::new_null(ctx.clone())))?,
    }
    match room_id {
        Some(room_id) => {
            let room_bigint = caught(ctx, BigInt::from_u64(ctx.clone(), room_id))?;
            caught(ctx, obj.set("room_id", room_bigint))?;
            caught(ctx, obj.set("room_id_text", room_id.to_string()))?;
            caught(ctx, obj.set("room_id_number", room_id as f64))?;
        }
        None => {
            caught(ctx, obj.set("room_id", Value::new_null(ctx.clone())))?;
            caught(ctx, obj.set("room_id_text", Value::new_null(ctx.clone())))?;
            caught(ctx, obj.set("room_id_number", Value::new_null(ctx.clone())))?;
        }
    }
    Ok(obj)
}

fn clear_commands(ctx: &Ctx<'_>) {
    let result = (|| -> JsHostResult<()> {
        let globals = ctx.globals();
        let reset: Function = caught(ctx, globals.get("__citadel_reset_commands"))?;
        caught(ctx, reset.call(()))
    })();
    if let Err(e) = result {
        tracing::warn!(error = %e, "failed to clear javascript command sink");
    }
}

fn clear_vm_commands(vm: &JsVm) {
    vm.context.with(|ctx| clear_commands(&ctx));
}

fn set_realtime_interceptor_mode(ctx: &Ctx<'_>, enabled: bool) -> JsHostResult<()> {
    caught(
        ctx,
        ctx.globals().set("__citadel_realtime_interceptor", enabled),
    )
}

fn clear_vm_realtime_interceptor_mode(vm: &JsVm) {
    vm.context.with(|ctx| {
        let _ = set_realtime_interceptor_mode(&ctx, false);
    });
}

fn ensure_realtime_effects_allowed(
    ctx: &Ctx<'_>,
    interceptor_mode: &Arc<AtomicBool>,
) -> rquickjs::Result<()> {
    if interceptor_mode.load(Ordering::Relaxed) {
        return throw_js(
            ctx,
            "domain, storage, and outbound HTTP APIs are unavailable in realtime interceptors"
                .to_string(),
        );
    }
    Ok(())
}

fn take_commands(ctx: &Ctx<'_>, label: &str, handler: &str) -> JsHostResult<Vec<OutboundCommand>> {
    let (commands, overflowed, logs) = drain_sink(ctx)?;
    emit_js_logs(logs, label);
    if overflowed {
        tracing::warn!(
            script = %label,
            handler,
            cap = MAX_OUTBOUND_COMMANDS,
            "javascript handler exceeded outbound command cap; extra commands dropped"
        );
    }
    parse_commands(commands)
}

fn discard_sink(ctx: &Ctx<'_>, label: &str, handler: &str) {
    match drain_sink(ctx) {
        Ok((_commands, overflowed, logs)) => {
            emit_js_logs(logs, label);
            if overflowed {
                tracing::warn!(
                    script = %label,
                    handler,
                    cap = MAX_OUTBOUND_COMMANDS,
                    "javascript handler exceeded outbound command cap; commands discarded"
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                script = %label,
                handler,
                error = %e,
                "failed to drain javascript command sink"
            );
        }
    }
}

fn drain_sink<'js>(ctx: &Ctx<'js>) -> JsHostResult<(Array<'js>, bool, Array<'js>)> {
    let globals = ctx.globals();
    let take: Function = caught(ctx, globals.get("__citadel_take_commands"))?;
    let taken: Array = caught(ctx, take.call(()))?;
    let commands = taken.get(0).map_err(|e| e.to_string())?;
    let overflowed = taken.get(1).map_err(|e| e.to_string())?;
    let logs = taken.get(2).map_err(|e| e.to_string())?;
    Ok((commands, overflowed, logs))
}

fn emit_js_logs(logs: Array<'_>, label: &str) {
    for item in logs.iter::<Array<'_>>() {
        let Ok(entry) = item else {
            continue;
        };
        let level = entry
            .get::<String>(0)
            .unwrap_or_else(|_| "info".to_string());
        let message = entry.get::<String>(1).unwrap_or_else(|_| String::new());
        match level.as_str() {
            "trace" | "debug" => {
                tracing::debug!(script = %label, message = %message, "javascript log")
            }
            "warn" | "warning" => {
                tracing::warn!(script = %label, message = %message, "javascript log")
            }
            "error" => tracing::error!(script = %label, message = %message, "javascript log"),
            _ => tracing::info!(script = %label, message = %message, "javascript log"),
        }
    }
}

fn parse_commands(commands: Array<'_>) -> JsHostResult<Vec<OutboundCommand>> {
    let mut out = Vec::with_capacity(commands.len());
    for item in commands.iter::<Array<'_>>() {
        let tuple = item.map_err(|e| e.to_string())?;
        let tag: String = tuple.get(0).map_err(|e| e.to_string())?;
        let command = match tag.as_str() {
            "broadcast" => OutboundCommand::Broadcast {
                kind: tuple.get(1).map_err(|e| e.to_string())?,
                body: bytes_from_js(tuple.get(2).map_err(|e| e.to_string())?)?,
                unreliable: tuple.get(3).map_err(|e| e.to_string())?,
            },
            "send" => {
                let session: String = tuple.get(1).map_err(|e| e.to_string())?;
                OutboundCommand::Send {
                    session: session
                        .parse::<u64>()
                        .map_err(|e| format!("invalid session id '{session}': {e}"))?,
                    kind: tuple.get(2).map_err(|e| e.to_string())?,
                    body: bytes_from_js(tuple.get(3).map_err(|e| e.to_string())?)?,
                    unreliable: tuple.get(4).map_err(|e| e.to_string())?,
                }
            }
            "spawn_actor" => OutboundCommand::SpawnActor {
                object_id: tuple.get(1).map_err(|e| e.to_string())?,
                archetype: tuple.get(2).map_err(|e| e.to_string())?,
                position: [
                    tuple.get(3).map_err(|e| e.to_string())?,
                    tuple.get(4).map_err(|e| e.to_string())?,
                    tuple.get(5).map_err(|e| e.to_string())?,
                ],
            },
            "move_actor" => OutboundCommand::MoveActor {
                object_id: tuple.get(1).map_err(|e| e.to_string())?,
                position: [
                    tuple.get(2).map_err(|e| e.to_string())?,
                    tuple.get(3).map_err(|e| e.to_string())?,
                    tuple.get(4).map_err(|e| e.to_string())?,
                ],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [
                    tuple.get(5).map_err(|e| e.to_string())?,
                    tuple.get(6).map_err(|e| e.to_string())?,
                    tuple.get(7).map_err(|e| e.to_string())?,
                ],
            },
            "despawn_actor" => OutboundCommand::DespawnActor {
                object_id: tuple.get(1).map_err(|e| e.to_string())?,
            },
            "set_physics" => {
                let options_json: Option<String> = tuple.get(2).map_err(|e| e.to_string())?;
                let opts = options_json
                    .as_deref()
                    .map(physics_options_from_json)
                    .transpose()?;
                OutboundCommand::SetPhysics {
                    object_id: tuple.get(1).map_err(|e| e.to_string())?,
                    opts,
                }
            }
            "apply_impulse" => OutboundCommand::ApplyImpulse {
                object_id: tuple.get(1).map_err(|e| e.to_string())?,
                impulse: [
                    tuple.get(2).map_err(|e| e.to_string())?,
                    tuple.get(3).map_err(|e| e.to_string())?,
                    tuple.get(4).map_err(|e| e.to_string())?,
                ],
            },
            "set_move_intent" => OutboundCommand::SetMoveIntent {
                object_id: tuple.get(1).map_err(|e| e.to_string())?,
                intent: [
                    tuple.get(2).map_err(|e| e.to_string())?,
                    tuple.get(3).map_err(|e| e.to_string())?,
                    tuple.get(4).map_err(|e| e.to_string())?,
                ],
            },
            other => return Err(format!("unknown outbound command tag: {other}")),
        };
        out.push(command);
    }
    Ok(out)
}

fn physics_options_from_json(input: &str) -> JsHostResult<PhysicsOptions> {
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

fn parse_rpc_reply(reply: Value<'_>) -> JsHostResult<JsRpcInner> {
    if reply.is_null() || reply.is_undefined() {
        return Ok(JsRpcInner::NoHandler);
    }
    let tuple = Array::from_value(reply).map_err(|e| e.to_string())?;
    let ok: bool = tuple.get(0).map_err(|e| e.to_string())?;
    if ok {
        let body: Value = tuple.get(1).map_err(|e| e.to_string())?;
        Ok(JsRpcInner::Reply(bytes_from_js(body)?))
    } else {
        Ok(JsRpcInner::HandlerErr(
            tuple.get(2).map_err(|e| e.to_string())?,
        ))
    }
}

fn parse_room_spec(spec: Value<'_>) -> JsHostResult<Option<RoomSpec>> {
    if spec.is_null() || spec.is_undefined() {
        return Ok(None);
    }
    let tuple = Array::from_value(spec).map_err(|e| e.to_string())?;
    let max_players: u32 = tuple.get(2).map_err(|e| e.to_string())?;
    Ok(Some(RoomSpec {
        map: tuple.get(0).map_err(|e| e.to_string())?,
        mode: tuple.get(1).map_err(|e| e.to_string())?,
        max_players: max_players.min(u32::from(u16::MAX)) as u16,
        open: tuple.get(3).map_err(|e| e.to_string())?,
    }))
}

fn bytes_from_js(value: Value<'_>) -> JsHostResult<Vec<u8>> {
    if value.is_null() || value.is_undefined() {
        return Ok(Vec::new());
    }
    if let Ok(typed) = TypedArray::<u8>::from_value(value.clone()) {
        return typed
            .as_bytes()
            .map(|bytes| bytes.to_vec())
            .ok_or_else(|| "Uint8Array buffer is detached".to_string());
    }
    if let Some(buffer) = rquickjs::ArrayBuffer::from_value(value.clone()) {
        return buffer
            .as_bytes()
            .map(|bytes| bytes.to_vec())
            .ok_or_else(|| "ArrayBuffer is detached".to_string());
    }
    if value.is_string() {
        let ctx = value.ctx().clone();
        let text = <String as rquickjs::FromJs>::from_js(&ctx, value).map_err(|e| e.to_string())?;
        return Ok(text.into_bytes());
    }
    let array = Array::from_value(value).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(array.len());
    for item in array.iter::<u8>() {
        out.push(item.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

fn js_array_strings(array: Array<'_>) -> JsHostResult<Vec<String>> {
    let mut out = Vec::with_capacity(array.len());
    for item in array.iter::<String>() {
        out.push(item.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

fn js_array_u32(array: Array<'_>) -> JsHostResult<Vec<u32>> {
    let mut out = Vec::with_capacity(array.len());
    for item in array.iter::<u32>() {
        out.push(item.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// Throw a sanitized JavaScript `Error` from a native friends host function.
fn throw_js<'js, T>(ctx: &Ctx<'js>, message: String) -> rquickjs::Result<T> {
    let exception = rquickjs::Exception::from_message(ctx.clone(), &message)?;
    Err(ctx.throw(exception.into_value()))
}

/// Register the native `citadel.friends_*` bridges over the domain-services seam
///, overwriting the prelude stubs. A no-op when no host is attached,
/// so the throwing stubs remain. Re-run after each VM build and hot-reload.
///
/// Each function calls the SYNCHRONOUS [`DomainHost`] seam directly (its
/// async→sync bridge runs on the multi-threaded runtime); the script passes the
/// acting `user` explicitly (trusted tier).
fn apply_domain_host(
    context: &Context,
    domain: &Option<Arc<dyn DomainHost>>,
    interceptor_mode: Arc<AtomicBool>,
) {
    let Some(host) = domain else {
        return;
    };
    let friends_host = Arc::clone(host);
    let friends_mode = Arc::clone(&interceptor_mode);
    let _ = context.with(|ctx| -> JsHostResult<()> {
        // A single native bridge returning a JSON-encoded result (a `String`, so
        // no `'js` value is returned from the closure — sidestepping rquickjs's
        // invariant-lifetime friction on returned objects/arrays). The prelude
        // `citadel.friends_*` wrappers `JSON.parse` the reply.
        let friends = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>,
                      op: String,
                      user: String,
                      other: String|
                      -> rquickjs::Result<String> {
                    ensure_realtime_effects_allowed(&ctx, &friends_mode)?;
                    let result: Result<String, String> = match op.as_str() {
                        "add" => friends_host
                            .friends_add(&user, &other)
                            .map(|state| serde_json::Value::String(state).to_string()),
                        "remove" => friends_host
                            .friends_remove(&user, &other)
                            .map(|removed| removed.to_string()),
                        "block" => friends_host
                            .friends_block(&user, &other)
                            .map(|()| "null".to_string()),
                        "list" => friends_host.friends_list(&user).map(|rows| {
                            let rows: Vec<serde_json::Value> = rows
                                .into_iter()
                                .map(|row| {
                                    serde_json::json!({
                                        "user_id": row.user_id,
                                        "state": row.state,
                                        "updated_unix_ms": row.updated_unix_ms,
                                    })
                                })
                                .collect();
                            serde_json::Value::Array(rows).to_string()
                        }),
                        other => Err(format!("unknown friends op: {other}")),
                    };
                    match result {
                        Ok(json) => Ok(json),
                        Err(message) => throw_js(&ctx, message),
                    }
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_friends", friends))?;
        let notifications_host = Arc::clone(host);
        let notifications_mode = Arc::clone(&interceptor_mode);
        let notifications = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>, op: String, payload: String| -> rquickjs::Result<String> {
                    ensure_realtime_effects_allowed(&ctx, &notifications_mode)?;
                    let result: Result<String, String> = (|| {
                        let value: serde_json::Value = serde_json::from_str(&payload)
                            .map_err(|_| "invalid notification payload".to_string())?;
                        match op.as_str() {
                            "send" => {
                                let recipient = value.get("recipient").and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| "missing recipient".to_string())?;
                                let code = value.get("code").and_then(serde_json::Value::as_i64)
                                    .ok_or_else(|| "missing code".to_string())? as i32;
                                let subject = value.get("subject").and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| "missing subject".to_string())?;
                                let content_json = value.get("content_json").and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| "missing content_json".to_string())?;
                                let sender = value.get("sender").and_then(serde_json::Value::as_str);
                                let delivery_key = value.get("delivery_key").and_then(serde_json::Value::as_str);
                                notifications_host.notifications_send(recipient, code, subject, content_json, sender, delivery_key)
                                    .and_then(|notification| serde_json::to_string(&notification).map_err(|e| e.to_string()))
                            }
                            "list" => {
                                let recipient = value.get("recipient").and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| "missing recipient".to_string())?;
                                let limit = value.get("limit").and_then(serde_json::Value::as_u64).unwrap_or(50) as usize;
                                let cursor = value.get("cursor").and_then(serde_json::Value::as_str);
                                notifications_host.notifications_list(recipient, limit, cursor)
                                    .map(|page| serde_json::json!({"items": page.items, "next_cursor": page.next_cursor}).to_string())
                            }
                            "mark_read" => {
                                let recipient = value.get("recipient").and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| "missing recipient".to_string())?;
                                let ids = value.get("ids").and_then(serde_json::Value::as_array)
                                    .ok_or_else(|| "missing ids".to_string())?
                                    .iter().map(serde_json::Value::as_str).collect::<Option<Vec<_>>>()
                                    .ok_or_else(|| "ids must contain strings".to_string())?
                                    .into_iter().map(ToOwned::to_owned).collect::<Vec<_>>();
                                notifications_host.notifications_mark_read(recipient, &ids)
                                    .map(|read_ids| serde_json::json!({"read_ids": read_ids}).to_string())
                            }
                            _ => Err(format!("unknown notifications op: {op}")),
                        }
                    })();
                    match result { Ok(json) => Ok(json), Err(message) => throw_js(&ctx, message) }
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_notifications", notifications))?;
        let groups_host = Arc::clone(host);
        let groups_mode = Arc::clone(&interceptor_mode);
        let groups = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>, actor: String, operation: String, payload: String| -> rquickjs::Result<String> {
                    ensure_realtime_effects_allowed(&ctx, &groups_mode)?;
                    match groups_host.groups_call(&actor, &operation, &payload) {
                        Ok(json) => Ok(json),
                        Err(message) => throw_js(&ctx, message),
                    }
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_groups", groups))?;
        let domain_host = Arc::clone(host);
        let domain_mode = Arc::clone(&interceptor_mode);
        let domain = caught(&ctx, Function::new(ctx.clone(), move |ctx: Ctx<'_>, domain: String, actor: String, operation: String, payload: String| -> rquickjs::Result<String> {
            ensure_realtime_effects_allowed(&ctx, &domain_mode)?;
            let result = match domain.as_str() {
                "leaderboards" => domain_host.leaderboards_call(&actor, &operation, &payload),
                "chat" => domain_host.chat_call(&actor, &operation, &payload),
                "wallet" => domain_host.wallet_call(&actor, &operation, &payload),
                _ => Err("unknown domain".to_string()),
            };
            match result { Ok(json) => Ok(json), Err(message) => throw_js(&ctx, message) }
        }))?;
        caught(&ctx, ctx.globals().set("__citadel_domain", domain))?;
        let storage_host = Arc::clone(host);
        let storage_mode = interceptor_mode;
        let storage = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>, request: String| -> rquickjs::Result<String> {
                    ensure_realtime_effects_allowed(&ctx, &storage_mode)?;
                    #[derive(serde::Deserialize)]
                    struct StorageRequest {
                        op: String,
                        user: String,
                        collection: String,
                        key: String,
                        value_json: Option<String>,
                        expected_version: Option<String>,
                        read_permission: Option<u8>,
                        write_permission: Option<u8>,
                        limit: Option<usize>,
                        included_index_names: Option<Vec<String>>,
                    }
                    let request: StorageRequest = match serde_json::from_str(&request) {
                        Ok(request) => request,
                        Err(_) => return throw_js(&ctx, "invalid storage request".to_string()),
                    };
                    let included_index_names_json = match request
                        .included_index_names
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                    {
                        Ok(value) => value,
                        Err(_) => return throw_js(&ctx, "invalid storage request".to_string()),
                    };
                    let result = match request.op.as_str() {
                        "read" => storage_host
                            .storage_read(&request.user, &request.collection, &request.key)
                            .map(|object| {
                                object
                                    .map(storage_object_json)
                                    .unwrap_or(serde_json::Value::Null)
                                    .to_string()
                            }),
                        "write" => storage_host
                            .storage_write(
                                StorageWriteInput::new(
                                    &request.user,
                                    &request.collection,
                                    &request.key,
                                    request.value_json.as_deref().unwrap_or(""),
                                )
                                .expecting(request.expected_version.as_deref())
                                .with_permissions(
                                    request.read_permission,
                                    request.write_permission,
                                )
                                .with_included_index_names_json(
                                    included_index_names_json.as_deref(),
                                ),
                            )
                            .map(|object| storage_object_json(object).to_string()),
                        "delete" => storage_host
                            .storage_delete(
                                &request.user,
                                &request.collection,
                                &request.key,
                                request.expected_version.as_deref(),
                            )
                            .map(|()| "null".to_string()),
                        "index_query" => storage_host
                            .storage_index_query(
                                &request.user,
                                request.value_json.as_deref().unwrap_or(""),
                                request.limit.unwrap_or(50),
                            )
                            .map(|objects| {
                                serde_json::Value::Array(
                                    objects
                                        .into_iter()
                                        .map(storage_index_object_json)
                                        .collect(),
                                )
                                .to_string()
                            }),
                        "index_candidates" => storage_host
                            .storage_index_candidates(
                                &request.user,
                                &request.collection,
                                &request.key,
                            )
                            .map(|names| serde_json::json!(names).to_string()),
                        other => Err(format!("unknown storage op: {other}")),
                    };
                    match result {
                        Ok(json) => Ok(json),
                        Err(message) => throw_js(&ctx, message),
                    }
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_storage", storage))?;
        Ok(())
    });
}

fn storage_object_json(object: crate::runtime::StorageObjectDto) -> serde_json::Value {
    serde_json::json!({
        "value_json": object.value_json,
        "version": object.version,
        "read_permission": object.read_permission,
        "write_permission": object.write_permission,
    })
}

fn storage_index_object_json(object: crate::runtime::StorageIndexObjectDto) -> serde_json::Value {
    serde_json::json!({
        "user_id": object.user_id,
        "collection": object.collection,
        "key": object.key,
        "value_json": object.object.value_json,
        "version": object.object.version,
        "read_permission": object.object.read_permission,
        "write_permission": object.object.write_permission,
    })
}

/// Install a native read-only map query. Returning a JSON string keeps rquickjs
/// values within the context closure; the prelude turns it into an object/null.
fn apply_map_catalog(context: &Context, maps: &Option<Arc<MapCatalog>>) {
    let Some(maps) = maps else {
        return;
    };
    let maps = Arc::clone(maps);
    let _ = context.with(|ctx| -> JsHostResult<()> {
        let map_info = caught(
            &ctx,
            Function::new(ctx.clone(), move |name: String| -> String {
                maps.info(&name)
                    .map(|info| {
                        serde_json::json!({
                            "bounds_min": info.bounds_min,
                            "bounds_max": info.bounds_max,
                            "vertex_count": info.vertex_count,
                            "triangle_count": info.triangle_count,
                        })
                        .to_string()
                    })
                    .unwrap_or_else(|| "null".to_owned())
            }),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_map_info", map_info))?;
        Ok(())
    });
}

/// Install a native synchronous physics-state query. JSON keeps QuickJS values
/// within the context closure; the prelude turns it into an object/null.
fn apply_transform_hub(context: &Context, hub: &Option<Arc<TransformHub>>) {
    let Some(hub) = hub else {
        return;
    };
    let hub = TransformHubHandle(Arc::clone(hub));
    let _ = context.with(|ctx| -> JsHostResult<()> {
        let physics_hub = hub.clone();
        let physics_state = caught(
            &ctx,
            Function::new(ctx.clone(), move |object_id: u32| -> String {
                physics_hub
                    .0
                    .physics_state(object_id)
                    .map(|state| {
                        serde_json::json!({
                            "grounded": state.grounded,
                            "position": state.position,
                            "velocity": state.velocity,
                        })
                        .to_string()
                    })
                    .unwrap_or_else(|| "null".to_owned())
            }),
        )?;
        caught(
            &ctx,
            ctx.globals().set("__citadel_physics_state", physics_state),
        )?;
        let raycast_hub = hub.clone();
        let raycast = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |origin: Vec<f32>, direction: Vec<f32>| -> String {
                    let Some(origin) = vector3(&origin) else {
                        return "null".to_owned();
                    };
                    let Some(direction) = vector3(&direction) else {
                        return "null".to_owned();
                    };
                    raycast_hub
                        .0
                        .raycast(origin, direction)
                        .map(|hit| {
                            serde_json::json!({
                                "point": hit.point,
                                "normal": hit.normal,
                                "distance": hit.distance,
                                "triangle_index": hit.triangle_index,
                            })
                            .to_string()
                        })
                        .unwrap_or_else(|| "null".to_owned())
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_raycast", raycast))?;
        let overlap_hub = hub.clone();
        let overlap = caught(
            &ctx,
            Function::new(ctx.clone(), move |centre: Vec<f32>, radius: f32| -> bool {
                vector3(&centre).is_some_and(|centre| {
                    radius.is_finite()
                        && radius >= 0.0
                        && overlap_hub.0.sphere_overlap(centre, radius)
                })
            }),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_sphere_overlap", overlap))?;
        let ground_hub = hub;
        let ground = caught(
            &ctx,
            Function::new(
                ctx.clone(),
                move |origin: Vec<f32>, max_distance: f32| -> String {
                    let Some(origin) = vector3(&origin) else {
                        return "null".to_owned();
                    };
                    if !max_distance.is_finite() || max_distance < 0.0 {
                        return "null".to_owned();
                    }
                    ground_hub
                        .0
                        .ground_height(origin, max_distance)
                        .map(|hit| {
                            serde_json::json!({
                                "point": hit.point,
                                "normal": hit.normal,
                                "distance": hit.distance,
                                "triangle_index": hit.triangle_index,
                            })
                            .to_string()
                        })
                        .unwrap_or_else(|| "null".to_owned())
                },
            ),
        )?;
        caught(&ctx, ctx.globals().set("__citadel_ground_height", ground))?;
        Ok(())
    });
}

fn vector3(value: &[f32]) -> Option<[f32; 3]> {
    (value.len() == 3 && value.iter().all(|coordinate| coordinate.is_finite()))
        .then_some([value[0], value[1], value[2]])
}

fn vm_has_any_handler(vm: &JsVm) -> bool {
    vm.context
        .with(|ctx| -> JsHostResult<bool> {
            let globals = ctx.globals();
            let func: Function = caught(&ctx, globals.get("__citadel_has_any_handler"))?;
            caught(&ctx, func.call(()))
        })
        .unwrap_or(false)
}

fn read_script(path: &Path) -> AppResult<String> {
    std::fs::read_to_string(path).map_err(|e| {
        AppError::new(
            ErrorCategory::Runtime,
            format!("cannot read JavaScript game script: {}", path.display()),
        )
        .with_detail(e.to_string())
    })
}

fn script_error(context: &str, err: rquickjs::Error) -> AppError {
    AppError::new(ErrorCategory::Runtime, context.to_string()).with_detail(err.to_string())
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::collections::HashSet;
    use std::time::Instant;

    use super::*;
    use crate::runtime::HOST_API_SURFACE;
    use crate::runtime::host_api_spec::HostApiStatus;

    fn runtime(src: &str) -> JsRuntime {
        JsRuntime::from_source(src, "test.js", 100).expect("javascript runtime loads")
    }

    #[test]
    fn realtime_interceptors_veto_and_observe_without_command_side_effects() {
        let rt = runtime(
            r#"
            let seen = "unset";
            citadel.before_realtime((ctx, body) => {
              citadel.broadcast(99, "must-discard");
              return false;
            });
            citadel.after_realtime((ctx, body) => {
              citadel.broadcast(98, "must-discard");
              seen = `${ctx.dropped ? "drop" : "pass"}:${ctx.delivered}:${Array.from(ctx.body).join(",")}`;
            });
            citadel.on_message(8, () => citadel.broadcast(9, seen));
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
                body: b"drop:0:4,5".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn invalid_before_realtime_result_fails_closed() {
        let rt = runtime("citadel.before_realtime(() => 'invalid');");
        assert_eq!(
            rt.before_realtime(7, None, None, 1, b"input"),
            RealtimeInterception::Drop
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn realtime_interceptors_reject_domain_storage_side_effects() {
        let rt = runtime(
            r#"
            let seen = "unset";
            citadel.before_realtime(() => {
              globalThis.__citadel_realtime_interceptor = false;
              citadel.register_storage_index_filter("profiles_by_score", () => {
                seen = "filter-mutated";
                return true;
              });
              citadel.storage_write("user", "profiles", "before", "{}");
              return true;
            });
            citadel.after_realtime(() => {
              globalThis.__citadel_realtime_interceptor = false;
              citadel.register_storage_index_filter("profiles_by_score", () => {
                seen = "filter-mutated";
                return true;
              });
              citadel.storage_write("user", "profiles", "after", "{}");
              seen = "mutated";
            });
            citadel.on_message(8, () => {
              citadel.storage_write("user", "profiles", "normal", '{"score":1}');
              citadel.broadcast(9, seen);
            });
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

    /// A throwaway directory for file-backed ESM and reload tests. Avoids a
    /// `tempfile` dependency while retaining isolated script roots.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};

            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir()
                .join(format!("citadel-js-esm-{}-{tag}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn write_main(&self, source: &str) {
            self.write(JS_ENTRYPOINT, source);
        }

        fn write(&self, relative: &str, source: &str) {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create module directory");
            }
            std::fs::write(path, source).expect("write script");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn file_runtime(dir: &TempDir) -> JsRuntime {
        JsRuntime::load(&dir.0, 100)
            .expect("load file runtime")
            .expect("main.js present")
    }

    fn first_broadcast_body(commands: Vec<OutboundCommand>) -> Vec<u8> {
        let OutboundCommand::Broadcast { body, .. } =
            commands.into_iter().next().expect("expected one broadcast")
        else {
            panic!("expected broadcast command");
        };
        body
    }

    #[test]
    fn host_api_surface_matches_manifest_js() {
        let shipped: HashSet<&'static str> = HOST_API_SURFACE
            .iter()
            .filter(|entry| entry.status == HostApiStatus::Shipped)
            .map(|entry| entry.name)
            .collect();
        assert_eq!(JsRuntime::registered_host_api_names(), shipped);
    }

    fn friends_host() -> Arc<dyn DomainHost> {
        use crate::repository::{
            InMemoryBackend, InMemoryChatRepository, InMemoryFriendsRepository,
            InMemoryGroupsRepository, InMemoryLeaderboardsRepository, InMemoryStorageRepository,
            InMemoryWalletRepository,
        };
        use crate::runtime::ServiceDomainHost;
        use crate::services::{
            ChatChannelAuthorizer, ChatService, FriendsService, GroupsService, LeaderboardService,
            PlayerNotificationService, WalletService,
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
        // and assert real effects — a name-claim stub throws here and fails.
        let rt = runtime(
            r#"
citadel.on_rpc("exercise", (ctx, body) => {
  const u = "prober", o = "target";
  const added = citadel.friends_add(u, o);
  citadel.friends_add(o, u);
  const chat = citadel.chat_call(u, "send", {target: {kind: "direct", other_user_id: o}, content: "hi"});
  const n1 = citadel.friends_list(u).length;
  citadel.friends_block(u, o);
  const blocked = citadel.friends_list(u)[0].state;
  const removed = citadel.friends_remove(u, o);
  const n2 = citadel.friends_list(u).length;
  const notification = citadel.notifications_send(u, 7, "hello", "{}", "server", "probe");
  const page = citadel.notifications_list(u);
  const read = citadel.notifications_mark_read(u, [notification.id]);
  const group = citadel.groups_call(u, "create", {name: "probers"});
  const boards = citadel.leaderboards_call(u, "list", {});
  const wallet = citadel.wallet_call(u, "balances", {});
  return added + "|" + n1 + "|" + blocked + "|" + removed + "|" + n2 + "|" + page.items.length + "|" + read.read_ids.length + "|" + group.name + "|" + boards.length + "|" + chat.id + "|" + Object.keys(wallet).length;
});
"#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("prober"), "exercise", b"") else {
            panic!("domain host functions must be wired, not stubbed");
        };
        assert_eq!(reply, b"invited_sent|1|blocked|true|0|1|1|probers|0|1|0");

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
        let rt = runtime(
            r#"
citadel.on_rpc("befriend", (ctx, other) => {
  citadel.friends_add(ctx.user_id, "bob");
  return "unreachable";
});
"#,
        );
        let RpcOutcome::Err(msg) = rt.call_rpc(1, Some("alice"), "befriend", b"") else {
            panic!("expected error");
        };
        assert!(!msg.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_index_query_is_wired_to_the_javascript_host() {
        let rt = runtime(
            r#"
citadel.on_rpc("search", (ctx, body) => {
  citadel.register_storage_index_filter(
    "profiles_by_score", (candidate) => {
      if (candidate.key === "boom") throw new Error("filter failed");
      return candidate.key === "main";
    });
  citadel.storage_write(ctx.user_id, "profiles", "skip", '{"score":7}');
  citadel.storage_write(ctx.user_id, "profiles", "main", '{"score":7}');
  let errored = false;
  try { citadel.storage_write(ctx.user_id, "profiles", "boom", '{"score":7}'); }
  catch (_) { errored = true; }
  const missing = citadel.storage_read(ctx.user_id, "profiles", "boom") === null;
  const found = citadel.storage_index_query("profiles_by_score", '{"score":7}', 10);
  return `${errored}|${missing}|${found.length}|${found[0].user_id}|${found[0].key}`;
});
"#,
        )
        .with_domain_host(friends_host());

        let RpcOutcome::Ok(reply) = rt.call_rpc(1, Some("alice"), "search", b"") else {
            panic!("storage index host must return a reply");
        };
        assert_eq!(reply, b"true|true|1|alice|main");
    }

    #[test]
    fn message_handler_broadcasts() {
        let rt = runtime(
            r#"
function u64be(n) {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, n, false);
  return out;
}
function concat(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}
citadel.on_message(1, (ctx, body) => {
  citadel.broadcast(2, concat(u64be(ctx.sender), body), true);
});
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
    fn imperative_registration_and_lifecycle_work() {
        let rt = runtime(
            r#"
function joined(ctx) {
  citadel.send(ctx.sender, 7, "joined");
}
citadel.on_join(joined);
"#,
        );
        assert_eq!(
            rt.dispatch_lifecycle(LifecycleHook::Join, 5, Some("user-5")),
            vec![OutboundCommand::Send {
                session: 5,
                kind: 7,
                body: b"joined".to_vec(),
                unreliable: false,
            }]
        );
    }

    #[test]
    fn rpc_replies_and_errors() {
        let rt = runtime(
            r#"
citadel.on_rpc("ping", () => citadel.Reply.ok("pong"));
citadel.on_rpc("nope", () => citadel.Reply.err("denied"));
"#,
        );
        assert_eq!(
            rt.call_rpc(1, None, "ping", b""),
            RpcOutcome::Ok(b"pong".to_vec())
        );
        assert_eq!(
            rt.call_rpc(1, None, "nope", b""),
            RpcOutcome::Err("denied".to_string())
        );
        assert_eq!(
            rt.call_rpc(1, None, "missing", b""),
            RpcOutcome::Err("unknown RPC method: missing".to_string())
        );
    }

    #[test]
    fn room_hooks_parse_spec_and_admission() {
        let rt = runtime(
            r#"
citadel.on_room_create(() => ({ map: "Arena", mode: "duel", max_players: 2, open: false }));
citadel.on_room_join((_ctx, roomId) => roomId === 7n);
"#,
        );
        assert_eq!(
            rt.call_room_create(1, None, b"{}"),
            Some(RoomSpec {
                map: "Arena".to_string(),
                mode: "duel".to_string(),
                max_players: 2,
                open: false,
            })
        );
        assert!(rt.call_room_join(1, None, 7));
        assert!(!rt.call_room_join(1, None, 8));
    }

    #[test]
    fn tick_and_actor_commands_work() {
        let rt = runtime(
            r#"
let actor = null;
citadel.on_join(() => {
  actor = citadel.spawn_actor({ archetype: 9, x: 1, y: 2, z: 3 });
});
citadel.on_tick((dt) => {
  if (actor !== null) {
    citadel.move_actor(actor, dt, 2, 3, 4, 5, 6);
  }
});
citadel.on_leave(() => {
  citadel.despawn_actor(actor);
});
"#,
        );
        let spawned = rt.dispatch_lifecycle(LifecycleHook::Join, 1, None);
        assert_eq!(
            spawned,
            vec![OutboundCommand::SpawnActor {
                object_id: 0x4000_0000,
                archetype: 9,
                position: [1.0, 2.0, 3.0],
            }]
        );
        assert!(rt.has_tick_handler());
        assert_eq!(
            rt.tick(Duration::from_millis(250), Duration::from_millis(100)),
            vec![OutboundCommand::MoveActor {
                object_id: 0x4000_0000,
                position: [0.25, 2.0, 3.0],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [4.0, 5.0, 6.0],
            }]
        );
        assert_eq!(
            rt.dispatch_lifecycle(LifecycleHook::Leave, 1, None),
            vec![OutboundCommand::DespawnActor {
                object_id: 0x4000_0000,
            }]
        );
    }

    #[test]
    fn deadline_interrupts_hung_handler() {
        let rt = JsRuntime::from_source(
            r#"
citadel.on_message(1, () => {
  while (true) {}
});
"#,
            "deadline.js",
            10,
        )
        .expect("runtime loads");
        let started = Instant::now();
        assert!(rt.dispatch(1, None, 1, b"").is_empty());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    // --------------------------- scoped ESM modules ------------------------ //

    #[test]
    fn esm_imports_resolve_nested_local_modules() {
        let dir = TempDir::new("nested");
        dir.write(
            "rules/bonus.js",
            "export function bonus(value) { return value + 2; }",
        );
        dir.write(
            "systems/combat.js",
            r#"
import { bonus } from "../rules/bonus.js";
export function damage(base) { return bonus(base) * 2; }
"#,
        );
        dir.write_main(
            r#"
import { damage } from "./systems/combat.js";
citadel.on_message(1, () => citadel.broadcast(2, String(damage(20)), false));
"#,
        );

        let runtime = file_runtime(&dir);
        assert_eq!(
            first_broadcast_body(runtime.dispatch(1, None, 1, b"")),
            b"44"
        );
    }

    #[test]
    fn esm_modules_are_cached_and_cycles_follow_native_esm_semantics() {
        let dir = TempDir::new("cache-cycle");
        dir.write(
            "counter.js",
            r#"
globalThis.__citadel_module_loads = (globalThis.__citadel_module_loads || 0) + 1;
export const loadCount = globalThis.__citadel_module_loads;
"#,
        );
        dir.write(
            "a.js",
            r#"
import { b } from "./b.js";
export function a() { return "a" + b(); }
"#,
        );
        dir.write(
            "b.js",
            r#"
import { a } from "./a.js";
export function b() { return "b" + (typeof a === "function" ? "" : "?"); }
"#,
        );
        dir.write_main(
            r#"
import { loadCount as first } from "./counter.js";
import { loadCount as second } from "./counter.js";
import { a } from "./a.js";
citadel.on_message(1, () => citadel.broadcast(2, `${first}:${second}:${a()}`, false));
"#,
        );

        let runtime = file_runtime(&dir);
        assert_eq!(
            first_broadcast_body(runtime.dispatch(1, None, 1, b"")),
            b"1:1:ab"
        );
    }

    #[test]
    fn esm_rejects_paths_and_system_modules_outside_the_game_root() {
        let dir = TempDir::new("escape");
        let game = dir.0.join("game");
        std::fs::create_dir_all(&game).expect("create game root");
        let secret = dir.0.join("secret.js");
        std::fs::write(&secret, "export const secret = 'leaked';").expect("write secret");

        for import in [
            "../secret.js",
            "/secret.js",
            "std",
            "node:fs",
            ".\\\\secret.js",
        ] {
            let source = format!("import value from \"{import}\";");
            let error = JsRuntime::from_source_with_root(&source, "escape.js", 100, &game)
                .expect_err("out-of-root and non-relative modules must fail");
            assert_eq!(error.category(), ErrorCategory::Runtime, "import: {import}");
            assert!(
                !error
                    .log_detail()
                    .unwrap_or_default()
                    .contains(&secret.display().to_string()),
                "rejecting {import} must not leak its host path"
            );
        }
    }

    #[test]
    fn esm_dependency_edits_are_watched_and_reloaded_atomically() {
        let dir = TempDir::new("reload");
        dir.write("systems/version.js", "export const version = 'v1';");
        dir.write_main(
            r#"
import { version } from "./systems/version.js";
citadel.on_message(1, () => citadel.broadcast(2, version, false));
"#,
        );

        let runtime = file_runtime(&dir);
        assert_eq!(
            first_broadcast_body(runtime.dispatch(1, None, 1, b"")),
            b"v1"
        );
        let dependency = dir
            .0
            .join("systems/version.js")
            .canonicalize()
            .expect("canonical dependency");
        assert!(runtime.reload_watch_paths().contains(&dependency));

        dir.write("systems/version.js", "export const version = 'v2';");
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
        assert_eq!(
            first_broadcast_body(runtime.dispatch(1, None, 1, b"")),
            b"v2"
        );

        dir.write("systems/version.js", "export const = ;");
        assert_eq!(runtime.reload(), ReloadOutcome::Rejected);
        assert_eq!(
            first_broadcast_body(runtime.dispatch(1, None, 1, b"")),
            b"v2",
            "the valid VM survives a broken imported module"
        );
    }
}
