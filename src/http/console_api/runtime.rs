//! Console API Explorer / Runtime section.
//!
//! - `GET /console/v1/runtime` — runtime facts: whether the embedded runtime
//!   is enabled/attached, the tick rate, and what the loaded script registered
//!   ([`RuntimeIntrospection`]: RPC names, handled message kinds, hooks).
//! - `POST /console/v1/runtime/rpc/{method}` — invoke a registered RPC with an
//!   operator-supplied payload (admin, audited). The call runs through
//!   [`LuaRuntime::call_rpc`](crate::runtime::LuaRuntime::call_rpc) — the same
//!   isolated, deadline-bounded path game traffic uses, with no participant
//!   sender bound (`ctx.sender = 0`, `ctx.user_id = nil`).
//!
//! RPC failures (unknown method, handler error, timeout) come back as
//! `ok: false` with the same short generic message a game client would see —
//! the console never widens the runtime's error surface.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::runtime::{RpcOutcome, RuntimeIntrospection};
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

/// The API Explorer / Runtime section route.
pub const RUNTIME_PATH: &str = "/console/v1/runtime";

/// RPC invocation route pattern.
pub const RUNTIME_RPC_PATH: &str = "/console/v1/runtime/rpc/:method";

/// The JSON response for [`RUNTIME_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeResponse {
    /// Whether `[runtime]` is enabled in configuration.
    pub enabled: bool,
    /// Explicit `[runtime] language`, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configured_language: Option<&'static str>,
    /// Selected language after explicit config or autodetection, if an entrypoint
    /// exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_language: Option<&'static str>,
    /// `explicit` or `autodetected`, when selected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_source: Option<&'static str>,
    /// Selected runtime entrypoint path, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    /// Configured runtime adapter.
    pub adapter: &'static str,
    /// Configured runtime trust tier.
    pub tier: &'static str,
    /// Whether a script runtime is attached to the realtime gateway (a script
    /// was actually loaded; `false` also before transports start).
    pub attached: bool,
    /// Configured `citadel.on_tick` rate (0 = no game loop).
    pub tick_hz: u32,
    /// Whether `runtime.require_script` gates match surfaces on this node.
    pub require_script: bool,
    /// GameScript readiness (state/revision/generation/recovery), present
    /// only on `require_script` nodes once the transports have started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessView>,
    /// What the loaded script registered, when attached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<RuntimeIntrospection>,
}

/// Operator-facing GameScript readiness, mirrored from the gate authority.
///
/// This is the operator surface: unlike client-facing rejections it names the
/// loaded revision and generation so a stuck gate is explainable.
#[derive(Debug, Clone, Serialize)]
pub struct ReadinessView {
    /// Stable state token (`no_script`/`validating`/`ready`/`activating`/
    /// `degraded`/`unavailable`).
    pub state: &'static str,
    /// Content identity of the most recently loaded script, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    /// Local monotonic load generation (0 before the first load).
    pub generation: u64,
    /// When the current state was entered (Unix millis).
    pub since_unix_millis: u64,
    /// Supervised-worker recovery posture, when the worker adapter reported
    /// one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<ReadinessRecoveryView>,
}

/// Worker restart posture for the readiness surface.
#[derive(Debug, Clone, Serialize)]
pub struct ReadinessRecoveryView {
    /// Whether the restart circuit breaker is open (budget exhausted).
    pub circuit_open: bool,
    /// Consecutive restart failures observed by the supervisor.
    pub consecutive_failures: u32,
    /// The supervisor's restart budget.
    pub restart_limit: u32,
}

/// The JSON body accepted by the RPC caller. `payload` is passed to the
/// handler as the raw request body string (may be empty).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcRequest {
    /// Raw payload handed to the Lua handler (UTF-8 string, may be empty).
    #[serde(default)]
    pub payload: String,
}

/// The JSON response of an RPC invocation.
#[derive(Debug, Clone, Serialize)]
pub struct RpcResponse {
    /// Whether the handler ran and returned a reply.
    pub ok: bool,
    /// The reply bytes rendered as UTF-8 (lossy), when `ok`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply: Option<String>,
    /// The short, generic error message, when not `ok` (identical to what a
    /// game client would receive).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `GET /console/v1/runtime`: runtime facts + script introspection.
pub(super) async fn get_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
) -> Json<RuntimeResponse> {
    app.metrics().record_http_request();
    let runtime_config = &app.config().runtime;
    let selection = runtime_config.resolve_selection();
    let (selected_language, selection_source, entrypoint) = match selection {
        Ok(Some(selection)) => (
            Some(selection.language.as_str()),
            Some(selection.source.as_str()),
            Some(selection.entrypoint.display().to_string()),
        ),
        Ok(None) | Err(_) => (None, None, None),
    };
    let gateway = app.realtime_gateway();
    let script = gateway
        .as_ref()
        .and_then(|gateway| gateway.runtime().map(|runtime| runtime.introspect()));
    let readiness = gateway
        .as_ref()
        .and_then(|gateway| gateway.script_readiness())
        .map(|authority| {
            let snapshot = authority.snapshot();
            ReadinessView {
                state: snapshot.state.code(),
                revision_id: snapshot.revision_id,
                generation: snapshot.generation,
                since_unix_millis: snapshot.since.unix_millis(),
                recovery: authority.recovery().map(|recovery| ReadinessRecoveryView {
                    circuit_open: recovery.circuit_open,
                    consecutive_failures: recovery.consecutive_failures,
                    restart_limit: recovery.restart_limit,
                }),
            }
        });
    Json(RuntimeResponse {
        enabled: runtime_config.enabled,
        configured_language: runtime_config.language.map(|language| language.as_str()),
        selected_language,
        selection_source,
        entrypoint,
        adapter: runtime_config.adapter.as_str(),
        tier: runtime_config.tier.as_str(),
        attached: script.is_some(),
        tick_hz: runtime_config.tick_hz,
        require_script: runtime_config.require_script,
        readiness,
        script,
    })
}

/// `POST /console/v1/runtime/rpc/{method}`: invoke a registered RPC (admin).
pub(super) async fn rpc_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(method): Path<String>,
    body: Result<Json<RpcRequest>, JsonRejection>,
) -> Result<Json<RpcResponse>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let runtime = app
        .realtime_gateway()
        .and_then(|gateway| gateway.runtime().cloned())
        .ok_or_else(|| AppError::not_found("no script runtime is attached"))?;
    // Same synchronous, deadline-bounded call the gateway makes for game
    // traffic; the handler budget keeps the block short.
    let outcome = runtime.call_rpc(0, None, &method, request.payload.as_bytes());
    let (ok, reply, error) = match outcome {
        RpcOutcome::Ok(bytes) => (
            true,
            Some(String::from_utf8_lossy(&bytes).into_owned()),
            None,
        ),
        RpcOutcome::Err(message) => (false, None, Some(message)),
    };
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "runtime.rpc",
        format!("rpc {method}"),
        if ok {
            "invoked ok".to_string()
        } else {
            format!("failed: {}", error.as_deref().unwrap_or("error"))
        },
    ));
    Ok(Json(RpcResponse { ok, reply, error }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::LuaRuntime;

    #[test]
    fn runtime_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&RUNTIME_PATH));
        assert!(RUNTIME_RPC_PATH.starts_with(RUNTIME_PATH));
    }

    #[test]
    fn rpc_request_defaults_to_empty_payload_and_rejects_unknown_fields() {
        let empty: RpcRequest = serde_json::from_str("{}").expect("parse");
        assert_eq!(empty.payload, "");
        assert!(serde_json::from_str::<RpcRequest>(r#"{"payload":"x","extra":1}"#).is_err());
    }

    #[test]
    fn introspection_lists_registered_surface() {
        let runtime = LuaRuntime::from_source(
            r#"
            citadel.on_rpc("ping", function(ctx, body) return "pong" end)
            citadel.on_rpc("add", function(ctx, body) return body end)
            citadel.on_message(1, function(ctx, body) end)
            citadel.on_join(function(ctx) end)
            citadel.on_tick(function(dt) end)
            "#,
            "introspection-test",
            100,
        )
        .expect("build runtime");
        let info = runtime.introspect();
        assert_eq!(info.rpcs, vec!["add".to_string(), "ping".to_string()]);
        assert_eq!(info.message_kinds, vec![1]);
        assert_eq!(
            info.hooks,
            vec!["on_join".to_string(), "on_tick".to_string()]
        );
        assert_eq!(info.source, "introspection-test");
        assert!(!info.reloadable);
        assert_eq!(info.deadline_ms, 100);
    }
}
