//! Runtime-defined HTTP endpoint contracts.
//!
//! The HTTP transport owns routing, authentication, request limits, and
//! response serialization. Game runtimes own only explicit endpoint
//! declarations and synchronous handler invocation represented here.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Reserved, non-Citadel path prefix for script-defined HTTP endpoints.
pub const RUNTIME_HTTP_ENDPOINT_PREFIX: &str = "/ext";

/// Node-startup policy for script-defined HTTP endpoints. This is copied from
/// validated runtime configuration, never supplied by a script, and retained
/// across source-only hot reloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeHttpEndpointPolicy {
    pub enabled: bool,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_requests_per_minute: u32,
}

impl Default for RuntimeHttpEndpointPolicy {
    fn default() -> Self {
        Self::from(&crate::config::CustomHttpEndpointsCapabilityConfig::default())
    }
}

impl From<&crate::config::CustomHttpEndpointsCapabilityConfig> for RuntimeHttpEndpointPolicy {
    fn from(config: &crate::config::CustomHttpEndpointsCapabilityConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_request_bytes: config.max_request_bytes,
            max_response_bytes: config.max_response_bytes,
            max_requests_per_minute: config.max_requests_per_minute,
        }
    }
}

/// HTTP methods that a runtime endpoint may explicitly register.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeHttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl RuntimeHttpMethod {
    /// Parse an ASCII HTTP method accepted by the runtime endpoint boundary.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "GET" => Some(Self::Get),
            "POST" => Some(Self::Post),
            "PUT" => Some(Self::Put),
            "PATCH" => Some(Self::Patch),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }

    /// Stable uppercase spelling used in runtime language bridges.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

/// Authentication requirement declared at endpoint registration time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeHttpAuth {
    /// The route is public; no session information is supplied to the script.
    #[default]
    Public,
    /// A valid Citadel player access bearer is required before script dispatch.
    Session,
}

impl RuntimeHttpAuth {
    /// Parse the explicit runtime-facing authentication value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "public" => Some(Self::Public),
            "session" => Some(Self::Session),
            _ => None,
        }
    }

    /// Stable lowercase spelling used in runtime language bridges.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Session => "session",
        }
    }
}

/// A validated, runtime-owned endpoint declaration.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeHttpEndpoint {
    pub method: RuntimeHttpMethod,
    /// Canonical path relative to [`RUNTIME_HTTP_ENDPOINT_PREFIX`].
    pub path: String,
    pub auth: RuntimeHttpAuth,
}

impl RuntimeHttpEndpoint {
    /// Validate the small, unambiguous relative-path grammar.
    pub fn new(
        method: RuntimeHttpMethod,
        path: impl Into<String>,
        auth: RuntimeHttpAuth,
    ) -> Result<Self, RuntimeHttpEndpointError> {
        let path = path.into();
        if !is_valid_runtime_http_path(&path) {
            return Err(RuntimeHttpEndpointError::InvalidPath);
        }
        Ok(Self { method, path, auth })
    }

    /// Full externally-routed path under the reserved runtime prefix.
    #[must_use]
    pub fn full_path(&self) -> String {
        format!("{RUNTIME_HTTP_ENDPOINT_PREFIX}{}", self.path)
    }
}

/// Request data made available to a registered runtime endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHttpRequest {
    pub method: RuntimeHttpMethod,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    /// Authenticated Citadel user id for session-authenticated endpoints only.
    pub user_id: Option<String>,
}

/// Script-produced response accepted by the HTTP transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// Result of dispatching an endpoint into a runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHttpOutcome {
    /// No handler is registered for this exact method/path pair.
    NotFound,
    /// Handler completed and returned a bounded response candidate.
    Response(RuntimeHttpResponse),
    /// Handler failed; its detail remains in runtime logs, never in the body.
    Failed,
}

/// Invalid declaration errors exposed to a runtime loader as safe strings.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeHttpEndpointError {
    #[error("runtime HTTP endpoint path is invalid")]
    InvalidPath,
}

/// Small node-local fixed-window limiter for externally reachable runtime
/// endpoints. The HTTP transport supplies a peer/user scoped key; scripts
/// cannot inspect or mutate this state.
#[derive(Debug, Default)]
pub struct RuntimeHttpEndpointRateLimiter {
    recent: Mutex<BTreeMap<String, RuntimeHttpEndpointRateWindow>>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeHttpEndpointRateWindow {
    started: Instant,
    count: u32,
}

impl RuntimeHttpEndpointRateLimiter {
    /// Admit at most `max_requests_per_minute` requests for one endpoint key.
    #[must_use]
    pub fn allow(&self, key: String, max_requests_per_minute: u32) -> bool {
        const MAX_TRACKED_KEYS: usize = 10_000;
        let now = Instant::now();
        let mut recent = self
            .recent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        recent.retain(|_, window| now.duration_since(window.started) < Duration::from_secs(60));
        // A source-IP keyed limiter must remain bounded even under a spoofed or
        // botnet-scale stream of first-seen callers. Fail closed once the
        // bounded tracking budget is occupied by active windows.
        if !recent.contains_key(&key) && recent.len() >= MAX_TRACKED_KEYS {
            return false;
        }
        let window = recent.entry(key).or_insert(RuntimeHttpEndpointRateWindow {
            started: now,
            count: 0,
        });
        if window.count >= max_requests_per_minute {
            return false;
        }
        window.count = window.count.saturating_add(1);
        true
    }
}

/// Canonical relative path grammar: `/` followed by ASCII URL-safe segments.
/// It excludes empty segments, dot segments, query/fragment delimiters, and
/// percent encoding so all languages and the Axum router agree on one key.
#[must_use]
pub fn is_valid_runtime_http_path(path: &str) -> bool {
    if !path.starts_with('/') || path.len() > 256 || path == "/" || path.ends_with('/') {
        return false;
    }
    path.split('/').skip(1).all(|segment| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_path_grammar_is_narrow_and_canonical() {
        assert!(is_valid_runtime_http_path("/leaderboard/top"));
        assert!(is_valid_runtime_http_path("/v1.alpha/player_01"));
        for path in [
            "/",
            "leaderboard",
            "/a/",
            "/a//b",
            "/a/../b",
            "/a?b",
            "/a%2fb",
        ] {
            assert!(!is_valid_runtime_http_path(path), "{path} must be rejected");
        }
    }

    #[test]
    fn endpoint_full_path_stays_under_reserved_prefix() {
        let endpoint = RuntimeHttpEndpoint::new(
            RuntimeHttpMethod::Post,
            "/webhook",
            RuntimeHttpAuth::Session,
        )
        .expect("valid endpoint");
        assert_eq!(endpoint.full_path(), "/ext/webhook");
    }

    #[test]
    fn endpoint_rate_limiter_is_scoped_to_its_key() {
        let limiter = RuntimeHttpEndpointRateLimiter::default();
        assert!(limiter.allow("a".to_string(), 1));
        assert!(!limiter.allow("a".to_string(), 1));
        assert!(limiter.allow("b".to_string(), 1));
    }

    #[test]
    fn endpoint_rate_limiter_bounds_distinct_active_callers() {
        let limiter = RuntimeHttpEndpointRateLimiter::default();
        for key in 0..10_000 {
            assert!(limiter.allow(format!("caller-{key}"), 1));
        }
        assert!(!limiter.allow("one-too-many".to_string(), 1));
    }
}
