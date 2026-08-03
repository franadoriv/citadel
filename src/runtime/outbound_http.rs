//! Bounded, Rust-owned outbound HTTP for trusted game runtimes.
//!
//! Lua never receives a socket or an HTTP client. The eventual host function
//! passes this small request DTO to [`TrustedHttpClient`], which owns DNS, TLS,
//! redirect and proxy policy. Keeping this boundary in Rust makes the safe
//! defaults auditable and reusable by every runtime language.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::redirect::Policy;
use reqwest::{Client, Method, Url, header::HeaderName, header::HeaderValue};
use tokio::sync::oneshot;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

/// Maximum request body accepted from game logic.
pub const MAX_OUTBOUND_HTTP_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum response body retained and returned to game logic.
pub const MAX_OUTBOUND_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;
/// Maximum headers accepted from game logic for one request.
pub const MAX_OUTBOUND_HTTP_HEADERS: usize = 64;
/// Maximum aggregate header-name and value bytes accepted from game logic.
pub const MAX_OUTBOUND_HTTP_HEADER_BYTES: usize = 16 * 1024;
/// Per-request wall-clock limit, including connection and response reads.
pub const OUTBOUND_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum outstanding or retained terminal requests per runtime host.
pub const MAX_OUTBOUND_HTTP_HANDLES: usize = 128;

/// A non-blocking request state returned by [`AsyncOutboundHttp::poll`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundHttpRequestState {
    Pending,
    Success(OutboundHttpResponse),
    Error(String),
    Timeout,
    Cancelled,
}

impl OutboundHttpRequestState {
    /// Stable, language-neutral status string exposed to script bindings.
    pub const fn status(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Success(_) => "success",
            Self::Error(_) => "error",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }
}

enum AsyncRequestEntry {
    Pending {
        receiver: oneshot::Receiver<OutboundHttpRequestState>,
        task: JoinHandle<()>,
    },
    Terminal(OutboundHttpRequestState),
}

/// Per-runtime bounded handle table for Rust-owned asynchronous HTTP.
///
/// Start only schedules work; poll uses `try_recv` and cancel aborts the Tokio
/// task, so no script callback or gameplay tick waits for network I/O.
#[derive(Clone)]
pub struct AsyncOutboundHttp {
    client: TrustedHttpClient,
    next_handle: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    entries: Arc<Mutex<HashMap<u64, AsyncRequestEntry>>>,
}

impl std::fmt::Debug for AsyncOutboundHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncOutboundHttp").finish_non_exhaustive()
    }
}

impl AsyncOutboundHttp {
    pub fn new(client: TrustedHttpClient) -> Self {
        Self {
            client,
            next_handle: Arc::new(AtomicU64::new(1)),
            generation: Arc::new(AtomicU64::new(1)),
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Schedules a request and returns an opaque, runtime-local handle.
    pub fn start(&self, request: OutboundHttpRequest) -> Result<u64, OutboundHttpError> {
        // Reject every deterministic policy/SSRF and shape violation before a
        // handle is observable. DNS resolution remains in `execute`, where its
        // result is pinned into Reqwest's per-request client before connect.
        self.client.preflight(&request)?;
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries.len() >= MAX_OUTBOUND_HTTP_HANDLES {
            if let Some(handle) = entries.iter().find_map(|(handle, entry)| {
                matches!(entry, AsyncRequestEntry::Terminal(_)).then_some(*handle)
            }) {
                entries.remove(&handle);
            } else {
                return Err(OutboundHttpError::HandleLimitReached);
            }
        }
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed).max(1);
        let client = self.client.clone();
        let (sender, receiver) = oneshot::channel();
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| {
            OutboundHttpError::RequestFailed(format!("outbound runtime unavailable: {e}"))
        })?;
        let task = runtime.spawn(async move {
            let result = match tokio::time::timeout(OUTBOUND_HTTP_TIMEOUT, client.execute(request))
                .await
            {
                Ok(Ok(response)) => OutboundHttpRequestState::Success(response),
                // Script-visible errors are a stable, redacted taxonomy;
                // transport/library details stay in Rust logs only.
                Ok(Err(error)) => OutboundHttpRequestState::Error(error.error_code().to_owned()),
                Err(_) => OutboundHttpRequestState::Timeout,
            };
            let _ = sender.send(result);
        });
        entries.insert(handle, AsyncRequestEntry::Pending { receiver, task });
        Ok(handle)
    }

    /// Reads a request state without waiting. Terminal entries remain readable
    /// until evicted by the bounded table's next starts.
    pub fn poll(&self, handle: u64) -> Result<OutboundHttpRequestState, OutboundHttpError> {
        // Polling must not become a capability bypass: even an old opaque handle
        // is unavailable after an operator disables outbound HTTP.
        self.client.ensure_enabled()?;
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let entry = entries
            .get_mut(&handle)
            .ok_or(OutboundHttpError::UnknownHandle)?;
        match entry {
            AsyncRequestEntry::Terminal(state) => Ok(state.clone()),
            AsyncRequestEntry::Pending { receiver, .. } => match receiver.try_recv() {
                Ok(state) => {
                    *entry = AsyncRequestEntry::Terminal(state.clone());
                    Ok(state)
                }
                Err(oneshot::error::TryRecvError::Empty) => Ok(OutboundHttpRequestState::Pending),
                Err(oneshot::error::TryRecvError::Closed) => {
                    let state = OutboundHttpRequestState::Cancelled;
                    *entry = AsyncRequestEntry::Terminal(state.clone());
                    Ok(state)
                }
            },
        }
    }

    /// Cancels a pending request. It is idempotent for already-terminal handles.
    pub fn cancel(&self, handle: u64) -> Result<OutboundHttpRequestState, OutboundHttpError> {
        // Cancellation is part of the script-visible async capability too; do
        // not reveal handle state while the capability is disabled.
        self.client.ensure_enabled()?;
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let entry = entries
            .get_mut(&handle)
            .ok_or(OutboundHttpError::UnknownHandle)?;
        if let AsyncRequestEntry::Pending { task, .. } = entry {
            task.abort();
            *entry = AsyncRequestEntry::Terminal(OutboundHttpRequestState::Cancelled);
        }
        match entry {
            AsyncRequestEntry::Terminal(state) => Ok(state.clone()),
            AsyncRequestEntry::Pending { .. } => unreachable!(),
        }
    }

    /// Abort and forget every request owned by this runtime host.
    ///
    /// Runtime reload/shutdown paths must call this only after a replacement VM
    /// is accepted (or while shutting down). Clearing the table invalidates old
    /// handles, while aborting each join handle prevents detached work from
    /// publishing a result into a later VM generation.
    pub fn cancel_all(&self) {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        for entry in entries.values() {
            if let AsyncRequestEntry::Pending { task, .. } = entry {
                task.abort();
            }
        }
        entries.clear();
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Monotonic generation for lifecycle owners that need to associate a VM
    /// generation with its handle table. It changes only after `cancel_all`.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

// The language VMs own the last references to their request table.  Explicit
// lifecycle owners may call `cancel_all` sooner, but dropping a replaced VM or
// a runtime during shutdown must never detach its Tokio requests.
impl Drop for AsyncOutboundHttp {
    fn drop(&mut self) {
        if Arc::strong_count(&self.entries) == 1 {
            self.cancel_all();
        }
    }
}

/// Runtime-owned policy applied before an outbound request reaches Reqwest.
/// The configuration layer supplies this policy; keeping it at the Rust edge
/// prevents language adapters from bypassing an operator-disabled capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundHttpPolicy {
    pub enabled: bool,
    pub max_concurrent_requests: u32,
    pub max_requests_per_minute: u32,
    pub allowed_hosts: Vec<String>,
    pub allowed_ports: Vec<u16>,
    pub allow_private_networks: bool,
}

impl Default for OutboundHttpPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_requests: 16,
            max_requests_per_minute: 120,
            allowed_hosts: Vec::new(),
            allowed_ports: vec![80, 443],
            allow_private_networks: false,
        }
    }
}

impl From<&crate::config::OutboundHttpCapabilityConfig> for OutboundHttpPolicy {
    fn from(config: &crate::config::OutboundHttpCapabilityConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_concurrent_requests: config.max_concurrent_requests,
            max_requests_per_minute: config.max_requests_per_minute,
            allowed_hosts: config.allowed_hosts.clone(),
            allowed_ports: config.allowed_ports.clone(),
            allow_private_networks: config.allow_private_networks,
        }
    }
}

/// A request from a trusted game runtime. Header order is normalized so callers
/// cannot use it as an accidental hidden channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// A bounded HTTP response returned to a runtime adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A request was malformed, exceeded a bound, or failed at the network edge.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum OutboundHttpError {
    #[error("outbound HTTP request body exceeds {MAX_OUTBOUND_HTTP_REQUEST_BYTES} bytes")]
    RequestTooLarge,
    #[error("outbound HTTP response exceeds {MAX_OUTBOUND_HTTP_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,
    #[error("invalid outbound HTTP method")]
    InvalidMethod,
    #[error("invalid outbound HTTP header")]
    InvalidHeader,
    #[error("outbound HTTP request contains too many or too-large headers")]
    HeadersTooLarge,
    #[error("outbound HTTP request must not override the Host or :authority header")]
    AuthorityHeaderForbidden,
    #[error("outbound HTTP capability is disabled by runtime policy")]
    Disabled,
    #[error("outbound HTTP URL must use http or https")]
    InvalidScheme,
    #[error("outbound HTTP URL is invalid")]
    InvalidUrl,
    #[error("outbound HTTP URL must not contain credentials")]
    UrlCredentialsForbidden,
    #[error("outbound HTTP URL must use a DNS hostname, not an IP literal")]
    IpLiteralForbidden,
    #[error("outbound HTTP hostname is not permitted by runtime policy")]
    HostForbidden,
    #[error("outbound HTTP port is not permitted by runtime policy")]
    PortForbidden,
    #[error("outbound HTTP hostname resolved to a non-public address")]
    PrivateAddressForbidden,
    #[error("outbound HTTP hostname did not resolve to an allowed address")]
    ResolutionFailed,
    #[error("outbound HTTP request concurrency limit reached")]
    ConcurrentLimitReached,
    #[error("outbound HTTP request rate limit reached")]
    RateLimitReached,
    #[error("outbound HTTP handle limit reached")]
    HandleLimitReached,
    #[error("unknown outbound HTTP request handle")]
    UnknownHandle,
    #[error("outbound HTTP request failed: {0}")]
    RequestFailed(String),
}

impl OutboundHttpError {
    /// Stable redacted code suitable for untrusted/script-visible responses.
    /// Never return the `Display` form across a runtime boundary: it can carry
    /// request URLs or dependency-specific network details.
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::RequestTooLarge => "request_too_large",
            Self::ResponseTooLarge => "response_too_large",
            Self::InvalidMethod => "invalid_method",
            Self::InvalidHeader => "invalid_header",
            Self::HeadersTooLarge => "headers_too_large",
            Self::AuthorityHeaderForbidden => "authority_header_forbidden",
            Self::Disabled => "capability_disabled",
            Self::InvalidScheme => "invalid_scheme",
            Self::InvalidUrl => "invalid_url",
            Self::UrlCredentialsForbidden => "url_credentials_forbidden",
            Self::IpLiteralForbidden => "ip_literal_forbidden",
            Self::HostForbidden => "host_forbidden",
            Self::PortForbidden => "port_forbidden",
            Self::PrivateAddressForbidden => "private_address_forbidden",
            Self::ResolutionFailed => "resolution_failed",
            Self::ConcurrentLimitReached => "concurrent_limit_reached",
            Self::RateLimitReached => "rate_limit_reached",
            Self::HandleLimitReached => "handle_limit_reached",
            Self::UnknownHandle => "unknown_handle",
            Self::RequestFailed(_) => "request_failed",
        }
    }
}

/// Reusable HTTP client with Citadel's non-ambient outbound policy.
#[derive(Clone, Debug)]
pub struct TrustedHttpClient {
    policy: OutboundHttpPolicy,
    request_slots: Arc<Semaphore>,
    request_timestamps: Arc<Mutex<VecDeque<Instant>>>,
}

impl TrustedHttpClient {
    /// Build the client once per runtime host.
    pub fn new() -> Result<Self, OutboundHttpError> {
        Self::new_with_policy(OutboundHttpPolicy::default())
    }

    /// Build the client with the policy selected by the runtime configuration.
    pub fn new_with_policy(policy: OutboundHttpPolicy) -> Result<Self, OutboundHttpError> {
        let concurrency = usize::try_from(policy.max_concurrent_requests)
            .map_err(|_| OutboundHttpError::ConcurrentLimitReached)?;
        Ok(Self {
            request_slots: Arc::new(Semaphore::new(concurrency)),
            request_timestamps: Arc::new(Mutex::new(VecDeque::new())),
            policy,
        })
    }

    /// Execute one bounded request. Redirects and ambient proxy configuration
    /// are disabled by the reusable client; a response is read incrementally so
    /// a missing or dishonest content length cannot exceed the body limit.
    pub async fn execute(
        &self,
        request: OutboundHttpRequest,
    ) -> Result<OutboundHttpResponse, OutboundHttpError> {
        self.preflight(&request)?;
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| OutboundHttpError::InvalidMethod)?;
        let url = self.validate_url(&request.url)?;
        if request.headers.len() > MAX_OUTBOUND_HTTP_HEADERS {
            return Err(OutboundHttpError::HeadersTooLarge);
        }
        let mut header_bytes = 0usize;
        let mut headers = Vec::with_capacity(request.headers.len());
        for (name, value) in request.headers {
            header_bytes = header_bytes
                .saturating_add(name.len())
                .saturating_add(value.len());
            if header_bytes > MAX_OUTBOUND_HTTP_HEADER_BYTES {
                return Err(OutboundHttpError::HeadersTooLarge);
            }
            if name.eq_ignore_ascii_case("host") || name == ":authority" {
                return Err(OutboundHttpError::AuthorityHeaderForbidden);
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| OutboundHttpError::InvalidHeader)?;
            let value =
                HeaderValue::from_str(&value).map_err(|_| OutboundHttpError::InvalidHeader)?;
            headers.push((name, value));
        }
        let _permit = self.acquire_request_slot()?;
        let (host, addresses) = self.resolve_target(&url).await?;
        // Resolve and pin each connection address on this request's dedicated
        // client. Reqwest therefore cannot issue a second DNS lookup between
        // validation and connect (DNS rebinding cannot reach a private target).
        let client_builder = Client::builder()
            .timeout(OUTBOUND_HTTP_TIMEOUT)
            .redirect(Policy::none())
            .no_proxy()
            .resolve_to_addrs(&host, &addresses);
        let client = client_builder
            .build()
            .map_err(|error| OutboundHttpError::RequestFailed(error.to_string()))?;
        let mut builder = client.request(method, url).body(request.body);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let mut response = builder
            .send()
            .await
            .map_err(|error| OutboundHttpError::RequestFailed(format!("{error:?}")))?;
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(MAX_OUTBOUND_HTTP_RESPONSE_BYTES).unwrap_or(u64::MAX)
        }) {
            return Err(OutboundHttpError::ResponseTooLarge);
        }
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| OutboundHttpError::RequestFailed(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_OUTBOUND_HTTP_RESPONSE_BYTES {
                return Err(OutboundHttpError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(OutboundHttpResponse { status, body })
    }

    /// Fail closed for every script-visible operation when outbound HTTP is
    /// disabled. Async `poll` and `cancel` share this guard with `start`.
    fn ensure_enabled(&self) -> Result<(), OutboundHttpError> {
        if self.policy.enabled {
            Ok(())
        } else {
            Err(OutboundHttpError::Disabled)
        }
    }

    /// Validate the synchronous, deterministic portion of the outbound policy.
    /// This is deliberately shared by `execute` and async `start`, so a denied
    /// request never consumes a handle just because it chose the async API.
    fn preflight(&self, request: &OutboundHttpRequest) -> Result<(), OutboundHttpError> {
        self.ensure_enabled()?;
        if request.body.len() > MAX_OUTBOUND_HTTP_REQUEST_BYTES {
            return Err(OutboundHttpError::RequestTooLarge);
        }
        Method::from_bytes(request.method.as_bytes())
            .map_err(|_| OutboundHttpError::InvalidMethod)?;
        self.validate_url(&request.url)?;
        if request.headers.len() > MAX_OUTBOUND_HTTP_HEADERS {
            return Err(OutboundHttpError::HeadersTooLarge);
        }
        let mut header_bytes = 0usize;
        for (name, value) in &request.headers {
            header_bytes = header_bytes
                .saturating_add(name.len())
                .saturating_add(value.len());
            if header_bytes > MAX_OUTBOUND_HTTP_HEADER_BYTES {
                return Err(OutboundHttpError::HeadersTooLarge);
            }
            if name.eq_ignore_ascii_case("host") || name == ":authority" {
                return Err(OutboundHttpError::AuthorityHeaderForbidden);
            }
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| OutboundHttpError::InvalidHeader)?;
            HeaderValue::from_str(value).map_err(|_| OutboundHttpError::InvalidHeader)?;
        }
        Ok(())
    }

    fn validate_url(&self, raw_url: &str) -> Result<Url, OutboundHttpError> {
        let url = Url::parse(raw_url).map_err(|_| OutboundHttpError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(OutboundHttpError::InvalidScheme);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(OutboundHttpError::UrlCredentialsForbidden);
        }
        let host = url.host_str().ok_or(OutboundHttpError::InvalidUrl)?;
        if host.parse::<IpAddr>().is_ok() {
            return Err(OutboundHttpError::IpLiteralForbidden);
        }
        if !self.policy.allowed_hosts.is_empty()
            && !self
                .policy
                .allowed_hosts
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(host))
        {
            return Err(OutboundHttpError::HostForbidden);
        }
        let port = url
            .port_or_known_default()
            .ok_or(OutboundHttpError::InvalidUrl)?;
        if !self.policy.allowed_ports.contains(&port) {
            return Err(OutboundHttpError::PortForbidden);
        }
        Ok(url)
    }

    fn acquire_request_slot(&self) -> Result<OwnedSemaphorePermit, OutboundHttpError> {
        let permit = Arc::clone(&self.request_slots)
            .try_acquire_owned()
            .map_err(|_| OutboundHttpError::ConcurrentLimitReached)?;
        let mut timestamps = self
            .request_timestamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        while timestamps
            .front()
            .is_some_and(|started| now.duration_since(*started) >= Duration::from_secs(60))
        {
            timestamps.pop_front();
        }
        if timestamps.len()
            >= usize::try_from(self.policy.max_requests_per_minute).unwrap_or(usize::MAX)
        {
            return Err(OutboundHttpError::RateLimitReached);
        }
        timestamps.push_back(now);
        Ok(permit)
    }

    async fn resolve_target(
        &self,
        url: &Url,
    ) -> Result<(String, Vec<SocketAddr>), OutboundHttpError> {
        let host = url
            .host_str()
            .ok_or(OutboundHttpError::InvalidUrl)?
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or(OutboundHttpError::InvalidUrl)?;
        let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|_| OutboundHttpError::ResolutionFailed)?
            .collect();
        if addresses.is_empty() {
            return Err(OutboundHttpError::ResolutionFailed);
        }
        if !self.policy.allow_private_networks
            && addresses
                .iter()
                .any(|address| !is_public_address(address.ip()))
        {
            return Err(OutboundHttpError::PrivateAddressForbidden);
        }
        Ok((host, addresses))
    }

    /// Synchronous adapter boundary for embedded language runtimes.
    ///
    /// Game handlers are synchronous today, while the network client is async.
    /// This is intentionally the only place an adapter may wait for outbound
    /// I/O: it keeps sockets and policy in Rust and avoids giving a language VM
    /// an executor or a raw network handle.
    pub fn execute_blocking(
        &self,
        request: OutboundHttpRequest,
    ) -> Result<OutboundHttpResponse, OutboundHttpError> {
        // Preserve the fail-closed policy even in a host context without a
        // Tokio reactor. This also keeps disabled capability attempts from
        // allocating or depending on an async runtime at all.
        if !self.policy.enabled {
            return Err(OutboundHttpError::Disabled);
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            OutboundHttpError::RequestFailed(format!("outbound runtime unavailable: {error}"))
        })?;
        tokio::task::block_in_place(|| handle.block_on(self.execute(request)))
    }
}

fn is_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => {
            let segments = address.segments();
            if segments[..6].iter().all(|segment| *segment == 0) {
                let octets = address.octets();
                return is_public_ipv4(std::net::Ipv4Addr::new(
                    octets[12], octets[13], octets[14], octets[15],
                ));
            }
            !address.is_loopback()
                && !address.is_unspecified()
                && !address.is_multicast()
                && !address.is_unicast_link_local()
                && !address.is_unique_local()
                && !is_ipv6_site_local(address)
                && !is_ipv6_documentation(address)
                && !is_ipv6_teredo(address)
                && !is_ipv6_6to4(address)
                && address
                    .to_ipv4_mapped()
                    .is_none_or(|mapped| is_public_address(IpAddr::V4(mapped)))
        }
    }
}

fn is_public_ipv4(address: std::net::Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    !address.is_private()
        && !address.is_loopback()
        && !address.is_link_local()
        && !address.is_unspecified()
        && !address.is_multicast()
        && !address.is_documentation()
        && first != 0
        && !(first == 100 && (64..=127).contains(&second))
        && !(first == 192 && second == 0 && third == 0)
        && !(first == 192 && second == 88 && third == 99)
        && !(first == 198 && (18..=19).contains(&second))
        && first < 240
}

fn is_ipv6_documentation(address: std::net::Ipv6Addr) -> bool {
    let segments = address.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

fn is_ipv6_site_local(address: std::net::Ipv6Addr) -> bool {
    address.segments()[0] & 0xffc0 == 0xfec0
}

fn is_ipv6_teredo(address: std::net::Ipv6Addr) -> bool {
    let segments = address.segments();
    segments[0] == 0x2001 && segments[1] == 0
}

fn is_ipv6_6to4(address: std::net::Ipv6Addr) -> bool {
    address.segments()[0] == 0x2002
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use tokio::net::TcpListener;

    #[test]
    fn client_uses_the_fixed_outbound_policy() {
        TrustedHttpClient::new().expect("bounded Rust-owned client builds");
    }

    #[test]
    fn policy_is_derived_from_operator_configuration() {
        let config = crate::config::OutboundHttpCapabilityConfig {
            enabled: false,
            max_concurrent_requests: 3,
            max_requests_per_minute: 7,
            allowed_hosts: vec!["api.example.test".to_string()],
            allowed_ports: vec![443],
            allow_private_networks: false,
        };
        assert_eq!(
            OutboundHttpPolicy::from(&config),
            OutboundHttpPolicy {
                enabled: false,
                max_concurrent_requests: 3,
                max_requests_per_minute: 7,
                allowed_hosts: vec!["api.example.test".to_string()],
                allowed_ports: vec![443],
                allow_private_networks: false,
            }
        );
    }

    #[tokio::test]
    async fn disabled_policy_rejects_before_network_io() {
        let client = TrustedHttpClient::new_with_policy(OutboundHttpPolicy {
            enabled: false,
            ..OutboundHttpPolicy::default()
        })
        .expect("client builds");
        let error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "http://127.0.0.1:1".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect_err("disabled capability rejects before network I/O");
        assert_eq!(error, OutboundHttpError::Disabled);
    }

    #[tokio::test]
    async fn async_handles_are_nonblocking_and_cancellable() {
        let client = TrustedHttpClient::new().expect("client");
        let async_client = AsyncOutboundHttp::new(client);
        let (_sender, receiver) = oneshot::channel();
        let task = tokio::spawn(std::future::pending());
        let handle = 99;
        async_client
            .entries
            .lock()
            .expect("table")
            .insert(handle, AsyncRequestEntry::Pending { receiver, task });
        assert_eq!(
            async_client.poll(handle).expect("known handle"),
            OutboundHttpRequestState::Pending
        );
        assert_eq!(
            async_client.cancel(handle).expect("known handle"),
            OutboundHttpRequestState::Cancelled
        );
        assert_eq!(
            async_client.poll(handle).expect("terminal handle"),
            OutboundHttpRequestState::Cancelled
        );
        assert!(matches!(
            async_client.poll(handle + 1),
            Err(OutboundHttpError::UnknownHandle)
        ));
    }

    #[tokio::test]
    async fn async_poll_returns_deterministic_success_and_timeout_states() {
        let async_client = AsyncOutboundHttp::new(TrustedHttpClient::new().expect("client"));
        let (success_sender, success_receiver) = oneshot::channel();
        let (timeout_sender, timeout_receiver) = oneshot::channel();
        async_client.entries.lock().expect("table").extend([
            (
                1,
                AsyncRequestEntry::Pending {
                    receiver: success_receiver,
                    task: tokio::spawn(std::future::pending()),
                },
            ),
            (
                2,
                AsyncRequestEntry::Pending {
                    receiver: timeout_receiver,
                    task: tokio::spawn(std::future::pending()),
                },
            ),
        ]);
        success_sender
            .send(OutboundHttpRequestState::Success(OutboundHttpResponse {
                status: 204,
                body: Vec::new(),
            }))
            .expect("receiver remains live");
        timeout_sender
            .send(OutboundHttpRequestState::Timeout)
            .expect("receiver remains live");

        assert_eq!(
            async_client.poll(1).expect("success handle"),
            OutboundHttpRequestState::Success(OutboundHttpResponse {
                status: 204,
                body: Vec::new(),
            })
        );
        assert_eq!(
            async_client.poll(2).expect("timeout handle"),
            OutboundHttpRequestState::Timeout
        );
    }

    #[test]
    fn async_start_preserves_disabled_capability_policy() {
        let client = TrustedHttpClient::new_with_policy(OutboundHttpPolicy {
            enabled: false,
            ..OutboundHttpPolicy::default()
        })
        .expect("client");
        let error = AsyncOutboundHttp::new(client)
            .start(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "https://example.test/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .expect_err("disabled before scheduling");
        assert_eq!(error, OutboundHttpError::Disabled);
    }

    #[tokio::test]
    async fn async_start_preflights_ssrf_before_allocating_a_handle() {
        let async_client = AsyncOutboundHttp::new(TrustedHttpClient::new().expect("client"));
        let error = async_client
            .start(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "http://127.0.0.1:1/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .expect_err("IP literal must fail before async handle allocation");
        assert_eq!(error, OutboundHttpError::IpLiteralForbidden);
        assert!(async_client.entries.lock().expect("table").is_empty());
    }

    #[tokio::test]
    async fn async_cancel_all_invalidates_old_handles_and_aborts_work() {
        let async_client = AsyncOutboundHttp::new(TrustedHttpClient::new().expect("client"));
        let generation = async_client.generation();
        let (_sender, receiver) = oneshot::channel();
        let task = tokio::spawn(std::future::pending());
        async_client
            .entries
            .lock()
            .expect("table")
            .insert(7, AsyncRequestEntry::Pending { receiver, task });

        async_client.cancel_all();
        assert_eq!(async_client.generation(), generation + 1);
        assert!(async_client.entries.lock().expect("table").is_empty());
        assert!(matches!(
            async_client.poll(7),
            Err(OutboundHttpError::UnknownHandle)
        ));
    }

    #[tokio::test]
    async fn dropping_the_last_runtime_handle_table_owner_aborts_pending_work() {
        struct AbortWitness(Option<oneshot::Sender<()>>);

        impl Drop for AbortWitness {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let async_client = AsyncOutboundHttp::new(TrustedHttpClient::new().expect("client"));
        let (started_sender, started_receiver) = oneshot::channel();
        let (aborted_sender, aborted_receiver) = oneshot::channel();
        let (_result_sender, result_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _witness = AbortWitness(Some(aborted_sender));
            let _ = started_sender.send(());
            std::future::pending::<()>().await;
        });
        started_receiver.await.expect("pending task started");
        async_client.entries.lock().expect("table").insert(
            7,
            AsyncRequestEntry::Pending {
                receiver: result_receiver,
                task,
            },
        );

        // Language VMs retain clones in host closures: only replacement of the
        // final owner may abort the pending request.
        let retained_owner = async_client.clone();
        drop(async_client);
        assert_eq!(
            retained_owner.poll(7).expect("retained handle"),
            OutboundHttpRequestState::Pending
        );
        drop(retained_owner);
        tokio::time::timeout(Duration::from_secs(1), aborted_receiver)
            .await
            .expect("last owner drop aborts the task")
            .expect("task abort drops its witness");
    }

    #[tokio::test]
    async fn async_handle_limit_refuses_new_pending_work() {
        let async_client = AsyncOutboundHttp::new(TrustedHttpClient::new().expect("client"));
        let mut entries = async_client.entries.lock().expect("table");
        for handle in 0..MAX_OUTBOUND_HTTP_HANDLES as u64 {
            let (_sender, receiver) = oneshot::channel();
            entries.insert(
                handle,
                AsyncRequestEntry::Pending {
                    receiver,
                    task: tokio::spawn(std::future::pending()),
                },
            );
        }
        drop(entries);
        let error = async_client
            .start(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "https://example.test/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .expect_err("all pending slots must reject rather than exceed the cap");
        assert_eq!(error, OutboundHttpError::HandleLimitReached);
        async_client.cancel_all();
    }

    #[test]
    fn async_error_codes_are_stable_and_redacted() {
        assert_eq!(OutboundHttpError::InvalidUrl.error_code(), "invalid_url");
        assert_eq!(
            OutboundHttpError::RequestFailed("http://secret.invalid/token".to_string())
                .error_code(),
            "request_failed"
        );
    }

    #[tokio::test]
    async fn invalid_method_is_rejected_before_any_network_io() {
        let client = TrustedHttpClient::new().expect("client");
        let error = client
            .execute(OutboundHttpRequest {
                method: "GET\n".to_string(),
                url: "http://127.0.0.1:1".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect_err("invalid methods never reach the network");
        assert_eq!(error, OutboundHttpError::InvalidMethod);
    }

    #[tokio::test]
    async fn ip_literals_and_private_dns_targets_are_rejected_by_default() {
        let client = TrustedHttpClient::new().expect("client");
        let ip_error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "http://127.0.0.1/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect_err("IP literals must never reach the network");
        assert_eq!(ip_error, OutboundHttpError::IpLiteralForbidden);

        let private_error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "http://localhost/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect_err("private DNS target must never reach the network");
        assert_eq!(private_error, OutboundHttpError::PrivateAddressForbidden);
    }

    #[test]
    fn shared_policy_enforces_concurrency_and_rate_limits() {
        let client = TrustedHttpClient::new_with_policy(OutboundHttpPolicy {
            max_concurrent_requests: 1,
            max_requests_per_minute: 1,
            ..OutboundHttpPolicy::default()
        })
        .expect("client");
        let permit = client
            .acquire_request_slot()
            .expect("first request admitted");
        assert!(matches!(
            client.acquire_request_slot(),
            Err(OutboundHttpError::ConcurrentLimitReached)
        ));
        drop(permit);
        assert!(matches!(
            client.acquire_request_slot(),
            Err(OutboundHttpError::RateLimitReached)
        ));
    }

    #[test]
    fn policy_rejects_unlisted_hosts_and_ports_before_network_io() {
        let client = TrustedHttpClient::new_with_policy(OutboundHttpPolicy {
            allowed_hosts: vec!["api.example.test".to_string()],
            allowed_ports: vec![443],
            ..OutboundHttpPolicy::default()
        })
        .expect("client");
        assert_eq!(
            client
                .validate_url("https://other.example.test/")
                .expect_err("unlisted host must be rejected"),
            OutboundHttpError::HostForbidden
        );
        assert_eq!(
            client
                .validate_url("https://api.example.test:8443/")
                .expect_err("unlisted port must be rejected"),
            OutboundHttpError::PortForbidden
        );
    }

    #[tokio::test]
    async fn authority_and_oversized_headers_are_rejected_before_network_io() {
        let client = TrustedHttpClient::new().expect("client");
        let mut headers = BTreeMap::new();
        headers.insert("Host".to_string(), "internal.example.test".to_string());
        let error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "https://api.example.test/".to_string(),
                headers,
                body: Vec::new(),
            })
            .await
            .expect_err("host override must be rejected");
        assert_eq!(error, OutboundHttpError::AuthorityHeaderForbidden);

        let mut headers = BTreeMap::new();
        headers.insert(
            "x-large".to_string(),
            "x".repeat(MAX_OUTBOUND_HTTP_HEADER_BYTES),
        );
        let error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "https://api.example.test/".to_string(),
                headers,
                body: Vec::new(),
            })
            .await
            .expect_err("oversized headers must be rejected");
        assert_eq!(error, OutboundHttpError::HeadersTooLarge);
    }

    #[test]
    fn non_public_address_ranges_are_rejected() {
        for address in [
            "0.1.2.3",
            "100.64.0.1",
            "198.18.0.1",
            "240.0.0.1",
            "::127.0.0.1",
            "::ffff:192.168.1.1",
            "fec0::1",
            "2001:0::1",
            "2002:7f00:1::1",
        ] {
            let address = address.parse::<IpAddr>().expect("valid address");
            assert!(!is_public_address(address), "{address} must be denied");
        }
        assert!(is_public_address(
            "8.8.8.8".parse::<IpAddr>().expect("valid public IPv4")
        ));
    }

    #[tokio::test]
    async fn executes_a_bounded_request_against_a_local_server() {
        let localhost = tokio::net::lookup_host(("localhost", 0))
            .await
            .expect("localhost resolves")
            .next()
            .expect("localhost has an address");
        let listener = TcpListener::bind((localhost.ip(), 0))
            .await
            .expect("bind local HTTP test server");
        let address = listener.local_addr().expect("local address");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/health", get(|| async { "healthy" }))
                    .into_make_service(),
            )
            .await
            .expect("local HTTP test server runs");
        });
        tokio::task::yield_now().await;

        let client = TrustedHttpClient::new_with_policy(OutboundHttpPolicy {
            allowed_hosts: vec!["localhost".to_string()],
            allowed_ports: vec![address.port()],
            allow_private_networks: true,
            ..OutboundHttpPolicy::default()
        })
        .expect("client");
        let response = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: format!("http://localhost:{}/health", address.port()),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect("local request succeeds");
        server.abort();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"healthy");
    }
}
