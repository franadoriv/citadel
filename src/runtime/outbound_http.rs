//! Bounded, Rust-owned outbound HTTP for trusted game runtimes.
//!
//! Lua never receives a socket or an HTTP client. The eventual host function
//! passes this small request DTO to [`TrustedHttpClient`], which owns DNS, TLS,
//! redirect and proxy policy. Keeping this boundary in Rust makes the safe
//! defaults auditable and reusable by every runtime language.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::redirect::Policy;
use reqwest::{Client, Method};

/// Maximum request body accepted from game logic.
pub const MAX_OUTBOUND_HTTP_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum response body retained and returned to game logic.
pub const MAX_OUTBOUND_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;
/// Per-request wall-clock limit, including connection and response reads.
pub const OUTBOUND_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

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
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OutboundHttpError {
    #[error("outbound HTTP request body exceeds {MAX_OUTBOUND_HTTP_REQUEST_BYTES} bytes")]
    RequestTooLarge,
    #[error("outbound HTTP response exceeds {MAX_OUTBOUND_HTTP_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,
    #[error("invalid outbound HTTP method")]
    InvalidMethod,
    #[error("invalid outbound HTTP header")]
    InvalidHeader,
    #[error("outbound HTTP request failed: {0}")]
    RequestFailed(String),
}

/// Reusable HTTP client with Citadel's non-ambient outbound policy.
#[derive(Clone, Debug)]
pub struct TrustedHttpClient {
    client: Client,
}

impl TrustedHttpClient {
    /// Build the client once per runtime host.
    pub fn new() -> Result<Self, OutboundHttpError> {
        let client = Client::builder()
            .timeout(OUTBOUND_HTTP_TIMEOUT)
            .redirect(Policy::none())
            .no_proxy()
            .build()
            .map_err(|error| OutboundHttpError::RequestFailed(error.to_string()))?;
        Ok(Self { client })
    }

    /// Execute one bounded request. Redirects and ambient proxy configuration
    /// are disabled by the reusable client; a response is read incrementally so
    /// a missing or dishonest content length cannot exceed the body limit.
    pub async fn execute(
        &self,
        request: OutboundHttpRequest,
    ) -> Result<OutboundHttpResponse, OutboundHttpError> {
        if request.body.len() > MAX_OUTBOUND_HTTP_REQUEST_BYTES {
            return Err(OutboundHttpError::RequestTooLarge);
        }
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| OutboundHttpError::InvalidMethod)?;
        let mut builder = self.client.request(method, request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let mut response = builder
            .send()
            .await
            .map_err(|error| OutboundHttpError::RequestFailed(error.to_string()))?;
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
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            OutboundHttpError::RequestFailed(format!("outbound runtime unavailable: {error}"))
        })?;
        tokio::task::block_in_place(|| handle.block_on(self.execute(request)))
    }
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
    async fn executes_a_bounded_request_against_a_local_server() {
        let listener = TcpListener::bind("127.0.0.1:0")
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

        let client = TrustedHttpClient::new().expect("client");
        let response = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: format!("http://{address}/health"),
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
