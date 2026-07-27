//! Minimal Rust client SDK for Citadel's realtime transports.
//!
//! Provides a small `connect / send / recv` surface over WebSocket and QUIC,
//! reusing the shared [`citadel_wire::Envelope`] so clients and the server speak
//! the exact same byte format. This crate is pure network/state logic with no
//! rendering, so it is fully testable; the native demo (`demo-client`) renders on
//! top of it.
//!
//! Delivery model:
//!
//! - WebSocket: reliable, ordered; one method [`WsClient::send`].
//! - QUIC: reliable streams ([`QuicClient::send_reliable`]) and unreliable
//!   datagrams ([`QuicClient::send_unreliable`]).
//!
//! Endpoints and certificates are parameters; no credentials are embedded.

pub mod http;
pub mod quic;
pub mod websocket;

pub use citadel_wire::Envelope;
pub use http::{
    CitadelHttpClient, EmailAuthenticationRequest, LookupUsersRequest, LookupUsersResponse,
    PublicProfile, SessionTokenPair, UpdateAccountRequest,
};
pub use quic::QuicClient;
pub use websocket::WsClient;

/// Errors returned by the client SDK.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Failed to connect or complete a handshake.
    #[error("connect failed: {0}")]
    Connect(String),
    /// Failed to send an envelope.
    #[error("send failed: {0}")]
    Send(String),
    /// Failed to receive or the connection closed.
    #[error("receive failed: {0}")]
    Receive(String),
    /// A received payload could not be decoded into an envelope.
    #[error("decode failed: {0}")]
    Decode(#[from] citadel_wire::WireError),
    /// TLS or endpoint configuration error.
    #[error("configuration error: {0}")]
    Config(String),
    /// A server-side RPC handler answered with an error status
    /// ([`citadel_wire::protocol::RPC_STATUS_ERROR`]). The `message` is the
    /// generic error text the server returned (never a stack trace or
    /// internals); `request_id` is the correlation id of the failed call.
    #[error("rpc call {request_id} failed: {message}")]
    Rpc {
        /// Correlation id of the RPC request that failed.
        request_id: u64,
        /// Short, generic error message returned by the server.
        message: String,
    },
    /// A sanitized error returned by Citadel's player HTTP API. `status` is
    /// absent for a transport failure; `code` and `message` never contain the
    /// caller's token or a server-internal error detail.
    #[error("http request failed ({code}): {message}")]
    Http {
        /// HTTP status, if Citadel replied.
        status: Option<u16>,
        /// Stable server error code, or `transport_error`/`invalid_response`.
        code: String,
        /// Sanitized server message or a generic local failure message.
        message: String,
    },
}

/// Convenient result alias for client SDK operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// The resolved outcome of the realtime auth handshake
/// ([`WsClient::authenticate`](websocket::WsClient::authenticate)).
///
/// The handshake is the first frame on a new connection (see ): the
/// client presents a session token (or requests an explicit guest session) and
/// the server answers with exactly one of these outcomes before it registers the
/// session and admits the connection to the room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthOutcome {
    /// The token validated; the connection is bound to this account.
    Authenticated {
        /// The server-resolved account id the connection now acts as.
        user_id: String,
    },
    /// The connection was accepted as an anonymous guest (no account bound). Only
    /// possible when the server allows guests.
    Guest,
    /// The handshake was refused and the server closes the connection. The
    /// `reason_class` is a coarse `AUTH_REASON_*` byte
    /// ([`citadel_wire::protocol`]), never a precise, enumeration-aiding cause.
    Rejected {
        /// Coarse rejection reason class (`AUTH_REASON_*`).
        reason_class: u8,
    },
}
