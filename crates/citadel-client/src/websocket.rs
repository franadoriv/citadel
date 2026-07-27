//! WebSocket client for Citadel's realtime transport.
//!
//! Connects to a Citadel WebSocket endpoint and exchanges framed
//! [`Envelope`](citadel_wire::Envelope)s inside binary WebSocket messages,
//! using the shared `citadel-wire` codec.

use bytes::BytesMut;
use citadel_wire::protocol::{
    KIND_AUTH, KIND_AUTH_RESULT, KIND_RPC_REQUEST, KIND_RPC_RESPONSE, decode_auth_result,
    decode_rpc_response, encode_rpc_request,
};
use citadel_wire::{Envelope, decode_framed};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{AuthOutcome, ClientError, ClientResult};

/// A connected WebSocket client.
///
/// Reliable, ordered delivery only. Multiple framed envelopes may arrive in one
/// binary message; the client buffers and decodes them in order.
pub struct WsClient {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    recv_buf: BytesMut,
    next_request_id: u64,
}

impl WsClient {
    /// Connect to a Citadel WebSocket endpoint, e.g. `ws://127.0.0.1:7352/`.
    pub async fn connect(url: &str) -> ClientResult<Self> {
        let (stream, _response) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| ClientError::Connect(e.to_string()))?;
        Ok(Self {
            stream,
            recv_buf: BytesMut::new(),
            next_request_id: 1,
        })
    }

    /// Connect and immediately complete the auth handshake as an explicit
    /// **guest** (empty token). Convenience wrapper over [`WsClient::connect`] +
    /// [`WsClient::authenticate`] for the common "no account" path.
    pub async fn connect_as_guest(url: &str) -> ClientResult<(Self, AuthOutcome)> {
        let mut client = Self::connect(url).await?;
        let outcome = client.authenticate(None).await?;
        Ok((client, outcome))
    }

    /// Connect and immediately complete the auth handshake with a session
    /// `token`. Convenience wrapper over [`WsClient::connect`] +
    /// [`WsClient::authenticate`].
    pub async fn connect_with_token(url: &str, token: &[u8]) -> ClientResult<(Self, AuthOutcome)> {
        let mut client = Self::connect(url).await?;
        let outcome = client.authenticate(Some(token)).await?;
        Ok((client, outcome))
    }

    /// Perform the realtime auth handshake as the **first** frame on this
    /// connection.
    ///
    /// Sends a [`KIND_AUTH`] envelope carrying `token` (the session-token bytes),
    /// or an empty body when `token` is `None` to request an explicit **guest**
    /// session, then awaits the single [`KIND_AUTH_RESULT`] the server returns and
    /// maps it to an [`AuthOutcome`].
    ///
    /// This must be called before any other send on a fresh connection: the
    /// server holds the connection pending until it sees the first frame and only
    /// registers the session (fires `on_join`, routes traffic) once the handshake
    /// resolves. A rejected handshake is returned as
    /// [`AuthOutcome::Rejected`]; the server closes the connection immediately
    /// after, so the caller should drop this client.
    pub async fn authenticate(&mut self, token: Option<&[u8]>) -> ClientResult<AuthOutcome> {
        let body = token.unwrap_or(&[]).to_vec();
        self.send(&Envelope::new(KIND_AUTH, body)).await?;

        match self.recv().await? {
            Some(env) if env.kind == KIND_AUTH_RESULT => {
                let result = decode_auth_result(&env.body).ok_or_else(|| {
                    ClientError::Connect(
                        "server sent a malformed KIND_AUTH_RESULT body".to_string(),
                    )
                })?;
                if result.is_authenticated() {
                    Ok(AuthOutcome::Authenticated {
                        user_id: result.user_id.to_owned(),
                    })
                } else if result.is_guest() {
                    Ok(AuthOutcome::Guest)
                } else {
                    Ok(AuthOutcome::Rejected {
                        reason_class: result.reason_class,
                    })
                }
            }
            Some(env) => Err(ClientError::Connect(format!(
                "expected KIND_AUTH_RESULT ({KIND_AUTH_RESULT}) as the first server \
                 frame, got kind {}",
                env.kind
            ))),
            None => Err(ClientError::Connect(
                "connection closed during the auth handshake".to_string(),
            )),
        }
    }

    /// Send an envelope as a framed binary WebSocket message.
    pub async fn send(&mut self, env: &Envelope) -> ClientResult<()> {
        let frame = env.encode_framed();
        self.stream
            .send(Message::Binary(frame.to_vec()))
            .await
            .map_err(|e| ClientError::Send(e.to_string()))
    }

    /// Receive the next envelope, awaiting more data as needed.
    ///
    /// Returns `Ok(None)` if the connection closed cleanly before another
    /// envelope arrived.
    pub async fn recv(&mut self) -> ClientResult<Option<Envelope>> {
        loop {
            // Serve any envelope already buffered from a previous message.
            if let Some(env) = decode_framed(&mut self.recv_buf)? {
                return Ok(Some(env));
            }
            match self.stream.next().await {
                Some(Ok(Message::Binary(data))) => {
                    self.recv_buf.extend_from_slice(&data);
                }
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(_)) => {} // ignore ping/pong/text
                Some(Err(e)) => return Err(ClientError::Receive(e.to_string())),
            }
        }
    }

    /// Call a server-side RPC method and await its correlated reply.
    ///
    /// Generates a fresh, monotonically increasing `request_id`, sends a
    /// [`KIND_RPC_REQUEST`] carrying `method` + `payload` (reliable, ordered),
    /// then reads envelopes until the matching [`KIND_RPC_RESPONSE`] arrives.
    /// Returns the handler's reply bytes on success, or [`ClientError::Rpc`] if
    /// the server answered with an error status.
    ///
    /// Correlation and usage notes:
    ///
    /// - The reply is matched by `request_id`, so a stale/duplicate response for
    ///   a different id is skipped rather than mistaken for this call's reply.
    /// - This helper is intended for a connection that is not concurrently used
    ///   for other receives: any non-RPC envelopes (e.g. relayed peer positions)
    ///   that arrive while awaiting the reply are discarded. Apps that also need
    ///   the relay stream should instead poll and dispatch by kind (as the Unity
    ///   sample's `RpcClient` does) rather than call this helper.
    /// - It does not impose a timeout; wrap the call in
    ///   [`tokio::time::timeout`] to bound how long you wait.
    pub async fn call_rpc(&mut self, method: &str, payload: &[u8]) -> ClientResult<Vec<u8>> {
        let request_id = self.next_request_id;
        // Wrapping is defensive only: 2^64 calls per connection is unreachable.
        self.next_request_id = self.next_request_id.wrapping_add(1);

        let body = encode_rpc_request(request_id, method, payload);
        self.send(&Envelope::new(KIND_RPC_REQUEST, body)).await?;

        loop {
            match self.recv().await? {
                Some(env) if env.kind == KIND_RPC_RESPONSE => {
                    let Some(response) = decode_rpc_response(&env.body) else {
                        // Malformed response body: cannot correlate it; keep
                        // waiting for a well-formed one.
                        continue;
                    };
                    if response.request_id != request_id {
                        // A response for a different (e.g. superseded) call.
                        continue;
                    }
                    if response.is_ok() {
                        return Ok(response.payload.to_vec());
                    }
                    return Err(ClientError::Rpc {
                        request_id,
                        message: String::from_utf8_lossy(response.payload).into_owned(),
                    });
                }
                // Unrelated envelope (e.g. a relayed peer position): skip it.
                Some(_) => continue,
                None => {
                    return Err(ClientError::Receive(
                        "connection closed before the RPC response arrived".to_string(),
                    ));
                }
            }
        }
    }

    /// Close the connection.
    pub async fn close(mut self) -> ClientResult<()> {
        self.stream
            .close(None)
            .await
            .map_err(|e| ClientError::Send(e.to_string()))
    }
}
