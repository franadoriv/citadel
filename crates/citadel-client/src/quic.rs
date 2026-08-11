//! QUIC client for Citadel's realtime transport.
//!
//! Connects to a Citadel QUIC endpoint and exchanges
//! [`Envelope`](citadel_wire::Envelope)s over both reliable bidirectional
//! streams ([`QuicClient::send_reliable`]) and unreliable datagrams
//! ([`QuicClient::send_unreliable`]), using the shared `citadel-wire` codec.
//!
//! TLS: a QUIC client validates the server certificate by default with the
//! bundled public CA roots and the `server_name` passed to [`QuicClient::connect`].
//! Development helpers can pin a known certificate DER or explicitly disable
//! verification; the latter must never be used against untrusted servers.

use std::net::SocketAddr;
use std::sync::Arc;

use std::time::Duration;

use bytes::BytesMut;
use citadel_wire::protocol::{KIND_RPC_REQUEST, encode_rpc_request};
use citadel_wire::{Envelope, decode_datagram, decode_framed};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::rpc::RpcResponsePump;
use crate::{ClientError, ClientResult};

/// ALPN protocol identifier; must match the server's `citadel/0`.
pub const CITADEL_ALPN: &[u8] = b"citadel/0";

/// Maximum bytes read from a single reliable stream response.
const MAX_STREAM_BYTES: usize = 16 * 1024 * 1024;

/// TLS configuration strategy for the QUIC client.
#[derive(Default)]
pub enum ClientTls {
    /// Verify a CA-issued certificate chain using the bundled public CA roots
    /// and the `server_name` passed to [`QuicClient::connect`].
    #[default]
    WebPkiRoots,
    /// Trust exactly the given certificate chain (e.g. a server dev cert).
    Trusting(Vec<CertificateDer<'static>>),
    /// Disable certificate verification entirely. Dev/test only; never
    /// validates server identity.
    InsecureSkipVerification,
}

impl ClientTls {
    /// Use the bundled public CA roots and hostname verification. This is the
    /// production default for public Citadel servers.
    #[must_use]
    pub fn webpki_roots() -> Self {
        Self::WebPkiRoots
    }

    /// Convenience constructor for [`ClientTls::Trusting`].
    #[must_use]
    pub fn trusting(cert_chain: Vec<CertificateDer<'static>>) -> Self {
        Self::Trusting(cert_chain)
    }

    /// Convenience constructor for [`ClientTls::InsecureSkipVerification`].
    #[must_use]
    pub fn insecure_skip_verification() -> Self {
        Self::InsecureSkipVerification
    }

    fn into_client_config(self) -> ClientResult<ClientConfig> {
        let mut rustls_config = match self {
            Self::WebPkiRoots => {
                let mut roots = rustls::RootCertStore::empty();
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth()
            }
            Self::Trusting(chain) => {
                let mut roots = rustls::RootCertStore::empty();
                for cert in chain {
                    roots
                        .add(cert)
                        .map_err(|e| ClientError::Config(e.to_string()))?;
                }
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth()
            }
            Self::InsecureSkipVerification => rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify))
                .with_no_client_auth(),
        };
        rustls_config.alpn_protocols = vec![CITADEL_ALPN.to_vec()];
        let quic = QuicClientConfig::try_from(rustls_config)
            .map_err(|e| ClientError::Config(e.to_string()))?;
        Ok(ClientConfig::new(Arc::new(quic)))
    }
}

/// A connected QUIC client.
pub struct QuicClient {
    connection: Connection,
    #[allow(dead_code)]
    endpoint: Endpoint,
    /// Monotonic source of RPC correlation ids for [`QuicClient::call_rpc`].
    next_request_id: u64,
}

/// Cap the QUIC handshake so a dead or unreachable server fails fast instead of
/// hanging the caller. The FFI runs `connect` on `block_on`, so without this a
/// caller on a game thread (e.g. Unreal `BeginPlay`) would freeze until quinn's
/// idle timeout. Fully non-blocking connect (a pollable `Connecting` state) is a
/// follow-up; this just bounds the worst case.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

impl QuicClient {
    /// Connect to a Citadel QUIC endpoint at `server_addr` with the given
    /// server name (used for SNI/verification) and TLS strategy. The handshake
    /// is bounded by [`CONNECT_TIMEOUT`] so an unreachable server errors quickly.
    pub async fn connect(
        server_addr: SocketAddr,
        server_name: &str,
        tls: ClientTls,
    ) -> ClientResult<Self> {
        let bind: SocketAddr = "0.0.0.0:0"
            .parse()
            .map_err(|_| ClientError::Config("invalid client bind address".to_string()))?;
        let mut endpoint =
            Endpoint::client(bind).map_err(|e| ClientError::Connect(e.to_string()))?;
        endpoint.set_default_client_config(tls.into_client_config()?);

        let connecting = endpoint
            .connect(server_addr, server_name)
            .map_err(|e| ClientError::Connect(e.to_string()))?;
        let connection = tokio::time::timeout(CONNECT_TIMEOUT, connecting)
            .await
            .map_err(|_| ClientError::Connect("connection attempt timed out".to_string()))?
            .map_err(|e| ClientError::Connect(e.to_string()))?;
        Ok(Self {
            connection,
            endpoint,
            next_request_id: 1,
        })
    }

    /// Send an envelope reliably over a fresh unidirectional stream
    /// (fire-and-forget).
    ///
    /// The realtime gateway routes inbound reliable streams to other sessions
    /// (it does not echo on the request stream), so this does not wait for a
    /// reply. Relayed reliable messages from peers arrive via [`Self::recv_uni`].
    pub async fn send_reliable(&self, env: &Envelope) -> ClientResult<()> {
        let mut send = self
            .connection
            .open_uni()
            .await
            .map_err(|e| ClientError::Send(e.to_string()))?;
        send.write_all(&env.encode_framed())
            .await
            .map_err(|e| ClientError::Send(e.to_string()))?;
        send.finish()
            .map_err(|e| ClientError::Send(e.to_string()))?;
        Ok(())
    }

    /// Send an envelope as an unreliable datagram.
    pub fn send_unreliable(&self, env: &Envelope) -> ClientResult<()> {
        self.connection
            .send_datagram(env.encode_datagram())
            .map_err(|e| ClientError::Send(e.to_string()))
    }

    /// Receive the next datagram envelope from the server (e.g. a relayed peer
    /// position).
    pub async fn recv_datagram(&self) -> ClientResult<Envelope> {
        let bytes = self
            .connection
            .read_datagram()
            .await
            .map_err(|e| ClientError::Receive(e.to_string()))?;
        Ok(decode_datagram(&bytes)?)
    }

    /// Accept the next server-opened unidirectional stream and read the framed
    /// envelopes it carries (e.g. relayed reliable peer messages).
    pub async fn recv_uni(&self) -> ClientResult<Vec<Envelope>> {
        let mut recv = self
            .connection
            .accept_uni()
            .await
            .map_err(|e| ClientError::Receive(e.to_string()))?;
        let data = recv
            .read_to_end(MAX_STREAM_BYTES)
            .await
            .map_err(|e| ClientError::Receive(e.to_string()))?;
        let mut buf = BytesMut::from(&data[..]);
        let mut out = Vec::new();
        while let Some(e) = decode_framed(&mut buf)? {
            out.push(e);
        }
        Ok(out)
    }

    /// Call a server-side RPC method and await its correlated reply.
    ///
    /// Sends a [`KIND_RPC_REQUEST`] reliably (fresh, monotonically increasing
    /// `request_id`), then accepts server-opened unidirectional streams until the
    /// matching RPC response arrives. Returns the handler's reply bytes
    /// on success, or [`ClientError::Rpc`] if the server answered with an error
    /// status.
    ///
    /// Correlation and usage notes mirror [`crate::WsClient::call_rpc`]: every
    /// envelope other than the matching response is synchronously delivered to
    /// `on_envelope`. The mutable borrow makes this the sole inbound poll owner.
    pub async fn call_rpc<F>(
        &mut self,
        method: &str,
        payload: &[u8],
        mut on_envelope: F,
    ) -> ClientResult<Vec<u8>>
    where
        F: FnMut(Envelope) -> ClientResult<()>,
    {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);

        let body = encode_rpc_request(request_id, method, payload);
        self.send_reliable(&Envelope::new(KIND_RPC_REQUEST, body))
            .await?;
        let mut pump = RpcResponsePump::new(request_id, &mut on_envelope);

        loop {
            if let Some(reply) = pump.handle_batch(self.recv_uni().await?)? {
                return Ok(reply);
            }
        }
    }

    /// Close the connection.
    pub fn close(&self) {
        self.connection.close(0u32.into(), b"client done");
    }
}

/// A certificate verifier that accepts any certificate. Dev/test only.
#[derive(Debug)]
struct NoVerify;

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_root_tls_config_builds() {
        ClientTls::webpki_roots()
            .into_client_config()
            .expect("public CA roots build a QUIC TLS config");
    }
}
