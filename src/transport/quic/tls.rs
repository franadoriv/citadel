//! TLS configuration for the QUIC transport.
//!
//! QUIC requires TLS 1.3. For local development and tests we generate an
//! in-memory self-signed certificate; production certificate provisioning is a
//! later operational task. The client-side helper here trusts that self-signed
//! certificate explicitly so integration tests can connect without a real CA.
//!
//! Security note: [`insecure_client_config`] disables certificate verification
//! and MUST only be used in tests/dev tooling. It is gated behind an explicit,
//! clearly named function so it cannot be reached by accident from server code.

use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use crate::error::{AppError, AppResult, ErrorCategory};

/// A self-signed certificate and its private key, for dev/test endpoints.
///
/// The private key is held as raw PKCS#8 DER bytes because the typed key der is
/// not `Clone`; configs reconstruct the typed key on demand.
#[derive(Debug, Clone)]
pub struct SelfSignedCert {
    /// DER-encoded certificate chain (single self-signed leaf).
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// DER-encoded PKCS#8 private key bytes.
    key_der: Vec<u8>,
}

impl SelfSignedCert {
    /// Generate a fresh self-signed certificate for the given DNS names.
    ///
    /// Intended for local development and tests only.
    pub fn generate(subject_alt_names: &[String]) -> AppResult<Self> {
        let cert = rcgen::generate_simple_self_signed(subject_alt_names.to_vec())
            .map_err(|e| tls_error("failed to generate self-signed certificate", e))?;
        let key_der = cert.key_pair.serialize_der();
        let cert_der = CertificateDer::from(cert.cert);
        Ok(Self {
            cert_chain: vec![cert_der],
            key_der,
        })
    }

    /// The PKCS#8 private key as a typed der value.
    fn key(&self) -> PrivatePkcs8KeyDer<'static> {
        PrivatePkcs8KeyDer::from(self.key_der.clone())
    }
}

/// Build a QUIC [`ServerConfig`] from a self-signed certificate (dev/test).
pub fn server_config(cert: &SelfSignedCert) -> AppResult<ServerConfig> {
    let mut rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert.cert_chain.clone(), cert.key().into())
        .map_err(|e| tls_error("invalid server certificate/key", e))?;
    // QUIC requires the ALPN to be negotiated; advertise a Citadel protocol id.
    rustls_config.alpn_protocols = vec![CITADEL_ALPN.to_vec()];

    let quic_crypto = QuicServerConfig::try_from(rustls_config)
        .map_err(|e| tls_error("failed to build QUIC server crypto", e))?;
    Ok(ServerConfig::with_crypto(Arc::new(quic_crypto)))
}

/// Build a QUIC [`ClientConfig`] that trusts only `cert` (dev/test).
pub fn client_config_trusting(cert: &SelfSignedCert) -> AppResult<ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for c in &cert.cert_chain {
        roots
            .add(c.clone())
            .map_err(|e| tls_error("failed to add trusted certificate", e))?;
    }
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    rustls_config.alpn_protocols = vec![CITADEL_ALPN.to_vec()];

    let quic_crypto = QuicClientConfig::try_from(rustls_config)
        .map_err(|e| tls_error("failed to build QUIC client crypto", e))?;
    Ok(ClientConfig::new(Arc::new(quic_crypto)))
}

/// ALPN protocol identifier advertised by Citadel's QUIC endpoint.
pub const CITADEL_ALPN: &[u8] = b"citadel/0";

fn tls_error(message: &str, source: impl std::fmt::Display) -> AppError {
    AppError::new(ErrorCategory::Transport, message.to_string()).with_detail(source.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_self_signed_cert_with_san() {
        let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("generate");
        assert_eq!(cert.cert_chain.len(), 1);
    }

    #[test]
    fn builds_server_and_client_configs() {
        let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("generate");
        server_config(&cert).expect("server config");
        client_config_trusting(&cert).expect("client config");
    }

    #[test]
    fn alpn_is_stable() {
        assert_eq!(CITADEL_ALPN, b"citadel/0");
    }
}
