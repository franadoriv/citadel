//! Development certificate for the WebTransport endpoint.
//!
//! Browsers accept a self-signed WebTransport server certificate only when it
//! is passed via `serverCertificateHashes` and meets strict rules: an ECDSA
//! P-256 key and a validity window of at most 14 days. `rcgen`'s default key is
//! ECDSA P-256; this module sets a short validity window and exposes the cert's
//! SHA-256 hash (hex and base64) so the browser demo can pin it.
//!
//! This is for local development only; production certificate provisioning is a
//! separate operational task.

use rcgen::{CertificateParams, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult, ErrorCategory};

/// Validity window for the dev cert, in days. Must stay <= 14 for browsers.
const VALIDITY_DAYS: i64 = 13;

/// A short-lived ECDSA P-256 self-signed certificate for WebTransport dev use.
#[derive(Debug, Clone)]
pub struct WebTransportDevCert {
    /// DER-encoded certificate chain (single self-signed leaf).
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// DER-encoded PKCS#8 private key bytes.
    key_der: Vec<u8>,
    /// SHA-256 digest of the leaf certificate DER.
    cert_sha256: [u8; 32],
}

impl WebTransportDevCert {
    /// Generate a fresh dev certificate for the given DNS names.
    pub fn generate(subject_alt_names: &[String]) -> AppResult<Self> {
        let key = KeyPair::generate()
            .map_err(|e| cert_err("failed to generate WebTransport key pair", e))?;
        let mut params = CertificateParams::new(subject_alt_names.to_vec())
            .map_err(|e| cert_err("invalid WebTransport certificate params", e))?;
        // Short validity window required by browsers for serverCertificateHashes.
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::hours(1);
        params.not_after = now + time::Duration::days(VALIDITY_DAYS);

        let cert = params
            .self_signed(&key)
            .map_err(|e| cert_err("failed to self-sign WebTransport certificate", e))?;
        let cert_der = cert.der().clone();
        let cert_sha256 = Sha256::digest(cert_der.as_ref()).into();

        Ok(Self {
            cert_chain: vec![cert_der],
            key_der: key.serialize_der(),
            cert_sha256,
        })
    }

    /// The PKCS#8 private key as a typed der value.
    pub fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()))
    }

    /// The leaf certificate SHA-256 digest.
    #[must_use]
    pub fn cert_sha256(&self) -> [u8; 32] {
        self.cert_sha256
    }

    /// The leaf certificate SHA-256 as lowercase hex (e.g. for logs).
    #[must_use]
    pub fn cert_sha256_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.cert_sha256 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// The leaf certificate SHA-256 as standard base64 (for the browser
    /// `serverCertificateHashes` value).
    #[must_use]
    pub fn cert_sha256_base64(&self) -> String {
        base64_standard(&self.cert_sha256)
    }
}

fn cert_err(message: &str, source: impl std::fmt::Display) -> AppError {
    AppError::new(ErrorCategory::Transport, message.to_string()).with_detail(source.to_string())
}

/// Minimal standard-alphabet base64 (no extra dependency) for a 32-byte digest.
fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_cert_with_chain_and_key() {
        let cert = WebTransportDevCert::generate(&["localhost".to_string()]).expect("generate");
        assert_eq!(cert.cert_chain.len(), 1);
        // Key der is non-empty PKCS#8.
        assert!(!cert.key_der.is_empty());
    }

    #[test]
    fn exposes_sha256_in_hex_and_base64() {
        let cert = WebTransportDevCert::generate(&["localhost".to_string()]).expect("generate");
        let hex = cert.cert_sha256_hex();
        assert_eq!(hex.len(), 64, "hex digest is 64 chars");
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        let b64 = cert.cert_sha256_base64();
        // 32 bytes -> 44 base64 chars (with one '=' pad).
        assert_eq!(b64.len(), 44);
        assert!(b64.ends_with('='));
    }

    #[test]
    fn base64_matches_known_vector() {
        // base64("foobar") == "Zm9vYmFy"
        assert_eq!(base64_standard(b"foobar"), "Zm9vYmFy");
        // base64("foo") == "Zm9v"; base64("fo") == "Zm8="
        assert_eq!(base64_standard(b"foo"), "Zm9v");
        assert_eq!(base64_standard(b"fo"), "Zm8=");
        assert_eq!(base64_standard(b"f"), "Zg==");
    }
}
