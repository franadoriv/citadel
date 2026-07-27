//! Token issuance seam for the reference session service.
//!
//! [`TokenIssuer`] is the boundary where a session's bearer tokens are minted.
//! Real issuance signs a token that encodes the session claims so a verifier can
//! recover the session without storing the secret; that is deferred (see
//! `src/session/token.rs`). This task provides [`CountingTokenIssuer`], a
//! deterministic, process-unique issuer good enough for the in-memory reference
//! service and its contract tests. It is explicitly **not** a secure token: it
//! is unsigned and predictable, and must not be used in production.

use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::error::{AppError, AppResult};
use crate::session::{IssuedSessionTokens, SessionTokenRef, SessionTokenSecret};

/// A freshly minted token set: the secret tokens plus a non-secret handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedTokenSet {
    /// Access (and optional refresh) secrets. Never log these.
    pub tokens: IssuedSessionTokens,
    /// Non-secret handle stored on the session for lookup.
    pub token_ref: SessionTokenRef,
}

/// Mints bearer tokens for new and refreshed sessions.
pub trait TokenIssuer: Send + Sync {
    /// Mint an access token, an optional refresh token (when `refreshable`), and
    /// a non-secret token reference.
    ///
    /// # Errors
    /// Returns a validation error only if a generated value violates a token
    /// invariant (not expected for the built-in issuer).
    fn issue(&self, refreshable: bool) -> AppResult<IssuedTokenSet>;
}

/// A deterministic, process-unique [`TokenIssuer`] backed by a counter.
///
/// Tokens are of the form `cit.access.<n>` / `cit.refresh.<n>` and references
/// `tref-<n>`, where `n` is a monotonically increasing counter. Uniqueness (not
/// secrecy) is the guarantee; do not use outside tests and local development.
#[derive(Debug, Default)]
pub struct CountingTokenIssuer {
    next: AtomicU64,
}

impl CountingTokenIssuer {
    /// Create a fresh issuer starting at 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }
}

impl TokenIssuer for CountingTokenIssuer {
    fn issue(&self, refreshable: bool) -> AppResult<IssuedTokenSet> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let access = SessionTokenSecret::new(format!("cit.access.{n}"))?;
        let refresh = if refreshable {
            Some(SessionTokenSecret::new(format!("cit.refresh.{n}"))?)
        } else {
            None
        };
        let token_ref = SessionTokenRef::new(format!("tref-{n}"))?;
        Ok(IssuedTokenSet {
            tokens: IssuedSessionTokens { access, refresh },
            token_ref,
        })
    }
}

/// A cryptographically secure [`TokenIssuer`] backed by the operating system's
/// CSPRNG (`getrandom`).
///
/// Every access and refresh token carries **256 bits** of fresh OS entropy,
/// base64url-encoded behind a readable `cit.access.` / `cit.refresh.` prefix, so
/// a token cannot be guessed, enumerated, or forged without the secret bytes —
/// closing the predictable-token session-hijack risk of [`CountingTokenIssuer`]
///. The non-secret `token_ref` is an independent 128-bit random
/// handle, so it leaks nothing about the secret it accompanies.
///
/// This is the issuer wired into the running node; [`CountingTokenIssuer`] stays
/// for deterministic contract tests only.
#[derive(Debug, Default)]
pub struct RandomTokenIssuer;

impl RandomTokenIssuer {
    /// Create a secure issuer. It is stateless — entropy comes from the OS per
    /// call, so no seeding or counter is kept.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Draw `n_bytes` of OS CSPRNG entropy and return them URL-safe base64url.
    fn random_b64(n_bytes: usize) -> AppResult<String> {
        let mut buf = vec![0u8; n_bytes];
        getrandom::fill(&mut buf).map_err(|e| {
            AppError::internal(format!("CSPRNG unavailable for token issuance: {e}"))
        })?;
        Ok(URL_SAFE_NO_PAD.encode(&buf))
    }
}

impl TokenIssuer for RandomTokenIssuer {
    fn issue(&self, refreshable: bool) -> AppResult<IssuedTokenSet> {
        let access = SessionTokenSecret::new(format!("cit.access.{}", Self::random_b64(32)?))?;
        let refresh = if refreshable {
            Some(SessionTokenSecret::new(format!(
                "cit.refresh.{}",
                Self::random_b64(32)?
            ))?)
        } else {
            None
        };
        let token_ref = SessionTokenRef::new(format!("tref-{}", Self::random_b64(16)?))?;
        Ok(IssuedTokenSet {
            tokens: IssuedSessionTokens { access, refresh },
            token_ref,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issues_unique_refreshable_tokens() {
        let issuer = CountingTokenIssuer::new();
        let a = issuer.issue(true).expect("issue a");
        let b = issuer.issue(true).expect("issue b");
        assert!(a.tokens.refresh.is_some());
        assert_ne!(
            a.tokens.access.expose_secret(),
            b.tokens.access.expose_secret()
        );
        assert_ne!(a.token_ref.as_str(), b.token_ref.as_str());
    }

    #[test]
    fn non_refreshable_omits_refresh_secret() {
        let issuer = CountingTokenIssuer::new();
        let set = issuer.issue(false).expect("issue");
        assert!(set.tokens.refresh.is_none());
    }

    #[test]
    fn random_issuer_tokens_are_high_entropy_and_unguessable() {
        let issuer = RandomTokenIssuer::new();
        let a = issuer.issue(true).expect("issue a");
        let b = issuer.issue(true).expect("issue b");

        let a_access = a.tokens.access.expose_secret();
        let b_access = b.tokens.access.expose_secret();

        // Prefixed for readability but the random tail is what matters.
        assert!(a_access.starts_with("cit.access."));
        // 32 random bytes base64url (no pad) = 43 chars, plus the prefix.
        let tail = a_access.trim_start_matches("cit.access.");
        assert_eq!(tail.len(), 43, "expected 256 bits of base64url entropy");

        // No counter correlation: distinct issues differ, and (unlike the
        // counting issuer) neither the access nor the ref is derivable from the
        // other or from a sequence.
        assert_ne!(a_access, b_access);
        assert_ne!(
            a.tokens.refresh.as_ref().map(|r| r.expose_secret()),
            b.tokens.refresh.as_ref().map(|r| r.expose_secret())
        );
        assert_ne!(a.token_ref.as_str(), b.token_ref.as_str());
        // The non-secret ref must not reveal the secret.
        assert!(!a.token_ref.as_str().contains(tail));
    }

    #[test]
    fn random_issuer_respects_refreshable_flag() {
        let issuer = RandomTokenIssuer::new();
        assert!(issuer.issue(true).expect("issue").tokens.refresh.is_some());
        assert!(issuer.issue(false).expect("issue").tokens.refresh.is_none());
    }

    #[test]
    fn random_issuer_has_no_collisions_across_many_issues() {
        use std::collections::HashSet;
        let issuer = RandomTokenIssuer::new();
        let mut seen = HashSet::new();
        for _ in 0..1_000 {
            let set = issuer.issue(true).expect("issue");
            assert!(
                seen.insert(set.tokens.access.expose_secret().to_owned()),
                "CSPRNG token collision"
            );
        }
    }
}
