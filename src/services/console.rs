//! Admin console authentication service.
//!
//! The console is an operator surface, not a player surface, so it does not
//! reuse the player identity/session stack. Operators log in with the static
//! credentials from [`ConsoleConfig`] and receive an opaque bearer token held
//! in an in-process [`ConsoleTokenStore`] with a fixed expiry. Tokens never
//! persist: a node restart logs every operator out, which is the safe default
//! for an admin surface.
//!
//! Roles are coarse by design: `admin` (full access, may mutate) and `viewer`
//! (read-only). Which configured password matched decides the role.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::ConsoleConfig;
use crate::error::{AppError, AppResult};

/// Entropy drawn per console bearer token. 256 bits matches the player session
/// tokens issued by [`RandomTokenIssuer`](crate::services::token).
const TOKEN_ENTROPY_BYTES: usize = 32;

/// Access level of an authenticated console operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsoleRole {
    /// Full access: may read every section and perform mutations.
    Admin,
    /// Read-only access: mutations are rejected with a permission error.
    Viewer,
}

impl ConsoleRole {
    /// Stable lowercase token used in responses and audit entries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }
}

/// An authenticated console operator, resolved from a bearer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleIdentity {
    /// The configured operator username the token was issued to.
    pub username: String,
    /// The operator's access level.
    pub role: ConsoleRole,
}

impl ConsoleIdentity {
    /// Require the `admin` role for a mutation.
    ///
    /// # Errors
    /// Returns a [`Permission`](crate::error::ErrorCategory::Permission) error
    /// (HTTP 403 at the boundary) when the operator is a viewer.
    pub fn require_admin(&self) -> AppResult<()> {
        match self.role {
            ConsoleRole::Admin => Ok(()),
            ConsoleRole::Viewer => Err(AppError::permission(
                "the viewer role cannot perform console mutations",
            )),
        }
    }
}

/// Check the presented credentials against the console configuration.
///
/// The admin password is checked before the viewer password, and both compares
/// are constant-time, so a response-timing observer cannot recover either
/// password byte-by-byte. Returns `None` on any mismatch — callers map that to
/// the uniform auth failure (no credential oracle).
#[must_use]
pub fn verify_login(config: &ConsoleConfig, username: &str, password: &str) -> Option<ConsoleRole> {
    if !constant_time_eq(username.as_bytes(), config.username.as_bytes()) {
        return None;
    }
    if constant_time_eq(password.as_bytes(), config.password.as_bytes()) {
        return Some(ConsoleRole::Admin);
    }
    if let Some(viewer) = &config.viewer_password
        && constant_time_eq(password.as_bytes(), viewer.as_bytes())
    {
        return Some(ConsoleRole::Viewer);
    }
    None
}

/// Constant-time byte-slice equality.
///
/// Folds XOR over every byte so a mismatch never short-circuits; only the
/// length can influence timing, which is not secret here (config-side lengths
/// are the operator's own choice).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// One issued token: who it authenticates and when it stops working.
struct TokenEntry {
    identity: ConsoleIdentity,
    expires_at: Instant,
}

/// In-process store of issued console bearer tokens.
///
/// Tokens are opaque 256-bit hex strings (see [`random_token`]) mapped to the
/// operator identity plus a monotonic expiry deadline. Expired entries are
/// purged lazily on issue so an abandoned node does not accumulate them.
pub struct ConsoleTokenStore {
    ttl: Duration,
    tokens: Mutex<HashMap<String, TokenEntry>>,
}

impl std::fmt::Debug for ConsoleTokenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render token strings.
        f.debug_struct("ConsoleTokenStore")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl ConsoleTokenStore {
    /// Create a store issuing tokens valid for `ttl`.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            tokens: Mutex::new(HashMap::new()),
        }
    }

    /// Create a store from the console configuration's expiry.
    #[must_use]
    pub fn from_config(config: &ConsoleConfig) -> Self {
        Self::new(Duration::from_secs(config.token_expiry_sec))
    }

    /// The configured token lifetime.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Issue a fresh bearer token for `identity`.
    ///
    /// # Errors
    /// Returns an [`Internal`](crate::error::ErrorCategory::Internal) error when
    /// the operating system CSPRNG is unavailable. Issuing a guessable operator
    /// token instead is never an acceptable fallback.
    pub fn issue(&self, identity: ConsoleIdentity) -> AppResult<String> {
        self.issue_at(identity, Instant::now())
    }

    /// Resolve a bearer token to its identity, if present and unexpired.
    #[must_use]
    pub fn validate(&self, token: &str) -> Option<ConsoleIdentity> {
        self.validate_at(token, Instant::now())
    }

    /// Drop a token (operator logout). Returns whether it existed.
    pub fn revoke(&self, token: &str) -> bool {
        self.lock().remove(token).is_some()
    }

    /// Clock-injectable issue, the unit-testable core of [`Self::issue`].
    fn issue_at(&self, identity: ConsoleIdentity, now: Instant) -> AppResult<String> {
        let token = random_token()?;
        let mut tokens = self.lock();
        // Lazy purge keeps the map bounded by the number of live logins.
        tokens.retain(|_, entry| entry.expires_at > now);
        tokens.insert(
            token.clone(),
            TokenEntry {
                identity,
                expires_at: now + self.ttl,
            },
        );
        Ok(token)
    }

    /// Clock-injectable validate, the unit-testable core of [`Self::validate`].
    fn validate_at(&self, token: &str, now: Instant) -> Option<ConsoleIdentity> {
        let tokens = self.lock();
        let entry = tokens.get(token)?;
        (entry.expires_at > now).then(|| entry.identity.clone())
    }

    /// Lock the token map, recovering from a poisoned lock.
    ///
    /// The map holds no invariants that a panicking writer could break halfway
    /// (single-insert/remove operations), so continuing with the inner value is
    /// safe and avoids `unwrap` in production code.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, TokenEntry>> {
        self.tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Generate an unpredictable 256-bit bearer token, hex-encoded.
///
/// Entropy comes straight from the operating system CSPRNG, matching the player
/// session tokens issued by [`RandomTokenIssuer`](crate::services::token). A
/// non-cryptographic hasher is deliberately not used here: `RandomState` is
/// SipHash-1-3 seeded from a thread-local key that is reused (with a counter
/// bump) for every token the thread issues, so observing one token would leak
/// information about the others.
fn random_token() -> AppResult<String> {
    use std::fmt::Write as _;

    let mut bytes = [0u8; TOKEN_ENTROPY_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(|e| AppError::internal(format!("CSPRNG unavailable for console token: {e}")))?;
    let mut token = String::with_capacity(TOKEN_ENTROPY_BYTES * 2);
    for byte in bytes {
        // Infallible: writing to a String never fails.
        let _ = write!(token, "{byte:02x}");
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> ConsoleIdentity {
        ConsoleIdentity {
            username: "admin".to_string(),
            role: ConsoleRole::Admin,
        }
    }

    #[test]
    fn verify_login_maps_passwords_to_roles() {
        let config = ConsoleConfig {
            username: "ops".to_string(),
            password: "s3cret".to_string(),
            viewer_password: Some("lookonly".to_string()),
            token_expiry_sec: 60,
        };
        assert_eq!(
            verify_login(&config, "ops", "s3cret"),
            Some(ConsoleRole::Admin)
        );
        assert_eq!(
            verify_login(&config, "ops", "lookonly"),
            Some(ConsoleRole::Viewer)
        );
        assert_eq!(verify_login(&config, "ops", "wrong"), None);
        assert_eq!(verify_login(&config, "intruder", "s3cret"), None);
    }

    #[test]
    fn verify_login_without_viewer_password_never_grants_viewer() {
        let config = ConsoleConfig::default();
        assert_eq!(
            verify_login(&config, "admin", "password"),
            Some(ConsoleRole::Admin)
        );
        assert_eq!(verify_login(&config, "admin", ""), None);
    }

    #[test]
    fn constant_time_eq_matches_equality_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn issued_tokens_validate_until_expiry() {
        let store = ConsoleTokenStore::new(Duration::from_secs(60));
        let now = Instant::now();
        let token = store.issue_at(admin(), now).expect("CSPRNG available");
        let identity = store
            .validate_at(&token, now + Duration::from_secs(59))
            .expect("token still valid before expiry");
        assert_eq!(identity.role, ConsoleRole::Admin);
        assert!(
            store
                .validate_at(&token, now + Duration::from_secs(61))
                .is_none(),
            "token invalid after expiry"
        );
    }

    #[test]
    fn unknown_and_revoked_tokens_do_not_validate() {
        let store = ConsoleTokenStore::new(Duration::from_secs(60));
        assert!(store.validate("no-such-token").is_none());
        let token = store.issue(admin()).expect("CSPRNG available");
        assert!(store.revoke(&token));
        assert!(store.validate(&token).is_none());
        assert!(!store.revoke(&token), "second revoke is a no-op");
    }

    #[test]
    fn expired_entries_are_purged_on_issue() {
        let store = ConsoleTokenStore::new(Duration::from_secs(10));
        let now = Instant::now();
        let stale = store.issue_at(admin(), now).expect("CSPRNG available");
        // Issuing far past the first token's expiry purges it from the map.
        let _fresh = store
            .issue_at(admin(), now + Duration::from_secs(3_600))
            .expect("CSPRNG available");
        assert!(!store.lock().contains_key(&stale), "stale token purged");
    }

    #[test]
    fn tokens_are_unique_and_opaque_hex() {
        let store = ConsoleTokenStore::new(Duration::from_secs(60));
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let token = store.issue(admin()).expect("CSPRNG available");
            assert_eq!(token.len(), 64);
            assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
            assert!(seen.insert(token), "issued tokens must never repeat");
        }
    }

    #[test]
    fn viewer_is_rejected_by_require_admin() {
        let viewer = ConsoleIdentity {
            username: "admin".to_string(),
            role: ConsoleRole::Viewer,
        };
        let err = viewer.require_admin().expect_err("viewer cannot mutate");
        assert_eq!(err.category(), crate::error::ErrorCategory::Permission);
        assert!(admin().require_admin().is_ok());
    }

    #[test]
    fn debug_never_renders_tokens() {
        let store = ConsoleTokenStore::new(Duration::from_secs(60));
        let token = store.issue(admin()).expect("CSPRNG available");
        let rendered = format!("{store:?}");
        assert!(!rendered.contains(&token));
    }
}
