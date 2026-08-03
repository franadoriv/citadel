//! Durable, privacy-preserving authentication admission controls.
//!
//! Plans use the existing atomic fixed-window repository primitive shared by
//! SQLite, Postgres, and CockroachDB. Keys are SHA-256 digests, so the durable
//! counter state never contains a raw email address or peer network address.

use sha2::{Digest, Sha256};

use crate::config::{AuthLimitsConfig, AuthRateLimitRule};
use crate::repository::ChatRateLimit;

/// Builds the multi-key admission plan for one public auth request.
#[derive(Debug, Clone)]
pub struct AuthenticationRateLimitPolicy {
    limits: AuthLimitsConfig,
}

impl AuthenticationRateLimitPolicy {
    /// Use the fully validated server configuration.
    #[must_use]
    pub const fn new(limits: AuthLimitsConfig) -> Self {
        Self { limits }
    }

    /// Limit device/custom authentication by direct peer address only.
    #[must_use]
    pub fn opaque_credential(&self, source: &str, registration: bool) -> Vec<ChatRateLimit> {
        let mut plan = vec![rule("source", &[source], self.limits.source)];
        if registration {
            plan.push(rule(
                "registration-source",
                &[source],
                self.limits.registration_source,
            ));
        }
        plan
    }

    /// Limit console operator login by both source and presented username.
    ///
    /// The operator surface has no KDF and a small credential space, so it is
    /// held to the stricter of the configured windows rather than the general
    /// per-source one: the username key throttles a distributed campaign against
    /// a single operator account, and the source key throttles one host walking
    /// a password list.
    #[must_use]
    pub fn console_login(&self, source: &str, username: &str) -> Vec<ChatRateLimit> {
        vec![
            rule("console-source", &[source], self.limits.console_login),
            rule("console-user", &[username], self.limits.console_login),
        ]
    }

    /// Limit email authentication by both source and normalized email. The
    /// email key works across a distributed password-guessing campaign.
    #[must_use]
    pub fn email(&self, source: &str, email: &str, registration: bool) -> Vec<ChatRateLimit> {
        let mut plan = vec![
            rule("source", &[source], self.limits.source),
            rule("email", &[email], self.limits.email),
        ];
        if registration {
            plan.push(rule(
                "registration-source",
                &[source],
                self.limits.registration_source,
            ));
        }
        plan
    }
}

fn rule(scope: &str, values: &[&str], policy: AuthRateLimitRule) -> ChatRateLimit {
    let mut digest = Sha256::new();
    digest.update(scope.as_bytes());
    for value in values {
        digest.update([0]);
        digest.update(value.as_bytes());
    }
    ChatRateLimit {
        key: format!("authrl:{scope}:{:x}", digest.finalize()),
        limit: policy.limit,
        window_ms: policy.window_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_plan_is_multi_key_and_redacts_inputs() {
        let policy = AuthenticationRateLimitPolicy::new(AuthLimitsConfig::default());
        let plan = policy.email("192.0.2.1", "ada@example.com", true);
        assert_eq!(plan.len(), 3);
        assert!(plan.iter().all(|rule| !rule.key.contains("192.0.2.1")));
        assert!(
            plan.iter()
                .all(|rule| !rule.key.contains("ada@example.com"))
        );
        assert!(plan.iter().all(|rule| rule.key.starts_with("authrl:")));
    }
}
