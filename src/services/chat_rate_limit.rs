//! Repository-owned multi-key chat rate-limit policy.
//!
//! Keys are SHA-256 digests of the action and trusted identity/channel values,
//! so neither the durable counter table nor maintenance telemetry reveal social
//! graph identifiers. The repository consumes an entire plan atomically.

use sha2::{Digest, Sha256};

use crate::config::{ChatLimitsConfig, ChatRateLimitRule};
use crate::repository::ChatRateLimit;

/// Builds the configured multi-key fixed-window plans for secure chat actions.
#[derive(Debug, Clone)]
pub struct ChatRateLimitPolicy {
    limits: ChatLimitsConfig,
}

impl Default for ChatRateLimitPolicy {
    fn default() -> Self {
        Self::new(ChatLimitsConfig::default())
    }
}

impl ChatRateLimitPolicy {
    /// Use the fully validated server configuration.
    #[must_use]
    pub const fn new(limits: ChatLimitsConfig) -> Self {
        Self { limits }
    }

    /// Limit a target authorization/join attempt by user and globally by action.
    #[must_use]
    pub fn join(&self, user_id: &str) -> Vec<ChatRateLimit> {
        vec![rule("join:user", &[user_id], self.limits.join)]
    }

    /// Limit a history read by user and globally by action.
    #[must_use]
    pub fn history(&self, user_id: &str) -> Vec<ChatRateLimit> {
        vec![rule("history:user", &[user_id], self.limits.history)]
    }

    /// Limit send by user, user/channel pair, and channel.
    #[must_use]
    pub fn send(&self, user_id: &str, channel_id: &str) -> Vec<ChatRateLimit> {
        vec![
            rule("send:user", &[user_id], self.limits.send_user),
            rule(
                "send:user-channel",
                &[user_id, channel_id],
                self.limits.send_user_channel,
            ),
            rule("send:channel", &[channel_id], self.limits.send_channel),
        ]
    }

    /// Limit author edits and deletes by user and user/channel pair.
    #[must_use]
    pub fn mutation(&self, user_id: &str, channel_id: &str) -> Vec<ChatRateLimit> {
        vec![
            rule("mutation:user", &[user_id], self.limits.mutation_user),
            rule(
                "mutation:user-channel",
                &[user_id, channel_id],
                self.limits.mutation_user_channel,
            ),
        ]
    }

    /// Limit operator moderation by operator and target channel.
    #[must_use]
    pub fn moderation(&self, operator_id: &str, channel_id: &str) -> Vec<ChatRateLimit> {
        vec![
            rule(
                "moderation:operator",
                &[operator_id],
                self.limits.moderation_operator,
            ),
            rule(
                "moderation:channel",
                &[channel_id],
                self.limits.moderation_channel,
            ),
        ]
    }
}

fn rule(scope: &str, values: &[&str], policy: ChatRateLimitRule) -> ChatRateLimit {
    let mut digest = Sha256::new();
    digest.update(scope.as_bytes());
    for value in values {
        digest.update([0]);
        digest.update(value.as_bytes());
    }
    let digest = digest.finalize();
    ChatRateLimit {
        key: format!("chatrl:{scope}:{digest:x}"),
        limit: policy.limit,
        window_ms: policy.window_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_are_multi_key_and_never_embed_raw_identifiers() {
        let policy = ChatRateLimitPolicy::default();
        let plan = policy.send("alice", "ch_secret");
        assert_eq!(plan.len(), 3);
        assert!(plan.iter().all(|rule| !rule.key.contains("alice")));
        assert!(plan.iter().all(|rule| !rule.key.contains("ch_secret")));
    }
}
