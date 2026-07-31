//! Service boundary contracts for Citadel.
//!
//! Bootstrap scope: this module introduces the shared
//! [`ServiceLifecycle`] trait and the [`Health`] reporting type used by the
//! bootstrap layer and the (future) health endpoint. Concrete services such as
//! identity, sessions, realtime gateway, router, runtime host, and repositories
//! are defined by their own tasks per `docs/architecture/service-boundaries.md`.
//!
//! Identity/session scope (, async in ): this module also
//! declares the [`AuthenticationService`], [`SessionService`], and
//! [`SessionDirectory`] contracts. They are async (via `async-trait`) and
//! object-safe and depend only on the identity/session domain types, never on a
//! concrete storage or transport. [`ServiceLifecycle`] stays synchronous: it
//! reports name/health without touching a backend.

pub mod audit;
pub mod auth_rate_limit;
pub mod authentication;
pub mod chat;
pub mod chat_access;
pub mod chat_authorization;
pub mod chat_rate_limit;
pub mod console;
pub mod database_explorer_rate_limit;
pub mod directory;
pub mod friends;
pub mod groups;
pub mod leaderboards;
pub mod matchmaker_directory;
pub mod memory;
pub mod notifications;
pub mod party_directory;
pub mod player_notifications;
pub mod purchases;
pub mod session;
pub mod token;
pub mod wallet;

pub use audit::{AuditEntry, AuditFilter, AuditLog, DEFAULT_AUDIT_CAPACITY};
pub use chat::{
    ChannelSummary, ChannelType, ChatDeliveryRequest, ChatMessage, ChatService,
    DEFAULT_AUTHOR_DELETE_WINDOW_MS, DEFAULT_AUTHOR_EDIT_WINDOW_MS, DEFAULT_CHANNEL_HISTORY_CAP,
    MAX_CHAT_CONTENT_BYTES, validate_chat_content,
};
pub use chat_access::ChatAccessCoordinator;
pub use chat_authorization::{
    AuthorizedChatChannel, AuthorizedChatLease, ChatChannelAuthorizer, ChatTarget,
};
pub use chat_rate_limit::ChatRateLimitPolicy;
pub use console::{ConsoleIdentity, ConsoleRole, ConsoleTokenStore, verify_login};
pub use friends::{FriendRow, FriendState, FriendsService};
pub use groups::{
    AdmissionKind, AdmissionOutcome, CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole,
    GroupsPage, GroupsService, Membership, PendingAdmission, UpdateGroupRequest,
};
pub use leaderboards::{
    CreateLeaderboardRequest, LeaderboardDefinition, LeaderboardRecord, LeaderboardService,
    LeaderboardSummary, Operator, RankedRecord, RecordsPage, SortOrder,
};
pub use notifications::{
    DEFAULT_NOTIFICATION_CAPACITY, Notification, NotificationPage, NotificationService, Recipient,
};
pub use player_notifications::{
    PlayerNotification, PlayerNotificationDelivery, PlayerNotificationPage,
    PlayerNotificationService, SendPlayerNotification, SendPlayerNotificationOutcome,
};
pub use purchases::{
    DevReceiptValidator, Purchase, PurchaseService, PurchaseStore, ReceiptValidator,
    SubscriptionRow, ValidatedReceipt,
};
pub use wallet::{LedgerEntry, WalletService};

pub use auth_rate_limit::AuthenticationRateLimitPolicy;
pub use authentication::{
    AuthenticationOptions, AuthenticationOutcome, AuthenticationService,
    CustomAuthenticationRequest, DeviceAuthenticationRequest, EmailAuthenticationRequest,
};
pub use database_explorer_rate_limit::DatabaseExplorerRateLimiter;
pub use directory::InMemorySessionDirectory;
pub use memory::{
    AuthenticationServiceImpl, InMemorySessionService, SharedAuthIdentityRepository,
    SharedSessionRepository, SharedSessionService, SharedUserRepository,
};
pub use session::{
    CreateSessionRequest, CreatedSession, RefreshSessionRequest, RevokeSessionRequest,
    SessionDirectory, SessionService, ValidateSessionRequest,
};
pub use token::{CountingTokenIssuer, IssuedTokenSet, RandomTokenIssuer, TokenIssuer};

/// Coarse health state for a service or the assembled application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Service is fully operational.
    Healthy,
    /// Service is up but operating in a reduced capacity.
    Degraded,
    /// Service is not able to serve requests.
    Unhealthy,
}

impl Health {
    /// Stable lowercase string used in health responses and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    /// Whether this state is considered acceptable for serving traffic.
    ///
    /// `Healthy` and `Degraded` are serviceable; `Unhealthy` is not.
    #[must_use]
    pub const fn is_serviceable(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

/// Lifecycle contract for Citadel services.
///
/// The bootstrap layer owns ordered startup, health aggregation, and graceful
/// shutdown. Implementations should be cancellation-aware once async startup
/// and shutdown wiring lands in the HTTP/server bootstrap task; for now the
/// contract is intentionally synchronous and side-effect free so it can be
/// unit tested without a runtime.
pub trait ServiceLifecycle {
    /// Stable, human-readable name used in logs and health reports.
    fn name(&self) -> &str;

    /// Current health of this service.
    ///
    /// The default reports [`Health::Healthy`]; services with real readiness
    /// signals override this.
    fn health(&self) -> Health {
        Health::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubService {
        name: String,
        health: Health,
    }

    impl ServiceLifecycle for StubService {
        fn name(&self) -> &str {
            &self.name
        }

        fn health(&self) -> Health {
            self.health
        }
    }

    #[test]
    fn health_strings_are_stable() {
        assert_eq!(Health::Healthy.as_str(), "healthy");
        assert_eq!(Health::Degraded.as_str(), "degraded");
        assert_eq!(Health::Unhealthy.as_str(), "unhealthy");
    }

    #[test]
    fn serviceable_states_exclude_unhealthy() {
        assert!(Health::Healthy.is_serviceable());
        assert!(Health::Degraded.is_serviceable());
        assert!(!Health::Unhealthy.is_serviceable());
    }

    #[test]
    fn default_health_is_healthy() {
        struct Minimal;
        impl ServiceLifecycle for Minimal {
            fn name(&self) -> &str {
                "minimal"
            }
        }
        assert_eq!(Minimal.health(), Health::Healthy);
    }

    #[test]
    fn lifecycle_reports_name_and_health() {
        let svc = StubService {
            name: "stub".to_string(),
            health: Health::Degraded,
        };
        assert_eq!(svc.name(), "stub");
        assert_eq!(svc.health(), Health::Degraded);
    }
}
