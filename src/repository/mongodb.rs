//! MongoDB durable-backend foundation.
//!
//! This module owns connection safety, transactional topology verification,
//! the versioned physical schema, and the durable MongoDB repository adapters.
//! Startup must never silently substitute in-memory state for a configured
//! MongoDB URL.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};
use mongodb::options::{
    ClientOptions, IndexOptions, ReadConcern, ReadPreference, ReturnDocument, SelectionCriteria,
    TransactionOptions, WriteConcern,
};
use mongodb::{Client, ClientSession, Database, IndexModel};

use crate::config::DatabaseConfig;
use crate::database_explorer::MongoMetadataExplorer;
use crate::error::{AppError, AppResult};
use crate::identity::{
    AccountState, AuthCredential, AuthIdentity, AuthProvider, CustomId, DeviceId, EmailAddress,
    PasswordVerifier, User, UserId, Username,
};
use crate::leaderboard_scheduler::{
    LeaderboardResetRepository, LeaderboardResetSnapshot, ResetEpoch, ResetOutboxRecord,
    SchedulerFencingToken, SchedulerLease,
};
use crate::repository::UserPage;
use crate::repository::backend::{Backend, BackendKind, UnitOfWork};
use crate::repository::chat::{
    ChannelSummary, ChannelType, ChatChannel, ChatDeliveryOutboxRecord, ChatDeliveryRequest,
    ChatMessage, ChatModerationAudit, ChatRateLimit, channel_not_found, finish_channel_listing,
    message_not_found, new_opaque_channel_id, serialize_delivery_event,
};
use crate::repository::gamescript::{
    AUDIT_ACTION_ACTIVATE, AUDIT_ACTION_PIN, AUDIT_ACTION_SUBMIT, AUDIT_ACTION_UNPIN,
    CreateGameScriptDraftRequest, GameScriptActivation, GameScriptAuditContext,
    GameScriptAuditRecord, GameScriptDiagnostic, GameScriptDiagnosticSeverity, GameScriptDraft,
    GameScriptLimits, GameScriptOutboxKind, GameScriptOutboxRecord, GameScriptRepository,
    GameScriptRevision, GameScriptSubmission, UpdateGameScriptDraftRequest,
    activation_audit_details, draft_not_found, gamescript_revision_content_hash,
    language_from_token, revision_not_found, submit_audit_details, validate_create_draft,
    validate_limit, validate_source,
};
use crate::repository::groups::{
    AdmissionKind, AdmissionOutcome, CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole,
    GroupsPage, Membership, UpdateGroupRequest, ensure_can_add_member, ensure_can_kick, paginate,
    plan_demote, plan_promote,
};
use crate::repository::leaderboards::{
    CreateLeaderboardRequest, LeaderboardDefinition, LeaderboardRecord, LeaderboardSummary,
    Operator, RankedRecord, RecordsPage, SortOrder, apply_submission, board_not_found,
};
use crate::repository::notifications::{
    Notification, NotificationPage, Recipient, notification_not_found, overflow_evictions,
};
use crate::repository::purchases::{
    Purchase, PurchaseStore, SubscriptionRow, duplicate_transaction, subscription_rows,
};
use crate::repository::tournaments::{
    CreateTournamentRequest, Tournament, TournamentEntry, TournamentResult,
    TournamentSettlementOutboxRecord, TournamentState, TournamentsRepository, can_transition,
    validate_schedule,
};
use crate::repository::wallet::{LedgerEntry, apply_delta, ledger_overflow};
use crate::repository::{
    AuthIdentityRepository, ChatRepository, FriendRow, FriendState, FriendsRepository,
    GroupsRepository, LeaderboardsRepository, NotificationsRepository, PurchasesRepository,
    SessionRepository, StorageRepository, UserRepository, WalletRepository, plan_add,
};
use crate::session::{RevocationReason, Session, SessionId, SessionTokenRef};
use crate::storage::{
    Accessor, AtomicBatchOperation, AtomicBatchResult, Collection, CollectionSummary, Cursor, Key,
    ListQuery, ObjectId, Owner, Page, Precondition, StorageIndexDefinition, StorageIndexMembership,
    StorageIndexQuery, StorageObject, Version, WriteRequest,
};
use crate::time::{DurationMillis, TimestampMillis};

const USERS: &str = "users";
const IDENTITIES: &str = "auth_identities";
const SESSIONS: &str = "sessions";
const FRIEND_EDGES: &str = "friend_edges";
const GROUPS: &str = "groups";
const GROUP_MEMBERSHIPS: &str = "group_memberships";
const GROUP_ADMISSIONS: &str = "group_admissions";
const GROUP_COUNTERS: &str = "citadel_counters";
const NOTIFICATIONS: &str = "notifications";
const WALLET_BALANCES: &str = "wallet_balances";
const WALLET_LEDGER: &str = "wallet_ledger";
const PURCHASES: &str = "purchases";
const CHAT_CHANNELS: &str = "chat_channels";
const CHAT_ACCESS_EPOCHS: &str = "chat_access_epochs";
const CHAT_MESSAGES: &str = "chat_messages";
const CHAT_EVENTS: &str = "chat_events";
const CHAT_MODERATION_AUDIT: &str = "chat_moderation_audit";
const CHAT_RATE_LIMITS: &str = "chat_rate_limits";
const CHAT_DELIVERY_OUTBOX: &str = "chat_delivery_outbox";
const LEADERBOARD_RESET_SCHEDULER_LEASE: &str = "leaderboard_reset_scheduler_lease";
const LEADERBOARD_RESET_EPOCHS: &str = "leaderboard_reset_epochs";
const LEADERBOARD_RESET_OUTBOX: &str = "leaderboard_reset_outbox";
const LEADERBOARD_RESET_SNAPSHOT_RECORDS: &str = "leaderboard_reset_snapshot_records";
const TOURNAMENTS: &str = "tournaments";
const TOURNAMENT_ENTRIES: &str = "tournament_entries";
const TOURNAMENT_RESULTS: &str = "tournament_results";
const TOURNAMENT_SETTLEMENT_OUTBOX: &str = "tournament_settlement_outbox";
const GAMESCRIPT_DRAFTS: &str = "gamescript_drafts";
const GAMESCRIPT_REVISIONS: &str = "gamescript_revisions";
const GAMESCRIPT_REVISION_PINS: &str = "gamescript_revision_pins";
const GAMESCRIPT_REVISION_DIAGNOSTICS: &str = "gamescript_revision_diagnostics";
const GAMESCRIPT_ACTIVATION_GENERATIONS: &str = "gamescript_activation_generations";
const GAMESCRIPT_ACTIVATIONS: &str = "gamescript_activations";
const GAMESCRIPT_AUDIT: &str = "gamescript_audit";
const GAMESCRIPT_OUTBOX: &str = "gamescript_outbox";
// Sequence documents for gamescript audit/outbox ids, allocated inside the
// same replica-set transaction as the rows they identify.
const GAMESCRIPT_COUNTERS: &str = "gamescript_counters";

const SCHEMA_COLLECTION: &str = "citadel_schema";
const SCHEMA_ID: &str = "mongodb-foundation";
const SCHEMA_VERSION: i64 = 6;
// MongoDB recommends retrying a whole transaction on
// `TransientTransactionError`, but retrying *only* the commit when the result
// is unknown.  A direct `UnitOfWork` has no replayable user closure, so it can
// only safely retry the latter; callers that need full-transaction retries use
// `MongoDatabase::with_transaction` below.
const TRANSACTION_RETRY_LIMIT: usize = 3;
// A burst of independent score submissions can legitimately contend on the
// same `(leaderboard_id, owner_id)` unique key.  Keep retries bounded, but give
// that hot-path enough attempts to serialize a small game-tick fan-in.
const LEADERBOARD_TRANSACTION_RETRY_LIMIT: usize = 8;
// Every enqueue touches the one global sequence document. A modest burst can
// therefore create more retryable write conflicts than per-leaderboard scores.
const NOTIFICATION_TRANSACTION_RETRY_LIMIT: usize = 32;
// Wallet writes serialize through the per-currency balance document and the
// global ledger sequence. A concurrent burst can therefore legitimately need
// more retries than the generic repository paths; keep the bound finite while
// allowing the full contractual burst to drain instead of returning a Database
// error for a retryable transaction conflict.
const WALLET_TRANSACTION_RETRY_LIMIT: usize = 64;
// GameScript audit/outbox ids serialize through one sequence document per
// kind, and activation generations through one document per scope, so a burst
// of concurrent operator actions legitimately write-write conflicts far more
// often than the scheduler's single-writer paths ever do. Mirror the wallet
// budget: bounded, but deep enough to drain the full contractual burst
// (the contract suite races eight submissions/allocations) instead of
// surfacing a retryable conflict as a Database error.
const GAMESCRIPT_TRANSACTION_RETRY_LIMIT: usize = 64;
const TRANSACTION_RETRY_BACKOFF: Duration = Duration::from_millis(20);

#[derive(Clone, Copy)]
struct IndexSpec {
    name: &'static str,
    keys: &'static [(&'static str, i32)],
    unique: bool,
}

#[derive(Clone, Copy)]
struct CollectionSpec {
    name: &'static str,
    indexes: &'static [IndexSpec],
}

const SCHEMA: &[CollectionSpec] = &[
    CollectionSpec {
        name: "storage_objects",
        indexes: &[
            IndexSpec {
                name: "storage_object_uq",
                keys: &[
                    ("owner_kind", 1),
                    ("owner_id", 1),
                    ("collection", 1),
                    ("object_key", 1),
                ],
                unique: true,
            },
            IndexSpec {
                name: "storage_collection_cursor",
                keys: &[
                    ("collection", 1),
                    ("owner_kind", 1),
                    ("owner_id", 1),
                    ("object_key", 1),
                ],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "storage_index_definitions",
        indexes: &[IndexSpec {
            name: "storage_index_definition_uq",
            keys: &[("index_name", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "storage_index_memberships",
        indexes: &[IndexSpec {
            name: "storage_index_membership_uq",
            keys: &[
                ("index_name", 1),
                ("owner_kind", 1),
                ("owner_id", 1),
                ("collection", 1),
                ("object_key", 1),
            ],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "users",
        indexes: &[
            IndexSpec {
                name: "users_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "users_username_uq",
                keys: &[("username", 1)],
                unique: true,
            },
        ],
    },
    CollectionSpec {
        name: "auth_identities",
        indexes: &[
            IndexSpec {
                name: "auth_identity_uq",
                keys: &[("provider", 1), ("external_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "auth_identity_user",
                keys: &[("user_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "sessions",
        indexes: &[
            IndexSpec {
                name: "session_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "session_user_state",
                keys: &[("user_id", 1), ("state_kind", 1)],
                unique: false,
            },
            IndexSpec {
                name: "session_token",
                keys: &[("token_ref", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "friend_edges",
        indexes: &[IndexSpec {
            name: "friend_edge_uq",
            keys: &[("owner_id", 1), ("other_id", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "groups",
        indexes: &[
            IndexSpec {
                name: "group_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "group_name_uq",
                keys: &[("name", 1)],
                unique: true,
            },
        ],
    },
    CollectionSpec {
        name: "group_memberships",
        indexes: &[
            IndexSpec {
                name: "group_member_uq",
                keys: &[("group_id", 1), ("user_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "group_membership_order",
                keys: &[("group_id", 1), ("joined_at_unix_ms", 1), ("user_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "group_admissions",
        indexes: &[
            IndexSpec {
                name: "group_admission_uq",
                keys: &[("group_id", 1), ("user_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "group_admission_user",
                keys: &[("user_id", 1), ("created_at_unix_ms", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "leaderboards",
        indexes: &[IndexSpec {
            name: "leaderboard_id_uq",
            keys: &[("id", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "leaderboard_records",
        indexes: &[
            IndexSpec {
                name: "leaderboard_record_uq",
                keys: &[("leaderboard_id", 1), ("owner_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "leaderboard_records_rank_asc",
                keys: &[
                    ("leaderboard_id", 1),
                    ("score", 1),
                    ("subscore", 1),
                    ("owner_id", 1),
                ],
                unique: false,
            },
            IndexSpec {
                name: "leaderboard_records_rank_desc",
                keys: &[
                    ("leaderboard_id", 1),
                    ("score", -1),
                    ("subscore", -1),
                    ("owner_id", 1),
                ],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: LEADERBOARD_RESET_SCHEDULER_LEASE,
        indexes: &[IndexSpec {
            name: "scheduler_lease_key_uq",
            keys: &[("lease_key", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: LEADERBOARD_RESET_EPOCHS,
        indexes: &[IndexSpec {
            name: "scheduler_epoch_uq",
            keys: &[("leaderboard_id", 1), ("due_at_unix_ms", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: LEADERBOARD_RESET_OUTBOX,
        indexes: &[
            IndexSpec {
                name: "scheduler_outbox_epoch_uq",
                keys: &[("leaderboard_id", 1), ("due_at_unix_ms", 1)],
                unique: true,
            },
            IndexSpec {
                name: "scheduler_outbox_pending_order",
                keys: &[
                    ("created_at_unix_ms", 1),
                    ("leaderboard_id", 1),
                    ("due_at_unix_ms", 1),
                ],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        // The compound key is both the immutable snapshot identity and the
        // deterministic owner-id read order used by `snapshot`.
        name: LEADERBOARD_RESET_SNAPSHOT_RECORDS,
        indexes: &[IndexSpec {
            name: "scheduler_snapshot_record_uq",
            keys: &[
                ("leaderboard_id", 1),
                ("due_at_unix_ms", 1),
                ("owner_id", 1),
            ],
            unique: true,
        }],
    },
    CollectionSpec {
        name: TOURNAMENTS,
        indexes: &[
            IndexSpec {
                name: "tournament_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "tournament_lifecycle",
                keys: &[("state", 1), ("ends_at_unix_ms", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: TOURNAMENT_ENTRIES,
        indexes: &[
            IndexSpec {
                name: "tournament_entry_uq",
                keys: &[("tournament_id", 1), ("user_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "tournament_entry_order",
                keys: &[("tournament_id", 1), ("registered_at_unix_ms", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: TOURNAMENT_RESULTS,
        indexes: &[
            IndexSpec {
                name: "tournament_result_user_uq",
                keys: &[("tournament_id", 1), ("user_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "tournament_result_rank_uq",
                keys: &[("tournament_id", 1), ("rank", 1)],
                unique: true,
            },
        ],
    },
    CollectionSpec {
        name: TOURNAMENT_SETTLEMENT_OUTBOX,
        indexes: &[
            IndexSpec {
                name: "tournament_settlement_outbox_uq",
                keys: &[("tournament_id", 1), ("due_at_unix_ms", 1)],
                unique: true,
            },
            IndexSpec {
                name: "tournament_settlement_outbox_pending_order",
                keys: &[
                    ("created_at_unix_ms", 1),
                    ("tournament_id", 1),
                    ("due_at_unix_ms", 1),
                ],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "chat_channels",
        indexes: &[
            IndexSpec {
                name: "chat_channel_id_uq",
                keys: &[("channel_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "chat_channel_canonical_uq",
                keys: &[("canonical_key", 1)],
                unique: true,
            },
            IndexSpec {
                name: "chat_channel_type",
                keys: &[("channel_type", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "chat_access_epochs",
        indexes: &[IndexSpec {
            name: "chat_access_epoch_uq",
            keys: &[("access_key", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "chat_messages",
        indexes: &[
            IndexSpec {
                name: "chat_message_uq",
                keys: &[("channel_id", 1), ("id", 1)],
                unique: true,
            },
            IndexSpec {
                // Keep the prior two-column foundation index untouched during
                // an in-place upgrade; the id suffix makes same-millisecond
                // pagination deterministic for the chat adapter.
                name: "chat_message_time_order",
                keys: &[("channel_id", 1), ("created_at_unix_ms", 1), ("id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "chat_events",
        indexes: &[IndexSpec {
            name: "chat_event_uq",
            keys: &[("channel_id", 1), ("event_id", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "chat_moderation_audit",
        indexes: &[IndexSpec {
            name: "chat_audit_expiry",
            keys: &[("occurred_at_unix_ms", 1), ("audit_id", 1)],
            unique: false,
        }],
    },
    CollectionSpec {
        name: "chat_rate_limits",
        indexes: &[
            IndexSpec {
                name: "chat_rate_limit_uq",
                keys: &[("key", 1), ("window_started_unix_ms", 1)],
                unique: true,
            },
            IndexSpec {
                name: "chat_rate_limit_expiry",
                keys: &[("expires_at_unix_ms", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "chat_delivery_outbox",
        indexes: &[
            IndexSpec {
                name: "chat_outbox_event_uq",
                keys: &[("channel_id", 1), ("event_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "chat_outbox_expiry",
                keys: &[("expires_at_unix_ms", 1), ("outbox_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: NOTIFICATIONS,
        indexes: &[
            IndexSpec {
                name: "notification_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "notification_recipient_id",
                keys: &[("recipient_id", 1), ("id", -1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "wallet_balances",
        indexes: &[IndexSpec {
            name: "wallet_balance_uq",
            keys: &[("user_id", 1), ("currency", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: "wallet_ledger",
        indexes: &[
            IndexSpec {
                name: "wallet_ledger_id_uq",
                keys: &[("id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "wallet_ledger_user",
                keys: &[("user_id", 1), ("id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: "purchases",
        indexes: &[
            IndexSpec {
                name: "purchase_transaction_uq",
                keys: &[("transaction_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "purchase_user_time",
                keys: &[
                    ("user_id", 1),
                    ("validated_at_unix_ms", -1),
                    ("transaction_id", -1),
                ],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: GAMESCRIPT_DRAFTS,
        indexes: &[
            IndexSpec {
                name: "gamescript_draft_uq",
                keys: &[("draft_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "gamescript_draft_retention",
                keys: &[("updated_at_unix_ms", 1), ("draft_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        // The unique content-hash identity both deduplicates identical
        // submissions and resolves the concurrent-submission race to one
        // document.
        name: GAMESCRIPT_REVISIONS,
        indexes: &[
            IndexSpec {
                name: "gamescript_revision_uq",
                keys: &[("revision_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "gamescript_revision_retention",
                keys: &[("created_at_unix_ms", 1), ("revision_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: GAMESCRIPT_REVISION_PINS,
        indexes: &[IndexSpec {
            name: "gamescript_revision_pin_uq",
            keys: &[("revision_id", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: GAMESCRIPT_REVISION_DIAGNOSTICS,
        indexes: &[IndexSpec {
            name: "gamescript_diagnostic_uq",
            keys: &[("revision_id", 1), ("seq", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: GAMESCRIPT_ACTIVATION_GENERATIONS,
        indexes: &[IndexSpec {
            name: "gamescript_generation_scope_uq",
            keys: &[("scope", 1)],
            unique: true,
        }],
    },
    CollectionSpec {
        name: GAMESCRIPT_ACTIVATIONS,
        indexes: &[
            IndexSpec {
                name: "gamescript_activation_uq",
                keys: &[("scope", 1), ("generation", 1)],
                unique: true,
            },
            IndexSpec {
                name: "gamescript_activation_revision",
                keys: &[("revision_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: GAMESCRIPT_AUDIT,
        indexes: &[
            IndexSpec {
                name: "gamescript_audit_uq",
                keys: &[("audit_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "gamescript_audit_order",
                keys: &[("created_at_unix_ms", -1), ("audit_id", -1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        name: GAMESCRIPT_OUTBOX,
        indexes: &[
            IndexSpec {
                name: "gamescript_outbox_uq",
                keys: &[("outbox_id", 1)],
                unique: true,
            },
            IndexSpec {
                name: "gamescript_outbox_pending_order",
                keys: &[("created_at_unix_ms", 1), ("outbox_id", 1)],
                unique: false,
            },
        ],
    },
    CollectionSpec {
        // Sequence documents keyed by `_id` ("audit"/"outbox"); no secondary
        // indexes. Declared so the manifest genuinely covers every projection
        // the adapter writes.
        name: GAMESCRIPT_COUNTERS,
        indexes: &[],
    },
];

/// Public, deterministic description of the foundation schema for operator and
/// integration tests. The actual index manifest is private to prevent later
/// domain tasks from treating it as a mutable public DDL API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoSchemaPlan {
    pub version: u32,
    pub collections: usize,
    pub indexes: usize,
}

impl MongoSchemaPlan {
    #[must_use]
    pub fn foundation() -> Self {
        Self {
            version: SCHEMA_VERSION as u32,
            collections: SCHEMA.len(),
            indexes: SCHEMA.iter().map(|spec| spec.indexes.len()).sum(),
        }
    }
}

/// A connected durable MongoDB backend with a verified transactional deployment
/// and idempotently materialized schema. Its `Debug` is intentionally redacted.
pub struct MongoDatabase {
    client: Client,
    database: Database,
    explorer: Arc<MongoMetadataExplorer>,
}

/// Identity/session Mongo execution target. A transaction target owns exactly
/// one mutable driver session behind an async mutex: repository handles may be
/// cloned, but their commands cannot concurrently use a `ClientSession`.
#[derive(Clone)]
enum MongoExecutor {
    Database(Database),
    Transaction(Arc<tokio::sync::Mutex<Option<ClientSession>>>, Database),
}

pub struct MongoUserRepository {
    executor: MongoExecutor,
}
pub struct MongoAuthIdentityRepository {
    executor: MongoExecutor,
}
pub struct MongoSessionRepository {
    executor: MongoExecutor,
}
/// Durable Mongo storage adapter. Documents retain the portable object payload
/// plus identity/permission columns used by keyset queries and authorization.
pub struct MongoStorageRepository {
    client: Client,
    database: Database,
    /// `Some` binds every storage command to the enclosing UnitOfWork's one
    /// session.  It must never start a nested transaction: aborting the UoW
    /// then discards both the object and its index memberships together.
    session: Option<Arc<tokio::sync::Mutex<Option<ClientSession>>>>,
}

/// Durable MongoDB implementation of the directed friend-edge graph.
///
/// Every mutating operation writes its reciprocal edges in one MongoDB
/// transaction. Pooled calls create a replayable transaction; calls obtained
/// from [`MongoUnitOfWork`] use its already-active `ClientSession` and never
/// begin a nested transaction.
pub struct MongoFriendsRepository {
    client: Client,
    database: Database,
    session: Option<Arc<tokio::sync::Mutex<Option<ClientSession>>>>,
}

/// Durable MongoDB groups, memberships, and admissions adapter.
pub struct MongoGroupsRepository {
    client: Client,
    database: Database,
    session: Option<Arc<tokio::sync::Mutex<Option<ClientSession>>>>,
}

/// Durable MongoDB leaderboards adapter.  Its score read-modify-write runs in
/// a retryable replica-set transaction, which serializes competing submissions
/// to a board without falling back to the process-local reference backend.
pub struct MongoLeaderboardsRepository {
    client: Client,
    database: Database,
}

/// Durable MongoDB scheduler adapter. Every claim snapshots and clears the
/// live records, then persists its epoch and callback outbox row in one
/// replayable replica-set transaction.
pub struct MongoLeaderboardResetRepository {
    client: Client,
    database: Database,
}

/// Durable MongoDB tournament adapter. Lifecycle mutations and epoch settlement
/// are executed in replayable replica-set transactions.
pub struct MongoTournamentsRepository {
    client: Client,
    database: Database,
}

/// Durable MongoDB GameScript revision adapter. Draft submission, activation
/// generation allocation, pinning, and retention pruning run in replayable
/// replica-set transactions so a state change and its audit/outbox documents
/// commit or disappear together. MongoDB has no foreign-key backstop for the
/// "active revisions are never pruned" rule, and its transactions abort only
/// on write-write document conflicts, so operations that depend on a revision
/// surviving (activation, pin, diagnostic append, and the identical-content
/// submit dedupe) deliberately WRITE the revision document (see
/// [`gamescript_touch_revision_in_session`]); pruning deletes then conflict
/// with those commits instead of skewing past them.
pub struct MongoGameScriptRepository {
    client: Client,
    database: Database,
    limits: GameScriptLimits,
}

/// Durable MongoDB notification store. Enqueues use a replica-set transaction
/// so allocating the global id, retaining the bounded window, and evicting its
/// oldest entries are one atomic transition.
pub struct MongoNotificationsRepository {
    client: Client,
    database: Database,
}

/// Durable wallet adapter. Each adjustment uses a replayable replica-set
/// transaction, so the balance read model, ledger append, sequence allocation,
/// and retention eviction either all commit or all disappear.
pub struct MongoWalletRepository {
    client: Client,
    database: Database,
}

/// Durable validated-purchase adapter. The transaction id unique index is the
/// idempotency key: a replay can never create a second charge record.
pub struct MongoPurchasesRepository {
    database: Database,
}

/// Durable MongoDB chat adapter. Its trait implementation is intentionally
/// separate from the backend foundation so chat mutations can share one
/// replica-set transaction across messages, events, authorization fences,
/// audits, limits, and delivery source rows.
pub struct MongoChatRepository {
    client: Client,
    database: Database,
    /// An enclosing `MongoUnitOfWork` session. When present, chat helpers use
    /// this exact session and never open a nested transaction.
    session: Option<Arc<tokio::sync::Mutex<Option<ClientSession>>>>,
}

fn json_data<T: serde::Serialize>(value: &T) -> AppResult<String> {
    serde_json::to_string(value).map_err(|_| AppError::internal("failed to encode MongoDB record"))
}
fn from_json<T: serde::de::DeserializeOwned>(value: &Document) -> AppResult<T> {
    let raw = value
        .get_str("data")
        .map_err(|_| AppError::internal("invalid MongoDB record"))?;
    serde_json::from_str(raw).map_err(|_| AppError::internal("failed to decode MongoDB record"))
}
fn credential_columns(credential: &AuthCredential) -> (&'static str, &str) {
    match credential {
        AuthCredential::Device(id) => (AuthProvider::Device.as_str(), id.as_str()),
        AuthCredential::Custom(id) => (AuthProvider::Custom.as_str(), id.as_str()),
        AuthCredential::Email(id) => (AuthProvider::Email.as_str(), id.as_str()),
    }
}
fn identity_from_doc(d: &Document) -> AppResult<AuthIdentity> {
    let provider = d
        .get_str("provider")
        .map_err(|_| AppError::internal("invalid MongoDB identity"))?;
    let external = d
        .get_str("external_id")
        .map_err(|_| AppError::internal("invalid MongoDB identity"))?;
    let credential = match provider {
        "device" => AuthCredential::Device(DeviceId::new(external)?),
        "custom" => AuthCredential::Custom(CustomId::new(external)?),
        "email" => AuthCredential::Email(EmailAddress::new(external)?),
        _ => return Err(AppError::internal("invalid MongoDB identity provider")),
    };
    let mut identity = AuthIdentity::new(
        credential,
        UserId::new(
            d.get_str("user_id")
                .map_err(|_| AppError::internal("invalid MongoDB identity"))?,
        )?,
        TimestampMillis::from_unix_millis(
            d.get_i64("created_at")
                .map_err(|_| AppError::internal("invalid MongoDB identity"))? as u64,
        ),
        TimestampMillis::from_unix_millis(
            d.get_i64("updated_at")
                .map_err(|_| AppError::internal("invalid MongoDB identity"))? as u64,
        ),
    )?;
    if let Ok(verifier) = d.get_str("password_verifier") {
        identity = identity.with_password_verifier(PasswordVerifier::new(verifier.to_owned())?)?;
    }
    Ok(identity)
}
fn duplicate(error: &mongodb::error::Error) -> bool {
    // The official driver exposes server errors as text; error code 11000 is
    // stable across supported MongoDB releases and never includes user values.
    error.to_string().contains("E11000") || error.to_string().contains("11000")
}
fn mongo_write_error(error: mongodb::error::Error, message: &'static str) -> AppError {
    if duplicate(&error) {
        AppError::conflict(message)
    } else {
        mongo_error(error)
    }
}

// Chat adapters deliberately keep BSON boundary checks here rather than
// duplicating ad-hoc coercions in every channel/message/outbox operation. They
// return stable errors and never include a chat payload or an identifier value.
#[cfg_attr(not(test), allow(dead_code))]
fn chat_id<'a>(value: &'a str, field: &'static str) -> AppResult<&'a str> {
    if value.is_empty() || value.len() > 512 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(AppError::validation(format!("invalid {field}")));
    }
    Ok(value)
}

#[cfg_attr(not(test), allow(dead_code))]
fn chat_timestamp(value: TimestampMillis) -> AppResult<i64> {
    i64::try_from(value.unix_millis())
        .map_err(|_| AppError::validation("chat timestamp is outside MongoDB range"))
}

impl MongoUserRepository {
    fn new(executor: MongoExecutor) -> Self {
        Self { executor }
    }
}
impl MongoAuthIdentityRepository {
    fn new(executor: MongoExecutor) -> Self {
        Self { executor }
    }
}
impl MongoSessionRepository {
    fn new(executor: MongoExecutor) -> Self {
        Self { executor }
    }
}
impl MongoStorageRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self {
            client,
            database,
            session: None,
        }
    }

    fn transactional(
        client: Client,
        database: Database,
        session: Arc<tokio::sync::Mutex<Option<ClientSession>>>,
    ) -> Self {
        Self {
            client,
            database,
            session: Some(session),
        }
    }
}

impl MongoGroupsRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self {
            client,
            database,
            session: None,
        }
    }

    fn transactional(
        client: Client,
        database: Database,
        session: Arc<tokio::sync::Mutex<Option<ClientSession>>>,
    ) -> Self {
        Self {
            client,
            database,
            session: Some(session),
        }
    }
}

impl MongoFriendsRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self {
            client,
            database,
            session: None,
        }
    }

    fn transactional(
        client: Client,
        database: Database,
        session: Arc<tokio::sync::Mutex<Option<ClientSession>>>,
    ) -> Self {
        Self {
            client,
            database,
            session: Some(session),
        }
    }
}

impl MongoLeaderboardsRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self { client, database }
    }
}

impl MongoLeaderboardResetRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self { client, database }
    }
}

impl MongoTournamentsRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self { client, database }
    }
}

fn scheduler_i64(value: u64, field: &'static str) -> AppResult<i64> {
    i64::try_from(value)
        .map_err(|_| AppError::internal(format!("scheduler {field} is outside MongoDB range")))
}

fn scheduler_token(value: i64) -> AppResult<SchedulerFencingToken> {
    u64::try_from(value)
        .map(SchedulerFencingToken::new)
        .map_err(|_| AppError::internal("invalid MongoDB scheduler fencing token"))
}

fn snapshot_record_doc(mut record: Document, due_at: i64) -> Document {
    // A snapshot is a new immutable document, never an alias of the live row.
    record.remove("_id");
    record.insert("due_at_unix_ms", due_at);
    record
}

fn snapshot_from_docs(
    epoch: &ResetEpoch,
    documents: Vec<Document>,
) -> AppResult<LeaderboardResetSnapshot> {
    let records = documents
        .iter()
        .map(leaderboard_record_from_doc)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(LeaderboardResetSnapshot {
        epoch: epoch.clone(),
        records,
    })
}

fn scheduler_lease_from_doc(document: &Document) -> AppResult<SchedulerLease> {
    Ok(SchedulerLease::new(
        document
            .get_str("node_id")
            .map_err(|_| AppError::internal("invalid MongoDB scheduler lease"))?
            .to_owned(),
        scheduler_token(
            document
                .get_i64("fencing_token")
                .map_err(|_| AppError::internal("invalid MongoDB scheduler lease"))?,
        )?,
        TimestampMillis::from_unix_millis(
            u64::try_from(
                document
                    .get_i64("expires_at_unix_ms")
                    .map_err(|_| AppError::internal("invalid MongoDB scheduler lease"))?,
            )
            .map_err(|_| AppError::internal("invalid MongoDB scheduler lease"))?,
        ),
    ))
}

#[derive(Debug)]
enum SchedulerTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}

impl From<AppError> for SchedulerTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}

impl From<mongodb::error::Error> for SchedulerTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

async fn scheduler_transaction<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, SchedulerTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(SchedulerTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(SchedulerTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(SchedulerTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await;
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded scheduler transaction retry returns or continues")
}

/// Replayable replica-set transaction runner for the GameScript adapter.
///
/// Identical retry semantics to [`scheduler_transaction`] (whole-closure
/// replay on `TransientTransactionError`, commit-only retry on
/// `UnknownTransactionCommitResult`, domain errors abort without retry), but
/// with the deeper [`GAMESCRIPT_TRANSACTION_RETRY_LIMIT`] budget: gamescript
/// operations funnel through shared sequence/counter documents, so concurrent
/// bursts conflict pairwise and need wallet-depth retries to converge.
async fn gamescript_transaction<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, SchedulerTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..GAMESCRIPT_TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(SchedulerTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(SchedulerTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < GAMESCRIPT_TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(SchedulerTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..GAMESCRIPT_TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < GAMESCRIPT_TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await;
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < GAMESCRIPT_TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded gamescript transaction retry returns or continues")
}

#[async_trait]
impl LeaderboardResetRepository for MongoLeaderboardResetRepository {
    async fn acquire_lease(
        &self,
        node_id: &str,
        now: TimestampMillis,
        ttl: DurationMillis,
    ) -> AppResult<Option<SchedulerLease>> {
        let expires_at = scheduler_i64(now.checked_add(ttl)?.unix_millis(), "lease expiry")?;
        let now = scheduler_i64(now.unix_millis(), "timestamp")?;
        let leases = self
            .database
            .collection::<Document>(LEADERBOARD_RESET_SCHEDULER_LEASE);
        let filter = doc! {
            "lease_key": "leaderboards",
            "$or": [
                { "expires_at_unix_ms": { "$lte": now } },
                { "node_id": node_id },
            ],
        };
        // A matching live owner renews without changing its token; an expired
        // owner is replaced with a strictly higher token. The unique lease_key
        // index turns an attempted upsert while another live node owns the row
        // into the normal no-lease result below.
        let update = vec![doc! { "$set": {
            "lease_key": "leaderboards",
            "node_id": node_id,
            "fencing_token": { "$cond": [
                { "$lte": ["$expires_at_unix_ms", now] },
                { "$add": [{ "$ifNull": ["$fencing_token", 0_i64] }, 1_i64] },
                { "$ifNull": ["$fencing_token", 1_i64] },
            ] },
            "expires_at_unix_ms": expires_at,
        }}];
        let result = leases
            .find_one_and_update(filter, update)
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await;
        match result {
            Ok(Some(document)) => scheduler_lease_from_doc(&document).map(Some),
            Ok(None) => Ok(None),
            Err(error) if duplicate(&error) => Ok(None),
            Err(error) => Err(mongo_error(error)),
        }
    }

    async fn claim_epoch(
        &self,
        epoch: ResetEpoch,
        token: SchedulerFencingToken,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let now = scheduler_i64(now.unix_millis(), "timestamp")?;
        let due_at = scheduler_i64(epoch.due_at.unix_millis(), "epoch timestamp")?;
        let token = scheduler_i64(token.get(), "fencing token")?;
        scheduler_transaction(&self.client, &self.database, move |database, session| {
            let epoch = epoch.clone();
            Box::pin(async move {
                let lease = database.collection::<Document>(LEADERBOARD_RESET_SCHEDULER_LEASE);
                // This no-op value write is intentional: it makes the fence
                // document part of the transaction's write set, so a concurrent
                // lease takeover conflicts and retries rather than allowing a
                // stale snapshot to stage an epoch.
                let fenced = lease
                    .update_one(
                        doc! {
                            "lease_key": "leaderboards",
                            "fencing_token": token,
                            "expires_at_unix_ms": { "$gt": now },
                        },
                        doc! { "$set": { "fencing_token": token } },
                    )
                    .session(&mut *session)
                    .await?;
                if fenced.matched_count == 0 {
                    return Err(AppError::conflict("scheduler lease is no longer current").into());
                }
                let epochs = database.collection::<Document>(LEADERBOARD_RESET_EPOCHS);
                match epochs
                    .insert_one(doc! {
                        "leaderboard_id": &epoch.leaderboard_id,
                        "due_at_unix_ms": due_at,
                        "fencing_token": token,
                        "claimed_at_unix_ms": now,
                    })
                    .session(&mut *session)
                    .await
                {
                    Ok(_) => {}
                    Err(error) if duplicate(&error) => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
                let live_records = database.collection::<Document>("leaderboard_records");
                let mut cursor = live_records
                    .find(doc! { "leaderboard_id": &epoch.leaderboard_id })
                    .sort(doc! { "owner_id": 1 })
                    .session(&mut *session)
                    .await?;
                let live = cursor.stream(&mut *session).try_collect::<Vec<_>>().await?;
                let snapshot_records = live
                    .into_iter()
                    .map(|record| snapshot_record_doc(record, due_at))
                    .collect::<Vec<_>>();
                if !snapshot_records.is_empty() {
                    database
                        .collection::<Document>(LEADERBOARD_RESET_SNAPSHOT_RECORDS)
                        .insert_many(snapshot_records)
                        .session(&mut *session)
                        .await?;
                }
                live_records
                    .delete_many(doc! { "leaderboard_id": &epoch.leaderboard_id })
                    .session(&mut *session)
                    .await?;
                database
                    .collection::<Document>(LEADERBOARD_RESET_OUTBOX)
                    .insert_one(doc! {
                        "leaderboard_id": &epoch.leaderboard_id,
                        "due_at_unix_ms": due_at,
                        "fencing_token": token,
                        "created_at_unix_ms": now,
                    })
                    .session(&mut *session)
                    .await?;
                Ok(true)
            })
        })
        .await
    }

    async fn snapshot(&self, epoch: &ResetEpoch) -> AppResult<Option<LeaderboardResetSnapshot>> {
        let due_at = scheduler_i64(epoch.due_at.unix_millis(), "epoch timestamp")?;
        let committed = self
            .database
            .collection::<Document>(LEADERBOARD_RESET_EPOCHS)
            .find_one(doc! {
                "leaderboard_id": &epoch.leaderboard_id,
                "due_at_unix_ms": due_at,
            })
            .await
            .map_err(mongo_error)?;
        if committed.is_none() {
            return Ok(None);
        }
        let records = self
            .database
            .collection::<Document>(LEADERBOARD_RESET_SNAPSHOT_RECORDS)
            .find(doc! {
                "leaderboard_id": &epoch.leaderboard_id,
                "due_at_unix_ms": due_at,
            })
            .sort(doc! { "owner_id": 1 })
            .await
            .map_err(mongo_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(mongo_error)?;
        snapshot_from_docs(epoch, records).map(Some)
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<ResetOutboxRecord>> {
        let limit =
            i64::try_from(limit).map_err(|_| AppError::validation("outbox limit out of range"))?;
        let documents = self
            .database
            .collection::<Document>(LEADERBOARD_RESET_OUTBOX)
            .find(doc! {})
            .sort(doc! { "created_at_unix_ms": 1, "leaderboard_id": 1, "due_at_unix_ms": 1 })
            .limit(limit)
            .await
            .map_err(mongo_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(mongo_error)?;
        documents
            .iter()
            .map(|document| {
                Ok(ResetOutboxRecord {
                    epoch: ResetEpoch::new(
                        document
                            .get_str("leaderboard_id")
                            .map_err(|_| AppError::internal("invalid MongoDB scheduler outbox"))?
                            .to_owned(),
                        TimestampMillis::from_unix_millis(
                            u64::try_from(document.get_i64("due_at_unix_ms").map_err(|_| {
                                AppError::internal("invalid MongoDB scheduler outbox")
                            })?)
                            .map_err(|_| AppError::internal("invalid MongoDB scheduler outbox"))?,
                        ),
                    ),
                    fencing_token: scheduler_token(
                        document
                            .get_i64("fencing_token")
                            .map_err(|_| AppError::internal("invalid MongoDB scheduler outbox"))?,
                    )?,
                })
            })
            .collect()
    }

    async fn acknowledge_outbox(&self, epoch: &ResetEpoch) -> AppResult<()> {
        self.database
            .collection::<Document>(LEADERBOARD_RESET_OUTBOX)
            .delete_one(doc! {
                "leaderboard_id": &epoch.leaderboard_id,
                "due_at_unix_ms": scheduler_i64(epoch.due_at.unix_millis(), "epoch timestamp")?,
            })
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

fn mongo_tournament(doc: &Document) -> AppResult<Tournament> {
    let state = TournamentState::from_token(
        doc.get_str("state")
            .map_err(|_| AppError::internal("invalid MongoDB tournament"))?,
    )?;
    let millis = |key| {
        doc.get_i64(key)
            .map_err(|_| AppError::internal("invalid MongoDB tournament"))
            .and_then(|value| {
                u64::try_from(value)
                    .map(TimestampMillis::from_unix_millis)
                    .map_err(|_| AppError::internal("invalid MongoDB tournament"))
            })
    };
    let leaderboard_id = doc
        .get_str("leaderboard_id")
        .map_err(|_| AppError::internal("invalid MongoDB tournament"))?
        .to_owned();
    Ok(Tournament {
        id: doc
            .get_str("id")
            .map_err(|_| AppError::internal("invalid MongoDB tournament"))?
            .to_owned(),
        leaderboard_id: leaderboard_id.clone(),
        state,
        registration_opens_at: millis("registration_opens_at_unix_ms")?,
        registration_closes_at: millis("registration_closes_at_unix_ms")?,
        starts_at: millis("starts_at_unix_ms")?,
        ends_at: millis("ends_at_unix_ms")?,
        settled_epoch: doc
            .get_i64("settled_due_at_unix_ms")
            .ok()
            .map(|due| {
                u64::try_from(due)
                    .map(|v| ResetEpoch::new(leaderboard_id, TimestampMillis::from_unix_millis(v)))
                    .map_err(|_| AppError::internal("invalid MongoDB tournament"))
            })
            .transpose()?,
        created_at: millis("created_at_unix_ms")?,
        updated_at: millis("updated_at_unix_ms")?,
    })
}

/// Materialize a standalone tournament projection from immutable reset records.
/// Only ranking columns are copied, so later live-leaderboard changes cannot
/// alter already-settled tournament results.
fn tournament_results_from_snapshot(
    tournament_id: &str,
    sort: SortOrder,
    snapshot: Vec<Document>,
) -> AppResult<Vec<Document>> {
    let mut rows = snapshot
        .into_iter()
        .map(|document| {
            Ok((
                document
                    .get_str("owner_id")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament snapshot"))?
                    .to_owned(),
                document
                    .get_i64("score")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament snapshot"))?,
                document
                    .get_i64("subscore")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament snapshot"))?,
            ))
        })
        .collect::<AppResult<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        let score = match sort {
            SortOrder::Asc => left.1.cmp(&right.1),
            SortOrder::Desc => right.1.cmp(&left.1),
        };
        let subscore = match sort {
            SortOrder::Asc => left.2.cmp(&right.2),
            SortOrder::Desc => right.2.cmp(&left.2),
        };
        score.then(subscore).then_with(|| left.0.cmp(&right.0))
    });
    rows.into_iter()
        .enumerate()
        .map(|(index, (user_id, score, subscore))| {
            let rank = i64::try_from(
                index
                    .checked_add(1)
                    .ok_or_else(|| AppError::internal("tournament rank out of range"))?,
            )
            .map_err(|_| AppError::internal("tournament rank out of range"))?;
            Ok(doc! {
                "tournament_id": tournament_id,
                "user_id": user_id,
                "rank": rank,
                "score": score,
                "subscore": subscore,
            })
        })
        .collect()
}

#[async_trait]
impl TournamentsRepository for MongoTournamentsRepository {
    async fn create(
        &self,
        request: CreateTournamentRequest,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        validate_schedule(&request)?;
        let now = scheduler_i64(now.unix_millis(), "tournament timestamp")?;
        let doc = doc! { "id": &request.id, "leaderboard_id": &request.leaderboard_id, "state": "draft", "registration_opens_at_unix_ms": scheduler_i64(request.registration_opens_at.unix_millis(), "tournament timestamp")?, "registration_closes_at_unix_ms": scheduler_i64(request.registration_closes_at.unix_millis(), "tournament timestamp")?, "starts_at_unix_ms": scheduler_i64(request.starts_at.unix_millis(), "tournament timestamp")?, "ends_at_unix_ms": scheduler_i64(request.ends_at.unix_millis(), "tournament timestamp")?, "created_at_unix_ms": now, "updated_at_unix_ms": now };
        self.database
            .collection::<Document>(TOURNAMENTS)
            .insert_one(doc)
            .await
            .map_err(|error| mongo_write_error(error, "tournament already exists"))?;
        self.get(&request.id)
            .await?
            .ok_or_else(|| AppError::internal("created tournament was not found"))
    }
    async fn get(&self, id: &str) -> AppResult<Option<Tournament>> {
        self.database
            .collection::<Document>(TOURNAMENTS)
            .find_one(doc! {"id": id})
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(mongo_tournament)
            .transpose()
    }
    async fn list(&self) -> AppResult<Vec<Tournament>> {
        let mut cursor = self
            .database
            .collection::<Document>(TOURNAMENTS)
            .find(doc! {})
            .sort(doc! {"starts_at_unix_ms": 1, "id": 1})
            .await
            .map_err(mongo_error)?;
        let mut tournaments = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            tournaments.push(mongo_tournament(&document)?);
        }
        Ok(tournaments)
    }
    async fn transition(
        &self,
        id: &str,
        to: TournamentState,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        let current = self
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found("no such tournament"))?;
        if !can_transition(current.state, to) {
            return Err(AppError::conflict("illegal tournament state transition"));
        }
        let update = self.database.collection::<Document>(TOURNAMENTS).update_one(doc! {"id": id, "state": current.state.as_str()}, doc! {"$set": {"state": to.as_str(), "updated_at_unix_ms": scheduler_i64(now.unix_millis(), "tournament timestamp")?}}).await.map_err(mongo_error)?;
        if update.matched_count != 1 {
            return Err(AppError::conflict("concurrent tournament state transition"));
        }
        Ok(Tournament {
            state: to,
            updated_at: now,
            ..current
        })
    }
    async fn register(
        &self,
        tournament_id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentEntry> {
        let tournament = self
            .get(tournament_id)
            .await?
            .ok_or_else(|| AppError::not_found("no such tournament"))?;
        if tournament.state != TournamentState::RegistrationOpen
            || now < tournament.registration_opens_at
            || now >= tournament.registration_closes_at
        {
            return Err(AppError::conflict("tournament registration is closed"));
        }
        self.database.collection::<Document>(TOURNAMENT_ENTRIES).insert_one(doc! {"tournament_id": tournament_id, "user_id": user_id, "registered_at_unix_ms": scheduler_i64(now.unix_millis(), "tournament timestamp")?}).await.map_err(|error| mongo_write_error(error, "tournament entry already exists"))?;
        Ok(TournamentEntry {
            tournament_id: tournament_id.to_owned(),
            user_id: user_id.to_owned(),
            registered_at: now,
        })
    }
    async fn entries(&self, tournament_id: &str) -> AppResult<Vec<TournamentEntry>> {
        let mut cursor = self
            .database
            .collection::<Document>(TOURNAMENT_ENTRIES)
            .find(doc! {"tournament_id": tournament_id})
            .sort(doc! {"user_id": 1})
            .await
            .map_err(mongo_error)?;
        let mut rows = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_error)? {
            rows.push(TournamentEntry {
                tournament_id: tournament_id.to_owned(),
                user_id: doc
                    .get_str("user_id")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament entry"))?
                    .to_owned(),
                registered_at: TimestampMillis::from_unix_millis(
                    u64::try_from(
                        doc.get_i64("registered_at_unix_ms")
                            .map_err(|_| AppError::internal("invalid MongoDB tournament entry"))?,
                    )
                    .map_err(|_| AppError::internal("invalid MongoDB tournament entry"))?,
                ),
            });
        }
        Ok(rows)
    }
    async fn results(&self, tournament_id: &str) -> AppResult<Vec<TournamentResult>> {
        let mut cursor = self
            .database
            .collection::<Document>(TOURNAMENT_RESULTS)
            .find(doc! {"tournament_id": tournament_id})
            .sort(doc! {"rank": 1, "user_id": 1})
            .await
            .map_err(mongo_error)?;
        let mut rows = Vec::new();
        while let Some(doc) = cursor.try_next().await.map_err(mongo_error)? {
            rows.push(TournamentResult {
                tournament_id: tournament_id.to_owned(),
                user_id: doc
                    .get_str("user_id")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament result"))?
                    .to_owned(),
                rank: u64::try_from(
                    doc.get_i64("rank")
                        .map_err(|_| AppError::internal("invalid MongoDB tournament result"))?,
                )
                .map_err(|_| AppError::internal("invalid MongoDB tournament result"))?,
                score: doc
                    .get_i64("score")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament result"))?,
                subscore: doc
                    .get_i64("subscore")
                    .map_err(|_| AppError::internal("invalid MongoDB tournament result"))?,
            });
        }
        Ok(rows)
    }
    async fn settle_from_epoch(
        &self,
        id: &str,
        epoch: ResetEpoch,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let id = id.to_owned();
        let due_at = scheduler_i64(epoch.due_at.unix_millis(), "epoch timestamp")?;
        let now = scheduler_i64(now.unix_millis(), "tournament timestamp")?;
        scheduler_transaction(&self.client, &self.database, move |database, session| {
            let id = id.clone();
            let epoch = epoch.clone();
            Box::pin(async move {
                let tournaments = database.collection::<Document>(TOURNAMENTS);
                let tournament_doc = tournaments
                    .find_one(doc! { "id": &id })
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| AppError::not_found("no such tournament"))?;
                let tournament = mongo_tournament(&tournament_doc)?;
                if tournament.state == TournamentState::Completed
                    && tournament.settled_epoch.as_ref() == Some(&epoch)
                {
                    return Ok(false);
                }
                if tournament.state != TournamentState::Running
                    || tournament.leaderboard_id != epoch.leaderboard_id
                {
                    return Err(AppError::conflict(
                        "tournament cannot settle from this reset epoch",
                    )
                    .into());
                }

                let epochs = database.collection::<Document>(LEADERBOARD_RESET_EPOCHS);
                if epochs
                    .find_one(doc! {
                        "leaderboard_id": &epoch.leaderboard_id,
                        "due_at_unix_ms": due_at,
                    })
                    .session(&mut *session)
                    .await?
                    .is_none()
                {
                    return Err(
                        AppError::conflict("tournament reset epoch is not committed").into(),
                    );
                }
                let leaderboard = database
                    .collection::<Document>("leaderboards")
                    .find_one(doc! { "id": &epoch.leaderboard_id })
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| AppError::conflict("tournament leaderboard does not exist"))?;
                let sort = leaderboard_definition_from_doc(&leaderboard)?.sort;
                let snapshots = database.collection::<Document>(LEADERBOARD_RESET_SNAPSHOT_RECORDS);
                let snapshot = snapshots
                    .find(doc! {
                        "leaderboard_id": &epoch.leaderboard_id,
                        "due_at_unix_ms": due_at,
                    })
                    .session(&mut *session)
                    .await?
                    .stream(&mut *session)
                    .try_collect::<Vec<_>>()
                    .await?;
                let results = tournament_results_from_snapshot(&id, sort, snapshot)?;
                if !results.is_empty() {
                    database
                        .collection::<Document>(TOURNAMENT_RESULTS)
                        .insert_many(results)
                        .session(&mut *session)
                        .await?;
                }
                database
                    .collection::<Document>(TOURNAMENT_SETTLEMENT_OUTBOX)
                    .insert_one(doc! {
                        "tournament_id": &id,
                        "leaderboard_id": &epoch.leaderboard_id,
                        "due_at_unix_ms": due_at,
                        "created_at_unix_ms": now,
                    })
                    .session(&mut *session)
                    .await?;
                let update = tournaments
                    .update_one(
                        doc! { "id": &id, "state": TournamentState::Running.as_str() },
                        doc! { "$set": {
                            "state": TournamentState::Completed.as_str(),
                            "settled_due_at_unix_ms": due_at,
                            "updated_at_unix_ms": now,
                        }},
                    )
                    .session(&mut *session)
                    .await?;
                if update.matched_count != 1 {
                    return Err(AppError::conflict("concurrent tournament settlement").into());
                }
                Ok(true)
            })
        })
        .await
    }

    async fn pending_settlement_outbox(
        &self,
        limit: usize,
    ) -> AppResult<Vec<TournamentSettlementOutboxRecord>> {
        let limit =
            i64::try_from(limit).map_err(|_| AppError::validation("outbox limit is too large"))?;
        let mut cursor = self
            .database
            .collection::<Document>(TOURNAMENT_SETTLEMENT_OUTBOX)
            .find(doc! {})
            .sort(doc! { "created_at_unix_ms": 1, "tournament_id": 1, "due_at_unix_ms": 1 })
            .limit(limit)
            .await
            .map_err(mongo_error)?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            records.push(TournamentSettlementOutboxRecord {
                tournament_id: document
                    .get_str("tournament_id")
                    .map_err(|_| {
                        AppError::internal("invalid MongoDB tournament settlement outbox")
                    })?
                    .to_owned(),
                epoch: ResetEpoch::new(
                    document
                        .get_str("leaderboard_id")
                        .map_err(|_| {
                            AppError::internal("invalid MongoDB tournament settlement outbox")
                        })?
                        .to_owned(),
                    TimestampMillis::from_unix_millis(
                        u64::try_from(document.get_i64("due_at_unix_ms").map_err(|_| {
                            AppError::internal("invalid MongoDB tournament settlement outbox")
                        })?)
                        .map_err(|_| {
                            AppError::internal("invalid MongoDB tournament settlement outbox")
                        })?,
                    ),
                ),
            });
        }
        Ok(records)
    }

    async fn acknowledge_settlement_outbox(
        &self,
        tournament_id: &str,
        epoch: &ResetEpoch,
    ) -> AppResult<()> {
        self.database
            .collection::<Document>(TOURNAMENT_SETTLEMENT_OUTBOX)
            .delete_one(doc! {
                "tournament_id": tournament_id,
                "leaderboard_id": &epoch.leaderboard_id,
                "due_at_unix_ms": scheduler_i64(epoch.due_at.unix_millis(), "epoch timestamp")?,
            })
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

impl MongoGameScriptRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self {
            client,
            database,
            limits: GameScriptLimits::default(),
        }
    }
}

fn gamescript_doc_str(document: &Document, field: &str) -> AppResult<String> {
    document
        .get_str(field)
        .map(str::to_owned)
        .map_err(|_| AppError::internal("invalid MongoDB gamescript record"))
}

fn gamescript_doc_u64(document: &Document, field: &str) -> AppResult<u64> {
    u64::try_from(
        document
            .get_i64(field)
            .map_err(|_| AppError::internal("invalid MongoDB gamescript record"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB gamescript record"))
}

fn gamescript_doc_millis(document: &Document, field: &str) -> AppResult<TimestampMillis> {
    Ok(TimestampMillis::from_unix_millis(gamescript_doc_u64(
        document, field,
    )?))
}

fn mongo_gamescript_draft(document: &Document) -> AppResult<GameScriptDraft> {
    Ok(GameScriptDraft {
        draft_id: gamescript_doc_str(document, "draft_id")?,
        language: language_from_token(&gamescript_doc_str(document, "language")?)?,
        entrypoint: gamescript_doc_str(document, "entrypoint")?,
        content: gamescript_doc_str(document, "content")?,
        created_by: gamescript_doc_str(document, "created_by")?,
        created_at: gamescript_doc_millis(document, "created_at_unix_ms")?,
        updated_at: gamescript_doc_millis(document, "updated_at_unix_ms")?,
    })
}

fn mongo_gamescript_revision(document: &Document) -> AppResult<GameScriptRevision> {
    Ok(GameScriptRevision {
        revision_id: gamescript_doc_str(document, "revision_id")?,
        language: language_from_token(&gamescript_doc_str(document, "language")?)?,
        entrypoint: gamescript_doc_str(document, "entrypoint")?,
        content: gamescript_doc_str(document, "content")?,
        size_bytes: gamescript_doc_u64(document, "size_bytes")?,
        created_by: gamescript_doc_str(document, "created_by")?,
        created_at: gamescript_doc_millis(document, "created_at_unix_ms")?,
    })
}

fn mongo_gamescript_activation(document: &Document) -> AppResult<GameScriptActivation> {
    Ok(GameScriptActivation {
        scope: gamescript_doc_str(document, "scope")?,
        generation: gamescript_doc_u64(document, "generation")?,
        revision_id: gamescript_doc_str(document, "revision_id")?,
        activated_by: gamescript_doc_str(document, "activated_by")?,
        activated_at: gamescript_doc_millis(document, "activated_at_unix_ms")?,
    })
}

fn mongo_gamescript_diagnostic(document: &Document) -> AppResult<GameScriptDiagnostic> {
    Ok(GameScriptDiagnostic {
        revision_id: gamescript_doc_str(document, "revision_id")?,
        seq: gamescript_doc_u64(document, "seq")?,
        severity: GameScriptDiagnosticSeverity::from_token(&gamescript_doc_str(
            document, "severity",
        )?)?,
        source: gamescript_doc_str(document, "source")?,
        message: gamescript_doc_str(document, "message")?,
        created_at: gamescript_doc_millis(document, "created_at_unix_ms")?,
    })
}

fn mongo_gamescript_audit(document: &Document) -> AppResult<GameScriptAuditRecord> {
    Ok(GameScriptAuditRecord {
        audit_id: gamescript_doc_u64(document, "audit_id")?,
        actor: gamescript_doc_str(document, "actor")?,
        action: gamescript_doc_str(document, "action")?,
        target: gamescript_doc_str(document, "target")?,
        details: serde_json::from_str(&gamescript_doc_str(document, "details")?)
            .map_err(|_| AppError::internal("invalid MongoDB gamescript audit details"))?,
        created_at: gamescript_doc_millis(document, "created_at_unix_ms")?,
    })
}

fn mongo_gamescript_outbox(document: &Document) -> AppResult<GameScriptOutboxRecord> {
    Ok(GameScriptOutboxRecord {
        outbox_id: gamescript_doc_u64(document, "outbox_id")?,
        kind: GameScriptOutboxKind::from_token(&gamescript_doc_str(document, "kind")?)?,
        scope: document.get_str("scope").ok().map(str::to_owned),
        revision_id: gamescript_doc_str(document, "revision_id")?,
        generation: document
            .get_i64("generation")
            .ok()
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| AppError::internal("invalid MongoDB gamescript record"))
            })
            .transpose()?,
        created_at: gamescript_doc_millis(document, "created_at_unix_ms")?,
    })
}

fn gamescript_details_json(details: &GameScriptAuditContext) -> AppResult<String> {
    serde_json::to_string(details)
        .map_err(|_| AppError::internal("failed to encode gamescript audit details"))
}

/// Allocate the next value of one gamescript sequence document inside the
/// enclosing replica-set transaction, so ids commit atomically with the rows
/// they identify.
async fn next_gamescript_sequence(
    database: &Database,
    session: &mut ClientSession,
    key: &str,
) -> Result<i64, SchedulerTransactionError> {
    let updated = database
        .collection::<Document>(GAMESCRIPT_COUNTERS)
        .find_one_and_update(doc! { "_id": key }, doc! { "$inc": { "value": 1_i64 } })
        .upsert(true)
        .return_document(ReturnDocument::After)
        .session(&mut *session)
        .await?
        .ok_or_else(|| {
            SchedulerTransactionError::App(AppError::internal(
                "gamescript sequence upsert returned no document",
            ))
        })?;
    updated.get_i64("value").map_err(|_| {
        SchedulerTransactionError::App(AppError::internal("invalid gamescript sequence document"))
    })
}

/// Existence check *and* write-skew guard for operations that need the
/// revision to survive their transaction (activation, pin, diagnostics, and
/// the identical-content dedupe branch of draft submission).
///
/// MongoDB transactions abort only on write-write document conflicts — reads
/// never conflict — so a read-only existence check would let a concurrent
/// `prune_revisions` delete the revision while this transaction commits an
/// activation/pin that references it (there is no foreign-key backstop on
/// MongoDB). Bumping the internal `retention_fence` field WRITES the revision
/// document: a concurrent prune's delete now conflicts with this transaction
/// and one side replays with the other's outcome visible. The field is
/// bookkeeping only — it is not part of the domain revision record, which
/// stays immutable (`mongo_gamescript_revision` never reads it).
async fn gamescript_touch_revision_in_session(
    database: &Database,
    session: &mut ClientSession,
    revision_id: &str,
) -> Result<bool, SchedulerTransactionError> {
    Ok(database
        .collection::<Document>(GAMESCRIPT_REVISIONS)
        .update_one(
            doc! { "revision_id": revision_id },
            doc! { "$inc": { "retention_fence": 1_i64 } },
        )
        .session(&mut *session)
        .await?
        .matched_count
        > 0)
}

async fn insert_gamescript_audit_in_session(
    database: &Database,
    session: &mut ClientSession,
    actor: &str,
    action: &str,
    target: &str,
    details: &GameScriptAuditContext,
    now: i64,
) -> Result<(), SchedulerTransactionError> {
    let audit_id = next_gamescript_sequence(database, session, "audit").await?;
    database
        .collection::<Document>(GAMESCRIPT_AUDIT)
        .insert_one(doc! {
            "audit_id": audit_id,
            "actor": actor,
            "action": action,
            "target": target,
            "details": gamescript_details_json(details)?,
            "created_at_unix_ms": now,
        })
        .session(&mut *session)
        .await?;
    Ok(())
}

async fn insert_gamescript_outbox_in_session(
    database: &Database,
    session: &mut ClientSession,
    kind: GameScriptOutboxKind,
    scope: Option<&str>,
    revision_id: &str,
    generation: Option<i64>,
    now: i64,
) -> Result<(), SchedulerTransactionError> {
    let outbox_id = next_gamescript_sequence(database, session, "outbox").await?;
    let mut entry = doc! {
        "outbox_id": outbox_id,
        "kind": kind.as_str(),
        "revision_id": revision_id,
        "created_at_unix_ms": now,
    };
    if let Some(scope) = scope {
        entry.insert("scope", scope);
    }
    if let Some(generation) = generation {
        entry.insert("generation", generation);
    }
    database
        .collection::<Document>(GAMESCRIPT_OUTBOX)
        .insert_one(entry)
        .session(&mut *session)
        .await?;
    Ok(())
}

#[async_trait]
impl GameScriptRepository for MongoGameScriptRepository {
    async fn create_draft(
        &self,
        request: CreateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_create_draft(&request, &self.limits)?;
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        self.database
            .collection::<Document>(GAMESCRIPT_DRAFTS)
            .insert_one(doc! {
                "draft_id": &request.draft_id,
                "language": request.language.as_str(),
                "entrypoint": &request.entrypoint,
                "content": &request.content,
                "created_by": &request.created_by,
                "created_at_unix_ms": now,
                "updated_at_unix_ms": now,
            })
            .await
            .map_err(|error| mongo_write_error(error, "gamescript draft already exists"))?;
        Ok(GameScriptDraft {
            draft_id: request.draft_id,
            language: request.language,
            entrypoint: request.entrypoint,
            content: request.content,
            created_by: request.created_by,
            created_at: TimestampMillis::from_unix_millis(now as u64),
            updated_at: TimestampMillis::from_unix_millis(now as u64),
        })
    }

    async fn update_draft(
        &self,
        draft_id: &str,
        update: UpdateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_source(&update.entrypoint, &update.content, &self.limits)?;
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        self.database
            .collection::<Document>(GAMESCRIPT_DRAFTS)
            .find_one_and_update(
                doc! { "draft_id": draft_id },
                doc! { "$set": {
                    "language": update.language.as_str(),
                    "entrypoint": &update.entrypoint,
                    "content": &update.content,
                    "updated_at_unix_ms": now,
                } },
            )
            .return_document(ReturnDocument::After)
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(mongo_gamescript_draft)
            .transpose()?
            .ok_or_else(|| draft_not_found(draft_id))
    }

    async fn get_draft(&self, draft_id: &str) -> AppResult<Option<GameScriptDraft>> {
        self.database
            .collection::<Document>(GAMESCRIPT_DRAFTS)
            .find_one(doc! { "draft_id": draft_id })
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(mongo_gamescript_draft)
            .transpose()
    }

    async fn list_drafts(&self, limit: usize) -> AppResult<Vec<GameScriptDraft>> {
        validate_limit(limit)?;
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_DRAFTS)
            .find(doc! {})
            .sort(doc! { "draft_id": 1 })
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?;
        let mut drafts = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            drafts.push(mongo_gamescript_draft(&document)?);
        }
        Ok(drafts)
    }

    async fn delete_draft(&self, draft_id: &str) -> AppResult<bool> {
        Ok(self
            .database
            .collection::<Document>(GAMESCRIPT_DRAFTS)
            .delete_one(doc! { "draft_id": draft_id })
            .await
            .map_err(mongo_error)?
            .deleted_count
            > 0)
    }

    async fn submit_draft(
        &self,
        draft_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptSubmission> {
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let draft_id = draft_id.to_owned();
        let actor = actor.to_owned();
        let context = context.clone();
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            let draft_id = draft_id.clone();
            let actor = actor.clone();
            let context = context.clone();
            Box::pin(async move {
                let draft = database
                    .collection::<Document>(GAMESCRIPT_DRAFTS)
                    .find_one(doc! { "draft_id": &draft_id })
                    .session(&mut *session)
                    .await?
                    .as_ref()
                    .map(mongo_gamescript_draft)
                    .transpose()?
                    .ok_or_else(|| draft_not_found(&draft_id))?;
                let revision_id = gamescript_revision_content_hash(
                    draft.language,
                    &draft.entrypoint,
                    &draft.content,
                );
                let revisions = database.collection::<Document>(GAMESCRIPT_REVISIONS);
                let existing = revisions
                    .find_one(doc! { "revision_id": &revision_id })
                    .session(&mut *session)
                    .await?
                    .as_ref()
                    .map(mongo_gamescript_revision)
                    .transpose()?;
                let (revision, deduplicated) = match existing {
                    Some(revision) => {
                        // A dedupe depends on the existing revision surviving
                        // this transaction exactly like activation, pin, and
                        // diagnostic append do, so it joins the same
                        // retention-fence protocol: the touch WRITES the
                        // revision document and a concurrent prune's delete
                        // conflicts instead of committing blindly past the
                        // dedupe read. The losing side replays; a replay that
                        // finds the revision pruned takes the insert branch
                        // below and re-creates the content instead of
                        // returning a revision that no longer exists.
                        if !gamescript_touch_revision_in_session(database, session, &revision_id)
                            .await?
                        {
                            return Err(revision_not_found(&revision_id).into());
                        }
                        (revision, true)
                    }
                    None => {
                        // A concurrent identical submission conflicts on the
                        // unique revision_id index; the transient-error retry
                        // replays this transaction, which then observes the
                        // committed document and deduplicates.
                        revisions
                            .insert_one(doc! {
                                "revision_id": &revision_id,
                                "language": draft.language.as_str(),
                                "entrypoint": &draft.entrypoint,
                                "content": &draft.content,
                                "size_bytes": scheduler_i64(
                                    draft.content.len() as u64,
                                    "gamescript source size",
                                )?,
                                "created_by": &actor,
                                "created_at_unix_ms": now,
                            })
                            .session(&mut *session)
                            .await?;
                        (
                            GameScriptRevision {
                                revision_id: revision_id.clone(),
                                language: draft.language,
                                entrypoint: draft.entrypoint.clone(),
                                content: draft.content.clone(),
                                size_bytes: draft.content.len() as u64,
                                created_by: actor.clone(),
                                created_at: TimestampMillis::from_unix_millis(now as u64),
                            },
                            false,
                        )
                    }
                };
                let details = submit_audit_details(&draft_id, &revision, deduplicated, &context);
                insert_gamescript_audit_in_session(
                    database,
                    session,
                    &actor,
                    AUDIT_ACTION_SUBMIT,
                    &revision_id,
                    &details,
                    now,
                )
                .await?;
                if !deduplicated {
                    insert_gamescript_outbox_in_session(
                        database,
                        session,
                        GameScriptOutboxKind::RevisionCreated,
                        None,
                        &revision_id,
                        None,
                        now,
                    )
                    .await?;
                }
                database
                    .collection::<Document>(GAMESCRIPT_DRAFTS)
                    .delete_one(doc! { "draft_id": &draft_id })
                    .session(&mut *session)
                    .await?;
                Ok(GameScriptSubmission {
                    revision,
                    deduplicated,
                })
            })
        })
        .await
    }

    async fn get_revision(&self, revision_id: &str) -> AppResult<Option<GameScriptRevision>> {
        self.database
            .collection::<Document>(GAMESCRIPT_REVISIONS)
            .find_one(doc! { "revision_id": revision_id })
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(mongo_gamescript_revision)
            .transpose()
    }

    async fn list_revisions(&self, limit: usize) -> AppResult<Vec<GameScriptRevision>> {
        validate_limit(limit)?;
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_REVISIONS)
            .find(doc! {})
            .sort(doc! { "created_at_unix_ms": 1, "revision_id": 1 })
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?;
        let mut revisions = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            revisions.push(mongo_gamescript_revision(&document)?);
        }
        Ok(revisions)
    }

    async fn append_diagnostic(
        &self,
        revision_id: &str,
        severity: GameScriptDiagnosticSeverity,
        source: &str,
        message: &str,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDiagnostic> {
        let revision_id = revision_id.to_owned();
        let source = source.to_owned();
        let message = message.to_owned();
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            let revision_id = revision_id.clone();
            let source = source.clone();
            let message = message.clone();
            Box::pin(async move {
                if !gamescript_touch_revision_in_session(database, session, &revision_id).await? {
                    return Err(revision_not_found(&revision_id).into());
                }
                let last = database
                    .collection::<Document>(GAMESCRIPT_REVISION_DIAGNOSTICS)
                    .find_one(doc! { "revision_id": &revision_id })
                    .sort(doc! { "seq": -1 })
                    .session(&mut *session)
                    .await?;
                let next_seq = match last {
                    Some(document) => {
                        document.get_i64("seq").map_err(|_| {
                            SchedulerTransactionError::App(AppError::internal(
                                "invalid MongoDB gamescript diagnostic",
                            ))
                        })? + 1
                    }
                    None => 1,
                };
                database
                    .collection::<Document>(GAMESCRIPT_REVISION_DIAGNOSTICS)
                    .insert_one(doc! {
                        "revision_id": &revision_id,
                        "seq": next_seq,
                        "severity": severity.as_str(),
                        "source": &source,
                        "message": &message,
                        "created_at_unix_ms": now,
                    })
                    .session(&mut *session)
                    .await?;
                Ok(GameScriptDiagnostic {
                    revision_id,
                    seq: u64::try_from(next_seq).map_err(|_| {
                        SchedulerTransactionError::App(AppError::internal(
                            "invalid gamescript diagnostic sequence",
                        ))
                    })?,
                    severity,
                    source,
                    message,
                    created_at: TimestampMillis::from_unix_millis(now as u64),
                })
            })
        })
        .await
    }

    async fn diagnostics(&self, revision_id: &str) -> AppResult<Vec<GameScriptDiagnostic>> {
        if self.get_revision(revision_id).await?.is_none() {
            return Err(revision_not_found(revision_id));
        }
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_REVISION_DIAGNOSTICS)
            .find(doc! { "revision_id": revision_id })
            .sort(doc! { "seq": 1 })
            .await
            .map_err(mongo_error)?;
        let mut diagnostics = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            diagnostics.push(mongo_gamescript_diagnostic(&document)?);
        }
        Ok(diagnostics)
    }

    async fn pin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            Box::pin(async move {
                if !gamescript_touch_revision_in_session(database, session, &revision_id).await? {
                    return Err(revision_not_found(&revision_id).into());
                }
                let pins = database.collection::<Document>(GAMESCRIPT_REVISION_PINS);
                if pins
                    .find_one(doc! { "revision_id": &revision_id })
                    .session(&mut *session)
                    .await?
                    .is_some()
                {
                    return Ok(false);
                }
                pins.insert_one(doc! {
                    "revision_id": &revision_id,
                    "pinned_by": &actor,
                    "pinned_at_unix_ms": now,
                })
                .session(&mut *session)
                .await?;
                insert_gamescript_audit_in_session(
                    database,
                    session,
                    &actor,
                    AUDIT_ACTION_PIN,
                    &revision_id,
                    &GameScriptAuditContext::new(),
                    now,
                )
                .await?;
                Ok(true)
            })
        })
        .await
    }

    async fn unpin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            Box::pin(async move {
                let removed = database
                    .collection::<Document>(GAMESCRIPT_REVISION_PINS)
                    .delete_one(doc! { "revision_id": &revision_id })
                    .session(&mut *session)
                    .await?
                    .deleted_count
                    > 0;
                if removed {
                    insert_gamescript_audit_in_session(
                        database,
                        session,
                        &actor,
                        AUDIT_ACTION_UNPIN,
                        &revision_id,
                        &GameScriptAuditContext::new(),
                        now,
                    )
                    .await?;
                }
                Ok(removed)
            })
        })
        .await
    }

    async fn allocate_activation_generation(
        &self,
        scope: &str,
        revision_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptActivation> {
        if scope.is_empty() {
            return Err(AppError::validation(
                "gamescript activation scope must not be empty",
            ));
        }
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let scope = scope.to_owned();
        let revision_id = revision_id.to_owned();
        let actor = actor.to_owned();
        let context = context.clone();
        let now = scheduler_i64(now.unix_millis(), "gamescript timestamp")?;
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            let scope = scope.clone();
            let revision_id = revision_id.clone();
            let actor = actor.clone();
            let context = context.clone();
            Box::pin(async move {
                // Roll-forward and rollback share this gate: the target must be
                // an existing, non-pruned revision before a generation is spent.
                if !gamescript_touch_revision_in_session(database, session, &revision_id).await? {
                    return Err(revision_not_found(&revision_id).into());
                }
                let counter = database
                    .collection::<Document>(GAMESCRIPT_ACTIVATION_GENERATIONS)
                    .find_one_and_update(
                        doc! { "scope": &scope },
                        doc! { "$inc": { "current_generation": 1_i64 } },
                    )
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| {
                        SchedulerTransactionError::App(AppError::internal(
                            "gamescript generation upsert returned no document",
                        ))
                    })?;
                let generation = counter.get_i64("current_generation").map_err(|_| {
                    SchedulerTransactionError::App(AppError::internal(
                        "invalid gamescript generation document",
                    ))
                })?;
                database
                    .collection::<Document>(GAMESCRIPT_ACTIVATIONS)
                    .insert_one(doc! {
                        "scope": &scope,
                        "generation": generation,
                        "revision_id": &revision_id,
                        "activated_by": &actor,
                        "activated_at_unix_ms": now,
                    })
                    .session(&mut *session)
                    .await?;
                let activation = GameScriptActivation {
                    scope: scope.clone(),
                    generation: u64::try_from(generation).map_err(|_| {
                        SchedulerTransactionError::App(AppError::internal(
                            "invalid gamescript activation generation",
                        ))
                    })?,
                    revision_id: revision_id.clone(),
                    activated_by: actor.clone(),
                    activated_at: TimestampMillis::from_unix_millis(now as u64),
                };
                let details = activation_audit_details(&activation, &context);
                insert_gamescript_audit_in_session(
                    database,
                    session,
                    &actor,
                    AUDIT_ACTION_ACTIVATE,
                    &revision_id,
                    &details,
                    now,
                )
                .await?;
                insert_gamescript_outbox_in_session(
                    database,
                    session,
                    GameScriptOutboxKind::ActivationCommitted,
                    Some(&scope),
                    &revision_id,
                    Some(generation),
                    now,
                )
                .await?;
                Ok(activation)
            })
        })
        .await
    }

    async fn current_activation(&self, scope: &str) -> AppResult<Option<GameScriptActivation>> {
        self.database
            .collection::<Document>(GAMESCRIPT_ACTIVATIONS)
            .find_one(doc! { "scope": scope })
            .sort(doc! { "generation": -1 })
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(mongo_gamescript_activation)
            .transpose()
    }

    async fn list_activations(
        &self,
        scope: &str,
        limit: usize,
    ) -> AppResult<Vec<GameScriptActivation>> {
        validate_limit(limit)?;
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_ACTIVATIONS)
            .find(doc! { "scope": scope })
            .sort(doc! { "generation": -1 })
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?;
        let mut activations = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            activations.push(mongo_gamescript_activation(&document)?);
        }
        Ok(activations)
    }

    async fn prune_drafts(
        &self,
        updated_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let cutoff = scheduler_i64(updated_before.unix_millis(), "gamescript timestamp")?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            Box::pin(async move {
                let drafts = database.collection::<Document>(GAMESCRIPT_DRAFTS);
                let mut cursor = drafts
                    .find(doc! { "updated_at_unix_ms": { "$lt": cutoff } })
                    .sort(doc! { "updated_at_unix_ms": 1, "draft_id": 1 })
                    .limit(limit)
                    .session(&mut *session)
                    .await?;
                let stale = cursor.stream(&mut *session).try_collect::<Vec<_>>().await?;
                let ids = stale
                    .iter()
                    .map(|document| gamescript_doc_str(document, "draft_id"))
                    .collect::<AppResult<Vec<_>>>()?;
                if ids.is_empty() {
                    return Ok(0);
                }
                let deleted = drafts
                    .delete_many(doc! { "draft_id": { "$in": &ids } })
                    .session(&mut *session)
                    .await?
                    .deleted_count;
                Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
            })
        })
        .await
    }

    async fn prune_revisions(
        &self,
        created_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let cutoff = scheduler_i64(created_before.unix_millis(), "gamescript timestamp")?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        gamescript_transaction(&self.client, &self.database, move |database, session| {
            Box::pin(async move {
                // Snapshot-isolated candidate selection: pinned and
                // activation-referenced revisions are excluded. Snapshot reads
                // alone would NOT close the race — MongoDB transactions abort
                // only on write-write document conflicts, and there is no
                // foreign-key backstop — so every operation that must keep its
                // revision alive (activation, pin, diagnostic append, submit
                // dedupe) WRITES the revision document via
                // `gamescript_touch_revision_in_session`. The deletes below
                // therefore conflict with any such concurrent commit; the
                // losing side replays and observes the other's outcome. That
                // write-conflict protocol is the enforcement point here.
                let revisions = database.collection::<Document>(GAMESCRIPT_REVISIONS);
                let mut cursor = revisions
                    .find(doc! { "created_at_unix_ms": { "$lt": cutoff } })
                    .sort(doc! { "created_at_unix_ms": 1, "revision_id": 1 })
                    .session(&mut *session)
                    .await?;
                let stale = cursor.stream(&mut *session).try_collect::<Vec<_>>().await?;
                let mut candidates = Vec::new();
                for document in &stale {
                    candidates.push(gamescript_doc_str(document, "revision_id")?);
                }
                let mut protected = std::collections::BTreeSet::new();
                let mut pins = database
                    .collection::<Document>(GAMESCRIPT_REVISION_PINS)
                    .find(doc! { "revision_id": { "$in": &candidates } })
                    .session(&mut *session)
                    .await?;
                for document in pins.stream(&mut *session).try_collect::<Vec<_>>().await? {
                    protected.insert(gamescript_doc_str(&document, "revision_id")?);
                }
                let mut activations = database
                    .collection::<Document>(GAMESCRIPT_ACTIVATIONS)
                    .find(doc! { "revision_id": { "$in": &candidates } })
                    .session(&mut *session)
                    .await?;
                for document in activations
                    .stream(&mut *session)
                    .try_collect::<Vec<_>>()
                    .await?
                {
                    protected.insert(gamescript_doc_str(&document, "revision_id")?);
                }
                let prunable = candidates
                    .into_iter()
                    .filter(|candidate| !protected.contains(candidate))
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                    .collect::<Vec<_>>();
                if prunable.is_empty() {
                    return Ok(0);
                }
                database
                    .collection::<Document>(GAMESCRIPT_REVISION_DIAGNOSTICS)
                    .delete_many(doc! { "revision_id": { "$in": &prunable } })
                    .session(&mut *session)
                    .await?;
                let deleted = revisions
                    .delete_many(doc! { "revision_id": { "$in": &prunable } })
                    .session(&mut *session)
                    .await?
                    .deleted_count;
                Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
            })
        })
        .await
    }

    async fn audit_log(&self, limit: usize) -> AppResult<Vec<GameScriptAuditRecord>> {
        validate_limit(limit)?;
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_AUDIT)
            .find(doc! {})
            .sort(doc! { "created_at_unix_ms": -1, "audit_id": -1 })
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            records.push(mongo_gamescript_audit(&document)?);
        }
        Ok(records)
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<GameScriptOutboxRecord>> {
        validate_limit(limit)?;
        let mut cursor = self
            .database
            .collection::<Document>(GAMESCRIPT_OUTBOX)
            .find(doc! {})
            .sort(doc! { "created_at_unix_ms": 1, "outbox_id": 1 })
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?;
        let mut records = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(mongo_error)? {
            records.push(mongo_gamescript_outbox(&document)?);
        }
        Ok(records)
    }

    async fn acknowledge_outbox(&self, outbox_id: u64) -> AppResult<bool> {
        let Ok(outbox_id) = i64::try_from(outbox_id) else {
            return Ok(false);
        };
        Ok(self
            .database
            .collection::<Document>(GAMESCRIPT_OUTBOX)
            .delete_one(doc! { "outbox_id": outbox_id })
            .await
            .map_err(mongo_error)?
            .deleted_count
            > 0)
    }
}

impl MongoNotificationsRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self { client, database }
    }
}

impl MongoWalletRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self { client, database }
    }
}

impl MongoPurchasesRepository {
    fn pooled(database: Database) -> Self {
        Self { database }
    }
}

impl MongoChatRepository {
    fn pooled(client: Client, database: Database) -> Self {
        Self {
            client,
            database,
            session: None,
        }
    }

    fn transactional(
        client: Client,
        database: Database,
        session: Arc<tokio::sync::Mutex<Option<ClientSession>>>,
    ) -> Self {
        Self {
            client,
            database,
            session: Some(session),
        }
    }

    /// Run a replayable chat operation. Pooled repositories replay the whole
    /// closure for `TransientTransactionError` and retry only commit for
    /// `UnknownTransactionCommitResult` (at most three attempts, 20ms
    /// exponential backoff). A session-bound repository is already inside the
    /// caller's UnitOfWork, so it invokes the closure once on that session and
    /// leaves commit/rollback to the UnitOfWork owner.
    pub async fn with_transaction<T, F>(&self, mut work: F) -> AppResult<T>
    where
        T: Send,
        F: for<'a> FnMut(
            &'a Database,
            &'a mut ClientSession,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, mongodb::error::Error>> + Send + 'a>,
        >,
    {
        if let Some(session) = &self.session {
            let mut guard = session.lock().await;
            let session = guard
                .as_mut()
                .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
            return work(&self.database, session).await.map_err(mongo_error);
        }
        run_mongo_transaction(&self.client, &self.database, work).await
    }
}

#[derive(Debug)]
enum MongoChatTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}
impl From<AppError> for MongoChatTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}
impl From<mongodb::error::Error> for MongoChatTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

async fn chat_transaction<T, F>(
    client: &Client,
    database: &Database,
    bound_session: Option<&Arc<tokio::sync::Mutex<Option<ClientSession>>>>,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoChatTransactionError>> + Send + 'a>,
    >,
{
    if let Some(cell) = bound_session {
        let mut guard = cell.lock().await;
        let session = guard
            .as_mut()
            .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
        return work(database, session).await.map_err(|error| match error {
            MongoChatTransactionError::App(error) => error,
            MongoChatTransactionError::Mongo(error) => mongo_error(error),
        });
    }
    for attempt in 0..TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(MongoChatTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(MongoChatTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoChatTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded chat transaction retry returns or continues")
}

fn chat_canonical_channel_from_doc(
    row: Document,
    canonical_key: String,
    requested_type: ChannelType,
) -> AppResult<ChatChannel> {
    let channel_type = ChannelType::from_token(
        row.get_str("channel_type")
            .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?,
    )?;
    if channel_type != requested_type {
        return Err(AppError::conflict("chat descriptor type conflict"));
    }
    Ok(ChatChannel {
        id: row
            .get_str("channel_id")
            .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?
            .to_owned(),
        channel_type,
        canonical_key,
    })
}

fn chat_message_from_doc(doc: &Document) -> AppResult<ChatMessage> {
    let positive = |name: &str| -> AppResult<u64> {
        u64::try_from(
            doc.get_i64(name)
                .map_err(|_| AppError::internal("invalid MongoDB chat message"))?,
        )
        .map_err(|_| AppError::internal("invalid MongoDB chat message"))
    };
    Ok(ChatMessage {
        id: positive("id")?,
        sender: doc
            .get_str("sender")
            .map_err(|_| AppError::internal("invalid MongoDB chat message"))?
            .to_owned(),
        content: doc
            .get_str("content")
            .map_err(|_| AppError::internal("invalid MongoDB chat message"))?
            .to_owned(),
        created_at_unix_ms: positive("created_at_unix_ms")?,
        updated_at_unix_ms: positive("updated_at_unix_ms")?,
        revision: positive("revision")?,
        last_event_id: positive("last_event_id")?,
        deleted: doc
            .get_bool("deleted")
            .map_err(|_| AppError::internal("invalid MongoDB chat message"))?,
    })
}

fn chat_i64(value: u64, name: &'static str) -> AppResult<i64> {
    i64::try_from(value)
        .map_err(|_| AppError::validation(format!("{name} is outside MongoDB range")))
}

fn chat_outbox_from_doc(doc: &Document) -> AppResult<ChatDeliveryOutboxRecord> {
    Ok(ChatDeliveryOutboxRecord {
        channel_id: doc
            .get_str("channel_id")
            .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?
            .to_owned(),
        event_id: u64::try_from(
            doc.get_i64("event_id")
                .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        )
        .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        authority_epoch: u64::try_from(
            doc.get_i64("authority_epoch")
                .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        )
        .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        payload: doc
            .get_str("payload")
            .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?
            .to_owned(),
        created_at: TimestampMillis::from_unix_millis(
            u64::try_from(
                doc.get_i64("created_at_unix_ms")
                    .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
            )
            .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        ),
        expires_at: TimestampMillis::from_unix_millis(
            u64::try_from(
                doc.get_i64("expires_at_unix_ms")
                    .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
            )
            .map_err(|_| AppError::internal("invalid MongoDB chat outbox"))?,
        ),
    })
}

fn chat_audit_doc(audit: &ChatModerationAudit) -> AppResult<Document> {
    Ok(
        doc! {"occurred_at_unix_ms": chat_i64(audit.occurred_at_unix_ms, "chat moderation timestamp")?, "actor_kind": &audit.actor_kind, "actor_id_hash": &audit.actor_id_hash, "action": &audit.action, "reason_code": &audit.reason_code, "channel_id_hash": &audit.channel_id_hash, "message_id": chat_i64(audit.message_id, "chat moderation message id")?, "author_id_hash": &audit.author_id_hash, "authority_epoch": chat_i64(audit.authority_epoch, "chat access epoch")?, "correlation_id": &audit.correlation_id, "node_id": &audit.node_id},
    )
}

#[allow(clippy::too_many_arguments)]
async fn insert_chat_event(
    db: &Database,
    session: &mut ClientSession,
    channel: &str,
    event_id: i64,
    message_id: i64,
    revision: i64,
    event_type: &str,
    now: i64,
) -> Result<(), mongodb::error::Error> {
    db.collection::<Document>(CHAT_EVENTS).insert_one(doc! {"channel_id": channel, "event_id": event_id, "message_id": message_id, "revision": revision, "event_type": event_type, "occurred_at_unix_ms": now}).session(session).await?;
    Ok(())
}

async fn chat_epoch(
    database: &Database,
    session: &mut ClientSession,
    access_key: &str,
) -> Result<u64, MongoChatTransactionError> {
    let row = database
        .collection::<Document>(CHAT_ACCESS_EPOCHS)
        .find_one(doc! {"access_key": access_key})
        .session(session)
        .await?;
    row.map(|d| {
        u64::try_from(d.get_i64("epoch").unwrap_or(0))
            .map_err(|_| AppError::internal("invalid chat access epoch").into())
    })
    .unwrap_or(Ok(0))
}

impl MongoChatRepository {
    #[allow(clippy::too_many_arguments, clippy::collapsible_if)]
    async fn post_inner(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        authorization: Option<(&str, u64)>,
        delivery: Option<ChatDeliveryRequest>,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let sender = chat_id(sender, "chat sender")?.to_owned();
        // Raw channel-post callers predate canonical descriptors. Give their
        // implicit descriptor a private, deterministic key so the unique
        // canonical-key index still permits more than one such channel.
        let canonical_key = format!("implicit:{channel}");
        let content = content.to_owned();
        let now = chat_timestamp(now)?;
        let authorization = match authorization {
            Some((key, epoch)) => Some((chat_id(key, "chat access key")?.to_owned(), epoch)),
            None => None,
        };
        chat_transaction(&self.client, &self.database, self.session.as_ref(), move |db,session| { let channel=channel.clone();let canonical_key=canonical_key.clone();let sender=sender.clone();let content=content.clone();let authorization=authorization.clone();let delivery=delivery.clone();Box::pin(async move {
            if let Some((key, expected))=authorization { if chat_epoch(db,session,&key).await? != expected { return Err(AppError::permission("chat authorization is no longer current").into()); } }
            let channels=db.collection::<Document>(CHAT_CHANNELS); let messages=db.collection::<Document>(CHAT_MESSAGES);
            // Include the requested type in the update predicate.  An existing
            // channel with another type must not receive either the sequence
            // increment or a message; the channel_id unique index turns that
            // attempted upsert into a conflict without changing the document.
            let row=match channels.find_one_and_update(doc!{"channel_id":&channel,"channel_type":channel_type.as_str()},doc!{"$inc":{"next_id":1_i64,"next_event_id":1_i64},"$set":{"last_activity_unix_ms":now},"$setOnInsert":{"channel_id":&channel,"canonical_key":&canonical_key,"channel_type":channel_type.as_str()}}).upsert(true).return_document(ReturnDocument::After).session(&mut *session).await {
                Ok(Some(row)) => row,
                Ok(None) => return Err(AppError::internal("chat channel upsert returned no document").into()),
                Err(error) if duplicate(&error) => return Err(AppError::conflict("chat channel type conflict").into()),
                Err(error) => return Err(error.into()),
            };
            let id=row.get_i64("next_id").map_err(|_|AppError::internal("invalid MongoDB chat channel"))?;let event=row.get_i64("next_event_id").map_err(|_|AppError::internal("invalid MongoDB chat channel"))?;
            messages.insert_one(doc!{"channel_id":&channel,"id":id,"sender":sender.clone(),"content":content.clone(),"created_at_unix_ms":now,"updated_at_unix_ms":now,"revision":1_i64,"last_event_id":event,"deleted":false}).session(&mut *session).await?;
            insert_chat_event(db, session, &channel, event, id, 1, "message_sent", now).await?;
            let watermark=id.saturating_sub(i64::try_from(capacity.max(1)).unwrap_or(i64::MAX)); if watermark>0 { messages.delete_many(doc!{"channel_id":&channel,"id":{"$lte":watermark}}).session(&mut *session).await?; }
            let message=chat_message_from_doc(&doc!{"id":id,"sender":sender,"content":content,"created_at_unix_ms":now,"updated_at_unix_ms":now,"revision":1_i64,"last_event_id":event,"deleted":false})?;
            if let Some(delivery)=delivery { db.collection::<Document>(CHAT_DELIVERY_OUTBOX).insert_one(doc!{"channel_id":&channel,"event_id":event,"authority_epoch":chat_i64(delivery.authority_epoch,"chat delivery authority epoch")?,"payload":serialize_delivery_event(&channel,channel_type,delivery.event_type,&message)?,"created_at_unix_ms":now,"expires_at_unix_ms":chat_timestamp(delivery.expires_at)?}).session(&mut *session).await?; }
            Ok(message)
        })}).await
    }
    #[allow(clippy::collapsible_if)]
    async fn history_inner(
        &self,
        channel: &str,
        limit: usize,
        before: Option<u64>,
        authorization: Option<(&str, u64)>,
    ) -> AppResult<Vec<ChatMessage>> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let authorization = match authorization {
            Some((key, epoch)) => Some((chat_id(key, "chat access key")?.to_owned(), epoch)),
            None => None,
        };
        chat_transaction(
            &self.client,
            &self.database,
            self.session.as_ref(),
            move |database, session| {
                let channel = channel.clone();
                let authorization = authorization.clone();
                Box::pin(async move {
                    if let Some((key, expected)) = authorization {
                        if chat_epoch(database, session, &key).await? != expected {
                            return Err(AppError::permission("chat channel unavailable").into());
                        }
                    }
                    let mut query = doc! {"channel_id": &channel};
                    if let Some(before) = before {
                        query.insert("id", doc! {"$lt": chat_i64(before, "chat cursor")?});
                    }
                    let messages = database.collection::<Document>(CHAT_MESSAGES);
                    let mut find = messages.find(query).sort(doc! {"id": -1});
                    if limit > 0 {
                        find = find.limit(i64::try_from(limit).unwrap_or(i64::MAX));
                    }
                    let documents = find
                        .session(&mut *session)
                        .await?
                        .stream(session)
                        .try_collect::<Vec<_>>()
                        .await?;
                    documents
                        .iter()
                        .map(chat_message_from_doc)
                        .collect::<AppResult<Vec<_>>>()
                        .map_err(Into::into)
                })
            },
        )
        .await
    }
    #[allow(clippy::collapsible_if)]
    async fn edit_inner(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        authorization: Option<(&str, u64)>,
        delivery: Option<(ChatDeliveryRequest, ChannelType)>,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let content = content.to_owned();
        let id = chat_i64(id, "chat message id")?;
        let now = chat_timestamp(now)?;
        let authorization = match authorization {
            Some((key, epoch)) => Some((chat_id(key, "chat access key")?.to_owned(), epoch)),
            None => None,
        };
        chat_transaction(&self.client,&self.database,self.session.as_ref(),move|db,session|{let channel=channel.clone();let content=content.clone();let authorization=authorization.clone();let delivery=delivery.clone();Box::pin(async move {if let Some((key,expected))=authorization {if chat_epoch(db,session,&key).await?!=expected{return Err(AppError::permission("chat authorization is no longer current").into())}}let channels=db.collection::<Document>(CHAT_CHANNELS);if channels.find_one(doc!{"channel_id":&channel}).session(&mut *session).await?.is_none(){return Err(channel_not_found().into())}let seq=channels.find_one_and_update(doc!{"channel_id":&channel},doc!{"$inc":{"next_event_id":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(channel_not_found)?;let event=seq.get_i64("next_event_id").map_err(|_|AppError::internal("invalid MongoDB chat channel"))?;let row=db.collection::<Document>(CHAT_MESSAGES).find_one_and_update(doc!{"channel_id":&channel,"id":id,"deleted":false},doc!{"$set":{"content":content,"updated_at_unix_ms":now,"last_event_id":event},"$inc":{"revision":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(message_not_found)?;let message=chat_message_from_doc(&row)?;if let Some((delivery,channel_type))=delivery {let expires=chat_timestamp(delivery.expires_at)?;if expires<=now{return Err(AppError::validation("chat delivery outbox expiry must be after creation").into())}let payload=serialize_delivery_event(&channel,channel_type,delivery.event_type,&message)?;db.collection::<Document>(CHAT_DELIVERY_OUTBOX).insert_one(doc!{"channel_id":&channel,"event_id":event,"authority_epoch":chat_i64(delivery.authority_epoch,"chat delivery authority epoch")?,"payload":payload,"created_at_unix_ms":now,"expires_at_unix_ms":expires}).session(&mut *session).await?;}insert_chat_event(db,session,&channel,event,id,message.revision as i64,"message_updated",now).await?;Ok(message)})}).await
    }
    #[allow(clippy::collapsible_if)]
    async fn delete_inner(
        &self,
        channel: &str,
        id: u64,
        authorization: Option<(&str, u64)>,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let id = chat_i64(id, "chat message id")?;
        let now = chat_timestamp(now)?;
        let authorization = match authorization {
            Some((key, epoch)) => Some((chat_id(key, "chat access key")?.to_owned(), epoch)),
            None => None,
        };
        chat_transaction(&self.client, &self.database, self.session.as_ref(), move |db,session|{let channel=channel.clone();let authorization=authorization.clone();Box::pin(async move {if let Some((key,expected))=authorization {if chat_epoch(db,session,&key).await?!=expected{return Err(AppError::permission("chat authorization is no longer current").into());}}let channels=db.collection::<Document>(CHAT_CHANNELS);if channels.find_one(doc!{"channel_id":&channel}).session(&mut *session).await?.is_none(){return Err(channel_not_found().into())}let messages=db.collection::<Document>(CHAT_MESSAGES);let existing=messages.find_one(doc!{"channel_id":&channel,"id":id}).session(&mut *session).await?.ok_or_else(message_not_found)?;if existing.get_bool("deleted").unwrap_or(false){return Ok(false)}let seq=channels.find_one_and_update(doc!{"channel_id":&channel},doc!{"$inc":{"next_event_id":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(channel_not_found)?;let event=seq.get_i64("next_event_id").map_err(|_|AppError::internal("invalid MongoDB chat channel"))?;let row=messages.find_one_and_update(doc!{"channel_id":&channel,"id":id,"deleted":false},doc!{"$set":{"content":"","deleted":true,"updated_at_unix_ms":now,"last_event_id":event},"$inc":{"revision":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(message_not_found)?;insert_chat_event(db,session,&channel,event,id,row.get_i64("revision").map_err(|_|AppError::internal("invalid MongoDB chat message"))?,"message_deleted",now).await?;Ok(true)})}).await
    }

    #[allow(clippy::collapsible_if)]
    async fn moderate_delete_inner(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        authorization: Option<(&str, u64)>,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let id = chat_i64(id, "chat message id")?;
        let audit = chat_audit_doc(audit)?;
        let now = chat_timestamp(now)?;
        let authorization = match authorization {
            Some((key, epoch)) => Some((chat_id(key, "chat access key")?.to_owned(), epoch)),
            None => None,
        };
        chat_transaction(&self.client, &self.database, self.session.as_ref(), move |db, session| {
            let channel = channel.clone();
            let audit = audit.clone();
            let authorization = authorization.clone();
            Box::pin(async move {
                if let Some((key, expected)) = authorization {
                    if chat_epoch(db, session, &key).await? != expected {
                        return Err(AppError::permission("chat authorization is no longer current").into());
                    }
                }
                let channels = db.collection::<Document>(CHAT_CHANNELS);
                if channels
                    .find_one(doc! {"channel_id": &channel})
                    .session(&mut *session)
                    .await?
                    .is_none()
                {
                    return Err(channel_not_found().into());
                }
                let messages = db.collection::<Document>(CHAT_MESSAGES);
                let existing = messages
                    .find_one(doc! {"channel_id": &channel, "id": id})
                    .session(&mut *session)
                    .await?
                    .ok_or_else(message_not_found)?;
                if existing.get_bool("deleted").unwrap_or(false) {
                    return Ok(false);
                }
                let seq = channels
                    .find_one_and_update(
                        doc! {"channel_id": &channel},
                        doc! {"$inc": {"next_event_id": 1_i64}},
                    )
                    .return_document(ReturnDocument::After)
                    .session(&mut *session)
                    .await?
                    .ok_or_else(channel_not_found)?;
                let event = seq
                    .get_i64("next_event_id")
                    .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?;
                let row = messages
                    .find_one_and_update(
                        doc! {"channel_id": &channel, "id": id, "deleted": false},
                        doc! {"$set": {"content": "", "deleted": true, "updated_at_unix_ms": now}, "$inc": {"revision": 1_i64}},
                    )
                    .return_document(ReturnDocument::After)
                    .session(&mut *session)
                    .await?
                    .ok_or_else(message_not_found)?;
                insert_chat_event(
                    db,
                    session,
                    &channel,
                    event,
                    id,
                    row.get_i64("revision")
                        .map_err(|_| AppError::internal("invalid MongoDB chat message"))?,
                    "message_deleted",
                    now,
                )
                .await?;
                db.collection::<Document>(CHAT_MODERATION_AUDIT)
                    .insert_one(audit)
                    .session(&mut *session)
                    .await?;
                Ok(true)
            })
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete_with_delivery_inner(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        access_key: &str,
        expected: u64,
        delivery: ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<Option<ChatMessage>> {
        let channel = chat_id(channel, "chat channel")?.to_owned();
        let key = chat_id(access_key, "chat access key")?.to_owned();
        let id = chat_i64(id, "chat message id")?;
        let now = chat_timestamp(now)?;
        let expires = chat_timestamp(delivery.expires_at)?;
        if expires <= now {
            return Err(AppError::validation(
                "chat delivery outbox expiry must be after creation",
            ));
        }
        chat_transaction(&self.client,&self.database,self.session.as_ref(),move|db,session|{let channel=channel.clone();let key=key.clone();let delivery=delivery.clone();Box::pin(async move {if chat_epoch(db,session,&key).await?!=expected{return Err(AppError::permission("chat authorization is no longer current").into())}let channels=db.collection::<Document>(CHAT_CHANNELS);if channels.find_one(doc!{"channel_id":&channel}).session(&mut *session).await?.is_none(){return Err(channel_not_found().into())}let messages=db.collection::<Document>(CHAT_MESSAGES);let existing=messages.find_one(doc!{"channel_id":&channel,"id":id}).session(&mut *session).await?.ok_or_else(message_not_found)?;if existing.get_bool("deleted").unwrap_or(false){return Ok(None)}let seq=channels.find_one_and_update(doc!{"channel_id":&channel},doc!{"$inc":{"next_event_id":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(channel_not_found)?;let event=seq.get_i64("next_event_id").map_err(|_|AppError::internal("invalid MongoDB chat channel"))?;let row=messages.find_one_and_update(doc!{"channel_id":&channel,"id":id,"deleted":false},doc!{"$set":{"content":"","deleted":true,"updated_at_unix_ms":now,"last_event_id":event},"$inc":{"revision":1_i64}}).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(message_not_found)?;let message=chat_message_from_doc(&row)?;let payload=serialize_delivery_event(&channel,channel_type,delivery.event_type,&message)?;insert_chat_event(db,session,&channel,event,id,message.revision as i64,"message_deleted",now).await?;db.collection::<Document>(CHAT_DELIVERY_OUTBOX).insert_one(doc!{"channel_id":&channel,"event_id":event,"authority_epoch":chat_i64(delivery.authority_epoch,"chat delivery authority epoch")?,"payload":payload,"created_at_unix_ms":now,"expires_at_unix_ms":expires}).session(&mut *session).await?;Ok(Some(message))})}).await
    }
}

#[async_trait]
impl ChatRepository for MongoChatRepository {
    async fn resolve_canonical_channel(
        &self,
        canonical_key: &str,
        channel_type: ChannelType,
        now: TimestampMillis,
    ) -> AppResult<ChatChannel> {
        let canonical_key = chat_id(canonical_key, "chat canonical key")?.to_owned();
        let now = chat_timestamp(now)?;
        let channels = self.database.collection::<Document>(CHAT_CHANNELS);
        let query = doc! {"canonical_key": &canonical_key};

        let existing = match &self.session {
            None => channels
                .find_one(query.clone())
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                channels
                    .find_one(query.clone())
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        if let Some(row) = existing {
            return chat_canonical_channel_from_doc(row, canonical_key, channel_type);
        }

        // This is deliberately a single-document insert rather than a
        // transaction: unique canonical_key is the compare-and-set primitive.
        // A concurrent creator can win after our read; in that case reread and
        // return the winner instead of surfacing E11000 as a database error.
        let id = new_opaque_channel_id()?;
        let document = doc! {"channel_id": &id, "canonical_key": &canonical_key, "channel_type": channel_type.as_str(), "next_id": 0_i64, "next_event_id": 0_i64, "last_activity_unix_ms": now};
        let inserted = match &self.session {
            None => channels.insert_one(document).await,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                channels.insert_one(document).session(session).await
            }
        };
        match inserted {
            Ok(_) => Ok(ChatChannel {
                id,
                channel_type,
                canonical_key,
            }),
            Err(error) if duplicate(&error) => {
                let row = match &self.session {
                    None => channels.find_one(query).await.map_err(mongo_error)?,
                    Some(cell) => {
                        let mut guard = cell.lock().await;
                        let session = guard.as_mut().ok_or_else(|| {
                            AppError::internal("MongoDB transaction is already closed")
                        })?;
                        channels
                            .find_one(query)
                            .session(session)
                            .await
                            .map_err(mongo_error)?
                    }
                }
                .ok_or_else(|| mongo_error(error))?;
                chat_canonical_channel_from_doc(row, canonical_key, channel_type)
            }
            Err(error) => Err(mongo_error(error)),
        }
    }
    async fn current_access_epoch(&self, access_key: &str) -> AppResult<u64> {
        let key = chat_id(access_key, "chat access key")?.to_owned();
        let epochs = self.database.collection::<Document>(CHAT_ACCESS_EPOCHS);
        let row = match &self.session {
            None => epochs
                .find_one(doc! {"access_key": &key})
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                epochs
                    .find_one(doc! {"access_key": &key})
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        match row {
            Some(row) => u64::try_from(row.get_i64("epoch").unwrap_or(0))
                .map_err(|_| AppError::internal("invalid chat access epoch")),
            None => Ok(0),
        }
    }
    async fn advance_access_epoch(&self, access_key: &str, now: TimestampMillis) -> AppResult<u64> {
        let key = chat_id(access_key, "chat access key")?.to_owned();
        let now = chat_timestamp(now)?;
        chat_transaction(&self.client, &self.database, self.session.as_ref(), move |db, session| { let key=key.clone(); Box::pin(async move { let row=db.collection::<Document>(CHAT_ACCESS_EPOCHS).find_one_and_update(doc!{"access_key":&key}, doc!{"$inc":{"epoch":1_i64},"$set":{"updated_at_unix_ms":now},"$setOnInsert":{"access_key":&key}}).upsert(true).return_document(ReturnDocument::After).session(&mut *session).await?.ok_or_else(|| AppError::internal("chat access epoch upsert returned no document"))?; u64::try_from(row.get_i64("epoch").unwrap_or(0)).map_err(|_| AppError::internal("invalid chat access epoch").into()) }) }).await
    }
    async fn post_message(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        self.post_inner(
            channel,
            channel_type,
            sender,
            content,
            capacity,
            None,
            None,
            now,
        )
        .await
        .map(|m| m.id)
    }
    async fn post_message_authorized(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        self.post_inner(
            channel,
            channel_type,
            sender,
            content,
            capacity,
            Some((access_key, expected_access_epoch)),
            None,
            now,
        )
        .await
        .map(|m| m.id)
    }
    async fn post_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        sender: &str,
        content: &str,
        capacity: usize,
        access_key: &str,
        expected: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        let message = self
            .post_inner(
                channel,
                channel_type,
                sender,
                content,
                capacity,
                Some((access_key, expected)),
                Some(delivery.clone()),
                now,
            )
            .await?;
        Ok(message)
    }
    async fn list_channels(
        &self,
        filter: Option<&str>,
        limit: usize,
    ) -> AppResult<Vec<ChannelSummary>> {
        let channels = self.database.collection::<Document>(CHAT_CHANNELS);
        let documents = match &self.session {
            None => channels
                .find(doc! {})
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                channels
                    .find(doc! {})
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let rows = documents
            .iter()
            .map(|document| {
                let channel_type = ChannelType::from_token(
                    document
                        .get_str("channel_type")
                        .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?,
                )?;
                Ok(ChannelSummary {
                    channel: document
                        .get_str("channel_id")
                        .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?
                        .to_owned(),
                    channel_type: channel_type.as_str(),
                    messages: u64::try_from(document.get_i64("next_id").unwrap_or(0))
                        .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?,
                    last_activity_unix_ms: u64::try_from(
                        document.get_i64("last_activity_unix_ms").unwrap_or(0),
                    )
                    .map_err(|_| AppError::internal("invalid MongoDB chat channel"))?,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;
        Ok(finish_channel_listing(rows, filter, limit))
    }
    async fn channel_count(&self) -> AppResult<usize> {
        let count = match &self.session {
            None => self
                .database
                .collection::<Document>(CHAT_CHANNELS)
                .count_documents(doc! {})
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>(CHAT_CHANNELS)
                    .count_documents(doc! {})
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        usize::try_from(count).map_err(|_| AppError::internal("chat channel count out of range"))
    }
    async fn channel_history(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<Vec<ChatMessage>> {
        self.history_inner(channel, limit, before_id, None).await
    }
    async fn channel_history_authorized(
        &self,
        channel: &str,
        limit: usize,
        before_id: Option<u64>,
        access_key: &str,
        expected_access_epoch: u64,
    ) -> AppResult<Vec<ChatMessage>> {
        self.history_inner(
            channel,
            limit,
            before_id,
            Some((access_key, expected_access_epoch)),
        )
        .await
    }
    async fn edit_message(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        self.edit_inner(channel, id, content, None, None, now).await
    }
    async fn edit_message_authorized(
        &self,
        channel: &str,
        id: u64,
        content: &str,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        self.edit_inner(
            channel,
            id,
            content,
            Some((access_key, expected_access_epoch)),
            None,
            now,
        )
        .await
    }
    async fn edit_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        content: &str,
        access_key: &str,
        expected: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<ChatMessage> {
        self.edit_inner(
            channel,
            id,
            content,
            Some((access_key, expected)),
            Some((delivery.clone(), channel_type)),
            now,
        )
        .await
    }
    async fn delete_message(
        &self,
        channel: &str,
        id: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        self.delete_inner(channel, id, None, now).await
    }
    async fn delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        access_key: &str,
        expected_access_epoch: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        self.delete_inner(channel, id, Some((access_key, expected_access_epoch)), now)
            .await
    }
    async fn delete_message_authorized_with_delivery(
        &self,
        channel: &str,
        channel_type: ChannelType,
        id: u64,
        access_key: &str,
        expected: u64,
        delivery: &ChatDeliveryRequest,
        now: TimestampMillis,
    ) -> AppResult<Option<ChatMessage>> {
        self.delete_with_delivery_inner(
            channel,
            channel_type,
            id,
            access_key,
            expected,
            delivery.clone(),
            now,
        )
        .await
    }
    async fn moderate_delete_message(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        self.moderate_delete_inner(channel, id, audit, None, now)
            .await
    }
    async fn moderate_delete_message_authorized(
        &self,
        channel: &str,
        id: u64,
        audit: &ChatModerationAudit,
        access_key: &str,
        expected: u64,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        self.moderate_delete_inner(channel, id, audit, Some((access_key, expected)), now)
            .await
    }
    async fn cleanup_moderation_audit(
        &self,
        before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        };
        let collection = self.database.collection::<Document>(CHAT_MODERATION_AUDIT);
        let query = doc! {"occurred_at_unix_ms":{"$lt":chat_timestamp(before)?}};
        let ids = match &self.session {
            None => collection
                .find(query)
                .sort(doc! {"occurred_at_unix_ms":1,"_id":1})
                .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .find(query)
                    .sort(doc! {"occurred_at_unix_ms":1,"_id":1})
                    .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?
            }
        };
        if ids.is_empty() {
            return Ok(0);
        }
        let query = doc! {"_id":{"$in":ids.into_iter().filter_map(|d|d.get_object_id("_id").ok()).collect::<Vec<_>>()}};
        let result = match &self.session {
            None => collection.delete_many(query).await.map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .delete_many(query)
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        usize::try_from(result.deleted_count)
            .map_err(|_| AppError::internal("audit cleanup count out of range"))
    }
    async fn moderation_audit_count(&self) -> AppResult<usize> {
        let collection = self.database.collection::<Document>(CHAT_MODERATION_AUDIT);
        let count = match &self.session {
            None => collection
                .count_documents(doc! {})
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .count_documents(doc! {})
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        usize::try_from(count)
            .map_err(|_| AppError::internal("chat moderation audit count out of range"))
    }
    async fn consume_rate_limits(
        &self,
        limits: &[ChatRateLimit],
        now: TimestampMillis,
    ) -> AppResult<()> {
        let now = chat_timestamp(now)?;
        for rule in limits {
            if rule.limit == 0 || rule.window_ms == 0 || rule.key.is_empty() {
                return Err(AppError::internal("invalid chat rate-limit rule"));
            }
        }
        chat_transaction(&self.client,&self.database,self.session.as_ref(),move|db,session|{let limits=limits.to_vec();Box::pin(async move { let collection=db.collection::<Document>(CHAT_RATE_LIMITS); for rule in &limits {let window=chat_i64((u64::try_from(now).map_err(|_|AppError::internal("invalid rate timestamp"))?/rule.window_ms)*rule.window_ms,"chat rate-limit window")?;let row=collection.find_one_and_update(doc!{"key":&rule.key,"window_started_unix_ms":window,"used":{"$lt":i64::from(rule.limit)}},doc!{"$inc":{"used":1_i64},"$setOnInsert":{"key":&rule.key,"window_started_unix_ms":window,"expires_at_unix_ms":window+chat_i64(rule.window_ms,"chat rate-limit window")?}}).upsert(true).return_document(ReturnDocument::After).session(&mut *session).await?;if row.is_none(){return Err(AppError::permission("CHAT_RATE_LIMITED").into());}} Ok(())})}).await
    }
    async fn cleanup_rate_limits(&self, before: TimestampMillis, limit: usize) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let collection = self.database.collection::<Document>(CHAT_RATE_LIMITS);
        let rows = collection
            .find(doc! {"expires_at_unix_ms":{"$lt":chat_timestamp(before)?}})
            .sort(doc! {"expires_at_unix_ms":1,"_id":1})
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(mongo_error)?;
        let ids = rows
            .into_iter()
            .filter_map(|d| d.get_object_id("_id").ok())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(0);
        }
        let result = collection
            .delete_many(doc! {"_id":{"$in":ids}})
            .await
            .map_err(mongo_error)?;
        usize::try_from(result.deleted_count)
            .map_err(|_| AppError::internal("rate-limit cleanup count out of range"))
    }
    async fn stage_delivery_outbox(&self, record: ChatDeliveryOutboxRecord) -> AppResult<bool> {
        if record.expires_at <= record.created_at {
            return Err(AppError::validation(
                "chat delivery outbox expiry must be after creation",
            ));
        }
        let doc = doc! {"channel_id":chat_id(&record.channel_id,"chat channel")?,"event_id":chat_i64(record.event_id,"chat delivery event id")?,"authority_epoch":chat_i64(record.authority_epoch,"chat delivery authority epoch")?,"payload":record.payload,"created_at_unix_ms":chat_timestamp(record.created_at)?,"expires_at_unix_ms":chat_timestamp(record.expires_at)?};
        let collection = self.database.collection::<Document>(CHAT_DELIVERY_OUTBOX);
        let inserted = match &self.session {
            None => collection.insert_one(doc).await,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection.insert_one(doc).session(session).await
            }
        };
        match inserted {
            Ok(_) => Ok(true),
            Err(error) if duplicate(&error) => Ok(false),
            Err(error) => Err(mongo_error(error)),
        }
    }
    async fn active_delivery_outbox(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> AppResult<Vec<ChatDeliveryOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let collection = self.database.collection::<Document>(CHAT_DELIVERY_OUTBOX);
        let query = doc! {"expires_at_unix_ms":{"$gt":chat_timestamp(now)?}};
        let documents = match &self.session {
            None => collection
                .find(query)
                .sort(doc! {"channel_id":1,"event_id":1})
                .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .find(query)
                    .sort(doc! {"channel_id":1,"event_id":1})
                    .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?
            }
        };
        documents.iter().map(chat_outbox_from_doc).collect()
    }
    async fn acknowledge_delivery_outbox(&self, channel: &str, event_id: u64) -> AppResult<bool> {
        let collection = self.database.collection::<Document>(CHAT_DELIVERY_OUTBOX);
        let query = doc! {"channel_id":chat_id(channel,"chat channel")?,"event_id":chat_i64(event_id,"chat delivery event id")?};
        let result = match &self.session {
            None => collection.delete_one(query).await.map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .delete_one(query)
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        Ok(result.deleted_count == 1)
    }
    async fn cleanup_delivery_outbox(
        &self,
        through: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let collection = self.database.collection::<Document>(CHAT_DELIVERY_OUTBOX);
        let query = doc! {"expires_at_unix_ms":{"$lte":chat_timestamp(through)?}};
        let rows = match &self.session {
            None => collection
                .find(query)
                .sort(doc! {"expires_at_unix_ms":1,"channel_id":1,"event_id":1})
                .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .find(query)
                    .sort(doc! {"expires_at_unix_ms":1,"channel_id":1,"event_id":1})
                    .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let ids = rows
            .into_iter()
            .filter_map(|d| d.get_object_id("_id").ok())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(0);
        }
        let query = doc! {"_id":{"$in":ids}};
        let result = match &self.session {
            None => collection.delete_many(query).await.map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                collection
                    .delete_many(query)
                    .session(session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        usize::try_from(result.deleted_count)
            .map_err(|_| AppError::internal("outbox cleanup count out of range"))
    }
}
#[derive(Debug)]
enum MongoNotificationsTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}

impl From<AppError> for MongoNotificationsTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}

impl From<mongodb::error::Error> for MongoNotificationsTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

fn notification_from_doc(doc: &Document) -> AppResult<Notification> {
    let id = u64::try_from(
        doc.get_i64("id")
            .map_err(|_| AppError::internal("invalid MongoDB notification"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB notification"))?;
    let created = u64::try_from(
        doc.get_i64("created_at_unix_ms")
            .map_err(|_| AppError::internal("invalid MongoDB notification"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB notification"))?;
    let content = serde_json::from_str(
        doc.get_str("content")
            .map_err(|_| AppError::internal("invalid MongoDB notification"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB notification content"))?;
    Ok(Notification {
        id,
        recipient: Recipient::from_column(doc.get_str("recipient_id").ok().map(ToOwned::to_owned)),
        subject: doc
            .get_str("subject")
            .map_err(|_| AppError::internal("invalid MongoDB notification"))?
            .to_owned(),
        content,
        code: doc
            .get_i32("code")
            .map_err(|_| AppError::internal("invalid MongoDB notification"))?,
        created_at: TimestampMillis::from_unix_millis(created),
        read: doc.get_i64("read_at_unix_ms").is_ok(),
    })
}

async fn notification_mutation<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoNotificationsTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..NOTIFICATION_TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(MongoNotificationsTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(MongoNotificationsTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < NOTIFICATION_TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoNotificationsTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..NOTIFICATION_TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < NOTIFICATION_TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await;
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < NOTIFICATION_TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded notification transaction retry either returns or continues")
}

#[async_trait]
impl NotificationsRepository for MongoNotificationsRepository {
    async fn enqueue(
        &self,
        recipient: Recipient,
        subject: &str,
        content: &serde_json::Value,
        code: i32,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<u64> {
        let (recipient_id, subject, content) = (
            recipient.user_id().map(ToOwned::to_owned),
            subject.to_owned(),
            serde_json::to_string(content)
                .map_err(|_| AppError::internal("failed to encode MongoDB notification"))?,
        );
        let now = i64::try_from(now.unix_millis())
            .map_err(|_| AppError::internal("notification timestamp out of range"))?;
        notification_mutation(&self.client, &self.database, move |database, session| {
            let (recipient_id, subject, content) =
                (recipient_id.clone(), subject.clone(), content.clone());
            Box::pin(async move {
                let notifications = database.collection::<Document>(NOTIFICATIONS);
                // Match the shared repository contract: ids are `MAX(id) + 1`
                // over retained rows. The retryable transaction serializes
                // concurrent attempts while preserving the documented reuse
                // after an operator deletes the newest row.
                let id = notifications
                    .find_one(doc! {})
                    .sort(doc! { "id": -1 })
                    .session(&mut *session)
                    .await?
                    .map(|row| row.get_i64("id").unwrap_or(0) + 1)
                    .unwrap_or(1);
                notifications
                    .insert_one(doc! {
                        "id": id, "recipient_id": recipient_id, "subject": subject,
                        "content": content, "code": code, "created_at_unix_ms": now,
                        "read_at_unix_ms": Bson::Null,
                    })
                    .session(&mut *session)
                    .await?;
                let retained = notifications
                    .count_documents(doc! {})
                    .session(&mut *session)
                    .await?;
                let evict = overflow_evictions(
                    usize::try_from(retained)
                        .map_err(|_| AppError::internal("notification count out of range"))?,
                    capacity,
                );
                if evict > 0 {
                    let oldest = notifications
                        .find(doc! {})
                        .sort(doc! { "id": 1 })
                        .limit(i64::try_from(evict).map_err(|_| {
                            AppError::internal("notification eviction count out of range")
                        })?)
                        .session(&mut *session)
                        .await?
                        .stream(session)
                        .try_collect::<Vec<_>>()
                        .await?;
                    let ids = oldest
                        .into_iter()
                        .filter_map(|doc| doc.get_i64("id").ok())
                        .collect::<Vec<_>>();
                    if !ids.is_empty() {
                        notifications
                            .delete_many(doc! { "id": { "$in": ids } })
                            .session(session)
                            .await?;
                    }
                }
                u64::try_from(id)
                    .map_err(|_| AppError::internal("notification id out of range").into())
            })
        })
        .await
    }

    async fn list(
        &self,
        user_id_filter: Option<&str>,
        limit: usize,
        before_id: Option<u64>,
    ) -> AppResult<NotificationPage> {
        let mut filter = Document::new();
        if let Some(user_id) = user_id_filter {
            filter.insert(
                "$or",
                vec![
                    doc! { "recipient_id": user_id },
                    doc! { "recipient_id": Bson::Null },
                ],
            );
        }
        let notifications = self.database.collection::<Document>(NOTIFICATIONS);
        let total = notifications
            .count_documents(filter.clone())
            .await
            .map_err(mongo_error)?;
        if let Some(before) = before_id {
            filter.insert(
                "id",
                doc! { "$lt": i64::try_from(before).map_err(|_| AppError::internal("notification cursor out of range"))? },
            );
        }
        let docs = if limit == 0 {
            Vec::new()
        } else {
            notifications
                .find(filter)
                .sort(doc! { "id": -1 })
                .limit(i64::try_from(limit).unwrap_or(i64::MAX))
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?
        };
        Ok(NotificationPage {
            items: docs
                .iter()
                .map(notification_from_doc)
                .collect::<AppResult<Vec<_>>>()?,
            total: usize::try_from(total)
                .map_err(|_| AppError::internal("notification count out of range"))?,
        })
    }

    async fn count(&self) -> AppResult<usize> {
        usize::try_from(
            self.database
                .collection::<Document>(NOTIFICATIONS)
                .count_documents(doc! {})
                .await
                .map_err(mongo_error)?,
        )
        .map_err(|_| AppError::internal("notification count out of range"))
    }

    async fn delete(&self, id: u64) -> AppResult<()> {
        let id =
            i64::try_from(id).map_err(|_| AppError::internal("notification id out of range"))?;
        let result = self
            .database
            .collection::<Document>(NOTIFICATIONS)
            .delete_one(doc! { "id": id })
            .await
            .map_err(mongo_error)?;
        if result.deleted_count == 0 {
            return Err(notification_not_found());
        }
        Ok(())
    }

    async fn mark_read(&self, id: u64, now: TimestampMillis) -> AppResult<()> {
        let id =
            i64::try_from(id).map_err(|_| AppError::internal("notification id out of range"))?;
        let now = i64::try_from(now.unix_millis())
            .map_err(|_| AppError::internal("notification timestamp out of range"))?;
        let result = self
            .database
            .collection::<Document>(NOTIFICATIONS)
            .update_one(
                // A conditional update is a CAS: the first acknowledge wins
                // the timestamp and a retry is still a successful no-op.
                doc! { "id": id, "read_at_unix_ms": Bson::Null },
                doc! { "$set": { "read_at_unix_ms": now } },
            )
            .await
            .map_err(mongo_error)?;
        if result.matched_count == 0
            && self
                .database
                .collection::<Document>(NOTIFICATIONS)
                .find_one(doc! { "id": id })
                .await
                .map_err(mongo_error)?
                .is_none()
        {
            return Err(notification_not_found());
        }
        Ok(())
    }
}

#[derive(Debug)]
enum MongoWalletTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}

impl From<AppError> for MongoWalletTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}

impl From<mongodb::error::Error> for MongoWalletTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

fn wallet_ledger_from_doc(doc: &Document) -> AppResult<LedgerEntry> {
    let seq = u64::try_from(
        doc.get_i64("id")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?,
    )
    .map_err(|_| AppError::internal("MongoDB wallet ledger id out of range"))?;
    let time_unix_ms = u64::try_from(
        doc.get_i64("created_at_unix_ms")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?,
    )
    .map_err(|_| AppError::internal("MongoDB wallet ledger timestamp out of range"))?;
    Ok(LedgerEntry {
        seq,
        user_id: doc
            .get_str("user_id")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?
            .to_owned(),
        currency: doc
            .get_str("currency")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?
            .to_owned(),
        delta: doc
            .get_i64("delta")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?,
        balance_after: doc
            .get_i64("balance_after")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?,
        reason: doc
            .get_str("reason")
            .map_err(|_| AppError::internal("invalid MongoDB wallet ledger"))?
            .to_owned(),
        time_unix_ms,
    })
}

fn purchase_from_doc(doc: &Document) -> AppResult<Purchase> {
    let validated_at_unix_ms = u64::try_from(
        doc.get_i64("validated_at_unix_ms")
            .map_err(|_| AppError::internal("invalid MongoDB purchase"))?,
    )
    .map_err(|_| AppError::internal("MongoDB purchase timestamp out of range"))?;
    let subscription_expiry_unix_ms = doc
        .get_i64("subscription_expiry_unix_ms")
        .ok()
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| AppError::internal("MongoDB purchase expiry out of range"))
        })
        .transpose()?;
    Ok(Purchase {
        transaction_id: doc
            .get_str("transaction_id")
            .map_err(|_| AppError::internal("invalid MongoDB purchase"))?
            .to_owned(),
        user_id: doc
            .get_str("user_id")
            .map_err(|_| AppError::internal("invalid MongoDB purchase"))?
            .to_owned(),
        product_id: doc
            .get_str("product_id")
            .map_err(|_| AppError::internal("invalid MongoDB purchase"))?
            .to_owned(),
        store: PurchaseStore::from_token(
            doc.get_str("store")
                .map_err(|_| AppError::internal("invalid MongoDB purchase"))?,
        )?,
        receipt_sha256: doc
            .get_str("receipt_sha256")
            .map_err(|_| AppError::internal("invalid MongoDB purchase"))?
            .to_owned(),
        validated_at_unix_ms,
        subscription_expiry_unix_ms,
    })
}

async fn wallet_mutation<T, F>(client: &Client, database: &Database, mut work: F) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoWalletTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..WALLET_TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(MongoWalletTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(MongoWalletTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < WALLET_TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoWalletTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..WALLET_TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < WALLET_TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await;
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < WALLET_TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded wallet transaction retry either returns or continues")
}

#[async_trait]
impl WalletRepository for MongoWalletRepository {
    async fn apply_change(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<LedgerEntry> {
        let user_id = user_id.to_owned();
        let currency = currency.to_owned();
        let reason = reason.to_owned();
        let created_at_unix_ms = i64::try_from(now.unix_millis())
            .map_err(|_| AppError::internal("wallet timestamp out of range"))?;
        wallet_mutation(&self.client, &self.database, move |database, session| {
            let user_id = user_id.clone();
            let currency = currency.clone();
            let reason = reason.clone();
            Box::pin(async move {
                let balances = database.collection::<Document>(WALLET_BALANCES);
                let balance = balances
                    .find_one_and_update(doc! {"user_id": &user_id, "currency": &currency}, doc! {
                        "$setOnInsert": {"user_id": &user_id, "currency": &currency, "balance": 0_i64},
                    })
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| AppError::internal("MongoDB wallet balance materialization failed"))?;
                let current = balance
                    .get_i64("balance")
                    .map_err(|_| AppError::internal("invalid MongoDB wallet balance"))?;
                let next = apply_delta(current, delta)?;

                let counter = database.collection::<Document>(GROUP_COUNTERS);
                let sequence = counter
                    .find_one_and_update(doc! {"_id": "wallet_ledger_sequence"}, doc! {"$inc": {"value": 1_i64}})
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| AppError::internal("MongoDB wallet sequence allocation failed"))?
                    .get_i64("value")
                    .map_err(|_| AppError::internal("invalid MongoDB wallet sequence"))?;

                let entry = doc! {
                    "id": sequence, "user_id": &user_id, "currency": &currency,
                    "delta": delta, "balance_after": next, "reason": &reason,
                    "created_at_unix_ms": created_at_unix_ms,
                };
                database.collection::<Document>(WALLET_LEDGER).insert_one(entry.clone()).session(&mut *session).await?;
                balances.update_one(doc! {"user_id": &user_id, "currency": &currency}, doc! {"$set": {"balance": next, "updated_at_unix_ms": created_at_unix_ms}}).session(&mut *session).await?;

                let ledger = database.collection::<Document>(WALLET_LEDGER);
                let retained = ledger.count_documents(doc! {}).session(&mut *session).await?;
                let evict = ledger_overflow(usize::try_from(retained).unwrap_or(usize::MAX), capacity);
                if evict > 0 {
                    let mut cursor = ledger
                        .find(doc! {})
                        .sort(doc! {"id": 1_i32})
                        .limit(i64::try_from(evict).unwrap_or(i64::MAX))
                        .session(&mut *session)
                        .await?;
                    let oldest: Vec<i64> = cursor
                        .stream(&mut *session)
                        .try_collect::<Vec<Document>>()
                        .await?
                        .into_iter()
                        .filter_map(|doc| doc.get_i64("id").ok())
                        .collect();
                    if !oldest.is_empty() {
                        ledger.delete_many(doc! {"id": {"$in": oldest}}).session(&mut *session).await?;
                    }
                }
                wallet_ledger_from_doc(&entry).map_err(Into::into)
            })
        }).await
    }

    async fn balances(&self, user_id: &str) -> AppResult<BTreeMap<String, i64>> {
        let docs: Vec<Document> = self
            .database
            .collection::<Document>(WALLET_BALANCES)
            .find(doc! {"user_id": user_id})
            .sort(doc! {"currency": 1_i32})
            .await
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)?;
        docs.into_iter()
            .map(|doc| {
                Ok((
                    doc.get_str("currency")
                        .map_err(|_| AppError::internal("invalid MongoDB wallet balance"))?
                        .to_owned(),
                    doc.get_i64("balance")
                        .map_err(|_| AppError::internal("invalid MongoDB wallet balance"))?,
                ))
            })
            .collect()
    }

    async fn ledger(&self, user_id: &str, limit: usize) -> AppResult<Vec<LedgerEntry>> {
        let docs: Vec<Document> = self
            .database
            .collection::<Document>(WALLET_LEDGER)
            .find(doc! {"user_id": user_id})
            .sort(doc! {"id": -1_i32})
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)?;
        docs.iter().map(wallet_ledger_from_doc).collect()
    }
}

#[async_trait]
impl PurchasesRepository for MongoPurchasesRepository {
    async fn record(&self, purchase: Purchase) -> AppResult<Purchase> {
        let expiry = purchase
            .subscription_expiry_unix_ms
            .map(|value| {
                i64::try_from(value).map_err(|_| AppError::internal("purchase expiry out of range"))
            })
            .transpose()?;
        let validated = i64::try_from(purchase.validated_at_unix_ms)
            .map_err(|_| AppError::internal("purchase timestamp out of range"))?;
        self.database.collection::<Document>(PURCHASES).insert_one(doc! {"transaction_id": &purchase.transaction_id, "user_id": &purchase.user_id, "product_id": &purchase.product_id, "store": purchase.store.as_str(), "receipt_sha256": &purchase.receipt_sha256, "validated_at_unix_ms": validated, "subscription_expiry_unix_ms": expiry}).await.map_err(|error| if duplicate(&error) { duplicate_transaction() } else { mongo_error(error) })?;
        Ok(purchase)
    }

    async fn list(&self, user_id: Option<&str>, limit: usize) -> AppResult<Vec<Purchase>> {
        let filter = user_id.map_or_else(|| doc! {}, |id| doc! {"user_id": id});
        let docs: Vec<Document> = self
            .database
            .collection::<Document>(PURCHASES)
            .find(filter)
            .sort(doc! {"validated_at_unix_ms": -1_i32, "transaction_id": -1_i32})
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .await
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)?;
        docs.iter().map(purchase_from_doc).collect()
    }

    async fn get(&self, transaction_id: &str) -> AppResult<Option<Purchase>> {
        self.database
            .collection::<Document>(PURCHASES)
            .find_one(doc! {"transaction_id": transaction_id})
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(purchase_from_doc)
            .transpose()
    }

    async fn subscriptions(
        &self,
        user_id: Option<&str>,
        limit: usize,
        now: TimestampMillis,
    ) -> AppResult<Vec<SubscriptionRow>> {
        let filter = user_id.map_or_else(
            || doc! {"subscription_expiry_unix_ms": {"$exists": true}},
            |id| doc! {"user_id": id, "subscription_expiry_unix_ms": {"$exists": true}},
        );
        let docs: Vec<Document> = self
            .database
            .collection::<Document>(PURCHASES)
            .find(filter)
            .sort(doc! {"validated_at_unix_ms": -1_i32, "transaction_id": -1_i32})
            .await
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)?;
        Ok(subscription_rows(
            docs.iter()
                .map(purchase_from_doc)
                .collect::<AppResult<Vec<_>>>()?,
            user_id,
            limit,
            now,
        ))
    }
}

#[derive(Debug)]
enum MongoLeaderboardsTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}

impl From<AppError> for MongoLeaderboardsTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}

impl From<mongodb::error::Error> for MongoLeaderboardsTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

fn leaderboard_definition_from_doc(doc: &Document) -> AppResult<LeaderboardDefinition> {
    let millis = doc
        .get_i64("created_at_unix_ms")
        .map_err(|_| AppError::internal("invalid MongoDB leaderboard"))?;
    Ok(LeaderboardDefinition {
        id: doc
            .get_str("id")
            .map_err(|_| AppError::internal("invalid MongoDB leaderboard"))?
            .to_owned(),
        sort: SortOrder::from_token(
            doc.get_str("sort_order")
                .map_err(|_| AppError::internal("invalid MongoDB leaderboard"))?,
        )?,
        operator: Operator::from_token(
            doc.get_str("operator")
                .map_err(|_| AppError::internal("invalid MongoDB leaderboard"))?,
        )?,
        reset_schedule: doc.get_str("reset_schedule").ok().map(ToOwned::to_owned),
        created_at: TimestampMillis::from_unix_millis(
            u64::try_from(millis)
                .map_err(|_| AppError::internal("invalid MongoDB leaderboard timestamp"))?,
        ),
    })
}

fn leaderboard_record_from_doc(doc: &Document) -> AppResult<LeaderboardRecord> {
    let metadata = doc
        .get_str("metadata")
        .ok()
        .map(|raw| {
            serde_json::from_str(raw)
                .map_err(|_| AppError::internal("invalid MongoDB leaderboard metadata"))
        })
        .transpose()?;
    let submissions = doc
        .get_i64("submissions")
        .map_err(|_| AppError::internal("invalid MongoDB leaderboard record"))?;
    let updated = doc
        .get_i64("updated_at_unix_ms")
        .map_err(|_| AppError::internal("invalid MongoDB leaderboard record"))?;
    Ok(LeaderboardRecord {
        user_id: doc
            .get_str("owner_id")
            .map_err(|_| AppError::internal("invalid MongoDB leaderboard record"))?
            .to_owned(),
        score: doc
            .get_i64("score")
            .map_err(|_| AppError::internal("invalid MongoDB leaderboard record"))?,
        subscore: doc
            .get_i64("subscore")
            .map_err(|_| AppError::internal("invalid MongoDB leaderboard record"))?,
        metadata,
        updated_at: TimestampMillis::from_unix_millis(
            u64::try_from(updated)
                .map_err(|_| AppError::internal("invalid MongoDB leaderboard timestamp"))?,
        ),
        submissions: u32::try_from(submissions)
            .map_err(|_| AppError::internal("leaderboard submissions out of range"))?,
    })
}

fn leaderboard_record_doc(board: &str, record: &LeaderboardRecord) -> AppResult<Document> {
    let metadata = record
        .metadata
        .as_ref()
        .map(|value| {
            serde_json::to_string(value)
                .map_err(|_| AppError::internal("failed to encode MongoDB leaderboard metadata"))
        })
        .transpose()?;
    Ok(doc! {
        "leaderboard_id": board,
        "owner_id": &record.user_id,
        "score": record.score,
        "subscore": record.subscore,
        "metadata": metadata,
        "submissions": i64::from(record.submissions),
        "updated_at_unix_ms": i64::try_from(record.updated_at.unix_millis()).map_err(|_| AppError::internal("leaderboard timestamp out of range"))?,
    })
}

async fn leaderboard_mutation<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoLeaderboardsTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..LEADERBOARD_TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(MongoLeaderboardsTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(MongoLeaderboardsTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < LEADERBOARD_TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoLeaderboardsTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..LEADERBOARD_TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < LEADERBOARD_TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < LEADERBOARD_TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded leaderboard transaction retry either returns or continues")
}

#[async_trait]
impl LeaderboardsRepository for MongoLeaderboardsRepository {
    async fn create(
        &self,
        request: CreateLeaderboardRequest,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardDefinition> {
        let created = i64::try_from(now.unix_millis())
            .map_err(|_| AppError::internal("leaderboard timestamp out of range"))?;
        self.database.collection::<Document>("leaderboards").insert_one(doc! {
            "id": &request.id, "sort_order": request.sort.as_str(), "operator": request.operator.as_str(),
            "reset_schedule": request.reset_schedule.as_deref(), "created_at_unix_ms": created,
        }).await.map_err(|error| mongo_write_error(error, "leaderboard already exists"))?;
        Ok(LeaderboardDefinition {
            id: request.id,
            sort: request.sort,
            operator: request.operator,
            reset_schedule: request.reset_schedule,
            created_at: now,
        })
    }

    async fn list(&self) -> AppResult<Vec<LeaderboardSummary>> {
        let boards = self
            .database
            .collection::<Document>("leaderboards")
            .find(doc! {})
            .sort(doc! { "id": 1 })
            .await
            .map_err(mongo_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(mongo_error)?;
        let records = self.database.collection::<Document>("leaderboard_records");
        let mut result = Vec::with_capacity(boards.len());
        for board in boards {
            let definition = leaderboard_definition_from_doc(&board)?;
            let count = records
                .count_documents(doc! { "leaderboard_id": &definition.id })
                .await
                .map_err(mongo_error)?;
            result.push(LeaderboardSummary {
                definition,
                records: usize::try_from(count)
                    .map_err(|_| AppError::internal("leaderboard record count out of range"))?,
            });
        }
        Ok(result)
    }

    async fn get(&self, id: &str) -> AppResult<Option<LeaderboardDefinition>> {
        self.database
            .collection::<Document>("leaderboards")
            .find_one(doc! { "id": id })
            .await
            .map_err(mongo_error)?
            .as_ref()
            .map(leaderboard_definition_from_doc)
            .transpose()
    }

    async fn delete(&self, id: &str) -> AppResult<bool> {
        let id = id.to_owned();
        leaderboard_mutation(&self.client, &self.database, move |database, session| {
            let id = id.clone();
            Box::pin(async move {
                let removed = database
                    .collection::<Document>("leaderboards")
                    .delete_one(doc! { "id": &id })
                    .session(&mut *session)
                    .await?;
                if removed.deleted_count > 0 {
                    database
                        .collection::<Document>("leaderboard_records")
                        .delete_many(doc! { "leaderboard_id": &id })
                        .session(session)
                        .await?;
                }
                Ok(removed.deleted_count > 0)
            })
        })
        .await
    }

    async fn submit(
        &self,
        board: &str,
        user_id: &str,
        score: i64,
        subscore: i64,
        metadata: Option<serde_json::Value>,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardRecord> {
        let (board, user_id) = (board.to_owned(), user_id.to_owned());
        leaderboard_mutation(&self.client, &self.database, move |database, session| {
            let (board, user_id, metadata) = (board.clone(), user_id.clone(), metadata.clone());
            Box::pin(async move {
                let definition = database
                    .collection::<Document>("leaderboards")
                    .find_one(doc! { "id": &board })
                    .session(&mut *session)
                    .await?
                    .ok_or_else(|| MongoLeaderboardsTransactionError::App(board_not_found(&board)))
                    .and_then(|doc| leaderboard_definition_from_doc(&doc).map_err(Into::into))?;
                let records = database.collection::<Document>("leaderboard_records");
                let existing = records
                    .find_one(doc! { "leaderboard_id": &board, "owner_id": &user_id })
                    .session(&mut *session)
                    .await?
                    .as_ref()
                    .map(leaderboard_record_from_doc)
                    .transpose()?;
                let record = apply_submission(
                    definition.operator,
                    definition.sort,
                    existing.as_ref(),
                    &user_id,
                    score,
                    subscore,
                    metadata,
                    now,
                );
                records
                    .replace_one(
                        doc! { "leaderboard_id": &board, "owner_id": &user_id },
                        leaderboard_record_doc(&board, &record)?,
                    )
                    .upsert(true)
                    .session(session)
                    .await?;
                Ok(record)
            })
        })
        .await
    }

    async fn records(&self, board: &str, limit: usize, offset: usize) -> AppResult<RecordsPage> {
        let definition = self
            .get(board)
            .await?
            .ok_or_else(|| board_not_found(board))?;
        let total = self
            .database
            .collection::<Document>("leaderboard_records")
            .count_documents(doc! { "leaderboard_id": board })
            .await
            .map_err(mongo_error)?;
        let direction = if definition.sort == SortOrder::Asc {
            1
        } else {
            -1
        };
        // MongoDB interprets a cursor limit of zero as "unbounded"; the
        // repository contract instead treats it as an empty page.
        let docs = if limit == 0 {
            Vec::new()
        } else {
            self.database
                .collection::<Document>("leaderboard_records")
                .find(doc! { "leaderboard_id": board })
                .sort(doc! { "score": direction, "subscore": direction, "owner_id": 1 })
                .skip(
                    u64::try_from(offset)
                        .map_err(|_| AppError::internal("leaderboard offset out of range"))?,
                )
                .limit(
                    i64::try_from(limit)
                        .map_err(|_| AppError::internal("leaderboard limit out of range"))?,
                )
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?
        };
        let items = docs
            .into_iter()
            .enumerate()
            .map(|(index, doc)| {
                let record = leaderboard_record_from_doc(&doc)?;
                Ok(RankedRecord {
                    rank: u64::try_from(
                        offset
                            .checked_add(index)
                            .and_then(|rank| rank.checked_add(1))
                            .ok_or_else(|| AppError::internal("leaderboard rank out of range"))?,
                    )
                    .map_err(|_| AppError::internal("leaderboard rank out of range"))?,
                    user_id: record.user_id,
                    score: record.score,
                    subscore: record.subscore,
                    metadata: record.metadata,
                    updated_at: record.updated_at,
                    submissions: record.submissions,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;
        Ok(RecordsPage {
            items,
            total: usize::try_from(total)
                .map_err(|_| AppError::internal("leaderboard record count out of range"))?,
        })
    }

    async fn delete_record(&self, board: &str, user_id: &str) -> AppResult<bool> {
        let (board, user_id) = (board.to_owned(), user_id.to_owned());
        leaderboard_mutation(&self.client, &self.database, move |database, session| {
            let (board, user_id) = (board.clone(), user_id.clone());
            Box::pin(async move {
                let exists = database
                    .collection::<Document>("leaderboards")
                    .find_one(doc! { "id": &board })
                    .session(&mut *session)
                    .await?
                    .is_some();
                if !exists {
                    return Err(MongoLeaderboardsTransactionError::App(board_not_found(
                        &board,
                    )));
                }
                let result = database
                    .collection::<Document>("leaderboard_records")
                    .delete_one(doc! { "leaderboard_id": &board, "owner_id": &user_id })
                    .session(session)
                    .await?;
                Ok(result.deleted_count > 0)
            })
        })
        .await
    }
}

#[derive(Debug)]
enum MongoFriendsTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}

impl From<mongodb::error::Error> for MongoFriendsTransactionError {
    fn from(error: mongodb::error::Error) -> Self {
        Self::Mongo(error)
    }
}

impl From<AppError> for MongoFriendsTransactionError {
    fn from(error: AppError) -> Self {
        Self::App(error)
    }
}

fn friend_from_doc(doc: &Document) -> AppResult<FriendRow> {
    let user_id = doc
        .get_str("other_id")
        .map_err(|_| AppError::internal("invalid MongoDB friend edge"))?
        .to_owned();
    let state = FriendState::from_token(
        doc.get_str("state")
            .map_err(|_| AppError::internal("invalid MongoDB friend edge"))?,
    )?;
    let updated_unix_ms = u64::try_from(
        doc.get_i64("updated_unix_ms")
            .map_err(|_| AppError::internal("invalid MongoDB friend edge"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB friend edge"))?;
    Ok(FriendRow {
        user_id,
        state,
        updated_unix_ms,
    })
}

async fn friend_edge_state(
    database: &Database,
    session: &mut ClientSession,
    owner: &str,
    other: &str,
) -> Result<Option<FriendState>, MongoFriendsTransactionError> {
    let edge = database
        .collection::<Document>(FRIEND_EDGES)
        .find_one(doc! { "owner_id": owner, "other_id": other })
        .session(&mut *session)
        .await?;
    edge.as_ref()
        .map(|doc| {
            FriendState::from_token(
                doc.get_str("state")
                    .map_err(|_| AppError::internal("invalid MongoDB friend edge"))?,
            )
        })
        .transpose()
        .map_err(Into::into)
}

async fn upsert_friend_edge(
    database: &Database,
    session: &mut ClientSession,
    owner: &str,
    other: &str,
    state: FriendState,
    updated_unix_ms: u64,
) -> Result<(), mongodb::error::Error> {
    database
        .collection::<Document>(FRIEND_EDGES)
        .replace_one(
            doc! { "owner_id": owner, "other_id": other },
            doc! {
                "owner_id": owner,
                "other_id": other,
                "state": state.as_str(),
                "updated_unix_ms": i64::try_from(updated_unix_ms).unwrap_or(i64::MAX),
            },
        )
        .upsert(true)
        .session(&mut *session)
        .await?;
    Ok(())
}

fn direct_access_key(user: &str, other: &str) -> String {
    let (lower, higher) = if user < other {
        (user, other)
    } else {
        (other, user)
    };
    format!("direct:{lower}:{higher}")
}

async fn advance_friend_chat_access_epoch(
    database: &Database,
    session: &mut ClientSession,
    user: &str,
    other: &str,
    updated_unix_ms: u64,
) -> Result<(), mongodb::error::Error> {
    database
        .collection::<Document>("chat_access_epochs")
        .update_one(
            doc! { "access_key": direct_access_key(user, other) },
            doc! { "$inc": { "epoch": 1_i64 }, "$set": { "updated_at_unix_ms": i64::try_from(updated_unix_ms).unwrap_or(i64::MAX) } },
        )
        .upsert(true)
        .session(session)
        .await?;
    Ok(())
}

async fn add_friend_edges(
    database: &Database,
    session: &mut ClientSession,
    user: &str,
    other: &str,
    now: TimestampMillis,
) -> Result<FriendState, MongoFriendsTransactionError> {
    let forward = friend_edge_state(database, session, user, other).await?;
    let backward = friend_edge_state(database, session, other, user).await?;
    let plan = plan_add(forward, backward)?;
    let millis = now.unix_millis();
    upsert_friend_edge(database, session, user, other, plan.owner_state, millis).await?;
    upsert_friend_edge(database, session, other, user, plan.other_state, millis).await?;
    advance_friend_chat_access_epoch(database, session, user, other, millis).await?;
    Ok(plan.owner_state)
}

async fn remove_friend_edges(
    database: &Database,
    session: &mut ClientSession,
    user: &str,
    other: &str,
) -> Result<bool, mongodb::error::Error> {
    let result = database
        .collection::<Document>(FRIEND_EDGES)
        .delete_many(doc! { "$or": [
            { "owner_id": user, "other_id": other },
            { "owner_id": other, "other_id": user },
        ] })
        .session(&mut *session)
        .await?;
    let removed = result.deleted_count > 0;
    if removed {
        advance_friend_chat_access_epoch(database, session, user, other, 0).await?;
    }
    Ok(removed)
}

async fn block_friend_edge(
    database: &Database,
    session: &mut ClientSession,
    user: &str,
    other: &str,
    now: TimestampMillis,
) -> Result<(), mongodb::error::Error> {
    let millis = now.unix_millis();
    upsert_friend_edge(database, session, user, other, FriendState::Blocked, millis).await?;
    database
        .collection::<Document>(FRIEND_EDGES)
        .delete_one(doc! { "owner_id": other, "other_id": user })
        .session(&mut *session)
        .await?;
    advance_friend_chat_access_epoch(database, session, user, other, millis).await?;
    Ok(())
}

async fn run_friends_transaction<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoFriendsTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(MongoFriendsTransactionError::App(error)) => {
                let _ = session.abort_transaction().await;
                return Err(error);
            }
            Err(MongoFriendsTransactionError::Mongo(error))
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
            {
                let _ = session.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoFriendsTransactionError::Mongo(error)) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };
        for commit_attempt in 0..TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error)
                    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && commit_attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(commit_attempt).await;
                }
                Err(error)
                    if error.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = session.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(error) => return Err(mongo_error(error)),
            }
        }
    }
    unreachable!("bounded friends transaction retry either returns or continues")
}

#[async_trait]
impl FriendsRepository for MongoFriendsRepository {
    async fn add(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<FriendState> {
        match &self.session {
            None => {
                let user = user.to_owned();
                let other = other.to_owned();
                run_friends_transaction(&self.client, &self.database, |database, session| {
                    let user = user.clone();
                    let other = other.clone();
                    Box::pin(async move {
                        add_friend_edges(database, session, &user, &other, now).await
                    })
                })
                .await
            }
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                add_friend_edges(&self.database, session, user, other, now)
                    .await
                    .map_err(|error| match error {
                        MongoFriendsTransactionError::App(error) => error,
                        MongoFriendsTransactionError::Mongo(error) => mongo_error(error),
                    })
            }
        }
    }

    async fn remove(&self, user: &str, other: &str) -> AppResult<bool> {
        match &self.session {
            None => {
                let user = user.to_owned();
                let other = other.to_owned();
                run_friends_transaction(&self.client, &self.database, |database, session| {
                    let user = user.clone();
                    let other = other.clone();
                    Box::pin(async move {
                        remove_friend_edges(database, session, &user, &other)
                            .await
                            .map_err(Into::into)
                    })
                })
                .await
            }
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                remove_friend_edges(&self.database, session, user, other)
                    .await
                    .map_err(mongo_error)
            }
        }
    }

    async fn block(&self, user: &str, other: &str, now: TimestampMillis) -> AppResult<()> {
        match &self.session {
            None => {
                let user = user.to_owned();
                let other = other.to_owned();
                run_friends_transaction(&self.client, &self.database, |database, session| {
                    let user = user.clone();
                    let other = other.clone();
                    Box::pin(async move {
                        block_friend_edge(database, session, &user, &other, now)
                            .await
                            .map_err(Into::into)
                    })
                })
                .await
            }
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                block_friend_edge(&self.database, session, user, other, now)
                    .await
                    .map_err(mongo_error)
            }
        }
    }

    async fn list(&self, user: &str) -> AppResult<Vec<FriendRow>> {
        let edges = self.database.collection::<Document>(FRIEND_EDGES);
        let docs: Vec<Document> = match &self.session {
            None => edges
                .find(doc! { "owner_id": user })
                .sort(doc! { "other_id": 1 })
                .await
                .map_err(mongo_error)?
                .try_collect::<Vec<_>>()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                edges
                    .find(doc! { "owner_id": user })
                    .sort(doc! { "other_id": 1 })
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?
            }
        };
        docs.iter().map(friend_from_doc).collect()
    }
}

// --- groups -----------------------------------------------------------------

#[derive(Debug)]
enum MongoGroupsTransactionError {
    App(AppError),
    Mongo(mongodb::error::Error),
}
impl From<AppError> for MongoGroupsTransactionError {
    fn from(value: AppError) -> Self {
        Self::App(value)
    }
}
impl From<mongodb::error::Error> for MongoGroupsTransactionError {
    fn from(value: mongodb::error::Error) -> Self {
        Self::Mongo(value)
    }
}

fn group_from_docs(group: &Document, members: Vec<Document>) -> AppResult<Group> {
    let id = u64::try_from(
        group
            .get_i64("id")
            .map_err(|_| AppError::internal("invalid MongoDB group"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB group"))?;
    let created = u64::try_from(
        group
            .get_i64("created_at_unix_ms")
            .map_err(|_| AppError::internal("invalid MongoDB group"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB group"))?;
    let members = members
        .into_iter()
        .map(|m| {
            Ok(Membership {
                user_id: m
                    .get_str("user_id")
                    .map_err(|_| AppError::internal("invalid MongoDB group membership"))?
                    .to_owned(),
                role: GroupRole::from_token(
                    m.get_str("role")
                        .map_err(|_| AppError::internal("invalid MongoDB group membership"))?,
                )?,
                joined_at: TimestampMillis::from_unix_millis(
                    u64::try_from(
                        m.get_i64("joined_at_unix_ms")
                            .map_err(|_| AppError::internal("invalid MongoDB group membership"))?,
                    )
                    .map_err(|_| AppError::internal("invalid MongoDB group membership"))?,
                ),
            })
        })
        .collect::<AppResult<Vec<_>>>()?;
    Ok(Group::from_parts(
        id,
        group
            .get_str("name")
            .map_err(|_| AppError::internal("invalid MongoDB group"))?
            .to_owned(),
        group
            .get_str("description")
            .map_err(|_| AppError::internal("invalid MongoDB group"))?
            .to_owned(),
        group
            .get_bool("open")
            .map_err(|_| AppError::internal("invalid MongoDB group"))?,
        u32::try_from(
            group
                .get_i64("max_size")
                .map_err(|_| AppError::internal("invalid MongoDB group"))?,
        )
        .map_err(|_| AppError::internal("invalid MongoDB group"))?,
        TimestampMillis::from_unix_millis(created),
        members,
    ))
}

async fn load_mongo_group(
    database: &Database,
    session: &mut ClientSession,
    id: GroupId,
) -> Result<Option<Group>, MongoGroupsTransactionError> {
    let id = i64::try_from(id).map_err(|_| AppError::internal("group id out of range"))?;
    let Some(group) = database
        .collection::<Document>(GROUPS)
        .find_one(doc! {"id": id})
        .session(&mut *session)
        .await?
    else {
        return Ok(None);
    };
    let members = database
        .collection::<Document>(GROUP_MEMBERSHIPS)
        .find(doc! {"group_id": id})
        .sort(doc! {"joined_at_unix_ms": 1, "user_id": 1})
        .session(&mut *session)
        .await?
        .stream(session)
        .try_collect()
        .await?;
    Ok(Some(group_from_docs(&group, members)?))
}
async fn list_mongo_groups(
    database: &Database,
    session: &mut ClientSession,
) -> Result<Vec<Group>, MongoGroupsTransactionError> {
    let docs: Vec<Document> = database
        .collection::<Document>(GROUPS)
        .find(doc! {})
        .sort(doc! {"id": 1})
        .session(&mut *session)
        .await?
        .stream(session)
        .try_collect()
        .await?;
    let mut groups = Vec::with_capacity(docs.len());
    for doc in docs {
        let id = u64::try_from(
            doc.get_i64("id")
                .map_err(|_| AppError::internal("invalid MongoDB group"))?,
        )
        .map_err(|_| AppError::internal("invalid MongoDB group"))?;
        groups.push(
            load_mongo_group(database, session, id)
                .await?
                .ok_or_else(|| AppError::internal("MongoDB group disappeared during list"))?,
        );
    }
    Ok(groups)
}

async fn require_mongo_group(
    database: &Database,
    session: &mut ClientSession,
    id: GroupId,
) -> Result<Group, MongoGroupsTransactionError> {
    load_mongo_group(database, session, id)
        .await?
        .ok_or_else(|| AppError::not_found("group not found").into())
}
async fn advance_group_epoch(
    database: &Database,
    session: &mut ClientSession,
    id: GroupId,
    at: u64,
) -> Result<(), mongodb::error::Error> {
    database.collection::<Document>("chat_access_epochs").update_one(doc! {"access_key": format!("group:{id}")}, doc! {"$inc":{"epoch":1_i64}, "$set":{"updated_at_unix_ms": i64::try_from(at).unwrap_or(i64::MAX)}}).upsert(true).session(session).await?;
    Ok(())
}
async fn next_group_id(
    database: &Database,
    session: &mut ClientSession,
) -> Result<GroupId, MongoGroupsTransactionError> {
    let doc = database
        .collection::<Document>(GROUP_COUNTERS)
        .find_one_and_update(doc! {"_id":"groups"}, doc! {"$inc":{"value":1_i64}})
        .upsert(true)
        .return_document(ReturnDocument::After)
        .session(session)
        .await?
        .ok_or_else(|| AppError::internal("MongoDB group counter missing"))?;
    u64::try_from(
        doc.get_i64("value")
            .map_err(|_| AppError::internal("invalid MongoDB group counter"))?,
    )
    .map_err(|_| AppError::internal("invalid MongoDB group counter").into())
}
async fn admission(
    database: &Database,
    session: &mut ClientSession,
    id: GroupId,
    user: &str,
) -> Result<Option<AdmissionKind>, MongoGroupsTransactionError> {
    let doc = database.collection::<Document>(GROUP_ADMISSIONS).find_one(doc! {"group_id": i64::try_from(id).map_err(|_| AppError::internal("group id out of range"))?, "user_id":user}).session(session).await?;
    doc.map(|d| {
        match d
            .get_str("kind")
            .map_err(|_| AppError::internal("invalid MongoDB group admission"))?
        {
            "request" => Ok(AdmissionKind::Request),
            "invitation" => Ok(AdmissionKind::Invitation),
            _ => Err(AppError::internal("unknown group admission kind")),
        }
    })
    .transpose()
    .map_err(Into::into)
}
async fn write_admission(
    database: &Database,
    session: &mut ClientSession,
    id: GroupId,
    user: &str,
    kind: AdmissionKind,
    inviter: Option<&str>,
    now: TimestampMillis,
) -> Result<(), mongodb::error::Error> {
    database.collection::<Document>(GROUP_ADMISSIONS).replace_one(doc! {"group_id":id as i64,"user_id":user}, doc! {"group_id":id as i64,"user_id":user,"kind":if kind == AdmissionKind::Request {"request"} else {"invitation"},"inviter_user_id":inviter,"created_at_unix_ms":now.unix_millis() as i64}).upsert(true).session(session).await?;
    Ok(())
}
async fn add_mongo_member(
    database: &Database,
    session: &mut ClientSession,
    mut group: Group,
    user: &str,
    now: TimestampMillis,
) -> Result<Group, MongoGroupsTransactionError> {
    ensure_can_add_member(
        group.find_member(user).is_some(),
        group.member_count(),
        group.max_size,
    )?;
    database.collection::<Document>(GROUP_MEMBERSHIPS).insert_one(doc! {"group_id":group.id as i64,"user_id":user,"role":"member","joined_at_unix_ms":now.unix_millis() as i64}).session(&mut *session).await?;
    group.push_member(Membership {
        user_id: user.to_owned(),
        role: GroupRole::Member,
        joined_at: now,
    });
    advance_group_epoch(database, session, group.id, now.unix_millis()).await?;
    Ok(group)
}
async fn group_create(
    database: &Database,
    session: &mut ClientSession,
    request: CreateGroupRequest,
) -> Result<Group, MongoGroupsTransactionError> {
    let id = next_group_id(database, session).await?;
    let millis = request.now.unix_millis() as i64;
    database.collection::<Document>(GROUPS).insert_one(doc! {"id":id as i64,"name":&request.name,"description":&request.description,"open":request.open,"max_size":request.max_size as i64,"creator_id":&request.creator_user_id,"created_at_unix_ms":millis}).session(&mut *session).await?;
    database.collection::<Document>(GROUP_MEMBERSHIPS).insert_one(doc! {"group_id":id as i64,"user_id":&request.creator_user_id,"role":"superadmin","joined_at_unix_ms":millis}).session(session).await?;
    Ok(Group::from_parts(
        id,
        request.name,
        request.description,
        request.open,
        request.max_size,
        request.now,
        vec![Membership {
            user_id: request.creator_user_id,
            role: GroupRole::Superadmin,
            joined_at: request.now,
        }],
    ))
}
async fn group_mutation<T, F>(client: &Client, database: &Database, work: F) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoGroupsTransactionError>> + Send + 'a>,
    >,
{
    // Reuse retry semantics, retaining domain errors instead of stringifying them.
    run_groups_transaction(client, database, work).await
}
async fn run_groups_transaction<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    ) -> Pin<
        Box<dyn Future<Output = Result<T, MongoGroupsTransactionError>> + Send + 'a>,
    >,
{
    for attempt in 0..TRANSACTION_RETRY_LIMIT {
        let mut s = client.start_session().await.map_err(mongo_error)?;
        s.start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        let value = match work(database, &mut s).await {
            Ok(v) => v,
            Err(MongoGroupsTransactionError::App(e)) => {
                let _ = s.abort_transaction().await;
                return Err(e);
            }
            Err(MongoGroupsTransactionError::Mongo(e))
                if e.contains_label(TRANSIENT_TRANSACTION_ERROR)
                    && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
            {
                let _ = s.abort_transaction().await;
                transaction_backoff(attempt).await;
                continue;
            }
            Err(MongoGroupsTransactionError::Mongo(e)) => {
                let _ = s.abort_transaction().await;
                return Err(mongo_write_error(e, "group state changed concurrently"));
            }
        };
        for ca in 0..TRANSACTION_RETRY_LIMIT {
            match s.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(e)
                    if e.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && ca + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    transaction_backoff(ca).await
                }
                Err(e)
                    if e.contains_label(TRANSIENT_TRANSACTION_ERROR)
                        && attempt + 1 < TRANSACTION_RETRY_LIMIT =>
                {
                    let _ = s.abort_transaction().await;
                    transaction_backoff(attempt).await;
                    break;
                }
                Err(e) => return Err(mongo_write_error(e, "group state changed concurrently")),
            }
        }
    }
    unreachable!()
}

#[async_trait]
impl GroupsRepository for MongoGroupsRepository {
    async fn create(&self, request: CreateGroupRequest) -> AppResult<Group> {
        match &self.session {
            None => {
                group_mutation(&self.client, &self.database, move |d, s| {
                    Box::pin(group_create(d, s, request.clone()))
                })
                .await
            }
            Some(c) => {
                let mut g = c.lock().await;
                group_create(
                    &self.database,
                    g.as_mut().ok_or_else(|| {
                        AppError::internal("MongoDB transaction is already closed")
                    })?,
                    request,
                )
                .await
                .map_err(|e| match e {
                    MongoGroupsTransactionError::App(e) => e,
                    MongoGroupsTransactionError::Mongo(e) => {
                        mongo_write_error(e, "a group with that name already exists")
                    }
                })
            }
        }
    }
    async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage> {
        let groups = match &self.session {
            None => {
                let mut session = self.client.start_session().await.map_err(mongo_error)?;
                list_mongo_groups(&self.database, &mut session).await
            }
            Some(cell) => {
                let mut guard = cell.lock().await;
                let session = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                list_mongo_groups(&self.database, session).await
            }
        }
        .map_err(|error| match error {
            MongoGroupsTransactionError::App(error) => error,
            MongoGroupsTransactionError::Mongo(error) => mongo_error(error),
        })?;
        Ok(paginate(groups, filter))
    }
    async fn get(&self, id: GroupId) -> AppResult<Option<Group>> {
        match &self.session {
            None => {
                let mut s = self.client.start_session().await.map_err(mongo_error)?;
                load_mongo_group(&self.database, &mut s, id)
                    .await
                    .map_err(|e| match e {
                        MongoGroupsTransactionError::App(e) => e,
                        MongoGroupsTransactionError::Mongo(e) => mongo_error(e),
                    })
            }
            Some(c) => {
                let mut g = c.lock().await;
                load_mongo_group(
                    &self.database,
                    g.as_mut().ok_or_else(|| {
                        AppError::internal("MongoDB transaction is already closed")
                    })?,
                    id,
                )
                .await
                .map_err(|e| match e {
                    MongoGroupsTransactionError::App(e) => e,
                    MongoGroupsTransactionError::Mongo(e) => mongo_error(e),
                })
            }
        }
    }
    async fn update(&self, id: GroupId, request: UpdateGroupRequest) -> AppResult<Group> {
        self.mutate_group(move |d, s| { let request = request.clone(); Box::pin(async move { let g = require_mongo_group(d, s, id).await?; let description = request.description.unwrap_or_else(|| g.description.clone()); let open = request.open.unwrap_or(g.open); let max_size = request.max_size.unwrap_or(g.max_size); d.collection::<Document>(GROUPS).update_one(doc! {"id": id as i64}, doc! {"$set": {"description": &description, "open": open, "max_size": max_size as i64}}).session(&mut *s).await?; let members = g.members().to_vec(); Ok(Group::from_parts(g.id, g.name, description, open, max_size, g.created_at, members)) }) }).await
    }
    async fn delete(&self, id: GroupId) -> AppResult<bool> {
        self.mutate_group(move |d, s| {
            Box::pin(async move {
                let r = d
                    .collection::<Document>(GROUPS)
                    .delete_one(doc! {"id":id as i64})
                    .session(&mut *s)
                    .await?;
                if r.deleted_count > 0 {
                    d.collection::<Document>(GROUP_MEMBERSHIPS)
                        .delete_many(doc! {"group_id":id as i64})
                        .session(&mut *s)
                        .await?;
                    d.collection::<Document>(GROUP_ADMISSIONS)
                        .delete_many(doc! {"group_id":id as i64})
                        .session(&mut *s)
                        .await?;
                    advance_group_epoch(d, s, id, 0).await?;
                }
                Ok(r.deleted_count > 0)
            })
        })
        .await
    }
    async fn add_member(&self, id: GroupId, user: &str, now: TimestampMillis) -> AppResult<Group> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                let g = require_mongo_group(d, s, id).await?;
                add_mongo_member(d, s, g, &user, now).await
            })
        })
        .await
    }
    async fn kick_member(&self, id: GroupId, user: &str) -> AppResult<Group> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                let mut g = require_mongo_group(d, s, id).await?;
                let role = g
                    .find_member(&user)
                    .ok_or_else(|| AppError::not_found("member not found"))?
                    .role;
                ensure_can_kick(role, g.superadmin_count())?;
                d.collection::<Document>(GROUP_MEMBERSHIPS)
                    .delete_one(doc! {"group_id":id as i64,"user_id":&user})
                    .session(&mut *s)
                    .await?;
                g.remove_member(&user);
                advance_group_epoch(d, s, id, 0).await?;
                Ok(g)
            })
        })
        .await
    }
    async fn promote(&self, id: GroupId, user: &str) -> AppResult<Group> {
        self.role(id, user, true).await
    }
    async fn demote(&self, id: GroupId, user: &str) -> AppResult<Group> {
        self.role(id, user, false).await
    }
    async fn join(
        &self,
        id: GroupId,
        user: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                let g = require_mongo_group(d, s, id).await?;
                if g.find_member(&user).is_some() {
                    return Ok(AdmissionOutcome::AlreadyMember(g));
                }
                if g.open {
                    let g = add_mongo_member(d, s, g, &user, now).await?;
                    d.collection::<Document>(GROUP_ADMISSIONS)
                        .delete_one(doc! {"group_id":id as i64,"user_id":&user})
                        .session(&mut *s)
                        .await?;
                    return Ok(AdmissionOutcome::Joined(g));
                }
                match admission(d, s, id, &user).await? {
                    Some(AdmissionKind::Invitation) => Ok(AdmissionOutcome::InvitationCreated),
                    Some(AdmissionKind::Request) => Ok(AdmissionOutcome::RequestCreated),
                    None => {
                        write_admission(d, s, id, &user, AdmissionKind::Request, None, now).await?;
                        Ok(AdmissionOutcome::RequestCreated)
                    }
                }
            })
        })
        .await
    }
    async fn invite(
        &self,
        id: GroupId,
        user: &str,
        inviter: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        let (user, inviter) = (user.to_owned(), inviter.to_owned());
        self.mutate_group(move |d, s| {
            let (user, inviter) = (user.clone(), inviter.clone());
            Box::pin(async move {
                let g = require_mongo_group(d, s, id).await?;
                if g.find_member(&user).is_some() {
                    return Ok(AdmissionOutcome::AlreadyMember(g));
                }
                write_admission(
                    d,
                    s,
                    id,
                    &user,
                    AdmissionKind::Invitation,
                    Some(&inviter),
                    now,
                )
                .await?;
                Ok(AdmissionOutcome::InvitationCreated)
            })
        })
        .await
    }
    async fn approve_request(
        &self,
        id: GroupId,
        user: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        self.admit(id, user, now, AdmissionKind::Request).await
    }
    async fn accept_invitation(
        &self,
        id: GroupId,
        user: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        self.admit(id, user, now, AdmissionKind::Invitation).await
    }
    async fn cancel_admission(&self, id: GroupId, user: &str) -> AppResult<()> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                require_mongo_group(d, s, id).await?;
                d.collection::<Document>(GROUP_ADMISSIONS)
                    .delete_one(doc! {"group_id":id as i64,"user_id":&user})
                    .session(&mut *s)
                    .await?;
                Ok(())
            })
        })
        .await
    }
    async fn transfer_ownership(&self, id: GroupId, from: &str, to: &str) -> AppResult<Group> {
        let (from, to) = (from.to_owned(), to.to_owned());
        self.mutate_group(move |d, s| {
            let (from, to) = (from.clone(), to.clone());
            Box::pin(async move {
                let mut g = require_mongo_group(d, s, id).await?;
                if g.find_member(&from).map(|m| m.role) != Some(GroupRole::Superadmin) {
                    return Err(AppError::permission("current superadmin role required").into());
                }
                if from == to {
                    return Ok(g);
                }
                if g.find_member(&to).is_none() {
                    return Err(AppError::not_found("target member not found").into());
                }
                let m = d.collection::<Document>(GROUP_MEMBERSHIPS);
                m.update_one(
                    doc! {"group_id":id as i64,"user_id":&from},
                    doc! {"$set":{"role":"admin"}},
                )
                .session(&mut *s)
                .await?;
                m.update_one(
                    doc! {"group_id":id as i64,"user_id":&to},
                    doc! {"$set":{"role":"superadmin"}},
                )
                .session(&mut *s)
                .await?;
                g.set_member_role(&from, GroupRole::Admin);
                g.set_member_role(&to, GroupRole::Superadmin);
                advance_group_epoch(d, s, id, 0).await?;
                Ok(g)
            })
        })
        .await
    }
}
impl MongoGroupsRepository {
    async fn mutate_group<T, F>(&self, mut work: F) -> AppResult<T>
    where
        T: Send,
        F: for<'a> FnMut(
            &'a Database,
            &'a mut ClientSession,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, MongoGroupsTransactionError>> + Send + 'a>,
        >,
    {
        match &self.session {
            None => group_mutation(&self.client, &self.database, work).await,
            Some(c) => {
                let mut g = c.lock().await;
                let s = g
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                work(&self.database, s).await.map_err(|e| match e {
                    MongoGroupsTransactionError::App(e) => e,
                    MongoGroupsTransactionError::Mongo(e) => {
                        mongo_write_error(e, "group state changed concurrently")
                    }
                })
            }
        }
    }
    async fn role(&self, id: GroupId, user: &str, promote: bool) -> AppResult<Group> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                let mut g = require_mongo_group(d, s, id).await?;
                let current = g
                    .find_member(&user)
                    .ok_or_else(|| AppError::not_found("member not found"))?
                    .role;
                let next = if promote {
                    plan_promote(current)?
                } else {
                    plan_demote(current, g.superadmin_count())?
                };
                d.collection::<Document>(GROUP_MEMBERSHIPS)
                    .update_one(
                        doc! {"group_id":id as i64,"user_id":&user},
                        doc! {"$set":{"role":next.as_str()}},
                    )
                    .session(&mut *s)
                    .await?;
                g.set_member_role(&user, next);
                advance_group_epoch(d, s, id, 0).await?;
                Ok(g)
            })
        })
        .await
    }
    async fn admit(
        &self,
        id: GroupId,
        user: &str,
        now: TimestampMillis,
        expected: AdmissionKind,
    ) -> AppResult<Group> {
        let user = user.to_owned();
        self.mutate_group(move |d, s| {
            let user = user.clone();
            Box::pin(async move {
                let g = require_mongo_group(d, s, id).await?;
                if admission(d, s, id, &user).await? != Some(expected) {
                    return Err(AppError::not_found("group admission not found").into());
                }
                let g = add_mongo_member(d, s, g, &user, now).await?;
                d.collection::<Document>(GROUP_ADMISSIONS)
                    .delete_one(doc! {"group_id":id as i64,"user_id":&user})
                    .session(&mut *s)
                    .await?;
                Ok(g)
            })
        })
        .await
    }
}

const MONGO_STORAGE_CURSOR: &str = "mongo-storage-v1:";

fn storage_owner(owner: &Owner) -> (i32, String) {
    match owner {
        Owner::System => (0, String::new()),
        Owner::User(id) => (1, id.as_str().to_owned()),
    }
}
fn storage_owner_from(kind: i32, id: &str) -> AppResult<Owner> {
    match kind {
        0 => Ok(Owner::System),
        1 => Ok(Owner::user(UserId::new(id)?)),
        _ => Err(AppError::internal("invalid MongoDB storage owner")),
    }
}
fn storage_doc(object: &StorageObject) -> AppResult<Document> {
    let (owner_kind, owner_id) = storage_owner(&object.id.owner);
    Ok(
        doc! {"owner_kind":owner_kind,"owner_id":owner_id,"collection":object.id.collection.as_str(),"object_key":object.id.key.as_str(),"version":object.version.as_str(),"read_permission":i32::from(object.permissions.read.code()),"write_permission":i32::from(object.permissions.write.code()),"data":json_data(object)?},
    )
}
fn storage_from_doc(doc: &Document) -> AppResult<StorageObject> {
    from_json(doc)
}
fn storage_filter(id: &ObjectId) -> Document {
    let (owner_kind, owner_id) = storage_owner(&id.owner);
    doc! {"owner_kind":owner_kind,"owner_id":owner_id,"collection":id.collection.as_str(),"object_key":id.key.as_str()}
}
fn storage_can_read(object: &StorageObject, accessor: &Accessor) -> bool {
    object.permissions.can_read(&object.id.owner, accessor)
}
fn storage_can_write(object: &StorageObject, accessor: &Accessor) -> bool {
    object.permissions.can_write(&object.id.owner, accessor)
}
fn storage_cursor(object: &StorageObject) -> Cursor {
    let (owner_kind, owner_id) = storage_owner(&object.id.owner);
    Cursor::from_token(format!(
        "{MONGO_STORAGE_CURSOR}{}",
        serde_json::json!({"c":object.id.collection.as_str(),"o":owner_kind,"i":owner_id,"k":object.id.key.as_str()})
    ))
}
fn storage_after(cursor: &Cursor, collection: &Collection) -> AppResult<(i32, String, String)> {
    let raw = cursor
        .as_str()
        .strip_prefix(MONGO_STORAGE_CURSOR)
        .ok_or_else(|| AppError::validation("invalid storage cursor"))?;
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| AppError::validation("invalid storage cursor"))?;
    if value.get("c").and_then(serde_json::Value::as_str) != Some(collection.as_str()) {
        return Err(AppError::validation("invalid storage cursor"));
    }
    Ok((
        value
            .get("o")
            .and_then(serde_json::Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or_else(|| AppError::validation("invalid storage cursor"))?,
        value
            .get("i")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::validation("invalid storage cursor"))?
            .to_owned(),
        value
            .get("k")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::validation("invalid storage cursor"))?
            .to_owned(),
    ))
}

#[async_trait]
impl StorageRepository for MongoStorageRepository {
    async fn atomic_batch(
        &self,
        operations: Vec<AtomicBatchOperation>,
    ) -> AppResult<Vec<AtomicBatchResult>> {
        // Do not inherit the trait default implicitly: Mongo storage batches
        // are deliberately outside this primitive's supported-backend contract.
        // Existing single-key Mongo writes remain transactionally safe, but a
        // replayable multi-key CAS requires dedicated transaction-retry logic
        // before it can be offered portably.
        crate::repository::validate_atomic_batch(&operations)?;
        Err(AppError::validation(
            "atomic storage batches are not supported by the MongoDB backend",
        ))
    }

    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
        let found = match &self.session {
            None => self
                .database
                .collection::<Document>("storage_objects")
                .find_one(storage_filter(id))
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_objects")
                    .find_one(storage_filter(id))
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        found
            .as_ref()
            .map(storage_from_doc)
            .transpose()
            .map(|object| object.filter(|o| storage_can_read(o, accessor)))
    }
    async fn write(&self, accessor: &Accessor, request: WriteRequest) -> AppResult<StorageObject> {
        self.write_indexed(accessor, request, None).await
    }
    async fn write_indexed(
        &self,
        accessor: &Accessor,
        request: WriteRequest,
        membership: Option<&StorageIndexMembership>,
    ) -> AppResult<StorageObject> {
        let filter = storage_filter(&request.id);
        let (existing, candidates) = match &self.session {
            None => {
                let existing = self
                    .database
                    .collection::<Document>("storage_objects")
                    .find_one(filter.clone())
                    .await
                    .map_err(mongo_error)?;
                let candidates = self
                    .database
                    .collection::<Document>("storage_index_definitions")
                    .find(doc! {"collection": request.id.collection.as_str()})
                    .await
                    .map_err(mongo_error)?
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?;
                (existing, candidates)
            }
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                let existing = self
                    .database
                    .collection::<Document>("storage_objects")
                    .find_one(filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?;
                let candidates = self
                    .database
                    .collection::<Document>("storage_index_definitions")
                    .find(doc! {"collection": request.id.collection.as_str()})
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(mongo_error)?;
                (existing, candidates)
            }
        };
        let existing = existing.as_ref().map(storage_from_doc).transpose()?;
        if let Some(old) = &existing {
            if !storage_can_write(old, accessor) {
                return Err(AppError::permission(
                    "not permitted to write storage object",
                ));
            }
        } else if !accessor.can_create(&request.id.owner) {
            return Err(AppError::permission(
                "not permitted to create storage object",
            ));
        }
        crate::repository::check_precondition(
            &request.expected,
            existing.as_ref().map(|o| &o.version),
        )?;
        let version = Version::of(&request.value);
        let object = StorageObject {
            id: request.id,
            value: request.value,
            version,
            permissions: request.permissions,
        };
        let candidates: std::collections::BTreeSet<_> = candidates
            .into_iter()
            .filter(|d| {
                d.get_str("object_key")
                    .ok()
                    .is_none_or(|k| k.is_empty() || k == object.id.key.as_str())
            })
            .filter_map(|d| {
                d.get_str("index_name")
                    .ok()
                    .and_then(|n| crate::storage::StorageIndexName::new(n).ok())
            })
            .collect();
        let membership = membership
            .cloned()
            .unwrap_or_else(|| StorageIndexMembership::include_all(candidates.clone()));
        if membership.candidates() != &candidates {
            return Err(AppError::validation(
                "storage index membership candidates do not match installed indexes",
            ));
        }
        let replacement = storage_doc(&object)?;
        let mut cas = filter;
        if let Some(old) = &existing {
            cas.insert("version", old.version.as_str());
        }
        let existed = existing.is_some();
        let written = match &self.session {
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                let objects = self.database.collection::<Document>("storage_objects");
                if existed {
                    objects
                        .replace_one(cas, replacement)
                        .session(&mut *session)
                        .await
                        .map_err(mongo_error)?
                        .matched_count
                        > 0
                } else {
                    match objects.insert_one(replacement).session(&mut *session).await {
                        Ok(_) => true,
                        Err(error) if duplicate(&error) => false,
                        Err(error) => return Err(mongo_error(error)),
                    }
                }
            }
            None => {
                let object_for_transaction = object.clone();
                let membership_for_transaction = membership.clone();
                run_mongo_transaction(&self.client, &self.database, move |database, session| {
                    let replacement = replacement.clone();
                    let cas = cas.clone();
                    let object = object_for_transaction.clone();
                    let membership = membership_for_transaction.clone();
                    Box::pin(async move {
                        let objects = database.collection::<Document>("storage_objects");
                        if existed {
                            if objects
                                .replace_one(cas, replacement)
                                .session(&mut *session)
                                .await?
                                .matched_count
                                == 0
                            {
                                return Ok(false);
                            }
                        } else if let Err(error) =
                            objects.insert_one(replacement).session(&mut *session).await
                        {
                            if duplicate(&error) {
                                return Ok(false);
                            }
                            return Err(error);
                        }
                        replace_storage_memberships(database, session, &object, &membership)
                            .await?;
                        Ok(true)
                    })
                })
                .await?
            }
        };
        if !written {
            return Err(AppError::conflict("storage object changed concurrently"));
        }
        if let Some(cell) = &self.session {
            let mut session = cell.lock().await;
            let session = session
                .as_mut()
                .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
            replace_storage_memberships(&self.database, session, &object, &membership)
                .await
                .map_err(mongo_error)?;
        }
        Ok(object)
    }
    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()> {
        let filter = storage_filter(id);
        let existing = match &self.session {
            None => self
                .database
                .collection::<Document>("storage_objects")
                .find_one(filter.clone())
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_objects")
                    .find_one(filter.clone())
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
            }
        };
        let Some(existing) = existing else {
            return crate::repository::check_precondition(&expected, None);
        };
        let object = storage_from_doc(&existing)?;
        if !storage_can_write(&object, accessor) {
            return Err(AppError::permission(
                "not permitted to delete storage object",
            ));
        }
        crate::repository::check_precondition(&expected, Some(&object.version))?;
        let mut cas = filter;
        cas.insert("version", object.version.as_str());
        let deleted = match &self.session {
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_index_memberships")
                    .delete_many(storage_filter(id))
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?;
                self.database
                    .collection::<Document>("storage_objects")
                    .find_one_and_delete(cas)
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .is_some()
            }
            None => {
                let id = id.clone();
                run_mongo_transaction(&self.client, &self.database, move |database, session| {
                    let cas = cas.clone();
                    let id = id.clone();
                    Box::pin(async move {
                        database
                            .collection::<Document>("storage_index_memberships")
                            .delete_many(storage_filter(&id))
                            .session(&mut *session)
                            .await?;
                        Ok(database
                            .collection::<Document>("storage_objects")
                            .find_one_and_delete(cas)
                            .session(&mut *session)
                            .await?
                            .is_some())
                    })
                })
                .await?
            }
        };
        if !deleted {
            return Err(AppError::conflict("storage object changed concurrently"));
        }
        Ok(())
    }
    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>> {
        if query.limit == 0 {
            return Err(AppError::validation(
                "storage list limit must be greater than zero",
            ));
        }
        let mut filter = doc! {"collection":query.collection.as_str()};
        if let Some(owner) = &query.owner {
            let (k, i) = storage_owner(owner);
            filter.insert("owner_kind", k);
            filter.insert("owner_id", i);
        }
        if let Some(cursor) = &query.cursor {
            let (k, i, key) = storage_after(cursor, &query.collection)?;
            filter.insert(
                "$or",
                vec![
                    doc! {"owner_kind":{"$gt":k}},
                    doc! {"owner_kind":k,"owner_id":{"$gt":&i}},
                    doc! {"owner_kind":k,"owner_id":&i,"object_key":{"$gt":key}},
                ],
            );
        }
        let docs: Vec<Document> = match &self.session {
            None => self
                .database
                .collection::<Document>("storage_objects")
                .find(filter)
                .sort(doc! {"owner_kind":1,"owner_id":1,"object_key":1})
                .limit((query.limit + 1) as i64)
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_objects")
                    .find(filter)
                    .sort(doc! {"owner_kind":1,"owner_id":1,"object_key":1})
                    .limit((query.limit + 1) as i64)
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let mut items: Vec<StorageObject> = docs
            .iter()
            .map(storage_from_doc)
            .collect::<AppResult<_>>()?;
        items.retain(|o| storage_can_read(o, accessor));
        let next = if items.len() > query.limit {
            items.truncate(query.limit);
            items.last().map(storage_cursor)
        } else {
            None
        };
        Ok(Page { items, next })
    }
    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()> {
        let key = index.key().map_or("", Key::as_str);
        let filter = doc! {"index_name": index.name().as_str()};
        let replacement = doc! {"index_name":index.name().as_str(),"collection":index.collection().as_str(),"object_key":key,"fields":index.fields().iter().map(|f|Bson::String(f.as_str().to_owned())).collect::<Vec<_>>()};
        match &self.session {
            None => {
                self.database
                    .collection::<Document>("storage_index_definitions")
                    .replace_one(filter, replacement)
                    .upsert(true)
                    .await
                    .map_err(mongo_error)?;
            }
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_index_definitions")
                    .replace_one(filter, replacement)
                    .upsert(true)
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?;
            }
        };
        Ok(())
    }
    async fn query_index(
        &self,
        accessor: &Accessor,
        query: &StorageIndexQuery,
    ) -> AppResult<Vec<StorageObject>> {
        let docs: Vec<Document> = match &self.session {
            None => self
                .database
                .collection::<Document>("storage_index_memberships")
                .find(doc! {"index_name":query.index().name().as_str()})
                .limit(query.limit() as i64)
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_index_memberships")
                    .find(doc! {"index_name":query.index().name().as_str()})
                    .limit(query.limit() as i64)
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let mut out = Vec::new();
        for d in docs {
            let id = ObjectId::new(
                storage_owner_from(
                    d.get_i32("owner_kind")
                        .map_err(|_| AppError::internal("invalid MongoDB membership"))?,
                    d.get_str("owner_id")
                        .map_err(|_| AppError::internal("invalid MongoDB membership"))?,
                )?,
                Collection::new(
                    d.get_str("collection")
                        .map_err(|_| AppError::internal("invalid MongoDB membership"))?,
                )?,
                Key::new(
                    d.get_str("object_key")
                        .map_err(|_| AppError::internal("invalid MongoDB membership"))?,
                )?,
            );
            if let Some(o) = self.read(accessor, &id).await?
                && query
                    .filters()
                    .iter()
                    .all(|(f, v)| v.matches_json(o.value.as_json().get(f.as_str())))
            {
                out.push(o);
            }
        }
        out.sort_by_key(|o| o.id.cursor_token());
        Ok(out)
    }
    async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>> {
        let pipeline = vec![
            doc! {"$group":{"_id":"$collection","objects":{"$sum":1_i32}}},
            doc! {"$sort":{"_id":1_i32}},
        ];
        let docs: Vec<Document> = match &self.session {
            None => self
                .database
                .collection::<Document>("storage_objects")
                .aggregate(pipeline)
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            Some(cell) => {
                let mut session = cell.lock().await;
                let session = session
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                self.database
                    .collection::<Document>("storage_objects")
                    .aggregate(pipeline)
                    .session(&mut *session)
                    .await
                    .map_err(mongo_error)?
                    .stream(session)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        docs.iter()
            .map(|d| {
                Ok(CollectionSummary {
                    collection: Collection::new(
                        d.get_str("_id")
                            .map_err(|_| AppError::internal("invalid MongoDB collection"))?,
                    )?,
                    objects: d
                        .get_i32("objects")
                        .map_err(|_| AppError::internal("invalid MongoDB collection"))?
                        as u64,
                })
            })
            .collect()
    }
}
async fn replace_storage_memberships(
    database: &Database,
    session: &mut ClientSession,
    object: &StorageObject,
    membership: &StorageIndexMembership,
) -> Result<(), mongodb::error::Error> {
    let memberships = database.collection::<Document>("storage_index_memberships");
    memberships
        .delete_many(storage_filter(&object.id))
        .session(&mut *session)
        .await?;
    let (owner_kind, owner_id) = storage_owner(&object.id.owner);
    for name in membership.included() {
        memberships
            .insert_one(doc! {
                "index_name": name.as_str(),
                "owner_kind": owner_kind,
                "owner_id": &owner_id,
                "collection": object.id.collection.as_str(),
                "object_key": object.id.key.as_str(),
            })
            .session(&mut *session)
            .await?;
    }
    Ok(())
}

/// Read the serialized session document once so a following CAS write uses the
/// state and version from the same snapshot.  This is intentionally separate
/// from `get_session`, which returns only the domain value.
async fn session_document(executor: &MongoExecutor, id: &SessionId) -> AppResult<Option<Document>> {
    let filter = doc! { "id": id.as_str() };
    match executor {
        MongoExecutor::Database(db) => db
            .collection::<Document>(SESSIONS)
            .find_one(filter)
            .await
            .map_err(mongo_error),
        MongoExecutor::Transaction(cell, db) => {
            let mut session = cell.lock().await;
            let session = session
                .as_mut()
                .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
            db.collection::<Document>(SESSIONS)
                .find_one(filter)
                .session(&mut *session)
                .await
                .map_err(mongo_error)
        }
    }
}

#[async_trait]
impl UserRepository for MongoUserRepository {
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>> {
        let filter = doc! { "id": id.as_str() };
        let found = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(USERS)
                .find_one(filter)
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(USERS)
                    .find_one(filter)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
            }
        };
        found.as_ref().map(from_json).transpose()
    }
    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>> {
        let filter = doc! {"username":username.as_str()};
        let found = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(USERS)
                .find_one(filter)
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(USERS)
                    .find_one(filter)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
            }
        };
        found.as_ref().map(from_json).transpose()
    }
    async fn create_user(&self, user: User) -> AppResult<User> {
        let data = json_data(&user)?;
        let doc = doc! {"id":user.id.as_str(),"username":user.username.as_str(),"data":data};
        let result = match &self.executor {
            MongoExecutor::Database(db) => db.collection::<Document>(USERS).insert_one(doc).await,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(USERS)
                    .insert_one(doc)
                    .session(&mut *s)
                    .await
            }
        };
        result.map_err(|e| mongo_write_error(e, "user id or username already exists"))?;
        Ok(user)
    }
    async fn update_user(&self, user: User) -> AppResult<User> {
        let Some(existing) = self.get_user(&user.id).await? else {
            return Err(AppError::not_found("user does not exist"));
        };
        if existing.created_at != user.created_at {
            return Err(AppError::conflict("user created_at is immutable"));
        }
        let data = json_data(&user)?;
        let update = doc! {"$set":{"username":user.username.as_str(),"data":data}};
        let result = match &self.executor {
            MongoExecutor::Database(db) => {
                db.collection::<Document>(USERS)
                    .update_one(doc! {"id":user.id.as_str()}, update)
                    .await
            }
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(USERS)
                    .update_one(doc! {"id":user.id.as_str()}, update)
                    .session(&mut *s)
                    .await
            }
        }
        .map_err(|e| mongo_write_error(e, "username already exists"))?;
        debug_assert!(result.matched_count > 0);
        Ok(user)
    }
    async fn set_user_state(
        &self,
        id: &UserId,
        state: AccountState,
        updated_at: TimestampMillis,
    ) -> AppResult<User> {
        let Some(mut user) = self.get_user(id).await? else {
            return Err(AppError::not_found("user does not exist"));
        };
        if updated_at < user.created_at {
            return Err(AppError::validation(
                "user updated_at must not precede created_at",
            ));
        };
        user.state = state;
        user.updated_at = updated_at;
        self.update_user(user).await
    }
    async fn list_users(
        &self,
        filter: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> AppResult<UserPage> {
        if limit == 0 {
            return Err(AppError::validation(
                "user listing limit must be greater than zero",
            ));
        }
        // Regex-free substring filtering retains literal user input semantics.
        let all: Vec<Document> = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(USERS)
                .find(doc! {})
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(USERS)
                    .find(doc! {})
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
                    .stream(s)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let needle = filter.unwrap_or("");
        let mut users: Vec<User> = all.iter().map(from_json).collect::<AppResult<_>>()?;
        users.retain(|u| {
            needle.is_empty()
                || u.id.as_str().contains(needle)
                || u.username.as_str().contains(needle)
        });
        users.sort_by(|a, b| a.username.cmp(&b.username).then(a.id.cmp(&b.id)));
        let total = users.len() as u64;
        Ok(UserPage {
            users: users.into_iter().skip(offset).take(limit).collect(),
            total,
        })
    }
}

#[async_trait]
impl AuthIdentityRepository for MongoAuthIdentityRepository {
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>> {
        let (p, e) = credential_columns(credential);
        let f = doc! {"provider":p,"external_id":e};
        let found = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(IDENTITIES)
                .find_one(f)
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(IDENTITIES)
                    .find_one(f)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
            }
        };
        found.as_ref().map(identity_from_doc).transpose()
    }
    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>> {
        let f = doc! {"user_id":user_id.as_str()};
        let docs: Vec<Document> = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(IDENTITIES)
                .find(f)
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(IDENTITIES)
                    .find(f)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
                    .stream(s)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let mut v: Vec<AuthIdentity> = docs
            .iter()
            .map(identity_from_doc)
            .collect::<AppResult<_>>()?;
        v.sort_by(|a, b| {
            a.provider()
                .as_str()
                .cmp(b.provider().as_str())
                .then(a.created_at.cmp(&b.created_at))
        });
        Ok(v)
    }
    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity> {
        let (p, e) = credential_columns(&identity.credential);
        if let Some(existing) = self.get_auth_identity(&identity.credential).await? {
            return if existing.user_id == identity.user_id {
                Ok(existing)
            } else {
                Err(AppError::conflict(
                    "credential already linked to another account",
                ))
            };
        };
        let doc = doc! {"provider":p,"external_id":e,"user_id":identity.user_id.as_str(),"created_at":identity.created_at.unix_millis() as i64,"updated_at":identity.updated_at.unix_millis() as i64,"password_verifier":identity.password_verifier().map(PasswordVerifier::encoded)};
        let result = match &self.executor {
            MongoExecutor::Database(db) => {
                db.collection::<Document>(IDENTITIES).insert_one(doc).await
            }
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(IDENTITIES)
                    .insert_one(doc)
                    .session(&mut *s)
                    .await
            }
        };
        match result {
            Ok(_) => Ok(identity),
            Err(e) if duplicate(&e) => {
                let existing = self
                    .get_auth_identity(&identity.credential)
                    .await?
                    .ok_or_else(|| mongo_error(e))?;
                if existing.user_id == identity.user_id {
                    Ok(existing)
                } else {
                    Err(AppError::conflict(
                        "credential already linked to another account",
                    ))
                }
            }
            Err(e) => Err(mongo_error(e)),
        }
    }
    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()> {
        let (p, e) = credential_columns(credential);
        let f = doc! {"provider":p,"external_id":e};
        match &self.executor {
            MongoExecutor::Database(db) => {
                db.collection::<Document>(IDENTITIES)
                    .delete_one(f)
                    .await
                    .map_err(mongo_error)?;
            }
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(IDENTITIES)
                    .delete_one(f)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SessionRepository for MongoSessionRepository {
    async fn get_session(&self, id: &SessionId) -> AppResult<Option<Session>> {
        let f = doc! {"id":id.as_str()};
        let d = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(SESSIONS)
                .find_one(f)
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(SESSIONS)
                    .find_one(f)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
            }
        };
        d.as_ref().map(from_json).transpose()
    }
    async fn get_session_by_token_ref(&self, t: &SessionTokenRef) -> AppResult<Option<Session>> {
        let f = doc! {"token_ref":t.as_str()};
        let d = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(SESSIONS)
                .find_one(f)
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(SESSIONS)
                    .find_one(f)
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
            }
        };
        d.as_ref().map(from_json).transpose()
    }
    async fn create_session(&self, session: Session) -> AppResult<Session> {
        let d = doc! {"id":session.id.as_str(),"user_id":session.user_id.as_str(),"token_ref":session.token_ref.as_ref().map(SessionTokenRef::as_str),"state_kind":session.state_kind().as_str(),"version":0_i64,"data":json_data(&session)?};
        let r = match &self.executor {
            MongoExecutor::Database(db) => db.collection::<Document>(SESSIONS).insert_one(d).await,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(SESSIONS)
                    .insert_one(d)
                    .session(&mut *s)
                    .await
            }
        };
        r.map_err(|e| mongo_write_error(e, "session id already exists"))?;
        Ok(session)
    }
    async fn update_session(&self, session: Session) -> AppResult<Session> {
        let Some(existing_doc) = session_document(&self.executor, &session.id).await? else {
            return Err(AppError::not_found("session does not exist"));
        };
        let existing: Session = from_json(&existing_doc)?;
        if existing.user_id != session.user_id
            || existing.issued_at != session.issued_at
            || existing.owner_node != session.owner_node
        {
            return Err(AppError::conflict("immutable session fields cannot change"));
        }
        if existing.state().is_terminal() && existing != session {
            return Err(AppError::conflict(
                "cannot update a terminal session (compare-and-set failed)",
            ));
        }
        // The read above validates immutable fields and gives us the exact
        // state/version precondition.  Keep that precondition in the write:
        // a revoke that lands between the read and write then makes this stale
        // refresh match zero documents instead of resurrecting the session.
        let version = existing_doc.get_i64("version").unwrap_or(0);
        let mut filter = doc! {
            "id": session.id.as_str(),
            "state_kind": existing.state_kind().as_str(),
            "version": version,
        };
        // Documents created before the CAS column was added are treated as
        // version zero for their first transition, then become versioned.
        if version == 0 && existing_doc.get("version").is_none() {
            filter.remove("version");
            filter.insert(
                "$or",
                vec![
                    doc! { "version": 0_i64 },
                    doc! { "version": { "$exists": false } },
                ],
            );
        }
        let u = doc! {"$set":{"token_ref":session.token_ref.as_ref().map(SessionTokenRef::as_str),"state_kind":session.state_kind().as_str(),"data":json_data(&session)?}, "$inc":{"version":1_i64}};
        let result = match &self.executor {
            MongoExecutor::Database(db) => {
                db.collection::<Document>(SESSIONS)
                    .update_one(filter, u)
                    .await
            }
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(SESSIONS)
                    .update_one(filter, u)
                    .session(&mut *s)
                    .await
            }
        }
        .map_err(mongo_error)?;
        if result.matched_count == 0 {
            return Err(AppError::conflict(
                "session changed concurrently (compare-and-set failed)",
            ));
        }
        Ok(session)
    }
    async fn revoke_user_sessions(
        &self,
        user_id: &UserId,
        at: TimestampMillis,
        reason: RevocationReason,
    ) -> AppResult<usize> {
        let docs: Vec<Document> = match &self.executor {
            MongoExecutor::Database(db) => db
                .collection::<Document>(SESSIONS)
                .find(doc! {"user_id":user_id.as_str(),"state_kind":"active"})
                .await
                .map_err(mongo_error)?
                .try_collect()
                .await
                .map_err(mongo_error)?,
            MongoExecutor::Transaction(cell, db) => {
                let mut s = cell.lock().await;
                let s = s
                    .as_mut()
                    .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
                db.collection::<Document>(SESSIONS)
                    .find(doc! {"user_id":user_id.as_str(),"state_kind":"active"})
                    .session(&mut *s)
                    .await
                    .map_err(mongo_error)?
                    .stream(s)
                    .try_collect()
                    .await
                    .map_err(mongo_error)?
            }
        };
        let mut n = 0;
        for d in docs {
            let mut x: Session = from_json(&d)?;
            x.revoke_at(at, reason)?;
            match self.update_session(x).await {
                Ok(_) => n += 1,
                // A concurrent refresh may win the first CAS. Re-read once
                // and revoke that newer active version rather than allowing a
                // bulk revoke to leave it alive. A terminal winner is already
                // the desired result and contributes no new transition.
                Err(error) if error.category() == crate::error::ErrorCategory::Conflict => {
                    let Some(mut latest) = self
                        .get_session(&SessionId::new(
                            d.get_str("id")
                                .map_err(|_| AppError::internal("invalid MongoDB session"))?,
                        )?)
                        .await?
                    else {
                        continue;
                    };
                    if !latest.state().is_terminal() {
                        latest.revoke_at(at, reason)?;
                        self.update_session(latest).await?;
                        n += 1;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(n)
    }
}

impl fmt::Debug for MongoDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoDatabase")
            .field("database", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl MongoDatabase {
    /// Parse the official MongoDB URI, require a transaction-capable topology,
    /// and reconcile the foundation schema before returning a usable handle.
    pub async fn connect(config: &DatabaseConfig) -> AppResult<Self> {
        config.validate_mongodb_policy()?;
        let url = config
            .url
            .as_deref()
            .ok_or_else(|| AppError::config("database.url is required for MongoDB"))?;
        let timeout = Duration::from_millis(config.connect_timeout_ms);
        let mut options = tokio::time::timeout(timeout, ClientOptions::parse(url))
            .await
            .map_err(|_| AppError::database("MongoDB connection timed out"))?
            .map_err(mongo_error)?;
        let database_name = options
            .default_database
            .clone()
            .ok_or_else(|| AppError::config("MongoDB URL must include a database name"))?;
        // Apply the validated policy to the driver, database handles, and
        // explicit transactions. URI auth/TLS settings remain untouched.
        apply_client_policy(&mut options, config);
        let client = Client::with_options(options).map_err(mongo_error)?;
        let database = client.database(&database_name);
        let instance = Self {
            client,
            explorer: Arc::new(MongoMetadataExplorer::new(database.clone())),
            database,
        };
        tokio::time::timeout(timeout, instance.verify_topology())
            .await
            .map_err(|_| AppError::database("MongoDB topology verification timed out"))??;
        tokio::time::timeout(timeout, instance.reconcile_schema())
            .await
            .map_err(|_| AppError::database("MongoDB schema reconciliation timed out"))??;
        Ok(instance)
    }

    #[must_use]
    pub const fn kind(&self) -> crate::repository::BackendKind {
        crate::repository::BackendKind::MongoDb
    }

    #[must_use]
    pub fn schema_plan(&self) -> MongoSchemaPlan {
        MongoSchemaPlan::foundation()
    }

    /// Pooled durable tournament repository.
    #[must_use]
    pub fn tournaments_repository(&self) -> Arc<dyn TournamentsRepository> {
        Arc::new(MongoTournamentsRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }

    /// Pooled durable GameScript revision repository.
    #[must_use]
    pub fn gamescript_repository(&self) -> Arc<dyn GameScriptRepository> {
        Arc::new(MongoGameScriptRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }

    /// Pooled durable repository for leaderboard reset scheduler state.
    #[must_use]
    pub fn leaderboard_reset_repository(&self) -> Arc<dyn LeaderboardResetRepository> {
        Arc::new(MongoLeaderboardResetRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }

    /// Pooled Mongo-backed chat repository handle.
    #[must_use]
    pub fn mongo_chat_repository(&self) -> MongoChatRepository {
        MongoChatRepository::pooled(self.client.clone(), self.database.clone())
    }

    /// Start the serialized session required by future repository adapters.
    pub async fn begin(&self) -> AppResult<MongoUnitOfWork> {
        let mut session = self.client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;
        Ok(MongoUnitOfWork {
            client: self.client.clone(),
            session: Arc::new(tokio::sync::Mutex::new(Some(session))),
            database: self.database.clone(),
        })
    }

    /// Execute a replayable MongoDB transaction with the driver's prescribed
    /// retry semantics. The callback is invoked again from the beginning only
    /// for `TransientTransactionError`; an `UnknownTransactionCommitResult`
    /// retries only `commitTransaction`. The callback must therefore contain
    /// no externally visible side effects outside MongoDB.
    ///
    /// This complements the object-safe `UnitOfWork` seam: a caller holding a
    /// bare UoW cannot be replayed safely because its arbitrary Rust work has
    /// already run. New Mongo-specific multi-document flows must use this API.
    pub async fn with_transaction<T, F>(&self, work: F) -> AppResult<T>
    where
        T: Send,
        F: for<'a> FnMut(
            &'a Database,
            &'a mut ClientSession,
        ) -> Pin<
            Box<dyn Future<Output = Result<T, mongodb::error::Error>> + Send + 'a>,
        >,
    {
        run_mongo_transaction(&self.client, &self.database, work).await
    }

    /// Clear the identity/session projections used by hermetic integration
    /// tests. This is deliberately narrow: it never drops the versioned
    /// schema, indexes, or projections belonging to other repository tasks.
    ///
    /// Test callers must use an isolated database selected solely for testing.
    pub async fn clear_identity_session_data_for_tests(&self) -> AppResult<()> {
        for collection in [USERS, IDENTITIES, SESSIONS] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only the storage projections in an isolated integration database.
    #[doc(hidden)]
    pub async fn clear_storage_data_for_tests(&self) -> AppResult<()> {
        for collection in [
            "storage_objects",
            "storage_index_definitions",
            "storage_index_memberships",
        ] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only friend edges in an isolated MongoDB integration database.
    #[doc(hidden)]
    pub async fn clear_friends_data_for_tests(&self) -> AppResult<()> {
        self.database
            .collection::<Document>(FRIEND_EDGES)
            .delete_many(doc! {})
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Clear only group projections in an isolated MongoDB integration database.
    #[doc(hidden)]
    pub async fn clear_groups_data_for_tests(&self) -> AppResult<()> {
        for collection in [GROUPS, GROUP_MEMBERSHIPS, GROUP_ADMISSIONS, GROUP_COUNTERS] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only leaderboard definitions and records in an isolated MongoDB
    /// integration database.
    #[doc(hidden)]
    pub async fn clear_leaderboards_data_for_tests(&self) -> AppResult<()> {
        for collection in ["leaderboards", "leaderboard_records"] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only scheduler lease, epoch, and outbox documents in an isolated
    /// MongoDB integration database.
    #[doc(hidden)]
    pub async fn clear_leaderboard_reset_data_for_tests(&self) -> AppResult<()> {
        for collection in [
            LEADERBOARD_RESET_OUTBOX,
            LEADERBOARD_RESET_SNAPSHOT_RECORDS,
            LEADERBOARD_RESET_EPOCHS,
            LEADERBOARD_RESET_SCHEDULER_LEASE,
        ] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only GameScript revision-store documents in an isolated MongoDB
    /// integration database.
    #[doc(hidden)]
    pub async fn clear_gamescript_data_for_tests(&self) -> AppResult<()> {
        for collection in [
            GAMESCRIPT_OUTBOX,
            GAMESCRIPT_AUDIT,
            GAMESCRIPT_ACTIVATIONS,
            GAMESCRIPT_ACTIVATION_GENERATIONS,
            GAMESCRIPT_REVISION_DIAGNOSTICS,
            GAMESCRIPT_REVISION_PINS,
            GAMESCRIPT_REVISIONS,
            GAMESCRIPT_DRAFTS,
            GAMESCRIPT_COUNTERS,
        ] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear only notification documents in an isolated MongoDB integration
    /// database.
    #[doc(hidden)]
    pub async fn clear_notifications_data_for_tests(&self) -> AppResult<()> {
        for collection in [NOTIFICATIONS] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        Ok(())
    }

    /// Clear economy projections in an isolated MongoDB integration database.
    #[doc(hidden)]
    pub async fn clear_wallet_purchases_data_for_tests(&self) -> AppResult<()> {
        for collection in [WALLET_BALANCES, WALLET_LEDGER, PURCHASES] {
            self.database
                .collection::<Document>(collection)
                .delete_many(doc! {})
                .await
                .map_err(mongo_error)?;
        }
        self.database
            .collection::<Document>(GROUP_COUNTERS)
            .delete_one(doc! {"_id": "wallet_ledger_sequence"})
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Test-only database access for replica-set fault-injection contracts.
    #[doc(hidden)]
    #[must_use]
    pub fn database_for_tests(&self) -> Database {
        self.database.clone()
    }

    /// Read-only administrative MongoDB explorer. This shares the durable
    /// database handle and never falls back to process-local state.
    #[must_use]
    pub fn database_explorer(&self) -> Arc<MongoMetadataExplorer> {
        Arc::clone(&self.explorer)
    }

    /// Test-only admin database access for one-shot server failpoints.
    #[doc(hidden)]
    #[must_use]
    pub fn admin_database_for_tests(&self) -> Database {
        self.client.database("admin")
    }

    async fn verify_topology(&self) -> AppResult<()> {
        let hello = self
            .database
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(mongo_error)?;
        if !supports_transactions(&hello) {
            return Err(AppError::config(
                "MongoDB must be a replica set or sharded cluster to satisfy transactional persistence",
            ));
        }
        Ok(())
    }

    async fn reconcile_schema(&self) -> AppResult<()> {
        let names = self
            .database
            .list_collection_names()
            .await
            .map_err(mongo_error)?;
        for spec in SCHEMA {
            if !names.iter().any(|name| name == spec.name) {
                self.database
                    .create_collection(spec.name)
                    .await
                    .map_err(mongo_error)?;
            }
            for index in spec.indexes {
                self.reconcile_index(spec.name, *index).await?;
            }
        }
        self.reconcile_friend_edges_validator().await?;
        let registry = self.database.collection::<Document>(SCHEMA_COLLECTION);
        match registry
            .find_one(doc! { "_id": SCHEMA_ID })
            .await
            .map_err(mongo_error)?
        {
            Some(existing) if existing.get_i64("version").unwrap_or_default() > SCHEMA_VERSION => {
                Err(AppError::database(
                    "MongoDB schema registry is newer than this Citadel binary",
                ))
            }
            Some(_) => {
                // The physical schema has already been reconciled above, so a
                // lower registry version is a supported forward upgrade.
                registry
                    .update_one(
                        doc! { "_id": SCHEMA_ID },
                        doc! { "$set": {
                            "version": SCHEMA_VERSION,
                            "collections": SCHEMA.len() as i64,
                            "indexes": MongoSchemaPlan::foundation().indexes as i64,
                        } },
                    )
                    .await
                    .map_err(mongo_error)?;
                Ok(())
            }
            None => {
                registry.insert_one(doc! { "_id": SCHEMA_ID, "version": SCHEMA_VERSION, "collections": SCHEMA.len() as i64, "indexes": MongoSchemaPlan::foundation().indexes as i64 }).await.map_err(mongo_error)?;
                Ok(())
            }
        }
    }

    /// Set the friend-edge document validator explicitly on every reconciliation.
    /// `collMod` changes only future writes (it does not rewrite existing data),
    /// and supplying the complete desired validator/action/level makes this safe
    /// to repeat and prevents a looser deployment configuration from persisting.
    async fn reconcile_friend_edges_validator(&self) -> AppResult<()> {
        self.database
            .run_command(doc! {
                "collMod": FRIEND_EDGES,
                "validator": friend_edges_validator(),
                "validationLevel": "strict",
                "validationAction": "error",
            })
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    async fn reconcile_index(&self, collection: &str, spec: IndexSpec) -> AppResult<()> {
        let result = self
            .database
            .run_command(doc! { "listIndexes": collection })
            .await
            .map_err(mongo_error)?;
        let indexes = result
            .get_document("cursor")
            .ok()
            .and_then(|cursor| cursor.get_array("firstBatch").ok())
            .cloned()
            .unwrap_or_default();
        let expected = index_keys(spec);
        if let Some(existing) = indexes
            .iter()
            .filter_map(Bson::as_document)
            .find(|index| index.get_str("name").ok() == Some(spec.name))
        {
            let keys_match = existing.get_document("key").ok() == Some(&expected);
            let unique = existing.get_bool("unique").unwrap_or(false);
            if !keys_match || unique != spec.unique {
                return Err(AppError::database(
                    "MongoDB schema contains an incompatible required index",
                ));
            }
            return Ok(());
        }
        let model = IndexModel::builder()
            .keys(expected)
            .options(
                IndexOptions::builder()
                    .name(spec.name.to_owned())
                    .unique(spec.unique)
                    .build(),
            )
            .build();
        self.database
            .collection::<Document>(collection)
            .create_index(model)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

/// A single Mongo client session with an active transaction. It is deliberately
/// non-cloneable: MongoDB sessions are mutable and concurrent use would violate
/// the UnitOfWork ordering contract.
pub struct MongoUnitOfWork {
    client: Client,
    session: Arc<tokio::sync::Mutex<Option<ClientSession>>>,
    database: Database,
}

impl MongoUnitOfWork {
    /// Bind chat foundation helpers to this UnitOfWork's existing session.
    /// This is an explicit internal-adapter seam, not a `ChatRepository`
    /// publication, so it cannot expose a partial Mongo chat implementation.
    #[must_use]
    pub fn mongo_chat_repository(&self) -> MongoChatRepository {
        MongoChatRepository::transactional(
            self.client.clone(),
            self.database.clone(),
            Arc::clone(&self.session),
        )
    }

    pub async fn commit(self) -> AppResult<()> {
        let mut session = self
            .session
            .lock()
            .await
            .take()
            .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
        for attempt in 0..TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(()),
                Err(error) if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) => {
                    if attempt + 1 < TRANSACTION_RETRY_LIMIT {
                        transaction_backoff(attempt).await;
                        continue;
                    }
                    // The outcome remains unknown; do not abort after a
                    // commit attempt because the server may already have
                    // committed it.  The driver will clean up the session.
                    return Err(mongo_error(error));
                }
                Err(error) if error.contains_label(TRANSIENT_TRANSACTION_ERROR) => {
                    // A `TransientTransactionError` means the whole callback
                    // must be re-executed, never merely committed again.  A
                    // bare UnitOfWork cannot replay caller code, so abort it
                    // and surface the sanitized error to its owner.
                    let _ = session.abort_transaction().await;
                    return Err(mongo_error(error));
                }
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    return Err(mongo_error(error));
                }
            }
        }
        unreachable!("bounded commit retry either returns or continues")
    }

    pub async fn rollback(self) -> AppResult<()> {
        let mut session = self
            .session
            .lock()
            .await
            .take()
            .ok_or_else(|| AppError::internal("MongoDB transaction is already closed"))?;
        session.abort_transaction().await.map_err(mongo_error)?;
        Ok(())
    }
}

async fn run_mongo_transaction<T, F>(
    client: &Client,
    database: &Database,
    mut work: F,
) -> AppResult<T>
where
    T: Send,
    F: for<'a> FnMut(
        &'a Database,
        &'a mut ClientSession,
    )
        -> Pin<Box<dyn Future<Output = Result<T, mongodb::error::Error>> + Send + 'a>>,
{
    for attempt in 0..TRANSACTION_RETRY_LIMIT {
        let mut session = client.start_session().await.map_err(mongo_error)?;
        session
            .start_transaction()
            .with_options(transaction_options())
            .await
            .map_err(mongo_error)?;

        let value = match work(database, &mut session).await {
            Ok(value) => value,
            Err(error) if error.contains_label(TRANSIENT_TRANSACTION_ERROR) => {
                let _ = session.abort_transaction().await;
                if attempt + 1 < TRANSACTION_RETRY_LIMIT {
                    transaction_backoff(attempt).await;
                    continue;
                }
                return Err(mongo_error(error));
            }
            Err(error) => {
                let _ = session.abort_transaction().await;
                return Err(mongo_error(error));
            }
        };

        for commit_attempt in 0..TRANSACTION_RETRY_LIMIT {
            match session.commit_transaction().await {
                Ok(()) => return Ok(value),
                Err(error) if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) => {
                    if commit_attempt + 1 < TRANSACTION_RETRY_LIMIT {
                        transaction_backoff(commit_attempt).await;
                        continue;
                    }
                    // Never abort after an indeterminate commit: MongoDB may
                    // have committed it, and aborting cannot make that safe.
                    return Err(mongo_error(error));
                }
                Err(error) if error.contains_label(TRANSIENT_TRANSACTION_ERROR) => {
                    let _ = session.abort_transaction().await;
                    if attempt + 1 < TRANSACTION_RETRY_LIMIT {
                        transaction_backoff(attempt).await;
                        break;
                    }
                    return Err(mongo_error(error));
                }
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    return Err(mongo_error(error));
                }
            }
        }
    }
    unreachable!("bounded whole-transaction retry either returns or continues")
}

async fn transaction_backoff(attempt: usize) {
    let multiplier = 1_u32 << attempt.min(5);
    tokio::time::sleep(TRANSACTION_RETRY_BACKOFF.saturating_mul(multiplier)).await;
}

#[async_trait]
impl Backend for MongoDatabase {
    fn kind(&self) -> BackendKind {
        BackendKind::MongoDb
    }

    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::new(MongoStorageRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(MongoUserRepository::new(MongoExecutor::Database(
            self.database.clone(),
        )))
    }
    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(MongoAuthIdentityRepository::new(MongoExecutor::Database(
            self.database.clone(),
        )))
    }
    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(MongoSessionRepository::new(MongoExecutor::Database(
            self.database.clone(),
        )))
    }
    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(MongoFriendsRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(MongoGroupsRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn leaderboards_repository(&self) -> Arc<dyn LeaderboardsRepository> {
        Arc::new(MongoLeaderboardsRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn leaderboard_reset_repository(&self) -> Arc<dyn LeaderboardResetRepository> {
        MongoDatabase::leaderboard_reset_repository(self)
    }
    fn tournaments_repository(&self) -> Arc<dyn TournamentsRepository> {
        MongoDatabase::tournaments_repository(self)
    }
    fn gamescript_repository(&self) -> Arc<dyn GameScriptRepository> {
        MongoDatabase::gamescript_repository(self)
    }
    fn chat_repository(&self) -> Arc<dyn ChatRepository> {
        Arc::new(self.mongo_chat_repository())
    }
    fn notifications_repository(&self) -> Arc<dyn NotificationsRepository> {
        Arc::new(MongoNotificationsRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn wallet_repository(&self) -> Arc<dyn WalletRepository> {
        Arc::new(MongoWalletRepository::pooled(
            self.client.clone(),
            self.database.clone(),
        ))
    }
    fn purchases_repository(&self) -> Arc<dyn PurchasesRepository> {
        Arc::new(MongoPurchasesRepository::pooled(self.database.clone()))
    }

    fn database_explorer(&self) -> Option<Arc<dyn crate::database_explorer::DatabaseExplorer>> {
        Some(self.explorer.clone() as Arc<dyn crate::database_explorer::DatabaseExplorer>)
    }

    async fn begin(&self) -> AppResult<Box<dyn UnitOfWork>> {
        Ok(Box::new(MongoDatabase::begin(self).await?))
    }
}

#[async_trait]
impl UnitOfWork for MongoUnitOfWork {
    fn storage_repository(&self) -> Arc<dyn StorageRepository> {
        Arc::new(MongoStorageRepository::transactional(
            self.client.clone(),
            self.database.clone(),
            Arc::clone(&self.session),
        ))
    }
    fn user_repository(&self) -> Arc<dyn UserRepository> {
        Arc::new(MongoUserRepository::new(MongoExecutor::Transaction(
            Arc::clone(&self.session),
            self.database.clone(),
        )))
    }
    fn auth_identity_repository(&self) -> Arc<dyn AuthIdentityRepository> {
        Arc::new(MongoAuthIdentityRepository::new(
            MongoExecutor::Transaction(Arc::clone(&self.session), self.database.clone()),
        ))
    }
    fn session_repository(&self) -> Arc<dyn SessionRepository> {
        Arc::new(MongoSessionRepository::new(MongoExecutor::Transaction(
            Arc::clone(&self.session),
            self.database.clone(),
        )))
    }
    fn friends_repository(&self) -> Arc<dyn FriendsRepository> {
        Arc::new(MongoFriendsRepository::transactional(
            self.client.clone(),
            self.database.clone(),
            Arc::clone(&self.session),
        ))
    }
    fn groups_repository(&self) -> Arc<dyn GroupsRepository> {
        Arc::new(MongoGroupsRepository::transactional(
            self.client.clone(),
            self.database.clone(),
            Arc::clone(&self.session),
        ))
    }
    async fn commit(self: Box<Self>) -> AppResult<()> {
        MongoUnitOfWork::commit(*self).await
    }
    async fn rollback(self: Box<Self>) -> AppResult<()> {
        MongoUnitOfWork::rollback(*self).await
    }
}

fn primary_selection() -> SelectionCriteria {
    SelectionCriteria::ReadPreference(ReadPreference::Primary)
}

fn transaction_options() -> TransactionOptions {
    TransactionOptions::builder()
        .read_concern(ReadConcern::majority())
        .write_concern(WriteConcern::majority())
        .selection_criteria(primary_selection())
        .build()
}

fn apply_client_policy(options: &mut ClientOptions, config: &DatabaseConfig) {
    options.max_pool_size = Some(config.max_connections);
    options.connect_timeout = Some(Duration::from_millis(config.connect_timeout_ms));
    options.server_selection_timeout = Some(Duration::from_millis(config.acquire_timeout_ms));
    options.selection_criteria = Some(primary_selection());
    options.read_concern = Some(ReadConcern::majority());
    options.write_concern = Some(WriteConcern::majority());
}

/// Determine transaction support from the server capabilities advertised by
/// `hello`, rather than treating the deployment label as sufficient proof.
fn supports_transactions(hello: &Document) -> bool {
    let sessions = hello.get_i32("logicalSessionTimeoutMinutes").is_ok();
    let wire_version = hello.get_i32("maxWireVersion").unwrap_or_default();
    let replica_set = hello.get("setName").is_some() && sessions && wire_version >= 7;
    // Cross-shard transactions became available with MongoDB 4.2 (wire v8).
    let sharded = hello.get_str("msg").ok() == Some("isdbgrid") && sessions && wire_version >= 8;
    replica_set || sharded
}

/// MongoDB equivalent of the SQL friend-edge checks: both ids must be
/// non-empty and distinct, while state stays in the durable enum.
fn friend_edges_validator() -> Document {
    doc! { "$and": [
        { "$jsonSchema": {
            "bsonType": "object",
            "required": ["owner_id", "other_id", "state", "updated_unix_ms"],
            "properties": {
                "owner_id": { "bsonType": "string", "minLength": 1_i32 },
                "other_id": { "bsonType": "string", "minLength": 1_i32 },
                "state": { "enum": ["invited_sent", "invited_received", "friend", "blocked"] },
                "updated_unix_ms": { "bsonType": "long" },
            },
        }},
        { "$expr": { "$ne": ["$owner_id", "$other_id"] }},
    ] }
}

fn index_keys(spec: IndexSpec) -> Document {
    spec.keys
        .iter()
        .map(|(field, direction)| ((*field).to_owned(), Bson::Int32(*direction)))
        .collect()
}

fn mongo_error(_error: mongodb::error::Error) -> AppError {
    // Driver errors can include the URI or server reply. Keep the public and
    // operator-visible Citadel error stable and credential-free.
    AppError::database("MongoDB backend operation failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DatabaseBackend, DatabaseConfig};
    use crate::error::ErrorCategory;
    use crate::repository::StorageRepository;
    use crate::storage::{Permissions, StorageValue};

    #[test]
    fn foundation_manifest_covers_every_existing_domain_projection() {
        let plan = MongoSchemaPlan::foundation();
        assert_eq!(plan.version, 6);
        assert_eq!(plan.collections, 40);
        assert!(plan.indexes >= 61);
    }

    #[test]
    fn tournament_results_copy_snapshot_in_leaderboard_order_with_stable_ties() {
        let snapshot = vec![
            doc! { "owner_id": "alice", "score": 10_i64, "subscore": 1_i64 },
            doc! { "owner_id": "zoe", "score": 20_i64, "subscore": 2_i64 },
            doc! { "owner_id": "bob", "score": 20_i64, "subscore": 2_i64 },
        ];

        let results = tournament_results_from_snapshot("weekly", SortOrder::Desc, snapshot)
            .expect("valid snapshot");

        assert_eq!(
            results,
            vec![
                doc! { "tournament_id": "weekly", "user_id": "bob", "rank": 1_i64, "score": 20_i64, "subscore": 2_i64 },
                doc! { "tournament_id": "weekly", "user_id": "zoe", "rank": 2_i64, "score": 20_i64, "subscore": 2_i64 },
                doc! { "tournament_id": "weekly", "user_id": "alice", "rank": 3_i64, "score": 10_i64, "subscore": 1_i64 },
            ]
        );
    }

    #[test]
    fn chat_schema_manifest_covers_contract_keys_and_orderings() {
        let spec = |name| {
            SCHEMA
                .iter()
                .find(|spec| spec.name == name)
                .expect("chat collection")
        };
        assert_eq!(spec("chat_channels").indexes.len(), 3);
        assert_eq!(spec("chat_access_epochs").indexes.len(), 1);
        assert_eq!(spec("chat_messages").indexes.len(), 2);
        assert_eq!(spec("chat_events").indexes.len(), 1);
        assert_eq!(spec("chat_moderation_audit").indexes.len(), 1);
        assert_eq!(spec("chat_rate_limits").indexes.len(), 2);
        assert_eq!(spec("chat_delivery_outbox").indexes.len(), 2);
        assert_eq!(
            spec("chat_messages").indexes[1].keys,
            [("channel_id", 1), ("created_at_unix_ms", 1), ("id", 1)]
        );
    }

    #[test]
    fn chat_bson_boundaries_reject_invalid_ids_and_unrepresentable_timestamps() {
        assert!(chat_id("channel-1", "channel id").is_ok());
        assert!(chat_id("", "channel id").is_err());
        assert!(chat_id("bad\nchannel", "channel id").is_err());
        assert_eq!(
            chat_timestamp(TimestampMillis::from_unix_millis(42)).expect("timestamp"),
            42
        );
        assert!(chat_timestamp(TimestampMillis::from_unix_millis(u64::MAX)).is_err());
    }

    #[tokio::test]
    async fn atomic_batch_returns_the_documented_unsupported_error_without_io() {
        // atomic_batch must reject before touching the client, so this contract
        // remains runnable without a MongoDB service.
        let client = Client::with_uri_str("mongodb://127.0.0.1:27017")
            .await
            .expect("parse local MongoDB URI without connecting");
        let repo = MongoStorageRepository::pooled(client.clone(), client.database("citadel_test"));
        let error = repo
            .atomic_batch(vec![AtomicBatchOperation::Write {
                accessor: Accessor::Runtime,
                request: WriteRequest::upsert(
                    ObjectId::new(
                        Owner::System,
                        Collection::new("atomic-batch").expect("collection"),
                        Key::new("unsupported").expect("key"),
                    ),
                    StorageValue::new(serde_json::json!({"score": 1})).expect("value"),
                    Permissions::public_read(),
                ),
                membership: None,
            }])
            .await
            .expect_err("MongoDB atomic batches remain explicitly unsupported");
        assert_eq!(error.category(), ErrorCategory::Validation);
        assert_eq!(
            error.message(),
            "atomic storage batches are not supported by the MongoDB backend"
        );
    }

    #[test]
    fn mongo_urls_select_mongo_and_require_consistent_policy() {
        let config = DatabaseConfig {
            url: Some("mongodb+srv://user:secret@example.test/citadel".to_owned()),
            ..DatabaseConfig::default()
        };
        assert_eq!(
            config.backend().expect("MongoDB URL is classified"),
            Some(DatabaseBackend::MongoDb)
        );
        assert!(config.validate_mongodb_policy().is_ok());
        let weak = DatabaseConfig {
            mongodb_write_concern: "w1".to_owned(),
            ..config
        };
        assert!(weak.validate_mongodb_policy().is_err());
    }

    #[test]
    fn client_and_transaction_policy_force_majority_primary() {
        let mut options = ClientOptions::default();
        let config = DatabaseConfig {
            max_connections: 17,
            connect_timeout_ms: 2_000,
            acquire_timeout_ms: 3_000,
            ..DatabaseConfig::default()
        };
        apply_client_policy(&mut options, &config);
        assert_eq!(options.max_pool_size, Some(17));
        assert_eq!(options.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(
            options.server_selection_timeout,
            Some(Duration::from_secs(3))
        );
        assert!(matches!(
            options.selection_criteria,
            Some(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        ));
        assert_eq!(options.read_concern, Some(ReadConcern::majority()));
        assert_eq!(options.write_concern, Some(WriteConcern::majority()));
        let tx = transaction_options();
        assert_eq!(tx.read_concern, Some(ReadConcern::majority()));
        assert_eq!(tx.write_concern, Some(WriteConcern::majority()));
        assert!(matches!(
            tx.selection_criteria,
            Some(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        ));
    }

    #[test]
    fn topology_requires_transaction_capabilities_not_just_mongos_label() {
        assert!(!supports_transactions(&doc! { "msg": "isdbgrid" }));
        assert!(!supports_transactions(&doc! {
            "setName": "rs0", "logicalSessionTimeoutMinutes": 30, "maxWireVersion": 6
        }));
        assert!(supports_transactions(&doc! {
            "setName": "rs0", "logicalSessionTimeoutMinutes": 30, "maxWireVersion": 7
        }));
        assert!(supports_transactions(&doc! {
            "msg": "isdbgrid", "logicalSessionTimeoutMinutes": 30, "maxWireVersion": 8
        }));
    }
}
