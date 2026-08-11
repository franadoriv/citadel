//! Repository contracts for storage and future database-backed services
//!.
//!
//! Service and runtime code depends on the [`StorageRepository`] trait, never on
//! a concrete database. The trait is asynchronous (via [`async_trait`]) and
//! object-safe, so it stays usable behind `Arc<dyn StorageRepository>` while a
//! future async Postgres/sqlx backend can implement the same contract (
//! / ). `async-trait` boxes each returned future
//! (`Pin<Box<dyn Future + Send>>`), which is what keeps the trait dyn-compatible
//! — native `async fn in trait` is not. The Phase 5 Postgres implementation
//! introduces its own provider and transaction context (per
//! `website/src/content/docs/guides/choose-a-database.mdx`) without changing the domain
//! types these methods exchange. The in-memory reference impls keep synchronous
//! bodies (they return ready futures and never `.await`).
//!
//! [`InMemoryStorageRepository`] is a contract-faithful reference: it enforces
//! the same optimistic-concurrency and permission semantics any real backend
//! must, so the contract tests in `tests/storage_repository_contract.rs` can be
//! reused against future implementations.

pub mod backend;
pub mod chat;
pub mod friends;
pub mod gamescript;
pub mod groups;
pub mod identity;
pub mod leaderboards;
pub mod mongodb;
pub mod notifications;
pub mod pg;
pub mod purchases;
pub mod session;
pub mod sqlite;
pub mod tournaments;
pub mod wallet;

pub use backend::{
    Backend, BackendKind, InMemoryBackend, InMemoryUnitOfWork, UnitOfWork, select_backend,
};
pub use chat::{
    ChannelSummary, ChannelType, ChatChannel, ChatDeliveryOutboxRecord, ChatDeliveryRequest,
    ChatMessage, ChatModerationAudit, ChatRateLimit, ChatRepository, DEFAULT_CHANNEL_HISTORY_CAP,
    InMemoryChatRepository,
};
pub use friends::{
    AddPlan, FriendRow, FriendState, FriendsRepository, InMemoryFriendsRepository, plan_add,
};
pub use gamescript::{
    CreateGameScriptDraftRequest, GameScriptActivation, GameScriptAuditContext,
    GameScriptAuditRecord, GameScriptDiagnostic, GameScriptDiagnosticSeverity, GameScriptDraft,
    GameScriptLimits, GameScriptOutboxKind, GameScriptOutboxRecord, GameScriptRepository,
    GameScriptRevision, GameScriptSubmission, InMemoryGameScriptRepository,
    PROVISIONAL_MAX_GAMESCRIPT_SOURCE_BYTES, UpdateGameScriptDraftRequest,
    gamescript_revision_content_hash, redact_gamescript_audit_details,
};
pub use groups::{
    CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole, GroupsPage, GroupsRepository,
    InMemoryGroupsRepository, Membership, UpdateGroupRequest,
};
pub use identity::{
    AuthIdentityRepository, InMemoryAuthIdentityRepository, InMemoryUserRepository, UserPage,
    UserRepository,
};
pub use leaderboards::{
    CreateLeaderboardRequest, InMemoryLeaderboardsRepository, LeaderboardDefinition,
    LeaderboardRecord, LeaderboardSummary, LeaderboardsRepository, Operator, RankedRecord,
    RecordsPage, SortOrder,
};
pub use mongodb::{
    MongoChatRepository, MongoDatabase, MongoLeaderboardResetRepository, MongoSchemaPlan,
    MongoUnitOfWork,
};
pub use notifications::{
    DEFAULT_NOTIFICATION_CAPACITY, InMemoryNotificationsRepository, Notification, NotificationPage,
    NotificationsRepository, Recipient,
};
pub use pg::{PgDatabase, PgUnitOfWork};
pub use purchases::{
    InMemoryPurchasesRepository, Purchase, PurchaseStore, PurchasesRepository, SubscriptionRow,
};
pub use session::{InMemorySessionRepository, SessionRepository};
pub use sqlite::{SqliteDatabase, SqliteUnitOfWork};
pub use tournaments::{
    CreateTournamentRequest, InMemoryTournamentsRepository, Tournament, TournamentEntry,
    TournamentResult, TournamentSettlementCallback, TournamentSettlementOutboxDispatcher,
    TournamentSettlementOutboxRecord, TournamentState, TournamentsRepository,
};
pub use wallet::{
    DEFAULT_LEDGER_CAPACITY, InMemoryWalletRepository, LedgerEntry, WalletRepository,
};

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::storage::{
    Accessor, AtomicBatchOperation, AtomicBatchResult, Collection, CollectionSummary, Cursor,
    ListQuery, ObjectId, Page, Precondition, StorageIndexDefinition, StorageIndexMembership,
    StorageIndexName, StorageIndexQuery, StorageObject, Version, WriteRequest,
};

/// Portable storage repository contract.
///
/// Every method takes an [`Accessor`] so the implementation can enforce the
/// runtime-authoritative vs client distinction. Permission denials surface as
/// [`ErrorCategory::Permission`](crate::error::ErrorCategory::Permission) and
/// optimistic-concurrency failures as
/// [`ErrorCategory::Conflict`](crate::error::ErrorCategory::Conflict).
#[async_trait]
pub trait StorageRepository: Send + Sync {
    /// Atomically execute a bounded, duplicate-free set of object mutations.
    ///
    /// All permissions, memberships, and preconditions are checked at one
    /// serialization point. On error no object or index membership changes are
    /// visible. Results retain request order.
    ///
    /// This primitive is supported by the in-memory, SQLite, PostgreSQL, and
    /// CockroachDB storage backends. MongoDB explicitly rejects it until its
    /// replayable multi-key transaction retry contract is implemented.
    async fn atomic_batch(
        &self,
        operations: Vec<AtomicBatchOperation>,
    ) -> AppResult<Vec<AtomicBatchResult>> {
        validate_atomic_batch(&operations)?;
        Err(AppError::internal(
            "atomic storage batches are not supported by this backend",
        ))
    }
    /// Read a single object.
    ///
    /// Returns `Ok(None)` both when the object does not exist and when it exists
    /// but `accessor` is not permitted to read it, so callers cannot probe for
    /// the existence of objects they cannot see.
    ///
    /// # Errors
    /// Returns an error only on an internal failure (for example a poisoned
    /// lock in the in-memory implementation).
    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>>;

    /// Create or overwrite an object, honoring permissions and the request's
    /// optimistic-concurrency [`Precondition`].
    ///
    /// # Errors
    /// - `Permission` if `accessor` may not write/create the object.
    /// - `Conflict` if the precondition fails (already exists / version
    ///   mismatch).
    /// - `Internal` on a backend failure.
    async fn write(&self, accessor: &Accessor, request: WriteRequest) -> AppResult<StorageObject>;

    /// Create or overwrite an object while atomically applying validated
    /// per-index membership decisions.
    ///
    /// `membership` is constructed by a trusted runtime after its index-filter
    /// callbacks execute. Implementations must verify that its candidates match
    /// the installed index definitions for the object; language runtime values
    /// and closures never enter this repository boundary. Passing `None` uses
    /// the normal default of including every configured candidate index.
    async fn write_indexed(
        &self,
        accessor: &Accessor,
        request: WriteRequest,
        membership: Option<&StorageIndexMembership>,
    ) -> AppResult<StorageObject>;

    /// Delete an object, honoring permissions and `expected`.
    ///
    /// Deleting a missing object is idempotent unless a
    /// [`Precondition::Match`] is supplied, which fails with `Conflict`.
    ///
    /// # Errors
    /// - `Permission` if `accessor` may not write the object.
    /// - `Conflict` if the precondition fails.
    /// - `Internal` on a backend failure.
    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()>;

    /// List objects in a collection that `accessor` is permitted to read,
    /// paginated by the query's limit and cursor.
    ///
    /// # Errors
    /// - `Validation` if `query.limit` is zero.
    /// - `Internal` on a backend failure.
    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>>;

    /// Install one operator-declared physical storage index.
    ///
    /// This method is called only from trusted startup/configuration paths. It
    /// is deliberately absent from client and game-script APIs so untrusted
    /// callers cannot issue DDL or create unbounded database indexes.
    ///
    /// # Errors
    /// Returns `Database`/`Internal` if the durable backend cannot create the
    /// physical index. The in-memory reference implementation validates no
    /// additional state because it evaluates the same contract in process.
    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()>;

    /// Query an installed storage index with portable equality predicates.
    ///
    /// Results are bounded by the validated query limit, ordered by storage
    /// identity, and filtered by the same read permission rule as [`Self::list`].
    ///
    /// # Errors
    /// Returns `Validation` for an invalid query and `Internal`/`Database` for
    /// backend failures.
    async fn query_index(
        &self,
        accessor: &Accessor,
        query: &StorageIndexQuery,
    ) -> AppResult<Vec<StorageObject>>;

    /// Administrative scan of every collection name with its object count,
    /// ordered by collection name.
    ///
    /// Unlike the other operations this deliberately takes no [`Accessor`]:
    /// counts include objects no client accessor could read, so the result
    /// must only flow to operator-gated surfaces (the admin console). It is
    /// a full-collection aggregate, not a hot game path.
    ///
    /// # Errors
    /// Returns an error only on an internal/backend failure.
    async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>>;
}

/// Reject malformed batches before any backend mutation begins.
pub(crate) fn validate_atomic_batch(operations: &[AtomicBatchOperation]) -> AppResult<()> {
    const MAX_ATOMIC_BATCH_OPERATIONS: usize = 64;
    if operations.is_empty() {
        return Err(AppError::validation(
            "atomic storage batch must not be empty",
        ));
    }
    if operations.len() > MAX_ATOMIC_BATCH_OPERATIONS {
        return Err(AppError::validation(
            "atomic storage batch exceeds 64 operations",
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    for operation in operations {
        if !ids.insert(operation.id().clone()) {
            return Err(AppError::validation(
                "atomic storage batch contains duplicate object identity",
            ));
        }
    }
    Ok(())
}

/// Evaluate an optimistic-concurrency precondition against the current version.
///
/// Shared by [`InMemoryStorageRepository`] and the Postgres backend
/// ([`pg::PgStorageRepository`]) so both enforce byte-for-byte identical
/// precondition semantics and error messages.
pub(crate) fn check_precondition(
    expected: &Precondition,
    current: Option<&Version>,
) -> AppResult<()> {
    match expected {
        Precondition::Any => Ok(()),
        Precondition::MustNotExist => match current {
            Some(_) => Err(AppError::conflict("object already exists")),
            None => Ok(()),
        },
        Precondition::Match(expected_version) => match current {
            Some(version) if version == expected_version => Ok(()),
            Some(_) => Err(AppError::conflict("storage version mismatch")),
            None => Err(AppError::conflict(
                "version precondition failed: object does not exist",
            )),
        },
    }
}

/// A contract-faithful, in-memory [`StorageRepository`].
///
/// Single-process and not durable, but it enforces the full permission and
/// optimistic-concurrency contract so it is safe to use in tests and local
/// development without silently weakening guarantees.
#[derive(Debug, Default)]
pub struct InMemoryStorageRepository {
    state: Mutex<InMemoryStorageState>,
}

/// All mutable in-memory storage state guarded together so an object and its
/// index projection can never be observed half-updated.
#[derive(Debug, Default)]
struct InMemoryStorageState {
    objects: BTreeMap<ObjectId, StorageObject>,
    indexes: BTreeMap<StorageIndexName, StorageIndexDefinition>,
    memberships: std::collections::BTreeSet<(StorageIndexName, ObjectId)>,
}

impl InMemoryStorageRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored objects (across all owners/collections).
    ///
    /// # Errors
    /// Returns an internal error if the lock is poisoned.
    pub fn len(&self) -> AppResult<usize> {
        Ok(self.guard()?.objects.len())
    }

    /// Whether the repository holds no objects.
    ///
    /// # Errors
    /// Returns an internal error if the lock is poisoned.
    pub fn is_empty(&self) -> AppResult<bool> {
        Ok(self.guard()?.objects.is_empty())
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, InMemoryStorageState>> {
        self.state
            .lock()
            .map_err(|_| AppError::internal("storage repository mutex poisoned"))
    }

    /// Whether an object with `id` exists, ignoring permissions.
    ///
    /// Used only by the in-memory unit of work to decide whether a write it is
    /// about to perform creates a new object (and therefore needs a compensating
    /// delete on rollback). Not a permission-aware read.
    ///
    /// # Errors
    /// Returns an internal error if the lock is poisoned.
    pub(crate) fn contains_object(&self, id: &ObjectId) -> AppResult<bool> {
        Ok(self.guard()?.objects.contains_key(id))
    }

    /// Compensating delete used only by the in-memory unit of work to roll back an
    /// object it created within an aborted transaction.
    ///
    /// Best-effort and synchronous (callable from `Drop`); no-ops on a poisoned
    /// lock. Only creations are compensated — an in-memory UoW does not restore an
    /// overwritten prior value (storage is not part of any multi-write workflow;
    /// the Postgres backend rolls the whole transaction back).
    pub(crate) fn remove_object_for_rollback(&self, id: &ObjectId) {
        if let Ok(mut state) = self.state.lock() {
            state.objects.remove(id);
            state.memberships.retain(|(_, object_id)| object_id != id);
        }
    }
}

#[async_trait]
impl StorageRepository for InMemoryStorageRepository {
    async fn atomic_batch(
        &self,
        operations: Vec<AtomicBatchOperation>,
    ) -> AppResult<Vec<AtomicBatchResult>> {
        validate_atomic_batch(&operations)?;
        let mut state = self.guard()?;
        // Stage the complete state. Every validation occurs against this one
        // snapshot; only the final assignment makes it observable.
        let mut staged = InMemoryStorageState {
            objects: state.objects.clone(),
            indexes: state.indexes.clone(),
            memberships: state.memberships.clone(),
        };
        let mut results = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                AtomicBatchOperation::Write {
                    accessor,
                    request,
                    membership,
                } => {
                    let object =
                        write_indexed_state(&mut staged, &accessor, request, membership.as_ref())?;
                    results.push(AtomicBatchResult::Written(object));
                }
                AtomicBatchOperation::Delete {
                    accessor,
                    id,
                    expected,
                } => {
                    delete_state(&mut staged, &accessor, &id, expected)?;
                    results.push(AtomicBatchResult::Deleted);
                }
            }
        }
        *state = staged;
        Ok(results)
    }
    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
        let state = self.guard()?;
        match state.objects.get(id) {
            Some(object) if object.permissions.can_read(&object.id.owner, accessor) => {
                Ok(Some(object.clone()))
            }
            // Exists-but-unreadable is reported as absent to avoid leaking
            // existence to callers that cannot see the object.
            _ => Ok(None),
        }
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
        let mut state = self.guard()?;
        write_indexed_state(&mut state, accessor, request, membership)
    }

    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()> {
        let mut state = self.guard()?;
        delete_state(&mut state, accessor, id, expected)
    }

    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>> {
        if query.limit == 0 {
            return Err(AppError::validation("list limit must be greater than zero"));
        }

        let state = self.guard()?;
        let after = query
            .cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());

        let mut matched: Vec<StorageObject> = state
            .objects
            .values()
            .filter(|object| object.id.collection == query.collection)
            .filter(|object| {
                query
                    .owner
                    .as_ref()
                    .is_none_or(|owner| &object.id.owner == owner)
            })
            .filter(|object| object.permissions.can_read(&object.id.owner, accessor))
            .filter(|object| {
                after
                    .as_ref()
                    .is_none_or(|cursor| object.id.cursor_token() > *cursor)
            })
            .cloned()
            .collect();

        // Sort and paginate by the same opaque token for internal consistency.
        matched.sort_by_key(|object| object.id.cursor_token());

        let next = if matched.len() > query.limit {
            matched.truncate(query.limit);
            matched
                .last()
                .map(|object| Cursor::from_token(object.id.cursor_token()))
        } else {
            None
        };
        Ok(Page {
            items: matched,
            next,
        })
    }

    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()> {
        let mut state = self.guard()?;
        state.indexes.insert(index.name().clone(), index.clone());
        state.memberships.retain(|(name, _)| name != index.name());
        let matching = state
            .objects
            .values()
            .filter(|object| index.matches_object(&object.id))
            .map(|object| object.id.clone())
            .collect::<Vec<_>>();
        for id in matching {
            state.memberships.insert((index.name().clone(), id));
        }
        Ok(())
    }
    async fn query_index(
        &self,
        accessor: &Accessor,
        query: &StorageIndexQuery,
    ) -> AppResult<Vec<StorageObject>> {
        let state = self.guard()?;
        let mut matched = state
            .objects
            .values()
            .filter(|object| object.id.collection == *query.index().collection())
            .filter(|object| query.index().key().is_none_or(|key| object.id.key == *key))
            .filter(|object| {
                state
                    .memberships
                    .contains(&(query.index().name().clone(), object.id.clone()))
            })
            .filter(|object| object.permissions.can_read(&object.id.owner, accessor))
            .filter(|object| {
                query.filters().iter().all(|(field, expected)| {
                    expected.matches_json(object.value.as_json().get(field.as_str()))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        matched.sort_by_key(|object| object.id.cursor_token());
        matched.truncate(query.limit());
        Ok(matched)
    }

    async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>> {
        let state = self.guard()?;
        let mut counts: BTreeMap<Collection, u64> = BTreeMap::new();
        for object in state.objects.values() {
            *counts.entry(object.id.collection.clone()).or_insert(0) += 1;
        }
        Ok(counts
            .into_iter()
            .map(|(collection, objects)| CollectionSummary {
                collection,
                objects,
            })
            .collect())
    }
}

fn write_indexed_state(
    state: &mut InMemoryStorageState,
    accessor: &Accessor,
    request: WriteRequest,
    membership: Option<&StorageIndexMembership>,
) -> AppResult<StorageObject> {
    match state.objects.get(&request.id) {
        Some(existing) => {
            if !existing.permissions.can_write(&existing.id.owner, accessor) {
                return Err(AppError::permission("write denied on existing object"));
            }
            check_precondition(&request.expected, Some(&existing.version))?;
        }
        None => {
            if !accessor.can_create(&request.id.owner) {
                return Err(AppError::permission(
                    "write denied: cannot create object for this owner",
                ));
            }
            check_precondition(&request.expected, None)?;
        }
    }
    let object = StorageObject {
        id: request.id,
        version: Version::of(&request.value),
        value: request.value,
        permissions: request.permissions,
    };
    let candidates = state
        .indexes
        .values()
        .filter(|index| index.matches_object(&object.id))
        .map(|index| index.name().clone())
        .collect::<std::collections::BTreeSet<_>>();
    let membership = membership
        .cloned()
        .unwrap_or_else(|| StorageIndexMembership::include_all(candidates.clone()));
    if !candidates.is_empty() && membership.candidates() != &candidates {
        return Err(AppError::validation(
            "storage index membership candidates do not match configured indexes",
        ));
    }
    let candidates = if candidates.is_empty() {
        membership.candidates().clone()
    } else {
        candidates
    };
    state.objects.insert(object.id.clone(), object.clone());
    state
        .memberships
        .retain(|(index, id)| id != &object.id || !candidates.contains(index));
    state.memberships.extend(
        membership
            .included()
            .iter()
            .cloned()
            .map(|index| (index, object.id.clone())),
    );
    Ok(object)
}

fn delete_state(
    state: &mut InMemoryStorageState,
    accessor: &Accessor,
    id: &ObjectId,
    expected: Precondition,
) -> AppResult<()> {
    match state.objects.get(id) {
        None => match expected {
            Precondition::Match(_) => Err(AppError::conflict(
                "delete precondition failed: object does not exist",
            )),
            Precondition::Any | Precondition::MustNotExist => Ok(()),
        },
        Some(existing) => {
            if !existing.permissions.can_write(&existing.id.owner, accessor) {
                return Err(AppError::permission("delete denied on existing object"));
            }
            check_precondition(&expected, Some(&existing.version))?;
            state.objects.remove(id);
            state.memberships.retain(|(_, object_id)| object_id != id);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        AtomicBatchOperation, Collection, Key, Owner, Permissions, Precondition, StorageValue,
        UserId,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn user(id: &str) -> UserId {
        UserId::new(id).expect("valid user id")
    }

    fn object_id(owner: Owner, collection: &str, key: &str) -> ObjectId {
        ObjectId::new(
            owner,
            Collection::new(collection).expect("collection"),
            Key::new(key).expect("key"),
        )
    }

    fn value(score: i64) -> StorageValue {
        StorageValue::new(json!({ "score": score })).expect("value")
    }

    #[tokio::test]
    async fn runtime_write_then_read_round_trips() {
        let repo = InMemoryStorageRepository::new();
        let id = object_id(Owner::user(user("u-1")), "saves", "slot-1");
        let request = WriteRequest::upsert(id.clone(), value(10), Permissions::owner_private());

        let written = repo
            .write(&Accessor::Runtime, request)
            .await
            .expect("write succeeds");
        let read = repo
            .read(&Accessor::Runtime, &id)
            .await
            .expect("read succeeds")
            .expect("object present");
        assert_eq!(read.value, written.value);
        assert_eq!(read.version, written.version);
    }

    #[tokio::test]
    async fn must_not_exist_conflicts_when_present() {
        let repo = InMemoryStorageRepository::new();
        let id = object_id(Owner::System, "config", "global");
        let create = WriteRequest::upsert(id.clone(), value(1), Permissions::runtime_only())
            .expecting(Precondition::MustNotExist);
        repo.write(&Accessor::Runtime, create)
            .await
            .expect("first create succeeds");

        let again = WriteRequest::upsert(id, value(2), Permissions::runtime_only())
            .expecting(Precondition::MustNotExist);
        let err = repo
            .write(&Accessor::Runtime, again)
            .await
            .expect_err("second create conflicts");
        assert_eq!(err.category(), crate::error::ErrorCategory::Conflict);
    }

    #[tokio::test]
    async fn stale_version_match_conflicts() {
        let repo = InMemoryStorageRepository::new();
        let id = object_id(Owner::System, "config", "global");
        let first = repo
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(id.clone(), value(1), Permissions::runtime_only()),
            )
            .await
            .expect("first write");

        // Overwrite so the stored version moves on.
        repo.write(
            &Accessor::Runtime,
            WriteRequest::upsert(id.clone(), value(2), Permissions::runtime_only()),
        )
        .await
        .expect("second write");

        let stale = WriteRequest::upsert(id, value(3), Permissions::runtime_only())
            .expecting(Precondition::Match(first.version));
        let err = repo
            .write(&Accessor::Runtime, stale)
            .await
            .expect_err("stale version conflicts");
        assert_eq!(err.category(), crate::error::ErrorCategory::Conflict);
    }

    #[tokio::test]
    async fn idempotent_delete_of_absent_object() {
        let repo = InMemoryStorageRepository::new();
        let id = object_id(Owner::System, "config", "missing");
        repo.delete(&Accessor::Runtime, &id, Precondition::Any)
            .await
            .expect("delete of absent object is idempotent");
    }

    #[tokio::test]
    async fn list_rejects_zero_limit() {
        let repo = InMemoryStorageRepository::new();
        let query = ListQuery::across_owners(Collection::new("saves").expect("collection"), 0);
        let err = repo
            .list(&Accessor::Runtime, &query)
            .await
            .expect_err("zero limit rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[tokio::test]
    async fn atomic_batch_rolls_back_every_prior_write_when_a_later_cas_fails() {
        let repo = InMemoryStorageRepository::new();
        let first = object_id(Owner::System, "batch", "first");
        let second = object_id(Owner::System, "batch", "second");
        repo.write(
            &Accessor::Runtime,
            WriteRequest::upsert(second.clone(), value(1), Permissions::runtime_only()),
        )
        .await
        .expect("seed");
        let error = repo
            .atomic_batch(vec![
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(
                        first.clone(),
                        value(1),
                        Permissions::runtime_only(),
                    ),
                    membership: None,
                },
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(
                        second.clone(),
                        value(2),
                        Permissions::runtime_only(),
                    )
                    .expecting(Precondition::MustNotExist),
                    membership: None,
                },
            ])
            .await
            .expect_err("later conflict aborts batch");
        assert_eq!(error.category(), crate::error::ErrorCategory::Conflict);
        assert!(
            repo.read(&Accessor::Runtime, &first)
                .await
                .expect("read")
                .is_none()
        );
        assert_eq!(
            repo.read(&Accessor::Runtime, &second)
                .await
                .expect("read")
                .expect("seed remains")
                .value,
            value(1)
        );
    }

    #[tokio::test]
    async fn overlapping_atomic_batches_have_one_cas_winner_without_partial_state() {
        let repo = Arc::new(InMemoryStorageRepository::new());
        let gate = object_id(Owner::System, "batch", "gate");
        let left = object_id(Owner::System, "batch", "left");
        let right = object_id(Owner::System, "batch", "right");
        let gate_object = repo
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(gate.clone(), value(0), Permissions::runtime_only()),
            )
            .await
            .expect("seed");
        let batch = |output: ObjectId| {
            vec![
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(
                        gate.clone(),
                        value(1),
                        Permissions::runtime_only(),
                    )
                    .expecting(Precondition::Match(gate_object.version.clone())),
                    membership: None,
                },
                AtomicBatchOperation::Write {
                    accessor: Accessor::Runtime,
                    request: WriteRequest::upsert(output, value(1), Permissions::runtime_only()),
                    membership: None,
                },
            ]
        };
        let a = {
            let repo = Arc::clone(&repo);
            let batch = batch(left.clone());
            tokio::spawn(async move { repo.atomic_batch(batch).await })
        };
        let b = {
            let repo = Arc::clone(&repo);
            let batch = batch(right.clone());
            tokio::spawn(async move { repo.atomic_batch(batch).await })
        };
        let outcomes = [a.await.expect("join"), b.await.expect("join")];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome
                    .as_ref()
                    .is_err_and(|e| e.category() == crate::error::ErrorCategory::Conflict))
                .count(),
            1
        );
        let outputs = usize::from(
            repo.read(&Accessor::Runtime, &left)
                .await
                .expect("read")
                .is_some(),
        ) + usize::from(
            repo.read(&Accessor::Runtime, &right)
                .await
                .expect("read")
                .is_some(),
        );
        assert_eq!(outputs, 1, "losing batch left no projection");
    }
}
