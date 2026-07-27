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
//! `docs/architecture/database-abstraction.md`) without changing the domain
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
pub mod groups;
pub mod identity;
pub mod leaderboards;
pub mod notifications;
pub mod pg;
pub mod purchases;
pub mod session;
pub mod sqlite;
pub mod wallet;

pub use backend::{
    Backend, BackendKind, InMemoryBackend, InMemoryUnitOfWork, UnitOfWork, select_backend,
};
pub use chat::{
    ChannelSummary, ChannelType, ChatChannel, ChatDeliveryOutboxRecord, ChatMessage,
    ChatModerationAudit, ChatRateLimit, ChatRepository, DEFAULT_CHANNEL_HISTORY_CAP,
    InMemoryChatRepository,
};
pub use friends::{
    AddPlan, FriendRow, FriendState, FriendsRepository, InMemoryFriendsRepository, plan_add,
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
pub use wallet::{
    DEFAULT_LEDGER_CAPACITY, InMemoryWalletRepository, LedgerEntry, WalletRepository,
};

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::storage::{
    Accessor, Collection, CollectionSummary, Cursor, ListQuery, ObjectId, Page, Precondition,
    StorageIndexDefinition, StorageIndexMembership, StorageIndexName, StorageIndexQuery,
    StorageObject, Version, WriteRequest,
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

        let version = Version::of(&request.value);
        let object = StorageObject {
            id: request.id,
            value: request.value,
            version,
            permissions: request.permissions,
        };
        let configured_candidates = state
            .indexes
            .values()
            .filter(|index| index.matches_object(&object.id))
            .map(|index| index.name().clone())
            .collect::<std::collections::BTreeSet<_>>();
        let membership = membership
            .cloned()
            .unwrap_or_else(|| StorageIndexMembership::include_all(configured_candidates.clone()));
        if !configured_candidates.is_empty() && membership.candidates() != &configured_candidates {
            return Err(AppError::validation(
                "storage index membership candidates do not match configured indexes",
            ));
        }
        let candidates = if configured_candidates.is_empty() {
            membership.candidates().clone()
        } else {
            configured_candidates
        };
        state.objects.insert(object.id.clone(), object.clone());
        state.memberships.retain(|(index_name, object_id)| {
            object_id != &object.id || !candidates.contains(index_name)
        });
        state.memberships.extend(
            membership
                .included()
                .iter()
                .cloned()
                .map(|index_name| (index_name, object.id.clone())),
        );
        Ok(object)
    }

    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()> {
        let mut state = self.guard()?;
        match state.objects.get(id) {
            None => match expected {
                Precondition::Match(_) => Err(AppError::conflict(
                    "delete precondition failed: object does not exist",
                )),
                // Idempotent delete of an absent object.
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
            .keys()
            .filter(|id| index.matches_object(id))
            .cloned()
            .collect::<Vec<_>>();
        state
            .memberships
            .extend(matching.into_iter().map(|id| (index.name().clone(), id)));
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
            *counts.entry(object.id.collection.clone).or_insert(0) += 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Collection, Key, Owner, Permissions, Precondition, StorageValue, UserId};
    use serde_json::json;

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
}
