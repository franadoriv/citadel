//! Host-service seam for script runtimes.
//!
//! Script runtimes are synchronous, serialized command generators (see
//! [`crate::runtime`]); the domain services they need to expose to game logic —
//! friends today, storage next — are `async`. A host function that
//! must return a VALUE to the script (e.g. `friends.list`) cannot use the
//! fire-and-forget [`OutboundCommand`](crate::runtime::OutboundCommand) model, so
//! this module defines a **synchronous** [`DomainHost`] seam and one production
//! impl that bridges to the async services.
//!
//! ## The async bridge
//!
//! The production bridge runs each async call with
//! [`tokio::task::block_in_place`] + [`tokio::runtime::Handle::block_on`]. On the
//! server's multi-threaded runtime (`src/main.rs`) this hands the worker's other
//! tasks to a sibling worker for the duration, so it does not deadlock the
//! runtime. The VM lock is held only for this runtime's own dispatch, so the
//! call serializes THIS runtime's script dispatch behind it — acceptable for the
//! trusted single-operator tier, and negligible for the fast in-memory/SQLite
//! backends.
//!
//! ## Constraint
//!
//! `block_in_place` panics on a current-thread runtime, so tests that exercise a
//! bridged host call must use `#[tokio::test(flavor = "multi_thread")]`.

use std::future::Future;
use std::sync::Arc;

use crate::error::ErrorCategory;
use crate::repository::StorageRepository;
use crate::services::{
    AdmissionOutcome, ChatChannelAuthorizer, ChatRateLimitPolicy, ChatService, ChatTarget,
    CreateGroupRequest, FriendsService, Group, GroupFilter, GroupsService, LeaderboardService,
    PlayerNotification, PlayerNotificationPage, PlayerNotificationService, SendPlayerNotification,
    TournamentDiscoveryService, TournamentRegistrationState, UpdateGroupRequest, WalletService,
};
use crate::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, Precondition, ReadPermission,
    StorageIndexDefinition, StorageIndexMembership, StorageIndexName, StorageIndexQuery,
    StorageObject, StorageValue, UserId, Version, WritePermission, WriteRequest,
};
use crate::time::{Clock, SystemClock};

/// One friend relation row handed to a script (language-neutral DTO).
///
/// Mirrors `crate::services::FriendRow` but decouples the runtime seam from the
/// repository value type and carries the stable string `state` token adapters
/// hand to scripts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FriendRowDto {
    /// The other account.
    pub user_id: String,
    /// Stable state token: `invited_sent`/`invited_received`/`friend`/`blocked`.
    pub state: String,
    /// When the relation last changed (Unix millis).
    pub updated_unix_ms: u64,
}

/// A storage object handed to a script without exposing a repository backend.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageObjectDto {
    /// JSON object payload, encoded for the language adapters to deserialize.
    pub value_json: String,
    /// Opaque optimistic-concurrency token.
    pub version: String,
    /// Numeric read permission (`0` runtime-only, `1` owner, `2` public).
    pub read_permission: u8,
    /// Numeric write permission (`0` runtime-only, `1` owner).
    pub write_permission: u8,
}

/// A storage-index result handed to a script without exposing repository types.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageIndexObjectDto {
    /// Owning user id, or `None` for a server/system-owned object.
    pub user_id: Option<String>,
    /// Indexed object's collection.
    pub collection: String,
    /// Indexed object's key.
    pub key: String,
    /// Versioned object payload and permissions.
    pub object: StorageObjectDto,
}

/// Input for one user-owned script-storage write.
///
/// Grouping the address, JSON value, precondition, and permissions keeps the
/// language-neutral host seam extensible without a long positional argument
/// list. The adapters retain their idiomatic public function signatures.
#[derive(Debug, Clone, Copy)]
pub struct StorageWriteInput<'a> {
    user: &'a str,
    collection: &'a str,
    key: &'a str,
    value_json: &'a str,
    expected_version: Option<&'a str>,
    read_permission: Option<u8>,
    write_permission: Option<u8>,
    included_index_names_json: Option<&'a str>,
}

impl<'a> StorageWriteInput<'a> {
    /// Start an unconstrained upsert with the default owner-only permissions.
    #[must_use]
    pub const fn new(
        user: &'a str,
        collection: &'a str,
        key: &'a str,
        value_json: &'a str,
    ) -> Self {
        Self {
            user,
            collection,
            key,
            value_json,
            expected_version: None,
            read_permission: None,
            write_permission: None,
            included_index_names_json: None,
        }
    }

    /// Require an object to be absent (`Some("")`) or match an opaque version.
    #[must_use]
    pub const fn expecting(mut self, version: Option<&'a str>) -> Self {
        self.expected_version = version;
        self
    }

    /// Override the default owner-only read/write permissions.
    #[must_use]
    pub const fn with_permissions(mut self, read: Option<u8>, write: Option<u8>) -> Self {
        self.read_permission = read;
        self.write_permission = write;
        self
    }

    /// Attach the JSON array of configured index names accepted by the runtime
    /// callbacks for this write. `None` keeps the normal include-all default.
    #[must_use]
    pub const fn with_included_index_names_json(mut self, names: Option<&'a str>) -> Self {
        self.included_index_names_json = names;
        self
    }
}

/// Synchronous host-facing view of the persisted domain services.
///
/// Adapters (Lua/Python/JS) call these synchronous methods from host functions;
/// the impl performs the async work internally. Every method acts as the given
/// `user`; the acting user is supplied explicitly by the script (in the trusted
/// tier the server script is authoritative and may act as any user by design).
/// Errors are already sanitized to short strings — no raw backend errors leak.
pub trait DomainHost: Send + Sync {
    /// Invite `other`, or accept their pending invite. Returns the new state
    /// token for `user`'s side of the relation.
    fn friends_add(&self, user: &str, other: &str) -> Result<String, String>;
    /// Remove any relation between the two (both directions). Returns whether
    /// anything was removed.
    fn friends_remove(&self, user: &str, other: &str) -> Result<bool, String>;
    /// Block `other` from `user`'s side.
    fn friends_block(&self, user: &str, other: &str) -> Result<(), String>;
    /// This user's relations, other-id-ordered.
    fn friends_list(&self, user: &str) -> Result<Vec<FriendRowDto>, String>;
    /// Persist and best-effort locally deliver one player notification.
    fn notifications_send(
        &self,
        recipient: &str,
        code: i32,
        subject: &str,
        content_json: &str,
        sender: Option<&str>,
        delivery_key: Option<&str>,
    ) -> Result<PlayerNotification, String>;
    /// List one recipient's durable inbox, newest first.
    fn notifications_list(
        &self,
        recipient: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<PlayerNotificationPage, String>;
    /// Idempotently mark recipient-owned notification ids read.
    fn notifications_mark_read(
        &self,
        recipient: &str,
        ids: &[String],
    ) -> Result<Vec<String>, String>;
    /// Execute a persisted groups operation as `actor`. `payload_json` and the
    /// returned JSON use the same schema as the built-in `groups.*` client RPC.
    fn groups_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String>;
    /// Execute leaderboard reads/submissions as `actor`, using client-RPC JSON.
    fn leaderboards_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String>;
    /// Execute player-facing tournament discovery as `actor`.
    fn tournaments_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String>;
    /// Execute durable chat send/history as `actor`, using client-RPC JSON.
    fn chat_call(&self, actor: &str, operation: &str, payload_json: &str)
    -> Result<String, String>;
    /// Execute wallet reads or a trusted authoritative adjustment as `actor`.
    fn wallet_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String>;
    /// Read one user-owned storage object. A missing object returns `None`.
    fn storage_read(
        &self,
        user: &str,
        collection: &str,
        key: &str,
    ) -> Result<Option<StorageObjectDto>, String>;
    /// Upsert or create one user-owned storage object.
    fn storage_write(&self, input: StorageWriteInput<'_>) -> Result<StorageObjectDto, String>;
    /// Return operator-configured indexes that cover this storage identity.
    /// Runtime adapters use this before invoking their locally registered
    /// callbacks. An unavailable/default host has no configured indexes.
    fn storage_index_candidates(
        &self,
        _user: &str,
        _collection: &str,
        _key: &str,
    ) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
    /// Delete one user-owned storage object.
    fn storage_delete(
        &self,
        user: &str,
        collection: &str,
        key: &str,
        expected_version: Option<&str>,
    ) -> Result<(), String>;
    /// Query one statically configured storage index using a JSON-object map of
    /// equality filters.
    fn storage_index_query(
        &self,
        index_name: &str,
        filters_json: &str,
        limit: usize,
    ) -> Result<Vec<StorageIndexObjectDto>, String>;
}

/// Production [`DomainHost`] over the async domain services.
///
/// Bridges each call with [`ServiceDomainHost::block`]. Constructed once at
/// server assembly from the selected backend's services and shared with the
/// runtime as `Arc<dyn DomainHost>`.
pub struct ServiceDomainHost {
    friends: Arc<FriendsService>,
    storage: Arc<dyn StorageRepository>,
    storage_indexes: Vec<StorageIndexDefinition>,
    player_notifications: Option<Arc<PlayerNotificationService>>,
    groups: Option<Arc<GroupsService>>,
    leaderboards: Option<Arc<LeaderboardService>>,
    tournaments: Option<Arc<TournamentDiscoveryService>>,
    chat: Option<Arc<ChatService>>,
    chat_authorizer: Option<Arc<ChatChannelAuthorizer>>,
    chat_rate_limits: Option<ChatRateLimitPolicy>,
    node_id: String,
    wallet: Option<Arc<WalletService>>,
}

impl ServiceDomainHost {
    /// Wrap the domain services in the synchronous host seam.
    #[must_use]
    pub fn new(friends: Arc<FriendsService>, storage: Arc<dyn StorageRepository>) -> Self {
        Self {
            friends,
            storage,
            storage_indexes: Vec::new(),
            player_notifications: None,
            groups: None,
            leaderboards: None,
            tournaments: None,
            chat: None,
            chat_authorizer: None,
            chat_rate_limits: None,
            node_id: "runtime".to_owned(),
            wallet: None,
        }
    }

    /// Add the player-inbox domain to this runtime host.
    #[must_use]
    pub fn with_player_notifications(mut self, service: Arc<PlayerNotificationService>) -> Self {
        self.player_notifications = Some(service);
        self
    }

    /// Attach the validated static storage-index declarations available to game
    /// logic. Physical durable indexes are installed during application
    /// bootstrap; this only supplies the name-to-definition registry used to
    /// reject undeclared query fields at the host boundary.
    #[must_use]
    pub fn with_storage_indexes(mut self, indexes: Vec<StorageIndexDefinition>) -> Self {
        self.storage_indexes = indexes;
        self
    }

    /// Add the groups/clans domain to this runtime host.
    #[must_use]
    pub fn with_groups(mut self, service: Arc<GroupsService>) -> Self {
        self.groups = Some(service);
        self
    }

    /// Add game leaderboard operations to this runtime host.
    #[must_use]
    pub fn with_leaderboards(mut self, service: Arc<LeaderboardService>) -> Self {
        self.leaderboards = Some(service);
        self
    }

    /// Add player-facing tournament discovery to this runtime host.
    #[must_use]
    pub fn with_tournaments(mut self, service: Arc<TournamentDiscoveryService>) -> Self {
        self.tournaments = Some(service);
        self
    }

    /// Add durable chat history operations to this runtime host.
    #[must_use]
    pub fn with_chat(mut self, service: Arc<ChatService>) -> Self {
        self.chat = Some(service);
        self
    }

    /// Attach the canonical target authorizer required by the script chat
    /// bridge. Without it the bridge remains unavailable rather than accepting
    /// the retired raw channel/type request shape.
    #[must_use]
    pub fn with_chat_authorizer(mut self, authorizer: Arc<ChatChannelAuthorizer>) -> Self {
        self.chat_authorizer = Some(authorizer);
        self
    }

    /// Add the configured cross-node rate-limit policy for script chat calls.
    #[must_use]
    pub fn with_chat_rate_limits(mut self, policy: ChatRateLimitPolicy) -> Self {
        self.chat_rate_limits = Some(policy);
        self
    }

    /// Attribute trusted runtime moderation actions to the serving node.
    #[must_use]
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = node_id;
        self
    }

    /// Add wallet reads and trusted adjustments to this runtime host.
    #[must_use]
    pub fn with_wallet(mut self, service: Arc<WalletService>) -> Self {
        self.wallet = Some(service);
        self
    }

    /// Run an async future to completion from a synchronous host call.
    ///
    /// Requires the multi-threaded tokio runtime (the server's); panics on a
    /// current-thread runtime, so bridged host calls must run under
    /// `#[tokio::test(flavor = "multi_thread")]` in tests.
    fn block<F: Future>(fut: F) -> F::Output {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
    }
}

impl DomainHost for ServiceDomainHost {
    fn friends_add(&self, user: &str, other: &str) -> Result<String, String> {
        Self::block(self.friends.add(user, other, SystemClock.now()))
            .map(|state| state.as_str().to_string())
            .map_err(|err| err.to_string())
    }

    fn friends_remove(&self, user: &str, other: &str) -> Result<bool, String> {
        Self::block(self.friends.remove(user, other)).map_err(|err| err.to_string())
    }

    fn friends_block(&self, user: &str, other: &str) -> Result<(), String> {
        Self::block(self.friends.block(user, other, SystemClock.now()))
            .map_err(|err| err.to_string())
    }

    fn friends_list(&self, user: &str) -> Result<Vec<FriendRowDto>, String> {
        Self::block(self.friends.list(user))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| FriendRowDto {
                        user_id: row.user_id,
                        state: row.state.as_str().to_string(),
                        updated_unix_ms: row.updated_unix_ms,
                    })
                    .collect()
            })
            .map_err(|err| err.to_string())
    }

    fn notifications_send(
        &self,
        recipient: &str,
        code: i32,
        subject: &str,
        content_json: &str,
        sender: Option<&str>,
        delivery_key: Option<&str>,
    ) -> Result<PlayerNotification, String> {
        let service = self
            .player_notifications
            .as_ref()
            .ok_or_else(|| "notifications host not available".to_string())?;
        let content = serde_json::from_str(content_json)
            .map_err(|_| "notification validation: content must be JSON".to_string())?;
        Self::block(service.send_now(SendPlayerNotification {
            recipient: recipient.to_owned(),
            code,
            subject: subject.to_owned(),
            content,
            sender: sender.map(ToOwned::to_owned),
            delivery_key: delivery_key.map(ToOwned::to_owned),
        }))
        .map(|outcome| outcome.notification)
        .map_err(|err| err.to_string())
    }

    fn notifications_list(
        &self,
        recipient: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<PlayerNotificationPage, String> {
        let service = self
            .player_notifications
            .as_ref()
            .ok_or_else(|| "notifications host not available".to_string())?;
        Self::block(service.list(recipient, limit, cursor)).map_err(|err| err.to_string())
    }

    fn notifications_mark_read(
        &self,
        recipient: &str,
        ids: &[String],
    ) -> Result<Vec<String>, String> {
        let service = self
            .player_notifications
            .as_ref()
            .ok_or_else(|| "notifications host not available".to_string())?;
        Self::block(service.mark_read(recipient, ids, SystemClock.now()))
            .map_err(|err| err.to_string())
    }

    fn groups_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let service = self
            .groups
            .as_ref()
            .ok_or_else(|| "groups host not available".to_string())?;
        let payload: serde_json::Value = serde_json::from_str(payload_json)
            .map_err(|_| "groups validation: payload must be a JSON object".to_string())?;
        let object = payload
            .as_object()
            .ok_or_else(|| "groups validation: payload must be a JSON object".to_string())?;
        let id = || {
            object
                .get("group_id")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    "groups validation: missing unsigned integer field: group_id".to_string()
                })
        };
        let user_id = || {
            object
                .get("user_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "groups validation: missing string field: user_id".to_string())
        };
        let result = match operation {
            "create" => {
                let name = object
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "groups validation: missing string field: name".to_string())?;
                let description = object
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let open = object
                    .get("open")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true);
                let max_size = object
                    .get("max_size")
                    .and_then(serde_json::Value::as_u64)
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| "groups validation: max_size must be u32".to_string())?
                    .unwrap_or(0);
                Self::block(service.create_for_player(
                    actor,
                    CreateGroupRequest {
                        name: name.to_owned(),
                        description: description.to_owned(),
                        open,
                        max_size,
                        creator_user_id: String::new(),
                        now: SystemClock.now(),
                    },
                ))
                .map(|group| groups_detail_json(&group))
            }
            "list" => {
                let name_contains = object
                    .get("name_contains")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned);
                let limit = object
                    .get("limit")
                    .and_then(serde_json::Value::as_u64)
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| "groups validation: limit must be usize".to_string())?
                    .unwrap_or(50)
                    .min(200);
                let offset = object
                    .get("offset")
                    .and_then(serde_json::Value::as_u64)
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| "groups validation: offset must be usize".to_string())?
                    .unwrap_or(0);
                Self::block(service.list(&GroupFilter { name_contains, limit, offset }))
                    .map(|page| serde_json::json!({"items": page.items.iter().map(groups_summary_json).collect::<Vec<_>>(), "total": page.total}))
            }
            "get" => Self::block(service.get(id()?)).map(|group| groups_detail_json(&group)),
            "update" => {
                let description = match object.get("description") {
                    Some(value) => Some(
                        value
                            .as_str()
                            .ok_or_else(|| {
                                "groups validation: description must be a string".to_string()
                            })?
                            .to_owned(),
                    ),
                    None => None,
                };
                let open =
                    match object.get("open") {
                        Some(value) => Some(value.as_bool().ok_or_else(|| {
                            "groups validation: open must be a boolean".to_string()
                        })?),
                        None => None,
                    };
                let max_size = object
                    .get("max_size")
                    .and_then(serde_json::Value::as_u64)
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| "groups validation: max_size must be u32".to_string())?;
                Self::block(service.update_as_player(
                    actor,
                    id()?,
                    UpdateGroupRequest {
                        description,
                        open,
                        max_size,
                    },
                ))
                .map(|group| groups_detail_json(&group))
            }
            "delete" => {
                Self::block(service.delete_as_player(actor, id()?)).map(|()| serde_json::json!({}))
            }
            "add_member" => Self::block(service.add_member_as_player(
                actor,
                id()?,
                user_id()?,
                SystemClock.now(),
            ))
            .map(|group| groups_detail_json(&group)),
            "leave" => Self::block(service.leave_as_player(actor, id()?))
                .map(|group| groups_detail_json(&group)),
            "kick" => Self::block(service.kick_member_as_player(actor, id()?, user_id()?))
                .map(|group| groups_detail_json(&group)),
            "promote" => Self::block(service.promote_as_player(actor, id()?, user_id()?))
                .map(|group| groups_detail_json(&group)),
            "demote" => Self::block(service.demote_as_player(actor, id()?, user_id()?))
                .map(|group| groups_detail_json(&group)),
            "join" => Self::block(service.join_as_player(actor, id()?, SystemClock.now()))
                .map(groups_admission_json),
            "invite" => {
                Self::block(service.invite_as_player(actor, id()?, user_id()?, SystemClock.now()))
                    .map(groups_admission_json)
            }
            "approve_request" => Self::block(service.approve_request_as_player(
                actor,
                id()?,
                user_id()?,
                SystemClock.now(),
            ))
            .map(|group| groups_detail_json(&group)),
            "accept_invitation" => {
                Self::block(service.accept_invitation_as_player(actor, id()?, SystemClock.now()))
                    .map(|group| groups_detail_json(&group))
            }
            "cancel_admission" => Self::block(service.cancel_admission_as_player(actor, id()?))
                .map(|()| serde_json::json!({})),
            "transfer_ownership" => {
                Self::block(service.transfer_ownership_as_player(actor, id()?, user_id()?))
                    .map(|group| groups_detail_json(&group))
            }
            _ => return Err("groups validation: unknown operation".to_string()),
        };
        result
            .map(|value| value.to_string())
            .map_err(|err| err.to_string())
    }

    fn leaderboards_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let service = self
            .leaderboards
            .as_ref()
            .ok_or_else(|| "leaderboards host not available".to_string())?;
        let payload = domain_json_object(payload_json, "leaderboards")?;
        let board = || {
            payload
                .get("board_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    "leaderboards validation: missing string field: board_id".to_string()
                })
        };
        let result = match operation {
            "list" => Self::block(service.list()).and_then(|items| {
                serde_json::to_value(items).map_err(|e| crate::AppError::internal(e.to_string()))
            }),
            "records" => {
                let limit = payload
                    .get("limit")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(50)
                    .min(200) as usize;
                let offset = payload
                    .get("offset")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0) as usize;
                Self::block(service.records(board()?, limit, offset)).and_then(|page| {
                    serde_json::to_value(page).map_err(|e| crate::AppError::internal(e.to_string()))
                })
            }
            "submit" => {
                let score = payload
                    .get("score")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        "leaderboards validation: missing signed integer field: score".to_string()
                    })?;
                let subscore = payload
                    .get("subscore")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                Self::block(service.submit(
                    board()?,
                    actor,
                    score,
                    subscore,
                    payload.get("metadata").cloned(),
                    SystemClock.now(),
                ))
                .and_then(|record| {
                    serde_json::to_value(record)
                        .map_err(|e| crate::AppError::internal(e.to_string()))
                })
            }
            _ => return Err("leaderboards validation: unknown operation".to_string()),
        };
        result
            .map(|value| value.to_string())
            .map_err(|err| err.to_string())
    }

    fn tournaments_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let service = self
            .tournaments
            .as_ref()
            .ok_or_else(|| "tournaments host not available".to_string())?;
        let payload = domain_json_object(payload_json, "tournaments")?;
        let id = || {
            payload
                .get("tournament_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    "tournaments validation: missing string field: tournament_id".to_string()
                })
        };
        let result = match operation {
            "list" => Self::block(service.list_active_and_upcoming()).and_then(|items| {
                serde_json::to_value(items)
                    .map_err(|error| crate::AppError::internal(error.to_string()))
            }),
            "get" => Self::block(service.get(id()?)).and_then(|tournament| {
                serde_json::to_value(tournament)
                    .map_err(|error| crate::AppError::internal(error.to_string()))
            }),
            "results" => Self::block(service.results(id()?)).and_then(|items| {
                serde_json::to_value(items)
                    .map_err(|error| crate::AppError::internal(error.to_string()))
            }),
            "registration" => {
                Self::block(service.registration_state(id()?, actor, SystemClock.now()))
                    .map(|state| match state {
                        TournamentRegistrationState::Registered => "registered",
                        TournamentRegistrationState::Open => "open",
                        TournamentRegistrationState::Closed => "closed",
                    })
                    .map(|state| serde_json::json!({"state": state}))
            }
            _ => return Err("tournaments validation: unknown operation".to_string()),
        };
        result
            .map(|value| value.to_string())
            .map_err(|error| error.to_string())
    }

    fn chat_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let service = self
            .chat
            .as_ref()
            .ok_or_else(|| "chat host not available".to_string())?;
        let authorizer = self
            .chat_authorizer
            .as_ref()
            .ok_or_else(|| "chat host not available".to_string())?;
        let payload = domain_json_object(payload_json, "chat")?;
        if payload.contains_key("channel") || payload.contains_key("channel_type") {
            return Err("CHAT_PROTOCOL_UPGRADE_REQUIRED".to_string());
        }
        let rate_limits = self.chat_rate_limits.clone().unwrap_or_default();
        let target = runtime_chat_target(&payload)?;
        let moderation_group_id = match (operation, &target) {
            ("moderate", ChatTarget::Group { group_id }) => Some(*group_id),
            ("moderate", _) => return Err("CHAT_UNAVAILABLE".to_string()),
            _ => None,
        };
        Self::block(service.consume_rate_limits(&rate_limits.join(actor), SystemClock.now()))
            .map_err(|error| error.to_string())?;
        let lease = Self::block(authorizer.authorize_fenced(actor, target))
            .map_err(|error| error.to_string())?;
        let channel = Self::block(service.resolve_canonical_channel(
            &lease.channel.canonical_key,
            lease.channel.channel_type,
            SystemClock.now(),
        ))
        .map_err(|error| error.to_string())?;
        let result = match operation {
            "send" => {
                let content = payload
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "chat validation: missing string field: content".to_string())?;
                Self::block(
                    service.consume_rate_limits(
                        &rate_limits.send(actor, &channel.id),
                        SystemClock.now(),
                    ),
                )
                .and_then(|()| {
                    Self::block(service.append_authorized(
                        &channel.id,
                        lease.channel.channel_type,
                        actor,
                        content,
                        &lease.channel.access_key,
                        lease.access_epoch,
                        SystemClock.now(),
                    ))
                })
                .map(|id| serde_json::json!({"channel_id": channel.id, "id": id}))
            }
            "history" => {
                let limit = payload
                    .get("limit")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(50)
                    .min(200) as usize;
                let before = payload.get("before_id").and_then(serde_json::Value::as_u64);
                Self::block(
                    service.consume_rate_limits(&rate_limits.history(actor), SystemClock.now()),
                )
                .and_then(|()| {
                    Self::block(service.authorized_messages(
                        &channel.id,
                        limit,
                        before,
                        &lease.channel.access_key,
                        lease.access_epoch,
                    ))
                })
                .and_then(|items| {
                    serde_json::to_value(items)
                        .map_err(|e| crate::AppError::internal(e.to_string()))
                })
            }
            "edit" => {
                let id = payload
                    .get("id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| "chat validation: missing unsigned field: id".to_string())?;
                let content = payload
                    .get("content")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "chat validation: missing string field: content".to_string())?;
                Self::block(service.consume_rate_limits(
                    &rate_limits.mutation(actor, &channel.id),
                    SystemClock.now(),
                ))
                .and_then(|()| {
                    Self::block(service.edit_as_author(
                        &channel.id,
                        id,
                        actor,
                        content,
                        &lease.channel.access_key,
                        lease.access_epoch,
                        crate::services::DEFAULT_AUTHOR_EDIT_WINDOW_MS,
                        SystemClock.now(),
                    ))
                })
                .and_then(|message| {
                    serde_json::to_value(message)
                        .map_err(|error| crate::AppError::internal(error.to_string()))
                })
            }
            "delete" => {
                let id = payload
                    .get("id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| "chat validation: missing unsigned field: id".to_string())?;
                Self::block(service.consume_rate_limits(
                    &rate_limits.mutation(actor, &channel.id),
                    SystemClock.now(),
                ))
                .and_then(|()| {
                    Self::block(service.delete_as_author(
                        &channel.id,
                        id,
                        actor,
                        &lease.channel.access_key,
                        lease.access_epoch,
                        crate::services::DEFAULT_AUTHOR_DELETE_WINDOW_MS,
                        SystemClock.now(),
                    ))
                })
                .map(|deleted| serde_json::json!({"channel_id": channel.id, "deleted": deleted}))
            }
            "moderate" => {
                let id = payload
                    .get("id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| "chat validation: missing unsigned field: id".to_string())?;
                let groups = self
                    .groups
                    .as_ref()
                    .ok_or_else(|| "chat host not available".to_string())?;
                let group_id = moderation_group_id.ok_or_else(|| "CHAT_UNAVAILABLE".to_string())?;
                let message = Self::block(service.authorized_messages(
                    &channel.id,
                    0,
                    None,
                    &lease.channel.access_key,
                    lease.access_epoch,
                ))
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|message| message.id == id)
                .ok_or_else(|| "CHAT_UNAVAILABLE".to_string())?;
                Self::block(groups.authorize_chat_moderation(actor, group_id, &message.sender))
                    .map_err(|error| error.to_string())?;
                Self::block(service.consume_rate_limits(
                    &rate_limits.moderation(actor, &channel.id),
                    SystemClock.now(),
                ))
                .and_then(|()| {
                    Self::block(service.moderate_delete_message_authorized(
                        &channel.id,
                        id,
                        "group_admin",
                        actor,
                        "group_moderation",
                        &lease.channel.access_key,
                        lease.access_epoch,
                        "",
                        &self.node_id,
                        SystemClock.now(),
                    ))
                })
                .map(|deleted| serde_json::json!({"channel_id": channel.id, "deleted": deleted}))
            }
            _ => return Err("chat validation: unknown operation".to_string()),
        };
        result
            .map(|value| value.to_string())
            .map_err(|err| err.to_string())
    }

    fn wallet_call(
        &self,
        actor: &str,
        operation: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let service = self
            .wallet
            .as_ref()
            .ok_or_else(|| "wallet host not available".to_string())?;
        let payload = domain_json_object(payload_json, "wallet")?;
        let user = payload
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(actor);
        let result = match operation {
            "balances" => Self::block(service.balances(user)).and_then(|items| {
                serde_json::to_value(items).map_err(|e| crate::AppError::internal(e.to_string()))
            }),
            "ledger" => Self::block(
                service.ledger(
                    user,
                    payload
                        .get("limit")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(50)
                        .min(200) as usize,
                ),
            )
            .and_then(|items| {
                serde_json::to_value(items).map_err(|e| crate::AppError::internal(e.to_string()))
            }),
            "adjust" => {
                let currency = payload
                    .get("currency")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        "wallet validation: missing string field: currency".to_string()
                    })?;
                let delta = payload
                    .get("delta")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        "wallet validation: missing signed integer field: delta".to_string()
                    })?;
                let reason = payload
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "wallet validation: missing string field: reason".to_string())?;
                Self::block(service.adjust(user, currency, delta, reason, SystemClock.now()))
                    .map(|balance| serde_json::json!({"balance": balance}))
            }
            _ => return Err("wallet validation: unknown operation".to_string()),
        };
        result
            .map(|value| value.to_string())
            .map_err(|err| err.to_string())
    }

    fn storage_read(
        &self,
        user: &str,
        collection: &str,
        key: &str,
    ) -> Result<Option<StorageObjectDto>, String> {
        let id = storage_id(user, collection, key)?;
        Self::block(self.storage.read(&Accessor::Runtime, &id))
            .map(|object| object.map(storage_object_dto))
            .map_err(storage_error)
    }

    fn storage_write(&self, input: StorageWriteInput<'_>) -> Result<StorageObjectDto, String> {
        let id = storage_id(input.user, input.collection, input.key)?;
        let json = serde_json::from_str(input.value_json)
            .map_err(|_| "storage validation: value must be a JSON object".to_string())?;
        let value = StorageValue::new(json)
            .map_err(|_| "storage validation: value must be a JSON object".to_string())?;
        let permissions = Permissions {
            read: input
                .read_permission
                .map(ReadPermission::from_code)
                .transpose()
                .map_err(storage_error)?
                .unwrap_or(ReadPermission::OwnerRead),
            write: input
                .write_permission
                .map(WritePermission::from_code)
                .transpose()
                .map_err(storage_error)?
                .unwrap_or(WritePermission::OwnerWrite),
        };
        let request = WriteRequest::upsert(id, value, permissions)
            .expecting(storage_precondition(input.expected_version));
        let candidates = self
            .storage_indexes
            .iter()
            .filter(|index| index.matches_object(&request.id))
            .map(|index| index.name().clone())
            .collect::<std::collections::BTreeSet<_>>();
        let membership = match input.included_index_names_json {
            Some(json) => {
                let names = serde_json::from_str::<Vec<String>>(json).map_err(|_| {
                    "storage validation: included_index_names must be a JSON string array"
                        .to_string()
                })?;
                let included = names
                    .into_iter()
                    .map(StorageIndexName::new)
                    .collect::<Result<std::collections::BTreeSet<_>, _>>()
                    .map_err(storage_error)?;
                StorageIndexMembership::new(candidates, included).map_err(storage_error)
            }
            None => Ok(StorageIndexMembership::include_all(candidates)),
        }?;
        Self::block(
            self.storage
                .write_indexed(&Accessor::Runtime, request, Some(&membership)),
        )
        .map(storage_object_dto)
        .map_err(storage_error)
    }

    fn storage_index_candidates(
        &self,
        user: &str,
        collection: &str,
        key: &str,
    ) -> Result<Vec<String>, String> {
        let id = storage_id(user, collection, key)?;
        Ok(self
            .storage_indexes
            .iter()
            .filter(|index| index.matches_object(&id))
            .map(|index| index.name().as_str().to_string())
            .collect())
    }

    fn storage_delete(
        &self,
        user: &str,
        collection: &str,
        key: &str,
        expected_version: Option<&str>,
    ) -> Result<(), String> {
        let id = storage_id(user, collection, key)?;
        Self::block(self.storage.delete(
            &Accessor::Runtime,
            &id,
            storage_precondition(expected_version),
        ))
        .map_err(storage_error)
    }

    fn storage_index_query(
        &self,
        index_name: &str,
        filters_json: &str,
        limit: usize,
    ) -> Result<Vec<StorageIndexObjectDto>, String> {
        let index_name = StorageIndexName::new(index_name).map_err(storage_error)?;
        let index = self
            .storage_indexes
            .iter()
            .find(|index| index.name() == &index_name)
            .cloned()
            .ok_or_else(|| "storage validation: storage index is not configured".to_string())?;
        let filters = serde_json::from_str::<serde_json::Value>(filters_json)
            .map_err(|_| "storage validation: filters_json must be a JSON object".to_string())?;
        let filters = filters
            .as_object()
            .ok_or_else(|| "storage validation: filters_json must be a JSON object".to_string())?;
        let query =
            StorageIndexQuery::from_json_filters(index, filters, limit).map_err(storage_error)?;
        Self::block(self.storage.query_index(&Accessor::Runtime, &query))
            .map(|objects| objects.into_iter().map(storage_index_object_dto).collect())
            .map_err(storage_error)
    }
}

fn runtime_chat_target(
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<ChatTarget, String> {
    let target = payload
        .get("target")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "chat validation: missing object field: target".to_string())?;
    let kind = target
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "chat validation: missing string field: target.kind".to_string())?;
    match kind {
        "direct" => target
            .get("other_user_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|other_user_id| ChatTarget::Direct {
                other_user_id: other_user_id.to_owned(),
            })
            .ok_or_else(|| {
                "chat validation: missing string field: target.other_user_id".to_string()
            }),
        "group" => target
            .get("group_id")
            .and_then(serde_json::Value::as_u64)
            .map(|group_id| ChatTarget::Group { group_id })
            .ok_or_else(|| "chat validation: missing unsigned field: target.group_id".to_string()),
        "room" => Err("chat validation: runtime room targets are unavailable".to_string()),
        _ => Err("chat validation: unknown target.kind".to_string()),
    }
}

fn groups_summary_json(group: &Group) -> serde_json::Value {
    serde_json::json!({"id": group.id, "name": group.name, "description": group.description,
        "open": group.open, "max_size": group.max_size, "member_count": group.member_count(),
        "created_at_unix_ms": group.created_at.unix_millis()})
}

fn groups_detail_json(group: &Group) -> serde_json::Value {
    let mut value = groups_summary_json(group);
    value["members"] = serde_json::Value::Array(group.members().iter().map(|member| serde_json::json!({
        "user_id": member.user_id, "role": member.role.as_str(), "joined_at_unix_ms": member.joined_at.unix_millis(),
    })).collect());
    value
}

fn groups_admission_json(outcome: AdmissionOutcome) -> serde_json::Value {
    match outcome {
        AdmissionOutcome::Joined(group) => {
            serde_json::json!({"outcome": "joined", "group": groups_detail_json(&group)})
        }
        AdmissionOutcome::RequestCreated => serde_json::json!({"outcome": "request_created"}),
        AdmissionOutcome::InvitationCreated => {
            serde_json::json!({"outcome": "invitation_created"})
        }
        AdmissionOutcome::AlreadyMember(group) => {
            serde_json::json!({"outcome": "already_member", "group": groups_detail_json(&group)})
        }
    }
}

fn domain_json_object(
    payload_json: &str,
    domain: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    serde_json::from_str::<serde_json::Value>(payload_json)
        .map_err(|_| format!("{domain} validation: payload must be a JSON object"))?
        .as_object()
        .cloned()
        .ok_or_else(|| format!("{domain} validation: payload must be a JSON object"))
}

fn storage_id(user: &str, collection: &str, key: &str) -> Result<ObjectId, String> {
    let user = UserId::new(user).map_err(storage_error)?;
    let collection = Collection::new(collection).map_err(storage_error)?;
    let key = Key::new(key).map_err(storage_error)?;
    Ok(ObjectId::new(Owner::User(user), collection, key))
}

fn storage_precondition(expected_version: Option<&str>) -> Precondition {
    match expected_version {
        None => Precondition::Any,
        Some("") => Precondition::MustNotExist,
        Some(version) => Precondition::Match(Version::from_token(version.to_owned())),
    }
}

fn storage_object_dto(object: StorageObject) -> StorageObjectDto {
    StorageObjectDto {
        value_json: object.value.into_json().to_string(),
        version: object.version.as_str().to_owned(),
        read_permission: object.permissions.read.code(),
        write_permission: object.permissions.write.code(),
    }
}

fn storage_index_object_dto(object: StorageObject) -> StorageIndexObjectDto {
    let user_id = match &object.id.owner {
        Owner::System => None,
        Owner::User(user) => Some(user.as_str().to_string()),
    };
    StorageIndexObjectDto {
        user_id,
        collection: object.id.collection.as_str().to_string(),
        key: object.id.key.as_str().to_string(),
        object: storage_object_dto(object),
    }
}

fn storage_error(error: crate::AppError) -> String {
    match error.category() {
        ErrorCategory::Validation => format!("storage validation: {}", error.message()),
        ErrorCategory::Conflict => "storage conflict".to_string(),
        ErrorCategory::Permission => "storage permission denied".to_string(),
        _ => "storage operation failed".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;
    use crate::repository::{InMemoryFriendsRepository, InMemoryStorageRepository, SqliteDatabase};

    fn host() -> ServiceDomainHost {
        host_with(Arc::new(InMemoryStorageRepository::new()))
    }

    fn host_with(storage: Arc<dyn StorageRepository>) -> ServiceDomainHost {
        ServiceDomainHost::new(
            Arc::new(FriendsService::new(Arc::new(
                InMemoryFriendsRepository::new(),
            ))),
            storage,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_then_list_bridges_the_async_service() {
        let host = host();
        assert_eq!(
            host.friends_add("alice", "bob").expect("add"),
            "invited_sent"
        );
        let rows = host.friends_list("alice").expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user_id, "bob");
        assert_eq!(rows[0].state, "invited_sent");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_reports_whether_anything_was_removed() {
        let host = host();
        host.friends_add("alice", "bob").expect("add");
        assert!(host.friends_remove("alice", "bob").expect("remove"));
        assert!(!host.friends_remove("alice", "bob").expect("remove-again"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn self_friendship_is_a_sanitized_error() {
        let host = host();
        let err = host
            .friends_add("alice", "alice")
            .expect_err("self add rejected");
        assert!(err.contains("cannot befriend yourself"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn block_is_one_sided_and_then_removable() {
        let host = host();
        host.friends_block("alice", "bob").expect("block");
        let rows = host.friends_list("alice").expect("list");
        assert_eq!(rows[0].state, "blocked");
        assert!(
            host.friends_remove("alice", "bob")
                .expect("unblock removes")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_write_read_conflict_and_delete_are_bridged_and_sanitized() {
        let host = host();
        let written = host
            .storage_write(
                StorageWriteInput::new("alice", "saves", "slot", r#"{"level": 1}"#)
                    .expecting(Some(""))
                    .with_permissions(Some(2), Some(1)),
            )
            .expect("create");
        assert_eq!(written.value_json, r#"{"level":1}"#);
        assert_eq!(written.read_permission, 2);
        let read = host
            .storage_read("alice", "saves", "slot")
            .expect("read")
            .expect("object");
        assert_eq!(read.version, written.version);
        assert_eq!(
            host.storage_write(
                StorageWriteInput::new("alice", "saves", "slot", r#"{"level": 2}"#)
                    .expecting(Some("")),
            )
            .expect_err("create-only conflicts"),
            "storage conflict"
        );
        let updated = host
            .storage_write(
                StorageWriteInput::new("alice", "saves", "slot", r#"{"level": 2}"#)
                    .expecting(Some(&written.version)),
            )
            .expect("matching update");
        assert_ne!(updated.version, written.version);
        assert_eq!(
            host.storage_delete("alice", "saves", "slot", Some("wrong"))
                .expect_err("wrong version conflicts"),
            "storage conflict"
        );
        host.storage_delete("alice", "saves", "slot", Some(&updated.version))
            .expect("delete");
        assert!(
            host.storage_read("alice", "saves", "slot")
                .expect("read missing")
                .is_none()
        );
        assert_eq!(
            host.storage_write(StorageWriteInput::new("alice", "saves", "slot", "[]"))
                .expect_err("bad JSON is cleanly isolated"),
            "storage validation: value must be a JSON object"
        );
        assert!(
            host.storage_write(StorageWriteInput::new("alice", "saves", "slot", r#"{}"#))
                .is_ok()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_index_query_uses_only_configured_fields() {
        let index = crate::storage::StorageIndexDefinition::new(
            crate::storage::StorageIndexName::new("profiles_by_score").expect("name"),
            Collection::new("profiles").expect("collection"),
            None,
            vec![crate::storage::StorageIndexField::new("score").expect("field")],
        )
        .expect("index");
        let host = host().with_storage_indexes(vec![index]);
        host.storage_write(StorageWriteInput::new(
            "alice",
            "profiles",
            "main",
            r#"{"score":7}"#,
        ))
        .expect("write");

        let matches = host
            .storage_index_query("profiles_by_score", r#"{"score":7}"#, 10)
            .expect("query");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].user_id.as_deref(), Some("alice"));
        assert_eq!(matches[0].collection, "profiles");
        assert_eq!(matches[0].key, "main");
        assert_eq!(
            host.storage_index_query("profiles_by_score", r#"{"unknown":7}"#, 10)
                .expect_err("unknown field rejected"),
            "storage validation: storage index query field `unknown` is not declared by index `profiles_by_score`"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_bridge_works_against_sqlite_without_wedging_the_runtime() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let database = SqliteDatabase::connect(&config)
            .await
            .expect("sqlite database");
        let host = host_with(database.storage_repository());
        let written = host
            .storage_write(StorageWriteInput::new(
                "alice",
                "profile",
                "main",
                r#"{"xp": 7}"#,
            ))
            .expect("sqlite write");
        let read = host
            .storage_read("alice", "profile", "main")
            .expect("sqlite read")
            .expect("object");
        assert_eq!(read.version, written.version);
    }
}
