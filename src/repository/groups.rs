//! Groups/clans repository contract.
//!
//! Persists the player-group domain (Nakama's
//! superadmin/admin/member, open/closed model) behind the same repository seam as
//! identity/session/storage/friends, so groups and their membership survive a
//! node restart. A group has a unique name, a description, an open/closed join
//! policy, and an optional member cap (`max_size == 0` means unlimited).
//! Membership is a three-tier role ladder — `member` → `admin` → `superadmin` —
//! with the invariant that **a group with at least one member always keeps at
//! least one `superadmin`**: the last superadmin can neither be demoted nor
//! kicked.
//!
//! Following the friends template, the read-modify-write decisions
//! (role transitions, the last-superadmin guard, the add-member uniqueness/cap
//! rules, and list pagination) live in exactly one place — the pure
//! [`plan_promote`] / [`plan_demote`] / [`ensure_can_kick`] /
//! [`ensure_can_add_member`] / [`paginate`] functions, unit-tested directly
//! here. Every backend ([`InMemoryGroupsRepository`], the Postgres
//! `PgGroupsRepository`, the SQLite `SqliteGroupsRepository`) only does
//! (lock/transaction) read → apply the pure decision → write, so the three
//! implementations cannot drift on the business rules.
//!
//! The service layer ([`crate::services::groups`]) is a thin validate-then-delegate
//! shell that keeps the blank-name / blank-creator rejection (a service-level
//! validation the repository never sees) and forwards everything else. The value
//! types are re-exported from the service so existing console/HTTP consumers keep
//! their `crate::services::…` paths.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// A group's server-assigned identifier.
pub type GroupId = u64;

/// A member's role within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupRole {
    /// Ordinary member: may participate but not administer the group.
    Member,
    /// May administer members (add/kick/promote/demote members, not other
    /// admins or superadmins) — enforcement of finer per-role limits is left
    /// to a future task; today any admin-console operator already gates
    /// mutations at the HTTP boundary.
    Admin,
    /// Full ownership of the group. Every group must keep at least one.
    Superadmin,
}

impl GroupRole {
    /// Stable lowercase token used in responses, audit entries, and the durable
    /// `role` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Admin => "admin",
            Self::Superadmin => "superadmin",
        }
    }

    /// Parse a stored `role` token back into a [`GroupRole`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the three known
    /// roles — a corrupt/foreign row rather than a client-visible condition.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "member" => Ok(Self::Member),
            "admin" => Ok(Self::Admin),
            "superadmin" => Ok(Self::Superadmin),
            other => Err(AppError::internal(format!(
                "unknown group role token `{other}`"
            ))),
        }
    }

    /// The next role up the ladder, or `None` if already at the top.
    #[must_use]
    pub const fn promoted(self) -> Option<Self> {
        match self {
            Self::Member => Some(Self::Admin),
            Self::Admin => Some(Self::Superadmin),
            Self::Superadmin => None,
        }
    }

    /// The next role down the ladder, or `None` if already at the bottom.
    #[must_use]
    pub const fn demoted(self) -> Option<Self> {
        match self {
            Self::Superadmin => Some(Self::Admin),
            Self::Admin => Some(Self::Member),
            Self::Member => None,
        }
    }
}

/// One group member: who, what role, and since when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    /// The member's user id.
    pub user_id: String,
    /// The member's current role.
    pub role: GroupRole,
    /// When this user joined (or was added to) the group.
    pub joined_at: TimestampMillis,
}

/// A pending group admission, owned by the group repository rather than the
/// notification system.  Exactly one pending admission may exist per
/// `(group_id, user_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAdmission {
    /// The account that may become a member.
    pub user_id: String,
    /// How the pending admission was created.
    pub kind: AdmissionKind,
    /// The administrator who sent an invitation, if applicable.
    pub inviter_user_id: Option<String>,
    /// Creation timestamp.
    pub created_at: TimestampMillis,
}

/// The two non-member states in the admission state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionKind {
    /// A player asked to join a closed group.
    Request,
    /// An administrator invited a player.
    Invitation,
}

/// Result of an idempotent admission command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// The caller became an ordinary member.
    Joined(Group),
    /// A closed-group request was recorded.
    RequestCreated,
    /// An administrator's invitation was recorded.
    InvitationCreated,
    /// The player was already a member; no data changed.
    AlreadyMember(Group),
}

/// A group and its member roll.
///
/// Members are kept in join order (a `Vec`, not a `HashMap`) so listings are
/// deterministic; groups are expected to be small enough that linear lookups
/// are not a concern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Server-assigned id.
    pub id: GroupId,
    /// The unique group name.
    pub name: String,
    /// Free-form description.
    pub description: String,
    /// Whether users may join without an admin adding them.
    ///
    /// Open/closed is advisory metadata today: there is no join-request flow yet
    /// (out of scope for /0261), so every membership change is driven by
    /// an admin-console `add_member` call regardless of `open`. A future
    /// join-request task can read this flag.
    pub open: bool,
    /// Maximum member count (`0` = unlimited).
    pub max_size: u32,
    /// When the group was created.
    pub created_at: TimestampMillis,
    members: Vec<Membership>,
}

impl Group {
    /// Assemble a group from its persisted parts. Used by the durable backends,
    /// which read the group row and its membership rows separately.
    pub(crate) fn from_parts(
        id: GroupId,
        name: String,
        description: String,
        open: bool,
        max_size: u32,
        created_at: TimestampMillis,
        members: Vec<Membership>,
    ) -> Self {
        Self {
            id,
            name,
            description,
            open,
            max_size,
            created_at,
            members,
        }
    }

    /// The current member roll, in join order.
    #[must_use]
    pub fn members(&self) -> &[Membership] {
        &self.members
    }

    /// Current member count.
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// How many members currently hold [`GroupRole::Superadmin`].
    #[must_use]
    pub fn superadmin_count(&self) -> usize {
        self.members
            .iter()
            .filter(|m| m.role == GroupRole::Superadmin)
            .count()
    }

    /// Find a member by user id.
    #[must_use]
    pub fn find_member(&self, user_id: &str) -> Option<&Membership> {
        self.members.iter().find(|m| m.user_id == user_id)
    }

    fn find_member_index(&self, user_id: &str) -> Option<usize> {
        self.members.iter().position(|m| m.user_id == user_id)
    }

    /// Append a member to the in-memory roll. Used by the durable backends to
    /// mirror a `group_memberships` insert without reloading the whole group.
    pub(crate) fn push_member(&mut self, member: Membership) {
        self.members.push(member);
    }

    /// Drop a member from the in-memory roll (mirrors a membership delete).
    pub(crate) fn remove_member(&mut self, user_id: &str) {
        self.members.retain(|m| m.user_id != user_id);
    }

    /// Set a member's role in the in-memory roll (mirrors a role update).
    pub(crate) fn set_member_role(&mut self, user_id: &str, role: GroupRole) {
        if let Some(member) = self.members.iter_mut().find(|m| m.user_id == user_id) {
            member.role = role;
        }
    }
}

/// Input for [`GroupsRepository::create`].
#[derive(Debug, Clone)]
pub struct CreateGroupRequest {
    /// The group's name. The service guarantees it is trimmed and non-blank.
    pub name: String,
    /// Free-form description.
    pub description: String,
    /// Whether the group is open (advisory; see [`Group::open`]).
    pub open: bool,
    /// Maximum member count (`0` = unlimited).
    pub max_size: u32,
    /// The user who becomes the founding superadmin. Trimmed, non-blank.
    pub creator_user_id: String,
    /// Creation/join timestamp for the founding member.
    pub now: TimestampMillis,
}

/// Input for [`GroupsRepository::update`]. Every field is an optional patch:
/// `None` leaves the current value unchanged.
#[derive(Debug, Clone, Default)]
pub struct UpdateGroupRequest {
    /// New description, if changing.
    pub description: Option<String>,
    /// New open/closed flag, if changing.
    pub open: Option<bool>,
    /// New member cap, if changing.
    pub max_size: Option<u32>,
}

/// Filter and page bounds for [`GroupsRepository::list`].
#[derive(Debug, Clone, Default)]
pub struct GroupFilter {
    /// Case-sensitive substring match over the group name.
    pub name_contains: Option<String>,
    /// Maximum groups returned. `0` means unbounded (bounded only by the
    /// total group count).
    pub limit: usize,
    /// Number of matching groups to skip before collecting the page.
    pub offset: usize,
}

/// One page of [`GroupsRepository::list`] results.
#[derive(Debug, Clone)]
pub struct GroupsPage {
    /// The groups in this page, id-ordered.
    pub items: Vec<Group>,
    /// Total groups matching the filter, before `offset`/`limit`.
    pub total: usize,
}

// --- Pure decision helpers (the unit-tested state machine) -------------------

/// Decide the new role for a promote given the member's current role.
///
/// # Errors
/// Returns a [`Conflict`](crate::error::ErrorCategory::Conflict) if the member
/// already holds the highest role.
pub fn plan_promote(current: GroupRole) -> AppResult<GroupRole> {
    current
        .promoted()
        .ok_or_else(|| AppError::conflict("member already holds the highest role"))
}

/// Decide the new role for a demote given the member's current role and how many
/// superadmins the group currently has.
///
/// # Errors
/// Returns a [`Conflict`](crate::error::ErrorCategory::Conflict) if the member is
/// the group's last superadmin, or already holds the lowest role.
pub fn plan_demote(current: GroupRole, superadmin_count: usize) -> AppResult<GroupRole> {
    if current == GroupRole::Superadmin && superadmin_count == 1 {
        return Err(AppError::conflict(
            "cannot demote the group's last superadmin",
        ));
    }
    current
        .demoted()
        .ok_or_else(|| AppError::conflict("member already holds the lowest role"))
}

/// Check whether a member may be kicked, given their role and the group's
/// superadmin count.
///
/// # Errors
/// Returns a [`Conflict`](crate::error::ErrorCategory::Conflict) if the member is
/// the group's last superadmin.
pub fn ensure_can_kick(current: GroupRole, superadmin_count: usize) -> AppResult<()> {
    if current == GroupRole::Superadmin && superadmin_count == 1 {
        return Err(AppError::conflict(
            "cannot kick the group's last superadmin",
        ));
    }
    Ok(())
}

/// Check whether a user may be added to a group as a `member`.
///
/// # Errors
/// Returns a [`Conflict`](crate::error::ErrorCategory::Conflict) if the user is
/// already a member, or the group is already at its [`Group::max_size`] cap.
pub fn ensure_can_add_member(is_member: bool, member_count: usize, max_size: u32) -> AppResult<()> {
    if is_member {
        return Err(AppError::conflict("user is already a member of this group"));
    }
    if max_size != 0 && member_count >= max_size as usize {
        return Err(AppError::conflict("group has reached its member limit"));
    }
    Ok(())
}

/// Filter, count, and page an id-ordered slice of groups.
///
/// The single place the list semantics live, so all three backends filter and
/// paginate identically. `groups` must already be ascending-id-ordered; the
/// caller supplies the ordering (the durable backends order by `id` in SQL, the
/// in-memory backend sorts). `total` is the match count before `offset`/`limit`.
#[must_use]
pub fn paginate(groups: Vec<Group>, filter: &GroupFilter) -> GroupsPage {
    let matched: Vec<Group> = groups
        .into_iter()
        .filter(|group| {
            filter
                .name_contains
                .as_deref()
                .is_none_or(|needle| group.name.contains(needle))
        })
        .collect();
    let total = matched.len();
    let limit = if filter.limit == 0 {
        total
    } else {
        filter.limit
    };
    let items = matched
        .into_iter()
        .skip(filter.offset)
        .take(limit)
        .collect();
    GroupsPage { items, total }
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for groups and their membership.
///
/// The service layer trims and rejects blank names/creators before delegating,
/// so implementations may assume `name`/`creator_user_id` are already non-blank.
/// Name uniqueness, the role ladder, and the last-superadmin invariant are
/// enforced here (in the pure helpers above) so every backend agrees.
#[async_trait]
pub trait GroupsRepository: Send + Sync {
    /// Create a group whose creator becomes its founding `superadmin`.
    ///
    /// # Errors
    /// - `Conflict` if the name is already taken.
    /// - A backend error on failure.
    async fn create(&self, request: CreateGroupRequest) -> AppResult<Group>;

    /// List groups matching `filter`, id-ordered.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage>;

    /// Fetch one group by id, or `None` if it does not exist.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get(&self, id: GroupId) -> AppResult<Option<Group>>;

    /// Apply a partial update (description/open/max_size).
    ///
    /// # Errors
    /// - `NotFound` if no such group exists.
    /// - A backend error on failure.
    async fn update(&self, id: GroupId, request: UpdateGroupRequest) -> AppResult<Group>;

    /// Delete a group outright (members and all). Returns whether a group was
    /// removed.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn delete(&self, id: GroupId) -> AppResult<bool>;

    /// Add a user as a `member`.
    ///
    /// # Errors
    /// - `NotFound` if the group does not exist.
    /// - `Conflict` if the user is already a member or the group is at
    ///   [`Group::max_size`].
    /// - A backend error on failure.
    async fn add_member(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group>;

    /// Remove a member from a group.
    ///
    /// # Errors
    /// - `NotFound` if the group or member does not exist.
    /// - `Conflict` if the member is the group's last `superadmin`.
    /// - A backend error on failure.
    async fn kick_member(&self, id: GroupId, user_id: &str) -> AppResult<Group>;

    /// Promote a member one tier: `member` → `admin` → `superadmin`.
    ///
    /// # Errors
    /// - `NotFound` if the group or member does not exist.
    /// - `Conflict` if the member already holds the highest role.
    /// - A backend error on failure.
    async fn promote(&self, id: GroupId, user_id: &str) -> AppResult<Group>;

    /// Demote a member one tier: `superadmin` → `admin` → `member`.
    ///
    /// # Errors
    /// - `NotFound` if the group or member does not exist.
    /// - `Conflict` if the member already holds the lowest role, or is the
    ///   group's last `superadmin`.
    /// - A backend error on failure.
    async fn demote(&self, id: GroupId, user_id: &str) -> AppResult<Group>;

    /// Join an open group, or record a request for a closed group.
    async fn join(
        &self,
        _id: GroupId,
        _user_id: &str,
        _now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        Err(AppError::internal(
            "groups admission is not implemented by this repository",
        ))
    }

    /// Record an administrator invitation. Repeating an existing invitation is
    /// idempotent; a request is upgraded to an invitation.
    async fn invite(
        &self,
        _id: GroupId,
        _user_id: &str,
        _inviter_user_id: &str,
        _now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        Err(AppError::internal(
            "groups admission is not implemented by this repository",
        ))
    }

    /// Turn a pending request into membership.
    async fn approve_request(
        &self,
        _id: GroupId,
        _user_id: &str,
        _now: TimestampMillis,
    ) -> AppResult<Group> {
        Err(AppError::internal(
            "groups admission is not implemented by this repository",
        ))
    }

    /// Turn the caller's pending invitation into membership.
    async fn accept_invitation(
        &self,
        _id: GroupId,
        _user_id: &str,
        _now: TimestampMillis,
    ) -> AppResult<Group> {
        Err(AppError::internal(
            "groups admission is not implemented by this repository",
        ))
    }

    /// Remove a pending request or invitation. Missing pending state is
    /// idempotent success.
    async fn cancel_admission(&self, _id: GroupId, _user_id: &str) -> AppResult<()> {
        Err(AppError::internal(
            "groups admission is not implemented by this repository",
        ))
    }

    /// Move the sole ownership role to an existing member.
    async fn transfer_ownership(
        &self,
        _id: GroupId,
        _from_user_id: &str,
        _to_user_id: &str,
    ) -> AppResult<Group> {
        Err(AppError::internal(
            "groups ownership transfer is not implemented by this repository",
        ))
    }
}

// --- In-memory reference implementation --------------------------------------

/// The group store: `id -> Group` (each group carries its own member roll). A
/// named alias keeps the `Mutex`/guard types readable (mirrors `EdgeStore` in
/// `friends.rs`).
type GroupStore = HashMap<GroupId, Group>;

/// A contract-faithful, in-memory [`GroupsRepository`] (the reference impl).
///
/// Single-process and not durable, but it enforces the full name-uniqueness /
/// role-ladder / last-superadmin contract through the shared pure helpers, so the
/// contract tests in `tests/groups_repository_contract.rs` can be reused against
/// the durable backends. Group ids are a sequential counter, unique within a
/// process (the durable backends use a database identity column).
#[derive(Debug, Default)]
pub struct InMemoryGroupsRepository {
    inner: Mutex<GroupsState>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct GroupsState {
    next_id: GroupId,
    groups: GroupStore,
    admissions: HashMap<(GroupId, String), PendingAdmission>,
}

impl InMemoryGroupsRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, GroupsState>> {
        self.inner
            .lock()
            .map_err(|_| AppError::internal("groups repository mutex poisoned"))
    }

    /// Capture all groups, memberships, admissions, and the id sequence for an
    /// enclosing in-memory transaction.
    pub(crate) fn snapshot_for_rollback(&self) -> AppResult<GroupsState> {
        Ok(self.guard()?.clone())
    }

    /// Restore a transaction snapshot without applying domain transitions.
    pub(crate) fn restore_for_rollback(&self, snapshot: GroupsState) {
        if let Ok(mut state) = self.inner.lock() {
            *state = snapshot;
        }
    }
}

#[async_trait]
impl GroupsRepository for InMemoryGroupsRepository {
    async fn create(&self, request: CreateGroupRequest) -> AppResult<Group> {
        let mut state = self.guard()?;
        if state.groups.values().any(|g| g.name == request.name) {
            return Err(AppError::conflict(format!(
                "a group named '{}' already exists",
                request.name
            )));
        }
        let id = state.next_id.wrapping_add(1);
        state.next_id = id;
        let group = Group {
            id,
            name: request.name,
            description: request.description,
            open: request.open,
            max_size: request.max_size,
            created_at: request.now,
            members: vec![Membership {
                user_id: request.creator_user_id,
                role: GroupRole::Superadmin,
                joined_at: request.now,
            }],
        };
        state.groups.insert(id, group.clone());
        Ok(group)
    }

    async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage> {
        let state = self.guard()?;
        let mut groups: Vec<Group> = state.groups.values().cloned().collect();
        groups.sort_by_key(|group| group.id);
        Ok(paginate(groups, filter))
    }

    async fn get(&self, id: GroupId) -> AppResult<Option<Group>> {
        Ok(self.guard()?.groups.get(&id).cloned())
    }

    async fn update(&self, id: GroupId, request: UpdateGroupRequest) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        if let Some(description) = request.description {
            group.description = description;
        }
        if let Some(open) = request.open {
            group.open = open;
        }
        if let Some(max_size) = request.max_size {
            group.max_size = max_size;
        }
        Ok(group.clone())
    }

    async fn delete(&self, id: GroupId) -> AppResult<bool> {
        let mut state = self.guard()?;
        let removed = state.groups.remove(&id).is_some();
        if removed {
            state.admissions.retain(|(group_id, _), _| *group_id != id);
        }
        Ok(removed)
    }

    async fn add_member(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        ensure_can_add_member(
            group.find_member(user_id).is_some(),
            group.member_count(),
            group.max_size,
        )?;
        group.members.push(Membership {
            user_id: user_id.to_string(),
            role: GroupRole::Member,
            joined_at: now,
        });
        Ok(group.clone())
    }

    async fn kick_member(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        let index = group
            .find_member_index(user_id)
            .ok_or_else(|| AppError::not_found("member not found"))?;
        ensure_can_kick(group.members[index].role, group.superadmin_count())?;
        group.members.remove(index);
        Ok(group.clone())
    }

    async fn promote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        let index = group
            .find_member_index(user_id)
            .ok_or_else(|| AppError::not_found("member not found"))?;
        let next = plan_promote(group.members[index].role)?;
        group.members[index].role = next;
        Ok(group.clone())
    }

    async fn demote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        let index = group
            .find_member_index(user_id)
            .ok_or_else(|| AppError::not_found("member not found"))?;
        let next = plan_demote(group.members[index].role, group.superadmin_count())?;
        group.members[index].role = next;
        Ok(group.clone())
    }

    async fn join(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        let mut state = self.guard()?;
        let key = (id, user_id.to_owned());
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        if group.find_member(user_id).is_some() {
            return Ok(AdmissionOutcome::AlreadyMember(group.clone()));
        }
        if group.open {
            ensure_can_add_member(false, group.member_count(), group.max_size)?;
            group.push_member(Membership {
                user_id: user_id.to_owned(),
                role: GroupRole::Member,
                joined_at: now,
            });
            let joined = group.clone();
            let _ = group;
            state.admissions.remove(&key);
            return Ok(AdmissionOutcome::Joined(joined));
        }
        match state.admissions.get(&key) {
            Some(PendingAdmission {
                kind: AdmissionKind::Invitation,
                ..
            }) => Ok(AdmissionOutcome::InvitationCreated),
            Some(_) => Ok(AdmissionOutcome::RequestCreated),
            None => {
                state.admissions.insert(
                    key,
                    PendingAdmission {
                        user_id: user_id.to_owned(),
                        kind: AdmissionKind::Request,
                        inviter_user_id: None,
                        created_at: now,
                    },
                );
                Ok(AdmissionOutcome::RequestCreated)
            }
        }
    }

    async fn invite(
        &self,
        id: GroupId,
        user_id: &str,
        inviter_user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        if group.find_member(user_id).is_some() {
            return Ok(AdmissionOutcome::AlreadyMember(group.clone()));
        }
        let key = (id, user_id.to_owned());
        state.admissions.insert(
            key,
            PendingAdmission {
                user_id: user_id.to_owned(),
                kind: AdmissionKind::Invitation,
                inviter_user_id: Some(inviter_user_id.to_owned()),
                created_at: now,
            },
        );
        Ok(AdmissionOutcome::InvitationCreated)
    }

    async fn approve_request(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let key = (id, user_id.to_owned());
        let mut state = self.guard()?;
        match state.admissions.get(&key).map(|admission| admission.kind) {
            Some(AdmissionKind::Request) => {}
            _ => return Err(AppError::not_found("join request not found")),
        }
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        ensure_can_add_member(
            group.find_member(user_id).is_some(),
            group.member_count(),
            group.max_size,
        )?;
        group.push_member(Membership {
            user_id: user_id.to_owned(),
            role: GroupRole::Member,
            joined_at: now,
        });
        let admitted = group.clone();
        let _ = group;
        state.admissions.remove(&key);
        Ok(admitted)
    }

    async fn accept_invitation(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let key = (id, user_id.to_owned());
        let mut state = self.guard()?;
        match state.admissions.get(&key).map(|admission| admission.kind) {
            Some(AdmissionKind::Invitation) => {}
            _ => return Err(AppError::not_found("group invitation not found")),
        }
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        ensure_can_add_member(
            group.find_member(user_id).is_some(),
            group.member_count(),
            group.max_size,
        )?;
        group.push_member(Membership {
            user_id: user_id.to_owned(),
            role: GroupRole::Member,
            joined_at: now,
        });
        let admitted = group.clone();
        let _ = group;
        state.admissions.remove(&key);
        Ok(admitted)
    }

    async fn cancel_admission(&self, id: GroupId, user_id: &str) -> AppResult<()> {
        let mut state = self.guard()?;
        if !state.groups.contains_key(&id) {
            return Err(AppError::not_found("group not found"));
        }
        state.admissions.remove(&(id, user_id.to_owned()));
        Ok(())
    }

    async fn transfer_ownership(
        &self,
        id: GroupId,
        from_user_id: &str,
        to_user_id: &str,
    ) -> AppResult<Group> {
        let mut state = self.guard()?;
        let group = state
            .groups
            .get_mut(&id)
            .ok_or_else(|| AppError::not_found("group not found"))?;
        if group.find_member(from_user_id).map(|member| member.role) != Some(GroupRole::Superadmin)
        {
            return Err(AppError::permission("current superadmin role required"));
        }
        if from_user_id == to_user_id {
            return Ok(group.clone());
        }
        if group.find_member(to_user_id).is_none() {
            return Err(AppError::not_found("target member not found"));
        }
        group.set_member_role(from_user_id, GroupRole::Admin);
        group.set_member_role(to_user_id, GroupRole::Superadmin);
        Ok(group.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;

    fn now(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    fn create_request(name: &str, creator: &str) -> CreateGroupRequest {
        CreateGroupRequest {
            name: name.to_string(),
            description: "a test group".to_string(),
            open: true,
            max_size: 0,
            creator_user_id: creator.to_string(),
            now: now(1),
        }
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn role_ladder_helpers_are_consistent() {
        assert_eq!(GroupRole::Member.as_str(), "member");
        assert_eq!(GroupRole::Admin.as_str(), "admin");
        assert_eq!(GroupRole::Superadmin.as_str(), "superadmin");
        assert_eq!(GroupRole::Member.promoted(), Some(GroupRole::Admin));
        assert_eq!(GroupRole::Superadmin.promoted(), None);
        assert_eq!(GroupRole::Superadmin.demoted(), Some(GroupRole::Admin));
        assert_eq!(GroupRole::Member.demoted(), None);
    }

    #[test]
    fn role_tokens_round_trip() {
        for role in [GroupRole::Member, GroupRole::Admin, GroupRole::Superadmin] {
            assert_eq!(GroupRole::from_token(role.as_str()).expect("parse"), role);
        }
        assert!(GroupRole::from_token("emperor").is_err());
    }

    #[test]
    fn plan_promote_walks_up_and_stops_at_the_top() {
        assert_eq!(
            plan_promote(GroupRole::Member).expect("up"),
            GroupRole::Admin
        );
        assert_eq!(
            plan_promote(GroupRole::Admin).expect("up"),
            GroupRole::Superadmin
        );
        assert_eq!(
            plan_promote(GroupRole::Superadmin)
                .expect_err("already top")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[test]
    fn plan_demote_walks_down_and_guards_last_superadmin() {
        // Two superadmins: demoting one is fine.
        assert_eq!(
            plan_demote(GroupRole::Superadmin, 2).expect("down"),
            GroupRole::Admin
        );
        // The last superadmin cannot be demoted.
        assert_eq!(
            plan_demote(GroupRole::Superadmin, 1)
                .expect_err("last superadmin")
                .category(),
            ErrorCategory::Conflict
        );
        // A member cannot be demoted further.
        assert_eq!(
            plan_demote(GroupRole::Member, 1)
                .expect_err("already bottom")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[test]
    fn ensure_can_kick_guards_last_superadmin_only() {
        assert!(ensure_can_kick(GroupRole::Member, 1).is_ok());
        assert!(ensure_can_kick(GroupRole::Superadmin, 2).is_ok());
        assert_eq!(
            ensure_can_kick(GroupRole::Superadmin, 1)
                .expect_err("last superadmin")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[test]
    fn ensure_can_add_member_enforces_uniqueness_and_cap() {
        assert!(ensure_can_add_member(false, 1, 0).is_ok());
        assert!(ensure_can_add_member(false, 99, 0).is_ok(), "0 = unlimited");
        assert_eq!(
            ensure_can_add_member(true, 1, 0)
                .expect_err("duplicate")
                .category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            ensure_can_add_member(false, 2, 2)
                .expect_err("full")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[test]
    fn paginate_filters_counts_and_pages() {
        let groups = vec![
            Group::from_parts(1, "alpha".into(), String::new(), true, 0, now(1), vec![]),
            Group::from_parts(2, "beta".into(), String::new(), true, 0, now(1), vec![]),
            Group::from_parts(3, "alpine".into(), String::new(), true, 0, now(1), vec![]),
        ];
        let all = paginate(groups.clone(), &GroupFilter::default());
        assert_eq!(all.total, 3);
        assert_eq!(all.items.len(), 3);

        let filtered = paginate(
            groups.clone(),
            &GroupFilter {
                name_contains: Some("alp".into()),
                ..GroupFilter::default()
            },
        );
        assert_eq!(filtered.total, 2, "alpha + alpine");
        let paged = paginate(
            groups,
            &GroupFilter {
                limit: 1,
                offset: 1,
                ..GroupFilter::default()
            },
        );
        assert_eq!(paged.total, 3, "total ignores paging");
        assert_eq!(paged.items.len(), 1);
        assert_eq!(paged.items[0].id, 2);
    }

    // --- InMemoryGroupsRepository (reference impl) --------------------------

    #[tokio::test]
    async fn create_makes_the_creator_a_superadmin() {
        let repo = InMemoryGroupsRepository::new();
        let group = repo
            .create(create_request("raiders", "u-1"))
            .await
            .expect("create");
        assert_eq!(group.member_count(), 1);
        assert_eq!(
            group.find_member("u-1").expect("creator").role,
            GroupRole::Superadmin
        );
    }

    #[tokio::test]
    async fn create_enforces_unique_names() {
        let repo = InMemoryGroupsRepository::new();
        repo.create(create_request("raiders", "u-1"))
            .await
            .expect("first");
        assert_eq!(
            repo.create(create_request("raiders", "u-2"))
                .await
                .expect_err("duplicate")
                .category(),
            ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn last_superadmin_cannot_be_demoted_or_kicked() {
        let repo = InMemoryGroupsRepository::new();
        let group = repo
            .create(create_request("raiders", "u-1"))
            .await
            .expect("create");
        assert_eq!(
            repo.demote(group.id, "u-1")
                .await
                .expect_err("demote")
                .category(),
            ErrorCategory::Conflict
        );
        assert_eq!(
            repo.kick_member(group.id, "u-1")
                .await
                .expect_err("kick")
                .category(),
            ErrorCategory::Conflict
        );
    }
}
