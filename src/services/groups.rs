//! Groups/clans service (, persisted in ).
//!
//! `GroupsService` is a thin validate-then-delegate layer over a
//! [`GroupsRepository`](crate::repository::GroupsRepository): it keeps the
//! blank-name / blank-creator rejection (and the name/creator trimming) and
//! forwards every operation to the selected persistence backend, so groups and
//! their membership now survive a node restart on the Postgres and SQLite
//! backends (the in-memory backend stays non-durable by design).
//!
//! The group domain model — the three-tier `member` → `admin` → `superadmin`
//! ladder, the last-superadmin invariant, the unique-name and member-cap rules,
//! and list pagination — lives in the repository layer
//! (`src/repository/groups.rs`) as pure, unit-tested helpers shared by all three
//! backends. The value types ([`Group`], [`GroupRole`], [`Membership`], the
//! request/filter/page types) are re-exported here so existing console/HTTP
//! consumers keep their `crate::services::…` paths.

use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::GroupsRepository;
use crate::services::ChatAccessCoordinator;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::Group` / `GroupRole` / … keep resolving for console/HTTP.
pub use crate::repository::groups::{
    AdmissionKind, AdmissionOutcome, CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole,
    GroupsPage, Membership, PendingAdmission, UpdateGroupRequest,
};

/// Groups/clans service backed by a persistence repository.
///
/// Holds an `Arc<dyn GroupsRepository>` from the selected backend. All methods
/// are `async` and delegate after the create-time blank-field validation.
#[derive(Clone)]
pub struct GroupsService {
    repo: Arc<dyn GroupsRepository>,
    chat_access: Arc<ChatAccessCoordinator>,
}

impl GroupsService {
    /// Create a service over a groups repository (from the selected backend).
    #[must_use]
    pub fn new(repo: Arc<dyn GroupsRepository>) -> Self {
        Self {
            repo,
            chat_access: Arc::new(ChatAccessCoordinator::new()),
        }
    }

    /// Use a shared authority coordinator so membership and role changes fence
    /// concurrent secure-chat operations for the same group.
    #[must_use]
    pub fn with_chat_access_coordinator(mut self, chat_access: Arc<ChatAccessCoordinator>) -> Self {
        self.chat_access = chat_access;
        self
    }

    /// Create a group. The creator becomes its founding `superadmin`.
    ///
    /// # Errors
    /// Returns a [`Validation`](crate::error::ErrorCategory::Validation) error
    /// for a blank name or creator id, or a
    /// [`Conflict`](crate::error::ErrorCategory::Conflict) error if the name is
    /// already taken.
    pub async fn create(&self, request: CreateGroupRequest) -> AppResult<Group> {
        let name = request.name.trim();
        if name.is_empty() {
            return Err(AppError::validation("group name must not be blank"));
        }
        let creator = request.creator_user_id.trim();
        if creator.is_empty() {
            return Err(AppError::validation("creator_user_id must not be blank"));
        }
        let normalized = CreateGroupRequest {
            name: name.to_string(),
            creator_user_id: creator.to_string(),
            ..request
        };
        self.repo.create(normalized).await
    }

    /// List groups matching `filter`, id-ordered.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage> {
        self.repo.list(filter).await
    }

    /// Fetch one group by id.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if no such
    /// group exists, or a backend error on failure.
    pub async fn get(&self, id: GroupId) -> AppResult<Group> {
        self.repo
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found("group not found"))
    }

    /// Apply a partial update (description/open/max_size).
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if no such
    /// group exists, or a backend error on failure.
    pub async fn update(&self, id: GroupId, request: UpdateGroupRequest) -> AppResult<Group> {
        self.repo.update(id, request).await
    }

    /// Delete a group outright (members and all).
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if no such
    /// group exists, or a backend error on failure.
    pub async fn delete(&self, id: GroupId) -> AppResult<()> {
        let _fence = self.chat_access.fence().await;
        if self.repo.delete(id).await? {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
            Ok(())
        } else {
            Err(AppError::not_found("group not found"))
        }
    }

    /// Add a user as a `member`.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the group
    /// does not exist, or [`Conflict`](crate::error::ErrorCategory::Conflict) if
    /// the user is already a member or the group is at [`Group::max_size`].
    pub async fn add_member(
        &self,
        id: GroupId,
        user_id: impl Into<String>,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.add_member(id, &user_id.into(), now).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Remove a member from a group.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the group
    /// or member does not exist, or
    /// [`Conflict`](crate::error::ErrorCategory::Conflict) if the member is the
    /// group's last `superadmin`.
    pub async fn kick_member(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.kick_member(id, user_id).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Promote a member one tier: `member` → `admin` → `superadmin`.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the group
    /// or member does not exist, or
    /// [`Conflict`](crate::error::ErrorCategory::Conflict) if the member already
    /// holds the highest role.
    pub async fn promote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.promote(id, user_id).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Demote a member one tier: `superadmin` → `admin` → `member`.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the group
    /// or member does not exist, or
    /// [`Conflict`](crate::error::ErrorCategory::Conflict) if the member already
    /// holds the lowest role, or is the group's last `superadmin`.
    pub async fn demote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.demote(id, user_id).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Join an open group, or create a pending request for a closed group.
    pub async fn join(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.join(id, user_id, now).await;
        if matches!(result, Ok(AdmissionOutcome::Joined(_))) {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Invite a player to a group.
    pub async fn invite(
        &self,
        id: GroupId,
        user_id: &str,
        inviter_user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        self.repo.invite(id, user_id, inviter_user_id, now).await
    }

    /// Approve a player's pending request.
    pub async fn approve_request(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.approve_request(id, user_id, now).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Accept a player's pending invitation.
    pub async fn accept_invitation(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self.repo.accept_invitation(id, user_id, now).await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Cancel a pending request or invitation. Repeating cancellation succeeds.
    pub async fn cancel_admission(&self, id: GroupId, user_id: &str) -> AppResult<()> {
        self.repo.cancel_admission(id, user_id).await
    }

    /// Transfer ownership to an existing member.
    pub async fn transfer_ownership(
        &self,
        id: GroupId,
        from_user_id: &str,
        to_user_id: &str,
    ) -> AppResult<Group> {
        let _fence = self.chat_access.fence().await;
        let result = self
            .repo
            .transfer_ownership(id, from_user_id, to_user_id)
            .await;
        if result.is_ok() {
            self.chat_access
                .advance(&ChatAccessCoordinator::group_key(id))
                .await?;
        }
        result
    }

    /// Create a group on behalf of an authenticated player.
    ///
    /// The caller is always the founding `superadmin`; client payloads never
    /// choose another account as the owner.
    pub async fn create_for_player(
        &self,
        creator_user_id: &str,
        mut request: CreateGroupRequest,
    ) -> AppResult<Group> {
        request.creator_user_id = creator_user_id.to_owned();
        self.create(request).await
    }

    /// Update group metadata as its current superadmin.
    pub async fn update_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        request: UpdateGroupRequest,
    ) -> AppResult<Group> {
        self.require_superadmin(actor_user_id, id).await?;
        self.update(id, request).await
    }

    /// Delete a group as its current superadmin.
    pub async fn delete_as_player(&self, actor_user_id: &str, id: GroupId) -> AppResult<()> {
        self.require_superadmin(actor_user_id, id).await?;
        self.delete(id).await
    }

    /// Add a member as an admin or superadmin. Admins may only manage ordinary
    /// members; superadmins can manage all non-owner membership changes.
    pub async fn add_member_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: impl Into<String>,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        self.require_admin(actor_user_id, id).await?;
        self.add_member(id, user_id, now).await
    }

    /// Remove a member, enforcing the role hierarchy at the player boundary.
    pub async fn kick_member_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: &str,
    ) -> AppResult<Group> {
        let group = self.get(id).await?;
        self.require_manage_target(&group, actor_user_id, user_id)?;
        self.kick_member(id, user_id).await
    }

    /// Leave a group. The repository retains the last-superadmin invariant.
    pub async fn leave_as_player(&self, user_id: &str, id: GroupId) -> AppResult<Group> {
        let group = self.get(id).await?;
        if group.find_member(user_id).is_none() {
            return Err(AppError::permission("caller is not a group member"));
        }
        self.kick_member(id, user_id).await
    }

    /// Promote a member one role. Only a superadmin can grant administrator or
    /// co-owner powers.
    pub async fn promote_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: &str,
    ) -> AppResult<Group> {
        self.require_superadmin(actor_user_id, id).await?;
        self.promote(id, user_id).await
    }

    /// Demote a member one role. Only a superadmin can revoke administrator or
    /// co-owner powers; the repository protects the last owner.
    pub async fn demote_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: &str,
    ) -> AppResult<Group> {
        self.require_superadmin(actor_user_id, id).await?;
        self.demote(id, user_id).await
    }

    /// Join as the authenticated player.
    pub async fn join_as_player(
        &self,
        user_id: &str,
        id: GroupId,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        self.join(id, user_id, now).await
    }

    /// Invite a player as an administrator.
    pub async fn invite_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        self.require_admin(actor_user_id, id).await?;
        self.invite(id, user_id, actor_user_id, now).await
    }

    /// Approve a pending request as an administrator.
    pub async fn approve_request_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        self.require_admin(actor_user_id, id).await?;
        self.approve_request(id, user_id, now).await
    }

    /// Accept the authenticated player's invitation.
    pub async fn accept_invitation_as_player(
        &self,
        user_id: &str,
        id: GroupId,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        self.accept_invitation(id, user_id, now).await
    }

    /// Cancel the authenticated player's own pending request or invitation.
    pub async fn cancel_admission_as_player(&self, user_id: &str, id: GroupId) -> AppResult<()> {
        self.cancel_admission(id, user_id).await
    }

    /// Transfer ownership as the current superadmin.
    pub async fn transfer_ownership_as_player(
        &self,
        actor_user_id: &str,
        id: GroupId,
        to_user_id: &str,
    ) -> AppResult<Group> {
        self.require_superadmin(actor_user_id, id).await?;
        self.transfer_ownership(id, actor_user_id, to_user_id).await
    }

    /// Authorize a player moderation action in a group channel without exposing
    /// group membership details to the chat boundary. Superadmins may moderate
    /// every retained message; admins may moderate members and former members,
    /// but never a current admin or superadmin. Ordinary members cannot
    /// moderate.
    pub async fn authorize_chat_moderation(
        &self,
        actor_user_id: &str,
        id: GroupId,
        author_user_id: &str,
    ) -> AppResult<()> {
        let group = self.get(id).await?;
        let Some(actor) = group.find_member(actor_user_id) else {
            return Err(AppError::permission("CHAT_UNAVAILABLE"));
        };
        match actor.role {
            GroupRole::Superadmin => Ok(()),
            GroupRole::Admin => match group.find_member(author_user_id).map(|member| member.role) {
                Some(GroupRole::Admin | GroupRole::Superadmin) => {
                    Err(AppError::permission("CHAT_UNAVAILABLE"))
                }
                Some(GroupRole::Member) | None => Ok(()),
            },
            GroupRole::Member => Err(AppError::permission("CHAT_UNAVAILABLE")),
        }
    }

    async fn require_superadmin(&self, actor_user_id: &str, id: GroupId) -> AppResult<()> {
        let group = self.get(id).await?;
        match group.find_member(actor_user_id).map(|member| member.role) {
            Some(GroupRole::Superadmin) => Ok(()),
            _ => Err(AppError::permission("group superadmin role required")),
        }
    }

    async fn require_admin(&self, actor_user_id: &str, id: GroupId) -> AppResult<()> {
        let group = self.get(id).await?;
        match group.find_member(actor_user_id).map(|member| member.role) {
            Some(GroupRole::Admin | GroupRole::Superadmin) => Ok(()),
            _ => Err(AppError::permission("group admin role required")),
        }
    }

    fn require_manage_target(
        &self,
        group: &Group,
        actor_user_id: &str,
        target_user_id: &str,
    ) -> AppResult<()> {
        let Some(actor) = group.find_member(actor_user_id) else {
            return Err(AppError::permission("group admin role required"));
        };
        let Some(target) = group.find_member(target_user_id) else {
            return Err(AppError::not_found("group member not found"));
        };
        match actor.role {
            GroupRole::Superadmin => Ok(()),
            GroupRole::Admin if target.role == GroupRole::Member => Ok(()),
            _ => Err(AppError::permission(
                "cannot manage a member with equal or higher role",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use crate::repository::InMemoryGroupsRepository;

    fn service() -> GroupsService {
        GroupsService::new(Arc::new(InMemoryGroupsRepository::new()))
    }

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

    #[tokio::test]
    async fn create_rejects_blank_name_and_creator_before_touching_the_repo() {
        let service = service();
        assert_eq!(
            service
                .create(create_request("   ", "u-1"))
                .await
                .expect_err("blank name")
                .category(),
            ErrorCategory::Validation
        );
        assert_eq!(
            service
                .create(create_request("raiders", "  "))
                .await
                .expect_err("blank creator")
                .category(),
            ErrorCategory::Validation
        );
    }

    #[tokio::test]
    async fn create_trims_name_and_creator() {
        let service = service();
        let group = service
            .create(create_request("  raiders  ", "  u-1  "))
            .await
            .expect("create");
        assert_eq!(group.name, "raiders");
        assert_eq!(
            group.find_member("u-1").expect("creator").role,
            GroupRole::Superadmin
        );
    }

    #[tokio::test]
    async fn get_missing_group_is_not_found() {
        let service = service();
        assert_eq!(
            service.get(9_999).await.expect_err("missing").category(),
            ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn delete_missing_group_is_not_found() {
        let service = service();
        let group = service
            .create(create_request("raiders", "u-1"))
            .await
            .expect("create");
        service.delete(group.id).await.expect("delete");
        assert_eq!(
            service
                .delete(group.id)
                .await
                .expect_err("already gone")
                .category(),
            ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn delegates_membership_lifecycle_to_the_repository() {
        let service = service();
        let group = service
            .create(create_request("raiders", "u-1"))
            .await
            .expect("create");
        service
            .add_member(group.id, "u-2", now(2))
            .await
            .expect("add");
        let promoted = service.promote(group.id, "u-2").await.expect("promote");
        assert_eq!(
            promoted.find_member("u-2").expect("member").role,
            GroupRole::Admin
        );
        let kicked = service.kick_member(group.id, "u-2").await.expect("kick");
        assert_eq!(kicked.member_count(), 1);
    }

    #[tokio::test]
    async fn player_admission_and_ownership_boundaries_are_role_safe() {
        let service = service();
        let group = service
            .create(create_request("raiders", "owner"))
            .await
            .expect("create");

        assert_eq!(
            service
                .invite_as_player("outsider", group.id, "player", now(2))
                .await
                .expect_err("outsider cannot invite")
                .category(),
            ErrorCategory::Permission
        );
        service
            .invite_as_player("owner", group.id, "player", now(3))
            .await
            .expect("invite");
        service
            .accept_invitation_as_player("player", group.id, now(4))
            .await
            .expect("accept invitation");

        assert_eq!(
            service
                .transfer_ownership_as_player("player", group.id, "owner")
                .await
                .expect_err("ordinary member cannot transfer")
                .category(),
            ErrorCategory::Permission
        );
        let transferred = service
            .transfer_ownership_as_player("owner", group.id, "player")
            .await
            .expect("transfer ownership");
        assert_eq!(
            transferred.find_member("player").expect("new owner").role,
            GroupRole::Superadmin
        );
    }

    #[tokio::test]
    async fn group_chat_moderation_respects_the_role_hierarchy() {
        let service = service();
        let group = service
            .create(create_request("raiders", "owner"))
            .await
            .expect("create");
        for user in ["moderator", "member", "co-admin"] {
            service
                .add_member(group.id, user, now(2))
                .await
                .expect("add member");
        }
        service
            .promote(group.id, "moderator")
            .await
            .expect("promote moderator");
        service
            .promote(group.id, "co-admin")
            .await
            .expect("promote co-admin");

        service
            .authorize_chat_moderation("moderator", group.id, "member")
            .await
            .expect("admin may moderate member");
        assert_eq!(
            service
                .authorize_chat_moderation("moderator", group.id, "co-admin")
                .await
                .expect_err("admin cannot moderate a peer")
                .category(),
            ErrorCategory::Permission
        );
        assert_eq!(
            service
                .authorize_chat_moderation("member", group.id, "moderator")
                .await
                .expect_err("member cannot moderate")
                .category(),
            ErrorCategory::Permission
        );
        service
            .authorize_chat_moderation("owner", group.id, "co-admin")
            .await
            .expect("superadmin may moderate an admin");
    }
}
