//! Server-side chat authorization primitives.
//!
//!  moves player chat away from client-selected channel strings. This
//! module owns the policy checks shared by the gateway and later live-delivery
//! work: direct messages require a current mutual friendship with no hard block;
//! group messages require current membership; room membership is supplied by the
//! gateway because it is local, ephemeral state.

use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::{ChannelType, FriendState, GroupId};
use crate::services::{ChatAccessCoordinator, FriendsService, GroupsService};
use tokio::sync::OwnedMutexGuard;

/// The non-disclosing error returned for every unavailable chat target.
pub const CHAT_UNAVAILABLE: &str = "CHAT_UNAVAILABLE";

/// A client-selected chat target. It deliberately contains no channel id, type,
/// or sender field: the server derives the canonical descriptor from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatTarget {
    /// A direct conversation with one other authenticated account.
    Direct { other_user_id: String },
    /// The durable channel attached to a group.
    Group { group_id: GroupId },
    /// The caller's current authoritative room. The room id is never accepted
    /// from the client payload.
    CurrentRoom { room_id: u64 },
}

/// A target that passed the current authorization policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedChatChannel {
    /// Stable, canonical key used by the persistence layer to obtain an opaque
    /// public channel id. It is never accepted from a new target request.
    pub canonical_key: String,
    /// Immutable channel kind.
    pub channel_type: ChannelType,
    /// The relevant authority subject used by future epoch fencing.
    pub access_key: String,
}

/// An authorization result that keeps the shared authority fence held.
///
/// The gateway holds this through descriptor resolution and durable send/history
/// execution, so a local social/group mutation cannot invalidate the check in
/// the middle of that operation.
pub struct AuthorizedChatLease {
    /// The canonical authorized channel.
    pub channel: AuthorizedChatChannel,
    /// Authority epoch observed while the fence was held.
    pub access_epoch: u64,
    _fence: OwnedMutexGuard<()>,
}

/// Shared authority policy for the secure player-chat boundary.
#[derive(Clone)]
pub struct ChatChannelAuthorizer {
    friends: Arc<FriendsService>,
    groups: Arc<GroupsService>,
    access: Arc<ChatAccessCoordinator>,
}

impl ChatChannelAuthorizer {
    /// Build an authorizer over the current social and group services.
    #[must_use]
    pub fn new(friends: Arc<FriendsService>, groups: Arc<GroupsService>) -> Self {
        Self::with_access_coordinator(friends, groups, Arc::new(ChatAccessCoordinator::new()))
    }

    /// Build an authorizer sharing a coordinator with social/group mutations.
    #[must_use]
    pub fn with_access_coordinator(
        friends: Arc<FriendsService>,
        groups: Arc<GroupsService>,
        access: Arc<ChatAccessCoordinator>,
    ) -> Self {
        Self {
            friends,
            groups,
            access,
        }
    }

    /// Validate one target for `actor_user_id` and return its canonical identity.
    ///
    /// Every failed validation deliberately becomes [`CHAT_UNAVAILABLE`]: neither
    /// a missing group nor a block relationship may be enumerated by a caller.
    pub async fn authorize(
        &self,
        actor_user_id: &str,
        target: ChatTarget,
    ) -> AppResult<AuthorizedChatChannel> {
        if actor_user_id.is_empty() {
            return Err(unavailable());
        }
        match target {
            ChatTarget::Direct { other_user_id } => {
                self.authorize_direct(actor_user_id, &other_user_id).await
            }
            ChatTarget::Group { group_id } => self.authorize_group(actor_user_id, group_id).await,
            ChatTarget::CurrentRoom { room_id } => {
                if room_id == 0 {
                    return Err(unavailable());
                }
                Ok(AuthorizedChatChannel {
                    canonical_key: format!("room:{room_id}"),
                    channel_type: ChannelType::Room,
                    access_key: format!("room:{room_id}"),
                })
            }
        }
    }

    /// Authorize a target while holding the shared mutation fence.
    pub async fn authorize_fenced(
        &self,
        actor_user_id: &str,
        target: ChatTarget,
    ) -> AppResult<AuthorizedChatLease> {
        let fence = self.access.fence().await;
        let channel = self.authorize(actor_user_id, target).await?;
        let access_epoch = self.access.epoch(&channel.access_key).await?;
        Ok(AuthorizedChatLease {
            channel,
            access_epoch,
            _fence: fence,
        })
    }

    async fn authorize_direct(
        &self,
        actor_user_id: &str,
        other_user_id: &str,
    ) -> AppResult<AuthorizedChatChannel> {
        if other_user_id.is_empty() || actor_user_id == other_user_id {
            return Err(unavailable());
        }
        let actor_rows = self
            .friends
            .list(actor_user_id)
            .await
            .map_err(|_| unavailable())?;
        let other_rows = self
            .friends
            .list(other_user_id)
            .await
            .map_err(|_| unavailable())?;
        let actor_state = actor_rows
            .iter()
            .find(|row| row.user_id == other_user_id)
            .map(|row| row.state);
        let other_state = other_rows
            .iter()
            .find(|row| row.user_id == actor_user_id)
            .map(|row| row.state);
        if actor_state != Some(FriendState::Friend) || other_state != Some(FriendState::Friend) {
            return Err(unavailable());
        }
        let (lower, higher) = if actor_user_id < other_user_id {
            (actor_user_id, other_user_id)
        } else {
            (other_user_id, actor_user_id)
        };
        Ok(AuthorizedChatChannel {
            canonical_key: format!("direct:{lower}:{higher}"),
            channel_type: ChannelType::Direct,
            access_key: format!("direct:{lower}:{higher}"),
        })
    }

    async fn authorize_group(
        &self,
        actor_user_id: &str,
        group_id: GroupId,
    ) -> AppResult<AuthorizedChatChannel> {
        let group = self.groups.get(group_id).await.map_err(|_| unavailable())?;
        if group.find_member(actor_user_id).is_none() {
            return Err(unavailable());
        }
        Ok(AuthorizedChatChannel {
            canonical_key: format!("group:{group_id}"),
            channel_type: ChannelType::Group,
            access_key: format!("group:{group_id}"),
        })
    }
}

fn unavailable() -> AppError {
    AppError::permission(CHAT_UNAVAILABLE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{
        CreateGroupRequest, InMemoryFriendsRepository, InMemoryGroupsRepository,
    };
    use crate::time::TimestampMillis;

    fn authorizer() -> ChatChannelAuthorizer {
        ChatChannelAuthorizer::new(
            Arc::new(FriendsService::new(Arc::new(
                InMemoryFriendsRepository::new(),
            ))),
            Arc::new(GroupsService::new(
                Arc::new(InMemoryGroupsRepository::new()),
            )),
        )
    }

    #[tokio::test]
    async fn direct_target_requires_mutual_friendship_and_is_canonical() {
        let friends = Arc::new(FriendsService::new(Arc::new(
            InMemoryFriendsRepository::new(),
        )));
        let groups = Arc::new(GroupsService::new(
            Arc::new(InMemoryGroupsRepository::new()),
        ));
        let authorizer = ChatChannelAuthorizer::new(Arc::clone(&friends), groups);
        let now = TimestampMillis::from_unix_millis(1);
        friends.add("alice", "bob", now).await.expect("invite");
        friends.add("bob", "alice", now).await.expect("accept");

        let channel = authorizer
            .authorize(
                "alice",
                ChatTarget::Direct {
                    other_user_id: "bob".to_owned(),
                },
            )
            .await
            .expect("authorized");
        assert_eq!(channel.canonical_key, "direct:alice:bob");

        friends.block("bob", "alice", now).await.expect("block");
        let err = authorizer
            .authorize(
                "alice",
                ChatTarget::Direct {
                    other_user_id: "bob".to_owned(),
                },
            )
            .await
            .expect_err("hard block must deny chat");
        assert_eq!(err.to_string(), format!("permission: {CHAT_UNAVAILABLE}"));
    }

    #[tokio::test]
    async fn group_target_requires_current_membership() {
        let friends = Arc::new(FriendsService::new(Arc::new(
            InMemoryFriendsRepository::new(),
        )));
        let groups = Arc::new(GroupsService::new(
            Arc::new(InMemoryGroupsRepository::new()),
        ));
        let authorizer = ChatChannelAuthorizer::new(friends, Arc::clone(&groups));
        let group = groups
            .create(CreateGroupRequest {
                name: "team".to_owned(),
                description: String::new(),
                open: true,
                max_size: 0,
                creator_user_id: "alice".to_owned(),
                now: TimestampMillis::from_unix_millis(1),
            })
            .await
            .expect("group");
        let channel = authorizer
            .authorize("alice", ChatTarget::Group { group_id: group.id })
            .await
            .expect("member authorized");
        assert_eq!(channel.canonical_key, format!("group:{}", group.id));
        assert!(
            authorizer
                .authorize("bob", ChatTarget::Group { group_id: group.id })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn room_target_uses_only_gateway_supplied_room_id() {
        let result = authorizer()
            .authorize("alice", ChatTarget::CurrentRoom { room_id: 7 })
            .await
            .expect("gateway current room is authoritative");
        assert_eq!(result.canonical_key, "room:7");
        assert!(
            authorizer()
                .authorize("alice", ChatTarget::CurrentRoom { room_id: 0 })
                .await
                .is_err()
        );
    }
}
