//! Console Groups section.
//!
//! Operator administration over the in-process
//! [`GroupsService`](crate::services::GroupsService): create/list/get/update/
//! delete groups, and add/kick/promote/demote members. Persistence is
//! explicitly out of scope (see the service's module docs) — a node restart
//! clears every group, which is recorded technical debt.
//!
//! - `GET /console/v1/groups` — paged group summaries, optional `filter`
//!   (name substring).
//! - `POST /console/v1/groups` — create a group (admin, audited).
//! - `GET /console/v1/groups/{id}` — one group with its member roll.
//! - `PUT /console/v1/groups/{id}` — patch description/open/max_size (admin,
//!   audited).
//! - `DELETE /console/v1/groups/{id}` — delete a group (admin, audited).
//! - `POST /console/v1/groups/{id}/members` — add a member (admin, audited).
//! - `POST /console/v1/groups/{id}/members/{user_id}/promote|demote|kick` —
//!   member role transitions / removal (admin, audited).

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{
    AuditEntry, ConsolePrincipal, CreateGroupRequest, Group, GroupFilter, GroupId,
    UpdateGroupRequest,
};
use crate::time::{Clock, SystemClock};

/// The Groups section route (list + create).
pub const GROUPS_PATH: &str = "/console/v1/groups";

/// Single-group route pattern (`GET`/`PUT`/`DELETE`).
pub const GROUP_DETAIL_PATH: &str = "/console/v1/groups/:id";

/// Member-add route pattern.
pub const GROUP_MEMBERS_PATH: &str = "/console/v1/groups/:id/members";

/// Promote-member route pattern.
pub const GROUP_MEMBER_PROMOTE_PATH: &str = "/console/v1/groups/:id/members/:user_id/promote";

/// Demote-member route pattern.
pub const GROUP_MEMBER_DEMOTE_PATH: &str = "/console/v1/groups/:id/members/:user_id/demote";

/// Kick-member route pattern.
pub const GROUP_MEMBER_KICK_PATH: &str = "/console/v1/groups/:id/members/:user_id/kick";

/// Default listing page size.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one listing page.
const MAX_LIMIT: usize = 200;

/// Query parameters for [`GROUPS_PATH`]'s `GET`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Case-sensitive substring filter over the group name.
    pub filter: Option<String>,
    /// Page size (default 50, capped at 200).
    pub limit: Option<usize>,
    /// Number of matching groups to skip.
    #[serde(default)]
    pub offset: usize,
}

/// One group row, without its member roll.
#[derive(Debug, Clone, Serialize)]
pub struct GroupSummary {
    /// Server-assigned id.
    pub id: GroupId,
    /// The unique group name.
    pub name: String,
    /// Free-form description.
    pub description: String,
    /// Whether the group is open (advisory today; see the service docs).
    pub open: bool,
    /// Maximum member count (`0` = unlimited).
    pub max_size: u32,
    /// Current member count.
    pub member_count: usize,
    /// Creation time (unix milliseconds).
    pub created_at_unix_ms: u64,
}

impl GroupSummary {
    fn from_group(group: &Group) -> Self {
        Self {
            id: group.id,
            name: group.name.clone(),
            description: group.description.clone(),
            open: group.open,
            max_size: group.max_size,
            member_count: group.member_count(),
            created_at_unix_ms: group.created_at.unix_millis(),
        }
    }
}

/// The JSON response for [`GROUPS_PATH`]'s `GET`.
#[derive(Debug, Clone, Serialize)]
pub struct GroupsPageBody {
    /// Matching groups, id-ordered.
    pub items: Vec<GroupSummary>,
    /// Total groups matching the filter, before paging.
    pub total: usize,
}

/// One member row in a group detail response.
#[derive(Debug, Clone, Serialize)]
pub struct MemberRow {
    /// The member's user id.
    pub user_id: String,
    /// The member's current role: `member`, `admin`, or `superadmin`.
    pub role: &'static str,
    /// When the user joined (unix milliseconds).
    pub joined_at_unix_ms: u64,
}

/// The JSON response for a single group: summary plus its member roll.
#[derive(Debug, Clone, Serialize)]
pub struct GroupDetailBody {
    /// The group summary fields.
    #[serde(flatten)]
    pub summary: GroupSummary,
    /// Every member, in join order.
    pub members: Vec<MemberRow>,
}

impl GroupDetailBody {
    fn from_group(group: &Group) -> Self {
        Self {
            summary: GroupSummary::from_group(group),
            members: group
                .members()
                .iter()
                .map(|m| MemberRow {
                    user_id: m.user_id.clone(),
                    role: m.role.as_str(),
                    joined_at_unix_ms: m.joined_at.unix_millis(),
                })
                .collect(),
        }
    }
}

/// The JSON body accepted by `POST /console/v1/groups`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateGroupBody {
    /// The new group's name (must be unique, non-blank).
    pub name: String,
    /// Free-form description (default empty).
    #[serde(default)]
    pub description: Option<String>,
    /// Whether the group is open (default `true`).
    #[serde(default)]
    pub open: Option<bool>,
    /// Maximum member count, `0` = unlimited (default `0`).
    #[serde(default)]
    pub max_size: Option<u32>,
    /// The founding superadmin's user id (default: the operator's username).
    #[serde(default)]
    pub creator_user_id: Option<String>,
}

/// The JSON body accepted by `PUT /console/v1/groups/{id}`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGroupBody {
    /// New description, if changing.
    #[serde(default)]
    pub description: Option<String>,
    /// New open/closed flag, if changing.
    #[serde(default)]
    pub open: Option<bool>,
    /// New member cap, if changing.
    #[serde(default)]
    pub max_size: Option<u32>,
}

/// The JSON body accepted by `POST /console/v1/groups/{id}/members`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddMemberBody {
    /// The user id to add as a `member`.
    pub user_id: String,
}

/// `GET /console/v1/groups`: paged group summaries.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(params): Query<ListParams>,
) -> Result<Json<GroupsPageBody>, ApiError> {
    app.metrics().record_http_request();
    let filter = GroupFilter {
        name_contains: params.filter,
        limit: params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        offset: params.offset,
    };
    let page = app.groups().list(&filter).await?;
    Ok(Json(GroupsPageBody {
        items: page.items.iter().map(GroupSummary::from_group).collect(),
        total: page.total,
    }))
}

/// `POST /console/v1/groups`: create a group (admin, audited).
pub(super) async fn create_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    body: Result<Json<CreateGroupBody>, JsonRejection>,
) -> Result<(StatusCode, Json<GroupDetailBody>), ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let now = SystemClock.now();
    let creator_user_id = body.creator_user_id.unwrap_or_else(|| operator.actor_id());
    let group = app
        .groups()
        .create(CreateGroupRequest {
            name: body.name,
            description: body.description.unwrap_or_default(),
            open: body.open.unwrap_or(true),
            max_size: body.max_size.unwrap_or(0),
            creator_user_id,
            now,
        })
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "groups.create",
        group.id.to_string(),
        format!("created group '{}'", group.name),
    ));
    Ok((
        StatusCode::CREATED,
        Json(GroupDetailBody::from_group(&group)),
    ))
}

/// `GET /console/v1/groups/{id}`: one group with its member roll.
pub(super) async fn detail_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Path(id): Path<GroupId>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    let group = app.groups().get(id).await?;
    Ok(Json(GroupDetailBody::from_group(&group)))
}

/// `PUT /console/v1/groups/{id}`: patch description/open/max_size (admin,
/// audited).
pub(super) async fn update_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(id): Path<GroupId>,
    body: Result<Json<UpdateGroupBody>, JsonRejection>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let group = app
        .groups()
        .update(
            id,
            UpdateGroupRequest {
                description: body.description,
                open: body.open,
                max_size: body.max_size,
            },
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "groups.update",
        id.to_string(),
        "updated group settings",
    ));
    Ok(Json(GroupDetailBody::from_group(&group)))
}

/// `DELETE /console/v1/groups/{id}`: delete a group (admin, audited).
pub(super) async fn delete_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(id): Path<GroupId>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    app.groups().delete(id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "groups.delete",
        id.to_string(),
        "deleted group",
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /console/v1/groups/{id}/members`: add a member (admin, audited).
pub(super) async fn add_member_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(id): Path<GroupId>,
    body: Result<Json<AddMemberBody>, JsonRejection>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let now = SystemClock.now();
    let group = app
        .groups()
        .add_member(id, body.user_id.clone(), now)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "groups.member.add",
        format!("{id}/{}", body.user_id),
        "added member",
    ));
    Ok(Json(GroupDetailBody::from_group(&group)))
}

/// `POST /console/v1/groups/{id}/members/{user_id}/promote` (admin, audited).
pub(super) async fn promote_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((id, user_id)): Path<(GroupId, String)>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let group = app.groups().promote(id, &user_id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "groups.member.promote",
        format!("{id}/{user_id}"),
        "promoted member",
    ));
    Ok(Json(GroupDetailBody::from_group(&group)))
}

/// `POST /console/v1/groups/{id}/members/{user_id}/demote` (admin, audited).
pub(super) async fn demote_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((id, user_id)): Path<(GroupId, String)>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let group = app.groups().demote(id, &user_id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "groups.member.demote",
        format!("{id}/{user_id}"),
        "demoted member",
    ));
    Ok(Json(GroupDetailBody::from_group(&group)))
}

/// `POST /console/v1/groups/{id}/members/{user_id}/kick` (admin, audited).
pub(super) async fn kick_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((id, user_id)): Path<(GroupId, String)>,
) -> Result<Json<GroupDetailBody>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let group = app.groups().kick_member(id, &user_id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "groups.member.kick",
        format!("{id}/{user_id}"),
        "kicked member",
    ));
    Ok(Json(GroupDetailBody::from_group(&group)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&GROUPS_PATH));
        assert!(GROUP_DETAIL_PATH.starts_with(GROUPS_PATH));
        assert!(GROUP_MEMBERS_PATH.starts_with(GROUPS_PATH));
        assert!(GROUP_MEMBER_PROMOTE_PATH.starts_with(GROUP_MEMBERS_PATH));
        assert!(GROUP_MEMBER_DEMOTE_PATH.starts_with(GROUP_MEMBERS_PATH));
        assert!(GROUP_MEMBER_KICK_PATH.starts_with(GROUP_MEMBERS_PATH));
    }

    #[test]
    fn create_body_defaults_are_open_and_unlimited() {
        let body: CreateGroupBody = serde_json::from_str(r#"{"name":"raiders"}"#).expect("parse");
        assert!(body.description.is_none());
        assert!(body.open.is_none());
        assert!(body.max_size.is_none());
        assert!(body.creator_user_id.is_none());
        // Unknown fields are rejected at the boundary.
        assert!(serde_json::from_str::<CreateGroupBody>(r#"{"name":"x","extra":1}"#).is_err());
    }

    #[tokio::test]
    async fn group_detail_flattens_summary_fields() {
        let service = crate::services::GroupsService::new(std::sync::Arc::new(
            crate::repository::InMemoryGroupsRepository::new(),
        ));
        let group = service
            .create(CreateGroupRequest {
                name: "raiders".to_string(),
                description: "desc".to_string(),
                open: true,
                max_size: 0,
                creator_user_id: "u-1".to_string(),
                now: crate::time::TimestampMillis::from_unix_millis(1),
            })
            .await
            .expect("create");
        let value = serde_json::to_value(GroupDetailBody::from_group(&group)).expect("serialize");
        assert_eq!(value["name"], "raiders");
        assert_eq!(value["member_count"], 1);
        assert_eq!(value["members"][0]["user_id"], "u-1");
        assert_eq!(value["members"][0]["role"], "superadmin");
    }
}
