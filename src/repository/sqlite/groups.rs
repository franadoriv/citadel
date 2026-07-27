//! SQLite groups repository.
//!
//! [`SqliteGroupsRepository`] is the durable single-file backend for
//! [`GroupsRepository`](crate::repository::GroupsRepository). It is the sibling
//! of the Postgres impl in `../pg/groups.rs`: groups live in a `groups` table
//! (an `INTEGER PRIMARY KEY AUTOINCREMENT` id, unique `name`, metadata, and the
//! founding `creator_id`) and their membership in `group_memberships`
//! (`PRIMARY KEY (group_id, user_id)`, `ON DELETE CASCADE`). The role ladder, the
//! last-superadmin invariant, and list pagination are reused from the shared pure
//! helpers in [`crate::repository::groups`], so the two backends cannot drift.
//!
//! SQLite has no `SELECT … FOR UPDATE`, so every read-modify-write runs under
//! `BEGIN IMMEDIATE`, which takes the writer slot up front and serializes the
//! decision the way the Postgres row lock does. The new group's id is read with
//! `last_insert_rowid` inside that same transaction. Roles are stored as their
//! stable [`GroupRole::as_str`] token (parsed back with [`GroupRole::from_token`])
//! and timestamps use the shared integer-millis conversion.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::repository::GroupsRepository;
use crate::repository::groups::{
    AdmissionKind, AdmissionOutcome, CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole,
    GroupsPage, Membership, UpdateGroupRequest, ensure_can_add_member, ensure_can_kick, paginate,
    plan_demote, plan_promote,
};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const INSERT_GROUP_SQL: &str = "\
INSERT INTO groups \
(name, description, open, max_size, creator_id, created_at_unix_ms, updated_at_unix_ms) \
VALUES (?, ?, ?, ?, ?, ?, ?)";

const LAST_ID_SQL: &str = "SELECT last_insert_rowid() AS id";

const INSERT_MEMBERSHIP_SQL: &str = "\
INSERT INTO group_memberships (group_id, user_id, role, joined_at_unix_ms) \
VALUES (?, ?, ?, ?)";

const SELECT_GROUP_SQL: &str = "\
SELECT id, name, description, open, max_size, created_at_unix_ms FROM groups WHERE id = ?";

const LIST_GROUPS_SQL: &str = "\
SELECT id, name, description, open, max_size, created_at_unix_ms FROM groups ORDER BY id";

const SELECT_MEMBERS_SQL: &str = "\
SELECT user_id, role, joined_at_unix_ms FROM group_memberships WHERE group_id = ? \
ORDER BY joined_at_unix_ms, user_id";

const UPDATE_GROUP_SQL: &str =
    "UPDATE groups SET description = ?, open = ?, max_size = ? WHERE id = ?";

const DELETE_GROUP_SQL: &str = "DELETE FROM groups WHERE id = ?";

const UPDATE_MEMBER_ROLE_SQL: &str =
    "UPDATE group_memberships SET role = ? WHERE group_id = ? AND user_id = ?";

const DELETE_MEMBER_SQL: &str = "DELETE FROM group_memberships WHERE group_id = ? AND user_id = ?";

const SELECT_ADMISSION_SQL: &str =
    "SELECT kind FROM group_admissions WHERE group_id = ? AND user_id = ?";
const UPSERT_ADMISSION_SQL: &str = "INSERT INTO group_admissions \
(group_id, user_id, kind, inviter_user_id, created_at_unix_ms) VALUES (?, ?, ?, ?, ?) \
ON CONFLICT(group_id, user_id) DO UPDATE SET kind = excluded.kind, inviter_user_id = excluded.inviter_user_id, created_at_unix_ms = excluded.created_at_unix_ms";
const DELETE_ADMISSION_SQL: &str =
    "DELETE FROM group_admissions WHERE group_id = ? AND user_id = ?";

const ADVANCE_CHAT_ACCESS_EPOCH_SQL: &str = "\
INSERT INTO chat_access_epochs (access_key, epoch, updated_at_unix_ms) VALUES (?, 1, ?) \
ON CONFLICT(access_key) DO UPDATE SET epoch = chat_access_epochs.epoch + 1, \
updated_at_unix_ms = excluded.updated_at_unix_ms";

// --- mapping helpers --------------------------------------------------------

/// The scalar (member-less) fields of a group row.
type GroupShell = (GroupId, String, String, bool, u32, TimestampMillis);

fn parse_group_row(row: &SqliteRow) -> AppResult<GroupShell> {
    let id: i64 = get(row, "id")?;
    let id = u64::try_from(id).map_err(|_| AppError::internal("negative group id"))?;
    let name: String = get(row, "name")?;
    let description: String = get(row, "description")?;
    let open: bool = get(row, "open")?;
    let max_size: i64 = get(row, "max_size")?;
    let max_size =
        u32::try_from(max_size).map_err(|_| AppError::internal("group max_size out of range"))?;
    let created: i64 = get(row, "created_at_unix_ms")?;
    Ok((
        id,
        name,
        description,
        open,
        max_size,
        millis_to_ts(created)?,
    ))
}

fn row_to_membership(row: &SqliteRow) -> AppResult<Membership> {
    let user_id: String = get(row, "user_id")?;
    let role: String = get(row, "role")?;
    let joined: i64 = get(row, "joined_at_unix_ms")?;
    Ok(Membership {
        user_id,
        role: GroupRole::from_token(&role)?,
        joined_at: millis_to_ts(joined)?,
    })
}

fn id_to_i64(id: GroupId) -> AppResult<i64> {
    i64::try_from(id).map_err(|_| AppError::internal("group id out of range for integer column"))
}

async fn advance_chat_access_epoch(
    conn: &mut SqliteConnection,
    id: GroupId,
    millis: i64,
) -> AppResult<()> {
    sqlx::query(ADVANCE_CHAT_ACCESS_EPOCH_SQL)
        .bind(format!("group:{id}"))
        .bind(millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

fn group_from_shell(shell: GroupShell, members: Vec<Membership>) -> Group {
    let (id, name, description, open, max_size, created_at) = shell;
    Group::from_parts(id, name, description, open, max_size, created_at, members)
}

async fn load_members(conn: &mut SqliteConnection, id: GroupId) -> AppResult<Vec<Membership>> {
    let rows = sqlx::query(SELECT_MEMBERS_SQL)
        .bind(id_to_i64(id)?)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_membership).collect()
}

async fn load_group(conn: &mut SqliteConnection, id: GroupId) -> AppResult<Option<Group>> {
    let row = sqlx::query(SELECT_GROUP_SQL)
        .bind(id_to_i64(id)?)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let shell = parse_group_row(&row)?;
    let members = load_members(conn, id).await?;
    Ok(Some(group_from_shell(shell, members)))
}

async fn require_group(conn: &mut SqliteConnection, id: GroupId) -> AppResult<Group> {
    load_group(conn, id)
        .await?
        .ok_or_else(|| AppError::not_found("group not found"))
}

// --- repository -------------------------------------------------------------

/// SQLite [`GroupsRepository`].
pub struct SqliteGroupsRepository {
    executor: SqliteExecutor,
}

impl SqliteGroupsRepository {
    /// Bind a groups repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                let result = {
                    let $conn = &mut *tx;
                    $body
                };
                match result {
                    Ok(value) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(value)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

macro_rules! with_conn {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl GroupsRepository for SqliteGroupsRepository {
    async fn create(&self, request: CreateGroupRequest) -> AppResult<Group> {
        with_tx!(self, conn => create_conn(conn, request).await)
    }

    async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage> {
        with_conn!(self, conn => list_conn(conn, filter).await)
    }

    async fn get(&self, id: GroupId) -> AppResult<Option<Group>> {
        with_conn!(self, conn => load_group(conn, id).await)
    }

    async fn update(&self, id: GroupId, request: UpdateGroupRequest) -> AppResult<Group> {
        with_tx!(self, conn => update_conn(conn, id, request).await)
    }

    async fn delete(&self, id: GroupId) -> AppResult<bool> {
        with_tx!(self, conn => delete_conn(conn, id).await)
    }

    async fn add_member(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        with_tx!(self, conn => add_member_conn(conn, id, user_id, now).await)
    }

    async fn kick_member(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        with_tx!(self, conn => kick_conn(conn, id, user_id).await)
    }

    async fn promote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        with_tx!(self, conn => role_change_conn(conn, id, user_id, RoleChange::Promote).await)
    }

    async fn demote(&self, id: GroupId, user_id: &str) -> AppResult<Group> {
        with_tx!(self, conn => role_change_conn(conn, id, user_id, RoleChange::Demote).await)
    }

    async fn join(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        with_tx!(self, conn => join_conn(conn, id, user_id, now).await)
    }

    async fn invite(
        &self,
        id: GroupId,
        user_id: &str,
        inviter_user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<AdmissionOutcome> {
        with_tx!(self, conn => invite_conn(conn, id, user_id, inviter_user_id, now).await)
    }

    async fn approve_request(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        with_tx!(self, conn => admit_conn(conn, id, user_id, now, AdmissionKind::Request).await)
    }

    async fn accept_invitation(
        &self,
        id: GroupId,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<Group> {
        with_tx!(self, conn => admit_conn(conn, id, user_id, now, AdmissionKind::Invitation).await)
    }

    async fn cancel_admission(&self, id: GroupId, user_id: &str) -> AppResult<()> {
        with_tx!(self, conn => cancel_admission_conn(conn, id, user_id).await)
    }

    async fn transfer_ownership(
        &self,
        id: GroupId,
        from_user_id: &str,
        to_user_id: &str,
    ) -> AppResult<Group> {
        with_tx!(self, conn => transfer_ownership_conn(conn, id, from_user_id, to_user_id).await)
    }
}

async fn create_conn(conn: &mut SqliteConnection, request: CreateGroupRequest) -> AppResult<Group> {
    let millis = ts_to_millis(request.now)?;
    let max_size = i64::from(request.max_size);
    sqlx::query(INSERT_GROUP_SQL)
        .bind(&request.name)
        .bind(&request.description)
        .bind(request.open)
        .bind(max_size)
        .bind(&request.creator_user_id)
        .bind(millis)
        .bind(millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let row = sqlx::query(LAST_ID_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let id: i64 = get(&row, "id")?;
    let id = u64::try_from(id).map_err(|_| AppError::internal("negative group id"))?;
    sqlx::query(INSERT_MEMBERSHIP_SQL)
        .bind(id_to_i64(id)?)
        .bind(&request.creator_user_id)
        .bind(GroupRole::Superadmin.as_str())
        .bind(millis)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
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

async fn list_conn(conn: &mut SqliteConnection, filter: &GroupFilter) -> AppResult<GroupsPage> {
    let rows = sqlx::query(LIST_GROUPS_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let mut groups = Vec::with_capacity(rows.len());
    for row in &rows {
        let shell = parse_group_row(row)?;
        let members = load_members(conn, shell.0).await?;
        groups.push(group_from_shell(shell, members));
    }
    Ok(paginate(groups, filter))
}

async fn update_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    request: UpdateGroupRequest,
) -> AppResult<Group> {
    let group = require_group(conn, id).await?;
    let description = request
        .description
        .unwrap_or_else(|| group.description.clone());
    let open = request.open.unwrap_or(group.open);
    let max_size = request.max_size.unwrap_or(group.max_size);
    sqlx::query(UPDATE_GROUP_SQL)
        .bind(&description)
        .bind(open)
        .bind(i64::from(max_size))
        .bind(id_to_i64(id)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(Group::from_parts(
        group.id,
        group.name.clone(),
        description,
        open,
        max_size,
        group.created_at,
        group.members().to_vec(),
    ))
}

async fn delete_conn(conn: &mut SqliteConnection, id: GroupId) -> AppResult<bool> {
    let result = sqlx::query(DELETE_GROUP_SQL)
        .bind(id_to_i64(id)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let deleted = result.rows_affected() > 0;
    if deleted {
        advance_chat_access_epoch(conn, id, 0).await?;
    }
    Ok(deleted)
}

async fn add_member_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    now: TimestampMillis,
) -> AppResult<Group> {
    let mut group = require_group(conn, id).await?;
    ensure_can_add_member(
        group.find_member(user_id).is_some(),
        group.member_count(),
        group.max_size,
    )?;
    sqlx::query(INSERT_MEMBERSHIP_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .bind(GroupRole::Member.as_str())
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.push_member(Membership {
        user_id: user_id.to_string(),
        role: GroupRole::Member,
        joined_at: now,
    });
    advance_chat_access_epoch(conn, id, ts_to_millis(now)?).await?;
    Ok(group)
}

async fn kick_conn(conn: &mut SqliteConnection, id: GroupId, user_id: &str) -> AppResult<Group> {
    let mut group = require_group(conn, id).await?;
    let role = group
        .find_member(user_id)
        .ok_or_else(|| AppError::not_found("member not found"))?
        .role;
    ensure_can_kick(role, group.superadmin_count())?;
    sqlx::query(DELETE_MEMBER_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.remove_member(user_id);
    advance_chat_access_epoch(conn, id, 0).await?;
    Ok(group)
}

enum RoleChange {
    Promote,
    Demote,
}

async fn role_change_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    change: RoleChange,
) -> AppResult<Group> {
    let mut group = require_group(conn, id).await?;
    let current = group
        .find_member(user_id)
        .ok_or_else(|| AppError::not_found("member not found"))?
        .role;
    let next = match change {
        RoleChange::Promote => plan_promote(current)?,
        RoleChange::Demote => plan_demote(current, group.superadmin_count())?,
    };
    sqlx::query(UPDATE_MEMBER_ROLE_SQL)
        .bind(next.as_str())
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.set_member_role(user_id, next);
    advance_chat_access_epoch(conn, id, 0).await?;
    Ok(group)
}

async fn admission_kind(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
) -> AppResult<Option<AdmissionKind>> {
    let row = sqlx::query(SELECT_ADMISSION_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.map(|row| match get::<String>(&row, "kind")?.as_str() {
        "request" => Ok(AdmissionKind::Request),
        "invitation" => Ok(AdmissionKind::Invitation),
        _ => Err(AppError::internal("unknown group admission kind")),
    })
    .transpose()
}

async fn write_admission(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    kind: AdmissionKind,
    inviter: Option<&str>,
    now: TimestampMillis,
) -> AppResult<()> {
    let kind = match kind {
        AdmissionKind::Request => "request",
        AdmissionKind::Invitation => "invitation",
    };
    sqlx::query(UPSERT_ADMISSION_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .bind(kind)
        .bind(inviter)
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn join_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    now: TimestampMillis,
) -> AppResult<AdmissionOutcome> {
    let mut group = require_group(conn, id).await?;
    if group.find_member(user_id).is_some() {
        return Ok(AdmissionOutcome::AlreadyMember(group));
    }
    if group.open {
        ensure_can_add_member(false, group.member_count(), group.max_size)?;
        sqlx::query(INSERT_MEMBERSHIP_SQL)
            .bind(id_to_i64(id)?)
            .bind(user_id)
            .bind(GroupRole::Member.as_str())
            .bind(ts_to_millis(now)?)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        sqlx::query(DELETE_ADMISSION_SQL)
            .bind(id_to_i64(id)?)
            .bind(user_id)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        group.push_member(Membership {
            user_id: user_id.to_owned(),
            role: GroupRole::Member,
            joined_at: now,
        });
        advance_chat_access_epoch(conn, id, ts_to_millis(now)?).await?;
        return Ok(AdmissionOutcome::Joined(group));
    }
    match admission_kind(conn, id, user_id).await? {
        Some(AdmissionKind::Invitation) => Ok(AdmissionOutcome::InvitationCreated),
        Some(AdmissionKind::Request) => Ok(AdmissionOutcome::RequestCreated),
        None => {
            write_admission(conn, id, user_id, AdmissionKind::Request, None, now).await?;
            Ok(AdmissionOutcome::RequestCreated)
        }
    }
}

async fn invite_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    inviter: &str,
    now: TimestampMillis,
) -> AppResult<AdmissionOutcome> {
    let group = require_group(conn, id).await?;
    if group.find_member(user_id).is_some() {
        return Ok(AdmissionOutcome::AlreadyMember(group));
    }
    write_admission(
        conn,
        id,
        user_id,
        AdmissionKind::Invitation,
        Some(inviter),
        now,
    )
    .await?;
    Ok(AdmissionOutcome::InvitationCreated)
}

async fn admit_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
    now: TimestampMillis,
    expected: AdmissionKind,
) -> AppResult<Group> {
    let mut group = require_group(conn, id).await?;
    if admission_kind(conn, id, user_id).await? != Some(expected) {
        return Err(AppError::not_found("group admission not found"));
    }
    ensure_can_add_member(
        group.find_member(user_id).is_some(),
        group.member_count(),
        group.max_size,
    )?;
    sqlx::query(INSERT_MEMBERSHIP_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .bind(GroupRole::Member.as_str())
        .bind(ts_to_millis(now)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(DELETE_ADMISSION_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.push_member(Membership {
        user_id: user_id.to_owned(),
        role: GroupRole::Member,
        joined_at: now,
    });
    advance_chat_access_epoch(conn, id, ts_to_millis(now)?).await?;
    Ok(group)
}

async fn cancel_admission_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    user_id: &str,
) -> AppResult<()> {
    require_group(conn, id).await?;
    sqlx::query(DELETE_ADMISSION_SQL)
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn transfer_ownership_conn(
    conn: &mut SqliteConnection,
    id: GroupId,
    from: &str,
    to: &str,
) -> AppResult<Group> {
    let mut group = require_group(conn, id).await?;
    if group.find_member(from).map(|member| member.role) != Some(GroupRole::Superadmin) {
        return Err(AppError::permission("current superadmin role required"));
    }
    if from == to {
        return Ok(group);
    }
    if group.find_member(to).is_none() {
        return Err(AppError::not_found("target member not found"));
    }
    sqlx::query(UPDATE_MEMBER_ROLE_SQL)
        .bind(GroupRole::Admin.as_str())
        .bind(id_to_i64(id)?)
        .bind(from)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(UPDATE_MEMBER_ROLE_SQL)
        .bind(GroupRole::Superadmin.as_str())
        .bind(id_to_i64(id)?)
        .bind(to)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.set_member_role(from, GroupRole::Admin);
    group.set_member_role(to, GroupRole::Superadmin);
    advance_chat_access_epoch(conn, id, 0).await?;
    Ok(group)
}
