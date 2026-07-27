//! Postgres groups repository.
//!
//! [`PgGroupsRepository`] is the durable backend for
//! [`GroupsRepository`](crate::repository::GroupsRepository). Groups live in a
//! `groups` table (a database identity `id`, unique `name`, metadata, and the
//! founding `creator_id`) and their membership in `group_memberships`
//! (`PRIMARY KEY (group_id, user_id)`, `ON DELETE CASCADE` from `groups`). The
//! role ladder, the last-superadmin invariant, the add-member uniqueness/cap
//! rules, and list pagination are reused from the shared pure helpers in
//! [`crate::repository::groups`], so this backend cannot drift from the in-memory
//! reference or the SQLite sibling.
//!
//! Every read-modify-write (`create`, `update`, `add_member`, `kick_member`,
//! `promote`, `demote`) runs in a transaction and locks the group row with
//! `SELECT … FOR UPDATE`, so concurrent membership mutations against the same
//! group serialize. Roles are stored as their stable [`GroupRole::as_str`] token
//! and parsed back with [`GroupRole::from_token`]; timestamps use the shared
//! bigint-millis conversion.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};

use crate::error::{AppError, AppResult};
use crate::repository::GroupsRepository;
use crate::repository::groups::{
    CreateGroupRequest, Group, GroupFilter, GroupId, GroupRole, GroupsPage, Membership,
    UpdateGroupRequest, ensure_can_add_member, ensure_can_kick, paginate, plan_demote,
    plan_promote,
};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const INSERT_GROUP_SQL: &str = "\
INSERT INTO groups \
(name, description, open, max_size, creator_id, created_at_unix_ms, updated_at_unix_ms) \
VALUES ($1, $2, $3, $4, $5, $6, $6) RETURNING id";

const INSERT_MEMBERSHIP_SQL: &str = "\
INSERT INTO group_memberships (group_id, user_id, role, joined_at_unix_ms) \
VALUES ($1, $2, $3, $4)";

const SELECT_GROUP_SQL: &str = "\
SELECT id, name, description, open, max_size, created_at_unix_ms FROM groups WHERE id = $1";

const SELECT_GROUP_LOCK_SQL: &str = "\
SELECT id, name, description, open, max_size, created_at_unix_ms FROM groups WHERE id = $1 \
FOR UPDATE";

const LIST_GROUPS_SQL: &str = "\
SELECT id, name, description, open, max_size, created_at_unix_ms FROM groups ORDER BY id";

const SELECT_MEMBERS_SQL: &str = "\
SELECT user_id, role, joined_at_unix_ms FROM group_memberships WHERE group_id = $1 \
ORDER BY joined_at_unix_ms, user_id";

const UPDATE_GROUP_SQL: &str =
    "UPDATE groups SET description = $2, open = $3, max_size = $4 WHERE id = $1";

const DELETE_GROUP_SQL: &str = "DELETE FROM groups WHERE id = $1";

const UPDATE_MEMBER_ROLE_SQL: &str =
    "UPDATE group_memberships SET role = $3 WHERE group_id = $1 AND user_id = $2";

const DELETE_MEMBER_SQL: &str =
    "DELETE FROM group_memberships WHERE group_id = $1 AND user_id = $2";

const ADVANCE_CHAT_ACCESS_EPOCH_SQL: &str = "\
INSERT INTO chat_access_epochs (access_key, epoch, updated_at_unix_ms) VALUES ($1, 1, $2) \
ON CONFLICT(access_key) DO UPDATE SET epoch = chat_access_epochs.epoch + 1, \
updated_at_unix_ms = excluded.updated_at_unix_ms";

// --- mapping helpers --------------------------------------------------------

/// The scalar (member-less) fields of a group row.
type GroupShell = (GroupId, String, String, bool, u32, TimestampMillis);

fn parse_group_row(row: &PgRow) -> AppResult<GroupShell> {
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

fn row_to_membership(row: &PgRow) -> AppResult<Membership> {
    let user_id: String = get(row, "user_id")?;
    let role: String = get(row, "role")?;
    let joined: i64 = get(row, "joined_at_unix_ms")?;
    Ok(Membership {
        user_id,
        role: GroupRole::from_token(&role)?,
        joined_at: millis_to_ts(joined)?,
    })
}

async fn load_members(conn: &mut PgConnection, id: GroupId) -> AppResult<Vec<Membership>> {
    let group_id = id_to_i64(id)?;
    let rows = sqlx::query(SELECT_MEMBERS_SQL)
        .bind(group_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_membership).collect()
}

fn id_to_i64(id: GroupId) -> AppResult<i64> {
    i64::try_from(id).map_err(|_| AppError::internal("group id out of range for bigint column"))
}

async fn advance_chat_access_epoch(
    conn: &mut PgConnection,
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

// --- repository -------------------------------------------------------------

/// Postgres [`GroupsRepository`].
pub struct PgGroupsRepository {
    executor: PgExecutor,
}

impl PgGroupsRepository {
    /// Bind a groups repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
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
            PgExecutor::Tx(cell) => {
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
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl GroupsRepository for PgGroupsRepository {
    async fn create(&self, request: CreateGroupRequest) -> AppResult<Group> {
        with_tx!(self, conn => create_conn(conn, request).await)
    }

    async fn list(&self, filter: &GroupFilter) -> AppResult<GroupsPage> {
        with_conn!(self, conn => list_conn(conn, filter).await)
    }

    async fn get(&self, id: GroupId) -> AppResult<Option<Group>> {
        with_conn!(self, conn => load_group(conn, id, false).await)
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
}

/// Load one group (row + members). `lock` takes a `FOR UPDATE` row lock for the
/// read-modify-write callers.
async fn load_group(conn: &mut PgConnection, id: GroupId, lock: bool) -> AppResult<Option<Group>> {
    let group_id = id_to_i64(id)?;
    let sql = if lock {
        SELECT_GROUP_LOCK_SQL
    } else {
        SELECT_GROUP_SQL
    };
    let row = sqlx::query(sql)
        .bind(group_id)
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

/// Load a group for a read-modify-write, mapping absence to `NotFound`.
async fn require_group(conn: &mut PgConnection, id: GroupId) -> AppResult<Group> {
    load_group(conn, id, true)
        .await?
        .ok_or_else(|| AppError::not_found("group not found"))
}

async fn create_conn(conn: &mut PgConnection, request: CreateGroupRequest) -> AppResult<Group> {
    let millis = ts_to_millis(request.now)?;
    let max_size = i64::from(request.max_size);
    let row = sqlx::query(INSERT_GROUP_SQL)
        .bind(&request.name)
        .bind(&request.description)
        .bind(request.open)
        .bind(max_size)
        .bind(&request.creator_user_id)
        .bind(millis)
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

async fn list_conn(conn: &mut PgConnection, filter: &GroupFilter) -> AppResult<GroupsPage> {
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
    conn: &mut PgConnection,
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
        .bind(id_to_i64(id)?)
        .bind(&description)
        .bind(open)
        .bind(i64::from(max_size))
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

async fn delete_conn(conn: &mut PgConnection, id: GroupId) -> AppResult<bool> {
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
    conn: &mut PgConnection,
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

async fn kick_conn(conn: &mut PgConnection, id: GroupId, user_id: &str) -> AppResult<Group> {
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
    conn: &mut PgConnection,
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
        .bind(id_to_i64(id)?)
        .bind(user_id)
        .bind(next.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    group.set_member_role(user_id, next);
    advance_chat_access_epoch(conn, id, 0).await?;
    Ok(group)
}
