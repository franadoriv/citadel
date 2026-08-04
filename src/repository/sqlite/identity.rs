//! SQLite identity repositories.
//!
//! [`SqliteUserRepository`] and [`SqliteAuthIdentityRepository`] are the durable
//! single-file backends for [`UserRepository`](crate::repository::UserRepository)
//! and [`AuthIdentityRepository`](crate::repository::AuthIdentityRepository). They
//! reproduce the in-memory reference impls and the Postgres impls exactly:
//!
//! - Account ids and usernames are unique (the composite `(provider,
//!   external_id)` is unique for credentials), enforced by database constraints
//!   rather than an application lock. A concurrent duplicate surfaces as a SQLite
//!   unique violation → [`Conflict`](crate::error::ErrorCategory::Conflict) (via
//!   [`super::db_err`], which detects the portable
//!   [`sqlx::error::ErrorKind::UniqueViolation`] rather than the Postgres `23505`),
//!   and the error detail never echoes the colliding value, so it is not a
//!   credential-existence oracle.
//! - `created_at` is immutable across updates.
//! - `link_auth_identity` keeps the one-credential-to-one-account invariant:
//!   re-linking the same pair is idempotent, linking to a different account is a
//!   conflict. It uses `INSERT ... ON CONFLICT DO NOTHING` plus a conditional read
//!   (exactly as Postgres does), so a concurrent linker never raises a bare
//!   unique-violation error and the follow-up read decides idempotent re-link vs.
//!   conflict without a credential-existence oracle.
//! - When bound to a [`SqliteUnitOfWork`](crate::repository::SqliteUnitOfWork), a
//!   create-user-then-link-identity workflow runs inside one shared SQLite
//!   transaction, so account creation is atomic (all-or-nothing) at the database,
//!   not the application, level.
//!
//! Dialect specifics kept behind this file: `?` positional placeholders, JSON
//! `metadata` stored as `TEXT` (serialized at the boundary), domain timestamps as
//! `INTEGER` epoch millis, and `AccountState` / the credential provider stored as
//! their stable lowercase tokens. SQLite has no `SELECT ... FOR UPDATE`; a pooled
//! update runs inside a `BEGIN IMMEDIATE` transaction that takes the single writer
//! slot up front, serializing the read-then-write decision like the Postgres row
//! lock.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::identity::{
    AccountState, AuthCredential, AuthIdentity, AuthProvider, CustomId, DeviceId, DisplayName,
    EmailAddress, PasswordVerifier, User, UserId, UserMetadata, Username,
};
use crate::repository::{AuthIdentityRepository, UserRepository};
use crate::time::{Clock, TimestampMillis};

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- users SQL --------------------------------------------------------------

const GET_USER_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users WHERE id = ?";

const GET_USER_BY_USERNAME_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users WHERE username = ?";

const INSERT_USER_SQL: &str = "\
INSERT INTO users (id, username, display_name, metadata, state, created_at, updated_at) \
VALUES (?, ?, ?, ?, ?, ?, ?)";

/// Read a user's `created_at` for the update immutability check. There is no
/// `FOR UPDATE` in SQLite; the enclosing `BEGIN IMMEDIATE` transaction serializes
/// the writer, so a plain read is sufficient.
const SELECT_USER_CREATED_SQL: &str = "SELECT created_at FROM users WHERE id = ?";

/// Administrative account listing with substring filter + keyset-free paging.
/// The filter is LIKE-escaped by the caller (see `like_pattern`).
const LIST_USERS_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users \
WHERE (? = '' OR id LIKE ? ESCAPE '\\' OR username LIKE ? ESCAPE '\\') \
ORDER BY username ASC, id ASC \
LIMIT ? OFFSET ?";

/// Total accounts matching the same filter as [`LIST_USERS_SQL`].
const COUNT_USERS_SQL: &str = "\
SELECT COUNT(*) AS total \
FROM users \
WHERE (? = '' OR id LIKE ? ESCAPE '\\' OR username LIKE ? ESCAPE '\\')";

const UPDATE_USER_SQL: &str = "\
UPDATE users \
SET username = ?, display_name = ?, metadata = ?, state = ?, updated_at = ? \
WHERE id = ?";

// --- auth_identities SQL ----------------------------------------------------

const GET_IDENTITY_SQL: &str = "\
SELECT provider, external_id, user_id, created_at, updated_at, password_verifier \
FROM auth_identities WHERE provider = ? AND external_id = ?";

const LIST_IDENTITIES_SQL: &str = "\
SELECT provider, external_id, user_id, created_at, updated_at, password_verifier \
FROM auth_identities WHERE user_id = ? \
ORDER BY provider ASC, created_at ASC";

/// Insert a link, or do nothing if the credential is already linked. `DO NOTHING`
/// (rather than a plain insert) means a concurrent linker never raises a unique
/// violation, so the transaction stays valid and the follow-up read can decide
/// idempotent re-link vs. conflict — matching the in-memory/Postgres single-lock
/// semantics without a credential-existence oracle.
const INSERT_IDENTITY_SQL: &str = "\
INSERT INTO auth_identities (provider, external_id, user_id, created_at, updated_at, password_verifier) \
VALUES (?, ?, ?, ?, ?, ?) \
ON CONFLICT (provider, external_id) DO NOTHING";

const DELETE_IDENTITY_SQL: &str = "\
DELETE FROM auth_identities WHERE provider = ? AND external_id = ?";

const SCOPED_DELETE_IDENTITY_SQL: &str = "\
DELETE FROM auth_identities WHERE provider = ? AND external_id = ? AND user_id = ?";

const COUNT_USER_IDENTITIES_SQL: &str = "SELECT COUNT(*) FROM auth_identities WHERE user_id = ?";

const INSERT_IDENTITY_CHANGE_OUTBOX_SQL: &str = "\
INSERT INTO identity_change_outbox \
    (user_id, event_type, provider, external_id_redacted, password_verifier, created_at) \
VALUES (?, 'credential_unlinked', ?, '[redacted]', NULL, ?)";

// --- mapping helpers --------------------------------------------------------

fn account_state_from_str(value: &str) -> AppResult<AccountState> {
    match value {
        "active" => Ok(AccountState::Active),
        "disabled" => Ok(AccountState::Disabled),
        "tombstoned" => Ok(AccountState::Tombstoned),
        other => Err(AppError::internal(format!(
            "invalid account state `{other}` in users row"
        ))),
    }
}

/// Split an [`AuthCredential`] into its `(provider, external_id)` columns.
fn credential_columns(credential: &AuthCredential) -> (&'static str, &str) {
    match credential {
        AuthCredential::Device(id) => (AuthProvider::Device.as_str(), id.as_str()),
        AuthCredential::Custom(id) => (AuthProvider::Custom.as_str(), id.as_str()),
        AuthCredential::Email(email) => (AuthProvider::Email.as_str(), email.as_str()),
    }
}

/// Rebuild an [`AuthCredential`] from its `(provider, external_id)` columns.
fn credential_from_columns(provider: &str, external_id: &str) -> AppResult<AuthCredential> {
    match provider {
        "device" => Ok(AuthCredential::Device(DeviceId::new(external_id)?)),
        "custom" => Ok(AuthCredential::Custom(CustomId::new(external_id)?)),
        "email" => Ok(AuthCredential::Email(EmailAddress::new(external_id)?)),
        other => Err(AppError::internal(format!(
            "invalid auth provider `{other}` in auth_identities row"
        ))),
    }
}

/// Serialize optional user metadata to the `TEXT` column value.
fn metadata_to_text(metadata: Option<&UserMetadata>) -> AppResult<Option<String>> {
    metadata
        .map(|m| {
            serde_json::to_string(m.as_json()).map_err(|e| {
                AppError::internal("failed to encode user metadata").with_detail(e.to_string())
            })
        })
        .transpose()
}

/// Parse the optional `TEXT` metadata column back into a domain [`UserMetadata`].
fn metadata_from_text(text: Option<String>) -> AppResult<Option<UserMetadata>> {
    text.map(|raw| {
        let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
            AppError::internal("failed to decode stored user metadata").with_detail(e.to_string())
        })?;
        UserMetadata::new(value)
    })
    .transpose()
}

fn row_to_user(row: &SqliteRow) -> AppResult<User> {
    let id: String = get(row, "id")?;
    let username: String = get(row, "username")?;
    let display_name: Option<String> = get(row, "display_name")?;
    let metadata: Option<String> = get(row, "metadata")?;
    let state: String = get(row, "state")?;
    let created_at: i64 = get(row, "created_at")?;
    let updated_at: i64 = get(row, "updated_at")?;

    let display_name = display_name.map(DisplayName::new).transpose()?;
    let metadata = metadata_from_text(metadata)?;
    User::new(
        UserId::new(id)?,
        Username::new(username)?,
        display_name,
        metadata,
        millis_to_ts(created_at)?,
        millis_to_ts(updated_at)?,
        account_state_from_str(&state)?,
    )
}

fn row_to_identity(row: &SqliteRow) -> AppResult<AuthIdentity> {
    let provider: String = get(row, "provider")?;
    let external_id: String = get(row, "external_id")?;
    let user_id: String = get(row, "user_id")?;
    let created_at: i64 = get(row, "created_at")?;
    let updated_at: i64 = get(row, "updated_at")?;
    let password_verifier: Option<String> = get(row, "password_verifier")?;

    let identity = AuthIdentity::new(
        credential_from_columns(&provider, &external_id)?,
        UserId::new(user_id)?,
        millis_to_ts(created_at)?,
        millis_to_ts(updated_at)?,
    )?;
    match password_verifier {
        Some(verifier) => identity.with_password_verifier(PasswordVerifier::new(verifier)?),
        None => Ok(identity),
    }
}

// --- user repository --------------------------------------------------------

/// SQLite [`UserRepository`].
pub struct SqliteUserRepository {
    executor: SqliteExecutor,
}

impl SqliteUserRepository {
    /// Bind a user repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl UserRepository for SqliteUserRepository {
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_user_conn(&mut conn, id).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_user_conn(&mut *tx, id).await
            }
        }
    }

    async fn list_users(
        &self,
        filter: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> AppResult<crate::repository::identity::UserPage> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_users_conn(&mut conn, filter, limit, offset).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_users_conn(&mut *tx, filter, limit, offset).await
            }
        }
    }

    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_user_by_username_conn(&mut conn, username).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_user_by_username_conn(&mut *tx, username).await
            }
        }
    }

    async fn create_user(&self, user: User) -> AppResult<User> {
        // A single INSERT: the primary key (id) and unique username index enforce
        // uniqueness, so autocommit is correct and atomic on the pool.
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                create_user_conn(&mut conn, user).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                create_user_conn(&mut *tx, user).await
            }
        }
    }

    async fn update_user(&self, user: User) -> AppResult<User> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                // `BEGIN IMMEDIATE` takes the single writer slot up front so the
                // read-then-write decision is serialized like the Postgres row
                // lock.
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match update_user_conn(&mut tx, user).await {
                    Ok(user) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(user)
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
                update_user_conn(&mut *tx, user).await
            }
        }
    }

    async fn set_user_state(
        &self,
        id: &UserId,
        state: AccountState,
        updated_at: TimestampMillis,
    ) -> AppResult<User> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match set_user_state_conn(&mut tx, id, state, updated_at).await {
                    Ok(user) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(user)
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
                set_user_state_conn(&mut *tx, id, state, updated_at).await
            }
        }
    }
}

async fn get_user_conn(conn: &mut SqliteConnection, id: &UserId) -> AppResult<Option<User>> {
    let row = sqlx::query(GET_USER_SQL)
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_user).transpose()
}

/// Escape a raw substring filter into a `LIKE ... ESCAPE '\'` pattern.
fn like_pattern(filter: Option<&str>) -> (String, String) {
    match filter {
        None | Some("") => (String::new(), String::new()),
        Some(raw) => {
            let escaped = raw
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            (raw.to_string(), format!("%{escaped}%"))
        }
    }
}

async fn list_users_conn(
    conn: &mut SqliteConnection,
    filter: Option<&str>,
    limit: usize,
    offset: usize,
) -> AppResult<crate::repository::identity::UserPage> {
    if limit == 0 {
        return Err(AppError::validation("list limit must be greater than zero"));
    }
    let (raw, pattern) = like_pattern(filter);
    let rows = sqlx::query(LIST_USERS_SQL)
        .bind(raw.as_str())
        .bind(pattern.as_str())
        .bind(pattern.as_str())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let users = rows
        .iter()
        .map(row_to_user)
        .collect::<AppResult<Vec<_>>>()?;
    let total_row = sqlx::query(COUNT_USERS_SQL)
        .bind(raw.as_str())
        .bind(pattern.as_str())
        .bind(pattern.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let total: i64 = get(&total_row, "total")?;
    Ok(crate::repository::identity::UserPage {
        users,
        total: u64::try_from(total).unwrap_or_default(),
    })
}

async fn get_user_by_username_conn(
    conn: &mut SqliteConnection,
    username: &Username,
) -> AppResult<Option<User>> {
    let row = sqlx::query(GET_USER_BY_USERNAME_SQL)
        .bind(username.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_user).transpose()
}

async fn create_user_conn(conn: &mut SqliteConnection, user: User) -> AppResult<User> {
    let metadata = metadata_to_text(user.metadata.as_ref())?;
    sqlx::query(INSERT_USER_SQL)
        .bind(user.id.as_str())
        .bind(user.username.as_str())
        .bind(user.display_name.as_ref().map(DisplayName::as_str))
        .bind(metadata)
        .bind(user.state.as_str())
        .bind(ts_to_millis(user.created_at)?)
        .bind(ts_to_millis(user.updated_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(user)
}

async fn update_user_conn(conn: &mut SqliteConnection, user: User) -> AppResult<User> {
    let existing = sqlx::query(SELECT_USER_CREATED_SQL)
        .bind(user.id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let Some(row) = existing else {
        return Err(AppError::not_found("user does not exist"));
    };
    let existing_created: i64 = get(&row, "created_at")?;
    // `created_at` is immutable; refuse to rewrite account history.
    if millis_to_ts(existing_created)? != user.created_at {
        return Err(AppError::conflict("user created_at is immutable"));
    }

    let metadata = metadata_to_text(user.metadata.as_ref())?;
    sqlx::query(UPDATE_USER_SQL)
        .bind(user.username.as_str())
        .bind(user.display_name.as_ref().map(DisplayName::as_str))
        .bind(metadata)
        .bind(user.state.as_str())
        .bind(ts_to_millis(user.updated_at)?)
        .bind(user.id.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(user)
}

async fn set_user_state_conn(
    conn: &mut SqliteConnection,
    id: &UserId,
    state: AccountState,
    updated_at: TimestampMillis,
) -> AppResult<User> {
    let row = sqlx::query(GET_USER_SQL)
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?
        .ok_or_else(|| AppError::not_found("user does not exist"))?;
    let existing = row_to_user(&row)?;

    // Rebuild through `User::new` exactly like the in-memory impl, so an invalid
    // `updated_at < created_at` is rejected as a validation error before any write.
    let updated = User::new(
        existing.id.clone(),
        existing.username.clone(),
        existing.display_name.clone(),
        existing.metadata.clone(),
        existing.created_at,
        updated_at,
        state,
    )?;

    let metadata = metadata_to_text(updated.metadata.as_ref())?;
    sqlx::query(UPDATE_USER_SQL)
        .bind(updated.username.as_str())
        .bind(updated.display_name.as_ref().map(DisplayName::as_str))
        .bind(metadata)
        .bind(updated.state.as_str())
        .bind(ts_to_millis(updated.updated_at)?)
        .bind(updated.id.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(updated)
}

// --- auth identity repository -----------------------------------------------

/// SQLite [`AuthIdentityRepository`].
pub struct SqliteAuthIdentityRepository {
    executor: SqliteExecutor,
}

impl SqliteAuthIdentityRepository {
    /// Bind an auth-identity repository to an execution handle.
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl AuthIdentityRepository for SqliteAuthIdentityRepository {
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_identity_conn(&mut conn, credential).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_identity_conn(&mut *tx, credential).await
            }
        }
    }

    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_identities_conn(&mut conn, user_id).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_identities_conn(&mut *tx, user_id).await
            }
        }
    }

    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match link_identity_conn(&mut tx, identity).await {
                    Ok(identity) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(identity)
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
                link_identity_conn(&mut *tx, identity).await
            }
        }
    }

    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                unlink_identity_conn(&mut conn, credential).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                unlink_identity_conn(&mut *tx, credential).await
            }
        }
    }

    async fn unlink_auth_identity_for_user(
        &self,
        user_id: &UserId,
        credential: &AuthCredential,
    ) -> AppResult<crate::repository::UnlinkResult> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match scoped_unlink_identity_conn(&mut tx, user_id, credential).await {
                    Ok(result) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(result)
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
                scoped_unlink_identity_conn(&mut *tx, user_id, credential).await
            }
        }
    }
}

async fn get_identity_conn(
    conn: &mut SqliteConnection,
    credential: &AuthCredential,
) -> AppResult<Option<AuthIdentity>> {
    let (provider, external_id) = credential_columns(credential);
    let row = sqlx::query(GET_IDENTITY_SQL)
        .bind(provider)
        .bind(external_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_identity).transpose()
}

async fn list_identities_conn(
    conn: &mut SqliteConnection,
    user_id: &UserId,
) -> AppResult<Vec<AuthIdentity>> {
    let rows = sqlx::query(LIST_IDENTITIES_SQL)
        .bind(user_id.as_str())
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_identity).collect()
}

async fn link_identity_conn(
    conn: &mut SqliteConnection,
    identity: AuthIdentity,
) -> AppResult<AuthIdentity> {
    let (provider, external_id) = credential_columns(&identity.credential);
    let inserted = sqlx::query(INSERT_IDENTITY_SQL)
        .bind(provider)
        .bind(external_id)
        .bind(identity.user_id.as_str())
        .bind(ts_to_millis(identity.created_at)?)
        .bind(ts_to_millis(identity.updated_at)?)
        .bind(identity.password_verifier().map(PasswordVerifier::encoded))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    // A fresh link inserted exactly one row.
    if inserted.rows_affected() == 1 {
        return Ok(identity);
    }

    // The credential is already linked. Re-linking the same account is idempotent
    // (return the stored row); a different account is a conflict. The conflict
    // message is identical regardless of which account owns it, so it never
    // reveals whether a specific credential/account pair exists.
    let row = sqlx::query(GET_IDENTITY_SQL)
        .bind(provider)
        .bind(external_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let existing = row_to_identity(&row)?;
    if existing.user_id != identity.user_id {
        return Err(AppError::conflict(
            "credential already linked to another account",
        ));
    }
    Ok(existing)
}

async fn unlink_identity_conn(
    conn: &mut SqliteConnection,
    credential: &AuthCredential,
) -> AppResult<()> {
    let (provider, external_id) = credential_columns(credential);
    sqlx::query(DELETE_IDENTITY_SQL)
        .bind(provider)
        .bind(external_id)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn scoped_unlink_identity_conn(
    conn: &mut SqliteConnection,
    user_id: &UserId,
    credential: &AuthCredential,
) -> AppResult<crate::repository::UnlinkResult> {
    let (provider, external_id) = credential_columns(credential);
    let owned: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM auth_identities WHERE provider = ? AND external_id = ? AND user_id = ?",
    )
    .bind(provider)
    .bind(external_id)
    .bind(user_id.as_str())
    .fetch_optional(&mut *conn)
    .await
    .map_err(db_err)?;
    if owned.is_none() {
        return Ok(crate::repository::UnlinkResult::NotOwned);
    }
    let count: i64 = sqlx::query_scalar(COUNT_USER_IDENTITIES_SQL)
        .bind(user_id.as_str())
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    if count <= 1 {
        return Ok(crate::repository::UnlinkResult::LastCredential);
    }
    sqlx::query(SCOPED_DELETE_IDENTITY_SQL)
        .bind(provider)
        .bind(external_id)
        .bind(user_id.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(INSERT_IDENTITY_CHANGE_OUTBOX_SQL)
        .bind(user_id.as_str())
        .bind(provider)
        .bind(i64::try_from(crate::time::SystemClock.now().unix_millis()).unwrap_or(i64::MAX))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(crate::repository::UnlinkResult::Unlinked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;
    use crate::repository::{SqliteDatabase, UnlinkResult};

    #[test]
    fn account_state_tokens_round_trip() {
        for state in [
            AccountState::Active,
            AccountState::Disabled,
            AccountState::Tombstoned,
        ] {
            assert_eq!(
                account_state_from_str(state.as_str()).expect("known state"),
                state
            );
        }
        assert!(account_state_from_str("bogus").is_err());
    }

    #[test]
    fn credential_columns_round_trip() {
        let device = AuthCredential::Device(DeviceId::new("d-1").expect("device"));
        let (provider, external) = credential_columns(&device);
        assert_eq!(provider, "device");
        assert_eq!(
            credential_from_columns(provider, external).expect("rebuild"),
            device
        );

        let custom = AuthCredential::Custom(CustomId::new("c-1").expect("custom"));
        let (provider, external) = credential_columns(&custom);
        assert_eq!(provider, "custom");
        assert_eq!(
            credential_from_columns(provider, external).expect("rebuild"),
            custom
        );

        assert!(credential_from_columns("email", "x").is_err());
    }

    #[test]
    fn metadata_text_round_trips() {
        let metadata = UserMetadata::new(serde_json::json!({"level": 3})).expect("metadata");
        let text = metadata_to_text(Some(&metadata)).expect("encode");
        assert_eq!(metadata_from_text(text).expect("decode"), Some(metadata));
        assert_eq!(metadata_to_text(None).expect("encode none"), None);
        assert_eq!(metadata_from_text(None).expect("decode none"), None);
    }

    #[tokio::test]
    async fn scoped_unlink_rolls_back_or_commits_its_redacted_outbox_audit_with_the_identity() {
        let db = SqliteDatabase::connect(&DatabaseConfig {
            url: Some("sqlite::memory:".to_owned()),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect");
        let user = UserId::new("unlink-user").expect("user");
        let removed =
            AuthCredential::Email(EmailAddress::new("unlink@example.test").expect("email"));
        let retained = AuthCredential::Device(DeviceId::new("unlink-device").expect("device"));
        let email_identity = AuthIdentity::new(
            removed.clone(),
            user.clone(),
            TimestampMillis::from_unix_millis(1),
            TimestampMillis::from_unix_millis(1),
        )
        .expect("email identity")
        .with_password_verifier(
            PasswordVerifier::new("test-verifier".to_owned()).expect("verifier"),
        )
        .expect("attach verifier");
        db.auth_identity_repository()
            .link_auth_identity(email_identity)
            .await
            .expect("link email");
        db.auth_identity_repository()
            .link_auth_identity(
                AuthIdentity::new(
                    retained,
                    user.clone(),
                    TimestampMillis::from_unix_millis(1),
                    TimestampMillis::from_unix_millis(1),
                )
                .expect("device identity"),
            )
            .await
            .expect("link device");

        let tx = db.begin().await.expect("begin");
        assert_eq!(
            tx.auth_identity_repository()
                .unlink_auth_identity_for_user(&user, &removed)
                .await
                .expect("unlink in transaction"),
            UnlinkResult::Unlinked
        );
        tx.rollback().await.expect("rollback");
        assert!(
            db.auth_identity_repository()
                .get_auth_identity(&removed)
                .await
                .expect("read after rollback")
                .is_some()
        );
        let rolled_back_audits: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM identity_change_outbox WHERE user_id = ?")
                .bind(user.as_str())
                .fetch_one(&db.pool)
                .await
                .expect("query audit outbox");
        assert_eq!(rolled_back_audits, 0);

        assert_eq!(
            db.auth_identity_repository()
                .unlink_auth_identity_for_user(&user, &removed)
                .await
                .expect("unlink"),
            UnlinkResult::Unlinked
        );
        assert!(
            db.auth_identity_repository()
                .get_auth_identity(&removed)
                .await
                .expect("read after unlink")
                .is_none()
        );
        let event = sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT event_type, provider, external_id_redacted, password_verifier \
             FROM identity_change_outbox WHERE user_id = ?",
        )
        .bind(user.as_str())
        .fetch_one(&db.pool)
        .await
        .expect("redacted outbox record");
        assert_eq!(event.0, "credential_unlinked");
        assert_eq!(event.1, "email");
        assert_eq!(event.2, "[redacted]");
        assert!(event.3.is_none(), "outbox must never retain a verifier");
    }
}
