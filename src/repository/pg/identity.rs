//! Postgres identity repositories.
//!
//! [`PgUserRepository`] and [`PgAuthIdentityRepository`] are the durable backends
//! for [`UserRepository`](crate::repository::UserRepository) and
//! [`AuthIdentityRepository`](crate::repository::AuthIdentityRepository). They
//! reproduce the in-memory reference impls exactly:
//!
//! - Account ids and usernames are unique (the composite `(provider,
//!   external_id)` is unique for credentials), enforced by database constraints
//!   rather than an application lock. A concurrent duplicate surfaces as
//!   `23505` → [`Conflict`](crate::error::ErrorCategory::Conflict), and the error
//!   detail never echoes the colliding value, so it is not a credential-existence
//!   oracle.
//! - `created_at` is immutable across updates.
//! - `link_auth_identity` keeps the one-credential-to-one-account invariant:
//!   re-linking the same pair is idempotent, linking to a different account is a
//!   conflict.
//! - When bound to a [`PgUnitOfWork`](crate::repository::PgUnitOfWork), a
//!   create-user-then-link-identity workflow runs inside one shared transaction,
//!   so account creation is atomic (all-or-nothing) at the database, not the
//!   application, level.
//!
//! `AccountState` and the credential provider are stored as small, stable text
//! tokens (`AccountState::as_str` / `AuthProvider::as_str`), so the schema is
//! self-describing and forward-compatible with new variants.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};

use crate::error::{AppError, AppResult};
use crate::identity::{
    AccountState, AuthCredential, AuthIdentity, AuthProvider, CustomId, DeviceId, DisplayName,
    EmailAddress, PasswordVerifier, User, UserId, UserMetadata, Username,
};
use crate::repository::{AuthIdentityRepository, UserRepository};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- users SQL --------------------------------------------------------------

const GET_USER_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users WHERE id = $1";

const GET_USER_BY_USERNAME_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users WHERE username = $1";

const INSERT_USER_SQL: &str = "\
INSERT INTO users (id, username, display_name, metadata, state, created_at, updated_at) \
VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// Administrative account listing with substring filter (LIKE-escaped by the
/// caller, see `like_pattern`) and offset paging.
const LIST_USERS_SQL: &str = "\
SELECT id, username, display_name, metadata, state, created_at, updated_at \
FROM users \
WHERE ($1 = '' OR id LIKE $2 ESCAPE '\\' OR username LIKE $2 ESCAPE '\\') \
ORDER BY username ASC, id ASC \
LIMIT $3 OFFSET $4";

/// Total accounts matching the same filter as [`LIST_USERS_SQL`].
const COUNT_USERS_SQL: &str = "\
SELECT COUNT(*) AS total \
FROM users \
WHERE ($1 = '' OR id LIKE $2 ESCAPE '\\' OR username LIKE $2 ESCAPE '\\')";

/// Lock a user row for an update decision (returns `created_at` for the
/// immutability check).
const LOCK_USER_SQL: &str = "SELECT created_at FROM users WHERE id = $1 FOR UPDATE";

const UPDATE_USER_SQL: &str = "\
UPDATE users \
SET username = $2, display_name = $3, metadata = $4, state = $5, updated_at = $6 \
WHERE id = $1";

// --- auth_identities SQL ----------------------------------------------------

const GET_IDENTITY_SQL: &str = "\
SELECT provider, external_id, user_id, created_at, updated_at, password_verifier \
FROM auth_identities WHERE provider = $1 AND external_id = $2";

const LIST_IDENTITIES_SQL: &str = "\
SELECT provider, external_id, user_id, created_at, updated_at, password_verifier \
FROM auth_identities WHERE user_id = $1 \
ORDER BY provider ASC, created_at ASC";

/// Insert a link, or do nothing if the credential is already linked. `DO NOTHING`
/// (rather than a plain insert) means a concurrent linker never raises `23505`,
/// so the transaction stays valid and the follow-up read can decide idempotent
/// re-link vs. conflict — matching the in-memory single-lock semantics without a
/// credential-existence oracle.
const INSERT_IDENTITY_SQL: &str = "\
INSERT INTO auth_identities (provider, external_id, user_id, created_at, updated_at, password_verifier) \
VALUES ($1, $2, $3, $4, $5, $6) \
ON CONFLICT (provider, external_id) DO NOTHING";

const DELETE_IDENTITY_SQL: &str = "\
DELETE FROM auth_identities WHERE provider = $1 AND external_id = $2";

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

fn row_to_user(row: &PgRow) -> AppResult<User> {
    let id: String = get(row, "id")?;
    let username: String = get(row, "username")?;
    let display_name: Option<String> = get(row, "display_name")?;
    let metadata: Option<serde_json::Value> = get(row, "metadata")?;
    let state: String = get(row, "state")?;
    let created_at: i64 = get(row, "created_at")?;
    let updated_at: i64 = get(row, "updated_at")?;

    let display_name = display_name.map(DisplayName::new).transpose()?;
    let metadata = metadata.map(UserMetadata::new).transpose()?;
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

fn row_to_identity(row: &PgRow) -> AppResult<AuthIdentity> {
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

/// Postgres [`UserRepository`].
pub struct PgUserRepository {
    executor: PgExecutor,
}

impl PgUserRepository {
    /// Bind a user repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl UserRepository for PgUserRepository {
    async fn get_user(&self, id: &UserId) -> AppResult<Option<User>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_user_conn(&mut conn, id).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_user_conn(&mut *tx, id).await
            }
        }
    }

    async fn get_user_by_username(&self, username: &Username) -> AppResult<Option<User>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_user_by_username_conn(&mut conn, username).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_user_by_username_conn(&mut *tx, username).await
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
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_users_conn(&mut conn, filter, limit, offset).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_users_conn(&mut *tx, filter, limit, offset).await
            }
        }
    }

    async fn create_user(&self, user: User) -> AppResult<User> {
        // A single INSERT: the primary key (id) and unique username constraints
        // enforce uniqueness, so autocommit is correct and atomic on the pool.
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                create_user_conn(&mut conn, user).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                create_user_conn(&mut *tx, user).await
            }
        }
    }

    async fn update_user(&self, user: User) -> AppResult<User> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
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
            PgExecutor::Tx(cell) => {
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
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
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
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                set_user_state_conn(&mut *tx, id, state, updated_at).await
            }
        }
    }
}

async fn get_user_conn(conn: &mut PgConnection, id: &UserId) -> AppResult<Option<User>> {
    let row = sqlx::query(GET_USER_SQL)
        .bind(id.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_user).transpose()
}

async fn get_user_by_username_conn(
    conn: &mut PgConnection,
    username: &Username,
) -> AppResult<Option<User>> {
    let row = sqlx::query(GET_USER_BY_USERNAME_SQL)
        .bind(username.as_str())
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
    conn: &mut PgConnection,
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
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let total: i64 = get(&total_row, "total")?;
    Ok(crate::repository::identity::UserPage {
        users,
        total: u64::try_from(total).unwrap_or_default(),
    })
}

async fn create_user_conn(conn: &mut PgConnection, user: User) -> AppResult<User> {
    let metadata = user
        .metadata
        .as_ref()
        .map(|m| sqlx::types::Json(m.as_json().clone()));
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

async fn update_user_conn(conn: &mut PgConnection, user: User) -> AppResult<User> {
    let existing = sqlx::query(LOCK_USER_SQL)
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

    let metadata = user
        .metadata
        .as_ref()
        .map(|m| sqlx::types::Json(m.as_json().clone()));
    sqlx::query(UPDATE_USER_SQL)
        .bind(user.id.as_str())
        .bind(user.username.as_str())
        .bind(user.display_name.as_ref().map(DisplayName::as_str))
        .bind(metadata)
        .bind(user.state.as_str())
        .bind(ts_to_millis(user.updated_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(user)
}

async fn set_user_state_conn(
    conn: &mut PgConnection,
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

    let metadata = updated
        .metadata
        .as_ref()
        .map(|m| sqlx::types::Json(m.as_json().clone()));
    sqlx::query(UPDATE_USER_SQL)
        .bind(updated.id.as_str())
        .bind(updated.username.as_str())
        .bind(updated.display_name.as_ref().map(DisplayName::as_str))
        .bind(metadata)
        .bind(updated.state.as_str())
        .bind(ts_to_millis(updated.updated_at)?)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(updated)
}

// --- auth identity repository -----------------------------------------------

/// Postgres [`AuthIdentityRepository`].
pub struct PgAuthIdentityRepository {
    executor: PgExecutor,
}

impl PgAuthIdentityRepository {
    /// Bind an auth-identity repository to an execution handle.
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl AuthIdentityRepository for PgAuthIdentityRepository {
    async fn get_auth_identity(
        &self,
        credential: &AuthCredential,
    ) -> AppResult<Option<AuthIdentity>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                get_identity_conn(&mut conn, credential).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                get_identity_conn(&mut *tx, credential).await
            }
        }
    }

    async fn list_auth_identities(&self, user_id: &UserId) -> AppResult<Vec<AuthIdentity>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_identities_conn(&mut conn, user_id).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_identities_conn(&mut *tx, user_id).await
            }
        }
    }

    async fn link_auth_identity(&self, identity: AuthIdentity) -> AppResult<AuthIdentity> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
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
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                link_identity_conn(&mut *tx, identity).await
            }
        }
    }

    async fn unlink_auth_identity(&self, credential: &AuthCredential) -> AppResult<()> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                unlink_identity_conn(&mut conn, credential).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                unlink_identity_conn(&mut *tx, credential).await
            }
        }
    }
}

async fn get_identity_conn(
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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
    conn: &mut PgConnection,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
