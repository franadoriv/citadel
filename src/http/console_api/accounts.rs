//! Console Accounts administration.
//!
//! Operator-scope account management over the node's real identity
//! repositories (in-memory, SQLite, or Postgres — the same seam player auth
//! uses):
//!
//! - `GET /console/v1/accounts` — paged listing with an id/username substring
//!   filter (includes disabled and tombstoned accounts).
//! - `POST /console/v1/accounts` — create an account (admin, audited).
//! - `GET /console/v1/accounts/{id}` — detail: profile, lifecycle state, and
//!   linked auth identities.
//! - `PUT /console/v1/accounts/{id}` — edit username/display name/metadata
//!   (admin, audited).
//! - `POST /console/v1/accounts/{id}/ban` / `/unban` — disable/re-enable the
//!   account (admin, audited). A ban also revokes the account's sessions, so
//!   a live token stops working immediately.
//! - `DELETE /console/v1/accounts/{id}` — logical delete: tombstone the
//!   account, unlink every credential, revoke sessions (admin, audited).
//! - `GET /console/v1/accounts/{id}/export` — the full account as JSON.
//!
//! Bans map onto the existing [`AccountState`] lifecycle: `Disabled` accounts
//! fail authentication with the uniform auth error (no oracle), which is
//! already enforced by the authentication service.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Json, extract::rejection::PathRejection};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::{AppError, AppResult};
use crate::http::error::ApiError;
use crate::identity::{
    AccountState, AuthCredential, AuthIdentity, DisplayName, User, UserMetadata, Username,
};
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::session::RevocationReason;
use crate::storage::UserId;
use crate::time::{Clock, SystemClock};

/// The Accounts section route.
pub const ACCOUNTS_PATH: &str = "/console/v1/accounts";

/// Single-account route pattern.
pub const ACCOUNT_DETAIL_PATH: &str = "/console/v1/accounts/:id";

/// Ban / unban / export route patterns.
pub const ACCOUNT_BAN_PATH: &str = "/console/v1/accounts/:id/ban";
/// See [`ACCOUNT_BAN_PATH`].
pub const ACCOUNT_UNBAN_PATH: &str = "/console/v1/accounts/:id/unban";
/// Full-account JSON export route pattern.
pub const ACCOUNT_EXPORT_PATH: &str = "/console/v1/accounts/:id/export";

/// Wallet panel route pattern.
pub const ACCOUNT_WALLET_PATH: &str = "/console/v1/accounts/:id/wallet";
/// Friends panel route pattern.
pub const ACCOUNT_FRIENDS_PATH: &str = "/console/v1/accounts/:id/friends";
/// Single-friend route pattern.
pub const ACCOUNT_FRIEND_PATH: &str = "/console/v1/accounts/:id/friends/:other";

/// Default listing page size.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one listing page.
const MAX_LIMIT: usize = 200;

/// Accepted query parameters for the listing route.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Substring filter over account id and username.
    pub filter: Option<String>,
    /// Page size (default 50, capped at 200).
    pub limit: Option<usize>,
    /// Page offset.
    pub offset: Option<usize>,
}

/// One account row in the listing.
#[derive(Debug, Clone, Serialize)]
pub struct AccountRow {
    /// Stable account id.
    pub id: String,
    /// Unique username handle.
    pub username: String,
    /// Human-facing display name, if set.
    pub display_name: Option<String>,
    /// Lifecycle state: `active`, `disabled` (banned), or `tombstoned`.
    pub state: &'static str,
    /// Creation time (Unix millis).
    pub created_at_unix_ms: u64,
    /// Last-update time (Unix millis).
    pub updated_at_unix_ms: u64,
}

impl AccountRow {
    fn from_user(user: &User) -> Self {
        Self {
            id: user.id.as_str().to_string(),
            username: user.username.as_str().to_string(),
            display_name: user.display_name.as_ref().map(|d| d.as_str().to_string()),
            state: user.state.as_str(),
            created_at_unix_ms: user.created_at.unix_millis(),
            updated_at_unix_ms: user.updated_at.unix_millis(),
        }
    }
}

/// The JSON response for the listing route.
#[derive(Debug, Clone, Serialize)]
pub struct AccountsPage {
    /// The page of accounts, username-ordered.
    pub items: Vec<AccountRow>,
    /// Total accounts matching the filter.
    pub total: u64,
}

/// One linked credential in the account detail/export.
#[derive(Debug, Clone, Serialize)]
pub struct LinkedIdentity {
    /// Credential provider: `device` or `custom`.
    pub provider: &'static str,
    /// The external credential id (operator surface: visible by design, like
    /// the Nakama console's device-id list).
    pub external_id: String,
    /// Link creation time (Unix millis).
    pub created_at_unix_ms: u64,
}

impl LinkedIdentity {
    fn from_identity(identity: &AuthIdentity) -> Self {
        let external_id = match &identity.credential {
            AuthCredential::Device(id) => id.as_str().to_string(),
            AuthCredential::Custom(id) => id.as_str().to_string(),
            AuthCredential::Email(email) => email.as_str().to_string(),
        };
        Self {
            provider: identity.provider().as_str(),
            external_id,
            created_at_unix_ms: identity.created_at.unix_millis(),
        }
    }
}

/// The JSON response for the detail and export routes.
#[derive(Debug, Clone, Serialize)]
pub struct AccountDetail {
    /// The account row.
    #[serde(flatten)]
    pub row: AccountRow,
    /// Account metadata (a JSON object), if set.
    pub metadata: Option<serde_json::Value>,
    /// Every credential linked to the account.
    pub identities: Vec<LinkedIdentity>,
}

/// The JSON body accepted by account creation.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBody {
    /// Unique username handle (required).
    pub username: String,
    /// Optional display name.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Optional metadata (a JSON object).
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// The JSON body accepted by account edit. Absent fields stay unchanged; an
/// empty-string `display_name` clears it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateBody {
    /// New username handle.
    #[serde(default)]
    pub username: Option<String>,
    /// New display name (`""` clears it).
    #[serde(default)]
    pub display_name: Option<String>,
    /// Replacement metadata object.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// `GET /console/v1/accounts`: paged, filtered account listing.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(params): Query<ListParams>,
) -> Result<Json<AccountsPage>, ApiError> {
    app.metrics().record_http_request();
    let page = app
        .backend()
        .user_repository()
        .list_users(
            params.filter.as_deref().filter(|f| !f.is_empty()),
            params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
            params.offset.unwrap_or(0),
        )
        .await?;
    Ok(Json(AccountsPage {
        items: page.users.iter().map(AccountRow::from_user).collect(),
        total: page.total,
    }))
}

/// `POST /console/v1/accounts`: create an account (admin).
pub(super) async fn create_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Result<(StatusCode, Json<AccountRow>), ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = parse_body(body)?;
    let now = SystemClock.now();
    let user = User::new(
        mint_user_id()?,
        Username::new(body.username)?,
        body.display_name.map(DisplayName::new).transpose()?,
        body.metadata.map(UserMetadata::new).transpose()?,
        now,
        now,
        AccountState::Active,
    )?;
    let created = app.backend().user_repository().create_user(user).await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.create",
        format!("account {}", created.id.as_str()),
        format!("created username {}", created.username.as_str()),
    ));
    Ok((StatusCode::CREATED, Json(AccountRow::from_user(&created))))
}

/// `GET /console/v1/accounts/{id}`: profile + linked identities.
pub(super) async fn detail_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<AccountDetail>, ApiError> {
    app.metrics().record_http_request();
    let detail = load_detail(&app, &parse_id(id)?).await?;
    Ok(Json(detail))
}

/// `GET /console/v1/accounts/{id}/export`: the full account as JSON.
///
/// Same shape as the detail today; kept as a dedicated route so future
/// account-adjacent data (wallet, friends, storage) joins the export without
/// reshaping the detail view.
pub(super) async fn export_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<AccountDetail>, ApiError> {
    app.metrics().record_http_request();
    let detail = load_detail(&app, &parse_id(id)?).await?;
    Ok(Json(detail))
}

/// `PUT /console/v1/accounts/{id}`: edit profile fields (admin).
pub(super) async fn update_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
    body: Result<Json<UpdateBody>, JsonRejection>,
) -> Result<Json<AccountRow>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = parse_id(id)?;
    let body = parse_body(body)?;
    let repo = app.backend().user_repository();
    let existing = repo
        .get_user(&id)
        .await?
        .ok_or_else(|| AppError::not_found("account not found"))?;
    let now = SystemClock.now();
    let username = match body.username {
        Some(username) => Username::new(username)?,
        None => existing.username.clone(),
    };
    let display_name = match body.display_name {
        None => existing.display_name.clone(),
        Some(value) if value.is_empty() => None,
        Some(value) => Some(DisplayName::new(value)?),
    };
    let metadata = match body.metadata {
        None => existing.metadata.clone(),
        Some(value) => Some(UserMetadata::new(value)?),
    };
    let updated = repo
        .update_user(User::new(
            existing.id.clone(),
            username,
            display_name,
            metadata,
            existing.created_at,
            now,
            existing.state,
        )?)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.update",
        format!("account {}", id.as_str()),
        "edited profile fields",
    ));
    Ok(Json(AccountRow::from_user(&updated)))
}

/// `POST /console/v1/accounts/{id}/ban`: disable the account (admin).
pub(super) async fn ban_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<AccountRow>, ApiError> {
    set_state(app, operator, id, AccountState::Disabled, "accounts.ban").await
}

/// `POST /console/v1/accounts/{id}/unban`: re-enable the account (admin).
pub(super) async fn unban_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<AccountRow>, ApiError> {
    set_state(app, operator, id, AccountState::Active, "accounts.unban").await
}

/// `DELETE /console/v1/accounts/{id}`: logical delete (admin).
///
/// Tombstones the account (it can never authenticate again), unlinks every
/// credential so the ids are reusable, and revokes live sessions.
pub(super) async fn delete_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = parse_id(id)?;
    let now = SystemClock.now();
    app.backend()
        .user_repository()
        .set_user_state(&id, AccountState::Tombstoned, now)
        .await?;
    let identities = app
        .backend()
        .auth_identity_repository()
        .list_auth_identities(&id)
        .await?;
    for identity in &identities {
        app.backend()
            .auth_identity_repository()
            .unlink_auth_identity(&identity.credential)
            .await?;
    }
    let revoked = app
        .backend()
        .session_repository()
        .revoke_user_sessions(&id, now, RevocationReason::UserDisabled)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.delete",
        format!("account {}", id.as_str()),
        format!(
            "tombstoned; unlinked {} credential(s), revoked {revoked} session(s)",
            identities.len()
        ),
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// Shared ban/unban implementation: state transition + session revocation on
/// ban + audit entry.
async fn set_state(
    app: App,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
    state: AccountState,
    action: &str,
) -> Result<Json<AccountRow>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = parse_id(id)?;
    let now = SystemClock.now();
    let updated = app
        .backend()
        .user_repository()
        .set_user_state(&id, state, now)
        .await?;
    let mut details = format!("account state set to {}", state.as_str());
    if state == AccountState::Disabled {
        let revoked = app
            .backend()
            .session_repository()
            .revoke_user_sessions(&id, now, RevocationReason::UserDisabled)
            .await?;
        details.push_str(&format!("; revoked {revoked} session(s)"));
    }
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        action,
        format!("account {}", id.as_str()),
        details,
    ));
    Ok(Json(AccountRow::from_user(&updated)))
}

/// Load the detail/export view for one account.
async fn load_detail(app: &App, id: &UserId) -> Result<AccountDetail, ApiError> {
    let user = app
        .backend()
        .user_repository()
        .get_user(id)
        .await?
        .ok_or_else(|| AppError::not_found("account not found"))?;
    let identities = app
        .backend()
        .auth_identity_repository()
        .list_auth_identities(id)
        .await?;
    Ok(AccountDetail {
        row: AccountRow::from_user(&user),
        metadata: user.metadata.map(UserMetadata::into_json),
        identities: identities
            .iter()
            .map(LinkedIdentity::from_identity)
            .collect(),
    })
}

/// Validate the path id through the domain newtype (400 on malformed input).
fn parse_id(id: Result<Path<String>, PathRejection>) -> Result<UserId, ApiError> {
    let Path(raw) = id.map_err(|rejection| {
        ApiError::from(
            AppError::validation("invalid account id").with_detail(rejection.body_text()),
        )
    })?;
    Ok(UserId::new(raw)?)
}

/// Extract a JSON body, mapping rejections to the uniform 400.
fn parse_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    match body {
        Ok(Json(body)) => Ok(body),
        Err(rejection) => Err(AppError::validation("invalid request body")
            .with_detail(rejection.body_text())
            .into()),
    }
}

// --- Wallet + friends panels ------------------------------------

/// The JSON response for the wallet panel.
#[derive(Debug, Clone, Serialize)]
pub struct WalletResponse {
    /// Currency-ordered balances.
    pub balances: std::collections::BTreeMap<String, i64>,
    /// Newest-first ledger entries for this user.
    pub ledger: Vec<crate::services::LedgerEntry>,
}

/// The JSON body accepted by a wallet adjustment.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletAdjustBody {
    /// Currency code (e.g. `coins`).
    pub currency: String,
    /// Signed change; negative debits. Must not be zero.
    pub delta: i64,
    /// Optional operator note recorded in the ledger.
    #[serde(default)]
    pub reason: Option<String>,
}

/// The JSON body accepted when creating a friendship from the console.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FriendAddBody {
    /// The other account id.
    pub user_id: String,
}

/// `GET /console/v1/accounts/{id}/wallet`: balances + newest-first ledger.
pub(super) async fn wallet_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<WalletResponse>, ApiError> {
    app.metrics().record_http_request();
    let id = parse_id(id)?;
    ensure_account_exists(&app, &id).await?;
    let balances = app.wallet().balances(id.as_str()).await?;
    let ledger = app.wallet().ledger(id.as_str(), 100).await?;
    Ok(Json(WalletResponse { balances, ledger }))
}

/// `POST /console/v1/accounts/{id}/wallet`: credit or debit (admin).
pub(super) async fn wallet_adjust_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
    body: Result<Json<WalletAdjustBody>, JsonRejection>,
) -> Result<Json<WalletResponse>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = parse_id(id)?;
    let body = parse_body(body)?;
    ensure_account_exists(&app, &id).await?;
    let now = SystemClock.now();
    let reason = body
        .reason
        .unwrap_or_else(|| "console adjustment".to_string());
    let balance = app
        .wallet()
        .adjust(id.as_str(), &body.currency, body.delta, &reason, now)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.wallet.adjust",
        format!("account {}", id.as_str()),
        format!(
            "{} {} by {} -> {balance}",
            if body.delta > 0 {
                "credited"
            } else {
                "debited"
            },
            body.currency,
            body.delta
        ),
    ));
    let balances = app.wallet().balances(id.as_str()).await?;
    let ledger = app.wallet().ledger(id.as_str(), 100).await?;
    Ok(Json(WalletResponse { balances, ledger }))
}

/// `GET /console/v1/accounts/{id}/friends`: this account's relations.
pub(super) async fn friends_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<Vec<crate::services::FriendRow>>, ApiError> {
    app.metrics().record_http_request();
    let id = parse_id(id)?;
    ensure_account_exists(&app, &id).await?;
    Ok(Json(app.friends().list(id.as_str()).await?))
}

/// `POST /console/v1/accounts/{id}/friends`: create/accept a friendship
/// (admin). An operator add acts for the account, so a repeat from the other
/// side completes the mutual friendship.
pub(super) async fn friend_add_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    id: Result<Path<String>, PathRejection>,
    body: Result<Json<FriendAddBody>, JsonRejection>,
) -> Result<Json<Vec<crate::services::FriendRow>>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = parse_id(id)?;
    let body = parse_body(body)?;
    let other = UserId::new(body.user_id)?;
    ensure_account_exists(&app, &id).await?;
    ensure_account_exists(&app, &other).await?;
    let now = SystemClock.now();
    let state = app.friends().add(id.as_str(), other.as_str(), now).await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.friends.add",
        format!("account {}", id.as_str()),
        format!("relation with {} -> {}", other.as_str(), state.as_str()),
    ));
    Ok(Json(app.friends().list(id.as_str()).await?))
}

/// `DELETE /console/v1/accounts/{id}/friends/{other}`: remove a relation
/// (admin; also how an operator unblocks).
pub(super) async fn friend_remove_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let Path((id, other)) = path.map_err(|rejection| {
        ApiError::from(
            AppError::validation("invalid account id").with_detail(rejection.body_text()),
        )
    })?;
    let id = UserId::new(id)?;
    let other = UserId::new(other)?;
    let now = SystemClock.now();
    app.friends().remove(id.as_str(), other.as_str()).await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "accounts.friends.remove",
        format!("account {}", id.as_str()),
        format!("removed relation with {}", other.as_str()),
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// 404 when the addressed account does not exist (panels never invent users).
async fn ensure_account_exists(app: &App, id: &UserId) -> Result<(), ApiError> {
    app.backend()
        .user_repository()
        .get_user(id)
        .await?
        .map(|_| ())
        .ok_or_else(|| ApiError::from(AppError::not_found("account not found")))
}

/// Mint a process-unique account id for console-created users.
///
/// Same scheme the authentication service uses (`user-{prefix}-{seq}`): an
/// OS-random per-process prefix plus an atomic counter, so console-created
/// and auth-created accounts never collide across restarts.
fn mint_user_id() -> AppResult<UserId> {
    static PREFIX: OnceLock<u64> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let prefix = *PREFIX.get_or_init(crate::repository::backend::random_instance_prefix);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    UserId::new(format!("user-{prefix:016x}-c{n}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&ACCOUNTS_PATH));
        for path in [
            ACCOUNT_DETAIL_PATH,
            ACCOUNT_BAN_PATH,
            ACCOUNT_UNBAN_PATH,
            ACCOUNT_EXPORT_PATH,
        ] {
            assert!(path.starts_with(ACCOUNTS_PATH), "path {path}");
        }
    }

    #[test]
    fn minted_user_ids_are_unique_and_validated() {
        let a = mint_user_id().expect("mint");
        let b = mint_user_id().expect("mint");
        assert_ne!(a, b);
        assert!(a.as_str().starts_with("user-"));
    }

    #[test]
    fn bodies_reject_unknown_fields() {
        assert!(serde_json::from_str::<CreateBody>(r#"{"username":"x","extra":1}"#).is_err());
        assert!(serde_json::from_str::<UpdateBody>(r#"{"usernme":"x"}"#).is_err());
    }
}
