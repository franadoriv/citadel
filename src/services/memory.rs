//! In-memory reference implementations of the session and authentication
//! services.
//!
//! [`InMemorySessionService`] owns session issuance/lifecycle over an injected
//! [`SessionRepository`] and [`TokenIssuer`], and [`InMemoryAuthenticationService`]
//! maps device/custom credentials to accounts and issues sessions through the
//! session service. Together with the in-memory repositories and
//! [`InMemorySessionDirectory`](super::directory::InMemorySessionDirectory) they
//! form a runnable, single-process identity/session stack that enforces the
//! hardening deferred by :
//!
//! - Non-zero and consistent TTLs (refresh window `>=` access window), with all
//!   time arithmetic checked for overflow.
//! - Token secrets are never logged: the token index is redacted in `Debug` and
//!   the raw secret is only ever read via `expose_secret` at the lookup boundary.
//! - Authentication returns a single uniform error for unknown/disabled/
//!   tombstoned/missing-user cases so it cannot be used as a credential oracle,
//!   and account creation is serialized so it is all-or-nothing.
//!
//! The token index maps an issued secret to its session id. A production issuer
//! verifies a signed token instead of storing anything; this reference keeps the
//! secret in-process only, never persists it to session state, and never logs it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use argon2::password_hash::SaltString;
use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::identity::{
    AccountState, AuthCredential, AuthIdentity, Password,
    PasswordVerifier as StoredPasswordVerifier, User,
};
use crate::repository::backend::random_instance_prefix;
use crate::repository::{AuthIdentityRepository, Backend, SessionRepository, UserRepository};
use crate::session::{
    IssuedSessionTokens, Session, SessionId, SessionInvalidity, SessionValidation,
};
use crate::storage::UserId;
use crate::time::DurationMillis;

use super::authentication::{
    AuthenticationOptions, AuthenticationOutcome, AuthenticationService,
    CustomAuthenticationRequest, DeviceAuthenticationRequest, EmailAuthenticationRequest,
};
use super::session::{
    CreateSessionRequest, CreatedSession, RefreshSessionRequest, RevokeSessionRequest,
    SessionService, ValidateSessionRequest,
};
use super::token::{CountingTokenIssuer, RandomTokenIssuer, TokenIssuer};
use super::{Health, ServiceLifecycle};

/// The single sanitized error surfaced for every authentication failure, so a
/// caller cannot distinguish an unknown credential from a disabled account.
fn authentication_failed() -> AppError {
    AppError::auth("authentication failed")
}

/// The password-storage work factor: 19 MiB, two iterations, one lane.
/// Parameters are encoded into every PHC verifier, enabling deliberate upgrades.
fn password_hasher() -> AppResult<Argon2<'static>> {
    let params = Params::new(19 * 1024, 2, 1, Some(32))
        .map_err(|_| AppError::internal("invalid password hash parameters"))?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

fn hash_password(password: &Password) -> AppResult<StoredPasswordVerifier> {
    let mut salt_bytes = [0_u8; 16];
    getrandom::fill(&mut salt_bytes)
        .map_err(|_| AppError::internal("failed to generate password salt"))?;
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|_| AppError::internal("failed to encode password salt"))?;
    let encoded = password_hasher()?
        .hash_password(password.expose().as_bytes(), &salt)
        .map_err(|_| AppError::internal("failed to hash password"))?
        .to_string();
    StoredPasswordVerifier::new(encoded)
}

fn verify_password(password: &Password, verifier: Option<&StoredPasswordVerifier>) -> bool {
    let Some(verifier) = verifier else {
        return false;
    };
    let Ok(parsed) = PasswordHash::new(verifier.encoded()) else {
        return false;
    };
    password_hasher()
        .and_then(|hasher| {
            hasher
                .verify_password(password.expose().as_bytes(), &parsed)
                .map_err(|_| authentication_failed())
        })
        .is_ok()
}

/// Validate that TTLs are non-zero and the refresh window is not shorter than
/// the access window. Shared by session creation, refresh, and authentication.
fn validate_ttls(
    session_ttl: DurationMillis,
    refresh_ttl: Option<DurationMillis>,
) -> AppResult<()> {
    if session_ttl.as_millis() == 0 {
        return Err(AppError::validation(
            "session_ttl must be greater than zero",
        ));
    }
    if let Some(refresh_ttl) = refresh_ttl {
        if refresh_ttl.as_millis() == 0 {
            return Err(AppError::validation(
                "refresh_ttl must be greater than zero",
            ));
        }
        if refresh_ttl < session_ttl {
            return Err(AppError::validation(
                "refresh_ttl must not be shorter than session_ttl",
            ));
        }
    }
    Ok(())
}

/// A shared, `Send + Sync` session repository handle.
pub type SharedSessionRepository = Arc<dyn SessionRepository + Send + Sync>;
/// A shared, `Send + Sync` user repository handle.
pub type SharedUserRepository = Arc<dyn UserRepository + Send + Sync>;
/// A shared, `Send + Sync` auth identity repository handle.
pub type SharedAuthIdentityRepository = Arc<dyn AuthIdentityRepository + Send + Sync>;
/// A shared, `Send + Sync` session service handle.
pub type SharedSessionService = Arc<dyn SessionService + Send + Sync>;

/// Redacted in-process index from an issued token secret to its session.
#[derive(Default)]
struct TokenIndex {
    access: HashMap<String, SessionId>,
    refresh: HashMap<String, SessionId>,
}

/// In-memory [`SessionService`]: issuance and lifecycle over an injected
/// repository and token issuer.
pub struct InMemorySessionService {
    repo: SharedSessionRepository,
    issuer: Arc<dyn TokenIssuer>,
    // Per-process random prefix; combined with the process-global sequence below
    // so session ids do not collide with rows a previous run persisted or with
    // another service instance sharing a durable backend (a monotonic-from-1
    // counter per instance would regenerate `sess-1` and conflict on insert).
    id_prefix: u64,
    tokens: Mutex<TokenIndex>,
}

/// Process-global session-id sequence (see [`NEXT_USER_SEQ`]).
static NEXT_SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

impl std::fmt::Debug for InMemorySessionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the token index: it maps raw secrets to sessions.
        f.debug_struct("InMemorySessionService")
            .field("tokens", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl InMemorySessionService {
    /// Assemble a service over a repository and token issuer.
    #[must_use]
    pub fn new(repo: SharedSessionRepository, issuer: Arc<dyn TokenIssuer>) -> Self {
        Self {
            repo,
            issuer,
            id_prefix: random_instance_prefix(),
            tokens: Mutex::new(TokenIndex::default()),
        }
    }

    /// Assemble a service with the deterministic [`CountingTokenIssuer`].
    ///
    /// The counting issuer mints predictable tokens and is for contract tests and
    /// local development only; production nodes must use [`Self::with_secure_issuer`].
    #[must_use]
    pub fn with_default_issuer(repo: SharedSessionRepository) -> Self {
        Self::new(repo, Arc::new(CountingTokenIssuer::new()))
    }

    /// Assemble a service with the CSPRNG-backed [`RandomTokenIssuer`] — the
    /// secure issuer wired into the running node.
    #[must_use]
    pub fn with_secure_issuer(repo: SharedSessionRepository) -> Self {
        Self::new(repo, Arc::new(RandomTokenIssuer::new()))
    }

    fn next_session_id(&self) -> AppResult<SessionId> {
        let n = NEXT_SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
        SessionId::new(format!("sess-{:016x}-{n}", self.id_prefix))
    }

    fn tokens(&self) -> AppResult<std::sync::MutexGuard<'_, TokenIndex>> {
        self.tokens
            .lock()
            .map_err(|_| AppError::internal("session token index mutex poisoned"))
    }

    fn index_tokens(&self, session_id: &SessionId, tokens: &IssuedSessionTokens) -> AppResult<()> {
        let mut index = self.tokens()?;
        index.access.insert(
            tokens.access.expose_secret().to_string(),
            session_id.clone(),
        );
        if let Some(refresh) = &tokens.refresh {
            index
                .refresh
                .insert(refresh.expose_secret().to_string(), session_id.clone());
        }
        Ok(())
    }

    /// Drop any secrets currently mapped to `session_id`, then index the new set.
    fn rotate_tokens(&self, session_id: &SessionId, tokens: &IssuedSessionTokens) -> AppResult<()> {
        let mut index = self.tokens()?;
        index.access.retain(|_, id| id != session_id);
        index.refresh.retain(|_, id| id != session_id);
        index.access.insert(
            tokens.access.expose_secret().to_string(),
            session_id.clone(),
        );
        if let Some(refresh) = &tokens.refresh {
            index
                .refresh
                .insert(refresh.expose_secret().to_string(), session_id.clone());
        }
        Ok(())
    }
}

impl ServiceLifecycle for InMemorySessionService {
    fn name(&self) -> &str {
        "session"
    }
}

#[async_trait]
impl SessionService for InMemorySessionService {
    async fn create_session(&self, request: CreateSessionRequest) -> AppResult<CreatedSession> {
        validate_ttls(request.session_ttl, request.refresh_ttl)?;
        let expires_at = request.now.checked_add(request.session_ttl)?;
        let refresh_expires_at = request
            .refresh_ttl
            .map(|ttl| request.now.checked_add(ttl))
            .transpose()?;
        let issued = self.issuer.issue(request.refresh_ttl.is_some())?;
        let session_id = self.next_session_id()?;
        let session = Session::new(
            session_id,
            request.user_id,
            request.owner_node,
            request.now,
            expires_at,
            refresh_expires_at,
            Some(issued.token_ref.clone()),
        )?;
        let stored = self.repo.create_session(session).await?;
        self.index_tokens(&stored.id, &issued.tokens)?;
        Ok(CreatedSession {
            session: stored,
            tokens: issued.tokens,
        })
    }

    async fn validate_session(
        &self,
        request: ValidateSessionRequest,
    ) -> AppResult<SessionValidation> {
        let session_id = self
            .tokens()?
            .access
            .get(request.access_token.expose_secret())
            .cloned();
        let Some(session_id) = session_id else {
            return Ok(SessionValidation::Invalid(SessionInvalidity::Unknown));
        };
        match self.repo.get_session(&session_id).await? {
            Some(session) => Ok(session.validate_at(request.now)),
            None => Ok(SessionValidation::Invalid(SessionInvalidity::Unknown)),
        }
    }

    async fn session_for_refresh_token(
        &self,
        refresh_token: crate::session::SessionTokenSecret,
    ) -> AppResult<Option<Session>> {
        let session_id = self
            .tokens()?
            .refresh
            .get(refresh_token.expose_secret())
            .cloned();
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        self.repo.get_session(&session_id).await
    }

    async fn refresh_session(&self, request: RefreshSessionRequest) -> AppResult<CreatedSession> {
        validate_ttls(request.session_ttl, request.refresh_ttl)?;
        let session_id = self
            .tokens()?
            .refresh
            .get(request.refresh_token.expose_secret())
            .cloned();
        let Some(session_id) = session_id else {
            // An unknown refresh token (including a presented access token) is an
            // auth failure, never a distinguishable error.
            return Err(AppError::auth("invalid refresh token"));
        };
        let mut session = self
            .repo
            .get_session(&session_id)
            .await?
            .ok_or_else(|| AppError::auth("invalid refresh token"))?;
        if !session.can_refresh_at(request.now) {
            return Err(AppError::auth("refresh token is not valid"));
        }
        let new_expires_at = request.now.checked_add(request.session_ttl)?;
        let new_refresh_expires_at = request
            .refresh_ttl
            .map(|ttl| request.now.checked_add(ttl))
            .transpose()?;
        let issued = self.issuer.issue(request.refresh_ttl.is_some())?;
        session.refresh_at(
            request.now,
            new_expires_at,
            new_refresh_expires_at,
            Some(issued.token_ref.clone()),
        )?;
        let stored = self.repo.update_session(session).await?;
        self.rotate_tokens(&stored.id, &issued.tokens)?;
        Ok(CreatedSession {
            session: stored,
            tokens: issued.tokens,
        })
    }

    async fn revoke_session(&self, request: RevokeSessionRequest) -> AppResult<Session> {
        let mut session = self
            .repo
            .get_session(&request.session_id)
            .await?
            .ok_or_else(|| AppError::not_found("session not found"))?;
        session.revoke_at(request.revoked_at, request.reason)?;
        let stored = self.repo.update_session(session).await?;
        // The token secrets stay indexed so validation of a revoked token reports
        // the precise `Revoked` reason rather than `Unknown`; both are invalid,
        // and a production verifier consults a revocation list / signed-token
        // expiry instead of an in-process index.
        Ok(stored)
    }
}

/// The concrete [`AuthenticationService`]: maps device/custom credentials to
/// accounts and issues sessions through an injected [`SessionService`].
///
/// It runs over any selected [`Backend`] (in-memory or Postgres). Account
/// creation — create the user, then link its auth identity — runs inside one
/// [`UnitOfWork`](crate::repository::UnitOfWork) transaction, so it is
/// all-or-nothing on either backend (a real database transaction on Postgres, a
/// write-lock-serialized, undo-logged scope in memory). The prior app-level
/// mutex is gone.
pub struct AuthenticationServiceImpl {
    backend: Arc<dyn Backend>,
    sessions: SharedSessionService,
    // Per-process random prefix so generated user ids do not collide with rows a
    // previous run persisted (a monotonic-from-1 counter would regenerate
    // existing ids after a restart and break account creation on a durable
    // backend). See `random_instance_prefix`.
    id_prefix: u64,
}

/// Process-global user-id sequence.
///
/// Shared across every `AuthenticationServiceImpl` in the process so two service
/// instances (e.g. two composed stacks, or tests) can never mint the same id,
/// even if their random prefixes were to coincide. Combined with the per-service
/// [`random_instance_prefix`], ids are unique within a process and in a distinct
/// space per process/node.
static NEXT_USER_SEQ: AtomicU64 = AtomicU64::new(1);

impl std::fmt::Debug for AuthenticationServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticationServiceImpl")
            .field("backend", &self.backend.kind())
            .finish_non_exhaustive()
    }
}

impl AuthenticationServiceImpl {
    /// Assemble the service over a selected backend and a session service.
    #[must_use]
    pub fn new(backend: Arc<dyn Backend>, sessions: SharedSessionService) -> Self {
        Self {
            backend,
            sessions,
            id_prefix: random_instance_prefix(),
        }
    }

    fn next_user_id(&self) -> AppResult<UserId> {
        let n = NEXT_USER_SEQ.fetch_add(1, Ordering::Relaxed);
        UserId::new(format!("user-{:016x}-{n}", self.id_prefix))
    }

    fn session_request(
        &self,
        user_id: UserId,
        options: &AuthenticationOptions,
    ) -> CreateSessionRequest {
        CreateSessionRequest {
            user_id,
            owner_node: options.owner_node.clone(),
            now: options.now,
            session_ttl: options.session_ttl,
            refresh_ttl: options.refresh_ttl,
        }
    }

    /// Issue a session for an already-known credential, rejecting accounts that
    /// cannot authenticate with the uniform failure error.
    async fn issue_for_existing(
        &self,
        identity: AuthIdentity,
        options: &AuthenticationOptions,
    ) -> AppResult<AuthenticationOutcome> {
        let user = self
            .backend
            .user_repository()
            .get_user(&identity.user_id)
            .await?
            .ok_or_else(authentication_failed)?;
        if !user.state.can_authenticate() {
            return Err(authentication_failed());
        }
        let created = self
            .sessions
            .create_session(self.session_request(user.id.clone(), options))
            .await?;
        Ok(AuthenticationOutcome {
            user,
            identity,
            session: created.session,
            tokens: created.tokens,
            account_created: false,
            identity_created: false,
        })
    }

    async fn authenticate(
        &self,
        credential: AuthCredential,
        options: AuthenticationOptions,
        password: Option<&Password>,
    ) -> AppResult<AuthenticationOutcome> {
        // Pre-validate TTLs so a bad request never creates an account or session.
        validate_ttls(options.session_ttl, options.refresh_ttl)?;

        // Fast path: a known credential just issues a session over the pooled
        // repositories — no transaction needed.
        if let Some(identity) = self
            .backend
            .auth_identity_repository()
            .get_auth_identity(&credential)
            .await?
        {
            if matches!(credential, AuthCredential::Email(_)) {
                let Some(password) = password else {
                    return Err(authentication_failed());
                };
                if !verify_password(password, identity.password_verifier()) {
                    return Err(authentication_failed());
                }
            }
            return self.issue_for_existing(identity, &options).await;
        }
        if !options.create_account {
            // Do not reveal whether the credential exists.
            return Err(authentication_failed());
        }

        let username = options
            .username
            .clone()
            .ok_or_else(|| AppError::validation("username is required to create an account"))?;

        // Create path: run create-user + link-identity inside ONE unit of work so
        // it commits or rolls back atomically on whichever backend is selected.
        let uow = self.backend.begin().await?;

        // Re-check inside the transaction: a concurrent request may have created
        // the account since the fast-path read. If so, roll back and reuse it.
        if uow
            .auth_identity_repository()
            .get_auth_identity(&credential)
            .await?
            .is_some()
        {
            uow.rollback().await?;
            return self
                .reuse_existing_or_fail(&credential, &options, password)
                .await;
        }

        let user = User::new(
            self.next_user_id()?,
            username,
            options.display_name.clone(),
            options.metadata.clone(),
            options.now,
            options.now,
            AccountState::Active,
        )?;
        let created_user = match uow.user_repository().create_user(user).await {
            Ok(user) => user,
            Err(err) => {
                uow.rollback().await?;
                // A concurrent create for the SAME credential may have won and
                // linked it (surfacing here as a username/id conflict). Reuse the
                // now-existing account; otherwise surface the original error (for
                // example a genuine username-taken conflict between two accounts).
                if let Some(identity) = self
                    .backend
                    .auth_identity_repository()
                    .get_auth_identity(&credential)
                    .await?
                {
                    return self.issue_for_existing(identity, &options).await;
                }
                return Err(err);
            }
        };
        let identity = AuthIdentity::new(
            credential.clone(),
            created_user.id.clone(),
            options.now,
            options.now,
        )?;
        let identity = match &credential {
            AuthCredential::Email(_) => identity.with_password_verifier(hash_password(
                password.ok_or_else(authentication_failed)?,
            )?)?,
            _ => identity,
        };
        let linked = match uow
            .auth_identity_repository()
            .link_auth_identity(identity)
            .await
        {
            Ok(identity) => identity,
            Err(_) => {
                // Lost the race to a concurrent creator between the re-check and
                // the link. Roll back (no orphan user) and reuse the account
                // rather than surfacing a raw conflict.
                uow.rollback().await?;
                return self
                    .reuse_existing_or_fail(&credential, &options, password)
                    .await;
            }
        };
        uow.commit().await?;

        // The account is durable; issue its session on the pooled session service.
        let created = self
            .sessions
            .create_session(self.session_request(created_user.id.clone(), &options))
            .await?;
        Ok(AuthenticationOutcome {
            user: created_user,
            identity: linked,
            session: created.session,
            tokens: created.tokens,
            account_created: true,
            identity_created: true,
        })
    }

    /// After a rolled-back create race, reuse the account that now owns the
    /// credential, or fail with the uniform auth error if it has vanished.
    async fn reuse_existing_or_fail(
        &self,
        credential: &AuthCredential,
        options: &AuthenticationOptions,
        password: Option<&Password>,
    ) -> AppResult<AuthenticationOutcome> {
        match self
            .backend
            .auth_identity_repository()
            .get_auth_identity(credential)
            .await?
        {
            Some(identity) => {
                if matches!(credential, AuthCredential::Email(_)) {
                    let Some(password) = password else {
                        return Err(authentication_failed());
                    };
                    if !verify_password(password, identity.password_verifier()) {
                        return Err(authentication_failed());
                    }
                }
                self.issue_for_existing(identity, options).await
            }
            None => Err(authentication_failed()),
        }
    }
}

impl ServiceLifecycle for AuthenticationServiceImpl {
    fn name(&self) -> &str {
        "authentication"
    }

    fn health(&self) -> Health {
        Health::Healthy
    }
}

#[async_trait]
impl AuthenticationService for AuthenticationServiceImpl {
    async fn authenticate_device(
        &self,
        request: DeviceAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome> {
        self.authenticate(
            AuthCredential::Device(request.device_id),
            request.options,
            None,
        )
        .await
    }

    async fn authenticate_custom(
        &self,
        request: CustomAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome> {
        self.authenticate(
            AuthCredential::Custom(request.custom_id),
            request.options,
            None,
        )
        .await
    }

    async fn authenticate_email(
        &self,
        request: EmailAuthenticationRequest,
    ) -> AppResult<AuthenticationOutcome> {
        self.authenticate(
            AuthCredential::Email(request.email),
            request.options,
            Some(&request.password),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use crate::identity::{DeviceId, Username};
    use crate::repository::{Backend, InMemoryBackend, InMemorySessionRepository};
    use crate::session::NodeId;
    use crate::time::TimestampMillis;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn ms(v: u64) -> DurationMillis {
        DurationMillis::from_millis(v)
    }

    fn session_service() -> InMemorySessionService {
        InMemorySessionService::with_default_issuer(Arc::new(InMemorySessionRepository::new()))
    }

    fn options(create: bool) -> AuthenticationOptions {
        AuthenticationOptions {
            create_account: create,
            username: Some(Username::new("player").expect("username")),
            display_name: None,
            metadata: None,
            now: ts(1_000),
            owner_node: NodeId::new("node-a").expect("node"),
            session_ttl: ms(1_000),
            refresh_ttl: Some(ms(5_000)),
        }
    }

    fn device_request(device: &str, create: bool) -> DeviceAuthenticationRequest {
        DeviceAuthenticationRequest {
            device_id: DeviceId::new(device).expect("device"),
            options: options(create),
        }
    }

    fn auth_stack() -> AuthenticationServiceImpl {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let sessions: SharedSessionService = Arc::new(InMemorySessionService::with_default_issuer(
            backend.session_repository(),
        ));
        AuthenticationServiceImpl::new(backend, sessions)
    }

    #[tokio::test]
    async fn create_validate_refresh_revoke_round_trip() {
        let svc = session_service();
        let created = svc
            .create_session(CreateSessionRequest {
                user_id: UserId::new("u-1").expect("test value"),
                owner_node: NodeId::new("node-a").expect("test value"),
                now: ts(1_000),
                session_ttl: ms(1_000),
                refresh_ttl: Some(ms(5_000)),
            })
            .await
            .expect("create");

        // Validate the access token before expiry.
        let validation = svc
            .validate_session(ValidateSessionRequest {
                access_token: created.tokens.access.clone(),
                now: ts(1_500),
            })
            .await
            .expect("validate");
        assert!(validation.is_valid());

        // Refresh rotates tokens; the old access token stops validating.
        let refresh_token = created.tokens.refresh.clone().expect("refreshable");
        let refreshed = svc
            .refresh_session(RefreshSessionRequest {
                refresh_token,
                now: ts(1_500),
                owner_node: NodeId::new("node-a").expect("test value"),
                session_ttl: ms(1_000),
                refresh_ttl: Some(ms(5_000)),
            })
            .await
            .expect("refresh");

        let old = svc
            .validate_session(ValidateSessionRequest {
                access_token: created.tokens.access.clone(),
                now: ts(1_600),
            })
            .await
            .expect("validate old");
        assert!(!old.is_valid(), "old access token invalid after rotation");

        let fresh = svc
            .validate_session(ValidateSessionRequest {
                access_token: refreshed.tokens.access.clone(),
                now: ts(1_600),
            })
            .await
            .expect("validate new");
        assert!(fresh.is_valid());

        // Revoke, then the new token stops validating too.
        svc.revoke_session(RevokeSessionRequest {
            session_id: refreshed.session.id.clone(),
            revoked_at: ts(1_700),
            reason: crate::session::RevocationReason::Logout,
        })
        .await
        .expect("revoke");
        let after = svc
            .validate_session(ValidateSessionRequest {
                access_token: refreshed.tokens.access,
                now: ts(1_800),
            })
            .await
            .expect("validate revoked");
        assert_eq!(after.invalidity(), Some(SessionInvalidity::Revoked));
    }

    #[tokio::test]
    async fn create_rejects_zero_and_inconsistent_ttls() {
        let svc = session_service();
        let base = CreateSessionRequest {
            user_id: UserId::new("u-1").expect("test value"),
            owner_node: NodeId::new("node-a").expect("test value"),
            now: ts(1_000),
            session_ttl: ms(0),
            refresh_ttl: None,
        };
        assert_eq!(
            svc.create_session(base.clone())
                .await
                .expect_err("zero ttl")
                .category(),
            ErrorCategory::Validation
        );
        let short_refresh = CreateSessionRequest {
            session_ttl: ms(1_000),
            refresh_ttl: Some(ms(500)),
            ..base
        };
        assert_eq!(
            svc.create_session(short_refresh)
                .await
                .expect_err("short refresh")
                .category(),
            ErrorCategory::Validation
        );
    }

    #[tokio::test]
    async fn access_token_cannot_be_used_as_refresh() {
        let svc = session_service();
        let created = svc
            .create_session(CreateSessionRequest {
                user_id: UserId::new("u-1").expect("test value"),
                owner_node: NodeId::new("node-a").expect("test value"),
                now: ts(1_000),
                session_ttl: ms(1_000),
                refresh_ttl: Some(ms(5_000)),
            })
            .await
            .expect("create");
        // Present the ACCESS token to refresh: rejected as auth failure.
        let err = svc
            .refresh_session(RefreshSessionRequest {
                refresh_token: created.tokens.access,
                now: ts(1_200),
                owner_node: NodeId::new("node-a").expect("test value"),
                session_ttl: ms(1_000),
                refresh_ttl: Some(ms(5_000)),
            })
            .await
            .expect_err("access as refresh");
        assert_eq!(err.category(), ErrorCategory::Auth);
    }

    #[tokio::test]
    async fn device_auth_creates_then_reuses_account() {
        let auth = auth_stack();
        let first = auth
            .authenticate_device(device_request("d-1", true))
            .await
            .expect("register");
        assert!(first.account_created);
        assert!(first.identity_created);

        // Second auth with the same device reuses the account.
        let second = auth
            .authenticate_device(device_request("d-1", false))
            .await
            .expect("login");
        assert!(!second.account_created);
        assert_eq!(second.user.id, first.user.id);
    }

    #[tokio::test]
    async fn unknown_credential_without_create_is_uniform_auth_error() {
        let auth = auth_stack();
        let err = auth
            .authenticate_device(device_request("ghost", false))
            .await
            .expect_err("no create");
        assert_eq!(err.category(), ErrorCategory::Auth);
        assert_eq!(err.to_string(), authentication_failed().to_string());
    }

    #[tokio::test]
    async fn disabled_account_reports_same_error_as_unknown() {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let sessions: SharedSessionService = Arc::new(InMemorySessionService::with_default_issuer(
            backend.session_repository(),
        ));
        let auth = AuthenticationServiceImpl::new(Arc::clone(&backend), sessions);
        // Register, then disable the account.
        let outcome = auth
            .authenticate_device(device_request("d-1", true))
            .await
            .expect("register");
        backend
            .user_repository()
            .set_user_state(&outcome.user.id, AccountState::Disabled, ts(2_000))
            .await
            .expect("disable");

        let err = auth
            .authenticate_device(device_request("d-1", false))
            .await
            .expect_err("disabled");
        assert_eq!(err.to_string(), authentication_failed().to_string());
    }

    #[tokio::test]
    async fn debug_never_leaks_token_secrets() {
        let svc = session_service();
        let created = svc
            .create_session(CreateSessionRequest {
                user_id: UserId::new("u-1").expect("test value"),
                owner_node: NodeId::new("node-a").expect("test value"),
                now: ts(1_000),
                session_ttl: ms(1_000),
                refresh_ttl: Some(ms(5_000)),
            })
            .await
            .expect("create");
        let secret = created.tokens.access.expose_secret().to_string();
        let rendered = format!("{svc:?}");
        assert!(!rendered.contains(&secret));
        assert!(rendered.contains("[redacted]"));
    }
}
