//! API-key lifecycle, bearer verification, and coalesced last-use telemetry.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::repository::{ApiKeyId, ApiKeyRepository, ApiKeyScope, ApiKeyVerifier, StoredApiKey};
use crate::time::TimestampMillis;

const PREFIX: &str = "ctdl_k1_";
const SECRET_BYTES: usize = 32;
const SECRET_ENCODED_LEN: usize = 43;
const TOKEN_LEN: usize = PREFIX.len() + ApiKeyId::HEX_LEN + 1 + SECRET_ENCODED_LEN;

#[derive(Debug, Clone)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub expires_at: Option<TimestampMillis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiKeyMetadata {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub generation: u64,
    pub created_at: TimestampMillis,
    pub expires_at: Option<TimestampMillis>,
    pub revoked_at: Option<TimestampMillis>,
    pub last_used_at: Option<TimestampMillis>,
}

impl From<&StoredApiKey> for ApiKeyMetadata {
    fn from(value: &StoredApiKey) -> Self {
        Self {
            id: value.id.clone(),
            name: value.name.clone(),
            scopes: value.scopes.clone(),
            generation: value.generation,
            created_at: value.created_at,
            expires_at: value.expires_at,
            revoked_at: value.revoked_at,
            last_used_at: value.last_used_at,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct ApiKeySecretResponse {
    pub key: ApiKeyMetadata,
    pub secret: String,
}

impl fmt::Debug for ApiKeySecretResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKeySecretResponse")
            .field("key", &self.key)
            .field("secret", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyPrincipal {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub generation: u64,
}

#[derive(Default, Debug)]
struct LastUsedCoalescer {
    dirty: Mutex<BTreeMap<(ApiKeyId, u64), TimestampMillis>>,
}

impl LastUsedCoalescer {
    fn observe(&self, id: &ApiKeyId, generation: u64, at: TimestampMillis) {
        let mut dirty = self
            .dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        dirty
            .entry((id.clone(), generation))
            .and_modify(|old| *old = (*old).max(at))
            .or_insert(at);
    }
    fn snapshot(&self) -> Vec<(ApiKeyId, u64, TimestampMillis)> {
        self.dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|((id, generation), at)| (id.clone(), *generation, *at))
            .collect()
    }
    fn clean(&self, id: &ApiKeyId, generation: u64, flushed: TimestampMillis) {
        let mut dirty = self
            .dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (id.clone(), generation);
        if dirty.get(&key).is_some_and(|pending| *pending <= flushed) {
            dirty.remove(&key);
        }
    }
    fn pending(&self, id: &ApiKeyId, generation: u64) -> Option<TimestampMillis> {
        self.dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(id.clone(), generation))
            .copied()
    }
}

pub struct ApiKeyService {
    repository: Arc<dyn ApiKeyRepository>,
    last_used: LastUsedCoalescer,
}

impl fmt::Debug for ApiKeyService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKeyService").finish_non_exhaustive()
    }
}

impl ApiKeyService {
    #[must_use]
    pub fn new(repository: Arc<dyn ApiKeyRepository>) -> Self {
        Self {
            repository,
            last_used: LastUsedCoalescer::default(),
        }
    }

    pub async fn create(
        &self,
        mut request: CreateApiKeyRequest,
        now: TimestampMillis,
    ) -> AppResult<ApiKeySecretResponse> {
        validate_request(&mut request, now)?;
        let (id, token, verifier) = issue_secret()?;
        let stored = StoredApiKey {
            id,
            name: request.name,
            scopes: request.scopes,
            verifier,
            generation: 1,
            created_at: now,
            expires_at: request.expires_at,
            revoked_at: None,
            last_used_at: None,
        };
        self.repository.create(stored.clone()).await?;
        Ok(ApiKeySecretResponse {
            key: ApiKeyMetadata::from(&stored),
            secret: token,
        })
    }

    pub async fn authenticate(
        &self,
        bearer: &str,
        now: TimestampMillis,
    ) -> AppResult<ApiKeyPrincipal> {
        let (id, verifier) = parse_and_hash(bearer).ok_or_else(auth_failed)?;
        let stored = self
            .repository
            .get(&id)
            .await
            .map_err(|_| auth_failed())?
            .ok_or_else(auth_failed)?;
        if stored.revoked_at.is_some()
            || stored.expires_at.is_some_and(|expiry| expiry <= now)
            || !constant_time_eq(verifier.as_bytes(), stored.verifier.as_bytes())
        {
            return Err(auth_failed());
        }
        self.last_used.observe(&id, stored.generation, now);
        Ok(ApiKeyPrincipal {
            id,
            name: stored.name,
            scopes: stored.scopes,
            generation: stored.generation,
        })
    }

    pub async fn rotate(
        &self,
        id: &ApiKeyId,
        generation: u64,
        at: TimestampMillis,
    ) -> AppResult<ApiKeySecretResponse> {
        self.flush_last_used().await?;
        let (_, token, verifier) = issue_secret_for(id.clone())?;
        let stored = self.repository.rotate(id, generation, verifier, at).await?;
        Ok(ApiKeySecretResponse {
            key: ApiKeyMetadata::from(&stored),
            secret: token,
        })
    }

    pub async fn revoke(
        &self,
        id: &ApiKeyId,
        generation: u64,
        at: TimestampMillis,
    ) -> AppResult<ApiKeyMetadata> {
        self.flush_last_used().await?;
        self.repository
            .revoke(id, generation, at)
            .await
            .map(|stored| ApiKeyMetadata::from(&stored))
    }

    pub async fn list(&self) -> AppResult<Vec<ApiKeyMetadata>> {
        self.flush_last_used().await?;
        self.repository
            .list()
            .await
            .map(|rows| rows.iter().map(ApiKeyMetadata::from).collect())
    }
    pub async fn get(&self, id: &ApiKeyId) -> AppResult<Option<ApiKeyMetadata>> {
        self.flush_last_used().await?;
        let mut key = self
            .repository
            .get(id)
            .await?
            .map(|row| ApiKeyMetadata::from(&row));
        if let Some(key) = &mut key
            && let Some(pending) = self.last_used.pending(id, key.generation)
        {
            key.last_used_at = Some(key.last_used_at.map_or(pending, |old| old.max(pending)));
        }
        Ok(key)
    }

    pub async fn flush_last_used(&self) -> AppResult<()> {
        for (id, generation, at) in self.last_used.snapshot() {
            self.repository
                .update_last_used(&id, generation, at)
                .await?;
            self.last_used.clean(&id, generation, at);
        }
        Ok(())
    }
}

fn validate_request(request: &mut CreateApiKeyRequest, now: TimestampMillis) -> AppResult<()> {
    request.name = request.name.trim().to_string();
    if request.name.is_empty() || request.name.len() > 128 {
        return Err(AppError::validation("API key name must be 1..=128 bytes"));
    }
    request.scopes.sort();
    request.scopes.dedup();
    if request.scopes.is_empty() {
        return Err(AppError::validation(
            "at least one API key scope is required",
        ));
    }
    if request.expires_at.is_some_and(|expiry| expiry <= now) {
        return Err(AppError::validation("API key expiry must be in the future"));
    }
    Ok(())
}

fn issue_secret() -> AppResult<(ApiKeyId, String, ApiKeyVerifier)> {
    let mut id_bytes = [0_u8; 16];
    getrandom::fill(&mut id_bytes)
        .map_err(|_| AppError::internal("CSPRNG unavailable for API key"))?;
    let id = ApiKeyId::new(
        id_bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )?;
    issue_secret_for(id)
}
fn issue_secret_for(id: ApiKeyId) -> AppResult<(ApiKeyId, String, ApiKeyVerifier)> {
    let mut secret = [0_u8; SECRET_BYTES];
    getrandom::fill(&mut secret)
        .map_err(|_| AppError::internal("CSPRNG unavailable for API key"))?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let token = format!("{PREFIX}{}_{encoded}", id.as_str());
    let verifier = ApiKeyVerifier::from_bytes(Sha256::digest(secret).into());
    Ok((id, token, verifier))
}
fn parse_and_hash(token: &str) -> Option<(ApiKeyId, ApiKeyVerifier)> {
    if token.len() != TOKEN_LEN || !token.is_ascii() {
        return None;
    }
    let rest = token.strip_prefix(PREFIX)?;
    let (id, secret) = rest.split_once('_')?;
    let id = ApiKeyId::new(id.to_string()).ok()?;
    if secret.len() != SECRET_ENCODED_LEN {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(secret)
        .ok()?;
    let secret: [u8; SECRET_BYTES] = bytes.try_into().ok()?;
    Some((
        id,
        ApiKeyVerifier::from_bytes(Sha256::digest(secret).into()),
    ))
}
fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0_u8, |difference, (left, right)| {
        difference | (left ^ right)
    }) == 0
}
fn auth_failed() -> AppError {
    AppError::auth("API key authentication failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parser_rejects_unbounded_or_ambiguous_tokens_before_lookup() {
        assert!(parse_and_hash(&"x".repeat(10_000)).is_none());
        assert!(parse_and_hash("ctdl_k1_bad").is_none());
    }
    #[test]
    fn secret_response_debug_is_redacted() {
        let response = ApiKeySecretResponse {
            key: ApiKeyMetadata {
                id: ApiKeyId::new("0".repeat(32)).expect("id"),
                name: "reader".into(),
                scopes: vec![ApiKeyScope::TelemetryRead],
                generation: 1,
                created_at: TimestampMillis::from_unix_millis(1),
                expires_at: None,
                revoked_at: None,
                last_used_at: None,
            },
            secret: "highly-secret".into(),
        };
        assert!(!format!("{response:?}").contains("highly-secret"));
    }
}
