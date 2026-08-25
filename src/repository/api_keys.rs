//! Dedicated API-key persistence contract and in-memory reference adapter.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Public, non-secret API-key identifier carried in every V1 bearer.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApiKeyId(String);

impl ApiKeyId {
    pub const HEX_LEN: usize = 32;

    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        if value.len() != Self::HEX_LEN
            || !value
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(AppError::validation("invalid API key id"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ApiKeyId").field(&self.0).finish()
    }
}

impl fmt::Display for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// SHA-256 verifier. Its bytes are persistence-only and always redacted from Debug.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKeyVerifier([u8; 32]);

impl ApiKeyVerifier {
    #[must_use]
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    #[must_use]
    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub(crate) fn from_slice(bytes: &[u8]) -> AppResult<Self> {
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| AppError::internal("invalid API key verifier in repository"))?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for ApiKeyVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKeyVerifier([redacted])")
    }
}

/// Read-only capabilities supported by V1 API keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ApiKeyScope {
    #[serde(rename = "telemetry:read")]
    TelemetryRead,
    #[serde(rename = "config:read")]
    ConfigRead,
    #[serde(rename = "audit:read")]
    AuditRead,
    #[serde(rename = "errors:read")]
    ErrorsRead,
    #[serde(rename = "accounts:read")]
    AccountsRead,
    #[serde(rename = "groups:read")]
    GroupsRead,
    #[serde(rename = "runtime:read")]
    RuntimeRead,
    #[serde(rename = "matches:read")]
    MatchesRead,
    #[serde(rename = "storage:read")]
    StorageRead,
    #[serde(rename = "database:read")]
    DatabaseRead,
    #[serde(rename = "chat:read")]
    ChatRead,
    #[serde(rename = "notifications:read")]
    NotificationsRead,
    #[serde(rename = "leaderboards:read")]
    LeaderboardsRead,
    #[serde(rename = "tournaments:read")]
    TournamentsRead,
    #[serde(rename = "purchases:read")]
    PurchasesRead,
    #[serde(rename = "subscriptions:read")]
    SubscriptionsRead,
    // Appended, never inserted: `validate_scopes` requires a stored scope
    // vector to be strictly ascending by this derived `Ord`, so a variant added
    // in the middle would invalidate every vector already in the database.
    #[serde(rename = "logs:read")]
    LogsRead,
}

impl ApiKeyScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TelemetryRead => "telemetry:read",
            Self::ConfigRead => "config:read",
            Self::AuditRead => "audit:read",
            Self::ErrorsRead => "errors:read",
            Self::AccountsRead => "accounts:read",
            Self::GroupsRead => "groups:read",
            Self::RuntimeRead => "runtime:read",
            Self::MatchesRead => "matches:read",
            Self::StorageRead => "storage:read",
            Self::DatabaseRead => "database:read",
            Self::ChatRead => "chat:read",
            Self::NotificationsRead => "notifications:read",
            Self::LeaderboardsRead => "leaderboards:read",
            Self::TournamentsRead => "tournaments:read",
            Self::PurchasesRead => "purchases:read",
            Self::SubscriptionsRead => "subscriptions:read",
            Self::LogsRead => "logs:read",
        }
    }

    pub fn parse(value: &str) -> AppResult<Self> {
        match value {
            "telemetry:read" => Ok(Self::TelemetryRead),
            "config:read" => Ok(Self::ConfigRead),
            "audit:read" => Ok(Self::AuditRead),
            "errors:read" => Ok(Self::ErrorsRead),
            "accounts:read" => Ok(Self::AccountsRead),
            "groups:read" => Ok(Self::GroupsRead),
            "runtime:read" => Ok(Self::RuntimeRead),
            "matches:read" => Ok(Self::MatchesRead),
            "storage:read" => Ok(Self::StorageRead),
            "database:read" => Ok(Self::DatabaseRead),
            "chat:read" => Ok(Self::ChatRead),
            "notifications:read" => Ok(Self::NotificationsRead),
            "leaderboards:read" => Ok(Self::LeaderboardsRead),
            "tournaments:read" => Ok(Self::TournamentsRead),
            "purchases:read" => Ok(Self::PurchasesRead),
            "subscriptions:read" => Ok(Self::SubscriptionsRead),
            "logs:read" => Ok(Self::LogsRead),
            _ => Err(AppError::validation("unsupported API key scope")),
        }
    }
}

pub(crate) fn validate_scopes(scopes: &[ApiKeyScope]) -> AppResult<()> {
    if scopes.is_empty() || scopes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(AppError::internal("invalid API key scopes in repository"));
    }
    Ok(())
}

pub(crate) fn decode_scopes_json(value: &str) -> AppResult<Vec<ApiKeyScope>> {
    let scopes: Vec<ApiKeyScope> = serde_json::from_str(value)
        .map_err(|_| AppError::internal("invalid API key scopes in repository"))?;
    validate_scopes(&scopes)?;
    Ok(scopes)
}

pub(crate) fn decode_scope_names<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> AppResult<Vec<ApiKeyScope>> {
    let scopes = values
        .into_iter()
        .map(|value| {
            ApiKeyScope::parse(value)
                .map_err(|_| AppError::internal("invalid API key scopes in repository"))
        })
        .collect::<AppResult<Vec<_>>>()?;
    validate_scopes(&scopes)?;
    Ok(scopes)
}

/// Persisted credential metadata and verifier. Serialization DTOs must use `ApiKeyMetadata`.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredApiKey {
    pub id: ApiKeyId,
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub verifier: ApiKeyVerifier,
    pub generation: u64,
    pub created_at: TimestampMillis,
    pub expires_at: Option<TimestampMillis>,
    pub revoked_at: Option<TimestampMillis>,
    pub last_used_at: Option<TimestampMillis>,
}

pub(crate) fn validate_stored_key(key: &StoredApiKey) -> AppResult<()> {
    if key.name.is_empty()
        || key.name.len() > 128
        || key.name.trim() != key.name
        || key.generation == 0
        || key.expires_at.is_some_and(|at| at <= key.created_at)
        || key.revoked_at.is_some_and(|at| at < key.created_at)
        || key.last_used_at.is_some_and(|at| at < key.created_at)
    {
        return Err(AppError::internal("invalid API key in repository"));
    }
    validate_scopes(&key.scopes)
}

impl fmt::Debug for StoredApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredApiKey")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("scopes", &self.scopes)
            .field("generation", &self.generation)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("revoked_at", &self.revoked_at)
            .field("last_used_at", &self.last_used_at)
            .finish()
    }
}

#[async_trait]
pub trait ApiKeyRepository: Send + Sync {
    async fn create(&self, key: StoredApiKey) -> AppResult<()>;
    async fn get(&self, id: &ApiKeyId) -> AppResult<Option<StoredApiKey>>;
    async fn list(&self) -> AppResult<Vec<StoredApiKey>>;
    async fn rotate(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        verifier: ApiKeyVerifier,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey>;
    async fn revoke(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey>;
    async fn update_last_used(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<()>;
}

#[derive(Default)]
pub struct InMemoryApiKeyRepository {
    keys: Mutex<BTreeMap<ApiKeyId, StoredApiKey>>,
}

impl fmt::Debug for InMemoryApiKeyRepository {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryApiKeyRepository")
            .finish_non_exhaustive()
    }
}

impl InMemoryApiKeyRepository {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn clear_for_tests(&self) {
        self.lock().clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<ApiKeyId, StoredApiKey>> {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl ApiKeyRepository for InMemoryApiKeyRepository {
    async fn create(&self, key: StoredApiKey) -> AppResult<()> {
        validate_stored_key(&key)?;
        let mut keys = self.lock();
        if keys.contains_key(&key.id) {
            return Err(AppError::conflict("API key already exists"));
        }
        keys.insert(key.id.clone(), key);
        Ok(())
    }
    async fn get(&self, id: &ApiKeyId) -> AppResult<Option<StoredApiKey>> {
        Ok(self.lock().get(id).cloned())
    }
    async fn list(&self) -> AppResult<Vec<StoredApiKey>> {
        let mut keys = self.lock().values().cloned().collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(keys)
    }
    async fn rotate(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        verifier: ApiKeyVerifier,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey> {
        let mut keys = self.lock();
        let key = keys
            .get_mut(id)
            .ok_or_else(|| AppError::conflict("API key state changed"))?;
        if at < key.created_at
            || key.revoked_at.is_some()
            || key.expires_at.is_some_and(|expiry| expiry <= at)
            || key.generation != expected_generation
        {
            return Err(AppError::conflict("API key state changed"));
        }
        key.generation = key
            .generation
            .checked_add(1)
            .ok_or_else(|| AppError::internal("API key generation overflow"))?;
        key.verifier = verifier;
        Ok(key.clone())
    }
    async fn revoke(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey> {
        let mut keys = self.lock();
        let key = keys
            .get_mut(id)
            .ok_or_else(|| AppError::conflict("API key state changed"))?;
        if at < key.created_at || key.generation != expected_generation {
            return Err(AppError::conflict("API key state changed"));
        }
        if key.revoked_at.is_some() {
            return Ok(key.clone());
        }
        key.revoked_at = Some(at);
        Ok(key.clone())
    }
    async fn update_last_used(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<()> {
        if let Some(key) = self.lock().get_mut(id).filter(|key| {
            key.generation == expected_generation
                && at >= key.created_at
                && key.revoked_at.is_none()
                && key.expires_at.is_none_or(|expiry| expiry > at)
        }) {
            key.last_used_at = Some(key.last_used_at.map_or(at, |old| old.max(at)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_decode_is_strict_and_canonical() {
        assert_eq!(
            decode_scopes_json(r#"["telemetry:read","audit:read"]"#).expect("valid scopes"),
            vec![ApiKeyScope::TelemetryRead, ApiKeyScope::AuditRead]
        );
        for invalid in [
            r#"[]"#,
            r#"["telemetry:read","telemetry:read"]"#,
            r#"["audit:read","telemetry:read"]"#,
            r#"["telemetry:write"]"#,
            r#"{"scope":"telemetry:read"}"#,
        ] {
            assert!(decode_scopes_json(invalid).is_err(), "accepted {invalid}");
        }
    }
}
