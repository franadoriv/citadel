//! Human-admin-only API-key management endpoints.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::repository::{ApiKeyId, ApiKeyScope};
use crate::services::{ApiKeyMetadata, AuditEntry, ConsolePrincipal, CreateApiKeyRequest};
use crate::time::{Clock, SystemClock, TimestampMillis};

pub const API_KEYS_PATH: &str = "/console/v1/api-keys";
pub const API_KEY_DETAIL_PATH: &str = "/console/v1/api-keys/:id";
pub const API_KEY_ROTATE_PATH: &str = "/console/v1/api-keys/:id/rotate";
pub const API_KEY_REVOKE_PATH: &str = "/console/v1/api-keys/:id/revoke";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBody {
    name: String,
    scopes: Vec<ApiKeyScope>,
    expires_at: Option<TimestampMillis>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationBody {
    generation: u64,
}

impl GenerationBody {
    fn validate(self) -> Result<u64, ApiError> {
        if self.generation == 0 {
            Err(AppError::validation("generation must be at least 1").into())
        } else {
            Ok(self.generation)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyMetadataBody {
    #[serde(flatten)]
    key: ApiKeyMetadata,
    status: &'static str,
}

#[derive(Clone, Serialize)]
pub struct SecretBody {
    key: KeyMetadataBody,
    secret: String,
}

impl std::fmt::Debug for SecretBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretBody")
            .field("key", &self.key)
            .field("secret", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Serialize)]
pub struct KeysBody {
    keys: Vec<KeyMetadataBody>,
}

fn metadata(key: ApiKeyMetadata, now: TimestampMillis) -> KeyMetadataBody {
    let status = if key.revoked_at.is_some() {
        "revoked"
    } else if key.expires_at.is_some_and(|expiry| expiry <= now) {
        "expired"
    } else {
        "active"
    };
    KeyMetadataBody { key, status }
}

fn invalid_body(rejection: JsonRejection) -> ApiError {
    AppError::validation("invalid request body")
        .with_detail(rejection.body_text())
        .into()
}

fn parse_id(id: String) -> Result<ApiKeyId, ApiError> {
    ApiKeyId::new(id).map_err(Into::into)
}

pub(super) async fn create_handler(
    State(app): State<App>,
    principal: ConsolePrincipal,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Result<
    (
        StatusCode,
        [(header::HeaderName, &'static str); 1],
        Json<SecretBody>,
    ),
    ApiError,
> {
    app.metrics().record_http_request();
    principal.require_admin()?;
    let Json(body) = body.map_err(invalid_body)?;
    let now = SystemClock.now();
    let issued = app
        .api_keys()
        .create(
            CreateApiKeyRequest {
                name: body.name,
                scopes: body.scopes,
                expires_at: body.expires_at,
            },
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::for_principal(
        now,
        &principal,
        "api_keys.create",
        issued.key.id.as_str(),
        format!("created API key {}", issued.key.name),
    ));
    Ok((
        StatusCode::CREATED,
        [(header::CACHE_CONTROL, "no-store")],
        Json(SecretBody {
            key: metadata(issued.key, now),
            secret: issued.secret,
        }),
    ))
}

pub(super) async fn list_handler(
    State(app): State<App>,
    principal: ConsolePrincipal,
) -> Result<Json<KeysBody>, ApiError> {
    app.metrics().record_http_request();
    principal.require_admin()?;
    let now = SystemClock.now();
    Ok(Json(KeysBody {
        keys: app
            .api_keys()
            .list()
            .await?
            .into_iter()
            .map(|key| metadata(key, now))
            .collect(),
    }))
}

pub(super) async fn detail_handler(
    State(app): State<App>,
    principal: ConsolePrincipal,
    Path(id): Path<String>,
) -> Result<Json<KeyMetadataBody>, ApiError> {
    app.metrics().record_http_request();
    principal.require_admin()?;
    let id = parse_id(id)?;
    let key = app
        .api_keys()
        .get(&id)
        .await?
        .ok_or_else(|| AppError::not_found("API key not found"))?;
    Ok(Json(metadata(key, SystemClock.now())))
}

pub(super) async fn rotate_handler(
    State(app): State<App>,
    principal: ConsolePrincipal,
    Path(id): Path<String>,
    body: Result<Json<GenerationBody>, JsonRejection>,
) -> Result<([(header::HeaderName, &'static str); 1], Json<SecretBody>), ApiError> {
    app.metrics().record_http_request();
    principal.require_admin()?;
    let id = parse_id(id)?;
    let Json(body) = body.map_err(invalid_body)?;
    let generation = body.validate()?;
    let now = SystemClock.now();
    let issued = app.api_keys().rotate(&id, generation, now).await?;
    app.audit_log().record(AuditEntry::for_principal(
        now,
        &principal,
        "api_keys.rotate",
        id.as_str(),
        format!("rotated API key {}", issued.key.name),
    ));
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(SecretBody {
            key: metadata(issued.key, now),
            secret: issued.secret,
        }),
    ))
}

pub(super) async fn revoke_handler(
    State(app): State<App>,
    principal: ConsolePrincipal,
    Path(id): Path<String>,
    body: Result<Json<GenerationBody>, JsonRejection>,
) -> Result<Json<KeyMetadataBody>, ApiError> {
    app.metrics().record_http_request();
    principal.require_admin()?;
    let id = parse_id(id)?;
    let Json(body) = body.map_err(invalid_body)?;
    let generation = body.validate()?;
    let now = SystemClock.now();
    let key = app.api_keys().revoke(&id, generation, now).await?;
    app.audit_log().record(AuditEntry::for_principal(
        now,
        &principal,
        "api_keys.revoke",
        id.as_str(),
        format!("revoked API key {}", key.name),
    ));
    Ok(Json(metadata(key, now)))
}
