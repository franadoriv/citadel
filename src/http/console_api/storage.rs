//! Console Storage browser.
//!
//! Operator-scope storage administration over the node's real
//! [`StorageRepository`](crate::repository::StorageRepository):
//!
//! - `GET /console/v1/storage` — every collection with its object count.
//! - `GET /console/v1/storage/{collection}` — paged object listing (optionally
//!   filtered to one owner), summaries only.
//! - `GET /console/v1/storage/{collection}/{key}` — one full object (value,
//!   version, permissions).
//! - `PUT` same — create/overwrite an object (admin, audited, optional
//!   version precondition).
//! - `DELETE` same — delete an object (admin, audited, optional version
//!   precondition).
//!
//! All operations run as [`Accessor::Runtime`]: the console is an operator
//! surface, so object permissions do not apply — the bearer token + role are
//! the gate. Ownership is addressed with an optional `user_id` query
//! parameter; absent means the system owner.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Json, extract::DefaultBodyLimit};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::storage::{
    Accessor, Collection, CollectionSummary, Cursor, Key, ListQuery, ObjectId, Owner, Permissions,
    Precondition, ReadPermission, StorageObject, StorageValue, UserId, Version, WritePermission,
    WriteRequest,
};
use crate::time::{Clock, SystemClock};

/// The Storage section route (collection scan).
pub const STORAGE_PATH: &str = "/console/v1/storage";

/// Object listing route pattern.
pub const STORAGE_COLLECTION_PATH: &str = "/console/v1/storage/:collection";

/// Single-object route pattern.
pub const STORAGE_OBJECT_PATH: &str = "/console/v1/storage/:collection/:key";

/// Storage object values can be bigger than ordinary console bodies; this
/// route-local cap replaces the router-wide one for the object routes.
pub(super) const MAX_STORAGE_BODY_BYTES: usize = 512 * 1024;

/// Default object-listing page size.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one object-listing page.
const MAX_LIMIT: usize = 200;

/// Resolve the addressed owner: `user_id` when present, else system.
fn owner_from(user_id: Option<&str>) -> Result<Owner, AppError> {
    match user_id {
        Some(id) => Ok(Owner::user(UserId::new(id)?)),
        None => Ok(Owner::System),
    }
}

/// Render an owner for responses and audit targets.
fn owner_label(owner: &Owner) -> Option<String> {
    match owner {
        Owner::System => None,
        Owner::User(id) => Some(id.as_str().to_string()),
    }
}

/// `collection/key` (+ owner) rendered for audit targets.
fn object_target(id: &ObjectId) -> String {
    match owner_label(&id.owner) {
        Some(user) => format!("{}/{} (user {user})", id.collection, id.key),
        None => format!("{}/{} (system)", id.collection, id.key),
    }
}

/// The JSON response for `GET /console/v1/storage`.
#[derive(Debug, Clone, Serialize)]
pub struct CollectionsResponse {
    /// Every collection with its total object count, name-ordered.
    pub collections: Vec<CollectionSummary>,
}

/// Query parameters for the object listing route.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Restrict to one owning user; absent lists across all owners.
    pub user_id: Option<String>,
    /// Page size (default 50, capped at 200).
    pub limit: Option<usize>,
    /// Opaque resume cursor from a previous page.
    pub cursor: Option<String>,
}

/// One object summary row in a listing page (no value payload).
#[derive(Debug, Clone, Serialize)]
pub struct ObjectSummary {
    /// Owning user id, or `null` for the system owner.
    pub user_id: Option<String>,
    /// Object key.
    pub key: String,
    /// Current version token.
    pub version: String,
    /// Read permission code (0/1/2).
    pub read_permission: u8,
    /// Write permission code (0/1).
    pub write_permission: u8,
}

/// The JSON response for the object listing route.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectsPage {
    /// The listed collection.
    pub collection: String,
    /// Object summaries in stable key order.
    pub items: Vec<ObjectSummary>,
    /// Cursor for the next page, when more objects remain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// Query parameters addressing a single object.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectParams {
    /// Owning user id; absent addresses the system owner.
    pub user_id: Option<String>,
    /// Optional version precondition for `DELETE` (ignored on `GET`).
    pub version: Option<String>,
}

/// One full object, as returned by the single-object `GET` and `PUT`.
#[derive(Debug, Clone, Serialize)]
pub struct ObjectBody {
    /// The collection name.
    pub collection: String,
    /// Owning user id, or `null` for the system owner.
    pub user_id: Option<String>,
    /// Object key.
    pub key: String,
    /// The stored JSON value.
    pub value: serde_json::Value,
    /// Current version token.
    pub version: String,
    /// Read permission code (0/1/2).
    pub read_permission: u8,
    /// Write permission code (0/1).
    pub write_permission: u8,
}

impl ObjectBody {
    fn from_object(object: StorageObject) -> Self {
        Self {
            collection: object.id.collection.as_str().to_string(),
            user_id: owner_label(&object.id.owner),
            key: object.id.key.as_str().to_string(),
            value: object.value.into_json(),
            version: object.version.as_str().to_string(),
            read_permission: object.permissions.read.code(),
            write_permission: object.permissions.write.code(),
        }
    }
}

/// The JSON body accepted by the object `PUT`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteBody {
    /// The JSON object to store.
    pub value: serde_json::Value,
    /// Read permission code (default 1 = owner read).
    #[serde(default = "default_read_permission")]
    pub read_permission: u8,
    /// Write permission code (default 1 = owner write).
    #[serde(default = "default_write_permission")]
    pub write_permission: u8,
    /// Optional optimistic-concurrency precondition: the write succeeds only
    /// if the current version matches.
    #[serde(default)]
    pub version: Option<String>,
}

fn default_read_permission() -> u8 {
    ReadPermission::OwnerRead.code()
}

fn default_write_permission() -> u8 {
    WritePermission::OwnerWrite.code()
}

/// `GET /console/v1/storage`: every collection with its object count.
pub(super) async fn collections_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
) -> Result<Json<CollectionsResponse>, ApiError> {
    app.metrics().record_http_request();
    let collections = app
        .backend()
        .storage_repository()
        .list_collections()
        .await?;
    Ok(Json(CollectionsResponse { collections }))
}

/// `GET /console/v1/storage/{collection}`: paged object summaries.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Path(collection): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<Json<ObjectsPage>, ApiError> {
    app.metrics().record_http_request();
    let collection = Collection::new(collection)?;
    let owner = match params.user_id.as_deref() {
        Some(id) => Some(Owner::user(UserId::new(id)?)),
        None => None,
    };
    let query = ListQuery {
        owner,
        collection: collection.clone(),
        limit: params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        // The token is opaque to the console; the repository validates it.
        cursor: params.cursor.map(Cursor::from_token),
    };
    let page = app
        .backend()
        .storage_repository()
        .list(&Accessor::Runtime, &query)
        .await?;
    Ok(Json(ObjectsPage {
        collection: collection.as_str().to_string(),
        items: page
            .items
            .into_iter()
            .map(|object| ObjectSummary {
                user_id: owner_label(&object.id.owner),
                key: object.id.key.as_str().to_string(),
                version: object.version.as_str().to_string(),
                read_permission: object.permissions.read.code(),
                write_permission: object.permissions.write.code(),
            })
            .collect(),
        next: page.next.map(|cursor| cursor.as_str().to_string()),
    }))
}

/// `GET /console/v1/storage/{collection}/{key}`: one full object.
pub(super) async fn get_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Path((collection, key)): Path<(String, String)>,
    Query(params): Query<ObjectParams>,
) -> Result<Json<ObjectBody>, ApiError> {
    app.metrics().record_http_request();
    let id = object_id(&collection, &key, params.user_id.as_deref())?;
    let object = app
        .backend()
        .storage_repository()
        .read(&Accessor::Runtime, &id)
        .await?
        .ok_or_else(|| AppError::not_found("storage object not found"))?;
    Ok(Json(ObjectBody::from_object(object)))
}

/// `PUT /console/v1/storage/{collection}/{key}`: create/overwrite (admin).
pub(super) async fn write_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((collection, key)): Path<(String, String)>,
    Query(params): Query<ObjectParams>,
    body: Result<Json<WriteBody>, JsonRejection>,
) -> Result<(StatusCode, Json<ObjectBody>), ApiError> {
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
    let id = object_id(&collection, &key, params.user_id.as_deref())?;
    let permissions = Permissions {
        read: ReadPermission::from_code(body.read_permission)?,
        write: WritePermission::from_code(body.write_permission)?,
    };
    let expected = match body.version {
        Some(version) => Precondition::Match(version_from_token(version)),
        None => Precondition::Any,
    };
    let request = WriteRequest::upsert(id.clone(), StorageValue::new(body.value)?, permissions)
        .expecting(expected);
    let object = app
        .backend()
        .storage_repository()
        .write(&Accessor::Runtime, request)
        .await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "storage.write",
        object_target(&id),
        format!("wrote version {}", object.version.as_str()),
    ));
    Ok((StatusCode::OK, Json(ObjectBody::from_object(object))))
}

/// `DELETE /console/v1/storage/{collection}/{key}`: delete (admin).
pub(super) async fn delete_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((collection, key)): Path<(String, String)>,
    Query(params): Query<ObjectParams>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let id = object_id(&collection, &key, params.user_id.as_deref())?;
    let expected = match params.version {
        Some(version) => Precondition::Match(version_from_token(version)),
        None => Precondition::Any,
    };
    app.backend()
        .storage_repository()
        .delete(&Accessor::Runtime, &id, expected)
        .await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.actor_id(),
        operator.role_label(),
        "storage.delete",
        object_target(&id),
        "deleted object",
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// Assemble a validated [`ObjectId`] from path/query input.
fn object_id(collection: &str, key: &str, user_id: Option<&str>) -> Result<ObjectId, AppError> {
    Ok(ObjectId::new(
        owner_from(user_id)?,
        Collection::new(collection)?,
        Key::new(key)?,
    ))
}

/// Re-wrap an operator-presented version token for a precondition.
///
/// `Version::from_token` is crate-internal; a token that never came from a
/// real object simply fails the `Match` precondition with a conflict.
fn version_from_token(token: String) -> Version {
    Version::from_token(token)
}

/// The route-local body limit layer for the object routes.
pub(super) fn body_limit() -> DefaultBodyLimit {
    DefaultBodyLimit::max(MAX_STORAGE_BODY_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_addressing_defaults_to_system() {
        assert_eq!(owner_from(None).expect("system"), Owner::System);
        let user = owner_from(Some("u-1")).expect("user");
        assert!(matches!(user, Owner::User(ref id) if id.as_str() == "u-1"));
        assert!(owner_from(Some("   ")).is_err(), "blank user id rejected");
    }

    #[test]
    fn audit_targets_name_collection_key_and_owner() {
        let system = object_id("saves", "slot-1", None).expect("id");
        assert_eq!(object_target(&system), "saves/slot-1 (system)");
        let user = object_id("saves", "slot-1", Some("u-9")).expect("id");
        assert_eq!(object_target(&user), "saves/slot-1 (user u-9)");
    }

    #[test]
    fn write_body_defaults_are_owner_private() {
        let body: WriteBody = serde_json::from_str(r#"{"value":{"hp":10}}"#).expect("parse");
        assert_eq!(body.read_permission, 1);
        assert_eq!(body.write_permission, 1);
        assert!(body.version.is_none());
        // Unknown fields are rejected at the boundary.
        assert!(serde_json::from_str::<WriteBody>(r#"{"value":{},"extra":1}"#).is_err());
    }

    #[test]
    fn storage_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&STORAGE_PATH));
        assert!(STORAGE_COLLECTION_PATH.starts_with(STORAGE_PATH));
        assert!(STORAGE_OBJECT_PATH.starts_with(STORAGE_PATH));
    }
}
