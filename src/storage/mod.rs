//! Storage domain contracts.
//!
//! These are the portable, database-agnostic domain types that storage and
//! future database-backed services depend on, following
//! `docs/architecture/database-abstraction.md`. Service and runtime code must
//! depend on these typed models and the
//! [`StorageRepository`](crate::repository::StorageRepository) trait rather than
//! on SQL, concrete database handles, or loosely structured JSON maps.
//!
//! Scope is deliberately the contract surface only: typed
//! owner/collection/key identity ([`Owner`], [`Collection`], [`Key`],
//! [`ObjectId`]), read/write permission levels ([`Permissions`]), an opaque
//! optimistic-concurrency token ([`Version`]), an opaque pagination [`Cursor`]
//! placeholder, and an [`Accessor`] that distinguishes the runtime-authoritative
//! path from client access. Postgres persistence, transactions, and timestamps
//! are introduced by the Phase 5 storage implementation task and must not change
//! the meaning of these types.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};

/// Maximum byte length for a [`Collection`] or [`Key`] label.
const MAX_LABEL_LEN: usize = 128;

/// A validated, opaque identity for an account that can own storage objects.
///
/// This is intentionally a string newtype rather than a concrete UUID so the
/// contract does not depend on a specific id representation; the identity track
/// and the persistence layer choose the concrete format.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UserId(String);

impl UserId {
    /// Construct a user id, rejecting empty/whitespace-only values.
    ///
    /// # Errors
    /// Returns [`ErrorCategory::Validation`](crate::error::ErrorCategory::Validation)
    /// if the value is empty or only whitespace.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(AppError::validation("user id must not be empty"));
        }
        Ok(Self(value))
    }

    /// The raw user id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The owner of a storage object.
///
/// [`Owner::System`] objects are server/runtime-owned (Nakama's "public/server"
/// ownership): they have no end-user owner and can only be created or mutated
/// through the runtime-authoritative path. [`Owner::User`] objects belong to a
/// specific account.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Owner {
    /// Server/runtime-owned object with no end-user owner.
    System,
    /// Object owned by a specific user account.
    User(UserId),
}

impl Owner {
    /// Convenience constructor for a user-owned object.
    #[must_use]
    pub fn user(id: UserId) -> Self {
        Self::User(id)
    }

    /// Whether this is a system/runtime-owned object.
    #[must_use]
    pub const fn is_system(&self) -> bool {
        matches!(self, Self::System)
    }

    /// A stable, ordering-friendly token used internally for pagination cursors.
    fn token(&self) -> String {
        match self {
            Self::System => "system".to_string(),
            Self::User(id) => format!("user:{}", id.as_str()),
        }
    }
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System => f.write_str("system"),
            Self::User(id) => write!(f, "user:{id}"),
        }
    }
}

/// Validate a collection/key label: non-empty, bounded, no control characters.
fn validate_label(kind: &str, value: &str) -> AppResult<()> {
    if value.is_empty() {
        return Err(AppError::validation(format!("{kind} must not be empty")));
    }
    if value.len() > MAX_LABEL_LEN {
        return Err(AppError::validation(format!(
            "{kind} must not exceed {MAX_LABEL_LEN} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(AppError::validation(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

/// A validated storage collection name (a namespace within an owner).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Collection(String);

impl Collection {
    /// Construct a collection name, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if the name is empty, too long, or contains
    /// control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate_label("collection", &value)?;
        Ok(Self(value))
    }

    /// The raw collection string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated storage key (unique within an owner+collection).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    /// Construct a key, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if the key is empty, too long, or contains
    /// control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate_label("key", &value)?;
        Ok(Self(value))
    }

    /// The raw key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The full identity of a stored object: `(owner, collection, key)`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectId {
    /// The owner of the object.
    pub owner: Owner,
    /// The collection namespace.
    pub collection: Collection,
    /// The key, unique within `owner`+`collection`.
    pub key: Key,
}

impl ObjectId {
    /// Assemble an object identity.
    #[must_use]
    pub fn new(owner: Owner, collection: Collection, key: Key) -> Self {
        Self {
            owner,
            collection,
            key,
        }
    }

    /// An opaque, ordering-stable token used by the in-memory repository to
    /// implement cursor pagination. Not part of the stable public API.
    pub(crate) fn cursor_token(&self) -> String {
        // `\u{1f}` (unit separator) is below printable characters, keeping the
        // composite ordering consistent with the field order. The repository
        // both sorts and paginates by this token, so the order is internally
        // consistent regardless of the chosen separator.
        format!(
            "{}\u{1f}{}\u{1f}{}",
            self.owner.token(),
            self.collection.as_str(),
            self.key.as_str()
        )
    }
}

/// A storage object value.
///
/// Values are JSON objects, mirroring the `jsonb` decision in
/// `docs/architecture/database-abstraction.md`. Opaque-bytes support with a
/// content type is an open question deferred to the persistence task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageValue(serde_json::Value);

impl StorageValue {
    /// Construct a value, requiring a top-level JSON object.
    ///
    /// # Errors
    /// Returns a validation error if `value` is not a JSON object.
    pub fn new(value: serde_json::Value) -> AppResult<Self> {
        if !value.is_object() {
            return Err(AppError::validation("storage value must be a JSON object"));
        }
        Ok(Self(value))
    }

    /// Borrow the underlying JSON.
    #[must_use]
    pub fn as_json(&self) -> &serde_json::Value {
        &self.0
    }

    /// Consume into the underlying JSON.
    #[must_use]
    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

/// Read permission level for a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadPermission {
    /// Only the runtime-authoritative path may read.
    NoRead,
    /// The runtime and the owning user may read.
    OwnerRead,
    /// Anyone (including unauthenticated/public callers) may read.
    PublicRead,
}

impl ReadPermission {
    /// Stable numeric code (mirrors Nakama's `0/1/2`).
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::NoRead => 0,
            Self::OwnerRead => 1,
            Self::PublicRead => 2,
        }
    }

    /// Parse a numeric read permission code.
    ///
    /// # Errors
    /// Returns a validation error for codes outside `0..=2`.
    pub fn from_code(code: u8) -> AppResult<Self> {
        match code {
            0 => Ok(Self::NoRead),
            1 => Ok(Self::OwnerRead),
            2 => Ok(Self::PublicRead),
            _ => Err(AppError::validation(
                "read permission code must be 0, 1, or 2",
            )),
        }
    }
}

/// Write permission level for a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WritePermission {
    /// Only the runtime-authoritative path may write.
    NoWrite,
    /// The runtime and the owning user may write.
    OwnerWrite,
}

impl WritePermission {
    /// Stable numeric code (mirrors Nakama's `0/1`).
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::NoWrite => 0,
            Self::OwnerWrite => 1,
        }
    }

    /// Parse a numeric write permission code.
    ///
    /// # Errors
    /// Returns a validation error for codes outside `0..=1`.
    pub fn from_code(code: u8) -> AppResult<Self> {
        match code {
            0 => Ok(Self::NoWrite),
            1 => Ok(Self::OwnerWrite),
            _ => Err(AppError::validation("write permission code must be 0 or 1")),
        }
    }
}

/// The read/write permissions attached to a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permissions {
    /// Read level.
    pub read: ReadPermission,
    /// Write level.
    pub write: WritePermission,
}

impl Permissions {
    /// Owner-private object: only the owner (and runtime) may read/write.
    #[must_use]
    pub const fn owner_private() -> Self {
        Self {
            read: ReadPermission::OwnerRead,
            write: WritePermission::OwnerWrite,
        }
    }

    /// Publicly readable, owner-writable object.
    #[must_use]
    pub const fn public_read() -> Self {
        Self {
            read: ReadPermission::PublicRead,
            write: WritePermission::OwnerWrite,
        }
    }

    /// Runtime-only object: no client read or write.
    #[must_use]
    pub const fn runtime_only() -> Self {
        Self {
            read: ReadPermission::NoRead,
            write: WritePermission::NoWrite,
        }
    }

    /// Whether `accessor` may read an object with this permission set, given the
    /// object's `owner`.
    #[must_use]
    pub fn can_read(&self, owner: &Owner, accessor: &Accessor) -> bool {
        match accessor {
            Accessor::Runtime => true,
            Accessor::User(user) => {
                self.read == ReadPermission::PublicRead
                    || (self.read == ReadPermission::OwnerRead
                        && matches!(owner, Owner::User(o) if o == user))
            }
            Accessor::Public => self.read == ReadPermission::PublicRead,
        }
    }

    /// Whether `accessor` may write/overwrite an object with this permission
    /// set, given the object's `owner`.
    #[must_use]
    pub fn can_write(&self, owner: &Owner, accessor: &Accessor) -> bool {
        match accessor {
            Accessor::Runtime => true,
            Accessor::User(user) => {
                self.write == WritePermission::OwnerWrite
                    && matches!(owner, Owner::User(o) if o == user)
            }
            Accessor::Public => false,
        }
    }
}

/// An opaque optimistic-concurrency token for a stored object.
///
/// Two writes that store identical content produce identical versions, which
/// lets clients use a previously read version as an `If-Match` precondition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Version(String);

impl Version {
    /// Compute the version of a value. Content-addressed for the in-memory
    /// contract; the persistence layer may choose a different stable scheme.
    #[must_use]
    pub(crate) fn of(value: &StorageValue) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let serialized = value.as_json().to_string();
        let mut hasher = DefaultHasher::new();
        serialized.hash(&mut hasher);
        Self(format!("{:016x}", hasher.finish()))
    }

    /// Re-wrap a previously stored version token (crate-internal).
    ///
    /// Used by persistence backends to hydrate a [`Version`] read back from
    /// storage. The token must have been produced by [`Version::of`] on the
    /// original value so the content-addressed identity is preserved across a
    /// round trip.
    #[must_use]
    pub(crate) fn from_token(token: String) -> Self {
        Self(token)
    }

    /// The raw version token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Optimistic-concurrency precondition for a write or delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// Unconditional upsert/delete.
    Any,
    /// Succeed only if the object does not already exist (create-only).
    MustNotExist,
    /// Succeed only if the current version equals this version.
    Match(Version),
}

/// A full stored object as returned by the repository.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageObject {
    /// Object identity.
    pub id: ObjectId,
    /// Object value.
    pub value: StorageValue,
    /// Current version token.
    pub version: Version,
    /// Read/write permissions.
    pub permissions: Permissions,
}

/// A request to create or overwrite a storage object.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteRequest {
    /// Target object identity.
    pub id: ObjectId,
    /// New value.
    pub value: StorageValue,
    /// Permissions to apply to the stored object.
    pub permissions: Permissions,
    /// Optimistic-concurrency precondition.
    pub expected: Precondition,
}

impl WriteRequest {
    /// Build an unconditional (upsert) write request.
    #[must_use]
    pub fn upsert(id: ObjectId, value: StorageValue, permissions: Permissions) -> Self {
        Self {
            id,
            value,
            permissions,
            expected: Precondition::Any,
        }
    }

    /// Set the optimistic-concurrency precondition.
    #[must_use]
    pub fn expecting(mut self, expected: Precondition) -> Self {
        self.expected = expected;
        self
    }
}

/// One bounded operation in an atomic storage batch.
///
/// Membership is owned so a batch can be assembled before it crosses the
/// repository boundary.  A batch never executes callbacks or scans storage.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomicBatchOperation {
    /// Write an object and replace its validated index memberships.
    Write {
        /// Actor performing this operation.
        accessor: Accessor,
        /// Object write and optimistic precondition.
        request: WriteRequest,
        /// Optional trusted index-membership decision.
        membership: Option<StorageIndexMembership>,
    },
    /// Delete an object and its index memberships.
    Delete {
        /// Actor performing this operation.
        accessor: Accessor,
        /// Object to remove.
        id: ObjectId,
        /// Optimistic precondition.
        expected: Precondition,
    },
}

impl AtomicBatchOperation {
    /// Identity touched by this operation.
    #[must_use]
    pub fn id(&self) -> &ObjectId {
        match self {
            Self::Write { request, .. } => &request.id,
            Self::Delete { id, .. } => id,
        }
    }
}

/// Result of an [`AtomicBatchOperation`] in request order.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomicBatchResult {
    /// A successfully written object.
    Written(StorageObject),
    /// A successful delete (including an idempotent missing delete).
    Deleted,
}

/// Who is performing a storage operation.
///
/// The runtime-authoritative path ([`Accessor::Runtime`]) bypasses permission
/// checks. Client paths ([`Accessor::User`], [`Accessor::Public`]) are subject
/// to the object's [`Permissions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Accessor {
    /// Server-side gamecode/runtime; authoritative, bypasses permission checks.
    Runtime,
    /// An authenticated end-user.
    User(UserId),
    /// An unauthenticated/public caller.
    Public,
}

impl Accessor {
    /// Whether this accessor may create a brand-new object for `owner`.
    ///
    /// Runtime may create for any owner (including [`Owner::System`]); a user may
    /// only create objects they own; public callers may not create.
    #[must_use]
    pub fn can_create(&self, owner: &Owner) -> bool {
        match self {
            Self::Runtime => true,
            Self::User(user) => matches!(owner, Owner::User(o) if o == user),
            Self::Public => false,
        }
    }
}

/// An opaque pagination cursor placeholder.
///
/// The encoding is intentionally private: callers treat it as opaque and pass a
/// previously returned cursor back to fetch the next page. The persistence layer
/// will replace the encoding without changing this contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cursor(String);

impl Cursor {
    /// Wrap an internal token as a cursor (crate-internal).
    pub(crate) fn from_token(token: String) -> Self {
        Self(token)
    }

    /// The opaque cursor string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A storage listing query.
#[derive(Debug, Clone, PartialEq)]
pub struct ListQuery {
    /// Restrict to a single owner, or `None` to list across owners (runtime).
    pub owner: Option<Owner>,
    /// Collection to list.
    pub collection: Collection,
    /// Maximum number of objects to return; must be greater than zero.
    pub limit: usize,
    /// Resume token from a previous page, if any.
    pub cursor: Option<Cursor>,
}

impl ListQuery {
    /// List a specific owner's collection.
    #[must_use]
    pub fn for_owner(owner: Owner, collection: Collection, limit: usize) -> Self {
        Self {
            owner: Some(owner),
            collection,
            limit,
            cursor: None,
        }
    }

    /// List a collection across all owners (runtime/admin scope).
    #[must_use]
    pub fn across_owners(collection: Collection, limit: usize) -> Self {
        Self {
            owner: None,
            collection,
            limit,
            cursor: None,
        }
    }

    /// Set the resume cursor.
    #[must_use]
    pub fn after(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// Maximum number of objects an indexed query may return in one call.
///
/// Indexed queries are intended for game-logic decisions, not bulk export. A
/// caller that needs an administrative scan must use an operator-only surface.
pub const MAX_INDEX_QUERY_LIMIT: usize = 100;

/// A validated operator-chosen name for a storage index.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StorageIndexName(String);

impl StorageIndexName {
    /// Build an index name suitable for stable configuration and safe physical
    /// index naming.
    ///
    /// Names are deliberately narrower than collection labels: they must be
    /// ASCII identifier-like tokens so they remain portable across every SQL
    /// backend and configuration format Citadel supports.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 40
            && value.bytes().enumerate().all(|(index, byte)| match byte {
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => true,
                b'0'..=b'9' => index != 0,
                _ => false,
            });
        if !valid {
            return Err(AppError::validation(
                "storage index name must be 1..=40 ASCII letters, digits, or underscores and must not start with a digit",
            ));
        }
        Ok(Self(value))
    }

    /// The configured index name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StorageIndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A top-level JSON object field that an index may filter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StorageIndexField(String);

impl StorageIndexField {
    /// Construct a portable top-level JSON field name.
    ///
    /// Nested JSON paths, full-text tokenization, and arbitrary expressions are
    /// intentionally not part of the first indexed-query contract. Keeping the
    /// selector to one identifier makes the SQL generated by durable backends
    /// safe and lets SQLite/PostgreSQL use matching expression indexes.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 64
            && value.bytes().enumerate().all(|(index, byte)| match byte {
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => true,
                b'0'..=b'9' => index != 0,
                _ => false,
            });
        if !valid {
            return Err(AppError::validation(
                "storage index field must be 1..=64 ASCII letters, digits, or underscores and must not start with a digit",
            ));
        }
        Ok(Self(value))
    }

    /// The top-level JSON field name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StorageIndexField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A scalar JSON value usable as an indexed equality filter.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageIndexValue {
    /// A JSON string.
    String(String),
    /// A signed JSON integer.
    Integer(i64),
    /// A finite JSON floating-point number.
    Float(f64),
    /// A JSON boolean.
    Boolean(bool),
}

impl StorageIndexValue {
    /// Convert a JSON scalar to the portable indexed-query value domain.
    ///
    /// Arrays, objects, null, and unsigned integers that cannot round-trip to
    /// `i64` are rejected rather than silently changing equality semantics
    /// between SQLite and PostgreSQL.
    pub fn from_json(value: &serde_json::Value) -> AppResult<Self> {
        match value {
            serde_json::Value::String(value) if value.len() <= 512 => {
                Ok(Self::String(value.clone()))
            }
            serde_json::Value::String(_) => Err(AppError::validation(
                "storage index string filter must not exceed 512 bytes",
            )),
            serde_json::Value::Number(value) if value.is_i64() => value
                .as_i64()
                .map(Self::Integer)
                .ok_or_else(|| AppError::validation("invalid storage index integer filter")),
            serde_json::Value::Number(value) if value.is_u64() => value
                .as_u64()
                .and_then(|value| i64::try_from(value).ok())
                .map(Self::Integer)
                .ok_or_else(|| {
                    AppError::validation(
                        "storage index unsigned integer filter exceeds supported range",
                    )
                }),
            serde_json::Value::Number(value) => value
                .as_f64()
                .filter(|value| value.is_finite())
                .map(Self::Float)
                .ok_or_else(|| AppError::validation("invalid storage index number filter")),
            serde_json::Value::Bool(value) => Ok(Self::Boolean(*value)),
            _ => Err(AppError::validation(
                "storage index filters must contain only string, number, or boolean values",
            )),
        }
    }

    /// Render the value exactly as PostgreSQL's `jsonb ->>` extractor does.
    #[must_use]
    pub(crate) fn postgres_text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
        }
    }

    /// Whether this value equals a top-level JSON scalar under the portable
    /// equality contract.
    #[must_use]
    pub(crate) fn matches_json(&self, value: Option<&serde_json::Value>) -> bool {
        value
            .and_then(|value| Self::from_json(value).ok())
            .is_some_and(|actual| actual == *self)
    }
}

/// Operator-declared definition of one physical storage index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndexDefinition {
    name: StorageIndexName,
    collection: Collection,
    key: Option<Key>,
    fields: Vec<StorageIndexField>,
}

impl StorageIndexDefinition {
    /// Validate and construct one storage-index definition.
    ///
    /// At least one unique indexed field is required. The optional key narrows
    /// the index to one well-known object key inside the collection.
    pub fn new(
        name: StorageIndexName,
        collection: Collection,
        key: Option<Key>,
        fields: Vec<StorageIndexField>,
    ) -> AppResult<Self> {
        if fields.is_empty() {
            return Err(AppError::validation(
                "storage index must declare at least one field",
            ));
        }
        let unique: BTreeSet<_> = fields.iter().collect();
        if unique.len() != fields.len() {
            return Err(AppError::validation(
                "storage index fields must not contain duplicates",
            ));
        }
        Ok(Self {
            name,
            collection,
            key,
            fields,
        })
    }

    /// The configured index name.
    #[must_use]
    pub fn name(&self) -> &StorageIndexName {
        &self.name
    }

    /// The collection the index covers.
    #[must_use]
    pub fn collection(&self) -> &Collection {
        &self.collection
    }

    /// The optional object-key restriction.
    #[must_use]
    pub fn key(&self) -> Option<&Key> {
        self.key.as_ref()
    }

    /// The JSON fields indexed by this definition, in declared order.
    #[must_use]
    pub fn fields(&self) -> &[StorageIndexField] {
        &self.fields
    }

    /// Whether a field is declared by this index.
    #[must_use]
    pub fn contains_field(&self, field: &StorageIndexField) -> bool {
        self.fields.contains(field)
    }

    /// Whether this definition covers `id`, regardless of the object's JSON
    /// value or permissions.
    #[must_use]
    pub fn matches_object(&self, id: &ObjectId) -> bool {
        id.collection == self.collection && self.key.as_ref().is_none_or(|key| id.key == *key)
    }

    /// A safe, deterministic physical index identifier for SQL backends.
    ///
    /// The digest includes the full definition, so changing declared fields
    /// creates a replacement physical index instead of reusing a stale one.
    /// The old index is harmless and can be reclaimed during an operator's
    /// schema-maintenance window; it never changes query results.
    #[must_use]
    pub(crate) fn physical_name(&self) -> String {
        let mut hasher = Sha256::new();
        for token in std::iter::once(self.name.as_str())
            .chain(std::iter::once(self.collection.as_str()))
            .chain(self.key.iter().map(Key::as_str))
            .chain(self.fields.iter().map(StorageIndexField::as_str))
        {
            hasher.update((token.len() as u64).to_be_bytes());
            hasher.update(token.as_bytes());
        }
        let digest = hasher.finalize();
        let suffix = digest[..10]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("citadel_stidx_{suffix}")
    }
}

/// Validated include/exclude decisions for every configured index that matches
/// one storage write.
///
/// The runtime produces this language-neutral value after its registered
/// callbacks run. Repository implementations validate it against their own
/// installed definitions, then update the durable index projection inside the
/// same transaction as the storage object. Runtime closures never cross that
/// repository boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndexMembership {
    candidates: BTreeSet<StorageIndexName>,
    included: BTreeSet<StorageIndexName>,
}

impl StorageIndexMembership {
    /// Include every configured candidate index.
    #[must_use]
    pub fn include_all(candidates: BTreeSet<StorageIndexName>) -> Self {
        Self {
            included: candidates.clone(),
            candidates,
        }
    }

    /// Build explicit callback decisions.
    ///
    /// An included name must also appear in `candidates`; otherwise a runtime
    /// could accidentally claim membership for an unrelated index.
    pub fn new(
        candidates: BTreeSet<StorageIndexName>,
        included: BTreeSet<StorageIndexName>,
    ) -> AppResult<Self> {
        if !included.is_subset(&candidates) {
            return Err(AppError::validation(
                "storage index membership includes an index that is not a write candidate",
            ));
        }
        Ok(Self {
            candidates,
            included,
        })
    }

    /// All configured candidate names for the write.
    #[must_use]
    pub fn candidates(&self) -> &BTreeSet<StorageIndexName> {
        &self.candidates
    }

    /// The candidate names the callback accepted.
    #[must_use]
    pub fn included(&self) -> &BTreeSet<StorageIndexName> {
        &self.included
    }
}

/// One bounded equality query over an operator-declared storage index.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageIndexQuery {
    index: StorageIndexDefinition,
    filters: BTreeMap<StorageIndexField, StorageIndexValue>,
    limit: usize,
}

impl StorageIndexQuery {
    /// Build a validated indexed query.
    ///
    /// Filters may be empty to list the bounded contents of an index, but every
    /// supplied field must appear in the definition. Ordering is always the
    /// storage identity `(owner, collection, key)` order for portable results.
    pub fn new(
        index: StorageIndexDefinition,
        filters: BTreeMap<StorageIndexField, StorageIndexValue>,
        limit: usize,
    ) -> AppResult<Self> {
        if limit == 0 || limit > MAX_INDEX_QUERY_LIMIT {
            return Err(AppError::validation(format!(
                "storage index query limit must be between 1 and {MAX_INDEX_QUERY_LIMIT}",
            )));
        }
        if let Some(field) = filters.keys().find(|field| !index.contains_field(field)) {
            return Err(AppError::validation(format!(
                "storage index query field `{field}` is not declared by index `{}`",
                index.name()
            )));
        }
        Ok(Self {
            index,
            filters,
            limit,
        })
    }

    /// Build a query from the JSON-object filter shape used by trusted runtimes.
    pub fn from_json_filters(
        index: StorageIndexDefinition,
        filters: &serde_json::Map<String, serde_json::Value>,
        limit: usize,
    ) -> AppResult<Self> {
        let filters = filters
            .iter()
            .map(|(field, value)| {
                Ok((
                    StorageIndexField::new(field)?,
                    StorageIndexValue::from_json(value)?,
                ))
            })
            .collect::<AppResult<BTreeMap<_, _>>>()?;
        Self::new(index, filters, limit)
    }

    /// The definition this query is authorized to use.
    #[must_use]
    pub fn index(&self) -> &StorageIndexDefinition {
        &self.index
    }

    /// Equality filters, in stable field-name order.
    #[must_use]
    pub fn filters(&self) -> &BTreeMap<StorageIndexField, StorageIndexValue> {
        &self.filters
    }

    /// Maximum result count.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

/// A page of results plus an optional cursor to fetch the next page.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    /// Items in this page.
    pub items: Vec<T>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next: Option<Cursor>,
}

/// One collection name with its total object count.
///
/// Produced by the administrative
/// [`list_collections`](crate::repository::StorageRepository::list_collections)
/// scan; counts include objects the counting caller could not read, so this
/// type must only flow to operator-gated surfaces (the admin console).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectionSummary {
    /// The collection name.
    pub collection: Collection,
    /// Total stored objects in the collection, across all owners.
    pub objects: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user(id: &str) -> UserId {
        UserId::new(id).expect("valid user id")
    }

    #[test]
    fn label_validation_rejects_empty_long_and_control() {
        assert!(Collection::new("").is_err());
        assert!(Key::new("").is_err());
        assert!(Collection::new("a".repeat(MAX_LABEL_LEN + 1)).is_err());
        assert!(Key::new("with\nnewline").is_err());
        assert!(Collection::new("saves").is_ok());
        assert!(Key::new("slot-1").is_ok());
    }

    #[test]
    fn user_id_rejects_blank() {
        assert!(UserId::new("   ").is_err());
        assert!(UserId::new("u-1").is_ok());
    }

    #[test]
    fn storage_value_requires_json_object() {
        assert!(StorageValue::new(json!({"score": 1})).is_ok());
        assert!(StorageValue::new(json!([1, 2, 3])).is_err());
        assert!(StorageValue::new(json!("string")).is_err());
        assert!(StorageValue::new(json!(42)).is_err());
    }

    #[test]
    fn permission_codes_round_trip() {
        for code in 0u8..=2 {
            let perm = ReadPermission::from_code(code).expect("valid read code");
            assert_eq!(perm.code(), code);
        }
        for code in 0u8..=1 {
            let perm = WritePermission::from_code(code).expect("valid write code");
            assert_eq!(perm.code(), code);
        }
        assert!(ReadPermission::from_code(3).is_err());
        assert!(WritePermission::from_code(2).is_err());
    }

    #[test]
    fn version_is_content_addressed() {
        let a = StorageValue::new(json!({"hp": 10})).expect("value");
        let b = StorageValue::new(json!({"hp": 10})).expect("value");
        let c = StorageValue::new(json!({"hp": 11})).expect("value");
        assert_eq!(Version::of(&a), Version::of(&b));
        assert_ne!(Version::of(&a), Version::of(&c));
    }

    #[test]
    fn runtime_can_always_read_and_write() {
        let perms = Permissions::runtime_only();
        let owner = Owner::user(user("u-1"));
        assert!(perms.can_read(&owner, &Accessor::Runtime));
        assert!(perms.can_write(&owner, &Accessor::Runtime));
    }

    #[test]
    fn owner_read_visible_only_to_owner_and_runtime() {
        let perms = Permissions::owner_private();
        let owner = Owner::user(user("u-1"));
        assert!(perms.can_read(&owner, &Accessor::User(user("u-1"))));
        assert!(!perms.can_read(&owner, &Accessor::User(user("u-2"))));
        assert!(!perms.can_read(&owner, &Accessor::Public));
    }

    #[test]
    fn public_read_visible_to_everyone() {
        let perms = Permissions::public_read();
        let owner = Owner::user(user("u-1"));
        assert!(perms.can_read(&owner, &Accessor::User(user("u-2"))));
        assert!(perms.can_read(&owner, &Accessor::Public));
    }

    #[test]
    fn only_owner_can_write_via_client_path() {
        let perms = Permissions::owner_private();
        let owner = Owner::user(user("u-1"));
        assert!(perms.can_write(&owner, &Accessor::User(user("u-1"))));
        assert!(!perms.can_write(&owner, &Accessor::User(user("u-2"))));
        assert!(!perms.can_write(&owner, &Accessor::Public));
    }

    #[test]
    fn create_authorization_matches_owner() {
        assert!(Accessor::Runtime.can_create(&Owner::System));
        assert!(Accessor::User(user("u-1")).can_create(&Owner::user(user("u-1"))));
        assert!(!Accessor::User(user("u-1")).can_create(&Owner::user(user("u-2"))));
        assert!(!Accessor::User(user("u-1")).can_create(&Owner::System));
        assert!(!Accessor::Public.can_create(&Owner::System));
    }

    #[test]
    fn storage_index_definition_rejects_unsafe_and_duplicate_fields() {
        assert!(StorageIndexName::new("bad-name").is_err());
        assert!(StorageIndexField::new("nested.path").is_err());
        let field = StorageIndexField::new("score").expect("field");
        let result = StorageIndexDefinition::new(
            StorageIndexName::new("profiles_by_score").expect("name"),
            Collection::new("profiles").expect("collection"),
            None,
            vec![field.clone(), field],
        );
        assert!(result.is_err());
    }

    #[test]
    fn storage_index_query_accepts_only_declared_scalar_filters_and_bounded_limits() {
        let definition = StorageIndexDefinition::new(
            StorageIndexName::new("profiles_by_score").expect("name"),
            Collection::new("profiles").expect("collection"),
            None,
            vec![StorageIndexField::new("score").expect("field")],
        )
        .expect("definition");
        let filters = serde_json::json!({"score": 7});
        let query = StorageIndexQuery::from_json_filters(
            definition.clone(),
            filters.as_object().expect("object"),
            10,
        )
        .expect("query");
        assert_eq!(query.limit(), 10);
        assert!(
            StorageIndexQuery::from_json_filters(
                definition.clone(),
                serde_json::json!({"unknown": 7})
                    .as_object()
                    .expect("object"),
                10,
            )
            .is_err()
        );
        assert!(
            StorageIndexQuery::from_json_filters(
                definition,
                serde_json::json!({"score": {"not": "scalar"}})
                    .as_object()
                    .expect("object"),
                MAX_INDEX_QUERY_LIMIT + 1,
            )
            .is_err()
        );
    }
}
