//! Postgres [`StorageRepository`] implementation.
//!
//! Every Postgres-specific choice (the `jsonb` value column, the composite
//! `(owner_kind, owner_id, collection, object_key)` key, `COLLATE "C"` ordering,
//! `pg_advisory_xact_lock`) stays behind this file.
//!
//! # Semantics parity
//!
//! [`PgStorageRepository`] reproduces [`InMemoryStorageRepository`](
//! crate::repository::InMemoryStorageRepository) exactly: permission checks run
//! before precondition checks on existing objects; `can_create` runs before the
//! precondition on absent objects; reads and lists filter by permission in SQL so
//! unreadable objects are indistinguishable from absent ones; and versions are
//! content-addressed via [`Version::of`] (identical content yields an identical
//! version), never recomputed from what Postgres echoes back.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::Acquire;
use sqlx::postgres::{PgConnection, PgRow};

use crate::config::PgFlavor;
use crate::error::{AppError, AppResult};
use crate::repository::{StorageRepository, check_precondition};
use crate::storage::{
    Accessor, Collection, CollectionSummary, Cursor, Key, ListQuery, ObjectId, Owner, Page,
    Permissions, Precondition, ReadPermission, StorageIndexDefinition, StorageIndexMembership,
    StorageIndexQuery, StorageIndexValue, StorageObject, StorageValue, UserId, Version,
    WritePermission, WriteRequest,
};

use super::{PgExecutor, db_err, get, tx_closed};

/// Opaque cursor scheme tag. Bumping this string invalidates old cursors.
const CURSOR_PREFIX: &str = "pg-storage-v1:";

// --- SQL (runtime-checked; never the compile-time `query!` macro) -----------

/// Read a single object, filtered by the accessor's read permission.
const READ_SQL: &str = "\
SELECT owner_kind, owner_id, collection, object_key, \
       value, version, read_permission, write_permission \
FROM storage_objects \
WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4 \
  AND ( \
       $5::text = 'runtime' \
    OR read_permission = 2 \
    OR ($5::text = 'user' AND read_permission = 1 AND owner_kind = 1 AND owner_id = $6) \
  )";

/// Lock the current row (if any) for a write/delete decision.
const SELECT_FOR_UPDATE_SQL: &str = "\
SELECT version, read_permission, write_permission \
FROM storage_objects \
WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4 \
FOR UPDATE";

/// Overwrite an existing object.
const UPDATE_SQL: &str = "\
UPDATE storage_objects \
SET value = $5, version = $6, read_permission = $7, write_permission = $8, updated_at = now() \
WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4";

/// Insert a brand-new object.
const INSERT_SQL: &str = "\
INSERT INTO storage_objects \
(owner_kind, owner_id, collection, object_key, value, version, read_permission, write_permission) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

/// Delete an object by identity.
const DELETE_SQL: &str = "\
DELETE FROM storage_objects \
WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4";

/// Administrative collection scan: every collection name with its object
/// count, regardless of permissions (operator-gated caller only).
const LIST_COLLECTIONS_SQL: &str = "\
SELECT collection, COUNT(*) AS objects \
FROM storage_objects \
GROUP BY collection \
ORDER BY collection ASC";

/// List a collection with permission filtering and keyset pagination.
const LIST_SQL: &str = "\
SELECT owner_kind, owner_id, collection, object_key, \
       value, version, read_permission, write_permission \
FROM storage_objects \
WHERE collection = $1 \
  AND (NOT $2::boolean OR (owner_kind = $3 AND owner_id = $4)) \
  AND ( \
       $5::text = 'runtime' \
    OR read_permission = 2 \
    OR ($5::text = 'user' AND read_permission = 1 AND owner_kind = 1 AND owner_id = $6) \
  ) \
  AND (NOT $7::boolean OR (owner_kind, owner_id, object_key) > ($8::smallint, $9::text, $10::text)) \
ORDER BY owner_kind ASC, owner_id ASC, object_key ASC \
LIMIT $11";

// --- Mapping helpers --------------------------------------------------------

/// Map an [`Owner`] to its `(owner_kind, owner_id)` column pair.
///
/// `Owner::System` is `(0, "")`; `Owner::User(id)` is `(1, id)`. The empty string
/// (never a valid user id) keeps the composite primary key free of the
/// NULL-is-distinct pitfall.
fn owner_columns(owner: &Owner) -> (i16, String) {
    match owner {
        Owner::System => (0, String::new()),
        Owner::User(id) => (1, id.as_str().to_string()),
    }
}

/// Rebuild an [`Owner`] from its column pair.
fn owner_from_columns(kind: i16, id: &str) -> AppResult<Owner> {
    match kind {
        0 => Ok(Owner::System),
        1 => Ok(Owner::user(UserId::new(id)?)),
        other => Err(AppError::internal(format!(
            "invalid owner_kind {other} in storage row"
        ))),
    }
}

/// Map an [`Accessor`] to `(kind, user_id)` bind values for the SQL predicates.
fn accessor_columns(accessor: &Accessor) -> (&'static str, String) {
    match accessor {
        Accessor::Runtime => ("runtime", String::new()),
        Accessor::User(user) => ("user", user.as_str().to_string()),
        Accessor::Public => ("public", String::new()),
    }
}

/// Deterministic 64-bit advisory-lock key for an object identity.
///
/// Serializing the write/delete of one object through `pg_advisory_xact_lock`
/// closes the absent-row race the plain `SELECT ... FOR UPDATE` cannot (two
/// concurrent creators both observing absence). Length-prefixing each field keeps
/// the hash injective across field boundaries.
fn advisory_key(id: &ObjectId) -> i64 {
    let (kind, owner_id) = owner_columns(&id.owner);
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, &kind.to_be_bytes());
    hash_field(&mut hasher, owner_id.as_bytes());
    hash_field(&mut hasher, id.collection.as_str().as_bytes());
    hash_field(&mut hasher, id.key.as_str().as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// Decode a full object row into a domain [`StorageObject`].
fn row_to_object(row: &PgRow) -> AppResult<StorageObject> {
    let owner_kind: i16 = get(row, "owner_kind")?;
    let owner_id: String = get(row, "owner_id")?;
    let collection: String = get(row, "collection")?;
    let object_key: String = get(row, "object_key")?;
    let value: serde_json::Value = get(row, "value")?;
    let version: String = get(row, "version")?;
    let read_permission: i16 = get(row, "read_permission")?;
    let write_permission: i16 = get(row, "write_permission")?;

    let owner = owner_from_columns(owner_kind, &owner_id)?;
    let id = ObjectId::new(owner, Collection::new(collection)?, Key::new(object_key)?);
    Ok(StorageObject {
        id,
        value: StorageValue::new(value)?,
        version: Version::from_token(version),
        permissions: permissions_from_codes(read_permission, write_permission)?,
    })
}

/// The existing version + permissions needed to decide a write/delete.
struct ExistingObject {
    version: Version,
    permissions: Permissions,
}

fn existing_from_row(row: &PgRow) -> AppResult<ExistingObject> {
    let version: String = get(row, "version")?;
    let read_permission: i16 = get(row, "read_permission")?;
    let write_permission: i16 = get(row, "write_permission")?;
    Ok(ExistingObject {
        version: Version::from_token(version),
        permissions: permissions_from_codes(read_permission, write_permission)?,
    })
}

fn permissions_from_codes(read: i16, write: i16) -> AppResult<Permissions> {
    let read =
        u8::try_from(read).map_err(|_| AppError::internal("read_permission out of range"))?;
    let write =
        u8::try_from(write).map_err(|_| AppError::internal("write_permission out of range"))?;
    Ok(Permissions {
        read: ReadPermission::from_code(read)?,
        write: WritePermission::from_code(write)?,
    })
}

/// The typed payload behind an opaque list [`Cursor`].
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CursorPayload {
    collection: String,
    owner_kind: i16,
    owner_id: String,
    key: String,
}

/// Encode the keyset position of the last returned object as an opaque cursor.
fn encode_cursor(object: &StorageObject) -> Cursor {
    let (owner_kind, owner_id) = owner_columns(&object.id.owner);
    let payload = CursorPayload {
        collection: object.id.collection.as_str().to_string(),
        owner_kind,
        owner_id,
        key: object.id.key.as_str().to_string(),
    };
    let json = serde_json::to_string(&payload).unwrap_or_default();
    Cursor::from_token(format!("{CURSOR_PREFIX}{json}"))
}

/// Decode an opaque cursor back into its keyset position.
fn decode_cursor(cursor: &Cursor) -> AppResult<CursorPayload> {
    let rest = cursor
        .as_str()
        .strip_prefix(CURSOR_PREFIX)
        .ok_or_else(|| AppError::validation("invalid storage cursor"))?;
    serde_json::from_str(rest)
        .map_err(|e| AppError::validation("invalid storage cursor").with_detail(e.to_string()))
}

/// The Postgres [`StorageRepository`] implementation.
pub struct PgStorageRepository {
    executor: PgExecutor,
    flavor: PgFlavor,
}

impl PgStorageRepository {
    /// Bind a storage repository to an execution handle (pool or transaction)
    /// and the PostgreSQL-wire dialect flavor it runs against.
    ///
    /// The `flavor` decides whether the per-object advisory lock is taken:
    /// standard PostgreSQL uses `pg_advisory_xact_lock`; CockroachDB skips it
    /// (its default `SERIALIZABLE` isolation plus the primary-key constraint
    /// already close the absent-row race, and CockroachDB does not implement the
    /// function).
    pub(super) fn new(executor: PgExecutor, flavor: PgFlavor) -> Self {
        Self { executor, flavor }
    }
}

#[async_trait]
impl StorageRepository for PgStorageRepository {
    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                read_conn(&mut conn, accessor, id).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                read_conn(&mut *tx, accessor, id).await
            }
        }
    }

    async fn write(&self, accessor: &Accessor, request: WriteRequest) -> AppResult<StorageObject> {
        self.write_indexed(accessor, request, None).await
    }

    async fn write_indexed(
        &self,
        accessor: &Accessor,
        request: WriteRequest,
        membership: Option<&StorageIndexMembership>,
    ) -> AppResult<StorageObject> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                match write_indexed_conn(&mut tx, accessor, request, membership, self.flavor).await
                {
                    Ok(object) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(object)
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
                write_indexed_conn(&mut *tx, accessor, request, membership, self.flavor).await
            }
        }
    }

    async fn delete(
        &self,
        accessor: &Accessor,
        id: &ObjectId,
        expected: Precondition,
    ) -> AppResult<()> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                match delete_conn(&mut tx, accessor, id, &expected, self.flavor).await {
                    Ok(()) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(())
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
                delete_conn(&mut *tx, accessor, id, &expected, self.flavor).await
            }
        }
    }

    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_conn(&mut conn, accessor, query).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_conn(&mut *tx, accessor, query).await
            }
        }
    }

    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()> {
        let sql = create_index_sql(index);
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                sqlx::query(&sql)
                    .execute(&mut *conn)
                    .await
                    .map_err(db_err)?;
                let mut tx = conn.begin().await.map_err(db_err)?;
                match install_index_projection_conn(&mut tx, index).await {
                    Ok(()) => tx.commit().await.map_err(db_err)?,
                    Err(error) => {
                        let _ = tx.rollback().await;
                        return Err(error);
                    }
                }
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                sqlx::query(&sql).execute(&mut **tx).await.map_err(db_err)?;
                install_index_projection_conn(&mut *tx, index).await?;
            }
        }
        Ok(())
    }

    async fn query_index(
        &self,
        accessor: &Accessor,
        query: &StorageIndexQuery,
    ) -> AppResult<Vec<StorageObject>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                query_index_conn(&mut conn, accessor, query).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                query_index_conn(&mut *tx, accessor, query).await
            }
        }
    }

    async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>> {
        match &self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_collections_conn(&mut conn).await
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_collections_conn(&mut *tx).await
            }
        }
    }
}

// --- Statement bodies (executor-agnostic over a single connection) ----------

async fn list_collections_conn(conn: &mut PgConnection) -> AppResult<Vec<CollectionSummary>> {
    let rows = sqlx::query(LIST_COLLECTIONS_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter()
        .map(|row| {
            let collection: String = get(row, "collection")?;
            let objects: i64 = get(row, "objects")?;
            Ok(CollectionSummary {
                collection: Collection::new(collection)?,
                objects: u64::try_from(objects).unwrap_or_default(),
            })
        })
        .collect()
}

async fn read_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    id: &ObjectId,
) -> AppResult<Option<StorageObject>> {
    let (owner_kind, owner_id) = owner_columns(&id.owner);
    let (accessor_kind, accessor_id) = accessor_columns(accessor);
    let row = sqlx::query(READ_SQL)
        .bind(owner_kind)
        .bind(owner_id)
        .bind(id.collection.as_str())
        .bind(id.key.as_str())
        .bind(accessor_kind)
        .bind(accessor_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_object).transpose()
}

async fn write_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    request: WriteRequest,
    flavor: PgFlavor,
) -> AppResult<StorageObject> {
    let (owner_kind, owner_id) = owner_columns(&request.id.owner);
    let version = Version::of(&request.value);
    let read_code = i16::from(request.permissions.read.code());
    let write_code = i16::from(request.permissions.write.code());

    lock_object(&mut *conn, &request.id, flavor).await?;
    let existing = sqlx::query(SELECT_FOR_UPDATE_SQL)
        .bind(owner_kind)
        .bind(owner_id.as_str())
        .bind(request.id.collection.as_str())
        .bind(request.id.key.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;

    let statement = match existing {
        Some(row) => {
            let existing = existing_from_row(&row)?;
            if !existing.permissions.can_write(&request.id.owner, accessor) {
                return Err(AppError::permission("write denied on existing object"));
            }
            check_precondition(&request.expected, Some(&existing.version))?;
            UPDATE_SQL
        }
        None => {
            if !accessor.can_create(&request.id.owner) {
                return Err(AppError::permission(
                    "write denied: cannot create object for this owner",
                ));
            }
            check_precondition(&request.expected, None)?;
            INSERT_SQL
        }
    };

    sqlx::query(statement)
        .bind(owner_kind)
        .bind(owner_id.as_str())
        .bind(request.id.collection.as_str())
        .bind(request.id.key.as_str())
        .bind(sqlx::types::Json(request.value.as_json()))
        .bind(version.as_str())
        .bind(read_code)
        .bind(write_code)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    Ok(StorageObject {
        id: request.id,
        value: request.value,
        version,
        permissions: request.permissions,
    })
}

/// Persist the base object and its index projection in the same transaction.
async fn write_indexed_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    request: WriteRequest,
    membership: Option<&StorageIndexMembership>,
    flavor: PgFlavor,
) -> AppResult<StorageObject> {
    let object = write_conn(conn, accessor, request, flavor).await?;
    sync_index_memberships_conn(conn, &object, membership).await?;
    Ok(object)
}

/// Read the configured index candidates from the durable operator projection.
async fn index_candidates_conn(
    conn: &mut PgConnection,
    id: &ObjectId,
) -> AppResult<std::collections::BTreeSet<crate::storage::StorageIndexName>> {
    let rows = sqlx::query(
        "SELECT index_name FROM storage_index_definitions \
         WHERE collection = $1 AND (object_key IS NULL OR object_key = $2) \
         ORDER BY index_name ASC",
    )
    .bind(id.collection.as_str())
    .bind(id.key.as_str())
    .fetch_all(&mut *conn)
    .await
    .map_err(db_err)?;
    rows.iter()
        .map(|row| crate::storage::StorageIndexName::new(get::<String>(row, "index_name")?))
        .collect()
}

/// Replace memberships for one immutable storage identity. This function is
/// always called inside the write transaction, so a rejected callback cannot
/// expose a new object with old membership (or the reverse).
async fn sync_index_memberships_conn(
    conn: &mut PgConnection,
    object: &StorageObject,
    membership: Option<&StorageIndexMembership>,
) -> AppResult<()> {
    let candidates = index_candidates_conn(conn, &object.id).await?;
    let membership = membership
        .cloned()
        .unwrap_or_else(|| StorageIndexMembership::include_all(candidates.clone()));
    if membership.candidates() != &candidates {
        return Err(AppError::validation(
            "storage index membership candidates do not match configured indexes",
        ));
    }

    let (owner_kind, owner_id) = owner_columns(&object.id.owner);
    sqlx::query(
        "DELETE FROM storage_index_memberships \
         WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4",
    )
    .bind(owner_kind)
    .bind(owner_id.as_str())
    .bind(object.id.collection.as_str())
    .bind(object.id.key.as_str())
    .execute(&mut *conn)
    .await
    .map_err(db_err)?;

    for index_name in membership.included() {
        sqlx::query(
            "INSERT INTO storage_index_memberships \
             (index_name, owner_kind, owner_id, collection, object_key) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(index_name.as_str())
        .bind(owner_kind)
        .bind(owner_id.as_str())
        .bind(object.id.collection.as_str())
        .bind(object.id.key.as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    }
    Ok(())
}

/// Register/rebuild the durable membership projection for one static index.
async fn install_index_projection_conn(
    conn: &mut PgConnection,
    index: &StorageIndexDefinition,
) -> AppResult<()> {
    sqlx::query("DELETE FROM storage_index_memberships WHERE index_name = $1")
        .bind(index.name().as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(
        "INSERT INTO storage_index_definitions (index_name, collection, object_key) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (index_name) DO UPDATE \
         SET collection = EXCLUDED.collection, object_key = EXCLUDED.object_key",
    )
    .bind(index.name().as_str())
    .bind(index.collection().as_str())
    .bind(index.key().map(Key::as_str))
    .execute(&mut *conn)
    .await
    .map_err(db_err)?;
    sqlx::query(
        "INSERT INTO storage_index_memberships \
         (index_name, owner_kind, owner_id, collection, object_key) \
         SELECT $1, owner_kind, owner_id, collection, object_key \
         FROM storage_objects \
         WHERE collection = $2 AND ($3::text IS NULL OR object_key = $3)",
    )
    .bind(index.name().as_str())
    .bind(index.collection().as_str())
    .bind(index.key().map(Key::as_str))
    .execute(&mut *conn)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn delete_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    id: &ObjectId,
    expected: &Precondition,
    flavor: PgFlavor,
) -> AppResult<()> {
    let (owner_kind, owner_id) = owner_columns(&id.owner);

    lock_object(&mut *conn, id, flavor).await?;
    let existing = sqlx::query(SELECT_FOR_UPDATE_SQL)
        .bind(owner_kind)
        .bind(owner_id.as_str())
        .bind(id.collection.as_str())
        .bind(id.key.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;

    match existing {
        None => match expected {
            Precondition::Match(_) => Err(AppError::conflict(
                "delete precondition failed: object does not exist",
            )),
            Precondition::Any | Precondition::MustNotExist => Ok(()),
        },
        Some(row) => {
            let existing = existing_from_row(&row)?;
            if !existing.permissions.can_write(&id.owner, accessor) {
                return Err(AppError::permission("delete denied on existing object"));
            }
            check_precondition(expected, Some(&existing.version))?;
            sqlx::query(
                "DELETE FROM storage_index_memberships \
                 WHERE owner_kind = $1 AND owner_id = $2 AND collection = $3 AND object_key = $4",
            )
            .bind(owner_kind)
            .bind(owner_id.as_str())
            .bind(id.collection.as_str())
            .bind(id.key.as_str())
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
            sqlx::query(DELETE_SQL)
                .bind(owner_kind)
                .bind(owner_id.as_str())
                .bind(id.collection.as_str())
                .bind(id.key.as_str())
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
            Ok(())
        }
    }
}

async fn list_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    query: &ListQuery,
) -> AppResult<Page<StorageObject>> {
    if query.limit == 0 {
        return Err(AppError::validation("list limit must be greater than zero"));
    }

    let (owner_filter, filter_kind, filter_id) = match &query.owner {
        Some(owner) => {
            let (kind, id) = owner_columns(owner);
            (true, kind, id)
        }
        None => (false, 0_i16, String::new()),
    };
    let (accessor_kind, accessor_id) = accessor_columns(accessor);
    let (has_cursor, cursor_kind, cursor_id, cursor_key) = match &query.cursor {
        Some(cursor) => {
            let payload = decode_cursor(cursor)?;
            (true, payload.owner_kind, payload.owner_id, payload.key)
        }
        None => (false, 0_i16, String::new(), String::new()),
    };

    let fetch_limit = i64::try_from(query.limit)
        .unwrap_or(i64::MAX)
        .saturating_add(1);

    let rows = sqlx::query(LIST_SQL)
        .bind(query.collection.as_str())
        .bind(owner_filter)
        .bind(filter_kind)
        .bind(filter_id)
        .bind(accessor_kind)
        .bind(accessor_id)
        .bind(has_cursor)
        .bind(cursor_kind)
        .bind(cursor_id)
        .bind(cursor_key)
        .bind(fetch_limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;

    let mut items = rows
        .iter()
        .map(row_to_object)
        .collect::<AppResult<Vec<StorageObject>>>()?;

    let next = if items.len() > query.limit {
        items.truncate(query.limit);
        items.last().map(encode_cursor)
    } else {
        None
    };

    Ok(Page { items, next })
}

/// Render one PostgreSQL expression index from a validated static definition.
///
/// All SQL identifiers originate from [`StorageIndexDefinition::physical_name`]
/// and field selectors are constrained by `StorageIndexField`, while user-facing
/// collection/key strings are SQL-quoted. No runtime/game-script input reaches
/// this DDL path.
fn create_index_sql(index: &StorageIndexDefinition) -> String {
    let fields = index
        .fields()
        .iter()
        .map(|field| format!("(value ->> '{}')", field.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut predicate = format!(
        "collection = '{}'",
        quote_sql_literal(index.collection().as_str())
    );
    if let Some(key) = index.key() {
        predicate.push_str(&format!(
            " AND object_key = '{}'",
            quote_sql_literal(key.as_str())
        ));
    }
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON storage_objects ({fields}, owner_kind, owner_id, object_key) WHERE {predicate}",
        index.physical_name()
    )
}

/// Query an index through the exact JSON expressions installed by
/// [`create_index_sql`].
async fn query_index_conn(
    conn: &mut PgConnection,
    accessor: &Accessor,
    query: &StorageIndexQuery,
) -> AppResult<Vec<StorageObject>> {
    let mut placeholder = 1_usize;
    let mut sql = "SELECT s.owner_kind, s.owner_id, s.collection, s.object_key, \
                   s.value, s.version, s.read_permission, s.write_permission \
                   FROM storage_objects AS s \
                   INNER JOIN storage_index_memberships AS m \
                   ON m.owner_kind = s.owner_kind AND m.owner_id = s.owner_id \
                   AND m.collection = s.collection AND m.object_key = s.object_key \
                   WHERE m.index_name = $1"
        .to_string();
    placeholder += 1;
    sql.push_str(&format!(" AND s.collection = ${placeholder}"));
    placeholder += 1;
    if query.index().key().is_some() {
        sql.push_str(&format!(" AND s.object_key = ${placeholder}"));
        placeholder += 1;
    }
    for (field, value) in query.filters() {
        let json_type = match value {
            StorageIndexValue::String(_) => "string",
            StorageIndexValue::Integer(_) | StorageIndexValue::Float(_) => "number",
            StorageIndexValue::Boolean(_) => "boolean",
        };
        sql.push_str(&format!(
            " AND jsonb_typeof(s.value -> '{}') = '{json_type}' \
             AND (s.value ->> '{}') = ${placeholder}",
            field.as_str(),
            field.as_str(),
        ));
        placeholder += 1;
    }
    let accessor_kind = placeholder;
    let accessor_id = placeholder + 1;
    sql.push_str(&format!(
        " AND (${accessor_kind}::text = 'runtime' \
             OR s.read_permission = 2 \
             OR (${accessor_kind}::text = 'user' AND s.read_permission = 1 \
                 AND s.owner_kind = 1 AND s.owner_id = ${accessor_id}))"
    ));
    placeholder += 2;
    sql.push_str(&format!(
        " ORDER BY s.owner_kind ASC, s.owner_id ASC, s.object_key ASC LIMIT ${placeholder}"
    ));

    let (accessor_kind_value, accessor_id_value) = accessor_columns(accessor);
    let mut statement = sqlx::query(&sql)
        .bind(query.index().name().as_str())
        .bind(query.index().collection().as_str());
    if let Some(key) = query.index().key() {
        statement = statement.bind(key.as_str());
    }
    for value in query.filters().values() {
        statement = statement.bind(value.postgres_text());
    }
    let limit = i64::try_from(query.limit()).unwrap_or(i64::MAX);
    let rows = statement
        .bind(accessor_kind_value)
        .bind(accessor_id_value)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_object).collect()
}

fn quote_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Take the per-object advisory transaction lock (released at transaction end).
///
/// On standard PostgreSQL this serializes concurrent writers of the same object
/// through `pg_advisory_xact_lock`, closing the absent-row race two concurrent
/// creators can hit under `READ COMMITTED`. CockroachDB does not implement
/// `pg_advisory_xact_lock` and does not need it: its default `SERIALIZABLE`
/// isolation (strictly stronger than PostgreSQL's default) plus the primary-key
/// constraint already reject the racing insert, so the lock is skipped.
async fn lock_object(conn: &mut PgConnection, id: &ObjectId, flavor: PgFlavor) -> AppResult<()> {
    if flavor == PgFlavor::Cockroach {
        return Ok(());
    }
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(advisory_key(id))
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user_id(value: &str) -> UserId {
        UserId::new(value).expect("valid user id")
    }

    fn object_id(owner: Owner, collection: &str, key: &str) -> ObjectId {
        ObjectId::new(
            owner,
            Collection::new(collection).expect("collection"),
            Key::new(key).expect("key"),
        )
    }

    #[test]
    fn owner_columns_round_trip() {
        let (kind, id) = owner_columns(&Owner::System);
        assert_eq!((kind, id.as_str()), (0, ""));
        assert_eq!(owner_from_columns(0, "").expect("system"), Owner::System);

        let owner = Owner::user(user_id("alice"));
        let (kind, id) = owner_columns(&owner);
        assert_eq!((kind, id.as_str()), (1, "alice"));
        assert_eq!(owner_from_columns(1, "alice").expect("user"), owner);

        assert!(owner_from_columns(2, "x").is_err());
    }

    #[test]
    fn accessor_columns_map_expected_kinds() {
        assert_eq!(accessor_columns(&Accessor::Runtime).0, "runtime");
        assert_eq!(accessor_columns(&Accessor::Public).0, "public");
        let (kind, id) = accessor_columns(&Accessor::User(user_id("bob")));
        assert_eq!((kind, id.as_str()), ("user", "bob"));
    }

    #[test]
    fn advisory_key_is_stable_and_distinguishes_objects() {
        let a = object_id(Owner::user(user_id("alice")), "saves", "slot-1");
        let a_again = object_id(Owner::user(user_id("alice")), "saves", "slot-1");
        let b = object_id(Owner::user(user_id("alice")), "saves", "slot-2");
        let c = object_id(Owner::System, "saves", "slot-1");
        assert_eq!(advisory_key(&a), advisory_key(&a_again));
        assert_ne!(advisory_key(&a), advisory_key(&b));
        assert_ne!(advisory_key(&a), advisory_key(&c));
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let object = StorageObject {
            id: object_id(Owner::user(user_id("alice")), "saves", "slot-1"),
            value: StorageValue::new(json!({"score": 1})).expect("value"),
            version: Version::from_token("deadbeef".to_string()),
            permissions: Permissions::owner_private(),
        };
        let cursor = encode_cursor(&object);
        assert!(cursor.as_str().starts_with(CURSOR_PREFIX));
        let payload = decode_cursor(&cursor).expect("decodes");
        assert_eq!(payload.owner_kind, 1);
        assert_eq!(payload.owner_id, "alice");
        assert_eq!(payload.key, "slot-1");

        let bogus = Cursor::from_token("not-a-pg-cursor".to_string());
        let err = decode_cursor(&bogus).expect_err("garbage rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
    }

    #[test]
    fn permissions_from_codes_validates_range() {
        assert!(permissions_from_codes(3, 0).is_err());
        assert!(permissions_from_codes(0, 5).is_err());
        let perms = permissions_from_codes(1, 1).expect("valid");
        assert_eq!(perms, Permissions::owner_private());
    }
}
