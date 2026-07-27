//! SQLite [`StorageRepository`] implementation.
//!
//! Every SQLite-specific choice (JSON stored as `TEXT`, the composite
//! `(owner_kind, owner_id, collection, object_key)` key, `?` positional
//! placeholders, row-value keyset comparison) stays behind this file.
//!
//! # Semantics parity
//!
//! [`SqliteStorageRepository`] reproduces [`InMemoryStorageRepository`](
//! crate::repository::InMemoryStorageRepository) and
//! [`PgStorageRepository`](crate::repository::pg::PgStorageRepository) exactly:
//! permission checks run before precondition checks on existing objects;
//! `can_create` runs before the precondition on absent objects; reads and lists
//! filter by permission in SQL so unreadable objects are indistinguishable from
//! absent ones; and versions are content-addressed via [`Version::of`], never
//! recomputed from what the database echoes back.
//!
//! Unlike Postgres there is no advisory lock and no `SELECT ... FOR UPDATE`:
//! SQLite permits a single writer, so each write/delete runs inside a transaction
//! and the `PRIMARY KEY` unique constraint is the final backstop against a
//! concurrent create (mapped to `Conflict` in [`super::db_err`]). SQLite's default
//! BINARY collation is byte-wise, matching Postgres `COLLATE "C"`, so keyset
//! ordering is identical.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use sqlx::{Row, Sqlite};

use crate::error::{AppError, AppResult};
use crate::repository::{StorageRepository, check_precondition};
use crate::storage::{
    Accessor, Collection, CollectionSummary, Cursor, Key, ListQuery, ObjectId, Owner, Page,
    Permissions, Precondition, ReadPermission, StorageIndexDefinition, StorageIndexMembership,
    StorageIndexQuery, StorageIndexValue, StorageObject, StorageValue, UserId, Version,
    WritePermission, WriteRequest,
};

use super::{SqliteExecutor, db_err, tx_closed};

/// Opaque cursor scheme tag. Bumping this string invalidates old cursors.
const CURSOR_PREFIX: &str = "sqlite-storage-v1:";

// --- SQL (runtime-checked; `?` positional placeholders bound in order) -------

/// Read a single object, filtered by the accessor's read permission.
const READ_SQL: &str = "\
SELECT owner_kind, owner_id, collection, object_key, \
       value, version, read_permission, write_permission \
FROM storage_objects \
WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ? \
  AND ( \
       ? = 'runtime' \
    OR read_permission = 2 \
    OR (? = 'user' AND read_permission = 1 AND owner_kind = 1 AND owner_id = ?) \
  )";

/// Read the current row (if any) for a write/delete decision.
const SELECT_CURRENT_SQL: &str = "\
SELECT version, read_permission, write_permission \
FROM storage_objects \
WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ?";

/// Overwrite an existing object.
const UPDATE_SQL: &str = "\
UPDATE storage_objects \
SET value = ?, version = ?, read_permission = ?, write_permission = ?, \
    updated_at = strftime('%s', 'now') \
WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ?";

/// Insert a brand-new object.
const INSERT_SQL: &str = "\
INSERT INTO storage_objects \
(owner_kind, owner_id, collection, object_key, value, version, read_permission, write_permission) \
VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

/// Delete an object by identity.
const DELETE_SQL: &str = "\
DELETE FROM storage_objects \
WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ?";

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
WHERE collection = ? \
  AND (NOT ? OR (owner_kind = ? AND owner_id = ?)) \
  AND ( \
       ? = 'runtime' \
    OR read_permission = 2 \
    OR (? = 'user' AND read_permission = 1 AND owner_kind = 1 AND owner_id = ?) \
  ) \
  AND (NOT ? OR (owner_kind, owner_id, object_key) > (?, ?, ?)) \
ORDER BY owner_kind ASC, owner_id ASC, object_key ASC \
LIMIT ?";

// --- Mapping helpers --------------------------------------------------------

/// Map an [`Owner`] to its `(owner_kind, owner_id)` column pair.
///
/// `Owner::System` is `(0, "")`; `Owner::User(id)` is `(1, id)`. The empty string
/// (never a valid user id) keeps the composite primary key free of the
/// NULL-is-distinct pitfall.
fn owner_columns(owner: &Owner) -> (i64, String) {
    match owner {
        Owner::System => (0, String::new()),
        Owner::User(id) => (1, id.as_str().to_string()),
    }
}

/// Rebuild an [`Owner`] from its column pair.
fn owner_from_columns(kind: i64, id: &str) -> AppResult<Owner> {
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

fn permissions_from_codes(read: i64, write: i64) -> AppResult<Permissions> {
    let read =
        u8::try_from(read).map_err(|_| AppError::internal("read_permission out of range"))?;
    let write =
        u8::try_from(write).map_err(|_| AppError::internal("write_permission out of range"))?;
    Ok(Permissions {
        read: ReadPermission::from_code(read)?,
        write: WritePermission::from_code(write)?,
    })
}

/// Fetch a typed column, mapping decode failures to an internal error.
fn get<'r, T>(row: &'r SqliteRow, column: &str) -> AppResult<T>
where
    T: sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get::<T, _>(column).map_err(|e| {
        AppError::internal(format!("failed to decode column `{column}`")).with_detail(e.to_string())
    })
}

/// Decode a full object row into a domain [`StorageObject`].
fn row_to_object(row: &SqliteRow) -> AppResult<StorageObject> {
    let owner_kind: i64 = get(row, "owner_kind")?;
    let owner_id: String = get(row, "owner_id")?;
    let collection: String = get(row, "collection")?;
    let object_key: String = get(row, "object_key")?;
    let value_text: String = get(row, "value")?;
    let version: String = get(row, "version")?;
    let read_permission: i64 = get(row, "read_permission")?;
    let write_permission: i64 = get(row, "write_permission")?;

    let value: serde_json::Value = serde_json::from_str(&value_text).map_err(|e| {
        AppError::internal("failed to decode stored JSON value").with_detail(e.to_string())
    })?;
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

fn existing_from_row(row: &SqliteRow) -> AppResult<ExistingObject> {
    let version: String = get(row, "version")?;
    let read_permission: i64 = get(row, "read_permission")?;
    let write_permission: i64 = get(row, "write_permission")?;
    Ok(ExistingObject {
        version: Version::from_token(version),
        permissions: permissions_from_codes(read_permission, write_permission)?,
    })
}

/// The typed payload behind an opaque list [`Cursor`].
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CursorPayload {
    collection: String,
    owner_kind: i64,
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

/// The SQLite [`StorageRepository`] implementation.
pub struct SqliteStorageRepository {
    executor: SqliteExecutor,
}

impl SqliteStorageRepository {
    /// Bind a storage repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl StorageRepository for SqliteStorageRepository {
    async fn read(&self, accessor: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                read_conn(&mut conn, accessor, id).await
            }
            SqliteExecutor::Tx(cell) => {
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
            SqliteExecutor::Pool(pool) => {
                // `BEGIN IMMEDIATE` takes SQLite's single writer slot up front, so
                // the read-then-write decision (permission -> precondition) is
                // serialized against other writers exactly like the Postgres
                // advisory lock — closing the concurrent-create race rather than
                // leaving it to a `SQLITE_BUSY` or a raw PK conflict.
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match write_indexed_conn(&mut tx, accessor, request, membership).await {
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
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                write_indexed_conn(&mut *tx, accessor, request, membership).await
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
            SqliteExecutor::Pool(pool) => {
                // See `write`: `BEGIN IMMEDIATE` serializes the writer decision.
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match delete_conn(&mut tx, accessor, id, &expected).await {
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
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                delete_conn(&mut *tx, accessor, id, &expected).await
            }
        }
    }

    async fn list(&self, accessor: &Accessor, query: &ListQuery) -> AppResult<Page<StorageObject>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_conn(&mut conn, accessor, query).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_conn(&mut *tx, accessor, query).await
            }
        }
    }

    async fn install_index(&self, index: &StorageIndexDefinition) -> AppResult<()> {
        let sql = create_index_sql(index);
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match async {
                    sqlx::query(&sql).execute(&mut *tx).await.map_err(db_err)?;
                    install_index_projection_conn(&mut tx, index).await
                }
                .await
                {
                    Ok(()) => tx.commit().await.map_err(db_err)?,
                    Err(error) => {
                        let _ = tx.rollback().await;
                        return Err(error);
                    }
                }
            }
            SqliteExecutor::Tx(cell) => {
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
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                query_index_conn(&mut conn, accessor, query).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                query_index_conn(&mut *tx, accessor, query).await
            }
        }
    }

    async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                list_collections_conn(&mut conn).await
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                list_collections_conn(&mut *tx).await
            }
        }
    }
}

// --- Statement bodies (executor-agnostic over a single connection) ----------

async fn read_conn(
    conn: &mut SqliteConnection,
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
        .bind(accessor_kind)
        .bind(accessor_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(row_to_object).transpose()
}

async fn list_collections_conn(conn: &mut SqliteConnection) -> AppResult<Vec<CollectionSummary>> {
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

async fn write_conn(
    conn: &mut SqliteConnection,
    accessor: &Accessor,
    request: WriteRequest,
) -> AppResult<StorageObject> {
    let (owner_kind, owner_id) = owner_columns(&request.id.owner);
    let version = Version::of(&request.value);
    let read_code = i64::from(request.permissions.read.code());
    let write_code = i64::from(request.permissions.write.code());

    let existing = sqlx::query(SELECT_CURRENT_SQL)
        .bind(owner_kind)
        .bind(owner_id.as_str())
        .bind(request.id.collection.as_str())
        .bind(request.id.key.as_str())
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;

    let is_update = match existing {
        Some(row) => {
            let existing = existing_from_row(&row)?;
            if !existing.permissions.can_write(&request.id.owner, accessor) {
                return Err(AppError::permission("write denied on existing object"));
            }
            check_precondition(&request.expected, Some(&existing.version))?;
            true
        }
        None => {
            if !accessor.can_create(&request.id.owner) {
                return Err(AppError::permission(
                    "write denied: cannot create object for this owner",
                ));
            }
            check_precondition(&request.expected, None)?;
            false
        }
    };

    let value_text = serde_json::to_string(request.value.as_json()).map_err(|e| {
        AppError::internal("failed to encode JSON value for storage").with_detail(e.to_string())
    })?;

    if is_update {
        sqlx::query(UPDATE_SQL)
            .bind(value_text)
            .bind(version.as_str())
            .bind(read_code)
            .bind(write_code)
            .bind(owner_kind)
            .bind(owner_id.as_str())
            .bind(request.id.collection.as_str())
            .bind(request.id.key.as_str())
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    } else {
        sqlx::query(INSERT_SQL)
            .bind(owner_kind)
            .bind(owner_id.as_str())
            .bind(request.id.collection.as_str())
            .bind(request.id.key.as_str())
            .bind(value_text)
            .bind(version.as_str())
            .bind(read_code)
            .bind(write_code)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }

    Ok(StorageObject {
        id: request.id,
        value: request.value,
        version,
        permissions: request.permissions,
    })
}

/// Persist the base object and its index projection in the same SQLite write
/// transaction.
async fn write_indexed_conn(
    conn: &mut SqliteConnection,
    accessor: &Accessor,
    request: WriteRequest,
    membership: Option<&StorageIndexMembership>,
) -> AppResult<StorageObject> {
    let object = write_conn(conn, accessor, request).await?;
    sync_index_memberships_conn(conn, &object, membership).await?;
    Ok(object)
}

async fn index_candidates_conn(
    conn: &mut SqliteConnection,
    id: &ObjectId,
) -> AppResult<std::collections::BTreeSet<crate::storage::StorageIndexName>> {
    let rows = sqlx::query(
        "SELECT index_name FROM storage_index_definitions \
         WHERE collection = ? AND (object_key IS NULL OR object_key = ?) \
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

async fn sync_index_memberships_conn(
    conn: &mut SqliteConnection,
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
         WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ?",
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
             VALUES (?, ?, ?, ?, ?)",
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

async fn install_index_projection_conn(
    conn: &mut SqliteConnection,
    index: &StorageIndexDefinition,
) -> AppResult<()> {
    sqlx::query("DELETE FROM storage_index_memberships WHERE index_name = ?")
        .bind(index.name().as_str())
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::query(
        "INSERT INTO storage_index_definitions (index_name, collection, object_key) \
         VALUES (?, ?, ?) \
         ON CONFLICT(index_name) DO UPDATE \
         SET collection = excluded.collection, object_key = excluded.object_key",
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
         SELECT ?, owner_kind, owner_id, collection, object_key \
         FROM storage_objects \
         WHERE collection = ? AND (? IS NULL OR object_key = ?)",
    )
    .bind(index.name().as_str())
    .bind(index.collection().as_str())
    .bind(index.key().map(Key::as_str))
    .bind(index.key().map(Key::as_str))
    .execute(&mut *conn)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn delete_conn(
    conn: &mut SqliteConnection,
    accessor: &Accessor,
    id: &ObjectId,
    expected: &Precondition,
) -> AppResult<()> {
    let (owner_kind, owner_id) = owner_columns(&id.owner);

    let existing = sqlx::query(SELECT_CURRENT_SQL)
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
                 WHERE owner_kind = ? AND owner_id = ? AND collection = ? AND object_key = ?",
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
    conn: &mut SqliteConnection,
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
        None => (false, 0_i64, String::new()),
    };
    let (accessor_kind, accessor_id) = accessor_columns(accessor);
    let (has_cursor, cursor_kind, cursor_id, cursor_key) = match &query.cursor {
        Some(cursor) => {
            let payload = decode_cursor(cursor)?;
            (true, payload.owner_kind, payload.owner_id, payload.key)
        }
        None => (false, 0_i64, String::new(), String::new()),
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

/// Render one SQLite JSON-expression index from a validated static definition.
fn create_index_sql(index: &StorageIndexDefinition) -> String {
    let fields = index
        .fields()
        .iter()
        .map(|field| format!("json_extract(value, '$.{}')", field.as_str()))
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

/// Execute the portable equality-query contract through SQLite's JSON1
/// extraction expression. Filter values retain their JSON scalar type so the
/// predicate uses the physical expression index installed above.
async fn query_index_conn(
    conn: &mut SqliteConnection,
    accessor: &Accessor,
    query: &StorageIndexQuery,
) -> AppResult<Vec<StorageObject>> {
    let mut sql = "SELECT s.owner_kind, s.owner_id, s.collection, s.object_key, \
                   s.value, s.version, s.read_permission, s.write_permission \
                   FROM storage_objects AS s \
                   INNER JOIN storage_index_memberships AS m \
                   ON m.owner_kind = s.owner_kind AND m.owner_id = s.owner_id \
                   AND m.collection = s.collection AND m.object_key = s.object_key \
                   WHERE m.index_name = ? AND s.collection = ?"
        .to_string();
    if query.index().key().is_some() {
        sql.push_str(" AND s.object_key = ?");
    }
    for (field, value) in query.filters() {
        let json_type = match value {
            StorageIndexValue::String(_) => "text",
            StorageIndexValue::Integer(_) | StorageIndexValue::Float(_) => "number",
            StorageIndexValue::Boolean(_) => "boolean",
        };
        let type_predicate = match json_type {
            "number" => format!(
                "json_type(s.value, '$.{}') IN ('integer', 'real')",
                field.as_str()
            ),
            "boolean" => format!(
                "json_type(s.value, '$.{}') IN ('true', 'false')",
                field.as_str()
            ),
            _ => format!("json_type(s.value, '$.{}') = 'text'", field.as_str()),
        };
        sql.push_str(&format!(
            " AND {type_predicate} AND json_extract(s.value, '$.{}') = ?",
            field.as_str(),
        ));
    }
    sql.push_str(
        " AND (? = 'runtime' \
             OR s.read_permission = 2 \
             OR (? = 'user' AND s.read_permission = 1 AND s.owner_kind = 1 AND s.owner_id = ?)) \
          ORDER BY s.owner_kind ASC, s.owner_id ASC, s.object_key ASC LIMIT ?",
    );

    let (accessor_kind, accessor_id) = accessor_columns(accessor);
    let mut statement = sqlx::query(&sql)
        .bind(query.index().name().as_str())
        .bind(query.index().collection().as_str());
    if let Some(key) = query.index().key() {
        statement = statement.bind(key.as_str());
    }
    for value in query.filters().values() {
        statement = match value {
            StorageIndexValue::String(value) => statement.bind(value),
            StorageIndexValue::Integer(value) => statement.bind(*value),
            StorageIndexValue::Float(value) => statement.bind(*value),
            StorageIndexValue::Boolean(value) => statement.bind(i64::from(*value)),
        };
    }
    let limit = i64::try_from(query.limit()).unwrap_or(i64::MAX);
    let rows = statement
        .bind(accessor_kind)
        .bind(accessor_kind)
        .bind(accessor_id)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(row_to_object).collect()
}

fn quote_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
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

        let bogus = Cursor::from_token("not-a-sqlite-cursor".to_string());
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
