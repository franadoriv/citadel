//! Typed, read-only contract for the operator database explorer.
//!
//! These types deliberately carry *logical* schema, table, and column names;
//! they are not SQL fragments. Backend adapters must resolve them against an
//! allowlisted metadata snapshot before quoting an identifier or executing a
//! query. Keeping the request validation here means the HTTP boundary and every
//! database flavour enforce the same small, bounded input surface.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;
use sqlx::types::Json;
use sqlx::{Column, PgPool, Row, SqlitePool, TypeInfo, ValueRef};

use crate::error::{AppError, AppResult, ErrorCategory};

/// Maximum accepted logical identifier length in bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 128;
/// Maximum number of structured filters in one row-listing request.
pub const MAX_FILTERS: usize = 8;
/// Maximum encoded filter value length in bytes.
pub const MAX_FILTER_VALUE_BYTES: usize = 1_024;
/// Maximum number of rows an explorer page may request.
pub const MAX_PAGE_SIZE: usize = 100;
/// Maximum serialized explorer response body before HTTP framing.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// Maximum key fields accepted in an opaque-row detail request.
pub const MAX_ROW_KEY_FIELDS: usize = 16;

const CURSOR_TTL: Duration = Duration::from_secs(300);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// A logical application schema and table name.
///
/// The values are validated for size and control characters only. In
/// particular, punctuation is not interpreted here: an adapter resolves the
/// complete pair against its metadata snapshot, never by concatenating it into
/// SQL text.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    /// Application schema, such as `public` for PostgreSQL.
    pub schema: String,
    /// Application table or view name.
    pub table: String,
}

impl TableRef {
    /// Construct a validated logical table reference.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> AppResult<Self> {
        let value = Self {
            schema: schema.into(),
            table: table.into(),
        };
        value.validate()?;
        Ok(value)
    }

    /// Validate both logical names before an adapter resolves them.
    pub fn validate(&self) -> AppResult<()> {
        validate_identifier("schema", &self.schema)?;
        validate_identifier("table", &self.table)
    }
}

/// One structured comparison that an adapter may translate after validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowFilter {
    /// A metadata-resolved visible column name.
    pub column: String,
    /// The allowlisted comparison operation.
    pub operator: FilterOperator,
    /// JSON value to bind as a query parameter; omitted only for `is_null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

impl RowFilter {
    /// Validate shape and bounds independently of a table's metadata.
    pub fn validate(&self) -> AppResult<()> {
        validate_identifier("filter column", &self.column)?;
        match (self.operator, self.value.as_ref()) {
            (FilterOperator::IsNull, None) => Ok(()),
            (FilterOperator::IsNull, Some(_)) => Err(AppError::validation(
                "the is_null filter does not accept a value",
            )),
            (_, None) => Err(AppError::validation("this filter requires a value")),
            (_, Some(value)) => {
                let encoded = serde_json::to_vec(value)
                    .map_err(|_| AppError::validation("filter value is not JSON serializable"))?;
                if encoded.len() > MAX_FILTER_VALUE_BYTES {
                    return Err(AppError::validation(
                        "filter value exceeds the maximum size",
                    ));
                }
                Ok(())
            }
        }
    }
}

/// Supported structured filter operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Contains,
    IsNull,
}

/// Direction for metadata-resolved ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

/// One metadata-resolved sort field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortSpec {
    /// A visible column selected from the table metadata.
    pub column: String,
    /// Explicit deterministic order direction.
    pub direction: SortDirection,
}

impl SortSpec {
    /// Validate the logical sort-column name.
    pub fn validate(&self) -> AppResult<()> {
        validate_identifier("sort column", &self.column)
    }
}

/// Validated portable row-listing request before backend metadata resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRowsRequest {
    pub table: TableRef,
    #[serde(default)]
    pub filters: Vec<RowFilter>,
    pub sort: SortSpec,
    /// Opaque cursor returned by an earlier page; adapters authenticate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Requested row count; the service supplies a default when absent.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// A backend-normalized application table visible to the explorer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableSummary {
    pub table: TableRef,
    pub kind: TableKind,
}

/// Type of an explorer-visible relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Table,
    View,
}

/// Backend-normalized metadata for a visible table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableDescription {
    pub table: TableRef,
    /// Capabilities the selected backend can actually provide for this table.
    /// Absent data is represented here instead of being fabricated from another
    /// database dialect.
    pub capabilities: ExplorerCapabilities,
    pub columns: Vec<ColumnDescription>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexDescription>,
    pub relations: Vec<RelationDescription>,
}

/// Per-backend metadata capabilities exposed by the portable explorer.
///
/// SQLite, PostgreSQL, and CockroachDB deliberately report the same shape;
/// adapters set only fields proved by their own metadata source. In particular,
/// a PostgreSQL-wire CockroachDB connection must not imply PostgreSQL catalog
/// support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ExplorerCapabilities {
    pub views: bool,
    pub indexes: bool,
    pub foreign_keys: bool,
    pub stable_keyset_pagination: bool,
}

impl ExplorerCapabilities {
    /// Capability profile for a table with a recognized stable unique key.
    #[must_use]
    pub const fn stable(views: bool, indexes: bool, foreign_keys: bool) -> Self {
        Self {
            views,
            indexes,
            foreign_keys,
            stable_keyset_pagination: true,
        }
    }

    /// Metadata-only capability profile for a table without a stable key.
    #[must_use]
    pub const fn metadata_only(views: bool, indexes: bool, foreign_keys: bool) -> Self {
        Self {
            views,
            indexes,
            foreign_keys,
            stable_keyset_pagination: false,
        }
    }
}

impl TableDescription {
    /// Validate metadata emitted by an adapter before exposing it to an API
    /// caller. This is a defence-in-depth check: adapters still own their
    /// allowlist and quoting rules.
    pub fn validate(&self) -> AppResult<()> {
        self.table.validate()?;
        if self.columns.is_empty() {
            return Err(AppError::internal(
                "database explorer table metadata has no columns",
            ));
        }
        for column in &self.columns {
            validate_identifier("column", &column.name)?;
        }
        for column in &self.primary_key {
            validate_identifier("primary key column", column)?;
        }
        if self.capabilities.stable_keyset_pagination && self.primary_key.is_empty() {
            return Err(AppError::internal(
                "stable database explorer pagination requires a primary key",
            ));
        }
        Ok(())
    }
}

/// Metadata for a visible column. `data_type` is descriptive only; it never
/// permits a caller to choose SQL syntax.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnDescription {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub sensitive: bool,
}

/// Metadata for an index a backend can expose safely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexDescription {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// A foreign-key relation between visible application tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelationDescription {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: TableRef,
    pub referenced_columns: Vec<String>,
}

/// One database value rendered without losing binary, numeric, or time
/// fidelity. Adapters must use `Redacted` for protected cells before these
/// values reach either a list page or row detail response.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum DatabaseValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    BinaryBase64(String),
    Json(serde_json::Value),
    Timestamp(String),
    Redacted,
}

/// One redacted, typed row result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatabaseRow {
    pub values: BTreeMap<String, DatabaseValue>,
    /// Opaque, authenticated identifier for the row detail route. It is not a
    /// SQL key and must be rejected by a different table or expired session.
    pub row_ref: String,
}

/// A keyset-paginated row page.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RowsPage {
    pub table: TableRef,
    pub rows: Vec<DatabaseRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// Request for a single row detail previously returned by a row page.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowDetailRequest {
    pub table: TableRef,
    pub row_ref: String,
}

impl RowDetailRequest {
    /// Validate portable bounds before verifying the opaque row reference.
    pub fn validate(&self) -> AppResult<()> {
        self.table.validate()?;
        if self.row_ref.is_empty() || self.row_ref.len() > 4_096 {
            return Err(AppError::validation(
                "database explorer row reference is out of range",
            ));
        }
        Ok(())
    }
}

/// Administrative read seam for database metadata and redacted values.
///
/// This trait is intentionally separate from [`crate::repository::Backend`]
/// and the domain repository traits. Implementations may hold a read-only
/// database connection/pool, but only expose logical, metadata-resolved reads
/// through this bounded contract.
#[async_trait]
pub trait DatabaseExplorer: Send + Sync {
    /// List application tables and views that passed the adapter allowlist.
    async fn list_tables(&self) -> AppResult<Vec<TableSummary>>;

    /// Describe one metadata-resolved application table.
    async fn describe_table(&self, table: &TableRef) -> AppResult<TableDescription>;

    /// List redacted rows using an adapter-authenticated keyset cursor.
    async fn list_rows(&self, request: &ListRowsRequest) -> AppResult<RowsPage>;

    /// Return one redacted row addressed by an opaque adapter-issued reference.
    async fn get_row(&self, request: &RowDetailRequest) -> AppResult<DatabaseRow>;
}

/// Server-state-bound opaque cursor registry.
///
/// Cursors are random 256-bit handles. Their binding (table and sort shape)
/// stays in process rather than in browser-visible payload data; a restart
/// invalidates outstanding cursors, which is safe for this diagnostic surface.
#[derive(Debug)]
pub struct ExplorerCursorStore {
    ttl: Duration,
    entries: Mutex<HashMap<String, CursorBinding>>,
}

#[derive(Debug, Clone)]
struct CursorBinding {
    table: TableRef,
    sort: SortSpec,
    position: Option<serde_json::Value>,
    expires_at: Instant,
}

impl ExplorerCursorStore {
    /// Create a store with the standard short cursor lifetime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ttl: CURSOR_TTL,
            entries: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Issue a fresh cursor bound to one table and sort shape.
    pub fn issue(&self, table: TableRef, sort: SortSpec) -> AppResult<String> {
        self.issue_inner(table, sort, None)
    }

    /// Issue a cursor whose keyset position remains server-side.
    pub fn issue_at_position(
        &self,
        table: TableRef,
        sort: SortSpec,
        position: serde_json::Value,
    ) -> AppResult<String> {
        self.issue_inner(table, sort, Some(position))
    }

    fn issue_inner(
        &self,
        table: TableRef,
        sort: SortSpec,
        position: Option<serde_json::Value>,
    ) -> AppResult<String> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|error| {
            AppError::internal(format!("CSPRNG unavailable for cursor: {error}"))
        })?;
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let now = Instant::now();
        let mut entries = self.lock();
        entries.retain(|_, binding| binding.expires_at > now);
        entries.insert(
            token.clone(),
            CursorBinding {
                table,
                sort,
                position,
                expires_at: now + self.ttl,
            },
        );
        Ok(token)
    }

    /// Verify that a cursor belongs to the requested table and sort shape.
    pub fn verify(&self, token: &str, table: &TableRef, sort: &SortSpec) -> AppResult<()> {
        let entries = self.lock();
        let valid = entries.get(token).is_some_and(|binding| {
            binding.expires_at > Instant::now() && binding.table == *table && binding.sort == *sort
        });
        valid
            .then_some(())
            .ok_or_else(|| AppError::validation("invalid database explorer cursor"))
    }

    /// Resolve the server-held keyset position after validating the cursor's
    /// table and sort binding.
    pub fn position(
        &self,
        token: &str,
        table: &TableRef,
        sort: &SortSpec,
    ) -> AppResult<Option<serde_json::Value>> {
        self.verify(token, table, sort)?;
        Ok(self
            .lock()
            .get(token)
            .and_then(|binding| binding.position.clone()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CursorBinding>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for ExplorerCursorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Server-state-bound opaque row-detail reference registry.
///
/// A row reference maps to a metadata-resolved primary-key tuple, but that
/// tuple never appears in a dashboard URL or JSON response. References expire
/// with the same short lifetime as cursors and are scoped to exactly one table.
#[derive(Debug)]
pub struct ExplorerRowStore {
    ttl: Duration,
    entries: Mutex<HashMap<String, RowBinding>>,
}

#[derive(Debug, Clone)]
struct RowBinding {
    table: TableRef,
    primary_key: serde_json::Value,
    expires_at: Instant,
}

impl ExplorerRowStore {
    /// Create a store with the normal explorer reference lifetime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ttl: CURSOR_TTL,
            entries: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Issue an opaque reference for a row's server-held primary-key tuple.
    pub fn issue(&self, table: TableRef, primary_key: serde_json::Value) -> AppResult<String> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|error| {
            AppError::internal(format!("CSPRNG unavailable for row reference: {error}"))
        })?;
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let now = Instant::now();
        let mut entries = self.lock();
        entries.retain(|_, binding| binding.expires_at > now);
        entries.insert(
            token.clone(),
            RowBinding {
                table,
                primary_key,
                expires_at: now + self.ttl,
            },
        );
        Ok(token)
    }

    /// Resolve a row's primary key only when the requesting table matches.
    pub fn primary_key(&self, token: &str, table: &TableRef) -> AppResult<serde_json::Value> {
        let entries = self.lock();
        entries
            .get(token)
            .filter(|binding| binding.expires_at > Instant::now() && binding.table == *table)
            .map(|binding| binding.primary_key.clone())
            .ok_or_else(|| AppError::validation("invalid database explorer row reference"))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, RowBinding>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for ExplorerRowStore {
    fn default() -> Self {
        Self::new()
    }
}

/// SQLite metadata adapter. It is intentionally limited to the `main`
/// application database and filters SQLite-owned objects before they become
/// explorer resources.
///
/// Row browsing will be added only after metadata resolution, redaction, and
/// opaque cursor verification share the same adapter boundary.
#[derive(Debug, Clone)]
pub struct SqliteMetadataExplorer {
    pool: SqlitePool,
    /// Opaque pagination state belongs to the server, not to a transient
    /// request object. Keeping it behind an `Arc` also means cloned adapters
    /// (the normal repository access pattern) authenticate the same cursors.
    cursor_store: Arc<ExplorerCursorStore>,
    /// Detail handles have the same server-owned lifecycle as pagination
    /// cursors, but bind a primary-key tuple rather than an ordering tuple.
    row_store: Arc<ExplorerRowStore>,
}

/// PostgreSQL-wire metadata adapter used for PostgreSQL and CockroachDB.
///
/// It intentionally starts from SQL-standard `information_schema` rather than
/// PostgreSQL-specific catalog tables. Cockroach-specific callers must still
/// report only capabilities covered by their own contract suite.
#[derive(Debug, Clone)]
pub struct PgMetadataExplorer {
    pool: PgPool,
    cursor_store: Arc<ExplorerCursorStore>,
    row_store: Arc<ExplorerRowStore>,
}

impl PgMetadataExplorer {
    /// Construct an adapter over the configured PostgreSQL-wire pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self::with_stores(
            pool,
            Arc::new(ExplorerCursorStore::new()),
            Arc::new(ExplorerRowStore::new()),
        )
    }

    /// Construct an adapter with server-owned opaque state. The durable
    /// backend keeps one instance, so cloned handles must share both stores.
    #[must_use]
    pub fn with_stores(
        pool: PgPool,
        cursor_store: Arc<ExplorerCursorStore>,
        row_store: Arc<ExplorerRowStore>,
    ) -> Self {
        Self {
            pool,
            cursor_store,
            row_store,
        }
    }

    /// List user-visible tables and views in the conventional application
    /// schema. The schema and table categories are parameterized values; no
    /// caller-controlled identifier is part of the statement.
    pub async fn list_tables(&self) -> AppResult<Vec<TableSummary>> {
        const QUERY: &str = "SELECT table_schema, table_name, table_type \
            FROM information_schema.tables \
            WHERE table_schema = $1 AND table_type IN ('BASE TABLE', 'VIEW') \
            ORDER BY table_name";
        let rows = sqlx::query(QUERY)
            .bind("public")
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let schema: String = row.try_get("table_schema").map_err(explorer_db_error)?;
            let name: String = row.try_get("table_name").map_err(explorer_db_error)?;
            let table_type: String = row.try_get("table_type").map_err(explorer_db_error)?;
            let Some(kind) = table_kind_from_information_schema(&table_type) else {
                continue;
            };
            tables.push(TableSummary {
                table: TableRef::new(schema, name).map_err(|error| {
                    AppError::database(
                        "database explorer received invalid information schema metadata",
                    )
                    .with_detail(error.to_string())
                })?,
                kind,
            });
        }
        Ok(tables)
    }

    /// Describe a current allowlisted table through parameterized
    /// `information_schema.columns` metadata. Index and relation capability
    /// stays false until their portable queries receive backend contract tests.
    pub async fn describe_table(&self, requested: &TableRef) -> AppResult<TableDescription> {
        let table = self
            .list_tables()
            .await?
            .into_iter()
            .find(|candidate| candidate.table == *requested)
            .map(|candidate| candidate.table)
            .ok_or_else(|| AppError::not_found("database explorer table is not available"))?;
        const QUERY: &str = "SELECT column_name, data_type, is_nullable \
            FROM information_schema.columns \
            WHERE table_schema = $1 AND table_name = $2 \
            ORDER BY ordinal_position";
        let rows = sqlx::query(QUERY)
            .bind(&table.schema)
            .bind(&table.table)
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.try_get("column_name").map_err(explorer_db_error)?;
            let data_type: String = row.try_get("data_type").map_err(explorer_db_error)?;
            let nullable: String = row.try_get("is_nullable").map_err(explorer_db_error)?;
            columns.push(ColumnDescription {
                sensitive: is_sensitive_column(&name),
                name,
                data_type,
                nullable: nullable == "YES",
            });
        }
        let primary_key = self.primary_key_columns(&table).await?;
        let description = TableDescription {
            table,
            capabilities: if primary_key.is_empty() {
                ExplorerCapabilities::metadata_only(true, false, false)
            } else {
                ExplorerCapabilities::stable(true, false, false)
            },
            columns,
            primary_key,
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        description.validate()?;
        Ok(description)
    }

    async fn primary_key_columns(&self, table: &TableRef) -> AppResult<Vec<String>> {
        const QUERY: &str = "SELECT key_column_usage.column_name \
            FROM information_schema.table_constraints \
            INNER JOIN information_schema.key_column_usage \
              ON table_constraints.constraint_catalog = key_column_usage.constraint_catalog \
             AND table_constraints.constraint_schema = key_column_usage.constraint_schema \
             AND table_constraints.constraint_name = key_column_usage.constraint_name \
            WHERE table_constraints.table_schema = $1 \
              AND table_constraints.table_name = $2 \
              AND table_constraints.constraint_type = 'PRIMARY KEY' \
            ORDER BY key_column_usage.ordinal_position";
        sqlx::query(QUERY)
            .bind(&table.schema)
            .bind(&table.table)
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?
            .into_iter()
            .map(|row| row.try_get("column_name").map_err(explorer_db_error))
            .collect()
    }

    /// Read one PostgreSQL-wire keyset page. Values are selected as `jsonb` so
    /// SQLx does not need to guess arbitrary application column types; the
    /// logical metadata still controls projection, redaction and ordering.
    pub async fn read_rows_page(&self, request: &ListRowsRequest) -> AppResult<PgRowsPage> {
        let requested_limit = request.limit.unwrap_or(50);
        let fetch_limit = requested_limit
            .checked_add(1)
            .ok_or_else(|| AppError::internal("database explorer page limit overflow"))?;
        let mut fetch_request = request.clone();
        fetch_request.limit = Some(fetch_limit);
        let description = self.describe_table(&request.table).await?;
        let position = request
            .cursor
            .as_deref()
            .map(|cursor| {
                self.cursor_store
                    .position(cursor, &request.table, &request.sort)
            })
            .transpose()?
            .flatten();
        let plan = plan_pg_row_query_at_position(&fetch_request, &description, position.as_ref())?;
        let mut raw_rows = execute_pg_row_plan(&self.pool, &plan).await?;
        let next = if raw_rows.len() > requested_limit {
            raw_rows.pop();
            let last = raw_rows.last().ok_or_else(|| {
                AppError::internal("database explorer keyset page unexpectedly has no last row")
            })?;
            let position = pg_cursor_position(
                last,
                &sqlite_order_columns(&request.sort, &description.primary_key),
            )?;
            Some(self.cursor_store.issue_at_position(
                request.table.clone(),
                request.sort.clone(),
                position,
            )?)
        } else {
            None
        };
        let rows = raw_rows
            .iter()
            .map(|row| {
                Ok(DatabaseRow {
                    values: pg_row_values(row, &description)?,
                    row_ref: self.row_store.issue(
                        request.table.clone(),
                        pg_cursor_position(row, &description.primary_key)?,
                    )?,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;
        Ok(PgRowsPage { rows, next })
    }

    /// Resolve one opaque PostgreSQL-wire row reference through a fixed,
    /// primary-key-only query and apply redaction before returning it.
    pub async fn read_row_detail(&self, request: &RowDetailRequest) -> AppResult<DatabaseRow> {
        request.validate()?;
        let description = self.describe_table(&request.table).await?;
        let primary_key = self
            .row_store
            .primary_key(&request.row_ref, &request.table)?;
        let plan = plan_pg_row_detail(&description, &primary_key)?;
        let row = execute_pg_row_plan(&self.pool, &plan)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| AppError::not_found("database explorer row is no longer available"))?;
        Ok(DatabaseRow {
            values: pg_row_values(&row, &description)?,
            row_ref: request.row_ref.clone(),
        })
    }
}

#[async_trait]
impl DatabaseExplorer for PgMetadataExplorer {
    async fn list_tables(&self) -> AppResult<Vec<TableSummary>> {
        Self::list_tables(self).await
    }

    async fn describe_table(&self, table: &TableRef) -> AppResult<TableDescription> {
        Self::describe_table(self, table).await
    }

    async fn list_rows(&self, request: &ListRowsRequest) -> AppResult<RowsPage> {
        let page = self.read_rows_page(request).await?;
        Ok(RowsPage {
            table: request.table.clone(),
            rows: page.rows,
            next: page.next,
        })
    }

    async fn get_row(&self, request: &RowDetailRequest) -> AppResult<DatabaseRow> {
        self.read_row_detail(request).await
    }
}

impl SqliteMetadataExplorer {
    /// Construct an adapter over an existing Citadel SQLite pool.
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self::with_stores(
            pool,
            Arc::new(ExplorerCursorStore::new()),
            Arc::new(ExplorerRowStore::new()),
        )
    }

    /// Construct an adapter with the server-owned cursor registry.
    ///
    /// The server creates this registry once and shares it with every explorer
    /// handle for the same backend. Supplying it explicitly makes that
    /// lifecycle testable without making cursor contents client-visible.
    #[must_use]
    pub fn with_cursor_store(pool: SqlitePool, cursor_store: Arc<ExplorerCursorStore>) -> Self {
        Self::with_stores(pool, cursor_store, Arc::new(ExplorerRowStore::new()))
    }

    /// Construct an adapter with explicitly server-owned cursor and row
    /// reference registries. This is primarily useful at the server boundary,
    /// where all cloned handles must share both lifetimes.
    #[must_use]
    pub fn with_stores(
        pool: SqlitePool,
        cursor_store: Arc<ExplorerCursorStore>,
        row_store: Arc<ExplorerRowStore>,
    ) -> Self {
        Self {
            pool,
            cursor_store,
            row_store,
        }
    }

    /// Enumerate application-owned tables and views through SQLite's fixed
    /// `table_list` metadata pragma. The statement contains no caller input;
    /// table names are later resolved from this allowlist before any per-table
    /// pragma or row query is constructed.
    pub async fn list_tables(&self) -> AppResult<Vec<TableSummary>> {
        let rows = sqlx::query("PRAGMA main.table_list")
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut tables = Vec::new();
        for row in rows {
            let schema: String = row.try_get("schema").map_err(explorer_db_error)?;
            let name: String = row.try_get("name").map_err(explorer_db_error)?;
            let table_type: String = row.try_get("type").map_err(explorer_db_error)?;
            let kind = match table_type.as_str() {
                "table" => TableKind::Table,
                "view" => TableKind::View,
                _ => continue,
            };
            if schema != "main" || name.starts_with("sqlite_") {
                continue;
            }
            let table = TableRef::new(schema, name).map_err(|error| {
                AppError::database("database explorer received invalid SQLite metadata")
                    .with_detail(error.to_string())
            })?;
            tables.push(TableSummary { table, kind });
        }
        tables.sort_by(|left, right| left.table.table.cmp(&right.table.table));
        Ok(tables)
    }

    /// Describe a table only after resolving it through the fresh application
    /// allowlist. SQLite cannot bind a PRAGMA table argument, so the resulting
    /// fixed query uses a SQL string literal escaped from the allowlisted name,
    /// never directly from the caller's `TableRef`.
    pub async fn describe_table(&self, requested: &TableRef) -> AppResult<TableDescription> {
        let visible_tables = self.list_tables().await?;
        let table = visible_tables
            .iter()
            .find(|candidate| candidate.table == *requested)
            .cloned()
            .map(|candidate| candidate.table)
            .ok_or_else(|| AppError::not_found("database explorer table is not available"))?;
        let statement = format!(
            "PRAGMA main.table_xinfo({})",
            sqlite_string_literal(&table.table)
        );
        let rows = sqlx::query(&statement)
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut columns = Vec::with_capacity(rows.len());
        let mut primary_key = Vec::new();
        for row in rows {
            let name: String = row.try_get("name").map_err(explorer_db_error)?;
            let data_type: Option<String> = row.try_get("type").map_err(explorer_db_error)?;
            let not_null: i64 = row.try_get("notnull").map_err(explorer_db_error)?;
            let primary_key_position: i64 = row.try_get("pk").map_err(explorer_db_error)?;
            if primary_key_position > 0 {
                primary_key.push((primary_key_position, name.clone()));
            }
            columns.push(ColumnDescription {
                sensitive: is_sensitive_column(&name),
                name,
                data_type: data_type.unwrap_or_default(),
                nullable: not_null == 0,
            });
        }
        primary_key.sort_by_key(|(position, _)| *position);
        let primary_key = primary_key
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();
        let indexes = self.sqlite_indexes(&table).await?;
        let relations = self.sqlite_relations(&table, &visible_tables).await?;
        let description = TableDescription {
            capabilities: if primary_key.is_empty() {
                ExplorerCapabilities::metadata_only(true, true, true)
            } else {
                ExplorerCapabilities::stable(true, true, true)
            },
            table,
            columns,
            primary_key,
            indexes,
            relations,
        };
        description.validate()?;
        Ok(description)
    }

    /// Build a safe SQLite row-query plan from a fresh metadata snapshot.
    /// This is the only adapter entry point for row-query planning, preventing
    /// callers from pairing a stale description with a newer table shape.
    pub async fn plan_rows(&self, request: &ListRowsRequest) -> AppResult<SqliteRowQueryPlan> {
        let description = self.describe_table(&request.table).await?;
        let position = request
            .cursor
            .as_deref()
            .map(|cursor| {
                self.cursor_store
                    .position(cursor, &request.table, &request.sort)
            })
            .transpose()?
            .flatten();
        plan_sqlite_row_query_at_position(request, &description, position.as_ref())
    }

    /// Read one bounded SQLite page and redact values before the caller
    /// receives them. A supplied cursor is authenticated and its server-held
    /// keyset position is incorporated into the query plan.
    pub async fn read_rows(
        &self,
        request: &ListRowsRequest,
    ) -> AppResult<Vec<BTreeMap<String, DatabaseValue>>> {
        Ok(self
            .read_rows_page(request)
            .await?
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect())
    }

    /// Read one SQLite page and issue a continuation token only when a record
    /// beyond the requested limit exists. The token keeps the ordered keyset
    /// position in process; it never serializes a key into the browser.
    pub async fn read_rows_page(&self, request: &ListRowsRequest) -> AppResult<SqliteRowsPage> {
        let requested_limit = request.limit.unwrap_or(50);
        let fetch_limit = requested_limit
            .checked_add(1)
            .ok_or_else(|| AppError::internal("database explorer page limit overflow"))?;
        let mut fetch_request = request.clone();
        fetch_request.limit = Some(fetch_limit);
        let description = self.describe_table(&request.table).await?;
        let plan = self.plan_rows(&fetch_request).await?;
        let mut raw_rows = execute_sqlite_row_plan(&self.pool, &plan).await?;
        let next = if raw_rows.len() > requested_limit {
            raw_rows.pop();
            let last = raw_rows.last().ok_or_else(|| {
                AppError::internal("database explorer keyset page unexpectedly has no last row")
            })?;
            let position = sqlite_cursor_position(
                last,
                &sqlite_order_columns(&request.sort, &description.primary_key),
            )?;
            Some(self.cursor_store.issue_at_position(
                request.table.clone(),
                request.sort.clone(),
                position,
            )?)
        } else {
            None
        };
        let rows = raw_rows
            .iter()
            .map(|row| {
                Ok(DatabaseRow {
                    values: sqlite_row_values(row, &description)?,
                    row_ref: self.row_store.issue(
                        request.table.clone(),
                        sqlite_cursor_position(row, &description.primary_key)?,
                    )?,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;
        Ok(SqliteRowsPage { rows, next })
    }

    /// Resolve one opaque row reference and return redacted current data. The
    /// primary-key tuple is supplied solely by the server-side reference
    /// store; this method never accepts a browser-provided key predicate.
    pub async fn read_row_detail(&self, request: &RowDetailRequest) -> AppResult<DatabaseRow> {
        request.validate()?;
        let description = self.describe_table(&request.table).await?;
        let primary_key = self
            .row_store
            .primary_key(&request.row_ref, &request.table)?;
        let plan = plan_sqlite_row_detail(&description, &primary_key)?;
        let row = execute_sqlite_row_plan(&self.pool, &plan)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| AppError::not_found("database explorer row is no longer available"))?;
        Ok(DatabaseRow {
            values: sqlite_row_values(&row, &description)?,
            row_ref: request.row_ref.clone(),
        })
    }

    async fn sqlite_indexes(&self, table: &TableRef) -> AppResult<Vec<IndexDescription>> {
        let statement = format!(
            "PRAGMA main.index_list({})",
            sqlite_string_literal(&table.table)
        );
        let rows = sqlx::query(&statement)
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut indexes = Vec::new();
        for row in rows {
            let name: String = row.try_get("name").map_err(explorer_db_error)?;
            if name.starts_with("sqlite_autoindex") {
                continue;
            }
            let unique: i64 = row.try_get("unique").map_err(explorer_db_error)?;
            let index_statement =
                format!("PRAGMA main.index_xinfo({})", sqlite_string_literal(&name));
            let index_columns = sqlx::query(&index_statement)
                .fetch_all(&self.pool)
                .await
                .map_err(explorer_db_error)?;
            let mut columns = Vec::new();
            for column in index_columns {
                let is_key: i64 = column.try_get("key").map_err(explorer_db_error)?;
                let sequence: i64 = column.try_get("seqno").map_err(explorer_db_error)?;
                let column_name: Option<String> =
                    column.try_get("name").map_err(explorer_db_error)?;
                if is_key != 0
                    && let Some(column_name) = column_name
                {
                    columns.push((sequence, column_name));
                }
            }
            columns.sort_by_key(|(sequence, _)| *sequence);
            let columns = columns.into_iter().map(|(_, name)| name).collect();
            indexes.push(IndexDescription {
                name,
                columns,
                unique: unique != 0,
            });
        }
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(indexes)
    }

    async fn sqlite_relations(
        &self,
        table: &TableRef,
        visible_tables: &[TableSummary],
    ) -> AppResult<Vec<RelationDescription>> {
        let statement = format!(
            "PRAGMA main.foreign_key_list({})",
            sqlite_string_literal(&table.table)
        );
        let rows = sqlx::query(&statement)
            .fetch_all(&self.pool)
            .await
            .map_err(explorer_db_error)?;
        let mut grouped = BTreeMap::<i64, (TableRef, Vec<(i64, String, String)>)>::new();
        for row in rows {
            let id: i64 = row.try_get("id").map_err(explorer_db_error)?;
            let sequence: i64 = row.try_get("seq").map_err(explorer_db_error)?;
            let target: String = row.try_get("table").map_err(explorer_db_error)?;
            let from: String = row.try_get("from").map_err(explorer_db_error)?;
            let to: String = row.try_get("to").map_err(explorer_db_error)?;
            let referenced_table = TableRef::new("main", target)?;
            if !visible_tables
                .iter()
                .any(|candidate| candidate.table == referenced_table)
            {
                continue;
            }
            let entry = grouped
                .entry(id)
                .or_insert_with(|| (referenced_table, Vec::new()));
            entry.1.push((sequence, from, to));
        }
        let mut relations = Vec::new();
        for (id, (referenced_table, mut columns)) in grouped {
            columns.sort_by_key(|(sequence, _, _)| *sequence);
            relations.push(RelationDescription {
                name: format!("fk_{}_{}", table.table, id),
                columns: columns.iter().map(|(_, from, _)| from.clone()).collect(),
                referenced_table,
                referenced_columns: columns.into_iter().map(|(_, _, to)| to).collect(),
            });
        }
        Ok(relations)
    }
}

#[async_trait]
impl DatabaseExplorer for SqliteMetadataExplorer {
    async fn list_tables(&self) -> AppResult<Vec<TableSummary>> {
        Self::list_tables(self).await
    }

    async fn describe_table(&self, table: &TableRef) -> AppResult<TableDescription> {
        Self::describe_table(self, table).await
    }

    async fn list_rows(&self, request: &ListRowsRequest) -> AppResult<RowsPage> {
        let page = self.read_rows_page(request).await?;
        Ok(RowsPage {
            table: request.table.clone(),
            rows: page.rows,
            next: page.next,
        })
    }

    async fn get_row(&self, request: &RowDetailRequest) -> AppResult<DatabaseRow> {
        self.read_row_detail(request).await
    }
}

fn explorer_db_error(error: impl std::fmt::Display) -> AppError {
    AppError::database("database explorer metadata query failed").with_detail(error.to_string())
}

/// Convert a SQLite result row into safe explorer values. The caller supplies
/// the metadata snapshot that selected the projection; a sensitive column is
/// replaced before its raw database value is decoded or serialized.
pub fn sqlite_row_values(
    row: &SqliteRow,
    description: &TableDescription,
) -> AppResult<BTreeMap<String, DatabaseValue>> {
    let mut values = BTreeMap::new();
    for (position, column) in row.columns().iter().enumerate() {
        let name = column.name();
        let metadata = description
            .columns
            .iter()
            .find(|candidate| candidate.name == name)
            .ok_or_else(|| {
                AppError::internal("database explorer row contains a column outside metadata")
            })?;
        if metadata.sensitive {
            values.insert(name.to_string(), DatabaseValue::Redacted);
            continue;
        }
        let raw = row.try_get_raw(position).map_err(explorer_db_error)?;
        let is_null = raw.is_null();
        let type_name = raw.type_info().name().to_owned();
        let value = if is_null {
            DatabaseValue::Null
        } else {
            match type_name.as_str() {
                "INTEGER" => {
                    DatabaseValue::Integer(row.try_get(position).map_err(explorer_db_error)?)
                }
                "REAL" => DatabaseValue::Decimal(
                    row.try_get::<f64, _>(position)
                        .map_err(explorer_db_error)?
                        .to_string(),
                ),
                "TEXT" => DatabaseValue::Text(row.try_get(position).map_err(explorer_db_error)?),
                "BLOB" => DatabaseValue::BinaryBase64(
                    URL_SAFE_NO_PAD.encode(
                        row.try_get::<Vec<u8>, _>(position)
                            .map_err(explorer_db_error)?,
                    ),
                ),
                other => {
                    return Err(AppError::validation(format!(
                        "database explorer does not support SQLite value type {other}"
                    )));
                }
            }
        };
        values.insert(name.to_string(), value);
    }
    Ok(values)
}

/// Convert a PostgreSQL-wire row selected through `to_jsonb(column)` into the
/// portable safe value representation. Sensitive columns are not decoded at
/// all: the query already projects a typed JSON null and this loop retains the
/// redaction marker before asking SQLx for any raw value.
pub fn pg_row_values(
    row: &PgRow,
    description: &TableDescription,
) -> AppResult<BTreeMap<String, DatabaseValue>> {
    let mut values = BTreeMap::new();
    for column in &description.columns {
        if column.sensitive {
            values.insert(column.name.clone(), DatabaseValue::Redacted);
            continue;
        }
        let value: Option<Json<serde_json::Value>> = row
            .try_get(column.name.as_str())
            .map_err(explorer_db_error)?;
        values.insert(
            column.name.clone(),
            value.map_or(DatabaseValue::Null, |value| pg_json_value(value.0)),
        );
    }
    Ok(values)
}

fn pg_json_value(value: serde_json::Value) -> DatabaseValue {
    match value {
        serde_json::Value::Null => DatabaseValue::Null,
        serde_json::Value::Bool(value) => DatabaseValue::Boolean(value),
        serde_json::Value::Number(value) => value.as_i64().map_or_else(
            || DatabaseValue::Decimal(value.to_string()),
            DatabaseValue::Integer,
        ),
        serde_json::Value::String(value) => DatabaseValue::Text(value),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => DatabaseValue::Json(value),
    }
}

fn sqlite_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_sensitive_column(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    ["password", "secret", "token", "api_key", "credential"]
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn table_kind_from_information_schema(value: &str) -> Option<TableKind> {
    match value {
        "BASE TABLE" => Some(TableKind::Table),
        "VIEW" => Some(TableKind::View),
        _ => None,
    }
}

impl ListRowsRequest {
    /// Validate portable bounds before database-specific metadata checks.
    pub fn validate(&self) -> AppResult<()> {
        self.table.validate()?;
        self.sort.validate()?;
        if self.filters.len() > MAX_FILTERS {
            return Err(AppError::validation("too many database explorer filters"));
        }
        for filter in &self.filters {
            filter.validate()?;
        }
        if self
            .limit
            .is_some_and(|limit| limit == 0 || limit > MAX_PAGE_SIZE)
        {
            return Err(AppError::validation(
                "database explorer page size is out of range",
            ));
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > 4_096)
        {
            return Err(AppError::validation(
                "database explorer cursor exceeds the maximum size",
            ));
        }
        Ok(())
    }
}

/// Validate a row request against freshly introspected table metadata before an
/// adapter constructs any database statement. Sensitive columns are neither
/// filterable nor sortable: allowing them would still create a value-existence
/// oracle even though their response cells are redacted.
pub fn validate_row_request_metadata(
    request: &ListRowsRequest,
    description: &TableDescription,
) -> AppResult<()> {
    request.validate()?;
    if request.table != description.table {
        return Err(AppError::validation(
            "database explorer request does not match table metadata",
        ));
    }
    if !description.capabilities.stable_keyset_pagination {
        return Err(AppError::validation(
            "database explorer table does not support stable pagination",
        ));
    }
    let visible_column = |name: &str| {
        description
            .columns
            .iter()
            .any(|column| column.name == name && !column.sensitive)
    };
    if !visible_column(&request.sort.column) {
        return Err(AppError::validation(
            "database explorer sort column is not available",
        ));
    }
    if description
        .primary_key
        .iter()
        .any(|column| !visible_column(column))
    {
        return Err(AppError::validation(
            "database explorer primary-key tie-breaker is not available",
        ));
    }
    if request
        .filters
        .iter()
        .any(|filter| !visible_column(&filter.column))
    {
        return Err(AppError::validation(
            "database explorer filter column is not available",
        ));
    }
    Ok(())
}

/// SQLite row-query plan whose SQL structure originates exclusively in the
/// resolved metadata. Filter and keyset values stay separate for parameter
/// binding by the adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteRowQueryPlan {
    pub statement: String,
    pub values: Vec<serde_json::Value>,
}

/// PostgreSQL-wire row plan. Identifiers originate solely in a fresh
/// information-schema description and every operator value is retained for
/// positional binding. The same plan shape is used by PostgreSQL and
/// CockroachDB, but execution remains adapter-specific until each backend has
/// contract coverage.
#[derive(Debug, Clone, PartialEq)]
pub struct PgRowQueryPlan {
    pub statement: String,
    pub values: Vec<serde_json::Value>,
}

/// Adapter-level SQLite page before the HTTP layer issues row-detail handles.
/// Its continuation token is opaque and authenticated by the same adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteRowsPage {
    pub rows: Vec<DatabaseRow>,
    pub next: Option<String>,
}

/// Adapter-level PostgreSQL-wire page with server-owned continuation and row
/// references. It mirrors [`SqliteRowsPage`] so the public trait never needs to
/// know which SQL dialect issued a handle.
#[derive(Debug, Clone, PartialEq)]
pub struct PgRowsPage {
    pub rows: Vec<DatabaseRow>,
    pub next: Option<String>,
}

/// Build a bounded, metadata-resolved SQLite row query without interpolating
/// any operator-supplied value into SQL text.
pub fn plan_sqlite_row_query(
    request: &ListRowsRequest,
    description: &TableDescription,
) -> AppResult<SqliteRowQueryPlan> {
    plan_sqlite_row_query_at_position(request, description, None)
}

/// Build a SQLite plan that resumes after a server-held keyset position.
///
/// The position is never parsed from browser data: callers obtain it only
/// after [`ExplorerCursorStore`] has authenticated the opaque cursor. Values
/// remain separate parameter bindings even though the predicate structure is
/// generated from metadata-resolved columns.
pub fn plan_sqlite_row_query_at_position(
    request: &ListRowsRequest,
    description: &TableDescription,
    position: Option<&serde_json::Value>,
) -> AppResult<SqliteRowQueryPlan> {
    validate_row_request_metadata(request, description)?;
    if request.cursor.is_some() != position.is_some() {
        return Err(AppError::validation(
            "database explorer cursor has no valid keyset position",
        ));
    }
    let projection = description
        .columns
        .iter()
        .map(|column| {
            let name = sqlite_identifier(&column.name);
            if column.sensitive {
                format!("NULL AS {name}")
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::new();
    let mut predicates = request
        .filters
        .iter()
        .map(|filter| {
            let column = sqlite_identifier(&filter.column);
            match filter.operator {
                FilterOperator::IsNull => format!("{column} IS NULL"),
                FilterOperator::Contains => {
                    values.push(filter.value.clone().unwrap_or(serde_json::Value::Null));
                    format!("{column} LIKE '%' || ? || '%'")
                }
                operator => {
                    values.push(filter.value.clone().unwrap_or(serde_json::Value::Null));
                    let operator = match operator {
                        FilterOperator::Eq => "=",
                        FilterOperator::Neq => "!=",
                        FilterOperator::Lt => "<",
                        FilterOperator::Lte => "<=",
                        FilterOperator::Gt => ">",
                        FilterOperator::Gte => ">=",
                        FilterOperator::Contains | FilterOperator::IsNull => unreachable!(),
                    };
                    format!("{column} {operator} ?")
                }
            }
        })
        .collect::<Vec<_>>();
    let direction = match request.sort.direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    // A requested sort column is not necessarily unique. Append every primary
    // key component as a deterministic tie-breaker so the first page has the
    // same total ordering that the forthcoming keyset cursor executor will
    // resume. Do not repeat a primary-key column that the operator selected as
    // the leading sort field.
    let order_columns = sqlite_order_columns(&request.sort, &description.primary_key);
    if let Some(position) = position {
        predicates.push(sqlite_keyset_predicate(
            &order_columns,
            request.sort.direction,
            position,
            &mut values,
        )?);
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let order_by = order_columns
        .iter()
        .map(|column| format!("{} {direction}", sqlite_identifier(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "SELECT {projection} FROM {}{where_clause} ORDER BY {order_by} LIMIT ?",
        sqlite_identifier(&description.table.table),
    );
    values.push(serde_json::json!(request.limit.unwrap_or(50)));
    Ok(SqliteRowQueryPlan { statement, values })
}

/// Build a bounded PostgreSQL-wire row plan from metadata resolved through
/// `information_schema`. Neither a dashboard-provided table name nor a filter
/// value is interpolated into SQL: identifiers have already passed the
/// adapter allowlist and values become `$n` bindings.
pub fn plan_pg_row_query(
    request: &ListRowsRequest,
    description: &TableDescription,
) -> AppResult<PgRowQueryPlan> {
    plan_pg_row_query_at_position(request, description, None)
}

/// Build a PostgreSQL-wire plan that resumes after a server-held keyset
/// position. The opaque browser cursor is authenticated by the adapter before
/// this function receives any position values.
pub fn plan_pg_row_query_at_position(
    request: &ListRowsRequest,
    description: &TableDescription,
    position: Option<&serde_json::Value>,
) -> AppResult<PgRowQueryPlan> {
    validate_row_request_metadata(request, description)?;
    if request.cursor.is_some() != position.is_some() {
        return Err(AppError::validation(
            "database explorer cursor has no valid keyset position",
        ));
    }

    let projection = description
        .columns
        .iter()
        .map(|column| {
            let name = pg_identifier(&column.name);
            if column.sensitive {
                format!("NULL::jsonb AS {name}")
            } else {
                format!("to_jsonb({name}) AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::new();
    let mut predicates = Vec::new();
    for filter in &request.filters {
        let column = pg_identifier(&filter.column);
        let predicate = match filter.operator {
            FilterOperator::IsNull => format!("{column} IS NULL"),
            FilterOperator::Contains => {
                values.push(filter.value.clone().unwrap_or(serde_json::Value::Null));
                format!("{column}::text LIKE '%' || ${}::text || '%'", values.len())
            }
            operator => {
                values.push(filter.value.clone().unwrap_or(serde_json::Value::Null));
                let operator = match operator {
                    FilterOperator::Eq => "=",
                    FilterOperator::Neq => "!=",
                    FilterOperator::Lt => "<",
                    FilterOperator::Lte => "<=",
                    FilterOperator::Gt => ">",
                    FilterOperator::Gte => ">=",
                    FilterOperator::Contains | FilterOperator::IsNull => unreachable!(),
                };
                format!("{column} {operator} ${}", values.len())
            }
        };
        predicates.push(predicate);
    }
    let direction = match request.sort.direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    let order_columns = sqlite_order_columns(&request.sort, &description.primary_key);
    if let Some(position) = position {
        predicates.push(pg_keyset_predicate(
            &order_columns,
            request.sort.direction,
            position,
            &mut values,
        )?);
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let order_by = order_columns
        .iter()
        .map(|column| format!("{} {direction}", pg_identifier(column)))
        .collect::<Vec<_>>()
        .join(", ");
    values.push(serde_json::json!(request.limit.unwrap_or(50)));
    Ok(PgRowQueryPlan {
        statement: format!(
            "SELECT {projection} FROM {}.{}{} ORDER BY {order_by} LIMIT ${}",
            pg_identifier(&description.table.schema),
            pg_identifier(&description.table.table),
            where_clause,
            values.len(),
        ),
        values,
    })
}

fn sqlite_order_columns(sort: &SortSpec, primary_key: &[String]) -> Vec<String> {
    let mut columns = vec![sort.column.clone()];
    columns.extend(
        primary_key
            .iter()
            .filter(|column| *column != &sort.column)
            .cloned(),
    );
    columns
}

/// Capture the metadata-resolved ordering tuple from the final emitted row.
/// This value never crosses the HTTP boundary; it is held by
/// [`ExplorerCursorStore`] and later rebound by `sqlite_keyset_predicate`.
fn sqlite_cursor_position(row: &SqliteRow, columns: &[String]) -> AppResult<serde_json::Value> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        let raw = row
            .try_get_raw(column.as_str())
            .map_err(explorer_db_error)?;
        if raw.is_null() {
            return Err(AppError::validation(
                "database explorer cannot keyset-page a null ordering value",
            ));
        }
        let value = match raw.type_info().name() {
            "INTEGER" => serde_json::json!(
                row.try_get::<i64, _>(column.as_str())
                    .map_err(explorer_db_error)?
            ),
            "REAL" => serde_json::json!(
                row.try_get::<f64, _>(column.as_str())
                    .map_err(explorer_db_error)?
            ),
            "TEXT" => serde_json::json!(
                row.try_get::<String, _>(column.as_str())
                    .map_err(explorer_db_error)?
            ),
            "BLOB" => {
                return Err(AppError::validation(
                    "database explorer cannot keyset-page a binary ordering value",
                ));
            }
            other => {
                return Err(AppError::validation(format!(
                    "database explorer does not support SQLite ordering value type {other}"
                )));
            }
        };
        values.push(value);
    }
    Ok(serde_json::Value::Array(values))
}

/// Capture a PostgreSQL-wire ordering tuple from the JSONB projection. The
/// tuple stays only in the cursor/row-reference stores and is later rebound as
/// separate parameters; it never becomes part of the opaque token.
fn pg_cursor_position(row: &PgRow, columns: &[String]) -> AppResult<serde_json::Value> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        let value: Option<Json<serde_json::Value>> =
            row.try_get(column.as_str()).map_err(explorer_db_error)?;
        let value = value.map(|value| value.0).ok_or_else(|| {
            AppError::validation("database explorer cannot keyset-page a null ordering value")
        })?;
        if matches!(
            value,
            serde_json::Value::Array(_) | serde_json::Value::Object(_)
        ) {
            return Err(AppError::validation(
                "database explorer cannot keyset-page a structured ordering value",
            ));
        }
        values.push(value);
    }
    Ok(serde_json::Value::Array(values))
}

/// Build the single-row detail query from a server-held primary-key tuple.
/// This is deliberately separate from the list planner: no operator input can
/// affect its predicate structure or select a different key column.
fn plan_sqlite_row_detail(
    description: &TableDescription,
    primary_key: &serde_json::Value,
) -> AppResult<SqliteRowQueryPlan> {
    description.validate()?;
    let key_values = primary_key
        .as_array()
        .ok_or_else(|| AppError::validation("database explorer row reference is invalid"))?;
    if key_values.len() != description.primary_key.len()
        || key_values.iter().any(|value| {
            value.is_null()
                || matches!(
                    value,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                )
        })
    {
        return Err(AppError::validation(
            "database explorer row reference has an invalid primary key",
        ));
    }
    let sensitive_key = description.primary_key.iter().any(|key| {
        description
            .columns
            .iter()
            .any(|column| column.name == *key && column.sensitive)
    });
    if sensitive_key {
        return Err(AppError::validation(
            "database explorer row detail has a protected primary key",
        ));
    }
    let projection = description
        .columns
        .iter()
        .map(|column| {
            let name = sqlite_identifier(&column.name);
            if column.sensitive {
                format!("NULL AS {name}")
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = description
        .primary_key
        .iter()
        .map(|column| format!("{} = ?", sqlite_identifier(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(SqliteRowQueryPlan {
        statement: format!(
            "SELECT {projection} FROM {} WHERE {predicate} LIMIT 1",
            sqlite_identifier(&description.table.table)
        ),
        values: key_values.clone(),
    })
}

/// Build a fixed PostgreSQL-wire row-detail query from the server-held
/// primary-key tuple. It shares the JSONB projection used by row paging, so
/// decoding and redaction remain identical for a page and its detail drawer.
fn plan_pg_row_detail(
    description: &TableDescription,
    primary_key: &serde_json::Value,
) -> AppResult<PgRowQueryPlan> {
    description.validate()?;
    let key_values = primary_key
        .as_array()
        .ok_or_else(|| AppError::validation("database explorer row reference is invalid"))?;
    if key_values.len() != description.primary_key.len()
        || key_values.iter().any(|value| {
            value.is_null()
                || matches!(
                    value,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                )
        })
    {
        return Err(AppError::validation(
            "database explorer row reference has an invalid primary key",
        ));
    }
    if description.primary_key.iter().any(|key| {
        description
            .columns
            .iter()
            .any(|column| column.name == *key && column.sensitive)
    }) {
        return Err(AppError::validation(
            "database explorer row detail has a protected primary key",
        ));
    }
    let projection = description
        .columns
        .iter()
        .map(|column| {
            let name = pg_identifier(&column.name);
            if column.sensitive {
                format!("NULL::jsonb AS {name}")
            } else {
                format!("to_jsonb({name}) AS {name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = description
        .primary_key
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ${}", pg_identifier(column), index + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(PgRowQueryPlan {
        statement: format!(
            "SELECT {projection} FROM {}.{} WHERE {predicate} LIMIT 1",
            pg_identifier(&description.table.schema),
            pg_identifier(&description.table.table),
        ),
        values: key_values.clone(),
    })
}

/// Add a lexicographic SQLite keyset predicate to an already bounded query.
/// All directions are identical because the user-visible order and each
/// primary-key tie-breaker deliberately share one [`SortDirection`].
fn sqlite_keyset_predicate(
    columns: &[String],
    direction: SortDirection,
    position: &serde_json::Value,
    values: &mut Vec<serde_json::Value>,
) -> AppResult<String> {
    let position = position
        .as_array()
        .ok_or_else(|| AppError::validation("database explorer cursor position is invalid"))?;
    if position.len() != columns.len() {
        return Err(AppError::validation(
            "database explorer cursor position does not match its sort shape",
        ));
    }
    if position.iter().any(|value| {
        value.is_null()
            || matches!(
                value,
                serde_json::Value::Array(_) | serde_json::Value::Object(_)
            )
    }) {
        return Err(AppError::validation(
            "database explorer cursor position contains an unsupported value",
        ));
    }
    let comparison = match direction {
        SortDirection::Asc => ">",
        SortDirection::Desc => "<",
    };
    let mut alternatives = Vec::with_capacity(columns.len());
    for index in 0..columns.len() {
        let mut terms = Vec::with_capacity(index + 1);
        for prefix in 0..index {
            terms.push(format!("{} = ?", sqlite_identifier(&columns[prefix])));
            values.push(position[prefix].clone());
        }
        terms.push(format!(
            "{} {comparison} ?",
            sqlite_identifier(&columns[index])
        ));
        values.push(position[index].clone());
        alternatives.push(format!("({})", terms.join(" AND ")));
    }
    Ok(format!("({})", alternatives.join(" OR ")))
}

/// PostgreSQL-wire lexicographic keyset predicate. Placeholder ordinals are
/// allocated from the already-bound filter list, so every server-held ordering
/// component remains a parameter rather than SQL text.
fn pg_keyset_predicate(
    columns: &[String],
    direction: SortDirection,
    position: &serde_json::Value,
    values: &mut Vec<serde_json::Value>,
) -> AppResult<String> {
    let position = position
        .as_array()
        .ok_or_else(|| AppError::validation("database explorer cursor position is invalid"))?;
    if position.len() != columns.len()
        || position.iter().any(|value| {
            value.is_null()
                || matches!(
                    value,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                )
        })
    {
        return Err(AppError::validation(
            "database explorer cursor position does not match its sort shape",
        ));
    }
    let comparison = match direction {
        SortDirection::Asc => ">",
        SortDirection::Desc => "<",
    };
    let mut alternatives = Vec::with_capacity(columns.len());
    for index in 0..columns.len() {
        let mut terms = Vec::with_capacity(index + 1);
        for prefix in 0..index {
            values.push(position[prefix].clone());
            terms.push(format!(
                "{} = ${}",
                pg_identifier(&columns[prefix]),
                values.len()
            ));
        }
        values.push(position[index].clone());
        terms.push(format!(
            "{} {comparison} ${}",
            pg_identifier(&columns[index]),
            values.len()
        ));
        alternatives.push(format!("({})", terms.join(" AND ")));
    }
    Ok(format!("({})", alternatives.join(" OR ")))
}

/// Execute a previously validated SQLite plan, binding every value by type.
/// JSON objects and arrays are rejected at this boundary rather than coerced
/// into driver-specific strings.
pub async fn execute_sqlite_row_plan(
    pool: &SqlitePool,
    plan: &SqliteRowQueryPlan,
) -> AppResult<Vec<SqliteRow>> {
    let mut query = sqlx::query(&plan.statement);
    for value in &plan.values {
        query = match value {
            serde_json::Value::Null => query.bind(Option::<String>::None),
            serde_json::Value::Bool(value) => query.bind(*value),
            serde_json::Value::String(value) => query.bind(value),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    query.bind(value)
                } else if let Some(value) = number.as_u64() {
                    query.bind(i64::try_from(value).map_err(|_| {
                        AppError::validation("database explorer integer parameter is out of range")
                    })?)
                } else if let Some(value) = number.as_f64() {
                    query.bind(value)
                } else {
                    return Err(AppError::validation(
                        "database explorer numeric parameter is invalid",
                    ));
                }
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                return Err(AppError::validation(
                    "database explorer filter values must be scalar",
                ));
            }
        };
    }
    tokio::time::timeout(QUERY_TIMEOUT, query.fetch_all(pool))
        .await
        .map_err(|_| AppError::new(ErrorCategory::Deadline, "database explorer query timed out"))?
        .map_err(explorer_db_error)
}

/// Execute a metadata-resolved PostgreSQL-wire plan with typed scalar binds.
/// Objects and arrays are rejected instead of being coerced into `jsonb`, which
/// keeps the portable explorer filter contract deliberately small and avoids
/// dialect-specific JSON comparison semantics.
pub async fn execute_pg_row_plan(pool: &PgPool, plan: &PgRowQueryPlan) -> AppResult<Vec<PgRow>> {
    let mut query = sqlx::query(&plan.statement);
    for value in &plan.values {
        query = match value {
            serde_json::Value::Null => query.bind(Option::<String>::None),
            serde_json::Value::Bool(value) => query.bind(*value),
            serde_json::Value::String(value) => query.bind(value),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    query.bind(value)
                } else if let Some(value) = number.as_u64() {
                    query.bind(i64::try_from(value).map_err(|_| {
                        AppError::validation("database explorer integer parameter is out of range")
                    })?)
                } else if let Some(value) = number.as_f64() {
                    query.bind(value)
                } else {
                    return Err(AppError::validation(
                        "database explorer numeric parameter is invalid",
                    ));
                }
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                return Err(AppError::validation(
                    "database explorer filter values must be scalar",
                ));
            }
        };
    }
    tokio::time::timeout(QUERY_TIMEOUT, query.fetch_all(pool))
        .await
        .map_err(|_| AppError::new(ErrorCategory::Deadline, "database explorer query timed out"))?
        .map_err(explorer_db_error)
}

fn sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn pg_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Serialize an explorer response only if it stays within the operation's
/// response budget. Route handlers use this before constructing `Json`, so a
/// wide table or unexpectedly large cell cannot bypass the page-size limit.
pub fn serialize_bounded_response(value: &impl Serialize) -> AppResult<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        AppError::internal("failed to serialize database explorer response")
            .with_detail(error.to_string())
    })?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(AppError::validation(
            "database explorer response exceeds the maximum size",
        ));
    }
    Ok(bytes)
}

fn validate_identifier(kind: &str, identifier: &str) -> AppResult<()> {
    if identifier.is_empty() || identifier.len() > MAX_IDENTIFIER_BYTES {
        return Err(AppError::validation(format!(
            "{kind} must be 1-{MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    if identifier.chars().any(char::is_control) {
        return Err(AppError::validation(format!(
            "{kind} cannot contain control characters"
        )));
    }
    Ok(())
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Eq => "eq",
            Self::Neq => "neq",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Contains => "contains",
            Self::IsNull => "is_null",
        };
        f.write_str(value)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // concise fixture setup; production paths return AppResult.
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    fn request() -> ListRowsRequest {
        ListRowsRequest {
            table: TableRef::new("public", "users").unwrap(),
            filters: vec![RowFilter {
                column: "state".to_string(),
                operator: FilterOperator::Eq,
                value: Some(serde_json::json!("active")),
            }],
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(50),
        }
    }

    #[test]
    fn valid_request_has_only_bounded_logical_inputs() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn identifiers_reject_control_characters_but_are_not_sql_grammar() {
        assert!(TableRef::new("public", "player events").is_ok());
        assert!(TableRef::new("public", "users\n--").is_err());
    }

    #[test]
    fn is_null_cannot_carry_a_value() {
        let filter = RowFilter {
            column: "deleted_at".to_string(),
            operator: FilterOperator::IsNull,
            value: Some(serde_json::Value::Null),
        };
        assert!(filter.validate().is_err());
    }

    #[test]
    fn request_rejects_unbounded_pages_filters_and_cursors() {
        let mut page = request();
        page.limit = Some(MAX_PAGE_SIZE + 1);
        assert!(page.validate().is_err());

        let mut filters = request();
        filters.filters = vec![filters.filters[0].clone(); MAX_FILTERS + 1];
        assert!(filters.validate().is_err());

        let mut cursor = request();
        cursor.cursor = Some("x".repeat(4_097));
        assert!(cursor.validate().is_err());
    }

    #[test]
    fn filter_values_are_size_bounded_before_adapter_execution() {
        let filter = RowFilter {
            column: "display_name".to_string(),
            operator: FilterOperator::Contains,
            value: Some(serde_json::json!("x".repeat(MAX_FILTER_VALUE_BYTES + 1))),
        };
        assert!(filter.validate().is_err());
    }

    #[test]
    fn row_requests_can_only_target_visible_stably_paged_metadata() {
        let mut description = TableDescription {
            table: TableRef::new("main", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "session_token".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: true,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let request = ListRowsRequest {
            table: description.table.clone(),
            filters: Vec::new(),
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(20),
        };
        assert!(validate_row_request_metadata(&request, &description).is_ok());

        let sensitive_sort = ListRowsRequest {
            sort: SortSpec {
                column: "session_token".to_string(),
                direction: SortDirection::Asc,
            },
            ..request.clone()
        };
        assert!(validate_row_request_metadata(&sensitive_sort, &description).is_err());

        description.capabilities = ExplorerCapabilities::metadata_only(true, true, true);
        assert!(validate_row_request_metadata(&request, &description).is_err());
    }

    #[test]
    fn explorer_responses_have_a_hard_serialized_byte_budget() {
        assert!(serialize_bounded_response(&"x".repeat(MAX_RESPONSE_BYTES - 2)).is_ok());
        assert!(serialize_bounded_response(&"x".repeat(MAX_RESPONSE_BYTES)).is_err());
    }

    #[test]
    fn sqlite_query_plan_quotes_metadata_and_binds_every_filter_value() {
        let description = TableDescription {
            table: TableRef::new("main", "players\" archive").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "token".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: true,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let request = ListRowsRequest {
            table: description.table.clone(),
            filters: vec![RowFilter {
                column: "id".to_string(),
                operator: FilterOperator::Eq,
                value: Some(serde_json::json!("x' OR 1=1 --")),
            }],
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(10),
        };
        let plan = plan_sqlite_row_query(&request, &description).unwrap();
        assert!(plan.statement.contains("FROM \"players\"\" archive\""));
        assert!(plan.statement.contains("NULL AS \"token\""));
        assert!(!plan.statement.contains("OR 1=1"));
        assert_eq!(
            plan.values,
            vec![serde_json::json!("x' OR 1=1 --"), serde_json::json!(10)]
        );
    }

    #[test]
    fn pg_query_plan_qualifies_allowlisted_metadata_and_uses_positional_binds() {
        let description = TableDescription {
            table: TableRef::new("public", "player\" archive").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, false, false),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "password_hash".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: true,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let request = ListRowsRequest {
            table: description.table.clone(),
            filters: vec![RowFilter {
                column: "id".to_string(),
                operator: FilterOperator::Eq,
                value: Some(serde_json::json!("x' OR 1=1 --")),
            }],
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(10),
        };
        let plan = plan_pg_row_query(&request, &description).unwrap();
        assert!(
            plan.statement
                .contains("FROM \"public\".\"player\"\" archive\"")
        );
        assert!(plan.statement.contains("NULL::jsonb AS \"password_hash\""));
        assert!(plan.statement.contains("\"id\" = $1"));
        assert!(plan.statement.ends_with("LIMIT $2"));
        assert!(!plan.statement.contains("OR 1=1"));
        assert_eq!(
            plan.values,
            vec![serde_json::json!("x' OR 1=1 --"), serde_json::json!(10)]
        );
    }

    #[test]
    fn pg_query_plan_rejects_cursors_until_keyset_execution_is_implemented() {
        let description = TableDescription {
            table: TableRef::new("public", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, false, false),
            columns: vec![ColumnDescription {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                nullable: false,
                sensitive: false,
            }],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let mut request = ListRowsRequest {
            table: description.table.clone(),
            filters: Vec::new(),
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(10),
        };
        request.cursor = Some("opaque".to_string());
        assert!(plan_pg_row_query(&request, &description).is_err());
    }

    #[test]
    fn pg_contains_filter_is_bound_and_never_becomes_sql_text() {
        let description = TableDescription {
            table: TableRef::new("public", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, false, false),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "display_name".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let needle = "%'; DROP TABLE players; --";
        let request = ListRowsRequest {
            table: description.table.clone(),
            filters: vec![RowFilter {
                column: "display_name".to_string(),
                operator: FilterOperator::Contains,
                value: Some(serde_json::json!(needle)),
            }],
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(5),
        };
        let plan = plan_pg_row_query(&request, &description).unwrap();
        assert!(
            plan.statement
                .contains("\"display_name\"::text LIKE '%' || $1::text || '%'")
        );
        assert!(plan.statement.ends_with("LIMIT $2"));
        assert!(!plan.statement.contains(needle));
        assert_eq!(
            plan.values,
            vec![serde_json::json!(needle), serde_json::json!(5)]
        );
    }

    #[test]
    fn pg_json_values_keep_safe_scalar_and_structured_shapes() {
        assert_eq!(
            pg_json_value(serde_json::json!(true)),
            DatabaseValue::Boolean(true)
        );
        assert_eq!(
            pg_json_value(serde_json::json!(42)),
            DatabaseValue::Integer(42)
        );
        assert_eq!(
            pg_json_value(serde_json::json!(12.5)),
            DatabaseValue::Decimal("12.5".to_string())
        );
        assert_eq!(
            pg_json_value(serde_json::json!({"rank": 1})),
            DatabaseValue::Json(serde_json::json!({"rank": 1}))
        );
    }

    #[test]
    fn sqlite_query_plan_uses_primary_key_as_a_total_order_tie_breaker() {
        let description = TableDescription {
            table: TableRef::new("main", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "display_name".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
            ],
            primary_key: vec!["tenant".to_string(), "id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let plan = plan_sqlite_row_query(
            &ListRowsRequest {
                table: description.table.clone(),
                filters: Vec::new(),
                sort: SortSpec {
                    column: "display_name".to_string(),
                    direction: SortDirection::Desc,
                },
                cursor: None,
                limit: Some(5),
            },
            &description,
        )
        .unwrap();

        assert!(
            plan.statement
                .contains("ORDER BY \"display_name\" DESC, \"tenant\" DESC, \"id\" DESC LIMIT ?")
        );
    }

    #[test]
    fn sqlite_keyset_plan_binds_server_held_position_without_interpolation() {
        let description = TableDescription {
            table: TableRef::new("main", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "display_name".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: false,
                },
            ],
            primary_key: vec!["tenant".to_string(), "id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let request = ListRowsRequest {
            table: description.table.clone(),
            filters: Vec::new(),
            sort: SortSpec {
                column: "display_name".to_string(),
                direction: SortDirection::Desc,
            },
            cursor: Some("opaque-server-token".to_string()),
            limit: Some(5),
        };
        let position = serde_json::json!(["Ada", "tenant-a", 7]);

        let plan =
            plan_sqlite_row_query_at_position(&request, &description, Some(&position)).unwrap();

        assert!(plan.statement.contains(
            "WHERE ((\"display_name\" < ?) OR (\"display_name\" = ? AND \"tenant\" < ?) OR (\"display_name\" = ? AND \"tenant\" = ? AND \"id\" < ?))"
        ));
        assert!(!plan.statement.contains("tenant-a"));
        assert_eq!(
            plan.values,
            vec![
                serde_json::json!("Ada"),
                serde_json::json!("Ada"),
                serde_json::json!("tenant-a"),
                serde_json::json!("Ada"),
                serde_json::json!("tenant-a"),
                serde_json::json!(7),
                serde_json::json!(5),
            ]
        );
    }

    #[tokio::test]
    async fn sqlite_query_executor_binds_scalar_filters_and_never_reads_sensitive_cells() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE players (id INTEGER PRIMARY KEY, name TEXT, token TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO players (id, name, token) VALUES (1, 'Ada', 'secret')")
            .execute(&pool)
            .await
            .unwrap();
        let description = TableDescription {
            table: TableRef::new("main", "players").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "token".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    sensitive: true,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let plan = plan_sqlite_row_query(
            &ListRowsRequest {
                table: description.table.clone(),
                filters: vec![RowFilter {
                    column: "name".to_string(),
                    operator: FilterOperator::Eq,
                    value: Some(serde_json::json!("Ada")),
                }],
                sort: SortSpec {
                    column: "id".to_string(),
                    direction: SortDirection::Asc,
                },
                cursor: None,
                limit: Some(5),
            },
            &description,
        )
        .unwrap();
        let rows = execute_sqlite_row_plan(&pool, &plan).await.unwrap();
        assert_eq!(rows.len(), 1);
        let values = sqlite_row_values(&rows[0], &description).unwrap();
        assert_eq!(
            values.get("name"),
            Some(&DatabaseValue::Text("Ada".to_string()))
        );
        assert_eq!(values.get("token"), Some(&DatabaseValue::Redacted));
    }

    #[tokio::test]
    async fn sqlite_row_pages_resume_from_an_opaque_server_held_keyset() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE players (id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, token TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO players (id, display_name, token) VALUES (1, 'Ada', 'one'), (2, 'Ada', 'two'), (3, 'Bob', 'three')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let explorer = SqliteMetadataExplorer::new(pool);
        let request = ListRowsRequest {
            table: TableRef::new("main", "players").unwrap(),
            filters: Vec::new(),
            sort: SortSpec {
                column: "display_name".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(2),
        };

        let first = explorer.read_rows_page(&request).await.unwrap();
        let cursor = first.next.clone().expect("first page has a continuation");
        assert_eq!(first.rows.len(), 2);
        assert_eq!(cursor.len(), 43, "cursor is a 256-bit base64url handle");
        assert_eq!(
            explorer
                .cursor_store
                .position(&cursor, &request.table, &request.sort)
                .unwrap(),
            Some(serde_json::json!(["Ada", 2])),
            "the ordering tuple is retained only in the server-owned store"
        );
        let detail = explorer
            .read_row_detail(&RowDetailRequest {
                table: request.table.clone(),
                row_ref: first.rows[0].row_ref.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            detail.values.get("display_name"),
            Some(&DatabaseValue::Text("Ada".to_string()))
        );
        assert_eq!(detail.values.get("token"), Some(&DatabaseValue::Redacted));

        let second = explorer
            .read_rows_page(&ListRowsRequest {
                cursor: Some(cursor),
                ..request
            })
            .await
            .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(
            second.rows[0].values.get("display_name"),
            Some(&DatabaseValue::Text("Bob".to_string()))
        );
        assert!(second.next.is_none());
    }

    #[test]
    fn row_detail_reference_is_bounded_and_scoped_to_a_table() {
        let detail = RowDetailRequest {
            table: TableRef::new("public", "users").unwrap(),
            row_ref: "opaque-signed-row-reference".to_string(),
        };
        assert!(detail.validate().is_ok());

        let too_large = RowDetailRequest {
            row_ref: "x".repeat(4_097),
            ..detail
        };
        assert!(too_large.validate().is_err());
    }

    #[test]
    fn opaque_cursor_is_random_and_bound_to_its_table_and_sort() {
        let store = ExplorerCursorStore::new();
        let table = TableRef::new("main", "players").unwrap();
        let sort = SortSpec {
            column: "id".to_string(),
            direction: SortDirection::Asc,
        };
        let cursor = store.issue(table.clone(), sort.clone()).unwrap();

        assert_eq!(cursor.len(), 43, "32 random bytes encoded without padding");
        assert!(store.verify(&cursor, &table, &sort).is_ok());
        assert!(
            store
                .verify(&cursor, &TableRef::new("main", "sessions").unwrap(), &sort)
                .is_err()
        );
        assert!(
            store
                .verify(
                    &cursor,
                    &table,
                    &SortSpec {
                        column: "id".to_string(),
                        direction: SortDirection::Desc,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn opaque_cursor_expires_before_it_can_be_reused() {
        let store = ExplorerCursorStore::with_ttl(Duration::ZERO);
        let table = TableRef::new("main", "players").unwrap();
        let sort = SortSpec {
            column: "id".to_string(),
            direction: SortDirection::Asc,
        };
        let cursor = store.issue(table.clone(), sort.clone()).unwrap();
        assert!(store.verify(&cursor, &table, &sort).is_err());
    }

    #[test]
    fn opaque_cursor_keeps_its_keyset_position_server_side() {
        let store = ExplorerCursorStore::new();
        let table = TableRef::new("main", "players").unwrap();
        let sort = SortSpec {
            column: "id".to_string(),
            direction: SortDirection::Asc,
        };
        let cursor = store
            .issue_at_position(table.clone(), sort.clone(), serde_json::json!(42))
            .unwrap();
        assert_eq!(cursor.len(), 43, "cursor is a random 256-bit handle");
        assert_eq!(
            store.position(&cursor, &table, &sort).unwrap(),
            Some(serde_json::json!(42))
        );
    }

    #[test]
    fn opaque_row_reference_is_random_scoped_and_expiring() {
        let table = TableRef::new("main", "players").unwrap();
        let store = ExplorerRowStore::new();
        let reference = store
            .issue(table.clone(), serde_json::json!(["tenant-a", 42]))
            .unwrap();

        assert_eq!(reference.len(), 43);
        assert_eq!(
            store.primary_key(&reference, &table).unwrap(),
            serde_json::json!(["tenant-a", 42])
        );
        assert!(
            store
                .primary_key(&reference, &TableRef::new("main", "sessions").unwrap())
                .is_err()
        );

        let expiry_store = ExplorerRowStore::with_ttl(Duration::ZERO);
        let expired = expiry_store
            .issue(table.clone(), serde_json::json!([42]))
            .unwrap();
        assert!(expiry_store.primary_key(&expired, &table).is_err());
    }

    #[tokio::test]
    async fn sqlite_adapter_clones_share_one_server_owned_cursor_registry() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let store = Arc::new(ExplorerCursorStore::new());
        let explorer = SqliteMetadataExplorer::with_cursor_store(pool, Arc::clone(&store));
        let clone = explorer.clone();
        let table = TableRef::new("main", "players").unwrap();
        let sort = SortSpec {
            column: "id".to_string(),
            direction: SortDirection::Asc,
        };

        let cursor = explorer
            .cursor_store
            .issue(table.clone(), sort.clone())
            .unwrap();

        assert!(clone.cursor_store.verify(&cursor, &table, &sort).is_ok());
        assert!(Arc::ptr_eq(&explorer.cursor_store, &store));
    }

    #[test]
    fn redacted_value_has_no_sensitive_payload() {
        let encoded = serde_json::to_value(DatabaseValue::Redacted).unwrap();
        assert_eq!(encoded, serde_json::json!({"kind": "redacted"}));
    }

    #[test]
    fn metadata_only_tables_cannot_claim_stable_paging() {
        let description = TableDescription {
            table: TableRef::new("main", "audit_log").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![ColumnDescription {
                name: "event".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                sensitive: false,
            }],
            primary_key: Vec::new(),
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        assert!(description.validate().is_err());

        let metadata_only = TableDescription {
            capabilities: ExplorerCapabilities::metadata_only(true, true, true),
            ..description
        };
        assert!(metadata_only.validate().is_ok());
    }

    #[test]
    fn information_schema_table_kinds_are_normalized_without_catalog_assumptions() {
        assert_eq!(
            table_kind_from_information_schema("BASE TABLE"),
            Some(TableKind::Table)
        );
        assert_eq!(
            table_kind_from_information_schema("VIEW"),
            Some(TableKind::View)
        );
        assert_eq!(table_kind_from_information_schema("FOREIGN TABLE"), None);
    }

    #[test]
    fn sensitive_column_classification_is_backend_neutral() {
        assert!(is_sensitive_column("refresh_token"));
        assert!(is_sensitive_column("PASSWORD_HASH"));
        assert!(!is_sensitive_column("display_name"));
    }

    #[tokio::test]
    async fn sqlite_metadata_adapter_lists_only_application_tables_and_views() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE player_profiles (id TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE VIEW player_names AS SELECT id FROM player_profiles")
            .execute(&pool)
            .await
            .unwrap();
        let tables = SqliteMetadataExplorer::new(pool)
            .list_tables()
            .await
            .unwrap();

        assert_eq!(
            tables,
            vec![
                TableSummary {
                    table: TableRef::new("main", "player_names").unwrap(),
                    kind: TableKind::View,
                },
                TableSummary {
                    table: TableRef::new("main", "player_profiles").unwrap(),
                    kind: TableKind::Table,
                },
            ]
        );
    }

    #[tokio::test]
    async fn sqlite_metadata_adapter_resolves_before_describing_and_marks_sensitive_columns() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE credentials (tenant TEXT, user_id TEXT, password_hash TEXT NOT NULL, PRIMARY KEY (tenant, user_id)); CREATE INDEX credentials_user_id_idx ON credentials(user_id)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO credentials (tenant, user_id, password_hash) VALUES ('alpha', 'u-1', 'hash')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let explorer = SqliteMetadataExplorer::new(pool);
        let description = explorer
            .describe_table(&TableRef::new("main", "credentials").unwrap())
            .await
            .unwrap();

        assert_eq!(description.primary_key, ["tenant", "user_id"]);
        assert!(description.capabilities.stable_keyset_pagination);
        let plan = explorer
            .plan_rows(&ListRowsRequest {
                table: TableRef::new("main", "credentials").unwrap(),
                filters: Vec::new(),
                sort: SortSpec {
                    column: "user_id".to_string(),
                    direction: SortDirection::Asc,
                },
                cursor: None,
                limit: Some(10),
            })
            .await
            .unwrap();
        assert!(plan.statement.contains("FROM \"credentials\""));
        assert!(plan.statement.contains("NULL AS \"password_hash\""));
        let rows = explorer
            .read_rows(&ListRowsRequest {
                table: TableRef::new("main", "credentials").unwrap(),
                filters: Vec::new(),
                sort: SortSpec {
                    column: "user_id".to_string(),
                    direction: SortDirection::Asc,
                },
                cursor: None,
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(rows[0].get("password_hash"), Some(&DatabaseValue::Redacted));
        assert!(
            explorer
                .read_rows(&ListRowsRequest {
                    table: TableRef::new("main", "credentials").unwrap(),
                    filters: Vec::new(),
                    sort: SortSpec {
                        column: "user_id".to_string(),
                        direction: SortDirection::Asc,
                    },
                    cursor: Some("unverified-cursor".to_string()),
                    limit: Some(10),
                })
                .await
                .is_err()
        );
        assert_eq!(
            description.indexes,
            vec![IndexDescription {
                name: "credentials_user_id_idx".to_string(),
                columns: vec!["user_id".to_string()],
                unique: false,
            }]
        );
        assert!(
            description
                .columns
                .iter()
                .find(|column| column.name == "password_hash")
                .unwrap()
                .sensitive
        );
        assert!(
            explorer
                .describe_table(&TableRef::new("main", "not_a_real_table").unwrap())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn sqlite_metadata_adapter_exposes_relations_only_between_visible_tables() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE players (id TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE player_items (id TEXT PRIMARY KEY, player_id TEXT NOT NULL REFERENCES players(id))",
        )
        .execute(&pool)
        .await
        .unwrap();
        let explorer = SqliteMetadataExplorer::new(pool);
        let description = explorer
            .describe_table(&TableRef::new("main", "player_items").unwrap())
            .await
            .unwrap();

        assert!(description.capabilities.foreign_keys);
        assert_eq!(
            description.relations,
            vec![RelationDescription {
                name: "fk_player_items_0".to_string(),
                columns: vec!["player_id".to_string()],
                referenced_table: TableRef::new("main", "players").unwrap(),
                referenced_columns: vec!["id".to_string()],
            }]
        );
    }

    #[tokio::test]
    async fn sqlite_row_values_redact_before_serialization_and_preserve_binary_data() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let row = sqlx::query(
            "SELECT 42 AS id, 'hidden' AS session_token, X'0102' AS payload, NULL AS note",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let description = TableDescription {
            table: TableRef::new("main", "sessions").unwrap(),
            capabilities: ExplorerCapabilities::stable(true, true, true),
            columns: vec![
                ColumnDescription {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "session_token".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    sensitive: true,
                },
                ColumnDescription {
                    name: "payload".to_string(),
                    data_type: "blob".to_string(),
                    nullable: false,
                    sensitive: false,
                },
                ColumnDescription {
                    name: "note".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    sensitive: false,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            relations: Vec::new(),
        };
        let values = sqlite_row_values(&row, &description).unwrap();

        assert_eq!(values.get("id"), Some(&DatabaseValue::Integer(42)));
        assert_eq!(values.get("session_token"), Some(&DatabaseValue::Redacted));
        assert_eq!(
            values.get("payload"),
            Some(&DatabaseValue::BinaryBase64("AQI".to_string()))
        );
        assert_eq!(values.get("note"), Some(&DatabaseValue::Null));
    }
}
