//! Read-only administrative database explorer.
//!
//! This surface accepts no SQL text. It forwards only typed, bounded requests
//! to the durable backend capability selected at startup. Both console roles
//! may read; audit entries deliberately omit filter values and returned cells.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use serde::Serialize;

use crate::app::App;
use crate::database_explorer::{
    DatabaseExplorer, DatabaseRow, ListRowsRequest, RowDetailRequest, RowsPage, TableDescription,
    TableRef, TableSummary, serialize_bounded_response,
};
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{AuditEntry, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

pub const DATABASE_PATH: &str = "/console/v1/database";
pub const DATABASE_TABLE_PATH: &str = "/console/v1/database/:schema/:table";
pub const DATABASE_ROWS_PATH: &str = "/console/v1/database/rows";
pub const DATABASE_ROW_PATH: &str = "/console/v1/database/row";
const API_KEYS_RELATION: &str = "api_keys";

#[derive(Debug, Clone, Serialize)]
pub struct TablesResponse {
    pub tables: Vec<TableSummary>,
}

fn explorer(app: &App) -> Result<std::sync::Arc<dyn DatabaseExplorer>, ApiError> {
    app.backend().database_explorer().ok_or_else(|| {
        AppError::validation("database explorer requires a durable database backend").into()
    })
}

fn bounded_json<T: Serialize>(value: T) -> Result<Json<T>, ApiError> {
    serialize_bounded_response(&value).map_err(ApiError::from)?;
    Ok(Json(value))
}

fn record_read(app: &App, operator: &ConsolePrincipal, action: &str, target: String) {
    app.audit_log().record(AuditEntry::for_principal(
        SystemClock.now(),
        operator,
        action,
        target,
        "read-only database explorer request",
    ));
}

fn admit(app: &App, operator: &ConsolePrincipal) -> Result<(), ApiError> {
    app.database_explorer_rate_limiter()
        .admit(&operator.actor_id())
        .map_err(ApiError::rate_limited)
}

fn may_inspect_api_keys(operator: &ConsolePrincipal) -> bool {
    operator.require_admin().is_ok()
}

fn guard_table(operator: &ConsolePrincipal, table: &TableRef) -> Result<(), ApiError> {
    if table.table == API_KEYS_RELATION && !may_inspect_api_keys(operator) {
        return Err(
            AppError::permission("only a human administrator may inspect API-key storage").into(),
        );
    }
    Ok(())
}

pub(super) async fn tables_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
) -> Result<Json<TablesResponse>, ApiError> {
    app.metrics().record_http_request();
    admit(&app, &operator)?;
    let mut tables = explorer(&app)?
        .list_tables()
        .await
        .map_err(ApiError::from)?;
    if !may_inspect_api_keys(&operator) {
        tables.retain(|summary| summary.table.table != API_KEYS_RELATION);
    }
    record_read(
        &app,
        &operator,
        "database.list_tables",
        "database".to_string(),
    );
    bounded_json(TablesResponse { tables })
}

pub(super) async fn table_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((schema, table)): Path<(String, String)>,
) -> Result<Json<TableDescription>, ApiError> {
    app.metrics().record_http_request();
    admit(&app, &operator)?;
    let table_ref = TableRef::new(schema, table).map_err(ApiError::from)?;
    guard_table(&operator, &table_ref)?;
    let description = explorer(&app)?
        .describe_table(&table_ref)
        .await
        .map_err(ApiError::from)?;
    record_read(&app, &operator, "database.describe_table", table_ref.table);
    bounded_json(description)
}

pub(super) async fn rows_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    body: Result<Json<ListRowsRequest>, JsonRejection>,
) -> Result<Json<RowsPage>, ApiError> {
    app.metrics().record_http_request();
    admit(&app, &operator)?;
    let request = body
        .map_err(|rejection| {
            AppError::validation("invalid database explorer request")
                .with_detail(rejection.body_text())
        })?
        .0;
    guard_table(&operator, &request.table)?;
    let target = request.table.table.clone();
    let page = explorer(&app)?
        .list_rows(&request)
        .await
        .map_err(ApiError::from)?;
    record_read(&app, &operator, "database.list_rows", target);
    bounded_json(page)
}

pub(super) async fn row_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    body: Result<Json<RowDetailRequest>, JsonRejection>,
) -> Result<Json<DatabaseRow>, ApiError> {
    app.metrics().record_http_request();
    admit(&app, &operator)?;
    let request = body
        .map_err(|rejection| {
            AppError::validation("invalid database explorer request")
                .with_detail(rejection.body_text())
        })?
        .0;
    guard_table(&operator, &request.table)?;
    let target = request.table.table.clone();
    let row = explorer(&app)?
        .get_row(&request)
        .await
        .map_err(ApiError::from)?;
    record_read(&app, &operator, "database.get_row", target);
    bounded_json(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_paths_are_a_registered_read_only_section() {
        assert!(super::super::SECTION_PATHS.contains(&DATABASE_PATH));
        assert!(DATABASE_TABLE_PATH.starts_with(DATABASE_PATH));
        assert!(DATABASE_ROWS_PATH.starts_with(DATABASE_PATH));
        assert!(DATABASE_ROW_PATH.starts_with(DATABASE_PATH));
    }
}
