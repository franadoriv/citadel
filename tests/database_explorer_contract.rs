//! Live SQL-backend contract checks for the read-only console database explorer
//!. PostgreSQL and CockroachDB runs are opt-in because CI/local
//! developers may not have the disposable services running.

use citadel::config::DatabaseConfig;
use citadel::database_explorer::{
    DatabaseExplorer, ListRowsRequest, SortDirection, SortSpec, TableRef,
};
use citadel::repository::{MongoDatabase, PgDatabase};

async fn explorer_contract(url: String) {
    let config = DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    };
    let backend = PgDatabase::connect(&config)
        .await
        .expect("connect and migrate SQL backend");
    let explorer = backend.database_explorer();
    let users = TableRef::new("public", "users").expect("valid table reference");

    assert!(
        explorer
            .list_tables()
            .await
            .expect("list tables")
            .iter()
            .any(|table| table.table == users)
    );
    let description = explorer
        .describe_table(&users)
        .await
        .expect("describe users table");
    assert_eq!(description.primary_key, ["id"]);
    assert!(description.capabilities.stable_keyset_pagination);
    assert!(!description.capabilities.indexes);
    assert!(!description.capabilities.foreign_keys);

    let page = explorer
        .list_rows(&ListRowsRequest {
            table: users,
            filters: Vec::new(),
            sort: SortSpec {
                column: "id".to_string(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(1),
        })
        .await
        .expect("execute bounded row page");
    assert!(page.rows.len() <= 1);
}

#[tokio::test]
async fn postgres_database_explorer_contract() {
    let Some(url) = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping PostgreSQL database explorer contract: set DATABASE_URL");
        return;
    };
    explorer_contract(url).await;
}

#[tokio::test]
async fn cockroach_database_explorer_contract() {
    let Some(raw) = std::env::var("CITADEL_TEST_COCKROACH_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!(
            "skipping CockroachDB database explorer contract: set CITADEL_TEST_COCKROACH_URL"
        );
        return;
    };
    let url = raw
        .strip_prefix("postgresql://")
        .map(|rest| format!("cockroach://{rest}"))
        .or_else(|| {
            raw.strip_prefix("postgres://")
                .map(|rest| format!("cockroach://{rest}"))
        })
        .unwrap_or(raw);
    explorer_contract(url).await;
}

#[tokio::test]
async fn mongodb_database_explorer_contract() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping MongoDB database explorer contract: set CITADEL_TEST_MONGODB_URL");
        return;
    };
    let backend = MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect MongoDB backend");
    let explorer = backend.database_explorer();
    let tables = explorer
        .list_tables()
        .await
        .expect("list MongoDB collections");
    let users = TableRef::new("mongodb", "users").expect("valid MongoDB collection reference");
    assert!(tables.iter().any(|table| table.table == users));
    let description = explorer
        .describe_table(&users)
        .await
        .expect("describe MongoDB collection");
    assert_eq!(description.primary_key, ["_id"]);
    assert!(description.capabilities.stable_keyset_pagination);
    assert!(description.capabilities.indexes);
    assert!(!description.capabilities.foreign_keys);
    let page = explorer
        .list_rows(&ListRowsRequest {
            table: users,
            filters: Vec::new(),
            sort: SortSpec {
                column: "_id".to_owned(),
                direction: SortDirection::Asc,
            },
            cursor: None,
            limit: Some(1),
        })
        .await
        .expect("execute bounded MongoDB row page");
    assert!(page.rows.len() <= 1);
}
