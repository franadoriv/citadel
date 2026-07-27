//! Apply Citadel's PostgreSQL migrations.
//!
//! This is the entrypoint behind `make db-migrate` / `.\make.ps1 db-migrate`.
//! It connects to the database named by `DATABASE_URL` (or
//! `CITADEL_DATABASE_URL`) and runs every embedded migration; `PgDatabase`
//! applies migrations on connect, so this is idempotent and safe to re-run.
//!
//! ```text
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo run --example db_migrate
//! ```

use std::process::ExitCode;

use citadel::config::DatabaseConfig;
use citadel::repository::PgDatabase;

#[tokio::main]
async fn main() -> ExitCode {
    let url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty());

    let Some(url) = url else {
        eprintln!(
            "db_migrate: set DATABASE_URL (or CITADEL_DATABASE_URL) to a \
             postgres:// connection string"
        );
        return ExitCode::FAILURE;
    };

    let config = DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    };

    match PgDatabase::connect(&config).await {
        Ok(_) => {
            println!("db_migrate: migrations applied successfully");
            ExitCode::SUCCESS
        }
        Err(error) => {
            // `operator_log` includes sanitized detail but never the URL/secret.
            eprintln!(
                "db_migrate: failed to apply migrations: {}",
                error.operator_log()
            );
            ExitCode::FAILURE
        }
    }
}
