//! Citadel: a Rust-native, modular game server.
//!
//! This crate hosts the reusable server library. The binary in `main.rs` stays
//! thin and delegates to these modules. The module layout was established by
//! ; CLI parsing and layered config loading landed in  and
//! observability/error handling in . The HTTP listener and graceful
//! shutdown for `citadel serve` are wired by . Identity/session domain
//! contracts (`identity`, `session`, `time`) landed in .

pub mod app;
pub mod chat_cluster;
pub mod cli;
pub mod config;
pub mod database_explorer;
pub mod error;
pub mod error_journal;
pub mod http;
pub mod identity;
pub mod lifecycle;
pub mod maps;
pub mod matchmaker;
pub mod matchmaker_cluster;
pub mod matchmaker_live;
pub mod matchmaker_transport;
pub mod observability;
pub mod party;
pub mod realtime;
pub mod repository;
pub mod runtime;
pub mod services;
pub mod session;
pub mod startup;
pub mod storage;
pub mod time;
pub mod transport;

mod validate;

pub use app::{App, VERSION};
pub use cli::{Cli, Command};
pub use config::Config;
pub use error::{AppError, AppResult, ErrorCategory};

/// Project name constant, used in startup diagnostics.
pub const PROJECT_NAME: &str = "citadel";

/// Human-readable startup identifier.
#[must_use]
pub fn startup_message() -> String {
    format!("{PROJECT_NAME}: Rust-native game server foundation")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_message_identifies_project() {
        assert_eq!(
            startup_message(),
            "citadel: Rust-native game server foundation"
        );
    }
}
