//! Command-line interface for Citadel.
//!
//!  scope: define the `clap` command surface (`serve`, `check`) and
//! the narrow global flags, plus a `check` execution path that loads and
//! validates configuration without starting any listener. The `serve` listener
//! and graceful shutdown are wired by ; this task only defines the
//! command and resolves config for it.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::{Config, ConfigOverrides};
use crate::error::AppResult;

/// Top-level Citadel CLI.
///
/// The subcommand is optional: running the binary with no subcommand defaults to
/// [`Command::Serve`] (the "unzip and run" story). Use [`Cli::command`] to
/// resolve the effective command.
#[derive(Debug, Parser)]
#[command(name = "citadel", version, about = "Citadel game server")]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Cli {
    /// The effective command, defaulting to [`Command::Serve`] when no
    /// subcommand was given.
    #[must_use]
    pub fn command(&self) -> Command {
        self.command.unwrap_or(Command::Serve)
    }
}

/// Narrow, high-signal startup flags shared by subcommands.
#[derive(Debug, Default, clap::Args)]
pub struct GlobalArgs {
    /// Path to a TOML config file.
    #[arg(long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// Override the log level directive (e.g. `info`, `debug`).
    #[arg(long, value_name = "LEVEL", global = true)]
    pub log_level: Option<String>,

    /// Override the HTTP bind address (e.g. `127.0.0.1:7350`).
    #[arg(long, value_name = "ADDR", global = true)]
    pub bind: Option<String>,

    /// Override the node identifier.
    #[arg(long, value_name = "ID", global = true)]
    pub node_id: Option<String>,

    /// Assume "yes" to first-run prompts and never open the interactive wizard.
    ///
    /// Equivalent to a non-interactive run: the server takes the existing silent
    /// auto-defaults instead of prompting. Aliased as `--non-interactive`.
    #[arg(long, visible_alias = "non-interactive", global = true)]
    pub yes: bool,
}

impl GlobalArgs {
    /// Convert CLI flags into config overrides.
    #[must_use]
    pub fn overrides(&self) -> ConfigOverrides {
        ConfigOverrides {
            log_level: self.log_level.clone(),
            bind: self.bind.clone(),
            node_id: self.node_id.clone(),
        }
    }

    /// Resolve configuration using this command's flags and the environment.
    pub fn resolve_config(&self) -> AppResult<Config> {
        Config::load(self.config.as_deref(), &self.overrides())
    }
}

/// Citadel subcommands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Load and validate configuration, then start the server.
    Serve,
    /// Load and validate configuration without starting listeners.
    Check,
}

/// Execute `citadel check`: resolve and validate configuration.
///
/// Returns the validated [`Config`] on success. Validation failures surface as
/// [`Config`](crate::error::ErrorCategory::Config) errors. No secrets are
/// printed; only field names and non-secret values appear in diagnostics.
pub fn run_check(global: &GlobalArgs) -> AppResult<Config> {
    let config = global.resolve_config()?;
    // `resolve_config` validates the static config shape. Build the selected
    // runtime too, then drop it, so `citadel check` catches a broken main.lua or
    // main.py before an operator starts listeners.
    crate::transport::validate_runtime_for_check(&config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_check_subcommand() {
        let cli = Cli::try_parse_from(["citadel", "check"]).expect("parses check");
        assert_eq!(cli.command(), Command::Check);
    }

    #[test]
    fn parses_serve_with_global_flags() {
        let cli = Cli::try_parse_from([
            "citadel",
            "--config",
            "citadel.toml",
            "--log-level",
            "debug",
            "serve",
        ])
        .expect("parses serve");
        assert_eq!(cli.command(), Command::Serve);
        assert_eq!(cli.global.config, Some(PathBuf::from("citadel.toml")));
        assert_eq!(cli.global.log_level.as_deref(), Some("debug"));
    }

    #[test]
    fn no_subcommand_defaults_to_serve() {
        let cli = Cli::try_parse_from(["citadel"]).expect("no subcommand is allowed");
        assert!(cli.command.is_none(), "no explicit subcommand parsed");
        assert_eq!(cli.command(), Command::Serve, "defaults to serve");
    }

    #[test]
    fn global_flags_work_without_a_subcommand() {
        let cli = Cli::try_parse_from(["citadel", "--bind", "127.0.0.1:9000", "--yes"])
            .expect("global flags parse with no subcommand");
        assert_eq!(cli.command(), Command::Serve);
        assert!(cli.global.yes);
        assert_eq!(
            cli.global.overrides().bind.as_deref(),
            Some("127.0.0.1:9000")
        );
    }

    #[test]
    fn non_interactive_alias_sets_yes() {
        let cli = Cli::try_parse_from(["citadel", "--non-interactive", "serve"])
            .expect("--non-interactive alias parses");
        assert!(cli.global.yes);
    }

    #[test]
    fn global_flags_map_to_overrides() {
        let cli =
            Cli::try_parse_from(["citadel", "--bind", "127.0.0.1:9000", "check"]).expect("parses");
        let overrides = cli.global.overrides();
        assert_eq!(overrides.bind.as_deref(), Some("127.0.0.1:9000"));
        assert!(overrides.log_level.is_none());
    }

    #[test]
    fn run_check_resolves_and_validates_config_with_no_flags() {
        // With no `--config`, config resolution discovers `./citadel.toml` when
        // present (the repo ships one) and otherwise uses the built-in defaults;
        // either way the resolved config must be valid with a non-empty node id.
        // (Discovery falling back to defaults is covered directly in `config`.)
        let global = GlobalArgs::default();
        let config = run_check(&global).expect("resolved config validates");
        assert!(!config.server.node_id.trim().is_empty());
    }

    #[test]
    fn run_check_rejects_invalid_bind_override() {
        let global = GlobalArgs {
            bind: Some("not-an-addr".to_string()),
            ..GlobalArgs::default()
        };
        let err = run_check(&global).expect_err("invalid bind must fail");
        assert_eq!(err.category(), crate::error::ErrorCategory::Config);
    }
}
