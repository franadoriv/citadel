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
        self.command.clone().unwrap_or(Command::Serve)
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
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Load and validate configuration, then start the server.
    Serve,
    /// Load and validate configuration without starting listeners.
    Check,
    /// Convert a supported Tiled TMX collision map into Citadel CMAP.
    CookTmx {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Internal supervised GameScript worker; not an operator-facing command.
    #[command(hide = true)]
    RuntimeWorker {
        #[arg(long)]
        bootstrap_endpoint: String,
        #[arg(long)]
        bootstrap_fd: i32,
        /// Pid of the supervising parent, used to detect a parent that died
        /// before the worker armed its parent-death signal (unix) and to
        /// validate the pipe server's identity before the handshake
        /// (windows).
        #[arg(long)]
        parent_pid: u32,
        /// Open-file limit from the supervisor's resource policy. Applied
        /// before the bootstrap secret is read on unix; surfaced as
        /// kernel-unenforceable on windows, where containment comes from the
        /// job object instead.
        #[arg(long)]
        max_open_files: u64,
        /// How often (in milliseconds) the worker emits a health frame after
        /// readiness; supplied by the supervisor's health policy.
        #[arg(long)]
        health_cadence_ms: u64,
        /// Engine token for the hosted GameScript (`lua` / `js` / `python`).
        /// Present only when the deployment runs a script under the
        /// external-worker adapter; requires the other `--data-*`/script
        /// flags.
        #[arg(long, requires_all = ["entrypoint", "script_deadline_ms", "tick_ms", "data_endpoint", "data_epoch"])]
        engine: Option<String>,
        /// Entry point file of the hosted script.
        #[arg(long, requires = "engine")]
        entrypoint: Option<std::path::PathBuf>,
        /// Per-invocation handler budget for the hosted script, in ms.
        #[arg(long, requires = "engine")]
        script_deadline_ms: Option<u64>,
        /// Scheduler round cadence for the hosted matches, in ms.
        #[arg(long, requires = "engine")]
        tick_ms: Option<u64>,
        /// Private endpoint of the match data plane (unix socket path or
        /// named-pipe name), created by the supervisor.
        #[arg(long, requires = "engine")]
        data_endpoint: Option<String>,
        /// Worker-generation epoch stamped on every data-plane frame.
        #[arg(long, requires = "engine")]
        data_epoch: Option<u64>,
    },
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
    fn parses_runtime_worker_only_with_bootstrap_endpoint() {
        let cli = Cli::try_parse_from([
            "citadel",
            "runtime-worker",
            "--bootstrap-endpoint",
            "/tmp/citadel.sock",
            "--bootstrap-fd",
            "3",
            "--parent-pid",
            "42",
            "--max-open-files",
            "64",
            "--health-cadence-ms",
            "250",
        ])
        .expect("internal worker parses with endpoint");
        assert_eq!(
            cli.command(),
            Command::RuntimeWorker {
                bootstrap_endpoint: "/tmp/citadel.sock".to_string(),
                bootstrap_fd: 3,
                parent_pid: 42,
                max_open_files: 64,
                health_cadence_ms: 250,
                engine: None,
                entrypoint: None,
                script_deadline_ms: None,
                tick_ms: None,
                data_endpoint: None,
                data_epoch: None,
            }
        );
        assert!(Cli::try_parse_from(["citadel", "runtime-worker"]).is_err());
        assert!(
            Cli::try_parse_from([
                "citadel",
                "runtime-worker",
                "--bootstrap-endpoint",
                "/tmp/citadel.sock",
                "--bootstrap-fd",
                "3",
                "--max-open-files",
                "64",
            ])
            .is_err(),
            "the supervising parent pid is mandatory"
        );
        assert!(
            Cli::try_parse_from([
                "citadel",
                "runtime-worker",
                "--bootstrap-endpoint",
                "/tmp/citadel.sock",
                "--bootstrap-fd",
                "3",
                "--parent-pid",
                "42",
            ])
            .is_err(),
            "the resource policy is mandatory"
        );
        assert!(
            Cli::try_parse_from([
                "citadel",
                "runtime-worker",
                "--bootstrap-endpoint",
                "/tmp/citadel.sock",
                "--bootstrap-fd",
                "3",
                "--parent-pid",
                "42",
                "--max-open-files",
                "64",
            ])
            .is_err(),
            "the health cadence is mandatory"
        );
    }

    #[test]
    fn parses_runtime_worker_script_hosting_flags_as_a_group() {
        let cli = Cli::try_parse_from([
            "citadel",
            "runtime-worker",
            "--bootstrap-endpoint",
            "/tmp/citadel.sock",
            "--bootstrap-fd",
            "3",
            "--parent-pid",
            "42",
            "--max-open-files",
            "64",
            "--health-cadence-ms",
            "250",
            "--engine",
            "lua",
            "--entrypoint",
            "/game/main.lua",
            "--script-deadline-ms",
            "50",
            "--tick-ms",
            "25",
            "--data-endpoint",
            "/tmp/citadel-data.sock",
            "--data-epoch",
            "7",
        ])
        .expect("script-hosting worker parses");
        let Command::RuntimeWorker {
            engine,
            entrypoint,
            script_deadline_ms,
            tick_ms,
            data_endpoint,
            data_epoch,
            ..
        } = cli.command()
        else {
            unreachable!("expected the runtime-worker command");
        };
        assert_eq!(engine.as_deref(), Some("lua"));
        assert_eq!(entrypoint, Some(PathBuf::from("/game/main.lua")));
        assert_eq!(script_deadline_ms, Some(50));
        assert_eq!(tick_ms, Some(25));
        assert_eq!(data_endpoint.as_deref(), Some("/tmp/citadel-data.sock"));
        assert_eq!(data_epoch, Some(7));
        // The script flags are all-or-nothing: an engine without its data
        // endpoint (or vice versa) must be rejected, never half-configured.
        assert!(
            Cli::try_parse_from([
                "citadel",
                "runtime-worker",
                "--bootstrap-endpoint",
                "/tmp/citadel.sock",
                "--bootstrap-fd",
                "3",
                "--parent-pid",
                "42",
                "--max-open-files",
                "64",
                "--health-cadence-ms",
                "250",
                "--engine",
                "lua",
            ])
            .is_err(),
            "an engine without the rest of the script flags must be rejected"
        );
        assert!(
            Cli::try_parse_from([
                "citadel",
                "runtime-worker",
                "--bootstrap-endpoint",
                "/tmp/citadel.sock",
                "--bootstrap-fd",
                "3",
                "--parent-pid",
                "42",
                "--max-open-files",
                "64",
                "--health-cadence-ms",
                "250",
                "--data-endpoint",
                "/tmp/citadel-data.sock",
            ])
            .is_err(),
            "a data endpoint without an engine must be rejected"
        );
    }

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
