//! Citadel binary entrypoint.
//!
//! The binary stays thin: parse the CLI, resolve and validate configuration,
//! initialize observability, and dispatch to the chosen subcommand. `check`
//! runs synchronously; `serve` builds a Tokio runtime and delegates to
//! [`citadel::http::run`], which binds the HTTP listener and serves with
//! graceful shutdown. `anyhow` is permitted here at the bootstrap boundary per
//! the core platform crate selection.

use std::io::{self, Write};

use anyhow::{Context, Result};
use clap::Parser;

use citadel::cli::{self, Cli, Command};
use citadel::config::Config;
use citadel::startup::{self, StdioPrompt, WizardPaths, WizardReport};
use citadel::{App, http, observability};

fn main() -> Result<()> {
    #[cfg(feature = "runtime-python")]
    let _python_bundle = citadel::runtime::configure_bundled_python_runtime();

    let cli = Cli::parse();

    match cli.command() {
        Command::Check => {
            let config = cli::run_check(&cli.global).context("configuration check failed")?;
            // Initialize logging after a successful check so diagnostics honor
            // the resolved logging settings.
            let _ = observability::init(&config.logging).context("failed to initialize logging")?;
            println!(
                "config ok: node_id={} bind={} log_level={} log_format={}",
                config.server.node_id,
                config.http.bind,
                config.logging.level,
                config.logging.format.as_str(),
            );
            Ok(())
        }
        Command::Serve => {
            let mut config = cli
                .global
                .resolve_config()
                .context("configuration check failed")?;
            let _ = observability::init(&config.logging).context("failed to initialize logging")?;

            // First-run wizard: on an interactive terminal (no `--config`, no
            // `--yes`), offer to scaffold a gameplay script and choose a
            // database. On CI/headless/non-interactive runs this is skipped and
            // the existing silent auto-defaults apply.
            run_wizard_if_interactive(&cli, &mut config).context("first-run setup failed")?;

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build Tokio runtime")?;
            runtime.block_on(async {
                // Select and connect the persistence backend before serving so
                // a configured-but-unreachable database fails fast at startup.
                let app = App::bootstrap(config)
                    .await
                    .context("failed to initialize persistence backend")?;
                http::run(app).await.context("HTTP server failed")
            })?;
            Ok(())
        }
    }
}

/// Run the interactive first-run wizard when the environment allows it.
///
/// Gated by [`startup::should_run_wizard`]: a real TTY, no explicit `--config`,
/// and no `--yes`/`--non-interactive`. When it runs and makes a choice, a concise
/// summary is printed so the operator sees what was created.
fn run_wizard_if_interactive(cli: &Cli, config: &mut Config) -> Result<()> {
    let explicit_config = cli.global.config.is_some();
    if !startup::should_run_wizard(explicit_config, cli.global.yes) {
        return Ok(());
    }
    let paths = WizardPaths::from_config(config, cli.global.config.as_deref());
    let stdin = io::stdin();
    let mut prompt = StdioPrompt::new(stdin.lock(), io::stdout());
    let report = startup::run_first_run_wizard(config, &paths, &mut prompt)
        .context("interactive setup failed")?;
    print_wizard_summary(&report, &paths);
    Ok(())
}

/// Print a short summary of what the wizard created, if anything.
fn print_wizard_summary(report: &WizardReport, paths: &WizardPaths) {
    let mut out = io::stdout().lock();
    if let Some(script) = &report.created_script {
        let _ = writeln!(out, "Created gameplay script: {}", script.display());
    }
    if let Some(choice) = &report.selected_database {
        let _ = writeln!(
            out,
            "Selected database: {} (saved to {})",
            choice.label(),
            paths.config_path.display()
        );
    }
    if report.made_changes() {
        let _ = writeln!(out);
    }
}
