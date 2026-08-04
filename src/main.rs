//! Citadel binary entrypoint.
//!
//! The binary stays thin: parse the CLI, resolve and validate configuration,
//! initialize observability, and dispatch to the chosen subcommand. `check`
//! runs synchronously; `serve` builds a Tokio runtime and delegates to
//! [`citadel::http::run`], which binds the HTTP listener and serves with
//! graceful shutdown. `anyhow` is permitted here at the bootstrap boundary per
//! the core platform crate selection.

use std::fs;
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use anyhow::{Context, Result};
use clap::Parser;

use citadel::cli::{self, Cli, Command};
use citadel::config::Config;
use citadel::startup::{self, StdioPrompt, WizardPaths, WizardReport};
use citadel::{App, error_reporting, http, observability};

#[cfg(unix)]
fn run_runtime_worker(endpoint: &str, bootstrap_fd: i32) -> Result<()> {
    let secret = citadel::runtime::worker_bootstrap::read_secret_from_fd(bootstrap_fd)
        .context("runtime worker bootstrap secret unavailable")?;
    let mut stream =
        UnixStream::connect(endpoint).context("runtime worker bootstrap connect failed")?;
    let frame = citadel::runtime::worker_protocol::read_control_frame(&mut stream)
        .map_err(|_| anyhow::anyhow!("runtime worker parent hello invalid"))?;
    let (protocol_version, nonce) = match frame {
        citadel::runtime::worker_protocol::ControlFrame::ParentHello {
            protocol_version,
            nonce,
        } => (protocol_version, nonce),
        _ => anyhow::bail!("runtime worker expected parent hello"),
    };
    if protocol_version != citadel::runtime::worker_protocol::PROTOCOL_VERSION {
        anyhow::bail!("runtime worker protocol version unsupported");
    }
    let proof = citadel::runtime::worker_protocol::challenge_proof(&secret, &nonce);
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerHello {
            protocol_version,
            proof: proof.to_vec(),
        },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker hello write failed"))?;
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerReady { protocol_version },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker readiness write failed"))?;
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerHealth { protocol_version },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker health write failed"))?;
    match citadel::runtime::worker_protocol::read_control_frame(&mut stream)
        .map_err(|_| anyhow::anyhow!("runtime worker shutdown frame invalid"))?
    {
        citadel::runtime::worker_protocol::ControlFrame::ParentShutdown {
            protocol_version: shutdown_version,
        } if shutdown_version == protocol_version => {}
        _ => anyhow::bail!("runtime worker expected parent shutdown"),
    }
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerStopped { protocol_version },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker stopped acknowledgement write failed"))?;
    Ok(())
}

fn main() -> Result<()> {
    // MongoDB's TLS stack may pull aws-lc-rs alongside Citadel's ring-backed
    // QUIC/WebTransport stack. Rustls requires applications to select one
    // process-wide provider when both are linked.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Install the local panic hook before configuration, logging, or the
    // first-run wizard can panic. Resolved retention replaces its defaults
    // just before serving begins.
    error_reporting::install_early_panic_capture();

    #[cfg(feature = "runtime-python")]
    let _python_bundle = citadel::runtime::configure_bundled_python_runtime();

    let cli = Cli::parse();

    match cli.command() {
        Command::CookTmx { input, output } => {
            let map = citadel_tmx::load(&input)
                .with_context(|| format!("failed to import {}", input.display()))?;
            fs::write(&output, map.encode())
                .with_context(|| format!("failed to write {}", output.display()))?;
            println!(
                "cooked TMX: map={} vertices={} triangles={}",
                map.metadata.name,
                map.collision.vertices.len(),
                map.collision.triangles.len()
            );
            Ok(())
        }
        Command::RuntimeWorker {
            bootstrap_endpoint,
            bootstrap_fd,
        } => run_runtime_worker(&bootstrap_endpoint, bootstrap_fd),
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

            // Local incident capture is always active. Sentry telemetry stays
            // dormant unless CITADEL_SENTRY_DSN is configured, and the guard
            // remains alive through the whole serving lifetime to flush events
            // during shutdown.
            let _reporting = error_reporting::initialize(&config.errors);

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("failed to build Tokio runtime")?;
            runtime.block_on(async {
                // Select and connect the persistence backend before serving so
                // a configured-but-unreachable database fails fast at startup.
                let app = App::bootstrap(config).await.map_err(|error| {
                    error_reporting::report_app_error("bootstrap", &error);
                    anyhow::Error::new(error).context("failed to initialize persistence backend")
                })?;
                http::run(app).await.map_err(|error| {
                    error_reporting::report_app_error("http.server", &error);
                    anyhow::Error::new(error).context("HTTP server failed")
                })
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
