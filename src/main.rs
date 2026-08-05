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
fn configure_parent_death_signal() -> Result<()> {
    nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGKILL)
        .map_err(|_| anyhow::anyhow!("runtime worker parent-death signal setup failed"))
}

#[cfg(unix)]
fn disable_core_dumps() -> Result<()> {
    nix::sys::resource::setrlimit(nix::sys::resource::Resource::RLIMIT_CORE, 0, 0)
        .map_err(|_| anyhow::anyhow!("runtime worker core-dump limit setup failed"))
}

#[cfg(unix)]
fn apply_open_file_limit(max_open_files: u64) -> Result<()> {
    let (_, hard_limit) =
        nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)
            .map_err(|_| anyhow::anyhow!("runtime worker open-file limit read failed"))?;
    if hard_limit < max_open_files {
        anyhow::bail!("runtime worker open-file hard limit is too low");
    }
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_NOFILE,
        max_open_files,
        hard_limit,
    )
    .map_err(|_| anyhow::anyhow!("runtime worker open-file limit setup failed"))
}

#[cfg(unix)]
fn ensure_supervising_parent(supervisor_pid: u32) -> Result<()> {
    // PDEATHSIG only covers parent deaths that happen after it is armed. If
    // the supervisor died between fork/exec and the arming in
    // `run_runtime_worker`, no signal will ever arrive: this process was
    // already reparented to init or a subreaper. Detect that by comparing the
    // live parent pid against the pid the supervisor passed on the command
    // line, and exit fail-closed instead of running unsupervised.
    let live_parent = nix::unistd::getppid().as_raw();
    if u32::try_from(live_parent) != Ok(supervisor_pid) {
        anyhow::bail!("runtime worker lost its supervising parent before pdeathsig was armed");
    }
    Ok(())
}

/// Read one control frame, treating a receive-timeout as "no frame yet".
///
/// The socket read timeout doubles as the worker's health pacing. A timeout
/// that fires mid-frame desynchronizes the stream, so any partial read fails
/// closed on the next parse and the supervisor replaces the worker; in
/// practice parent frames are tiny single-write messages.
#[cfg(unix)]
fn try_read_control_frame(
    stream: &mut UnixStream,
) -> Result<Option<citadel::runtime::worker_protocol::ControlFrame>> {
    use std::io::Read;

    let mut prefix = [0u8; 4];
    match stream.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length > citadel::runtime::worker_protocol::MAX_CONTROL_FRAME_BYTES {
        anyhow::bail!("runtime worker received an oversized parent frame");
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .context("runtime worker parent frame truncated")?;
    citadel::runtime::worker_protocol::decode_frame(&payload)
        .map(Some)
        .map_err(|_| anyhow::anyhow!("runtime worker parent frame invalid"))
}

/// Script-hosting arguments of a data-plane worker, collected all-or-nothing
/// from the CLI (clap already enforces the grouping; this is the fail-closed
/// re-check at the trust boundary).
#[cfg(any(unix, windows))]
struct ScriptArgs {
    engine: String,
    entrypoint: std::path::PathBuf,
    script_deadline_ms: u64,
    tick_ms: u64,
    data_endpoint: String,
    data_epoch: u64,
}

#[cfg(any(unix, windows))]
#[allow(clippy::too_many_arguments)]
fn collect_script_args(
    engine: Option<String>,
    entrypoint: Option<std::path::PathBuf>,
    script_deadline_ms: Option<u64>,
    tick_ms: Option<u64>,
    data_endpoint: Option<String>,
    data_epoch: Option<u64>,
) -> Result<Option<ScriptArgs>> {
    match (
        engine,
        entrypoint,
        script_deadline_ms,
        tick_ms,
        data_endpoint,
        data_epoch,
    ) {
        (None, None, None, None, None, None) => Ok(None),
        (
            Some(engine),
            Some(entrypoint),
            Some(script_deadline_ms),
            Some(tick_ms),
            Some(data_endpoint),
            Some(data_epoch),
        ) => Ok(Some(ScriptArgs {
            engine,
            entrypoint,
            script_deadline_ms,
            tick_ms,
            data_endpoint,
            data_epoch,
        })),
        _ => anyhow::bail!("runtime worker script flags must be given all together or not at all"),
    }
}

/// The deployment's one engine plus its pinned revision identity.
#[cfg(any(unix, windows))]
struct ScriptHost {
    engine: Box<dyn citadel::runtime::engine_host::MatchEngine>,
    identity: String,
}

/// Load and validate the hosted script before readiness is reported.
///
/// A broken script must fail the bootstrap (the supervisor logs and applies
/// its restart policy) instead of being discovered match by match, so a probe
/// context is built and dropped here.
#[cfg(any(unix, windows))]
fn load_script_host(args: &ScriptArgs) -> Result<ScriptHost> {
    let source = fs::read_to_string(&args.entrypoint).with_context(|| {
        format!(
            "runtime worker failed to read the script {}",
            args.entrypoint.display()
        )
    })?;
    let identity = citadel::runtime::external_worker::script_identity(source.as_bytes());
    let mut engine: Box<dyn citadel::runtime::engine_host::MatchEngine> = match args.engine.as_str()
    {
        "lua" => Box::new(citadel::runtime::engine_host::LuaMatchEngine::new(
            source,
            args.script_deadline_ms,
        )),
        #[cfg(feature = "runtime-js")]
        "js" => Box::new(citadel::runtime::engine_host::JsMatchEngine::new(
            source,
            args.script_deadline_ms,
        )),
        #[cfg(not(feature = "runtime-js"))]
        "js" => anyhow::bail!(
            "runtime worker was built without the 'runtime-js' feature and cannot host js"
        ),
        #[cfg(feature = "runtime-python")]
        "python" => Box::new(citadel::runtime::engine_host::PythonMatchEngine::new(
            source,
            args.script_deadline_ms,
        )),
        #[cfg(not(feature = "runtime-python"))]
        "python" => anyhow::bail!(
            "runtime worker was built without the 'runtime-python' feature and cannot host python"
        ),
        other => anyhow::bail!("runtime worker does not know the engine '{other}'"),
    };
    engine
        .open_match(u64::MAX)
        .map_err(|fault| anyhow::anyhow!("runtime worker script failed to load: {fault:?}"))?;
    Ok(ScriptHost { engine, identity })
}

/// Worker side of the data-plane handshake: prove knowledge of this
/// generation's bootstrap secret on the freshly connected data stream.
#[cfg(any(unix, windows))]
fn worker_data_handshake<S: std::io::Read + std::io::Write>(
    stream: &mut S,
    secret: &[u8; 32],
) -> Result<()> {
    let frame = citadel::runtime::worker_protocol::read_control_frame(stream)
        .map_err(|_| anyhow::anyhow!("runtime worker data-plane hello invalid"))?;
    let (protocol_version, nonce) = match frame {
        citadel::runtime::worker_protocol::ControlFrame::ParentHello {
            protocol_version,
            nonce,
        } => (protocol_version, nonce),
        _ => anyhow::bail!("runtime worker expected a data-plane parent hello"),
    };
    if protocol_version != citadel::runtime::worker_protocol::PROTOCOL_VERSION {
        anyhow::bail!("runtime worker data-plane protocol version unsupported");
    }
    let proof = citadel::runtime::worker_protocol::challenge_proof(secret, &nonce);
    citadel::runtime::worker_protocol::write_control_frame(
        stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerHello {
            protocol_version,
            proof: proof.to_vec(),
        },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker data-plane hello write failed"))?;
    Ok(())
}

/// Worker-side frame source over a unix domain socket: reads may block
/// freely, the peer socket write path is independent.
#[cfg(unix)]
struct UnixFrameSource(UnixStream);

#[cfg(unix)]
impl citadel::runtime::worker_engine::FrameSource for UnixFrameSource {
    fn read_frame(&mut self) -> Option<citadel::runtime::worker_data_protocol::DataFrame> {
        citadel::runtime::worker_data_protocol::read_data_frame(&mut self.0).ok()
    }
}

/// Worker-side frame source over a synchronous named-pipe handle.
///
/// A blocked `ReadFile` on a synchronous pipe file object serializes with
/// `WriteFile` from the engine thread (same object through `try_clone`), so
/// the source peeks for a complete length prefix — with the same 5ms grain
/// the control plane uses — before committing to a read.
#[cfg(windows)]
struct PipeFrameSource(std::fs::File);

#[cfg(windows)]
impl citadel::runtime::worker_engine::FrameSource for PipeFrameSource {
    fn read_frame(&mut self) -> Option<citadel::runtime::worker_data_protocol::DataFrame> {
        loop {
            let available = citadel_win_proc::named_pipe_bytes_available(&self.0).ok()?;
            if available >= 4 {
                return citadel::runtime::worker_data_protocol::read_data_frame(&mut self.0).ok();
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

/// Run the hosted engine on its own thread over the connected data stream.
///
/// The heartbeat cadence reuses the supervisor's health cadence rather than
/// inventing a second timing constant.
#[cfg(any(unix, windows))]
fn start_engine_thread<S, W>(
    reader: S,
    writer: W,
    host: ScriptHost,
    args: &ScriptArgs,
    health_cadence_ms: u64,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    healthy: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()>
where
    S: citadel::runtime::worker_engine::FrameSource,
    W: std::io::Write + Send + 'static,
{
    let engine_loop = citadel::runtime::worker_engine::EngineLoop::new(
        host.engine,
        citadel::runtime::engine_host::MatchSchedulerPolicy::default(),
        args.data_epoch,
        host.identity,
    );
    let tick = std::time::Duration::from_millis(args.tick_ms.max(1));
    let heartbeat = std::time::Duration::from_millis(health_cadence_ms.max(1));
    std::thread::Builder::new()
        .name("citadel-worker-engine".to_owned())
        .spawn(move || {
            citadel::runtime::worker_engine::run_worker_data_plane(
                reader,
                writer,
                engine_loop,
                tick,
                heartbeat,
                &stop,
                &healthy,
            );
        })
        .expect("spawn worker engine thread")
}

/// Verify the supervising parent is still alive before bootstrap (Windows).
///
/// The Job Object's kill-on-close covers any supervisor death after the
/// worker was assigned to the job, and assignment is proven by the secret
/// read below (the supervisor writes the secret only after assigning). The
/// uncovered window is a supervisor that died before that; detect it here
/// and exit fail-closed instead of running unsupervised.
#[cfg(windows)]
fn ensure_supervising_parent(supervisor_pid: u32) -> Result<()> {
    let alive = citadel_win_proc::process_is_alive(supervisor_pid)
        .map_err(|_| anyhow::anyhow!("runtime worker parent liveness check failed"))?;
    if !alive {
        anyhow::bail!("runtime worker lost its supervising parent before bootstrap");
    }
    Ok(())
}

/// Surface the open-file limit as unenforceable on Windows.
///
/// `RLIMIT_NOFILE` has no Windows kernel equivalent and Job Objects cannot
/// cap handle counts, so the supervisor's `--max-open-files` policy cannot be
/// kernel-enforced here. It is reported loudly instead of silently ignored;
/// Windows containment comes from the Job Object (kill-on-close, group
/// termination) rather than per-resource rlimits.
#[cfg(windows)]
fn surface_unsupported_open_file_limit(max_open_files: u64) {
    eprintln!(
        "runtime worker: open-file limit {max_open_files} is not kernel-enforceable on windows; \
         relying on job-object containment"
    );
}

/// Read one control frame without blocking, or `None` when no frame waits.
///
/// Synchronous pipe handles have no read timeout, so the worker peeks for a
/// complete length prefix before committing to a blocking read. Parent
/// frames are tiny two-write messages: once the prefix is visible the
/// payload follows immediately, so the blocking read cannot stall the loop.
#[cfg(windows)]
fn try_read_control_frame(
    stream: &mut std::fs::File,
) -> Result<Option<citadel::runtime::worker_protocol::ControlFrame>> {
    if citadel_win_proc::named_pipe_bytes_available(stream)
        .context("runtime worker pipe peek failed")?
        < 4
    {
        return Ok(None);
    }
    citadel::runtime::worker_protocol::read_control_frame(stream)
        .map(Some)
        .map_err(|_| anyhow::anyhow!("runtime worker parent frame invalid"))
}

#[cfg(windows)]
fn run_runtime_worker(
    endpoint: &str,
    bootstrap_fd: i32,
    parent_pid: u32,
    max_open_files: u64,
    health_cadence_ms: u64,
    script: Option<ScriptArgs>,
) -> Result<()> {
    // Job containment is owned by the supervisor: the worker was assigned to
    // a kill-on-close Job Object before the bootstrap secret below was
    // written, so reading the secret proves containment is already armed —
    // the same "applied before the secret is read" ordering as the unix
    // resource limits.
    ensure_supervising_parent(parent_pid)?;
    surface_unsupported_open_file_limit(max_open_files);
    let handle_value = usize::try_from(bootstrap_fd)
        .map_err(|_| anyhow::anyhow!("runtime worker bootstrap handle invalid"))?;
    let secret = citadel_win_proc::read_secret_from_handle(handle_value)
        .context("runtime worker bootstrap secret unavailable")?;
    let mut stream = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(endpoint)
        .context("runtime worker bootstrap connect failed")?;
    // Mutual peer validation: the pipe server must be the supervisor that
    // spawned this worker, so a squatted endpoint never sees a proof.
    let server_pid = citadel_win_proc::named_pipe_server_process_id(&stream)
        .context("runtime worker pipe peer query failed")?;
    if server_pid != parent_pid {
        anyhow::bail!("runtime worker bootstrap endpoint is not owned by the supervisor");
    }
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
    // Load and validate the hosted script (if any) before reporting
    // readiness, so a broken script fails the bootstrap instead of being
    // discovered match by match.
    let script_host = script.as_ref().map(load_script_host).transpose()?;
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerReady {
            protocol_version,
            script_identity: script_host.as_ref().map(|host| host.identity.clone()),
        },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker readiness write failed"))?;
    // Connect the match data plane after readiness: its endpoint is a second
    // parent-private pipe, validated to belong to the supervisor and
    // authenticated with the same generation secret.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let healthy = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut engine_thread = None;
    if let (Some(host), Some(args)) = (script_host, script.as_ref()) {
        let mut data_stream = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&args.data_endpoint)
            .context("runtime worker data-plane connect failed")?;
        let server_pid = citadel_win_proc::named_pipe_server_process_id(&data_stream)
            .context("runtime worker data-plane peer query failed")?;
        if server_pid != parent_pid {
            anyhow::bail!("runtime worker data-plane endpoint is not owned by the supervisor");
        }
        worker_data_handshake(&mut data_stream, &secret)?;
        let reader = data_stream
            .try_clone()
            .context("runtime worker data-plane stream clone failed")?;
        engine_thread = Some(start_engine_thread(
            PipeFrameSource(reader),
            data_stream,
            host,
            args,
            health_cadence_ms,
            std::sync::Arc::clone(&stop),
            std::sync::Arc::clone(&healthy),
        ));
    }
    // Health is a continuous signal: emit one frame per cadence until the
    // parent orders shutdown. Pacing polls the pipe with the same 5ms grain
    // the unix supervisor uses in its accept loop — each cycle watches for a
    // parent frame for up to one cadence before reporting health again.
    let cadence = std::time::Duration::from_millis(health_cadence_ms.max(1));
    let poll_grain = std::time::Duration::from_millis(5);
    'health: loop {
        if !healthy.load(std::sync::atomic::Ordering::SeqCst) {
            // The engine can no longer serve (quarantine budget exhausted,
            // engine death, or a broken data plane): stop reassuring the
            // supervisor and exit so the process is replaced.
            anyhow::bail!("runtime worker engine is unhealthy; awaiting replacement");
        }
        citadel::runtime::worker_protocol::write_control_frame(
            &mut stream,
            &citadel::runtime::worker_protocol::ControlFrame::WorkerHealth { protocol_version },
        )
        .map_err(|_| anyhow::anyhow!("runtime worker health write failed"))?;
        let cycle_end = std::time::Instant::now() + cadence;
        loop {
            match try_read_control_frame(&mut stream)? {
                Some(citadel::runtime::worker_protocol::ControlFrame::ParentShutdown {
                    protocol_version: shutdown_version,
                }) if shutdown_version == protocol_version => break 'health,
                Some(_) => anyhow::bail!("runtime worker expected parent shutdown"),
                None => {}
            }
            if !healthy.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("runtime worker engine is unhealthy; awaiting replacement");
            }
            if std::time::Instant::now() >= cycle_end {
                break;
            }
            std::thread::sleep(poll_grain);
        }
    }
    // Orderly stop: let the engine loop flush its shutdown closes before the
    // stop acknowledgement ends this generation.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(engine_thread) = engine_thread {
        let _ = engine_thread.join();
    }
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerStopped { protocol_version },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker stopped acknowledgement write failed"))?;
    Ok(())
}

#[cfg(unix)]
fn run_runtime_worker(
    endpoint: &str,
    bootstrap_fd: i32,
    parent_pid: u32,
    max_open_files: u64,
    health_cadence_ms: u64,
    script: Option<ScriptArgs>,
) -> Result<()> {
    // Process-group isolation is owned by the supervisor, which places this
    // process in its own group pre-exec (see `SupervisedWorker::spawn`), so
    // group membership cannot depend on worker cooperation.
    //
    // Arm the parent-death signal as the very first act: from here on the
    // kernel delivers SIGKILL when the supervisor dies. Together with the
    // parent re-check below this closes the classic pdeathsig race — a parent
    // death before arming leaves the worker reparented, which the re-check
    // detects; a death after arming is covered by the signal itself. The only
    // residual window is exec-to-first-instruction, and the re-check
    // terminates the worker immediately after it.
    configure_parent_death_signal()?;
    ensure_supervising_parent(parent_pid)?;
    disable_core_dumps()?;
    // The open-file limit comes from the supervisor's resource policy and is
    // applied before the bootstrap secret is read, so an over-limit worker
    // never reaches the protocol.
    apply_open_file_limit(max_open_files)?;
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
    // Load and validate the hosted script (if any) before reporting
    // readiness, so a broken script fails the bootstrap instead of being
    // discovered match by match.
    let script_host = script.as_ref().map(load_script_host).transpose()?;
    citadel::runtime::worker_protocol::write_control_frame(
        &mut stream,
        &citadel::runtime::worker_protocol::ControlFrame::WorkerReady {
            protocol_version,
            script_identity: script_host.as_ref().map(|host| host.identity.clone()),
        },
    )
    .map_err(|_| anyhow::anyhow!("runtime worker readiness write failed"))?;
    // Connect the match data plane after readiness: its endpoint is a second
    // parent-private socket, authenticated with the same generation secret.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let healthy = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut engine_thread = None;
    if let (Some(host), Some(args)) = (script_host, script.as_ref()) {
        let mut data_stream = UnixStream::connect(&args.data_endpoint)
            .context("runtime worker data-plane connect failed")?;
        worker_data_handshake(&mut data_stream, &secret)?;
        let reader = data_stream
            .try_clone()
            .context("runtime worker data-plane stream clone failed")?;
        engine_thread = Some(start_engine_thread(
            UnixFrameSource(reader),
            data_stream,
            host,
            args,
            health_cadence_ms,
            std::sync::Arc::clone(&stop),
            std::sync::Arc::clone(&healthy),
        ));
    }
    // Health is a continuous signal: emit one frame per cadence until the
    // parent orders shutdown. The read timeout provides the pacing — each
    // cycle waits up to one cadence for a parent frame before reporting
    // health again.
    let cadence = std::time::Duration::from_millis(health_cadence_ms.max(1));
    stream
        .set_read_timeout(Some(cadence))
        .context("runtime worker health pacing setup failed")?;
    loop {
        if !healthy.load(std::sync::atomic::Ordering::SeqCst) {
            // The engine can no longer serve (quarantine budget exhausted,
            // engine death, or a broken data plane): stop reassuring the
            // supervisor and exit so the process is replaced.
            anyhow::bail!("runtime worker engine is unhealthy; awaiting replacement");
        }
        citadel::runtime::worker_protocol::write_control_frame(
            &mut stream,
            &citadel::runtime::worker_protocol::ControlFrame::WorkerHealth { protocol_version },
        )
        .map_err(|_| anyhow::anyhow!("runtime worker health write failed"))?;
        match try_read_control_frame(&mut stream)? {
            Some(citadel::runtime::worker_protocol::ControlFrame::ParentShutdown {
                protocol_version: shutdown_version,
            }) if shutdown_version == protocol_version => break,
            Some(_) => anyhow::bail!("runtime worker expected parent shutdown"),
            // No parent frame within one cadence: keep reporting health.
            None => {}
        }
    }
    // Orderly stop: let the engine loop flush its shutdown closes before the
    // stop acknowledgement ends this generation.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(engine_thread) = engine_thread {
        let _ = engine_thread.join();
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
        #[cfg(any(unix, windows))]
        Command::RuntimeWorker {
            bootstrap_endpoint,
            bootstrap_fd,
            parent_pid,
            max_open_files,
            health_cadence_ms,
            engine,
            entrypoint,
            script_deadline_ms,
            tick_ms,
            data_endpoint,
            data_epoch,
        } => run_runtime_worker(
            &bootstrap_endpoint,
            bootstrap_fd,
            parent_pid,
            max_open_files,
            health_cadence_ms,
            collect_script_args(
                engine,
                entrypoint,
                script_deadline_ms,
                tick_ms,
                data_endpoint,
                data_epoch,
            )?,
        ),
        #[cfg(not(any(unix, windows)))]
        Command::RuntimeWorker { .. } => {
            anyhow::bail!("the runtime-worker subcommand requires a unix or windows host")
        }
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

#[cfg(all(test, unix))]
mod tests {
    #[test]
    fn worker_disables_core_dumps_in_child() {
        if std::env::var_os("CITADEL_CORE_LIMIT_TEST_CHILD").is_some() {
            super::disable_core_dumps().expect("disable core dumps");
            let limits = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_CORE)
                .expect("read core limits");
            assert_eq!(limits, (0, 0));
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("tests::worker_disables_core_dumps_in_child")
            .env("CITADEL_CORE_LIMIT_TEST_CHILD", "1")
            .status()
            .expect("run disposable child");
        assert!(status.success());
    }

    #[test]
    fn worker_applies_open_file_limit() {
        let resource = nix::sys::resource::Resource::RLIMIT_NOFILE;
        let (soft, hard) = nix::sys::resource::getrlimit(resource).expect("read original limits");
        super::apply_open_file_limit(32).expect("apply limit");
        assert_eq!(
            nix::sys::resource::getrlimit(resource)
                .expect("read applied limit")
                .0,
            32
        );
        nix::sys::resource::setrlimit(resource, soft, hard).expect("restore limits");
    }

    #[test]
    fn worker_parent_check_accepts_the_live_parent() {
        let live_parent =
            u32::try_from(nix::unistd::getppid().as_raw()).expect("parent pid fits u32");
        super::ensure_supervising_parent(live_parent).expect("live parent is accepted");
    }

    #[test]
    fn worker_parent_check_rejects_a_reparented_worker() {
        // This process's own pid can never be its parent pid, so this models a
        // supervisor that died before pdeathsig was armed (the worker got
        // reparented and the recorded supervisor pid no longer matches).
        assert!(super::ensure_supervising_parent(std::process::id()).is_err());
    }

    #[test]
    fn worker_configures_parent_death_signal() {
        let original = nix::sys::prctl::get_pdeathsig().expect("read original signal");
        super::configure_parent_death_signal().expect("configure signal");
        assert_eq!(
            nix::sys::prctl::get_pdeathsig().expect("read configured signal"),
            Some(nix::sys::signal::Signal::SIGKILL)
        );
        nix::sys::prctl::set_pdeathsig(original).expect("restore original signal");
    }
}
