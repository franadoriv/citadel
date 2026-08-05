//! Parent-side runtime adapter for the supervised external worker.
//!
//! [`ExternalWorkerRuntime`] implements the existing [`Runtime`] trait so the
//! gateway needs no knowledge of process hosting: the per-match dispatch
//! surface is forwarded through data-plane frames to the worker process, and
//! the command batches the worker's matches produce are applied back through
//! a [`MatchCommandSink`] (the gateway) when they arrive.
//!
//! The adapter is generation-fenced end to end. Every worker boot gets a
//! fresh epoch from [`ExternalWorkerRuntime::allocate_epoch`]; frames in both
//! directions carry it, the receive pump validates with the fail-closed
//! [`DataPlaneRx`], and a pump that outlives its generation drops everything
//! it still drains — a crashed worker's buffered frames can never mutate a
//! newer generation's matches.
//!
//! Dispatch is asynchronous by design: the worker schedules matches fairly on
//! its own cadence, so an invocation returns no commands inline; results
//! arrive as fenced `MatchCommands` frames and are applied room-scoped. The
//! non-match surface (global messages, RPC, lifecycle hooks) is not routed to
//! the worker yet and fails visibly instead of silently: RPC calls return an
//! error outcome and other hooks produce no commands.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::config::RuntimeLanguage;
use crate::error::{AppError, AppResult, ErrorCategory};
use crate::runtime::lua::{
    LifecycleHook, OutboundCommand, ReloadOutcome, RoomSpec, RpcOutcome, RuntimeIntrospection,
};
use crate::runtime::worker_data_protocol::{
    DATA_PROTOCOL_VERSION, DataFrame, DataPlaneRx, EngineReport, FrameHeader, MatchCloseReason,
    RxCounters, decode_commands,
};

use super::Runtime;

/// PROVISIONAL round cadence used when `runtime.tick_hz` is disabled: the
/// worker still needs a scheduler cadence to drain match mailboxes and run
/// the layered watchdog. Replace once the latency distribution of real
/// multiplexed-match dispatch under the external adapter has been measured.
pub const DEFAULT_MATCH_ROUND_CADENCE_MS: u64 = 25;

/// Everything the worker process needs to host the deployment's one script.
///
/// Built by the transport layer from the resolved runtime selection and
/// carried to the worker on its command line by the supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerScriptSpec {
    /// The deployment's one engine (single-engine invariant).
    pub language: RuntimeLanguage,
    /// Entry point file the worker loads.
    pub entrypoint: PathBuf,
    /// Per-invocation handler budget in milliseconds.
    pub deadline_ms: u64,
    /// Worker-side scheduler round cadence in milliseconds. Matches tick on
    /// this cadence when their mailboxes are idle.
    pub tick_ms: u64,
}

/// Compute the revision-fencing identity of a script source.
#[must_use]
pub fn script_identity(source: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(source);
    let digest = hasher.finalize();
    let mut identity = String::with_capacity(7 + digest.len() * 2);
    identity.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(identity, "{byte:02x}");
    }
    identity
}

/// Where the worker's match results land: the gateway.
///
/// A seam instead of a direct gateway reference so the adapter (and its
/// tests) never depend on realtime internals, and so the gateway can be held
/// weakly — worker frames arriving during shutdown apply to nothing.
pub trait MatchCommandSink: Send + Sync + 'static {
    /// Apply one match's command batch (room-scoped broadcast semantics).
    fn apply_match_commands(&self, room_id: u64, commands: Vec<OutboundCommand>) -> usize;

    /// The worker closed the match; members must be informed and requeued.
    fn on_match_closed(&self, room_id: u64, reason: MatchCloseReason);
}

/// The frame was dropped: the connection is saturated or gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameSendError;

/// Sender half of one generation's data-plane connection.
///
/// Implemented by the IPC layer (a bounded channel into the pump thread) and
/// by test fakes. Sending is best-effort fail-closed: an error means the
/// frame was dropped and the connection should be considered gone.
pub trait FrameSender: Send + Sync {
    fn send(&self, frame: DataFrame) -> Result<(), FrameSendError>;
}

impl FrameSender for std::sync::mpsc::SyncSender<DataFrame> {
    fn send(&self, frame: DataFrame) -> Result<(), FrameSendError> {
        self.try_send(frame).map_err(|_| FrameSendError)
    }
}

/// One live worker generation's parent-side transmit state.
struct ActiveGeneration {
    epoch: u64,
    sender: Arc<dyn FrameSender>,
    /// Per-match outbound sequence counters; presence means "open".
    tx_seqs: HashMap<u64, u64>,
    /// Receive-side validator, shared with the pump thread.
    rx: Arc<Mutex<DataPlaneRx>>,
}

#[derive(Default)]
struct GenerationState {
    next_epoch: u64,
    active: Option<ActiveGeneration>,
}

/// Monotone adapter-level drop counters (observability + tests).
#[derive(Debug, Default)]
pub struct AdapterCounters {
    /// Frames dropped because no worker generation was installed or the
    /// connection refused them.
    pub dropped_sends: AtomicU64,
    /// Frames drained by a pump that outlived its generation.
    pub stale_generation_frames: AtomicU64,
}

/// The `runtime.adapter = "external-worker"` implementation of [`Runtime`].
pub struct ExternalWorkerRuntime {
    spec: WorkerScriptSpec,
    identity: String,
    budget: Duration,
    sink: Mutex<Option<Weak<dyn MatchCommandSink>>>,
    state: Mutex<GenerationState>,
    counters: AdapterCounters,
    /// Last worker heartbeat, for observability.
    last_heartbeat: Mutex<Option<EngineReport>>,
}

impl ExternalWorkerRuntime {
    /// Build the adapter, reading the entrypoint to pin its revision identity.
    pub fn load(spec: WorkerScriptSpec) -> AppResult<Self> {
        let source = std::fs::read(&spec.entrypoint).map_err(|error| {
            AppError::new(
                ErrorCategory::Runtime,
                format!(
                    "failed to read the external-worker script {}",
                    spec.entrypoint.display()
                ),
            )
            .with_detail(error.to_string())
        })?;
        let identity = script_identity(&source);
        let budget = Duration::from_millis(spec.deadline_ms.max(1));
        Ok(Self {
            spec,
            identity,
            budget,
            sink: Mutex::new(None),
            state: Mutex::new(GenerationState::default()),
            counters: AdapterCounters::default(),
            last_heartbeat: Mutex::new(None),
        })
    }

    /// The script spec the supervisor hands to the worker process.
    #[must_use]
    pub fn spec(&self) -> &WorkerScriptSpec {
        &self.spec
    }

    /// The pinned script revision identity (`sha256:...`).
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Attach the gateway-side command sink (weakly, to avoid an Arc cycle:
    /// the gateway already owns this runtime).
    pub fn attach_sink(&self, sink: Weak<dyn MatchCommandSink>) {
        *self.sink.lock().unwrap_or_else(|e| e.into_inner()) = Some(sink);
    }

    /// Adapter-level drop counters.
    #[must_use]
    pub fn counters(&self) -> &AdapterCounters {
        &self.counters
    }

    /// Receive-side drop counters of the active generation, if any.
    #[must_use]
    pub fn data_plane_counters(&self) -> Option<RxCounters> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active.as_ref().map(|generation| {
            generation
                .rx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .counters()
        })
    }

    /// The last heartbeat received from the active worker, if any.
    #[must_use]
    pub fn last_heartbeat(&self) -> Option<EngineReport> {
        self.last_heartbeat
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Allocate the next worker generation's epoch.
    ///
    /// Called by the supervisor before each worker boot so the epoch can
    /// travel on the worker's command line; monotone across restarts, so a
    /// replayed frame from any previous generation always fails the fence.
    pub fn allocate_epoch(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.next_epoch += 1;
        state.next_epoch
    }

    /// Install a freshly authenticated worker generation's send half.
    ///
    /// The previous generation (if any) is discarded: its matches are gone
    /// with its process, and its pump — fenced by epoch — can no longer apply
    /// anything.
    pub fn install_generation(&self, epoch: u64, sender: Arc<dyn FrameSender>) {
        let rx = Arc::new(Mutex::new(DataPlaneRx::new(epoch)));
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = Some(ActiveGeneration {
            epoch,
            sender,
            tx_seqs: HashMap::new(),
            rx: Arc::clone(&rx),
        });
    }

    /// Drop the active generation unconditionally (the supervisor observed
    /// the worker's death before booting any replacement).
    pub fn clear_active_generation(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = None;
    }

    /// Drop the active generation if it still is `epoch` (a late death
    /// notification must never clear a newer generation).
    pub fn clear_generation(&self, epoch: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .active
            .as_ref()
            .is_some_and(|generation| generation.epoch == epoch)
        {
            state.active = None;
        }
    }

    /// Whether `epoch` is the active generation.
    fn is_active_epoch(&self, epoch: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .as_ref()
            .is_some_and(|generation| generation.epoch == epoch)
    }

    fn sink(&self) -> Option<Arc<dyn MatchCommandSink>> {
        self.sink
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Spawn the receive pump for one generation.
    ///
    /// `receiver` is fed by the IPC layer with frames read from the worker;
    /// the pump validates each one fail-closed against the generation's
    /// [`DataPlaneRx`], refuses to apply anything once its generation is no
    /// longer active, and exits when the channel closes (connection gone).
    pub fn spawn_rx_pump(
        self: &Arc<Self>,
        epoch: u64,
        receiver: std::sync::mpsc::Receiver<DataFrame>,
    ) -> std::thread::JoinHandle<()> {
        let runtime = Arc::clone(self);
        let rx = {
            let state = runtime.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .active
                .as_ref()
                .filter(|generation| generation.epoch == epoch)
                .map(|generation| Arc::clone(&generation.rx))
        };
        std::thread::Builder::new()
            .name(format!("citadel-worker-rx-{epoch}"))
            .spawn(move || {
                let Some(rx) = rx else {
                    return;
                };
                while let Ok(frame) = receiver.recv() {
                    runtime.handle_worker_frame(epoch, &rx, frame);
                }
            })
            .expect("spawn worker rx pump thread")
    }

    /// Validate and apply one worker→gateway frame (pump body; separated for
    /// deterministic tests).
    pub fn handle_worker_frame(
        &self,
        epoch: u64,
        rx: &Arc<Mutex<DataPlaneRx>>,
        frame: DataFrame,
    ) {
        // Generation fence first: a pump outliving its generation applies
        // nothing, however valid its frames look for their own epoch.
        if !self.is_active_epoch(epoch) {
            self.counters
                .stale_generation_frames
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        if rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .accept(&frame)
            .is_err()
        {
            return;
        }
        match frame {
            DataFrame::MatchCommands {
                header, commands, ..
            } => {
                let Ok(commands) = decode_commands(&commands) else {
                    tracing::warn!(
                        match_id = header.match_id,
                        "dropped an undecodable worker command batch"
                    );
                    return;
                };
                if let Some(sink) = self.sink() {
                    sink.apply_match_commands(header.match_id, commands);
                }
            }
            DataFrame::MatchClosed { header, reason, .. } => {
                {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(generation) = state
                        .active
                        .as_mut()
                        .filter(|generation| generation.epoch == epoch)
                    {
                        generation.tx_seqs.remove(&header.match_id);
                        generation
                            .rx
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .close_match(header.match_id);
                    }
                }
                tracing::warn!(
                    room_id = header.match_id,
                    ?reason,
                    "external worker closed a match"
                );
                if let Some(sink) = self.sink() {
                    sink.on_match_closed(header.match_id, reason);
                }
            }
            DataFrame::EngineReport { report, .. } => {
                if let EngineReport::EngineDead { engine } = &report {
                    tracing::error!(engine, "external worker reported its engine dead");
                }
                *self
                    .last_heartbeat
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(report);
            }
            DataFrame::MatchOpen { .. } | DataFrame::MatchEvent { .. } => {
                tracing::warn!("dropped a gateway-scoped frame received from the worker");
            }
        }
    }

    /// Send `frame` on the active generation, opening `room_id` first when it
    /// has not been opened on this generation yet.
    fn send_match_event(
        &self,
        room_id: u64,
        sender_id: u64,
        user_id: Option<&str>,
        kind: u16,
        body: &[u8],
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(generation) = state.active.as_mut() else {
            self.counters.dropped_sends.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let mut frames = Vec::with_capacity(2);
        if !generation.tx_seqs.contains_key(&room_id) {
            generation
                .rx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .open_match(room_id);
            let header = next_header(generation, room_id);
            frames.push(DataFrame::MatchOpen {
                protocol_version: DATA_PROTOCOL_VERSION,
                header,
                script_identity: Some(self.identity.clone()),
            });
        }
        let header = next_header(generation, room_id);
        frames.push(DataFrame::MatchEvent {
            protocol_version: DATA_PROTOCOL_VERSION,
            header,
            sender: sender_id,
            user_id: user_id.map(str::to_owned),
            kind,
            body: body.to_vec(),
        });
        for frame in frames {
            if generation.sender.send(frame).is_err() {
                self.counters.dropped_sends.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Open `room_id` on the active generation without an event (the
    /// room-join admission path), so join-driven matches begin ticking before
    /// their first routed message.
    fn ensure_match_open(&self, room_id: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(generation) = state.active.as_mut() else {
            return;
        };
        if generation.tx_seqs.contains_key(&room_id) {
            return;
        }
        generation
            .rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .open_match(room_id);
        let header = next_header(generation, room_id);
        let frame = DataFrame::MatchOpen {
            protocol_version: DATA_PROTOCOL_VERSION,
            header,
            script_identity: Some(self.identity.clone()),
        };
        if generation.sender.send(frame).is_err() {
            self.counters.dropped_sends.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Tell the worker a match ended gateway-side so it frees the context.
    pub fn notify_match_closed(&self, room_id: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(generation) = state.active.as_mut() else {
            return;
        };
        if !generation.tx_seqs.contains_key(&room_id) {
            return;
        }
        let header = next_header(generation, room_id);
        generation.tx_seqs.remove(&room_id);
        generation
            .rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .close_match(room_id);
        let frame = DataFrame::MatchClosed {
            protocol_version: DATA_PROTOCOL_VERSION,
            header,
            reason: MatchCloseReason::Shutdown,
        };
        if generation.sender.send(frame).is_err() {
            self.counters.dropped_sends.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn next_header(generation: &mut ActiveGeneration, match_id: u64) -> FrameHeader {
    let seq = generation.tx_seqs.entry(match_id).or_insert(0);
    *seq += 1;
    FrameHeader {
        match_id,
        epoch: generation.epoch,
        seq: *seq,
    }
}

impl Runtime for ExternalWorkerRuntime {
    fn dispatch(
        &self,
        _sender: u64,
        _user_id: Option<&str>,
        kind: u16,
        _body: &[u8],
    ) -> Vec<OutboundCommand> {
        // Roomless dispatch has no match to scope to; the external worker
        // executes match-scoped logic only. Visible in traces, not silent.
        tracing::debug!(
            kind,
            "external worker runtime dropped a roomless message (no match scope)"
        );
        Vec::new()
    }

    fn dispatch_in_room(
        &self,
        sender: u64,
        user_id: Option<&str>,
        room_id: u64,
        kind: u16,
        body: &[u8],
    ) -> Vec<OutboundCommand> {
        self.send_match_event(room_id, sender, user_id, kind, body);
        // Results arrive asynchronously as fenced MatchCommands frames and
        // are applied through the MatchCommandSink.
        Vec::new()
    }

    fn dispatch_lifecycle(
        &self,
        _hook: LifecycleHook,
        _sender: u64,
        _user_id: Option<&str>,
    ) -> Vec<OutboundCommand> {
        Vec::new()
    }

    fn tick(&self, _dt: Duration, _budget: Duration) -> Vec<OutboundCommand> {
        // The worker ticks its matches on its own scheduler cadence.
        Vec::new()
    }

    fn on_match_closed(&self, room_id: u64) {
        // The gateway closed the match (or is echoing a worker-initiated
        // close, in which case the transmit state is already gone and this
        // is a no-op): tell the worker to release the execution context.
        self.notify_match_closed(room_id);
    }

    fn call_rpc(
        &self,
        _sender: u64,
        _user_id: Option<&str>,
        method: &str,
        _body: &[u8],
    ) -> RpcOutcome {
        tracing::debug!(
            method,
            "external worker runtime does not route RPC yet; failing the call"
        );
        RpcOutcome::Err("rpc is not available under the external-worker adapter yet".to_string())
    }

    fn call_room_create(
        &self,
        _sender: u64,
        _user_id: Option<&str>,
        _params: &[u8],
    ) -> Option<RoomSpec> {
        // No script-side room policy yet: the gateway's built-in room
        // creation applies.
        None
    }

    fn call_room_join(&self, _sender: u64, _user_id: Option<&str>, room_id: u64) -> bool {
        // Admit, and open the match context on join so join-driven matches
        // begin ticking before their first routed message.
        self.ensure_match_open(room_id);
        true
    }

    fn has_tick_handler(&self) -> bool {
        false
    }

    fn budget(&self) -> Duration {
        self.budget
    }

    fn introspect(&self) -> RuntimeIntrospection {
        RuntimeIntrospection {
            source: format!(
                "{} (external worker, {})",
                self.spec.entrypoint.display(),
                self.spec.language.as_str()
            ),
            reloadable: false,
            deadline_ms: self.spec.deadline_ms,
            rpcs: Vec::new(),
            message_kinds: Vec::new(),
            hooks: Vec::new(),
        }
    }

    fn is_reloadable(&self) -> bool {
        false
    }

    fn reload(&self) -> ReloadOutcome {
        ReloadOutcome::NotReloadable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::worker_data_protocol::encode_commands;

    struct RecordingSink {
        commands: Mutex<Vec<(u64, Vec<OutboundCommand>)>>,
        closed: Mutex<Vec<(u64, MatchCloseReason)>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                commands: Mutex::new(Vec::new()),
                closed: Mutex::new(Vec::new()),
            })
        }
    }

    impl MatchCommandSink for RecordingSink {
        fn apply_match_commands(&self, room_id: u64, commands: Vec<OutboundCommand>) -> usize {
            let delivered = commands.len();
            self.commands
                .lock()
                .expect("commands lock")
                .push((room_id, commands));
            delivered
        }

        fn on_match_closed(&self, room_id: u64, reason: MatchCloseReason) {
            self.closed
                .lock()
                .expect("closed lock")
                .push((room_id, reason));
        }
    }

    struct CapturingSender(Mutex<Vec<DataFrame>>);

    impl CapturingSender {
        fn new() -> Arc<Self> {
            Arc::new(Self(Mutex::new(Vec::new())))
        }

        fn frames(&self) -> Vec<DataFrame> {
            self.0.lock().expect("frames lock").clone()
        }
    }

    impl FrameSender for CapturingSender {
        fn send(&self, frame: DataFrame) -> Result<(), FrameSendError> {
            self.0.lock().expect("frames lock").push(frame);
            Ok(())
        }
    }

    fn runtime_with_script() -> (Arc<ExternalWorkerRuntime>, tempdir::TempDirGuard) {
        let dir = tempdir::create("external-worker-runtime");
        let entrypoint = dir.path.join("main.lua");
        std::fs::write(&entrypoint, "-- test script").expect("write script");
        let runtime = Arc::new(
            ExternalWorkerRuntime::load(WorkerScriptSpec {
                language: RuntimeLanguage::Lua,
                entrypoint,
                deadline_ms: 50,
                tick_ms: 16,
            })
            .expect("load"),
        );
        (runtime, dir)
    }

    /// Minimal unique temp dirs with drop cleanup (no external crate).
    mod tempdir {
        pub struct TempDirGuard {
            pub path: std::path::PathBuf,
        }

        impl Drop for TempDirGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }

        pub fn create(label: &str) -> TempDirGuard {
            let path = std::env::temp_dir().join(format!("citadel-{label}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("temp dir");
            TempDirGuard { path }
        }
    }

    fn active_rx(runtime: &ExternalWorkerRuntime) -> Arc<Mutex<DataPlaneRx>> {
        let state = runtime.state.lock().expect("state lock");
        Arc::clone(&state.active.as_ref().expect("active generation").rx)
    }

    #[test]
    fn dispatch_in_room_emits_fenced_open_and_event_frames() {
        let (runtime, _dir) = runtime_with_script();
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender.clone());

        assert!(
            runtime
                .dispatch_in_room(7, Some("user-a"), 4, 9, b"hello")
                .is_empty(),
            "results arrive asynchronously"
        );
        let frames = sender.frames();
        assert_eq!(frames.len(), 2, "open + event: {frames:?}");
        assert_eq!(
            frames[0],
            DataFrame::MatchOpen {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch: 1,
                    seq: 1
                },
                script_identity: Some(runtime.identity().to_string()),
            }
        );
        assert_eq!(
            frames[1],
            DataFrame::MatchEvent {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch: 1,
                    seq: 2
                },
                sender: 7,
                user_id: Some("user-a".to_string()),
                kind: 9,
                body: b"hello".to_vec(),
            }
        );
        // The second dispatch reuses the open match and advances the sequence.
        runtime.dispatch_in_room(7, Some("user-a"), 4, 9, b"again");
        let frames = sender.frames();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[2].header().seq, 3);
    }

    #[test]
    fn room_join_admission_opens_the_match() {
        let (runtime, _dir) = runtime_with_script();
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender.clone());
        assert!(runtime.call_room_join(7, None, 4), "join is admitted");
        let frames = sender.frames();
        assert_eq!(frames.len(), 1);
        assert!(matches!(frames[0], DataFrame::MatchOpen { .. }));
        // The subsequent event does not re-open.
        runtime.dispatch_in_room(7, None, 4, 9, b"x");
        assert_eq!(sender.frames().len(), 2);
    }

    #[test]
    fn no_active_generation_drops_events_fail_closed() {
        let (runtime, _dir) = runtime_with_script();
        assert!(runtime.dispatch_in_room(7, None, 4, 9, b"x").is_empty());
        assert_eq!(runtime.counters().dropped_sends.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn worker_command_frames_apply_to_the_sink() {
        let (runtime, _dir) = runtime_with_script();
        let sink = RecordingSink::new();
        runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender);
        runtime.dispatch_in_room(7, None, 4, 9, b"x");

        let commands = vec![OutboundCommand::Broadcast {
            kind: 40,
            body: b"pong".to_vec(),
            unreliable: false,
        }];
        let rx = active_rx(&runtime);
        runtime.handle_worker_frame(
            epoch,
            &rx,
            DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch,
                    seq: 1,
                },
                commands: encode_commands(&commands).expect("encode"),
            },
        );
        assert_eq!(
            sink.commands.lock().expect("commands").as_slice(),
            &[(4, commands)]
        );
    }

    #[test]
    fn stale_epoch_and_unknown_match_frames_apply_nothing() {
        let (runtime, _dir) = runtime_with_script();
        let sink = RecordingSink::new();
        runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender);
        runtime.dispatch_in_room(7, None, 4, 9, b"x");
        let rx = active_rx(&runtime);
        let batch = encode_commands(&[OutboundCommand::Broadcast {
            kind: 40,
            body: Vec::new(),
            unreliable: false,
        }])
        .expect("encode");
        // Wrong epoch inside the frame: rejected by the DataPlaneRx.
        runtime.handle_worker_frame(
            epoch,
            &rx,
            DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch: epoch + 7,
                    seq: 1,
                },
                commands: batch.clone(),
            },
        );
        // Unknown match (never opened on this generation).
        runtime.handle_worker_frame(
            epoch,
            &rx,
            DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 9,
                    epoch,
                    seq: 1,
                },
                commands: batch,
            },
        );
        assert!(sink.commands.lock().expect("commands").is_empty());
        let counters = runtime.data_plane_counters().expect("active generation");
        assert_eq!(counters.stale_epoch, 1);
        assert_eq!(counters.unknown_match, 1);
    }

    #[test]
    fn worker_close_frames_reach_the_sink_and_allow_reopen() {
        let (runtime, _dir) = runtime_with_script();
        let sink = RecordingSink::new();
        runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender.clone());
        runtime.dispatch_in_room(7, None, 4, 9, b"x");
        let rx = active_rx(&runtime);
        runtime.handle_worker_frame(
            epoch,
            &rx,
            DataFrame::MatchClosed {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch,
                    seq: 1,
                },
                reason: MatchCloseReason::ServerError,
            },
        );
        assert_eq!(
            sink.closed.lock().expect("closed").as_slice(),
            &[(4, MatchCloseReason::ServerError)]
        );
        // A later dispatch for the same room opens a fresh match context.
        runtime.dispatch_in_room(7, None, 4, 9, b"y");
        let frames = sender.frames();
        let reopens = frames
            .iter()
            .filter(|frame| matches!(frame, DataFrame::MatchOpen { .. }))
            .count();
        assert_eq!(reopens, 2, "close + dispatch must re-open: {frames:?}");
    }

    #[test]
    fn a_pump_outliving_its_generation_applies_nothing() {
        let (runtime, _dir) = runtime_with_script();
        let sink = RecordingSink::new();
        runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
        let old_sender = CapturingSender::new();
        let old_epoch = runtime.allocate_epoch();
        runtime.install_generation(old_epoch, old_sender);
        runtime.dispatch_in_room(7, None, 4, 9, b"x");
        let old_rx = active_rx(&runtime);

        // The worker dies; a new generation is installed.
        let new_sender = CapturingSender::new();
        let new_epoch = runtime.allocate_epoch();
        runtime.install_generation(new_epoch, new_sender.clone());

        // A frame the old pump still drains — valid for its own epoch — must
        // not reach the sink.
        runtime.handle_worker_frame(
            old_epoch,
            &old_rx,
            DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch: old_epoch,
                    seq: 1,
                },
                commands: encode_commands(&[OutboundCommand::Broadcast {
                    kind: 40,
                    body: Vec::new(),
                    unreliable: false,
                }])
                .expect("encode"),
            },
        );
        assert!(sink.commands.lock().expect("commands").is_empty());
        assert_eq!(
            runtime
                .counters()
                .stale_generation_frames
                .load(Ordering::Relaxed),
            1
        );
        // The new generation starts with an empty match table: the next
        // dispatch opens the room fresh under the new epoch.
        runtime.dispatch_in_room(7, None, 4, 9, b"y");
        let frames = new_sender.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].header().epoch, new_epoch);
        assert!(matches!(frames[0], DataFrame::MatchOpen { .. }));
    }

    #[test]
    fn rx_pump_thread_applies_frames_from_the_channel() {
        let (runtime, _dir) = runtime_with_script();
        let sink = RecordingSink::new();
        runtime.attach_sink(Arc::downgrade(&sink) as Weak<dyn MatchCommandSink>);
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender);
        runtime.dispatch_in_room(7, None, 4, 9, b"x");

        let (frame_tx, frame_rx) = std::sync::mpsc::channel();
        let pump = runtime.spawn_rx_pump(epoch, frame_rx);
        frame_tx
            .send(DataFrame::MatchCommands {
                protocol_version: DATA_PROTOCOL_VERSION,
                header: FrameHeader {
                    match_id: 4,
                    epoch,
                    seq: 1,
                },
                commands: encode_commands(&[OutboundCommand::Send {
                    session: 7,
                    kind: 99,
                    body: b"pong".to_vec(),
                    unreliable: false,
                }])
                .expect("encode"),
            })
            .expect("send frame");
        drop(frame_tx);
        pump.join().expect("pump exits when the channel closes");
        assert_eq!(sink.commands.lock().expect("commands").len(), 1);
    }

    #[test]
    fn notify_match_closed_sends_a_fenced_close() {
        let (runtime, _dir) = runtime_with_script();
        let sender = CapturingSender::new();
        let epoch = runtime.allocate_epoch();
        runtime.install_generation(epoch, sender.clone());
        runtime.dispatch_in_room(7, None, 4, 9, b"x");
        runtime.notify_match_closed(4);
        let frames = sender.frames();
        assert!(matches!(
            frames.last(),
            Some(DataFrame::MatchClosed {
                header: FrameHeader { match_id: 4, .. },
                reason: MatchCloseReason::Shutdown,
                ..
            })
        ));
        // Unknown rooms are a no-op.
        runtime.notify_match_closed(99);
        assert_eq!(sender.frames().len(), 3);
    }

    #[test]
    fn runtime_surface_is_honest_about_unrouted_paths() {
        let (runtime, _dir) = runtime_with_script();
        assert!(matches!(
            runtime.call_rpc(7, None, "friends.list", b"{}"),
            RpcOutcome::Err(_)
        ));
        assert!(runtime.dispatch(7, None, 9, b"x").is_empty());
        assert!(runtime.call_room_create(7, None, b"arena").is_none());
        assert!(!runtime.has_tick_handler());
        assert!(!runtime.is_reloadable());
        assert_eq!(runtime.budget(), Duration::from_millis(50));
        let introspection = runtime.introspect();
        assert!(introspection.source.contains("external worker"));
        assert_eq!(introspection.deadline_ms, 50);
    }
}
