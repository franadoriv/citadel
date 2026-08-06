//! IO-free worker-side data-plane loop around the [`EngineHost`].
//!
//! The worker process owns exactly one [`EngineLoop`]: frames in, frames out.
//! Every inbound frame is validated fail-closed by the same [`DataPlaneRx`]
//! the gateway uses on its side (epoch fence, per-match sequence, open-match
//! table), every outbound frame carries the worker generation's epoch and a
//! per-match monotone sequence. Keeping the loop free of IO and clocks makes
//! the whole worker-side protocol behavior testable without processes or
//! sockets; the binary's data-plane thread supplies the transport and the
//! round cadence.

use std::collections::HashMap;
use std::time::Duration;

use super::engine_host::MatchSchedulerPolicy;
use super::engine_host::{EngineHost, HostOpenError, HostOutput, MatchEngine, MatchInvocation};
use super::worker_data_protocol::{
    DATA_PROTOCOL_VERSION, DataFrame, DataPlaneRx, EngineReport, FrameHeader, MatchCloseReason,
    RxCounters, WORKER_SCOPE_MATCH_ID, encode_commands,
};

/// The worker-side data-plane state machine.
pub struct EngineLoop {
    host: EngineHost,
    rx: DataPlaneRx,
    epoch: u64,
    /// Identity of the script revision this worker loaded; a `MatchOpen`
    /// fenced to a different revision is refused (the members requeue instead
    /// of silently running foreign code paths).
    script_identity: String,
    /// Per-match outbound sequence counters (worker → gateway direction).
    tx_seqs: HashMap<u64, u64>,
    /// Whether the one-shot engine-death report was already emitted.
    engine_death_reported: bool,
}

impl EngineLoop {
    #[must_use]
    pub fn new(
        engine: Box<dyn MatchEngine>,
        policy: MatchSchedulerPolicy,
        epoch: u64,
        script_identity: impl Into<String>,
    ) -> Self {
        Self {
            host: EngineHost::new(engine, policy),
            rx: DataPlaneRx::new(epoch),
            epoch,
            script_identity: script_identity.into(),
            tx_seqs: HashMap::new(),
            engine_death_reported: false,
        }
    }

    /// The worker generation's epoch, stamped on every outbound frame.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether the hosted engine may keep serving (see [`EngineHost::is_healthy`]).
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.host.is_healthy()
    }

    /// Point-in-time copy of the receive-side drop counters.
    #[must_use]
    pub fn rx_counters(&self) -> RxCounters {
        self.rx.counters()
    }

    /// Point-in-time copy of the host's drop/overrun/quarantine counters.
    #[must_use]
    pub fn host_counters(&self) -> super::engine_host::HostCounters {
        self.host.counters()
    }

    /// Handle one received frame, returning the frames it produced.
    ///
    /// Reception is fail-closed: a frame that is not a well-formed,
    /// current-epoch, in-sequence gateway→worker frame for this generation is
    /// dropped and counted, mutating nothing.
    pub fn handle_frame(&mut self, frame: DataFrame) -> Vec<DataFrame> {
        // Direction check before sequence accounting: a worker→gateway
        // variant arriving here must not consume the match's sequence.
        match &frame {
            DataFrame::MatchOpen { .. } | DataFrame::MatchEvent { .. } => {}
            DataFrame::MatchClosed { .. } => {}
            DataFrame::MatchCommands { .. } | DataFrame::EngineReport { .. } => {
                tracing::warn!("worker dropped a worker-scoped frame received from the gateway");
                return Vec::new();
            }
        }
        let header = frame.header();
        if matches!(frame, DataFrame::MatchOpen { .. }) {
            // The open frame is what registers the match, so register it
            // before sequence validation; a replayed open still fails on its
            // consumed sequence number.
            self.rx.open_match(header.match_id);
        }
        // Validation runs on a header-carrying stub so event bodies are never
        // cloned just to be checked.
        let stub = DataFrame::MatchEvent {
            protocol_version: frame.protocol_version(),
            header,
            sender: 0,
            user_id: None,
            kind: 0,
            body: Vec::new(),
        };
        if self.rx.accept(&stub).is_err() {
            return Vec::new();
        }
        match frame {
            DataFrame::MatchOpen {
                script_identity, ..
            } => self.open_match(header.match_id, script_identity.as_deref()),
            DataFrame::MatchEvent {
                sender,
                user_id,
                kind,
                body,
                ..
            } => {
                self.host.enqueue_event(
                    header.match_id,
                    MatchInvocation {
                        sender,
                        user_id,
                        kind,
                        body,
                    },
                );
                self.drain_frames()
            }
            DataFrame::MatchClosed { .. } => {
                // Gateway-initiated close (the room ended): evict silently.
                // Echoing a close back would only pollute the gateway's
                // unknown-match counters for a match it already forgot.
                self.host.evict_match(header.match_id);
                self.rx.close_match(header.match_id);
                self.tx_seqs.remove(&header.match_id);
                Vec::new()
            }
            DataFrame::MatchCommands { .. } | DataFrame::EngineReport { .. } => Vec::new(),
        }
    }

    /// Run one fair scheduling round (see [`EngineHost::run_round`]) and
    /// return the frames it produced.
    pub fn run_round(&mut self, dt: Duration) -> Vec<DataFrame> {
        self.host.run_round(dt);
        self.drain_frames()
    }

    /// Produce one scheduler-liveness heartbeat frame.
    pub fn heartbeat(&mut self) -> DataFrame {
        let report = self.host.heartbeat();
        self.report_frame(report)
    }

    /// Orderly shutdown: close every match and return the closure frames.
    pub fn shutdown(&mut self) -> Vec<DataFrame> {
        self.host.shutdown();
        self.drain_frames()
    }

    fn open_match(&mut self, match_id: u64, parent_identity: Option<&str>) -> Vec<DataFrame> {
        if let Some(parent_identity) = parent_identity
            && parent_identity != self.script_identity
        {
            // Revision fence: the gateway opened this match against a script
            // revision this worker did not load. Refuse instead of running
            // mismatched code; the members requeue.
            tracing::warn!(
                match_id,
                "worker refused a match fenced to a different script revision"
            );
            self.rx.close_match(match_id);
            return vec![self.close_frame(match_id, MatchCloseReason::ServerError)];
        }
        match self.host.open_match(match_id) {
            Ok(()) => self.drain_frames(),
            Err(HostOpenError::AlreadyOpen) => Vec::new(),
            Err(HostOpenError::EngineDead) => {
                // The host emitted closes for the previously open matches when
                // the engine died; this match never opened, so tell the
                // gateway about it explicitly.
                self.rx.close_match(match_id);
                let mut frames = self.drain_frames();
                frames.push(self.close_frame(match_id, MatchCloseReason::EngineDead));
                frames
            }
            Err(HostOpenError::ContextFailed) => {
                self.rx.close_match(match_id);
                self.drain_frames()
            }
        }
    }

    /// Drain host outputs into fenced frames, appending the one-shot
    /// engine-death report when the engine just died.
    fn drain_frames(&mut self) -> Vec<DataFrame> {
        let outputs = self.host.drain_outputs();
        let mut frames = Vec::with_capacity(outputs.len());
        for output in outputs {
            match output {
                HostOutput::Commands { match_id, commands } => {
                    let Ok(commands) = encode_commands(&commands) else {
                        tracing::warn!(match_id, "worker dropped an unencodable command batch");
                        continue;
                    };
                    if commands.len() > super::worker_data_protocol::MAX_DATA_FRAME_BYTES {
                        tracing::warn!(
                            match_id,
                            "worker dropped a command batch exceeding the data frame cap"
                        );
                        continue;
                    }
                    let header = self.next_header(match_id);
                    frames.push(DataFrame::MatchCommands {
                        protocol_version: DATA_PROTOCOL_VERSION,
                        header,
                        commands,
                    });
                }
                HostOutput::Closed { match_id, reason } => {
                    self.rx.close_match(match_id);
                    frames.push(self.close_frame(match_id, reason));
                }
            }
        }
        if self.host.engine_dead() && !self.engine_death_reported {
            self.engine_death_reported = true;
            let engine = self.host.engine().to_string();
            frames.push(self.report_frame(EngineReport::EngineDead { engine }));
        }
        frames
    }

    fn close_frame(&mut self, match_id: u64, reason: MatchCloseReason) -> DataFrame {
        let header = self.next_header(match_id);
        self.tx_seqs.remove(&match_id);
        DataFrame::MatchClosed {
            protocol_version: DATA_PROTOCOL_VERSION,
            header,
            reason,
        }
    }

    fn report_frame(&mut self, report: EngineReport) -> DataFrame {
        let header = self.next_header(WORKER_SCOPE_MATCH_ID);
        DataFrame::EngineReport {
            protocol_version: DATA_PROTOCOL_VERSION,
            header,
            report,
        }
    }

    fn next_header(&mut self, match_id: u64) -> FrameHeader {
        let seq = self.tx_seqs.entry(match_id).or_insert(0);
        *seq += 1;
        FrameHeader {
            match_id,
            epoch: self.epoch,
            seq: *seq,
        }
    }
}

/// Source of inbound data-plane frames for [`run_worker_data_plane`].
///
/// A seam instead of `std::io::Read` because the platforms differ in how a
/// read may block: on unix a socket read can block freely, but a Windows
/// synchronous pipe handle serializes a blocked `ReadFile` with `WriteFile`
/// on the same file object, so the Windows source must peek before
/// committing to a read (the same discipline the control plane uses).
pub trait FrameSource: Send + 'static {
    /// Block until the next frame arrives; `None` ends the stream.
    fn read_frame(&mut self) -> Option<DataFrame>;
}

/// Drive one worker generation's data plane over a connected byte stream.
///
/// Owns the whole worker-side IO shape so the binary stays thin: a reader
/// thread turns the stream into frames (whole-frame reads never
/// desynchronize mid-frame the way timeout reads could), the loop body runs
/// scheduler rounds at `tick` cadence with real elapsed `dt`, emits
/// heartbeats at `heartbeat` cadence, and writes every produced frame back.
/// Returns on orderly stop (`stop` observed), on a broken connection, or
/// once the host self-reports unhealthy — in every case after clearing
/// `healthy` when the engine can no longer serve, so the health loop stops
/// reassuring the supervisor and the process is replaced.
pub fn run_worker_data_plane<S, W>(
    source: S,
    mut writer: W,
    mut engine_loop: EngineLoop,
    tick: Duration,
    heartbeat: Duration,
    stop: &std::sync::atomic::AtomicBool,
    healthy: &std::sync::atomic::AtomicBool,
) where
    S: FrameSource,
    W: std::io::Write,
{
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let (frame_tx, frame_rx) = std::sync::mpsc::sync_channel::<DataFrame>(
        super::engine_host::DEFAULT_MATCH_MAILBOX_CAPACITY,
    );
    // The reader thread applies backpressure to the transport when the frame
    // channel is full (`send` blocks) and exits on stream EOF or error. It is
    // deliberately detached: on shutdown it sits in a blocking read that ends
    // with the process.
    let _ = std::thread::Builder::new()
        .name("citadel-worker-data-rx".to_owned())
        .spawn(move || {
            let mut source = source;
            while let Some(frame) = source.read_frame() {
                if frame_tx.send(frame).is_err() {
                    break;
                }
            }
        });
    let tick = tick.max(Duration::from_millis(1));
    let heartbeat = heartbeat.max(Duration::from_millis(1));
    let mut last_round = Instant::now();
    let mut next_round = last_round + tick;
    let mut next_heartbeat = last_round + heartbeat;
    let write_frames = |frames: Vec<DataFrame>, writer: &mut W| -> bool {
        for frame in frames {
            if super::worker_data_protocol::write_data_frame(writer, &frame).is_err() {
                return false;
            }
        }
        true
    };
    loop {
        if stop.load(Ordering::SeqCst) {
            // Orderly shutdown: close every match so the gateway can inform
            // the members before the process exits.
            let frames = engine_loop.shutdown();
            let _ = write_frames(frames, &mut writer);
            return;
        }
        let now = Instant::now();
        let timeout = next_round.saturating_duration_since(now);
        match frame_rx.recv_timeout(timeout) {
            Ok(frame) => {
                let frames = engine_loop.handle_frame(frame);
                if !write_frames(frames, &mut writer) {
                    healthy.store(false, Ordering::SeqCst);
                    return;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                // The data connection is gone; without it no match can be
                // served, so the worker reports unhealthy and is replaced.
                healthy.store(false, Ordering::SeqCst);
                return;
            }
        }
        let now = Instant::now();
        if now >= next_round {
            let frames = engine_loop.run_round(now.duration_since(last_round));
            last_round = now;
            next_round = now + tick;
            if !write_frames(frames, &mut writer) {
                healthy.store(false, Ordering::SeqCst);
                return;
            }
        }
        if now >= next_heartbeat {
            next_heartbeat = now + heartbeat;
            let frame = engine_loop.heartbeat();
            if !write_frames(vec![frame], &mut writer) {
                healthy.store(false, Ordering::SeqCst);
                return;
            }
        }
        if !engine_loop.is_healthy() {
            // Layered watchdog top level: quarantine budget exhausted or the
            // engine died. Stop reassuring the supervisor so the whole
            // process is replaced (a replacement never resumes old matches).
            healthy.store(false, Ordering::SeqCst);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::engine_host::{MatchContext, MatchFault};
    use super::*;
    use crate::runtime::OutboundCommand;
    use crate::runtime::worker_data_protocol::decode_commands;

    /// Context scripted per match id: even ids echo, odd behaviors selectable.
    enum Behavior {
        /// Answers every event with a `Send` carrying its running count.
        Echoes,
        /// Blows its per-invocation budget on every quantum.
        Overruns,
        /// Kills the whole engine on its first event.
        DiesOnEvent,
    }

    struct FakeContext {
        behavior: Behavior,
        count: u64,
    }

    impl MatchContext for FakeContext {
        fn handle_event(
            &mut self,
            invocation: &MatchInvocation,
        ) -> Result<Vec<OutboundCommand>, MatchFault> {
            match self.behavior {
                Behavior::Echoes => {
                    self.count += 1;
                    Ok(vec![OutboundCommand::Send {
                        session: invocation.sender,
                        kind: 99,
                        body: self.count.to_string().into_bytes(),
                        unreliable: false,
                    }])
                }
                Behavior::Overruns => Err(MatchFault::Overrun),
                Behavior::DiesOnEvent => Err(MatchFault::EngineDead),
            }
        }

        fn tick(&mut self, _dt: Duration) -> Result<Vec<OutboundCommand>, MatchFault> {
            match self.behavior {
                Behavior::Overruns => Err(MatchFault::Overrun),
                _ => Ok(Vec::new()),
            }
        }
    }

    struct FakeEngine {
        behaviors: fn(u64) -> Behavior,
    }

    impl MatchEngine for FakeEngine {
        fn engine(&self) -> &'static str {
            "fake"
        }

        fn open_match(&mut self, match_id: u64) -> Result<Box<dyn MatchContext>, MatchFault> {
            Ok(Box::new(FakeContext {
                behavior: (self.behaviors)(match_id),
                count: 0,
            }))
        }
    }

    fn echo_loop(epoch: u64) -> EngineLoop {
        EngineLoop::new(
            Box::new(FakeEngine {
                behaviors: |_| Behavior::Echoes,
            }),
            MatchSchedulerPolicy::default(),
            epoch,
            "sha256:test",
        )
    }

    fn open(match_id: u64, epoch: u64, seq: u64) -> DataFrame {
        DataFrame::MatchOpen {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: FrameHeader {
                match_id,
                epoch,
                seq,
            },
            script_identity: Some("sha256:test".to_string()),
        }
    }

    fn event(match_id: u64, epoch: u64, seq: u64) -> DataFrame {
        DataFrame::MatchEvent {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: FrameHeader {
                match_id,
                epoch,
                seq,
            },
            sender: 7,
            user_id: None,
            kind: 1,
            body: b"ping".to_vec(),
        }
    }

    fn sent_bodies(frames: &[DataFrame], match_id: u64) -> Vec<Vec<u8>> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                DataFrame::MatchCommands {
                    header, commands, ..
                } if header.match_id == match_id => {
                    Some(decode_commands(commands).expect("decodable batch"))
                }
                _ => None,
            })
            .flatten()
            .filter_map(|command| match command {
                OutboundCommand::Send { body, .. } => Some(body),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn match_open_and_event_produce_fenced_command_frames() {
        let mut engine_loop = echo_loop(5);
        assert!(engine_loop.handle_frame(open(1, 5, 1)).is_empty());
        assert!(engine_loop.handle_frame(event(1, 5, 2)).is_empty());
        let frames = engine_loop.run_round(Duration::from_millis(16));
        assert_eq!(frames.len(), 1);
        let DataFrame::MatchCommands {
            protocol_version,
            header,
            commands,
        } = &frames[0]
        else {
            unreachable!("expected a command frame: {frames:?}");
        };
        assert_eq!(*protocol_version, DATA_PROTOCOL_VERSION);
        // The worker's outbound stream is fenced to its generation and
        // per-match sequenced from 1.
        assert_eq!(
            *header,
            FrameHeader {
                match_id: 1,
                epoch: 5,
                seq: 1
            }
        );
        assert_eq!(
            decode_commands(commands).expect("decode"),
            vec![OutboundCommand::Send {
                session: 7,
                kind: 99,
                body: b"1".to_vec(),
                unreliable: false,
            }]
        );
        // The next command frame for the match advances the sequence.
        engine_loop.handle_frame(event(1, 5, 3));
        let frames = engine_loop.run_round(Duration::from_millis(16));
        assert_eq!(frames[0].header().seq, 2);
    }

    #[test]
    fn stale_epoch_and_unknown_match_frames_are_dropped_fail_closed() {
        let mut engine_loop = echo_loop(5);
        engine_loop.handle_frame(open(1, 5, 1));
        // Old worker generation's event: dropped, counted, mutates nothing.
        assert!(engine_loop.handle_frame(event(1, 4, 2)).is_empty());
        // Event for a match the gateway never opened on this generation.
        assert!(engine_loop.handle_frame(event(2, 5, 1)).is_empty());
        // Replayed sequence for the open match.
        engine_loop.handle_frame(event(1, 5, 2));
        assert!(engine_loop.handle_frame(event(1, 5, 2)).is_empty());
        let counters = engine_loop.rx_counters();
        assert_eq!(counters.stale_epoch, 1);
        assert_eq!(counters.unknown_match, 1);
        assert_eq!(counters.replayed_seq, 1);
        // Exactly one accepted event reached the match.
        let frames = engine_loop.run_round(Duration::from_millis(16));
        assert_eq!(sent_bodies(&frames, 1), vec![b"1".to_vec()]);
    }

    #[test]
    fn worker_scoped_variants_from_the_gateway_are_ignored() {
        let mut engine_loop = echo_loop(5);
        engine_loop.handle_frame(open(1, 5, 1));
        let bogus = DataFrame::MatchCommands {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: FrameHeader {
                match_id: 1,
                epoch: 5,
                seq: 2,
            },
            commands: Vec::new(),
        };
        assert!(engine_loop.handle_frame(bogus).is_empty());
        // The bogus frame consumed no sequence number: the real event with
        // the same sequence still validates.
        engine_loop.handle_frame(event(1, 5, 2));
        let frames = engine_loop.run_round(Duration::from_millis(16));
        assert_eq!(sent_bodies(&frames, 1), vec![b"1".to_vec()]);
    }

    #[test]
    fn deadline_overruns_close_only_that_match_over_the_wire() {
        let mut engine_loop = EngineLoop::new(
            Box::new(FakeEngine {
                behaviors: |match_id| {
                    if match_id == 1 {
                        Behavior::Overruns
                    } else {
                        Behavior::Echoes
                    }
                },
            }),
            MatchSchedulerPolicy::default().with_overrun_limit(2),
            5,
            "sha256:test",
        );
        engine_loop.handle_frame(open(1, 5, 1));
        engine_loop.handle_frame(open(2, 5, 1));
        let mut frames = Vec::new();
        for seq in 2..4 {
            engine_loop.handle_frame(event(1, 5, seq));
            engine_loop.handle_frame(event(2, 5, seq));
            frames.extend(engine_loop.run_round(Duration::from_millis(16)));
        }
        // A closed as a server error; B answered both events.
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DataFrame::MatchClosed {
                header: FrameHeader {
                    match_id: 1,
                    epoch: 5,
                    ..
                },
                reason: MatchCloseReason::ServerError,
                ..
            }
        )));
        assert_eq!(sent_bodies(&frames, 2), vec![b"1".to_vec(), b"2".to_vec()]);
        assert!(engine_loop.is_healthy(), "the worker stays healthy");
        // Late traffic for the closed match is dropped and counted.
        assert!(engine_loop.handle_frame(event(1, 5, 9)).is_empty());
        assert_eq!(engine_loop.rx_counters().unknown_match, 1);
    }

    #[test]
    fn script_identity_mismatch_refuses_the_match() {
        let mut engine_loop = echo_loop(5);
        let frames = engine_loop.handle_frame(DataFrame::MatchOpen {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: FrameHeader {
                match_id: 1,
                epoch: 5,
                seq: 1,
            },
            script_identity: Some("sha256:other".to_string()),
        });
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DataFrame::MatchClosed {
                header: FrameHeader { match_id: 1, .. },
                reason: MatchCloseReason::ServerError,
                ..
            }
        )));
        // The refused match never opened: its events are unknown.
        assert!(engine_loop.handle_frame(event(1, 5, 2)).is_empty());
        assert_eq!(engine_loop.rx_counters().unknown_match, 1);
    }

    #[test]
    fn engine_death_reports_once_and_closes_every_match() {
        let mut engine_loop = EngineLoop::new(
            Box::new(FakeEngine {
                behaviors: |match_id| {
                    if match_id == 1 {
                        Behavior::DiesOnEvent
                    } else {
                        Behavior::Echoes
                    }
                },
            }),
            MatchSchedulerPolicy::default(),
            5,
            "sha256:test",
        );
        engine_loop.handle_frame(open(1, 5, 1));
        engine_loop.handle_frame(open(2, 5, 1));
        engine_loop.handle_frame(event(1, 5, 2));
        let frames = engine_loop.run_round(Duration::from_millis(16));
        for match_id in [1, 2] {
            assert!(
                frames.iter().any(|frame| matches!(
                    frame,
                    DataFrame::MatchClosed {
                        header: FrameHeader { match_id: id, .. },
                        reason: MatchCloseReason::EngineDead,
                        ..
                    } if *id == match_id
                )),
                "match {match_id} must close engine-dead: {frames:?}"
            );
        }
        let death_reports = frames
            .iter()
            .filter(|frame| {
                matches!(
                    frame,
                    DataFrame::EngineReport {
                        header: FrameHeader {
                            match_id: WORKER_SCOPE_MATCH_ID,
                            ..
                        },
                        report: EngineReport::EngineDead { .. },
                        ..
                    }
                )
            })
            .count();
        assert_eq!(death_reports, 1, "one engine-death report: {frames:?}");
        assert!(!engine_loop.is_healthy());
        // The report is one-shot: later rounds do not repeat it.
        assert!(engine_loop.run_round(Duration::from_millis(16)).is_empty());
    }

    #[test]
    fn gateway_initiated_close_evicts_without_echo() {
        let mut engine_loop = echo_loop(5);
        engine_loop.handle_frame(open(1, 5, 1));
        let frames = engine_loop.handle_frame(DataFrame::MatchClosed {
            protocol_version: DATA_PROTOCOL_VERSION,
            header: FrameHeader {
                match_id: 1,
                epoch: 5,
                seq: 2,
            },
            reason: MatchCloseReason::Shutdown,
        });
        assert!(frames.is_empty(), "no echo for a gateway-initiated close");
        // The evicted match is gone from the receive table too.
        assert!(engine_loop.handle_frame(event(1, 5, 3)).is_empty());
        assert_eq!(engine_loop.rx_counters().unknown_match, 1);
        assert_eq!(engine_loop.host_counters().unknown_match_events, 0);
    }

    #[test]
    fn heartbeats_are_worker_scoped_and_sequenced() {
        let mut engine_loop = echo_loop(3);
        engine_loop.handle_frame(open(1, 3, 1));
        let first = engine_loop.heartbeat();
        let second = engine_loop.heartbeat();
        let DataFrame::EngineReport {
            header,
            report: EngineReport::Heartbeat { live_matches, .. },
            ..
        } = &first
        else {
            unreachable!("expected a heartbeat: {first:?}");
        };
        assert_eq!(
            *header,
            FrameHeader {
                match_id: WORKER_SCOPE_MATCH_ID,
                epoch: 3,
                seq: 1
            }
        );
        assert_eq!(*live_matches, 1);
        assert_eq!(second.header().seq, 2);
    }

    #[test]
    fn shutdown_emits_orderly_closes() {
        let mut engine_loop = echo_loop(5);
        engine_loop.handle_frame(open(1, 5, 1));
        engine_loop.handle_frame(open(2, 5, 1));
        let frames = engine_loop.shutdown();
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|frame| matches!(
            frame,
            DataFrame::MatchClosed {
                reason: MatchCloseReason::Shutdown,
                ..
            }
        )));
    }
}
