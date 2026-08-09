//! Console stress simulator: one async task per bot, compact gzip JSONL event logging,
//! shared collision-aware movement, and Citadel QUIC/WebSocket transports.

mod map;

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use citadel_client::{AuthOutcome, Envelope, QuicClient, WsClient, quic::ClientTls};
use citadel_wire::{
    protocol::{
        KIND_AUTH, KIND_AUTH_RESULT, KIND_ROOM_CREATE, KIND_ROOM_JOINED, decode_auth_result,
    },
    room::{RoomCreate, RoomJoined},
};
use flate2::{Compression, write::GzEncoder};
use serde::Serialize;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::JoinHandle,
    time::{Instant as TokioInstant, timeout},
};

use crate::map::Map;

const KIND_POSITION: u16 = 200;
const KIND_PEER_POSITION: u16 = 201;
const KIND_POSITION_ACK: u16 = 202;
const KIND_PLAYER_ID: u16 = 203;
const KIND_PEER_SNAPSHOT: u16 = 204;
const MOVE_BLOCKED: u8 = 0;
const MOVE_ACCEPTED: u8 = 1;
const MOVE_CLAMPED: u8 = 2;
const DEFAULT_URL: &str = "ws://127.0.0.1:7352/";
const MOVE_INTERVAL: Duration = Duration::from_millis(250);
const RECEIVE_SLICE: Duration = Duration::from_millis(25);
const CONNECTION_SETTLE: Duration = Duration::from_secs(2);
/// Receive acknowledgements already in flight before closing a bot. This is
/// intentionally outside the measured movement window, so the analyzer does
/// not mistake shutdown for server-side packet loss.
const ACK_DRAIN: Duration = Duration::from_secs(2);
const MATCH_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type PeerPosition = (u64, u32, f32, f32, u64);

#[derive(Clone)]
struct RunClock {
    started_at: SystemTime,
    started_monotonic: Instant,
}

#[derive(Clone, Copy)]
struct Timestamp {
    unix_ns: u64,
    monotonic_ns: u64,
}

impl RunClock {
    fn new() -> Self {
        Self {
            started_at: SystemTime::now(),
            started_monotonic: Instant::now(),
        }
    }

    fn now(&self) -> Timestamp {
        let unix_ns = self
            .started_at
            .elapsed()
            .map(|elapsed| {
                self.started_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .saturating_add(elapsed.as_nanos())
            })
            .unwrap_or_else(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            });
        Timestamp {
            unix_ns: unix_ns.min(u128::from(u64::MAX)) as u64,
            monotonic_ns: self
                .started_monotonic
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        }
    }
}

struct LogRecord {
    unix_ns: u64,
    monotonic_ns: u64,
    event: &'static str,
    scope: &'static str,
    bot: usize,
    match_index: Option<usize>,
    player_id: Option<u64>,
    sequence: Option<u32>,
    peer_id: Option<u64>,
    x: Option<f32>,
    z: Option<f32>,
    latency_ns: Option<u64>,
    sequence_gap: Option<u32>,
    detail: Option<String>,
}

/// Compact, documented on-disk representation. The human-readable `LogRecord`
/// remains deliberately rich because it is also printed by verbose mode.
///
/// Absent optional values are omitted, local scope is implicit, and the player
/// ID is emitted only once when the server assigns it to the bot. Together this
/// avoids repeating a large collection of `null`s and stable labels for every
/// peer-position event during high-bot runs.
#[derive(Serialize)]
struct CompactLogRecord<'a> {
    #[serde(rename = "t")]
    #[serde(skip_serializing_if = "Option::is_none")]
    unix_ns: Option<u64>,
    #[serde(rename = "m")]
    monotonic_ns: u64,
    #[serde(rename = "e")]
    event: u8,
    #[serde(rename = "s", skip_serializing_if = "Option::is_none")]
    scope: Option<u8>,
    #[serde(rename = "b")]
    bot: usize,
    /// Match index (one-based). It lets the streaming analyzer prove that a
    /// multi-match run did not accidentally collapse into a shared lobby.
    #[serde(rename = "h", skip_serializing_if = "Option::is_none")]
    match_index: Option<usize>,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    player_id: Option<u64>,
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    sequence: Option<u32>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    peer_id: Option<u64>,
    #[serde(rename = "x", skip_serializing_if = "Option::is_none")]
    x: Option<f32>,
    #[serde(rename = "z", skip_serializing_if = "Option::is_none")]
    z: Option<f32>,
    #[serde(rename = "l", skip_serializing_if = "Option::is_none")]
    latency_ns: Option<u64>,
    #[serde(rename = "g", skip_serializing_if = "Option::is_none")]
    sequence_gap: Option<u32>,
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    detail: Option<&'a str>,
}

impl<'a> From<&'a LogRecord> for CompactLogRecord<'a> {
    fn from(record: &'a LogRecord) -> Self {
        Self {
            // The run metadata anchors the monotonic timeline to wall-clock
            // time. Repeating a 19-digit epoch timestamp for every peer frame
            // adds hundreds of MiB to a stress log without adding evidence.
            unix_ns: (record.event == "run_metadata").then_some(record.unix_ns),
            monotonic_ns: record.monotonic_ns,
            event: event_code(record.event),
            // Event codes 13 and 14 are the only externally scoped events.
            // Local is omitted because it is the overwhelmingly common case.
            scope: (record.scope == "external").then_some(1),
            bot: record.bot,
            match_index: record.match_index,
            // `player_id_assigned` establishes the bot -> server player ID
            // mapping. Repeating it on every subsequent local/peer record does
            // not add forensic value and dominates large-run file sizes.
            player_id: (record.event == "player_id_assigned")
                .then_some(record.player_id)
                .flatten(),
            sequence: record.sequence,
            peer_id: record.peer_id,
            // Peer coordinates remain available in verbose console output,
            // while the compact trace keeps the fields needed to diagnose
            // delivery: recipient, peer, sequence, age and receive time.
            x: (record.event != "peer_position")
                .then_some(record.x)
                .flatten(),
            z: (record.event != "peer_position")
                .then_some(record.z)
                .flatten(),
            latency_ns: record.latency_ns,
            sequence_gap: record.sequence_gap,
            detail: record.detail.as_deref(),
        }
    }
}

fn event_code(event: &str) -> u8 {
    match event {
        "connect_start" => 1,
        "connected" => 2,
        "connect_error" => 3,
        "disconnected" => 4,
        "position_sent" => 5,
        "send_error" => 6,
        "simulation_finished" => 7,
        "close_error" => 8,
        "move_ack" => 9,
        "move_rejected" => 10,
        "move_clamped" => 11,
        "malformed_ack" => 12,
        "peer_position" => 13,
        "sequence_gap" => 14,
        "malformed_peer_position" => 15,
        "player_id_assigned" => 16,
        "malformed_player_id" => 17,
        "receive_error" => 18,
        "unhandled_message" => 19,
        "run_metadata" => 20,
        "match_joined" => 21,
        "match_join_error" => 22,
        // The simulator only writes the known events above. Keeping an
        // explicit reserved code means a future event cannot masquerade as a
        // known one in an analysis report.
        _ => u8::MAX,
    }
}

#[derive(Clone)]
struct Logger {
    clock: Arc<RunClock>,
    sender: mpsc::Sender<LogRecord>,
    match_for_bot: Arc<[usize]>,
}

impl Logger {
    fn at(&self) -> Timestamp {
        self.clock.now()
    }

    async fn write(&self, record: LogRecord) {
        // Backpressure is intentional: a full log channel slows the simulator
        // instead of silently losing the forensic evidence the run was meant to
        // collect.
        let _ = self.sender.send(record).await;
    }

    fn record(&self, timestamp: Timestamp, event: &'static str, bot: usize) -> LogRecord {
        LogRecord {
            unix_ns: timestamp.unix_ns,
            monotonic_ns: timestamp.monotonic_ns,
            event,
            scope: "local",
            bot,
            match_index: bot
                .checked_sub(1)
                .and_then(|index| self.match_for_bot.get(index).copied()),
            player_id: None,
            sequence: None,
            peer_id: None,
            x: None,
            z: None,
            latency_ns: None,
            sequence_gap: None,
            detail: None,
        }
    }
}

fn record_for_player(
    logger: &Logger,
    timestamp: Timestamp,
    event: &'static str,
    bot: usize,
    player_id: Option<u64>,
) -> LogRecord {
    let mut record = logger.record(timestamp, event, bot);
    record.player_id = player_id;
    record
}

#[derive(Default)]
struct Stats {
    connecting: AtomicU64,
    connected: AtomicU64,
    failures: AtomicU64,
    sent: AtomicU64,
    acknowledgements: AtomicU64,
    rejected: AtomicU64,
    received: AtomicU64,
    sequence_gaps: AtomicU64,
    matches_joined: AtomicU64,
    match_join_failures: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    Quic,
    WebSocket,
}

impl Transport {
    fn label(self) -> &'static str {
        match self {
            Self::Quic => "quic",
            Self::WebSocket => "websocket",
        }
    }

    fn default_endpoint(self) -> &'static str {
        match self {
            Self::Quic => "127.0.0.1:7351",
            Self::WebSocket => DEFAULT_URL,
        }
    }
}

#[derive(Clone)]
struct Config {
    endpoint: String,
    transport: Transport,
    force_blocked_first_move: bool,
    connection_start: TokioInstant,
    connection_ramp: Duration,
    movement_start: TokioInstant,
    deadline: TokioInstant,
    match_names: Arc<[String]>,
    users_per_match: usize,
}

enum BotConnection {
    Quic {
        client: QuicClient,
        reliable: VecDeque<Envelope>,
    },
    WebSocket(Box<WsClient>),
}

impl BotConnection {
    async fn connect(transport: Transport, endpoint: &str) -> Result<Self, String> {
        match transport {
            Transport::WebSocket => {
                let (client, outcome) = WsClient::connect_as_guest(endpoint)
                    .await
                    .map_err(|error| error.to_string())?;
                validate_guest_outcome(outcome)?;
                Ok(Self::WebSocket(Box::new(client)))
            }
            Transport::Quic => {
                let address: SocketAddr = endpoint
                    .parse()
                    .map_err(|error| format!("dirección QUIC inválida: {error}"))?;
                let client = QuicClient::connect(
                    address,
                    "localhost",
                    ClientTls::insecure_skip_verification(),
                )
                .await
                .map_err(|error| error.to_string())?;
                client
                    .send_reliable(&Envelope::new(KIND_AUTH, Vec::new()))
                    .await
                    .map_err(|error| error.to_string())?;
                let envelopes = timeout(Duration::from_secs(5), client.recv_uni())
                    .await
                    .map_err(|_| "timeout esperando la autenticación QUIC".to_string())?
                    .map_err(|error| error.to_string())?;
                validate_guest_auth(&envelopes)?;
                Ok(Self::Quic {
                    client,
                    // A server may bundle the guest result and the initial
                    // PLAYER_ID on one reliable stream. Keep every frame
                    // except the authentication result so the simulation
                    // still observes its assigned server id.
                    reliable: envelopes
                        .into_iter()
                        .filter(|envelope| envelope.kind != KIND_AUTH_RESULT)
                        .collect(),
                })
            }
        }
    }

    async fn recv(&mut self) -> Result<Option<Envelope>, String> {
        match self {
            Self::WebSocket(client) => client.recv().await.map_err(|error| error.to_string()),
            Self::Quic { client, reliable } => {
                if let Some(envelope) = reliable.pop_front() {
                    return Ok(Some(envelope));
                }
                tokio::select! {
                    datagram = client.recv_datagram() => datagram
                        .map(Some)
                        .map_err(|error| error.to_string()),
                    incoming = client.recv_uni() => {
                        let mut envelopes: VecDeque<_> = incoming
                            .map_err(|error| error.to_string())?
                            .into();
                        let first = envelopes.pop_front();
                        reliable.extend(envelopes);
                        Ok(first)
                    }
                }
            }
        }
    }

    async fn send_position(&mut self, envelope: &Envelope) -> Result<(), String> {
        match self {
            Self::WebSocket(client) => client
                .send(envelope)
                .await
                .map_err(|error| error.to_string()),
            Self::Quic { client, .. } => client
                .send_unreliable(envelope)
                .map_err(|error| error.to_string()),
        }
    }

    async fn send_reliable(&mut self, envelope: &Envelope) -> Result<(), String> {
        match self {
            Self::WebSocket(client) => client
                .send(envelope)
                .await
                .map_err(|error| error.to_string()),
            Self::Quic { client, .. } => client
                .send_reliable(envelope)
                .await
                .map_err(|error| error.to_string()),
        }
    }

    async fn close(self) -> Result<(), String> {
        match self {
            Self::WebSocket(client) => client.close().await.map_err(|error| error.to_string()),
            Self::Quic { client, .. } => {
                client.close();
                Ok(())
            }
        }
    }
}

fn validate_guest_outcome(outcome: AuthOutcome) -> Result<(), String> {
    match outcome {
        AuthOutcome::Guest => Ok(()),
        AuthOutcome::Authenticated { .. } => {
            Err("el servidor autenticó una sesión cuando se solicitó un guest".to_string())
        }
        AuthOutcome::Rejected { reason_class } => Err(format!(
            "el servidor rechazó la autenticación guest (reason_class={reason_class})"
        )),
    }
}

#[derive(Clone, Copy)]
struct SentMove {
    timestamp: Timestamp,
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state
    }

    fn range(&mut self, min: f32, max: f32) -> f32 {
        let fraction = (self.next_u64() >> 40) as f32 / (1_u64 << 24) as f32;
        min + (max - min) * fraction
    }
}

#[tokio::main]
async fn main() -> AppResult<()> {
    print_banner();
    let matches = ask_usize("Cantidad de matches simultáneos (1-1000)", 1, 1, 1_000)?;
    let users_per_match = ask_usize("Usuarios por match (1-1000)", 10, 1, 1_000)?;
    let bots = matches
        .checked_mul(users_per_match)
        .filter(|total| *total <= 1_000)
        .ok_or("matches × usuarios por match debe estar entre 1 y 1000")?;
    let minutes = ask_u64("Duración de la simulación en minutos", 1, 1, 1_440)?;
    let transport = ask_transport()?;
    let endpoint = ask_string(
        &format!("Endpoint {}", transport.label()),
        transport.default_endpoint(),
    )?;
    let ramp_default = std::env::var("CITADEL_STRESS_CONNECT_RAMP_MS")
        .ok()
        .and_then(|milliseconds| milliseconds.parse::<u64>().ok())
        .unwrap_or(25);
    let connection_ramp_ms = ask_u64(
        "Tiempo entre conexiones (ms; 0=ráfaga)",
        ramp_default,
        0,
        60_000,
    )?;
    let verbose = ask_yes_no(
        "¿Imprimir cada paquete? (puede frenar cargas grandes)",
        false,
    )?;

    let log_path = create_log_path().await?;
    let clock = Arc::new(RunClock::new());
    let (log_sender, log_receiver) = mpsc::channel(262_144);
    let match_for_bot: Arc<[usize]> = (0..bots)
        .map(|bot| bot / users_per_match + 1)
        .collect::<Vec<_>>()
        .into();
    let logger = Logger {
        clock,
        sender: log_sender,
        match_for_bot,
    };
    let compressed_log_path = compressed_log_path(&log_path);
    let writer = tokio::spawn(write_logs(log_path.clone(), log_receiver, verbose));
    let stats = Arc::new(Stats::default());
    let selected_duration = Duration::from_secs(minutes.saturating_mul(60));
    let duration = std::env::var("CITADEL_STRESS_DURATION_SECONDS")
        .ok()
        .and_then(|seconds| seconds.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(selected_duration);
    let connection_start = TokioInstant::now();
    let connection_ramp = Duration::from_millis(connection_ramp_ms);
    let ramp_duration = scaled_duration(connection_ramp, bots.saturating_sub(1));
    let movement_start = connection_start + ramp_duration + CONNECTION_SETTLE;
    let deadline = movement_start + duration;
    let config = Config {
        endpoint,
        transport,
        force_blocked_first_move: std::env::var("CITADEL_STRESS_FORCE_BLOCKED")
            .is_ok_and(|value| value == "1"),
        connection_start,
        connection_ramp,
        movement_start,
        deadline,
        match_names: (1..=matches)
            .map(|match_index| format!("stress-match-{match_index:04}|{users_per_match}"))
            .collect::<Vec<_>>()
            .into(),
        users_per_match,
    };
    let duration_label = if duration == selected_duration {
        format!("{minutes} minuto(s)")
    } else {
        format!("{} segundo(s) [override de prueba]", duration.as_secs())
    };

    println!(
        "{} {} matches × {} bots = {} bots por {} durante {}; rampa={} ms/bot, calentamiento={} ms, movimiento cada {} ms.",
        color("▶", "36"),
        color(&matches.to_string(), "1;36"),
        color(&users_per_match.to_string(), "1;36"),
        color(&bots.to_string(), "1;36"),
        color(transport.label(), "1;35"),
        color(&duration_label, "1;36"),
        connection_ramp_ms,
        CONNECTION_SETTLE.as_millis(),
        MOVE_INTERVAL.as_millis()
    );
    let mut metadata = logger.record(logger.at(), "run_metadata", 0);
    metadata.detail = Some(format!(
        "transport={}; unreliable_delivery=latest-wins; matches={matches}; users_per_match={users_per_match}; connection_ramp_ms={connection_ramp_ms}; run_unix_ns={}",
        transport.label(),
        metadata.unix_ns,
    ));
    logger.write(metadata).await;
    println!(
        "{} Log JSONL comprimido al finalizar: {}",
        color("●", "33"),
        compressed_log_path.display()
    );

    let summary = tokio::spawn(print_summary(stats.clone(), deadline));
    let mut handles = Vec::with_capacity(bots);
    for bot in 1..=bots {
        handles.push(tokio::spawn(run_bot(
            bot,
            config.clone(),
            logger.clone(),
            stats.clone(),
        )));
    }

    for handle in handles {
        await_bot(handle).await;
    }
    summary.abort();
    drop(logger);
    match writer.await {
        Ok(Ok(())) => match tokio::task::spawn_blocking({
            let raw_path = log_path.clone();
            let compressed_path = compressed_log_path.clone();
            move || compress_log(raw_path, compressed_path)
        })
        .await
        {
            Ok(Ok(())) => println!(
                "{} Log comprimido: {}",
                color("✓", "32"),
                compressed_log_path.display()
            ),
            Ok(Err(error)) => {
                eprintln!("{} no se pudo comprimir el log: {error}", color("!", "31"))
            }
            Err(error) => eprintln!(
                "{} tarea de compresión cancelada: {error}",
                color("!", "31")
            ),
        },
        Ok(Err(error)) => eprintln!("{} no se pudo cerrar el log: {error}", color("!", "31")),
        Err(error) => eprintln!("{} tarea de log cancelada: {error}", color("!", "31")),
    }

    print_final_summary(&stats, &log_path);
    Ok(())
}

async fn await_bot(handle: JoinHandle<()>) {
    if let Err(error) = handle.await {
        eprintln!("{} tarea de bot cancelada: {error}", color("!", "31"));
    }
}

async fn run_bot(bot: usize, config: Config, logger: Logger, stats: Arc<Stats>) {
    let connect_at =
        config.connection_start + scaled_duration(config.connection_ramp, bot.saturating_sub(1));
    if TokioInstant::now() < connect_at {
        tokio::time::sleep_until(connect_at).await;
    }
    stats.connecting.fetch_add(1, Ordering::Relaxed);
    logger
        .write(logger.record(logger.at(), "connect_start", bot))
        .await;

    let mut client = match BotConnection::connect(config.transport, &config.endpoint).await {
        Ok(client) => client,
        Err(error) => {
            stats.failures.fetch_add(1, Ordering::Relaxed);
            let mut record = logger.record(logger.at(), "connect_error", bot);
            record.detail = Some(error.to_string());
            logger.write(record).await;
            return;
        }
    };
    stats.connected.fetch_add(1, Ordering::Relaxed);
    logger
        .write(logger.record(logger.at(), "connected", bot))
        .await;

    let map = Map::default();
    let mut rng = Rng::new(bot as u64);
    let mut x = random_free_point(&map, &mut rng);
    let mut z = random_free_point(&map, &mut rng);
    // The independent calls above almost always produce a free pair; this loop
    // makes that guarantee explicit before the first server-authoritative move.
    while !map.is_free(x, z) {
        x = random_free_point(&map, &mut rng);
        z = random_free_point(&map, &mut rng);
    }
    let mut sequence = 0_u32;
    let mut last_move = config.movement_start - MOVE_INTERVAL;
    let mut pending = HashMap::<u32, SentMove>::new();
    let mut last_seen = HashMap::<u64, u32>::new();
    let mut server_player_id = None;

    let match_index = bot.saturating_sub(1) / config.users_per_match;
    let match_name = config
        .match_names
        .get(match_index)
        .cloned()
        .unwrap_or_default();
    if let Err(error) = join_match(
        bot,
        &match_name,
        &mut client,
        &mut x,
        &mut z,
        &mut pending,
        &mut last_seen,
        &mut server_player_id,
        &logger,
        &stats,
    )
    .await
    {
        stats.failures.fetch_add(1, Ordering::Relaxed);
        stats.match_join_failures.fetch_add(1, Ordering::Relaxed);
        let mut record = record_for_player(
            &logger,
            logger.at(),
            "match_join_error",
            bot,
            server_player_id,
        );
        record.detail = Some(error);
        logger.write(record).await;
        let _ = client.close().await;
        return;
    }

    while TokioInstant::now() < config.deadline {
        let remaining = config
            .deadline
            .saturating_duration_since(TokioInstant::now());
        let receive_for = remaining.min(RECEIVE_SLICE);
        match timeout(receive_for, client.recv()).await {
            Ok(Ok(Some(envelope))) => {
                handle_envelope(
                    bot,
                    &envelope,
                    &mut x,
                    &mut z,
                    &mut pending,
                    &mut last_seen,
                    &mut server_player_id,
                    &logger,
                    &stats,
                )
                .await;
            }
            Ok(Ok(None)) => {
                logger
                    .write(record_for_player(
                        &logger,
                        logger.at(),
                        "disconnected",
                        bot,
                        server_player_id,
                    ))
                    .await;
                return;
            }
            Ok(Err(error)) => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                let mut record =
                    record_for_player(&logger, logger.at(), "receive_error", bot, server_player_id);
                record.detail = Some(error.to_string());
                logger.write(record).await;
                return;
            }
            Err(_) => {}
        }

        if TokioInstant::now() >= config.movement_start
            && TokioInstant::now().duration_since(last_move) >= MOVE_INTERVAL
        {
            last_move = TokioInstant::now();
            let (next_x, next_z) = if config.force_blocked_first_move && sequence == 0 {
                // Deliberately targets a known static obstacle. It is only a
                // smoke-test switch for proving that Lua is authoritative.
                (-784.0, -778.0)
            } else {
                choose_move(&map, &mut rng, x, z)
            };
            sequence = sequence.wrapping_add(1);
            let timestamp = logger.at();
            let body = encode_move(sequence, next_x, next_z, timestamp.unix_ns);
            let mut record =
                record_for_player(&logger, timestamp, "position_sent", bot, server_player_id);
            record.sequence = Some(sequence);
            record.x = Some(next_x);
            record.z = Some(next_z);
            logger.write(record).await;

            match client
                .send_position(&Envelope::new(KIND_POSITION, body))
                .await
            {
                Ok(()) => {
                    stats.sent.fetch_add(1, Ordering::Relaxed);
                    pending.insert(sequence, SentMove { timestamp });
                }
                Err(error) => {
                    stats.failures.fetch_add(1, Ordering::Relaxed);
                    let mut error_record = record_for_player(
                        &logger,
                        logger.at(),
                        "send_error",
                        bot,
                        server_player_id,
                    );
                    error_record.sequence = Some(sequence);
                    error_record.detail = Some(error.to_string());
                    logger.write(error_record).await;
                    return;
                }
            }
        }
    }

    logger
        .write(record_for_player(
            &logger,
            logger.at(),
            "simulation_finished",
            bot,
            server_player_id,
        ))
        .await;
    let drain_deadline = TokioInstant::now() + ACK_DRAIN;
    while !pending.is_empty() && TokioInstant::now() < drain_deadline {
        let remaining = drain_deadline.saturating_duration_since(TokioInstant::now());
        match timeout(remaining.min(RECEIVE_SLICE), client.recv()).await {
            Ok(Ok(Some(envelope))) => {
                handle_envelope(
                    bot,
                    &envelope,
                    &mut x,
                    &mut z,
                    &mut pending,
                    &mut last_seen,
                    &mut server_player_id,
                    &logger,
                    &stats,
                )
                .await;
            }
            Ok(Ok(None)) | Err(_) => break,
            Ok(Err(error)) => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                let mut record =
                    record_for_player(&logger, logger.at(), "receive_error", bot, server_player_id);
                record.detail = Some(error.to_string());
                logger.write(record).await;
                break;
            }
        }
    }
    if let Err(error) = client.close().await {
        let mut record =
            record_for_player(&logger, logger.at(), "close_error", bot, server_player_id);
        record.detail = Some(error.to_string());
        logger.write(record).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn join_match(
    bot: usize,
    match_name: &str,
    client: &mut BotConnection,
    x: &mut f32,
    z: &mut f32,
    pending: &mut HashMap<u32, SentMove>,
    last_seen: &mut HashMap<u64, u32>,
    server_player_id: &mut Option<u64>,
    logger: &Logger,
    stats: &Stats,
) -> Result<(), String> {
    client
        .send_reliable(&Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: match_name.as_bytes().to_vec(),
            }
            .encode(),
        ))
        .await?;
    let deadline = TokioInstant::now() + MATCH_JOIN_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(TokioInstant::now());
        if remaining.is_zero() {
            return Err("timeout esperando ROOM_JOINED".to_owned());
        }
        match timeout(remaining.min(RECEIVE_SLICE), client.recv()).await {
            Ok(Ok(Some(envelope))) if envelope.kind == KIND_ROOM_JOINED => {
                let joined = RoomJoined::decode(&envelope.body)
                    .map_err(|error| format!("ROOM_JOINED inválido: {error}"))?;
                stats.matches_joined.fetch_add(1, Ordering::Relaxed);
                let mut record =
                    record_for_player(logger, logger.at(), "match_joined", bot, *server_player_id);
                record.detail = Some(format!(
                    "name={match_name}; room_id={}; map={}; mode={}",
                    joined.room_id, joined.map, joined.mode
                ));
                logger.write(record).await;
                return Ok(());
            }
            Ok(Ok(Some(envelope))) => {
                handle_envelope(
                    bot,
                    &envelope,
                    x,
                    z,
                    pending,
                    last_seen,
                    server_player_id,
                    logger,
                    stats,
                )
                .await;
            }
            Ok(Ok(None)) => return Err("conexión cerrada esperando ROOM_JOINED".to_owned()),
            Ok(Err(error)) => return Err(format!("error esperando ROOM_JOINED: {error}")),
            Err(_) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_envelope(
    bot: usize,
    envelope: &Envelope,
    x: &mut f32,
    z: &mut f32,
    pending: &mut HashMap<u32, SentMove>,
    last_seen: &mut HashMap<u64, u32>,
    server_player_id: &mut Option<u64>,
    logger: &Logger,
    stats: &Stats,
) {
    let received_at = logger.at();
    match envelope.kind {
        KIND_POSITION_ACK => {
            if let Some((sequence, ack_x, ack_z, status)) = decode_ack(&envelope.body) {
                let latency_ns = pending.remove(&sequence).map(|sent| {
                    received_at
                        .monotonic_ns
                        .saturating_sub(sent.timestamp.monotonic_ns)
                });
                *x = ack_x;
                *z = ack_z;
                stats.acknowledgements.fetch_add(1, Ordering::Relaxed);
                let event = if status == MOVE_BLOCKED {
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                    "move_rejected"
                } else if status == MOVE_CLAMPED {
                    "move_clamped"
                } else {
                    "move_ack"
                };
                let mut record =
                    record_for_player(logger, received_at, event, bot, *server_player_id);
                record.sequence = Some(sequence);
                record.x = Some(ack_x);
                record.z = Some(ack_z);
                record.latency_ns = latency_ns;
                if status != MOVE_ACCEPTED && status != MOVE_CLAMPED && status != MOVE_BLOCKED {
                    record.detail = Some(format!("unknown_ack_status={status}"));
                }
                logger.write(record).await;
            } else {
                malformed_record(bot, "malformed_ack", logger).await;
            }
        }
        KIND_PEER_POSITION => {
            if let Some((peer_id, sequence, peer_x, peer_z, sent_unix_ns)) =
                decode_peer(&envelope.body)
            {
                observe_peer_position(
                    bot,
                    peer_id,
                    sequence,
                    peer_x,
                    peer_z,
                    sent_unix_ns,
                    received_at,
                    last_seen,
                    *server_player_id,
                    logger,
                    stats,
                )
                .await;
            } else {
                malformed_record(bot, "malformed_peer_position", logger).await;
            }
        }
        KIND_PEER_SNAPSHOT => {
            if let Some(entries) = decode_peer_snapshot(&envelope.body) {
                for (peer_id, sequence, peer_x, peer_z, sent_unix_ns) in entries {
                    observe_peer_position(
                        bot,
                        peer_id,
                        sequence,
                        peer_x,
                        peer_z,
                        sent_unix_ns,
                        received_at,
                        last_seen,
                        *server_player_id,
                        logger,
                        stats,
                    )
                    .await;
                }
            } else {
                malformed_record(bot, "malformed_peer_position", logger).await;
            }
        }
        KIND_PLAYER_ID => {
            if let Some(bytes) = envelope.body.get(0..8)
                && let Ok(bytes) = bytes.try_into()
            {
                *server_player_id = Some(u64::from_be_bytes(bytes));
                let mut record = record_for_player(
                    logger,
                    received_at,
                    "player_id_assigned",
                    bot,
                    *server_player_id,
                );
                record.detail = Some("local player id assigned by server".to_string());
                logger.write(record).await;
            } else {
                malformed_record(bot, "malformed_player_id", logger).await;
            }
        }
        other => {
            let mut record = record_for_player(
                logger,
                received_at,
                "unhandled_message",
                bot,
                *server_player_id,
            );
            record.detail = Some(format!("kind={other}; bytes={}", envelope.body.len()));
            logger.write(record).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn observe_peer_position(
    bot: usize,
    peer_id: u64,
    sequence: u32,
    peer_x: f32,
    peer_z: f32,
    sent_unix_ns: u64,
    received_at: Timestamp,
    last_seen: &mut HashMap<u64, u32>,
    server_player_id: Option<u64>,
    logger: &Logger,
    stats: &Stats,
) {
    if server_player_id == Some(peer_id) {
        return;
    }
    stats.received.fetch_add(1, Ordering::Relaxed);
    let mut record = record_for_player(logger, received_at, "peer_position", bot, server_player_id);
    record.scope = "external";
    record.sequence = Some(sequence);
    record.peer_id = Some(peer_id);
    record.x = Some(peer_x);
    record.z = Some(peer_z);
    record.latency_ns = Some(received_at.unix_ns.saturating_sub(sent_unix_ns));
    logger.write(record).await;

    if let Some(previous) = last_seen.insert(peer_id, sequence)
        && sequence > previous.wrapping_add(1)
    {
        let gap = sequence.saturating_sub(previous).saturating_sub(1);
        stats
            .sequence_gaps
            .fetch_add(u64::from(gap), Ordering::Relaxed);
        let mut gap_record =
            record_for_player(logger, received_at, "sequence_gap", bot, server_player_id);
        gap_record.scope = "external";
        gap_record.peer_id = Some(peer_id);
        gap_record.sequence = Some(sequence);
        gap_record.sequence_gap = Some(gap);
        logger.write(gap_record).await;
    }
}

async fn malformed_record(bot: usize, event: &'static str, logger: &Logger) {
    logger.write(logger.record(logger.at(), event, bot)).await;
}

fn encode_move(sequence: u32, x: f32, z: f32, unix_ns: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&x.to_be_bytes());
    body.extend_from_slice(&z.to_be_bytes());
    body.extend_from_slice(&unix_ns.to_be_bytes());
    body
}

fn decode_ack(body: &[u8]) -> Option<(u32, f32, f32, u8)> {
    if body.len() != 13 {
        return None;
    }
    Some((
        u32::from_be_bytes(body.get(0..4)?.try_into().ok()?),
        f32::from_be_bytes(body.get(4..8)?.try_into().ok()?),
        f32::from_be_bytes(body.get(8..12)?.try_into().ok()?),
        *body.get(12)?,
    ))
}

fn decode_peer(body: &[u8]) -> Option<PeerPosition> {
    if body.len() != 28 {
        return None;
    }
    Some((
        u64::from_be_bytes(body.get(0..8)?.try_into().ok()?),
        u32::from_be_bytes(body.get(8..12)?.try_into().ok()?),
        f32::from_be_bytes(body.get(12..16)?.try_into().ok()?),
        f32::from_be_bytes(body.get(16..20)?.try_into().ok()?),
        u64::from_be_bytes(body.get(20..28)?.try_into().ok()?),
    ))
}

fn decode_peer_snapshot(body: &[u8]) -> Option<Vec<PeerPosition>> {
    let count = u16::from_be_bytes(body.get(2..4)?.try_into().ok()?);
    let expected_len = 4 + usize::from(count).checked_mul(28)?;
    if body.len() != expected_len {
        return None;
    }
    let mut entries = Vec::with_capacity(usize::from(count));
    for offset in (4..expected_len).step_by(28) {
        entries.push(decode_peer(body.get(offset..offset + 28)?)?);
    }
    Some(entries)
}

fn validate_guest_auth(envelopes: &[Envelope]) -> Result<(), String> {
    let result = envelopes
        .iter()
        .find(|envelope| envelope.kind == KIND_AUTH_RESULT)
        .and_then(|envelope| decode_auth_result(&envelope.body))
        .ok_or_else(|| "invalid or missing QUIC authentication result".to_string())?;
    if result.is_guest() {
        Ok(())
    } else if result.is_rejected() {
        Err(format!(
            "QUIC guest authentication rejected (reason_class={})",
            result.reason_class
        ))
    } else {
        Err("unexpected identity returned for QUIC guest authentication".to_string())
    }
}

fn random_free_point(_map: &Map, rng: &mut Rng) -> f32 {
    // This helper returns one coordinate. `choose_move` and the spawn loop
    // always validate the final pair against the complete collision map.
    rng.range(-900.0, 900.0)
}

fn choose_move(map: &Map, rng: &mut Rng, x: f32, z: f32) -> (f32, f32) {
    for _ in 0..12 {
        let next_x = x + rng.range(-22.0, 22.0);
        let next_z = z + rng.range(-22.0, 22.0);
        if map.segment_is_free(x, z, next_x, next_z) {
            return (next_x, next_z);
        }
    }
    (x, z)
}

async fn create_log_path() -> AppResult<PathBuf> {
    let directory = PathBuf::from("logs");
    tokio::fs::create_dir_all(&directory).await?;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(directory.join(format!(
        "bot-stress-{timestamp}-{}.jsonl",
        std::process::id()
    )))
}

fn compressed_log_path(path: &Path) -> PathBuf {
    path.with_extension("jsonl.gz")
}

async fn write_logs(
    path: PathBuf,
    mut receiver: mpsc::Receiver<LogRecord>,
    verbose: bool,
) -> AppResult<()> {
    let file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await?;
    let mut writer = tokio::io::BufWriter::with_capacity(1_048_576, file);
    while let Some(record) = receiver.recv().await {
        if verbose {
            print_verbose(&record);
        }
        let line = serde_json::to_string(&CompactLogRecord::from(&record))?;
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;
    Ok(())
}

fn compress_log(raw_path: PathBuf, compressed_path: PathBuf) -> AppResult<()> {
    let source = std::fs::File::open(&raw_path)?;
    let destination = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&compressed_path)?;
    let mut writer = GzEncoder::new(
        io::BufWriter::with_capacity(1_048_576, destination),
        Compression::fast(),
    );
    let mut reader = io::BufReader::with_capacity(1_048_576, source);
    io::copy(&mut reader, &mut writer)?;
    let mut destination = writer.finish()?;
    destination.flush()?;
    std::fs::remove_file(raw_path)?;
    Ok(())
}

async fn print_summary(stats: Arc<Stats>, deadline: TokioInstant) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        if TokioInstant::now() >= deadline {
            return;
        }
        let failures = stats.failures.load(Ordering::Relaxed);
        let status_tone = if failures == 0 { "32" } else { "1;31" };
        println!(
            "{} conectados={} envíos={} ack={} recibidos={} bloqueados={} huecos={} fallos={}",
            color("●", status_tone),
            stats.connected.load(Ordering::Relaxed),
            stats.sent.load(Ordering::Relaxed),
            stats.acknowledgements.load(Ordering::Relaxed),
            stats.received.load(Ordering::Relaxed),
            stats.rejected.load(Ordering::Relaxed),
            stats.sequence_gaps.load(Ordering::Relaxed),
            color(
                &failures.to_string(),
                if failures == 0 { "32" } else { "1;31" }
            ),
        );
    }
}

fn print_final_summary(stats: &Stats, path: &Path) {
    let failures = stats.failures.load(Ordering::Relaxed);
    let status_tone = if failures == 0 { "1;32" } else { "1;31" };
    let status_icon = if failures == 0 { "✓" } else { "!" };
    let status_label = if failures == 0 {
        "simulación terminada"
    } else {
        "simulación terminada con fallos"
    };
    println!("{} {status_label}", color(status_icon, status_tone));
    println!(
        "  conexiones={} envíos={} ack={} recibidos={} bloqueados={} huecos={} fallos={}",
        stats.connected.load(Ordering::Relaxed),
        stats.sent.load(Ordering::Relaxed),
        stats.acknowledgements.load(Ordering::Relaxed),
        stats.received.load(Ordering::Relaxed),
        stats.rejected.load(Ordering::Relaxed),
        stats.sequence_gaps.load(Ordering::Relaxed),
        color(
            &failures.to_string(),
            if failures == 0 { "32" } else { "1;31" }
        ),
    );
    println!("  {} {}", color("log", "33"), path.display());
}

fn print_verbose(record: &LogRecord) {
    let tone = match record.event {
        event
            if event.ends_with("_error")
                || matches!(event, "disconnected" | "move_rejected")
                || event.starts_with("malformed_") =>
        {
            "1;31"
        }
        "connected" | "move_ack" => "32",
        "sequence_gap" | "move_clamped" => "33",
        _ => "36",
    };
    println!(
        "{} scope={} bot-local={} jugador-local={:?} jugador-externo={:?} event={} seq={:?} pos=({:?},{:?}) latency_ns={:?}",
        color("·", tone),
        record.scope,
        record.bot,
        record.player_id,
        record.peer_id,
        record.event,
        record.sequence,
        record.x,
        record.z,
        record.latency_ns,
    );
}

fn ask_usize(label: &str, default: usize, min: usize, max: usize) -> AppResult<usize> {
    loop {
        let value = ask_string(label, &default.to_string())?;
        if let Ok(parsed) = value.parse::<usize>()
            && (min..=max).contains(&parsed)
        {
            return Ok(parsed);
        }
        println!(
            "{} Debe ser un número entre {min} y {max}.",
            color("!", "31")
        );
    }
}

fn ask_u64(label: &str, default: u64, min: u64, max: u64) -> AppResult<u64> {
    loop {
        let value = ask_string(label, &default.to_string())?;
        if let Ok(parsed) = value.parse::<u64>()
            && (min..=max).contains(&parsed)
        {
            return Ok(parsed);
        }
        println!(
            "{} Debe ser un número entre {min} y {max}.",
            color("!", "31")
        );
    }
}

fn ask_yes_no(label: &str, default: bool) -> AppResult<bool> {
    let default_label = if default { "S/n" } else { "s/N" };
    loop {
        let answer = ask_string(&format!("{label} [{default_label}]"), "")?;
        match answer.trim().to_ascii_lowercase().as_str() {
            "" => return Ok(default),
            "s" | "si" | "sí" | "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => println!("{} Responde s o n.", color("!", "31")),
        }
    }
}

fn ask_string(label: &str, default: &str) -> AppResult<String> {
    print!("{} {} [{}]: ", color("?", "1;36"), label, default);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let value = line.trim();
    Ok(if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    })
}

fn ask_transport() -> AppResult<Transport> {
    loop {
        let value = ask_string("Transporte: 1=WebSocket, 2=QUIC", "2")?;
        match value.trim() {
            "1" | "ws" | "websocket" => return Ok(Transport::WebSocket),
            "2" | "quic" => return Ok(Transport::Quic),
            _ => println!("{} Selecciona 1 (WebSocket) o 2 (QUIC).", color("!", "31")),
        }
    }
}

fn print_banner() {
    println!("{}", color("CITADEL · BOT STRESS SIMULATOR", "1;35"));
    println!("Mapa 2000×2000, 80 obstáculos, QUIC/WebSocket y JSONL compacto.");
}

fn color(value: &str, code: &str) -> String {
    format!("\x1b[{code}m{value}\x1b[0m")
}

fn scaled_duration(unit: Duration, multiplier: usize) -> Duration {
    let nanoseconds = unit
        .as_nanos()
        .saturating_mul(multiplier as u128)
        .min(Duration::MAX.as_nanos());
    Duration::new(
        (nanoseconds / 1_000_000_000) as u64,
        (nanoseconds % 1_000_000_000) as u32,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use citadel_client::{AuthOutcome, Envelope};
    use citadel_wire::protocol::{
        AUTH_REASON_AUTH_REQUIRED, KIND_AUTH_RESULT, encode_auth_guest, encode_auth_rejected,
    };

    use super::{
        CompactLogRecord, LogRecord, decode_ack, decode_peer, decode_peer_snapshot, encode_move,
        event_code, scaled_duration, validate_guest_auth, validate_guest_outcome,
    };

    #[test]
    fn compact_logs_use_codes_and_omit_repeated_or_empty_fields() {
        let peer_record = LogRecord {
            unix_ns: 10,
            monotonic_ns: 5,
            event: "peer_position",
            scope: "external",
            bot: 7,
            match_index: Some(3),
            player_id: Some(44),
            sequence: Some(9),
            peer_id: Some(88),
            x: Some(1.5),
            z: Some(-2.0),
            latency_ns: Some(12),
            sequence_gap: None,
            detail: None,
        };
        let encoded = serde_json::to_value(CompactLogRecord::from(&peer_record))
            .expect("compact peer record serializes");
        assert_eq!(
            encoded.get("e").and_then(serde_json::Value::as_u64),
            Some(13)
        );
        assert_eq!(
            encoded.get("s").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert!(encoded.get("p").is_none());
        assert!(encoded.get("t").is_none());
        assert!(encoded.get("x").is_none());
        assert!(encoded.get("z").is_none());
        assert!(encoded.get("g").is_none());
        assert!(encoded.get("d").is_none());
        assert_eq!(
            encoded.get("h").and_then(serde_json::Value::as_u64),
            Some(3)
        );

        let assigned_record = LogRecord {
            event: "player_id_assigned",
            scope: "local",
            ..peer_record
        };
        let encoded = serde_json::to_value(CompactLogRecord::from(&assigned_record))
            .expect("compact assignment record serializes");
        assert_eq!(
            encoded.get("e").and_then(serde_json::Value::as_u64),
            Some(16)
        );
        assert_eq!(
            encoded.get("p").and_then(serde_json::Value::as_u64),
            Some(44)
        );
        assert!(encoded.get("s").is_none());
        assert_eq!(event_code("unhandled_message"), 19);
        assert_eq!(event_code("run_metadata"), 20);
    }

    #[test]
    fn websocket_guest_handshake_accepts_only_guest_outcomes() {
        assert!(validate_guest_outcome(AuthOutcome::Guest).is_ok());
        assert!(validate_guest_outcome(AuthOutcome::Rejected { reason_class: 1 }).is_err());
        assert!(
            validate_guest_outcome(AuthOutcome::Authenticated {
                user_id: "unexpected".to_string(),
            })
            .is_err()
        );
    }

    #[test]
    fn connection_ramp_duration_scales_without_overflowing() {
        assert_eq!(
            scaled_duration(Duration::from_millis(25), 199),
            Duration::from_millis(4_975)
        );
        assert_eq!(scaled_duration(Duration::MAX, usize::MAX), Duration::MAX);
    }

    #[test]
    fn move_body_round_trips_at_its_exact_wire_size() {
        let body = encode_move(42, -12.5, 99.25, 7_000_000);
        assert_eq!(body.len(), 20);
        assert_eq!(
            body.get(0..4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_be_bytes),
            Some(42)
        );
        assert_eq!(
            body.get(4..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(f32::from_be_bytes),
            Some(-12.5)
        );
        assert_eq!(
            body.get(8..12)
                .and_then(|bytes| bytes.try_into().ok())
                .map(f32::from_be_bytes),
            Some(99.25)
        );
    }

    #[test]
    fn ack_and_peer_decoders_reject_wrong_lengths() {
        assert!(decode_ack(&[0; 12]).is_none());
        assert!(decode_peer(&[0; 27]).is_none());
    }

    #[test]
    fn peer_snapshot_decoder_reads_datagram_sized_batches() {
        let mut snapshot = vec![0, 3, 0, 2];
        for (player, sequence, x, z, sent_ns) in [
            (11_u64, 7_u32, 1.5_f32, -2.0_f32, 99_u64),
            (22_u64, 8_u32, -3.5_f32, 4.0_f32, 100_u64),
        ] {
            snapshot.extend_from_slice(&player.to_be_bytes());
            snapshot.extend_from_slice(&sequence.to_be_bytes());
            snapshot.extend_from_slice(&x.to_be_bytes());
            snapshot.extend_from_slice(&z.to_be_bytes());
            snapshot.extend_from_slice(&sent_ns.to_be_bytes());
        }
        assert_eq!(
            decode_peer_snapshot(&snapshot),
            Some(vec![(11, 7, 1.5, -2.0, 99), (22, 8, -3.5, 4.0, 100)])
        );
        assert!(decode_peer_snapshot(&snapshot[..snapshot.len() - 1]).is_none());
    }

    #[test]
    fn peer_decoder_reads_server_tag_and_sender_timestamp() {
        let mut body = Vec::new();
        body.extend_from_slice(&99_u64.to_be_bytes());
        body.extend_from_slice(&7_u32.to_be_bytes());
        body.extend_from_slice(&4.5_f32.to_be_bytes());
        body.extend_from_slice(&(-2.0_f32).to_be_bytes());
        body.extend_from_slice(&123_u64.to_be_bytes());
        assert_eq!(decode_peer(&body), Some((99, 7, 4.5, -2.0, 123)));
    }

    #[test]
    fn quic_guest_handshake_accepts_only_a_valid_guest_result() {
        let guest = Envelope::new(KIND_AUTH_RESULT, encode_auth_guest());
        assert!(validate_guest_auth(&[guest]).is_ok());

        let rejected = Envelope::new(
            KIND_AUTH_RESULT,
            encode_auth_rejected(AUTH_REASON_AUTH_REQUIRED),
        );
        assert!(validate_guest_auth(&[rejected]).is_err());
        assert!(validate_guest_auth(&[Envelope::new(KIND_AUTH_RESULT, Vec::new())]).is_err());
    }
}
