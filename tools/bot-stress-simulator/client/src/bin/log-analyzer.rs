//! Deterministic, streaming analysis for bot-stress JSONL logs.
//!
//! The analyzer intentionally does not use an LLM or keep every event in
//! memory. It can process multi-gigabyte runs while emitting a stable JSON
//! report that CI or another visualization tool can consume.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};

// The simulator emits a move every 250 ms. By default an ACK is considered
// late only once it misses that next authoritative movement window; callers
// can lower this threshold for stricter latency objectives.
const DEFAULT_ACK_WARN_MS: u64 = 250;
const DEFAULT_PEER_WARN_MS: u64 = 1_000;
const DEFAULT_LOSS_WARN_PERCENT: f64 = 1.0;
const DEFAULT_ACK_BALANCE_WARN_PERCENT: f64 = 1.0;
const MAX_MALFORMED_EXAMPLES: usize = 20;
const MAX_LOG_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
struct Config {
    input: Option<PathBuf>,
    report: Option<PathBuf>,
    write_report: bool,
    ack_warn_ms: u64,
    peer_warn_ms: u64,
    loss_warn_percent: f64,
    ack_balance_warn_percent: f64,
    use_color: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            input: None,
            report: None,
            write_report: true,
            ack_warn_ms: DEFAULT_ACK_WARN_MS,
            peer_warn_ms: DEFAULT_PEER_WARN_MS,
            loss_warn_percent: DEFAULT_LOSS_WARN_PERCENT,
            ack_balance_warn_percent: DEFAULT_ACK_BALANCE_WARN_PERCENT,
            use_color: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EventField {
    Text(String),
    Code(u8),
}

impl EventField {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Text(name) => Some(name),
            Self::Code(code) => match code {
                1 => Some("connect_start"),
                2 => Some("connected"),
                3 => Some("connect_error"),
                4 => Some("disconnected"),
                5 => Some("position_sent"),
                6 => Some("send_error"),
                7 => Some("simulation_finished"),
                8 => Some("close_error"),
                9 => Some("move_ack"),
                10 => Some("move_rejected"),
                11 => Some("move_clamped"),
                12 => Some("malformed_ack"),
                13 => Some("peer_position"),
                14 => Some("sequence_gap"),
                15 => Some("malformed_peer_position"),
                16 => Some("player_id_assigned"),
                17 => Some("malformed_player_id"),
                18 => Some("receive_error"),
                19 => Some("unhandled_message"),
                20 => Some("run_metadata"),
                21 => Some("match_joined"),
                22 => Some("match_join_error"),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScopeField {
    Text(String),
    Code(u8),
}

impl ScopeField {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Text(name) => Some(name),
            Self::Code(0) => Some("local"),
            Self::Code(1) => Some("external"),
            Self::Code(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LogRecord {
    #[serde(default, alias = "m")]
    monotonic_ns: Option<u64>,
    #[serde(default, alias = "e")]
    event: Option<EventField>,
    #[serde(default, alias = "s")]
    scope: Option<ScopeField>,
    #[serde(default, alias = "b")]
    bot: Option<usize>,
    #[serde(default, alias = "h")]
    match_index: Option<usize>,
    #[serde(default, alias = "p")]
    player_id: Option<u64>,
    #[serde(default, alias = "r")]
    peer_id: Option<u64>,
    #[serde(default, alias = "l")]
    latency_ns: Option<u64>,
    #[serde(default, alias = "g")]
    sequence_gap: Option<u32>,
    #[serde(default, alias = "d")]
    detail: Option<String>,
}

#[derive(Debug, Default)]
struct LatencyAccumulator {
    samples: u64,
    min_ns: Option<u64>,
    max_ns: Option<u64>,
    sum_ns: u128,
    over_threshold: u64,
}

impl LatencyAccumulator {
    fn observe(&mut self, latency_ns: u64, threshold_ms: u64) {
        self.samples += 1;
        self.min_ns = Some(
            self.min_ns
                .map_or(latency_ns, |value| value.min(latency_ns)),
        );
        self.max_ns = Some(
            self.max_ns
                .map_or(latency_ns, |value| value.max(latency_ns)),
        );
        self.sum_ns += u128::from(latency_ns);
        if latency_ns > threshold_ms.saturating_mul(1_000_000) {
            self.over_threshold += 1;
        }
    }

    fn summary(&self, threshold_ms: u64) -> LatencySummary {
        let average_ms =
            (self.samples > 0).then(|| (self.sum_ns as f64 / self.samples as f64) / 1_000_000.0);
        LatencySummary {
            samples: self.samples,
            min_ms: self.min_ns.map(ns_to_ms),
            average_ms,
            max_ms: self.max_ns.map(ns_to_ms),
            threshold_ms,
            samples_over_threshold: self.over_threshold,
        }
    }
}

#[derive(Debug, Serialize)]
struct LatencySummary {
    samples: u64,
    min_ms: Option<f64>,
    average_ms: Option<f64>,
    max_ms: Option<f64>,
    threshold_ms: u64,
    samples_over_threshold: u64,
}

#[derive(Debug, Serialize)]
struct Summary {
    total_lines: u64,
    valid_records: u64,
    malformed_records: u64,
    duration_seconds: Option<f64>,
    bots_started: usize,
    bots_connected: usize,
    bots_with_server_id: usize,
    matches_joined: usize,
    bots_joined_matches: usize,
    bots_finished: usize,
    connection_failures: usize,
    position_attempts: u64,
    position_acknowledgements: u64,
    unacknowledged_position_attempts: u64,
    bots_with_unacknowledged_attempts: usize,
    peer_updates: u64,
    sequence_gap_events: u64,
    inferred_missing_peer_updates: u64,
    inferred_peer_loss_percent: Option<f64>,
    unreliable_delivery_mode: String,
    move_rejections: u64,
    move_clamps: u64,
    ack_latency: LatencySummary,
    peer_update_age: LatencySummary,
    peer_delivery_interval: LatencySummary,
}

#[derive(Debug, Serialize)]
struct Anomaly {
    severity: Severity,
    code: &'static str,
    message: String,
}

#[derive(Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u8,
    input_file: String,
    generated_unix_ns: u64,
    status: &'static str,
    summary: Summary,
    event_counts: BTreeMap<String, u64>,
    scope_counts: BTreeMap<String, u64>,
    error_event_counts: BTreeMap<String, u64>,
    connection_error_details: BTreeMap<String, u64>,
    malformed_examples: Vec<String>,
    anomalies: Vec<Anomaly>,
}

#[derive(Default)]
struct Collector {
    total_lines: u64,
    valid_records: u64,
    malformed_records: u64,
    malformed_examples: Vec<String>,
    event_counts: BTreeMap<String, u64>,
    scope_counts: BTreeMap<String, u64>,
    error_event_counts: BTreeMap<String, u64>,
    connection_error_details: BTreeMap<String, u64>,
    started_bots: BTreeSet<usize>,
    connected_bots: BTreeSet<usize>,
    bots_with_server_id: BTreeSet<usize>,
    matches_with_joined_bots: BTreeMap<usize, BTreeSet<usize>>,
    finished_bots: BTreeSet<usize>,
    position_attempts_by_bot: BTreeMap<usize, u64>,
    acknowledgements_by_bot: BTreeMap<usize, u64>,
    peer_updates: u64,
    sequence_gap_events: u64,
    inferred_missing_peer_updates: u64,
    move_rejections: u64,
    move_clamps: u64,
    first_monotonic_ns: Option<u64>,
    last_monotonic_ns: Option<u64>,
    ack_latency: LatencyAccumulator,
    peer_update_age: LatencyAccumulator,
    peer_delivery_interval: LatencyAccumulator,
    last_peer_update_by_recipient: BTreeMap<(usize, u64), u64>,
    finished_at_by_bot: BTreeMap<usize, u64>,
    unreliable_latest_wins: bool,
}

impl Collector {
    fn record_malformed(&mut self, line_number: u64, message: impl Into<String>) {
        self.malformed_records += 1;
        if self.malformed_examples.len() < MAX_MALFORMED_EXAMPLES {
            self.malformed_examples
                .push(format!("line {line_number}: {}", message.into()));
        }
    }

    fn observe(&mut self, record: LogRecord, config: &Config) {
        self.valid_records += 1;
        let event = record
            .event
            .as_ref()
            .and_then(EventField::name)
            .unwrap_or("unknown_event");
        let scope = record
            .scope
            .as_ref()
            .and_then(ScopeField::name)
            .unwrap_or("local");
        *self.event_counts.entry(event.to_owned()).or_default() += 1;
        *self.scope_counts.entry(scope.to_owned()).or_default() += 1;

        if let Some(timestamp) = record.monotonic_ns {
            self.first_monotonic_ns = Some(
                self.first_monotonic_ns
                    .map_or(timestamp, |current| current.min(timestamp)),
            );
            self.last_monotonic_ns = Some(
                self.last_monotonic_ns
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
        }

        if event.contains("error") || event.starts_with("malformed_") || event == "unknown_event" {
            *self.error_event_counts.entry(event.to_owned()).or_default() += 1;
        }

        match event {
            "connect_start" => insert_bot(&mut self.started_bots, record.bot),
            "connected" => insert_bot(&mut self.connected_bots, record.bot),
            // Old logs wrote the local server ID in `peer_id`; compact logs
            // write it in `p`. Treat the legacy location as a player ID only
            // for this assignment event, never for peer-position records.
            "player_id_assigned" if record.player_id.or(record.peer_id).is_some() => {
                insert_bot(&mut self.bots_with_server_id, record.bot);
            }
            "match_joined" => {
                if let (Some(match_index), Some(bot)) = (record.match_index, record.bot) {
                    self.matches_with_joined_bots
                        .entry(match_index)
                        .or_default()
                        .insert(bot);
                }
            }
            "simulation_finished" => {
                insert_bot(&mut self.finished_bots, record.bot);
                if let (Some(bot), Some(timestamp)) = (record.bot, record.monotonic_ns) {
                    self.finished_at_by_bot
                        .entry(bot)
                        .and_modify(|current| *current = (*current).max(timestamp))
                        .or_insert(timestamp);
                }
            }
            "position_sent" => increment_bot(&mut self.position_attempts_by_bot, record.bot),
            "move_ack" | "move_rejected" | "move_clamped" => {
                increment_bot(&mut self.acknowledgements_by_bot, record.bot);
                if let Some(latency_ns) = record.latency_ns {
                    self.ack_latency.observe(latency_ns, config.ack_warn_ms);
                }
                if event == "move_rejected" {
                    self.move_rejections += 1;
                }
                if event == "move_clamped" {
                    self.move_clamps += 1;
                }
            }
            "peer_position" => {
                self.peer_updates += 1;
                if let Some(age_ns) = record.latency_ns {
                    self.peer_update_age.observe(age_ns, config.peer_warn_ms);
                }
                if let (Some(bot), Some(peer_id), Some(received_ns)) =
                    (record.bot, record.peer_id, record.monotonic_ns)
                {
                    let key = (bot, peer_id);
                    if let Some(previous_ns) = self.last_peer_update_by_recipient.get(&key)
                        && received_ns >= *previous_ns
                    {
                        self.peer_delivery_interval.observe(
                            received_ns.saturating_sub(*previous_ns),
                            config.peer_warn_ms,
                        );
                    }
                    self.last_peer_update_by_recipient
                        .entry(key)
                        .and_modify(|current| *current = (*current).max(received_ns))
                        .or_insert(received_ns);
                }
            }
            "sequence_gap" => {
                self.sequence_gap_events += 1;
                self.inferred_missing_peer_updates += u64::from(record.sequence_gap.unwrap_or(0));
            }
            "connect_error" => {
                let detail = record
                    .detail
                    .as_deref()
                    .map(truncate_detail)
                    .unwrap_or_else(|| "no detail supplied".to_owned());
                *self.connection_error_details.entry(detail).or_default() += 1;
            }
            "run_metadata" => {
                self.unreliable_latest_wins = record
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("unreliable_delivery=latest-wins"));
            }
            _ => {}
        }
    }

    fn into_report(mut self, input: &Path, config: &Config) -> Report {
        let position_attempts = self.position_attempts_by_bot.values().sum();
        let position_acknowledgements = self.acknowledgements_by_bot.values().sum();
        let mut unacknowledged_position_attempts = 0;
        let mut bots_with_unacknowledged_attempts = 0;
        for (bot, sent) in &self.position_attempts_by_bot {
            let acknowledged = self.acknowledgements_by_bot.get(bot).copied().unwrap_or(0);
            let missing = sent.saturating_sub(acknowledged);
            if missing > 0 {
                bots_with_unacknowledged_attempts += 1;
                unacknowledged_position_attempts += missing;
            }
        }

        let duration_seconds = match (self.first_monotonic_ns, self.last_monotonic_ns) {
            (Some(first), Some(last)) => Some(ns_to_seconds(last.saturating_sub(first))),
            _ => None,
        };
        let connection_failures = self
            .started_bots
            .len()
            .saturating_sub(self.connected_bots.len());
        let inferred_peer_loss_percent = percentage(
            self.inferred_missing_peer_updates,
            self.inferred_missing_peer_updates + self.peer_updates,
        );
        // Inter-arrival gaps reveal stutter that packet age alone cannot: a
        // newly sent packet may be fresh while a particular peer has been
        // absent for seconds. Include the tail up to that bot's simulation end
        // so a stream that stops just before shutdown is still visible.
        for ((bot, _peer), last_received_ns) in &self.last_peer_update_by_recipient {
            let horizon = self
                .finished_at_by_bot
                .get(bot)
                .copied()
                .or(self.last_monotonic_ns)
                .unwrap_or(*last_received_ns);
            if horizon >= *last_received_ns {
                self.peer_delivery_interval.observe(
                    horizon.saturating_sub(*last_received_ns),
                    config.peer_warn_ms,
                );
            }
        }
        let summary = Summary {
            total_lines: self.total_lines,
            valid_records: self.valid_records,
            malformed_records: self.malformed_records,
            duration_seconds,
            bots_started: self.started_bots.len(),
            bots_connected: self.connected_bots.len(),
            bots_with_server_id: self.bots_with_server_id.len(),
            matches_joined: self.matches_with_joined_bots.len(),
            bots_joined_matches: self
                .matches_with_joined_bots
                .values()
                .map(|bots| bots.len())
                .sum(),
            bots_finished: self.finished_bots.len(),
            connection_failures,
            position_attempts,
            position_acknowledgements,
            unacknowledged_position_attempts,
            bots_with_unacknowledged_attempts,
            peer_updates: self.peer_updates,
            sequence_gap_events: self.sequence_gap_events,
            inferred_missing_peer_updates: self.inferred_missing_peer_updates,
            inferred_peer_loss_percent,
            unreliable_delivery_mode: if self.unreliable_latest_wins {
                "latest-wins".to_owned()
            } else {
                "unknown-or-legacy".to_owned()
            },
            move_rejections: self.move_rejections,
            move_clamps: self.move_clamps,
            ack_latency: self.ack_latency.summary(config.ack_warn_ms),
            peer_update_age: self.peer_update_age.summary(config.peer_warn_ms),
            peer_delivery_interval: self.peer_delivery_interval.summary(config.peer_warn_ms),
        };
        let anomalies = detect_anomalies(
            &summary,
            &self.error_event_counts,
            self.unreliable_latest_wins,
            config,
        );
        let status = if anomalies
            .iter()
            .any(|item| item.severity == Severity::Error)
        {
            "anomalies"
        } else if anomalies
            .iter()
            .any(|item| item.severity == Severity::Warning)
        {
            "warnings"
        } else {
            "clean"
        };

        Report {
            schema_version: 2,
            input_file: input.display().to_string(),
            generated_unix_ns: unix_now_ns(),
            status,
            summary,
            event_counts: self.event_counts,
            scope_counts: self.scope_counts,
            error_event_counts: self.error_event_counts,
            connection_error_details: self.connection_error_details,
            malformed_examples: self.malformed_examples,
            anomalies,
        }
    }
}

fn insert_bot(set: &mut BTreeSet<usize>, bot: Option<usize>) {
    if let Some(bot) = bot {
        set.insert(bot);
    }
}

fn increment_bot(counts: &mut BTreeMap<usize, u64>, bot: Option<usize>) {
    if let Some(bot) = bot {
        *counts.entry(bot).or_default() += 1;
    }
}

fn detect_anomalies(
    summary: &Summary,
    error_event_counts: &BTreeMap<String, u64>,
    unreliable_latest_wins: bool,
    config: &Config,
) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();
    if summary.malformed_records > 0 {
        anomalies.push(Anomaly {
            severity: Severity::Error,
            code: "malformed_log_records",
            message: format!(
                "{} records could not be parsed; the trace is incomplete.",
                summary.malformed_records
            ),
        });
    }
    if summary.connection_failures > 0 {
        let rate = percentage(
            summary.connection_failures as u64,
            summary.bots_started as u64,
        )
        .unwrap_or(0.0);
        anomalies.push(Anomaly {
            severity: Severity::Error,
            code: "connection_failures",
            message: format!(
                "{} of {} bots did not complete the connection ({rate:.2}%).",
                summary.connection_failures, summary.bots_started
            ),
        });
    }
    let without_server_id = summary
        .bots_connected
        .saturating_sub(summary.bots_with_server_id);
    if without_server_id > 0 {
        anomalies.push(Anomaly {
            severity: Severity::Error,
            code: "missing_server_player_id",
            message: format!(
                "{without_server_id} connected bots never received a server player ID."
            ),
        });
    }
    let unfinished = summary.bots_connected.saturating_sub(summary.bots_finished);
    if unfinished > 0 {
        anomalies.push(Anomaly {
            severity: Severity::Warning,
            code: "unfinished_simulations",
            message: format!("{unfinished} connected bots have no simulation_finished event."),
        });
    }
    if summary.position_attempts > 0 {
        let imbalance_percent = percentage(
            summary.unacknowledged_position_attempts,
            summary.position_attempts,
        )
        .unwrap_or(0.0);
        if imbalance_percent > config.ack_balance_warn_percent {
            anomalies.push(Anomaly {
                severity: if imbalance_percent > 10.0 {
                    Severity::Error
                } else {
                    Severity::Warning
                },
                code: "acknowledgement_imbalance",
                message: format!(
                    "{} position attempts have no acknowledgement ({imbalance_percent:.2}%, threshold {:.2}%).",
                    summary.unacknowledged_position_attempts, config.ack_balance_warn_percent
                ),
            });
        }
    }
    if let Some(loss_percent) = summary.inferred_peer_loss_percent
        && loss_percent > config.loss_warn_percent
    {
        anomalies.push(Anomaly {
            severity: if unreliable_latest_wins {
                Severity::Info
            } else if loss_percent > 10.0 {
                Severity::Error
            } else {
                Severity::Warning
            },
            code: if unreliable_latest_wins {
                "peer_state_coalesced"
            } else {
                "peer_sequence_gaps"
            },
            message: if unreliable_latest_wins {
                format!(
                    "{} peer sequence numbers were intentionally skipped by latest-wins delivery or QUIC datagrams ({loss_percent:.2}%). Freshness is the primary health signal.",
                    summary.inferred_missing_peer_updates
                )
            } else {
                format!(
                    "{} peer updates are inferred missing ({loss_percent:.2}%, threshold {:.2}%).",
                    summary.inferred_missing_peer_updates, config.loss_warn_percent
                )
            },
        });
    }
    push_latency_anomaly(
        &mut anomalies,
        "slow_acknowledgements",
        "acknowledgements",
        &summary.ack_latency,
    );
    push_latency_anomaly(
        &mut anomalies,
        "stale_peer_updates",
        "peer updates",
        &summary.peer_update_age,
    );
    push_latency_anomaly(
        &mut anomalies,
        "stalled_peer_streams",
        "peer delivery intervals",
        &summary.peer_delivery_interval,
    );
    for (event, count) in error_event_counts {
        if event != "connect_error" && *count > 0 {
            anomalies.push(Anomaly {
                severity: Severity::Error,
                code: "client_or_protocol_errors",
                message: format!("{count} `{event}` events were recorded."),
            });
        }
    }
    if summary.move_rejections > 0 {
        anomalies.push(Anomaly {
            severity: Severity::Info,
            code: "server_collision_rejections",
            message: format!(
                "{} moves were rejected by the authoritative collision map.",
                summary.move_rejections
            ),
        });
    }
    if summary.move_clamps > 0 {
        anomalies.push(Anomaly {
            severity: Severity::Info,
            code: "server_boundary_clamps",
            message: format!(
                "{} moves were clamped to map bounds by the server.",
                summary.move_clamps
            ),
        });
    }
    anomalies
}

fn push_latency_anomaly(
    anomalies: &mut Vec<Anomaly>,
    code: &'static str,
    label: &'static str,
    latency: &LatencySummary,
) {
    if latency.samples_over_threshold == 0 {
        return;
    }
    let rate = percentage(latency.samples_over_threshold, latency.samples).unwrap_or(0.0);
    anomalies.push(Anomaly {
        severity: if rate > 10.0 {
            Severity::Error
        } else {
            Severity::Warning
        },
        code,
        message: format!(
            "{} of {} {label} exceeded {} ms ({rate:.2}%).",
            latency.samples_over_threshold, latency.samples, latency.threshold_ms
        ),
    });
}

fn analyze(input: &Path, config: &Config) -> Result<Report, Box<dyn Error>> {
    let file = File::open(input)?;
    if input.extension().is_some_and(|extension| extension == "gz") {
        let reader = BufReader::with_capacity(1024 * 1024, GzDecoder::new(file));
        analyze_reader(reader, input, config)
    } else {
        let reader = BufReader::with_capacity(1024 * 1024, file);
        analyze_reader(reader, input, config)
    }
}

fn analyze_reader<R: BufRead>(
    mut reader: R,
    input: &Path,
    config: &Config,
) -> Result<Report, Box<dyn Error>> {
    let mut collector = Collector::default();
    let mut line = Vec::new();

    while let Some(too_long) = read_line_capped(&mut reader, &mut line)? {
        collector.total_lines += 1;
        if too_long {
            collector.record_malformed(
                collector.total_lines,
                format!("record exceeds the {MAX_LOG_RECORD_BYTES} byte safety limit"),
            );
            continue;
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<LogRecord>(&line) {
            Ok(record)
                if record
                    .event
                    .as_ref()
                    .and_then(EventField::name)
                    .is_some_and(|event| !event.is_empty()) =>
            {
                collector.observe(record, config);
            }
            Ok(_) => collector.record_malformed(
                collector.total_lines,
                "missing event field or unknown compact event code",
            ),
            Err(error) => collector.record_malformed(collector.total_lines, error.to_string()),
        }
    }
    Ok(collector.into_report(input, config))
}

/// Read one JSONL record without allowing a corrupt line to consume unbounded
/// memory. The entire oversized record is discarded so following valid records
/// remain analyzable.
fn read_line_capped<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> Result<Option<bool>, std::io::Error> {
    line.clear();
    let mut saw_bytes = false;
    let mut too_long = false;

    loop {
        let (consumed, ended) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if saw_bytes {
                    return Ok(Some(too_long));
                }
                return Ok(None);
            }
            saw_bytes = true;
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map_or(available.len(), |index| index + 1);
            if !too_long {
                let remaining = MAX_LOG_RECORD_BYTES.saturating_sub(line.len());
                if consumed > remaining {
                    line.extend_from_slice(&available[..remaining]);
                    too_long = true;
                } else {
                    line.extend_from_slice(&available[..consumed]);
                }
            }
            (consumed, newline.is_some())
        };
        reader.consume(consumed);
        if ended {
            return Ok(Some(too_long));
        }
    }
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Config, String> {
    let mut config = Config::default();
    let mut arguments = arguments.into_iter();
    let _program = arguments.next();
    while let Some(argument) = arguments.next() {
        match argument.to_string_lossy().as_ref() {
            "-h" | "--help" => return Err(usage()),
            "-i" | "--input" => config.input = Some(next_path(&mut arguments, "--input")?),
            "--report" => config.report = Some(next_path(&mut arguments, "--report")?),
            "--no-report" => config.write_report = false,
            "--ack-warn-ms" => config.ack_warn_ms = next_number(&mut arguments, "--ack-warn-ms")?,
            "--peer-warn-ms" => {
                config.peer_warn_ms = next_number(&mut arguments, "--peer-warn-ms")?
            }
            "--loss-warn-percent" => {
                config.loss_warn_percent = next_decimal(&mut arguments, "--loss-warn-percent")?
            }
            "--ack-balance-warn-percent" => {
                config.ack_balance_warn_percent =
                    next_decimal(&mut arguments, "--ack-balance-warn-percent")?
            }
            "--no-color" => config.use_color = false,
            unknown => return Err(format!("Unknown argument `{unknown}`.\n\n{}", usage())),
        }
    }
    if config.report.is_some() && !config.write_report {
        return Err("--report cannot be used together with --no-report".to_owned());
    }
    Ok(config)
}

fn next_path(
    arguments: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<PathBuf, String> {
    arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| format!("{flag} requires a path"))
}

fn next_number(arguments: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<u64, String> {
    let value = arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a whole number"))?;
    value
        .to_string_lossy()
        .parse()
        .map_err(|_| format!("{flag} requires a whole number"))
}

fn next_decimal(arguments: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<f64, String> {
    let value = arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a number"))?;
    let value = value
        .to_string_lossy()
        .parse::<f64>()
        .map_err(|_| format!("{flag} requires a number"))?;
    if value.is_sign_negative() || !value.is_finite() {
        return Err(format!(
            "{flag} must be a finite number greater than or equal to zero"
        ));
    }
    Ok(value)
}

fn newest_log(logs_directory: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let mut newest: Option<(SystemTime, PathBuf)> = None;
    for entry in fs::read_dir(logs_directory)? {
        let entry = entry?;
        let path = entry.path();
        if !is_json_log(&path) {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if newest.as_ref().is_none_or(|(latest, _)| modified > *latest) {
            newest = Some((modified, path));
        }
    }
    newest
        .map(|(_, path)| path)
        .ok_or_else(|| format!("No .jsonl log was found in {}", logs_directory.display()).into())
}

fn is_json_log(path: &Path) -> bool {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("jsonl") => true,
        Some("gz") => path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .is_some_and(|extension| extension == "jsonl"),
        _ => false,
    }
}

fn default_report_path(input: &Path) -> PathBuf {
    let stem = input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("bot-stress");
    PathBuf::from("reports").join(format!(
        "{stem}-analysis-{}-{}.json",
        unix_now_ns(),
        std::process::id()
    ))
}

fn write_report(path: &Path, report: &Report) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn print_report(report: &Report, use_color: bool) {
    let summary = &report.summary;
    let status_color = match report.status {
        "clean" => "32",
        "warnings" => "33",
        _ => "31",
    };
    println!(
        "{}",
        paint(
            use_color,
            status_color,
            &format!("ANÁLISIS {}", report.status.to_uppercase())
        )
    );
    println!(
        "archivo={} registros={} duración={}s",
        report.input_file,
        summary.valid_records,
        format_seconds(summary.duration_seconds)
    );
    println!(
        "bots inicio={} conectados={} id-servidor={} match-join={} matches={} finalizaron={} fallos-conexión={}",
        summary.bots_started,
        summary.bots_connected,
        summary.bots_with_server_id,
        summary.bots_joined_matches,
        summary.matches_joined,
        summary.bots_finished,
        summary.connection_failures
    );
    println!(
        "movimientos intentos={} ack={} sin-ack={} peers={} saltos={} no-entregados={} modo={}",
        summary.position_attempts,
        summary.position_acknowledgements,
        summary.unacknowledged_position_attempts,
        summary.peer_updates,
        summary.inferred_missing_peer_updates,
        format_percent(summary.inferred_peer_loss_percent),
        summary.unreliable_delivery_mode
    );
    println!(
        "latencia-ack={} | antigüedad-peer={} | intervalo-peer={}",
        format_latency(&summary.ack_latency),
        format_latency(&summary.peer_update_age),
        format_latency(&summary.peer_delivery_interval)
    );
    if report.anomalies.is_empty() {
        println!(
            "{}",
            paint(use_color, "32", "Sin anormalidades detectadas.")
        );
    } else {
        for anomaly in &report.anomalies {
            let color = match anomaly.severity {
                Severity::Info => "36",
                Severity::Warning => "33",
                Severity::Error => "31",
            };
            println!(
                "{}",
                paint(
                    use_color,
                    color,
                    &format!(
                        "[{}] {}: {}",
                        severity_label(&anomaly.severity),
                        anomaly.code,
                        anomaly.message
                    )
                )
            );
        }
    }
}

fn format_latency(latency: &LatencySummary) -> String {
    match (latency.samples, latency.average_ms, latency.max_ms) {
        (0, _, _) => "sin muestras".to_owned(),
        (_, Some(average), Some(maximum)) => format!(
            "avg={average:.3}ms max={maximum:.3}ms sobre-{}ms={}/{}",
            latency.threshold_ms, latency.samples_over_threshold, latency.samples
        ),
        _ => "muestras inválidas".to_owned(),
    }
}

fn format_seconds(seconds: Option<f64>) -> String {
    seconds.map_or_else(|| "desconocida".to_owned(), |value| format!("{value:.3}"))
}

fn format_percent(percent: Option<f64>) -> String {
    percent.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.2}%"))
}

fn severity_label(severity: &Severity) -> &'static str {
    match severity {
        Severity::Info => "INFO",
        Severity::Warning => "WARN",
        Severity::Error => "ERROR",
    }
}

fn paint(use_color: bool, color: &str, text: &str) -> String {
    if use_color {
        format!("\x1b[{color}m{text}\x1b[0m")
    } else {
        text.to_owned()
    }
}

fn percentage(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 * 100.0 / denominator as f64)
}

fn ns_to_ms(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn ns_to_seconds(value: u64) -> f64 {
    value as f64 / 1_000_000_000.0
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn truncate_detail(detail: &str) -> String {
    const LIMIT: usize = 160;
    let mut characters = detail.chars();
    let truncated: String = characters.by_ref().take(LIMIT).collect();
    if characters.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn usage() -> String {
    format!(
        "Usage:\n  cargo run --release --bin log-analyzer -- [options]\n\n\
         The newest logs/*.jsonl is used when --input is omitted.\n\n\
         Options:\n\
           -i, --input <path>                  JSONL log to inspect\n\
               --report <path>                 Write the JSON report to this new path\n\
               --no-report                     Only print the terminal report\n\
               --ack-warn-ms <n>               Slow ACK threshold (default {DEFAULT_ACK_WARN_MS})\n\
               --peer-warn-ms <n>              Stale peer update threshold (default {DEFAULT_PEER_WARN_MS})\n\
               --loss-warn-percent <n>         Sequence-loss warning threshold (default {DEFAULT_LOSS_WARN_PERCENT})\n\
               --ack-balance-warn-percent <n>  Missing ACK warning threshold (default {DEFAULT_ACK_BALANCE_WARN_PERCENT})\n\
               --no-color                      Disable ANSI colours\n"
    )
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = parse_args(env::args_os()).map_err(|message| {
        if message == usage() {
            println!("{message}");
            std::process::exit(0);
        }
        message
    })?;
    let input = match config.input.as_deref() {
        Some(input) => input.to_path_buf(),
        None => newest_log(Path::new("logs"))?,
    };
    let report = analyze(&input, &config)?;
    print_report(&report, config.use_color);
    if config.write_report {
        let output = config
            .report
            .clone()
            .unwrap_or_else(|| default_report_path(&input));
        write_report(&output, &report)?;
        println!("reporte-json={}", output.display());
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("log-analyzer error: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            ack_warn_ms: 100,
            use_color: false,
            ..Config::default()
        }
    }

    #[test]
    fn detects_connection_loss_sequence_gaps_and_stale_peers() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl");
        fs::write(
            &input,
            concat!(
                "{\"unix_ns\":1,\"monotonic_ns\":1,\"event\":\"connect_start\",\"scope\":\"local\",\"bot\":1}\n",
                "{\"unix_ns\":2,\"monotonic_ns\":2,\"event\":\"connected\",\"scope\":\"local\",\"bot\":1}\n",
                "{\"unix_ns\":3,\"monotonic_ns\":3,\"event\":\"player_id_assigned\",\"scope\":\"local\",\"bot\":1,\"peer_id\":91}\n",
                "{\"unix_ns\":4,\"monotonic_ns\":4,\"event\":\"position_sent\",\"scope\":\"local\",\"bot\":1}\n",
                "{\"unix_ns\":5,\"monotonic_ns\":5,\"event\":\"move_ack\",\"scope\":\"local\",\"bot\":1,\"latency_ns\":200000000}\n",
                "{\"unix_ns\":6,\"monotonic_ns\":6,\"event\":\"peer_position\",\"scope\":\"external\",\"bot\":1,\"latency_ns\":2000000000}\n",
                "{\"unix_ns\":7,\"monotonic_ns\":7,\"event\":\"sequence_gap\",\"scope\":\"external\",\"bot\":1,\"sequence_gap\":3}\n",
                "{\"unix_ns\":8,\"monotonic_ns\":8,\"event\":\"simulation_finished\",\"scope\":\"local\",\"bot\":1}\n",
                "{\"unix_ns\":9,\"monotonic_ns\":9,\"event\":\"connect_start\",\"scope\":\"local\",\"bot\":2}\n",
                "{\"unix_ns\":10,\"monotonic_ns\":10,\"event\":\"connect_error\",\"scope\":\"local\",\"bot\":2,\"detail\":\"refused\"}\n",
            ),
        )
        .expect("test log");

        let report = analyze(&input, &config()).expect("analysis succeeds");
        assert_eq!(report.summary.bots_started, 2);
        assert_eq!(report.summary.bots_connected, 1);
        assert_eq!(report.summary.bots_with_server_id, 1);
        assert_eq!(report.summary.connection_failures, 1);
        assert_eq!(report.summary.inferred_missing_peer_updates, 3);
        assert_eq!(report.summary.ack_latency.samples_over_threshold, 1);
        assert_eq!(report.summary.peer_update_age.samples_over_threshold, 1);
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.code == "connection_failures")
        );
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.code == "peer_sequence_gaps")
        );
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn malformed_lines_are_reported_without_stopping_analysis() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl");
        fs::write(
            &input,
            "not json\n{\"t\":1,\"m\":1,\"e\":1,\"b\":1}\n{\"t\":2,\"m\":2,\"e\":19,\"b\":1,\"d\":\"kind=99\"}\n",
        )
        .expect("test log");

        let report = analyze(&input, &config()).expect("analysis succeeds");
        assert_eq!(report.summary.malformed_records, 1);
        assert_eq!(report.summary.valid_records, 2);
        assert_eq!(report.event_counts.get("unhandled_message"), Some(&1));
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.code == "malformed_log_records")
        );
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn latest_wins_metadata_turns_sequence_skips_into_an_informational_signal() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl");
        fs::write(
            &input,
            concat!(
                "{\"t\":1,\"m\":1,\"e\":20,\"b\":0,\"d\":\"transport=quic; unreliable_delivery=latest-wins\"}\n",
                "{\"t\":2,\"m\":2,\"e\":13,\"s\":1,\"b\":1,\"q\":1,\"r\":2,\"l\":1000000}\n",
                "{\"t\":3,\"m\":3,\"e\":14,\"s\":1,\"b\":1,\"q\":4,\"r\":2,\"g\":2}\n",
            ),
        )
        .expect("test log");

        let report = analyze(&input, &Config::default()).expect("analysis succeeds");
        assert_eq!(report.summary.unreliable_delivery_mode, "latest-wins");
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.code == "peer_state_coalesced" && item.severity == Severity::Info)
        );
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn detects_stutter_per_peer_even_when_latest_state_is_fresh() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl");
        fs::write(
            &input,
            concat!(
                "{\"t\":1,\"m\":1,\"e\":20,\"b\":0,\"d\":\"unreliable_delivery=latest-wins\"}\n",
                "{\"t\":2,\"m\":1,\"e\":13,\"s\":1,\"b\":1,\"r\":9,\"l\":1}\n",
                "{\"t\":3,\"m\":2000000001,\"e\":13,\"s\":1,\"b\":1,\"r\":9,\"l\":1}\n",
                "{\"t\":4,\"m\":2100000001,\"e\":7,\"b\":1}\n",
            ),
        )
        .expect("test log");

        let report = analyze(&input, &Config::default()).expect("analysis succeeds");
        assert_eq!(
            report.summary.peer_delivery_interval.samples_over_threshold,
            1
        );
        assert!(
            report
                .anomalies
                .iter()
                .any(|item| item.code == "stalled_peer_streams")
        );
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn oversized_records_are_discarded_without_blocking_following_records() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl");
        let oversized = "x".repeat(MAX_LOG_RECORD_BYTES + 1);
        fs::write(
            &input,
            format!("{oversized}\n{{\"t\":1,\"m\":1,\"e\":1,\"b\":1}}\n"),
        )
        .expect("test log");

        let report = analyze(&input, &config()).expect("analysis succeeds");
        assert_eq!(report.summary.malformed_records, 1);
        assert_eq!(report.summary.valid_records, 1);
        assert_eq!(report.summary.bots_started, 1);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn reads_compact_gzip_jsonl_without_loading_it_all() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-log-analyzer-test-{}-{}",
            std::process::id(),
            unix_now_ns()
        ));
        fs::create_dir_all(&directory).expect("test directory");
        let input = directory.join("run.jsonl.gz");
        let file = File::create(&input).expect("gzip output");
        let mut writer = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        writer
            .write_all(b"{\"m\":1,\"e\":1,\"b\":1}\n{\"m\":2,\"e\":2,\"b\":1}\n")
            .expect("gzip record");
        writer.finish().expect("finish gzip");

        let report = analyze(&input, &Config::default()).expect("analysis succeeds");
        assert_eq!(report.summary.valid_records, 2);
        assert_eq!(report.summary.bots_connected, 1);
        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
