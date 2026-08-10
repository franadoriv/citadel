//! Two-node matchmaker load probe.
//!
//! This deliberately exercises the distributed control plane rather than the
//! single-node room-create path in the main stress simulator.  Every cohort is
//! one authenticated player connected to node A plus one connected to node B.

use std::{
    error::Error,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use citadel_client::{AuthOutcome, Envelope, WsClient};
use citadel_wire::protocol::{
    KIND_MATCHMAKER_MATCHED, KIND_ROOM_JOINED, KIND_RPC_REQUEST, KIND_RPC_RESPONSE,
    decode_rpc_response, encode_rpc_request,
};
use serde::Deserialize;
use tokio::time::timeout;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct NodeEndpoint {
    name: &'static str,
    websocket: String,
    http: String,
}

#[derive(Clone)]
struct Config {
    nodes: [NodeEndpoint; 2],
    run_id: u128,
}

#[derive(Default)]
struct Stats {
    authenticated: AtomicU64,
    add_ok: AtomicU64,
    handoffs: AtomicU64,
    accepted: AtomicU64,
    rooms_joined: AtomicU64,
    failures: AtomicU64,
    node_a_sessions: AtomicU64,
    node_b_sessions: AtomicU64,
    add_latency_ms: Mutex<Vec<u64>>,
    handoff_latency_ms: Mutex<Vec<u64>>,
    accept_latency_ms: Mutex<Vec<u64>>,
}

#[derive(Deserialize)]
struct AuthReply {
    token: String,
}

#[derive(Deserialize)]
struct AddReply {
    ticket_id: String,
}

#[derive(Deserialize)]
struct Handoff {
    ticket_id: String,
    join_token: String,
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let bots = ask_usize("Bots totales (par, 2-1000)", 20, 2, 1_000)?;
    if !bots.is_multiple_of(2) {
        return Err("el modo clúster necesita un número par: cada cohorte tiene A + B".into());
    }
    let node_a = NodeEndpoint {
        name: "node-a",
        websocket: ask_string("WebSocket node-a", "ws://127.0.0.1:7352/")?,
        http: ask_string("HTTP node-a", "http://127.0.0.1:7350/")?,
    };
    let node_b = NodeEndpoint {
        name: "node-b",
        websocket: ask_string("WebSocket node-b", "ws://127.0.0.1:7356/")?,
        http: ask_string("HTTP node-b", "http://127.0.0.1:7354/")?,
    };
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let config = Config {
        nodes: [node_a, node_b],
        run_id,
    };
    let stats = Arc::new(Stats::default());
    println!(
        "▶ cluster-matchmaker: {} cohorts A+B ({} bots); cada pareja sólo acepta a su peer remoto.",
        bots / 2,
        bots
    );

    let mut tasks = Vec::with_capacity(bots);
    for bot in 0..bots {
        tasks.push(tokio::spawn(run_bot(bot, config.clone(), Arc::clone(&stats))));
    }
    for task in tasks {
        if let Err(error) = task.await {
            eprintln!("! tarea cancelada: {error}");
            stats.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    print_summary(&stats, bots);
    if stats.failures.load(Ordering::Relaxed) != 0
        || stats.rooms_joined.load(Ordering::Relaxed) != bots as u64
    {
        return Err("la prueba de clúster tuvo fallos; consulta los errores anteriores".into());
    }
    Ok(())
}

async fn run_bot(bot: usize, config: Config, stats: Arc<Stats>) {
    let node_index = bot % 2;
    let node = &config.nodes[node_index];
    if node_index == 0 {
        stats.node_a_sessions.fetch_add(1, Ordering::Relaxed);
    } else {
        stats.node_b_sessions.fetch_add(1, Ordering::Relaxed);
    }
    let pair = bot / 2;
    // Lease renewal is a durable compare-and-swap.  A tiny deterministic
    // stagger prevents the probe itself from turning its first millisecond
    // into a synthetic write-conflict benchmark, while still keeping every
    // cohort cross-node and allowing many pairs to be in flight.
    tokio::time::sleep(Duration::from_millis(
        u64::try_from(pair).unwrap_or(u64::MAX).saturating_mul(100)
            // The first node deterministically establishes the initial lease;
            // node B then exercises the remote submit path instead of both
            // nodes attempting a first acquisition at the same instant.
            + u64::try_from(node_index).unwrap_or(0).saturating_mul(400),
    ))
    .await;
    let result = run_bot_inner(bot, pair, node, config.run_id, &stats).await;
    if let Err(error) = result {
        stats.failures.fetch_add(1, Ordering::Relaxed);
        eprintln!("! bot {} ({}): {error}", bot + 1, node.name);
    }
}

async fn run_bot_inner(
    bot: usize,
    pair: usize,
    node: &NodeEndpoint,
    run_id: u128,
    stats: &Stats,
) -> Result<(), String> {
    let token = authenticate(node, bot, run_id).await?;
    let (mut client, outcome) = WsClient::connect_with_token(&node.websocket, token.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    if !matches!(outcome, AuthOutcome::Authenticated { .. }) {
        return Err("el servidor no autenticó el token recién emitido".to_owned());
    }
    stats.authenticated.fetch_add(1, Ordering::Relaxed);

    // Both members of a pair use the same exact predicate, but are pinned to
    // opposite session nodes by `bot % 2`. This makes every formed cohort cross
    // the node boundary even when all cohorts are submitted concurrently.
    let pair_name = format!("cluster-pair-{run_id}-{pair}");
    let request = serde_json::json!({
        "query": format!("pair == \"{pair_name}\""),
        "properties": { "pair": pair_name },
        "min_count": 2,
        "max_count": 2,
        "ttl_ms": 60_000,
    });
    let started = Instant::now();
    let (reply, pending) = rpc_wait(&mut client, 1, "matchmaker.add", request.to_string().as_bytes())
        .await?;
    let add: AddReply = serde_json::from_slice(&reply)
        .map_err(|error| format!("respuesta add inválida: {error}"))?;
    stats.add_ok.fetch_add(1, Ordering::Relaxed);
    observe(&stats.add_latency_ms, started.elapsed());

    let handoff_started = Instant::now();
    let handoff = wait_for_handoff(&mut client, pending, &add.ticket_id).await?;
    stats.handoffs.fetch_add(1, Ordering::Relaxed);
    observe(&stats.handoff_latency_ms, handoff_started.elapsed());

    let accept_started = Instant::now();
    let accept_request = serde_json::json!({
        "ticket_id": handoff.ticket_id,
        "join_token": handoff.join_token,
    });
    let (_, pending) = rpc_wait(
        &mut client,
        2,
        "matchmaker.accept",
        accept_request.to_string().as_bytes(),
    )
    .await?;
    stats.accepted.fetch_add(1, Ordering::Relaxed);
    observe(&stats.accept_latency_ms, accept_started.elapsed());
    wait_for_room_joined(&mut client, pending).await?;
    stats.rooms_joined.fetch_add(1, Ordering::Relaxed);
    client.close().await.map_err(|error| error.to_string())
}

async fn authenticate(node: &NodeEndpoint, bot: usize, run_id: u128) -> Result<String, String> {
    let response = reqwest::Client::new()
        .post(format!("{}/v1/auth/custom", node.http.trim_end_matches('/')))
        .json(&serde_json::json!({
            "id": format!("cluster-stress-{run_id}-{bot}"),
            "create": true,
            "username": format!("cluster_bot_{run_id}_{bot}"),
        }))
        .send()
        .await
        .map_err(|_| "no se pudo conectar a la API de autenticación".to_owned())?;
    if !response.status().is_success() {
        return Err(format!("auth HTTP respondió {}", response.status()));
    }
    response
        .json::<AuthReply>()
        .await
        .map(|reply| reply.token)
        .map_err(|_| "auth HTTP devolvió JSON inválido".to_owned())
}

async fn rpc_wait(
    client: &mut WsClient,
    request_id: u64,
    method: &str,
    payload: &[u8],
) -> Result<(Vec<u8>, Vec<Envelope>), String> {
    client
        .send(&Envelope::new(
            KIND_RPC_REQUEST,
            encode_rpc_request(request_id, method, payload),
        ))
        .await
        .map_err(|error| error.to_string())?;
    timeout(RPC_TIMEOUT, async {
        let mut pending = Vec::new();
        loop {
            let envelope = client
                .recv()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "conexión cerrada esperando respuesta RPC".to_owned())?;
            if envelope.kind != KIND_RPC_RESPONSE {
                pending.push(envelope);
                continue;
            }
            let response = decode_rpc_response(&envelope.body)
                .ok_or_else(|| "respuesta RPC inválida".to_owned())?;
            if response.request_id != request_id {
                pending.push(envelope);
                continue;
            }
            if !response.is_ok() {
                return Err(format!(
                    "RPC {method} rechazado: {}",
                    String::from_utf8_lossy(response.payload)
                ));
            }
            return Ok((response.payload.to_vec(), pending));
        }
    })
    .await
    .map_err(|_| format!("timeout en {method}"))?
}

async fn wait_for_handoff(
    client: &mut WsClient,
    pending: Vec<Envelope>,
    ticket_id: &str,
) -> Result<Handoff, String> {
    timeout(HANDOFF_TIMEOUT, async {
        for envelope in pending {
            if envelope.kind == KIND_MATCHMAKER_MATCHED {
                let handoff: Handoff = serde_json::from_slice(&envelope.body)
                    .map_err(|error| format!("handoff inválido: {error}"))?;
                if handoff.ticket_id == ticket_id {
                    return Ok(handoff);
                }
            }
        }
        loop {
            let envelope = client
                .recv()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "conexión cerrada esperando handoff".to_owned())?;
            if envelope.kind != KIND_MATCHMAKER_MATCHED {
                continue;
            }
            let handoff: Handoff = serde_json::from_slice(&envelope.body)
                .map_err(|error| format!("handoff inválido: {error}"))?;
            if handoff.ticket_id == ticket_id {
                return Ok(handoff);
            }
        }
    })
    .await
    .map_err(|_| "timeout esperando KIND_MATCHMAKER_MATCHED".to_owned())?
}

async fn wait_for_room_joined(client: &mut WsClient, pending: Vec<Envelope>) -> Result<(), String> {
    timeout(HANDOFF_TIMEOUT, async {
        if pending.iter().any(|envelope| envelope.kind == KIND_ROOM_JOINED) {
            return Ok(());
        }
        loop {
            let envelope: Envelope = client
                .recv()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "conexión cerrada esperando ROOM_JOINED".to_owned())?;
            if envelope.kind == KIND_ROOM_JOINED {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| "timeout esperando KIND_ROOM_JOINED".to_owned())?
}

fn observe(samples: &Mutex<Vec<u64>>, elapsed: Duration) {
    if let Ok(mut samples) = samples.lock() {
        samples.push(elapsed.as_millis().min(u128::from(u64::MAX)) as u64);
    }
}

fn percentile(samples: &Mutex<Vec<u64>>, percent: usize) -> Option<u64> {
    let mut values = samples.lock().ok()?.clone();
    values.sort_unstable();
    let index = values.len().checked_sub(1)?.saturating_mul(percent) / 100;
    values.get(index).copied()
}

fn print_summary(stats: &Stats, bots: usize) {
    println!("\ncluster-matchmaker summary");
    println!(
        "sessions: node-a={}; node-b={}; authenticated={}/{}",
        stats.node_a_sessions.load(Ordering::Relaxed),
        stats.node_b_sessions.load(Ordering::Relaxed),
        stats.authenticated.load(Ordering::Relaxed),
        bots,
    );
    println!(
        "add={}; handoffs={}; accepted={}; room_joined={}; failures={}",
        stats.add_ok.load(Ordering::Relaxed),
        stats.handoffs.load(Ordering::Relaxed),
        stats.accepted.load(Ordering::Relaxed),
        stats.rooms_joined.load(Ordering::Relaxed),
        stats.failures.load(Ordering::Relaxed),
    );
    for (label, samples) in [
        ("matchmaker.add", &stats.add_latency_ms),
        ("handoff", &stats.handoff_latency_ms),
        ("matchmaker.accept", &stats.accept_latency_ms),
    ] {
        if let (Some(p50), Some(p95), Some(p99)) = (
            percentile(samples, 50),
            percentile(samples, 95),
            percentile(samples, 99),
        ) {
            println!("{label} latency ms: p50={p50}; p95={p95}; p99={p99}");
        }
    }
}

fn ask_usize(prompt: &str, default: usize, min: usize, max: usize) -> AppResult<usize> {
    loop {
        let value = ask_string(prompt, &default.to_string())?;
        match value.parse::<usize>() {
            Ok(value) if (min..=max).contains(&value) => return Ok(value),
            _ => eprintln!("Introduce un entero entre {min} y {max}."),
        }
    }
}

fn ask_string(prompt: &str, default: &str) -> AppResult<String> {
    print!("{prompt} [{default}]: ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let value = line.trim();
    Ok(if value.is_empty() {
        default.to_owned()
    } else {
        value.to_owned()
    })
}
