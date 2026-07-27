//! Simulador de combate: un solo exe que levanta N bots contra un servidor
//! Citadel real (WebSocket), cada uno en su propia task de tokio.
//!
//! Cada bot: se conecta, hace el handshake guest, aprende su propio id
//! (KIND_WELCOME), descubre peers por sus posiciones relayadas, y cada ~1.2 s le
//! pega a un peer al azar (KIND_HIT). Imprime los eventos autoritativos que el
//! servidor emite: HEALTH / DEATH / RESPAWN. Empareja los kinds y layouts de
//! `bin/server/scripts/main.lua`.
//!
//! Arranca el servidor (`cd bin/server && cargo run` en el repo, o el exe
//! standalone) y luego:
//!
//! ```text
//! cargo run -p citadel-client --example combat_sim -- 4
//! cargo run -p citadel-client --example combat_sim -- 4 ws://127.0.0.1:7352/
//! ```

use std::time::{Duration, Instant};

use citadel_client::{ClientError, ClientResult, Envelope, WsClient};
use citadel_wire::protocol::{
    KIND_AUTH, KIND_AUTH_RESULT, KIND_PEER_POSITION, KIND_POSITION, split_sender,
};

// Kinds de combate (deben coincidir con main.lua). >=100 evita el rango
// reservado del netcode (7..25).
const KIND_HIT: u16 = 100;
const KIND_HEALTH: u16 = 101;
const KIND_DEATH: u16 = 102;
const KIND_RESPAWN: u16 = 103;
const KIND_WELCOME: u16 = 104;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let bots: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(3);
    let url = args
        .next()
        .unwrap_or_else(|| "ws://127.0.0.1:7352/".to_string());

    println!("Levantando {bots} bots contra {url}");
    let mut handles = Vec::with_capacity(bots);
    for idx in 0..bots {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_bot(idx, &url).await {
                eprintln!("[bot {idx}] error: {e}");
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Un bot: conecta, hace handshake, y corre el bucle de simulación hasta que el
/// servidor cierra o falla la conexión.
async fn run_bot(idx: usize, url: &str) -> ClientResult<()> {
    let mut client = WsClient::connect(url).await?;

    // Handshake guest: KIND_AUTH vacío, y drenamos el KIND_AUTH_RESULT que nos
    // registra en el gateway (esto dispara on_join en el servidor).
    client.send(&Envelope::new(KIND_AUTH, Vec::new())).await?;
    loop {
        match client.recv().await? {
            Some(env) if env.kind == KIND_AUTH_RESULT => break,
            Some(_) => continue,
            None => {
                return Err(ClientError::Receive(
                    "el servidor cerró antes del ack de auth".to_string(),
                ));
            }
        }
    }
    println!("[bot {idx}] conectado");

    let mut rng = Rng::new(idx as u64 + 1);
    let mut my_id: Option<u64> = None;
    let mut peers: Vec<u64> = Vec::new();
    let mut last_pos = Instant::now();
    let mut last_hit = Instant::now();

    loop {
        // Recibe con timeout corto: si no llega nada, hacemos las acciones
        // periódicas. Así send y recv se serializan en un solo &mut client.
        match tokio::time::timeout(Duration::from_millis(150), client.recv()).await {
            Ok(Ok(Some(env))) => handle_env(idx, &env, &mut my_id, &mut peers),
            Ok(Ok(None)) => {
                println!("[bot {idx}] el servidor cerró la conexión");
                return Ok(());
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {} // timeout: seguimos a las acciones periódicas
        }

        // Manda posición cada 500 ms para que los peers aprendan nuestro id.
        if last_pos.elapsed() >= Duration::from_millis(500) {
            last_pos = Instant::now();
            client
                .send(&Envelope::new(KIND_POSITION, dummy_position(&mut rng)))
                .await?;
        }

        // Pega a un peer al azar cada ~1.2 s.
        if last_hit.elapsed() >= Duration::from_millis(1200) && !peers.is_empty() {
            last_hit = Instant::now();
            let target = peers[(rng.next() as usize) % peers.len()];
            let damage: f32 = 20.0 + (rng.next() % 15) as f32; // 20..34
            let mut body = Vec::with_capacity(12);
            body.extend_from_slice(&target.to_be_bytes());
            body.extend_from_slice(&damage.to_be_bytes());
            client.send(&Envelope::new(KIND_HIT, body)).await?;
            println!("[bot {idx}] -> HIT a {target} por {damage:.0}");
        }
    }
}

/// Despacha un envelope entrante actualizando el estado local del bot.
fn handle_env(idx: usize, env: &Envelope, my_id: &mut Option<u64>, peers: &mut Vec<u64>) {
    match env.kind {
        KIND_WELCOME => {
            if let Some(id) = read_u64(&env.body) {
                *my_id = Some(id);
                println!("[bot {idx}] mi id = {id}");
            }
        }
        // Posición relayada: el prefijo es el id del peer. Así los descubrimos.
        KIND_PEER_POSITION => {
            if let Some((sender, _)) = split_sender(&env.body) {
                remember(peers, sender, my_id);
            }
        }
        // HEALTH: who(u64) + hp(f32) + max(f32).
        KIND_HEALTH => {
            if let Some((who, rest)) = split_sender(&env.body)
                && let (Some(hp), Some(max)) = (read_f32(rest, 0), read_f32(rest, 4))
            {
                remember(peers, who, my_id);
                let tag = if *my_id == Some(who) { " (yo)" } else { "" };
                println!("[bot {idx}] HEALTH{tag}: {who} = {hp:.0}/{max:.0}");
            }
        }
        // DEATH: victim(u64) + killer(u64).
        KIND_DEATH => {
            if let (Some(victim), Some(killer)) =
                (read_u64_at(&env.body, 0), read_u64_at(&env.body, 8))
            {
                let tag = if *my_id == Some(victim) {
                    " (¡yo morí!)"
                } else {
                    ""
                };
                println!("[bot {idx}] DEATH{tag}: {killer} mató a {victim}");
            }
        }
        // RESPAWN: who(u64).
        KIND_RESPAWN => {
            if let Some(who) = read_u64(&env.body) {
                let tag = if *my_id == Some(who) { " (yo)" } else { "" };
                println!("[bot {idx}] RESPAWN{tag}: {who}");
            }
        }
        _ => {}
    }
}

/// Recuerda un peer, salvo que seamos nosotros mismos.
fn remember(peers: &mut Vec<u64>, id: u64, my_id: &Option<u64>) {
    if *my_id != Some(id) && !peers.contains(&id) {
        peers.push(id);
    }
}

/// Lee un u64 big-endian de los primeros 8 bytes de `b`.
fn read_u64(b: &[u8]) -> Option<u64> {
    read_u64_at(b, 0)
}

/// Lee un u64 big-endian en el offset `off` de `b`.
fn read_u64_at(b: &[u8], off: usize) -> Option<u64> {
    b.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_be_bytes)
}

/// Lee un f32 big-endian en el offset `off` de `b`.
fn read_f32(b: &[u8], off: usize) -> Option<f32> {
    b.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .map(f32::from_be_bytes)
}

/// Posición ficticia (3x f32 big-endian) solo para alimentar el relay.
fn dummy_position(rng: &mut Rng) -> Vec<u8> {
    let mut v = Vec::with_capacity(12);
    for _ in 0..3 {
        let f = (rng.next() % 1000) as f32;
        v.extend_from_slice(&f.to_be_bytes());
    }
    v
}

/// PRNG xorshift64 minimalista (evita añadir una dependencia solo para el demo).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Semilla distinta por bot; nunca cero.
        Self((seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
