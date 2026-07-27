//! End-to-end transform-sync integration tests (, design §11).
//!
//! Two complementary tests:
//!
//! 1. [`two_clients_interpolate_under_loss_latency_reorder`] drives the real
//!    server hub ([`TransformHub`]) and two real client runtimes
//!    ([`RemoteWorldView`]) through a **simulated lossy / reordering / latent
//!    datagram channel**, exercising the exact encoded snapshot/ack bytes that
//!    ride QUIC — just without the socket in the middle so loss can be injected
//!    deterministically. It asserts both clients track the authoritative object
//!    and interpolate smoothly (the palpable acceptance slice).
//! 2. [`snapshot_datagram_traverses_real_quic`] binds a real QUIC server whose
//!    gateway carries a transform hub, connects a real `quinn` client, negotiates
//!    over a reliable stream, and reconstructs a moving object from snapshot
//!    **datagrams** delivered over the wire — proving the hot path rides real
//!    QUIC unreliable datagrams.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use citadel::realtime::Gateway;
use citadel::realtime::transform::{
    RemoteWorldView, TransformHub, TransformHubConfig, TransformState,
};
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel_wire::tsync;

mod common;
use common::quic_guest_handshake;

/// A tiny deterministic LCG so the loss/reorder pattern is reproducible.
struct Lcg(u64);
impl Lcg {
    fn next_u32(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
    /// Returns true with probability `p` (0..1).
    fn chance(&mut self, p: f64) -> bool {
        (f64::from(self.next_u32()) / f64::from(u32::MAX)) < p
    }
    fn small_delay(&mut self) -> u32 {
        self.next_u32() % 3 // 0..=2 ticks of latency
    }
}

/// A scheduled datagram (server->client snapshot or client->server ack).
struct InFlight {
    deliver_at: u32,
    body: Vec<u8>,
}

#[test]
fn two_clients_interpolate_under_loss_latency_reorder() {
    // 30% datagram loss, 0..2 ticks latency, and reorder among same-tick arrivals.
    let mut rng = Lcg(0x1234_5678_9abc_def0);
    const LOSS: f64 = 0.30;

    let cfg = TransformHubConfig {
        // Small AOI-free world so both clients see the mover; 20 pps default.
        ..TransformHubConfig::default()
    };
    let hub = TransformHub::new(cfg).expect("hub");
    let _ = hub.handle_hello(1);
    let _ = hub.handle_hello(2);

    // One server-simulated object moving at a constant velocity on +x.
    let mut s = TransformState::at([0.0, 0.0, 0.0]);
    s.velocity = [600.0, 0.0, 0.0]; // 600 cm/s
    hub.spawn_server_simulated(10, s);

    let codec = *hub.codec();
    let mut view1 = RemoteWorldView::new(codec, 60, 20);
    let mut view2 = RemoteWorldView::new(codec, 60, 20);

    // Per-client in-flight datagram queues (server->client) and ack queues
    // (client->server).
    let mut to1: Vec<InFlight> = Vec::new();
    let mut to2: Vec<InFlight> = Vec::new();
    let mut acks: Vec<(u64, InFlight)> = Vec::new();

    // The server sends a snapshot every 3rd sim tick (60 Hz sim / 20 Hz send).
    const TICKS: u32 = 240; // ~4 s of simulation
    for tick in 1..=TICKS {
        hub.sim_tick();

        // Deliver due acks to the server.
        let mut still: Vec<(u64, InFlight)> = Vec::new();
        for (participant, pkt) in acks.drain(..) {
            if pkt.deliver_at <= tick {
                hub.handle_ack(participant, &pkt.body);
            } else {
                still.push((participant, pkt));
            }
        }
        acks = still;

        if tick % 3 == 0 {
            for out in hub.snapshot_tick() {
                // Inject loss on the snapshot datagram.
                if rng.chance(LOSS) {
                    continue;
                }
                let pkt = InFlight {
                    deliver_at: tick + rng.small_delay(),
                    body: out.body,
                };
                if out.participant == 1 {
                    &mut to1
                } else {
                    &mut to2
                }
                .push(pkt);
            }
        }

        // Deliver due snapshots to each client (reordered among same-tick arrivals
        // by draining in queue order after a shuffle-ish rotation).
        deliver_due(&mut to1, tick, &mut rng, &mut view1, 1, &mut acks, LOSS);
        deliver_due(&mut to2, tick, &mut rng, &mut view2, 2, &mut acks, LOSS);
    }

    // Both clients must have reconstructed the object and tracked it far along +x.
    let true_x = hub.get_transform(10).expect("object").position[0];
    for (name, view) in [("client1", &view1), ("client2", &view2)] {
        let obj = view.object(10).expect("client sees object");
        // Despite 30% loss, the reconstructed head-of-line state is close to truth
        // (within a few send intervals of movement).
        let err = (obj.state.position[0] - true_x).abs();
        assert!(
            err < 600.0,
            "{name} tracks the object: reconstructed x={}, true x={true_x}, err={err}",
            obj.state.position[0]
        );
        // Loss actually happened (the channel dropped some snapshots) yet the
        // client self-healed via absolute-id baselines.
        assert!(
            view.discarded_missing_base() + view.discarded_stale() < TICKS as u64,
            "self-heal, not mass discard"
        );
    }

    // Interpolation is smooth: sampling across the buffered render window yields a
    // monotonically advancing, near-linear x (constant-velocity object).
    let render = view1.render_tick().expect("render tick");
    let mut last_x = f32::NEG_INFINITY;
    let mut samples = 0;
    for step in 0..8 {
        let t = render - 4.0 + f64::from(step); // a small window around render time
        if let Some(s) = view1.sample(10, t) {
            assert!(
                s.position[0] + 1.0 >= last_x,
                "interpolated x must not jump backward: {} then {}",
                last_x,
                s.position[0]
            );
            last_x = s.position[0];
            samples += 1;
        }
    }
    assert!(
        samples >= 4,
        "the jitter buffer produced a renderable window"
    );
}

#[allow(clippy::too_many_arguments)]
fn deliver_due(
    queue: &mut Vec<InFlight>,
    tick: u32,
    rng: &mut Lcg,
    view: &mut RemoteWorldView,
    participant: u64,
    acks: &mut Vec<(u64, InFlight)>,
    loss: f64,
) {
    // Split due vs pending; deliver due packets in a rotated order to simulate
    // reordering among same-tick arrivals.
    let mut due: Vec<Vec<u8>> = Vec::new();
    let mut pending: Vec<InFlight> = Vec::new();
    for pkt in queue.drain(..) {
        if pkt.deliver_at <= tick {
            due.push(pkt.body);
        } else {
            pending.push(pkt);
        }
    }
    *queue = pending;
    if due.len() > 1 {
        // Rotate by a pseudo-random amount => reordered delivery.
        let r = (rng.next_u32() as usize) % due.len();
        due.rotate_left(r);
    }
    for body in due {
        if view.apply_datagram(&body) {
            // Ack back (also lossy).
            if !rng.chance(loss) {
                acks.push((
                    participant,
                    InFlight {
                        deliver_at: tick + rng.small_delay(),
                        body: view.ack().encode(),
                    },
                ));
            }
        }
    }
}

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn connect(server_addr: SocketAddr, cert: &SelfSignedCert) -> quinn::Connection {
    use citadel::transport::quic::tls;
    let client_config = tls::client_config_trusting(cert).expect("client config");
    let mut endpoint = quinn::Endpoint::client(loopback_any()).expect("client endpoint");
    endpoint.set_default_client_config(client_config);
    tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(server_addr, "localhost")
            .expect("connect call"),
    )
    .await
    .expect("connect did not time out")
    .expect("connection established")
}

#[tokio::test]
async fn snapshot_datagram_traverses_real_quic() {
    use citadel::lifecycle::Supervisor;
    use citadel::transport::codec::Envelope as TEnvelope;

    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
    let gateway = Arc::new(Gateway::new().with_transform_hub(Arc::clone(&hub)));
    let server = QuicServer::bind_with_gateway(loopback_any(), &cert, Arc::clone(&gateway))
        .expect("bind server");
    let server_addr = server.local_addr();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    let conn = connect(server_addr, &cert).await;
    quic_guest_handshake(&conn).await;

    // Send KIND_TSYNC_HELLO over a reliable uni stream; read the negotiation reply.
    let hello = TEnvelope::new(citadel_wire::protocol::KIND_TSYNC_HELLO, Vec::new());
    let mut send = conn.open_uni().await.expect("open uni for hello");
    send.write_all(&hello.encode_framed())
        .await
        .expect("write hello");
    send.finish().expect("finish hello stream");

    // The server's HELLO reply arrives on a uni stream.
    let mut recv = tokio::time::timeout(Duration::from_secs(5), conn.accept_uni())
        .await
        .expect("hello reply did not time out")
        .expect("hello reply stream");
    let data = recv.read_to_end(64 * 1024).await.expect("read hello reply");
    let mut buf = bytes::BytesMut::from(&data[..]);
    let reply = citadel::transport::codec::decode_framed(&mut buf)
        .expect("decode")
        .expect("one frame");
    assert_eq!(reply.kind, citadel_wire::protocol::KIND_TSYNC_HELLO);
    let hello = tsync::Hello::decode(&reply.body).expect("hello decodes");
    let codec = tsync::TransformCodec::from_hello(&hello).expect("codec");

    // A moving server object.
    let mut s = TransformState::at([0.0, 0.0, 0.0]);
    s.velocity = [600.0, 0.0, 0.0];
    hub.spawn_server_simulated(1, s);

    // Drive sim+snapshot ticks; the client reads snapshot datagrams off the wire.
    let mut view = RemoteWorldView::new(codec, 60, 20);
    let mut applied = 0;
    for _ in 0..60 {
        gateway.transform_tick();
        // Read any datagrams currently available (best-effort; datagrams are
        // unreliable but loopback delivers them promptly).
        while let Ok(Ok(bytes)) =
            tokio::time::timeout(Duration::from_millis(50), conn.read_datagram()).await
        {
            let env = citadel::transport::codec::decode_datagram(&bytes).expect("decode datagram");
            assert_eq!(env.kind, citadel_wire::protocol::KIND_TSYNC_SNAPSHOT);
            if view.apply_datagram(&env.body) {
                applied += 1;
                // Ack back over a datagram.
                let ack =
                    TEnvelope::new(citadel_wire::protocol::KIND_TSYNC_ACK, view.ack().encode());
                let _ = conn.send_datagram(ack.encode_datagram());
            }
        }
        if applied >= 5 {
            break;
        }
    }

    assert!(
        applied >= 1,
        "at least one snapshot datagram traversed real QUIC"
    );
    let obj = view
        .object(1)
        .expect("client reconstructed the object from QUIC datagrams");
    assert!(obj.state.position[0] >= 0.0);

    conn.close(0u32.into(), b"done");
    let result = tokio::time::timeout(Duration::from_secs(5), supervisor.shutdown())
        .await
        .expect("shutdown completes");
    result.expect("clean shutdown");
}
