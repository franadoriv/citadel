//! End-to-end transform-sync P2 integration tests (, design §5, §11).
//!
//! These drive the real server hub ([`TransformHub`]) plus the client-side
//! prediction reference ([`PredictionRing`]) and remote view
//! ([`RemoteWorldView`]) through a **simulated lossy/latent datagram channel**,
//! exercising the exact encoded owner-input / snapshot / ack / rewind bytes that
//! ride QUIC — without a socket so loss is injected deterministically. They cover
//! the palpable acceptance slice: an owning client feels input-latency-free and
//! reconciles without a visible snap-back, and a shot registers against the
//! rewound position the shooter saw (favor-the-shooter), off above the RTT cutoff.

use citadel::realtime::transform::{
    LagProfile, PredictionRing, ReconcileConfig, RemoteWorldView, SyncRole, TransformAuthority,
    TransformHub, TransformHubConfig, TransformState,
};
use citadel_wire::tsync::{FireCommand, InputBundle, InputFrame, RewindResult};

/// A tiny deterministic LCG so any loss pattern is reproducible.
struct Lcg(u64);
impl Lcg {
    fn next_u32(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
    fn drops(&mut self, pct: u32) -> bool {
        self.next_u32() % 100 < pct
    }
}

fn hub() -> TransformHub {
    TransformHub::new(TransformHubConfig::default()).expect("hub")
}

/// The owning client predicts locally, sends redundant inputs over a lossy
/// channel, and reconciles against the server ack — ending in agreement with the
/// server with no residual visual snap, even when input packets are dropped.
#[test]
fn owner_predicts_and_reconciles_without_snap_back_under_loss() {
    let hub = hub();
    let participant = 1u64;
    let _ = hub.handle_hello(participant);

    // Spawn an object owned + predicted by our client.
    hub.spawn(TransformAuthority::new(
        10,
        SyncRole::ServerSimulated,
        TransformState::default(),
    ));
    let role = hub.assign_owner(10, participant).expect("owner");
    let epoch = role.ownership_epoch;

    let codec = *hub.codec();
    let mut view = RemoteWorldView::new(codec, 60, 20);
    let mut ring = PredictionRing::new(TransformState::default(), ReconcileConfig::default());
    let mut rng = Lcg(0xC0FFEE);

    // Drive 60 owner ticks: predict a constant +x movement, and (like a real
    // reconciling client) bundle every *unacked* input redundantly so a lost
    // packet self-heals via the next one, drop ~25% of input packets, and
    // periodically snapshot back + reconcile. `sent` retains all frames; the
    // bundle is the unacked tail (bounded by MAX_INPUT_FRAMES).
    let mut sent: Vec<InputFrame> = Vec::new();
    let bundle_unacked = |sent: &[InputFrame], acked: u32| -> Vec<InputFrame> {
        let tail: Vec<InputFrame> = sent
            .iter()
            .filter(|f| f.input_seq > acked)
            .cloned()
            .collect();
        let start = tail.len().saturating_sub(32); // MAX_INPUT_FRAMES
        tail[start..].to_vec()
    };
    for tick in 0..60u32 {
        let seq = ring.push_input([300.0, 0.0, 0.0], 1.0 / 60.0);
        sent.push(InputFrame {
            input_seq: seq,
            sim_tick: tick,
            dt: 1.0 / 60.0,
            object_id: 10,
            ownership_epoch: epoch,
            move_velocity: [300.0, 0.0, 0.0],
            payload: vec![],
            fire: None,
        });
        let acked = view.owner_ack(10).unwrap_or(0);
        if !rng.drops(25) {
            let bundle = InputBundle {
                acked_snapshot_id: view.ack().acked_snapshot_id,
                last_seen_snapshot_id: view.ack().acked_snapshot_id,
                frames: bundle_unacked(&sent, acked),
            };
            let _ = hub.handle_input(participant, &bundle.encode());
        }

        // Every 3 ticks the server sends a snapshot; the owner reconciles.
        if tick % 3 == 0 {
            hub.sim_tick();
            for out in hub.snapshot_tick() {
                if out.participant == participant {
                    assert!(view.apply_datagram(&out.body));
                }
            }
            if let (Some(ack), Some(auth)) = (view.owner_ack(10), view.authoritative_state(10)) {
                ring.reconcile(auth, ack);
            }
            ring.advance_smoothing();
        }
    }

    // Flush: resend the unacked tail (loss-free) until the server catches up,
    // then reconcile. The client must converge to the server position with a tiny
    // residual visual offset (no snap-back).
    for _ in 0..12 {
        let acked = view.owner_ack(10).unwrap_or(0);
        let bundle = InputBundle {
            acked_snapshot_id: view.ack().acked_snapshot_id,
            last_seen_snapshot_id: view.ack().acked_snapshot_id,
            frames: bundle_unacked(&sent, acked),
        };
        let _ = hub.handle_input(participant, &bundle.encode());
        hub.sim_tick();
        for out in hub.snapshot_tick() {
            if out.participant == participant {
                view.apply_datagram(&out.body);
            }
        }
        if let (Some(ack), Some(auth)) = (view.owner_ack(10), view.authoritative_state(10)) {
            ring.reconcile(auth, ack);
        }
        ring.advance_smoothing();
    }

    let server_x = hub.get_transform(10).expect("object").position[0];
    let sim_x = ring.predicted_state().position[0];
    // The owner advanced far (input-latency-free prediction moved it immediately).
    assert!(
        server_x > 100.0,
        "server advanced the owned object: {server_x}"
    );
    // Sim state agrees with the server (reconciliation converged).
    assert!(
        (sim_x - server_x).abs() < 5.0,
        "predicted sim {sim_x} tracks server {server_x}"
    );
    // No residual visual snap: the rendered offset has smoothed to ~zero.
    let offset = ring.visual_offset();
    let mag = (offset[0] * offset[0] + offset[1] * offset[1] + offset[2] * offset[2]).sqrt();
    assert!(mag < 5.0, "no lingering snap-back: |offset|={mag}");
}

/// A shot registers against the position the shooter saw (favor-the-shooter),
/// and disables above the RTT cutoff — end to end through encoded input/rewind
/// bytes. The client never resolves the hit; it only reads the authoritative
/// [`RewindResult`].
#[test]
fn favor_the_shooter_hit_registers_over_encoded_frames() {
    let hub = hub();
    let shooter = 1u64;
    let _ = hub.handle_hello(shooter);

    // Shooter object at the origin.
    hub.spawn(TransformAuthority::new(
        1,
        SyncRole::ServerSimulated,
        TransformState::at([0.0, 0.0, 0.0]),
    ));
    let role = hub.assign_owner(1, shooter).expect("owner");
    let epoch = role.ownership_epoch;

    // A hit-eligible target that starts on the +x ray and drifts off it on +y.
    let mut target = TransformState::at([100.0, 0.0, 0.0]);
    target.velocity = [0.0, 300.0, 0.0];
    hub.spawn_server_simulated(2, target);
    hub.set_hit_eligible(2, true);
    for _ in 0..40 {
        hub.sim_tick();
    }

    let fire_bundle = |seq: u32| {
        InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![InputFrame {
                input_seq: seq,
                sim_tick: 0,
                dt: 0.0,
                object_id: 1,
                ownership_epoch: epoch,
                move_velocity: [0.0; 3],
                payload: vec![],
                fire: Some(FireCommand {
                    origin: [0.0, 0.0, 0.0],
                    direction: [1.0, 0.0, 0.0],
                }),
            }],
        }
        .encode()
    };

    // Below the cutoff: rewind into the past where the target was on the ray.
    hub.set_lag_profile(
        shooter,
        LagProfile {
            owd_ticks: 20.0,
            interp_delay_ticks: 18.0,
            rtt_ms: 120.0,
        },
    );
    let replies = hub.handle_input(shooter, &fire_bundle(1));
    assert_eq!(replies.len(), 1);
    let hit = RewindResult::decode(&replies[0].body).expect("decode");
    assert!(hit.hit, "favor-the-shooter hit");
    assert_eq!(hit.object_id, 2);

    // A redundant resend of the same fire seq must NOT re-resolve (exactly once).
    let dup = hub.handle_input(shooter, &fire_bundle(1));
    assert!(dup.is_empty(), "duplicate fire seq does not re-resolve");

    // Above the cutoff: lag comp disables, the shot resolves at present => miss.
    hub.set_lag_profile(
        shooter,
        LagProfile {
            owd_ticks: 20.0,
            interp_delay_ticks: 18.0,
            rtt_ms: 400.0,
        },
    );
    let replies2 = hub.handle_input(shooter, &fire_bundle(2));
    assert_eq!(replies2.len(), 1);
    let miss = RewindResult::decode(&replies2[0].body).expect("decode");
    assert!(
        !miss.hit,
        "above the RTT cutoff resolves at present => miss"
    );
}
