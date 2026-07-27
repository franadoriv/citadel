//! Transform-sync P3 hardening evidence.
//!
//! These deterministic tests keep the operational limits executable: thousands
//! of objects in a uniform AOI grid, a conservative QUIC-datagram payload budget,
//! and the latency consequence of loss-tolerant datagrams versus an ordered
//! reliable stream. The latency model intentionally measures delivery semantics,
//! not a second congestion controller: Quinn/QUIC owns pacing in production.

use citadel::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
use citadel_wire::interest::{InterestGrid, RelevanceSet};

const SAFE_DATAGRAM_BYTES: usize = 1_200;

#[test]
fn aoi_grid_handles_four_thousand_entities_without_global_fanout() {
    let mut grid = InterestGrid::new(100.0);
    for y in 0..64u64 {
        for x in 0..64u64 {
            let id = y * 64 + x + 1;
            grid.insert_or_move(id, [x as f32 * 100.0, y as f32 * 100.0, 0.0]);
        }
    }
    assert_eq!(grid.len(), 4_096);

    let viewer = [3_200.0, 3_200.0, 0.0];
    let candidates = grid.candidates_for(viewer);
    assert!(
        candidates.len() <= 9,
        "the 3x3 broad phase, not all 4096 entities, reaches the precise check"
    );
    let mut relevance = RelevanceSet::new();
    let delta = relevance.update(&grid, viewer, 175.0, 225.0);
    assert!(!delta.entered.is_empty());
    assert!(delta.entered.len() <= 9);
}

#[test]
fn snapshot_budget_stays_under_the_conservative_mtu_payload() {
    let hub = TransformHub::new(TransformHubConfig {
        budget: 16,
        ..TransformHubConfig::default()
    })
    .expect("valid hub");
    let _ = hub.handle_hello(1);
    for id in 1..=128 {
        hub.spawn_server_simulated(id, TransformState::at([id as f32 * 10.0, 0.0, 0.0]));
    }
    hub.sim_tick();
    let snapshots = hub.snapshot_tick();
    assert_eq!(snapshots.len(), 1);
    let snapshot = &snapshots[0];
    // This is an envelope body. The QUIC datagram adds a two-byte kind, leaving
    // headroom below the common 1200-byte safe UDP payload budget.
    assert!(
        snapshot.body.len() + 2 <= SAFE_DATAGRAM_BYTES,
        "{} bytes exceeds the {} byte budget",
        snapshot.body.len() + 2,
        SAFE_DATAGRAM_BYTES
    );
}

#[test]
fn loss_model_records_lower_datagram_tail_than_ordered_reliable_stream() {
    // 5% deterministic loss, one-tick nominal delivery, six-tick recovery. A
    // datagram loss drops only that stale snapshot; an ordered reliable stream
    // holds later state behind the retransmission. This is the benchmark harness
    // used to pin the operational default, not a substitute for QUIC pacing.
    let mut datagram_delays = Vec::new();
    let mut reliable_delays = Vec::new();
    let mut stream_available_at = 0u32;
    for sent_at in 0..10_000u32 {
        let lost = sent_at % 20 == 0;
        if !lost {
            datagram_delays.push(1u32);
        }
        let arrival = (sent_at + 1).max(stream_available_at);
        stream_available_at = if lost { arrival + 6 } else { arrival };
        reliable_delays.push(stream_available_at - sent_at);
    }
    let datagram_p99 = percentile_99(&mut datagram_delays);
    let reliable_p99 = percentile_99(&mut reliable_delays);
    assert_eq!(datagram_p99, 1);
    assert!(
        reliable_p99 >= 6,
        "ordered recovery must expose head-of-line tail latency"
    );
}

fn percentile_99(samples: &mut [u32]) -> u32 {
    samples.sort_unstable();
    let index = ((samples.len() - 1) as f64 * 0.99).ceil() as usize;
    samples[index]
}
