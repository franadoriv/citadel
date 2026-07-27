//! Adaptive congestion control (design §6.5, §13.7,  P2-c).
//!
//! **QUIC owns byte-level pacing and the congestion window; Citadel must not run
//! a second congestion controller that fights it.** So the application layer
//! adapts *what it puts in the byte budget*, not the byte rate itself:
//!
//! - The primary knob is the **per-client bandwidth budget + object priority**
//!   ([`CongestionController::budget`]): under pressure, send fewer/lower-priority
//!   objects per snapshot. QUIC still decides when the bytes actually leave.
//! - The snapshot **send rate** steps between a good rate (20-30) and a floor
//!   (10) pps as a coarse app signal, driven by **composite datagram-loss /
//!   ack-age / jitter / send-queue-drop signals (with QUIC path RTT as *one*
//!   input, never a bare RTT threshold)**, with **hysteresis** so it changes
//!   slowly and does not flap (design §6.5, Gaffer two-mode adapted).
//! - Interpolation-delay changes are slow (the client ramps its buffer gradually
//!   from the broadcast send rate) so a rate step never jerks the rewind time.

/// Which coarse send-rate mode the controller is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendMode {
    /// Healthy link: full send rate + full object budget.
    Good,
    /// Pressured link: floor send rate + reduced object budget.
    Floor,
}

/// The signals the controller reacts to (design §6.5). All are app/QUIC-observed,
/// **not** a bare RTT threshold: RTT is only one input.
#[derive(Debug, Clone, Copy, Default)]
pub struct CongestionSignals {
    /// Recent datagram-loss fraction in `[0, 1]`.
    pub datagram_loss: f32,
    /// How stale the newest snapshot ack is, in sim ticks.
    pub ack_age_ticks: f64,
    /// Recent one-way jitter in milliseconds.
    pub jitter_ms: f64,
    /// Send-queue drops observed since the last observation.
    pub send_queue_drops: u32,
    /// QUIC path RTT in milliseconds (one input among several).
    pub path_rtt_ms: f64,
}

/// Static config for [`CongestionController`].
#[derive(Debug, Clone, Copy)]
pub struct CongestionConfig {
    /// Good send rate (pps), 20-30.
    pub good_rate_hz: u8,
    /// Floor send rate (pps), ~10.
    pub floor_rate_hz: u8,
    /// Object budget per snapshot in `Good` mode (`0` = unbounded).
    pub good_budget: usize,
    /// Object budget per snapshot in `Floor` mode.
    pub floor_budget: usize,
    /// Datagram-loss fraction above which the link is "bad".
    pub loss_bad: f32,
    /// Ack-age (ticks) above which the link is "bad".
    pub ack_age_bad_ticks: f64,
    /// Jitter (ms) above which the link is "bad".
    pub jitter_bad_ms: f64,
    /// Path RTT (ms) above which the link is "bad" (one input, not the sole gate).
    pub rtt_bad_ms: f64,
    /// Seconds of sustained badness required to step Good -> Floor (debounce).
    pub enter_floor_secs: f64,
    /// Base seconds of sustained goodness required to recover Floor -> Good; this
    /// value doubles on a quick re-flap (capped at [`MAX_RECOVER_SECS`]) and
    /// halves after a long stable Good stretch.
    pub base_recover_secs: f64,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            good_rate_hz: 20,
            floor_rate_hz: 10,
            good_budget: 0,
            floor_budget: 16,
            loss_bad: 0.05,
            ack_age_bad_ticks: 30.0,
            jitter_bad_ms: 40.0,
            rtt_bad_ms: 200.0,
            enter_floor_secs: 1.0,
            base_recover_secs: 4.0,
        }
    }
}

/// The upper cap on the Good->Bad recovery wait (design §6.5: "≤60 s").
const MAX_RECOVER_SECS: f64 = 60.0;
/// The lower floor on the recovery wait after halving.
const MIN_RECOVER_SECS: f64 = 1.0;
/// Continuous good time (s) in Good mode that "rewards" a halved recover wait
/// (design §6.5: "halve after ≥1 s stable").
const REWARD_STABLE_SECS: f64 = 1.0;

/// A per-client adaptive congestion controller (design §6.5).
///
/// Feed it observations with [`observe`](CongestionController::observe); read the
/// coarse send rate with [`send_rate_hz`](CongestionController::send_rate_hz) and
/// the object budget with [`budget`](CongestionController::budget). It never
/// throttles the byte rate — QUIC does that.
#[derive(Debug, Clone)]
pub struct CongestionController {
    config: CongestionConfig,
    mode: SendMode,
    /// Continuous seconds the signals have been bad.
    bad_secs: f64,
    /// Continuous seconds the signals have been good.
    good_secs: f64,
    /// Current recovery wait (doubles on flap, halves when stable).
    recover_secs: f64,
}

impl CongestionController {
    /// A new controller in `Good` mode.
    #[must_use]
    pub fn new(config: CongestionConfig) -> Self {
        let recover_secs = config
            .base_recover_secs
            .clamp(MIN_RECOVER_SECS, MAX_RECOVER_SECS);
        Self {
            config,
            mode: SendMode::Good,
            bad_secs: 0.0,
            good_secs: 0.0,
            recover_secs,
        }
    }

    /// The current coarse send rate (pps): good rate in `Good`, floor in `Floor`.
    #[must_use]
    pub fn send_rate_hz(&self) -> u8 {
        match self.mode {
            SendMode::Good => self.config.good_rate_hz,
            SendMode::Floor => self.config.floor_rate_hz,
        }
    }

    /// The current per-snapshot object budget (fewer objects under pressure).
    #[must_use]
    pub fn budget(&self) -> usize {
        match self.mode {
            SendMode::Good => self.config.good_budget,
            SendMode::Floor => self.config.floor_budget,
        }
    }

    /// The current mode.
    #[must_use]
    pub fn mode(&self) -> SendMode {
        self.mode
    }

    /// The current recovery wait in seconds (observability/tests).
    #[must_use]
    pub fn recover_secs(&self) -> f64 {
        self.recover_secs
    }

    /// Whether `signals` indicate a bad link. **Composite, and never a bare RTT
    /// threshold** (design §6.5, §13.7; review): a bad verdict is driven by
    /// the delivery-quality signals — datagram loss / ack-age / jitter / send-queue
    /// drops. QUIC path RTT is only a **corroborating** input: a high RTT counts
    /// alongside at least one *soft* delivery-quality signal (above half its
    /// threshold), so it can escalate/sustain a bad verdict but can **never trip
    /// the mode on its own** — throttling purely on RTT would fight QUIC's own
    /// RTT/loss control loop and oscillate.
    #[must_use]
    fn is_bad(&self, s: &CongestionSignals) -> bool {
        let c = &self.config;
        let loss_bad = s.datagram_loss.is_finite() && s.datagram_loss > c.loss_bad;
        let ack_bad = s.ack_age_ticks.is_finite() && s.ack_age_ticks > c.ack_age_bad_ticks;
        let jitter_bad = s.jitter_ms.is_finite() && s.jitter_ms > c.jitter_bad_ms;
        let queue_bad = s.send_queue_drops > 0;
        // Any delivery-quality signal over its threshold is a bad link.
        if loss_bad || ack_bad || jitter_bad || queue_bad {
            return true;
        }
        // Otherwise a high RTT trips only when a delivery-quality signal is at
        // least *soft* (half its threshold): RTT corroborates, never gates alone.
        let rtt_high = s.path_rtt_ms.is_finite() && s.path_rtt_ms > c.rtt_bad_ms;
        let soft = (s.datagram_loss.is_finite() && s.datagram_loss > c.loss_bad * 0.5)
            || (s.ack_age_ticks.is_finite() && s.ack_age_ticks > c.ack_age_bad_ticks * 0.5)
            || (s.jitter_ms.is_finite() && s.jitter_ms > c.jitter_bad_ms * 0.5);
        rtt_high && soft
    }

    /// Observe `signals` over `dt_secs` and update the mode with hysteresis.
    ///
    /// Good -> Floor requires badness sustained for `enter_floor_secs` (so a
    /// single bad sample cannot flap the rate). Floor -> Good requires goodness
    /// sustained for `recover_secs`; a quick re-flap doubles that wait (capped at
    /// 60 s), and a long stable Good stretch halves it (floor 1 s).
    pub fn observe(&mut self, signals: &CongestionSignals, dt_secs: f64) {
        let dt = if dt_secs.is_finite() && dt_secs > 0.0 {
            dt_secs
        } else {
            0.0
        };
        let bad = self.is_bad(signals);
        if bad {
            self.bad_secs += dt;
            self.good_secs = 0.0;
        } else {
            self.good_secs += dt;
            self.bad_secs = 0.0;
        }

        match self.mode {
            SendMode::Good => {
                if bad && self.bad_secs >= self.config.enter_floor_secs {
                    // Step down. A *quick* prior flap (we had not yet earned the
                    // reward window of stable good time) doubles the recover wait.
                    if self.good_secs < REWARD_STABLE_SECS {
                        self.recover_secs = (self.recover_secs * 2.0).min(MAX_RECOVER_SECS);
                    }
                    self.mode = SendMode::Floor;
                    self.bad_secs = 0.0;
                    self.good_secs = 0.0;
                } else if !bad && self.good_secs >= REWARD_STABLE_SECS {
                    // Rewarded for stability: relax the recover wait toward the base.
                    self.recover_secs = (self.recover_secs * 0.5).max(MIN_RECOVER_SECS);
                    self.good_secs = 0.0;
                }
            }
            SendMode::Floor => {
                if !bad && self.good_secs >= self.recover_secs {
                    self.mode = SendMode::Good;
                    self.good_secs = 0.0;
                    self.bad_secs = 0.0;
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn bad() -> CongestionSignals {
        CongestionSignals {
            datagram_loss: 0.2,
            ..Default::default()
        }
    }

    fn good() -> CongestionSignals {
        CongestionSignals::default()
    }

    #[test]
    fn stays_good_under_a_single_bad_blip() {
        let mut c = CongestionController::new(CongestionConfig::default());
        // A brief blip below the debounce does not flip the rate.
        c.observe(&bad(), 0.2);
        assert_eq!(c.mode(), SendMode::Good);
        assert_eq!(c.send_rate_hz(), 20);
    }

    #[test]
    fn sustained_bad_steps_to_floor_then_recovers() {
        let mut c = CongestionController::new(CongestionConfig::default());
        // Sustained badness beyond enter_floor_secs (1 s) steps down.
        c.observe(&bad(), 0.6);
        c.observe(&bad(), 0.6);
        assert_eq!(c.mode(), SendMode::Floor);
        assert_eq!(c.send_rate_hz(), 10);
        assert!(c.budget() > 0, "floor budget is reduced but non-zero");

        // Not yet recovered: needs sustained good for recover_secs.
        c.observe(&good(), 1.0);
        assert_eq!(c.mode(), SendMode::Floor);
        // Enough continuous good time recovers to Good.
        c.observe(&good(), 10.0);
        assert_eq!(c.mode(), SendMode::Good);
        assert_eq!(c.send_rate_hz(), 20);
    }

    #[test]
    fn quick_reflap_doubles_recover_wait() {
        let mut c = CongestionController::new(CongestionConfig {
            enter_floor_secs: 0.5,
            base_recover_secs: 4.0,
            ..CongestionConfig::default()
        });
        let before = c.recover_secs();
        // Step to Floor (sustained bad), recover, then immediately re-flap.
        c.observe(&bad(), 1.0);
        assert_eq!(c.mode(), SendMode::Floor);
        c.observe(&good(), 100.0); // recover to Good
        assert_eq!(c.mode(), SendMode::Good);
        // Immediate re-flap (no stable reward window earned) doubles the wait.
        c.observe(&bad(), 1.0);
        assert_eq!(c.mode(), SendMode::Floor);
        assert!(
            c.recover_secs() > before,
            "recover wait grew: {} -> {}",
            before,
            c.recover_secs()
        );
        assert!(c.recover_secs() <= MAX_RECOVER_SECS);
    }

    #[test]
    fn composite_signals_not_bare_rtt() {
        let mut c = CongestionController::new(CongestionConfig::default());
        // High jitter alone (RTT fine) still trips badness -> a delivery-quality
        // signal, not a bare-RTT gate.
        let jittery = CongestionSignals {
            jitter_ms: 100.0,
            path_rtt_ms: 20.0,
            ..Default::default()
        };
        c.observe(&jittery, 2.0);
        assert_eq!(c.mode(), SendMode::Floor);
    }

    #[test]
    fn bare_high_rtt_alone_never_trips() {
        // A sustained high RTT with NO delivery-quality pressure must NOT step the
        // rate/budget — that would fight QUIC's own RTT loop (design §6.5, §13.7).
        let mut c = CongestionController::new(CongestionConfig::default());
        let rtt_only = CongestionSignals {
            path_rtt_ms: 500.0, // way over rtt_bad_ms
            ..Default::default()
        };
        for _ in 0..10 {
            c.observe(&rtt_only, 10.0);
        }
        assert_eq!(c.mode(), SendMode::Good, "bare RTT never gates the mode");

        // But a high RTT CORROBORATED by soft loss (below the hard threshold) does
        // trip it — RTT is a real input, just never the sole gate.
        let mut c2 = CongestionController::new(CongestionConfig::default());
        let rtt_plus_soft = CongestionSignals {
            path_rtt_ms: 500.0,
            datagram_loss: 0.03, // > loss_bad*0.5 (0.025) but < loss_bad (0.05)
            ..Default::default()
        };
        c2.observe(&rtt_plus_soft, 2.0);
        assert_eq!(c2.mode(), SendMode::Floor, "RTT + soft pressure trips");
    }
}
