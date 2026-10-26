//! Watchdog module - Fault Tolerance & Degraded Mode (Component E)
//!
//! # Responsibilities
//!
//! ## 1. Network Timeout Detection
//! Reads `LAST_PACKET_TIMESTAMP` (written by `ingestion.rs`) every second.
//! If more than 10 seconds have elapsed without a packet, it signals the
//! ingestion layer to reconnect by setting `NETWORK_RESET_REQUESTED`.
//!
//! ## 2. Degraded Mode
//! Monitors `P99_NS` (written by `scheduling.rs`).
//! - If p99 > 2 ms for **10 consecutive watchdog ticks** (≈10 seconds),
//!   `pipeline::DEGRADED_MODE` is set to `true`.
//!   Workers then skip ALL bot edits to reduce load.
//! - Recovery: when p99 drops below 1.5 ms, normal mode resumes.
//!
//! # Thread Safety
//! The watchdog runs on its own OS thread and only touches atomics — no
//! Mutex is acquired in the watchdog loop, so it can never deadlock.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::pipeline::DEGRADED_MODE;
use crate::scheduling::{P99_NS, MISSED_DEADLINES};
use crate::scheduling::SchedulerHandle;

// ============================================================================
// Constants
// ============================================================================

/// How often the watchdog wakes (1 second).
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);

/// No-data timeout before triggering a network reset (10 seconds).
const NETWORK_TIMEOUT_SECS: u64 = 10;

/// How many consecutive over-deadline ticks before entering degraded mode.
const DEGRADED_MODE_THRESHOLD_TICKS: u32 = 10;

/// p99 threshold (ns) for entering degraded mode (2 ms).
const DEGRADED_ENTRY_NS: u64 = 2_000_000;

/// p99 threshold (ns) for leaving degraded mode (1.5 ms).
const DEGRADED_RECOVERY_NS: u64 = 1_500_000;

/// Ticks in degraded mode with no new drops before auto-clearing ring buffer.
/// 5 ticks × 1s = 5 seconds after burst ends → auto-reset.
/// Reduced from 30 — long timeout caused system to stay stuck in degraded mode.
const STUCK_DEGRADED_TIMEOUT: u32 = 5;

// ============================================================================
// Shared Flags
// ============================================================================

/// Set to `true` by the watchdog when a network timeout is detected.
/// `ingestion.rs` should poll this and reconnect when it sees `true`,
/// then clear it back to `false`.
pub static NETWORK_RESET_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Whether the system is currently in degraded mode.
/// Mirrors `pipeline::DEGRADED_MODE` but exposed here for GUI reads.
pub static IN_DEGRADED_MODE: AtomicBool = AtomicBool::new(false);

// ============================================================================
// Watchdog State
// ============================================================================

struct WatchdogState {
    /// Number of consecutive ticks where p99 exceeded the deadline.
    overload_tick_count: u32,
    /// Total network resets triggered in this session.
    network_resets: u32,
    /// Total times degraded mode was activated.
    degraded_activations: u32,
    /// Ticks spent in degraded mode with no new dropped packets.
    /// When this reaches STUCK_DEGRADED_TIMEOUT, ring buffer is auto-cleared.
    stuck_degraded_ticks: u32,
    /// Snapshot of DROPPED_PACKETS from the previous watchdog tick.
    last_dropped_count: u64,
}

impl WatchdogState {
    fn new() -> Self {
        Self {
            overload_tick_count: 0,
            network_resets: 0,
            degraded_activations: 0,
            stuck_degraded_ticks: 0,
            last_dropped_count: 0,
        }
    }
}

// ============================================================================
// Watchdog Thread Entry Point
// ============================================================================

/// Spawn the watchdog on a dedicated OS thread.
///
/// # Arguments
/// * `running` - Shared flag; set to `false` to stop the watchdog.
/// * `last_packet_ts` - `AtomicU64` updated by ingestion (µs since epoch).
///
/// Returns immediately; the watchdog runs in the background.
pub fn start_watchdog(
    running: Arc<AtomicBool>,
    last_packet_ts: &'static AtomicU64,
    scheduler: SchedulerHandle,
) {
    std::thread::Builder::new()
        .name("watchdog".to_string())
        .spawn(move || {
            watchdog_loop(running, last_packet_ts, scheduler);
        })
        .expect("failed to spawn watchdog thread");
}

/// Main watchdog loop.
fn watchdog_loop(running: Arc<AtomicBool>, last_packet_ts: &'static AtomicU64, scheduler: SchedulerHandle) {
    let mut state = WatchdogState::new();
    eprintln!("[watchdog] started");

    while running.load(Ordering::Relaxed) {
        std::thread::sleep(WATCHDOG_INTERVAL);

        tick_network_check(last_packet_ts, &mut state);
        tick_degraded_mode_check(&mut state, &scheduler);
    }

    eprintln!(
        "[watchdog] stopped — network_resets={} degraded_activations={}",
        state.network_resets, state.degraded_activations
    );
}

// ============================================================================
// Network Timeout Check
// ============================================================================

fn tick_network_check(last_packet_ts: &AtomicU64, state: &mut WatchdogState) {
    let last_ts_micros = last_packet_ts.load(Ordering::Acquire);
    if last_ts_micros == 0 {
        // Ingestion hasn't started yet — nothing to check.
        return;
    }

    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    let elapsed_secs = (now_micros.saturating_sub(last_ts_micros)) / 1_000_000;

    if elapsed_secs >= NETWORK_TIMEOUT_SECS {
        state.network_resets += 1;
        eprintln!(
            "[watchdog] NETWORK TIMEOUT — {}s without data, requesting reconnect (reset #{})",
            elapsed_secs, state.network_resets
        );
        NETWORK_RESET_REQUESTED.store(true, Ordering::Release);
    }
}

// ============================================================================
// Degraded Mode Check
// ============================================================================

fn tick_degraded_mode_check(state: &mut WatchdogState, scheduler: &SchedulerHandle) {
    let p99_ns = P99_NS.load(Ordering::Relaxed);
    let currently_degraded = DEGRADED_MODE.load(Ordering::Relaxed);

    if !currently_degraded {
        // ── Normal mode: check if we need to enter degraded ──────────────────
        if p99_ns > DEGRADED_ENTRY_NS {
            state.overload_tick_count += 1;
            eprintln!(
                "[watchdog] p99={:.2}ms > 2ms (tick {}/{})",
                p99_ns as f64 / 1_000_000.0,
                state.overload_tick_count,
                DEGRADED_MODE_THRESHOLD_TICKS,
            );

            if state.overload_tick_count >= DEGRADED_MODE_THRESHOLD_TICKS {
                state.overload_tick_count = 0;
                state.degraded_activations += 1;
                eprintln!(
                    "[watchdog] *** DEGRADED MODE ACTIVATED — dropping bot edits (activation #{})",
                    state.degraded_activations
                );
                DEGRADED_MODE.store(true, Ordering::Release);
                IN_DEGRADED_MODE.store(true, Ordering::Release);
            }
        } else {
            // p99 is healthy — reset the consecutive-tick counter.
            state.overload_tick_count = 0;
        }
    } else {
        // ── Degraded mode: check if we can recover ────────────────────────────
        if p99_ns < DEGRADED_RECOVERY_NS {
            eprintln!(
                "[watchdog] p99={:.2}ms < 1.5ms — RECOVERING to normal mode",
                p99_ns as f64 / 1_000_000.0,
            );
            DEGRADED_MODE.store(false, Ordering::Release);
            IN_DEGRADED_MODE.store(false, Ordering::Release);
            state.overload_tick_count = 0;
            // Reset ring buffer so burst samples don't pollute future p99 readings
            scheduler.reset_percentiles();
        } else {
            // Check if the burst is actually over (no new drops)
            let current_dropped = crate::pipeline::DROPPED_PACKETS.load(Ordering::Relaxed);
            if current_dropped == state.last_dropped_count {
                // No new packets dropped — burst is over, ring buffer is just stale
                state.stuck_degraded_ticks += 1;
                eprintln!(
                    "[watchdog] still degraded: p99={:.2}ms (burst ended, auto-clear in {}s)",
                    p99_ns as f64 / 1_000_000.0,
                    STUCK_DEGRADED_TIMEOUT - state.stuck_degraded_ticks,
                );
                if state.stuck_degraded_ticks >= STUCK_DEGRADED_TIMEOUT {
                    eprintln!("[watchdog] AUTO-CLEARING stale ring buffer — burst ended {}s ago",
                        STUCK_DEGRADED_TIMEOUT);
                    scheduler.reset_percentiles();
                    state.stuck_degraded_ticks = 0;
                }
            } else {
                // Burst still active — new drops happening
                state.stuck_degraded_ticks = 0;
                eprintln!(
                    "[watchdog] still degraded: p99={:.2}ms (burst active, +{} drops)",
                    p99_ns as f64 / 1_000_000.0,
                    current_dropped - state.last_dropped_count,
                );
            }
            state.last_dropped_count = current_dropped;
        }
    }
}

// ============================================================================
// Status Snapshot (for GUI)
// ============================================================================

/// A point-in-time status snapshot for the GUI thread.
#[derive(Debug, Clone)]
pub struct WatchdogStatus {
    pub in_degraded_mode: bool,
    pub network_reset_pending: bool,
    pub p99_ms: f64,
    pub missed_deadlines: u64,
}

/// Read a non-blocking status snapshot.
pub fn status_snapshot() -> WatchdogStatus {
    WatchdogStatus {
        in_degraded_mode: IN_DEGRADED_MODE.load(Ordering::Relaxed),
        network_reset_pending: NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
        p99_ms: P99_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        missed_deadlines: MISSED_DEADLINES.load(Ordering::Relaxed),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduling::SchedulerHandle;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Serialise tests that write shared atomics (P99_NS, DEGRADED_MODE, etc.)
    // Rust unit tests run on multiple threads — without this, parallel tests
    // corrupt each other's global state mid-run.
    static UNIT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn now_micros() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    #[test]
    fn test_no_network_reset_if_recent_packet() {
        static TS: AtomicU64 = AtomicU64::new(0);
        TS.store(now_micros(), Ordering::Release);
        NETWORK_RESET_REQUESTED.store(false, Ordering::Release);

        let mut state = WatchdogState::new();
        tick_network_check(&TS, &mut state);

        assert!(!NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
                "should not reset if packet was just received");
    }

    #[test]
    fn test_network_reset_if_old_packet() {
        let _guard = UNIT_TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        static TS: AtomicU64 = AtomicU64::new(0);
        // Simulate a packet from 20 seconds ago
        let old = now_micros().saturating_sub(20 * 1_000_000);
        TS.store(old, Ordering::Release);
        NETWORK_RESET_REQUESTED.store(false, Ordering::Release);

        let mut state = WatchdogState::new();
        tick_network_check(&TS, &mut state);

        assert!(NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
                "should request reset after timeout");

        // Clean up
        NETWORK_RESET_REQUESTED.store(false, Ordering::Release);
    }

    #[test]
    fn test_degraded_mode_activates_after_threshold_ticks() {
        let _guard = UNIT_TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // Reset to known state before starting
        DEGRADED_MODE.store(false, Ordering::SeqCst);
        IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
        P99_NS.store(3_000_000, Ordering::SeqCst); // 3ms > 2ms threshold

        // Use ONE persistent WatchdogState across all ticks —
        // if we create a new one each tick the overload_tick_count resets to 0
        // and degraded mode never accumulates.
        // Also re-set P99_NS each tick in case a parallel test resets it.
        let mut state = WatchdogState::new();
        let scheduler = SchedulerHandle::new();

        for _ in 0..DEGRADED_MODE_THRESHOLD_TICKS {
            // Re-assert p99 > threshold before each tick (parallel tests may clear it)
            P99_NS.store(3_000_000, Ordering::SeqCst);
            tick_degraded_mode_check(&mut state, &scheduler);
        }

        let result = DEGRADED_MODE.load(Ordering::Relaxed);

        // Clean up before asserting so other tests see a clean state
        DEGRADED_MODE.store(false, Ordering::SeqCst);
        IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
        P99_NS.store(0, Ordering::SeqCst);

        assert!(result, "degraded mode should activate after {} consecutive ticks of p99 > 2ms",
                DEGRADED_MODE_THRESHOLD_TICKS);
    }

    #[test]
    fn test_degraded_mode_recovers() {
        let _guard = UNIT_TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        DEGRADED_MODE.store(true, Ordering::SeqCst);
        IN_DEGRADED_MODE.store(true, Ordering::SeqCst);
        P99_NS.store(1_000_000, Ordering::SeqCst); // 1 ms < 1.5 ms recovery threshold

        let mut state = WatchdogState::new();
        tick_degraded_mode_check(&mut state, &SchedulerHandle::new());

        assert!(!DEGRADED_MODE.load(Ordering::Relaxed),
                "degraded mode should recover when p99 < 1.5ms");

        // Clean up
        DEGRADED_MODE.store(false, Ordering::SeqCst);
        IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
        P99_NS.store(0, Ordering::SeqCst);
    }
}

// ============================================================================
// Test helpers — expose internal tick functions for integration tests
// ============================================================================

pub mod test_helpers {
    use std::sync::atomic::AtomicU64;
    use super::{WatchdogState, tick_network_check, tick_degraded_mode_check};
    use crate::scheduling::SchedulerHandle;

    /// Simulate one network-check tick of the watchdog.
    pub fn tick_network(last_packet_ts: &AtomicU64) {
        let mut state = WatchdogState::new();
        tick_network_check(last_packet_ts, &mut state);
    }

    /// Simulate one degraded-mode-check tick of the watchdog.
    pub fn tick_degraded(scheduler: &SchedulerHandle) {
        let mut state = WatchdogState::new();
        tick_degraded_mode_check(&mut state, scheduler);
    }
}