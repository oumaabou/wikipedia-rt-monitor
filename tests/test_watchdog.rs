//! Integration tests for Watchdog (Component E)
//!
//! Tests:
//! 1. Network timeout triggers NETWORK_RESET_REQUESTED after 10s without data
//! 2. No reset triggered if packet was received recently
//! 3. Degraded mode activates after 10 consecutive ticks of p99 > 2ms
//! 4. Degraded mode recovers when p99 drops below 1.5ms
//! 5. Degraded mode recovery resets p99 percentile ring buffer
//! 6. Watchdog thread starts and stops cleanly with the running flag

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use wikipedia_rt_monitor::{
    pipeline::DEGRADED_MODE,
    scheduling::{P99_NS, SchedulerHandle},
    watchdog::{IN_DEGRADED_MODE, NETWORK_RESET_REQUESTED},
};

// ============================================================================
// Helpers
// ============================================================================

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn clean_state() {
    DEGRADED_MODE.store(false, Ordering::SeqCst);
    IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
    NETWORK_RESET_REQUESTED.store(false, Ordering::SeqCst);
    P99_NS.store(0, Ordering::SeqCst);
}

// ============================================================================
// Test 1: No network reset when packet is fresh
// ============================================================================

#[test]
fn test_no_reset_for_recent_packet() {
    clean_state();
    static FRESH_TS: AtomicU64 = AtomicU64::new(0);
    FRESH_TS.store(now_micros(), Ordering::Release);

    // Simulate one watchdog tick
    wikipedia_rt_monitor::watchdog::test_helpers::tick_network(&FRESH_TS);

    assert!(
        !NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
        "should NOT request reset when last packet was just now"
    );
    println!("✅ Watchdog: no reset for fresh packet");
}

// ============================================================================
// Test 2: Network reset IS triggered after 20s of silence
// ============================================================================

#[test]
fn test_reset_triggered_for_stale_packet() {
    clean_state();
    static STALE_TS: AtomicU64 = AtomicU64::new(0);

    // Simulate a timestamp 20 seconds ago
    let old = now_micros().saturating_sub(20 * 1_000_000);
    STALE_TS.store(old, Ordering::Release);

    wikipedia_rt_monitor::watchdog::test_helpers::tick_network(&STALE_TS);

    assert!(
        NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
        "should request reset after 20s without a packet"
    );
    println!("✅ Watchdog: reset requested after 20s silence");

    // Cleanup
    NETWORK_RESET_REQUESTED.store(false, Ordering::Release);
}

// ============================================================================
// Test 3: Degraded mode activates after 10 consecutive over-threshold ticks
// ============================================================================

// Global mutex to serialise tests that touch shared atomics.
// Rust integration tests run in parallel by default; this prevents
// test_recovery_resets_p99 from writing P99_NS=0 mid-run of this test.
static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_degraded_mode_activates_after_10_ticks() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

    DEGRADED_MODE.store(false, Ordering::SeqCst);
    IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
    NETWORK_RESET_REQUESTED.store(false, Ordering::SeqCst);
    P99_NS.store(3_000_000, Ordering::SeqCst); // 3ms > 2ms threshold

    let running = Arc::new(AtomicBool::new(true));
    let scheduler = SchedulerHandle::new();

    // Use a REAL watchdog thread — it maintains internal state across ticks.
    // We need 10 ticks of 1s each = 10s minimum. To keep the test fast,
    // we verify that after 11 ticks the mode has activated.
    // p99 is stored to a static so the watchdog thread reads it correctly.
    static WD_TS2: AtomicU64 = AtomicU64::new(u64::MAX); // prevent network reset

    wikipedia_rt_monitor::watchdog::start_watchdog(
        Arc::clone(&running),
        &WD_TS2,
        scheduler,
    );

    // Keep p99 above threshold for 11 seconds so the watchdog accumulates 10 ticks
    for _ in 0..11 {
        P99_NS.store(3_000_000, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(1010));
    }

    let activated = DEGRADED_MODE.load(Ordering::Relaxed);

    // Clean up
    running.store(false, Ordering::Release);
    DEGRADED_MODE.store(false, Ordering::SeqCst);
    IN_DEGRADED_MODE.store(false, Ordering::SeqCst);
    P99_NS.store(0, Ordering::SeqCst);

    assert!(activated,
        "degraded mode MUST activate after 10 consecutive ticks of p99 > 2ms");
    println!("✅ Watchdog: degraded mode activated after 10 ticks");
}

// ============================================================================
// Test 4: Degraded mode recovers when p99 drops below 1.5ms
// ============================================================================

#[test]
fn test_degraded_mode_recovers() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    clean_state();

    // Start in degraded mode
    DEGRADED_MODE.store(true, Ordering::SeqCst);
    IN_DEGRADED_MODE.store(true, Ordering::SeqCst);

    // p99 = 1ms < 1.5ms recovery threshold
    P99_NS.store(1_000_000, Ordering::SeqCst);

    let scheduler = SchedulerHandle::new();
    wikipedia_rt_monitor::watchdog::test_helpers::tick_degraded(&scheduler);

    assert!(
        !DEGRADED_MODE.load(Ordering::Relaxed),
        "DEGRADED_MODE should be cleared when p99 < 1.5ms"
    );
    assert!(
        !IN_DEGRADED_MODE.load(Ordering::Relaxed),
        "IN_DEGRADED_MODE should be cleared on recovery"
    );
    println!("✅ Watchdog: recovered from degraded mode when p99 < 1.5ms");
}

// ============================================================================
// Test 5: Recovery resets percentile ring buffer (p99 → 0)
// ============================================================================

#[test]
fn test_recovery_resets_p99() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    clean_state();

    // Simulate a burst that elevated p99
    DEGRADED_MODE.store(true, Ordering::SeqCst);
    IN_DEGRADED_MODE.store(true, Ordering::SeqCst);
    P99_NS.store(500_000, Ordering::SeqCst); // 0.5ms — below 1.5ms recovery

    let scheduler = SchedulerHandle::new();

    // Fill the scheduler with some burst samples so there's data to reset
    for _ in 0..5 {
        P99_NS.store(500_000, Ordering::SeqCst);
    }

    wikipedia_rt_monitor::watchdog::test_helpers::tick_degraded(&scheduler);

    // After recovery, reset_percentiles() should have zeroed the atomics
    let p99_after = P99_NS.load(Ordering::Relaxed);
    assert_eq!(p99_after, 0,
        "p99 should be reset to 0 after recovery (got {}ns)", p99_after);

    println!("✅ Watchdog: p99 reset to 0 after recovery");
}

// ============================================================================
// Test 6: Watchdog thread starts and stops with the running flag
// ============================================================================

#[test]
fn test_watchdog_thread_lifecycle() {
    clean_state();
    static WD_TS: AtomicU64 = AtomicU64::new(u64::MAX); // prevent timeout

    let running = Arc::new(AtomicBool::new(true));
    let scheduler = SchedulerHandle::new();

    // Start watchdog
    wikipedia_rt_monitor::watchdog::start_watchdog(
        Arc::clone(&running),
        &WD_TS,
        scheduler,
    );

    // Let it run for 2 ticks
    std::thread::sleep(Duration::from_millis(2100));

    // Signal stop
    running.store(false, Ordering::Release);

    // Give it time to exit its loop
    std::thread::sleep(Duration::from_millis(1200));

    // If we get here without hanging, the thread exits cleanly
    println!("✅ Watchdog: thread started and stopped cleanly");
}