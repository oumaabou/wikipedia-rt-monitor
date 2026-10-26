//! Integration tests for scheduling metrics, leaderboard and sync benchmark
//!
//! 1. Fresh packets show ~zero queue drift
//! 2. Leaderboard keeps the top 3 domains sorted descending
//! 3. p50 <= p90 <= p99 invariant holds
//! 4. reset_percentiles() clears the published atomics
//! 5. The 10 000-sample ring buffer stays bounded under heavy load
//! 6. Atomics are the fastest sync primitive in the built-in benchmark

use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use wikipedia_rt_monitor::{
    ingestion::RawPacket,
    metrics::{run_sync_benchmark, MetricsHandle},
    processing::process_packet,
    scheduling::{SchedulerHandle, P50_NS, P90_NS, P99_NS},
};

// Shared global atomics -> serialise tests in this file.
static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn make_packet(user: &str, is_bot: bool) -> RawPacket {
    let json = format!(
        r#"{{"user":"{}","bot":{},"server_name":"en.wikipedia.org","type":"edit"}}"#,
        user, is_bot
    );
    let t = Instant::now();
    RawPacket {
        data: Bytes::from(json.into_bytes()),
        ingestion_time: t,
        processing_start: t,
        is_bot,
        server_name: "en.wikipedia.org".to_string(),
    }
}

/// Record `n` packets whose processing time ramps up from `step_us` microseconds.
fn record_ramp(handle: &SchedulerHandle, n: u64, step_us: u64) {
    for i in 1..=n {
        let mut p = make_packet("Ramp", false);
        p.processing_start = Instant::now() - Duration::from_micros(i * step_us);
        handle.record(&process_packet(&p).unwrap());
    }
}

#[test]
fn test_drift_near_zero_for_fresh_packet() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    // ingestion_time == processing_start -> zero queue wait
    handle.record(&process_packet(&make_packet("Fresh", false)).unwrap());

    let drift = SchedulerHandle::human_avg_drift_ms();
    assert!(drift < 1.0, "fresh packet drift should be ~0 ms, got {:.3} ms", drift);
    println!("✅ Scheduling: fresh packet drift ≈ 0 ({:.4} ms)", drift);
}

#[test]
fn test_leaderboard_top3_is_sorted_descending() {
    let metrics = MetricsHandle::new();
    let plan = [
        ("en.wikipedia.org", 5),
        ("de.wikipedia.org", 4),
        ("fr.wikipedia.org", 3),
        ("commons.wikimedia.org", 2),
        ("ru.wikinews.org", 1),
    ];
    for (domain, n) in plan {
        for _ in 0..n {
            metrics.record_edit(domain);
        }
    }

    let top = metrics.leaderboard_snapshot();
    assert_eq!(top.len(), 3, "leaderboard must hold exactly the top 3");
    assert_eq!(top[0], ("en.wikipedia.org".to_string(), 5));
    assert_eq!(top[1], ("de.wikipedia.org".to_string(), 4));
    assert_eq!(top[2], ("fr.wikipedia.org".to_string(), 3));
    assert!(top.windows(2).all(|w| w[0].1 >= w[1].1), "must be sorted descending");
    println!("✅ Scheduling: leaderboard top-3 sorted");
}

#[test]
fn test_percentile_ordering_invariant() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    record_ramp(&handle, 400, 50);

    let (p50, p90, p99) = (
        P50_NS.load(Ordering::Relaxed),
        P90_NS.load(Ordering::Relaxed),
        P99_NS.load(Ordering::Relaxed),
    );
    assert!(p50 > 0);
    assert!(p50 <= p90, "p50 ({}) must be <= p90 ({})", p50, p90);
    assert!(p90 <= p99, "p90 ({}) must be <= p99 ({})", p90, p99);
    assert!(handle.p50_ms() <= handle.p90_ms() && handle.p90_ms() <= handle.p99_ms());
    println!("✅ Scheduling: p50 <= p90 <= p99");
}

#[test]
fn test_reset_percentiles_clears_atomics() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();

    record_ramp(&handle, 200, 100);
    assert!(P99_NS.load(Ordering::Relaxed) > 0, "precondition: percentiles published");

    handle.reset_percentiles();

    assert_eq!(P50_NS.load(Ordering::Relaxed), 0);
    assert_eq!(P90_NS.load(Ordering::Relaxed), 0);
    assert_eq!(P99_NS.load(Ordering::Relaxed), 0);
    println!("✅ Scheduling: reset_percentiles cleared atomics");
}

#[test]
fn test_ring_buffer_bounded() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    // More than the 10 000-sample capacity: must wrap, not grow or panic,
    // and percentiles must still be published.
    for _ in 0..10_500 {
        let mut p = make_packet("Flood", false);
        p.processing_start = Instant::now() - Duration::from_micros(500);
        handle.record(&process_packet(&p).unwrap());
    }

    let p99 = handle.p99_ms();
    assert!(p99 >= 0.5, "p99 should reflect the ~0.5 ms samples, got {:.3} ms", p99);
    assert!(p99 < 1_000.0, "p99 should be sane after wrap-around, got {:.3} ms", p99);
    println!("✅ Scheduling: ring buffer wrapped safely (p99={:.3} ms)", p99);
}

#[test]
fn test_sync_benchmark_atomic_fastest() {
    let results = run_sync_benchmark();
    assert_eq!(results.len(), 3, "expected Mutex, RwLock, AtomicU64");

    let atomic = results.iter().find(|r| r.primitive == "AtomicU64").expect("atomic result");
    let mutex = results.iter().find(|r| r.primitive == "Mutex").expect("mutex result");

    assert!(
        atomic.ns_per_op <= mutex.ns_per_op,
        "atomic ({:.1} ns/op) should not be slower than Mutex ({:.1} ns/op)",
        atomic.ns_per_op,
        mutex.ns_per_op
    );
    for r in &results {
        println!("   {}", r.report());
    }
    println!("✅ Scheduling: atomics fastest in sync benchmark");
}
