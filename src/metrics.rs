//! Metrics module - Leaderboard & Synchronisation Comparison (Component D)
//!
//! # Leaderboard
//! Tracks the top-3 most active Wikipedia domains by edit count.
//! Protected by a `Mutex<HashMap<String, u32>>` internally; a snapshot
//! sorted Vec is published to an atomic-friendly `Arc<Mutex<Vec>>` for
//! the GUI to read with a short lock.
//!
//! # Sync Comparison Benchmark
//! Runs 1 000 000 increment operations under three primitives and reports
//! wall-clock time for each:
//!
//! | Primitive            | Expected speed |
//! |----------------------|----------------|
//! | `std::sync::Mutex`   | Slowest        |
//! | `std::sync::RwLock`  | Middle         |
//! | `AtomicU64`          | Fastest        |
//!
//! This provides empirical evidence for the report section on synchronisation
//! overhead.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Constants
// ============================================================================

/// Number of domains to display on the leaderboard.
const LEADERBOARD_SIZE: usize = 3;

/// Iterations for the sync benchmark.
const BENCHMARK_ITERS: u64 = 1_000_000;

// ============================================================================
// Global Throughput Counter
// ============================================================================

/// Incremented every time an edit is recorded; GUI reads this to compute rate.
pub static EDIT_COUNTER: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// Leaderboard
// ============================================================================

/// Internal full-domain → count map.
struct LeaderboardInner {
    counts: HashMap<String, u32>,
}

impl LeaderboardInner {
    fn new() -> Self {
        Self {
            counts: HashMap::with_capacity(256),
        }
    }

    /// Increment the count for `domain` and return the updated top-N list.
    fn record_edit(&mut self, domain: &str) -> Vec<(String, u32)> {
        let entry = self.counts.entry(domain.to_string()).or_insert(0);
        *entry += 1;

        // Build sorted top-N snapshot
        let mut all: Vec<(String, u32)> = self.counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        all.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        all.truncate(LEADERBOARD_SIZE);
        all
    }
}

/// Cheaply clonable handle to the metrics subsystem.
#[derive(Clone)]
pub struct MetricsHandle {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    leaderboard: Mutex<LeaderboardInner>,
    /// Latest top-3 snapshot, published after each edit for GUI reads.
    snapshot: Mutex<Vec<(String, u32)>>,
}

impl MetricsHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                leaderboard: Mutex::new(LeaderboardInner::new()),
                snapshot: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Record a new edit for `domain`.
    ///
    /// Updates the internal count map and publishes a new snapshot.
    pub fn record_edit(&self, domain: &str) {
        EDIT_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut lb) = self.inner.leaderboard.lock() {
            let top = lb.record_edit(domain);
            if let Ok(mut snap) = self.inner.snapshot.lock() {
                *snap = top;
            }
        }
    }

    /// Return the current top-3 leaderboard snapshot.
    ///
    /// GUI calls this; the lock is held only for the duration of the clone.
    pub fn leaderboard_snapshot(&self) -> Vec<(String, u32)> {
        self.inner.snapshot.lock()
            .map(|snap| snap.clone())
            .unwrap_or_default()
    }

    /// Current edit count (for throughput calculation).
    pub fn total_edits(&self) -> u64 {
        EDIT_COUNTER.load(Ordering::Relaxed)
    }
}

impl Default for MetricsHandle {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Synchronisation Comparison Benchmark (Component D Extra)
// ============================================================================

/// Result of the sync benchmark for one primitive.
#[derive(Debug, Clone)]
pub struct BenchResult {
    pub primitive: &'static str,
    pub iterations: u64,
    pub elapsed: Duration,
    pub ops_per_second: f64,
    pub ns_per_op: f64,
}

impl BenchResult {
    pub fn report(&self) -> String {
        format!(
            "{:<12} | {:>12} iters | {:>8.2} ms | {:>12.0} ops/s | {:>6.1} ns/op",
            self.primitive,
            self.iterations,
            self.elapsed.as_secs_f64() * 1000.0,
            self.ops_per_second,
            self.ns_per_op,
        )
    }
}

/// Run the full Mutex / RwLock / Atomic comparison.
///
/// Each benchmark performs `BENCHMARK_ITERS` increments of a shared counter
/// from a **single thread** (removing scheduling noise).  For a fair
/// multi-threaded comparison see `benches/comparison.rs` (Criterion).
pub fn run_sync_benchmark() -> Vec<BenchResult> {
    let mut results = Vec::with_capacity(3);

    // ── std::sync::Mutex ─────────────────────────────────────────────────────
    {
        let counter = Mutex::new(0u64);
        let start = Instant::now();
        for _ in 0..BENCHMARK_ITERS {
            let mut guard = counter.lock().unwrap();
            *guard += 1;
        }
        let elapsed = start.elapsed();
        let ops = BENCHMARK_ITERS as f64 / elapsed.as_secs_f64();
        results.push(BenchResult {
            primitive: "Mutex",
            iterations: BENCHMARK_ITERS,
            elapsed,
            ops_per_second: ops,
            ns_per_op: elapsed.as_nanos() as f64 / BENCHMARK_ITERS as f64,
        });
    }

    // ── std::sync::RwLock (write path) ───────────────────────────────────────
    {
        let counter = RwLock::new(0u64);
        let start = Instant::now();
        for _ in 0..BENCHMARK_ITERS {
            let mut guard = counter.write().unwrap();
            *guard += 1;
        }
        let elapsed = start.elapsed();
        let ops = BENCHMARK_ITERS as f64 / elapsed.as_secs_f64();
        results.push(BenchResult {
            primitive: "RwLock",
            iterations: BENCHMARK_ITERS,
            elapsed,
            ops_per_second: ops,
            ns_per_op: elapsed.as_nanos() as f64 / BENCHMARK_ITERS as f64,
        });
    }

    // ── AtomicU64 ────────────────────────────────────────────────────────────
    {
        let counter = AtomicU64::new(0);
        let start = Instant::now();
        for _ in 0..BENCHMARK_ITERS {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let elapsed = start.elapsed();
        let ops = BENCHMARK_ITERS as f64 / elapsed.as_secs_f64();
        results.push(BenchResult {
            primitive: "AtomicU64",
            iterations: BENCHMARK_ITERS,
            elapsed,
            ops_per_second: ops,
            ns_per_op: elapsed.as_nanos() as f64 / BENCHMARK_ITERS as f64,
        });
    }

    // Print a summary table
    println!("\n=== Sync Comparison Benchmark ({} iters) ===", BENCHMARK_ITERS);
    println!("{:<12} | {:>12} | {:>10} | {:>14} | {:>10}", "Primitive", "Iterations", "Elapsed", "Ops/sec", "ns/op");
    println!("{}", "─".repeat(70));
    for r in &results {
        println!("{}", r.report());
    }
    println!();

    results
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaderboard_top3_ordering() {
        let metrics = MetricsHandle::new();

        for _ in 0..10 { metrics.record_edit("en.wikipedia.org"); }
        for _ in 0..5  { metrics.record_edit("fr.wikipedia.org"); }
        for _ in 0..3  { metrics.record_edit("de.wikipedia.org"); }
        for _ in 0..1  { metrics.record_edit("ja.wikipedia.org"); } // 4th — excluded

        let snap = metrics.leaderboard_snapshot();
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].0, "en.wikipedia.org");
        assert_eq!(snap[0].1, 10);
        assert_eq!(snap[1].0, "fr.wikipedia.org");
        assert_eq!(snap[2].0, "de.wikipedia.org");
        // ja should not appear
        assert!(!snap.iter().any(|(d, _)| d == "ja.wikipedia.org"));
    }

    #[test]
    fn test_edit_counter_increments() {
        // Snapshot BEFORE our calls so other parallel tests don't interfere.
        // We only assert the counter went up by exactly 2 from OUR calls.
        let before = EDIT_COUNTER.load(Ordering::Relaxed);
        let metrics = MetricsHandle::new();
        metrics.record_edit("en.wikipedia.org");
        metrics.record_edit("en.wikipedia.org");
        let after = EDIT_COUNTER.load(Ordering::Relaxed);
        assert!(
            after >= before + 2,
            "EDIT_COUNTER should have increased by at least 2 (before={} after={})",
            before, after
        );
    }

    #[test]
    fn test_benchmark_runs_and_returns_three_results() {
        let results = run_sync_benchmark();
        assert_eq!(results.len(), 3);
        // AtomicU64 should be fastest (highest ops/sec)
        let atomic = results.iter().find(|r| r.primitive == "AtomicU64").unwrap();
        let mutex  = results.iter().find(|r| r.primitive == "Mutex").unwrap();
        assert!(atomic.ops_per_second > mutex.ops_per_second,
                "Atomic should outperform Mutex");
    }
}