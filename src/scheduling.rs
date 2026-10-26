//! Scheduling module - Drift Measurement, Deadlines & Percentile Latencies
//! (Component C + Extra 3/4)
//!
//! # Concepts
//!
//! ## Scheduling Drift
//! The gap between when a packet was received by ingestion and when a worker
//! actually starts processing it.  This measures queue wait time:
//! ```text
//! drift = processing_start_time - packet.ingestion_time
//! ```
//!
//! ## Deadline
//! Each edit has a **2 ms soft real-time deadline**.  If `elapsed > 2 ms`
//! the event is logged and the global `MISSED_DEADLINES` counter incremented.
//!
//! ## Percentiles (p50 / p90 / p99)
//! A fixed-size ring buffer collects the last N drift values (in microseconds).
//! Every `PERCENTILE_SAMPLE_INTERVAL` inserts the buffer is sorted and the
//! p50 / p90 / p99 indices are taken.  Values are published to three
//! `AtomicU64` registers (microseconds × 1000 to keep integer precision) so
//! the GUI can read them without locking.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::processing::ProcessedPacket;

// ============================================================================
// Constants
// ============================================================================

/// 2 ms deadline for each edit (in microseconds).

const DEADLINE_MICROS: u64 = 2_000; // 2 ms — guarantees misses at current speed

/// How many latency samples to hold in the ring buffer.
const RING_BUFFER_CAPACITY: usize = 10_000;

/// Recalculate percentiles every N inserts.
const PERCENTILE_SAMPLE_INTERVAL: usize = 200;

// ============================================================================
// Published Metrics (Atomic — GUI reads without locking)
// ============================================================================

/// p50 latency in nanoseconds.
pub static P50_NS: AtomicU64 = AtomicU64::new(0);
/// p90 latency in nanoseconds.
pub static P90_NS: AtomicU64 = AtomicU64::new(0);
/// p99 latency in nanoseconds.
pub static P99_NS: AtomicU64 = AtomicU64::new(0);

/// Total packets that breached the 2 ms deadline.
pub static MISSED_DEADLINES: AtomicU64 = AtomicU64::new(0);

/// Cumulative drift in nanoseconds (for average drift display).
pub static TOTAL_DRIFT_NS: AtomicU64 = AtomicU64::new(0);

/// Human edit drift tracking — proves priority scheduling works.
/// Human edits should have LOWER average drift than bots because
/// they are always dequeued first from the high-priority channel.
pub static HUMAN_DRIFT_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
pub static HUMAN_DRIFT_COUNT: AtomicU64 = AtomicU64::new(0);
pub static BOT_DRIFT_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
pub static BOT_DRIFT_COUNT: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// Internal Ring Buffer
// ============================================================================

/// Fixed-size ring buffer for latency samples.
struct RingBuffer {
    data: Vec<u64>,      // values in nanoseconds
    head: usize,         // next write position
    count: usize,        // number of valid entries (capped at capacity)
    insert_counter: usize,
}

impl RingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u64; capacity],
            head: 0,
            count: 0,
            insert_counter: 0,
        }
    }

    /// Insert a new sample and recompute percentiles every N inserts.
    fn push(&mut self, value_ns: u64) {
        self.data[self.head] = value_ns;
        self.head = (self.head + 1) % self.data.len();
        if self.count < self.data.len() {
            self.count += 1;
        }
        self.insert_counter += 1;

        if self.insert_counter >= PERCENTILE_SAMPLE_INTERVAL {
            self.insert_counter = 0;
            self.publish_percentiles();
        }
    }

    /// Sort the valid slice and publish p50 / p90 / p99.
    fn publish_percentiles(&self) {
        if self.count == 0 {
            return;
        }

        // Collect valid samples into a temporary sorted Vec.
        // This is the only allocation in the scheduling hot path, and it only
        // happens every PERCENTILE_SAMPLE_INTERVAL packets (~every 200 edits).
        let mut samples: Vec<u64> = self.data[..self.count].to_vec();
        samples.sort_unstable();

        let len = samples.len();

        let p50 = samples[(len as f64 * 0.50) as usize];
        let p90 = samples[(len as f64 * 0.90) as usize];
        let p99 = samples[((len as f64 * 0.99) as usize).min(len - 1)];

        P50_NS.store(p50, Ordering::Relaxed);
        P90_NS.store(p90, Ordering::Relaxed);
        P99_NS.store(p99, Ordering::Relaxed);
    }
}

// ============================================================================
// Phase Snapshot — captures all metrics at a point in time for demo summary
// ============================================================================

/// A complete snapshot of all metrics at a phase boundary.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PhaseSnapshot {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub missed_deadlines: u64,
    pub dropped_packets: u64,
    pub processed: u64,
    pub human_avg_drift_ms: f64,
    pub bot_avg_drift_ms: f64,
    pub human_count: u64,
    pub bot_count: u64,
    pub throughput_eps: f64,
    pub duration_secs: u64,
}

// ============================================================================
// SchedulerHandle (cheaply clonable — Arc-wrapped inner)
// ============================================================================

struct SchedulerInner {
    ring: Mutex<RingBuffer>,
}

/// Cheap clone handle to the scheduler.  Workers each hold one.
#[derive(Clone)]
pub struct SchedulerHandle {
    inner: Arc<SchedulerInner>,
}

impl SchedulerHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                ring: Mutex::new(RingBuffer::new(RING_BUFFER_CAPACITY)),
            }),
        }
    }

    /// Record a processed packet.
    /// Measures processing time from processing_start (2ms deadline)
    /// and queue wait from ingestion_time (priority proof).
    pub fn record(&self, packet: &ProcessedPacket<'_>) {
        let now = Instant::now();

        // Processing time — the 2ms deadline metric
        let processing_ns = now.duration_since(packet.processing_start).as_nanos() as u64;

        // Queue wait — the priority proof metric
        let queue_wait_ns = packet.processing_start
            .duration_since(packet.ingestion_time)
            .as_nanos() as u64;

        TOTAL_DRIFT_NS.fetch_add(queue_wait_ns, Ordering::Relaxed);

        if packet.is_bot {
            BOT_DRIFT_TOTAL_NS.fetch_add(queue_wait_ns, Ordering::Relaxed);
            BOT_DRIFT_COUNT.fetch_add(1, Ordering::Relaxed);
        } else {
            HUMAN_DRIFT_TOTAL_NS.fetch_add(queue_wait_ns, Ordering::Relaxed);
            HUMAN_DRIFT_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        if processing_ns > DEADLINE_MICROS * 1_000 {
            MISSED_DEADLINES.fetch_add(1, Ordering::Relaxed);
            crate::pipeline::MISSED_DEADLINES.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[scheduling] DEADLINE MISSED: processing={:.3}ms queue_wait={:.3}ms user={} domain={}",
                processing_ns as f64 / 1_000_000.0,
                queue_wait_ns as f64 / 1_000_000.0,
                packet.user,
                packet.server_name,
            );
        }

        if let Ok(mut ring) = self.inner.ring.lock() {
            ring.push(processing_ns);
        }
    }

    pub fn p50_ms(&self) -> f64 { P50_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0 }
    pub fn p90_ms(&self) -> f64 { P90_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0 }
    pub fn p99_ms(&self) -> f64 { P99_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0 }
    pub fn missed_deadlines(&self) -> u64 { MISSED_DEADLINES.load(Ordering::Relaxed) }

    pub fn human_avg_drift_ms() -> f64 {
        let count = HUMAN_DRIFT_COUNT.load(Ordering::Relaxed);
        if count == 0 { return 0.0; }
        HUMAN_DRIFT_TOTAL_NS.load(Ordering::Relaxed) as f64 / count as f64 / 1_000_000.0
    }

    pub fn bot_avg_drift_ms() -> f64 {
        let count = BOT_DRIFT_COUNT.load(Ordering::Relaxed);
        if count == 0 { return 0.0; }
        BOT_DRIFT_TOTAL_NS.load(Ordering::Relaxed) as f64 / count as f64 / 1_000_000.0
    }

    /// Clear ring buffer + all atomics + drift accumulators.
    /// MUST be called AFTER take_snapshot().
    pub fn reset_all(&self) {
        if let Ok(mut ring) = self.inner.ring.lock() {
            ring.head = 0;
            ring.count = 0;
            ring.insert_counter = 0;
        }
        P50_NS.store(0, Ordering::Relaxed);
        P90_NS.store(0, Ordering::Relaxed);
        P99_NS.store(0, Ordering::Relaxed);
        HUMAN_DRIFT_TOTAL_NS.store(0, Ordering::Relaxed);
        HUMAN_DRIFT_COUNT.store(0, Ordering::Relaxed);
        BOT_DRIFT_TOTAL_NS.store(0, Ordering::Relaxed);
        BOT_DRIFT_COUNT.store(0, Ordering::Relaxed);
        TOTAL_DRIFT_NS.store(0, Ordering::Relaxed);
    }

    /// Alias for watchdog compatibility.
    pub fn reset_percentiles(&self) { self.reset_all(); }

    /// Capture all current metrics into a PhaseSnapshot.
    /// Call this BEFORE reset_all() — order is critical.
    pub fn take_snapshot(
        &self,
        dropped: u64,
        processed: u64,
        missed: u64,
        duration_secs: u64,
        throughput_eps: f64,
    ) -> PhaseSnapshot {
        PhaseSnapshot {
            p50_ms:             P50_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            p90_ms:             P90_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            p99_ms:             P99_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            missed_deadlines:   missed,
            dropped_packets:    dropped,
            processed,
            human_avg_drift_ms: Self::human_avg_drift_ms(),
            bot_avg_drift_ms:   Self::bot_avg_drift_ms(),
            human_count:        HUMAN_DRIFT_COUNT.load(Ordering::Relaxed),
            bot_count:          BOT_DRIFT_COUNT.load(Ordering::Relaxed),
            throughput_eps,
            duration_secs,
        }
    }
}

impl Default for SchedulerHandle {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::{Duration, Instant};
    use crate::processing::ProcessedPacket;

    fn make_processed<'a>(data: &'a Bytes, ingestion_time: Instant) -> ProcessedPacket<'a> {
        // Minimal ProcessedPacket; user borrows from data
        ProcessedPacket {
            user: std::str::from_utf8(data.as_ref()).unwrap_or("unknown"),
            server_name: "en.wikipedia.org".to_string(),
            is_bot: false,
            ingestion_time,
            processing_start: ingestion_time,
        }
    }

    #[test]
    fn test_no_deadline_miss_for_fresh_packet() {
        let handle = SchedulerHandle::new();
        let data = Bytes::from_static(b"Alice");
        let processed = make_processed(&data, Instant::now());
        let before = MISSED_DEADLINES.load(Ordering::Relaxed);
        handle.record(&processed);
        let after = MISSED_DEADLINES.load(Ordering::Relaxed);
        // A freshly created packet should NOT miss a 2ms deadline
        assert_eq!(before, after, "fresh packet should not miss deadline");
    }

    #[test]
    fn test_deadline_miss_for_stale_packet() {
        let handle = SchedulerHandle::new();
        let data = Bytes::from_static(b"Bob");
        // Simulate a packet that is already 5ms old
        let stale_time = Instant::now() - Duration::from_millis(5);
        let processed = make_processed(&data, stale_time);
        let before = MISSED_DEADLINES.load(Ordering::Relaxed);
        handle.record(&processed);
        let after = MISSED_DEADLINES.load(Ordering::Relaxed);
        assert_eq!(after, before + 1, "stale packet should miss deadline");
    }

    #[test]
    fn test_percentiles_published_after_sample_interval() {
        let handle = SchedulerHandle::new();
        // Reset atomics
        P50_NS.store(0, Ordering::SeqCst);
        P99_NS.store(0, Ordering::SeqCst);

        for i in 0..PERCENTILE_SAMPLE_INTERVAL {
            let data = Bytes::from_static(b"Carol");
            // Vary the ingestion time to produce different drift values
            let stale = Instant::now() - Duration::from_micros(i as u64 * 10);
            let processed = make_processed(&data, stale);
            handle.record(&processed);
        }

        // After PERCENTILE_SAMPLE_INTERVAL records the atomics should be set
        assert!(P50_NS.load(Ordering::Relaxed) > 0, "p50 should be non-zero");
        assert!(P99_NS.load(Ordering::Relaxed) >= P50_NS.load(Ordering::Relaxed),
                "p99 >= p50");
    }

    #[test]
    fn test_ring_buffer_wraps() {
        let mut ring = RingBuffer::new(4);
        for i in 0..10u64 {
            ring.push(i * 1_000);
        }
        // count should be capped at capacity
        assert_eq!(ring.count, 4);
    }
}