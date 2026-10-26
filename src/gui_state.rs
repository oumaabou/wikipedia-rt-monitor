//! Shared mutable control state for the HTTP GUI server.
//!
//! Wraps the fields that the React frontend can read/write (running flag,
//! pipeline mode, live feed ring buffer, throughput rolling window) behind
//! atomics and a Mutex — all cheap to clone via Arc.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::pipeline::PipelineMode;

// ============================================================================
// Live feed ring buffer
// ============================================================================

const LIVE_FEED_CAPACITY: usize = 100;

pub struct LiveFeed {
    inner: Mutex<Vec<String>>,
}

impl LiveFeed {
    pub fn new() -> Self {
        Self { inner: Mutex::new(Vec::with_capacity(LIVE_FEED_CAPACITY)) }
    }

    pub fn push(&self, entry: String) {
        if let Ok(mut v) = self.inner.lock() {
            if v.len() >= LIVE_FEED_CAPACITY {
                v.remove(0);
            }
            v.push(entry);
        }
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.inner.lock().map(|v| v.clone()).unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut v) = self.inner.lock() { v.clear(); }
    }
}

// ============================================================================
// GuiState
// ============================================================================

pub struct GuiState {
    /// GUI display flag — also read by workers to pause (not exit).
    is_running: AtomicBool,
    /// Global kill flag shared with main.rs — only set to false on Ctrl-C shutdown.
    /// Workers exit their loop when this becomes false.
    pipeline_running: Arc<AtomicBool>,
    mode: Mutex<PipelineMode>,
    live_feed: LiveFeed,
    // Throughput rolling window
    last_edit_count: AtomicU64,
    last_tick_instant: Mutex<Instant>,
    cached_throughput: Mutex<f64>,
}

impl GuiState {
    pub fn new(pipeline_running: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            is_running: AtomicBool::new(true),
            pipeline_running,
            mode: Mutex::new(PipelineMode::Async),
            live_feed: LiveFeed::new(),
            last_edit_count: AtomicU64::new(0),
            last_tick_instant: Mutex::new(Instant::now()),
            cached_throughput: Mutex::new(0.0),
        })
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn set_running(&self, v: bool) {
        // Only update the GUI flag. Workers read is_running() to decide
        // whether to process or pause — they never exit due to this flag.
        // pipeline_running is only set false on Ctrl-C shutdown in main.rs.
        self.is_running.store(v, Ordering::Relaxed);
    }

    pub fn mode_string(&self) -> String {
        match *self.mode.lock().unwrap() {
            PipelineMode::Async => "async".into(),
            PipelineMode::Threaded => "threaded".into(),
        }
    }

    pub fn set_mode(&self, m: PipelineMode) {
        *self.mode.lock().unwrap() = m;
    }

    pub fn push_feed(&self, entry: String) {
        self.live_feed.push(entry);
    }

    pub fn live_feed_snapshot(&self) -> Vec<String> {
        self.live_feed.snapshot()
    }

    /// Recomputes throughput every 0.5s.
    /// Counts PROCESSED + DROPPED so the rate stays non-zero even in degraded mode.
    pub fn throughput(&self) -> f64 {
        let mut last_time = self.last_tick_instant.lock().unwrap();
        let elapsed = last_time.elapsed().as_secs_f64();
        if elapsed >= 0.5 {
            let processed = crate::pipeline::PROCESSED_COUNT.load(Ordering::Relaxed);
            let dropped   = crate::pipeline::DROPPED_PACKETS.load(Ordering::Relaxed);
            let current   = processed + dropped;
            let prev = self.last_edit_count.swap(current, Ordering::Relaxed);
            let eps = (current.saturating_sub(prev)) as f64 / elapsed;
            *self.cached_throughput.lock().unwrap() = eps;
            *last_time = Instant::now();
        }
        *self.cached_throughput.lock().unwrap()
    }
}