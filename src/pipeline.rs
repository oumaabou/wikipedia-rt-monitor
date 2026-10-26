//! Pipeline module - Dual Architecture Worker Pool (Component A)
//!
//! Both architectures share the same `try_process_one()` core which:
//! 1. Drains HIGH priority (human edits) first.
//! 2. Falls through to LOW priority (bot edits) only when HIGH is empty
//!    AND the system is NOT in degraded mode.
//! 3. Pushes a formatted line to the GUI live feed after every processed packet.
//! 4. Increments DROPPED_PACKETS when a bot packet is skipped in degraded mode.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};

use crate::gui_state::GuiState;
use crate::ingestion::RawPacket;
use crate::metrics::MetricsHandle;
use crate::processing::process_packet;
use crate::scheduling::SchedulerHandle;

// ============================================================================
// Constants
// ============================================================================

pub const CHANNEL_CAPACITY: usize = 20; // reduced for demo (was 1_000)
const WORKER_COUNT: usize = 2;
const HIGH_PRIORITY_DRAIN_ITERS: usize = 8;

// ============================================================================
// Shared atomics
// ============================================================================

pub static DEGRADED_MODE: AtomicBool = AtomicBool::new(false);
pub static PROCESSED_COUNT: AtomicU64 = AtomicU64::new(0);
pub static MISSED_DEADLINES: AtomicU64 = AtomicU64::new(0);

/// Incremented when:
/// (a) ingestion's try_send fails (channel full), OR
/// (b) a bot packet is skipped because degraded mode is active.
pub static DROPPED_PACKETS: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// Channel types
// ============================================================================

#[derive(Clone)]
pub struct ChannelSenders {
    pub high_priority: Sender<RawPacket>,
    pub low_priority:  Sender<RawPacket>,
}

#[derive(Clone)]
pub struct ChannelReceivers {
    pub high_priority: Receiver<RawPacket>,
    pub low_priority:  Receiver<RawPacket>,
}

pub fn create_channels() -> (ChannelSenders, ChannelReceivers) {
    let (high_tx, high_rx) = bounded::<RawPacket>(CHANNEL_CAPACITY);
    let (low_tx,  low_rx)  = bounded::<RawPacket>(CHANNEL_CAPACITY);
    (
        ChannelSenders { high_priority: high_tx, low_priority: low_tx },
        ChannelReceivers { high_priority: high_rx, low_priority: low_rx },
    )
}

// ============================================================================
// Architecture selector
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineMode {
    Async,
    Threaded,
}

// ============================================================================
// Core worker logic
// ============================================================================

/// Process one packet end-to-end.
///
/// Returns `true` if a packet was dequeued (processed OR deliberately dropped),
/// `false` if both channels were empty (caller should back off).
fn try_process_one(
    receivers:  &ChannelReceivers,
    scheduler:  &SchedulerHandle,
    metrics:    &MetricsHandle,
    gui_state:  &GuiState,
) -> bool {
    let degraded = DEGRADED_MODE.load(Ordering::Relaxed);

    // ── 1. High priority (human edits) ───────────────────────────────────────
    match receivers.high_priority.try_recv() {
        Ok(packet) => {
            handle_packet(packet, scheduler, metrics, gui_state);
            return true;
        }
        Err(TryRecvError::Disconnected) => return false,
        Err(TryRecvError::Empty) => {} // fall through
    }

    // ── 2. Low priority (bot edits) — skip in degraded mode ──────────────────
    if degraded {
        // Drain one bot packet so the channel doesn't back up,
        // but count it as dropped instead of processing.
        match receivers.low_priority.try_recv() {
            Ok(_) => {
                DROPPED_PACKETS.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Err(_) => return false,
        }
    }

    match receivers.low_priority.try_recv() {
        Ok(packet) => {
            handle_packet(packet, scheduler, metrics, gui_state);
            true
        }
        Err(_) => false,
    }
}

/// Run one packet through processing → scheduling → metrics → live feed.
fn handle_packet(
    mut packet: RawPacket,
    scheduler:  &SchedulerHandle,
    metrics:    &MetricsHandle,
    gui_state:  &GuiState,
) {
    // Stamp the moment this worker dequeues the packet.
    // The 2ms deadline is measured from here (not from ingestion_time).
    // Queue wait = processing_start - ingestion_time (shows priority effect).
    packet.processing_start = std::time::Instant::now();

    // Demo spin: inflate processing time so deadline misses are visible
    // and priority separation is measurable. ~60µs extra per packet.
    // Remove after presentation.
    for _ in 0..5_000 { std::hint::spin_loop(); }

    if let Some(processed) = process_packet(&packet) {
        scheduler.record(&processed);
        metrics.record_edit(&processed.server_name);
        PROCESSED_COUNT.fetch_add(1, Ordering::Relaxed);

        // ── Live feed ─────────────────────────────────────────────────────────
        // Queue wait = time from SSE arrival to worker dequeue
        // This is what demonstrates human priority: humans wait less
        let queue_wait_ms = processed.processing_start
            .duration_since(processed.ingestion_time)
            .as_secs_f64() * 1000.0;
        // Processing time = time from dequeue to now (the actual 2ms deadline)
        let drift_ms = processed.processing_start.elapsed().as_secs_f64() * 1000.0;
        let _ = queue_wait_ms; // available for future display
        let icon = if processed.is_bot { "🤖" } else { "👤" };

        // Safe Unicode truncation — find the char boundary at or before byte 19.
        // Simple byte slicing panics on multi-byte chars (Cyrillic, CJK, Arabic).
        let user = truncate_unicode(processed.user, 20);
        let domain = truncate_unicode(&processed.server_name, 24);

        gui_state.push_feed(format!(
            "{} {:<21} {:<25} {:.2}ms",
            icon, user, domain, drift_ms
        ));
    }
}

// ============================================================================
// Unicode-safe string truncation helper
// ============================================================================

/// Truncate `s` to at most `max_chars` Unicode scalar values.
/// Never panics — always lands on a valid char boundary.
fn truncate_unicode(s: &str, max_chars: usize) -> String {
    let mut char_count = 0;
    let mut byte_end = s.len();
    for (byte_idx, _ch) in s.char_indices() {
        if char_count == max_chars {
            byte_end = byte_idx;
            break;
        }
        char_count += 1;
    }
    if char_count < max_chars {
        // String is shorter than max_chars — return as-is
        s.to_string()
    } else {
        format!("{}…", &s[..byte_end])
    }
}

// ============================================================================
// Async pipeline
// ============================================================================

pub fn start_async_pipeline(
    receivers:  ChannelReceivers,
    scheduler:  SchedulerHandle,
    metrics:    MetricsHandle,
    gui_state:  Arc<GuiState>,
    running:    Arc<AtomicBool>,
) {
    let high_rx = Arc::new(receivers.high_priority);
    let low_rx  = Arc::new(receivers.low_priority);

    for worker_id in 0..WORKER_COUNT {
        let high_rx   = Arc::clone(&high_rx);
        let low_rx    = Arc::clone(&low_rx);
        let scheduler = scheduler.clone();
        let metrics   = metrics.clone();
        let gui_state = Arc::clone(&gui_state);
        let running   = Arc::clone(&running);

        tokio::spawn(async move {
            eprintln!("[pipeline] async worker {} started", worker_id);
            while running.load(Ordering::Relaxed) {
                // PAUSE check: if gui_state says stopped, yield instead of processing.
                // Workers stay alive so START can resume them instantly.
                if !gui_state.is_running() {
                    tokio::task::yield_now().await;
                    continue;
                }
                let rx_view = ChannelReceivers {
                    high_priority: (*high_rx).clone(),
                    low_priority:  (*low_rx).clone(),
                };
                let mut did_work = false;
                for _ in 0..HIGH_PRIORITY_DRAIN_ITERS {
                    if try_process_one(&rx_view, &scheduler, &metrics, &gui_state) {
                        did_work = true;
                    } else {
                        break;
                    }
                }
                if !did_work {
                    tokio::task::yield_now().await;
                }
            }
            eprintln!("[pipeline] async worker {} stopped", worker_id);
        });
    }
}

// ============================================================================
// Threaded pipeline
// ============================================================================

pub fn start_threaded_pipeline(
    receivers:  ChannelReceivers,
    scheduler:  SchedulerHandle,
    metrics:    MetricsHandle,
    gui_state:  Arc<GuiState>,
    running:    Arc<AtomicBool>,
) {
    let high_rx = Arc::new(receivers.high_priority);
    let low_rx  = Arc::new(receivers.low_priority);

    for worker_id in 0..WORKER_COUNT {
        let high_rx   = Arc::clone(&high_rx);
        let low_rx    = Arc::clone(&low_rx);
        let scheduler = scheduler.clone();
        let metrics   = metrics.clone();
        let gui_state = Arc::clone(&gui_state);
        let running   = Arc::clone(&running);

        std::thread::Builder::new()
            .name(format!("pipeline-worker-{}", worker_id))
            .spawn(move || {
                eprintln!("[pipeline] threaded worker {} started", worker_id);
                while running.load(Ordering::Relaxed) {
                    // PAUSE check: sleep briefly instead of processing.
                    // Workers stay alive so START can resume them instantly.
                    if !gui_state.is_running() {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        continue;
                    }
                    let rx_view = ChannelReceivers {
                        high_priority: (*high_rx).clone(),
                        low_priority:  (*low_rx).clone(),
                    };
                    let mut did_work = false;
                    for _ in 0..HIGH_PRIORITY_DRAIN_ITERS {
                        if try_process_one(&rx_view, &scheduler, &metrics, &gui_state) {
                            did_work = true;
                        } else {
                            break;
                        }
                    }
                    if !did_work {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                }
                eprintln!("[pipeline] threaded worker {} stopped", worker_id);
            })
            .expect("failed to spawn worker thread");
    }
}

// ============================================================================
// Unified entry point — now takes Arc<GuiState>
// ============================================================================

pub fn start_pipeline(
    mode:      PipelineMode,
    receivers: ChannelReceivers,
    scheduler: SchedulerHandle,
    metrics:   MetricsHandle,
    gui_state: Arc<GuiState>,
    running:   Arc<AtomicBool>,
) {
    match mode {
        PipelineMode::Async    => start_async_pipeline(receivers, scheduler, metrics, gui_state, running),
        PipelineMode::Threaded => start_threaded_pipeline(receivers, scheduler, metrics, gui_state, running),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Instant;

    fn dummy_packet(is_bot: bool, user: &str, domain: &str) -> RawPacket {
        let json = format!(
            r#"{{"user":"{}","bot":{},"server_name":"{}","type":"edit"}}"#,
            user, is_bot, domain
        );
        let t = Instant::now();
        RawPacket {
            data: Bytes::from(json.into_bytes()),
            ingestion_time: t,
            processing_start: t,
            is_bot,
            server_name: domain.to_string(),
        }
    }

    #[test]
    fn test_create_channels() {
        let (senders, receivers) = create_channels();
        senders.high_priority.try_send(dummy_packet(false, "Alice", "en.wikipedia.org")).unwrap();
        let received = receivers.high_priority.try_recv().unwrap();
        assert!(!received.is_bot);
    }

    #[test]
    fn test_channel_bounded() {
        let (senders, _receivers) = create_channels();
        let mut sent = 0;
        let mut dropped = 0;
        for i in 0..(CHANNEL_CAPACITY + 50) {
            let pkt = dummy_packet(false, &format!("User{}", i), "en.wikipedia.org");
            if senders.high_priority.try_send(pkt).is_ok() { sent += 1; } else { dropped += 1; }
        }
        assert_eq!(sent, CHANNEL_CAPACITY);
        assert_eq!(dropped, 50);
    }

    #[test]
    fn test_degraded_increments_dropped() {
        let (senders, receivers) = create_channels();
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let gs = GuiState::new(Arc::clone(&running));
        senders.low_priority.try_send(dummy_packet(true, "BotA", "en.wikipedia.org")).unwrap();

        DEGRADED_MODE.store(true, Ordering::SeqCst);
        let before = DROPPED_PACKETS.load(Ordering::Relaxed);

        let rx_view = ChannelReceivers {
            high_priority: receivers.high_priority.clone(),
            low_priority:  receivers.low_priority.clone(),
        };
        try_process_one(&rx_view, &SchedulerHandle::new(), &MetricsHandle::new(), &gs);

        assert_eq!(DROPPED_PACKETS.load(Ordering::Relaxed), before + 1);
        DEGRADED_MODE.store(false, Ordering::SeqCst);
    }
}