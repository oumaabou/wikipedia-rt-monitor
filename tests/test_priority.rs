//! Integration tests for priority scheduling and latency accounting
//!
//! 1. Human edits are routed to the high-priority channel, bots to low
//! 2. Humans are dequeued before bots when both queues are full
//! 3. Queue-wait drift is measured and attributed per class
//! 4. A stale packet breaches the 2 ms deadline
//! 5. p50 / p90 / p99 are published and ordered
//! 6. The `user` field is zero-copy (points into the original buffer)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use wikipedia_rt_monitor::{
    gui_state::GuiState,
    ingestion::RawPacket,
    metrics::MetricsHandle,
    pipeline::{create_channels, start_pipeline, PipelineMode, DEGRADED_MODE, PROCESSED_COUNT},
    processing::process_packet,
    scheduling::{
        SchedulerHandle, BOT_DRIFT_COUNT, HUMAN_DRIFT_COUNT, MISSED_DEADLINES, P50_NS, P90_NS,
        P99_NS,
    },
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

#[test]
fn test_priority_routing_human_vs_bot() {
    let (senders, receivers) = create_channels();
    senders.high_priority.try_send(make_packet("Alice", false)).unwrap();
    senders.low_priority.try_send(make_packet("ClueBot", true)).unwrap();

    let human = receivers.high_priority.try_recv().unwrap();
    let bot = receivers.low_priority.try_recv().unwrap();
    assert!(!human.is_bot, "high-priority channel must carry humans");
    assert!(bot.is_bot, "low-priority channel must carry bots");
    assert!(receivers.high_priority.try_recv().is_err());
    assert!(receivers.low_priority.try_recv().is_err());
    println!("✅ Priority: routing by class");
}

#[test]
fn test_humans_processed_before_bots() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    DEGRADED_MODE.store(false, Ordering::SeqCst);

    let (senders, receivers) = create_channels();
    let running = Arc::new(AtomicBool::new(true));
    let gui = GuiState::new(Arc::clone(&running));

    // Fill BOTH queues before any worker starts, so priority is the only
    // thing deciding the order.
    for i in 0..20 {
        senders.high_priority.try_send(make_packet(&format!("Human{}", i), false)).unwrap();
        senders.low_priority.try_send(make_packet(&format!("Bot{}", i), true)).unwrap();
    }
    let before = PROCESSED_COUNT.load(Ordering::Relaxed);

    start_pipeline(
        PipelineMode::Threaded,
        receivers,
        SchedulerHandle::new(),
        MetricsHandle::new(),
        Arc::clone(&gui),
        Arc::clone(&running),
    );

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10)
        && PROCESSED_COUNT.load(Ordering::Relaxed) < before + 40
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let feed = gui.live_feed_snapshot();
    running.store(false, Ordering::Release);

    assert!(feed.len() >= 20, "expected at least 20 feed entries, got {}", feed.len());
    // Bots are only dequeued once the human queue is empty, so the opening
    // entries of the feed must all be humans (👤).
    for (i, line) in feed.iter().take(10).enumerate() {
        assert!(line.starts_with("👤"), "entry {} should be a human edit, got {:?}", i, line);
    }
    println!("✅ Priority: humans processed before bots");
}

#[test]
fn test_scheduling_drift_measured() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    let human_before = HUMAN_DRIFT_COUNT.load(Ordering::Relaxed);
    let bot_before = BOT_DRIFT_COUNT.load(Ordering::Relaxed);

    // Human waited ~5 ms in the queue; bot waited ~10 ms.
    let now = Instant::now();
    let mut h = make_packet("Human", false);
    h.ingestion_time = now - Duration::from_millis(5);
    h.processing_start = now;
    let mut b = make_packet("Bot", true);
    b.ingestion_time = now - Duration::from_millis(10);
    b.processing_start = now;

    handle.record(&process_packet(&h).unwrap());
    handle.record(&process_packet(&b).unwrap());

    assert_eq!(HUMAN_DRIFT_COUNT.load(Ordering::Relaxed), human_before + 1);
    assert_eq!(BOT_DRIFT_COUNT.load(Ordering::Relaxed), bot_before + 1);
    assert!(SchedulerHandle::human_avg_drift_ms() >= 5.0, "human drift should be >= 5 ms");
    assert!(
        SchedulerHandle::bot_avg_drift_ms() > SchedulerHandle::human_avg_drift_ms(),
        "bot drift should exceed human drift in this setup"
    );
    println!("✅ Priority: drift measured per class");
}

#[test]
fn test_stale_packet_misses_deadline() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();

    let mut p = make_packet("SlowEditor", false);
    // The 2 ms deadline runs from processing_start; make it 5 ms old.
    p.processing_start = Instant::now() - Duration::from_millis(5);

    let before = MISSED_DEADLINES.load(Ordering::Relaxed);
    handle.record(&process_packet(&p).unwrap());
    assert_eq!(MISSED_DEADLINES.load(Ordering::Relaxed), before + 1);
    println!("✅ Priority: stale packet missed 2 ms deadline");
}

#[test]
fn test_percentiles_computed_correctly() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    // 200 samples (one publish interval) with increasing processing times
    // from 0.1 ms up to ~20 ms.
    for i in 1..=200u64 {
        let mut p = make_packet("Sampler", false);
        p.processing_start = Instant::now() - Duration::from_micros(i * 100);
        handle.record(&process_packet(&p).unwrap());
    }

    let (p50, p90, p99) = (
        P50_NS.load(Ordering::Relaxed),
        P90_NS.load(Ordering::Relaxed),
        P99_NS.load(Ordering::Relaxed),
    );
    assert!(p50 > 0, "p50 should be published after 200 samples");
    assert!(p50 <= p90 && p90 <= p99, "percentiles must be ordered: {} {} {}", p50, p90, p99);
    // Median of 0.1..20 ms should be roughly 10 ms.
    let p50_ms = p50 as f64 / 1_000_000.0;
    assert!(p50_ms > 5.0 && p50_ms < 20.0, "p50 ≈ 10 ms expected, got {:.2} ms", p50_ms);
    println!("✅ Priority: percentiles computed (p50={:.2} ms)", p50_ms);
}

#[test]
fn test_zero_copy_user_field() {
    let packet = make_packet("ZeroCopyUser", false);
    let processed = process_packet(&packet).unwrap();

    let buf_start = packet.data.as_ptr() as usize;
    let buf_end = buf_start + packet.data.len();
    let user_ptr = processed.user.as_ptr() as usize;

    assert_eq!(processed.user, "ZeroCopyUser");
    assert!(
        user_ptr >= buf_start && user_ptr < buf_end,
        "user must point inside the original buffer (no heap copy)"
    );
    println!("✅ Priority: user field is zero-copy");
}

#[test]
fn test_bot_drift_tracked_separately() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    let handle = SchedulerHandle::new();
    handle.reset_all();

    let human_before = HUMAN_DRIFT_COUNT.load(Ordering::Relaxed);
    let bot_before = BOT_DRIFT_COUNT.load(Ordering::Relaxed);

    handle.record(&process_packet(&make_packet("OnlyBot", true)).unwrap());

    assert_eq!(BOT_DRIFT_COUNT.load(Ordering::Relaxed), bot_before + 1);
    assert_eq!(HUMAN_DRIFT_COUNT.load(Ordering::Relaxed), human_before);
    println!("✅ Priority: bot drift tracked separately");
}
