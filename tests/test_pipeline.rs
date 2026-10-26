//! Integration tests for the worker pipeline (Component A)
//!
//! 1. Channels are bounded at CHANNEL_CAPACITY (back-pressure, no unbounded growth)
//! 2. Threaded pipeline processes packets end-to-end
//! 3. Async pipeline processes packets end-to-end
//! 4. Processed packets appear in the GUI live feed
//! 5. Degraded mode drops bot packets instead of processing them

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use wikipedia_rt_monitor::{
    gui_state::GuiState,
    ingestion::RawPacket,
    metrics::MetricsHandle,
    pipeline::{
        create_channels, start_pipeline, PipelineMode, CHANNEL_CAPACITY, DEGRADED_MODE,
        DROPPED_PACKETS, PROCESSED_COUNT,
    },
    scheduling::SchedulerHandle,
};

// Tests share global atomics (DEGRADED_MODE, PROCESSED_COUNT, ...), so serialise them.
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

/// Poll `cond` until it is true or `timeout` elapses.
fn wait_until(timeout: Duration, cond: impl Fn() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    cond()
}

#[test]
fn test_channel_capacity_bounded() {
    let (senders, _receivers) = create_channels();
    let mut accepted = 0;
    let mut rejected = 0;
    for i in 0..(CHANNEL_CAPACITY + 30) {
        let pkt = make_packet(&format!("User{}", i), false);
        if senders.high_priority.try_send(pkt).is_ok() {
            accepted += 1;
        } else {
            rejected += 1;
        }
    }
    assert_eq!(accepted, CHANNEL_CAPACITY, "channel must accept exactly its capacity");
    assert_eq!(rejected, 30, "overflow must be rejected, not buffered");
    println!("✅ Pipeline: channel bounded at {}", CHANNEL_CAPACITY);
}

#[test]
fn test_threaded_pipeline_processes_packets() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    DEGRADED_MODE.store(false, Ordering::SeqCst);

    let (senders, receivers) = create_channels();
    let running = Arc::new(AtomicBool::new(true));
    let gui = GuiState::new(Arc::clone(&running));

    let before = PROCESSED_COUNT.load(Ordering::Relaxed);
    for i in 0..10 {
        senders.high_priority.try_send(make_packet(&format!("Human{}", i), false)).unwrap();
    }

    start_pipeline(
        PipelineMode::Threaded,
        receivers,
        SchedulerHandle::new(),
        MetricsHandle::new(),
        Arc::clone(&gui),
        Arc::clone(&running),
    );

    let ok = wait_until(Duration::from_secs(5), || {
        PROCESSED_COUNT.load(Ordering::Relaxed) >= before + 10
    });
    running.store(false, Ordering::Release);

    assert!(ok, "threaded workers should process all 10 packets");
    println!("✅ Pipeline: threaded workers processed packets");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_async_pipeline_processes_packets() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    DEGRADED_MODE.store(false, Ordering::SeqCst);

    let (senders, receivers) = create_channels();
    let running = Arc::new(AtomicBool::new(true));
    let gui = GuiState::new(Arc::clone(&running));

    let before = PROCESSED_COUNT.load(Ordering::Relaxed);
    for i in 0..10 {
        senders.high_priority.try_send(make_packet(&format!("Human{}", i), false)).unwrap();
    }

    start_pipeline(
        PipelineMode::Async,
        receivers,
        SchedulerHandle::new(),
        MetricsHandle::new(),
        Arc::clone(&gui),
        Arc::clone(&running),
    );

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5)
        && PROCESSED_COUNT.load(Ordering::Relaxed) < before + 10
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let processed = PROCESSED_COUNT.load(Ordering::Relaxed) - before;
    running.store(false, Ordering::Release);

    assert!(processed >= 10, "async workers should process all 10 packets (got {})", processed);
    println!("✅ Pipeline: async workers processed packets");
}

#[test]
fn test_live_feed_populated() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    DEGRADED_MODE.store(false, Ordering::SeqCst);

    let (senders, receivers) = create_channels();
    let running = Arc::new(AtomicBool::new(true));
    let gui = GuiState::new(Arc::clone(&running));

    senders.high_priority.try_send(make_packet("FeedTester", false)).unwrap();

    start_pipeline(
        PipelineMode::Threaded,
        receivers,
        SchedulerHandle::new(),
        MetricsHandle::new(),
        Arc::clone(&gui),
        Arc::clone(&running),
    );

    let ok = wait_until(Duration::from_secs(5), || !gui.live_feed_snapshot().is_empty());
    let feed = gui.live_feed_snapshot();
    running.store(false, Ordering::Release);

    assert!(ok, "live feed should receive an entry after a packet is processed");
    assert!(feed[0].contains("FeedTester"), "feed entry should show the username: {:?}", feed[0]);
    println!("✅ Pipeline: live feed populated");
}

#[test]
fn test_degraded_mode_drops_bots() {
    let _g = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

    let (senders, receivers) = create_channels();
    let running = Arc::new(AtomicBool::new(true));
    let gui = GuiState::new(Arc::clone(&running));

    DEGRADED_MODE.store(true, Ordering::SeqCst);
    let dropped_before = DROPPED_PACKETS.load(Ordering::Relaxed);
    let processed_before = PROCESSED_COUNT.load(Ordering::Relaxed);

    for i in 0..5 {
        senders.low_priority.try_send(make_packet(&format!("Bot{}", i), true)).unwrap();
    }

    start_pipeline(
        PipelineMode::Threaded,
        receivers,
        SchedulerHandle::new(),
        MetricsHandle::new(),
        Arc::clone(&gui),
        Arc::clone(&running),
    );

    let ok = wait_until(Duration::from_secs(5), || {
        DROPPED_PACKETS.load(Ordering::Relaxed) >= dropped_before + 5
    });
    let processed_after = PROCESSED_COUNT.load(Ordering::Relaxed);

    running.store(false, Ordering::Release);
    DEGRADED_MODE.store(false, Ordering::SeqCst);

    assert!(ok, "all 5 bot packets should be counted as dropped in degraded mode");
    assert_eq!(processed_after, processed_before, "bots must NOT be processed in degraded mode");
    println!("✅ Pipeline: degraded mode dropped bot packets");
}
