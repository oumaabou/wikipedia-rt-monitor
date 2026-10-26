//! main.rs - Entry point

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use wikipedia_rt_monitor::{
    gui::{run_server, ServerState},
    gui_state::GuiState,
    ingestion::{self, LAST_PACKET_TIMESTAMP},
    metrics::MetricsHandle,
    pipeline::{create_channels, start_pipeline, PipelineMode},
    scheduling::SchedulerHandle,
    utils::{ensure_log_dirs, CliConfig},
    watchdog,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. CLI ────────────────────────────────────────────────────────────────
    let cli = CliConfig::from_args();

    // ── 2. Logging ────────────────────────────────────────────────────────────
    ensure_log_dirs();
    ingestion::init_async_logger();
    eprintln!("[main] Wikipedia RT Monitor starting");

    // ── 3. Channels ───────────────────────────────────────────────────────────
    let (senders, receivers) = create_channels();

    // ── 4. Shared handles ─────────────────────────────────────────────────────
    let metrics_handle:   MetricsHandle   = MetricsHandle::new();
    let scheduler_handle: SchedulerHandle = SchedulerHandle::new();
    // running is created first so GuiState can hold a clone —
    // the START/STOP button then directly controls the pipeline workers.
    let running:          Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    let gui_state:        Arc<GuiState>   = GuiState::new(Arc::clone(&running));

    // ── Demo phase tracking ────────────────────────────────────────────────────
    let phase             = Arc::new(AtomicU8::new(0u8));
    let phase_start       = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
    let async_snapshot    : Arc<std::sync::Mutex<Option<wikipedia_rt_monitor::scheduling::PhaseSnapshot>>> = Arc::new(std::sync::Mutex::new(None));
    let threaded_snapshot : Arc<std::sync::Mutex<Option<wikipedia_rt_monitor::scheduling::PhaseSnapshot>>> = Arc::new(std::sync::Mutex::new(None));

    // ── 5. Watchdog ───────────────────────────────────────────────────────────
    watchdog::start_watchdog(Arc::clone(&running), &LAST_PACKET_TIMESTAMP, scheduler_handle.clone());

    // ── 6. Pipeline (now receives gui_state for live feed) ────────────────────
    let pipeline_mode: PipelineMode = if cli.use_threaded {
        PipelineMode::Threaded
    } else {
        PipelineMode::Async
    };
    gui_state.set_mode(pipeline_mode);

    start_pipeline(
        pipeline_mode,
        receivers,
        scheduler_handle.clone(),
        metrics_handle.clone(),
        Arc::clone(&gui_state),   // ← wired in so workers can push feed lines
        Arc::clone(&running),
    );

    // ── 7. Ingestion ──────────────────────────────────────────────────────────
    if cli.run_benchmark {
        wikipedia_rt_monitor::metrics::run_sync_benchmark();
    }

    let high_tx = senders.high_priority.clone();
    let low_tx  = senders.low_priority.clone();

    match (cli.replay_file.clone(), cli.save_file.clone()) {
        (Some(path), _) => {
            eprintln!("[main] replay mode: {}", path);
            std::thread::Builder::new()
                .name("ingestion-replay".into())
                .spawn(move || {
                    if let Err(e) = ingestion::run_replay_mode(high_tx, low_tx, &path) {
                        eprintln!("[main] replay error: {}", e);
                    }
                })
                .expect("failed to spawn replay thread");
        }
        (None, Some(path)) => {
            eprintln!("[main] save mode: {}", path);
            tokio::spawn(async move {
                if let Err(e) = ingestion::run_save_mode(high_tx, low_tx, &path).await {
                    eprintln!("[main] save error: {}", e);
                }
            });
        }
        (None, None) => {
            eprintln!("[main] live mode: connecting to Wikipedia SSE…");
            tokio::spawn(async move {
                if let Err(e) = ingestion::run_live_mode(high_tx, low_tx).await {
                    eprintln!("[main] ingestion error: {}", e);
                }
            });
        }
    }

    // ── 8. HTTP server ────────────────────────────────────────────────────────
    let server_state = ServerState {
        metrics:    metrics_handle,
        gui_state:  Arc::clone(&gui_state),
        scheduler:  scheduler_handle.clone(),
        start_time: std::time::Instant::now(),
        phase:             Arc::clone(&phase),
        phase_start:       Arc::clone(&phase_start),
        async_snapshot:    Arc::clone(&async_snapshot),
        threaded_snapshot: Arc::clone(&threaded_snapshot),
    };

    // ── Phase controller ──────────────────────────────────────────────────────
    {
        let phase             = Arc::clone(&phase);
        let phase_start       = Arc::clone(&phase_start);
        let async_snapshot    = Arc::clone(&async_snapshot);
        let threaded_snapshot = Arc::clone(&threaded_snapshot);
        let scheduler         = scheduler_handle.clone();
        let gui_state_demo    = Arc::clone(&gui_state);

        tokio::spawn(async move {
            const PHASE_SECS: u64 = 240;
            eprintln!("[demo] Phase 1 ASYNC — {} seconds", PHASE_SECS);
            tokio::time::sleep(std::time::Duration::from_secs(PHASE_SECS)).await;

            // snapshot BEFORE reset
            let p_async = wikipedia_rt_monitor::pipeline::PROCESSED_COUNT.load(Ordering::Relaxed);
            let d_async = wikipedia_rt_monitor::pipeline::DROPPED_PACKETS.load(Ordering::Relaxed);
            let m_async = wikipedia_rt_monitor::pipeline::MISSED_DEADLINES.load(Ordering::Relaxed);
            *async_snapshot.lock().unwrap() = Some(scheduler.take_snapshot(
                d_async, p_async, m_async, PHASE_SECS, p_async as f64 / PHASE_SECS as f64,
            ));
            eprintln!("[demo] async snapshot taken — resetting + switching to threaded");

            scheduler.reset_all();
            gui_state_demo.set_mode(wikipedia_rt_monitor::pipeline::PipelineMode::Threaded);
            phase.store(1, Ordering::Release);
            *phase_start.lock().unwrap() = std::time::Instant::now();
            eprintln!("[demo] Phase 2 THREADED — {} seconds", PHASE_SECS);

            tokio::time::sleep(std::time::Duration::from_secs(PHASE_SECS)).await;

            let p_end = wikipedia_rt_monitor::pipeline::PROCESSED_COUNT.load(Ordering::Relaxed);
            let d_end = wikipedia_rt_monitor::pipeline::DROPPED_PACKETS.load(Ordering::Relaxed);
            let m_end = wikipedia_rt_monitor::pipeline::MISSED_DEADLINES.load(Ordering::Relaxed);
            let tp = p_end.saturating_sub(p_async);
            let td = d_end.saturating_sub(d_async);
            let tm = m_end.saturating_sub(m_async);
            *threaded_snapshot.lock().unwrap() = Some(scheduler.take_snapshot(
                td, tp, tm, PHASE_SECS, tp as f64 / PHASE_SECS as f64,
            ));
            eprintln!("[demo] threaded snapshot taken — entering summary");

            phase.store(2, Ordering::Release);
            gui_state_demo.set_running(false);
            eprintln!("[demo] Summary phase — demo complete");
        });
    }

    eprintln!("[main] HTTP server → http://localhost:3000");
    eprintln!("[main] React dashboard → http://localhost:5173");

    tokio::select! {
        result = run_server(server_state) => {
            if let Err(e) = result { eprintln!("[main] server error: {}", e); }
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("[main] Ctrl-C — shutting down");
        }
    }

    // ── 9. Shutdown ───────────────────────────────────────────────────────────
    running.store(false, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    eprintln!(
        "[main] done — processed={} missed={} dropped={}",
        wikipedia_rt_monitor::pipeline::PROCESSED_COUNT.load(Ordering::Relaxed),
        wikipedia_rt_monitor::pipeline::MISSED_DEADLINES.load(Ordering::Relaxed),
        wikipedia_rt_monitor::pipeline::DROPPED_PACKETS.load(Ordering::Relaxed),
    );
    Ok(())
}