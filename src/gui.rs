//! GUI module — lightweight Axum HTTP metrics server
//!
//! Replaces the egui native window with a JSON API that the React dashboard
//! (frontend/) polls every second.
//!
//! # Endpoints
//! | Method | Path           | Description                      |
//! |--------|----------------|----------------------------------|
//! | GET    | /api/metrics   | JSON snapshot of all live metrics|
//! | POST   | /api/control   | start / stop / set_mode          |
//! | GET    | /              | Plain-text hint for the browser  |
//!
//! # CORS
//! Wide-open (`*`) so the Vite dev server on :5173 can call :3000 freely.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::Method;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;

use crate::gui_state::GuiState;
use crate::metrics::MetricsHandle;
use crate::pipeline::{
    PipelineMode, DROPPED_PACKETS, MISSED_DEADLINES, PROCESSED_COUNT,
};
use crate::scheduling::{
    P50_NS, P90_NS, P99_NS, SchedulerHandle, PhaseSnapshot,
    HUMAN_DRIFT_COUNT, BOT_DRIFT_COUNT,
};
use crate::watchdog::{IN_DEGRADED_MODE, NETWORK_RESET_REQUESTED};

// ============================================================================
// Shared server state
// ============================================================================

#[derive(Clone)]
pub struct ServerState {
    pub metrics: MetricsHandle,
    pub gui_state: Arc<GuiState>,
    pub scheduler: SchedulerHandle,
    pub start_time: Instant,
    /// 0 = async phase, 1 = threaded phase, 2 = summary
    pub phase: Arc<AtomicU8>,
    pub phase_start: Arc<std::sync::Mutex<Instant>>,
    pub async_snapshot: Arc<std::sync::Mutex<Option<PhaseSnapshot>>>,
    pub threaded_snapshot: Arc<std::sync::Mutex<Option<PhaseSnapshot>>>,
}

// ============================================================================
// JSON shapes
// ============================================================================

#[derive(Serialize)]
pub struct MetricsResponse {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub throughput_eps: f64,
    pub processed_total: u64,
    pub missed_deadlines: u64,
    pub dropped_packets: u64,
    pub is_running: bool,
    pub degraded_mode: bool,
    pub network_reset_pending: bool,
    pub pipeline_mode: String,
    pub leaderboard: Vec<LeaderboardEntry>,
    pub live_feed: Vec<String>,
    pub uptime_secs: u64,
    // Priority proof: human avg drift should be lower than bot avg drift
    pub human_avg_drift_ms: f64,
    pub bot_avg_drift_ms: f64,
    pub human_processed: u64,
    pub bot_processed: u64,
    // Demo phase info
    pub phase: u8,
    pub phase_elapsed_secs: u64,
    pub phase_remaining_secs: u64,
    pub async_snapshot: Option<PhaseSnapshot>,
    pub threaded_snapshot: Option<PhaseSnapshot>,
}

#[derive(Serialize, Clone)]
pub struct LeaderboardEntry {
    pub rank: usize,
    pub domain: String,
    pub count: u32,
}

#[derive(Deserialize)]
pub struct ControlRequest {
    pub action: String,        // "start" | "stop" | "set_mode"
    pub mode: Option<String>,  // "async" | "threaded"
}

#[derive(Serialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub message: String,
}

// ============================================================================
// Handlers
// ============================================================================

async fn metrics_handler(State(state): State<ServerState>) -> Json<MetricsResponse> {
    Json(MetricsResponse {
        p50_ms:               P50_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        p90_ms:               P90_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        p99_ms:               P99_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        throughput_eps:       state.gui_state.throughput(),
        processed_total:      PROCESSED_COUNT.load(Ordering::Relaxed),
        missed_deadlines:     MISSED_DEADLINES.load(Ordering::Relaxed),
        dropped_packets:      DROPPED_PACKETS.load(Ordering::Relaxed),
        is_running:           state.gui_state.is_running(),
        degraded_mode:        IN_DEGRADED_MODE.load(Ordering::Relaxed),
        network_reset_pending: NETWORK_RESET_REQUESTED.load(Ordering::Relaxed),
        pipeline_mode:        state.gui_state.mode_string(),
        uptime_secs:          state.start_time.elapsed().as_secs(),
        leaderboard:          state.metrics.leaderboard_snapshot()
                                  .into_iter()
                                  .enumerate()
                                  .map(|(i, (d, c))| LeaderboardEntry { rank: i + 1, domain: d, count: c })
                                  .collect(),
        live_feed:            state.gui_state.live_feed_snapshot(),
        human_avg_drift_ms:   SchedulerHandle::human_avg_drift_ms(),
        bot_avg_drift_ms:     SchedulerHandle::bot_avg_drift_ms(),
        human_processed:      HUMAN_DRIFT_COUNT.load(std::sync::atomic::Ordering::Relaxed),
        bot_processed:        BOT_DRIFT_COUNT.load(std::sync::atomic::Ordering::Relaxed),
        phase: {
            state.phase.load(Ordering::Relaxed)
        },
        phase_elapsed_secs: {
            let start = state.phase_start.lock().unwrap();
            start.elapsed().as_secs()
        },
        phase_remaining_secs: {
            let phase = state.phase.load(Ordering::Relaxed);
            if phase < 2 {
                let start = state.phase_start.lock().unwrap();
                let elapsed = start.elapsed().as_secs();
                240u64.saturating_sub(elapsed)
            } else {
                0
            }
        },
        async_snapshot:    state.async_snapshot.lock().unwrap().clone(),
        threaded_snapshot: state.threaded_snapshot.lock().unwrap().clone(),
    })
}

async fn control_handler(
    State(state): State<ServerState>,
    Json(req): Json<ControlRequest>,
) -> Json<ControlResponse> {
    match req.action.as_str() {
        "start" => {
            state.gui_state.set_running(true);
            Json(ControlResponse { ok: true, message: "pipeline started".into() })
        }
        "stop" => {
            state.gui_state.set_running(false);
            Json(ControlResponse { ok: true, message: "pipeline stopped".into() })
        }
        "reset_metrics" => {
            // Call reset_all() which clears BOTH the ring buffer AND the atomics.
            // reset_metrics_static() was broken — it only zeroed the atomics,
            // so p99 would bounce back to 114ms 4 seconds later when
            // publish_percentiles() re-read the unchanged ring buffer.
            state.scheduler.reset_all();
            Json(ControlResponse { ok: true, message: "metrics reset".into() })
        }
        "set_mode" => match req.mode.as_deref() {
            Some("async")    => { state.gui_state.set_mode(PipelineMode::Async);    Json(ControlResponse { ok: true, message: "mode: async".into() }) }
            Some("threaded") => { state.gui_state.set_mode(PipelineMode::Threaded); Json(ControlResponse { ok: true, message: "mode: threaded".into() }) }
            _                => Json(ControlResponse { ok: false, message: "unknown mode (use 'async' or 'threaded')".into() }),
        },
        other => Json(ControlResponse { ok: false, message: format!("unknown action: {}", other) }),
    }
}

async fn root_handler() -> &'static str {
    "Wikipedia RT Monitor — API running.\nOpen the React dashboard at http://localhost:5173"
}

// ============================================================================
// Server entry point (called from main.rs)
// ============================================================================

pub async fn run_server(state: ServerState) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_origin(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/api/metrics", get(metrics_handler))
        .route("/api/control", post(control_handler))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    eprintln!("[gui] HTTP server listening on http://localhost:3000");
    axum::serve(listener, app).await?;
    Ok(())
}