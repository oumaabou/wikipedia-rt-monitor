//! Ingestion module - Wikipedia SSE stream consumer
//!
//! # Real SSE wire format (from stream.wikimedia.org)
//!
//! ```text
//! :ok                          ← keep-alive comment, ignore
//!
//! event: message               ← event type, ignore
//! id: [{"topic":...}]          ← partition id, ignore
//! data: {"$schema":...,...}    ← THE JSON we want
//!                              ← empty line → dispatch event
//! ```
//!
//! Parsing rules (RFC 8895 §9.2):
//! - Lines starting with `:` are comments → skip.
//! - `event:` and `id:` lines → skip (we don't need them).
//! - `data:` lines → strip prefix + one optional space, append to buffer.
//! - Empty line → dispatch accumulated buffer as one complete event, reset.
//!
//! # Design
//! - `bytes::Bytes` for zero-copy across thread boundaries.
//! - Byte-level field extraction in the hot path (no JSON parse).
//! - Non-blocking `try_send`; overflow logged via off-thread logger.
//! - Exponential-backoff reconnection on any network failure.
//! - Replay mode (JSONL file) for valgrind / offline testing.
//! - Save mode (live → JSONL + forward) for building test corpora.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use crossbeam_channel::{unbounded, Sender};
use crate::pipeline::DROPPED_PACKETS;
use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::time::sleep;
use tokio_util::io::StreamReader;

// ============================================================================
// Constants
// ============================================================================

const LINE_BUFFER_CAPACITY: usize = 8_192;
const BACKOFF_BASE_SECS: u64 = 1;
const BACKOFF_MAX_SECS: u64 = 60;
const WIKIPEDIA_SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

// ============================================================================
// Watchdog heartbeat
// ============================================================================

/// Microseconds since UNIX epoch of the last received SSE event.
/// Written here, read by `watchdog.rs`.
pub static LAST_PACKET_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// Off-thread async logger
// ============================================================================

enum LogMessage {
    Overflow {
        timestamp_us: u128,
        channel: &'static str,
        packet_age_ms: f64,
    },
    Info(String),
    Warn(String),
}

static LOG_TX: once_cell::sync::OnceCell<crossbeam_channel::Sender<LogMessage>> =
    once_cell::sync::OnceCell::new();

/// Call once at program start before any ingestion begins.
pub fn init_async_logger() {
    let (tx, rx) = unbounded::<LogMessage>();
    let _ = LOG_TX.set(tx); // ignore error if already set (e.g. in tests)

    std::thread::Builder::new()
        .name("logger".into())
        .spawn(move || {
            // Bring Write into scope for writeln! and write_all
            use std::io::Write;

            let _ = std::fs::create_dir_all("logs");

            let mut overflow_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("logs/overflow_events.jsonl")
                .ok();

            let mut info_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("logs/ingestion.log")
                .ok();

            for msg in rx {
                match msg {
                    LogMessage::Overflow { timestamp_us, channel, packet_age_ms } => {
                        let line = format!(
                            "{{\"ts\":{},\"event\":\"overflow\",\"ch\":\"{}\",\"age_ms\":{:.3}}}\n",
                            timestamp_us, channel, packet_age_ms
                        );
                        if let Some(f) = overflow_file.as_mut() {
                            let _ = f.write_all(line.as_bytes());
                        }
                        eprint!("[overflow] {}", line);
                    }
                    LogMessage::Info(m) => {
                        if let Some(f) = info_file.as_mut() {
                            let _ = writeln!(f, "[INFO] {}", m);
                        }
                        eprintln!("[ingestion] {}", m);
                    }
                    LogMessage::Warn(m) => {
                        if let Some(f) = info_file.as_mut() {
                            let _ = writeln!(f, "[WARN] {}", m);
                        }
                        eprintln!("[ingestion] WARN: {}", m);
                    }
                }
            }
        })
        .expect("failed to spawn logger thread");
}

#[inline]
fn log_overflow(ingestion_time: Instant, channel: &'static str) {
    if let Some(tx) = LOG_TX.get() {
        let _ = tx.try_send(LogMessage::Overflow {
            timestamp_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
            channel,
            packet_age_ms: ingestion_time.elapsed().as_micros() as f64 / 1_000.0,
        });
    }
}

#[inline]
fn log_info(msg: impl Into<String>) {
    if let Some(tx) = LOG_TX.get() {
        let _ = tx.try_send(LogMessage::Info(msg.into()));
    }
}

#[inline]
fn log_warn(msg: impl Into<String>) {
    if let Some(tx) = LOG_TX.get() {
        let _ = tx.try_send(LogMessage::Warn(msg.into()));
    }
}

// ============================================================================
// RawPacket
// ============================================================================

/// A complete SSE `data:` payload, ready to hand off to the pipeline.
///
/// # Zero-Copy Guarantee
/// `data` is `bytes::Bytes`: an `Arc`-backed immutable byte slice.
/// Cloning it increments a reference count — no `memcpy`.
#[derive(Debug, Clone)]
pub struct RawPacket {
    /// Raw JSON from the SSE `data:` field.
    pub data: Bytes,
    /// Monotonic timestamp taken at the moment the SSE event arrived.
    /// Used to measure total queue wait time (priority proof).
    pub ingestion_time: Instant,
    /// Stamped by the pipeline worker the instant it dequeues this packet.
    /// The 2ms deadline is measured from here — this is when processing starts.
    /// None until a worker picks it up.
    pub processing_start: Instant,
    /// Pre-extracted from `"bot":true/false` — no full JSON parse.
    pub is_bot: bool,
    /// Pre-extracted `"server_name":"..."` value.
    pub server_name: String,
}

// ============================================================================
// Fast-path byte-level field extraction
// ============================================================================

/// Search for `pattern` inside `haystack` using a sliding window.
#[inline]
fn find_bytes(haystack: &[u8], pattern: &[u8]) -> Option<usize> {
    if pattern.is_empty() || haystack.len() < pattern.len() {
        return None;
    }
    haystack.windows(pattern.len()).position(|w| w == pattern)
}

/// Extract `"bot":true` / `"bot":false` without a JSON parser.
/// Returns `false` if absent.
pub fn fast_path_is_bot(data: &[u8]) -> bool {
    if let Some(pos) = find_bytes(data, b"\"bot\":") {
        let mut i = pos + 6;
        while i < data.len() && matches!(data[i], b' ' | b'\t') {
            i += 1;
        }
        if data.get(i..i + 4) == Some(b"true") {
            return true;
        }
    }
    false
}

/// Extract the value of `"server_name":"..."` without a JSON parser.
/// Returns `"unknown"` if absent.
pub fn fast_path_server_name(data: &[u8]) -> String {
    const KEY: &[u8] = b"\"server_name\":\"";
    if let Some(pos) = find_bytes(data, KEY) {
        let start = pos + KEY.len();
        if let Some(len) = data[start..].iter().position(|&b| b == b'"') {
            return String::from_utf8_lossy(&data[start..start + len]).into_owned();
        }
    }
    "unknown".to_string()
}

// ============================================================================
// SSE accumulator
// ============================================================================

/// Stateful accumulator that reconstructs one SSE event from its lines.
struct SseAccumulator {
    buf: String,
    in_event: bool,
}

impl SseAccumulator {
    fn new() -> Self {
        Self {
            buf: String::with_capacity(LINE_BUFFER_CAPACITY),
            in_event: false,
        }
    }

    /// Feed one line (trailing `\r\n` / `\n` already stripped by the caller).
    ///
    /// Returns `Some(Bytes)` when an empty line terminates a complete event.
    fn feed(&mut self, line: &str) -> Option<Bytes> {
        // Empty line → dispatch
        if line.is_empty() {
            if self.in_event && !self.buf.is_empty() {
                let event = Bytes::copy_from_slice(self.buf.as_bytes());
                self.buf.clear();
                self.in_event = false;
                return Some(event);
            }
            return None;
        }

        // Comment (`:ok`, heartbeat) → ignore
        if line.starts_with(':') {
            return None;
        }

        self.in_event = true;

        // `data: <json>` → accumulate
        if let Some(rest) = line.strip_prefix("data:") {
            let content = rest.strip_prefix(' ').unwrap_or(rest);
            if !self.buf.is_empty() {
                self.buf.push('\n');
            }
            self.buf.push_str(content);
        }
        // `event:` and `id:` → silently ignored

        None
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.in_event = false;
    }
}

// ============================================================================
// Shared dispatch
// ============================================================================

/// Stamp, classify, and route one complete SSE event.
fn dispatch(event_data: Bytes, high_tx: &Sender<RawPacket>, low_tx: &Sender<RawPacket>) {
    let ingestion_time = Instant::now();

    // Watchdog heartbeat
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    LAST_PACKET_TIMESTAMP.store(now_us, Ordering::Release);

    let is_bot = fast_path_is_bot(&event_data);
    let server_name = fast_path_server_name(&event_data);

    let packet = RawPacket {
        data: event_data,
        ingestion_time,
        processing_start: ingestion_time, // overwritten by worker on dequeue
        is_bot,
        server_name,
    };

    // Human edits → high priority; bot edits → low priority
    let result = if is_bot {
        low_tx.try_send(packet)
    } else {
        high_tx.try_send(packet)
    };

    if result.is_err() {
        // Channel full — drop & log (never block the SSE reader)
        DROPPED_PACKETS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log_overflow(
            ingestion_time,
            if is_bot { "low_priority" } else { "high_priority" },
        );
    }
}

/// Channel-depth-aware backpressure.
///
/// Called after every dispatched event. When channels fill up, we
/// slow down reading from Wikipedia — this keeps queue wait times low
/// so packets never sit for hundreds of milliseconds before a worker
/// picks them up. Better to slow ingestion than to drop packets or
/// accumulate stale drift.
///
/// Thresholds (combined depth of both channels, cap=2000 total):
///   0–25%  → no pause (full speed)
///   25–50% → 2ms pause
///   50–75% → 10ms pause
///   75%+   → 25ms pause
#[inline]
pub async fn backpressure_pause(high_tx: &Sender<RawPacket>, low_tx: &Sender<RawPacket>) {
    let depth = high_tx.len() + low_tx.len();
    let capacity = crate::pipeline::CHANNEL_CAPACITY * 2; // both channels

    let pause_ms: u64 = if depth < capacity / 4 {
        0
    } else if depth < capacity / 2 {
        2
    } else if depth < (capacity * 3) / 4 {
        10
    } else {
        25
    };

    if pause_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(pause_ms)).await;
    }
}

// ============================================================================
// Core async SSE reader
// ============================================================================

async fn read_sse_stream(
    reader: impl tokio::io::AsyncBufRead + Unpin,
    high_tx: &Sender<RawPacket>,
    low_tx: &Sender<RawPacket>,
) -> Result<()> {
    let mut acc = SseAccumulator::new();
    let mut lines = TokioBufReader::new(reader).lines();

    while let Some(result) = lines.next_line().await.transpose() {
        match result {
            Ok(line) => {
                if let Some(event) = acc.feed(&line) {
                    dispatch(event, high_tx, low_tx);
                    // backpressure_pause(high_tx, low_tx).await; // disabled for demo
                }
            }
            Err(e) => {
                acc.reset();
                return Err(anyhow!("SSE read error: {}", e));
            }
        }
    }

    Ok(())
}

// ============================================================================
// Public ingestion modes
// ============================================================================

/// Connect to the Wikipedia SSE firehose and forward packets indefinitely.
/// Reconnects automatically with exponential backoff on any failure.
pub async fn run_live_mode(
    high_tx: Sender<RawPacket>,
    low_tx: Sender<RawPacket>,
) -> Result<()> {
    log_info("live mode: connecting to Wikipedia SSE…");
    let mut attempt: u32 = 0;

    loop {
        // Honour a watchdog-triggered network reset request
        if crate::watchdog::NETWORK_RESET_REQUESTED
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            log_warn("watchdog requested reconnect");
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("WikipediaMonitor/1.0 (university assignment)")
            .build()?;

        match client.get(WIKIPEDIA_SSE_URL).send().await {
            Ok(resp) => {
                log_info(format!("connected (attempt {})", attempt + 1));
                attempt = 0;

                let byte_stream = resp.bytes_stream().map(|r| {
                    r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                });
                let buf_read = TokioBufReader::new(StreamReader::new(byte_stream));

                if let Err(e) = read_sse_stream(buf_read, &high_tx, &low_tx).await {
                    log_warn(format!("stream ended: {} — reconnecting", e));
                }
            }
            Err(e) => {
                log_warn(format!("connection failed: {}", e));
            }
        }

        let delay = (BACKOFF_BASE_SECS * 2_u64.pow(attempt.min(6))).min(BACKOFF_MAX_SECS);
        log_info(format!("reconnecting in {}s…", delay));
        sleep(Duration::from_secs(delay)).await;
        attempt += 1;
    }
}

/// Read from a saved JSONL file — one JSON object per line.
/// Used for valgrind / offline testing (no network, no non-determinism).
pub fn run_replay_mode(
    high_tx: Sender<RawPacket>,
    low_tx: Sender<RawPacket>,
    path: &str,
) -> Result<()> {
    use std::io::BufRead;
    log_info(format!("replay mode: {}", path));

    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);

    for raw_line in reader.lines() {
        let line = raw_line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        std::thread::sleep(Duration::from_micros(500));
        let data = Bytes::copy_from_slice(trimmed.as_bytes());
        dispatch(data, &high_tx, &low_tx);
    }

    log_info("replay mode: finished");
    Ok(())
}

/// Capture the live stream to JSONL while also forwarding to the pipeline.
pub async fn run_save_mode(
    high_tx: Sender<RawPacket>,
    low_tx: Sender<RawPacket>,
    save_path: &str,
) -> Result<()> {
    log_info(format!("save mode: writing to {}", save_path));

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(save_path)
        .await?;
    let mut writer = tokio::io::BufWriter::new(file);

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("WikipediaMonitor/1.0 (university assignment)")
        .build()?;

    let resp = client.get(WIKIPEDIA_SSE_URL).send().await?;
    let byte_stream = resp.bytes_stream().map(|r| {
        r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });
    let buf_reader = TokioBufReader::new(StreamReader::new(byte_stream));

    let mut acc = SseAccumulator::new();
    let mut lines = buf_reader.lines();

    while let Some(result) = lines.next_line().await.transpose() {
        match result {
            Ok(line) => {
                if let Some(event) = acc.feed(&line) {
                    writer.write_all(&event).await?;
                    writer.write_all(b"\n").await?;
                    dispatch(event, &high_tx, &low_tx);
                    // backpressure_pause(&high_tx, &low_tx).await; // disabled for demo
                }
            }
            Err(e) => {
                acc.reset();
                return Err(anyhow!("save mode read error: {}", e));
            }
        }
    }

    writer.flush().await?;
    log_info("save mode: stream ended");
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── SseAccumulator ────────────────────────────────────────────────────────

    #[test]
    fn test_comment_line_ignored() {
        let mut acc = SseAccumulator::new();
        assert!(acc.feed(":ok").is_none());
        assert!(acc.feed("").is_none());
    }

    #[test]
    fn test_event_and_id_lines_ignored() {
        let mut acc = SseAccumulator::new();
        assert!(acc.feed("event: message").is_none());
        assert!(acc.feed("id: [{\"topic\":\"eqiad\"}]").is_none());
        assert!(acc.feed("").is_none()); // no data: line → nothing dispatched
    }

    #[test]
    fn test_full_event_dispatched() {
        let mut acc = SseAccumulator::new();
        acc.feed("event: message");
        acc.feed("id: [{}]");
        acc.feed(r#"data: {"user":"Alice","bot":false,"server_name":"en.wikipedia.org"}"#);
        let result = acc.feed("");
        assert!(result.is_some());
        let bytes = result.unwrap();
        assert!(bytes.starts_with(b"{\"user\""));
    }

    #[test]
    fn test_data_prefix_and_space_stripped() {
        let mut acc = SseAccumulator::new();
        acc.feed("data: hello world");
        let result = acc.feed("").unwrap();
        assert_eq!(&result[..], b"hello world");
    }

    #[test]
    fn test_accumulator_resets_between_events() {
        let mut acc = SseAccumulator::new();
        acc.feed("data: first");
        acc.feed("");
        acc.feed("data: second");
        let result = acc.feed("").unwrap();
        assert_eq!(&result[..], b"second");
    }

    // ── Fast-path extraction ──────────────────────────────────────────────────

    #[test]
    fn test_bot_true() {
        assert!(fast_path_is_bot(br#"{"user":"BotA","bot":true}"#));
    }

    #[test]
    fn test_bot_false() {
        assert!(!fast_path_is_bot(br#"{"user":"Alice","bot":false}"#));
    }

    #[test]
    fn test_bot_missing_defaults_false() {
        assert!(!fast_path_is_bot(br#"{"user":"Alice"}"#));
    }

    #[test]
    fn test_server_name_extracted() {
        let json = br#"{"user":"Alice","bot":false,"server_name":"commons.wikimedia.org"}"#;
        assert_eq!(fast_path_server_name(json), "commons.wikimedia.org");
    }

    #[test]
    fn test_server_name_missing_returns_unknown() {
        assert_eq!(fast_path_server_name(br#"{"user":"Alice"}"#), "unknown");
    }

    /// Round-trip with real data from the live stream
    #[test]
    fn test_real_sample_atukr() {
        let raw = br#"{"$schema":"/mediawiki/recentchange/1.0.0","user":"AtUkr","bot":false,"server_name":"ru.wikinews.org","type":"log"}"#;
        assert!(!fast_path_is_bot(raw));
        assert_eq!(fast_path_server_name(raw), "ru.wikinews.org");
    }

    #[test]
    fn test_real_sample_commons() {
        let raw = br#"{"user":"PantheraLeo1359531","bot":false,"server_name":"commons.wikimedia.org","type":"categorize"}"#;
        assert!(!fast_path_is_bot(raw));
        assert_eq!(fast_path_server_name(raw), "commons.wikimedia.org");
    }
}