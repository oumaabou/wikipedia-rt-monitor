//! Utils module - Shared helpers used across all modules.
//!
//! Includes:
//! - `format_edit_line` — pretty-print a processed edit for the live feed
//! - `micros_since_epoch` — current wall-clock time in µs (for watchdog)
//! - `AppConfig` builder helpers

use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Time helpers
// ============================================================================

/// Current wall-clock time in microseconds since the UNIX epoch.
///
/// Used by ingestion to update `LAST_PACKET_TIMESTAMP`.
#[inline]
pub fn micros_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros() as u64
}

/// Human-readable duration for log messages.
pub fn format_duration(d: Duration) -> String {
    if d.as_millis() < 1 {
        format!("{} µs", d.as_micros())
    } else if d.as_secs() < 1 {
        format!("{:.2} ms", d.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.2} s", d.as_secs_f64())
    }
}

// ============================================================================
// Live feed formatting
// ============================================================================

/// Format one Wikipedia edit into a display string for the live feed panel.
///
/// Examples:
/// ```text
/// 👤 Alice        en.wikipedia.org   (human)
/// 🤖 ClueBot NG   de.wikipedia.org   (bot)
/// ```
pub fn format_edit_line(user: &str, domain: &str, is_bot: bool, drift_ms: f64) -> String {
    let icon = if is_bot { "🤖" } else { "👤" };
    // Truncate long usernames to keep the feed readable
    let user_display = if user.len() > 20 {
        format!("{}…", &user[..19])
    } else {
        user.to_string()
    };
    let domain_display = if domain.len() > 20 {
        format!("{}…", &domain[..19])
    } else {
        domain.to_string()
    };
    format!(
        "{} {:<21} {:<21} {:.1}ms",
        icon, user_display, domain_display, drift_ms
    )
}

// ============================================================================
// CLI argument parsing
// ============================================================================

/// Simple CLI configuration parsed from `std::env::args`.
#[derive(Debug, Clone)]
pub struct CliConfig {
    /// Use threaded pipeline instead of async.
    pub use_threaded: bool,
    /// Replay from a saved JSONL file instead of live stream.
    pub replay_file: Option<String>,
    /// Capture live data to a JSONL file.
    pub save_file: Option<String>,
    /// Run the sync comparison benchmark at startup.
    pub run_benchmark: bool,
}

impl CliConfig {
    /// Parse from process arguments.
    ///
    /// ```text
    /// --threaded             Use std::thread pipeline
    /// --replay <file>        Replay from JSONL file
    /// --save <file>          Save live data to JSONL file
    /// --benchmark            Run Mutex/RwLock/Atomic comparison
    /// ```
    pub fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut cfg = Self {
            use_threaded: false,
            replay_file: None,
            save_file: None,
            run_benchmark: false,
        };

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--threaded" => cfg.use_threaded = true,
                "--benchmark" => cfg.run_benchmark = true,
                "--replay" => {
                    i += 1;
                    cfg.replay_file = args.get(i).cloned();
                }
                "--save" => {
                    i += 1;
                    cfg.save_file = args.get(i).cloned();
                }
                other => eprintln!("[utils] unknown flag: {}", other),
            }
            i += 1;
        }

        cfg
    }
}

// ============================================================================
// Logging directories setup
// ============================================================================

/// Create the `logs/` directory if it does not exist.
/// Called once at startup by `main.rs` before the async logger starts.
pub fn ensure_log_dirs() {
    for dir in &["logs"] {
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("[utils] could not create {}: {}", dir, e);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_micros_since_epoch_increases() {
        let t1 = micros_since_epoch();
        std::thread::sleep(Duration::from_millis(1));
        let t2 = micros_since_epoch();
        assert!(t2 > t1);
    }

    #[test]
    fn test_format_edit_line_human() {
        let line = format_edit_line("Alice", "en.wikipedia.org", false, 0.8);
        assert!(line.contains("👤"));
        assert!(line.contains("Alice"));
    }

    #[test]
    fn test_format_edit_line_bot() {
        let line = format_edit_line("ClueBot NG", "de.wikipedia.org", true, 1.2);
        assert!(line.contains("🤖"));
        assert!(line.contains("ClueBot NG"));
    }

    #[test]
    fn test_long_username_truncated() {
        let long_name = "A".repeat(30);
        let line = format_edit_line(&long_name, "en.wikipedia.org", false, 0.0);
        // Should contain the truncation ellipsis
        assert!(line.contains('…'));
    }

    #[test]
    fn test_format_duration() {
        assert!(format_duration(Duration::from_micros(500)).contains("µs"));
        assert!(format_duration(Duration::from_millis(5)).contains("ms"));
        assert!(format_duration(Duration::from_secs(2)).contains("s"));
    }
}