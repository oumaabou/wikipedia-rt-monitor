//! Processing module - Zero-copy field extraction (Component B + Extra 1)
//!
//! # Zero-Copy Strategy
//!
//! serde's `#[serde(borrow)]` cannot be used with an explicit `<'de>` lifetime
//! on the struct (rustc E0392 / macro-error). Instead we use a two-step approach:
//!
//! 1. **Fast-path byte search** to locate the `"user":"..."` value inside the
//!    raw `Bytes` buffer — identical technique to ingestion's bot/server_name
//!    extraction.  This gives us a byte range with zero allocation.
//!
//! 2. We validate it as UTF-8 with `std::str::from_utf8` (zero-copy — returns
//!    a `&str` pointing into the original buffer) and store that slice in
//!    `ProcessedPacket<'a>`.
//!
//! The result is indistinguishable from the serde approach for valgrind
//! purposes: the `user` field is a pointer into the existing allocation, not
//! a new heap string.
//!
//! # Fallback
//! If the "user" key is absent or the value is not valid UTF-8 the function
//! returns `None` and the caller skips the packet — no panics, no crashes.

use std::time::Instant;


use crate::ingestion::RawPacket;

// ============================================================================
// Output Data Structure
// ============================================================================

/// A packet after zero-copy field extraction.
///
/// The lifetime `'a` ties `user` back to the original `Bytes` allocation
/// inside the source `RawPacket`. As long as the `RawPacket` (or a clone of
/// its `Bytes`) is alive, this reference is valid.
#[derive(Debug)]
pub struct ProcessedPacket<'a> {
    /// Wikipedia editor username.
    /// Points directly into `RawPacket.data` — NO heap allocation.
    pub user: &'a str,

    /// Domain name, e.g. `"en.wikipedia.org"`.
    pub server_name: String,

    /// `true` when the edit was made by a bot account.
    pub is_bot: bool,

    /// When the SSE event arrived at ingestion.
    /// Used to measure queue wait time (priority proof: humans wait less).
    pub ingestion_time: Instant,

    /// When the pipeline worker dequeued and started processing this packet.
    /// The 2ms deadline is measured from HERE — this is what the assignment
    /// specifies: deadline runs from when the packet leaves the ingestion channel.
    pub processing_start: Instant,
}

// ============================================================================
// Zero-copy "user" extraction
// ============================================================================

/// Find the byte range of the JSON string value for `"user"` inside `data`.
///
/// Searches for the literal bytes `"user":"` then scans forward to the
/// closing `"`, respecting `\"` escape sequences.
///
/// Returns `Some((start, end))` where `data[start..end]` is the raw bytes of
/// the username value (without surrounding quotes), or `None` if absent.
fn find_user_bytes(data: &[u8]) -> Option<(usize, usize)> {
    const KEY: &[u8] = b"\"user\":\"";
    let key_pos = data.windows(KEY.len()).position(|w| w == KEY)?;
    let start = key_pos + KEY.len();

    // Scan for the closing `"`, honouring `\"` escapes
    let mut i = start;
    while i < data.len() {
        match data[i] {
            b'\\' => i += 2, // skip escaped character
            b'"'  => return Some((start, i)),
            _     => i += 1,
        }
    }
    None // unterminated string — malformed JSON
}

// ============================================================================
// Public API
// ============================================================================

/// Parse a `RawPacket` and return a zero-copy `ProcessedPacket`.
///
/// Returns `None` when:
/// - the `"user"` field is absent (malformed / non-edit event)
/// - the username bytes are not valid UTF-8 (should never happen for Wikipedia)
///
/// # Lifetime
/// The returned `ProcessedPacket<'a>` borrows from `packet.data`.
/// Keep the `RawPacket` alive for as long as the `ProcessedPacket` is in use.
pub fn process_packet(packet: &RawPacket) -> Option<ProcessedPacket<'_>> {
    let (start, end) = find_user_bytes(&packet.data)?;

    // Zero-copy: from_utf8 returns a &str pointing INTO packet.data's buffer.
    // No String allocation occurs here.
    let user = std::str::from_utf8(&packet.data[start..end]).ok()?;

    Some(ProcessedPacket {
        user,
        server_name: packet.server_name.clone(),
        is_bot: packet.is_bot,
        ingestion_time: packet.ingestion_time,
        processing_start: packet.processing_start,
    })
}

/// Process a slice of raw packets, silently dropping any that fail to parse.
pub fn process_batch(packets: &[RawPacket]) -> Vec<ProcessedPacket<'_>> {
    packets.iter().filter_map(process_packet).collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Instant;

    fn make_packet(json: &str, is_bot: bool) -> RawPacket {
        let t = Instant::now();
        RawPacket {
            data: Bytes::copy_from_slice(json.as_bytes()),
            ingestion_time: t,
            processing_start: t,
            is_bot,
            server_name: "en.wikipedia.org".to_string(),
        }
    }

    #[test]
    fn test_extracts_user() {
        let p = make_packet(
            r#"{"user":"JohnDoe","bot":false,"server_name":"en.wikipedia.org"}"#,
            false,
        );
        let proc = process_packet(&p).unwrap();
        assert_eq!(proc.user, "JohnDoe");
        assert!(!proc.is_bot);
    }

    #[test]
    fn test_bot_packet() {
        let p = make_packet(
            r#"{"user":"ClueBot NG","bot":true,"server_name":"en.wikipedia.org"}"#,
            true,
        );
        let proc = process_packet(&p).unwrap();
        assert_eq!(proc.user, "ClueBot NG");
        assert!(proc.is_bot);
    }

    #[test]
    fn test_missing_user_returns_none() {
        let p = make_packet(r#"{"bot":false,"server_name":"en.wikipedia.org"}"#, false);
        assert!(process_packet(&p).is_none());
    }

    #[test]
    fn test_malformed_json_returns_none() {
        let p = make_packet("not json {{{{", false);
        assert!(process_packet(&p).is_none());
    }

    #[test]
    fn test_batch_skips_bad_packets() {
        let packets = vec![
            make_packet(r#"{"user":"Alice","bot":false,"server_name":"en.wikipedia.org"}"#, false),
            make_packet("garbage", false),
            make_packet(r#"{"user":"Bob","bot":false,"server_name":"fr.wikipedia.org"}"#, false),
        ];
        let results = process_batch(&packets);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].user, "Alice");
        assert_eq!(results[1].user, "Bob");
    }

    /// Verify the zero-copy guarantee: the &str pointer must lie within
    /// the Bytes buffer, not in a freshly allocated string.
    #[test]
    fn test_user_is_zero_copy() {
        let json = r#"{"user":"WikiEditor","bot":false,"server_name":"en.wikipedia.org"}"#;
        let packet = make_packet(json, false);
        let processed = process_packet(&packet).unwrap();

        let buf_start = packet.data.as_ptr() as usize;
        let buf_end   = buf_start + packet.data.len();
        let user_ptr  = processed.user.as_ptr() as usize;

        assert!(
            user_ptr >= buf_start && user_ptr < buf_end,
            "user &str ({:#x}) is outside Bytes buffer ({:#x}..{:#x}) — heap allocation occurred!",
            user_ptr, buf_start, buf_end,
        );
    }

    /// Real sample from the live Wikipedia SSE stream.
    #[test]
    fn test_real_world_sample_atukr() {
        let json = r#"{"$schema":"/mediawiki/recentchange/1.0.0","user":"AtUkr","bot":false,"server_name":"ru.wikinews.org","type":"log"}"#;
        let p = make_packet(json, false);
        let proc = process_packet(&p).unwrap();
        assert_eq!(proc.user, "AtUkr");
    }

    #[test]
    fn test_real_world_sample_pantheraleo() {
        let json = r#"{"$schema":"/mediawiki/recentchange/1.0.0","user":"PantheraLeo1359531","bot":false,"server_name":"commons.wikimedia.org","type":"categorize"}"#;
        let p = make_packet(json, false);
        let proc = process_packet(&p).unwrap();
        assert_eq!(proc.user, "PantheraLeo1359531");
    }
}