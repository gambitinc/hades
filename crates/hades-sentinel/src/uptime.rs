//! The uptime ledger: a heartbeat timestamp on disk plus an append-only
//! JSONL of classified downtime windows. The daemon writes the heartbeat
//! every `heartbeat_secs`; on startup it reads the gap and classifies it
//! (the classifier itself lives in hades-host's probe module so it can see
//! pmset history).

use std::io::Write;
use std::path::PathBuf;

use chrono::{DateTime, Duration, Utc};
use hades_core::events::DowntimeCause;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DowntimeRecord {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub secs: u64,
    pub cause: DowntimeCause,
}

pub struct UptimeLedger {
    heartbeat_path: PathBuf,
    ledger_path: PathBuf,
}

impl UptimeLedger {
    pub fn new(heartbeat_path: PathBuf, ledger_path: PathBuf) -> Self {
        Self {
            heartbeat_path,
            ledger_path,
        }
    }

    pub fn write_heartbeat(&self) -> std::io::Result<()> {
        // write-then-rename so a crash mid-write can't corrupt the timestamp
        let tmp = self.heartbeat_path.with_extension("tmp");
        std::fs::write(&tmp, Utc::now().to_rfc3339())?;
        std::fs::rename(&tmp, &self.heartbeat_path)
    }

    pub fn last_heartbeat(&self) -> Option<DateTime<Utc>> {
        let raw = std::fs::read_to_string(&self.heartbeat_path).ok()?;
        DateTime::parse_from_rfc3339(raw.trim())
            .ok()
            .map(|t| t.with_timezone(&Utc))
    }

    pub fn append(&self, rec: &DowntimeRecord) -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.ledger_path)?;
        writeln!(f, "{}", serde_json::to_string(rec).expect("record serializes"))
    }

    pub fn windows_since(&self, since: DateTime<Utc>) -> Vec<DowntimeRecord> {
        let Ok(raw) = std::fs::read_to_string(&self.ledger_path) else {
            return Vec::new();
        };
        raw.lines()
            .filter_map(|l| serde_json::from_str::<DowntimeRecord>(l).ok())
            .filter(|r| r.end >= since)
            .collect()
    }

    /// Availability % over the window [since, now]: 1 - downtime/total.
    pub fn availability_pct(&self, since: DateTime<Utc>) -> f64 {
        let now = Utc::now();
        let total = (now - since).num_seconds().max(1) as f64;
        let down: f64 = self
            .windows_since(since)
            .iter()
            .map(|r| {
                // clamp each window to the report range
                let start = r.start.max(since);
                let end = r.end.min(now);
                (end - start).num_seconds().max(0) as f64
            })
            .sum();
        ((1.0 - down / total) * 100.0).clamp(0.0, 100.0)
    }
}

/// Gap-detection helper for the live heartbeat loop: given the previous tick
/// time, decide whether the host slept through the interval.
pub fn detect_gap(
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
    tick_secs: u64,
) -> Option<Duration> {
    let gap = now - prev;
    if gap > Duration::seconds((tick_secs * 3 + 5) as i64) {
        Some(gap)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_round_trip_and_availability() {
        let dir = std::env::temp_dir().join(format!("hades-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = UptimeLedger::new(dir.join("hb"), dir.join("uptime.jsonl"));

        ledger.write_heartbeat().unwrap();
        assert!(ledger.last_heartbeat().is_some());

        let now = Utc::now();
        ledger
            .append(&DowntimeRecord {
                start: now - Duration::hours(2),
                end: now - Duration::hours(1),
                secs: 3600,
                cause: DowntimeCause::Slept,
            })
            .unwrap();

        let windows = ledger.windows_since(now - Duration::days(1));
        assert_eq!(windows.len(), 1);

        // 1 hour down in the last 10 hours => 90%
        let pct = ledger.availability_pct(now - Duration::hours(10));
        assert!((pct - 90.0).abs() < 1.0, "got {pct}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn gap_detection() {
        let now = Utc::now();
        assert!(detect_gap(now - Duration::seconds(31), now, 30).is_none());
        assert!(detect_gap(now - Duration::seconds(300), now, 30).is_some());
    }
}
