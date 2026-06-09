//! OS probes. Everything behind `HostProbe` so a Linux implementation can
//! slot in; `MacProbe` shells out to pmset/ioreg/sysctl/df, which are stable
//! interfaces and avoid private-framework bindings.

use chrono::{DateTime, TimeZone, Utc};
use hades_core::events::DowntimeCause;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerSnapshot {
    pub on_ac: bool,
    pub battery_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatteryHealth {
    pub cycle_count: Option<u64>,
    pub design_capacity_mah: Option<u64>,
    pub nominal_capacity_mah: Option<u64>,
    pub temperature_c: Option<f64>,
}

impl BatteryHealth {
    pub fn capacity_pct_of_design(&self) -> Option<f64> {
        match (self.nominal_capacity_mah, self.design_capacity_mah) {
            (Some(n), Some(d)) if d > 0 => Some(n as f64 / d as f64 * 100.0),
            _ => None,
        }
    }
}

pub trait HostProbe: Send + Sync {
    fn power(&self) -> Option<PowerSnapshot>;
    fn battery_health(&self) -> Option<BatteryHealth>;
    /// Timestamps at which the machine entered sleep, newest-last.
    fn sleep_events(&self) -> Vec<DateTime<Utc>>;
    fn boot_time(&self) -> Option<DateTime<Utc>>;
    /// Whether the machine is configured to sleep while on AC power.
    fn sleeps_on_ac(&self) -> Option<bool>;
    fn disk_free_gb(&self) -> Option<f64>;
    fn mac_total_memory_mb(&self) -> Option<u64>;
}

pub struct MacProbe;

fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        None
    }
}

/// Pull `"Key" = 123` style integers out of `ioreg -rn AppleSmartBattery`.
fn ioreg_int(raw: &str, key: &str) -> Option<i64> {
    for line in raw.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix(&format!("\"{key}\" = ")) {
            return rest.trim().parse::<i64>().ok();
        }
    }
    None
}

impl HostProbe for MacProbe {
    fn power(&self) -> Option<PowerSnapshot> {
        let raw = run("pmset", &["-g", "batt"])?;
        let on_ac = raw.contains("AC Power");
        let battery_pct = raw
            .lines()
            .find_map(|l| l.split_whitespace().find(|w| w.ends_with("%;")))
            .and_then(|w| w.trim_end_matches("%;").parse::<f64>().ok());
        Some(PowerSnapshot { on_ac, battery_pct })
    }

    fn battery_health(&self) -> Option<BatteryHealth> {
        let raw = run("ioreg", &["-rn", "AppleSmartBattery"])?;
        if raw.trim().is_empty() {
            return None; // desktop Mac: no battery
        }
        Some(BatteryHealth {
            cycle_count: ioreg_int(&raw, "CycleCount").map(|v| v as u64),
            design_capacity_mah: ioreg_int(&raw, "DesignCapacity").map(|v| v as u64),
            nominal_capacity_mah: ioreg_int(&raw, "NominalChargeCapacity")
                .or_else(|| ioreg_int(&raw, "AppleRawMaxCapacity"))
                .map(|v| v as u64),
            // ioreg reports centi-degrees C (e.g. 3042 = 30.42°C)
            temperature_c: ioreg_int(&raw, "Temperature").map(|v| v as f64 / 100.0),
        })
    }

    fn sleep_events(&self) -> Vec<DateTime<Utc>> {
        // `pmset -g log` lines look like:
        // 2026-06-09 01:23:45 -0700 Sleep   Entering Sleep state due to ...
        let Some(raw) = run("pmset", &["-g", "log"]) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for line in raw.lines() {
            if line.contains(" Sleep ") && line.contains("Entering Sleep") {
                if let Some(ts) = parse_pmset_timestamp(line) {
                    out.push(ts);
                }
            }
        }
        out
    }

    fn boot_time(&self) -> Option<DateTime<Utc>> {
        // kern.boottime: { sec = 1760000000, usec = 0 } Mon Jun  8 ...
        let raw = run("sysctl", &["-n", "kern.boottime"])?;
        let sec = raw
            .split("sec =")
            .nth(1)?
            .split(',')
            .next()?
            .trim()
            .parse::<i64>()
            .ok()?;
        Utc.timestamp_opt(sec, 0).single()
    }

    fn sleeps_on_ac(&self) -> Option<bool> {
        // In `pmset -g custom`, the "AC Power:" section's `sleep` value;
        // 0 means never sleep on AC.
        let raw = run("pmset", &["-g", "custom"])?;
        let mut in_ac = false;
        for line in raw.lines() {
            let t = line.trim();
            if t.starts_with("AC Power") {
                in_ac = true;
                continue;
            }
            if t.starts_with("Battery Power") {
                in_ac = false;
                continue;
            }
            if in_ac {
                if let Some(rest) = t.strip_prefix("sleep") {
                    if let Ok(v) = rest.split_whitespace().next().unwrap_or("").parse::<i64>() {
                        return Some(v != 0);
                    }
                }
            }
        }
        None
    }

    fn disk_free_gb(&self) -> Option<f64> {
        let raw = run("df", &["-Pk", "/"])?;
        let line = raw.lines().nth(1)?;
        let avail_kb: f64 = line.split_whitespace().nth(3)?.parse().ok()?;
        Some(avail_kb / (1024.0 * 1024.0))
    }

    fn mac_total_memory_mb(&self) -> Option<u64> {
        let raw = run("sysctl", &["-n", "hw.memsize"])?;
        raw.trim().parse::<u64>().ok().map(|b| b / (1024 * 1024))
    }
}

fn parse_pmset_timestamp(line: &str) -> Option<DateTime<Utc>> {
    // "2026-06-09 01:23:45 -0700 ..." — parse the first three fields.
    let mut parts = line.split_whitespace();
    let date = parts.next()?;
    let time = parts.next()?;
    let tz = parts.next()?;
    let s = format!("{date} {time} {tz}");
    DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S %z")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Classify a downtime window. Pure so it's testable: given when we last
/// heartbeat, when the kernel booted, and the machine's sleep history,
/// decide what the gap was.
pub fn classify_downtime(
    last_heartbeat: DateTime<Utc>,
    boot_time: Option<DateTime<Utc>>,
    sleep_events: &[DateTime<Utc>],
    now: DateTime<Utc>,
) -> DowntimeCause {
    if let Some(boot) = boot_time {
        if boot > last_heartbeat {
            return DowntimeCause::Rebooted;
        }
    }
    // a sleep entry shortly after (or just before) the last heartbeat means
    // the machine napped through the gap
    let slack = chrono::Duration::seconds(90);
    if sleep_events
        .iter()
        .any(|t| *t >= last_heartbeat - slack && *t <= now)
    {
        return DowntimeCause::Slept;
    }
    // machine stayed up and awake but the daemon went quiet: it died
    if boot_time.is_some() {
        DowntimeCause::Crashed
    } else {
        DowntimeCause::Unknown
    }
}

/// Used by the daemon's live heartbeat loop: a wall-clock jump much larger
/// than the tick interval means we slept rather than ran.
pub fn is_wall_clock_jump(expected_tick_secs: u64, actual_gap_secs: u64) -> bool {
    actual_gap_secs > expected_tick_secs * 3 + 5
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn classify_reboot_wins() {
        let cause = classify_downtime(
            t("2026-06-09T10:00:00Z"),
            Some(t("2026-06-09T10:05:00Z")),
            &[t("2026-06-09T10:00:30Z")],
            t("2026-06-09T10:10:00Z"),
        );
        assert_eq!(cause, DowntimeCause::Rebooted);
    }

    #[test]
    fn classify_sleep() {
        let cause = classify_downtime(
            t("2026-06-09T10:00:00Z"),
            Some(t("2026-06-09T01:00:00Z")),
            &[t("2026-06-09T10:00:20Z")],
            t("2026-06-09T10:30:00Z"),
        );
        assert_eq!(cause, DowntimeCause::Slept);
    }

    #[test]
    fn classify_crash() {
        let cause = classify_downtime(
            t("2026-06-09T10:00:00Z"),
            Some(t("2026-06-09T01:00:00Z")),
            &[t("2026-06-09T02:00:00Z")], // old sleep, not in window
            t("2026-06-09T10:30:00Z"),
        );
        assert_eq!(cause, DowntimeCause::Crashed);
    }

    #[test]
    fn pmset_timestamp_parses() {
        let line = "2026-06-09 01:23:45 -0700 Sleep               Entering Sleep state due to 'Clamshell Sleep'";
        let ts = parse_pmset_timestamp(line).unwrap();
        assert_eq!(ts, t("2026-06-09T08:23:45Z"));
    }

    #[test]
    fn ioreg_parsing() {
        let raw = r#"
  | {
      "CycleCount" = 201
      "DesignCapacity" = 8694
      "NominalChargeCapacity" = 8050
      "Temperature" = 3042
    }
"#;
        assert_eq!(ioreg_int(raw, "CycleCount"), Some(201));
        let h = BatteryHealth {
            cycle_count: Some(201),
            design_capacity_mah: Some(8694),
            nominal_capacity_mah: Some(8050),
            temperature_c: Some(30.42),
        };
        let pct = h.capacity_pct_of_design().unwrap();
        assert!((pct - 92.59).abs() < 0.1);
    }
}
