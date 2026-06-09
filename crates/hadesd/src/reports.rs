//! Report builders for the host-truth commands: battery diagnosis, uptime
//! ledger rendering, and the full process-registry tree.

use std::sync::Arc;

use chrono::{Duration, Utc};
use hades_api::types::*;
use hades_core::HadesPaths;

use crate::daemon::Daemon;
use crate::power::BatterySample;

fn read_samples(paths: &HadesPaths, days: f64) -> Vec<BatterySample> {
    let Ok(raw) = std::fs::read_to_string(paths.battery_metrics()) else {
        return Vec::new();
    };
    let cutoff = Utc::now() - Duration::seconds((days * 86400.0) as i64);
    raw.lines()
        .filter_map(|l| serde_json::from_str::<BatterySample>(l).ok())
        .filter(|s| s.at >= cutoff)
        .collect()
}

/// The "why is my battery degrading" diagnosis, computed from the telemetry
/// series. Honest about what Hades contributed.
pub fn battery_report(paths: &HadesPaths, days: f64) -> BatteryReport {
    let samples = read_samples(paths, days);
    let n = samples.len();

    let latest = samples.last();
    let earliest = samples.first();

    let cycle_count = latest.and_then(|s| s.cycle_count);
    let capacity_pct = |s: &BatterySample| -> Option<f64> {
        match (s.nominal_capacity_mah, s.design_capacity_mah) {
            (Some(nm), Some(d)) if d > 0 => Some(nm as f64 / d as f64 * 100.0),
            _ => None,
        }
    };
    let capacity_now = latest.and_then(capacity_pct);
    let capacity_then = earliest.and_then(capacity_pct);
    let capacity_trend = match (capacity_now, capacity_then) {
        (Some(now), Some(then)) => Some(now - then),
        _ => None,
    };

    let cycles_per_week = match (
        earliest.and_then(|s| s.cycle_count),
        latest.and_then(|s| s.cycle_count),
        earliest.zip(latest),
    ) {
        (Some(c0), Some(c1), Some((s0, s1))) => {
            let weeks = (s1.at - s0.at).num_seconds() as f64 / (7.0 * 86400.0);
            if weeks > 0.01 {
                Some((c1.saturating_sub(c0)) as f64 / weeks)
            } else {
                None
            }
        }
        _ => None,
    };

    // High-SoC dwell: % of on-AC samples pinned at >=95% charge. The main
    // silent degrader for an always-plugged host.
    let ac_samples: Vec<&BatterySample> = samples.iter().filter(|s| s.on_ac).collect();
    let high_soc_dwell_pct = if ac_samples.is_empty() {
        None
    } else {
        let high = ac_samples
            .iter()
            .filter(|s| s.soc_pct.map(|p| p >= 95.0).unwrap_or(false))
            .count();
        Some(high as f64 / ac_samples.len() as f64 * 100.0)
    };

    let temps: Vec<f64> = samples.iter().filter_map(|s| s.temperature_c).collect();
    let avg_temp_c = if temps.is_empty() {
        None
    } else {
        Some(temps.iter().sum::<f64>() / temps.len() as f64)
    };

    let cpu: Vec<f64> = samples.iter().map(|s| s.hades_cpu_pct).collect();
    let hades_cpu_share_pct = if cpu.is_empty() {
        None
    } else {
        Some(cpu.iter().sum::<f64>() / cpu.len() as f64)
    };

    let mut recommendations = Vec::new();
    if let Some(dwell) = high_soc_dwell_pct {
        if dwell > 80.0 {
            recommendations.push(format!(
                "battery sits at >=95% charge {dwell:.0}% of plugged-in time — enable macOS Optimized Battery Charging (or a charge limiter) to cut high-SoC dwell, the main silent degrader for an always-plugged host"
            ));
        }
    }
    if let Some(cpw) = cycles_per_week {
        if cpw > 3.0 {
            recommendations.push(format!(
                "{cpw:.1} charge cycles/week is high for a plugged-in host — check whether the machine is actually staying on AC"
            ));
        }
    }
    if let Some(t) = avg_temp_c {
        if t > 35.0 {
            recommendations.push(format!(
                "average battery temperature {t:.1}°C is elevated — heavy container load heats the battery; consider lowering app CPU quotas or improving airflow"
            ));
        }
    }
    if let Some(share) = hades_cpu_share_pct {
        if share > 100.0 {
            recommendations.push(format!(
                "Hades containers average {share:.0}% CPU — if battery wear matters, schedule heavy apps to pause on battery (power.on_battery = \"pause\")"
            ));
        }
    }
    if recommendations.is_empty() && n > 0 {
        recommendations
            .push("no battery red flags in the sample window — keep an eye on capacity trend".into());
    }
    if n == 0 {
        recommendations.push(
            "no battery telemetry yet — the daemon samples every 5 minutes; check back soon (or this is a desktop Mac with no battery)"
                .into(),
        );
    }

    let sampled_over_days = match (earliest, latest) {
        (Some(a), Some(b)) => (b.at - a.at).num_seconds() as f64 / 86400.0,
        _ => 0.0,
    };

    BatteryReport {
        sampled_over_days,
        sample_count: n,
        cycle_count,
        capacity_pct_of_design: capacity_now,
        capacity_trend_pct: capacity_trend,
        cycles_per_week,
        high_soc_dwell_pct,
        avg_temp_c,
        hades_cpu_share_pct,
        recommendations,
    }
}

pub fn uptime_report(d: &Daemon, days: f64) -> UptimeReport {
    let since = Utc::now() - Duration::seconds((days * 86400.0) as i64);
    let windows = d
        .ledger
        .windows_since(since)
        .into_iter()
        .map(|r| DowntimeWindow {
            start: r.start,
            end: r.end,
            secs: r.secs,
            cause: r.cause,
        })
        .collect();
    UptimeReport {
        since,
        availability_pct: d.ledger.availability_pct(since),
        windows,
    }
}

fn rss_mb_of_pid(pid: u32) -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kb: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kb / 1024.0)
}

/// Everything Hades runs, with live RSS where available: the daemon itself,
/// every labeled container, every cloudflared.
pub async fn ps_report(d: &Arc<Daemon>) -> PsReport {
    let mut entries = Vec::new();

    let my_pid = std::process::id();
    entries.push(PsEntry {
        kind: PsKind::Daemon,
        app: None,
        id: my_pid.to_string(),
        detail: "hadesd".into(),
        rss_mb: rss_mb_of_pid(my_pid),
        cpu_pct: None,
        state: "running".into(),
    });

    if let Ok(containers) = d.runtime.list_labeled().await {
        for c in containers {
            let stats = if c.state == "running" {
                d.runtime.stats_once(&c.id).await.ok()
            } else {
                None
            };
            entries.push(PsEntry {
                kind: PsKind::Container,
                app: Some(c.app.clone()),
                id: c.id.chars().take(12).collect(),
                detail: c.name,
                rss_mb: stats.as_ref().map(|s| s.memory_used_mb),
                cpu_pct: stats.as_ref().map(|s| s.cpu_pct),
                state: c.state,
            });
        }
    }

    let reg = d.store.registry_snapshot();
    for (app, pid) in reg.cloudflared {
        let rss = rss_mb_of_pid(pid);
        entries.push(PsEntry {
            kind: PsKind::Cloudflared,
            app: Some(app),
            id: pid.to_string(),
            detail: "cloudflared quick tunnel".into(),
            rss_mb: rss,
            cpu_pct: None,
            state: if rss.is_some() { "running" } else { "dead" }.into(),
        });
    }

    PsReport { entries }
}
