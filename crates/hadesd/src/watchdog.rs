//! Runtime enforcement: replica supervision (OOM detection, restart with
//! crash-loop cutoff), resource sampling to metrics, memory-pressure level
//! tracking, disk watch, and the reaper that kills anything labeled
//! `hades.app` that desired state no longer explains.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hades_api::types::AppState;
use hades_core::events::{HostEvent, PressureLevel};
use hades_host::{HostProbe, MacProbe};
use serde::Serialize;

use crate::daemon::Daemon;
use crate::policy;

#[derive(Serialize)]
struct ResourceSample {
    at: chrono::DateTime<chrono::Utc>,
    app: String,
    container_id: String,
    memory_used_mb: f64,
    memory_limit_mb: f64,
    cpu_pct: f64,
}

pub async fn run(d: Arc<Daemon>) {
    let mut tick = tokio::time::interval(Duration::from_secs(15));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut disk_low = false;
    let mut reap_counter = 0u32;

    loop {
        tick.tick().await;
        supervise_replicas(&d).await;
        sample_and_pressure(&d).await;

        // disk watch
        let probe = MacProbe;
        if let Some(free) = probe.disk_free_gb() {
            let low = free < d.config.disk_min_free_gb;
            if low && !disk_low {
                d.emit(HostEvent::DiskLow { free_gb: free });
            }
            disk_low = low;
        }

        // reaper: every ~10 minutes
        reap_counter += 1;
        if reap_counter >= 40 {
            reap_counter = 0;
            reap(&d).await;
        }
    }
}

/// Inspect every replica; restart dead ones (with crash-loop cutoff),
/// distinguishing OOM kills (exit 137 / OOMKilled) from plain crashes.
async fn supervise_replicas(d: &Arc<Daemon>) {
    let apps = d.store.snapshot();
    for (name, rec) in apps {
        if rec.state == AppState::Paused || rec.state == AppState::CrashLoop {
            continue;
        }
        for r in &rec.replicas {
            let Ok(st) = d.runtime.inspect_state(&r.container_id).await else {
                continue;
            };
            if st.running || st.paused {
                continue;
            }

            let oom = st.oom_killed || st.exit_code == Some(137);
            let (restarts, crash_loop) = d
                .store
                .update(|apps| {
                    apps.get_mut(&name)
                        .map(|rec| {
                            rec.restarts += 1;
                            rec.oom_times.push(Utc::now());
                            let cutoff =
                                Utc::now() - chrono::Duration::seconds(policy::CRASH_LOOP_WINDOW_SECS);
                            rec.oom_times.retain(|t| *t >= cutoff);
                            let looped = policy::is_crash_loop(&rec.oom_times);
                            if looped {
                                rec.state = AppState::CrashLoop;
                            }
                            (rec.restarts, looped)
                        })
                        .unwrap_or((0, false))
                })
                .to_owned();

            if oom {
                d.emit(HostEvent::AppOomKilled {
                    app: name.clone(),
                    memory_mb: rec.spec.resources.memory_mb,
                    restarts,
                });
            } else {
                d.emit(HostEvent::AppUnhealthy {
                    app: name.clone(),
                    detail: format!(
                        "container exited (code {:?}), restarting",
                        st.exit_code
                    ),
                });
            }

            if crash_loop {
                d.emit(HostEvent::AppCrashLoop {
                    app: name.clone(),
                    restarts,
                });
                break; // stop touching this app's replicas
            }

            // backoff scaled by recent restart count, then restart in place
            let backoff = Duration::from_secs(2u64.pow(restarts.min(5)));
            tokio::time::sleep(backoff).await;
            if let Err(e) = d.runtime.restart(&r.container_id).await {
                tracing::warn!(app = %name, "restart failed: {e}");
            }
        }
    }
}

/// Append per-replica stats to metrics/resources.jsonl, update the shared
/// "Hades CPU share" figure, and track memory-pressure level transitions.
async fn sample_and_pressure(d: &Arc<Daemon>) {
    let apps = d.store.snapshot();
    let mut samples = Vec::new();
    let mut total_cpu = 0.0;
    for (name, rec) in &apps {
        if rec.state != AppState::Running {
            continue;
        }
        for r in &rec.replicas {
            if let Ok(s) = d.runtime.stats_once(&r.container_id).await {
                total_cpu += s.cpu_pct;
                samples.push(ResourceSample {
                    at: Utc::now(),
                    app: name.clone(),
                    container_id: r.container_id.clone(),
                    memory_used_mb: s.memory_used_mb,
                    memory_limit_mb: s.memory_limit_mb,
                    cpu_pct: s.cpu_pct,
                });
            }
        }
    }
    *d.last_total_cpu_pct.lock().unwrap() = total_cpu;

    if !samples.is_empty() {
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(d.paths.resource_metrics())
        {
            for s in &samples {
                let _ = writeln!(f, "{}", serde_json::to_string(s).unwrap());
            }
        }
    }

    // host-side pressure from available memory; an unreadable sample must
    // never shed apps
    let Some(available_mb) = available_memory_mb() else {
        return;
    };
    let level = if available_mb < d.config.pressure_critical_mb {
        PressureLevel::Critical
    } else if available_mb < d.config.pressure_warn_mb {
        PressureLevel::Warn
    } else {
        PressureLevel::Normal
    };

    // debounce: a level change must hold for two consecutive ticks before we
    // emit (and thus before the policy engine pauses/resumes anything)
    static PENDING: std::sync::Mutex<Option<PressureLevel>> = std::sync::Mutex::new(None);
    let confirmed = {
        let mut pending = PENDING.lock().unwrap();
        let current = *d.pressure.lock().unwrap();
        if level == current {
            *pending = None;
            None
        } else if *pending == Some(level) {
            *pending = None;
            Some(level)
        } else {
            *pending = Some(level);
            None
        }
    };
    if let Some(level) = confirmed {
        *d.pressure.lock().unwrap() = level;
        d.emit(HostEvent::MemoryPressure {
            level,
            available_mb,
        });
    }
}

/// Available memory in MB. sysinfo first; on macOS it sometimes reports 0,
/// so fall back to summing vm_stat's free + inactive + speculative +
/// purgeable pages. None = unknown (callers must not shed on unknown).
fn available_memory_mb() -> Option<u64> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let mb = sys.available_memory() / (1024 * 1024);
    if mb > 0 {
        return Some(mb);
    }

    let out = std::process::Command::new("vm_stat").output().ok()?;
    let raw = String::from_utf8_lossy(&out.stdout);
    let page_size: u64 = raw
        .lines()
        .next()
        .and_then(|l| l.split("page size of").nth(1))
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(16384);
    let mut pages: u64 = 0;
    let mut found = false;
    for line in raw.lines() {
        for key in [
            "Pages free:",
            "Pages inactive:",
            "Pages speculative:",
            "Pages purgeable:",
        ] {
            if let Some(rest) = line.strip_prefix(key) {
                if let Ok(v) = rest.trim().trim_end_matches('.').parse::<u64>() {
                    pages += v;
                    found = true;
                }
            }
        }
    }
    found.then(|| pages * page_size / (1024 * 1024))
}

/// Kill anything we own that desired state no longer explains: labeled
/// containers without an app record, cloudflared PIDs for destroyed apps.
pub async fn reap(d: &Arc<Daemon>) {
    let apps = d.store.snapshot();

    if let Ok(labeled) = d.runtime.list_labeled().await {
        let known: std::collections::HashSet<String> = apps
            .values()
            .flat_map(|r| r.replicas.iter().map(|x| x.container_id.clone()))
            .collect();
        for c in labeled {
            let orphan = !apps.contains_key(&c.app) || !known.contains(&c.id);
            if orphan {
                tracing::info!(container = %c.name, app = %c.app, "reaping orphan container");
                let _ = d.runtime.stop_remove(&c.id).await;
            }
        }
    }

    let reg = d.store.registry_snapshot();
    for (app, pid) in reg.cloudflared {
        // "__control" is the daemon's own API tunnel, not an app's
        if app != "__control" && !apps.contains_key(&app) {
            tracing::info!(app, pid, "reaping orphan cloudflared");
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .output();
            d.store.update_registry(|r| {
                r.cloudflared.remove(&app);
            });
        }
    }
}
