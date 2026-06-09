use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Everything observable in the system flows through this enum. Producers:
/// reconcile loop, watchdog, power monitor, tunnel supervisor, sentinel.
/// Consumers: notification fan-out, policy engine, the JSONL ledger, and the
/// `/events` NDJSON stream (i.e. `hades events --follow`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostEvent {
    /// Synthesized on recovery: the host was unreachable for `secs`,
    /// classified by the uptime ledger.
    HostUp {
        downtime_secs: u64,
        cause: DowntimeCause,
    },
    DaemonStarted {
        unclean_shutdown: bool,
    },
    AppDeployed {
        app: String,
        replaced: bool,
    },
    AppDestroyed {
        app: String,
    },
    AppPaused {
        app: String,
        reason: PauseReason,
    },
    AppResumed {
        app: String,
    },
    AppOomKilled {
        app: String,
        memory_mb: u64,
        restarts: u32,
    },
    AppCrashLoop {
        app: String,
        restarts: u32,
    },
    AppUnhealthy {
        app: String,
        detail: String,
    },
    UrlChanged {
        app: String,
        old: Option<String>,
        new: String,
    },
    TunnelDown {
        app: String,
    },
    MemoryPressure {
        level: PressureLevel,
        available_mb: u64,
    },
    OnBattery,
    OnAc,
    BatteryHealthDelta {
        capacity_pct: f64,
        cycle_count: u64,
    },
    DiskLow {
        free_gb: f64,
    },
    DeployRejected {
        app: String,
        reason: String,
    },
    DoctorRed {
        failed_checks: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DowntimeCause {
    Slept,
    Rebooted,
    Crashed,
    Unknown,
}

impl std::fmt::Display for DowntimeCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DowntimeCause::Slept => "slept",
            DowntimeCause::Rebooted => "rebooted",
            DowntimeCause::Crashed => "crashed",
            DowntimeCause::Unknown => "unknown",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum PressureLevel {
    Normal,
    Warn,
    Critical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    MemoryPressure,
    OnBattery,
    Manual,
}

/// Notification urgency, mapped onto ntfy.sh priority levels by the sentinel.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Default,
    Urgent,
}

impl HostEvent {
    pub fn severity(&self) -> Severity {
        use HostEvent::*;
        match self {
            HostUp { cause, .. } => match cause {
                DowntimeCause::Crashed | DowntimeCause::Unknown => Severity::Urgent,
                _ => Severity::Default,
            },
            AppOomKilled { .. } | AppCrashLoop { .. } | DoctorRed { .. } | DiskLow { .. } => {
                Severity::Urgent
            }
            MemoryPressure { level, .. } if *level == PressureLevel::Critical => Severity::Urgent,
            UrlChanged { .. } | OnBattery | OnAc | AppPaused { .. } | TunnelDown { .. } => {
                Severity::Default
            }
            _ => Severity::Info,
        }
    }

    /// One-line human summary used as the notification body.
    pub fn summary(&self) -> String {
        use HostEvent::*;
        match self {
            HostUp { downtime_secs, cause } => format!(
                "host back up after {} ({})",
                humanize_secs(*downtime_secs),
                cause
            ),
            DaemonStarted { unclean_shutdown } => {
                if *unclean_shutdown {
                    "daemon started after unclean shutdown".into()
                } else {
                    "daemon started".into()
                }
            }
            AppDeployed { app, replaced } => {
                if *replaced {
                    format!("{app}: redeployed")
                } else {
                    format!("{app}: deployed")
                }
            }
            AppDestroyed { app } => format!("{app}: destroyed"),
            AppPaused { app, reason } => format!("{app}: paused ({reason:?})"),
            AppResumed { app } => format!("{app}: resumed"),
            AppOomKilled { app, memory_mb, restarts } => format!(
                "{app}: OOM-killed at {memory_mb}MB limit (restart #{restarts}) — consider raising resources.memory"
            ),
            AppCrashLoop { app, restarts } => {
                format!("{app}: crash loop after {restarts} restarts — restarts stopped")
            }
            AppUnhealthy { app, detail } => format!("{app}: unhealthy — {detail}"),
            UrlChanged { app, new, .. } => format!("{app}: new public URL {new}"),
            TunnelDown { app } => format!("{app}: tunnel down, re-provisioning"),
            MemoryPressure { level, available_mb } => {
                format!("memory pressure {level:?}: {available_mb}MB available")
            }
            OnBattery => "host on battery power".into(),
            OnAc => "host back on AC power".into(),
            BatteryHealthDelta { capacity_pct, cycle_count } => format!(
                "battery health: {capacity_pct:.1}% of design capacity, {cycle_count} cycles"
            ),
            DiskLow { free_gb } => format!("disk low: {free_gb:.1}GB free"),
            DeployRejected { app, reason } => format!("{app}: deploy rejected — {reason}"),
            DoctorRed { failed_checks } => {
                format!("doctor red: {} — deploys refused", failed_checks.join(", "))
            }
        }
    }
}

pub fn humanize_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// What actually lands in `ledger/events.jsonl` and streams over `/events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub event: HostEvent,
}

impl EventEnvelope {
    pub fn now(event: HostEvent) -> Self {
        Self { at: Utc::now(), event }
    }
}
