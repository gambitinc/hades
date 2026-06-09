use chrono::{DateTime, Utc};
use hades_core::events::DowntimeCause;
use hades_core::{AppSpec, Priority};
use serde::{Deserialize, Serialize};

pub const DEFAULT_API_PORT: u16 = 8786;
pub const DEFAULT_PROXY_PORT: u16 = 8787;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppState {
    Deploying,
    Running,
    Paused,
    CrashLoop,
    Stopped,
}

impl std::fmt::Display for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AppState::Deploying => "deploying",
            AppState::Running => "running",
            AppState::Paused => "paused",
            AppState::CrashLoop => "crash_loop",
            AppState::Stopped => "stopped",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppInfo {
    pub name: String,
    pub state: AppState,
    /// Which fleet device runs this app. None = the host you asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    pub priority: Priority,
    pub replicas_desired: u8,
    pub replicas_running: u8,
    /// Public (tunnel) URL if a tunnel is up.
    pub url: Option<String>,
    pub local_url: String,
    pub url_changed_at: Option<DateTime<Utc>>,
    pub memory_mb: u64,
    pub cpu: f64,
    pub created_at: DateTime<Utc>,
    pub spec: AppSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployResponse {
    pub app: AppInfo,
    /// True when the deploy replaced a running version (upsert path).
    pub replaced: bool,
    /// Set when no tunnel could be provisioned; explains the degrade.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppStats {
    pub name: String,
    pub requests_total: u64,
    pub inflight: u32,
    pub shed_total: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub replicas: Vec<ReplicaStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaStats {
    pub container_id: String,
    pub memory_used_mb: f64,
    pub memory_limit_mb: u64,
    pub cpu_pct: f64,
    pub state: String,
}

/// The resource ledger: VM-denominated capacity vs declared allocations.
/// This is what an overcommit rejection embeds so an agent knows exactly
/// what to shrink or destroy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLedger {
    pub vm_memory_mb: u64,
    pub vm_cpus: u32,
    pub mac_memory_mb: u64,
    pub reserve_pct: u8,
    pub reserved_mb: u64,
    pub allocatable_mb: u64,
    pub allocated_mb: u64,
    pub allocations: Vec<Allocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub app: String,
    pub memory_mb: u64,
    pub cpu: f64,
    pub replicas: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostStatus {
    pub daemon_version: String,
    pub started_at: DateTime<Utc>,
    pub doctor_green: bool,
    /// Public URL of the daemon's own API (the control tunnel) — what a
    /// remote `hades login --host` connects to. None without cloudflared.
    #[serde(default)]
    pub control_url: Option<String>,
    pub ledger: ResourceLedger,
    pub power: PowerStatus,
    pub availability_pct_7d: Option<f64>,
    pub apps: Vec<AppInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerStatus {
    pub on_ac: bool,
    pub battery_pct: Option<f64>,
    pub cycle_count: Option<u64>,
    pub capacity_pct_of_design: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatteryReport {
    pub sampled_over_days: f64,
    pub sample_count: usize,
    pub cycle_count: Option<u64>,
    pub capacity_pct_of_design: Option<f64>,
    /// Capacity trend over the sample window, percentage points (negative =
    /// degrading).
    pub capacity_trend_pct: Option<f64>,
    pub cycles_per_week: Option<f64>,
    /// % of samples sitting at >=95% charge while on AC — high-SoC dwell is
    /// the main silent degrader for an always-plugged host.
    pub high_soc_dwell_pct: Option<f64>,
    pub avg_temp_c: Option<f64>,
    /// Hades' share of system CPU during the window (container CPU seconds /
    /// total), best-effort.
    pub hades_cpu_share_pct: Option<f64>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DowntimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub secs: u64,
    pub cause: DowntimeCause,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UptimeReport {
    pub since: DateTime<Utc>,
    pub availability_pct: f64,
    pub windows: Vec<DowntimeWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsEntry {
    pub kind: PsKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    pub id: String,
    pub detail: String,
    pub rss_mb: Option<f64>,
    pub cpu_pct: Option<f64>,
    pub state: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PsKind {
    Daemon,
    Container,
    Cloudflared,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsReport {
    pub entries: Vec<PsEntry>,
}

/// A device asking to join a fleet: its name plus how the hub reaches it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub name: String,
    pub control_url: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetDeviceView {
    pub name: String,
    pub control_url: String,
    pub healthy: bool,
    pub vm_memory_mb: u64,
    pub allocatable_mb: u64,
    pub allocated_mb: u64,
    pub free_mb: u64,
    pub apps: u32,
    pub last_seen: Option<DateTime<Utc>>,
    pub added_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetView {
    /// The hub itself, as the first entry (name "local").
    pub devices: Vec<FleetDeviceView>,
}

/// Set/unset secrets for one app. Values are write-only: they go in here
/// and never come back out of any endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretsUpdate {
    #[serde(default)]
    pub set: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub unset: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretsView {
    pub app: String,
    /// Key names only — values never leave the host.
    pub keys: Vec<String>,
    /// Whether the store is keychain-encrypted at rest on that host.
    pub encrypted: bool,
    /// True when running replicas were restarted to pick the change up.
    pub applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub version: String,
    pub doctor_green: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyTestResponse {
    pub delivered_to: Vec<String>,
}
