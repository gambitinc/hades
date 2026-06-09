use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::HadesError;

/// Daemon configuration at `~/.hades/config.toml`, written by
/// `hades host init` and read by both `hadesd` and the CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HadesConfig {
    pub api_port: u16,
    pub proxy_port: u16,
    /// Percent of Docker-VM memory kept out of the admission budget.
    pub reserve_pct: u8,
    /// Heartbeat cadence for the uptime ledger, seconds.
    pub heartbeat_secs: u64,
    /// Available-memory thresholds (MB) for pressure levels.
    pub pressure_warn_mb: u64,
    pub pressure_critical_mb: u64,
    /// Free-disk threshold (GB) below which doctor goes red.
    pub disk_min_free_gb: f64,
    /// Bearer token required on every API call except /health. Generated at
    /// init (or by the daemon on first start) — this is what `hades login
    /// --token` presents from another machine.
    pub auth_token: Option<String>,
    pub notify: NotifyConfig,
    pub fleet: FleetConfig,
}

/// Device-side record of who owns this machine. Written by
/// `hades host join`; presence means "this device is part of a fleet".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FleetConfig {
    pub hub_url: Option<String>,
    pub hub_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NotifyConfig {
    /// Random ntfy.sh topic generated at init; phone subscribes to it.
    pub ntfy_topic: Option<String>,
    /// Use macOS local notifications too.
    pub mac_notifications: bool,
    /// healthchecks.io ping URL for the dead-man's switch (true host-down
    /// detection needs a third party; a dead host can't speak).
    pub healthchecks_url: Option<String>,
}

impl Default for HadesConfig {
    fn default() -> Self {
        Self {
            api_port: 8786,
            proxy_port: 8787,
            reserve_pct: 20,
            heartbeat_secs: 30,
            pressure_warn_mb: 1024,
            pressure_critical_mb: 512,
            disk_min_free_gb: 5.0,
            auth_token: None,
            notify: NotifyConfig::default(),
            fleet: FleetConfig::default(),
        }
    }
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            ntfy_topic: None,
            mac_notifications: true,
            healthchecks_url: None,
        }
    }
}

impl HadesConfig {
    pub fn load(path: &Path) -> Result<Self, HadesError> {
        let raw = std::fs::read_to_string(path)?;
        toml::from_str(&raw).map_err(|e| HadesError::Other(format!("bad config: {e}")))
    }

    pub fn load_or_default(path: &Path) -> Self {
        Self::load(path).unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> Result<(), HadesError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self).expect("config serializes"))?;
        // the auth token lives in here
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }
}
