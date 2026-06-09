//! Persisted desired state + the process registry. JSON files behind a
//! small store so the reconcile loop survives daemon restarts; the `Store`
//! shape is the seam for SQLite later.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use hades_api::types::AppState;
use hades_core::events::PauseReason;
use hades_core::AppSpec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub container_id: String,
    pub host_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    pub spec: AppSpec,
    pub state: AppState,
    pub created_at: DateTime<Utc>,
    pub replicas: Vec<ReplicaRecord>,
    pub tunnel_url: Option<String>,
    pub url_changed_at: Option<DateTime<Utc>>,
    pub paused_reason: Option<PauseReason>,
    pub restarts: u32,
    /// Recent OOM-kill timestamps, pruned to the crash-loop window.
    #[serde(default)]
    pub oom_times: Vec<DateTime<Utc>>,
}

impl AppRecord {
    pub fn new(spec: AppSpec) -> Self {
        Self {
            spec,
            state: AppState::Deploying,
            created_at: Utc::now(),
            replicas: Vec::new(),
            tunnel_url: None,
            url_changed_at: None,
            paused_reason: None,
            restarts: 0,
            oom_times: Vec::new(),
        }
    }
}

/// Cloudflared PIDs we own, so the reaper can kill orphans after a crash.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Registry {
    pub cloudflared: HashMap<String, u32>,
    pub daemon_pid: Option<u32>,
}

/// The hub's record of its fleet: joined devices plus where remote apps
/// were placed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FleetFile {
    pub devices: Vec<crate::fleet::FleetDeviceRecord>,
    /// app name -> device name (apps absent here run on the hub itself)
    pub placements: HashMap<String, String>,
}

pub struct Store {
    apps_path: PathBuf,
    registry_path: PathBuf,
    fleet_path: PathBuf,
    apps: Mutex<HashMap<String, AppRecord>>,
    registry: Mutex<Registry>,
    fleet: Mutex<FleetFile>,
}

impl Store {
    pub fn load(apps_path: PathBuf, registry_path: PathBuf, fleet_path: PathBuf) -> Self {
        let apps = std::fs::read_to_string(&apps_path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let registry = std::fs::read_to_string(&registry_path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let fleet = std::fs::read_to_string(&fleet_path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        Self {
            apps_path,
            registry_path,
            fleet_path,
            apps: Mutex::new(apps),
            registry: Mutex::new(registry),
            fleet: Mutex::new(fleet),
        }
    }

    fn persist_apps(&self, apps: &HashMap<String, AppRecord>) {
        if let Ok(json) = serde_json::to_string_pretty(apps) {
            let tmp = self.apps_path.with_extension("tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, &self.apps_path);
            }
        }
    }

    fn persist_registry(&self, reg: &Registry) {
        if let Ok(json) = serde_json::to_string_pretty(reg) {
            let tmp = self.registry_path.with_extension("tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, &self.registry_path);
            }
        }
    }

    /// Read-only access to a snapshot of all apps.
    pub fn snapshot(&self) -> HashMap<String, AppRecord> {
        self.apps.lock().unwrap().clone()
    }

    pub fn get(&self, name: &str) -> Option<AppRecord> {
        self.apps.lock().unwrap().get(name).cloned()
    }

    /// Mutate one app record (or the whole map) and persist.
    pub fn update<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HashMap<String, AppRecord>) -> R,
    {
        let mut apps = self.apps.lock().unwrap();
        let r = f(&mut apps);
        self.persist_apps(&apps);
        r
    }

    pub fn update_registry<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Registry) -> R,
    {
        let mut reg = self.registry.lock().unwrap();
        let r = f(&mut reg);
        self.persist_registry(&reg);
        r
    }

    pub fn registry_snapshot(&self) -> Registry {
        self.registry.lock().unwrap().clone()
    }

    pub fn fleet_snapshot(&self) -> FleetFile {
        self.fleet.lock().unwrap().clone()
    }

    pub fn update_fleet<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut FleetFile) -> R,
    {
        let mut fleet = self.fleet.lock().unwrap();
        let r = f(&mut fleet);
        if let Ok(json) = serde_json::to_string_pretty(&*fleet) {
            let tmp = self.fleet_path.with_extension("tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, &self.fleet_path);
            }
        }
        r
    }
}
