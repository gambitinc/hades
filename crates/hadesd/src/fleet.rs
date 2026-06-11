//! The fleet: other machines you own, joined to this hub. The hub stores
//! how to reach each device (control URL + token), polls their health and
//! capacity, places deploys on whichever machine has the most free declared
//! memory, and proxies app commands to wherever the app lives.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hades_api::types::{FleetDeviceView, FleetView};
use hades_api::DaemonClient;
use serde::{Deserialize, Serialize};

use crate::daemon::Daemon;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetDeviceRecord {
    pub name: String,
    pub control_url: String,
    pub token: String,
    pub added_at: DateTime<Utc>,
}

/// What the poller learned about a device last time it answered.
#[derive(Debug, Clone, Default)]
pub struct DeviceStatus {
    pub healthy: bool,
    pub vm_memory_mb: u64,
    pub allocatable_mb: u64,
    pub allocated_mb: u64,
    pub apps: u32,
    pub last_seen: Option<DateTime<Utc>>,
    pub req_per_sec: f64,
}

impl Daemon {
    pub fn device_client(&self, name: &str) -> Option<DaemonClient> {
        let fleet = self.store.fleet_snapshot();
        let rec = fleet.devices.iter().find(|d| d.name == name)?;
        Some(DaemonClient::for_host(
            &rec.control_url,
            Some(rec.token.clone()),
        ))
    }

    /// Where the named app lives ("device name"), if not on this hub.
    pub fn placement_of(&self, app: &str) -> Option<String> {
        self.store.fleet_snapshot().placements.get(app).cloned()
    }

    /// Pick the device with the most free declared memory — self included.
    /// Returns None when this hub should run the app itself.
    pub async fn choose_device(&self, needed_mb: u64) -> Option<String> {
        let fleet = self.store.fleet_snapshot();
        if fleet.devices.is_empty() {
            return None;
        }
        let self_free = match self.resource_ledger().await {
            Ok(l) => l.allocatable_mb.saturating_sub(l.allocated_mb),
            Err(_) => 0,
        };
        let statuses = self.fleet_status.lock().unwrap().clone();
        let mut best: Option<(String, u64)> = None;
        for d in &fleet.devices {
            if let Some(st) = statuses.get(&d.name) {
                if st.healthy {
                    let free = st.allocatable_mb.saturating_sub(st.allocated_mb);
                    if free >= needed_mb && best.as_ref().map(|(_, f)| free > *f).unwrap_or(true)
                    {
                        best = Some((d.name.clone(), free));
                    }
                }
            }
        }
        match best {
            Some((name, free)) if free > self_free => Some(name),
            _ => None,
        }
    }

    /// The whole fleet as rows: THIS machine (the hub) first, then every
    /// joined device, each with live req/s.
    pub async fn fleet_view(&self) -> FleetView {
        let fleet = self.store.fleet_snapshot();
        let statuses = self.fleet_status.lock().unwrap().clone();
        let placements = &fleet.placements;

        let l = self.resource_ledger().await.ok();
        let vm = l.as_ref().map(|x| x.vm_memory_mb).unwrap_or(0);
        let alloc = l.as_ref().map(|x| x.allocatable_mb).unwrap_or(0);
        let used = l.as_ref().map(|x| x.allocated_mb).unwrap_or(0);
        let mut devices = vec![FleetDeviceView {
            name: short_hostname(),
            control_url: self.control_url.lock().unwrap().clone().unwrap_or_default(),
            healthy: self
                .doctor_green
                .load(std::sync::atomic::Ordering::Relaxed),
            vm_memory_mb: vm,
            allocatable_mb: alloc,
            allocated_mb: used,
            free_mb: alloc.saturating_sub(used),
            apps: self.store.snapshot().len() as u32,
            last_seen: Some(Utc::now()),
            added_at: self.started_at,
            is_self: true,
            req_per_sec: *self.req_per_sec.lock().unwrap(),
        }];

        for d in &fleet.devices {
            let st = statuses.get(&d.name).cloned().unwrap_or_default();
            let placed = placements.values().filter(|v| **v == d.name).count() as u32;
            devices.push(FleetDeviceView {
                name: d.name.clone(),
                control_url: d.control_url.clone(),
                healthy: st.healthy,
                vm_memory_mb: st.vm_memory_mb,
                allocatable_mb: st.allocatable_mb,
                allocated_mb: st.allocated_mb,
                free_mb: st.allocatable_mb.saturating_sub(st.allocated_mb),
                apps: st.apps.max(placed),
                last_seen: st.last_seen,
                added_at: d.added_at,
                is_self: false,
                req_per_sec: st.req_per_sec,
            });
        }
        FleetView { devices }
    }
}

/// Poll every device's /host on a steady cadence so placement and
/// `hades fleet` work from a warm cache.
pub async fn poll(d: Arc<Daemon>) {
    loop {
        let fleet = d.store.fleet_snapshot();
        let mut health_changed = false;
        for dev in &fleet.devices {
            let was_healthy = d
                .fleet_status
                .lock()
                .unwrap()
                .get(&dev.name)
                .map(|s| s.healthy)
                .unwrap_or(false);
            let client = DaemonClient::for_host(&dev.control_url, Some(dev.token.clone()));
            let status = match client.host_status().await {
                Ok(s) => DeviceStatus {
                    healthy: s.doctor_green,
                    vm_memory_mb: s.ledger.vm_memory_mb,
                    allocatable_mb: s.ledger.allocatable_mb,
                    allocated_mb: s.ledger.allocated_mb,
                    apps: s.apps.len() as u32,
                    last_seen: Some(Utc::now()),
                    req_per_sec: s.req_per_sec,
                },
                Err(e) => {
                    tracing::debug!(device = %dev.name, "fleet poll failed: {}", e.error);
                    let mut prev = d
                        .fleet_status
                        .lock()
                        .unwrap()
                        .get(&dev.name)
                        .cloned()
                        .unwrap_or_default();
                    prev.healthy = false;
                    prev
                }
            };
            if status.healthy != was_healthy {
                health_changed = true;
            }
            d.fleet_status
                .lock()
                .unwrap()
                .insert(dev.name.clone(), status);
        }
        // a device flipping health changes which spread backends are live, so
        // re-home routes through this hub right away
        if health_changed {
            d.refresh_all_routes();
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// Refresh one device immediately (used right after a join).
pub async fn poll_one(d: &Arc<Daemon>, name: &str) {
    let fleet = d.store.fleet_snapshot();
    if let Some(dev) = fleet.devices.iter().find(|x| x.name == name) {
        let client = DaemonClient::for_host(&dev.control_url, Some(dev.token.clone()));
        if let Ok(s) = client.host_status().await {
            d.fleet_status.lock().unwrap().insert(
                dev.name.clone(),
                DeviceStatus {
                    healthy: s.doctor_green,
                    vm_memory_mb: s.ledger.vm_memory_mb,
                    allocatable_mb: s.ledger.allocatable_mb,
                    allocated_mb: s.ledger.allocated_mb,
                    apps: s.apps.len() as u32,
                    last_seen: Some(Utc::now()),
                    req_per_sec: s.req_per_sec,
                },
            );
        }
    }
}

/// Merge the hub's own apps with every device's, for `hades apps list`.
pub async fn merged_apps(d: &Arc<Daemon>) -> Vec<hades_api::types::AppInfo> {
    let mut out: Vec<hades_api::types::AppInfo> = d
        .store
        .snapshot()
        .values()
        .map(|r| d.app_info(r))
        .collect();
    let fleet = d.store.fleet_snapshot();
    for dev in &fleet.devices {
        let client = DaemonClient::for_host(&dev.control_url, Some(dev.token.clone()));
        if let Ok(apps) = client.list_apps().await {
            for mut a in apps {
                a.device = Some(dev.name.clone());
                out.push(a);
            }
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub type FleetStatusMap = HashMap<String, DeviceStatus>;

pub(crate) fn short_hostname() -> String {
    std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "host".into())
}
