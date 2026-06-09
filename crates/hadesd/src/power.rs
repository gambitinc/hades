//! Power Intelligence: AC/battery transition events (which the policy
//! engine turns into opt-in pauses) and 5-minute battery telemetry samples
//! that feed the `hades host battery` degradation diagnosis.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use hades_core::events::HostEvent;
use hades_host::{HostProbe, MacProbe};
use serde::{Deserialize, Serialize};

use crate::daemon::Daemon;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatterySample {
    pub at: chrono::DateTime<chrono::Utc>,
    pub on_ac: bool,
    pub soc_pct: Option<f64>,
    pub cycle_count: Option<u64>,
    pub design_capacity_mah: Option<u64>,
    pub nominal_capacity_mah: Option<u64>,
    pub temperature_c: Option<f64>,
    /// Sum of container CPU% at sample time — Hades' concurrent load.
    pub hades_cpu_pct: f64,
}

pub async fn run(d: Arc<Daemon>) {
    let probe = MacProbe;
    let mut prev_on_ac: Option<bool> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sample_countdown = 0u32;

    loop {
        tick.tick().await;

        let Some(power) = probe.power() else { continue };
        if let Some(prev) = prev_on_ac {
            if prev && !power.on_ac {
                d.emit(HostEvent::OnBattery);
            } else if !prev && power.on_ac {
                d.emit(HostEvent::OnAc);
            }
        }
        prev_on_ac = Some(power.on_ac);

        // every 10th tick = 5 minutes
        if sample_countdown == 0 {
            sample_countdown = 10;
            let health = probe.battery_health().unwrap_or_default();
            let sample = BatterySample {
                at: Utc::now(),
                on_ac: power.on_ac,
                soc_pct: power.battery_pct,
                cycle_count: health.cycle_count,
                design_capacity_mah: health.design_capacity_mah,
                nominal_capacity_mah: health.nominal_capacity_mah,
                temperature_c: health.temperature_c,
                hades_cpu_pct: *d.last_total_cpu_pct.lock().unwrap(),
            };
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(d.paths.battery_metrics())
            {
                let _ = writeln!(f, "{}", serde_json::to_string(&sample).unwrap());
            }
        }
        sample_countdown -= 1;
    }
}
