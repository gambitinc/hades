//! `hades test` — a networking self-check run from the hub. Verifies each
//! machine is up, measures real response rate/latency against each app's
//! public URL, and confirms spread apps actually load-balance across the
//! fleet (by reading each machine's served-request counter before and after a
//! controlled burst).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hades_api::types::{FleetView, TestCheck, TestReport};

use crate::daemon::Daemon;

const BURST: usize = 40;

pub async fn run(d: &Arc<Daemon>, scope: &str) -> TestReport {
    let mut checks = Vec::new();
    let fleet = d.fleet_view().await;
    let self_name = fleet
        .devices
        .iter()
        .find(|m| m.is_self)
        .map(|m| m.name.clone())
        .unwrap_or_else(|| "hub".into());

    // a machine is in scope for "fleet", for its own name, or "local" = the hub
    let in_scope = |name: &str, is_self: bool| {
        scope == "fleet" || scope == name || (scope == "local" && is_self)
    };

    // 1 · uptime / health
    for m in &fleet.devices {
        if !in_scope(&m.name, m.is_self) {
            continue;
        }
        checks.push(TestCheck {
            name: format!("{} up & healthy", m.name),
            pass: m.healthy,
            detail: if m.healthy {
                format!(
                    "doctor green · {} apps · max {} r/s (hardware {} r/s)",
                    m.apps,
                    m.capacity_req_per_sec.map(|c| c.round() as u64).unwrap_or(0),
                    m.machine_req_per_sec.map(|c| c.round() as u64).unwrap_or(0),
                )
            } else {
                "unreachable or doctor red".into()
            },
            group: "uptime".into(),
        });
    }

    // 2 & 3 · response rate + load balance per app that has a public URL
    let apps = d.store.snapshot();
    let spreads = d.store.fleet_snapshot().spreads;
    let mut names: Vec<String> = apps.keys().cloned().collect();
    names.sort();

    for name in names {
        let Some(claim) = d.store.claim_for(&name) else {
            continue; // no public URL to drive
        };
        let spread_to = spreads.get(&name).cloned().unwrap_or_default();
        let is_spread = !spread_to.is_empty();

        // device scope: only apps that actually run on that device
        if scope != "fleet" && scope != "local" && !spread_to.iter().any(|x| x == scope) {
            continue;
        }

        let url = format!("https://{}/", claim.hostname);
        let before = served_totals(d, &fleet, &self_name).await;
        let (ok, p50, p95, elapsed) = fire(&d.http, &url, BURST).await;
        let after = served_totals(d, &fleet, &self_name).await;

        let rps = if elapsed > 0.0 { ok as f64 / elapsed } else { 0.0 };
        checks.push(TestCheck {
            name: format!("{name} response"),
            pass: ok == BURST,
            detail: format!(
                "{ok}/{BURST} ok · p50 {p50:.0}ms · p95 {p95:.0}ms · {rps:.0} req/s",
            ),
            group: "response".into(),
        });

        if is_spread {
            let deltas: Vec<(String, u64)> = after
                .iter()
                .map(|(m, a)| (m.clone(), a.saturating_sub(*before.get(m).unwrap_or(&0))))
                .filter(|(_, v)| *v > 0)
                .collect();
            let total: u64 = deltas.iter().map(|(_, v)| v).sum();
            let machines_used = deltas.len();
            let detail = if total == 0 {
                "no per-machine attribution this run".into()
            } else {
                deltas
                    .iter()
                    .map(|(m, v)| format!("{m} {v}"))
                    .collect::<Vec<_>>()
                    .join(" · ")
            };
            // healthy balance = the burst was served by >=2 machines and most
            // of it was attributed
            let pass = machines_used >= 2 && total >= (BURST as u64 * 7 / 10);
            checks.push(TestCheck {
                name: format!("{name} load balance"),
                pass,
                detail: format!("{detail} (across {machines_used} machine(s))"),
                group: "load balance".into(),
            });
        }
    }

    let passed = checks.iter().filter(|c| c.pass).count();
    let failed = checks.len() - passed;
    TestReport {
        scope: scope.into(),
        checks,
        passed,
        failed,
    }
}

/// Each machine's cumulative served-request counter: the hub from its own
/// metrics, devices fresh from their /host.
async fn served_totals(
    d: &Arc<Daemon>,
    fleet: &FleetView,
    self_name: &str,
) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    out.insert(self_name.to_string(), d.proxy_metrics.snapshot().0);
    for m in &fleet.devices {
        if m.is_self {
            continue;
        }
        if let Some(client) = d.device_client(&m.name) {
            if let Ok(s) = client.host_status().await {
                out.insert(m.name.clone(), s.total_served);
            }
        }
    }
    out
}

/// Fire `k` sequential GETs and return (successes, p50ms, p95ms, elapsed_secs).
async fn fire(http: &reqwest::Client, url: &str, k: usize) -> (usize, f64, f64, f64) {
    let start = Instant::now();
    let mut oks = 0;
    let mut lat = Vec::new();
    for _ in 0..k {
        let t = Instant::now();
        if let Ok(r) = http.get(url).timeout(Duration::from_secs(10)).send().await {
            if r.status().is_success() {
                oks += 1;
                lat.push(t.elapsed().as_secs_f64() * 1000.0);
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| {
        if lat.is_empty() {
            0.0
        } else {
            lat[(((lat.len() - 1) as f64) * q) as usize]
        }
    };
    (oks, pct(0.5), pct(0.95), elapsed)
}
