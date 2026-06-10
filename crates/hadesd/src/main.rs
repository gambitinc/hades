//! hadesd — the host daemon. Wires the event bus to every subsystem:
//! API server, reverse proxy, reconcile, watchdog, power monitor, uptime
//! heartbeat, dead-man's switch, doctor refresh, and the policy engine.

mod api;
mod daemon;
mod fleet;
mod policy;
mod power;
mod reports;
mod secrets;
mod state;
mod watchdog;

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use hades_core::events::{DowntimeCause, HostEvent, PressureLevel, Severity};
use hades_core::{EventEnvelope, HadesConfig, HadesPaths};
use hades_host::{HostProbe, MacProbe};
use hades_proxy::RouteTable;
use hades_runtime::Runtime;
use hades_sentinel::uptime::DowntimeRecord;
use hades_sentinel::{DeadMansSwitch, Notifiers, UptimeLedger};
use hades_tunnel::QuickTunnelProvider;

use crate::daemon::Daemon;
use crate::state::Store;

fn short_hostname() -> String {
    std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "device".into())
}

fn generate_token() -> String {
    // 32 bytes of OS randomness, hex-encoded, no extra deps
    let mut buf = [0u8; 32];
    getrandom(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

fn getrandom(buf: &mut [u8]) {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(buf))
        .expect("cannot read /dev/urandom");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let paths = HadesPaths::new();
    if let Err(e) = paths.ensure_dirs() {
        eprintln!("fatal: cannot create {}: {e}", paths.root.display());
        std::process::exit(1);
    }
    let mut config = HadesConfig::load_or_default(&paths.config());
    // self-heal older configs: the API is never served without a token
    if config.auth_token.is_none() {
        config.auth_token = Some(generate_token());
        if let Err(e) = config.save(&paths.config()) {
            tracing::warn!("could not persist generated auth token: {e}");
        }
    }

    let runtime = match Runtime::connect() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("fatal: cannot construct docker client: {e}");
            std::process::exit(1);
        }
    };

    let (bus, _) = tokio::sync::broadcast::channel::<EventEnvelope>(1024);
    let ledger = UptimeLedger::new(paths.heartbeat(), paths.uptime_ledger());

    // --- startup downtime classification (before the first new heartbeat) ---
    let probe = MacProbe;
    let last_heartbeat = ledger.last_heartbeat();
    let now = Utc::now();
    let mut startup_events: Vec<HostEvent> = Vec::new();
    let mut unclean = false;
    if let Some(last) = last_heartbeat {
        let gap = (now - last).num_seconds().max(0) as u64;
        if gap > config.heartbeat_secs * 3 + 5 {
            let cause = hades_host::probe::classify_downtime(
                last,
                probe.boot_time(),
                &probe.sleep_events(),
                now,
            );
            unclean = matches!(cause, DowntimeCause::Crashed | DowntimeCause::Unknown);
            let _ = ledger.append(&DowntimeRecord {
                start: last,
                end: now,
                secs: gap,
                cause,
            });
            startup_events.push(HostEvent::HostUp {
                downtime_secs: gap,
                cause,
            });
        }
    }
    startup_events.insert(
        0,
        HostEvent::DaemonStarted {
            unclean_shutdown: unclean,
        },
    );

    let d = Arc::new(Daemon {
        notifiers: Notifiers::from_config(&config.notify),
        provider: QuickTunnelProvider::detect(),
        store: Store::load(paths.apps_state(), paths.registry(), paths.fleet_state(), paths.claims_state()),
        table: RouteTable::new(),
        tunnel_cancels: Mutex::new(Default::default()),
        doctor_green: AtomicBool::new(false),
        doctor_failed: Mutex::new(vec!["starting".into()]),
        control_url: Mutex::new(None),
        started_at: now,
        ledger,
        last_total_cpu_pct: Mutex::new(0.0),
        pressure: Mutex::new(PressureLevel::Normal),
        http: reqwest::Client::new(),
        fleet_status: Mutex::new(Default::default()),
        secrets: secrets::SecretStore::open(&paths.root),
        ssh_tunnel: tokio::sync::Mutex::new(None),
        proxy_metrics: std::sync::Arc::new(hades_proxy::Metrics::default()),
        upload_mbps: Mutex::new(None),
        req_per_sec: Mutex::new(0.0),
        fleet_events: Mutex::new(Vec::new()),
        runtime,
        bus: bus.clone(),
        config: config.clone(),
        paths: paths.clone(),
    });

    d.store.update_registry(|reg| {
        reg.daemon_pid = Some(std::process::id());
    });

    // keep the host awake while it's hosting: caffeinate -s holds a power
    // assertion for as long as this child lives (kill_on_drop reaps it).
    // Lid-close sleep is separate and still needs `sudo pmset -c sleep 0`.
    if config.keep_awake {
        match tokio::process::Command::new("caffeinate")
            .arg("-s")
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => {
                tracing::info!("keep-awake: holding a power assertion (caffeinate -s)");
                // leak the handle into the runtime so it lives as long as we do
                std::mem::forget(child);
            }
            Err(e) => tracing::warn!("keep-awake: caffeinate failed: {e}"),
        }
    }

    // --- event consumers ---

    // 1. JSONL persister: every event lands in the ledger.
    {
        let mut rx = bus.subscribe();
        let path = paths.events_ledger();
        tokio::spawn(async move {
            while let Ok(env) = rx.recv().await {
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                {
                    let _ = writeln!(f, "{}", serde_json::to_string(&env).unwrap());
                }
            }
        });
    }

    // 2. Notification fan-out (severity-gated inside Notifiers).
    {
        let mut rx = bus.subscribe();
        let d2 = d.clone();
        tokio::spawn(async move {
            while let Ok(env) = rx.recv().await {
                if env.event.severity() >= Severity::Default {
                    d2.notifiers.notify_event(&env.event).await;
                }
            }
        });
    }

    // 3. Policy engine: events -> pause/resume actions.
    {
        let mut rx = bus.subscribe();
        let d2 = d.clone();
        tokio::spawn(async move {
            while let Ok(env) = rx.recv().await {
                let views: Vec<policy::AppView> = d2
                    .store
                    .snapshot()
                    .values()
                    .map(|r| policy::AppView {
                        name: r.spec.name.clone(),
                        priority: r.spec.priority,
                        state: r.state,
                        paused_reason: r.paused_reason,
                        on_battery: r.spec.power.on_battery,
                    })
                    .collect();
                for action in policy::decide(&env.event, &views) {
                    match action {
                        policy::Action::Pause { app, reason } => {
                            if let Err(e) = d2.pause_app(&app, reason).await {
                                tracing::warn!(app, "policy pause failed: {e}");
                            }
                        }
                        policy::Action::Resume { app } => {
                            if let Err(e) = d2.resume_app(&app).await {
                                tracing::warn!(app, "policy resume failed: {e}");
                            }
                        }
                    }
                }
            }
        });
    }

    // --- doctor: initial pass + refresh loop (gates deploys) ---
    {
        let d2 = d.clone();
        let config2 = config.clone();
        let paths2 = paths.clone();
        tokio::spawn(async move {
            let mut was_green = false;
            loop {
                let report = hades_host::run_doctor(&config2, &paths2, true).await;
                let failed = report.failed_names();
                d2.doctor_green.store(report.green, Ordering::Relaxed);
                *d2.doctor_failed.lock().unwrap() = failed.clone();
                if was_green && !report.green {
                    d2.emit(HostEvent::DoctorRed {
                        failed_checks: failed,
                    });
                }
                was_green = report.green;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    // --- proxy ---
    {
        let table = d.table.clone();
        let port = config.proxy_port;
        let metrics = d.proxy_metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = hades_proxy::run_proxy(table, port, metrics).await {
                tracing::error!("proxy died: {e}");
            }
        });
    }

    // --- dashboard data: req/s sampler (1Hz) + bandwidth probe (periodic) ---
    {
        let d2 = d.clone();
        tokio::spawn(async move {
            let mut last = d2.proxy_metrics.snapshot().0;
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let now = d2.proxy_metrics.snapshot().0;
                *d2.req_per_sec.lock().unwrap() = now.saturating_sub(last) as f64;
                last = now;
            }
        });
    }
    {
        let d2 = d.clone();
        tokio::spawn(async move {
            // measure upstream throughput by POSTing a few MB to Cloudflare's
            // speed backend and timing it; the network is the real ceiling on
            // how many requests a home host can serve.
            let http = reqwest::Client::new();
            let payload = vec![0u8; 4 * 1024 * 1024]; // 4 MB
            loop {
                let start = std::time::Instant::now();
                let ok = http
                    .post("https://speed.cloudflare.com/__up")
                    .body(payload.clone())
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await
                    .is_ok();
                if ok {
                    let secs = start.elapsed().as_secs_f64().max(0.001);
                    let mbps = (payload.len() as f64 * 8.0) / 1_000_000.0 / secs;
                    *d2.upload_mbps.lock().unwrap() = Some(mbps);
                }
                tokio::time::sleep(Duration::from_secs(180)).await;
            }
        });
    }

    // --- reconcile persisted state, then emit startup events ---
    d.reconcile_all().await;
    watchdog::reap(&d).await;
    for ev in startup_events {
        d.emit(ev);
    }

    // --- background loops ---
    tokio::spawn(watchdog::run(d.clone()));
    tokio::spawn(fleet::poll(d.clone()));

    // fleet-log aggregator: pull each device's recent events every 5s so the
    // dashboard can show a combined fleet feed without hammering devices.
    {
        let d2 = d.clone();
        tokio::spawn(async move {
            loop {
                let fleet = d2.store.fleet_snapshot();
                let mut merged: Vec<serde_json::Value> = Vec::new();
                for dev in &fleet.devices {
                    let client = hades_api::DaemonClient::for_host(
                        &dev.control_url,
                        Some(dev.token.clone()),
                    );
                    if let Ok(evs) = client.recent_events(1.0).await {
                        for e in evs.into_iter().rev().take(20) {
                            merged.push(serde_json::json!({
                                "at": e.at,
                                "type": serde_json::to_value(&e.event).ok()
                                    .and_then(|v| v.get("type").cloned())
                                    .unwrap_or_default(),
                                "summary": e.event.summary(),
                                "device": dev.name,
                            }));
                        }
                    }
                }
                merged.sort_by(|a, b| b["at"].as_str().cmp(&a["at"].as_str()));
                merged.truncate(40);
                *d2.fleet_events.lock().unwrap() = merged;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
    tokio::spawn(power::run(d.clone()));

    // uptime heartbeat + live sleep detection
    {
        let d2 = d.clone();
        let hb_secs = config.heartbeat_secs;
        tokio::spawn(async move {
            let mut prev = Utc::now();
            loop {
                tokio::time::sleep(Duration::from_secs(hb_secs)).await;
                let now = Utc::now();
                if let Some(gap) = hades_sentinel::uptime::detect_gap(prev, now, hb_secs) {
                    let probe = MacProbe;
                    let cause = hades_host::probe::classify_downtime(
                        prev,
                        probe.boot_time(),
                        &probe.sleep_events(),
                        now,
                    );
                    let secs = gap.num_seconds().max(0) as u64;
                    let _ = d2.ledger.append(&DowntimeRecord {
                        start: prev,
                        end: now,
                        secs,
                        cause,
                    });
                    d2.emit(HostEvent::HostUp {
                        downtime_secs: secs,
                        cause,
                    });
                }
                let _ = d2.ledger.write_heartbeat();
                prev = now;
            }
        });
    }

    // stable URLs: a single named tunnel for the whole host when a domain
    // is configured. It carries api.<domain> (control) and *.<domain>
    // (apps, by Host header), so nothing rotates on restart.
    let named = d
        .config
        .domain
        .name
        .as_deref()
        .zip(d.config.domain.tunnel_name.as_deref())
        .and_then(|(dom, tun)| hades_tunnel::NamedTunnel::detect(dom, tun));
    let domain_mode = named.is_some();
    if let Some(nt) = named {
        let d2 = d.clone();
        let domain = nt.domain.clone();
        let api_port = config.api_port;
        let proxy_port = config.proxy_port;
        let cfg_dir = paths.root.clone();
        tokio::spawn(async move {
            let id = match nt.ensure().await {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!("named tunnel setup failed: {e}");
                    return;
                }
            };
            // route api.<domain> once; per-app hostnames are routed at deploy
            nt.route_dns(&format!("api.{domain}")).await;
            *d2.control_url.lock().unwrap() = Some(format!("https://api.{domain}"));
            let cfg = match nt.write_config(&cfg_dir, &id, api_port, proxy_port) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("named tunnel config failed: {e}");
                    return;
                }
            };
            loop {
                match nt.run(&cfg) {
                    Ok(mut child) => {
                        tracing::info!("named tunnel up on {domain}");
                        let _ = child.wait().await;
                        tracing::warn!("named tunnel exited; respawning");
                    }
                    Err(e) => tracing::error!("named tunnel run: {e}"),
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    // control tunnel: the daemon's own API, publicly reachable (auth'd),
    // so `hades login --host <url> --token <t>` works from another machine.
    // Skipped in domain mode — api.<domain> already serves the control API.
    if !domain_mode && d.provider.is_some() {
        let d2 = d.clone();
        tokio::spawn(async move {
            while let Some(provider) = d2.provider.as_ref() {
                match provider.provision("__control", d2.config.api_port).await {
                    Ok(mut tunnel) => {
                        if let Some(pid) = tunnel.pid {
                            d2.store.update_registry(|r| {
                                r.cloudflared.insert("__control".into(), pid);
                            });
                        }
                        *d2.control_url.lock().unwrap() = Some(tunnel.url.clone());
                        tracing::info!("control tunnel up: {}", tunnel.url);
                        // fleet self-heal: our URL just changed, so tell the
                        // hub where we live now (join is an upsert by name)
                        if let (Some(hub), Some(hub_tok), Some(own_tok)) = (
                            d2.config.fleet.hub_url.clone(),
                            d2.config.fleet.hub_token.clone(),
                            d2.config.auth_token.clone(),
                        ) {
                            let req = hades_api::types::JoinRequest {
                                name: short_hostname(),
                                control_url: tunnel.url.clone(),
                                token: own_tok,
                            };
                            let hub_client =
                                hades_api::DaemonClient::for_host(&hub, Some(hub_tok));
                            match hub_client.join(&req).await {
                                Ok(_) => tracing::info!("re-registered with hub {hub}"),
                                Err(e) => tracing::warn!(
                                    "hub re-register failed (stale hub URL? re-run `hades host join`): {}",
                                    e.error
                                ),
                            }
                        }
                        tunnel.wait().await;
                        *d2.control_url.lock().unwrap() = None;
                        d2.store.update_registry(|r| {
                            r.cloudflared.remove("__control");
                        });
                        tracing::warn!("control tunnel died; re-provisioning");
                    }
                    Err(e) => {
                        tracing::warn!("control tunnel provision failed: {e}");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // dead-man's switch
    {
        let dms = DeadMansSwitch::new(config.notify.healthchecks_url.clone());
        if dms.enabled() {
            tokio::spawn(async move {
                loop {
                    dms.ping().await;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            });
        }
    }

    // --- API server (blocks forever) ---
    let app = api::router(d.clone());
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.api_port));
    tracing::info!("hadesd API on http://{addr}");
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fatal: cannot bind API port {}: {e}", config.api_port);
            std::process::exit(1);
        }
    };
    axum::serve(listener, app).await.expect("axum serve");
}
