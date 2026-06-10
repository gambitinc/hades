//! The daemon's shared state and app lifecycle: deploy (idempotent upsert),
//! destroy, pause/resume, tunnel supervision, admission control, and the
//! startup reconcile that brings desired state back to life.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hades_api::types::*;
use hades_core::events::{HostEvent, PauseReason, PressureLevel};
use hades_core::{AppSpec, EventEnvelope, HadesConfig, HadesError, HadesPaths};
use hades_proxy::RouteTable;
use hades_runtime::Runtime;
use hades_sentinel::{Notifiers, UptimeLedger};
use hades_tunnel::QuickTunnelProvider;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::state::{AppRecord, ReplicaRecord, Store};

pub struct Daemon {
    pub config: HadesConfig,
    pub paths: HadesPaths,
    pub store: Store,
    pub runtime: Runtime,
    pub table: RouteTable,
    pub bus: broadcast::Sender<EventEnvelope>,
    pub notifiers: Notifiers,
    pub provider: Option<QuickTunnelProvider>,
    pub tunnel_cancels: Mutex<HashMap<String, CancellationToken>>,
    pub doctor_green: AtomicBool,
    pub doctor_failed: Mutex<Vec<String>>,
    /// Public URL of the control tunnel (the daemon's own API), when up.
    pub control_url: Mutex<Option<String>>,
    pub started_at: DateTime<Utc>,
    pub ledger: UptimeLedger,
    /// Sum of container CPU% from the last watchdog sample — feeds the
    /// battery report's "Hades' share" figure.
    pub last_total_cpu_pct: Mutex<f64>,
    pub pressure: Mutex<PressureLevel>,
    pub http: reqwest::Client,
    /// Last-known health/capacity of each fleet device (poller cache).
    pub fleet_status: Mutex<crate::fleet::FleetStatusMap>,
    /// Per-app secret store (keychain-encrypted where available).
    pub secrets: crate::secrets::SecretStore,
    /// Long-lived ssh:// tunnel for `hades ssh`, spawned on first use.
    pub ssh_tunnel: tokio::sync::Mutex<Option<hades_tunnel::Tunnel>>,
    /// Live proxy throughput counters (for the dashboard).
    pub proxy_metrics: std::sync::Arc<hades_proxy::Metrics>,
    /// Measured upstream bandwidth in Mbps (None until the first probe).
    pub upload_mbps: Mutex<Option<f64>>,
    /// Live requests/sec, sampled once a second from the proxy counters.
    pub req_per_sec: Mutex<f64>,
    /// Recent events pulled from fleet devices (for the dashboard's fleet log).
    pub fleet_events: Mutex<Vec<serde_json::Value>>,
}

impl Daemon {
    pub fn emit(&self, event: HostEvent) {
        tracing::info!(event = ?event, "event");
        let _ = self.bus.send(EventEnvelope::now(event));
    }

    // ----- resource ledger / admission -----

    pub async fn resource_ledger(&self) -> Result<ResourceLedger, HadesError> {
        let info = self.runtime.engine_info().await?;
        let probe = hades_host::MacProbe;
        use hades_host::HostProbe;
        let mac_mb = probe.mac_total_memory_mb().unwrap_or(0);

        let apps = self.store.snapshot();
        let allocations: Vec<Allocation> = apps
            .values()
            .map(|r| Allocation {
                app: r.spec.name.clone(),
                memory_mb: r.spec.resources.memory_mb,
                cpu: r.spec.resources.cpu,
                replicas: r.spec.replicas,
            })
            .collect();
        let allocated_mb: u64 = allocations
            .iter()
            .map(|a| a.memory_mb * a.replicas as u64)
            .sum();
        let reserved_mb = info.vm_memory_mb * self.config.reserve_pct as u64 / 100;
        Ok(ResourceLedger {
            vm_memory_mb: info.vm_memory_mb,
            vm_cpus: info.vm_cpus,
            mac_memory_mb: mac_mb,
            reserve_pct: self.config.reserve_pct,
            reserved_mb,
            allocatable_mb: info.vm_memory_mb.saturating_sub(reserved_mb),
            allocated_mb,
            allocations,
        })
    }

    /// Declared-memory admission control. The error carries the full ledger
    /// as JSON detail so an agent knows exactly what to shrink or destroy.
    pub async fn admit(&self, spec: &AppSpec) -> Result<(), (HadesError, ResourceLedger)> {
        let ledger = match self.resource_ledger().await {
            Ok(l) => l,
            Err(e) => {
                return Err((
                    HadesError::Docker(format!("cannot read engine capacity: {e}")),
                    ResourceLedger {
                        vm_memory_mb: 0,
                        vm_cpus: 0,
                        mac_memory_mb: 0,
                        reserve_pct: self.config.reserve_pct,
                        reserved_mb: 0,
                        allocatable_mb: 0,
                        allocated_mb: 0,
                        allocations: vec![],
                    },
                ))
            }
        };
        // upsert semantics: the app's own existing allocation is freed
        let others_mb: u64 = ledger
            .allocations
            .iter()
            .filter(|a| a.app != spec.name)
            .map(|a| a.memory_mb * a.replicas as u64)
            .sum();
        let requested = spec.resources.memory_mb * spec.replicas as u64;
        if others_mb + requested > ledger.allocatable_mb {
            let reason = format!(
                "requested {}MB ({} x {} replicas); VM has {}MB, {}MB reserved ({}%), {}MB already allocated to [{}]",
                requested,
                spec.resources.memory_mb,
                spec.replicas,
                ledger.vm_memory_mb,
                ledger.reserved_mb,
                ledger.reserve_pct,
                others_mb,
                ledger
                    .allocations
                    .iter()
                    .filter(|a| a.app != spec.name)
                    .map(|a| format!("{}: {}MB", a.app, a.memory_mb * a.replicas as u64))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            self.emit(HostEvent::DeployRejected {
                app: spec.name.clone(),
                reason: reason.clone(),
            });
            return Err((HadesError::AdmissionRejected(reason), ledger));
        }
        Ok(())
    }

    // ----- deploy (idempotent upsert) -----

    pub async fn deploy(
        self: &std::sync::Arc<Self>,
        spec: AppSpec,
        context_tar_gz: Option<Vec<u8>>,
        device: Option<String>,
    ) -> Result<DeployResponse, (HadesError, Option<ResourceLedger>)> {
        spec.validate().map_err(|e| (e, None))?;

        // fleet placement: explicit pin, or capacity-weighted choice across
        // joined devices; "local" always means this hub
        let target = match device.as_deref() {
            Some("local") | Some("") => None,
            Some(name) => {
                if self.device_client(name).is_none() {
                    return Err((
                        HadesError::AppNotFound(format!("no fleet device named '{name}'")),
                        None,
                    ));
                }
                Some(name.to_string())
            }
            None => {
                let needed = spec.resources.memory_mb * spec.replicas as u64;
                self.choose_device(needed).await
            }
        };
        if let Some(dev) = target {
            let client = self.device_client(&dev).expect("device exists");
            let mut resp = client
                .deploy(&spec, context_tar_gz, Some("local"))
                .await
                .map_err(|e| (e.error, None))?;
            resp.app.device = Some(dev.clone());
            self.store.update_fleet(|f| {
                f.placements.insert(spec.name.clone(), dev.clone());
            });
            self.emit(HostEvent::AppDeployed {
                app: format!("{} → {}", spec.name, dev),
                replaced: resp.replaced,
            });
            return Ok(resp);
        }
        // a hub-local deploy clears any stale remote placement
        self.store.update_fleet(|f| {
            f.placements.remove(&spec.name);
        });

        if !self.doctor_green.load(Ordering::Relaxed) {
            let failed = self.doctor_failed.lock().unwrap().join(", ");
            return Err((
                HadesError::DoctorRed(format!(
                    "host not ready ({failed}) — run `hades host doctor`"
                )),
                None,
            ));
        }
        self.admit(&spec).await.map_err(|(e, l)| (e, Some(l)))?;

        // image: build from uploaded context or pull
        let image = if let Some(build) = &spec.build {
            let tar = context_tar_gz.ok_or_else(|| {
                (
                    HadesError::InvalidSpec(
                        "spec has [build] but no context tarball was uploaded".into(),
                    ),
                    None,
                )
            })?;
            self.runtime
                .build_image(&spec.name, &build.dockerfile, &tar, |line| {
                    tracing::info!(app = %spec.name, "build: {line}")
                })
                .await
                .map_err(|e| (e, None))?
        } else {
            let image = spec.image.clone().unwrap();
            self.runtime
                .pull_image(&image, |s| tracing::info!(app = %spec.name, "pull: {s}"))
                .await
                .map_err(|e| (e, None))?;
            image
        };

        let previous = self.store.get(&spec.name);
        let replaced = previous.is_some();
        let old_replicas = previous
            .as_ref()
            .map(|r| r.replicas.clone())
            .unwrap_or_default();

        // start new replicas on fresh ports (old ones keep serving);
        // secrets ride in at this moment only
        let launch_spec = self.spec_with_secrets(&spec);
        let mut new_replicas = Vec::new();
        for i in 0..spec.replicas {
            let port = Runtime::free_host_port().map_err(|e| (e, None))?;
            // replica indices offset by generation so names never collide
            // with still-running old containers
            let idx = if replaced { 100 + i } else { i };
            match self.runtime.run_replica(&launch_spec, &image, idx, port).await {
                Ok(h) => new_replicas.push(ReplicaRecord {
                    container_id: h.container_id,
                    host_port: h.host_port,
                }),
                Err(e) => {
                    for r in &new_replicas {
                        let _ = self.runtime.stop_remove(&r.container_id).await;
                    }
                    return Err((e, None));
                }
            }
        }

        // readiness gate before traffic swap
        if let Err(e) = self.wait_ready(&spec, &new_replicas).await {
            for r in &new_replicas {
                let _ = self.runtime.stop_remove(&r.container_id).await;
            }
            return Err((e, None));
        }

        // swap routes, then retire the old generation
        let record = self.store.update(|apps| {
            let mut rec = apps
                .remove(&spec.name)
                .map(|mut r| {
                    r.spec = spec.clone();
                    r
                })
                .unwrap_or_else(|| AppRecord::new(spec.clone()));
            rec.replicas = new_replicas.clone();
            rec.state = AppState::Running;
            rec.paused_reason = None;
            apps.insert(spec.name.clone(), rec.clone());
            rec
        });
        self.install_routes(&record);

        for r in &old_replicas {
            let _ = self.runtime.stop_remove(&r.container_id).await;
        }

        // rename new generation containers? Not needed — names only matter
        // for collision-avoidance; labels carry identity.

        let mut tunnel_note = None;
        if let Some(dom) = self.config.domain.name.clone() {
            // stable URL: just point DNS at the named tunnel; the proxy
            // already routes <app>.<domain> by Host header
            if let (Some(tun), true) = (
                self.config.domain.tunnel_name.clone(),
                hades_tunnel::cert_exists(),
            ) {
                if let Some(nt) = hades_tunnel::NamedTunnel::detect(&dom, &tun) {
                    let host = format!("{}.{}", spec.name, dom);
                    nt.route_dns(&host).await;
                }
            }
        } else if self.provider.is_some() {
            self.ensure_tunnel_task(&spec.name);
        } else {
            tunnel_note = Some(
                "cloudflared not installed — local URL only (brew install cloudflared)"
                    .to_string(),
            );
        }

        self.emit(HostEvent::AppDeployed {
            app: spec.name.clone(),
            replaced,
        });

        let info = self.app_info(&self.store.get(&spec.name).unwrap());
        Ok(DeployResponse {
            app: info,
            replaced,
            tunnel_note,
        })
    }

    /// Readiness: TCP connect (and HTTP health path when declared) on every
    /// new replica before it receives traffic.
    async fn wait_ready(
        &self,
        spec: &AppSpec,
        replicas: &[ReplicaRecord],
    ) -> Result<(), HadesError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        for r in replicas {
            loop {
                if tokio::time::Instant::now() > deadline {
                    return Err(HadesError::Other(format!(
                        "replica on port {} not ready within 60s",
                        r.host_port
                    )));
                }
                let tcp_ok = tokio::net::TcpStream::connect(("127.0.0.1", r.host_port))
                    .await
                    .is_ok();
                if tcp_ok {
                    match &spec.health_check {
                        None => break,
                        Some(hc) => {
                            let url =
                                format!("http://127.0.0.1:{}{}", r.host_port, hc.path);
                            let ok = self
                                .http
                                .get(&url)
                                .timeout(Duration::from_secs(hc.timeout_secs))
                                .send()
                                .await
                                .map(|resp| resp.status().is_success())
                                .unwrap_or(false);
                            if ok {
                                break;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    /// The spec as the container actually sees it: manifest env with the
    /// app's secrets merged over the top. Secrets exist only at this
    /// boundary — the persisted spec never contains them.
    pub fn spec_with_secrets(&self, spec: &AppSpec) -> AppSpec {
        let mut s = spec.clone();
        for (k, v) in self.secrets.load(&spec.name) {
            s.env.insert(k, v);
        }
        s
    }

    /// Roll an app's replicas in place (same spec & image, fresh env):
    /// start new, health-gate, swap routes, retire old. Used when secrets
    /// change so the containers actually pick them up.
    pub async fn redeploy_in_place(&self, name: &str) -> Result<(), HadesError> {
        let rec = self
            .store
            .get(name)
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        if rec.state != AppState::Running {
            return Ok(()); // paused/crash-looped apps pick secrets up later
        }
        let image = if rec.spec.build.is_some() {
            Runtime::image_tag(name)
        } else {
            rec.spec.image.clone().unwrap_or_default()
        };
        let launch_spec = self.spec_with_secrets(&rec.spec);
        let old = rec.replicas.clone();
        let mut fresh = Vec::new();
        for i in 0..rec.spec.replicas {
            let port = Runtime::free_host_port()?;
            match self
                .runtime
                .run_replica(&launch_spec, &image, 150 + i, port)
                .await
            {
                Ok(h) => fresh.push(ReplicaRecord {
                    container_id: h.container_id,
                    host_port: h.host_port,
                }),
                Err(e) => {
                    for r in &fresh {
                        let _ = self.runtime.stop_remove(&r.container_id).await;
                    }
                    return Err(e);
                }
            }
        }
        if let Err(e) = self.wait_ready(&rec.spec, &fresh).await {
            for r in &fresh {
                let _ = self.runtime.stop_remove(&r.container_id).await;
            }
            return Err(e);
        }
        let rec = self.store.update(|apps| {
            apps.get_mut(name).map(|r| {
                r.replicas = fresh.clone();
                r.clone()
            })
        });
        if let Some(rec) = rec {
            self.install_routes(&rec);
        }
        for r in &old {
            let _ = self.runtime.stop_remove(&r.container_id).await;
        }
        Ok(())
    }

    /// (Re)install the proxy routes for an app: local hostname plus the
    /// tunnel alias when one is live.
    pub fn install_routes(&self, rec: &AppRecord) {
        let mut hostnames = vec![rec.spec.local_hostname()];
        if let Some(dom) = &self.config.domain.name {
            hostnames.push(format!("{}.{}", rec.spec.name, dom));
        }
        if let Some(claim) = self.store.claim_for(&rec.spec.name) {
            if claim.name == "@" {
                // apex: serve both the bare domain and www
                hostnames.push(format!("www.{}", claim.hostname));
            }
            hostnames.push(claim.hostname);
        }
        if let Some(url) = &rec.tunnel_url {
            if let Some(host) = url.strip_prefix("https://") {
                hostnames.push(host.to_string());
            }
        }
        let backends = rec
            .replicas
            .iter()
            .map(|r| {
                format!("127.0.0.1:{}", r.host_port)
                    .parse()
                    .expect("loopback addr parses")
            })
            .collect();
        self.table.set_app(
            &rec.spec.name,
            hostnames,
            backends,
            rec.spec.max_concurrent_requests,
        );
    }

    // ----- tunnels -----

    /// Spawn (once) the per-app tunnel supervision task: provision a quick
    /// tunnel, register the alias, watch for death, re-provision. URLs are
    /// cattle; every re-provision emits UrlChanged.
    pub fn ensure_tunnel_task(self: &std::sync::Arc<Self>, app: &str) {
        let mut cancels = self.tunnel_cancels.lock().unwrap();
        if cancels.contains_key(app) {
            return;
        }
        let token = CancellationToken::new();
        cancels.insert(app.to_string(), token.clone());
        drop(cancels);

        let d = self.clone();
        let app = app.to_string();
        tokio::spawn(async move {
            loop {
                if token.is_cancelled() {
                    break;
                }
                let Some(provider) = d.provider.as_ref() else { break };
                match provider.provision(&app, d.config.proxy_port).await {
                    Ok(mut tunnel) => {
                        let new_url = tunnel.url.clone();
                        if let Some(pid) = tunnel.pid {
                            d.store.update_registry(|reg| {
                                reg.cloudflared.insert(app.clone(), pid);
                            });
                        }
                        let old = d.store.update(|apps| {
                            apps.get_mut(&app).map(|rec| {
                                let old = rec.tunnel_url.replace(new_url.clone());
                                rec.url_changed_at = Some(Utc::now());
                                old
                            })
                        });
                        let Some(old) = old else {
                            // app destroyed in the meantime
                            tunnel.kill().await;
                            break;
                        };
                        if let Some(rec) = d.store.get(&app) {
                            d.install_routes(&rec);
                        }
                        d.emit(HostEvent::UrlChanged {
                            app: app.clone(),
                            old,
                            new: new_url,
                        });

                        tokio::select! {
                            _ = token.cancelled() => {
                                tunnel.kill().await;
                                d.store.update_registry(|reg| {
                                    reg.cloudflared.remove(&app);
                                });
                                break;
                            }
                            _ = tunnel.wait() => {
                                d.store.update_registry(|reg| {
                                    reg.cloudflared.remove(&app);
                                });
                                if !token.is_cancelled() {
                                    d.emit(HostEvent::TunnelDown { app: app.clone() });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(app, "tunnel provision failed: {e}");
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    pub fn stop_tunnel_task(&self, app: &str) {
        if let Some(token) = self.tunnel_cancels.lock().unwrap().remove(app) {
            token.cancel();
        }
    }

    // ----- destroy / pause / resume -----

    pub async fn destroy(&self, name: &str) -> Result<AppInfo, HadesError> {
        let rec = self
            .store
            .get(name)
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        self.stop_tunnel_task(name);
        self.table.remove_app(name);
        for r in &rec.replicas {
            let _ = self.runtime.stop_remove(&r.container_id).await;
        }
        self.store.update(|apps| {
            apps.remove(name);
        });
        self.store.update_registry(|reg| {
            reg.cloudflared.remove(name);
        });
        self.secrets.remove(name);
        if self.store.claim_for(name).is_some() {
            let _ = self.domain_release(name).await;
        }
        self.emit(HostEvent::AppDestroyed { app: name.into() });
        let mut info = self.app_info(&rec);
        info.state = AppState::Stopped;
        Ok(info)
    }

    pub async fn pause_app(
        &self,
        name: &str,
        reason: PauseReason,
    ) -> Result<AppInfo, HadesError> {
        let rec = self
            .store
            .get(name)
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        if rec.state == AppState::Paused {
            return Ok(self.app_info(&rec));
        }
        for r in &rec.replicas {
            self.runtime.pause(&r.container_id).await?;
        }
        let rec = self
            .store
            .update(|apps| {
                apps.get_mut(name).map(|rec| {
                    rec.state = AppState::Paused;
                    rec.paused_reason = Some(reason);
                    rec.clone()
                })
            })
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        self.emit(HostEvent::AppPaused {
            app: name.into(),
            reason,
        });
        Ok(self.app_info(&rec))
    }

    pub async fn resume_app(&self, name: &str) -> Result<AppInfo, HadesError> {
        let rec = self
            .store
            .get(name)
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        if rec.state != AppState::Paused {
            return Ok(self.app_info(&rec));
        }
        for r in &rec.replicas {
            self.runtime.unpause(&r.container_id).await?;
        }
        let rec = self
            .store
            .update(|apps| {
                apps.get_mut(name).map(|rec| {
                    rec.state = AppState::Running;
                    rec.paused_reason = None;
                    rec.clone()
                })
            })
            .ok_or_else(|| HadesError::AppNotFound(name.into()))?;
        self.emit(HostEvent::AppResumed { app: name.into() });
        Ok(self.app_info(&rec))
    }

    // ----- views -----

    pub fn app_info(&self, rec: &AppRecord) -> AppInfo {
        // env values are config the user typed into a manifest, but they
        // still don't belong in API responses, logs, or agent transcripts
        let mut spec = rec.spec.clone();
        for v in spec.env.values_mut() {
            *v = "•••".into();
        }
        AppInfo {
            name: rec.spec.name.clone(),
            state: rec.state,
            device: None,
            priority: rec.spec.priority,
            replicas_desired: rec.spec.replicas,
            replicas_running: rec.replicas.len() as u8,
            url: self
                .store
                .claim_for(&rec.spec.name)
                .map(|c| format!("https://{}", c.hostname))
                .or_else(|| {
                    self.config
                        .domain
                        .name
                        .as_ref()
                        .map(|d| format!("https://{}.{}", rec.spec.name, d))
                })
                .or_else(|| rec.tunnel_url.clone()),
            local_url: format!(
                "http://{}:{}",
                rec.spec.local_hostname(),
                self.config.proxy_port
            ),
            url_changed_at: rec.url_changed_at,
            memory_mb: rec.spec.resources.memory_mb,
            cpu: rec.spec.resources.cpu,
            created_at: rec.created_at,
            spec,
        }
    }

    // ----- stable-domain claims -----

    /// Claim <name>.<domain> for an app via the operator's coordinator:
    /// get a connector token, run cloudflared with it, alias the hostname
    /// to the app. The coordinator already pointed DNS + ingress at us.
    pub async fn domain_claim(
        self: &std::sync::Arc<Self>,
        app: &str,
        name: &str,
        coord_override: Option<(String, Option<String>)>,
    ) -> Result<crate::state::ClaimRecord, HadesError> {
        if self.store.get(app).is_none() {
            return Err(HadesError::AppNotFound(app.into()));
        }
        // a forwarding hub passes its coordinator down so devices don't need
        // it configured; otherwise fall back to this host's own config
        let (coord, coord_secret) = match coord_override {
            Some((url, secret)) => (url, secret),
            None => (
                self.config.domain.coordinator_url.clone().ok_or_else(|| {
                    HadesError::Other(
                        "no coordinator configured; set domain.coordinator_url (ask your operator)"
                            .into(),
                    )
                })?,
                self.config.domain.coordinator_secret.clone(),
            ),
        };
        // release any previous claim for this app first
        if self.store.claim_for(app).is_some() {
            let _ = self.domain_release(app).await;
        }

        let proxy_url = format!("http://localhost:{}", self.config.proxy_port);
        let mut req = self
            .http
            .post(format!("{}/claim", coord.trim_end_matches('/')))
            .json(&serde_json::json!({ "name": name, "proxy_url": proxy_url }));
        if let Some(sec) = &coord_secret {
            req = req.header("x-hades-secret", sec);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| HadesError::Other(format!("coordinator unreachable: {e}")))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            let msg = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(String::from))
                .unwrap_or(body);
            return Err(HadesError::Other(format!("claim refused: {msg}")));
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| HadesError::Other(e.to_string()))?;
        let hostname = v["hostname"].as_str().unwrap_or_default().to_string();
        let token = v["connector_token"].as_str().unwrap_or_default().to_string();
        if hostname.is_empty() || token.is_empty() {
            return Err(HadesError::Other("coordinator returned an incomplete claim".into()));
        }

        let record = crate::state::ClaimRecord {
            app: app.to_string(),
            name: name.to_string(),
            hostname: hostname.clone(),
            connector_token: token.clone(),
        };
        self.store.update_claims(|c| {
            c.insert(app.to_string(), record.clone());
        });
        self.spawn_claim_tunnel(&record);
        if let Some(rec) = self.store.get(app) {
            self.install_routes(&rec);
        }
        self.emit(HostEvent::UrlChanged {
            app: app.to_string(),
            old: None,
            new: format!("https://{hostname}"),
        });
        Ok(record)
    }

    pub async fn domain_release(&self, app: &str) -> Result<String, HadesError> {
        let claim = self
            .store
            .claim_for(app)
            .ok_or_else(|| HadesError::Other(format!("{app} has no claimed domain")))?;
        // stop the tunnel
        self.stop_tunnel_task(&format!("__claim_{app}"));
        if let Some(pid) = self
            .store
            .registry_snapshot()
            .cloudflared
            .get(&format!("__claim_{app}"))
            .copied()
        {
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .output();
        }
        self.store.update_registry(|r| {
            r.cloudflared.remove(&format!("__claim_{app}"));
        });
        // tell the coordinator to drop the tunnel + DNS
        if let Some(coord) = &self.config.domain.coordinator_url {
            let mut req = self
                .http
                .delete(format!("{}/claim/{}", coord.trim_end_matches('/'), claim.name));
            if let Some(sec) = &self.config.domain.coordinator_secret {
                req = req.header("x-hades-secret", sec);
            }
            let _ = req.send().await;
        }
        self.store.update_claims(|c| {
            c.remove(app);
        });
        if let Some(rec) = self.store.get(app) {
            self.install_routes(&rec);
        }
        Ok(claim.hostname)
    }

    /// Supervise one claim's cloudflared (respawn on exit until released).
    pub fn spawn_claim_tunnel(self: &std::sync::Arc<Self>, claim: &crate::state::ClaimRecord) {
        let key = format!("__claim_{}", claim.app);
        let mut cancels = self.tunnel_cancels.lock().unwrap();
        if cancels.contains_key(&key) {
            return;
        }
        let token = CancellationToken::new();
        cancels.insert(key.clone(), token.clone());
        drop(cancels);

        let d = self.clone();
        let claim = claim.clone();
        tokio::spawn(async move {
            loop {
                if token.is_cancelled() {
                    break;
                }
                match hades_tunnel::run_token_tunnel(&claim.connector_token) {
                    Ok(mut child) => {
                        if let Some(pid) = child.id() {
                            d.store.update_registry(|r| {
                                r.cloudflared.insert(key.clone(), pid);
                            });
                        }
                        tracing::info!(app = %claim.app, host = %claim.hostname, "claim tunnel up");
                        tokio::select! {
                            _ = token.cancelled() => { let _ = child.kill().await; break; }
                            _ = child.wait() => {
                                tracing::warn!(app = %claim.app, "claim tunnel exited; respawning");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(app = %claim.app, "claim tunnel failed: {e}");
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ----- startup reconcile -----

    /// Bring persisted desired state back to life after a daemon restart:
    /// containers running, routes registered, tunnels supervised.
    pub async fn reconcile_all(self: &std::sync::Arc<Self>) {
        // bring claimed-domain tunnels back up
        for claim in self.store.claims_snapshot().values() {
            self.spawn_claim_tunnel(claim);
        }
        let apps = self.store.snapshot();
        for (name, rec) in apps {
            let mut alive = Vec::new();
            for r in &rec.replicas {
                match self.runtime.inspect_state(&r.container_id).await {
                    Ok(st) if st.running || st.paused => alive.push(r.clone()),
                    Ok(_) => {
                        // container exists but stopped: try restarting it
                        if self.runtime.restart(&r.container_id).await.is_ok() {
                            alive.push(r.clone());
                        }
                    }
                    Err(_) => {} // gone entirely; respawn below
                }
            }
            // respawn missing replicas from the spec's image
            if alive.len() < rec.spec.replicas as usize {
                let image = if rec.spec.build.is_some() {
                    Runtime::image_tag(&name)
                } else {
                    rec.spec.image.clone().unwrap_or_default()
                };
                let launch_spec = self.spec_with_secrets(&rec.spec);
                for i in alive.len()..rec.spec.replicas as usize {
                    if let Ok(port) = Runtime::free_host_port() {
                        if let Ok(h) = self
                            .runtime
                            .run_replica(&launch_spec, &image, 200 + i as u8, port)
                            .await
                        {
                            alive.push(ReplicaRecord {
                                container_id: h.container_id,
                                host_port: h.host_port,
                            });
                        }
                    }
                }
            }
            let rec = self.store.update(|apps| {
                apps.get_mut(&name).map(|r| {
                    r.replicas = alive.clone();
                    // tunnel URL from before the restart is dead; the tunnel
                    // task will mint a fresh one
                    r.tunnel_url = None;
                    if r.state != AppState::Paused && r.state != AppState::CrashLoop {
                        r.state = AppState::Running;
                    }
                    r.clone()
                })
            });
            if let Some(rec) = rec {
                self.install_routes(&rec);
                if self.provider.is_some() && rec.state == AppState::Running {
                    self.ensure_tunnel_task(&name);
                }
            }
            tracing::info!(app = %name, "reconciled");
        }
    }
}
