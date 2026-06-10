//! The loopback HTTP API. JSON in/out matching hades-api's types; errors as
//! `{ "error": { code, message, detail } }` with stable codes.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use hades_api::types::*;
use hades_core::error::ErrorBody;
use hades_core::events::PauseReason;
use hades_core::{AppSpec, HadesError};
use serde::Deserialize;

use crate::daemon::Daemon;
use crate::fleet;
use crate::reports;

type D = Arc<Daemon>;

fn status_for(e: &HadesError) -> StatusCode {
    match e.code() {
        "invalid_spec" | "manifest_not_found" => StatusCode::BAD_REQUEST,
        "app_not_found" => StatusCode::NOT_FOUND,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "admission_rejected" => StatusCode::CONFLICT,
        "doctor_red" => StatusCode::SERVICE_UNAVAILABLE,
        "daemon_unreachable" => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn err_response(e: &HadesError, detail: Option<serde_json::Value>) -> Response {
    let body = match detail {
        Some(d) => ErrorBody::with_detail(e, d),
        None => ErrorBody::new(e),
    };
    (status_for(e), Json(body)).into_response()
}

/// Bearer-token gate on everything but /health. The control tunnel makes
/// this API publicly reachable, so an unauthenticated request must never
/// touch a handler that can run containers.
async fn require_auth(
    State(d): State<D>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let ok = match (&d.config.auth_token, presented) {
        (Some(expected), Some(got)) => {
            // constant-time-ish compare; tokens are fixed-length random hex
            expected.len() == got.len()
                && expected
                    .bytes()
                    .zip(got.bytes())
                    .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                    == 0
        }
        _ => false,
    };
    if ok {
        next.run(req).await
    } else {
        err_response(
            &HadesError::Unauthorized(
                "missing or invalid token — get the login command from `hades host connect-info` on the host".into(),
            ),
            None,
        )
    }
}

pub fn router(d: D) -> Router {
    let open = Router::new().route("/health", get(health));
    let protected = Router::new()
        .route("/apps", post(deploy).get(list_apps))
        .route("/apps/{name}", get(get_app))
        .route("/apps/{name}", delete(destroy))
        .route("/apps/{name}/logs", get(logs))
        .route("/apps/{name}/stats", get(stats))
        .route("/apps/{name}/secrets", get(secrets_list).put(secrets_update))
        .route("/apps/{name}/pause", post(pause))
        .route("/apps/{name}/resume", post(resume))
        .route("/host", get(host_status))
        .route("/host/battery", get(battery))
        .route("/host/uptime", get(uptime))
        .route("/host/ps", get(ps))
        .route("/host/src", get(host_src))
        .route("/host/update", post(host_update))
        .route("/host/ssh", post(host_ssh))
        .route("/domain/claim", post(domain_claim))
        .route("/domain/claim/{app}", delete(domain_release))
        .route("/domain/claims", get(domain_claims))
        .route("/fleet/devices/{name}/ssh", post(fleet_ssh))
        .route("/fleet", get(fleet_list))
        .route("/fleet/devices", post(fleet_join))
        .route("/fleet/devices/{name}", delete(fleet_remove))
        .route("/fleet/update", post(fleet_update))
        .route("/events", get(events))
        .route("/notify/test", post(notify_test))
        .route_layer(axum::middleware::from_fn_with_state(d.clone(), require_auth));
    open.merge(protected)
        // build contexts can be large
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .with_state(d)
}

async fn health(State(d): State<D>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        version: env!("CARGO_PKG_VERSION").into(),
        doctor_green: d.doctor_green.load(Ordering::Relaxed),
    })
}

#[derive(Deserialize)]
struct DeployQuery {
    #[serde(default)]
    device: Option<String>,
}

async fn deploy(
    State(d): State<D>,
    Query(q): Query<DeployQuery>,
    mut multipart: Multipart,
) -> Response {
    let mut spec: Option<AppSpec> = None;
    let mut context: Option<Vec<u8>> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("spec") => {
                let Ok(text) = field.text().await else {
                    return err_response(
                        &HadesError::InvalidSpec("unreadable spec field".into()),
                        None,
                    );
                };
                match serde_json::from_str::<AppSpec>(&text) {
                    Ok(s) => spec = Some(s),
                    Err(e) => {
                        return err_response(
                            &HadesError::InvalidSpec(format!("bad spec JSON: {e}")),
                            None,
                        )
                    }
                }
            }
            Some("context") => match field.bytes().await {
                Ok(b) => context = Some(b.to_vec()),
                Err(e) => {
                    return err_response(
                        &HadesError::InvalidSpec(format!("bad context upload: {e}")),
                        None,
                    )
                }
            },
            _ => {}
        }
    }
    let Some(spec) = spec else {
        return err_response(
            &HadesError::InvalidSpec("multipart 'spec' field missing".into()),
            None,
        );
    };

    match d.deploy(spec, context, q.device).await {
        Ok(resp) => Json(resp).into_response(),
        Err((e, ledger)) => err_response(
            &e,
            ledger.map(|l| serde_json::to_value(l).expect("ledger serializes")),
        ),
    }
}

async fn list_apps(State(d): State<D>) -> Json<Vec<AppInfo>> {
    Json(fleet::merged_apps(&d).await)
}

/// Apps placed on a fleet device get their commands proxied through.
fn remote_for(d: &D, app: &str) -> Option<(String, hades_api::DaemonClient)> {
    let dev = d.placement_of(app)?;
    let client = d.device_client(&dev)?;
    Some((dev, client))
}

fn tag(mut info: AppInfo, device: Option<String>) -> AppInfo {
    info.device = device;
    info
}

async fn get_app(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.get_app(&name).await {
            Ok(info) => Json(tag(info, Some(dev))).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.store.get(&name) {
        Some(rec) => Json(d.app_info(&rec)).into_response(),
        None => err_response(&HadesError::AppNotFound(name), None),
    }
}

async fn destroy(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.destroy(&name).await {
            Ok(info) => {
                d.store.update_fleet(|f| {
                    f.placements.remove(&name);
                });
                Json(tag(info, Some(dev))).into_response()
            }
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.destroy(&name).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn pause(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.pause(&name).await {
            Ok(info) => Json(tag(info, Some(dev))).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.pause_app(&name, PauseReason::Manual).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn resume(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.resume(&name).await {
            Ok(info) => Json(tag(info, Some(dev))).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.resume_app(&name).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn secrets_list(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.secrets_list(&name).await {
            Ok(mut v) => {
                v.device = Some(dev);
                Json(v).into_response()
            }
            Err(e) => err_response(&e.error, None),
        };
    }
    let map = d.secrets.load(&name);
    Json(SecretsView {
        app: name,
        keys: map.keys().cloned().collect(),
        encrypted: d.secrets.encrypted(),
        applied: false,
        device: None,
    })
    .into_response()
}

async fn secrets_update(
    State(d): State<D>,
    Path(name): Path<String>,
    Json(update): Json<SecretsUpdate>,
) -> Response {
    if let Some((dev, client)) = remote_for(&d, &name) {
        return match client.secrets_update(&name, &update).await {
            Ok(mut v) => {
                v.device = Some(dev);
                Json(v).into_response()
            }
            Err(e) => err_response(&e.error, None),
        };
    }
    let mut map = d.secrets.load(&name);
    for (k, v) in &update.set {
        map.insert(k.clone(), v.clone());
    }
    for k in &update.unset {
        map.remove(k);
    }
    if let Err(e) = d.secrets.save(&name, &map) {
        return err_response(&e, None);
    }
    // running replicas restart so the change is real, not pending
    let applied = match d.redeploy_in_place(&name).await {
        Ok(()) => d.store.get(&name).is_some(),
        Err(e) => {
            tracing::warn!(app = %name, "secret apply restart failed: {e}");
            false
        }
    };
    Json(SecretsView {
        app: name,
        keys: map.keys().cloned().collect(),
        encrypted: d.secrets.encrypted(),
        applied,
        device: None,
    })
    .into_response()
}

/// The host serves its own source so fleet devices can update from it —
/// software shared host to host, no registry in between.
async fn host_src(State(d): State<D>) -> Response {
    let src = d
        .config
        .source_dir
        .clone()
        .map(std::path::PathBuf::from)
        .filter(|p| p.join("Cargo.toml").exists())
        .or_else(|| {
            let p = d.paths.root.join("src");
            p.join("Cargo.toml").exists().then_some(p)
        });
    let Some(src) = src else {
        return err_response(
            &HadesError::Other("this host has no source to share (no source_dir)".into()),
            None,
        );
    };
    let out = tokio::process::Command::new("tar")
        .args(["czf", "-", "--exclude", ".git", "--exclude", "target", "-C"])
        .arg(&src)
        .arg(".")
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => Response::builder()
            .header("content-type", "application/gzip")
            .body(Body::from(o.stdout))
            .unwrap(),
        _ => err_response(&HadesError::Other("tar failed".into()), None),
    }
}

/// Kick off a detached self-update: the spawned `hades update` rebuilds,
/// swaps binaries, and bounces this daemon. We answer before we die.
async fn host_update(State(d): State<D>) -> Response {
    let bin = d.paths.root.join("bin").join("hades");
    let cli = if bin.exists() {
        bin.display().to_string()
    } else {
        "hades".to_string()
    };
    let log = d.paths.logs_dir().join("update.log");
    let cmd = format!(
        "sleep 1; {} update >> {} 2>&1",
        cli,
        log.display()
    );
    match std::process::Command::new("sh").args(["-c", &cmd]).spawn() {
        Ok(_) => Json(serde_json::json!({
            "started": true,
            "log": log.display().to_string(),
        }))
        .into_response(),
        Err(e) => err_response(&HadesError::Other(format!("could not spawn update: {e}")), None),
    }
}

/// Fan a self-update out to every joined device: the hub holds their
/// tokens, each device pulls source back from this hub and rebuilds.
async fn fleet_update(State(d): State<D>) -> Response {
    let fleet = d.store.fleet_snapshot();
    let mut results = serde_json::Map::new();
    for dev in &fleet.devices {
        let client =
            hades_api::DaemonClient::for_host(&dev.control_url, Some(dev.token.clone()));
        let r = match client.trigger_update().await {
            Ok(v) => v,
            Err(e) => serde_json::json!({ "started": false, "error": e.error.to_string() }),
        };
        results.insert(dev.name.clone(), r);
    }
    Json(serde_json::json!({ "devices": results })).into_response()
}

/// Open (or reuse) an ssh:// tunnel to this machine's sshd. The tunnel is
/// only a road — authentication stays plain old ssh against this Mac's
/// user accounts.
#[derive(Deserialize)]
struct ClaimBody {
    app: String,
    name: String,
}

async fn domain_claim(State(d): State<D>, Json(b): Json<ClaimBody>) -> Response {
    // an app placed on a fleet device claims through that device
    if let Some((_dev, client)) = remote_for(&d, &b.app) {
        return match client.domain_claim(&b.app, &b.name).await {
            Ok(c) => Json(c).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.domain_claim(&b.app, &b.name).await {
        Ok(rec) => Json(hades_api::types::DomainClaim {
            app: rec.app,
            name: rec.name,
            hostname: rec.hostname,
        })
        .into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn domain_release(State(d): State<D>, Path(app): Path<String>) -> Response {
    if let Some((_dev, client)) = remote_for(&d, &app) {
        return match client.domain_release(&app).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    match d.domain_release(&app).await {
        Ok(host) => Json(serde_json::json!({ "released": host })).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn domain_claims(State(d): State<D>) -> Json<hades_api::types::DomainClaimList> {
    let mut claims: Vec<_> = d
        .store
        .claims_snapshot()
        .into_values()
        .map(|c| hades_api::types::DomainClaim {
            app: c.app,
            name: c.name,
            hostname: c.hostname,
        })
        .collect();
    claims.sort_by(|a, b| a.app.cmp(&b.app));
    Json(hades_api::types::DomainClaimList { claims })
}

async fn host_ssh(State(d): State<D>) -> Response {
    let sshd_up = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio::net::TcpStream::connect(("127.0.0.1", 22)),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false);
    if !sshd_up {
        return err_response(
            &HadesError::Other(
                "Remote Login is off on this machine — System Settings → General → Sharing → Remote Login".into(),
            ),
            None,
        );
    }
    let Some(provider) = d.provider.as_ref() else {
        return err_response(
            &HadesError::Tunnel("cloudflared not installed on this host".into()),
            None,
        );
    };
    let mut slot = d.ssh_tunnel.lock().await;
    let reusable = slot.as_mut().map(|t| t.alive()).unwrap_or(false);
    let url = if reusable {
        slot.as_ref().unwrap().url.clone()
    } else {
        match provider.provision_raw("__ssh", "ssh://localhost:22").await {
            Ok(t) => {
                if let Some(pid) = t.pid {
                    d.store.update_registry(|r| {
                        r.cloudflared.insert("__ssh".into(), pid);
                    });
                }
                let url = t.url.clone();
                *slot = Some(t);
                url
            }
            Err(e) => return err_response(&e, None),
        }
    };
    let user = std::env::var("USER").unwrap_or_default();
    Json(serde_json::json!({ "url": url, "user_hint": user })).into_response()
}

/// Hub side: ask a joined device to open its ssh tunnel.
async fn fleet_ssh(State(d): State<D>, Path(name): Path<String>) -> Response {
    let Some(client) = d.device_client(&name) else {
        return err_response(&HadesError::AppNotFound(format!("device {name}")), None);
    };
    match client.host_ssh().await {
        Ok(v) => Json(v).into_response(),
        Err(e) => err_response(&e.error, None),
    }
}

async fn fleet_join(
    State(d): State<D>,
    Json(req): Json<hades_api::types::JoinRequest>,
) -> Response {
    if req.name == "local" || req.name.is_empty() {
        return err_response(
            &HadesError::InvalidSpec("device name must be non-empty and not 'local'".into()),
            None,
        );
    }
    d.store.update_fleet(|f| {
        f.devices.retain(|x| x.name != req.name);
        f.devices.push(crate::fleet::FleetDeviceRecord {
            name: req.name.clone(),
            control_url: req.control_url.clone(),
            token: req.token.clone(),
            added_at: chrono::Utc::now(),
        });
    });
    fleet::poll_one(&d, &req.name).await;
    tracing::info!(device = %req.name, "fleet: device joined");
    Json(d.fleet_view()).into_response()
}

async fn fleet_list(State(d): State<D>) -> Json<hades_api::types::FleetView> {
    Json(d.fleet_view())
}

async fn fleet_remove(State(d): State<D>, Path(name): Path<String>) -> Response {
    let existed = d.store.update_fleet(|f| {
        let before = f.devices.len();
        f.devices.retain(|x| x.name != name);
        f.placements.retain(|_, v| *v != name);
        f.devices.len() != before
    });
    d.fleet_status.lock().unwrap().remove(&name);
    if existed {
        Json(d.fleet_view()).into_response()
    } else {
        err_response(&HadesError::AppNotFound(format!("device {name}")), None)
    }
}

#[derive(Deserialize)]
struct LogsQuery {
    #[serde(default)]
    follow: bool,
}

async fn logs(
    State(d): State<D>,
    Path(name): Path<String>,
    Query(q): Query<LogsQuery>,
) -> Response {
    if let Some((_dev, client)) = remote_for(&d, &name) {
        return match client.logs(&name, q.follow).await {
            Ok(resp) => {
                let stream = resp.bytes_stream().map(|c| {
                    c.map_err(|e| std::io::Error::other(e.to_string()))
                });
                Response::builder()
                    .header("content-type", "text/plain; charset=utf-8")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
            Err(e) => err_response(&e.error, None),
        };
    }
    let Some(rec) = d.store.get(&name) else {
        return err_response(&HadesError::AppNotFound(name), None);
    };
    let Some(first) = rec.replicas.first() else {
        return err_response(
            &HadesError::Other(format!("{name} has no running replicas")),
            None,
        );
    };
    let stream = d
        .runtime
        .logs(&first.container_id, q.follow, 200)
        .map(|item| match item {
            Ok(line) => Ok::<_, std::io::Error>(axum::body::Bytes::from(line)),
            Err(e) => Ok(axum::body::Bytes::from(format!("[log error: {e}]\n"))),
        });
    Response::builder()
        .header("content-type", "text/plain; charset=utf-8")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn stats(State(d): State<D>, Path(name): Path<String>) -> Response {
    if let Some((_dev, client)) = remote_for(&d, &name) {
        return match client.app_stats(&name).await {
            Ok(st) => Json(st).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    let Some(rec) = d.store.get(&name) else {
        return err_response(&HadesError::AppNotFound(name), None);
    };
    let proxy = d.table.stats(&name).unwrap_or_default();
    let mut replicas = Vec::new();
    for r in &rec.replicas {
        let st = d
            .runtime
            .inspect_state(&r.container_id)
            .await
            .map(|s| s.status)
            .unwrap_or_else(|_| "unknown".into());
        let s = d.runtime.stats_once(&r.container_id).await.ok();
        replicas.push(ReplicaStats {
            container_id: r.container_id.chars().take(12).collect(),
            memory_used_mb: s.as_ref().map(|x| x.memory_used_mb).unwrap_or(0.0),
            memory_limit_mb: rec.spec.resources.memory_mb,
            cpu_pct: s.as_ref().map(|x| x.cpu_pct).unwrap_or(0.0),
            state: st,
        });
    }
    Json(AppStats {
        name: name.clone(),
        requests_total: proxy.requests_total,
        inflight: proxy.inflight,
        shed_total: proxy.shed_total,
        p50_ms: proxy.p50_ms,
        p95_ms: proxy.p95_ms,
        replicas,
    })
    .into_response()
}

async fn host_status(State(d): State<D>) -> Response {
    let ledger = match d.resource_ledger().await {
        Ok(l) => l,
        Err(e) => return err_response(&e, None),
    };
    use hades_host::HostProbe;
    let probe = hades_host::MacProbe;
    let power = probe.power();
    let health = probe.battery_health();
    let mut apps: Vec<AppInfo> = d
        .store
        .snapshot()
        .values()
        .map(|r| d.app_info(r))
        .collect();
    apps.sort_by(|a, b| a.name.cmp(&b.name));

    let week_ago = chrono::Utc::now() - chrono::Duration::days(7);
    Json(HostStatus {
        daemon_version: env!("CARGO_PKG_VERSION").into(),
        started_at: d.started_at,
        doctor_green: d.doctor_green.load(Ordering::Relaxed),
        control_url: d.control_url.lock().unwrap().clone(),
        ledger,
        power: PowerStatus {
            on_ac: power.as_ref().map(|p| p.on_ac).unwrap_or(true),
            battery_pct: power.and_then(|p| p.battery_pct),
            cycle_count: health.as_ref().and_then(|h| h.cycle_count),
            capacity_pct_of_design: health.and_then(|h| h.capacity_pct_of_design()),
        },
        availability_pct_7d: Some(d.ledger.availability_pct(week_ago)),
        apps,
    })
    .into_response()
}

async fn battery(State(d): State<D>) -> Json<BatteryReport> {
    Json(reports::battery_report(&d.paths, 30.0))
}

#[derive(Deserialize)]
struct DaysQuery {
    #[serde(default = "default_days")]
    days: f64,
}
fn default_days() -> f64 {
    7.0
}

async fn uptime(State(d): State<D>, Query(q): Query<DaysQuery>) -> Json<UptimeReport> {
    Json(reports::uptime_report(&d, q.days))
}

async fn ps(State(d): State<D>) -> Json<PsReport> {
    Json(reports::ps_report(&d).await)
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    follow: bool,
    #[serde(default, rename = "days")]
    days: Option<f64>,
}

/// NDJSON: replay the ledger (filtered by ?days=), then keep streaming live
/// events while ?follow=true.
async fn events(State(d): State<D>, Query(q): Query<EventsQuery>) -> Response {
    let cutoff = q
        .days
        .map(|days| chrono::Utc::now() - chrono::Duration::seconds((days * 86400.0) as i64));

    let backlog: Vec<String> = std::fs::read_to_string(d.paths.events_ledger())
        .map(|raw| {
            raw.lines()
                .filter(|l| {
                    let Some(cutoff) = cutoff else { return true };
                    serde_json::from_str::<hades_core::EventEnvelope>(l)
                        .map(|e| e.at >= cutoff)
                        .unwrap_or(false)
                })
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    let mut rx = d.bus.subscribe();
    let stream = async_stream::stream! {
        for line in backlog {
            yield Ok::<_, std::io::Error>(axum::body::Bytes::from(line + "\n"));
        }
        if q.follow {
            loop {
                match rx.recv().await {
                    Ok(env) => {
                        let line = serde_json::to_string(&env).unwrap();
                        yield Ok(axum::body::Bytes::from(line + "\n"));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }
    };
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn notify_test(State(d): State<D>) -> Json<NotifyTestResponse> {
    let delivered = d
        .notifiers
        .send(&hades_sentinel::Notification {
            title: "hades".into(),
            body: "test notification — your host can reach you".into(),
            severity: hades_core::events::Severity::Urgent,
        })
        .await;
    Json(NotifyTestResponse {
        delivered_to: delivered,
    })
}
