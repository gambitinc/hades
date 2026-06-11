//! The loopback HTTP API. JSON in/out matching hades-api's types; errors as
//! `{ "error": { code, message, detail } }` with stable codes.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, delete, get, post};
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
                "missing or invalid token; get the login command from `hades host connect-info` on the host".into(),
            ),
            None,
        )
    }
}

pub fn router(d: D) -> Router {
    let open = Router::new()
        .route("/health", get(health))
        .route("/dashboard", get(dashboard_page));
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
        .route("/host/stabilize", post(host_stabilize))
        .route("/host/adopt-control", post(host_adopt_control))
        .route("/fleet/control-token", post(fleet_control_token))
        .route("/host/ssh", post(host_ssh))
        .route("/dashboard/metrics", get(dashboard_metrics))
        .route("/domain/claim", post(domain_claim))
        .route("/domain/claim/{app}", delete(domain_release))
        .route("/domain/claims", get(domain_claims))
        .route("/fleet/devices/{name}/ssh", post(fleet_ssh))
        .route("/fleet", get(fleet_list))
        .route("/fleet/devices", post(fleet_join))
        .route("/fleet/devices/{name}", delete(fleet_remove))
        .route("/fleet/update", post(fleet_update))
        .route("/apps/{name}/spread", post(app_spread))
        .route("/apps/{name}/spread/{device}", delete(app_gather))
        .route("/_relay/{*path}", any(relay))
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
    /// Stream build progress as NDJSON (keeps the connection alive through a
    /// long build so a remote spread doesn't hit the tunnel-edge timeout).
    #[serde(default)]
    stream: bool,
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

    if q.stream {
        return deploy_streamed(d, spec, context, q.device);
    }
    match d.deploy(spec, context, q.device, None).await {
        Ok(resp) => Json(resp).into_response(),
        Err((e, ledger)) => err_response(
            &e,
            ledger.map(|l| serde_json::to_value(l).expect("ledger serializes")),
        ),
    }
}

/// Run a deploy while streaming NDJSON progress: `{"log":…}` lines during the
/// build (plus a 15s heartbeat so a quiet build still keeps the connection
/// alive), then a final `{"result":…}` or `{"error":…}` line.
fn deploy_streamed(
    d: D,
    spec: AppSpec,
    context: Option<Vec<u8>>,
    device: Option<String>,
) -> Response {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let d2 = d.clone();
    tokio::spawn(async move {
        let result = d2.deploy(spec, context, device, Some(tx.clone())).await;
        let final_json = match result {
            Ok(resp) => serde_json::json!({ "result": resp }),
            Err((e, ledger)) => {
                let eb = match ledger.map(|l| serde_json::to_value(l).expect("ledger serializes")) {
                    Some(detail) => ErrorBody::with_detail(&e, detail),
                    None => ErrorBody::new(&e),
                };
                serde_json::json!({ "error": eb })
            }
        };
        // a record-separator byte marks the terminal line
        let _ = tx.send(format!("\u{1e}{final_json}"));
    });

    let stream = async_stream::stream! {
        let mut hb = tokio::time::interval(std::time::Duration::from_secs(15));
        hb.tick().await; // first tick is immediate; skip it
        loop {
            tokio::select! {
                _ = hb.tick() => {
                    yield Ok::<_, std::io::Error>(axum::body::Bytes::from("{\"log\":\"…\"}\n"));
                }
                msg = rx.recv() => match msg {
                    Some(line) if line.starts_with('\u{1e}') => {
                        yield Ok(axum::body::Bytes::from(line[1..].to_string() + "\n"));
                        break;
                    }
                    Some(line) => {
                        let j = serde_json::json!({ "log": line }).to_string();
                        yield Ok(axum::body::Bytes::from(j + "\n"));
                    }
                    None => break,
                }
            }
        }
    };
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap()
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

#[derive(Deserialize)]
struct StabilizeBody {
    name: String,
}

/// Claim a stable control hostname for THIS host (uses its own coordinator).
async fn host_stabilize(State(d): State<D>, Json(b): Json<StabilizeBody>) -> Response {
    match d.control_claim(&b.name).await {
        Ok(hostname) => Json(serde_json::json!({
            "hostname": hostname,
            "control_url": format!("https://{hostname}"),
        }))
        .into_response(),
        Err(e) => err_response(&e, None),
    }
}

#[derive(Deserialize)]
struct ControlTokenBody {
    name: String,
    #[serde(default = "default_api_port")]
    api_port: u16,
}
fn default_api_port() -> u16 {
    8786
}

/// Hub side: mint a stable control hostname for a device using the hub's
/// coordinator, and hand back the connector token the device runs locally.
async fn fleet_control_token(State(d): State<D>, Json(b): Json<ControlTokenBody>) -> Response {
    let Some(coord) = d.config.domain.coordinator_url.clone() else {
        return err_response(
            &HadesError::Other("this hub has no coordinator configured".into()),
            None,
        );
    };
    let secret = d.config.domain.coordinator_secret.clone();
    match d
        .mint_control_hostname(&coord, secret.as_deref(), &b.name, b.api_port)
        .await
    {
        Ok((hostname, token)) => Json(serde_json::json!({
            "hostname": hostname,
            "connector_token": token,
        }))
        .into_response(),
        Err(e) => err_response(&e, None),
    }
}

#[derive(Deserialize)]
struct AdoptControlBody {
    name: String,
    hostname: String,
    connector_token: String,
}

/// Device side: adopt a hub-minted control hostname (run its named tunnel and
/// serve the API over it as this host's stable control URL).
async fn host_adopt_control(State(d): State<D>, Json(b): Json<AdoptControlBody>) -> Response {
    let record = crate::state::ClaimRecord {
        app: crate::daemon::CONTROL_CLAIM_KEY.to_string(),
        name: b.name,
        hostname: b.hostname.clone(),
        connector_token: b.connector_token,
    };
    d.install_control_claim(record);
    Json(serde_json::json!({
        "hostname": b.hostname,
        "control_url": format!("https://{}", b.hostname),
    }))
    .into_response()
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
    #[serde(default)]
    coordinator_url: Option<String>,
    #[serde(default)]
    coordinator_secret: Option<String>,
}

async fn domain_claim(State(d): State<D>, Json(b): Json<ClaimBody>) -> Response {
    // an app placed on a fleet device claims through that device — and the
    // hub injects its own coordinator so the device needs no config of its own
    if let Some((_dev, client)) = remote_for(&d, &b.app) {
        let cu = d.config.domain.coordinator_url.clone();
        let cs = d.config.domain.coordinator_secret.clone();
        return match client
            .domain_claim(&b.app, &b.name, cu.as_deref(), cs.as_deref())
            .await
        {
            Ok(c) => Json(c).into_response(),
            Err(e) => err_response(&e.error, None),
        };
    }
    // a forwarded claim carries the hub's coordinator in the body; prefer it
    let override_ = b
        .coordinator_url
        .clone()
        .map(|u| (u, b.coordinator_secret.clone()));
    match d.domain_claim(&b.app, &b.name, override_).await {
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
        .filter(|c| c.app != crate::daemon::CONTROL_CLAIM_KEY)
        .map(|c| hades_api::types::DomainClaim {
            app: c.app,
            name: c.name,
            hostname: c.hostname,
        })
        .collect();
    claims.sort_by(|a, b| a.app.cmp(&b.app));
    Json(hades_api::types::DomainClaimList { claims })
}

async fn dashboard_page() -> Response {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(Body::from(include_str!("dashboard.html")))
        .unwrap()
}

fn short_hostname() -> String {
    std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "host".into())
}

/// Read the last `n` events from the local ledger, with summaries.
fn local_events(d: &D, n: usize) -> Vec<serde_json::Value> {
    let raw = std::fs::read_to_string(d.paths.events_ledger()).unwrap_or_default();
    let mut out: Vec<serde_json::Value> = raw
        .lines()
        .rev()
        .take(n)
        .filter_map(|l| serde_json::from_str::<hades_core::EventEnvelope>(l).ok())
        .map(|e| {
            serde_json::json!({
                "at": e.at,
                "type": serde_json::to_value(&e.event).ok()
                    .and_then(|v| v.get("type").cloned()).unwrap_or_default(),
                "summary": e.event.summary(),
            })
        })
        .collect();
    out.reverse(); // oldest first so the log reads top-down then we autoscroll
    out
}

async fn dashboard_metrics(State(d): State<D>) -> Response {
    // A device has no fleet registry of its own, so mirror the hub's dashboard:
    // pulling it up on any machine shows the same whole-fleet view. Falls
    // through to the local view if the hub is unreachable.
    if let (Some(hub), Some(hub_tok)) = (
        d.config.fleet.hub_url.clone(),
        d.config.fleet.hub_token.clone(),
    ) {
        if let Ok(r) = d
            .http
            .get(format!("{}/dashboard/metrics", hub.trim_end_matches('/')))
            .bearer_auth(hub_tok)
            .timeout(std::time::Duration::from_secs(8))
            .send()
            .await
        {
            if r.status().is_success() {
                if let Ok(body) = r.bytes().await {
                    return Response::builder()
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap();
                }
            }
        }
    }

    let ledger = d.resource_ledger().await.ok();
    let (req_total, byte_total, inflight) = d.proxy_metrics.snapshot();

    // capacity: the network is usually the binding constraint, so req/s ≈
    // upstream bytes/sec divided by the average response size, capped by a
    // rough CPU ceiling.
    let avg_resp = if req_total > 0 {
        (byte_total as f64 / req_total as f64).max(200.0)
    } else {
        30_000.0
    };
    let upload_mbps = *d.upload_mbps.lock().unwrap();
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let cpu_cap = cores as f64 * 8000.0;
    let net_cap = upload_mbps.map(|m| (m * 1_000_000.0 / 8.0) / avg_resp);
    let (capacity, cpu_bound) = match net_cap {
        Some(n) => (Some(n.min(cpu_cap)), cpu_cap < n),
        None => (None, false),
    };

    use hades_host::HostProbe;
    let probe = hades_host::MacProbe;
    let power = probe.power();
    let health = probe.battery_health();

    let week = chrono::Utc::now() - chrono::Duration::days(7);
    let fleet = d.fleet_view().await;
    let is_hub = d.config.fleet.hub_url.is_none();
    let self_healthy = d.doctor_green.load(Ordering::Relaxed);

    // fleet.devices already includes THIS machine as the first row (is_self);
    // per-machine load is its live req/s, and the fleet total is their sum
    let mut healthy_count = 0u32;
    let mut fleet_req_per_sec = 0f64;
    let devices: Vec<serde_json::Value> = fleet
        .devices
        .iter()
        .map(|dev| {
            if dev.healthy {
                healthy_count += 1;
            }
            fleet_req_per_sec += dev.req_per_sec;
            serde_json::json!({
                "name": dev.name,
                "is_self": dev.is_self,
                "healthy": dev.healthy,
                "free_mb": dev.free_mb,
                "apps": dev.apps,
                "load": dev.req_per_sec,
                "last_seen": if dev.is_self {
                    "now".to_string()
                } else {
                    dev.last_seen
                        .map(|t| t.format("%H:%M:%S").to_string())
                        .unwrap_or_else(|| "never".into())
                },
            })
        })
        .collect();

    // apps and where each one's instances live (this machine + spreads)
    let fleet_file = d.store.fleet_snapshot();
    let statuses = d.fleet_status.lock().unwrap().clone();
    let self_name = short_hostname();
    let mut app_rows: Vec<serde_json::Value> = Vec::new();
    for (name, rec) in d.store.snapshot() {
        let mut instances = vec![serde_json::json!({
            "machine": self_name,
            "role": "hub",
            "is_self": true,
            "state": rec.state.to_string(),
            "replicas": rec.replicas.len(),
            "healthy": self_healthy && rec.state == AppState::Running,
        })];
        if let Some(devs) = fleet_file.spreads.get(&name) {
            for dev in devs {
                let healthy = statuses.get(dev).map(|s| s.healthy).unwrap_or(false);
                instances.push(serde_json::json!({
                    "machine": dev,
                    "role": "device",
                    "is_self": false,
                    "state": if healthy { "running" } else { "unreachable" },
                    "replicas": rec.spec.replicas,
                    "healthy": healthy,
                }));
            }
        }
        let st = d.table.stats(&name);
        app_rows.push(serde_json::json!({
            "name": name,
            "hostname": d.store.claim_for(&name).map(|c| c.hostname),
            "spread": fleet_file.spreads.get(&name).map(|v| !v.is_empty()).unwrap_or(false),
            "requests": st.as_ref().map(|s| s.requests_total).unwrap_or(0),
            "p95_ms": st.as_ref().map(|s| s.p95_ms).unwrap_or(0.0),
            "instances": instances,
        }));
    }
    for (name, dev) in &fleet_file.placements {
        let healthy = statuses.get(dev).map(|s| s.healthy).unwrap_or(false);
        app_rows.push(serde_json::json!({
            "name": name,
            "hostname": null,
            "spread": false,
            "requests": 0,
            "p95_ms": 0.0,
            "instances": [serde_json::json!({
                "machine": dev,
                "role": "device",
                "is_self": false,
                "state": if healthy { "running" } else { "unreachable" },
                "replicas": 1,
                "healthy": healthy,
            })],
        }));
    }
    app_rows.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    let body = serde_json::json!({
        "machine": {
            "name": short_hostname(),
            "is_hub": is_hub,
            "uptime_pct_7d": d.ledger.availability_pct(week),
            "started": d.started_at.format("%Y-%m-%d %H:%M UTC").to_string(),
            "doctor_green": self_healthy,
            "on_ac": power.as_ref().map(|p| p.on_ac).unwrap_or(true),
            "battery_pct": power.and_then(|p| p.battery_pct),
            "capacity_pct_of_design": health.and_then(|h| h.capacity_pct_of_design()),
            "allocated_mb": ledger.as_ref().map(|l| l.allocated_mb).unwrap_or(0),
            "allocatable_mb": ledger.as_ref().map(|l| l.allocatable_mb).unwrap_or(0),
            "apps": d.store.snapshot().len(),
        },
        "capacity": {
            "req_per_sec": capacity,
            "cpu_bound": cpu_bound,
            "upload_mbps": upload_mbps,
            "avg_response_kb": avg_resp / 1024.0,
            "cpu_cores": cores,
            "current_req_per_sec": *d.req_per_sec.lock().unwrap(),
            "inflight": inflight,
            "total_requests": req_total,
        },
        "fleet": {
            "count": fleet.devices.len(),
            "healthy": healthy_count,
            "req_per_sec": fleet_req_per_sec,
            "devices": devices,
        },
        "apps": app_rows,
        "machine_events": local_events(&d, 40),
        "fleet_events": *d.fleet_events.lock().unwrap(),
    });
    Json(body).into_response()
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
                "Remote Login is off on this machine; System Settings → General → Sharing → Remote Login".into(),
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
    Json(d.fleet_view().await).into_response()
}

async fn fleet_list(State(d): State<D>) -> Json<hades_api::types::FleetView> {
    Json(d.fleet_view().await)
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
        Json(d.fleet_view().await).into_response()
    } else {
        err_response(&HadesError::AppNotFound(format!("device {name}")), None)
    }
}

#[derive(Deserialize)]
struct SpreadBody {
    device: String,
}

/// Run another instance of a hub-hosted app on a fleet device (`all` = every
/// healthy device).
async fn app_spread(
    State(d): State<D>,
    Path(name): Path<String>,
    Json(b): Json<SpreadBody>,
) -> Response {
    if b.device == "all" {
        let view = d.fleet_view().await;
        let mut spread_to = Vec::new();
        let mut errors = Vec::new();
        for dev in view.devices.iter().filter(|x| !x.is_self && x.healthy) {
            match d.spread(&name, &dev.name).await {
                Ok(()) => spread_to.push(dev.name.clone()),
                Err(e) => errors.push(format!("{}: {e}", dev.name)),
            }
        }
        return Json(serde_json::json!({
            "app": name, "spread_to": spread_to, "errors": errors,
        }))
        .into_response();
    }
    match d.spread(&name, &b.device).await {
        Ok(()) => Json(serde_json::json!({ "app": name, "spread_to": b.device })).into_response(),
        Err(e) => err_response(&e, None),
    }
}

/// Stop running an app on one spread device (`all` clears every spread).
async fn app_gather(State(d): State<D>, Path((name, device)): Path<(String, String)>) -> Response {
    let dev = if device == "all" { None } else { Some(device.as_str()) };
    match d.gather(&name, dev).await {
        Ok(removed) => Json(serde_json::json!({ "app": name, "gathered": removed })).into_response(),
        Err(e) => err_response(&e, None),
    }
}

/// One catch-all for `/_relay/<app>[/...]`. A wildcard (rather than `{app}`
/// plus `{app}/{*rest}`) is the only form that matches the bare and
/// trailing-slash cases too, which is exactly what a visitor hitting `/`
/// produces on the hub side.
async fn relay(
    State(d): State<D>,
    Path(path): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let app = path.split('/').next().unwrap_or("").to_string();
    relay_inner(d, app, req).await
}

fn relay_gateway_error() -> Response {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(Body::empty())
        .unwrap()
}

/// Forward a relayed request from the hub to this machine's local instance of
/// the app. Auth'd (the hub presents this device's token); a 502 here tells
/// the hub to try another backend.
async fn relay_inner(d: D, app: String, req: axum::extract::Request) -> Response {
    let Some(addr) = d.table.local_backend(&app) else {
        return relay_gateway_error();
    };
    let prefix = format!("/_relay/{app}");
    let pq = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/")
        .to_string();
    let rest = pq.strip_prefix(&prefix).unwrap_or("");
    let rest = if rest.is_empty() { "/" } else { rest };
    let url = format!("http://{addr}{rest}");

    let (parts, body) = req.into_parts();
    let Ok(body_bytes) = axum::body::to_bytes(body, 256 * 1024 * 1024).await else {
        return relay_gateway_error();
    };
    let Ok(method) = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()) else {
        return relay_gateway_error();
    };
    let mut rb = d.http.request(method, url).body(body_bytes);
    for (name, value) in parts.headers.iter() {
        let n = name.as_str().to_ascii_lowercase();
        if matches!(
            n.as_str(),
            "host" | "authorization" | "content-length" | "connection" | "transfer-encoding"
        ) {
            continue;
        }
        rb = rb.header(name.as_str(), value.as_bytes());
    }
    // a relayed request is real load on THIS machine — count it so this host's
    // live req/s reflects the work it does for spread apps
    use std::sync::atomic::Ordering;
    d.proxy_metrics.requests.fetch_add(1, Ordering::Relaxed);
    d.proxy_metrics.inflight.fetch_add(1, Ordering::Relaxed);
    let out = match rb.send().await {
        Ok(resp) => {
            let mut builder = Response::builder().status(resp.status().as_u16());
            for (name, value) in resp.headers().iter() {
                let n = name.as_str().to_ascii_lowercase();
                if matches!(n.as_str(), "transfer-encoding" | "connection" | "content-length") {
                    continue;
                }
                builder = builder.header(name.as_str(), value.as_bytes());
            }
            let bytes = resp.bytes().await.unwrap_or_default();
            d.proxy_metrics
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            builder.body(Body::from(bytes)).unwrap_or_else(|_| relay_gateway_error())
        }
        Err(e) => {
            tracing::warn!(app = %app, "relay upstream error: {e}");
            relay_gateway_error()
        }
    };
    d.proxy_metrics.inflight.fetch_sub(1, Ordering::Relaxed);
    out
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
        req_per_sec: *d.req_per_sec.lock().unwrap(),
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
            body: "test notification: your host can reach you".into(),
            severity: hades_core::events::Severity::Urgent,
        })
        .await;
    Json(NotifyTestResponse {
        delivered_to: delivered,
    })
}
