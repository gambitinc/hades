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
use crate::reports;

type D = Arc<Daemon>;

fn status_for(e: &HadesError) -> StatusCode {
    match e.code() {
        "invalid_spec" | "manifest_not_found" => StatusCode::BAD_REQUEST,
        "app_not_found" => StatusCode::NOT_FOUND,
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
        .route("/apps/{name}/pause", post(pause))
        .route("/apps/{name}/resume", post(resume))
        .route("/host", get(host_status))
        .route("/host/battery", get(battery))
        .route("/host/uptime", get(uptime))
        .route("/host/ps", get(ps))
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

async fn deploy(State(d): State<D>, mut multipart: Multipart) -> Response {
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

    match d.deploy(spec, context).await {
        Ok(resp) => Json(resp).into_response(),
        Err((e, ledger)) => err_response(
            &e,
            ledger.map(|l| serde_json::to_value(l).expect("ledger serializes")),
        ),
    }
}

async fn list_apps(State(d): State<D>) -> Json<Vec<AppInfo>> {
    let mut apps: Vec<AppInfo> = d
        .store
        .snapshot()
        .values()
        .map(|r| d.app_info(r))
        .collect();
    apps.sort_by(|a, b| a.name.cmp(&b.name));
    Json(apps)
}

async fn get_app(State(d): State<D>, Path(name): Path<String>) -> Response {
    match d.store.get(&name) {
        Some(rec) => Json(d.app_info(&rec)).into_response(),
        None => err_response(&HadesError::AppNotFound(name), None),
    }
}

async fn destroy(State(d): State<D>, Path(name): Path<String>) -> Response {
    match d.destroy(&name).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn pause(State(d): State<D>, Path(name): Path<String>) -> Response {
    match d.pause_app(&name, PauseReason::Manual).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
    }
}

async fn resume(State(d): State<D>, Path(name): Path<String>) -> Response {
    match d.resume_app(&name).await {
        Ok(info) => Json(info).into_response(),
        Err(e) => err_response(&e, None),
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
