//! hades-coordinator — the operator-run service that hands out stable
//! subdomains under one Cloudflare-managed domain, so end users get a
//! custom name with no Cloudflare account of their own.
//!
//! A host calls POST /claim {name, proxy_url}; the coordinator creates a
//! Cloudflare tunnel, points <name>.<domain> at it, configures ingress to
//! the host's proxy, and returns the connector token the host runs
//! cloudflared with. First-come naming; a shared secret keeps randoms out.
//!
//! Config from the environment (set these as hades secrets when you deploy
//! it as a hades app):
//!   CF_API_TOKEN          operator Cloudflare token (Tunnel:Edit + DNS:Edit)
//!   CF_ACCOUNT_ID         operator Cloudflare account id
//!   HADES_PARENT_DOMAIN   e.g. tryhades.com
//!   COORDINATOR_SECRET    shared secret hosts present to claim (optional but
//!                         strongly recommended)
//!   COORDINATOR_STATE     path to the claims json (default ./claims.json)

mod cloudflare;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use cloudflare::Cloudflare;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
struct Claim {
    name: String,
    hostname: String,
    tunnel_id: String,
    dns_ids: Vec<String>,
    claimed_at: DateTime<Utc>,
}

struct App {
    cf: Cloudflare,
    domain: String,
    secret: Option<String>,
    state_path: String,
    claims: Mutex<HashMap<String, Claim>>,
}

type S = Arc<App>;

#[derive(Deserialize)]
struct ClaimReq {
    name: String,
    /// The host's local proxy the tunnel should reach (default localhost:8787).
    #[serde(default)]
    proxy_url: Option<String>,
}

#[derive(Serialize)]
struct ClaimResp {
    hostname: String,
    connector_token: String,
    tunnel_id: String,
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": msg.into() }))).into_response()
}

fn valid_name(n: &str) -> bool {
    if n == "@" {
        return true; // the apex (root domain + www)
    }
    !n.is_empty()
        && n.len() <= 40
        && n.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !n.starts_with('-')
        && !n.ends_with('-')
        && n != "api"
        && n != "www"
}

fn check_secret(s: &S, headers: &axum::http::HeaderMap) -> bool {
    match &s.secret {
        None => true,
        Some(expected) => headers
            .get("x-hades-secret")
            .and_then(|v| v.to_str().ok())
            .map(|got| got == expected)
            .unwrap_or(false),
    }
}

async fn save(s: &S) {
    let claims = s.claims.lock().await;
    if let Ok(j) = serde_json::to_string_pretty(&*claims) {
        let _ = std::fs::write(&s.state_path, j);
    }
}

async fn claim(
    State(s): State<S>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ClaimReq>,
) -> Response {
    if !check_secret(&s, &headers) {
        return err(StatusCode::UNAUTHORIZED, "bad or missing X-Hades-Secret");
    }
    let name = req.name.to_lowercase();
    if !valid_name(&name) {
        return err(
            StatusCode::BAD_REQUEST,
            "name must be '@' (apex) or lowercase [a-z0-9-], 1–40 chars, not 'api'/'www'",
        );
    }
    let apex = name == "@";
    let hostname = if apex {
        s.domain.clone()
    } else {
        format!("{name}.{}", s.domain)
    };

    {
        let claims = s.claims.lock().await;
        if claims.contains_key(&name) {
            return err(StatusCode::CONFLICT, format!("{hostname} is already taken"));
        }
    }
    // hostnames this claim serves; apex also carries www
    let (hostnames, dns_subs): (Vec<String>, Vec<String>) = if apex {
        (
            vec![s.domain.clone(), format!("www.{}", s.domain)],
            vec!["@".to_string(), "www".to_string()],
        )
    } else {
        (vec![hostname.clone()], vec![name.clone()])
    };
    // guard against records that exist in DNS but not our state
    for h in &hostnames {
        match s.cf.find_dns(h).await {
            Ok(Some(_)) => return err(StatusCode::CONFLICT, format!("{h} already exists in DNS")),
            Ok(None) => {}
            Err(e) => return err(StatusCode::BAD_GATEWAY, format!("cloudflare: {e}")),
        }
    }

    let proxy = req
        .proxy_url
        .unwrap_or_else(|| "http://localhost:8787".to_string());

    // 1 · tunnel
    let tname = if apex { "hades-apex".to_string() } else { format!("hades-{name}") };
    let (tunnel_id, connector_token) = match s.cf.create_tunnel(&tname).await {
        Ok(t) => t,
        Err(e) => return err(StatusCode::BAD_GATEWAY, format!("create tunnel: {e}")),
    };
    // 2 · ingress → the host's proxy (it routes by Host header)
    if let Err(e) = s.cf.set_ingress(&tunnel_id, &hostnames, &proxy).await {
        let _ = s.cf.delete_tunnel(&tunnel_id).await;
        return err(StatusCode::BAD_GATEWAY, format!("set ingress: {e}"));
    }
    // 3 · dns (one or two records)
    let mut dns_ids = Vec::new();
    for sub in &dns_subs {
        match s.cf.create_dns(sub, &tunnel_id).await {
            Ok(id) => dns_ids.push(id),
            Err(e) => {
                for id in &dns_ids {
                    let _ = s.cf.delete_dns(id).await;
                }
                let _ = s.cf.delete_tunnel(&tunnel_id).await;
                return err(StatusCode::BAD_GATEWAY, format!("create dns: {e}"));
            }
        }
    }

    s.claims.lock().await.insert(
        name.clone(),
        Claim {
            name: name.clone(),
            hostname: hostname.clone(),
            tunnel_id: tunnel_id.clone(),
            dns_ids,
            claimed_at: Utc::now(),
        },
    );
    save(&s).await;
    tracing::info!(%hostname, "claimed");

    Json(ClaimResp {
        hostname,
        connector_token,
        tunnel_id,
    })
    .into_response()
}

async fn release(
    State(s): State<S>,
    headers: axum::http::HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if !check_secret(&s, &headers) {
        return err(StatusCode::UNAUTHORIZED, "bad or missing X-Hades-Secret");
    }
    let claim = s.claims.lock().await.remove(&name);
    let Some(claim) = claim else {
        return err(StatusCode::NOT_FOUND, "no such claim");
    };
    for id in &claim.dns_ids {
        let _ = s.cf.delete_dns(id).await;
    }
    let _ = s.cf.delete_tunnel(&claim.tunnel_id).await;
    save(&s).await;
    Json(serde_json::json!({ "released": claim.hostname })).into_response()
}

async fn list(State(s): State<S>) -> Response {
    let claims = s.claims.lock().await;
    let mut names: Vec<String> = claims.values().map(|c| c.hostname.clone()).collect();
    names.sort();
    Json(serde_json::json!({ "domain": s.domain, "claims": names })).into_response()
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let token = env("CF_API_TOKEN").expect("CF_API_TOKEN required");
    let account = env("CF_ACCOUNT_ID").expect("CF_ACCOUNT_ID required");
    let domain = env("HADES_PARENT_DOMAIN").expect("HADES_PARENT_DOMAIN required");
    let secret = env("COORDINATOR_SECRET");
    let state_path = env("COORDINATOR_STATE").unwrap_or_else(|| "claims.json".into());

    let zone_id = Cloudflare::resolve_zone(&token, &domain)
        .await
        .unwrap_or_else(|e| panic!("cannot resolve zone for {domain}: {e}"));

    let claims: HashMap<String, Claim> = std::fs::read_to_string(&state_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();

    let app = Arc::new(App {
        cf: Cloudflare::new(token, account, zone_id),
        domain: domain.clone(),
        secret,
        state_path,
        claims: Mutex::new(claims),
    });

    let router = Router::new()
        .route("/health", get(health))
        .route("/claim", post(claim))
        .route("/claim/{name}", axum::routing::delete(release))
        .route("/claims", get(list))
        .with_state(app);

    let port: u16 = env("PORT").and_then(|p| p.parse().ok()).unwrap_or(8000);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("coordinator for {domain} on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();
}
