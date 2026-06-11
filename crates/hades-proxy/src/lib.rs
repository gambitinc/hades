//! Reverse proxy: one listener, Host-header routing. A hostname maps to a
//! set of replica backends (round-robin), an app can own several hostnames
//! (local `<app>.localhost` plus the tunnel alias), and each app carries an
//! optional in-flight cap — over the cap we shed with 503 + Retry-After
//! rather than letting one app starve the machine.
//!
//! Backends can be local (a loopback replica port) or remote (an instance of
//! the same app on another fleet machine, reached through that machine's
//! control tunnel at `/_relay/<app>`). When an app is spread across machines
//! the hub's route holds both kinds and round-robins across all of them, so
//! the hub is the load balancer and a single dead machine just drops out.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderValue, HOST, RETRY_AFTER};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};

const LATENCY_WINDOW: usize = 1024;

/// One place an app's traffic can go: a loopback replica on this machine, or
/// the same app running on another fleet machine (reached via its relay).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Backend {
    Local(SocketAddr),
    /// `relay` is a full URL prefix like
    /// `https://api.dev.tryhades.com/_relay/<app>`; `token` is that machine's
    /// bearer token. The original request path+query is appended.
    Remote { relay: String, token: String },
}

impl Backend {
    pub fn local(addr: SocketAddr) -> Self {
        Backend::Local(addr)
    }
}

/// Process-wide proxy throughput, read by the dashboard. Cumulative counters;
/// the daemon samples them once a second to derive the live request rate.
#[derive(Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub bytes: AtomicU64,
    pub inflight: AtomicU32,
}

impl Metrics {
    pub fn snapshot(&self) -> (u64, u64, u32) {
        (
            self.requests.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
            self.inflight.load(Ordering::Relaxed),
        )
    }
}

/// Per-app routing state shared by all of its hostnames.
pub struct AppRoute {
    pub app: String,
    backends: RwLock<Vec<Backend>>,
    rr: AtomicUsize,
    max_inflight: Option<u32>,
    inflight: AtomicU32,
    requests: AtomicU64,
    shed: AtomicU64,
    latencies_ms: Mutex<Vec<f64>>,
}

impl AppRoute {
    /// The backend set in round-robin order starting at the next index, so a
    /// caller can try each in turn and fall through a dead one.
    fn backend_order(&self) -> Vec<Backend> {
        let backends = self.backends.read().unwrap();
        let n = backends.len();
        if n == 0 {
            return Vec::new();
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;
        (0..n).map(|k| backends[(start + k) % n].clone()).collect()
    }

    /// A loopback backend for this app, if any (used by the relay handler).
    fn pick_local(&self) -> Option<SocketAddr> {
        let backends = self.backends.read().unwrap();
        let locals: Vec<SocketAddr> = backends
            .iter()
            .filter_map(|b| match b {
                Backend::Local(a) => Some(*a),
                _ => None,
            })
            .collect();
        if locals.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % locals.len();
        Some(locals[i])
    }

    fn record_latency(&self, ms: f64) {
        let mut l = self.latencies_ms.lock().unwrap();
        if l.len() >= LATENCY_WINDOW {
            l.remove(0);
        }
        l.push(ms);
    }
}

/// Proxy-side stats snapshot for one app.
#[derive(Debug, Clone, Default)]
pub struct RouteStats {
    pub requests_total: u64,
    pub inflight: u32,
    pub shed_total: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
}

#[derive(Clone, Default)]
pub struct RouteTable {
    // hostname (lowercased, no port) -> shared app route
    by_host: Arc<RwLock<HashMap<String, Arc<AppRoute>>>>,
    by_app: Arc<RwLock<HashMap<String, Arc<AppRoute>>>>,
}

impl RouteTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install (or replace) an app's route: all `hostnames` resolve to the
    /// same backend set. Existing hostnames for the app that aren't listed
    /// are dropped — this is how a dead tunnel alias gets retired.
    pub fn set_app(
        &self,
        app: &str,
        hostnames: Vec<String>,
        backends: Vec<Backend>,
        max_inflight: Option<u32>,
    ) {
        let entry = {
            let by_app = self.by_app.read().unwrap();
            by_app.get(app).cloned()
        };
        let entry = match entry {
            Some(e) => {
                *e.backends.write().unwrap() = backends;
                e
            }
            None => {
                let e = Arc::new(AppRoute {
                    app: app.to_string(),
                    backends: RwLock::new(backends),
                    rr: AtomicUsize::new(0),
                    max_inflight,
                    inflight: AtomicU32::new(0),
                    requests: AtomicU64::new(0),
                    shed: AtomicU64::new(0),
                    latencies_ms: Mutex::new(Vec::new()),
                });
                self.by_app
                    .write()
                    .unwrap()
                    .insert(app.to_string(), e.clone());
                e
            }
        };

        let mut by_host = self.by_host.write().unwrap();
        by_host.retain(|_, e| e.app != app);
        for h in hostnames {
            by_host.insert(normalize_host(&h), entry.clone());
        }
    }

    /// Add one hostname alias to an existing app route (tunnel URL arrival).
    pub fn add_alias(&self, app: &str, hostname: &str) -> bool {
        let by_app = self.by_app.read().unwrap();
        if let Some(e) = by_app.get(app) {
            self.by_host
                .write()
                .unwrap()
                .insert(normalize_host(hostname), e.clone());
            true
        } else {
            false
        }
    }

    pub fn remove_app(&self, app: &str) {
        self.by_app.write().unwrap().remove(app);
        self.by_host.write().unwrap().retain(|_, e| e.app != app);
    }

    pub fn hostnames(&self, app: &str) -> Vec<String> {
        self.by_host
            .read()
            .unwrap()
            .iter()
            .filter(|(_, e)| e.app == app)
            .map(|(h, _)| h.clone())
            .collect()
    }

    pub fn stats(&self, app: &str) -> Option<RouteStats> {
        let by_app = self.by_app.read().unwrap();
        let e = by_app.get(app)?;
        let lat = e.latencies_ms.lock().unwrap();
        let mut sorted: Vec<f64> = lat.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| -> f64 {
            if sorted.is_empty() {
                0.0
            } else {
                sorted[((sorted.len() as f64 - 1.0) * p) as usize]
            }
        };
        Some(RouteStats {
            requests_total: e.requests.load(Ordering::Relaxed),
            inflight: e.inflight.load(Ordering::Relaxed),
            shed_total: e.shed.load(Ordering::Relaxed),
            p50_ms: pct(0.50),
            p95_ms: pct(0.95),
        })
    }

    fn lookup(&self, host: &str) -> Option<Arc<AppRoute>> {
        self.by_host.read().unwrap().get(host).cloned()
    }

    /// A loopback backend for an app by name (the relay forwards here).
    pub fn local_backend(&self, app: &str) -> Option<SocketAddr> {
        self.by_app.read().unwrap().get(app)?.pick_local()
    }
}

fn normalize_host(h: &str) -> String {
    h.split(':').next().unwrap_or(h).to_ascii_lowercase()
}

type ProxyBody = BoxBody<Bytes, hyper::Error>;
type ProxyClient = Client<HttpConnector, ProxyBody>;

fn empty_response(status: StatusCode) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
        .unwrap()
}

fn full_response(status: StatusCode, body: Bytes) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(Full::new(body).map_err(|never| match never {}).boxed())
        .unwrap()
}

/// Headers that belong to a single hop and must not be relayed onward.
fn is_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

async fn handle(
    req: Request<Incoming>,
    table: RouteTable,
    client: ProxyClient,
    http: reqwest::Client,
    metrics: Arc<Metrics>,
) -> Result<Response<ProxyBody>, hyper::Error> {
    let host = req
        .headers()
        .get(HOST)
        .and_then(|v| v.to_str().ok())
        .map(normalize_host)
        .or_else(|| req.uri().host().map(|h| h.to_ascii_lowercase()));

    let Some(route) = host.as_deref().and_then(|h| table.lookup(h)) else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };

    // Concurrency cap: shed rather than queue forever.
    if let Some(cap) = route.max_inflight {
        if route.inflight.load(Ordering::Relaxed) >= cap {
            route.shed.fetch_add(1, Ordering::Relaxed);
            let mut resp = empty_response(StatusCode::SERVICE_UNAVAILABLE);
            resp.headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            return Ok(resp);
        }
    }

    let backends = route.backend_order();
    if backends.is_empty() {
        return Ok(empty_response(StatusCode::BAD_GATEWAY));
    }

    // Buffer the request so it can be replayed across backends on failover.
    // The proxy serves web apps (the large-upload path is the daemon API on a
    // different port), so buffering here is cheap and lets a dead backend fall
    // through to the next instance instead of failing the request.
    let (parts, body) = req.into_parts();
    let path_q = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let body_bytes = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => return Ok(empty_response(StatusCode::BAD_GATEWAY)),
    };

    // Per-machine load is counted where a request is SERVED: a request served
    // by a local replica counts on this machine; one relayed to another machine
    // is counted there (in its relay handler). So the host counters never
    // double-count a request, and a fleet-wide total is a clean sum.
    route.inflight.fetch_add(1, Ordering::Relaxed);
    route.requests.fetch_add(1, Ordering::Relaxed);
    metrics.inflight.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();

    let n = backends.len();
    let mut response: Option<Response<ProxyBody>> = None;
    for (i, backend) in backends.into_iter().enumerate() {
        let last = i + 1 == n;
        let attempt = match &backend {
            Backend::Local(addr) => {
                let mut p = parts.clone();
                p.uri = format!("http://{addr}{path_q}")
                    .parse::<Uri>()
                    .expect("backend uri parses");
                let outgoing = Request::from_parts(
                    p,
                    Full::new(body_bytes.clone())
                        .map_err(|never| match never {})
                        .boxed(),
                );
                match client.request(outgoing).await {
                    Ok(resp) => {
                        // served locally → counts as this machine's load
                        metrics.requests.fetch_add(1, Ordering::Relaxed);
                        if let Some(len) = resp
                            .headers()
                            .get(hyper::header::CONTENT_LENGTH)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                        {
                            metrics.bytes.fetch_add(len, Ordering::Relaxed);
                        }
                        Some(resp.map(|b| b.boxed()))
                    }
                    Err(e) => {
                        tracing::warn!(app = %route.app, backend = %addr, "local upstream error: {e}");
                        None
                    }
                }
            }
            Backend::Remote { relay, token } => {
                forward_remote(&http, relay, token, &parts, &path_q, &body_bytes).await
            }
        };
        if let Some(resp) = attempt {
            response = Some(resp);
            break;
        }
        if last {
            tracing::warn!(app = %route.app, "all backends failed");
        }
    }

    route.inflight.fetch_sub(1, Ordering::Relaxed);
    metrics.inflight.fetch_sub(1, Ordering::Relaxed);
    route.record_latency(started.elapsed().as_secs_f64() * 1000.0);

    Ok(response.unwrap_or_else(|| empty_response(StatusCode::BAD_GATEWAY)))
}

/// Forward a buffered request to an app instance on another fleet machine via
/// its relay endpoint. Returns None on a transport failure so the caller can
/// try the next backend.
async fn forward_remote(
    http: &reqwest::Client,
    relay: &str,
    token: &str,
    parts: &hyper::http::request::Parts,
    path_q: &str,
    body: &Bytes,
) -> Option<Response<ProxyBody>> {
    let url = format!("{relay}{path_q}");
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes()).ok()?;
    let mut rb = http
        .request(method, url)
        .bearer_auth(token)
        .body(body.clone())
        .timeout(std::time::Duration::from_secs(25));
    for (name, value) in parts.headers.iter() {
        if !is_hop_header(name.as_str()) && !name.as_str().eq_ignore_ascii_case("authorization") {
            rb = rb.header(name.as_str(), value.as_bytes());
        }
    }
    let resp = match rb.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(relay, "remote relay error: {e}");
            return None;
        }
    };
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    // A relay answering 502/503 means "this instance can't serve right now"
    // (its local replica is paused, gone, or shedding). Fall through to the
    // next backend instead of handing the gateway error to the visitor.
    if status == StatusCode::BAD_GATEWAY || status == StatusCode::SERVICE_UNAVAILABLE {
        return None;
    }
    let headers = resp.headers().clone();
    let bytes = resp.bytes().await.unwrap_or_default();
    // bytes/requests for a relayed response are counted on the machine that
    // served it, in its relay handler — not here.
    let mut out = full_response(status, bytes);
    for (name, value) in headers.iter() {
        if is_hop_header(name.as_str()) {
            continue;
        }
        if let (Ok(n), Ok(v)) = (
            hyper::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            out.headers_mut().insert(n, v);
        }
    }
    Some(out)
}

/// Run the proxy listener forever on 127.0.0.1:<port>.
pub async fn run_proxy(table: RouteTable, port: u16, metrics: Arc<Metrics>) -> std::io::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let client: ProxyClient = Client::builder(TokioExecutor::new()).build_http();
    // short connect timeout so a dead fleet machine fails fast and the request
    // falls through to a live backend, rather than hanging the visitor
    let http = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap_or_default();
    tracing::info!("proxy listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let table = table.clone();
        let client = client.clone();
        let http = http.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                handle(req, table.clone(), client.clone(), http.clone(), metrics.clone())
            });
            if let Err(e) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!("proxy connection error: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_and_aliases() {
        let t = RouteTable::new();
        let b1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let b2: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        t.set_app(
            "hello",
            vec!["hello.localhost".into()],
            vec![Backend::Local(b1), Backend::Local(b2)],
            None,
        );
        assert!(t.add_alias("hello", "rand.trycloudflare.com:443"));

        let r = t.lookup("hello.localhost").unwrap();
        // backend_order starts at the next index each call, so two calls cover
        // both backends in alternating lead position
        let first = r.backend_order();
        let second = r.backend_order();
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert_ne!(first[0], second[0]);

        // alias resolves to the same route
        let via_alias = t.lookup("rand.trycloudflare.com").unwrap();
        assert_eq!(via_alias.app, "hello");

        // replacing routes drops stale hostnames
        t.set_app(
            "hello",
            vec!["hello.localhost".into()],
            vec![Backend::Local(b1)],
            None,
        );
        assert!(t.lookup("rand.trycloudflare.com").is_none());

        t.remove_app("hello");
        assert!(t.lookup("hello.localhost").is_none());
    }

    #[test]
    fn local_backend_lookup_skips_remote() {
        let t = RouteTable::new();
        let local: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        t.set_app(
            "x",
            vec!["x.localhost".into()],
            vec![
                Backend::Local(local),
                Backend::Remote {
                    relay: "https://api.dev.example.com/_relay/x".into(),
                    token: "t".into(),
                },
            ],
            Some(2),
        );
        // the relay handler only ever forwards to a loopback replica
        assert_eq!(t.local_backend("x"), Some(local));
        let s = t.stats("x").unwrap();
        assert_eq!(s.requests_total, 0);
        assert_eq!(s.inflight, 0);
    }
}
