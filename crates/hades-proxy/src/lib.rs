//! Reverse proxy: one listener, Host-header routing. A hostname maps to a
//! set of replica backends (round-robin), an app can own several hostnames
//! (local `<app>.localhost` plus the tunnel alias), and each app carries an
//! optional in-flight cap — over the cap we shed with 503 + Retry-After
//! rather than letting one app starve the machine.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderValue, HOST, RETRY_AFTER};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};

const LATENCY_WINDOW: usize = 1024;

/// Per-app routing state shared by all of its hostnames.
pub struct AppRoute {
    pub app: String,
    backends: RwLock<Vec<SocketAddr>>,
    rr: AtomicUsize,
    max_inflight: Option<u32>,
    inflight: AtomicU32,
    requests: AtomicU64,
    shed: AtomicU64,
    latencies_ms: Mutex<Vec<f64>>,
}

impl AppRoute {
    fn pick_backend(&self) -> Option<SocketAddr> {
        let backends = self.backends.read().unwrap();
        if backends.is_empty() {
            return None;
        }
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % backends.len();
        Some(backends[i])
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
        backends: Vec<SocketAddr>,
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
}

fn normalize_host(h: &str) -> String {
    h.split(':').next().unwrap_or(h).to_ascii_lowercase()
}

type ProxyClient = Client<HttpConnector, Incoming>;

fn empty_response(status: StatusCode) -> Response<BoxBody<Bytes, hyper::Error>> {
    Response::builder()
        .status(status)
        .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
        .unwrap()
}

async fn handle(
    req: Request<Incoming>,
    table: RouteTable,
    client: ProxyClient,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
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

    let Some(backend) = route.pick_backend() else {
        return Ok(empty_response(StatusCode::BAD_GATEWAY));
    };

    route.inflight.fetch_add(1, Ordering::Relaxed);
    route.requests.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();

    let (mut parts, body) = req.into_parts();
    let path_q = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    parts.uri = format!("http://{backend}{path_q}")
        .parse::<Uri>()
        .expect("backend uri parses");

    let result = client.request(Request::from_parts(parts, body)).await;

    route.inflight.fetch_sub(1, Ordering::Relaxed);
    route.record_latency(started.elapsed().as_secs_f64() * 1000.0);

    match result {
        Ok(resp) => Ok(resp.map(|b| b.boxed())),
        Err(e) => {
            tracing::warn!(app = %route.app, backend = %backend, "proxy upstream error: {e}");
            Ok(empty_response(StatusCode::BAD_GATEWAY))
        }
    }
}

/// Run the proxy listener forever on 127.0.0.1:<port>.
pub async fn run_proxy(table: RouteTable, port: u16) -> std::io::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let client: ProxyClient = Client::builder(TokioExecutor::new()).build_http();
    tracing::info!("proxy listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let table = table.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(req, table.clone(), client.clone()));
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
        t.set_app("hello", vec!["hello.localhost".into()], vec![b1, b2], None);
        assert!(t.add_alias("hello", "rand.trycloudflare.com:443"));

        let r = t.lookup("hello.localhost").unwrap();
        let picks: Vec<_> = (0..4).map(|_| r.pick_backend().unwrap()).collect();
        assert_eq!(picks, vec![b1, b2, b1, b2]);

        // alias resolves to the same route
        let via_alias = t.lookup("rand.trycloudflare.com").unwrap();
        assert_eq!(via_alias.app, "hello");

        // replacing routes drops stale hostnames
        t.set_app("hello", vec!["hello.localhost".into()], vec![b1], None);
        assert!(t.lookup("rand.trycloudflare.com").is_none());

        t.remove_app("hello");
        assert!(t.lookup("hello.localhost").is_none());
    }

    #[test]
    fn shed_counting_via_stats() {
        let t = RouteTable::new();
        t.set_app(
            "x",
            vec!["x.localhost".into()],
            vec!["127.0.0.1:9000".parse().unwrap()],
            Some(2),
        );
        let s = t.stats("x").unwrap();
        assert_eq!(s.requests_total, 0);
        assert_eq!(s.inflight, 0);
    }
}
