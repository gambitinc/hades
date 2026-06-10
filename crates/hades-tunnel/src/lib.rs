//! cloudflared lifecycle. Default provider: quick tunnels — one cloudflared
//! process per app pointed at the proxy, no account needed. The assigned
//! `*.trycloudflare.com` hostname is scraped from cloudflared's output and
//! handed back so the daemon can register it as an alias route. URLs are
//! cattle: when a tunnel dies it is re-provisioned and the URL changes.
//!
//! `UrlProvider` is the seam for named tunnels (stable custom domains, one
//! process for every app) later.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use hades_core::HadesError;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// A live tunnel: the public URL plus the cloudflared child process. The
/// daemon owns the child and watches `wait()` to detect death.
pub struct Tunnel {
    pub app: String,
    pub url: String,
    pub pid: Option<u32>,
    child: Child,
}

impl Tunnel {
    /// Resolves when cloudflared exits (i.e. the tunnel died).
    pub async fn wait(&mut self) {
        let _ = self.child.wait().await;
    }

    pub async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }

    /// Still running? (used to reuse long-lived utility tunnels)
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

/// Locate cloudflared: $PATH, then the usual Homebrew prefixes.
pub fn find_cloudflared() -> Option<PathBuf> {
    if let Ok(out) = std::process::Command::new("which")
        .arg("cloudflared")
        .output()
    {
        if out.status.success() {
            let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !p.is_empty() {
                return Some(PathBuf::from(p));
            }
        }
    }
    for candidate in [
        "/opt/homebrew/bin/cloudflared",
        "/usr/local/bin/cloudflared",
    ] {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

/// A named Cloudflare tunnel: stable hostnames on your own domain. One
/// process carries everything — `*.<domain>` lands on the proxy (which
/// routes by Host header) and `api.<domain>` on the daemon API.
pub struct NamedTunnel {
    pub domain: String,
    pub tunnel: String,
    binary: PathBuf,
}

/// Run `cloudflared tunnel run --token <token>` for a remotely-managed
/// tunnel the coordinator created. Returns the supervised child.
pub fn run_token_tunnel(token: &str) -> Result<Child, HadesError> {
    let bin = find_cloudflared()
        .ok_or_else(|| HadesError::Tunnel("cloudflared not installed".into()))?;
    Command::new(bin)
        .args(["tunnel", "--no-autoupdate", "run", "--token", token])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| HadesError::Tunnel(format!("tunnel run --token: {e}")))
}

pub fn cert_exists() -> bool {
    std::env::var("HOME")
        .map(|h| std::path::Path::new(&h).join(".cloudflared/cert.pem").exists())
        .unwrap_or(false)
}

impl NamedTunnel {
    /// Some only when cloudflared is installed AND `cloudflared tunnel
    /// login` has been completed (cert.pem present).
    pub fn detect(domain: &str, tunnel: &str) -> Option<Self> {
        if !cert_exists() {
            return None;
        }
        Some(Self {
            domain: domain.to_string(),
            tunnel: tunnel.to_string(),
            binary: find_cloudflared()?,
        })
    }

    async fn find_id(&self) -> Result<Option<String>, HadesError> {
        let list = Command::new(&self.binary)
            .args(["tunnel", "list", "--output", "json"])
            .output()
            .await
            .map_err(|e| HadesError::Tunnel(format!("cloudflared tunnel list: {e}")))?;
        if !list.status.success() {
            return Err(HadesError::Tunnel(format!(
                "tunnel list failed: {}",
                String::from_utf8_lossy(&list.stderr).trim()
            )));
        }
        let v: serde_json::Value = serde_json::from_slice(&list.stdout)
            .map_err(|e| HadesError::Tunnel(format!("bad tunnel list json: {e}")))?;
        Ok(v.as_array().and_then(|arr| {
            arr.iter()
                .find(|t| t["name"].as_str() == Some(self.tunnel.as_str()))
                .and_then(|t| t["id"].as_str().map(String::from))
        }))
    }

    /// Create the tunnel if it doesn't exist; return its UUID.
    pub async fn ensure(&self) -> Result<String, HadesError> {
        if let Some(id) = self.find_id().await? {
            return Ok(id);
        }
        let create = Command::new(&self.binary)
            .args(["tunnel", "create", &self.tunnel])
            .output()
            .await
            .map_err(|e| HadesError::Tunnel(format!("tunnel create: {e}")))?;
        if !create.status.success() {
            return Err(HadesError::Tunnel(format!(
                "tunnel create failed: {}",
                String::from_utf8_lossy(&create.stderr).trim()
            )));
        }
        self.find_id()
            .await?
            .ok_or_else(|| HadesError::Tunnel("created tunnel but cannot find its id".into()))
    }

    /// Ingress config: api.<domain> → daemon, *.<domain> → proxy, 404 sink.
    pub fn write_config(
        &self,
        dir: &std::path::Path,
        tunnel_id: &str,
        api_port: u16,
        proxy_port: u16,
    ) -> Result<PathBuf, HadesError> {
        let home = std::env::var("HOME").unwrap_or_default();
        let mut cfg = String::new();
        cfg.push_str(&format!("tunnel: {tunnel_id}\n"));
        cfg.push_str(&format!("credentials-file: {home}/.cloudflared/{tunnel_id}.json\n"));
        cfg.push_str("ingress:\n");
        cfg.push_str(&format!("  - hostname: api.{}\n", self.domain));
        cfg.push_str(&format!("    service: http://localhost:{api_port}\n"));
        cfg.push_str(&format!("  - hostname: \"*.{}\"\n", self.domain));
        cfg.push_str(&format!("    service: http://localhost:{proxy_port}\n"));
        cfg.push_str("  - service: http_status:404\n");
        let path = dir.join("cloudflared.yml");
        std::fs::write(&path, cfg)?;
        Ok(path)
    }

    /// Point a hostname at the tunnel (idempotent; "already exists" is fine).
    pub async fn route_dns(&self, hostname: &str) {
        let out = Command::new(&self.binary)
            .args(["tunnel", "route", "dns", &self.tunnel, hostname])
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => {
                tracing::info!(hostname, "dns route ready");
            }
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr);
                if !err.contains("already exists") && !err.contains("already configured") {
                    tracing::warn!(hostname, "dns route: {}", err.trim());
                }
            }
            Err(e) => tracing::warn!(hostname, "dns route failed: {e}"),
        }
    }

    /// Run the tunnel (caller supervises; respawn on exit).
    pub fn run(&self, config_path: &std::path::Path) -> Result<Child, HadesError> {
        Command::new(&self.binary)
            .args(["tunnel", "--config"])
            .arg(config_path)
            .arg("run")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| HadesError::Tunnel(format!("tunnel run: {e}")))
    }
}

pub struct QuickTunnelProvider {
    binary: PathBuf,
}

impl QuickTunnelProvider {
    /// None when cloudflared isn't installed — deploys then degrade to
    /// local-only URLs with an install hint.
    pub fn detect() -> Option<Self> {
        find_cloudflared().map(|binary| Self { binary })
    }

    /// Spawn a quick tunnel pointed at the local proxy port and scrape the
    /// assigned trycloudflare hostname from cloudflared's startup output.
    pub async fn provision(&self, app: &str, local_port: u16) -> Result<Tunnel, HadesError> {
        self.provision_raw(app, &format!("http://localhost:{local_port}"))
            .await
    }

    /// Same, for arbitrary protocols cloudflared understands — e.g.
    /// `ssh://localhost:22` for `hades ssh`.
    pub async fn provision_raw(&self, app: &str, target: &str) -> Result<Tunnel, HadesError> {
        let mut child = Command::new(&self.binary)
            .args(["tunnel", "--no-autoupdate", "--url", target])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| HadesError::Tunnel(format!("failed to spawn cloudflared: {e}")))?;

        // cloudflared prints the assigned URL to stderr inside an ASCII box.
        // The reader task owns stderr for the tunnel's whole lifetime —
        // dropping it early would close the pipe and SIGPIPE cloudflared.
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HadesError::Tunnel("no stderr from cloudflared".into()))?;
        let url_re = regex::Regex::new(r"https://[a-z0-9-]+\.trycloudflare\.com").unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let app_name = app.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            let mut tx = Some(tx);
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(app = %app_name, "cloudflared: {line}");
                if tx.is_some() {
                    if let Some(m) = url_re.find(&line) {
                        let _ = tx.take().unwrap().send(m.as_str().to_string());
                    }
                }
            }
        });

        match tokio::time::timeout(Duration::from_secs(45), rx).await {
            Ok(Ok(url)) => Ok(Tunnel {
                app: app.to_string(),
                url,
                pid: child.id(),
                child,
            }),
            Ok(Err(_)) => {
                let _ = child.kill().await;
                Err(HadesError::Tunnel(
                    "cloudflared exited before assigning a URL".into(),
                ))
            }
            Err(_) => {
                let _ = child.kill().await;
                Err(HadesError::Tunnel(
                    "timed out waiting for trycloudflare URL (45s)".into(),
                ))
            }
        }
    }
}
