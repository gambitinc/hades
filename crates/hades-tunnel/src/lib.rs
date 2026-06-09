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
        let mut child = Command::new(&self.binary)
            .args([
                "tunnel",
                "--no-autoupdate",
                "--url",
                &format!("http://localhost:{local_port}"),
            ])
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
