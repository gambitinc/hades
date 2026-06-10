//! Preflight + ongoing health. Every check has a name, a verdict, a detail
//! line, and — when red — the exact remedy. The daemon refuses deploys
//! while the doctor is red; that is what "the host is ready" means.

use hades_core::{HadesConfig, HadesPaths};
use hades_runtime::Runtime;
use serde::{Deserialize, Serialize};

use crate::probe::{HostProbe, MacProbe};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    /// Soft failures (warnings) don't turn the report red.
    pub warn: bool,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

impl Check {
    fn pass(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok: true,
            warn: false,
            detail: detail.into(),
            remedy: None,
        }
    }
    fn fail(name: &str, detail: impl Into<String>, remedy: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok: false,
            warn: false,
            detail: detail.into(),
            remedy: Some(remedy.into()),
        }
    }
    fn warn(name: &str, detail: impl Into<String>, remedy: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ok: false,
            warn: true,
            detail: detail.into(),
            remedy: Some(remedy.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    pub green: bool,
    pub checks: Vec<Check>,
}

impl DoctorReport {
    pub fn failed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|c| !c.ok && !c.warn)
            .map(|c| c.name.clone())
            .collect()
    }
}

/// Run the full battery of checks. `inside_daemon` skips the checks that
/// only make sense from outside (daemon reachability) or that would collide
/// with the daemon's own listeners (proxy port bind).
pub async fn run_doctor(
    config: &HadesConfig,
    paths: &HadesPaths,
    inside_daemon: bool,
) -> DoctorReport {
    let probe = MacProbe;
    let mut checks = Vec::new();

    // 1. Docker engine reachable + capacity honesty (VM vs Mac).
    match Runtime::connect() {
        Ok(rt) => match rt.ping().await {
            Ok(()) => match rt.engine_info().await {
                Ok(info) => {
                    let mac_mb = probe.mac_total_memory_mb().unwrap_or(0);
                    checks.push(Check::pass(
                        "docker",
                        format!(
                            "engine up — VM capacity {}MB / {} CPUs (Mac has {}MB; the VM is the real budget)",
                            info.vm_memory_mb, info.vm_cpus, mac_mb
                        ),
                    ));
                }
                Err(e) => checks.push(Check::fail(
                    "docker",
                    format!("engine reachable but /info failed: {e}"),
                    "restart Docker Desktop (or `colima restart`)",
                )),
            },
            Err(e) => checks.push(Check::fail(
                "docker",
                format!("engine not responding: {e}"),
                "start Docker Desktop (`open -a Docker`) or `colima start`; install with `brew install --cask docker`",
            )),
        },
        Err(e) => checks.push(Check::fail(
            "docker",
            format!("cannot construct client: {e}"),
            "install Docker Desktop: `brew install --cask docker`",
        )),
    }

    // 2. cloudflared present (warn-only: deploys degrade to local URLs).
    match hades_tunnel::find_cloudflared() {
        Some(p) => checks.push(Check::pass("cloudflared", p.display().to_string())),
        None => checks.push(Check::warn(
            "cloudflared",
            "not found — apps will get local URLs only",
            "brew install cloudflared",
        )),
    }

    // 3. Disk headroom.
    match probe.disk_free_gb() {
        Some(free) if free >= config.disk_min_free_gb => {
            checks.push(Check::pass("disk", format!("{free:.1}GB free")))
        }
        Some(free) => checks.push(Check::fail(
            "disk",
            format!("{free:.1}GB free (threshold {}GB)", config.disk_min_free_gb),
            "free disk space or prune images: `docker system prune`",
        )),
        None => checks.push(Check::warn(
            "disk",
            "could not determine free space",
            "check `df -h /` manually",
        )),
    }

    // 4. Proxy port (only meaningful when the daemon isn't the one holding it).
    if !inside_daemon {
        let daemon_up = daemon_health(config.api_port).await;
        if daemon_up {
            checks.push(Check::pass(
                "proxy-port",
                format!("port {} held by running daemon", config.proxy_port),
            ));
        } else {
            match std::net::TcpListener::bind(("127.0.0.1", config.proxy_port)) {
                Ok(_) => checks.push(Check::pass(
                    "proxy-port",
                    format!("port {} bindable", config.proxy_port),
                )),
                Err(e) => checks.push(Check::fail(
                    "proxy-port",
                    format!("cannot bind 127.0.0.1:{}: {e}", config.proxy_port),
                    format!("free the port: `lsof -i :{}`", config.proxy_port),
                )),
            }
        }
    }

    // 5. Network egress to Cloudflare (tunnels + ntfy need outbound 443).
    let egress = reqwest::Client::new()
        .head("https://www.cloudflare.com")
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await;
    match egress {
        Ok(_) => checks.push(Check::pass("egress", "outbound 443 to Cloudflare ok")),
        Err(e) => checks.push(Check::fail(
            "egress",
            format!("cannot reach cloudflare.com: {e}"),
            "check network connection / firewall",
        )),
    }

    // 6. Sleep posture: a host that naps on AC isn't a host.
    match probe.sleeps_on_ac() {
        Some(false) => checks.push(Check::pass("sleep", "machine stays awake on AC")),
        Some(true) => checks.push(Check::warn(
            "sleep",
            "machine sleeps on AC — apps and tunnels die when the lid closes",
            "run `sudo pmset -c sleep 0` (and consider `sudo pmset -c disablesleep 1` for clamshell)",
        )),
        None => checks.push(Check::warn(
            "sleep",
            "could not read pmset settings",
            "check `pmset -g custom`",
        )),
    }

    // 7a. Keep-awake assertion present when configured.
    if config.keep_awake {
        let held = std::process::Command::new("pmset")
            .arg("-g")
            .arg("assertions")
            .output()
            .ok()
            .map(|o| {
                let s = String::from_utf8_lossy(&o.stdout);
                s.contains("PreventUserIdleSystemSleep") && s.contains("caffeinate")
            })
            .unwrap_or(false);
        if held {
            checks.push(Check::pass("keep-awake", "power assertion held — host won't idle-sleep"));
        } else {
            checks.push(Check::warn(
                "keep-awake",
                "no power assertion yet (daemon just started, or caffeinate missing)",
                "the daemon holds caffeinate -s; check ~/.hades/logs if this persists",
            ));
        }
    }

    // 7b. Domain mode.
    if let Some(dom) = &config.domain.name {
        checks.push(Check::pass(
            "domain",
            format!("stable URLs on {dom} (api.{dom} + *.{dom})"),
        ));
    }

    // 7. Notification channel configured.
    match &config.notify.ntfy_topic {
        Some(topic) => checks.push(Check::pass(
            "notify",
            format!("ntfy topic configured (ntfy.sh/{topic})"),
        )),
        None => checks.push(Check::warn(
            "notify",
            "no ntfy topic — you won't get pushes when the host misbehaves",
            "run `hades host init` to generate one",
        )),
    }

    // 8. State dir writable.
    match paths.ensure_dirs() {
        Ok(()) => checks.push(Check::pass(
            "state-dir",
            paths.root.display().to_string(),
        )),
        Err(e) => checks.push(Check::fail(
            "state-dir",
            format!("cannot create {}: {e}", paths.root.display()),
            "check permissions on ~/.hades",
        )),
    }

    // 9. Daemon alive (outside view only).
    if !inside_daemon {
        if daemon_health(config.api_port).await {
            checks.push(Check::pass(
                "daemon",
                format!("hadesd responding on 127.0.0.1:{}", config.api_port),
            ));
        } else {
            checks.push(Check::fail(
                "daemon",
                "hadesd not responding",
                "run `hades host init` (installs + starts the launchd service)",
            ));
        }
    }

    let green = checks.iter().all(|c| c.ok || c.warn);
    DoctorReport { green, checks }
}

pub async fn daemon_health(api_port: u16) -> bool {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{api_port}/health"))
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}
