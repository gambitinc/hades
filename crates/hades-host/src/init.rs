//! `hades host init` — the guided, idempotent bootstrap. Each step reports
//! what it found or fixed; nothing is silently installed. Designed to be
//! re-run safely at any time.

use std::path::PathBuf;

use hades_core::{HadesConfig, HadesPaths};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::doctor::{daemon_health, run_doctor, DoctorReport};
use crate::launchd::{LaunchdManager, ServiceManager};
use crate::probe::{HostProbe, MacProbe};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitStep {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitReport {
    pub steps: Vec<InitStep>,
    pub ntfy_subscribe_url: Option<String>,
    pub doctor: DoctorReport,
}

fn random_topic() -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (0..12)
        .map(|_| {
            let chars = b"abcdefghijklmnopqrstuvwxyz0123456789";
            chars[rng.gen_range(0..chars.len())] as char
        })
        .collect();
    format!("hades-{suffix}")
}

/// Resolve the hadesd binary: same directory as the current (hades) binary.
pub fn find_hadesd() -> Option<PathBuf> {
    let me = std::env::current_exe().ok()?;
    let sibling = me.parent()?.join("hadesd");
    sibling.exists().then_some(sibling)
}

pub struct InitOptions {
    /// Skip launchd installation (dev mode: run `hadesd` by hand).
    pub no_launchd: bool,
    /// Optional healthchecks.io ping URL for the dead-man's switch.
    pub healthchecks_url: Option<String>,
}

pub async fn run_init(opts: InitOptions) -> InitReport {
    let paths = HadesPaths::new();
    let mut steps = Vec::new();

    // 1. Directories.
    match paths.ensure_dirs() {
        Ok(()) => steps.push(InitStep {
            name: "dirs".into(),
            ok: true,
            detail: format!("{} ready", paths.root.display()),
        }),
        Err(e) => steps.push(InitStep {
            name: "dirs".into(),
            ok: false,
            detail: format!("cannot create {}: {e}", paths.root.display()),
        }),
    }

    // 2. Config: load existing or create with a fresh ntfy topic. Idempotent —
    //    an existing topic is kept so the user's phone subscription survives.
    let mut config = HadesConfig::load_or_default(&paths.config());
    if config.notify.ntfy_topic.is_none() {
        config.notify.ntfy_topic = Some(random_topic());
    }
    if config.auth_token.is_none() {
        let mut rng = rand::thread_rng();
        let token: String = (0..64)
            .map(|_| {
                let chars = b"0123456789abcdef";
                chars[rng.gen_range(0..chars.len())] as char
            })
            .collect();
        config.auth_token = Some(token);
    }
    if let Some(url) = &opts.healthchecks_url {
        config.notify.healthchecks_url = Some(url.clone());
    }
    let topic = config.notify.ntfy_topic.clone().unwrap();
    let ntfy_subscribe_url = Some(format!("https://ntfy.sh/{topic}"));
    match config.save(&paths.config()) {
        Ok(()) => steps.push(InitStep {
            name: "config".into(),
            ok: true,
            detail: format!("{} written", paths.config().display()),
        }),
        Err(e) => steps.push(InitStep {
            name: "config".into(),
            ok: false,
            detail: format!("cannot write config: {e}"),
        }),
    }

    // 3. Docker detection (report-only; remedies live in doctor output too).
    let docker_ok = match hades_runtime::Runtime::connect() {
        Ok(rt) => rt.ping().await.is_ok(),
        Err(_) => false,
    };
    steps.push(InitStep {
        name: "docker".into(),
        ok: docker_ok,
        detail: if docker_ok {
            "engine reachable".into()
        } else {
            "engine not reachable; install/start Docker Desktop (`brew install --cask docker`, then `open -a Docker`) or colima".into()
        },
    });

    // 4. cloudflared detection (optional — degrade is graceful).
    let cf = hades_tunnel::find_cloudflared();
    steps.push(InitStep {
        name: "cloudflared".into(),
        ok: true, // never blocks init
        detail: match &cf {
            Some(p) => format!("found at {}", p.display()),
            None => "not found; public URLs disabled until `brew install cloudflared`".into(),
        },
    });

    // 5. Power posture.
    let probe = MacProbe;
    match probe.sleeps_on_ac() {
        Some(false) => steps.push(InitStep {
            name: "power".into(),
            ok: true,
            detail: "machine stays awake on AC".into(),
        }),
        Some(true) => steps.push(InitStep {
            name: "power".into(),
            ok: true, // warn, not block
            detail: "machine sleeps on AC; run `sudo pmset -c sleep 0` so your host doesn't nap (needs sudo, so we won't do it for you)".into(),
        }),
        None => steps.push(InitStep {
            name: "power".into(),
            ok: true,
            detail: "could not read power settings (pmset)".into(),
        }),
    }

    // 6. launchd supervision.
    if opts.no_launchd {
        steps.push(InitStep {
            name: "launchd".into(),
            ok: true,
            detail: "skipped (--no-launchd); run `hadesd` manually".into(),
        });
    } else {
        match find_hadesd() {
            Some(bin) => {
                let mgr = LaunchdManager;
                match mgr.install(&bin, &paths.logs_dir()) {
                    Ok(()) => steps.push(InitStep {
                        name: "launchd".into(),
                        ok: true,
                        detail: format!("installed {} (starts on login, restarts on crash)", crate::launchd::DAEMON_LABEL),
                    }),
                    Err(e) => steps.push(InitStep {
                        name: "launchd".into(),
                        ok: false,
                        detail: format!("install failed: {e}"),
                    }),
                }
            }
            None => steps.push(InitStep {
                name: "launchd".into(),
                ok: false,
                detail: "hadesd binary not found next to hades; build with `cargo build --workspace` or reinstall".into(),
            }),
        }
    }

    // 7. Wait briefly for the daemon to come up.
    if !opts.no_launchd {
        let mut up = false;
        for _ in 0..20 {
            if daemon_health(config.api_port).await {
                up = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        steps.push(InitStep {
            name: "daemon".into(),
            ok: up,
            detail: if up {
                format!("hadesd healthy on 127.0.0.1:{}", config.api_port)
            } else {
                "daemon did not become healthy within 10s; check `~/.hades/logs/hadesd.err.log`".into()
            },
        });
    }

    // 8. Final doctor pass.
    let doctor = run_doctor(&config, &paths, false).await;

    InitReport {
        steps,
        ntfy_subscribe_url,
        doctor,
    }
}
