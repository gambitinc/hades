//! `hades` — the agent-friendly CLI. Just a CLI: every command supports
//! `--json` (one final JSON object on stdout, progress on stderr) and exits
//! with stable codes: 0 ok, 1 generic, 2 doctor-red, 3 admission-rejected,
//! 4 not-found, 5 daemon-unreachable, 6 invalid spec/manifest.

mod context;
mod render;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use hades_api::DaemonClient;
use hades_core::{HadesConfig, HadesError, HadesPaths, Manifest};

#[derive(Parser)]
#[command(
    name = "hades",
    about = "Turn this machine into a personal cloud: deploy containerized apps, get public links.",
    long_about = "Turn this machine into a personal cloud host.\n\
    \n\
    Agent contract: every command accepts --json and then prints exactly one JSON\n\
    object (or NDJSON for streams) on stdout; progress goes to stderr. Exit codes:\n\
    0 ok, 1 error, 2 host-not-ready, 3 deploy-rejected-overcommit, 4 not-found,\n\
    5 daemon-unreachable, 6 invalid-spec.",
    version
)]
struct Cli {
    /// Emit machine-readable JSON on stdout.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Host lifecycle: init, doctor, status, battery, uptime, ps.
    Host {
        #[command(subcommand)]
        cmd: HostCmd,
    },
    /// Connect to a hades host and verify it's ready. Local by default;
    /// for a remote host pass --host and --token (get both from
    /// `hades host connect-info` on the host machine).
    Login {
        /// Host API address or URL. Default: this machine.
        #[arg(long)]
        host: Option<String>,
        /// Bearer token (required for remote hosts).
        #[arg(long)]
        token: Option<String>,
    },
    /// Forget the current session (next commands talk to the local host).
    Logout,
    /// Scaffold a Hades.toml in the current directory.
    Init,
    /// Deploy the app described by ./Hades.toml (idempotent upsert). Prints the link.
    Deploy {
        /// Override the app name from the manifest.
        #[arg(long)]
        app: Option<String>,
        /// Directory containing Hades.toml (default: cwd).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Pin to a fleet device by name ("local" = the hub itself).
        #[arg(long)]
        device: Option<String>,
    },
    /// Manage deployed apps.
    Apps {
        #[command(subcommand)]
        cmd: AppsCmd,
    },
    /// Your devices: list them, add one, remove one. Deploys are placed on
    /// whichever device has the most free memory.
    Fleet {
        #[command(subcommand)]
        cmd: Option<FleetCmd>,
    },
    /// Print an app's current public URL (URLs change when tunnels restart).
    Url { name: String },
    /// The host's event stream (NDJSON with --json): deploys, OOM kills,
    /// pressure, power transitions, URL changes, downtime.
    Events {
        #[arg(long)]
        follow: bool,
        /// Only events from the last N days.
        #[arg(long)]
        days: Option<f64>,
    },
    /// Notification utilities.
    Notify {
        #[command(subcommand)]
        cmd: NotifyCmd,
    },
}

#[derive(Subcommand)]
enum HostCmd {
    /// Guided, idempotent bootstrap: prerequisites, config, ntfy topic,
    /// launchd supervision, doctor.
    Init {
        /// Skip launchd installation (run `hadesd` manually).
        #[arg(long)]
        no_launchd: bool,
        /// healthchecks.io ping URL for the dead-man's switch (host-down push).
        #[arg(long)]
        healthchecks_url: Option<String>,
    },
    /// Preflight checks with remedies. The daemon refuses deploys while red.
    Doctor,
    /// One-screen host truth: capacity vs allocated, power, availability, apps.
    Status,
    /// Battery health + degradation diagnosis (why is it degrading, and how
    /// much of that is Hades' fault).
    Battery,
    /// Downtime ledger: availability %, windows, causes (slept/crashed/rebooted).
    Uptime {
        #[arg(long, default_value_t = 7.0)]
        days: f64,
    },
    /// Everything Hades runs: daemon, containers, tunnels — with live RSS/CPU.
    Ps,
    /// Print the command another machine runs to control this host
    /// (control-tunnel URL + bearer token). Treat it like a password.
    ConnectInfo,
    /// Join this machine to a fleet: registers this device with the hub and
    /// marks it as yours. Run on the NEW device.
    Join {
        /// The hub's control URL (from `hades host connect-info` on the hub).
        #[arg(long)]
        hub: String,
        /// The hub's bearer token.
        #[arg(long)]
        token: String,
        /// Name for this device (default: this Mac's hostname).
        #[arg(long)]
        name: Option<String>,
    },
}

#[derive(Subcommand)]
enum AppsCmd {
    /// List deployed apps.
    List,
    /// Stream an app's logs.
    Logs {
        name: String,
        #[arg(long)]
        follow: bool,
    },
    /// Proxy + replica stats (requests, latency, shed count, memory, CPU).
    Stats { name: String },
    /// Pause an app (docker pause; keeps state, frees CPU).
    Pause { name: String },
    /// Resume a paused app.
    Resume { name: String },
    /// Destroy an app: container(s), routes, tunnel.
    Destroy { name: String },
}

#[derive(Subcommand)]
enum FleetCmd {
    /// List devices with live capacity (default).
    List,
    /// Print the two lines to run on a new machine to add it.
    Add,
    /// Remove a device from the fleet (its apps keep running on it).
    Remove { name: String },
}

#[derive(Subcommand)]
enum NotifyCmd {
    /// Send a test push through every configured channel.
    Test,
}

fn fail(e: impl Into<hades_api::ApiError>, json: bool) -> ExitCode {
    let e = e.into();
    if json {
        println!("{}", serde_json::to_string_pretty(&e.body).unwrap());
    } else {
        eprintln!("error: {}", e.error);
        if let HadesError::DaemonUnreachable(_) = e.error {
            eprintln!("hint: is hadesd running? `hades host init` sets it up.");
        }
    }
    ExitCode::from(e.error.exit_code() as u8)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Session {
    host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    logged_in_at: chrono::DateTime<chrono::Utc>,
    daemon_version: String,
}

fn session_path() -> std::path::PathBuf {
    HadesPaths::new().root.join("session.json")
}

fn load_session() -> Option<Session> {
    serde_json::from_str(&std::fs::read_to_string(session_path()).ok()?).ok()
}

/// The session (written by `hades login`) decides which host commands talk
/// to; without one, fall back to the local daemon + local config token.
fn client() -> DaemonClient {
    if let Some(s) = load_session() {
        return DaemonClient::for_host(&s.host, s.token);
    }
    let paths = HadesPaths::new();
    let config = HadesConfig::load_or_default(&paths.config());
    DaemonClient::for_host(
        &format!("127.0.0.1:{}", config.api_port),
        config.auth_token,
    )
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let json = cli.json;

    match cli.cmd {
        Cmd::Host { cmd } => host_cmd(cmd, json).await,
        Cmd::Login { host, token } => login(host, token, json).await,
        Cmd::Logout => {
            let _ = std::fs::remove_file(session_path());
            if json {
                render::json(&serde_json::json!({ "logged_out": true }));
            } else {
                println!("logged out — commands now target the local host");
            }
            ExitCode::SUCCESS
        }
        Cmd::Init => scaffold_manifest(json),
        Cmd::Deploy { app, dir, device } => deploy(app, dir, device, json).await,
        Cmd::Apps { cmd } => apps_cmd(cmd, json).await,
        Cmd::Fleet { cmd } => fleet_cmd(cmd.unwrap_or(FleetCmd::List), json).await,
        Cmd::Url { name } => match client().get_app(&name).await {
            Ok(info) => {
                if json {
                    render::json(&serde_json::json!({
                        "name": info.name,
                        "url": info.url,
                        "local_url": info.local_url,
                        "url_changed_at": info.url_changed_at,
                    }));
                } else {
                    match &info.url {
                        Some(u) => println!("{u}"),
                        None => println!("{} (no public URL — local only)", info.local_url),
                    }
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        Cmd::Events { follow, days } => stream_events(follow, days, json).await,
        Cmd::Notify { cmd: NotifyCmd::Test } => match client().notify_test().await {
            Ok(r) => {
                if json {
                    render::json(&r);
                } else if r.delivered_to.is_empty() {
                    println!("no channels delivered — run `hades host init` to configure ntfy");
                } else {
                    println!("delivered to: {}", r.delivered_to.join(", "));
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
    }
}

/// Connect to the host (local or remote), verify it's alive and ready,
/// record the session, and orient the user.
async fn login(host: Option<String>, token: Option<String>, json: bool) -> ExitCode {
    let paths = HadesPaths::new();
    let config = HadesConfig::load_or_default(&paths.config());
    let is_remote = host.is_some();
    let host = host.unwrap_or_else(|| format!("127.0.0.1:{}", config.api_port));
    // local logins read the token straight off the machine; remote ones
    // must present it
    let token = token.or_else(|| {
        if is_remote { None } else { config.auth_token.clone() }
    });
    if is_remote && token.is_none() {
        return fail(
            HadesError::Unauthorized(
                "remote hosts need --token (run `hades host connect-info` on the host)".into(),
            ),
            json,
        );
    }
    let c = DaemonClient::for_host(&host, token.clone());

    let health = match c.health().await {
        Ok(h) => h,
        Err(_) => {
            return fail(
                HadesError::DaemonUnreachable(format!(
                    "no hadesd at {host} — run the install script or `hades host init` first"
                )),
                json,
            )
        }
    };
    let status = match c.host_status().await {
        Ok(s) => s,
        Err(e) => return fail(e, json),
    };

    // record the session so every later command knows where home is
    let session = Session {
        host: host.clone(),
        token,
        logged_in_at: chrono::Utc::now(),
        daemon_version: health.version.clone(),
    };
    let _ = paths.ensure_dirs();
    let _ = std::fs::write(
        session_path(),
        serde_json::to_string_pretty(&session).unwrap(),
    );

    if json {
        render::json(&serde_json::json!({
            "logged_in": true,
            "host": session.host,
            "daemon_version": health.version,
            "doctor_green": health.doctor_green,
            "allocatable_mb": status.ledger.allocatable_mb,
            "allocated_mb": status.ledger.allocated_mb,
            "apps": status.apps.len(),
        }));
    } else {
        println!();
        println!("  \u{2696} you have entered the underworld");
        println!();
        println!("  host      {host} — hadesd v{}", health.version);
        println!(
            "  doctor    {}",
            if health.doctor_green {
                "GREEN — ready for deploys"
            } else {
                "RED — run `hades host doctor` for remedies"
            }
        );
        println!(
            "  capacity  {}MB allocatable \u{b7} {}MB claimed \u{b7} {} apps",
            status.ledger.allocatable_mb,
            status.ledger.allocated_mb,
            status.apps.len()
        );
        if let Some(cu) = &status.control_url {
            println!("  remote    {cu} (use `hades host connect-info` for the full command)");
        }
        println!();
        println!("  next:");
        println!("    hades init        scaffold a Hades.toml in your project");
        println!("    hades deploy      ship it — returns a link");
        println!("    hades host status the one-screen truth");
        println!();
    }
    if health.doctor_green {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

async fn host_cmd(cmd: HostCmd, json: bool) -> ExitCode {
    let paths = HadesPaths::new();
    let config = HadesConfig::load_or_default(&paths.config());
    match cmd {
        HostCmd::Init {
            no_launchd,
            healthchecks_url,
        } => {
            eprintln!("hades host init — bootstrapping this machine as a cloud host…");
            let report = hades_host::init::run_init(hades_host::init::InitOptions {
                no_launchd,
                healthchecks_url,
            })
            .await;
            if json {
                render::json(&report);
            } else {
                for s in &report.steps {
                    println!("{} {:<12} {}", if s.ok { "\u{2713}" } else { "\u{2717}" }, s.name, s.detail);
                }
                if let Some(url) = &report.ntfy_subscribe_url {
                    println!();
                    println!("\u{1F4F1} subscribe your phone to host alerts: {url}");
                    println!("   (install the ntfy app, subscribe to that topic — no account needed)");
                }
                println!();
                render::doctor(&report.doctor);
            }
            if report.doctor.green {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
        HostCmd::Doctor => {
            let report = hades_host::run_doctor(&config, &paths, false).await;
            if json {
                render::json(&report);
            } else {
                render::doctor(&report);
            }
            if report.green {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
        HostCmd::Status => match client().host_status().await {
            Ok(s) => {
                if json {
                    render::json(&s);
                } else {
                    render::host_status(&s);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        HostCmd::Battery => match client().battery().await {
            Ok(b) => {
                if json {
                    render::json(&b);
                } else {
                    render::battery(&b);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        HostCmd::Uptime { days } => match client().uptime(days).await {
            Ok(u) => {
                if json {
                    render::json(&u);
                } else {
                    render::uptime(&u);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        HostCmd::ConnectInfo => {
            let local = DaemonClient::for_host(
                &format!("127.0.0.1:{}", config.api_port),
                config.auth_token.clone(),
            );
            match local.host_status().await {
                Ok(s) => {
                    let Some(token) = config.auth_token else {
                        return fail(
                            HadesError::Other("no auth token in config — run `hades host init`".into()),
                            json,
                        );
                    };
                    match s.control_url {
                        Some(url) => {
                            if json {
                                render::json(&serde_json::json!({
                                    "host": url, "token": token,
                                    "command": format!("hades login --host {url} --token {token}"),
                                }));
                            } else {
                                println!();
                                println!("  on the other machine, run:");
                                println!();
                                println!("    hades login --host {url} --token {token}");
                                println!();
                                println!("  the URL changes when the host restarts; the token does not.");
                                println!("  treat this line like a password.");
                            }
                            ExitCode::SUCCESS
                        }
                        None => fail(
                            HadesError::Other(
                                "no control tunnel — install cloudflared (`brew install cloudflared`) and restart the daemon".into(),
                            ),
                            json,
                        ),
                    }
                }
                Err(e) => fail(e, json),
            }
        }
        HostCmd::Join { hub, token, name } => {
            // this device must be up with a control tunnel before it can join
            let local = DaemonClient::for_host(
                &format!("127.0.0.1:{}", config.api_port),
                config.auth_token.clone(),
            );
            let status = match local.host_status().await {
                Ok(s) => s,
                Err(e) => return fail(e, json),
            };
            let Some(control_url) = status.control_url else {
                return fail(
                    HadesError::Other(
                        "this device has no control tunnel yet — install cloudflared and restart the daemon, then re-run join".into(),
                    ),
                    json,
                );
            };
            let Some(own_token) = config.auth_token.clone() else {
                return fail(
                    HadesError::Other("no auth token in config — run `hades host init`".into()),
                    json,
                );
            };
            let device_name = name.unwrap_or_else(|| {
                std::process::Command::new("hostname")
                    .arg("-s")
                    .output()
                    .ok()
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "device".into())
            });

            // mark ownership on this device
            let mut cfg = config.clone();
            cfg.fleet.hub_url = Some(hub.clone());
            cfg.fleet.hub_token = Some(token.clone());
            if let Err(e) = cfg.save(&paths.config()) {
                return fail(HadesError::Other(format!("cannot save config: {e}")), json);
            }

            let hub_client = DaemonClient::for_host(&hub, Some(token));
            match hub_client
                .join(&hades_api::types::JoinRequest {
                    name: device_name.clone(),
                    control_url,
                    token: own_token,
                })
                .await
            {
                Ok(view) => {
                    if json {
                        render::json(&view);
                    } else {
                        println!();
                        println!("  ⚖ {device_name} joined the fleet");
                        println!();
                        render::fleet(&view);
                        println!();
                        println!("  deploys from the hub now consider this device.");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        HostCmd::Ps => match client().ps().await {
            Ok(p) => {
                if json {
                    render::json(&p);
                } else {
                    render::ps(&p);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
    }
}

fn scaffold_manifest(json: bool) -> ExitCode {
    let path = std::env::current_dir().unwrap().join(Manifest::FILENAME);
    if path.exists() {
        return fail(
            HadesError::Other(format!("{} already exists", path.display())),
            json,
        );
    }
    let dir_name = std::env::current_dir()
        .ok()
        .and_then(|d| d.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "app".into())
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();

    let template = format!(
        r#"# Hades app manifest — `hades deploy` reads this.
[app]
name = "{dir_name}"
# Exactly one of `image` (registry image) or [app.build] (local Dockerfile):
# image = "nginx:alpine"
ports = [8000]          # first port receives proxied traffic
replicas = 1
priority = "normal"     # critical | normal | low — shedding order under pressure
# max_concurrent_requests = 64

[app.build]
dockerfile = "Dockerfile"
context = "."

[app.resources]
cpu = 0.5
memory = "256mb"        # mandatory: admission control needs a declared number

[app.power]
on_battery = "run"      # or "pause" to save battery when unplugged

# [app.health_check]
# path = "/"
"#
    );
    std::fs::write(&path, template).unwrap();
    if json {
        render::json(&serde_json::json!({ "created": path }));
    } else {
        println!("created {}", path.display());
        println!("edit it, then run `hades deploy`");
    }
    ExitCode::SUCCESS
}

async fn deploy(
    app: Option<String>,
    dir: Option<PathBuf>,
    device: Option<String>,
    json: bool,
) -> ExitCode {
    let dir = dir.unwrap_or_else(|| std::env::current_dir().unwrap());
    let (mut manifest, manifest_path) = match Manifest::load(&dir) {
        Ok(m) => m,
        Err(e) => return fail(e, json),
    };
    if let Some(name) = app {
        manifest.app.name = name;
    }
    let spec = manifest.app.clone();

    let context_tar = if let Some(build) = &spec.build {
        let ctx_dir = manifest_path
            .parent()
            .unwrap()
            .join(&build.context);
        eprintln!("packing build context {}…", ctx_dir.display());
        match context::pack(&ctx_dir) {
            Ok(bytes) => {
                eprintln!("context: {:.1}KB", bytes.len() as f64 / 1024.0);
                Some(bytes)
            }
            Err(e) => return fail(e, json),
        }
    } else {
        None
    };

    eprintln!(
        "deploying {} ({} replica{})…",
        spec.name,
        spec.replicas,
        if spec.replicas == 1 { "" } else { "s" }
    );
    match client().deploy(&spec, context_tar, device.as_deref()).await {
        Ok(resp) => {
            if json {
                render::json(&resp);
            } else {
                render::deploy(&resp);
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(e, json),
    }
}

async fn apps_cmd(cmd: AppsCmd, json: bool) -> ExitCode {
    let c = client();
    match cmd {
        AppsCmd::List => match c.list_apps().await {
            Ok(list) => {
                if json {
                    render::json(&list);
                } else {
                    render::apps(&list);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        AppsCmd::Logs { name, follow } => match c.logs(&name, follow).await {
            Ok(resp) => {
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(b) => {
                            use std::io::Write;
                            let _ = std::io::stdout().write_all(&b);
                            let _ = std::io::stdout().flush();
                        }
                        Err(_) => break,
                    }
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        AppsCmd::Stats { name } => match c.app_stats(&name).await {
            Ok(s) => {
                if json {
                    render::json(&s);
                } else {
                    render::stats(&s);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        AppsCmd::Pause { name } => match c.pause(&name).await {
            Ok(info) => {
                if json {
                    render::json(&info);
                } else {
                    println!("{} paused", info.name);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        AppsCmd::Resume { name } => match c.resume(&name).await {
            Ok(info) => {
                if json {
                    render::json(&info);
                } else {
                    println!("{} resumed", info.name);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        AppsCmd::Destroy { name } => match c.destroy(&name).await {
            Ok(info) => {
                if json {
                    render::json(&info);
                } else {
                    println!("{} destroyed (container, routes, tunnel)", info.name);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
    }
}

async fn fleet_cmd(cmd: FleetCmd, json: bool) -> ExitCode {
    let c = client();
    match cmd {
        FleetCmd::List => match c.fleet().await {
            Ok(view) => {
                if json {
                    render::json(&view);
                } else if view.devices.is_empty() {
                    println!("no devices joined — `hades fleet add` prints what to run on a new machine");
                } else {
                    render::fleet(&view);
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        FleetCmd::Remove { name } => match c.fleet_remove(&name).await {
            Ok(view) => {
                if json {
                    render::json(&view);
                } else {
                    println!("{name} removed from the fleet (its apps keep running there)");
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        FleetCmd::Add => match c.host_status().await {
            Ok(s) => {
                let paths = HadesPaths::new();
                let config = HadesConfig::load_or_default(&paths.config());
                let (Some(url), Some(token)) = (s.control_url, config.auth_token) else {
                    return fail(
                        HadesError::Other(
                            "the hub needs a control tunnel + token first (install cloudflared, restart the daemon)".into(),
                        ),
                        json,
                    );
                };
                if json {
                    render::json(&serde_json::json!({
                        "join_command": format!("hades host join --hub {url} --token {token}"),
                    }));
                } else {
                    println!();
                    println!("  on the new machine:");
                    println!();
                    println!("    1. install hades (the site's install script), then:");
                    println!("    2. hades host join --hub {url} --token {token}");
                    println!();
                    println!("  the installer also offers this step interactively.");
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
    }
}

async fn stream_events(follow: bool, days: Option<f64>, json: bool) -> ExitCode {
    match client().events(follow, days).await {
        Ok(resp) => {
            let mut stream = resp.bytes_stream();
            let mut buf = Vec::new();
            while let Some(chunk) = stream.next().await {
                let Ok(bytes) = chunk else { break };
                buf.extend_from_slice(&bytes);
                while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).collect();
                    let line = String::from_utf8_lossy(&line);
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if json {
                        println!("{line}");
                    } else if let Some(env) = hades_api::client::parse_event_line(line) {
                        println!(
                            "{}  {}",
                            env.at.format("%Y-%m-%d %H:%M:%S"),
                            env.event.summary()
                        );
                    }
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(e, json),
    }
}
