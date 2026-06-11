//! `hades` — the agent-friendly CLI. Just a CLI: every command supports
//! `--json` (one final JSON object on stdout, progress on stderr) and exits
//! with stable codes: 0 ok, 1 generic, 2 doctor-red, 3 admission-rejected,
//! 4 not-found, 5 daemon-unreachable, 6 invalid spec/manifest.

mod context;
mod render;
mod update;

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
    /// Open the live fleet dashboard in your browser (served from the local
    /// daemon at localhost). Works on any machine in the fleet.
    Dashboard,
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
    /// Per-app secrets: set on the host that runs the app, injected as env
    /// at container start. Never in the manifest, never in git, never
    /// echoed back. Running apps restart to pick changes up.
    Secrets {
        #[command(subcommand)]
        cmd: SecretsCmd,
    },
    /// Your devices: list them, add one, remove one. Deploys are placed on
    /// whichever device has the most free memory.
    Fleet {
        #[command(subcommand)]
        cmd: Option<FleetCmd>,
    },
    /// Run another instance of a hub-hosted app on a fleet device. The hub
    /// keeps the link and load-balances across every instance, so the app
    /// survives one machine going down or hitting memory.
    Spread {
        /// App to spread (default: the Hades.toml here).
        app: Option<String>,
        /// Device to also run it on (as shown by `hades fleet`).
        #[arg(long)]
        to: String,
    },
    /// Send feedback or report an issue. Collected centrally.
    Feedback {
        /// The issue / feedback text (omit to be prompted).
        #[arg(trailing_var_arg = true)]
        issue: Vec<String>,
    },
    /// Run networking self-tests — uptime, response rate, load balancing —
    /// on the whole fleet or one machine. Run with no flag to pick.
    Test {
        /// Machine to test: "local" (this hub) or a device name.
        #[arg(long)]
        device: Option<String>,
        /// Test the whole fleet (skip the prompt).
        #[arg(long)]
        fleet: bool,
    },
    /// Stop running an app on a spread device and re-home its traffic.
    Gather {
        /// App to gather (default: the Hades.toml here).
        app: Option<String>,
        /// Device to pull it off; omit to gather from every device.
        #[arg(long)]
        from: Option<String>,
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
    /// SSH into one of your fleet devices. Hades opens the road (an ssh://
    /// tunnel via the device's daemon); authentication is plain ssh against
    /// that machine's user accounts. Requires Remote Login enabled there.
    Ssh {
        /// Device name (as shown by `hades fleet`).
        device: String,
        /// Username on the remote machine (default: its daemon's user).
        #[arg(long)]
        user: Option<String>,
    },
    /// Update hades on this machine: refresh source (checkout > git >
    /// your hub > --from), rebuild, swap binaries, restart the daemon.
    Update {
        /// A hades site URL to pull source from (e.g. the gates app).
        #[arg(long)]
        from: Option<String>,
    },
    /// Claim a stable custom URL for an app: https://<name>.<domain>,
    /// served through your operator's coordinator. No Cloudflare account
    /// needed. The URL never changes.
    Domain {
        #[command(subcommand)]
        cmd: DomainCmd,
    },
    /// Notification utilities.
    Notify {
        #[command(subcommand)]
        cmd: NotifyCmd,
    },
}

#[derive(Subcommand)]
enum DomainCmd {
    /// Claim <name> for an app — it becomes https://<name>.<domain> forever.
    Claim {
        /// The subdomain label you want (lowercase [a-z0-9-]).
        name: String,
        /// App to point it at (default: the Hades.toml here).
        #[arg(long)]
        app: Option<String>,
    },
    /// Give up an app's claimed domain.
    Release {
        #[arg(long)]
        app: Option<String>,
    },
    /// List claimed domains.
    List,
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
    /// Give this host a STABLE control URL (a named tunnel at
    /// <name>.<domain>) so it stays reachable across restarts instead of
    /// rotating a quick tunnel. Run on the hub, then on each device.
    Stabilize {
        /// Subdomain label (default: this machine's short hostname).
        name: Option<String>,
    },
    /// (operator) Put this host on a domain you own via a named tunnel:
    /// apps at https://<app>.<domain>, API at https://api.<domain>.
    /// Requires `cloudflared tunnel login` first.
    Domain {
        domain: String,
        #[arg(long, default_value = "hades")]
        tunnel: String,
    },
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
enum SecretsCmd {
    /// Set one or more KEY=VALUE pairs.
    Set {
        /// KEY=VALUE pairs.
        #[arg(required = true)]
        pairs: Vec<String>,
        /// App name (default: the Hades.toml in this directory).
        #[arg(long)]
        app: Option<String>,
    },
    /// List secret KEY names (values never come back).
    List {
        #[arg(long)]
        app: Option<String>,
    },
    /// Remove keys.
    Unset {
        #[arg(required = true)]
        keys: Vec<String>,
        #[arg(long)]
        app: Option<String>,
    },
}

#[derive(Subcommand)]
enum FleetCmd {
    /// List devices with live capacity (default).
    List,
    /// Print the two lines to run on a new machine to add it.
    Add,
    /// Remove a device from the fleet (its apps keep running on it).
    Remove { name: String },
    /// Tell every joined device to self-update (each pulls source from
    /// this hub, rebuilds, and restarts its daemon).
    Update,
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
fn hades_host_cert_present() -> bool {
    std::env::var("HOME")
        .map(|h| std::path::Path::new(&h).join(".cloudflared/cert.pem").exists())
        .unwrap_or(false)
}

fn host_uid() -> u32 {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(501)
}

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
        Cmd::Dashboard => {
            let paths = HadesPaths::new();
            let config = HadesConfig::load_or_default(&paths.config());
            let token = config.auth_token.clone().unwrap_or_default();
            let url = format!("http://127.0.0.1:{}/dashboard#{}", config.api_port, token);
            // token rides in the URL fragment, which browsers never send to a
            // server, so it stays on this machine
            let _ = std::process::Command::new("open").arg(&url).status();
            if json {
                render::json(&serde_json::json!({ "dashboard": url }));
            } else {
                println!("opening the dashboard: http://127.0.0.1:{}/dashboard", config.api_port);
            }
            ExitCode::SUCCESS
        }
        Cmd::Logout => {
            let _ = std::fs::remove_file(session_path());
            if json {
                render::json(&serde_json::json!({ "logged_out": true }));
            } else {
                println!("logged out; commands now target the local host");
            }
            ExitCode::SUCCESS
        }
        Cmd::Init => scaffold_manifest(json),
        Cmd::Deploy { app, dir, device } => deploy(app, dir, device, json).await,
        Cmd::Apps { cmd } => apps_cmd(cmd, json).await,
        Cmd::Secrets { cmd } => secrets_cmd(cmd, json).await,
        Cmd::Fleet { cmd } => fleet_cmd(cmd.unwrap_or(FleetCmd::List), json).await,
        Cmd::Feedback { issue } => feedback_cmd(issue, json).await,
        Cmd::Test { device, fleet } => test_cmd(device, fleet, json).await,
        Cmd::Spread { app, to } => spread_cmd(app, to, json).await,
        Cmd::Gather { app, from } => gather_cmd(app, from, json).await,
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
                        None => println!("{} (no public URL, local only)", info.local_url),
                    }
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
        Cmd::Ssh { device, user } => ssh_cmd(device, user, json).await,
        Cmd::Update { from } => {
            match update::run(from, |msg| eprintln!("  ◆ {msg}")).await {
                Ok(o) => {
                    if json {
                        render::json(&serde_json::json!({
                            "updated": true, "source": o.source,
                            "old_version": o.old_version, "new_version": o.new_version,
                            "daemon_restarted": o.daemon_restarted,
                        }));
                    } else {
                        println!();
                        println!(
                            "  updated from {}: v{} → v{}",
                            o.source,
                            o.old_version.unwrap_or_else(|| "?".into()),
                            o.new_version
                        );
                        if !o.daemon_restarted {
                            println!("  daemon did not come back by itself; start hadesd (or check launchd)");
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        Cmd::Events { follow, days } => stream_events(follow, days, json).await,
        Cmd::Domain { cmd } => domain_cmd(cmd, json).await,
        Cmd::Notify { cmd: NotifyCmd::Test } => match client().notify_test().await {
            Ok(r) => {
                if json {
                    render::json(&r);
                } else if r.delivered_to.is_empty() {
                    println!("no channels delivered; run `hades host init` to configure ntfy");
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
    // `--token -` reads from stdin so the secret stays out of shell history
    let token = match token.as_deref() {
        Some("-") => {
            let mut line = String::new();
            let _ = std::io::stdin().read_line(&mut line);
            Some(line.trim().to_string()).filter(|t| !t.is_empty())
        }
        _ => token,
    };
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
                    "no hadesd at {host}; run the install script or `hades host init` first"
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
        println!("  host      {host} · hadesd v{}", health.version);
        println!(
            "  doctor    {}",
            if health.doctor_green {
                "GREEN · ready for deploys"
            } else {
                "RED · run `hades host doctor` for remedies"
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
        println!("    hades deploy      ship it; returns a link");
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
            eprintln!("hades host init: bootstrapping this machine as a cloud host…");
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
                    println!("   (install the ntfy app, subscribe to that topic, no account needed)");
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
        HostCmd::Domain { domain, tunnel } => {
            if !hades_host_cert_present() {
                return fail(
                    HadesError::Other(
                        "first authorize cloudflared with your domain:\n    cloudflared tunnel login\nthen re-run this command".into(),
                    ),
                    json,
                );
            }
            let mut cfg = config.clone();
            cfg.domain.name = Some(domain.clone());
            cfg.domain.tunnel_name = Some(tunnel.clone());
            if let Err(e) = cfg.save(&paths.config()) {
                return fail(HadesError::Other(format!("cannot save config: {e}")), json);
            }
            // restart the daemon so it brings up the named tunnel
            let _ = std::process::Command::new("launchctl")
                .args([
                    "kickstart",
                    "-k",
                    &format!("gui/{}/com.hades.daemon", host_uid()),
                ])
                .output();
            if json {
                render::json(&serde_json::json!({
                    "domain": domain, "tunnel": tunnel,
                    "api": format!("https://api.{domain}"),
                    "app_pattern": format!("https://<app>.{domain}"),
                }));
            } else {
                println!();
                println!("  ⚖ this host is now {domain}");
                println!();
                println!("    apps     https://<app>.{domain}");
                println!("    api      https://api.{domain}");
                println!();
                println!("  add a wildcard CNAME at your DNS so new apps resolve:");
                println!("    *.{domain}   →   (cloudflared creates per-app records too)");
                println!();
                println!("  the daemon is restarting onto the named tunnel.");
                println!("  re-run `hades host connect-info` for the stable join line.");
            }
            ExitCode::SUCCESS
        }
        HostCmd::ConnectInfo => {
            let local = DaemonClient::for_host(
                &format!("127.0.0.1:{}", config.api_port),
                config.auth_token.clone(),
            );
            match local.host_status().await {
                Ok(s) => {
                    let Some(token) = config.auth_token else {
                        return fail(
                            HadesError::Other("no auth token in config; run `hades host init`".into()),
                            json,
                        );
                    };
                    match s.control_url {
                        Some(url) => {
                            if json {
                                render::json(&serde_json::json!({
                                    "host": url, "token": token,
                                    // join = add that machine to this fleet; login = drive this host from there
                                    "join_command": format!("hades host join --hub {url} --token {token}"),
                                    "login_command": format!("hades login --host {url} --token {token}"),
                                }));
                            } else {
                                println!();
                                println!("  to add another machine to this fleet, run THERE:");
                                println!();
                                println!("    hades host join --hub {url} --token {token}");
                                println!();
                                println!("  (or, to drive this host from another machine instead of");
                                println!("   joining it: hades login --host {url} --token {token})");
                                println!();
                                println!("  the URL changes when the host restarts; the token does not.");
                                println!("  treat this line like a password.");
                            }
                            ExitCode::SUCCESS
                        }
                        None => fail(
                            HadesError::Other(
                                "no control tunnel; install cloudflared (`brew install cloudflared`) and restart the daemon".into(),
                            ),
                            json,
                        ),
                    }
                }
                Err(e) => fail(e, json),
            }
        }
        HostCmd::Stabilize { name } => stabilize_cmd(name, json).await,
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
                        "this device has no control tunnel yet; install cloudflared and restart the daemon, then re-run join".into(),
                    ),
                    json,
                );
            };
            let Some(own_token) = config.auth_token.clone() else {
                return fail(
                    HadesError::Other("no auth token in config; run `hades host init`".into()),
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
        r#"# Hades app manifest. `hades deploy` reads this.
[app]
name = "{dir_name}"
# Exactly one of `image` (registry image) or [app.build] (local Dockerfile):
# image = "nginx:alpine"
ports = [8000]          # first port receives proxied traffic
replicas = 1
priority = "normal"     # critical | normal | low (shedding order under pressure)
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

fn app_or_manifest(app: Option<String>) -> Result<String, HadesError> {
    if let Some(a) = app {
        return Ok(a);
    }
    let dir = std::env::current_dir().map_err(|e| HadesError::Other(e.to_string()))?;
    Manifest::load(&dir)
        .map(|(m, _)| m.app.name)
        .map_err(|_| {
            HadesError::Other("no --app given and no Hades.toml here".into())
        })
}

fn render_secrets(v: &hades_api::types::SecretsView) {
    let where_ = v.device.as_deref().unwrap_or("local");
    println!(
        "{} · {} secret{} on {} ({}){}",
        v.app,
        v.keys.len(),
        if v.keys.len() == 1 { "" } else { "s" },
        where_,
        if v.encrypted { "keychain-encrypted" } else { "file permissions only" },
        if v.applied { " · running replicas restarted" } else { "" },
    );
    for k in &v.keys {
        println!("  {k}");
    }
}

async fn secrets_cmd(cmd: SecretsCmd, json: bool) -> ExitCode {
    let c = client();
    match cmd {
        SecretsCmd::Set { pairs, app } => {
            let app = match app_or_manifest(app) {
                Ok(a) => a,
                Err(e) => return fail(e, json),
            };
            let mut update = hades_api::types::SecretsUpdate::default();
            for pair in &pairs {
                let Some((k, v)) = pair.split_once('=') else {
                    return fail(
                        HadesError::InvalidSpec(format!("'{pair}' is not KEY=VALUE")),
                        json,
                    );
                };
                update.set.insert(k.trim().to_string(), v.to_string());
            }
            match c.secrets_update(&app, &update).await {
                Ok(v) => {
                    if json {
                        render::json(&v);
                    } else {
                        render_secrets(&v);
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        SecretsCmd::List { app } => {
            let app = match app_or_manifest(app) {
                Ok(a) => a,
                Err(e) => return fail(e, json),
            };
            match c.secrets_list(&app).await {
                Ok(v) => {
                    if json {
                        render::json(&v);
                    } else if v.keys.is_empty() {
                        println!("{app} has no secrets; `hades secrets set KEY=VALUE --app {app}`");
                    } else {
                        render_secrets(&v);
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        SecretsCmd::Unset { keys, app } => {
            let app = match app_or_manifest(app) {
                Ok(a) => a,
                Err(e) => return fail(e, json),
            };
            let update = hades_api::types::SecretsUpdate {
                set: Default::default(),
                unset: keys,
            };
            match c.secrets_update(&app, &update).await {
                Ok(v) => {
                    if json {
                        render::json(&v);
                    } else {
                        render_secrets(&v);
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
    }
}

/// Open the device's ssh tunnel through the hub, then hand this terminal
/// to ssh with cloudflared as the transport.
async fn ssh_cmd(device: String, user: Option<String>, json: bool) -> ExitCode {
    // ssh needs cloudflared locally as the ProxyCommand
    let cf_ok = std::process::Command::new("which")
        .arg("cloudflared")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        || std::path::Path::new("/opt/homebrew/bin/cloudflared").exists();
    if !cf_ok {
        return fail(
            HadesError::Other("cloudflared is needed locally for the transport: brew install cloudflared".into()),
            json,
        );
    }
    let v = match client().fleet_ssh(&device).await {
        Ok(v) => v,
        Err(e) => return fail(e, json),
    };
    let Some(url) = v["url"].as_str() else {
        return fail(HadesError::Other("device returned no tunnel url".into()), json);
    };
    let host = url.trim_start_matches("https://").trim_start_matches("http://");
    let user = user
        .or_else(|| v["user_hint"].as_str().map(String::from).filter(|u| !u.is_empty()))
        .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "root".into()));

    if json {
        // agents get the connection recipe instead of an interactive session
        render::json(&serde_json::json!({
            "device": device, "host": host, "user": user,
            "command": format!(
                "ssh -o ProxyCommand='cloudflared access ssh --hostname %h' {user}@{host}"
            ),
        }));
        return ExitCode::SUCCESS;
    }

    eprintln!("  ⚖ road open: {host}");
    eprintln!("    connecting as {user} (auth is that machine's own login)
");
    use std::os::unix::process::CommandExt;
    let err = std::process::Command::new("ssh")
        .arg("-o")
        .arg("ProxyCommand=cloudflared access ssh --hostname %h")
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg(format!("{user}@{host}"))
        .exec(); // replaces this process; only returns on failure
    eprintln!("error: could not exec ssh: {err}");
    ExitCode::from(1)
}

async fn fleet_cmd(cmd: FleetCmd, json: bool) -> ExitCode {
    let c = client();
    match cmd {
        FleetCmd::List => match c.fleet().await {
            Ok(view) => {
                if json {
                    render::json(&view);
                } else if view.devices.is_empty() {
                    println!("no devices joined; `hades fleet add` prints what to run on a new machine");
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
        FleetCmd::Update => match c.fleet_update().await {
            Ok(v) => {
                if json {
                    render::json(&v);
                } else {
                    let empty = serde_json::Map::new();
                    let devices = v["devices"].as_object().unwrap_or(&empty);
                    if devices.is_empty() {
                        println!("no devices to update");
                    } else {
                        for (name, r) in devices {
                            if r["started"].as_bool().unwrap_or(false) {
                                println!("{name}: updating (rebuild + daemon restart; takes a few minutes)");
                            } else {
                                println!("{name}: failed: {}", r["error"].as_str().unwrap_or("?"));
                            }
                        }
                        println!();
                        println!("watch a device come back with `hades fleet`");
                    }
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
                    println!("  on the new machine, one line does everything:");
                    println!();
                    println!("    curl -fsSL <site>/install.sh | HADES_HUB={url} HADES_TOKEN={token} sh");
                    println!();
                    println!("  or, if hades is already installed there:");
                    println!();
                    println!("    hades host join --hub {url} --token {token}");
                    println!();
                    println!("  the URL rotates when this hub restarts; the token does not.");
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e, json),
        },
    }
}

/// This machine's short hostname as a DNS-safe subdomain label.
fn host_label() -> String {
    let raw = std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "host".into());
    let label: String = raw
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    label.trim_matches('-').to_string()
}

async fn stabilize_cmd(name: Option<String>, json: bool) -> ExitCode {
    let paths = HadesPaths::new();
    let config = HadesConfig::load_or_default(&paths.config());
    let label = name.unwrap_or_else(host_label);
    let local = client();

    if config.domain.coordinator_url.is_some() {
        // hub: the local daemon mints + installs via its own coordinator
        eprintln!("claiming a stable control URL for {label}…");
        match local.host_stabilize(&label).await {
            Ok(v) => {
                let url = v["control_url"].as_str().unwrap_or_default();
                if json {
                    render::json(&v);
                } else {
                    println!();
                    println!("  ⚓ this host's control URL is now stable:");
                    println!("     {url}");
                    println!();
                    println!("  it survives restarts and never rotates. point devices at it:");
                    println!("     hades host join --hub {url} --token <this host's token>");
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e.error, json),
        }
    } else if let Some(hub) = config.fleet.hub_url.clone() {
        // device: ask the hub to mint a hostname, then adopt it locally
        eprintln!("asking the hub to mint a stable control URL…");
        let hub_client = DaemonClient::for_host(&hub, config.fleet.hub_token.clone());
        let minted = match hub_client.fleet_control_token(&label, config.api_port).await {
            Ok(v) => v,
            Err(e) => return fail(e.error, json),
        };
        let hostname = minted["hostname"].as_str().unwrap_or_default().to_string();
        let token = minted["connector_token"].as_str().unwrap_or_default().to_string();
        if hostname.is_empty() || token.is_empty() {
            return fail(
                HadesError::Other("the hub returned an incomplete control claim".into()),
                json,
            );
        }
        match local.host_adopt_control(&label, &hostname, &token).await {
            Ok(v) => {
                if json {
                    render::json(&v);
                } else {
                    println!();
                    println!("  ⚓ this device now has a stable control URL:");
                    println!("     https://{hostname}");
                    println!();
                    println!("  it re-registers with the hub automatically and won't rotate again.");
                }
                ExitCode::SUCCESS
            }
            Err(e) => fail(e.error, json),
        }
    } else {
        fail(
            HadesError::Other(
                "nothing to stabilize against: this host has no coordinator and hasn't joined a hub".into(),
            ),
            json,
        )
    }
}

/// Where feedback is collected (a Convex HTTP action). Override with
/// HADES_FEEDBACK_URL. This is a public ingest URL, not a secret.
const FEEDBACK_URL: &str = "https://dependable-bird-390.convex.site/feedback";

async fn feedback_cmd(issue: Vec<String>, json: bool) -> ExitCode {
    use std::io::Write;
    let mut text = issue.join(" ").trim().to_string();
    if text.is_empty() {
        print!("what's the issue? ");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        text = line.trim().to_string();
    }
    if text.is_empty() {
        return fail(HadesError::Other("no feedback text given".into()), json);
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let url = std::env::var("HADES_FEEDBACK_URL").unwrap_or_else(|_| FEEDBACK_URL.to_string());
    let body = serde_json::json!({ "issue": text, "timestamp": timestamp });
    match reqwest::Client::new()
        .post(&url)
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => {
            if json {
                render::json(&serde_json::json!({ "sent": true, "timestamp": timestamp }));
            } else {
                println!("  ✓ feedback sent — thank you");
            }
            ExitCode::SUCCESS
        }
        Ok(r) => fail(
            HadesError::Other(format!("feedback endpoint returned {}", r.status())),
            json,
        ),
        Err(e) => fail(
            HadesError::Other(format!("could not reach the feedback endpoint: {e}")),
            json,
        ),
    }
}

async fn test_cmd(device: Option<String>, fleet: bool, json: bool) -> ExitCode {
    use std::io::Write;
    let c = client();
    let scope = if fleet {
        "fleet".to_string()
    } else if let Some(dev) = device {
        dev
    } else {
        // pick: whole fleet, or a specific machine
        let view = match c.fleet().await {
            Ok(v) => v,
            Err(e) => return fail(e.error, json),
        };
        println!("what should I test?");
        println!("  0) the whole fleet");
        for (i, d) in view.devices.iter().enumerate() {
            println!("  {}) {}{}", i + 1, d.name, if d.is_self { " (this machine)" } else { "" });
        }
        print!("> ");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        match line.trim().parse::<usize>() {
            Ok(0) | Err(_) => "fleet".to_string(),
            Ok(n) => view
                .devices
                .get(n - 1)
                .map(|d| if d.is_self { "local".to_string() } else { d.name.clone() })
                .unwrap_or_else(|| "fleet".into()),
        }
    };

    eprintln!("running networking tests on {scope}…");
    match c.run_test(&scope).await {
        Ok(report) => {
            if json {
                render::json(&report);
            } else {
                render::test_report(&report);
            }
            if report.failed == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(e) => fail(e.error, json),
    }
}

async fn spread_cmd(app: Option<String>, to: String, json: bool) -> ExitCode {
    let app = match app_or_manifest(app) {
        Ok(a) => a,
        Err(e) => return fail(e, json),
    };
    eprintln!("spreading {app} to {to}…");
    match client().app_spread(&app, &to).await {
        Ok(v) => {
            if json {
                render::json(&v);
            } else {
                println!();
                println!("  ⚖ {app} now runs on this hub and {to}");
                println!("    the link load-balances across both; lose one and traffic stays up.");
                println!("    see it live: hades dashboard");
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(e, json),
    }
}

async fn gather_cmd(app: Option<String>, from: Option<String>, json: bool) -> ExitCode {
    let app = match app_or_manifest(app) {
        Ok(a) => a,
        Err(e) => return fail(e, json),
    };
    let target = from.clone().unwrap_or_else(|| "all".into());
    match client().app_gather(&app, &target).await {
        Ok(v) => {
            if json {
                render::json(&v);
            } else {
                let where_ = from.unwrap_or_else(|| "every device".into());
                println!("gathered {app} from {where_}; traffic re-homed to the remaining instances");
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(e, json),
    }
}

async fn domain_cmd(cmd: DomainCmd, json: bool) -> ExitCode {
    let c = client();
    match cmd {
        DomainCmd::Claim { name, app } => {
            let app = match app_or_manifest(app) {
                Ok(a) => a,
                Err(e) => return fail(e, json),
            };
            eprintln!("claiming {name} for {app}…");
            match c.domain_claim(&app, &name, None, None).await {
                Ok(claim) => {
                    if json {
                        render::json(&claim);
                    } else {
                        println!();
                        println!("  ⚖ {} is yours", claim.hostname);
                        println!();
                        println!("    https://{}", claim.hostname);
                        println!();
                        println!("  this URL is stable; it survives restarts and never rotates.");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        DomainCmd::Release { app } => {
            let app = match app_or_manifest(app) {
                Ok(a) => a,
                Err(e) => return fail(e, json),
            };
            match c.domain_release(&app).await {
                Ok(v) => {
                    if json {
                        render::json(&v);
                    } else {
                        println!("released {}", v["released"].as_str().unwrap_or(&app));
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => fail(e, json),
            }
        }
        DomainCmd::List => match c.domain_list().await {
            Ok(list) => {
                if json {
                    render::json(&list);
                } else if list.claims.is_empty() {
                    println!("no claimed domains; `hades domain claim <name>`");
                } else {
                    for cl in &list.claims {
                        println!("{:<16} https://{}", cl.app, cl.hostname);
                    }
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
