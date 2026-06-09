//! `hades update` — self-update for one host. Source resolution, in order:
//!
//!   1. `--from <url>`: fetch `<url>/hades-src.tar.gz` (a hades site)
//!   2. the directory you're standing in, if it's a hades checkout
//!   3. `~/.hades/src` with a .git — `git pull`
//!   4. this device's hub (`[fleet]` in config) — GET /host/src, so fleet
//!      devices update from the machine they joined; software propagates
//!      host to host with no registry in between
//!
//! Then: `cargo build --release`, swap binaries via rename (safe while
//! running), bounce the daemon, wait for health.

use std::path::PathBuf;
use std::process::Command;

use hades_api::DaemonClient;
use hades_core::{HadesConfig, HadesError, HadesPaths};

pub struct UpdateOutcome {
    pub source: String,
    pub old_version: Option<String>,
    pub new_version: String,
    pub daemon_restarted: bool,
}

fn is_checkout(dir: &std::path::Path) -> bool {
    let cargo = dir.join("Cargo.toml");
    cargo.exists()
        && std::fs::read_to_string(cargo)
            .map(|s| s.contains("hades-cli"))
            .unwrap_or(false)
}

fn run_in(dir: &std::path::Path, cmd: &str, args: &[&str]) -> Result<(), HadesError> {
    let out = Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| HadesError::Other(format!("{cmd} failed to start: {e}")))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(HadesError::Other(format!(
            "{cmd} {} failed:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
                .lines()
                .rev()
                .take(12)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n")
        )))
    }
}

fn extract_tar_gz(bytes: &[u8], dest: &std::path::Path, strip: u8) -> Result<(), HadesError> {
    std::fs::create_dir_all(dest)?;
    let tmp = dest.with_extension("dl.tar.gz");
    std::fs::write(&tmp, bytes)?;
    let strip_arg = format!("--strip-components={strip}");
    let mut args = vec!["xzf", tmp.to_str().unwrap(), "-C", dest.to_str().unwrap()];
    if strip > 0 {
        args.push(&strip_arg);
    }
    let r = run_in(dest, "tar", &args);
    let _ = std::fs::remove_file(&tmp);
    r
}

pub async fn run(from: Option<String>, progress: impl Fn(&str)) -> Result<UpdateOutcome, HadesError> {
    let paths = HadesPaths::new();
    let config = HadesConfig::load_or_default(&paths.config());
    let managed_src = paths.root.join("src");

    let old_version = DaemonClient::for_host(
        &format!("127.0.0.1:{}", config.api_port),
        config.auth_token.clone(),
    )
    .health()
    .await
    .ok()
    .map(|h| h.version);

    // ── 1 · bring the source up to date ───────────────────────────────
    let (src, source_desc): (PathBuf, String) = if let Some(url) = from {
        let url = url.trim_end_matches('/').to_string();
        let full = if url.ends_with(".tar.gz") {
            url.clone()
        } else {
            format!("{url}/hades-src.tar.gz")
        };
        progress(&format!("fetching {full}"));
        let bytes = reqwest::get(&full)
            .await
            .map_err(|e| HadesError::Other(format!("download failed: {e}")))?
            .bytes()
            .await
            .map_err(|e| HadesError::Other(format!("download failed: {e}")))?;
        // site tarballs carry a hades/ prefix
        extract_tar_gz(&bytes, &managed_src, 1)?;
        (managed_src.clone(), full)
    } else if is_checkout(&std::env::current_dir()?) {
        let cwd = std::env::current_dir()?;
        progress(&format!("using this checkout: {}", cwd.display()));
        (cwd, "local checkout".into())
    } else if managed_src.join(".git").exists() {
        progress("git pull in ~/.hades/src");
        run_in(&managed_src, "git", &["pull", "--ff-only"])?;
        (managed_src.clone(), "git".into())
    } else if let (Some(hub), token) = (
        config.fleet.hub_url.clone(),
        config.fleet.hub_token.clone(),
    ) {
        progress(&format!("fetching source from the hub ({hub})"));
        let client = DaemonClient::for_host(&hub, token);
        let bytes = client.fetch_src().await.map_err(|e| e.error)?;
        // hub tarballs are prefix-less (tar -C src .)
        extract_tar_gz(&bytes, &managed_src, 0)?;
        (managed_src.clone(), format!("hub {hub}"))
    } else if is_checkout(&managed_src) {
        progress("no update source — rebuilding ~/.hades/src as-is");
        (managed_src.clone(), "existing sources".into())
    } else {
        return Err(HadesError::Other(
            "nowhere to update from — run inside a checkout, join a fleet, or pass --from <site-url>"
                .into(),
        ));
    };

    // ── 2 · build ──────────────────────────────────────────────────────
    progress("building (release) — a few minutes if the cache is cold");
    let cargo = if Command::new("cargo").arg("--version").output().is_ok() {
        "cargo".to_string()
    } else {
        format!("{}/.cargo/bin/cargo", std::env::var("HOME").unwrap_or_default())
    };
    run_in(&src, &cargo, &["build", "--release", "--workspace"])?;

    // ── 3 · swap binaries (rename is safe while the old ones run) ─────
    let bin = paths.root.join("bin");
    std::fs::create_dir_all(&bin)?;
    for name in ["hades", "hadesd"] {
        let built = src.join("target/release").join(name);
        let staged = bin.join(format!(".{name}.new"));
        let final_ = bin.join(name);
        std::fs::copy(&built, &staged)?;
        std::fs::rename(&staged, &final_)?;
    }
    progress("binaries swapped in ~/.hades/bin");

    // ── 4 · bounce the daemon ──────────────────────────────────────────
    let kicked = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("gui/{}/com.hades.daemon", uid())])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !kicked {
        // not under launchd (dev setups): a plain kill; KeepAlive or the
        // operator brings it back
        let _ = Command::new("pkill").args(["-f", "hadesd"]).output();
    }
    progress("daemon restarting…");

    let local = DaemonClient::for_host(
        &format!("127.0.0.1:{}", config.api_port),
        config.auth_token.clone(),
    );
    let mut restarted = false;
    let mut new_version = env!("CARGO_PKG_VERSION").to_string();
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if let Ok(h) = local.health().await {
            new_version = h.version;
            restarted = true;
            break;
        }
    }

    Ok(UpdateOutcome {
        source: source_desc,
        old_version,
        new_version,
        daemon_restarted: restarted,
    })
}

fn uid() -> u32 {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(501)
}
