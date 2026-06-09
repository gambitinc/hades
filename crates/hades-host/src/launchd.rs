//! Service supervision. `ServiceManager` is the OS seam — `LaunchdManager`
//! now, a systemd implementation later.

use std::path::{Path, PathBuf};
use std::process::Command;

use hades_core::HadesError;

pub const DAEMON_LABEL: &str = "com.hades.daemon";

pub trait ServiceManager {
    /// Install (idempotently) and start the supervised daemon.
    fn install(&self, program: &Path, log_dir: &Path) -> Result<(), HadesError>;
    fn uninstall(&self) -> Result<(), HadesError>;
    fn is_loaded(&self) -> bool;
    fn kickstart(&self) -> Result<(), HadesError>;
}

pub struct LaunchdManager;

impl LaunchdManager {
    fn plist_path() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .expect("HOME not set")
            .join("Library/LaunchAgents")
            .join(format!("{DAEMON_LABEL}.plist"))
    }

    fn gui_domain() -> String {
        // launchctl addresses per-user agents as gui/<uid>
        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u32>().ok())
            .unwrap_or(501);
        format!("gui/{uid}")
    }

    fn plist_contents(program: &Path, log_dir: &Path) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{program}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{out}</string>
    <key>StandardErrorPath</key>
    <string>{err}</string>
</dict>
</plist>
"#,
            label = DAEMON_LABEL,
            program = program.display(),
            out = log_dir.join("hadesd.out.log").display(),
            err = log_dir.join("hadesd.err.log").display(),
        )
    }
}

impl ServiceManager for LaunchdManager {
    fn install(&self, program: &Path, log_dir: &Path) -> Result<(), HadesError> {
        let plist = Self::plist_path();
        if let Some(parent) = plist.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&plist, Self::plist_contents(program, log_dir))?;

        // idempotent re-install: boot out any old copy first, ignore failure
        let _ = Command::new("launchctl")
            .args(["bootout", &Self::gui_domain(), &plist.display().to_string()])
            .output();

        let out = Command::new("launchctl")
            .args([
                "bootstrap",
                &Self::gui_domain(),
                &plist.display().to_string(),
            ])
            .output()?;
        if !out.status.success() {
            // fall back to legacy `load` for older macOS
            let legacy = Command::new("launchctl")
                .args(["load", "-w", &plist.display().to_string()])
                .output()?;
            if !legacy.status.success() {
                return Err(HadesError::Other(format!(
                    "launchctl bootstrap failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                )));
            }
        }
        Ok(())
    }

    fn uninstall(&self) -> Result<(), HadesError> {
        let plist = Self::plist_path();
        let _ = Command::new("launchctl")
            .args(["bootout", &Self::gui_domain(), &plist.display().to_string()])
            .output();
        if plist.exists() {
            std::fs::remove_file(plist)?;
        }
        Ok(())
    }

    fn is_loaded(&self) -> bool {
        Command::new("launchctl")
            .args(["print", &format!("{}/{}", Self::gui_domain(), DAEMON_LABEL)])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn kickstart(&self) -> Result<(), HadesError> {
        let out = Command::new("launchctl")
            .args([
                "kickstart",
                "-k",
                &format!("{}/{}", Self::gui_domain(), DAEMON_LABEL),
            ])
            .output()?;
        if out.status.success() {
            Ok(())
        } else {
            Err(HadesError::Other(format!(
                "launchctl kickstart failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }
}

