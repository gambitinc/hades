use std::path::PathBuf;

/// Canonical on-disk layout under `~/.hades/`.
#[derive(Debug, Clone)]
pub struct HadesPaths {
    pub root: PathBuf,
}

impl HadesPaths {
    pub fn new() -> Self {
        let root = std::env::var_os("HADES_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::home_dir()
                    .expect("cannot determine home directory")
                    .join(".hades")
            });
        Self { root }
    }

    pub fn config(&self) -> PathBuf {
        self.root.join("config.toml")
    }
    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }
    pub fn apps_state(&self) -> PathBuf {
        self.state_dir().join("apps.json")
    }
    pub fn registry(&self) -> PathBuf {
        self.state_dir().join("registry.json")
    }
    pub fn fleet_state(&self) -> PathBuf {
        self.state_dir().join("fleet.json")
    }
    pub fn claims_state(&self) -> PathBuf {
        self.state_dir().join("claims.json")
    }
    pub fn ledger_dir(&self) -> PathBuf {
        self.root.join("ledger")
    }
    pub fn uptime_ledger(&self) -> PathBuf {
        self.ledger_dir().join("uptime.jsonl")
    }
    pub fn events_ledger(&self) -> PathBuf {
        self.ledger_dir().join("events.jsonl")
    }
    pub fn heartbeat(&self) -> PathBuf {
        self.ledger_dir().join("heartbeat")
    }
    pub fn metrics_dir(&self) -> PathBuf {
        self.root.join("metrics")
    }
    pub fn battery_metrics(&self) -> PathBuf {
        self.metrics_dir().join("battery.jsonl")
    }
    pub fn resource_metrics(&self) -> PathBuf {
        self.metrics_dir().join("resources.jsonl")
    }
    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        for d in [
            self.root.clone(),
            self.state_dir(),
            self.ledger_dir(),
            self.metrics_dir(),
            self.logs_dir(),
        ] {
            std::fs::create_dir_all(d)?;
        }
        // credentials live under here (config, sessions, fleet tokens) —
        // tighten pre-existing files too, not just future writes
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.root, std::fs::Permissions::from_mode(0o700));
            let _ =
                std::fs::set_permissions(self.state_dir(), std::fs::Permissions::from_mode(0o700));
            for f in [self.config(), self.root.join("session.json")] {
                if f.exists() {
                    let _ = std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600));
                }
            }
        }
        Ok(())
    }
}

impl Default for HadesPaths {
    fn default() -> Self {
        Self::new()
    }
}
