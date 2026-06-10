use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::HadesError;

/// Declarative description of one app. This is what `Hades.toml` deserializes
/// into and what the daemon persists as desired state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AppSpec {
    pub name: String,
    /// Pre-built image reference. Exactly one of `image` / `build` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<BuildSpec>,
    /// Optional base-OS hint, informational for now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    pub resources: Resources,
    /// Container ports to expose. The first entry receives proxied traffic.
    pub ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheck>,
    #[serde(default = "default_replicas")]
    pub replicas: u8,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub power: PowerPolicy,
    /// Max concurrent in-flight requests the proxy allows before shedding
    /// with 503. None = unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_requests: Option<u32>,
}

fn default_replicas() -> u8 {
    1
}

impl AppSpec {
    pub fn validate(&self) -> Result<(), HadesError> {
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(HadesError::InvalidSpec(format!(
                "app name '{}' must be non-empty lowercase [a-z0-9-]",
                self.name
            )));
        }
        match (&self.image, &self.build) {
            (None, None) => {
                return Err(HadesError::InvalidSpec(
                    "spec needs either 'image' or a [build] section".into(),
                ))
            }
            (Some(_), Some(_)) => {
                return Err(HadesError::InvalidSpec(
                    "spec must set 'image' or [build], not both".into(),
                ))
            }
            _ => {}
        }
        if self.ports.is_empty() {
            return Err(HadesError::InvalidSpec(
                "spec must expose at least one port".into(),
            ));
        }
        if self.resources.memory_mb == 0 {
            return Err(HadesError::InvalidSpec(
                "resources.memory is mandatory (e.g. \"256mb\"); admission control needs a declared number".into(),
            ));
        }
        if self.replicas == 0 {
            return Err(HadesError::InvalidSpec("replicas must be >= 1".into()));
        }
        Ok(())
    }

    /// The container port that receives proxied traffic.
    pub fn primary_port(&self) -> u16 {
        self.ports[0]
    }

    pub fn local_hostname(&self) -> String {
        format!("{}.localhost", self.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BuildSpec {
    #[serde(default = "default_dockerfile")]
    pub dockerfile: String,
    /// Build context directory, relative to the manifest.
    #[serde(default = "default_context")]
    pub context: String,
}

fn default_dockerfile() -> String {
    "Dockerfile".into()
}
fn default_context() -> String {
    ".".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// CPU quota in cores (may be fractional, e.g. 0.5).
    #[serde(default = "default_cpu")]
    pub cpu: f64,
    /// Declared memory ceiling. Mandatory; parsed from "512mb" / "2gb".
    #[serde(
        rename = "memory",
        deserialize_with = "de_memory",
        serialize_with = "ser_memory"
    )]
    pub memory_mb: u64,
    /// Disk hint in MB, informational for now.
    #[serde(
        default,
        rename = "disk",
        deserialize_with = "de_memory_opt",
        serialize_with = "ser_memory_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub disk_mb: Option<u64>,
}

fn default_cpu() -> f64 {
    1.0
}

/// Parse "512mb", "2gb", "256" (MB) into MB.
pub fn parse_size_mb(s: &str) -> Result<u64, String> {
    let t = s.trim().to_ascii_lowercase();
    let (num, mult) = if let Some(n) = t.strip_suffix("gb") {
        (n, 1024)
    } else if let Some(n) = t.strip_suffix("mb") {
        (n, 1)
    } else {
        (t.as_str(), 1)
    };
    num.trim()
        .parse::<f64>()
        .map_err(|_| format!("cannot parse size '{s}'"))
        .map(|v| (v * mult as f64).round() as u64)
}

pub fn format_size_mb(mb: u64) -> String {
    if mb >= 1024 && mb.is_multiple_of(1024) {
        format!("{}gb", mb / 1024)
    } else {
        format!("{mb}mb")
    }
}

fn de_memory<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    let s = String::deserialize(d)?;
    parse_size_mb(&s).map_err(serde::de::Error::custom)
}
fn ser_memory<S: serde::Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_size_mb(*v))
}
fn de_memory_opt<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    let s = Option::<String>::deserialize(d)?;
    s.map(|s| parse_size_mb(&s).map_err(serde::de::Error::custom))
        .transpose()
}
fn ser_memory_opt<S: serde::Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(mb) => s.serialize_some(&format_size_mb(*mb)),
        None => s.serialize_none(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HealthCheck {
    /// HTTP path probed on the app's primary port.
    pub path: String,
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_interval() -> u64 {
    10
}
fn default_timeout() -> u64 {
    3
}

/// Shedding order under resource pressure: `low` apps are paused first,
/// `critical` apps are never touched.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    Critical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct PowerPolicy {
    #[serde(default)]
    pub on_battery: OnBattery,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnBattery {
    #[default]
    Run,
    Pause,
}

/// `Hades.toml` on disk: `[app]` table holding the spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub app: AppSpec,
}

impl Manifest {
    pub const FILENAME: &'static str = "Hades.toml";

    pub fn load(dir: &Path) -> Result<(Self, PathBuf), HadesError> {
        let path = dir.join(Self::FILENAME);
        let raw = std::fs::read_to_string(&path).map_err(|e| {
            HadesError::ManifestNotFound(format!("{}: {e}", path.display()))
        })?;
        let m: Manifest = toml::from_str(&raw)
            .map_err(|e| HadesError::InvalidSpec(format!("{}: {e}", path.display())))?;
        m.app.validate()?;
        Ok((m, path))
    }

    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).expect("manifest serializes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &str = r#"
[app]
name = "hello"
ports = [8000]
replicas = 2
priority = "low"

[app.build]
context = "."

[app.resources]
cpu = 0.5
memory = "256mb"

[app.power]
on_battery = "pause"

[app.health_check]
path = "/"
"#;

    #[test]
    fn manifest_round_trip() {
        let m: Manifest = toml::from_str(HELLO).unwrap();
        m.app.validate().unwrap();
        assert_eq!(m.app.resources.memory_mb, 256);
        assert_eq!(m.app.priority, Priority::Low);
        assert_eq!(m.app.power.on_battery, OnBattery::Pause);
        let back: Manifest = toml::from_str(&m.to_toml()).unwrap();
        assert_eq!(back.app, m.app);
    }

    #[test]
    fn sizes_parse() {
        assert_eq!(parse_size_mb("512mb").unwrap(), 512);
        assert_eq!(parse_size_mb("2gb").unwrap(), 2048);
        assert_eq!(parse_size_mb("256").unwrap(), 256);
        assert!(parse_size_mb("lots").is_err());
    }

    #[test]
    fn validation_rejects_missing_memory() {
        let bad = r#"
[app]
name = "x"
image = "nginx"
ports = [80]
[app.resources]
memory = "0mb"
"#;
        let m: Manifest = toml::from_str(bad).unwrap();
        assert!(m.app.validate().is_err());
    }
}
