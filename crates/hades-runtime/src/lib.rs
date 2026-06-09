//! Docker orchestration via bollard: build images from uploaded tarballs,
//! run replicas with hard resource limits and `hades.app` labels, detect OOM
//! kills, sample stats, stream logs. Every container Hades creates is
//! labeled so the reaper can always find strays.

use std::collections::HashMap;
use std::io::Read;

use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, LogOutput, LogsOptions,
    RemoveContainerOptions, StatsOptions, StopContainerOptions,
};
use bollard::image::{BuildImageOptions, CreateImageOptions};
use bollard::models::{HostConfig, PortBinding, RestartPolicy, RestartPolicyNameEnum};
use bollard::Docker;
use futures_util::{Stream, StreamExt, TryStreamExt};
use hades_core::{AppSpec, HadesError};

pub const LABEL_APP: &str = "hades.app";
pub const LABEL_REPLICA: &str = "hades.replica";

#[derive(Clone)]
pub struct Runtime {
    docker: Docker,
}

#[derive(Debug, Clone)]
pub struct EngineInfo {
    /// Memory available to containers — the Docker VM on macOS, NOT the Mac.
    pub vm_memory_mb: u64,
    pub vm_cpus: u32,
}

#[derive(Debug, Clone)]
pub struct ReplicaHandle {
    pub container_id: String,
    pub host_port: u16,
}

#[derive(Debug, Clone, Default)]
pub struct ContainerStateInfo {
    pub running: bool,
    pub paused: bool,
    pub oom_killed: bool,
    pub exit_code: Option<i64>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ContainerStats {
    pub memory_used_mb: f64,
    pub memory_limit_mb: f64,
    pub cpu_pct: f64,
}

#[derive(Debug, Clone)]
pub struct LabeledContainer {
    pub id: String,
    pub app: String,
    pub name: String,
    pub state: String,
}

fn dockerr(e: bollard::errors::Error) -> HadesError {
    HadesError::Docker(e.to_string())
}

impl Runtime {
    pub fn connect() -> Result<Self, HadesError> {
        let docker = Docker::connect_with_local_defaults().map_err(dockerr)?;
        Ok(Self { docker })
    }

    pub async fn ping(&self) -> Result<(), HadesError> {
        self.docker.ping().await.map(|_| ()).map_err(dockerr)
    }

    pub async fn engine_info(&self) -> Result<EngineInfo, HadesError> {
        let info = self.docker.info().await.map_err(dockerr)?;
        Ok(EngineInfo {
            vm_memory_mb: (info.mem_total.unwrap_or(0) / (1024 * 1024)) as u64,
            vm_cpus: info.ncpu.unwrap_or(0) as u32,
        })
    }

    pub fn image_tag(app: &str) -> String {
        format!("hades/{app}:latest")
    }

    /// Build an image from a gzipped build-context tarball. Returns the tag.
    /// `progress` receives human-readable build output lines.
    pub async fn build_image(
        &self,
        app: &str,
        dockerfile: &str,
        context_tar_gz: &[u8],
        mut progress: impl FnMut(String),
    ) -> Result<String, HadesError> {
        let mut tar = Vec::new();
        flate2::read::GzDecoder::new(context_tar_gz)
            .read_to_end(&mut tar)
            .map_err(|e| HadesError::Docker(format!("bad build context: {e}")))?;

        let tag = Self::image_tag(app);
        let opts = BuildImageOptions {
            dockerfile: dockerfile.to_string(),
            t: tag.clone(),
            rm: true,
            ..Default::default()
        };
        let mut stream = self
            .docker
            .build_image(opts, None, Some(bytes::Bytes::from(tar)));
        while let Some(item) = stream.next().await {
            let info = item.map_err(dockerr)?;
            if let Some(err) = info.error {
                return Err(HadesError::Docker(format!("build failed: {err}")));
            }
            if let Some(line) = info.stream {
                let line = line.trim_end();
                if !line.is_empty() {
                    progress(line.to_string());
                }
            }
        }
        Ok(tag)
    }

    /// Pull a registry image if not already present locally.
    pub async fn pull_image(
        &self,
        image: &str,
        mut progress: impl FnMut(String),
    ) -> Result<(), HadesError> {
        if self.docker.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        let mut stream = self.docker.create_image(
            Some(CreateImageOptions {
                from_image: image.to_string(),
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(item) = stream.next().await {
            let info = item.map_err(dockerr)?;
            if let Some(s) = info.status {
                progress(s);
            }
        }
        Ok(())
    }

    /// Pick a free loopback port for a replica's published port.
    pub fn free_host_port() -> Result<u16, HadesError> {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        Ok(l.local_addr()?.port())
    }

    pub fn container_name(app: &str, replica: u8) -> String {
        format!("hades-{app}-{replica}")
    }

    /// Create + start one replica with hard limits, publishing the primary
    /// port on 127.0.0.1:<host_port>.
    pub async fn run_replica(
        &self,
        spec: &AppSpec,
        image: &str,
        replica: u8,
        host_port: u16,
    ) -> Result<ReplicaHandle, HadesError> {
        let name = Self::container_name(&spec.name, replica);
        // idempotent: clear any stale container holding this name
        let _ = self
            .docker
            .remove_container(
                &name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;

        let mut labels = HashMap::new();
        labels.insert(LABEL_APP.to_string(), spec.name.clone());
        labels.insert(LABEL_REPLICA.to_string(), replica.to_string());

        let mut port_bindings: HashMap<String, Option<Vec<PortBinding>>> = HashMap::new();
        port_bindings.insert(
            format!("{}/tcp", spec.primary_port()),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some(host_port.to_string()),
            }]),
        );
        let exposed: HashMap<String, HashMap<(), ()>> = spec
            .ports
            .iter()
            .map(|p| (format!("{p}/tcp"), HashMap::new()))
            .collect();

        let env: Vec<String> = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();

        let host_config = HostConfig {
            memory: Some(spec.resources.memory_mb as i64 * 1024 * 1024),
            nano_cpus: Some((spec.resources.cpu * 1e9) as i64),
            port_bindings: Some(port_bindings),
            // The daemon supervises restarts (backoff + crash-loop detection);
            // Docker-level restart would fight it.
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::NO),
                ..Default::default()
            }),
            ..Default::default()
        };

        let config = Config {
            image: Some(image.to_string()),
            env: Some(env),
            labels: Some(labels),
            exposed_ports: Some(exposed),
            host_config: Some(host_config),
            ..Default::default()
        };

        let created = self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .map_err(dockerr)?;
        self.docker
            .start_container::<String>(&created.id, None)
            .await
            .map_err(dockerr)?;
        Ok(ReplicaHandle {
            container_id: created.id,
            host_port,
        })
    }

    pub async fn stop_remove(&self, container_id: &str) -> Result<(), HadesError> {
        let _ = self
            .docker
            .stop_container(container_id, Some(StopContainerOptions { t: 5 }))
            .await;
        match self
            .docker
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            Err(e) => Err(dockerr(e)),
        }
    }

    pub async fn pause(&self, container_id: &str) -> Result<(), HadesError> {
        self.docker.pause_container(container_id).await.map_err(dockerr)
    }

    pub async fn unpause(&self, container_id: &str) -> Result<(), HadesError> {
        self.docker
            .unpause_container(container_id)
            .await
            .map_err(dockerr)
    }

    pub async fn restart(&self, container_id: &str) -> Result<(), HadesError> {
        self.docker
            .start_container::<String>(container_id, None)
            .await
            .map_err(dockerr)
    }

    pub async fn inspect_state(
        &self,
        container_id: &str,
    ) -> Result<ContainerStateInfo, HadesError> {
        let info = self
            .docker
            .inspect_container(container_id, None)
            .await
            .map_err(dockerr)?;
        let st = info.state.unwrap_or_default();
        Ok(ContainerStateInfo {
            running: st.running.unwrap_or(false),
            paused: st.paused.unwrap_or(false),
            oom_killed: st.oom_killed.unwrap_or(false),
            exit_code: st.exit_code,
            status: st
                .status
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".into()),
        })
    }

    pub async fn stats_once(&self, container_id: &str) -> Result<ContainerStats, HadesError> {
        let mut s = self.docker.stats(
            container_id,
            Some(StatsOptions {
                stream: false,
                one_shot: false,
            }),
        );
        let stats = s
            .next()
            .await
            .ok_or_else(|| HadesError::Docker("no stats returned".into()))?
            .map_err(dockerr)?;

        let mem_used = stats.memory_stats.usage.unwrap_or(0) as f64 / (1024.0 * 1024.0);
        let mem_limit = stats.memory_stats.limit.unwrap_or(0) as f64 / (1024.0 * 1024.0);

        let cpu_delta = stats.cpu_stats.cpu_usage.total_usage as f64
            - stats.precpu_stats.cpu_usage.total_usage as f64;
        let sys_delta = stats.cpu_stats.system_cpu_usage.unwrap_or(0) as f64
            - stats.precpu_stats.system_cpu_usage.unwrap_or(0) as f64;
        let ncpu = stats.cpu_stats.online_cpus.unwrap_or(1) as f64;
        let cpu_pct = if sys_delta > 0.0 {
            (cpu_delta / sys_delta) * ncpu * 100.0
        } else {
            0.0
        };

        Ok(ContainerStats {
            memory_used_mb: mem_used,
            memory_limit_mb: mem_limit,
            cpu_pct,
        })
    }

    /// Stream container logs as plain text lines.
    pub fn logs(
        &self,
        container_id: &str,
        follow: bool,
        tail_lines: u32,
    ) -> impl Stream<Item = Result<String, HadesError>> {
        self.docker
            .logs(
                container_id,
                Some(LogsOptions::<String> {
                    follow,
                    stdout: true,
                    stderr: true,
                    tail: tail_lines.to_string(),
                    ..Default::default()
                }),
            )
            .map_ok(|out| match out {
                LogOutput::StdOut { message }
                | LogOutput::StdErr { message }
                | LogOutput::Console { message } => {
                    String::from_utf8_lossy(&message).into_owned()
                }
                LogOutput::StdIn { .. } => String::new(),
            })
            .map_err(dockerr)
    }

    /// Every container carrying the hades label, running or not — the
    /// reaper's view of the world.
    pub async fn list_labeled(&self) -> Result<Vec<LabeledContainer>, HadesError> {
        let mut filters = HashMap::new();
        filters.insert("label".to_string(), vec![LABEL_APP.to_string()]);
        let list = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(dockerr)?;
        Ok(list
            .into_iter()
            .map(|c| LabeledContainer {
                id: c.id.unwrap_or_default(),
                app: c
                    .labels
                    .as_ref()
                    .and_then(|l| l.get(LABEL_APP).cloned())
                    .unwrap_or_default(),
                name: c
                    .names
                    .unwrap_or_default()
                    .first()
                    .cloned()
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_string(),
                state: c.state.unwrap_or_default(),
            })
            .collect())
    }
}
