use hades_core::error::ErrorBody;
use hades_core::{AppSpec, EventEnvelope, HadesError};
use reqwest::multipart;

use crate::types::*;

/// A typed error plus the verbatim wire body, so callers (the CLI's --json
/// path especially) can pass through server-side detail like the resource
/// ledger on an admission rejection.
pub struct ApiError {
    pub error: HadesError,
    pub body: ErrorBody,
}

impl From<HadesError> for ApiError {
    fn from(error: HadesError) -> Self {
        let body = ErrorBody::new(&error);
        Self { error, body }
    }
}

impl From<ErrorBody> for ApiError {
    fn from(body: ErrorBody) -> Self {
        Self {
            error: body.to_error(),
            body,
        }
    }
}

/// Client for the daemon API — local loopback or a remote host through its
/// control tunnel. Every request except /health carries the bearer token.
pub struct DaemonClient {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl DaemonClient {
    pub fn new(api_port: u16) -> Self {
        Self::for_host(&format!("127.0.0.1:{api_port}"), None)
    }

    /// `host` may be `127.0.0.1:8786`, `https://x.trycloudflare.com`, or any
    /// http(s) base URL.
    pub fn for_host(host: &str, token: Option<String>) -> Self {
        let base = if host.starts_with("http://") || host.starts_with("https://") {
            host.trim_end_matches('/').to_string()
        } else {
            format!("http://{host}")
        };
        Self {
            base,
            token,
            http: reqwest::Client::new(),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    async fn check<T: serde::de::DeserializeOwned>(
        resp: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<T, ApiError> {
        let resp =
            resp.map_err(|e| ApiError::from(HadesError::DaemonUnreachable(e.to_string())))?;
        let status = resp.status();
        if status.is_success() {
            resp.json::<T>().await.map_err(|e| {
                ApiError::from(HadesError::Other(format!("bad response from daemon: {e}")))
            })
        } else {
            let body = resp.text().await.unwrap_or_default();
            match serde_json::from_str::<ErrorBody>(&body) {
                Ok(eb) => Err(ApiError::from(eb)),
                Err(_) => Err(ApiError::from(HadesError::Other(format!(
                    "daemon returned {status}: {body}"
                )))),
            }
        }
    }

    pub async fn health(&self) -> Result<HealthResponse, ApiError> {
        Self::check(self.http.get(format!("{}/health", self.base)).send().await).await
    }

    /// Deploy (upsert). `context_tar_gz` is the gzipped build-context tarball
    /// when the spec has a [build] section. `device` pins fleet placement
    /// ("local" forces the hub itself; None lets the hub choose).
    pub async fn deploy(
        &self,
        spec: &AppSpec,
        context_tar_gz: Option<Vec<u8>>,
        device: Option<&str>,
    ) -> Result<DeployResponse, ApiError> {
        let mut form = multipart::Form::new().text(
            "spec",
            serde_json::to_string(spec).expect("spec serializes"),
        );
        if let Some(bytes) = context_tar_gz {
            form = form.part(
                "context",
                multipart::Part::bytes(bytes)
                    .file_name("context.tar.gz")
                    .mime_str("application/gzip")
                    .expect("static mime"),
            );
        }
        let mut url = format!("{}/apps", self.base);
        if let Some(d) = device {
            url.push_str(&format!("?device={d}"));
        }
        Self::check(
            self.auth(self.http.post(url))
                .multipart(form)
                // builds can be slow; let the daemon decide when to give up
                .timeout(std::time::Duration::from_secs(600))
                .send()
                .await,
        )
        .await
    }

    /// Deploy with a streamed NDJSON response (build logs + a final
    /// {"result"|"error"} line). The streaming keeps the connection alive
    /// through a multi-minute build, so a remote build doesn't hit the ~100s
    /// tunnel-edge timeout. Used for spreads to a device.
    pub async fn deploy_streamed(
        &self,
        spec: &AppSpec,
        context_tar_gz: Option<Vec<u8>>,
        device: Option<&str>,
    ) -> Result<DeployResponse, ApiError> {
        let mut form = multipart::Form::new().text(
            "spec",
            serde_json::to_string(spec).expect("spec serializes"),
        );
        if let Some(bytes) = context_tar_gz {
            form = form.part(
                "context",
                multipart::Part::bytes(bytes)
                    .file_name("context.tar.gz")
                    .mime_str("application/gzip")
                    .expect("static mime"),
            );
        }
        let dev = device.unwrap_or("");
        let url = format!("{}/apps?stream=true&device={dev}", self.base);
        let resp = self
            .auth(self.http.post(url))
            .multipart(form)
            .timeout(std::time::Duration::from_secs(900))
            .send()
            .await
            .map_err(|e| ApiError::from(HadesError::DaemonUnreachable(e.to_string())))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(serde_json::from_str::<ErrorBody>(&body)
                .map(ApiError::from)
                .unwrap_or_else(|_| ApiError::from(HadesError::Other(body))));
        }
        // the body streams while the build runs; reading it to completion waits
        // for the final result line
        let text = resp
            .text()
            .await
            .map_err(|e| ApiError::from(HadesError::Other(e.to_string())))?;
        for line in text.lines().rev() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(r) = v.get("result") {
                return serde_json::from_value::<DeployResponse>(r.clone())
                    .map_err(|e| ApiError::from(HadesError::Other(e.to_string())));
            }
            if let Some(e) = v.get("error") {
                return Err(serde_json::from_value::<ErrorBody>(e.clone())
                    .map(ApiError::from)
                    .unwrap_or_else(|_| ApiError::from(HadesError::Other(e.to_string()))));
            }
        }
        Err(ApiError::from(HadesError::Other(
            "deploy stream ended without a result".into(),
        )))
    }

    pub async fn join(&self, req: &JoinRequest) -> Result<FleetView, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/fleet/devices", self.base)))
                .json(req)
                .send()
                .await,
        )
        .await
    }

    /// Run the networking self-test (`scope` = "fleet", "local", or a device).
    pub async fn run_test(&self, scope: &str) -> Result<TestReport, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/test?scope={scope}", self.base)))
                .timeout(std::time::Duration::from_secs(180))
                .send()
                .await,
        )
        .await
    }

    pub async fn fleet(&self) -> Result<FleetView, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/fleet", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn fleet_remove(&self, name: &str) -> Result<FleetView, ApiError> {
        Self::check(
            self.auth(
                self.http
                    .delete(format!("{}/fleet/devices/{name}", self.base)),
            )
            .send()
            .await,
        )
        .await
    }

    /// Run another instance of a hub-hosted app on a fleet device.
    pub async fn app_spread(&self, app: &str, device: &str) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/apps/{app}/spread", self.base)))
                .json(&serde_json::json!({ "device": device }))
                .timeout(std::time::Duration::from_secs(600))
                .send()
                .await,
        )
        .await
    }

    /// Stop running an app on a spread device (`all` = every device).
    pub async fn app_gather(&self, app: &str, device: &str) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(
                self.http
                    .delete(format!("{}/apps/{app}/spread/{device}", self.base)),
            )
            .send()
            .await,
        )
        .await
    }

    pub async fn list_apps(&self) -> Result<Vec<AppInfo>, ApiError> {
        Self::check(self.auth(self.http.get(format!("{}/apps", self.base))).send().await).await
    }

    pub async fn get_app(&self, name: &str) -> Result<AppInfo, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/apps/{name}", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn app_stats(&self, name: &str) -> Result<AppStats, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/apps/{name}/stats", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn destroy(&self, name: &str) -> Result<AppInfo, ApiError> {
        Self::check(
            self.auth(self.http.delete(format!("{}/apps/{name}", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn pause(&self, name: &str) -> Result<AppInfo, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/apps/{name}/pause", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn resume(&self, name: &str) -> Result<AppInfo, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/apps/{name}/resume", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn host_status(&self) -> Result<HostStatus, ApiError> {
        Self::check(self.auth(self.http.get(format!("{}/host", self.base))).send().await).await
    }

    pub async fn battery(&self) -> Result<BatteryReport, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/host/battery", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn uptime(&self, since_days: f64) -> Result<UptimeReport, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/host/uptime?days={since_days}", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn ps(&self) -> Result<PsReport, ApiError> {
        Self::check(self.auth(self.http.get(format!("{}/host/ps", self.base))).send().await).await
    }

    pub async fn secrets_update(
        &self,
        app: &str,
        update: &SecretsUpdate,
    ) -> Result<SecretsView, ApiError> {
        Self::check(
            self.auth(self.http.put(format!("{}/apps/{app}/secrets", self.base)))
                .json(update)
                .send()
                .await,
        )
        .await
    }

    pub async fn secrets_list(&self, app: &str) -> Result<SecretsView, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/apps/{app}/secrets", self.base)))
                .send()
                .await,
        )
        .await
    }

    /// Download this host's source tarball (for hub→device updates).
    pub async fn fetch_src(&self) -> Result<Vec<u8>, ApiError> {
        let resp = self
            .auth(self.http.get(format!("{}/host/src", self.base)))
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| ApiError::from(HadesError::DaemonUnreachable(e.to_string())))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(serde_json::from_str::<ErrorBody>(&body)
                .map(ApiError::from)
                .unwrap_or_else(|_| ApiError::from(HadesError::Other(body))));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| ApiError::from(HadesError::Other(e.to_string())))
    }

    pub async fn domain_claim(
        &self,
        app: &str,
        name: &str,
        coordinator_url: Option<&str>,
        coordinator_secret: Option<&str>,
    ) -> Result<DomainClaim, ApiError> {
        let mut body = serde_json::json!({ "app": app, "name": name });
        if let Some(u) = coordinator_url {
            body["coordinator_url"] = u.into();
        }
        if let Some(sec) = coordinator_secret {
            body["coordinator_secret"] = sec.into();
        }
        Self::check(
            self.auth(self.http.post(format!("{}/domain/claim", self.base)))
                .json(&body)
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await,
        )
        .await
    }

    pub async fn domain_release(&self, app: &str) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.delete(format!("{}/domain/claim/{app}", self.base)))
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await,
        )
        .await
    }

    pub async fn domain_list(&self) -> Result<DomainClaimList, ApiError> {
        Self::check(
            self.auth(self.http.get(format!("{}/domain/claims", self.base)))
                .send()
                .await,
        )
        .await
    }

    /// Open an ssh tunnel on this host (returns {url, user_hint}).
    pub async fn host_ssh(&self) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/host/ssh", self.base)))
                .timeout(std::time::Duration::from_secs(90))
                .send()
                .await,
        )
        .await
    }

    /// Ask a joined device (via the hub) to open its ssh tunnel.
    pub async fn fleet_ssh(&self, device: &str) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(
                self.http
                    .post(format!("{}/fleet/devices/{device}/ssh", self.base)),
            )
            .timeout(std::time::Duration::from_secs(90))
            .send()
            .await,
        )
        .await
    }

    /// Hub-side fan-out: every joined device self-updates from this hub.
    pub async fn fleet_update(&self) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/fleet/update", self.base)))
                .send()
                .await,
        )
        .await
    }

    /// Claim a stable control hostname for this host (its own coordinator).
    pub async fn host_stabilize(&self, name: &str) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/host/stabilize", self.base)))
                .json(&serde_json::json!({ "name": name }))
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await,
        )
        .await
    }

    /// Hub side: mint a stable control hostname + connector token for a device.
    pub async fn fleet_control_token(
        &self,
        name: &str,
        api_port: u16,
    ) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/fleet/control-token", self.base)))
                .json(&serde_json::json!({ "name": name, "api_port": api_port }))
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await,
        )
        .await
    }

    /// Device side: adopt a hub-minted control hostname locally.
    pub async fn host_adopt_control(
        &self,
        name: &str,
        hostname: &str,
        connector_token: &str,
    ) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/host/adopt-control", self.base)))
                .json(&serde_json::json!({
                    "name": name,
                    "hostname": hostname,
                    "connector_token": connector_token,
                }))
                .timeout(std::time::Duration::from_secs(60))
                .send()
                .await,
        )
        .await
    }

    /// Ask a host to update itself (detached; it rebuilds and restarts).
    pub async fn trigger_update(&self) -> Result<serde_json::Value, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/host/update", self.base)))
                .send()
                .await,
        )
        .await
    }

    pub async fn notify_test(&self) -> Result<NotifyTestResponse, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/notify/test", self.base)))
                .send()
                .await,
        )
        .await
    }

    /// Raw NDJSON line stream of app logs (`follow` keeps it open).
    pub async fn logs(
        &self,
        name: &str,
        follow: bool,
    ) -> Result<reqwest::Response, ApiError> {
        let resp = self
            .auth(self.http.get(format!("{}/apps/{name}/logs?follow={follow}", self.base)))
            .send()
            .await
            .map_err(|e| ApiError::from(HadesError::DaemonUnreachable(e.to_string())))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(serde_json::from_str::<ErrorBody>(&body)
                .map(ApiError::from)
                .unwrap_or_else(|_| ApiError::from(HadesError::Other(body))));
        }
        Ok(resp)
    }

    /// Raw NDJSON stream of `EventEnvelope`s.
    pub async fn events(
        &self,
        follow: bool,
        since_days: Option<f64>,
    ) -> Result<reqwest::Response, ApiError> {
        let mut url = format!("{}/events?follow={follow}", self.base);
        if let Some(d) = since_days {
            url.push_str(&format!("&days={d}"));
        }
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| ApiError::from(HadesError::DaemonUnreachable(e.to_string())))?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(serde_json::from_str::<ErrorBody>(&body)
                .map(ApiError::from)
                .unwrap_or_else(|_| ApiError::from(HadesError::Other(body))));
        }
        Ok(resp)
    }
}

impl DaemonClient {
    /// Fetch a host's recent events (parsed), for the fleet dashboard.
    pub async fn recent_events(
        &self,
        days: f64,
    ) -> Result<Vec<hades_core::EventEnvelope>, ApiError> {
        let resp = self.events(false, Some(days)).await?;
        let body = resp
            .text()
            .await
            .map_err(|e| ApiError::from(hades_core::HadesError::Other(e.to_string())))?;
        Ok(body
            .lines()
            .filter_map(|l| serde_json::from_str::<hades_core::EventEnvelope>(l).ok())
            .collect())
    }
}

/// Parse one NDJSON line into an event envelope (helper for CLI rendering).
pub fn parse_event_line(line: &str) -> Option<EventEnvelope> {
    serde_json::from_str(line).ok()
}
