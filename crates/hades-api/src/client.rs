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

    pub async fn join(&self, req: &JoinRequest) -> Result<FleetView, ApiError> {
        Self::check(
            self.auth(self.http.post(format!("{}/fleet/devices", self.base)))
                .json(req)
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

/// Parse one NDJSON line into an event envelope (helper for CLI rendering).
pub fn parse_event_line(line: &str) -> Option<EventEnvelope> {
    serde_json::from_str(line).ok()
}
