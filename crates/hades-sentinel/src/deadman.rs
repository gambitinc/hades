//! The dead-man's switch: hadesd pings a heartbeat URL every interval; when
//! pings stop, the third-party service (healthchecks.io or compatible)
//! pushes the "your host is down" priority alert. This is the one feature
//! that needs outside help — a dead host can't report its own death.

#[derive(Clone)]
pub struct DeadMansSwitch {
    url: Option<String>,
    http: reqwest::Client,
}

impl DeadMansSwitch {
    pub fn new(url: Option<String>) -> Self {
        Self {
            url,
            http: reqwest::Client::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.url.is_some()
    }

    pub async fn ping(&self) {
        let Some(url) = &self.url else { return };
        match self
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => tracing::warn!("dead-man ping returned {}", r.status()),
            Err(e) => tracing::warn!("dead-man ping failed: {e}"),
        }
    }
}
