use hades_core::events::{HostEvent, Severity};
use hades_core::NotifyConfig;

#[derive(Debug, Clone)]
pub struct Notification {
    pub title: String,
    pub body: String,
    pub severity: Severity,
}

impl Notification {
    pub fn from_event(event: &HostEvent) -> Self {
        Self {
            title: "hades".into(),
            body: event.summary(),
            severity: event.severity(),
        }
    }
}

/// Config-driven fan-out. Severity policy: `Info` events stay in the event
/// ledger only; `Default`+ go to ntfy; `Default`+ also hit the local macOS
/// notification center when enabled.
#[derive(Clone)]
pub struct Notifiers {
    ntfy_topic: Option<String>,
    mac: bool,
    http: reqwest::Client,
}

impl Notifiers {
    pub fn from_config(cfg: &NotifyConfig) -> Self {
        Self {
            ntfy_topic: cfg.ntfy_topic.clone(),
            mac: cfg.mac_notifications,
            http: reqwest::Client::new(),
        }
    }

    pub fn channels(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(t) = &self.ntfy_topic {
            out.push(format!("ntfy.sh/{t}"));
        }
        if self.mac {
            out.push("macos-notification-center".into());
        }
        out
    }

    /// Returns the channels actually delivered to.
    pub async fn send(&self, n: &Notification) -> Vec<String> {
        let mut delivered = Vec::new();
        if n.severity < Severity::Default {
            return delivered;
        }

        if let Some(topic) = &self.ntfy_topic {
            let priority = match n.severity {
                Severity::Urgent => "urgent",
                _ => "default",
            };
            let res = self
                .http
                .post(format!("https://ntfy.sh/{topic}"))
                .header("Title", n.title.clone())
                .header("Priority", priority)
                .header("Tags", "hades")
                .body(n.body.clone())
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => delivered.push(format!("ntfy.sh/{topic}")),
                Ok(r) => tracing::warn!("ntfy push failed: {}", r.status()),
                Err(e) => tracing::warn!("ntfy push failed: {e}"),
            }
        }

        if self.mac {
            let script = format!(
                "display notification \"{}\" with title \"{}\"",
                n.body.replace('"', "'"),
                n.title.replace('"', "'")
            );
            let ok = tokio::process::Command::new("osascript")
                .args(["-e", &script])
                .output()
                .await
                .map(|o| o.status.success())
                .unwrap_or(false);
            if ok {
                delivered.push("macos-notification-center".into());
            }
        }

        delivered
    }

    /// Notify for an event, honoring its severity.
    pub async fn notify_event(&self, event: &HostEvent) -> Vec<String> {
        self.send(&Notification::from_event(event)).await
    }
}
