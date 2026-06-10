//! The thin Cloudflare API client the coordinator needs: create/delete a
//! remotely-managed tunnel, push its ingress config, and create/delete the
//! DNS record that points a name at it. One operator-held API token drives
//! all of it; end users never see Cloudflare.

use serde_json::{json, Value};

pub struct Cloudflare {
    token: String,
    account_id: String,
    zone_id: String,
    http: reqwest::Client,
}

const API: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug)]
pub struct CfError(pub String);
impl std::fmt::Display for CfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
type R<T> = Result<T, CfError>;

fn ok(v: Value) -> R<Value> {
    if v["success"].as_bool().unwrap_or(false) {
        Ok(v["result"].clone())
    } else {
        Err(CfError(
            v["errors"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|e| e["message"].as_str().unwrap_or("?").to_string())
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .unwrap_or_else(|| "unknown cloudflare error".into()),
        ))
    }
}

impl Cloudflare {
    pub fn new(token: String, account_id: String, zone_id: String) -> Self {
        Self {
            token,
            account_id,
            zone_id,
            http: reqwest::Client::new(),
        }
    }

    /// Resolve a zone id from the parent domain (so the operator only has to
    /// supply token + account + domain).
    pub async fn resolve_zone(
        token: &str,
        domain: &str,
    ) -> R<String> {
        let http = reqwest::Client::new();
        let v: Value = http
            .get(format!("{API}/zones?name={domain}"))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| CfError(e.to_string()))?
            .json()
            .await
            .map_err(|e| CfError(e.to_string()))?;
        let res = ok(v)?;
        res.as_array()
            .and_then(|a| a.first())
            .and_then(|z| z["id"].as_str().map(String::from))
            .ok_or_else(|| CfError(format!("zone {domain} not found on this account")))
    }

    async fn post(&self, path: &str, body: Value) -> R<Value> {
        let v: Value = self
            .http
            .post(format!("{API}{path}"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| CfError(e.to_string()))?
            .json()
            .await
            .map_err(|e| CfError(e.to_string()))?;
        ok(v)
    }

    async fn put(&self, path: &str, body: Value) -> R<Value> {
        let v: Value = self
            .http
            .put(format!("{API}{path}"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| CfError(e.to_string()))?
            .json()
            .await
            .map_err(|e| CfError(e.to_string()))?;
        ok(v)
    }

    async fn get(&self, path: &str) -> R<Value> {
        let v: Value = self
            .http
            .get(format!("{API}{path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| CfError(e.to_string()))?
            .json()
            .await
            .map_err(|e| CfError(e.to_string()))?;
        ok(v)
    }

    async fn delete(&self, path: &str) -> R<()> {
        let v: Value = self
            .http
            .delete(format!("{API}{path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| CfError(e.to_string()))?
            .json()
            .await
            .map_err(|e| CfError(e.to_string()))?;
        ok(v).map(|_| ())
    }

    // ----- tunnels -----

    /// Create a remotely-managed tunnel; returns (tunnel_id, connector_token).
    pub async fn create_tunnel(&self, name: &str) -> R<(String, String)> {
        let res = self
            .post(
                &format!("/accounts/{}/cfd_tunnel", self.account_id),
                json!({ "name": name, "config_src": "cloudflare" }),
            )
            .await?;
        let id = res["id"]
            .as_str()
            .ok_or_else(|| CfError("tunnel create returned no id".into()))?
            .to_string();
        // token is sometimes inline, otherwise fetched
        let token = match res["token"].as_str() {
            Some(t) => t.to_string(),
            None => {
                let t = self
                    .get(&format!("/accounts/{}/cfd_tunnel/{id}/token", self.account_id))
                    .await?;
                t.as_str()
                    .ok_or_else(|| CfError("no connector token".into()))?
                    .to_string()
            }
        };
        Ok((id, token))
    }

    /// Point a tunnel's ingress at the host's local proxy for one or more
    /// hostnames (apex claims carry both the bare domain and www).
    pub async fn set_ingress(
        &self,
        tunnel_id: &str,
        hostnames: &[String],
        service: &str,
    ) -> R<()> {
        let mut ingress: Vec<Value> = hostnames
            .iter()
            .map(|h| json!({ "hostname": h, "service": service }))
            .collect();
        ingress.push(json!({ "service": "http_status:404" }));
        self.put(
            &format!(
                "/accounts/{}/cfd_tunnel/{tunnel_id}/configurations",
                self.account_id
            ),
            json!({ "config": { "ingress": ingress } }),
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_tunnel(&self, tunnel_id: &str) -> R<()> {
        // a tunnel must have no active connections to delete; cleanup is
        // best-effort, so ignore the "has connections" race
        self.delete(&format!(
            "/accounts/{}/cfd_tunnel/{tunnel_id}",
            self.account_id
        ))
        .await
    }

    // ----- dns -----

    /// CNAME `<sub>` → `<tunnel_id>.cfargotunnel.com`, proxied.
    pub async fn create_dns(&self, sub: &str, tunnel_id: &str) -> R<String> {
        let res = self
            .post(
                &format!("/zones/{}/dns_records", self.zone_id),
                json!({
                    "type": "CNAME",
                    "name": sub,
                    "content": format!("{tunnel_id}.cfargotunnel.com"),
                    "proxied": true
                }),
            )
            .await?;
        Ok(res["id"].as_str().unwrap_or_default().to_string())
    }

    pub async fn find_dns(&self, fqdn: &str) -> R<Option<String>> {
        let res = self
            .get(&format!("/zones/{}/dns_records?name={fqdn}", self.zone_id))
            .await?;
        Ok(res
            .as_array()
            .and_then(|a| a.first())
            .and_then(|r| r["id"].as_str().map(String::from)))
    }

    pub async fn delete_dns(&self, record_id: &str) -> R<()> {
        self.delete(&format!("/zones/{}/dns_records/{record_id}", self.zone_id))
            .await
    }
}
