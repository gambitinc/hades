//! Per-app secrets, fly.io style: set via the CLI, stored encrypted on the
//! host that runs the app, injected as env at container start. Never in the
//! manifest, never in git, never in the build context, never serialized
//! back out of the API.
//!
//! At-rest encryption uses XChaCha20-Poly1305 with a per-host master key
//! kept in the macOS Keychain (`security` CLI — no extra daemons, unlocked
//! with the user's login session, in keeping with "your hardware is
//! enough"). If the Keychain is unavailable the store still works but
//! plainly says so in its files and the logs.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{AeadCore, XChaCha20Poly1305};
use hades_core::HadesError;
use serde::{Deserialize, Serialize};

const KEYCHAIN_SERVICE: &str = "hades-master-key";

#[derive(Serialize, Deserialize)]
struct SecretFile {
    encrypted: bool,
    /// hex nonce + hex ciphertext when encrypted; plain JSON map otherwise
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nonce: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plain: Option<BTreeMap<String, String>>,
}

pub struct SecretStore {
    dir: PathBuf,
    key: Option<[u8; 32]>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

/// Fetch (or mint) the per-host master key from the login Keychain.
fn keychain_master_key() -> Option<[u8; 32]> {
    let find = std::process::Command::new("security")
        .args(["find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"])
        .output()
        .ok()?;
    if find.status.success() {
        let s = String::from_utf8_lossy(&find.stdout).trim().to_string();
        let bytes = unhex(&s)?;
        return bytes.try_into().ok();
    }
    // mint one
    let mut key = [0u8; 32];
    use rand::RngCore;
    rand::rngs::OsRng.fill_bytes(&mut key);
    let user = std::env::var("USER").unwrap_or_else(|_| "hades".into());
    let add = std::process::Command::new("security")
        .args([
            "add-generic-password",
            "-a",
            &user,
            "-s",
            KEYCHAIN_SERVICE,
            "-w",
            &hex(&key),
            "-U",
        ])
        .output()
        .ok()?;
    add.status.success().then_some(key)
}

impl SecretStore {
    pub fn open(root: &std::path::Path) -> Self {
        let dir = root.join("secrets");
        let _ = std::fs::create_dir_all(&dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        let key = keychain_master_key();
        if key.is_none() {
            tracing::warn!(
                "keychain unavailable — secrets will be stored with file permissions only"
            );
        }
        Self { dir, key }
    }

    pub fn encrypted(&self) -> bool {
        self.key.is_some()
    }

    fn path(&self, app: &str) -> PathBuf {
        self.dir.join(format!("{app}.json"))
    }

    pub fn load(&self, app: &str) -> BTreeMap<String, String> {
        let Ok(raw) = std::fs::read_to_string(self.path(app)) else {
            return BTreeMap::new();
        };
        let Ok(file) = serde_json::from_str::<SecretFile>(&raw) else {
            return BTreeMap::new();
        };
        if !file.encrypted {
            return file.plain.unwrap_or_default();
        }
        let (Some(key), Some(nonce), Some(data)) = (self.key, file.nonce, file.data) else {
            tracing::warn!(app, "cannot decrypt secrets (no keychain key)");
            return BTreeMap::new();
        };
        let (Some(nonce), Some(ct)) = (unhex(&nonce), unhex(&data)) else {
            return BTreeMap::new();
        };
        let cipher = XChaCha20Poly1305::new((&key).into());
        match cipher.decrypt(nonce.as_slice().into(), ct.as_slice()) {
            Ok(pt) => serde_json::from_slice(&pt).unwrap_or_default(),
            Err(_) => {
                tracing::warn!(app, "secret store failed to decrypt — wrong master key?");
                BTreeMap::new()
            }
        }
    }

    pub fn save(&self, app: &str, map: &BTreeMap<String, String>) -> Result<(), HadesError> {
        let file = match self.key {
            Some(key) => {
                let cipher = XChaCha20Poly1305::new((&key).into());
                let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
                let pt = serde_json::to_vec(map).expect("map serializes");
                let ct = cipher
                    .encrypt(&nonce, pt.as_slice())
                    .map_err(|_| HadesError::Other("secret encryption failed".into()))?;
                SecretFile {
                    encrypted: true,
                    nonce: Some(hex(&nonce)),
                    data: Some(hex(&ct)),
                    plain: None,
                }
            }
            None => SecretFile {
                encrypted: false,
                nonce: None,
                data: None,
                plain: Some(map.clone()),
            },
        };
        let path = self.path(app);
        let mut f = std::fs::File::create(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        f.write_all(serde_json::to_string_pretty(&file).unwrap().as_bytes())?;
        Ok(())
    }

    pub fn remove(&self, app: &str) {
        let _ = std::fs::remove_file(self.path(app));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_without_keychain() {
        let dir = std::env::temp_dir().join(format!("hades-sec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = SecretStore {
            dir: dir.clone(),
            key: Some([7u8; 32]),
        };
        let mut m = BTreeMap::new();
        m.insert("API_KEY".to_string(), "sk_live_123".to_string());
        store.save("shop", &m).unwrap();

        // raw file must not contain the plaintext
        let raw = std::fs::read_to_string(dir.join("shop.json")).unwrap();
        assert!(!raw.contains("sk_live_123"));

        assert_eq!(store.load("shop"), m);

        // wrong key reads as empty, not as garbage
        let other = SecretStore { dir: dir.clone(), key: Some([9u8; 32]) };
        assert!(other.load("shop").is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }
}
