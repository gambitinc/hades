use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Shared error type. Every variant has a stable machine-readable `code()`
/// and an `exit_code()` the CLI maps onto, so agents can branch on failures
/// without parsing prose.
#[derive(Debug, Error)]
pub enum HadesError {
    #[error("invalid spec: {0}")]
    InvalidSpec(String),

    #[error("no Hades.toml found: {0}")]
    ManifestNotFound(String),

    #[error("app not found: {0}")]
    AppNotFound(String),

    #[error("deploy rejected: {0}")]
    AdmissionRejected(String),

    #[error("host not ready: {0}")]
    DoctorRed(String),

    #[error("docker error: {0}")]
    Docker(String),

    #[error("tunnel error: {0}")]
    Tunnel(String),

    #[error("daemon unreachable: {0}")]
    DaemonUnreachable(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl HadesError {
    pub fn code(&self) -> &'static str {
        match self {
            HadesError::InvalidSpec(_) => "invalid_spec",
            HadesError::ManifestNotFound(_) => "manifest_not_found",
            HadesError::AppNotFound(_) => "app_not_found",
            HadesError::AdmissionRejected(_) => "admission_rejected",
            HadesError::DoctorRed(_) => "doctor_red",
            HadesError::Docker(_) => "docker",
            HadesError::Tunnel(_) => "tunnel",
            HadesError::DaemonUnreachable(_) => "daemon_unreachable",
            HadesError::Unauthorized(_) => "unauthorized",
            HadesError::Io(_) => "io",
            HadesError::Other(_) => "other",
        }
    }

    /// Stable CLI exit codes: 0 ok, 1 generic, 2 doctor-red, 3 admission
    /// rejected, 4 not found, 5 daemon unreachable, 6 invalid spec/manifest,
    /// 7 unauthorized.
    pub fn exit_code(&self) -> i32 {
        match self {
            HadesError::DoctorRed(_) => 2,
            HadesError::AdmissionRejected(_) => 3,
            HadesError::AppNotFound(_) => 4,
            HadesError::DaemonUnreachable(_) => 5,
            HadesError::InvalidSpec(_) | HadesError::ManifestNotFound(_) => 6,
            HadesError::Unauthorized(_) => 7,
            _ => 1,
        }
    }
}

/// Wire shape for errors: `{ "error": { "code", "message", "detail" } }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ErrorInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInner {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl ErrorBody {
    pub fn new(err: &HadesError) -> Self {
        Self {
            error: ErrorInner {
                code: err.code().to_string(),
                message: err.to_string(),
                detail: None,
            },
        }
    }

    pub fn with_detail(err: &HadesError, detail: serde_json::Value) -> Self {
        Self {
            error: ErrorInner {
                code: err.code().to_string(),
                message: err.to_string(),
                detail: Some(detail),
            },
        }
    }

    /// Reconstruct a typed error from a wire body (CLI side).
    pub fn to_error(&self) -> HadesError {
        let msg = self.error.message.clone();
        match self.error.code.as_str() {
            "invalid_spec" => HadesError::InvalidSpec(msg),
            "manifest_not_found" => HadesError::ManifestNotFound(msg),
            "app_not_found" => HadesError::AppNotFound(msg),
            "admission_rejected" => HadesError::AdmissionRejected(msg),
            "doctor_red" => HadesError::DoctorRed(msg),
            "docker" => HadesError::Docker(msg),
            "tunnel" => HadesError::Tunnel(msg),
            "daemon_unreachable" => HadesError::DaemonUnreachable(msg),
            "unauthorized" => HadesError::Unauthorized(msg),
            _ => HadesError::Other(msg),
        }
    }
}
