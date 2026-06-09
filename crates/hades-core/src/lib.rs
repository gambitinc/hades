//! Shared vocabulary for every Hades crate: app specs, the host event
//! taxonomy, error codes with stable exit-code mapping, and canonical paths.

pub mod config;
pub mod error;
pub mod events;
pub mod paths;
pub mod spec;

pub use config::{HadesConfig, NotifyConfig};
pub use error::HadesError;
pub use events::{EventEnvelope, HostEvent, Severity};
pub use paths::HadesPaths;
pub use spec::{
    AppSpec, BuildSpec, HealthCheck, Manifest, PowerPolicy, Priority, Resources,
};
