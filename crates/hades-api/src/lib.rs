//! Request/response types shared by `hadesd` (server) and `hades-cli`
//! (client), plus a thin reqwest client. The JSON shapes here are the
//! agent-facing contract — change them deliberately.

pub mod client;
pub mod types;

pub use client::{ApiError, DaemonClient};
pub use types::*;
