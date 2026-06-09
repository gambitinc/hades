//! Uptime as a feature. The host can't tell you it's down — so: (1) a
//! dead-man's switch pings a third-party heartbeat URL and *their* infra
//! alerts your phone when pings stop; (2) everything the host *can* report
//! (back up, OOM, doctor red, new URLs) fans out through notifiers, with
//! ntfy.sh as the zero-account push channel; (3) a heartbeat file on disk
//! lets the daemon classify every downtime window after the fact.

pub mod deadman;
pub mod notify;
pub mod uptime;

pub use deadman::DeadMansSwitch;
pub use notify::{Notification, Notifiers};
pub use uptime::{DowntimeRecord, UptimeLedger};
