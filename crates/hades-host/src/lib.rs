//! The host-bootstrap subsystem: turning *someone else's* computer into a
//! trustworthy cloud host. Probes (power, battery, sleep history, disk),
//! the doctor (preflight gate — deploys are refused while red), launchd
//! supervision, and the guided `hades host init` flow.

pub mod doctor;
pub mod init;
pub mod launchd;
pub mod probe;

pub use doctor::{run_doctor, Check, DoctorReport};
pub use launchd::{LaunchdManager, ServiceManager, DAEMON_LABEL};
pub use probe::{BatteryHealth, HostProbe, MacProbe, PowerSnapshot};
