//! The policy engine: a pure function from (event, app states) to actions.
//! All the "what should the host do about it" decisions live here so they
//! can be unit-tested without Docker, notifications, or clocks.

use hades_api::types::AppState;
use hades_core::events::{HostEvent, PauseReason, PressureLevel};
use hades_core::spec::OnBattery;
use hades_core::Priority;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Pause { app: String, reason: PauseReason },
    Resume { app: String },
}

/// The slice of app state the policy needs.
#[derive(Debug, Clone)]
pub struct AppView {
    pub name: String,
    pub priority: Priority,
    pub state: AppState,
    pub paused_reason: Option<PauseReason>,
    pub on_battery: OnBattery,
}

pub fn decide(event: &HostEvent, apps: &[AppView]) -> Vec<Action> {
    match event {
        // Critical pressure: shed lowest priority first; never touch
        // `critical` apps. Deterministic order: priority, then name.
        HostEvent::MemoryPressure {
            level: PressureLevel::Critical,
            ..
        } => {
            let mut candidates: Vec<&AppView> = apps
                .iter()
                .filter(|a| {
                    a.state == AppState::Running && a.priority != Priority::Critical
                })
                .collect();
            candidates.sort_by(|a, b| a.priority.cmp(&b.priority).then(a.name.cmp(&b.name)));
            candidates
                .into_iter()
                .map(|a| Action::Pause {
                    app: a.name.clone(),
                    reason: PauseReason::MemoryPressure,
                })
                .collect()
        }

        // Pressure cleared: resume only what *we* paused for pressure.
        HostEvent::MemoryPressure {
            level: PressureLevel::Normal,
            ..
        } => apps
            .iter()
            .filter(|a| {
                a.state == AppState::Paused
                    && a.paused_reason == Some(PauseReason::MemoryPressure)
            })
            .map(|a| Action::Resume { app: a.name.clone() })
            .collect(),

        // On battery: opt-in per-app pause.
        HostEvent::OnBattery => apps
            .iter()
            .filter(|a| a.state == AppState::Running && a.on_battery == OnBattery::Pause)
            .map(|a| Action::Pause {
                app: a.name.clone(),
                reason: PauseReason::OnBattery,
            })
            .collect(),

        // Back on AC: resume only battery-paused apps.
        HostEvent::OnAc => apps
            .iter()
            .filter(|a| {
                a.state == AppState::Paused && a.paused_reason == Some(PauseReason::OnBattery)
            })
            .map(|a| Action::Resume { app: a.name.clone() })
            .collect(),

        _ => Vec::new(),
    }
}

/// Crash-loop rule: N OOM kills within the window stops further restarts.
pub const CRASH_LOOP_OOMS: usize = 3;
pub const CRASH_LOOP_WINDOW_SECS: i64 = 600;

pub fn is_crash_loop(oom_times: &[chrono::DateTime<chrono::Utc>]) -> bool {
    if oom_times.len() < CRASH_LOOP_OOMS {
        return false;
    }
    let now = chrono::Utc::now();
    oom_times
        .iter()
        .filter(|t| (now - **t).num_seconds() <= CRASH_LOOP_WINDOW_SECS)
        .count()
        >= CRASH_LOOP_OOMS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str, prio: Priority, state: AppState) -> AppView {
        AppView {
            name: name.into(),
            priority: prio,
            state,
            paused_reason: None,
            on_battery: OnBattery::Run,
        }
    }

    #[test]
    fn critical_pressure_sheds_by_priority_never_critical() {
        let apps = vec![
            app("api", Priority::Critical, AppState::Running),
            app("worker", Priority::Normal, AppState::Running),
            app("batch", Priority::Low, AppState::Running),
        ];
        let actions = decide(
            &HostEvent::MemoryPressure {
                level: PressureLevel::Critical,
                available_mb: 100,
            },
            &apps,
        );
        assert_eq!(
            actions,
            vec![
                Action::Pause {
                    app: "batch".into(),
                    reason: PauseReason::MemoryPressure
                },
                Action::Pause {
                    app: "worker".into(),
                    reason: PauseReason::MemoryPressure
                },
            ]
        );
    }

    #[test]
    fn pressure_clear_resumes_only_pressure_paused() {
        let mut a = app("batch", Priority::Low, AppState::Paused);
        a.paused_reason = Some(PauseReason::MemoryPressure);
        let mut b = app("manual", Priority::Low, AppState::Paused);
        b.paused_reason = Some(PauseReason::Manual);
        let actions = decide(
            &HostEvent::MemoryPressure {
                level: PressureLevel::Normal,
                available_mb: 4096,
            },
            &[a, b],
        );
        assert_eq!(actions, vec![Action::Resume { app: "batch".into() }]);
    }

    #[test]
    fn battery_policy_is_opt_in_and_symmetric() {
        let mut sleepy = app("sleepy", Priority::Normal, AppState::Running);
        sleepy.on_battery = OnBattery::Pause;
        let stay = app("stay", Priority::Normal, AppState::Running);

        let on_batt = decide(&HostEvent::OnBattery, &[sleepy.clone(), stay.clone()]);
        assert_eq!(
            on_batt,
            vec![Action::Pause {
                app: "sleepy".into(),
                reason: PauseReason::OnBattery
            }]
        );

        sleepy.state = AppState::Paused;
        sleepy.paused_reason = Some(PauseReason::OnBattery);
        let on_ac = decide(&HostEvent::OnAc, &[sleepy, stay]);
        assert_eq!(on_ac, vec![Action::Resume { app: "sleepy".into() }]);
    }

    #[test]
    fn crash_loop_detection_windows() {
        let now = chrono::Utc::now();
        let recent: Vec<_> = (0..3)
            .map(|i| now - chrono::Duration::seconds(i * 60))
            .collect();
        assert!(is_crash_loop(&recent));
        let stale: Vec<_> = (0..3)
            .map(|i| now - chrono::Duration::seconds(3600 + i * 60))
            .collect();
        assert!(!is_crash_loop(&stale));
    }
}
