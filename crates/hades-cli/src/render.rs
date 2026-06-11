//! Human rendering. The contract everywhere: `--json` puts ONE final JSON
//! object on stdout (progress and decoration go to stderr); human mode
//! prints readable tables/lines. Agents pipe stdout; people read the rest.

use hades_api::types::*;
use hades_core::events::humanize_secs;
use hades_host::doctor::DoctorReport;

pub fn json<T: serde::Serialize>(value: &T) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

pub fn doctor(report: &DoctorReport) {
    for c in &report.checks {
        let mark = if c.ok {
            "\u{2713}"
        } else if c.warn {
            "!"
        } else {
            "\u{2717}"
        };
        println!("{mark} {:<12} {}", c.name, c.detail);
        if let Some(r) = &c.remedy {
            if !c.ok {
                println!("  {:<12} \u{21b3} {r}", "");
            }
        }
    }
    println!();
    if report.green {
        println!("doctor: GREEN, host is ready for deploys");
    } else {
        println!("doctor: RED, deploys are refused until the failures above are fixed");
    }
}

pub fn app_line(a: &AppInfo) {
    let url = a.url.as_deref().unwrap_or("-");
    println!(
        "{:<14} {:<8} {:<10} {:>2}/{:<2} {:>6}MB {:>4.1}cpu  {}  {}",
        a.name,
        a.device.as_deref().unwrap_or("local"),
        a.state.to_string(),
        a.replicas_running,
        a.replicas_desired,
        a.memory_mb,
        a.cpu,
        a.local_url,
        url
    );
}

pub fn apps(list: &[AppInfo]) {
    if list.is_empty() {
        println!("no apps deployed; `hades init` then `hades deploy` to get one running");
        return;
    }
    println!(
        "{:<14} {:<8} {:<10} {:<5} {:>8} {:>7}  LOCAL  PUBLIC",
        "NAME", "DEVICE", "STATE", "REPL", "MEM", "CPU"
    );
    for a in list {
        app_line(a);
    }
}

pub fn deploy(resp: &DeployResponse) {
    let a = &resp.app;
    eprintln!();
    println!(
        "{} deployed{}",
        a.name,
        if resp.replaced { " (replaced running version)" } else { "" }
    );
    println!("  local:  {}", a.local_url);
    match &a.url {
        Some(url) => println!("  public: {url}"),
        None => {
            if let Some(note) = &resp.tunnel_note {
                println!("  public: (none); {note}");
            } else {
                println!("  public: provisioning… run `hades url {}` in a few seconds", a.name);
            }
        }
    }
}

pub fn host_status(s: &HostStatus) {
    println!(
        "hadesd v{}, up since {}, doctor {}",
        s.daemon_version,
        s.started_at.format("%Y-%m-%d %H:%M UTC"),
        if s.doctor_green { "GREEN" } else { "RED" }
    );
    let l = &s.ledger;
    println!(
        "capacity: Docker VM {}MB / {} CPUs (Mac has {}MB; the VM is the real budget)",
        l.vm_memory_mb, l.vm_cpus, l.mac_memory_mb
    );
    println!(
        "memory:   {}MB allocated of {}MB allocatable ({}MB reserved, {}%)",
        l.allocated_mb, l.allocatable_mb, l.reserved_mb, l.reserve_pct
    );
    let power = if s.power.on_ac { "AC power" } else { "ON BATTERY" };
    let batt = s
        .power
        .battery_pct
        .map(|p| format!(", battery {p:.0}%"))
        .unwrap_or_default();
    let health = s
        .power
        .capacity_pct_of_design
        .map(|p| format!(" (health {p:.0}% of design)"))
        .unwrap_or_default();
    println!("power:    {power}{batt}{health}");
    if let Some(av) = s.availability_pct_7d {
        println!("uptime:   {av:.2}% availability over the last 7 days");
    }
    println!();
    apps(&s.apps);
}

pub fn battery(b: &BatteryReport) {
    println!(
        "battery report: {} samples over {:.1} days",
        b.sample_count, b.sampled_over_days
    );
    if let Some(c) = b.cycle_count {
        println!("  cycles:        {c}");
    }
    if let Some(p) = b.capacity_pct_of_design {
        println!("  capacity:      {p:.1}% of design");
    }
    if let Some(t) = b.capacity_trend_pct {
        println!("  trend:         {t:+.2} pct-points over the window");
    }
    if let Some(c) = b.cycles_per_week {
        println!("  cycle burn:    {c:.1}/week");
    }
    if let Some(d) = b.high_soc_dwell_pct {
        println!("  high-SoC dwell: {d:.0}% of plugged-in time at >=95% charge");
    }
    if let Some(t) = b.avg_temp_c {
        println!("  avg temp:      {t:.1}°C");
    }
    if let Some(s) = b.hades_cpu_share_pct {
        println!("  hades load:    {s:.0}% avg container CPU during window");
    }
    println!();
    for r in &b.recommendations {
        println!("  • {r}");
    }
}

pub fn uptime(u: &UptimeReport) {
    println!(
        "availability {:.2}% since {}",
        u.availability_pct,
        u.since.format("%Y-%m-%d %H:%M UTC")
    );
    if u.windows.is_empty() {
        println!("no downtime windows recorded");
        return;
    }
    println!("{:<22} {:<22} {:>9}  CAUSE", "START", "END", "DURATION");
    for w in &u.windows {
        println!(
            "{:<22} {:<22} {:>9}  {}",
            w.start.format("%Y-%m-%d %H:%M:%S"),
            w.end.format("%Y-%m-%d %H:%M:%S"),
            humanize_secs(w.secs),
            w.cause
        );
    }
}

pub fn ps(p: &PsReport) {
    println!(
        "{:<12} {:<14} {:<14} {:>9} {:>7}  DETAIL",
        "KIND", "APP", "ID", "RSS", "CPU"
    );
    for e in &p.entries {
        let kind = match e.kind {
            PsKind::Daemon => "daemon",
            PsKind::Container => "container",
            PsKind::Cloudflared => "cloudflared",
        };
        println!(
            "{:<12} {:<14} {:<14} {:>8}  {:>6}  {} [{}]",
            kind,
            e.app.as_deref().unwrap_or("-"),
            e.id,
            e.rss_mb.map(|m| format!("{m:.0}MB")).unwrap_or("-".into()),
            e.cpu_pct.map(|c| format!("{c:.1}%")).unwrap_or("-".into()),
            e.detail,
            e.state
        );
    }
}

pub fn fleet(v: &FleetView) {
    println!(
        "{:<22} {:<8} {:>9} {:>9} {:>9} {:<6} LAST SEEN",
        "MACHINE", "HEALTH", "LOAD", "EST CAP", "FREE", "APPS"
    );
    for d in &v.devices {
        let name = if d.is_self {
            format!("{} (this)", d.name)
        } else {
            d.name.clone()
        };
        let cap = d
            .capacity_req_per_sec
            .map(|c| format!("{} r/s", c.round() as u64))
            .unwrap_or_else(|| "—".into());
        println!(
            "{:<22} {:<8} {:>5} r/s {:>9} {:>7}MB {:<6} {}",
            name,
            if d.healthy { "green" } else { "red" },
            d.req_per_sec.round() as u64,
            cap,
            d.free_mb,
            d.apps,
            if d.is_self {
                "now".into()
            } else {
                d.last_seen
                    .map(|t| t.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "never".into())
            },
        );
    }
}

pub fn stats(s: &AppStats) {
    println!("{}: proxy traffic", s.name);
    println!(
        "  requests {}  inflight {}  shed {}  p50 {:.1}ms  p95 {:.1}ms",
        s.requests_total, s.inflight, s.shed_total, s.p50_ms, s.p95_ms
    );
    println!("  replicas:");
    for r in &s.replicas {
        println!(
            "    {}  {:.0}MB / {}MB  cpu {:.1}%  [{}]",
            r.container_id, r.memory_used_mb, r.memory_limit_mb, r.cpu_pct, r.state
        );
    }
}
