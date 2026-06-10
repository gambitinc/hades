# HADES

**Host-Aware Deployment & Execution System.** Turn the machine you already
own into a real cloud. Declare an app, get a container, a route, and a public
link. When one machine isn't enough, add another and the load spreads.

```sh
curl -fsSL https://tryhades.com/install.sh | sh   # raise the host
hades login                                        # enter
hades init && hades deploy                          # ship; returns a link
```

Running, today, off a laptop: [tryhades.com](https://tryhades.com).

---

## The premise

Most people don't need the cloud. A MacBook can serve a simple site tens of
thousands of times a second. The bottleneck is your home internet, and even
that holds thousands of concurrent visitors. You already paid for the
computer; it just sits there asleep. **Software should be shared, from your
own desk, the way the early web worked.**

Hosting on a personal device has four real drawbacks. Hades doesn't paper over
them. It instruments each until it's a feature:

| Drawback | What hades does |
|---|---|
| **Uptime** (laptops sleep) | Every downtime window is detected and *classified* (slept / crashed / rebooted) in an uptime ledger. Your phone gets a priority push when the host returns; a dead-man's switch covers true host-down. `hades host uptime` prints availability with causes. A `caffeinate` power assertion keeps the host from idle-sleeping. |
| **Battery** (24/7 load degrades it) | Five-minute telemetry feeds `hades host battery`, a degradation *diagnosis* (capacity trend, cycle burn, high-charge dwell, temperature) including how much of the load was hades' own fault. Apps can opt into `power.on_battery = "pause"`. |
| **Memory** (scarce, shared with you) | Declared memory is mandatory. Overcommitting deploys are rejected *with the full allocation ledger in the error*. Hard per-container limits; OOM kills are detected, restarted with backoff, crash-looped after three strikes. Under pressure, low-priority apps pause first, automatically. |
| **Load** (one contended machine) | Replicas are round-robined by the built-in proxy. Per-app concurrency caps shed excess with `503 + Retry-After`. CPU quotas bound contention. Across a fleet, deploys land on the device with the most free memory. |

Everything the host does flows through one event stream. `hades events
--follow` is the live wire.

## Install

One script, idempotent, re-run it any time (re-running is also how you
update). It checks Docker, cloudflared and Rust, builds the binaries, puts the
daemon under launchd, wires a phone notification channel, runs the doctor, and
offers to join an existing fleet.

```sh
curl -fsSL https://tryhades.com/install.sh | sh
```

Only deploying *from* this machine, not hosting on it? The CLI alone:

```sh
curl -fsSL https://tryhades.com/cli.sh | sh
```

Or from a checkout:

```sh
cargo build --release --workspace
export PATH="$PWD/target/release:$PATH"
hades host init
```

macOS-first (launchd, pmset/ioreg probes, Keychain). Linux/systemd is an
explicit extension point behind traits.

## A deploy, end to end

```toml
# Hades.toml
[app]
name = "my-app"
ports = [8000]
replicas = 2
priority = "normal"      # critical | normal | low (shedding order)

[app.build]              # or:  image = "nginx:alpine"
dockerfile = "Dockerfile"

[app.resources]
cpu = 0.5
memory = "256mb"         # mandatory: admission control needs a number

[app.health_check]
path = "/"
```

```sh
$ hades deploy
my-app deployed
  local:  http://my-app.localhost:8787
  public: https://lazy-otter-4242.trycloudflare.com
```

The CLI tars the build context, ships it to the daemon, which builds the
image, starts replicas with hard limits, health-gates them, swaps routes, and
provisions a tunnel. Deploys are **idempotent upserts**: same name replaces
with zero downtime; retries are safe.

## Stable custom domains, no Cloudflare account for users

Quick-tunnel URLs rotate on restart. For a permanent URL, an operator runs one
**coordinator** for a domain they own, and everyone else just claims a name
under it, never touching Cloudflare themselves:

```sh
$ hades domain claim coolname --app my-app
⚖ coolname.example.com is yours
  https://coolname.example.com        # stable, survives restarts & reboots

$ hades domain claim @ --app site      # the apex: example.com + www
```

The coordinator (a workspace binary, deployable as a hades app) holds one
Cloudflare API token and creates a tunnel + DNS record per claim, handing the
host a connector token. End users need no account, no login, no DNS knowledge.
If you own a domain and run your own single host, skip the coordinator:
`hades host domain apps.example.com` puts the whole host on a named tunnel.

## A fleet of machines

```sh
# on the hub
$ hades fleet add                      # prints the join line

# on each new machine (or unattended: HADES_HUB=… HADES_TOKEN=… sh)
$ hades host join --hub https://… --token …
```

```sh
$ hades fleet
DEVICE     HEALTH   FREE      ALLOCATABLE  APPS   LAST SEEN
studio     green    11468MB   12700MB      3      14:02:11
macbook    green     4121MB    6348MB      1      14:02:13

$ hades deploy                # placed on the freest device automatically
$ hades deploy --device local # or pin it
$ hades ssh macbook           # shell into a device (auth stays plain SSH)
$ hades fleet update          # every device self-updates from the hub
```

The hub holds each device's control URL + token, polls health, places deploys
by free memory, and proxies app commands to wherever an app lives. Devices
re-register with their hub whenever their tunnel URL rotates (self-heal). Hosts
serve their own source, so updates propagate machine-to-machine with no
registry.

## Secrets

```sh
$ hades secrets set STRIPE_KEY=sk_live_… --app shop
$ hades secrets list --app shop        # key names only, values never leave the host
```

Per-app, injected as env at container start, **never in the manifest, git, or
the build context, and never returned by the API.** At rest: XChaCha20-Poly1305
with a per-host master key in the macOS Keychain. Setting a secret rolls the
running replicas in place.

## For agents

The CLI is the entire interface: no MCP server, no SDK, no dashboard. Every
command takes `--json` (one JSON object on stdout, progress on stderr; NDJSON
for streams) with stable exit codes:

```sh
hades deploy --json | jq -r .app.url
```

| exit | meaning |
|---|---|
| 0 | ok |
| 2 | host not ready (doctor red) |
| 3 | deploy rejected (overcommit); the error's `detail` carries the ledger |
| 4 | not found · 5 daemon unreachable · 6 invalid spec · 7 unauthorized |

## Architecture

A Cargo workspace, Rust + tokio:

```
crates/
├── hades-core         shared types: AppSpec, manifest, events, errors, config
├── hades-api          the CLI⇄daemon wire contract + client
├── hades-host         bootstrap, doctor, launchd, macOS probes (pmset/ioreg)
├── hades-sentinel     uptime ledger, notifiers, dead-man's switch
├── hades-runtime      Docker via bollard: build-from-tar, limits, OOM detection
├── hades-proxy        Host-header reverse proxy: aliases, replicas, shed caps
├── hades-tunnel       cloudflared: quick / named / ssh tunnels
├── hadesd             the daemon: API, reconcile, watchdog, policy engine
├── hades-cli          the `hades` binary
└── hades-coordinator  operator subdomain service (Cloudflare-backed claims)
```

The **event bus** is the spine: the reconcile loop, watchdog, power monitor and
tunnel supervisor publish `HostEvent`s; notifications, the policy engine, the
JSONL ledger and the `/events` stream consume them. The policy engine is a pure
function `(event, app states) → actions`, unit-tested without Docker. Desired
state is a JSON file; on restart the daemon converges reality back to it.

State lives under `~/.hades/`: config, desired state, the process registry,
the uptime and event ledgers, battery and resource telemetry, encrypted
secrets, and logs.

Full reference: [tryhades.com/docs.html](https://tryhades.com/docs.html).

## Status

macOS-first, single-owner, working end to end: real containers, real routes,
real public links over Cloudflare, real fleet placement and stable custom
domains (all running live on `tryhades.com`). Named extension points, not yet
built: multi-machine request-level load balancing, Linux/systemd, auto-failover
of apps off a dead device, a SQLite store.

## License

MIT.
