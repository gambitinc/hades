# hades

Turn a personal machine into a fly.io-style cloud host. Declare an app's spec;
the daemon runs it in a container, routes traffic to it, and hands back a
public link.

```
cd examples/hello && hades deploy
# → https://random-words-1234.trycloudflare.com
```

## The premise

Hosting on a personal device has four structural drawbacks — uptime (laptops
sleep), battery (24/7 load degrades it), scarce memory (containers compete
with your apps inside a fixed-size Docker VM), and one contended CPU. Hades
doesn't paper over them; it instruments them until each is a feature:

| Drawback | What hades does about it |
|---|---|
| **Uptime** | Every downtime window is detected and *classified* (slept / crashed / rebooted) in an uptime ledger. Your phone gets a priority push when the host comes back (and, via a dead-man's switch, when it goes down). `hades host uptime` prints availability % with causes. |
| **Battery** | 5-minute telemetry (cycles, capacity vs design, temperature, charge dwell) feeds `hades host battery` — a degradation *diagnosis* that includes how much of the load was hades' fault, with concrete remedies. Apps can opt into `power.on_battery = "pause"`. |
| **Memory** | Declared memory is mandatory. Deploys that overcommit the Docker VM are rejected *with the full allocation ledger in the error*. Containers get hard limits; OOM kills are detected, restarted with backoff, crash-looped after 3 strikes. Under host memory pressure, low-priority apps are paused first, automatically, and resumed when it clears. |
| **Load** | Replicas are round-robined by the built-in proxy. Per-app concurrency caps shed excess with `503 + Retry-After`. CPU quotas bound contention. One priority system (`critical/normal/low`) decides who gets paused when something must give. |

Everything the host does flows through one event stream — `hades events
--follow` is the live wire.

## Quickstart

The short way — the website (in `site/`, and dogfooded: it deploys on hades
itself as the app `gates`) serves a copy-able installer that takes a bare Mac
to a green doctor, then tells you the three rites:

```sh
curl -fsSL https://hades.sh/install.sh | sh   # raise the host
hades login                                    # enter — verifies doctor, shows capacity
hades init && hades deploy                     # ship — returns a link
```

The long way, from this checkout:

```sh
# 1. build
cargo build --workspace
export PATH="$PWD/target/debug:$PATH"

# 2. bootstrap this machine (idempotent; safe to re-run)
hades host init
#   ✓ checks Docker + cloudflared (tells you the exact brew command if missing)
#   ✓ warns if the machine sleeps on AC (a host that naps isn't a host)
#   ✓ generates an ntfy.sh topic — subscribe on your phone, no account needed
#   ✓ installs launchd supervision (starts on login, restarts on crash)
#   ✓ runs the doctor

# 3. deploy something
cd examples/hello
hades deploy
# hello deployed
#   local:  http://hello.localhost:8787
#   public: https://lazy-otter-4242.trycloudflare.com

# 4. operate
hades apps list
hades apps logs hello --follow
hades apps stats hello          # requests, p95, shed count, per-replica memory
hades host status               # capacity vs allocated (the Docker VM's, not the Mac's)
hades host ps                   # every process hades owns, with live RSS
hades events --follow           # the event stream
hades apps destroy hello
```

## For agents

The CLI is the entire interface — no MCP server, no SDK. The contract:

- `--json` on every command: exactly one JSON object on stdout (NDJSON for
  `events`/`logs` streams); progress goes to stderr.
  `hades deploy --json | jq -r .app.url` just works.
- Stable exit codes: `0` ok, `1` error, `2` host-not-ready (doctor red),
  `3` deploy-rejected (overcommit — the error's `detail` carries the full
  resource ledger so you know what to shrink), `4` not-found,
  `5` daemon-unreachable, `6` invalid-spec.
- Deploys are idempotent upserts: same name = replace, retries are safe.
- Public URLs are cattle (quick tunnels mint a new URL on every restart).
  `hades url <name> --json` always tells the current truth; `url_changed_at`
  lets you detect staleness cheaply.

## Architecture

Cargo workspace, Rust + tokio:

```
crates/
├── hades-core/       shared types: AppSpec, Hades.toml, events, errors, config
├── hades-api/        the CLI↔daemon wire contract + reqwest client
├── hades-host/       bootstrap, doctor, launchd, macOS probes (pmset/ioreg)
├── hades-sentinel/   uptime ledger, ntfy/macOS notifiers, dead-man's switch
├── hades-runtime/    Docker via bollard: build-from-tar, hard limits, OOM detection
├── hades-proxy/      Host-header reverse proxy: aliases, replicas, shed caps
├── hades-tunnel/     cloudflared quick tunnels: provision, scrape URL, supervise
├── hadesd/           the daemon: API, reconcile, watchdog, policy engine, power
└── hades-cli/        `hades`
```

Design notes worth knowing:

- **Tunnel topology.** A quick tunnel maps one hostname to one port, so each
  app gets its own cloudflared pointed *at the proxy*, and the scraped
  hostname is registered as an alias route. The proxy stays the single
  ingress for local + public traffic.
- **Capacity honesty.** On macOS, containers live in the Docker VM. All
  admission math uses the VM's capacity (bollard `/info`), and `hades host
  status` shows both numbers so nobody budgets against the Mac's 64GB.
- **The event bus is the spine.** Reconcile, watchdog, power monitor, and
  tunnel supervisor publish `HostEvent`s; the notifier, policy engine, JSONL
  ledger, and `/events` stream consume them. The policy engine is a pure
  function (event, app states) → actions, unit-tested without Docker.
- **Nothing untracked.** Every container carries a `hades.app` label; every
  cloudflared PID is registered. A reaper kills anything labeled that desired
  state no longer explains.

State lives under `~/.hades/` (config, state, ledgers, metrics, logs).

## Extension points (deliberately out of scope for now)

Multi-machine scheduling (the route table already supports multiple backends
per hostname), Linux/systemd (`ServiceManager`/`HostProbe` traits), named
tunnels with stable domains (`UrlProvider`), SQLite store, secrets, TLS for
local routes, autoscaling.
