#!/bin/sh
# ── hades ──────────────────────────────────────────────────────────────
#   turn this mac into a personal cloud.
#   one script: prerequisites → build → daemon under launchd → doctor.
#   idempotent — run it again any time.
#
#   curl -fsSL <this url> | sh
# ───────────────────────────────────────────────────────────────────────
set -eu

BONE='\033[0;33m'; RED='\033[0;31m'; DIM='\033[2m'; OFF='\033[0m'
say()  { printf "  ${BONE}◆${OFF} %s\n" "$1"; }
note() { printf "    ${DIM}%s${OFF}\n" "$1"; }
die()  { printf "  ${RED}✗ %s${OFF}\n" "$1"; exit 1; }

printf "\n  ${RED}HADES${OFF} ${DIM}— the underworld, installed${OFF}\n\n"

# ── 0 · this is a mac ──────────────────────────────────────────────────
[ "$(uname -s)" = "Darwin" ] || die "hades is macOS-first for now (Linux/systemd is on the map)"

# ── 1 · sources ────────────────────────────────────────────────────────
# Resolution order: local checkout you're standing in → existing sources →
# tarball from the host that served this script → git clone.
# __ORIGIN__ is templated in by the hades host serving this file.
ORIGIN="__ORIGIN__"
REPO="${HADES_REPO:-https://github.com/gambitinc/hades}"
SRC="${HADES_SRC:-$HOME/.hades/src}"
if [ -f "./Cargo.toml" ] && grep -q 'hades-cli' ./Cargo.toml 2>/dev/null; then
  SRC="$(pwd)"
  say "using this checkout: $SRC"
elif [ -d "$SRC/.git" ]; then
  say "git pull in $SRC"
  (cd "$SRC" && git pull --ff-only >/dev/null 2>&1 || true)
elif [ "${ORIGIN#__}" = "$ORIGIN" ] && curl -fsSL "$ORIGIN/hades-src.tar.gz" -o /tmp/hades-src.tar.gz 2>/dev/null; then
  # fresh tarball every run — re-running this script IS the update path
  say "fetching sources from $ORIGIN (the host serves its own source)"
  [ "$SRC" = "$HOME/.hades/src" ] && rm -rf "$SRC"
  mkdir -p "$SRC"
  tar xzf /tmp/hades-src.tar.gz -C "$SRC" --strip-components 1
  rm -f /tmp/hades-src.tar.gz
elif [ -f "$SRC/Cargo.toml" ]; then
  note "offline: rebuilding the sources already in $SRC"
else
  command -v git >/dev/null 2>&1 || die "git not found — install Xcode CLT: xcode-select --install"
  say "cloning $REPO"
  mkdir -p "$(dirname "$SRC")"
  git clone --depth 1 "$REPO" "$SRC" 2>/dev/null || die "clone failed — set HADES_SRC to a local checkout"
fi

# ── 2 · rust ───────────────────────────────────────────────────────────
if ! command -v cargo >/dev/null 2>&1; then
  if [ -x "$HOME/.cargo/bin/cargo" ]; then
    PATH="$HOME/.cargo/bin:$PATH"
  else
    say "rust not found — installing via rustup (this is the longest step)"
    curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path
    PATH="$HOME/.cargo/bin:$PATH"
  fi
fi
say "rust $(rustc --version | awk '{print $2}')"

# ── 3 · docker ─────────────────────────────────────────────────────────
if ! command -v docker >/dev/null 2>&1; then
  if command -v brew >/dev/null 2>&1; then
    say "docker not found — installing Docker Desktop (brew cask)"
    brew install --cask docker || die "docker install failed — install Docker Desktop manually, then re-run"
  else
    die "docker not found and no homebrew — install Docker Desktop from docker.com, then re-run"
  fi
fi
if ! docker info >/dev/null 2>&1; then
  say "starting Docker Desktop…"
  open -a Docker || true
  i=0
  while ! docker info >/dev/null 2>&1; do
    i=$((i+1)); [ $i -gt 60 ] && die "docker engine didn't come up in 2 minutes"
    sleep 2
  done
fi
say "docker engine up"

# ── 4 · cloudflared (public links; optional but worth it) ──────────────
if ! command -v cloudflared >/dev/null 2>&1 && [ ! -x /opt/homebrew/bin/cloudflared ]; then
  if command -v brew >/dev/null 2>&1; then
    say "installing cloudflared (public *.trycloudflare.com links)"
    brew install cloudflared || note "cloudflared install failed — apps will get local URLs only"
  else
    note "no cloudflared — apps will get local URLs only (brew install cloudflared)"
  fi
fi

# ── 5 · build & install binaries ───────────────────────────────────────
say "building hades (release) — a few minutes on first build"
(cd "$SRC" && cargo build --release --workspace >/dev/null 2>&1) || die "build failed — run 'cargo build --release' in $SRC to see why"
BIN="$HOME/.hades/bin"
mkdir -p "$BIN"
cp "$SRC/target/release/hades" "$SRC/target/release/hadesd" "$BIN/"
say "installed hades + hadesd → $BIN"

case ":$PATH:" in
  *":$BIN:"*) ;;
  *)
    PROFILE="$HOME/.zprofile"
    grep -qs 'hades/bin' "$PROFILE" 2>/dev/null || printf '\nexport PATH="$HOME/.hades/bin:$PATH"\n' >> "$PROFILE"
    note "added $BIN to PATH in ~/.zprofile (new terminals pick it up)"
    ;;
esac
PATH="$BIN:$PATH"

# ── 6 · raise the host ─────────────────────────────────────────────────
say "running hades host init (config · ntfy topic · launchd · doctor)"
"$BIN/hades" host init || true
# record where the source lives: `hades update` rebuilds from it and the
# daemon serves it to fleet devices at /host/src
CFG="$HOME/.hades/config.toml"
# top-level TOML keys must precede any [table], so prepend rather than append
if ! grep -qs '^source_dir' "$CFG" 2>/dev/null; then
  { printf 'source_dir = "%s"\n' "$SRC"; cat "$CFG" 2>/dev/null; } > "$CFG.tmp" && mv "$CFG.tmp" "$CFG"
  chmod 600 "$CFG"
fi

# ── 7 · fleet: is this machine joining the others? ─────────────────────
# non-interactive path for rolling out many machines:
#   curl -fsSL …/install.sh | HADES_HUB=<url> HADES_TOKEN=<token> sh
if [ -n "${HADES_HUB:-}" ] && [ -n "${HADES_TOKEN:-}" ]; then
  say "joining the fleet at $HADES_HUB"
  i=0
  until "$BIN/hades" host join --hub "$HADES_HUB" --token "$HADES_TOKEN" 2>/dev/null; do
    i=$((i+1)); [ $i -gt 6 ] && { note "join didn't go through — run later: hades host join --hub $HADES_HUB --token <token>"; break; }
    sleep 5
  done
# /dev/tty so the prompt works even though stdin is the curl pipe
elif [ -e /dev/tty ]; then
  printf "\n  ${BONE}already running hades on another machine?${OFF}\n"
  printf "  ${DIM}paste its connect line (from \`hades host connect-info\` there)\n"
  printf "  to add this device to your fleet — or press Enter to make this\n"
  printf "  the first host.${OFF}\n\n  > "
  read -r JOIN_LINE < /dev/tty || JOIN_LINE=""
  if [ -n "$JOIN_LINE" ]; then
    HUB_URL=$(printf '%s' "$JOIN_LINE" | sed -n 's/.*--host \([^ ]*\).*/\1/p')
    HUB_TOK=$(printf '%s' "$JOIN_LINE" | sed -n 's/.*--token \([^ ]*\).*/\1/p')
    if [ -n "$HUB_URL" ] && [ -n "$HUB_TOK" ]; then
      say "joining the fleet at $HUB_URL"
      # the control tunnel can take a few seconds after first boot
      i=0
      until "$BIN/hades" host join --hub "$HUB_URL" --token "$HUB_TOK" 2>/dev/null; do
        i=$((i+1)); [ $i -gt 6 ] && { note "join didn't go through — run later: hades host join --hub $HUB_URL --token <token>"; break; }
        sleep 5
      done
    else
      note "couldn't parse that line — run later: hades host join --hub <url> --token <token>"
    fi
  fi
fi

# ── next steps ─────────────────────────────────────────────────────────
printf "\n  ${RED}the host is raised.${OFF}\n\n"
printf "  enter:          ${BONE}hades login${OFF}\n"
printf "  make an app:    ${BONE}hades init${OFF}      (in any project directory)\n"
printf "  ship it:        ${BONE}hades deploy${OFF}    — returns a public link\n"
printf "  other machines: ${BONE}hades host connect-info${OFF}  — prints the remote login command\n\n"
printf "  ${DIM}subscribe your phone to the topic init printed — the host\n"
printf "  will tell you when it sleeps, wakes, or kills something.${OFF}\n\n"
