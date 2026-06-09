#!/bin/sh
# ── hades cli ──────────────────────────────────────────────────────────
#   just the CLI — for the machine you work from, not the one that hosts.
#   builds `hades`, puts it on PATH, then you log into your host:
#
#     hades login --host <url> --token <token>
#
#   (get that line from `hades host connect-info` on the host.)
# ───────────────────────────────────────────────────────────────────────
set -eu

BONE='\033[0;33m'; RED='\033[0;31m'; DIM='\033[2m'; OFF='\033[0m'
say()  { printf "  ${BONE}◆${OFF} %s\n" "$1"; }
note() { printf "    ${DIM}%s${OFF}\n" "$1"; }
die()  { printf "  ${RED}✗ %s${OFF}\n" "$1"; exit 1; }

printf "\n  ${RED}HADES${OFF} ${DIM}— the CLI alone${OFF}\n\n"
[ "$(uname -s)" = "Darwin" ] || die "macOS-first for now"

# sources: local checkout → existing → tarball from this origin → git
ORIGIN="__ORIGIN__"
REPO="${HADES_REPO:-https://github.com/dhilanshah/hades}"
SRC="${HADES_SRC:-$HOME/.hades/src}"
if [ -f "./Cargo.toml" ] && grep -q 'hades-cli' ./Cargo.toml 2>/dev/null; then
  SRC="$(pwd)"
  say "using this checkout: $SRC"
elif [ -d "$SRC/.git" ] || [ -f "$SRC/Cargo.toml" ]; then
  say "sources already in $SRC"
elif [ "${ORIGIN#__}" = "$ORIGIN" ] && curl -fsSL "$ORIGIN/hades-src.tar.gz" -o /tmp/hades-src.tar.gz 2>/dev/null; then
  say "fetching sources from $ORIGIN"
  mkdir -p "$SRC"
  tar xzf /tmp/hades-src.tar.gz -C "$SRC" --strip-components 1
  rm -f /tmp/hades-src.tar.gz
else
  command -v git >/dev/null 2>&1 || die "git not found — xcode-select --install"
  say "cloning $REPO"
  mkdir -p "$(dirname "$SRC")"
  git clone --depth 1 "$REPO" "$SRC" 2>/dev/null || die "clone failed"
fi

# rust
if ! command -v cargo >/dev/null 2>&1; then
  if [ -x "$HOME/.cargo/bin/cargo" ]; then
    PATH="$HOME/.cargo/bin:$PATH"
  else
    say "installing rust via rustup"
    curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path
    PATH="$HOME/.cargo/bin:$PATH"
  fi
fi

say "building the hades CLI"
(cd "$SRC" && cargo build --release -p hades-cli >/dev/null 2>&1) || die "build failed — run 'cargo build --release -p hades-cli' in $SRC"
BIN="$HOME/.hades/bin"
mkdir -p "$BIN"
cp "$SRC/target/release/hades" "$BIN/"
case ":$PATH:" in
  *":$BIN:"*) ;;
  *)
    PROFILE="$HOME/.zprofile"
    grep -qs 'hades/bin' "$PROFILE" 2>/dev/null || printf '\nexport PATH="$HOME/.hades/bin:$PATH"\n' >> "$PROFILE"
    note "added $BIN to PATH in ~/.zprofile"
    ;;
esac

printf "\n  ${RED}done.${OFF} now connect to your host:\n\n"
printf "    ${BONE}hades login --host <url> --token <token>${OFF}\n\n"
printf "  ${DIM}get that line by running \`hades host connect-info\` on the host.\n"
printf "  then: hades init · hades deploy · hades fleet${OFF}\n\n"
