#!/bin/bash
# Serve the live Syncthing home with the binary named in tmp/CURRENT-BINARY.
# Quit Syncthing.app first. Do not run two instances against the same home.
set -euo pipefail

ROOT=/Users/jhf/Projects/syncthing
PTR="$ROOT/tmp/CURRENT-BINARY"
LOG="$ROOT/tmp/custom-serve.log"
PIDFILE="$ROOT/tmp/custom-serve.pid"
WATCH="$ROOT/tmp/watch-dev.sh"
HOME_DIR="$HOME/Library/Application Support/Syncthing"

if [ ! -f "$PTR" ]; then
  echo "missing $PTR" >&2
  exit 1
fi

BINARY=$(awk -F= '/^BINARY=/{print $2; exit}' "$PTR")
KIND=$(awk -F= '/^KIND=/{print $2; exit}' "$PTR")
VERSION=$(awk -F= '/^VERSION=/{print $2; exit}' "$PTR")

if [ ! -x "$BINARY" ]; then
  echo "not executable: $BINARY" >&2
  exit 1
fi

if pgrep -x syncthing >/dev/null; then
  echo "syncthing already running:" >&2
  pgrep -lf '[Ss]yncthing' >&2
  echo "quit Syncthing.app / the existing serve first" >&2
  exit 1
fi

export STNOUPGRADE=1
# NOTE: this variable was misspelled STDBDELETERETIONINTERVAL until 2026-10-12,
# so the 2160h override silently never applied and the default 10920h was used.
export STDBDELETERETENTIONINTERVAL="${STDBDELETERETENTIONINTERVAL:-2160h}"
# pprof endpoint for goroutine dumps on the live daemon (yarlung's 6060 was
# stolen by crowdsec; on this machine 6060 is free). Localhost only.
export STPROFILER="${STPROFILER:-127.0.0.1:6060}"
# Model tracing for completion/need diagnostics; heal telemetry for the notify
# fork's FSEvents replay (lost once in a manual relaunch, keep pinned here).
export STTRACE="${STTRACE:-model}"
export NOTIFY_DEBUG_HEAL="${NOTIFY_DEBUG_HEAL:-1}"

echo "starting KIND=$KIND VERSION=$VERSION"
echo "binary $BINARY"
echo "home   $HOME_DIR"
echo "log    $LOG"

# nice 15: the scan + sweep of a 2.4M-file tree pegs a core for ~20 min after every
# restart; at this priority it yields the machine whenever anything else wants it.
nohup nice -n 15 "$BINARY" serve \
  --home "$HOME_DIR" \
  --no-browser \
  --no-restart \
  >"$LOG" 2>&1 &
echo $! > "$PIDFILE"
echo "pid $! written to $PIDFILE"

if [ -x "$WATCH" ]; then
  nohup "$WATCH" >/dev/null 2>&1 &
  echo "watch pid $! -> $ROOT/tmp/dev-watch.log"
fi
