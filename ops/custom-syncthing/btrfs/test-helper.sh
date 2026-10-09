#!/bin/bash
# test-helper.sh — regression test for syncthing-btrfs-helper, no root needed.
#
# A fake `btrfs` on PATH stands in for the real one: `send` sleeps like a
# disk-bound send (yarlung 2026-11-25: a diff-baseline ran 600 s), and
# `subvolume delete` refuses while a send still reads the snapshot, as the
# kernel does ("attempt to delete subvolume 2047 during send", EPERM). The
# real helper script runs against a temp staging dir (STBTRFS_HELPER_DIR).
#
# Checks: an rmsnap issued while a diff's send still runs (the client gave
# up) kills the send, waits for it, deletes the snapshot and reports the
# lag; the diff logs its cost line.
#
# Run on Linux (needs procps pgrep/pkill):
#   bash ops/custom-syncthing/btrfs/test-helper.sh
set -euo pipefail
cd "$(dirname "$0")"
HELPER=$PWD/syncthing-btrfs-helper
T=$(mktemp -d)
cleanup() { pkill -KILL -f "^btrfs send --no-data -p $T/" 2>/dev/null || true; rm -rf "$T"; }
trap cleanup EXIT

mkdir -p "$T/bin" "$T/snaps/baseline" "$T/snaps/boot-x-1"
# The fake send re-execs as a process whose command line is the real one
# ("btrfs send --no-data -p <parent> <child>"), which is what the helper's
# cancel matches on.
cat > "$T/bin/btrfs" <<'EOF'
#!/bin/bash
case "$1" in
  send) exec -a btrfs perl -e '$0 = join(" ", "btrfs", @ARGV); sleep 30' "$@" ;;
  subvolume)
    if [ "$2" = delete ]; then
      if pgrep -f "^btrfs send --no-data -p .* $3\$" >/dev/null; then
        echo "ERROR: Could not destroy subvolume/snapshot: Operation not permitted" >&2
        exit 1
      fi
      rm -rf "$3"
    fi
    ;;
  receive) cat >/dev/null ;;
esac
EOF
chmod +x "$T/bin/btrfs"
export PATH="$T/bin:$PATH" STBTRFS_HELPER_DIR="$T/snaps"

fail() { echo "FAIL: $*"; exit 1; }
sendpat="^btrfs send --no-data -p .* $T/snaps/boot-x-1\$"

# 1. A diff whose send is still running when the client gives up and
#    removes the snapshot.
bash "$HELPER" diff-baseline boot-x-1 >"$T/diff.out" 2>"$T/diff.err" &
diffpid=$!
for _ in $(seq 50); do pgrep -f "$sendpat" >/dev/null && break; sleep 0.1; done
pgrep -f "$sendpat" >/dev/null || fail "fake send never started"

t0=$(date +%s)
out=$(bash "$HELPER" rmsnap boot-x-1 2>"$T/rm.err") || fail "rmsnap failed: $(cat "$T/rm.err")"
took=$(($(date +%s) - t0))
[ ! -d "$T/snaps/boot-x-1" ] || fail "snapshot still present after rmsnap"
if pgrep -f "$sendpat" >/dev/null; then fail "send still running after rmsnap"; fi
[[ "$out" =~ cancelled-send-exit=[0-9.]+ ]] || fail "rmsnap did not report the cancel lag: '$out'"
[ "$took" -lt 10 ] || fail "rmsnap took ${took}s"
grep -q "cancelled running send" "$T/rm.err" || fail "no cancel log line: $(cat "$T/rm.err")"

if wait "$diffpid"; then fail "diff of a killed send reported success"; fi
grep -q "send took=.* rc=137" "$T/diff.err" || fail "diff cost line missing or wrong rc: $(cat "$T/diff.err")"

# 2. rmsnap with no send running stays idempotent and prints nothing.
mkdir -p "$T/snaps/boot-x-2"
out2=$(bash "$HELPER" rmsnap boot-x-2 2>&1) || fail "plain rmsnap failed: $out2"
[ -z "$out2" ] || fail "rmsnap without a send printed '$out2'"
out2=$(bash "$HELPER" rmsnap boot-x-2 2>&1) || fail "idempotent rmsnap failed: $out2"

echo "PASS: rmsnap killed the running send, waited, deleted the snapshot ($out, ${took}s); $(cat "$T/diff.err")"
