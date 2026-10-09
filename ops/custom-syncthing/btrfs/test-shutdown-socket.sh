#!/bin/bash
# test-shutdown-socket.sh — can syncthing reach the helper while the system
# shuts down? Runs the repo's syncthing-btrfs.socket under the host's systemd
# as transient units in /run (removed afterwards); needs root on a systemd
# host (yarlung), touches no installed unit.
#
# Shutdown cannot be started for a test, so a stand-in target plays it:
# whatever the socket unit conflicts with as "shutdown.target" (systemd adds
# that conflict through default dependencies) is made to conflict with the
# stand-in instead. A client ordered After= the socket connects from its
# ExecStop, as syncthing's shutdown pairing does. systemd refuses connections
# to a socket whose stop is pending; on 2026-11-25 and 2026-11-28 that was
# "connection reset by peer" in the shutdown pairing.
#
#   scp ops/custom-syncthing/btrfs/{test-shutdown-socket.sh,syncthing-btrfs.socket} root@yarlung:/tmp/
#   ssh root@yarlung bash /tmp/test-shutdown-socket.sh /tmp/syncthing-btrfs.socket
set -euo pipefail
UNIT=${1:-$(dirname "$0")/syncthing-btrfs.socket}
D=/run/systemd/system
N=stsocktest
cleanup() {
    systemctl stop "$N-client.service" "$N.socket" "$N-down.target" 2>/dev/null || true
    rm -rf "$D/$N.socket" "$D/$N.socket.d" "$D/$N@.service" "$D/$N-client.service" "$D/$N-down.target" \
        "/run/$N.sock" "/run/$N.result" "/run/$N-probe.py"
    systemctl daemon-reload
}
trap cleanup EXIT

# The socket under test, listening on a private path.
sed -e "s|^ListenStream=.*|ListenStream=/run/$N.sock|" -e '/^SocketGroup=/d' -e '/^\[Install\]/,$d' \
    "$UNIT" > "$D/$N.socket"
cat > "$D/$N@.service" <<EOF
[Unit]
DefaultDependencies=no
[Service]
ExecStart=/bin/echo pong
StandardInput=socket
StandardOutput=socket
EOF
cat > "$D/$N-down.target" <<EOF
[Unit]
Description=stand-in for shutdown.target
EOF
cat > "/run/$N-probe.py" <<EOF
import socket
try:
    s = socket.socket(socket.AF_UNIX); s.connect("/run/$N.sock")
    print(s.recv(16).decode().strip() or "connection refused by systemd (empty read)")
except Exception as e:
    print("error:", e)
EOF
cat > "$D/$N-client.service" <<EOF
[Unit]
Wants=$N.socket
After=$N.socket
Conflicts=$N-down.target
Before=$N-down.target
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/true
ExecStop=/bin/sh -c "sleep 1; python3 /run/$N-probe.py > /run/$N.result 2>&1"
EOF
systemctl daemon-reload
if systemctl show "$N.socket" -p Conflicts --value | tr ' ' '\n' | grep -qx shutdown.target; then
    mkdir -p "$D/$N.socket.d"
    printf '[Unit]\nConflicts=%s-down.target\nBefore=%s-down.target\n' "$N" "$N" > "$D/$N.socket.d/down.conf"
    systemctl daemon-reload
    echo "socket conflicts with shutdown.target (stand-in applied)"
fi
systemctl start "$N-client.service"
systemctl start "$N-down.target"
for _ in $(seq 50); do [ -s "/run/$N.result" ] && break; sleep 0.1; done
got=$(cat "/run/$N.result" 2>/dev/null || echo "no result")
echo "client during shutdown: $got"
if [ "$got" != pong ]; then
    echo "FAIL: the helper socket refused syncthing's shutdown connection"
    exit 1
fi
echo "PASS"
