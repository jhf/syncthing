#!/bin/bash
# install.sh [host] — install or update the btrfs helper stack on yarlung.
#
# Uses root ssh (BatchMode keys) for the install step. This is a deliberate
# choice over a sudo NOPASSWD entry for jhf: the helper runs as root via
# systemd, so anyone who can install it controls root-executed content — a
# NOPASSWD install path would be a standing root-equivalence grant on the
# jhf account. Root ssh keeps installation a consciously-root act with
# sshd's audit trail, at the same one-command convenience.
#
# Idempotent: skips files whose installed content already matches. Restarts
# the socket unit only when a unit file or the adapter changed (the helper
# itself is re-read per connection — socket-activated, no restart needed).
# Verifies at the end by asking the installed helper its version through
# the socket as jhf — no root needed for drift detection.
set -euo pipefail

HOST="${1:-yarlung.veridit.no}"
ROOT="root@$HOST"
USER_SSH="jhf@$HOST"
cd "$(dirname "$0")"

SBIN=/usr/local/sbin
UNITS=/etc/systemd/system
HELPER=syncthing-btrfs-helper
ADAPTER=syncthing-btrfs-socket-adapter

remote_sha() { ssh "$ROOT" "sha256sum '$1' 2>/dev/null | cut -d' ' -f1"; }
local_sha() { shasum -a 256 "$1" | cut -d' ' -f1; }

install_file() { # install_file <local> <remote-path> <mode>
    local src="$1" dst="$2" mode="$3"
    if [ "$(local_sha "$src")" = "$(remote_sha "$dst")" ]; then
        echo "unchanged: $dst"
        return 1
    fi
    scp -q "$src" "$ROOT:$dst.install-tmp"
    if [[ "$src" == *.sh || "$src" == *helper || "$src" == *adapter ]]; then
        ssh "$ROOT" "bash -n '$dst.install-tmp'" # syntax-gate before it goes live
    fi
    ssh "$ROOT" "install -m$mode '$dst.install-tmp' '$dst' && rm -f '$dst.install-tmp'"
    echo "installed: $dst"
    return 0
}

units_changed=0
install_file "$HELPER"  "$SBIN/$HELPER"  755 || true
install_file "$ADAPTER" "$SBIN/$ADAPTER" 755 && units_changed=1 || true
install_file syncthing-btrfs.socket    "$UNITS/syncthing-btrfs.socket"    644 && units_changed=1 || true
install_file 'syncthing-btrfs@.service' "$UNITS/syncthing-btrfs@.service" 644 && units_changed=1 || true

if [ "$units_changed" = 1 ]; then
    ssh "$ROOT" 'systemctl daemon-reload && systemctl restart syncthing-btrfs.socket'
    echo "socket unit reloaded"
fi

echo -n "installed helper reports: "
ssh "$USER_SSH" 'printf "version\n" | nc -U /run/syncthing-btrfs.sock' | tr -d '\0' | sed 's/status=.*//'
