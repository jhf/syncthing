# btrfs scan-replay helper stack (yarlung)

Privileged side of the btrfs snapshot-diff scan replay (see
tmp/btrfs-scan-replay.md). syncthing talks to it over a unix socket; the
syncthing unit itself stays fully hardened (no override, no sudoers).

Install (as root on the btrfs host, service user `jhf` here):

    install -m755 syncthing-btrfs-helper        /usr/local/sbin/
    install -m755 syncthing-btrfs-socket-adapter /usr/local/sbin/
    install -m644 syncthing-btrfs.socket        /etc/systemd/system/
    install -m644 'syncthing-btrfs@.service'    /etc/systemd/system/
    install -D -m644 syncthing-btrfs-order.conf \
        /etc/systemd/system/syncthing@jhf.service.d/btrfs-helper-order.conf
    systemctl daemon-reload
    systemctl enable --now syncthing-btrfs.socket

Shutdown ordering: syncthing's shutdown pairing needs the helper while
syncthing@jhf stops. The drop-in orders syncthing After= the socket (so
the socket stops later), and the helper instances run with
DefaultDependencies=no in system.slice so no instance is refused once
shutdown.target is queued (2026-11-25 02:00 reboot: "connection reset by
peer"). That was not enough: the socket itself had default dependencies,
hence Conflicts=shutdown.target, and systemd refuses every connection to a
socket whose stop job is pending, however late the stop is ordered (the
2026-11-28 09:16 reboot failed the same way). The socket now sets
DefaultDependencies=no and keeps only the sysinit/sockets ordering, so it
listens until the manager exits. `test-shutdown-socket.sh` (root, any
systemd host, transient units in /run only) replays this with a stand-in
target: with the old socket unit it reports "FAIL: the helper socket
refused syncthing's shutdown connection", with this one "PASS". Check the
installed units with `systemctl show syncthing-btrfs.socket
'syncthing-btrfs@x.service' -p Conflicts` (expect both empty).

Update (from the Mac, one command): `./install.sh` — idempotent install
over root ssh (BatchMode keys), syntax-gates bash before it goes live,
restarts the socket unit only when a unit or the adapter changed, and
verifies by asking the installed helper its version through the socket.

Updates deliberately do NOT use a sudo NOPASSWD entry for jhf: the helper
runs as root, so whoever can install it controls root-executed content —
a NOPASSWD install path would be a standing root-equivalence grant on the
jhf account (any process running as jhf could escalate). Root ssh keeps
installation a consciously-root act with sshd's audit trail at the same
convenience. jhf's authority stays at invocation, via the socket.

Drift check (no root): `printf 'version\n' | nc -U /run/syncthing-btrfs.sock`
on yarlung prints `helper-version=N`; compare with the `version` arm here.

Protocol: client writes one line ("<verb> [name...]") and half-closes;
the adapter execs the helper (which owns ALL validation: fixed verbs,
name charset [a-z0-9.-], baseline deletion refused), streams its stdout
back, and appends a status trailer "\x00status=<exitcode>\n" so failures
can never pass as silent success. Requests are logged to the journal
(`journalctl -u 'syncthing-btrfs@*'`).

Cancelling a diff (helper v5): a client that gives up on a slow
diff-baseline closes its connection, but `btrfs send` writes to a temp
file, not the socket, so it does not notice. The client's cleanup rmsnap
therefore kills any send still reading that snapshot, waits for it to
exit, then deletes the snapshot (btrfs refuses to delete a snapshot under
an active send with EPERM) and prints `cancelled-send-exit=<seconds>`.
Measured on yarlung 2026-11-26: SIGKILL to exit 1.8 s cached and 60.0 s
disk-bound; the kernel's tree-compare loop has no signal check, so the
kill lands at send's next stream write. diff and rmsnap log their cost
(`send took= streamBytes= readBytes= rc=`) to the journal.

Test (Linux, no root, fake btrfs on PATH): `bash test-helper.sh`.

syncthing-btrfs-helper variables at the top of the script: subvolume and
snapshots dir. Socket path override for syncthing: SYNCTHING_BTRFS_SOCK.
