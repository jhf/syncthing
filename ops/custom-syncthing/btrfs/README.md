# btrfs scan-replay helper stack (yarlung)

Privileged side of the btrfs snapshot-diff scan replay (see
tmp/btrfs-scan-replay.md). syncthing talks to it over a unix socket; the
syncthing unit itself stays fully hardened (no override, no sudoers).

Install (as root on the btrfs host, service user `jhf` here):

    install -m755 syncthing-btrfs-helper        /usr/local/sbin/
    install -m755 syncthing-btrfs-socket-adapter /usr/local/sbin/
    install -m644 syncthing-btrfs.socket        /etc/systemd/system/
    install -m644 'syncthing-btrfs@.service'    /etc/systemd/system/
    systemctl daemon-reload
    systemctl enable --now syncthing-btrfs.socket

Protocol: client writes one line ("<verb> [name...]") and half-closes;
the adapter execs the helper (which owns ALL validation: fixed verbs,
name charset [a-z0-9.-], baseline deletion refused), streams its stdout
back, and appends a status trailer "\x00status=<exitcode>\n" so failures
can never pass as silent success. Requests are logged to the journal
(`journalctl -u 'syncthing-btrfs@*'`).

syncthing-btrfs-helper variables at the top of the script: subvolume and
snapshots dir. Socket path override for syncthing: SYNCTHING_BTRFS_SOCK.
