# Btrfs snapshot-diff scan replay (Linux)

Companion to `fsevents-journal-replay.md`: the same "skip the initial full
scan" goal for Linux, using btrfs snapshots instead of an event journal.
Target host: yarlung.veridit.no (`/home` on btrfs subvol `@home`), where the
initial scan of `/home/jhf` takes ~14 minutes.

Branch: `feature/btrfs-scan-replay` (off the FSEvents replay tip).

## Why a snapshot diff needs a different safety design than FSEvents

FSEvents replay is event-based: the journal records every change since the
watermark, so index + replayed events == current disk.

A snapshot diff is state-based: `btrfs send --no-data -p A B` reports the net
difference between two trees. A file created, indexed by the watch, and
deleted again between the baseline and the next boot is absent at both ends,
leaves no trace in the diff, and the index still holds it. Arming from such
a diff would resurrect the deleted file to the peers.

Verified live on yarlung: `TestBtrfsScanDiff` asserts a create+delete inside
one generation gap is NOT reported by the diff.

Consequence: the baseline snapshot is only sound when paired with a provably
consistent index: index == disk(snapshot) at pairing time.

## The design: airtight shutdown pairing

Pairing happens only at a converged shutdown:

1. Gate: folder eligible (see below), watcher enabled, and `fullScanOK` -
   a per-run flag set when a full scan completed or the replay armed in
   THIS run, cleared whenever a full scan starts. A run that is still
   mid-initial-scan (14 min on yarlung) must not pair: the watch drain
   cannot repair an index that is stale from downtime changes.
2. Stop `monitorWatch` so the shutdown procedure is the only consumer of
   watch batches. The aggregator and the watch backend stay alive on a
   context detached from Serve's (`context.WithoutCancel`), torn down by
   Serve's `defer stopWatch`.
3. Convergence loop (up to 3 attempts, 90s total budget):
   a. Drain: flush the aggregator (new `AggregateWithFlush` API) and scan
      every pending batch until 2s of quiet after a flush.
   b. Snapshot the subvolume (`snap shutdown-<folder>-<ns>`).
   c. Settle check: flush again, 2s of quiet. Any arriving batch is still
      SCANNED (harmless: the retry's snapshot includes the change) but
      means this snapshot is not paired with a quiet index, so the attempt
      retries with a fresh snapshot. The aggregator emits flushed batches
      before closing the flush ack, so batches are received while awaiting
      the ack (a receive-after-ack order would deadlock).
   d. On quiet: `setbaseline` (baseline = this snapshot, old baseline
      deleted), then write the clean-shutdown marker KV
      `model/btrfsScanBaseline/<folderID>`.
4. Any failure, timeout, or busy filesystem: no marker. Next boot does the
   regular full scan. Correctness over speed, always.

Boot arming:

1. `consumeBtrfsBaselineMarker` reads and DELETES the marker at folder
   start. Deleting eagerly is what makes it crash-proof: a crash never
   leaves a marker behind.
2. Order: watch starts FIRST, then the boot snapshot (`snap boot-...`) and
   the diff `baseline -> boot-snap`. The diff covers everything up to the
   snapshot, the watch everything after its start; watch-first makes the
   two overlap instead of leaving a gap.
3. Changed paths (folder-relative, space-unescaped, capped at 200k) go
   through `scanSubdirs`. Success: skip the initial scan timer, close
   `initialScanFinished`, `fullScanOK = true`. Any failure: regular full
   scan.
4. The boot snapshot is removed after the diff; the baseline stays (it is
   only replaced by the next successful shutdown pairing).

Eligibility (cached per folder lifetime):

- Linux only, helper installed (`BtrfsScanReplaySupported`).
- Folder on the helper's btrfs subvolume: statfs magic `0x9123683e` and the
  folder path resolves under the helper's subvolume path (prefix check).
- No nested subvolume inside the folder: a snapshot diff does not cross
  subvolume boundaries, so a nested subvolume would silently escape
  coverage. Detected via `btrfs subvolume list -o`.

## The helper

Root-only operations run as `/usr/local/sbin/syncthing-btrfs-helper`
(fixed path, fixed subcommands). syncthing reaches it over a unix socket
served by a socket-activated systemd service, NOT via sudo (see the ops
section for why the sudo design was replaced). Version 2 primitives:

    snap <name>            snapshot subvolume -> snapshots dir/<name>
    diff <a> <b>           btrfs send --no-data -p <a> <b> | receive --dump
    diff-baseline <name>   diff baseline <name>
    setbaseline <name>     baseline = <name>, delete old baseline
    rmsnap <name>          delete snapshot (never the baseline)
    status | subvol | subvols

Snapshot names restricted to `[a-z0-9.-]`, `..` rejected; the helper never
deletes `baseline`.

## Parser

`lib/fs/btrfsreplay.go` (pure, untagged): parses `btrfs receive --dump`
output into net changed paths. Handles backslash-escaped spaces, renames
(via btrfs temp names), symlink `link dest=` (no `./` prefix), hardlinks,
rmdir, utimes. Teeth-tested against canned real dumps captured on yarlung.

## Tests

- `lib/fs/btrfsreplay_test.go`: parser, canned dumps (green everywhere).
- `lib/fs/btrfsreplay_linux_test.go`: end-to-end helper contract, gated on
  `ST_BTRFS_TEST=1` (OWNS the helper baseline - never run while yarlung
  syncthing uses replay). Run on yarlung: compile as devops, run as jhf.
  Asserts the create+delete-in-one-generation path is dropped, promotion
  settles. PASS 2026-10-09.
- `lib/watchaggregator`: `TestAggregateFlush` - flush forces pending events
  out immediately (teeth-tested by breaking `flushAll`).

## Ops: socket-activated helper service (replaced the sudo design)

Syncthing talks to the helper over `/run/syncthing-btrfs.sock`
(override: `SYNCTHING_BTRFS_SOCK`). The stock hardened Debian unit runs
UNTOUCHED - no NoNewPrivileges/RestrictSUIDSGID relaxation, no extra
capabilities; the network-facing process holds nothing. Three installed
pieces:

- `/usr/local/sbin/syncthing-btrfs-helper` - the validated fixed-verb
  script (unchanged; still directly runnable for ops/debug).
- `/usr/local/sbin/syncthing-btrfs-socket-adapter` - reads one request
  line ("<verb> [name...]") from the socket, logs it to the journal
  (audit, like sudo did), execs the helper, then appends a status
  trailer `\x00status=<exitcode>\n` so a failed helper can never pass as
  silent success for the output-less verbs (snap/setbaseline/rmsnap).
  NUL cannot appear in btrfs paths or helper output, so the trailer is
  unambiguous.
- `/etc/systemd/system/syncthing-btrfs.socket` - `Accept=true`
  (inetd-style: one root helper instance per connection),
  `ListenStream=/run/syncthing-btrfs.sock`, `SocketMode=0660
  SocketGroup=jhf` - same authorization boundary the sudoers entry gave.
- `/etc/systemd/system/syncthing-btrfs@.service` - runs the adapter as
  root, sandboxed: NoNewPrivileges, CapabilityBoundingSet=CAP_SYS_ADMIN
  CAP_CHOWN CAP_FOWNER CAP_DAC_OVERRIDE, ProtectSystem=strict,
  ProtectHome=read-only + ReadWritePaths=/home/.snapshots/syncthing,
  PrivateTmp/PrivateDevices/PrivateNetwork, DevicePolicy=closed,
  SystemCallFilter=@system-service, kernel/clock/hostname protections.

Verified properties (2026-10-09): all verbs work through the socket as
jhf; `snap ../../etc`, `rmsnap baseline`, unknown verbs rejected
(exit 2, journal-logged); a non-jhf user gets EACCES on connect. Failure
semantics: missing socket/service or nonzero helper status degrades to
the normal full-scan fallback (`errBtrfsReplayUnsupported`, Info log).

### History: the sudo design and why it was replaced

The first deployment used `sudo -n` + a NOPASSWD sudoers entry. Debian's
syncthing@.service blocks that three ways: `NoNewPrivileges=true` +
`RestrictSUIDSGID=true` neutralize sudo's setuid bit, and
`CapabilityBoundingSet=CAP_CHOWN CAP_FOWNER` (a) lacks
CAP_SETUID/CAP_SETGID, which makes sudo-rs fail setresuid, and (b) lacks
CAP_SYS_ADMIN, which the btrfs ioctls need. The workaround override
(`/etc/systemd/system/syncthing@jhf.service.d/btrfs-helper.conf`,
removed 2026-10-09) relaxed NoNewPrivileges/RestrictSUIDSGID and added
CAP_SETUID/CAP_SETGID/CAP_SYS_ADMIN to the bounding set - which meant
the network-facing syncthing process itself carried CAP_SYS_ADMIN. The
socket design removes that: the only privileged code is the fixed-verb
helper behind a 0660 root:jhf socket. Diagnosis trick that found the
three-way block: replicate the sandbox without touching the service via
`systemd-run --uid=jhf --wait --pipe -p CapabilityBoundingSet=... sudo -n
/usr/local/sbin/syncthing-btrfs-helper subvol`.

## Commits

- `7de36f2bc` feat(model): skip the initial full scan on btrfs via snapshot diff
- `f555aec99` flush API + shutdown-pairing rework
- `06ea5e7b0` gate pairing on per-run scan completeness (fullScanOK)
- `d395bdfab` fix(fs): reach the btrfs helper over a socket-activated service

### Socket-transport acceptance (yarlung, 2026-10-09 evening)

Binary: v2.1.7-dev.41.gd395bdfa, sha256 a50604e3c63b... (remote build,
`build-remote.sh HEAD install`, 31s).

- Live tests `TestBtrfsScanDiff` + `TestBtrfsScanNestedSubvols` PASS as
  unprivileged jhf through the socket (compile as devops, run as jhf).
- sudoers entry and the hardening override REMOVED; unit back to stock:
  `NoNewPrivileges=yes`, `RestrictSUIDSGID=yes`,
  `CapabilityBoundingSet=cap_chown cap_fowner`.
- Full cycle on the stock unit, all via the socket (adapter journal shows
  every request; zero sudo lines from syncthing): stop 21:34:10 -> paired
  21:34:15 (5s, drain+snap+settle+setbaseline+rmsnap) -> boot 21:34:17 ->
  'Skipping initial scan, applied btrfs snapshot diff (changedPaths=9)'
  21:34:18.

## Live acceptance log (yarlung, 2026-10-09)

Binary: v2.1.7-dev.40.gde6eb17a, sha256 dc40689692f9..., installed over the
dpkg-diverted /usr/bin/syncthing (sidecars alongside).

Throwaway instance (STTRACE=model, folder on /home/jhf/pair-test-data):

- Clean SIGTERM: 'Btrfs scan baseline paired with index at shutdown' (~5s:
  drain 2s + snap + settle 2s), marker KV written.
- Next boot: 'Skipping initial scan, applied btrfs snapshot diff
  (changedPaths=0)'.
- Downtime test: file A (indexed while up) deleted while down, file B
  created while down, file C created+deleted while down. Boot:
  changedPaths=2 (exactly A and B). REST browse: A absent, B present, C
  never existed, pre-stop drain-caught change to file1.txt indexed.

Real service (folder home = /home/jhf):

- 17:21:50 systemctl stop: attempting pairing -> snap shutdown-home-... ->
  settle -> setbaseline -> 'Btrfs scan baseline paired with index at
  shutdown' (5s). Marker KV model/btrfsScanBaseline/home verified in
  index-v2/main.db.
- Downtime: rm .btrfs-accept-A.txt, create .btrfs-accept-B.txt,
  create+delete .btrfs-accept-C.txt.
- 17:22:08 systemctl start: eligibility -> status -> snap boot-... ->
  diff-baseline -> 'Marking file as deleted (path=.btrfs-accept-A.txt)' ->
  'Skipping initial scan, applied btrfs snapshot diff (changedPaths=14)'
  in ~1s (A, B, and 12 paths of own .config/syncthing churn). No 12-minute
  walk.
- Mac peer verified seconds later: .btrfs-accept-B.txt present with correct
  content, .btrfs-accept-A.txt gone. C never appeared anywhere.
- 17:22:59 restart without STTRACE: paired again (17:23:03), boot armed
  again (changedPaths=9). The feature is self-sustaining: every clean
  shutdown re-pairs, every boot replays.

Failures found and fixed during acceptance:

1. Drain deadlock: the aggregator emits flushed batches BEFORE closing the
   flush ack; a wait-ack-then-receive drain deadlocks (commit 7eb9299fb).
2. Post-snapshot discard would lose pre-snapshot changes; settle scans and
   retries instead (same commit).
3. Cancelling monitorWatch at shutdown killed the aggregator via its
   deferred aggrCancel; monitorWatch never consumed watchChan in the first
   place, so the split context was removed (commit c1431dc0a).
4. systemd hardening silently disabled the helper: see the ops section.
   Diagnosed via a throwaway instance and systemd-run replication; gate
   logging added (commit de6eb17ac).
