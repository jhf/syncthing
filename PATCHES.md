# Maintained patches on top of upstream Syncthing

This file is the inventory of the patches this fork carries. It lives on the
fork's `main` branch, which is assembled from the per-patch branches listed
below. See `FORK.md` for how the fork is maintained and rebased.

Upstream tracking issue: https://github.com/syncthing/syncthing/issues/10921
(presents the six headline changes with measurements; PRs offered one per
patch if upstream wants them).

Patches are ordered in dependency order. Each `patch/NN-*` branch contains
everything in the branches before it plus its own piece, so
`git diff patch/NN-1-... patch/NN-...` shows exactly one piece.

| # | Branch | Piece | Commits |
|---|--------|-------|---------|
| 01 | `patch/01-skip-mtime-on-exists` | Skip virtual mtime lookup for existence checks (the 40% scan speedup) | 7 |
| 02 | `patch/02-gc-chunked-commit` | GC commits each hash-range chunk; blocklist hashes retired at update; keep needs-full-scan flag across GC interruption | 3 |
| 03 | `patch/03-need-list-perf` | Need-list queries: no SQL name ordering, cached remote need list, cached per-device need count, iterator cost accounting | 6 |
| 04 | `patch/04-chunked-scan` | Chunked scanning: resumable walks, pull/watch servicing between chunks, timer-enforced chunk budget, mid-sweep pull servicing | 15 |
| 05 | `patch/05-fsevents-journal-replay` | Boot-time FSEvents journal replay instead of the initial full scan; overflow healing without killing the watch; pins the notify fork | 4 |
| 06 | `patch/06-btrfs-scan-replay` | BTRFS snapshot-diff scan replay (Linux receiver side); shutdown-paired scans; per-run scan completeness gating | 8 |
| 07 | `patch/07-btrfs-helper-ops` | Socket-activated btrfs helper stack (systemd units + helper scripts under `ops/`) | 1 |
| 08 | `patch/08-watch-buffer-batch-throttle` | Watch backend buffer 500→65536; skip full-folder watch batches while a full scan runs | 2 |
| 09 | `patch/09-gui-event-loop` | Fetch-based GUI event loop (immune to background-tab timer throttling); CSRF header on the poll | 2 |
| 10 | `patch/10-need-tables` | Incrementally maintained need tracking tables (no full scans for need queries) | 2 |
| 11 | `patch/11-gc-lock-scope` | GC must not hold updateLock for minutes; busy_timeout so concurrent writers wait; no updateLock re-acquire in GC | 3 |
| 12 | `patch/12-fsevents-rearm-after-scan` | Arm journal replay after a completed full scan too | 1 |
| 13 | `patch/13-btrfs-canary-barrier` | Pair BTRFS baseline with index via canary barrier; runtime baseline refresh; canaries pass the watch ignore filter | 2 |
| 14 | `patch/14-fsevents-poll-mode` | Poll-mode journal replay replaces the live FSEvents stream (5s cadence); IgnoreSelf via notify fork | 2 |
| 15 | `patch/15-db-busy-resilience` | Index updates retry on SQLite lock contention (bounded backoff) instead of killing the device connection; `busy_timeout` 5s → 30s | 1 |
| 16 | `patch/16-watch-flush-wait` | Watch flush request waits (15s) for a busy aggregator instead of aborting a valid baseline pairing | 1 |

## Depends on

- `github.com/jhf/notify` branch `fsevents-replay` (pinned via `replace` in
  `go.mod`): boot replay primitives, lossy-replay verdicts, IgnoreSelf,
  `SetPollOnly` poll mode. That repo has its own `PATCHES.md`.

## Headline results (measured on this fork, Oct 2026)

- 2.4M-file home folder on a Mac Studio: boot replay of a 4.17M-event /
  5.8-hour journal gap completes in 6 s. Stock syncthing took ~40 minutes of
  full scan for the same gap.
- Poll mode: fseventsd goes from a permanent real-time subscriber
  (104 % CPU / 19.3 GB RSS under event storms) to an occasional batch reader
  (~0.2–3 % CPU, 14–16 MB RSS). 11,877+ poll cycles with zero drop flags.
- Sync latency with 5 s polling: 10–23 s end to end (Mac↔Linux), deletes
  included.
- yarlung (Ubuntu 26.04, BTRFS RAID1 on 2×3TB HDD): first full chunked scan
  completed; subsequent boots and runtime scans use snapshot-diff replay.
- Full data: https://github.com/syncthing/syncthing/issues/10921

## Not included in the clean chain

The deployed history (`feature/btrfs-scan-replay`, kept as the ops archive
branch) additionally contains: temporary debug instrumentation (two commits
marked TEMPORARY), the STGUISTATELOG UI-correlation hook, binary artifacts of
every dev build under `tmp/`, and the incident/deployment ops log. Those are
intentionally absent from `main` and the `patch/` branches.
