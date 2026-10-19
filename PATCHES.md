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
| 04 | `patch/04-chunked-scan` | Chunked scanning: resumable walks, pull/watch servicing between chunks, timer-enforced chunk budget, mid-sweep pull servicing; tests hardened for CI runners (native-name DB compare on Windows, budget-timing retries, unique test content to keep the rename bucket O(n)) | 15 |
| 05 | `patch/05-fsevents-journal-replay` | Boot-time FSEvents journal replay instead of the initial full scan; overflow healing without killing the watch; pins the notify fork | 4 |
| 06 | `patch/06-btrfs-scan-replay` | BTRFS snapshot-diff scan replay (Linux receiver side); shutdown-paired scans; per-run scan completeness gating; 32-bit-safe btrfs magic compare (CI-found) | 8 |
| 07 | `patch/07-btrfs-helper-ops` | Socket-activated btrfs helper stack (systemd units + helper scripts under `ops/`) | 1 |
| 08 | `patch/08-watch-buffer-batch-throttle` | Watch backend buffer 500→65536; skip full-folder watch batches while a full scan runs; aggregator overflow collapses to the affected subtrees instead of a full-tree scan (dogfood: hourly full scans from build bursts, 2026-10-17) | 3 |
| 09 | `patch/09-gui-event-loop` | Fetch-based GUI event loop (immune to background-tab timer throttling); CSRF header on the poll | 2 |
| 10 | `patch/10-need-tables` | Incrementally maintained need tracking tables (no full scans for need queries); folderDB.Close waits for the async need-table builder so test tempdirs close cleanly on Windows (CI-found) | 2 |
| 11 | `patch/11-gc-lock-scope` | GC must not hold updateLock for minutes; busy_timeout so concurrent writers wait; no updateLock re-acquire in GC | 3 |
| 12 | `patch/12-fsevents-rearm-after-scan` | Arm journal replay after a completed full scan too | 1 |
| 13 | `patch/13-btrfs-canary-barrier` | Pair BTRFS baseline with index via canary barrier; runtime baseline refresh; canaries pass the watch ignore filter; barrier test helper collects GC-garbage dir handles before tempdir removal on Windows (CI-found) | 2 |
| 14 | `patch/14-fsevents-poll-mode` | Poll-mode journal replay replaces the live FSEvents stream (5s cadence); IgnoreSelf (own writes filtered server-side) as a serve-startup opt-in | 2 |
| 15 | `patch/15-db-busy-resilience` | Index updates retry on SQLite lock contention (bounded backoff) instead of killing the device connection; `busy_timeout` 5s → 30s | 1 |
| 16 | `patch/16-watch-flush-wait` | Watch flush request waits (15s) for a busy aggregator instead of aborting a valid baseline pairing | 1 |
| 17 | `patch/17-files-needed-index` | `files_needed` partial index: local pull-queue query stops scanning every file (adopted from unmerged upstream fix, syncthing#10902) | 1 |
| 18 | `patch/18-scan-observability` | Full-scan outcomes and btrfs replay bail reasons are logged; pairing marker consumed only when the replay can actually be attempted; btrfs probe skipped when the journal replay already armed (found by dogfooding the fast path) | 1 |
| 19 | `patch/19-btrfs-repair-after-replay` | Re-arm the baseline pairing after a boot diff replay on a Serve-loop retry timer — the shutdown pairing races the process-exit deadline, and an inline boot pairing fails while the watch aggregator is still in its setup pass; both found by dogfooding | 1 |
| 20 | `patch/20-stale-request-repair` | A remote request that fails ENOENT while the index claims the file exists feeds the path into the watch aggregator: debounced subtree scan, tombstone, requester converges — self-healing for missed local changes (dogfood: one manifest retried 9449 times over 17h, 2026-10-18) | 1 |
| 21 | `patch/21-adaptive-scan-budget` | The scan chunk quantum doubles at each boundary that found no queued pull or watch work (250ms → 5s cap) and snaps back when real work is serviced: an idle machine converges at speed instead of paying a boundary-forced commit flush per 250ms; a busy machine keeps fine-grained sharing. The deletion sweep logs per-chunk entries/marks/wall/boundary/per-entry cost — motivated by a 197k-entry sweep observed at ~100 marks/s at 0% CPU with no code path to explain it (2026-10-19) | 1 |
| 23 | `patch/23-passive-checkpoints` | WAL checkpoints on the update path become PASSIVE: RESTART/TRUNCATE must wait for readers, the scan sweep's own iteration snapshot is one, and the retry escalation ground the serve loop at ~100 marks/s at 0% CPU (goroutine-profile attribution, 2026-10-19). PASSIVE never waits; the WAL stays bounded by journal_size_limit and drains fully once the machine is quiet. (patch/22 will be the bulk-delete collapse; numbered after it as it lands) | 1 |

## Depends on

- `github.com/jhf/notify` branch `fsevents-replay` (pinned via `replace` in
  `go.mod`): boot replay primitives, lossy-replay verdicts, IgnoreSelf,
  `SetPollOnly` poll mode. That repo has its own `PATCHES.md`.

## Measured results

All numbers from this fork's own logs and instruments, on the production
workload: the local Mac's home tree (2.38M files, 394k directories,
262 GiB) mirroring to the remote Linux receiver (Ubuntu, BTRFS RAID1 on
HDD) over WAN. "Before" is stock upstream v2.1.x on the same machines.

| What | Before | After | Patch(es) |
|------|--------|-------|-----------|
| Boot after 5.8 h offline (Mac) | ~40 min full scan | 6 s journal replay (4.17M events) | 05, 14 |
| Scan of the full tree (Mac) | baseline | −40 % wall time | 01 |
| Watcher cost under event storms | fseventsd 104–110 % CPU, 9–19 GB RSS, drop-flag heals adding more load | 0.2–3 % CPU, 14–16 MB RSS; 11,877+ poll cycles, zero drops | 08, 14 |
| First scan on the remote receiver after reboot | ~14–45 min full walk of HDD RAID | snapshot diff, seconds (when a baseline is paired) | 06, 07, 13, 16 |
| End-to-end change latency | watch-driven when the watcher survived; under storms the watch dropped events and healed with scans | 10–23 s stable under storm load, deletes included | 04, 08, 14 |
| Need-list queries (out-of-sync view, pull queue) | full-table scans per query (upstream reporter: 40.5 s median file iteration on a large tree) | maintained need tables; remaining iteration via `files_needed` partial index (reporter: 1.3 s) | 03, 10, 17 |
| GC on 45.5M-row blocks table | single 5-min+ transaction, interrupted, flag lost | per-chunk commits, interrupt-safe, no lock held across | 02, 11 |
| Index updates racing a writer | SQLITE_BUSY killed the device connection (~20 s flap loop, 172 errors in one session, index frozen) | 30 s busy_timeout + bounded retry; zero lock errors since dev.92 | 11, 15 |
| GUI event stream in a background tab | throttled timers stall the UI | fetch-based poll loop | 09 |

Verification status notes live per patch in the table above and in commit
messages; the pre-2026-10-15 numbers are reproduced and extended in
https://github.com/syncthing/syncthing/issues/10921.

## Not included in the clean chain

The deployed history (`feature/btrfs-scan-replay`, kept as the ops archive
branch) additionally contains: temporary debug instrumentation (two commits
marked TEMPORARY), the STGUISTATELOG UI-correlation hook, binary artifacts of
every dev build under `tmp/`, and the incident/deployment ops log. Those are
intentionally absent from `main` and the `patch/` branches.
