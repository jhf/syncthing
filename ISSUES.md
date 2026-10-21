# Upstream issues that matter to this fork

A working map of syncthing/syncthing issues that touch our North Star workload, kept so that
when something misbehaves on our machines we can check here first: is this a known upstream
bug, does someone have code for it, did we already adopt it?

Status as of 2026-10-21 (scout refresh: tmp/upstream-inventory-issues.md; prior sweep:
tmp/upstream-fix-sweep.md). Fork base: v2.1.7 final (d6d0181f7, rebased 2026-10-21,
zero conflicts across our 147 commits).

**How to use:** find your symptom in the index, open the issue, check "code" and "us".
"us" says what we did: adopted (with patch number), carried (via dependency pin), parked
(why), or watching.

## Symptom index

| Symptom on our machines | Issue(s) |
|---|---|
| "database is locked", SQLITE_BUSY, folder stops | #10529, #10559 |
| Connection killed while handling index updates | #10372, #10529 |
| WAL file grows unbounded, checkpoints never finish | #10559 |
| DB GC interrupts / "exceeding time limit" | #10868 |
| Crash: "runtime: out of memory" with RAM free (vm.max_map_count) | #10913 |
| Full scan takes hours with many identical files | #10869 (#10777) |
| Slow "sync-preparing" / file iteration at scale | #10902 |
| Watcher silently stops seeing a directory | #7879, #9612, #7950, #8203 |
| Mac watcher memory grows forever | #10894 |
| Scan re-walks ignored subtrees / every create triggers dir rescan | #6681, #6572 |
| Startup does a full rescan it shouldn't need | #5353 |
| "directory not empty" pull errors, resurrected files | #6615, #9982, #10414, #9766 |
| Stuck "unsynced item" after ignore timing | #9946 |
| Leftover `.tmp` file synced to peers | #10895 |
| Connections never recover after sleep/wake | PR #10814 |
| Connection gauge over-counts / dead connection not replaced after network change | #10150, #9337 |
| Mac folder stuck at startup, API silent | #10876 |
| Phantom "Local Additions" on the receiver | #10574, #10599 |
| High RAM during big pulls | #4976 |
| Out-of-sync counts that both sides deny | #8735, #7474 |
| Model deadlock under DB pool exhaustion | #10798 |

## Database / SQLite

### #10529 — Consider better reaction to SQLITE_BUSY — OPEN
Vanilla hits SQLITE_BUSY and stops the folder. Root cause in comments: writes serialized
under updateLock (incl. wal_checkpoint), reads not; RESTART checkpoint can BUSY readers.
tbodt: "Simply add `busy_timeout = 5000` here".
**code:** one-liner in issue comments. **us: adopted** — patch/11 (busy_timeout) and
patch/15 (30s timeout + lock-contention retry in the index handler).
https://github.com/syncthing/syncthing/issues/10529

### #10559 — Checkpoint starvation — OPEN
WAL never truncates: reads in quick succession leave no gap for wal_checkpoint(TRUNCATE);
positive feedback loop (bigger WAL → slower readers → fewer gaps). 11.6 GiB folder WAL
measured by schaurian. dmih: fixing this probably also fixes #10529.
**code:** none merged; see PR #10556. **us: covered for our workload** — patch/40 couples the
WAL autocheckpoint threshold to the probed CoW state (128 MiB under NOCOW: checkpoints are
in-place and cheap, so the threshold's remaining jobs are replay time and WAL size), and
patch/44 moved the database into a nested NOCOW subvolume so our own baseline snapshots no
longer re-share its extents. Live 2026-10-21: zero .wal files at rest (checkpoints complete),
main.db 1 extent. Still watching upstream's general fix — the starvation loop itself is
theirs to close.
https://github.com/syncthing/syncthing/issues/10559

### #10868 — Frequent GC when dealing with a large number of small files — OPEN
"GC was interrupted due to exceeding time limit (processed=141 runtime=5m table=blocks
rows=45,542,293)". We commented with our measurements: the actual orphan set was 46 block
rows — GC never gets there.
**code:** none upstream. **us: fixed for ourselves** — patch/02 (GC rewrite).
https://github.com/syncthing/syncthing/issues/10868

### #10913 — OOM via vm.max_map_count: ~19k 64 KiB SQLite-allocator mappings — OPEN
Crash during folder scan with 9 GiB free: the modernc.org/sqlite allocator accumulates tens
of thousands of small anonymous mappings until mmap fails. Static binary (no cgo). Two
panics attached, one in lib/scanner walk growslice.
**code:** PR #10915 (merged upstream 2026-10-07, caps unbounded prepared statements) is a
PARTIAL fix — issue remains open. In our chain via the v2.1.7 final base (verified in-tree
2026-10-21).
**us: watching** — yarlung runs long uptimes and huge scans on exactly this allocator.
If our linux-amd64 build crashes with "out of memory" and RAM free: check
`wc -l /proc/$(pidof syncthing)/maps` against 65530.
https://github.com/syncthing/syncthing/issues/10913

### #10798 — promoteConnections holds m.mut waiting for a SQLite connection — CLOSED
Receiver stalled 1,954 minutes; 69k goroutines. One exhausted folder DB pool blocked
unrelated model operations.
**code:** PR #10816 (closed unmerged) with a regression test. Issue closed upstream,
possibly fixed another way — unverified.
**us: watching** — if our receiver ever stalls with goroutine/fd explosion, start here and
check whether the fix actually landed in our base.
https://github.com/syncthing/syncthing/issues/10798

### #10372 — Repeated "Lost device connection" shortly after startup — OPEN
Multi-version connection loop; error mutates over time, currently "handling index: no such
folder". Index-handling kills the connection — the exact failure class patch/15 armors
against.
**Root cause demonstrated 2026-10-15 (VoDmAl, goroutine profiles attached):** on the peer
with a large folder, `dispatcherLoop → handleIndexUpdate → indexHandler.receive →
folderDB.Update` waits for the folder DB mutex while
`garbageCollectBlocklistsAndBlocksLocked ← (*Service).periodic` holds it for 5-12 minutes.
Each new connection is "additional", the old index handler is abandoned, and the far side
sees "folder is not running" — the churn loop. Waves follow the 8h DBMaintenanceInterval.
That is precisely the hazard patch/11 removes: our GC commits and releases the folder lock
per chunk, so index updates are never wedged behind it.
**us: likely fixed** (patch/11 GC lock scope + chunked commit, patch/15 lock-contention
retry). Remaining sub-issue from the same analysis: a connection whose socket is gone is
kept as the device's primary — see #10150/#9337.
https://github.com/syncthing/syncthing/issues/10372

### #10264 — SQLite database doesn't scale — OPEN
blocks/blocklists insert ~350 blocks/s in bad cases; ZFS sync interaction (fixed in ZFS
2.4.0); gaby's draft parallel walkers (gaby/syncthing#8).
**us: watching** — our chunked-scan work (patch/04) attacks the same wall differently.
https://github.com/syncthing/syncthing/issues/10264

## Scanning

### #10869 — Regression since 2.1.0: very slow scanning with identical files — OPEN
Successor to #10777 (closed, unfixable-per-reporter). Rename tracking (PR #10653, in our
base) made findRepeat... `findRename` repeat `IsDeleted` Lstats on same-BlocksHash files →
near-quadratic scans (one user projected >14 days). PR #10833 (merged, in our base) fixes
the 400+400-file case but NOT across update-batch boundaries (SoongVilda's benchmarks).
Reporter needed PR #10849 (the support-bundle metrics fix, merged 2026-08-05, in our base)
before supplying new diagnostics — #10849 is tooling, not a scan fix.
**code:** #10833 (partial, in base).
**us: watching closely** — our headline feature is scan speed on 2.38M files; if our scan
times regress after adding many identical files, this is why.
https://github.com/syncthing/syncthing/issues/10869

### #10902 — File iteration can take too long for too many files — OPEN
Single missing DB index on files(local_flags): reporter measured file iteration
40.5s → 1.3s, sync-preparing 25-35s → 9ms. Branch `MahdiBM:mmbm-faster-file-iter`.
**us: ADOPTED as patch/17** (mutation-verified teeth).
https://github.com/syncthing/syncthing/issues/10902

### #6572 — Non-recursive scans for directory change events — OPEN
Every file create triggers a full recursive scan of the parent dir. calmh agrees it's
"rather crap". Edge cases around moved-in trees.
**us: adjacent** — patch/04 chunking bounds the cost differently. Watching.
https://github.com/syncthing/syncthing/issues/6572

### #6681 — Smarter subtree pruning with ignore exclusions — OPEN
Watcher/walker traverse huge fully-ignored subtrees, burning I/O.
**us: watching** — our .stignore carries big ignored subtrees.
https://github.com/syncthing/syncthing/issues/6681

### #5353 — Skip full rescan on startup if interval not exceeded — OPEN
calmh: only safe when watcher disabled — needs a scan baseline.
**us: we built the thing this issue wants** — our btrfs baseline pairing is exactly a
provable scan baseline (for btrfs receivers). Watching for upstream's portable version.
https://github.com/syncthing/syncthing/issues/5353

### #10554 — Break up incremental vacuum — OPEN
The issue PR #10556 addresses: loop-based incremental_vacuum(500) + PASSIVE checkpoints so
vacuum doesn't hold the WAL hostage (see #10559).
**us: parked** with #10556 (see PRs.md).
https://github.com/syncthing/syncthing/issues/10554

### #4976 — Excessive RAM usage when syncing large folders — OPEN (calmh)
Pull queue for millions of files held in memory, sorted by size/time/random. Proposal: cap
the queue, iterate the DB in passes. See also imsodin's draft PR #10183 (pull queues in the
DB).
**us: watching** — directly our scale; if the Mac's resident set climbs during big pulls,
this is the upstream conversation.
https://github.com/syncthing/syncthing/issues/4976

### #8418 — Use concurrency in scanner.Walk — OPEN
calmh (quoted): "The database supports concurrent reads, and the filesystem supports
concurrent stats ... one walk routine that feeds filenames to a queue and multiple
processing routines."
**us: watching** — philosophical support for our chunked-parallel scan direction
(patch/04); no upstream code.
https://github.com/syncthing/syncthing/issues/8418

## Watching / fs events

### #10894 — macOS FSEvents watcher memory grows without bound — OPEN
watch.prev grows forever on churn (3.5 GB live heap measured), including ignored paths.
Fix branch fluffypony/notify `fsevents-bounded-history` (commit fbdebde, caps history at
2×8192 paths).
**us: carried** — the pinned notify fork in our go.mod includes this fix since the dev.44
era; we additionally replaced the live stream with poll mode (patch/14).
https://github.com/syncthing/syncthing/issues/10894

### #7879 / #9612 — Watcher misses recreated directories — OPEN (both)
Delete a dir, recreate it (or rename away and recreate the name) → its contents are never
watched again until restart. Confirmed on 2.1.5 with kernel evidence (only one inotify
watch left). Likely rjeczalik/notify#200/#227-adjacent.
**us: parked** — real bug class, not our observed symptom; if yarlung ever goes quiet on a
recreated tree, suspect this and fix in the notify fork like #10894.
https://github.com/syncthing/syncthing/issues/7879
https://github.com/syncthing/syncthing/issues/9612

### #7950 — Wrong event paths after rename-away-and-back — OPEN
mkdir b; mv b c; mv c b; touch b/foo → aggregator tracks the stale path. May share the
#7879 root cause.
**us: parked** (same bucket).
https://github.com/syncthing/syncthing/issues/7950

### #8203 — Watcher never re-armed after transient permission denial — OPEN since 2022
Brief permission denial at dir creation → never watched until rescan/restart.
**us: parked.**
https://github.com/syncthing/syncthing/issues/8203

## Pulling / sync correctness

### #6615 / #9982 / #9766 — "directory not empty" and resurrected files — OPEN/OPEN/CLOSED
Parent dir errors cascade from child errors (ignored children, leftover .sync-conflict
files); the dir gets "put back" and deleted files reappear. #9766 (type-mismatch delete
loop) closed without a fix reference — recheck if seen.
**us: watching** — closest upstream match to our zig-toolchain leftover-temp-file incident.
https://github.com/syncthing/syncthing/issues/6615
https://github.com/syncthing/syncthing/issues/9982
https://github.com/syncthing/syncthing/issues/9766

### #10414 — Trash Can versioning vs file→dir name reuse — OPEN
Versioning into .stversions collides when a dir reuses a deleted file's name → resurrected
files / "Failed to sync".
**us: not applicable** (we don't run Trash Can versioning) — listed so we remember why.
https://github.com/syncthing/syncthing/issues/10414

### #9946 — .stignore timing → permanently stuck unsynced item — OPEN
Create folder, ignore it, remove it with the right timing → stuck forever; "a restart fixes
this mostly. … mostly."
**us: watching** — we edit ignores regularly (see .stignore-shared history).
https://github.com/syncthing/syncthing/issues/9946

### #10895 — UTF8 normalization failure leaves and propagates `<name>.tmp` — OPEN
applyNormalization returns tempPath on a failed second rename → mangled temp name indexed
and synced. Two proposed fixes in comments (bunnysayzz rollback; dajiaohuang branch
`fix/utf8-normalize-rename-rollback-casefs-cache`).
**us: watching** — leftover-file class; if a stray `.tmp` ever propagates, pick a fix here.
https://github.com/syncthing/syncthing/issues/10895

## Connections / state reporting

### #10150 — Connection accounting doesn't properly account closed connections — OPEN (calmh)
Gauge over-counts ("it does not have 64 open connections; it has 8"); calmh: "may be a
symptom of a more serious problem in connection close handling".
**code:** calmh/syncthing@7ac09883 (lib/connections/service.go).
**us: watching** — if our connection counts ever look inflated, the fix commit is linked.
https://github.com/syncthing/syncthing/issues/10150

### #9337 — New same-priority connections not accepted until long timeout (>5 min) — OPEN
VPN→LAN address change keeps the dead connection: "rejected: already connected to this
device".
**us: watching** — relevant to any WAN path change between our endpoints.
https://github.com/syncthing/syncthing/issues/9337

### #10876 — macOS TCC denial hangs folder init forever, /rest/db endpoints hang — OPEN
If macOS privacy permission is missing, folder initialization hangs with no error logged.
**us: watching** — Mac-side failure mode; if our GUI ever shows a folder stuck at startup
with silent API, check System Settings → Privacy permissions first.
https://github.com/syncthing/syncthing/issues/10876

### #10574 / #10599 — Receive-only mtime confusion — OPEN (both)
Self-induced dir mtime changes during sync get detected as "Local Additions"; wrong mtimes
after rename-then-set-mtime on error paths.
**us: watching** — ours are send-receive, but yarlung is effectively receive-mostly; if it
ever reports phantom local additions, start here.
https://github.com/syncthing/syncthing/issues/10574
https://github.com/syncthing/syncthing/issues/10599

### #8735 / #7474 — Negated ignores + ignored parents → divergent local/global state — OPEN
Whitelist-style ignores make identical configs disagree about out-of-sync; with
receive-only folders, "likely leads to broken global state … both sides claim in sync".
**us: watching** — our ignore file is additive-only (no `!` negations), so we're clear;
keep it that way.
https://github.com/syncthing/syncthing/issues/8735

## Maintenance

Refresh procedure: re-run the scout prompt (session history: "upstream inventory scout"),
which rewrites tmp/upstream-inventory-issues.md with fresh states; then update this file's
status lines. Sweep method and query gaps: tmp/upstream-fix-sweep.md. Adoptions are tracked
in PATCHES.md; open-PR landscape in PRs.md.
