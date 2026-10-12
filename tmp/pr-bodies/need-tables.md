# PR: db/sqlite: incrementally maintained need tables

Branch: upstream/need-tables (3 commits on main)

## Symptom

On a folder with 2.8M file names (5.1M global rows in the sqlite backend), every
caller that asks "what does device X need" pays a correlated NOT EXISTS scan of
the whole files table. Measured against the live daemon (fully in sync, idle):

- `GET /rest/db/need?folder=home&perpage=1` — 9.28 s (9.3-9.7 s stable)
- `GET /rest/db/remoteneed?...&perpage=100` — 21.98 / 21.80 s
- the same query standalone against the db file, no daemon: 11.6 s warm,
  19.7 s cold

These run on every folder summary pass, every GUI poll of the out-of-sync
view, and every remote-completion calculation — so the GUI's folder state and
"Out of Sync Items" modal are seconds-to-tens-of-seconds stale on large
folders even when nothing is happening.

## Why query tuning can't fix it

We benchmarked the alternatives on a snapshot of the live db: tuple NOT IN,
LEFT JOIN, and covering indexes all land within ~30% of the baseline, because
every shape still visits all 5.1M rows. The information has to be maintained,
not recomputed.

## Fix

Maintain the answer incrementally:

- `need_detail(device_idx, name_idx, type, size, deleted)` — one row per
  (device, file) the device needs. Updated per file name at the end of
  `recalcGlobalForFile`, which already loads every device's row for that name,
  so maintenance is a handful of row ops inside the existing update
  transaction.
- `need_totals(device_idx, type, deleted, count, size)` — aggregates for an
  O(1) `CountNeed`.
- `need_build_journal(name_idx)` — names touched by updates while the
  one-time background build runs; drained under the update lock before the
  built flag flips.
- `need_devices(device_idx)` — devices with complete coverage; a device first
  seen after the build is backfilled in `Update` before its batch lands.

Reads gate on `needBuilt && needDeviceKnown` and otherwise fall back to the
existing full-scan queries, so a build failure or a DropDevice/DropAllFiles
reset degrades to today's behavior instead of returning wrong numbers.
DropDevice/DropAllFiles bump a generation counter that aborts an in-flight
builder.

## Numbers

On the live 2.8M-name folder snapshot:

- CountNeed: 2.7-3.1 s -> 38-100 µs (~70,000x), byte-identical to the
  full-scan oracle, including a simulated 100k-row divergence (99,920 detail
  rows, counts match exactly).
- One-time background build: 17-21 s in 100k-name chunks of short
  transactions (never holds the update lock except the final journal drain).
  On first production boot 474 updates were journaled mid-build and correctly
  re-derived.
- Live API after deploy: `/rest/db/remoteneed?perpage=1` answers in 15 ms
  (was ~22 s).

## Tests

- `TestNeedTrackingEquivalence`: maintained tables vs full-scan oracle across
  updates, version changes, deletes, late-joining device backfill,
  DropAllFiles and DropDevice resets.
- `TestNeedTrackingJournalOnLateBuild`: updates landing mid-build are
  journaled and reflected.
- `TestNeedTrackingRealDB` (env-gated on SYNCTHING_NEEDBENCH_DB): real-data
  equivalence + the timings above.

## Preceding commits in this PR

- `perf(db): stop ordering need lists by name in SQL` — ordering by name makes
  SQLite walk every name in the folder; callers sort in memory instead.
- `perf(model): stop ordering the send-only need list in SQL` — same.
## Companion commit: sqlite busy_timeout

The background build writes its 100k-name chunks in transactions that do not
hold `updateLock`, so during the first post-upgrade boot the build is a second
writer on the folder database file. On modernc (nocgo) builds `busy_timeout`
defaults to zero: an incoming index update whose first statement raced a live
build chunk failed instantly with SQLITE_BUSY, which the protocol layer treats
as connection-fatal. Observed live on a Linux receiver running this branch:
six consecutive connections killed in 13 s (`handling index-update for home:
update: deviceidxlocked: database is locked`) during the initial build. The
final commit in this PR sets `busy_timeout(5000)` on both driver DSNs (the
mattn cgo driver already has a nonzero default), with teeth-verified tests:
removed, they fail with `database is locked (5) (SQLITE_BUSY)`.

## Post-review fix: GC self-deadlock (caught by production telemetry)

The first live run of this branch deadlocked within hours: the periodic GC
calls `garbageCollectOldDeletedLocked` with `updateLock` already held, and
the need-tables branch of that function re-acquired it. sync.Mutex is not
reentrant, so the GC service deadlocked against itself while holding the
lock; every later `Update` (folder scans, incoming index updates) wedged
behind it, and a peer connection whose dispatcher was parked in `Update`
when the peer went away could not finish `internalClose`, leaving a phantom
"primary" connection (frozen counters, no kernel socket) and a silent sync
stall. Diagnosed from a support-bundle goroutine dump: GC service, folder
scan, and connection dispatcher all parked on the same mutex, the holder
being the GC goroutine itself. The fix (no re-lock; the caller holds it) is
squashed into the main commit, with a regression test that deadlocks on
timeout when the re-lock is restored.
