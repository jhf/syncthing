# Incident 2026-10-13: dev.62 first GC pass wedges sync for ~15 min

## Symptom

yarlung (dev.62, `v2.1.7-dev.62.g5fa9f75a-btrfs-scan-replay`), ~10 minutes
after boot: primary connection's in/out counters frozen (107923/243,
inBytes rising only on secondaries), sync stalled — the same visible
pattern as the 2026-10-12 phantom-connection incident, but with the
self-deadlock fix deployed.

## Evidence

- `incident-2026-10-13-yarlung-goroutines.txt` — pprof goroutine dump,
  231 goroutines, taken 11:29 UTC (~24 min uptime, ~14 min into the stall).
- `incident-2026-10-13-yarlung-connections.json` — connections snapshot
  at the same instant.

## Root cause (from the dump, goroutine 100, state `runnable`)

```
modernc.org/sqlite/lib._statPush            <- ANALYZE sampling
modernc.org/sqlite.(*conn).ExecContext
database/sql.(*Conn).ExecContext
sqlite.tidy                    db_service.go:188   <- `ANALYZE`
sqlite.(*Service).periodic.func2.1  db_service.go:172  <- inside the updateLock-holding closure
```

The periodic GC service acquired `updateLock` and held it across **tidy()**
(`ANALYZE`, `PRAGMA optimize`, `wal_checkpoint(TRUNCATE)`). ANALYZE on a
multi-million-row folder DB runs many minutes, CPU-bound (`runnable`, not
blocked) — during which every `folderDB.Update` (folder scans, incoming
index updates) parks on the mutex, and a connection dispatcher parked in
`Update` cannot finish `internalClose` if the peer dies: the 2026-10-12
phantom-connection mechanism, re-armed by a different holder.

Earlier in the same GC pass the retired-blocklist GC had run its whole
queue in ONE transaction (blocks DELETE = full scan of the largest table,
no `blocklist_hash`-first index), holding the SQLite write lock for
minutes. So the pass had two long wedge phases: write-lock phase, then
mutex phase.

Not a cross-connection write-write deadlock; the busy_timeout fix was
never the relevant lever. The GC service was the lock holder; scan,
dispatcher and Updates were victims.

## Fix (ee6906afe)

1. `tidy` runs outside `updateLock`.
2. Retired-blocklist GC processes the queue in batches
   (`gcRetiredBatchSize = 10_000`), holding `updateLock` per batch only;
   stops when a batch makes no progress (referenced hashes stay queued).
3. Schema 61: `blocks(blocklist_hash)` index so the batched blocks delete
   is index-driven instead of a full scan per batch.
4. `garbageCollectBlocklistsAndBlocksLocked` renamed (dropped the
   Unlock/defer-Lock inversion); caller must not hold the lock. Service
   holds it only around `garbageCollectOldDeletedLocked` +
   `garbageCollectNamesAndVersions`, whose NOT EXISTS predicates need it.

Teeth test `db_gc_retired_test.go`: interleave hook proves lock release
between batches and revives a hash mid-run; simulating the old single-tx
behavior fails both assertions.

## Residual risks

- `garbageCollectOldDeletedLocked` and `garbageCollectNamesAndVersions`
  still run under the service-held lock with unchunked/one-tx DELETEs;
  if either proves slow on a huge DB the same wedge pattern returns.
  Watch GC debug timings.
- First boot with schema 61 builds the blocks index one-time; on
  yarlung-size DBs expect a slow folder-open (minutes).
