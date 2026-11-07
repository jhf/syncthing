# patch/73: rescan sweep reads by name range, and a WAL backstop

## Problem

Yarlung's folder WAL grew all morning on 2026-11-07, from 527 MiB at 07:30 to 824 MiB at 10:40. The PASSIVE checkpoints kept stopping short of the end. The 07:35 one moved 134,765 of 135,207 pages.

**The reader that pinned it.** Six goroutine dumps from `/rest/debug/support` (no restart), taken 25 s apart from 10:12 to 10:15, all show the same single goroutine inside SQLite:

```text
github.com/mattn/go-sqlite3._Cfunc__sqlite3_step_internal
...AllLocalFilesWithPrefix.iterStructs...
github.com/syncthing/syncthing/lib/model.(*folder).scanSubdirsDeletedAndIgnored
github.com/syncthing/syncthing/lib/model.(*folder).scanSubdirs
github.com/syncthing/syncthing/lib/model.(*sendReceiveFolder).pullScannerRoutine.(*folder).Scan.func1
```

That is the post-pull rescan of the few paths a pull touched. One minute of model debug logging showed what it costs:

```text
Sweep chunk (entries=1 marks=0 wall=2.332928978s ... perEntry=2.035086226s)
Sweep chunk (entries=2 marks=0 wall=4.703432854s ... perEntry=2.260630593s)
```

That is about 2 s per path. `EXPLAIN QUERY PLAN` on the live database shows why:

```text
SEARCH d USING COVERING INDEX sqlite_autoindex_devices_1 (device_id=?)
SEARCH f USING INDEX files_device_name (device_idx=?)        <- every local file
SEARCH n USING INTEGER PRIMARY KEY (rowid=?)                 <- name fetched, then discarded
```

Upstream's query (unchanged at b34734376) joins `devices`, and the planner drives from `files_device_name(device_idx=?)`. So it walks all ~2.9M local files in `name_idx` order, which is insertion order, and fetches each name only to throw it away. Back-to-back sweep calls keep one read open continuously. PASSIVE checkpoints cannot pass that read, so the WAL never resets.

**Nothing bounded the WAL.** Every in-process checkpoint is PASSIVE, on purpose: a waiting checkpoint run from the writer's goroutine can wait on that goroutine's own read, which caused the 2026-10-19 stall. `journal_size_limit` only applies when the WAL resets. Only a restart shrank it: yesterday's 2320 MiB WAL went to 91 MiB across the patch/70 restart.

## Change

1. **The query** (`folderdb_local.go`, `localPrefixQuery`) now drives from the name range and does a point lookup per name:
   ```text
   SEARCH n USING COVERING INDEX sqlite_autoindex_file_names_1 (name>? AND name<?)
   SEARCH f USING INDEX files_device_name (device_idx=? AND name_idx=?)
   ```
   `CROSS JOIN` pins the join order. The device is a scalar subquery. Results are ordered by name. The string-prefix semantics are unchanged.
2. **The backstop** (`db_walbackstop.go`). After a periodic checkpoint that falls short, if the WAL file exceeds 512 MiB (4 × the 128 MiB autocheckpoint), one goroutine per database retries with backoff (5 s doubling to 5 min, WRN after 30 min):
   - an unlocked PASSIVE checkpoint, which does the slow copying and never waits;
   - only if that reached the end, a `wal_checkpoint(TRUNCATE)` under `updateLock` with a 200 ms busy timeout. It copies only frames written in between, then resets and truncates the WAL. A reader in the way means it gives up after 200 ms and tries again later.

   It never blocks the writer that triggered it, and it holds `updateLock` for at most about 200 ms. It logs `WAL over backstop limit` and then `WAL truncated by backstop`, with sizes, wait time and attempt count.

## Measured on yarlung (live DB, read-only, `qbench.py`)

Eight random recently synced paths. The order alternated per path, so neither query always got the other's warm cache:

```text
#1 rows=1    old=   2414.6 ms  new=      8.7 ms  first=new  ssb/statbus/tmp/rune-measure/README.md
#2 rows=1    old=   2589.8 ms  new=     12.5 ms  first=old  Veridit/src/seid-lang/.git/objects/b9/14de695a58fed89cebb071
#3 rows=1    old=   2354.0 ms  new=      0.1 ms  first=new  ssb/statbus_test/.git/refs/remotes/origin/master
#4 rows=1    old=   2550.7 ms  new=      0.1 ms  first=old  Projects/muninn/.git/worktrees/wt-ts/index
#5 rows=1    old=   2538.3 ms  new=      0.1 ms  first=new  Veridit/src/seid-lang/.backlog/evidence/TASK-1960/rc_probe.t
#6 rows=1    old=   2544.0 ms  new=      0.2 ms  first=old  Veridit/src/seid-lang/.git/objects/04/d1c42944b97797f47983de
#7 rows=1    old=   2435.6 ms  new=      0.1 ms  first=new  ssb/statbus_test/.git/objects/86/58baba150c33cb14959bccc7428
#8 rows=1    old=   2563.6 ms  new=      0.0 ms  first=old  Veridit/src/seid-lang/.backlog/evidence/TASK-1960/entry_def_
total old=19.99 s  new=0.02 s
```

Large prefixes are dominated by fetching their rows cold from disk under either plan, and the first runs above were order-biased. For example, `WebCore.build` with 4348 rows: new run first and cold took 28.1 s, the old run then took 2.6 s on the warm cache. The production case is the post-pull rescan of the exact paths a pull touched, which is what the table measures.

## Regression tests (fail on 74b8a490a, pass after)

- `TestAllLocalFilesWithPrefixUsesNameRange` (internal/db/sqlite). The fixture carries yarlung's real `sqlite_stat1` figures (2 devices, 12.5M names, 17.9M file rows). The test checks:
  - the result is complete and in name order;
  - the plan starts with a range search on `file_names(name)`, with no device-wide `files_device_name (device_idx=?)` and no `SCAN`.

  Before:
  ```text
  plan must start with a range search on file_names(name), got ["SCAN fi" "SEARCH f USING INTEGER PRIMARY KEY (rowid=?)" "SEARCH n USING INTEGER PRIMARY KEY (rowid=?)" ...]
  ```
- `TestWALBackstopTruncatesAfterReaderGap`. An open reader pins the WAL while 13 update rounds grow it to 44.4 MiB, with the limit set to 1 MiB for the test. No writer may block more than 5 s. After the reader closes, the WAL must fall under the limit within 10 s.
  - Before: `WAL still 44.4 MiB 10 s after the reader closed: no backstop`.
  - After: `WAL truncated by backstop (... walBytesAfter=0 waited=648.409542ms attempts=14)`.

`testdata/write_inventory.json` has been reviewed for 4 entries: the query function, its new constant, `periodicCheckpointLocked`, and `tryTruncateCheckpoint`.

Gates:
- `ok internal/db/sqlite 9.793s`, `ok lib/model 73.087s`, `ok lib/syncthing 1.236s`.
- With CGO disabled: `ok internal/db/sqlite 14.117s`.
- Race detector, 5 runs on the touched and single-writer tests: `ok 21.642s`.
- Linux cross-build passes.
