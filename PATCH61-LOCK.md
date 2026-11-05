# Patch/61 SQLite lock investigation

## Scope and verdict

Read-only investigation at `7f2db730b6af15de21468b83a8b926e09fff457c`, 2026-11-05 UTC. Only this report is changed. No production edits, pushes, deployments, restarts, database queries, or remote writes were performed. Remote access was `ssh -o BatchMode=yes jhf@yarlung.veridit.no` to list and read the specified logs.

**The logs cannot identify the actual holder.** The strongest long-duration code candidate is **unlocked maintenance `tidy`**, specifically full `ANALYZE` at `internal/db/sqlite/db_service.go:201`. It writes statistics while scanning the database, without the folder's `updateLock`. The same function also runs an unlocked TRUNCATE checkpoint at line 205. Background need-build chunks at `folderdb_need.go:369-410` are another genuine concurrent writer. There is no evidence sufficient to assign any of the 34 events to either candidate. In particular, pull churn, scan commits, and index-retry messages are not evidence that those operations own the SQLite writer lock.

The long incident is compatible with one long ANALYZE, but does not prove it: 20 folder errors from 2026-10-29 21:53:12 through 22:21:14, plus mtime begin failures immediately before it. The source comment at `db_service.go:182-185` reports a prior many-minute ANALYZE on yarlung. That is historical source commentary, not a measurement of this incident. Do not call this a confirmed ANALYZE diagnosis.

## Verification, verbatim results

Commands ran sequentially, in the requested order. Environment: `GOOS=darwin`, `GOARCH=arm64`, `CGO_ENABLED=1`, empty `GOFLAGS`. These are local cgo checks, not a reproduction of yarlung storage latency.

1. `go build ./...`: no stdout/stderr, exit status 0. Harness result:

```text
go build ./... exit=0
```

2. `go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1`:

```text
ok  	github.com/syncthing/syncthing/lib/model	71.141s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	9.765s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.588s
```

3. `go test ./lib/model -run '^(TestDeleteConflictLongNameCurrentlyStaysNeeded|TestConflictName.*|TestConflictLongNamePruning|TestPullDBBusy.*)$' -race -count=20`:

```text
ok  	github.com/syncthing/syncthing/lib/model	10.190s
```

The expression covers all tests in `folder_sendrecv_longname_test.go` and `folder_sendrecv_dbbusy_test.go`, including `TestConflictNameByteLimit`. Each top-level test executes 20 times. No race diagnostics or test failures. Package-test and race-test harness exit statuses were both 0.

## Lock model and complete production write-path inventory

All locations below are relative to `internal/db/sqlite/` unless qualified. Both driver DSNs set immediate transactions and 30000 ms busy timeout (`db_open_cgo.go:30`, `db_open_nocgo.go:23`). `baseDB.updateLock` (`basedb.go:43`) is **per baseDB/file**, not global to the process. `Update` acquires it at `folderdb_update.go:38`, then `deviceIdxLocked` executes an UPSERT at `folderdb_open.go:146-159`, before the index transaction begins (`folderdb_update.go:48`). Even a previously known device takes a SQLite write lock. Therefore `update: deviceidxlocked: database is locked` identifies the failing writer, not the competing holder.

A 30-second busy timeout is a wait budget, not a measured duration in these timestamp-only logs, and generally not proof that every SQLITE_BUSY error waited the full budget. For this first autocommit UPSERT, the evidence is consistent with exhaustion waiting for another SQLite writer. It is not a read-to-write snapshot-upgrade failure inside the later Update transaction.

| Entry path | File and lines | Same-file updateLock? | Lock lifetime and 30-second potential |
|---|---|---|---|
| Local scan, pull completion, remote index update | `folderdb_update.go:37-206` | Yes | Device UPSERT followed by immediate batch transaction. Names, versions, files, fileinfos, blocklists, blocks, index sequence, global/need rows, retirement queue, journal and SQL-trigger effects all belong to this transaction. Backfill for a new remote device (`folderdb_need.go:253-288`) can be large. No wall-clock bound, so can exceed 30 s, but another writer honoring this same mutex waits in Go, not at SQLite. |
| Device/file removal, global rebuild | `folderdb_update.go:275-437` (`DropDevice`, `DropAllFiles`, `DropFilesNamed`) | Yes | Transactions can cascade and recalculate many rows. No duration cap. Need-table reset, recalc and retirement helper writes are part of these parent transactions, not independent connections. |
| Block index drop and population | `folderdb_update.go:448-545` | Yes | DELETE / full population transaction can exceed 30 s. Serialized against Update. |
| Index ID initialization/set/drop | `folderdb_indexid.go:36-88` | Yes | UPSERT/DELETE and device lookup, potentially scanning files for initial sequence. Not a concurrent unlocked holder. |
| Mtime batch flush, both timer and threshold/close paths | `folderdb_mtimes.go:79-133`, especially 95-98 | **Yes** | Up to a normal 512-operation threshold, plus concurrent queued work, with one immediate transaction. Slow I/O can exceed 30 s, no transaction deadline. The timer being a separate goroutine does **not** bypass updateLock. Its failed begin log identifies another victim. |
| Need builder range chunks | `folderdb_need.go:44-47,325-410` | **No** | Background goroutine, each 100000-name range runs two INSERT SELECTs across devices in one immediate transaction on its own pool connection. Chunk size bounds names, not elapsed time or output rows. Can hold writer lock beyond 30 s on HDD/large device cross-product. The builder has no per-chunk deadline. |
| Need builder final pass | `folderdb_need.go:417-481` | Yes | Aggregate rebuild, journal drain, device-known rows and completion flag in one transaction. Can exceed 30 s, but is mutex-serialized. Crucially the chunk commits before the builder tries to acquire updateLock for this final pass. |
| Maintenance old-deletion GC, names and versions | `db_service.go:167-175,209-317` | Yes, caller holds it | Long DELETEs, trigger cascades and global recalculation can exceed 30 s. Helper name alone does not establish locking, caller does. |
| Retired blocklist queue GC and fallback full GC | `db_service.go:324-583`, especially 403-469 and 546-576 | Yes, per batch/chunk | Dedicated `Connx`, foreign keys temporarily disabled. Each write transaction still takes the same folder mutex. Five-minute GC runtime limit is checked between work units, not a hard SQL deadline. Can stall updates for >30 s but does not explain same-object Update failing in SQLite. |
| Periodic `tidy` | `db_service.go:135-138,182-205` | **No** | Dedicated `Conn`, full ANALYZE, optimize (may analyze), incremental_vacuum, journal_size_limit, TRUNCATE checkpoint. ANALYZE and vacuum-related writes can exceed 30 s. TRUNCATE takes the SQLite writer lock and can wait on pinned readers while excluding new writers. Busy timeout limits lock waits, not the total duration of copying/syncing pages. Errors and result rows are discarded here. This is the strongest recurring long-writer candidate. |
| KV writes | `db_kv.go:27-43` | Yes, of the receiving baseDB | `PutKV`/`DeleteKV` cover folder metadata, GC metadata and main configuration/statistics/service timestamps. Autocommit writes can be I/O-delayed, but do not bypass the mutex. |
| Folder registry create/update/drop | `db_folderdb.go:66-78`, `folderdb_update.go:671-685`, `db_update.go:22-38` | Yes, **main DB** | Main `folders` rows use main.db's mutex and WAL, not the folder WAL. Drop closes/removes folder handles, not an attached multi-file SQLite transaction. |
| Schema initialization, migrations and post-migration maintenance | `basedb.go:97-100,126-215,294-305,325-378` | No, initialization before publishing object | Dedicated schema connection and transaction, PRAGMA setup, embedded DDL/DML, schema-version INSERT. Followed by VACUUM/optimize/TRUNCATE after migrations. Can take minutes, but not an ordinary concurrent writer on a live, published folder object. `folderdb_open.go:100-101` also initializes local device via UPSERT before publication. |
| Bulk migration opener | `db_open.go:117-150`, `folderdb_open.go:108-143` | Initialization, exclusive SQLite mode | Separate one-connection migration configuration with journal OFF and locking EXCLUSIVE. Offline migration path, not normal live operation. |
| Explicit update checkpoint | `folderdb_update.go:202-206,746-801` | Yes, caller still holds it | Separate connection, but **PASSIVE**, not FULL/RESTART/TRUNCATE. PASSIVE does not take the SQLite writer lock to exclude writers. It may take real time doing I/O, despite the optimistic source comment saying it never blocks. It can hold updateLock during that I/O, not independently cause a competing same-mutex Update to time out. journal_size_limit is not a hard WAL cap while readers pin frames. |
| Automatic WAL checkpoints / last-connection cleanup | `db_open_cgo.go:49-55`, `db_open_nocgo.go:37`, `db_walcheckpoint.go:43-70` | In the originating connection's operation | Autocheckpoints are PASSIVE, not an independent FULL/TRUNCATE writer. Close/SQLite cleanup can need locks but normally occurs only when folder service shuts down. Not shown at incident times. |
| NOCOW copy, defrag, subvolume preparation | `basedb.go:54-74` | No live SQL connection yet | Filesystem work before `sqlx.Open`, not an active SQLite write transaction competing with the published folder object. Startup logs on October 20 do not establish maintenance at any later incident. |

`stmt.Get` can write (`RETURNING`), so the inventory is not limited to `.Exec` calls. SQL triggers in `sql/schema` and migration scripts execute inside their parent statement/transaction, not on a secret independent connection. Statistics/need queries and file iterators can pin read snapshots but do not themselves take the WAL writer lock. `db_stats.go` and `debug.go` add no independent writer path.

`main.db` is opened at `db_open.go:88-89`. Folder objects are cached under `folderDBsMut` (`db_folderdb.go:56-98`), each with its own `baseDB`, SQL pool and filename. Multiple connections to a folder file contend on its SQLite locks even when only some honor its Go mutex. A write to main.db or a different folder file cannot directly hold this folder's SQLite writer lock, though shared disk pressure can lengthen work. No production ATTACH path was found. A second process or separately opened object accessing the same file could bypass the mutex too. The logs do not rule that out, and no database was opened to investigate it.

Incident-era check: startup log identifies `b1e06359` at 2026-10-28 12:05:28, before the October 29 run. `git show b1e06359:internal/db/sqlite/db_service.go` and `.../folderdb_need.go` confirm the same unlocked tidy and need-chunk paths. This is not an assumption that patch/61 was already running during the historical incidents.

SQLite primary documentation consulted for semantics: `https://www.sqlite.org/c3ref/wal_checkpoint_v2.html` (PASSIVE versus FULL/RESTART/TRUNCATE writer locks and busy handling), `https://www.sqlite.org/lang_analyze.html` (statistics writes and unrestricted ANALYZE), and `https://www.sqlite.org/pragma.html` (busy timeout). These explain semantics, not which process held a historical lock.

## Log window method and classification

Read `~/.local/state/syncthing/custom-launcher.log` and all three existing `custom-serve*.log` files, without changing remote files. Local snapshot is `/Users/jhf/.jcode/scratch/patch61/remote-logs.txt`: 4637594 lines, 1379411709 bytes, including four initial `ls` output lines. Launcher ends on November 4. Rotated serve files cover subsequent data. Concatenation order is launcher, serve.0, serve.1, serve.log, so windows are selected by parsed timestamps, **not adjacency or sort order**.

Found 34 matching `Error on folder ... database is locked` records since October 27, all for `home`. **Count correction:** the specified October 29 21:53-22:21 UTC run contains 20 matching events in the retrieved logs, not 21. October 29 has 21 total when its separate 12:41:59 event is included. For each, extracted every parseable timestamped log line in the inclusive lower, exclusive upper interval `[error - 60 s, error)`, preceding the event in its source. This includes earlier error records if their timestamp falls in that interval. Second-resolution logging cannot order records within a second precisely. Windows contain 840 lines including intentional overlap between windows. UTC follows the incident timestamps and launcher UTC correspondence. The unabridged lines, source line numbers and extraction script are retained locally in `windows.txt`, `windows-summary.txt`, `analyze.py` under that scratch directory. No large raw log was added to the repository.

Classification is of **observed activity**, not holder attribution. D = directory recreation/pull churn, P = file sync/metadata/pull churn, I = remote-index contention retry, M = mtime begin contention, B = explicitly committed metabatch, Q = no lines. Multiple labels are allowed.

| UTC error timestamp | Preceding lines | Observed classes and evidence |
|---|---:|---|
| 2026-10-27 16:05:00 | 0 | Q |
| 2026-10-28 10:04:22 | 1 | I |
| 2026-10-28 19:08:56 | 8 | P, I |
| 2026-10-29 12:41:59 | 0 | Q |
| 2026-10-29 21:53:12 | 2 | M, two failed begins |
| 2026-10-29 21:55:12 | 39 | D, 28 directory messages |
| 2026-10-29 21:57:12 | 38 | D, 28 directory messages |
| 2026-10-29 21:58:12 | 48 | D, 28 directory messages, connection changes and prior folder error |
| 2026-10-29 22:00:12 | 39 | D, 28 directory messages |
| 2026-10-29 22:01:13 | 12 | P, I, churn without directory messages |
| 2026-10-29 22:02:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:04:13 | 38 | D, 28 directory messages |
| 2026-10-29 22:05:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:07:13 | 38 | D, 28 directory messages |
| 2026-10-29 22:09:13 | 41 | D, I, 28 directory messages |
| 2026-10-29 22:10:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:12:13 | 38 | D, 28 directory messages |
| 2026-10-29 22:13:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:14:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:15:13 | 49 | D, 28 directory messages, connection changes and prior folder error |
| 2026-10-29 22:16:13 | 44 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:18:13 | 38 | D, 28 directory messages |
| 2026-10-29 22:19:13 | 43 | D, 28 directory messages and prior folder error |
| 2026-10-29 22:21:14 | 38 | D, 28 directory messages |
| 2026-10-30 06:33:39 | 8 | P, 2 synced files and 1 metadata message |
| 2026-10-31 08:51:52 | 3 | B, 2 committed metabatch messages, folder retry |
| 2026-11-01 02:43:14 | 0 | Q |
| 2026-11-01 11:48:22 | 7 | P, I |
| 2026-11-02 14:29:54 | 1 | M |
| 2026-11-02 14:31:24 | 12 | P, M |
| 2026-11-02 23:04:09 | 13 | P, 4 synced files, 2 metadata messages, connection loss and failed sync |
| 2026-11-03 08:14:48 | 11 | P, I, 3 synced files and 1 metadata message |
| 2026-11-03 17:17:06 | 2 | M, two failed begins |
| 2026-11-05 21:33:51 | 14 | P, I, 1 synced file, failed sync and folder retries |

Counts per observed class, nonexclusive: **D 18, P 8, I 7, M 4, B 1, Q 3**. Exclusive partition: 18 directory/churn windows, 5 other pull/churn windows with index contention, 2 other pull/churn windows without index/mtime contention, 1 pull/churn window with mtime contention, 1 index-only window, 3 mtime-only windows, 1 metabatch window, 3 empty windows = 34. Index contention also appears in one of the directory windows. No class establishes the holder.

Confirmed holder-operation observations across the windows: **checkpoint 0, need-table build 0, scan commit 0, large index batch 0, defrag 0, periodic maintenance 0**. These are counts of explicit messages, not claims those activities were absent. Logging is insufficient: `Periodic start/done` and checkpoint messages are Debug, tidy has no per-statement Info logs, and need chunks have no start/duration messages. Path names containing “maintenance” inside churn samples are not maintenance-operation evidence. Index-retry messages establish a waiting writer and sometimes a batch size, not that the batch is large or owns a lock.

### Selected exact log evidence

Line numbers below refer to the local concatenated snapshot, not original remote-file line numbers.

```text
4056014:2026-10-29 21:52:12 WRN Mtime batch flush failed to begin (error="database is locked" log.pkg=db/sqlite)
4056015:2026-10-29 21:52:42 WRN Mtime batch flush failed to begin (error="database is locked" log.pkg=db/sqlite)
4056017:2026-10-29 21:53:12 WRN Error on folder (error="update: deviceidxlocked: database is locked" folder.id=home folder.type=sendreceive log.pkg=model)
4299149:2026-11-02 23:03:38 INF Synced file (folder.id=home folder.type=sendreceive file.name=ssb/jsonb_stats/src/delta.rs file.modified="2026-11-02 23:02:18.630289644 +0000 UTC" file.permissions=0644 file.size=27405 file.blocksize=131072 blocks.local=0 blocks.download=1 log.pkg=model)
4299154:2026-11-02 23:04:09 WRN Error on folder (error="update: deviceidxlocked: database is locked" folder.id=home folder.type=sendreceive log.pkg=model)
4581874:2026-11-05 21:33:21 INF Index update hit database lock contention, retrying (folder=home device=YMIQQYL files=2 log.pkg=model)
4581876:2026-11-05 21:33:51 WRN Error on folder (error="update: deviceidxlocked: database is locked" folder.id=home folder.type=sendreceive log.pkg=model)
```

The mtime begin failures and the later two-file index retry are independent victims consistent with an unmutexed SQLite holder. They do not identify which holder. Successful file work 31 seconds before the November 2 error also does not measure the competing transaction. A run of repeated errors does not prove a single uninterrupted 28-minute transaction.

## The single missing diagnostic

The most useful single line at a failed write would be an active-writer snapshot, for example:

```text
SQLite writer wait expired db=folder.0001-ra43td7w.db waiter=Update/deviceIdx owner=tidy/ANALYZE owner_conn=17 owner_since=<UTC> owner_elapsed_ms=<n> owner_holds_updateLock=false
```

This is a **proposed diagnostic, not existing output**. To make it truthful, track operation/connection/file and monotonic acquisition time after acquiring each write transaction, including ANALYZE/vacuum and TRUNCATE, and clear it on release. Snapshot the tracked active operation when a wait expires. Label it unknown if no tracked in-process holder is present. Merely timing `Update` or counting SQLITE_BUSY will not identify a competing connection. A start/end log around every individual tidy statement and need chunk is a simpler approximation, but “statement started” alone can mean that statement is itself waiting, not holding a lock. No instrumentation was added in this task.

## Unbounded patch/61 retry and deadlock assessment

**No intrinsic deadlock against ANALYZE or a need-build chunk was found. Unbounded retry is nevertheless not a guarantee of progress, and a literal uncancellable infinite loop could wedge the pull/shutdown indefinitely.**

At `lib/model/folder_sendrecv.go:2265-2279`, retries happen after `updateLocalsFromPulling` returns. `folderdb.Update` releases updateLock on every return (`folderdb_update.go:38-39`) and rolls back any unsuccessful transaction (`:52`). The mutex is not held across the retry sleep. `deviceIdxLocked` fails before the batch transaction, so this error cannot leave that Update holding an open batch write transaction.

* **ANALYZE / incremental vacuum:** `tidy` uses its own connection and does not wait for the pull-completion channel or updateLock. Retrying cannot prevent its SQL work from completing. After ANALYZE releases its write transaction, the waiting batch can try again. Severe I/O hangs, unfair repeated acquisition, or an external holder can still cause indefinite waiting, but that is not an identified application lock cycle.
* **Need-build chunk:** its SQL/commit path has no dependency on draining pull completions. It commits or rolls back before reaching final-pass updateLock acquisition (`folderdb_need.go:409-417`). There is no “builder owns SQLite writer lock, waits for updateLock held by retrying pull” inversion across those phases. Final-pass contention is serialized by updateLock.
* **TRUNCATE checkpoint:** a subtler bounded wait cycle is possible. A pull/need iterator may pin a reader while its downstream producer blocks on `dbUpdateChan`; an indefinitely retrying updater can stop draining that channel (`folder_sendrecv.go:2295-2334`). An unlocked TRUNCATE checkpoint can hold the SQLite writer lock while waiting for such readers. However its busy handler has the configured 30-second budget and it falls back/returns BUSY instead of requiring the reader to finish forever. `tidy` ignores the result and advances. Thus this is a plausible transient reader/writer stall, not a demonstrated permanent deadlock with the current bounded checkpoint wait. Actual I/O is not bounded by busy_timeout, so hung storage remains a separate liveness risk.
* **Lifecycle/backpressure:** dbUpdaterRoutine has no context parameter or cancellation branch during its callback retry, uses `time.Sleep`, and pullerIteration waits for it (`folder_sendrecv.go:311-313` and the iteration's `updateWg.Wait` at line 378). Removing the cap without designing cancellation/durable retention can indefinitely stop producer drain and shutdown/folder stop. Simply aborting on cancellation would in turn violate “never drop a completion” unless pending evidence is retained durably or the shutdown contract explicitly preserves it.

The present schedule is 7 retries after the initial attempt (`lib/model/indexhandler.go:481-489`), with 31.75 seconds total sleep plus up to eight 30-second SQLite waits, approximately 271.75 seconds for consistently exhausted lock waits, excluding other work. Unbounded lock-only retries avoid this particular exhaustion/drop path for a finite holder, but cannot promise eventual completion against a truly stuck holder. Non-lock errors remain terminal unless separately designed. The existing race regressions establish bounded-retry behavior and false-conflict protection for the tested cases, not the safety of an unimplemented unbounded loop or real 30-second checkpoint interactions.

## Evidence artifacts and limits

Local scratch directory `/Users/jhf/.jcode/scratch/patch61/` contains `build.log`, `tests.log`, `race.log`, `write-sites.txt`, `remote-logs.txt`, `ssh.err`, `analyze.py`, `windows.txt`, and `windows-summary.txt`. The remote log stream was read live rather than atomically snapshotted. There is no statement-duration telemetry, active transaction ownership trace, or historical stack dump in the supplied log set that ties the lock to ANALYZE, a chunk, or a checkpoint. No live database or production process was disturbed to fill that gap.
