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

## Addendum: retaining completed pulls indefinitely, with bounded memory and progress

Requested at 2026-11-05 23:52:58 UTC. Analysis only, no retry or persistence implementation. The design requirement is accepted: **lock contention must delay a completed pull, not discard its installation evidence.** A progress watchdog should diagnose and escalate a stalled holder, not turn a timeout into permission to forget completed work. The conclusions below distinguish bounded in-flight completion work from the whole process heap, and cancellation from process termination.

### 1. Backpressure is real, but a constant byte bound for the whole pipeline is not established

Let C be effective Copiers, capped at `2*runtime.NumCPU()` (`lib/model/folder_sendrecv.go:168-169`), and P be `PullerMaxPendingKiB*1024`. All four iteration channels have capacity C (`:296-299`). The updater runs its batch callback synchronously, so during its retry sleep it does not receive another job (`:2255-2271,2295-2334`). It retains the same batch, not one new batch per attempt.

| Stage | Exact blocking point | Consequence while updater waits |
|---|---|---|
| Finisher | `performFinish` sends `dbUpdateChan <- ...` at `folder_sendrecv.go:2182`, called by C finishers at `:2186-2194` | At most C buffered DB jobs, then up to C finishers can each block with a job/state. These senders have already installed bytes, so their evidence also matters at shutdown. `finalClose` released its state mutex and closed its writer before `performFinish` (`sharedpullerstate.go:322-362`). |
| Copier | Sends a block to `pullChan` at `folder_sendrecv.go:1852` and a state to `finisherChan` at `:1862` | C copier workers and bounded channel slots, no unbounded goroutine spawn per file. Filling downstream channels eventually blocks them. |
| Pull dispatcher and request workers | `:2002,2017,2023,2129` send to finisher; `:2021` acquires byte tokens **before** `wg.Go` at `:2027` | Worker count is not simply C: concurrent requests are byte-limited. `Give(bytes)` is deferred in the worker wrapper (`:2028`), after `pullBlock` returns, hence after its possibly blocked final send. Tokens are not released early to create unlimited workers waiting to send. Zero-sized/failed fast paths run inline and block without spawning. |
| Queue producer | `handleFile` sends `copyChan <- cs` at `:1624`; called synchronously by `processQueue` at `:661` | Once copy capacity is exhausted, ordinary file production stops. Rename/metadata shortcuts instead block on DB sends (`:1532,1788,2182`). Deletions have one additional goroutine (`:345-349`) and also block on DB sends (`:1388`, collapse sends `:841-849`). |
| Need walk | Direct jobs send at `:423,431,503`, or synchronous directory/symlink/shortcut handlers send | It blocks when it reaches such a send and DB capacity is full. **It does not send on every row:** ordinary files are first accumulated in the queue at `:493`, not dispatched yet. Therefore the walk need not stop immediately when the updater sleeps. |

This establishes that an unchanged held batch does not by itself cause a retry-count-proportional heap leak. It does **not** establish the stronger assertion “nothing grows while waiting.” Specific exceptions:

* `processNeeded` allocates `dirDeletions`, `fileDeletions`, `buckets`, `neededBlocks`, `pulledBlocksHashes`, `deletedLocals` at `:389-397`, grows them at `:443,456-460,474-477`, and pushes all ordinary file names at `:493`. There is no numeric backlog or byte cap. After a timer flush blocks the updater, the walk can still traverse all remaining ordinary-file rows without another DB send. Its retained hash set and job queue can thus grow to the **whole need snapshot**, not C or 4000. The queue itself appends without a cap (`lib/model/queue.go:33-37`), and round-held membership also appends (`:107-114`). The need query has LIMIT -1 when invoked with limit 0 (`internal/db/sqlite/folderdb_global.go:130-151`). A fixed snapshot is finite, but arbitrarily large across folders/datasets.
* Deletion state and rename buckets retain full FileInfos and block lists, not only paths. `collapseDeletions` adds further round-sized maps/slices (`folder_sendrecv.go:752-789`). These are not enlarged on every retry, but can remain live throughout it.
* `processQueue` does not append new normal jobs, but can keep consuming exceptional/no-source/obsolete/held rows without making a blocking successful send (`:577-605,646-658`). `tempPullErrors` accepts one entry per distinct failed path without a numeric cap (`:2438-2453`). Thus even after the need walk, some backlog-sized ancillary state can grow before a successful-send path provides backpressure. It cannot consume a new whole pull round while this round is waiting.
* `scanChan` is unbuffered (`:190`), but its consumer immediately receives into an **uncapped deduplication map**, `toBeScanned` (`:2365-2369`). That goroutine does not stop on `ctx.Done`; it drains until channel close. A producer can send arbitrarily many distinct scan paths from a finite large backlog, especially failures that do not enqueue DB completions. The channel therefore does not impose a path-count bound. At channel close it additionally builds a slice of all paths (`:2372-2378`).
* Index updates are independent of the local completion queue. They can commit between retries and change global state while the held completion is retained. If an index write blocks, protocol dispatch is synchronous (`lib/protocol/protocol.go:471-483`), the inbox is unbuffered (`:261`), and the reader blocks delivering its next message (`:410-427`). Model handling and registry `ReceiveIndex` call through synchronously (`lib/model/model.go:1205-1215`, `lib/model/indexhandler.go:705-713,438-440`). There is no unlimited in-memory index-message queue in this path. However each connection can retain its active converted message, conversion backing storage and the reader's next decoded message, with allocations beyond encoded size. The wire ceiling is **500000000 bytes per message**, not 1000 files as a receiver-enforced memory budget (`lib/protocol/protocol.go:44-45,533-534`). The outgoing batching defaults are not a global heap bound. Connection count, other folders and independent services are additional terms.

#### Worst-case bytes, not a false “4 MiB maximum”

**There is no finite, configuration-independent worst-case byte ceiling established by this code.** Backlog size, total block hashes, file metadata and connections are not capped by C. The requested worst-case answer for total retained heap is therefore **unbounded by these constants**, not a numeric number of MiB. This is a static limit conclusion, not a claim of an observed leak on yarlung.

Even the held batch has only a file-count threshold and a *soft* block threshold. `FileInfoBatch.Append` appends first (`lib/model/fileinfobatch.go:61-69`), then the caller checks `Full` (`:72-81`, `folder_sendrecv.go:2325-2326`). Thus F <= 4000, but block count can reach **19999 + Bmax**, where Bmax is the block count of the last whole file added. One very large file can exceed 20000 blocks. Strings, vectors, xattrs, encrypted metadata, old slice capacity and per-file block lists remain variable-sized (`lib/protocol/bep_fileinfo.go:115-149,610-614,763-769`). Channel entries also carry whole variable-sized FileInfos or pointers to states retaining both current and desired versions (`sharedpullerstate.go:26-55`).

A scratch-only `unsafe.Sizeof`/append measurement on `go1.26.4 darwin/arm64`, run from the same repository, produced:

```text
FileInfo=280 BlockInfo=40 Vector=24 Counter=16 batch_len=4000 batch_cap=4417 batch_backing_bytes=1236760 nominal_4000FI_20000blocks_hash32=2560000
```

Source is `/Users/jhf/.jcode/scratch/patch61/sizes.go`, not a repository change. For exactly 4000 files and 20000 blocks with independently retained 32-byte hashes, the measured FileInfo backing array plus block structs and hash bytes is **2676760 bytes**, before names, vectors, platform fields, allocator overhead, batch-side maps, other channels and goroutines. The nominal 2560000 figure uses length rather than allocated FileInfo capacity. Neither is an upper bound. Slice growth is toolchain-dependent, and backing storage can alias, so summing all reachable slice lengths can also double-count shared storage.

A useful parameterized accounting is:

```text
held batch = 280*cap(FileInfo slice)
           + 40*sum(unique retained block-array capacities)
           + sum(unique retained hash/byte-array capacities)
           + 16*sum(unique vector-counter capacities)
           + names/platform/xattr storage + allocator overhead
other state = round queues/maps/deletion FileInfos + scan-path set
            + bounded channel/worker states with their variable-sized metadata
            + request payloads/stacks + independent connection messages/services
```

The DB channel alone has C*288 inline bytes on this 64-bit layout (280-byte FileInfo plus `dbUpdateType int`, `folder_sendrecv.go:85,125-128`), excluding channel overhead and referenced payloads. The finisher channel has C*8 pointer bytes, excluding its states. Neither small inline figure bounds retained bytes. Under normal valid requests no larger than MaxBlockSize, payload tokens cap active request sizes at P (`:1997,2012-2029`), with P default **33554432 bytes** (`:120`, `protocol.go:51`), plus copier buffers and metadata. Token accounting does not cover all allocations: semaphore requests larger than its capacity are clamped (`lib/semaphore/semaphore.go:62-64`), and per-file block metadata is independent of byte tokens. Do not convert a payload budget into a whole-heap guarantee.

For scale, a queue of N independently allocated, distinct 100-byte paths has at least **132*N bytes** of name bytes and 32-byte `jobQueueEntry` elements on this layout, before slice spare capacity and maps. A million such paths already accounts for 132000000 bytes in that term alone. `neededBlocks` adds at least 32 bytes per distinct SHA-256 hash before map/key overhead. These are explicit illustrative lower components, not measured yarlung usage or total upper bounds.

**Design consequence:** retaining the same completed batch while waiting is sound with respect to per-attempt memory. Keep retry state constant-sized, saturate/reuse the backoff schedule rather than accumulating attempt records, and rate-limit progress warnings. A hard total-memory requirement additionally needs a byte budget for variable metadata and a paged/spooled need/scan backlog or an explicitly accepted dataset-sized baseline. The existing channels alone do not satisfy that stronger contract. Progress from other writers does not repair an unbounded metadata budget.

### 2. Locks and transactions across retry sleep

**The updater goroutine holds no SQLite transaction or updateLock across `time.Sleep` at `folder_sendrecv.go:2270`.** The called `updateLocalsFromPulling` and `updateLocals` have returned (`lib/model/folder.go:2399-2413`). Folder DB Update releases updateLock via defer (`internal/db/sqlite/folderdb_update.go:38-39`), and rolls back any failed batch transaction via defer (`:52`). Device lookup failure occurs before that transaction. The successful-path `forcedRescanPathsMut` is unlocked at `folder.go:2423`. Durability flushing has already returned before retry (`folder_sendrecv.go:2256-2257`), including waiting for directory-fsync workers (`:2360`). There is no batch mutex enclosing the callback (`fileinfobatch.go:86-98`).

There **is** other retained synchronization in the larger operation, which must not be hidden by saying “no other locks”:

* The folder-level pull holds **one ioLimiter semaphore token** for the entire pull (`lib/model/folder.go:639-642`), hence across a retry sleep. This reduces concurrency for other work but does not itself deadlock against `tidy` or a need-build chunk: neither requires that token to finish. Holding any token is not by itself proof of a circular wait.
* While `processNeeded` is blocked on a send, its need iterator can retain a **SQLite read snapshot**. Query creation is at `folderdb_global.go:145-151`; `iterStructs` closes rows only as iteration exits (`internal/db/sqlite/util.go:32-44`). This is in another goroutine, not an updater write transaction. It is exactly the reader dependency that can stall an unlocked TRUNCATE checkpoint, as assessed above. The checkpoint busy timeout breaks that lock wait; a hung I/O operation still has no elapsed-time bound.
* A blocked request worker retains its request-limiter tokens until its finisher send returns. Those tokens intentionally produce backpressure, and ANALYZE/need chunks do not require them.

Consequently the concrete holder candidates do not create a newly identified infinite application lock cycle merely because retries become unbounded. They can cause arbitrarily long delay, and the whole pull can reserve resources throughout. Dropping the completed batch is not necessary to release updateLock, since it is already released between attempts.

### 3. Progress: prefer data_version for commits, not WAL file length

SQLite specifies that `PRAGMA data_version` changes when another connection commits, and its values are comparable **only on the same physical connection over time**. Commits made by that same connection do not change its value. Source: `https://www.sqlite.org/pragma.html#pragma_data_version`.

For this pool-based implementation, repeatedly calling `db.sql.Get(...)` is therefore insufficient: calls can land on different pooled connections. A diagnostic should keep one dedicated observer `sql.Conn` for the **same folder file**, sample outside explicit transactions, fully consume/close each scalar query, and use it for observation only. Do not keep a read transaction open across waits or sample from the failed write transaction. This costs one pool slot/cache for the lifetime of the observation, plus a cheap scalar PRAGMA per sample, rather than a table scan. Give each observation its own error/timeout handling and interpret read failures as unknown, not no-progress. Observer acquisition must also be bounded so an exhausted pool cannot stall the retry path before it even observes anything.

* A changed data_version is positive evidence of at least one other connection's intervening commit. That rules out one continuously exclusive writer transaction occupying the entire observed interval. It does **not** identify the holder, prove fairness, guarantee this batch will ever win, or distinguish useful model changes from maintenance/statistics commits.
* An unchanged value is **not proof of a stuck transaction**. A legitimate ANALYZE, need chunk, checkpoint copy/fsync, or slow I/O can be making progress without committing. A checkpoint can progress without a new logical data commit. The strongest diagnostic needs active-holder operation/age and operation-specific progress, for example chunk number or pages processed; absence of commit progress should trigger investigation, not abandonment of the batch.
* WAL size is cheaper to obtain as a filesystem stat with no SQL pool slot, but is **not a reliable commit signal**. A large uncommitted transaction can grow/spill WAL frames, rollback can leave reusable file space, subsequent commits can reuse existing allocated space without changing length, and checkpoint/reset/truncation can shrink it. File length alone does not say which frames are committed. WAL format sources: `https://www.sqlite.org/wal.html` (checkpoint/reuse) and `https://www.sqlite.org/walformat.html` (commit frames versus physical file and wal-index committed-frame fields). Raw WAL/SHM parsing would add synchronization/VFS issues and is not cheaper or safer than the supported PRAGMA.

**Choice:** use a dedicated observer's data_version as the inexpensive, reliable *positive commit heartbeat*, optionally sample WAL bytes as supplementary I/O activity, and combine with the missing holder/age diagnostic for no-commit intervals. Neither signal alone can classify “stuck” conclusively. A changing heartbeat permits a justified “other writers commit, our retained batch is starved” diagnosis, not a reason to impose a lossy retry limit.

### 4. Cancellation, termination and the cheapest persistence boundary

**Correction to the proposed premise:** cancelling the folder context mid-wait does **not immediately discard the held batch in the current patch/61 code**. `dbUpdaterRoutine` receives no context (`folder_sendrecv.go:2245`), sleeps with `time.Sleep`, and `updateLocalsFromPulling` has no context argument. SQLite Update uses `context.Background()` (`folderdb_update.go:48`). Cancellation can stop upstream work at its context checks, but the iteration still waits for producers and updater (`folder_sendrecv.go:357-378`); several sends are not cancellation-selects. Today the batch is discarded on retry exhaustion/terminal error and ultimately routine exit, or lost when the process is forcibly terminated/crashes. A naive unbounded loop changes cancellation behavior to **potentially unbounded shutdown**, not automatic early cancellation loss.

If a future retry loop explicitly exits on context cancellation, then yes, returning without durable retention loses the in-memory completion evidence. Patch/62's proposed pre-install durable intent in `PATCH61.md` covers the larger unavoidable window: process death **after install, before index commit**, including before the updater has even received a job. The install occurs before its channel send (`performFinish`, `folder_sendrecv.go:2174-2182`), and blocked finishers plus buffered DB jobs can already represent installed bytes outside the held batch. The bounded-batch-only snapshot is not enough to cover them.

There are cheaper *narrower* mechanisms, with explicit limits:

1. **Graceful shutdown only:** stop further installs, serialize all retained completion evidence to a separate sidecar spool, fsync the file, atomically publish it and fsync its directory before allowing exit. Drain/journal buffered jobs and finishers that already installed, not just the callback batch. Replay before scanning/pulling on restart. This avoids waiting for the busy folder DB lock but does **not** cover a crash or forced kill before/during that shutdown handoff. Keeping the goroutine alive or doing one last retry is not persistence.
2. **Minimal crash-safe implementation of install intent:** a per-folder append-only, framed/checksummed sidecar journal containing folder identity, path, full intended FileInfo/version and an operation identity. Durably append before the install becomes visible, retain until the index update is durably acknowledged, and perform idempotent validated replay/compaction. Group fsync is possible only if publication waits for that group's durable append. This is conceptually patch/62 with a simple file spool instead of a new SQL subsystem, not an alternative that removes the ordering requirements. A separate journal avoids needing the very folder SQLite write lock we are waiting for.

For either mechanism, names alone, current-global lookups or a hash-only record cannot recover the exact installed sender version after global moves on. A batch snapshot should serialize the full retained FileInfos, not file contents, and the restart path must validate disk equivalence and non-regressing local counters as already specified in `PATCH61.md`. Concurrent local edits, deletes, rename shortcuts, metadata-only installs and encrypted metadata need the same conflict/replay policy. Strict crash durability must also coordinate the existing `synchronous=NORMAL` SQLite setting with intent retirement: do not durably erase the only intent based solely on a commit that can be lost on power failure. Conservative retention plus validated/idempotent replay is safer than premature deletion. This task has not designed or implemented that recovery protocol.

**Overall:** indefinite retention is preferable to dropping completed pulls. Existing backpressure bounds completion transport for a fixed dataset/configuration, and retry sleep does not retain the competing writer lock. It does not yet provide a hard whole-process byte ceiling, a definitive stuck-holder detector, or crash-safe cancellation/exit. Those are explicit design requirements, not reasons to discard the evidence. The addendum was validated by source inspection and a scratch allocation-layout measurement, not by implementing or testing an infinite retry loop.
