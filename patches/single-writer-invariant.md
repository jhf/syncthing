# single-writer-invariant: one queued SQLite writer

This record merges the design notes of the revisions folded into the
`single-writer-invariant` patch, in the order they were written: bounded
tidy, device-scaled need build, the guard that proves both, then planner
statistics moved offline after the bounded tidy's ANALYZE still held the
lock for minutes on yarlung.

## `tidy-single-writer`: Bounded tidy in the single-writer queue

### Rule and design

Every live main/folder tidy statement acquires that baseDB's updateLock,
closes its result rows, then releases the lock before the next statement.
Errors are returned, rather than discarded. INF telemetry reports DB path,
SQL text, elapsed lock-held statement duration, result-row count and the
last result row (including freelist pages and checkpoint page counts).
Logging itself is outside the lock.

Read `git show e98c127c1`: patch/gc-lock-scope removed the lock after an unrestricted
ANALYZE stalled Updates for about ten minutes. Restoring that unrestricted
scan under the lock would reintroduce the bug. Instead, analysis_limit=1000
is set on the dedicated tidy connection, and ANALYZE is split by table.
All indices of one table share one bounded-sampling step. optimize uses
the same limited connection. The SQLite documentation recommends hundreds
or thousands of rows per index as useful approximate-analysis limits:
https://www.sqlite.org/pragma.html#pragma_analysis_limit
https://www.sqlite.org/lang_analyze.html#approximate_analyze_for_large_databases
This trades exact statistics for limited work. It is not a promise that
hung storage or an arbitrarily large schema completes in a fixed time.

Vacuum removes at most 64 freelist pages per statement, draining returned
rows fully (important for drivers that step a PRAGMA lazily). The loop
yields the mutex after each statement, stops on no progress, and caps total
work at the initial freelist size, so concurrent frees cannot perpetuate it.
Freelist queries have their own short lock holds and log the observed pages.

Checkpoint is downgraded from TRUNCATE to PASSIVE, still under the mutex.
TRUNCATE can wait for readers whose downstream Update needs this mutex:
being the sole writer is not proof that readers can make progress.
PASSIVE never invokes the busy handler to wait for readers or writers:
https://www.sqlite.org/c3ref/wal_checkpoint_v2.html
The existing 8 MiB journal_size_limit remains, but is not a hard live WAL
cap and this patch does not guarantee immediate physical WAL truncation.
The choice is also recorded in the coordinator's db-QUESTIONS.md.

### Regression

TestTidySingleWriter wraps the real SQLite driver after exclusive startup.
It observes writes through direct execution, queries, prepared statements
and transaction begins, and flags an unheld updateLock. The same test
compiles against base 371de9964 via a signature adapter, not copied SQL.
The fixture has 100000 indexed rows and 512 freed blob pages. An actual
folder Update is queued at the first tidy statement, and the test observes
its completion before a later tidy statement. A 5 ms test-only pause in the
driver forces the waiter into the mutex's fair-handoff regime. It does not
pause production. The test also requires multiple vacuum batches and a
maximum measured statement time below a generous 2 s local test bound.

Before and after logs and per-patch gates are in the worktree's ignored
`tmp/overnight-2026-11-06/64-*.log`. The full before failure lists all five
unlocked production statements and the single unbounded vacuum call.

### Observed regression output (verbatim)

Base 371de9964 with the new test only:

```text
    db_singlewriter_test.go:296: unlocked production writes: [ANALYZE PRAGMA optimize PRAGMA incremental_vacuum PRAGMA journal_size_limit = 8388608 PRAGMA wal_checkpoint(TRUNCATE)]
    db_singlewriter_test.go:302: vacuum was not chunked: 1 statements
    db_singlewriter_test.go:307: tidy fixture=100000 indexed rows steps=5 vacuumSteps=1 maxStep=12.010667ms updateInterleaved=true
--- FAIL: TestTidySingleWriter (0.45s)
FAIL
FAIL	github.com/syncthing/syncthing/internal/db/sqlite	0.956s
FAIL
```

After:

```text
    db_singlewriter_test.go:307: tidy fixture=100000 indexed rows steps=41 vacuumSteps=9 maxStep=3.942167ms updateInterleaved=true
--- PASS: TestTidySingleWriter (0.37s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	0.802s
```

The maxStep measurement spans actual driver execution and row draining,
not time waiting for updateLock or the injected fair-handoff pause.

### Required gates (verbatim)

Fresh worktree initially lacked ignored GUI assets (`undefined: auto.Assets`).
Ran the repository's `go run build.go assets`, then reran all gates on the
final patch tree. No generated assets are committed.

```text
go build ./... exit=0
ok  	github.com/syncthing/syncthing/lib/model	71.260s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.597s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.068s
```

`go test ./internal/db/sqlite -run '^TestTidySingleWriter$' -race -count=20`:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	8.178s
```

### Limits

These are local APFS/NVMe measurements, not yarlung HDD latency guarantees.
No live DB, running Syncthing, deployment or network write was touched.
Work-unit bounds cannot bound fsync, page faults or blocked storage.
Initialization/migration is exclusive before publication and remains outside
this live-write rule. Other long locked operations are unchanged.

## `need-build-single-writer`: Queued, device-scaled need build ranges

### Change and bound

Each range transaction now takes the folder's updateLock before BeginTxx,
keeps it through commit/rollback, and releases it before the next range.
It rechecks the build generation after acquiring the mutex, so a reset
winning the mutex cannot be followed by an old-generation range write.

The old range width was 100000 names regardless of device fanout. The new
name-device work budget is 256: width=max(1,256/snapshotted-device-count).
Device count and the maximum covered device ID are read together, rather
than dividing by an ID that can be sparse after UPSERTs. The ID high-water
mark still restricts which devices this builder covers. The fixture's nine
devices including local yield 28 names/range, at most 224 remote detail rows
per range. If there are more than 256 devices, one name is the minimum
quantum and can exceed the nominal pair budget. Two INSERT SELECTs share
one transaction. A name's elected global is unique, so both branches cannot
contribute the same name's valid and deleted state in one snapshot.

Debug per-chunk telemetry records lo, hi, elapsed lock-held time, device
count and high-water mark. It is not INF because multi-million-name builds
have many thousands of chunks. Work limits are not hard wall-clock bounds:
slow storage/fsync and the unchanged final pass can still take real time.
Small ranges add commits and can lengthen total build time. Sparse name
index ranges can incur empty transactions. No timeout repeatedly rolls back
an expensive range, which would risk permanent lack of progress.

### Correctness with concurrent Updates

The existing protocol is retained, not replaced by range locking:

1. ensureNeedBuild publishes running before the initial snapshots. Updates
   journal every touched name while running is true.
2. Each range reads one transactional snapshot of globals and covered
   devices. Mutex serialization ensures no Update/reset changes it during
   this transaction. Updates between ranges still journal their names.
3. An Update affecting an already visited name is repaired by the final
   journal drain. One affecting a future range may be represented by that
   range and is harmlessly re-derived again in the drain. New names beyond
   the original name high-water mark are included by that same journal.
4. The final pass still acquires updateLock before its transaction and holds
   it through aggregate rebuild, journal re-derivation/deletion, covered
   device marks, completion flag commit, and publishing needBuilt=true.
   No Update can slip between drain and flag publication. Devices created
   beyond the covered high-water mark remain unknown and fall back until
   their first post-build Update backfills them.
5. A generation reset clears tables and changes gen under the same mutex.
   The inner generation recheck prevents any further stale range. The final
   pass's existing generation recheck prevents stale flag publication.

Per-chunk locking therefore strengthens isolation without weakening the
journal's correctness argument or holding a transaction while waiting for
the final mutex acquisition.

### Regression and measurement

TestNeedBuildSingleWriterChunks uses the real driver observer introduced in
the bounded-tidy revision. Setup goes through real Updates: 20000 global names, eight remote
devices, 160000 potential detail rows. An actual concurrent Update changes
an already covered name, a future name and a new name beyond the snapshot.
The test requires it to finish before a later range, requires zero unlocked
write entry points, then compares all eight maintained counts/sizes with
the existing full-scan oracle and checks that the journal is empty.

The observer measures complete transaction lifetimes and distinguishes
range transactions from the final pass and Update. It does not include
waiting for the Go mutex or the 5 ms test-only fair-handoff pause. The
production chunk timer does include that artificial pause in this test.

Pre65 production (7925128aa), with exactly the new test and observer:

```text
    folderdb_need_singlewriter_test.go:123: no Update completed between range chunks
    folderdb_need_singlewriter_test.go:129: unlocked writes=3, first=BEGIN IMMEDIATE
    folderdb_need_singlewriter_test.go:135: oversized range chunks: 2 statements
    folderdb_need_singlewriter_test.go:163: need fixtureNames=20000 remoteDevices=8 rangeStatements=2 commits=3 maxTransaction=388.927666ms maxRangeTransaction=388.927666ms maxRangeSQL=367.602167ms updateInterleaved=false
--- FAIL: TestNeedBuildSingleWriterChunks (2.61s)
FAIL
FAIL	github.com/syncthing/syncthing/internal/db/sqlite	3.154s
FAIL
```

After:

```text
    folderdb_need_singlewriter_test.go:163: need fixtureNames=20000 remoteDevices=8 rangeStatements=1430 commits=717 maxTransaction=98.027459ms maxRangeTransaction=43.872417ms maxRangeSQL=43.167375ms updateInterleaved=true
--- PASS: TestNeedBuildSingleWriterChunks (3.48s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	4.100s
```

This is local APFS/NVMe evidence, not a yarlung latency prediction. The
initial test harness held its statistics mutex while invoking oracle SQL,
which called the observer recursively and deadlocked. The test-only mutex
was fixed before the recorded before/after runs and all gates. No production
deadlock fix is claimed from that harness error.

### Required gates (verbatim)

`go build ./...` and `go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1`:

```text
go build ./... exit=0
ok  	github.com/syncthing/syncthing/lib/model	76.081s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.193s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.541s
```

`go test ./internal/db/sqlite -run '^(TestTidySingleWriter|TestNeedBuildSingleWriterChunks)$' -race -count=20 -timeout=10m`:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	234.840s
```

Logs: worktree `tmp/overnight-2026-11-06/65-{before,after,build,gates,race}.log`.
No live Syncthing, database or remote host was touched.

## Guard: single-writer invariant and contention regression

### Invariant and scope

For a published folder or main baseDB, every in-process writing SQL statement
and immediate transaction belongs to an updateLock critical section for
that same baseDB. Transactions finish before releasing the mutex. Startup
schema/migration and initial device initialization are exclusive, before
publication. Read-only queries and connection-local settings do not need a
write mutex. SQLite trigger effects run inside their originating statement.

The normal DB/folder cache supplies one baseDB per file. Separate DB objects
opened on the same path, another process, and storage failures remain outside
this proof. Work bounds do not make hung storage or the existing final need
journal drain take a fixed elapsed time.

### Guard layers

1. `TestSQLWriteInventory` parses every production Go file in this package,
   including all platform/driver variants. It fingerprints complete bodies
   containing SQL boundary method references, SQL literals or calls/references
   to locked helper functions. It also scans package-level initializers.
   This includes Query/Get/RETURNING, prepared statements and method aliases,
   not just Begin/Exec. New/changed/deleted boundaries require a reviewed
   allowlist entry with an explicit lock/caller/startup/read-only reason.
   Full-body hashes pin lock placement, transaction lifetimes and helper
   calls, not fragile line numbers. The lint's own synthetic regression
   includes a Query RETURNING writer, helper bypass, method alias, initializer
   writer and read-only query. See testdata/write_inventory.json.
2. The real-driver writerProbe observes direct execution/query, prepared
   statement execution/query and immediate begins, and records actual SQLite
   lock errors including lazy row stepping. Its negative self-test deliberately
   executes an unheld Exec, Query RETURNING and Begin, then confirms ordinary
   protected PutKV is accepted. It does not replace SQLite or return fake
   errors. The mutex observation detects an unheld lock, not goroutine
   ownership: a different goroutine holding the mutex can mask an unprotected
   writer. That is why it complements, rather than replaces, source review
   and the reviewed lint allowlist.
3. `TestSingleWriterNoBusy` drives concurrent real folder/main tidy, need
   build, retirement-queue GC, live-table GC, four Update producers, mtime
   flushes, KV/index-ID writes and main folder registry writes. Every pool
   connection has busy_timeout=50 ms. A barrier pauses after the first real
   need INSERT has executed in its still-open immediate transaction. Other
   operations run during a 150 ms hold. Before the bounded-tidy and need-build revisions they compete at SQLite
   and time out. Afterwards they queue in Go, with zero SQLite lock errors.
   The exact same behavioral test compiles against base 371de9964, using the
   the bounded-tidy revision signature adapter, not copied or reimplemented production SQL.

The lint is an auditable maintenance guard, not a Go/SQL formal verifier.
Do not blindly regenerate hashes to fix a failure. Review SQL, same-file
mutex and transaction lifetime, plus all unlocked-helper callers, then
update the specific reasoned entry. Embedded schema/migration scripts run
only at exclusive initialization, or as triggers in the caller's guarded
transaction. A runtime "mutex is locked" assertion would not establish
ownership and could give false assurance, so no such production assertion
was added.

### Evidence

Before/after logs and gates are retained in the worktree's ignored
`tmp/overnight-2026-11-06/66-*.log`. No running Syncthing, live DB, production
configuration, installation intent or deployment is changed.

#### Behavioural regression

Command on both trees:
`go test ./internal/db/sqlite -run '^TestSingleWriterNoBusy$' -count=1 -v -timeout=2m`.
For the baseline run, restored db_service.go and folderdb_need.go to
371de9964 while retaining the identical test overlay. These are the only
executable production changes in the three revisions, so the production code is the
base tree, not a simulation of it. Restored the fixed files before rerunning.

Before, exit 1, verbatim full test output:

```text
=== RUN   TestSingleWriterNoBusy
2026-11-06 01:28:38 INF Need tracking tables built (journaledNames=3 folder=test db=folder.0001-dejpxyku.db log.pkg=db/sqlite)
    db_writer_concurrency_test.go:153: Updates 0: update: deviceidxlocked: database is locked
    db_writer_concurrency_test.go:153: Updates 3: update: deviceidxlocked: database is locked
    db_writer_concurrency_test.go:168: busy_timeout_ms=50 forcedWriterHold=150ms operations=11 folderBusy=4 mainBusy=0 folderUnlocked=2 mainUnlocked=0
    db_writer_concurrency_test.go:170: SQLITE_BUSY observed: folder=[ANALYZE: database is locked INSERT INTO devices(device_id) VALUES (?) ON CONFLICT(device_id) DO UPDATE SET device_id = excluded.device_id RETURNING idx: database is locked PRAGMA optimize: database is locked INSERT INTO devices(device_id) VALUES (?) ON CONFLICT(device_id) DO UPDATE SET device_id = excluded.device_id RETURNING idx: database is locked] mainCount=0
    db_writer_concurrency_test.go:173: unlocked production writer entries: folder=2 main=0
--- FAIL: TestSingleWriterNoBusy (0.46s)
FAIL
FAIL	github.com/syncthing/syncthing/internal/db/sqlite	1.146s
FAIL
```

After, exit 0, verbatim result tail (full INF statement output in 66-after.log):

```text
2026-11-06 01:28:42 INF Database tidy statement (db=/Users/jhf/.jcode/scratch/TestSingleWriterNoBusy614769024/001/folder.0001-thp7j3ku.db statement="PRAGMA wal_checkpoint(PASSIVE)" duration=14.738208ms rows=1 result="[0 742 742]" error=<nil> log.pkg=db/sqlite)
    db_writer_concurrency_test.go:168: busy_timeout_ms=50 forcedWriterHold=150ms operations=11 folderBusy=0 mainBusy=0 folderUnlocked=0 mainUnlocked=0
2026-11-06 01:28:42 INF Need tracking tables built (journaledNames=32 folder=test db=folder.0001-thp7j3ku.db log.pkg=db/sqlite)
--- PASS: TestSingleWriterNoBusy (0.65s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	1.236s
```

#### Invariant regression

The same baseline also fails `TestSQLWriteInventory`, identifying the changed
Service.periodic, tidy and buildNeedTracking bodies against the reviewed fixed
inventory. Exact output: 66-inventory-before.log. Fixed self-tests:

```text
=== RUN   TestSQLWriteInventory
    db_write_inventory_test.go:164: reviewed SQL boundary/helper functions=93
--- PASS: TestSQLWriteInventory (0.02s)
=== RUN   TestSQLWriteInventoryDetectsNewPaths
    db_write_inventory_test.go:191: detected new SQL boundaries=[new.go:.alias new.go:.bypass new.go:.hidden new.go:.read new.go:declaration:initWrite]
--- PASS: TestSQLWriteInventoryDetectsNewPaths (0.00s)
=== RUN   TestWriterProbeDetectsUnheldEntries
    db_writer_concurrency_test.go:206: probe detected unheld Exec/Query-RETURNING/Begin=3; protected PutKV accepted
--- PASS: TestWriterProbeDetectsUnheldEntries (0.01s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	0.582s
```

### Required gates (verbatim)

`go build ./...` and
`go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1`:

```text
go build ./... exit=0
ok  	github.com/syncthing/syncthing/lib/model	69.904s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.673s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.562s
```

All new tests from the three revisions together:
`go test ./internal/db/sqlite -run '^(TestTidySingleWriter|TestNeedBuildSingleWriterChunks|TestSingleWriterNoBusy|TestSQLWriteInventory.*|TestWriterProbeDetectsUnheldEntries)$' -race -count=20 -timeout=15m`:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	229.119s
```

Additional alternate SQLite driver check, same test selection with
`CGO_ENABLED=0`, `-count=1 -timeout=5m`, without race:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.369s
```

### Measured steps and open risks

The the bounded-tidy revision 100000-row fixture measured 41 tidy statements including nine
vacuum steps, max actual statement/drain 3.942167 ms, Update interleaved.
The the need-build revision 20000-name/eight-remote-device fixture measured max range
transaction 43.872417 ms versus 388.927666 ms before, with Update interleaved
only after the change. Final/non-range max transaction was 98.027459 ms.
These local APFS/NVMe observations are not HDD or hung-storage guarantees.
The unchanged final aggregate/journal drain remains an atomic, potentially
long transaction. Approximate statistics can change plans. PASSIVE does not
force immediate physical WAL truncation. Small need ranges add commits and
total build overhead, and sparse name IDs can cause empty transactions.
The minimum one-name quantum can exceed 256 name-device pairs with more
than 256 devices. Invariant-probe and separate-handle limits are stated above.

## `offline-planner-statistics`: Refresh planner statistics offline, never while syncing

### Problem

The bounded-tidy revision serialized `tidy` under `updateLock`, one statement per hold, so that no in-process writer could hit `SQLITE_BUSY`. Per-table `ANALYZE` was one of those statements. On yarlung (10 GB folder DB, btrfs on 2x ST33000650NS) a single table took minutes, holding the lock the whole time:

```text
15:00:19 2.067508064s   ANALYZE "blocklists"
15:08:07 7m47.869581633s ANALYZE "blocks"
15:09:02 54.849714207s  ANALYZE "file_names"
```

A goroutine dump (14:46 UTC run) showed `tidy` in a syscall for 5 minutes at `db_service.go:209`, `RecoverPendingInstalls` waiting 5 minutes on `updateLock` (`db_pull_commit.go:69`), and an index `Update` waiting 5 minutes (`folderdb_update.go:38`). Yarlung stopped reading from the Mac's connection, giving `connection reset by peer` and `read timeout`: 0 resets in the hour before the deploy, then 7, 14 and 23 per hour after it.

`blocks` has 4,007,548 rows (`sqlite_stat1`). `analysis_limit` caps rows SAMPLED per index, not rows read. A read-only `count(*)` over the table took 292 s on that storage.

Moving `ANALYZE` outside the lock was tried and rejected. It brings back `SQLITE_BUSY` (`TestSingleWriterNoBusy` failed 5/10 under race: `ANALYZE "blocklists": database is locked`).

### Why not add a ROWID to `blocks`?

`blocks` is `WITHOUT ROWID`, keyed `(hash, blocklist_hash, idx)`, a single B-tree. Upstream chose this deliberately: the schema comment reads "We eschew most indexes here as inserting millions of blocks is common and performance is critical." A rowid table would need a second B-tree for the primary key. That roughly doubles the table, adds write cost to every pull, and gives ANALYZE two structures to read instead of one. It would also require a full 10 GB rebuild. The cost is ANALYZE reading 4M rows from spinning disks, not the row layout.

### Change

- **Daily tidy runs no `ANALYZE`.** Only `PRAGMA optimize`, under `updateLock`, through `tidyBudgetedStep` with a 10 s budget (`tidyOptimizeBudget`). Cancellation maps to `sqlite3_interrupt` in both drivers; an interrupted optimize rolls back, is logged with `interrupted=true`, and is retried the next day. `optimize` re-analyzes only tables whose row count drifted materially.
- **Full `ANALYZE` runs offline** in `openBase`, after migrations and before the database is published, so no writer can wait on it (`analyzeOffline`, `db_analyze.go`). It runs when:
  - the `cyclical-maintenance-scheduling` scheduler armed `<db>.analyze-pending` in the predicted idle window (weekly, `analyzeMaxAge` = 7 days, marker `<db>.analyzed`);
  - the database has no statistics at all;
  - `STDBANALYZE=1` (forced). `STDBANALYZE=0` disables it.
- **Scheduler** (`lib/syncthing/maintenance.go`): the maintenance restart is now taken when a defrag OR a weekly ANALYZE is due. Admission control adds the per-byte `db-analyze` EMA to the `db-defrag` estimate.
- **All databases are analyzed together.** When any database is due, the scheduler arms every database. Otherwise each `<db>.analyzed` marker carries its own finish time, and a week later the databases come due that many seconds apart, each with its own restart: on the Mac 2026-11-21 `main.db` (finished 09:12:01 the week before) restarted at 09:12:33, the folder DB (09:13:03) at 09:13:35. `main.db` costs 122 µs to analyze.
- Existing statistics from before this patch seed the marker on first open, so the first night does not restart just for this.

### Regressions (`internal/db/sqlite/db_analyze_test.go`)

Before (deployed `tidy`/`openBase`):

```text
    db_analyze_test.go:50: daily tidy ran ANALYZE: [ANALYZE "blocklists" ANALYZE "blocks" ANALYZE "counts" ...
--- FAIL: TestTidyRunsNoAnalyze (0.02s)
    db_analyze_test.go:136: analyze-pending flag not consumed
--- FAIL: TestOfflineAnalyzeAtScheduledRestart (0.03s)
```

After:

```text
--- PASS: TestTidyRunsNoAnalyze (0.03s)
    db_analyze_test.go:86: Update waited 59.239166ms behind optimize (budget 300ms)
--- PASS: TestTidyOptimizeBudgetBoundsUpdateWait (0.08s)
--- PASS: TestOfflineAnalyzeAtScheduledRestart (0.03s)
```

Scheduler (`lib/syncthing/maintenance_test.go`), markers 62 s apart as on the Mac:

```text
    maintenance_test.go:110: maintenance window took 2 restarts, want 1
--- FAIL: TestMaintenanceOneRestartPerWindow/markers_a_minute_apart
```

After: `--- PASS: TestMaintenanceOneRestartPerWindow`.

`TestSingleWriterNoBusy` stays green (`folderBusy=0 mainBusy=0 folderUnlocked=0`): the single-writer invariant is intact, with no exception added.

### Gates

See `tmp/patch70/gates.log`.

### Risks

- Statistics can be up to 7 days old between offline runs. `optimize` covers large drifts within its budget.
- On yarlung the offline ANALYZE takes roughly 10 minutes of the maintenance restart, which admission control now budgets.
- A planner choice that depended on fresh `ANALYZE` from the daily run would now wait for the weekly one. No such dependency was found.

```text
go build ./... exit=0
linux/amd64 exit=0
vet clean
ok  	github.com/syncthing/syncthing/lib/model	89.093s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	14.759s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.256s
ok  	github.com/syncthing/syncthing/lib/syncthing	1.601s
nocgo ok  	github.com/syncthing/syncthing/internal/db/sqlite	16.661s
race20    1 ok  	github.com/syncthing/syncthing/internal/db/sqlite	590.865s
race5-model    1 ok  	github.com/syncthing/syncthing/lib/model	53.862s
```
