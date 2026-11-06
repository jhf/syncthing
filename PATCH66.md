# patch/66: single-writer invariant guard and contention regression

## Invariant and scope

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

## Guard layers

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
   operations run during a 150 ms hold. Before 64/65 they compete at SQLite
   and time out. Afterwards they queue in Go, with zero SQLite lock errors.
   The exact same behavioral test compiles against base 371de9964, using the
   patch/64 signature adapter, not copied or reimplemented production SQL.

The lint is an auditable maintenance guard, not a Go/SQL formal verifier.
Do not blindly regenerate hashes to fix a failure. Review SQL, same-file
mutex and transaction lifetime, plus all unlocked-helper callers, then
update the specific reasoned entry. Embedded schema/migration scripts run
only at exclusive initialization, or as triggers in the caller's guarded
transaction. A runtime "mutex is locked" assertion would not establish
ownership and could give false assurance, so no such production assertion
was added.

## Evidence

Before/after logs and gates are retained in the worktree's ignored
`tmp/overnight-2026-11-06/66-*.log`. No running Syncthing, live DB, production
configuration, installation intent or deployment is changed.

### Behavioural regression

Command on both trees:
`go test ./internal/db/sqlite -run '^TestSingleWriterNoBusy$' -count=1 -v -timeout=2m`.
For the baseline run, restored db_service.go and folderdb_need.go to
371de9964 while retaining the identical test overlay. These are the only
executable production changes in 64/65/66, so the production code is the
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

### Invariant regression

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

## Required gates (verbatim)

`go build ./...` and
`go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1`:

```text
go build ./... exit=0
ok  	github.com/syncthing/syncthing/lib/model	69.904s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.673s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.562s
```

All new tests from 64/65/66 together:
`go test ./internal/db/sqlite -run '^(TestTidySingleWriter|TestNeedBuildSingleWriterChunks|TestSingleWriterNoBusy|TestSQLWriteInventory.*|TestWriterProbeDetectsUnheldEntries)$' -race -count=20 -timeout=15m`:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	229.119s
```

Additional alternate SQLite driver check, same test selection with
`CGO_ENABLED=0`, `-count=1 -timeout=5m`, without race:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.369s
```

## Measured steps and open risks

The patch/64 100000-row fixture measured 41 tidy statements including nine
vacuum steps, max actual statement/drain 3.942167 ms, Update interleaved.
The patch/65 20000-name/eight-remote-device fixture measured max range
transaction 43.872417 ms versus 388.927666 ms before, with Update interleaved
only after the change. Final/non-range max transaction was 98.027459 ms.
These local APFS/NVMe observations are not HDD or hung-storage guarantees.
The unchanged final aggregate/journal drain remains an atomic, potentially
long transaction. Approximate statistics can change plans. PASSIVE does not
force immediate physical WAL truncation. Small need ranges add commits and
total build overhead, and sparse name IDs can cause empty transactions.
The minimum one-name quantum can exceed 256 name-device pairs with more
than 256 devices. Invariant-probe and separate-handle limits are stated above.

