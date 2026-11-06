# patch/64: bounded tidy in the single-writer queue

## Rule and design

Every live main/folder tidy statement acquires that baseDB's updateLock,
closes its result rows, then releases the lock before the next statement.
Errors are returned, rather than discarded. INF telemetry reports DB path,
SQL text, elapsed lock-held statement duration, result-row count and the
last result row (including freelist pages and checkpoint page counts).
Logging itself is outside the lock.

Read `git show e98c127c1`: patch/11 removed the lock after an unrestricted
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

## Regression

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

## Observed regression output (verbatim)

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

## Required gates (verbatim)

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

## Limits

These are local APFS/NVMe measurements, not yarlung HDD latency guarantees.
No live DB, running Syncthing, deployment or network write was touched.
Work-unit bounds cannot bound fsync, page faults or blocked storage.
Initialization/migration is exclusive before publication and remains outside
this live-write rule. Other long locked operations are unchanged.
