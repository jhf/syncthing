# patch/70: refresh planner statistics offline, never while syncing

## Problem

Patch/64 serialized `tidy` under `updateLock`, one statement per hold, so that no in-process writer could hit `SQLITE_BUSY`. Per-table `ANALYZE` was one of those statements. On yarlung (10 GB folder DB, btrfs on 2x ST33000650NS) a single table took minutes, holding the lock the whole time:

```text
15:00:19 2.067508064s   ANALYZE "blocklists"
15:08:07 7m47.869581633s ANALYZE "blocks"
15:09:02 54.849714207s  ANALYZE "file_names"
```

A goroutine dump (14:46 UTC run) showed `tidy` in a syscall for 5 minutes at `db_service.go:209`, `RecoverPendingInstalls` waiting 5 minutes on `updateLock` (`db_pull_commit.go:69`), and an index `Update` waiting 5 minutes (`folderdb_update.go:38`). Yarlung stopped reading from the Mac's connection, giving `connection reset by peer` and `read timeout`: 0 resets in the hour before the deploy, then 7, 14 and 23 per hour after it.

`blocks` has 4,007,548 rows (`sqlite_stat1`). `analysis_limit` caps rows SAMPLED per index, not rows read. A read-only `count(*)` over the table took 292 s on that storage.

Moving `ANALYZE` outside the lock was tried and rejected. It brings back `SQLITE_BUSY` (`TestSingleWriterNoBusy` failed 5/10 under race: `ANALYZE "blocklists": database is locked`).

## Why not add a ROWID to `blocks`?

`blocks` is `WITHOUT ROWID`, keyed `(hash, blocklist_hash, idx)`, a single B-tree. Upstream chose this deliberately: the schema comment reads "We eschew most indexes here as inserting millions of blocks is common and performance is critical." A rowid table would need a second B-tree for the primary key. That roughly doubles the table, adds write cost to every pull, and gives ANALYZE two structures to read instead of one. It would also require a full 10 GB rebuild. The cost is ANALYZE reading 4M rows from spinning disks, not the row layout.

## Change

- **Daily tidy runs no `ANALYZE`.** Only `PRAGMA optimize`, under `updateLock`, through `tidyBudgetedStep` with a 10 s budget (`tidyOptimizeBudget`). Cancellation maps to `sqlite3_interrupt` in both drivers; an interrupted optimize rolls back, is logged with `interrupted=true`, and is retried the next day. `optimize` re-analyzes only tables whose row count drifted materially.
- **Full `ANALYZE` runs offline** in `openBase`, after migrations and before the database is published, so no writer can wait on it (`analyzeOffline`, `db_analyze.go`). It runs when:
  - the patch/43/53 scheduler armed `<db>.analyze-pending` in the predicted idle window (weekly, `analyzeMaxAge` = 7 days, marker `<db>.analyzed`);
  - the database has no statistics at all;
  - `STDBANALYZE=1` (forced). `STDBANALYZE=0` disables it.
- **Scheduler** (`lib/syncthing/maintenance.go`): the maintenance restart is now taken when a defrag OR a weekly ANALYZE is due. Admission control adds the per-byte `db-analyze` EMA to the `db-defrag` estimate.
- Existing statistics from before this patch seed the marker on first open, so the first night does not restart just for this.

## Regressions (`internal/db/sqlite/db_analyze_test.go`)

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

`TestSingleWriterNoBusy` stays green (`folderBusy=0 mainBusy=0 folderUnlocked=0`): the single-writer invariant is intact, with no exception added.

## Gates

See `tmp/patch70/gates.log`.

## Risks

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
