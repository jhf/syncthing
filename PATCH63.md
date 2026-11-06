# patch/63: never discard a locked pull database batch

## Behavior

The pull updater receives the folder context and retries the same FileInfoBatch callback indefinitely while internal/db.IsLockContention classifies the returned error as lock contention. Backoff begins at 250 ms, doubles to a 30 s cap, and saturates there. There is no retry count limit and no growing schedule or attempt history. One reusable timer handles real waits. No updateLock or SQLite transaction is held across the wait. Non-lock errors remain terminal and keep the old producer-draining behavior, with patch/62's regular-file install evidence retained on disk.

Every five minutes of elapsed waiting, the updater logs `Pull database update still waiting` at Warn with `files`, `elapsed`, `attempts` and `data_version_moved` (`true`, `false`, or `unknown`). The warning increments bounded churn key `db_update.lock_wait_warning`. The old exhaustion key is retained at zero for schema compatibility, not used as a loss mechanism. The next sleep is clipped to the next warning deadline. An in-flight SQLite call can still delay a warning until it returns.

## Dedicated heartbeat connection

New `internal/db/sqlite/db_data_version.go` implements OpenDataVersionObserver and DataVersion on a reserved physical sql.Conn for the same registry-selected folder file. One observer at most is opened lazily per folder updater and reused across its held batches, with a nonblocking folder-registry TryRLock, cached-folder-only lookup, a one-second acquisition context and a one-second context per scalar sample. No transaction or result rows survive a sample. The observer is closed when the updater exits. Acquisition failure is diagnostic only, makes progress unknown, and is not retried on every write attempt. A read failure breaks comparability until a new baseline has been observed on that same connection.

The initial sample supplies the baseline, then only warnings sample again. Values from different physical connections are never compared. A moved data_version is positive evidence of an intervening commit, not identification of the lock holder or a guarantee of fairness. An unchanged value is not proof of a stuck writer. Neither `db_service.go` nor `folderdb_need.go` is edited.

## Shutdown

Context cancellation stops retrying immediately at the next cancellation boundary. The callback returns context.Canceled, then the updater drains producers until the channel closes without accepting further batch work. This is necessary because several existing producer sends intentionally are not cancellation-selects. Simply returning the updater on cancellation would strand those senders and hang pull shutdown. Already installed regular files have their patch/62 pre-publication journal records, including files still in the transport channels or finishers. The existing underlying DB.Update API is not context-aware: this patch interrupts waits, not an already executing SQLite operation or blocked filesystem sync.

## Regression evidence and tests

The new TestPullDBNeverDropsHeldBatch first ran against committed patch/62's finite schedule. It injected six busy boundaries with the existing three-entry shortened schedule and failed after four attempts. After replacement, that same held batch commits on attempt seven, is scanned without a receiver counter and is not reinstalled. The prolonged patch/58 controls now exceed the former retry limit for both named fixtures and both predecessor cases. Structural errors still make exactly one attempt and drain a full batch plus ten producer jobs.

TestPullDBCancelWaitRestart cancels mid-wait, verifies that no local index commit occurred and the journal remains, then uses independent SQLite handles and a fresh folder object to adopt the exact intended sender vector. TestPullDBCancelDrainsProducers fills the real 4000-file batch, cancels a busy wait and requires all ten extra producer sends and updater shutdown to finish without a commit or erased intent.

TestPullDBWaitWarnings runs 12.5 minutes on an injected clock with the same held slice address throughout 26 attempts. It requires warnings at five and ten minutes, one observer acquisition, three scalar reads, one close and exactly two churn counts. It covers moving, unchanged, failed-read and failed-acquisition outcomes. Real SQLite tests verify that the dedicated observer sees another connection's commit, stable samples stay unchanged, canceled reads surface errors without leaking a transaction, exhausted-pool acquisition honors its deadline, a busy folder-registry mutex fails immediately rather than queuing behind an opener, and Close releases the slot.

TestPullDBRetryStateConstant runs the pure retry-state operations for 10 and 1,000,000 iterations with testing.AllocsPerRun and requires zero allocations at both sizes. It also verifies capped delay and saturating attempt count. This is a deterministic retry-state test, not a noisy whole-process runtime.MemStats/heap-size assertion. The held batch, existing need snapshot, scan dedup and queue retain their patch/61 baseline bounds and limitations. A global process heap cap is not claimed.

## Validation

Verbatim before-run (`tmp/patch63/before.log`):

```text
$ go test ./lib/model -run '^TestPullDBNeverDropsHeldBatch$' -count=1
--- FAIL: TestPullDBNeverDropsHeldBatch (0.02s)
    pull_db_retry_test.go:22: completed pull dropped after 4 attempts: SQLITE_BUSY: database is locked
FAIL
FAIL	github.com/syncthing/syncthing/lib/model	0.615s
FAIL
```

After, same command (`tmp/patch63/neverdrop-after.log`):

```text
ok  	github.com/syncthing/syncthing/lib/model	0.550s
```

Final gates (`tmp/patch63/build-final.log`, `full-final.log`, `race-final.log`), with no failures or race diagnostics:

```text
$ go build ./...
go build ./... exit=0
$ go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1
ok  	github.com/syncthing/syncthing/lib/model	80.485s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	7.522s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.093s
$ go test -race ./lib/model ./internal/db/sqlite -run '^(TestPullDB.*|TestDataVersionObserver.*|TestInstallIntent.*|TestPullRetryFailedCommitRetainsStateAndDrains|TestDripHeldScanAcrossPull|TestMetabatchHeldScanAcrossPull)$' -count=20
ok  	github.com/syncthing/syncthing/lib/model	75.937s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	4.719s
```

Implementation references: `lib/model/folder_sendrecv.go:2256` threads ctx into the updater, `:2278` retries inside the latched batch callback, and `:2299` stops DB work while draining on cancellation. `lib/model/pull_db_retry.go:140` emits the exact progress warning and `:153-158` implements the reusable cancellable timer wait. `internal/db/sqlite/db_data_version.go:23` avoids a registry mutex wait, `:31` acquires the physical connection with ctx, and `:40` consumes each scalar sample.

## Open risks

This does not identify a foreign lock holder or guarantee eventual fairness. Cancellation cannot interrupt the existing context-free Update call or an OS sync already executing. One observer consumes a pool slot/cache until updater exit. Failed observer acquisition stays unknown for that updater lifetime. The regular-file journal's limits and conservative mismatch rejection from PATCH62.md remain in effect. No push, deployment, running-daemon changes, production performance measurement or in-process physical 30-second SQLite busy-timeout timing claim.
