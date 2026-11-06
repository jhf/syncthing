# patch/67: install files only after their index commit is durable

## Scope and history

Withdraw local patch/62 (`9e40ca03b`), the former journal-grouping patch/67 (`c5eb839ea`), and its documentation commit (`aec70714f`). Rebase 63 through 66 onto `371de9964`, retaining `db_data_version.go` and removing the journal implementation, interface methods and SQL inventory entry. `baseline/install-after-commit-v2-66` names `0b1cc6e8b`. `backup/patch67-before-v2-326c38858` preserves the complete pre-v2 unpushed history, in addition to the original `backup/install-before-commit-aec70714f`. Patch/63's pre-67 non-lock controls again document receiver adoption of already-published bytes, and patch/67 alone flips them to require disk/index S0 and reusable temp S1 on terminal error. The heartbeat inventory follow-up is folded into patch/66 so every retained commit passes its own tests. No push, deployment, service restart, live folder access or live database access was performed.

The new mechanism has no sidecar, replay log, schema migration or protobuf field addition. One bit of the existing host-local FileInfo flags field, `FlagLocalInstallPending`, distinguishes a durably indexed regular-file install awaiting publication. Its FileInfo, vector and blocklist are committed together using the existing database representation. Ordinary scan/index writes remain NORMAL.

## Ordering and grouping

1. Finish download, close its writer, set permissions/platform data/mtime on the deterministic temp, then sync the temp. The old destination is still untouched. `lib/model/install_after_commit.go:47`.
2. The sole folder updater drains jobs already queued, nonblocking, bounded by the existing file/block caps. It syncs the temp parent directories so the recovery names themselves are durable, then commits the FileInfos and pending bits durably. `lib/model/folder_sendrecv.go:2218`, `lib/model/install_after_commit.go:112`.
3. After the FULL transaction returns, recheck each target using the existing `scanIfItemChanged` check, perform conflict/replacement work and rename the temp into place. Sync affected directories, then clear successful pending bits in an ordinary NORMAL transaction. Losing that last transaction only repeats idempotent recovery. `lib/model/install_after_commit.go:157`, `:179`, `:201`.
4. Finishers hand the prepared temp and pull state to the committer and immediately continue. The committer owns publication, per-file error/retry accounting, ReceivedFile, queue.Done, progress deregistration, ItemFinished and the buffered done notification. The rename shortcut copies/reflinks into the temp instead of removing the indexed source early, and its caller still waits before retiring that source.

There is no install-path ticker, fixed delay, 100 ms window or elapsed-time cap. `dbUpdaterRoutine` collects a bounded next group while exactly one committer commits and publishes the previous group. When that committer returns, the next group drains any already-queued jobs up to the existing files/blocks caps, then starts. Reaching a cap disables intake and applies the existing dbUpdateChan backpressure, never starts a second flush. Both the in-flight group and collected next group are capped, with the usual one-file block-cap overshoot. No goroutine is spawned per file, and all completion work joins before updater shutdown. The lock retry timer retained from patch/63 is not an install batching timer.

`TestInstallAfterCommitSlowFlush` retains the 500 ms flush-rate/concurrency checks. `TestInstallAfterCommitSlowFlushGroupGrowth` additionally sends 200 files through the real copier and eight finishers with the production eight-deep DB channel and a 200 ms injected flush. It requires max group > 8, fewer than 200/8 flushes, at most one active flush, complete publication, and cleared pending bits. `TestInstallAfterCommitFinisherOwnership` holds the commit boundary until all finishers return, verifies work remains in progress with no early ReceivedFile, then injects a terminal error and requires every file's error, queue and progress cleanup. `TestInstallAfterCommitPerFileResult` verifies one publication failure does not poison a successfully published sibling's acknowledgement. Directory maps already deduplicated fences in v1 and remain deduplicated: `TestInstallAfterCommitDirectoryFencesDeduplicated` requires exactly four real directory syncs for six files in two directories in one group, one precommit and one postrename fence per directory.

## Durability choice: SQLite FULL on one reserved physical connection

`internal/db/sqlite/db_pull_commit.go:21` reserves a physical `sqlx.Conn` from the existing pool while `folderDB.Update` holds `updateLock` (`internal/db/sqlite/folderdb_update.go:37`). It reads the connection-local settings, enables `synchronous=FULL` (`db_pull_commit.go:45`) and, on Darwin, `fullfsync=ON` (`:49`). The same connection opens the immediate transaction and commits it. The lock remains held through COMMIT, deferred rollback, settings restoration and connection release. If restoration fails, `driver.ErrBadConn` discards that physical connection instead of leaking FULL into ordinary writers (`:36`). This is not a pool-wide PRAGMA change.

Only groups containing regular-file install jobs request `db.WithDurable()`. A rare genuine-edit resolution also uses a durable update before moving the deterministic temp to a conflict name. Scanner, remote index, need maintenance, metadata-only and normal pending-bit acknowledgement transactions retain their previous NORMAL setting. A mixed pull group may include non-install jobs without adding a separate flush for them.

`TestPullCommitConnectionDurability` checks FULL and Darwin fullfsync on the reserved connection, then simultaneously reserves every pool slot and verifies every ordinary connection is back at NORMAL. It also exercises an actual durable pending update, block/vector round-trip and pending-bit removal. Both cgo and CGO_ENABLED=0 package gates exercise this test. The new SQL boundaries, including the read snapshot in `PendingInstalls`, hold updateLock and have reasoned fingerprints in `internal/db/sqlite/testdata/write_inventory.json`. There are 96 reviewed boundary/helper bodies.

### Why not NORMAL under the lock, then external WAL sync outside it?

The attractive alternative is to commit NORMAL, release updateLock and sync the WAL while scan/index writers proceed. WAL reset by itself is NOT a sufficient reason to reject it: SQLite's WAL documentation says that checkpoint syncs the WAL before copying pages and syncs the database before resetting the WAL. The PRAGMA documentation specifies NORMAL's critical checkpoint and WAL-reuse barriers. Thus, with the usual Unix VFS and a correctly pinned identity, a completed reset can transfer durability responsibility from WAL to the main database. References: SQLite `https://www.sqlite.org/wal.html` section 2.3, and `https://www.sqlite.org/pragma.html#pragma_synchronous`.

However, a raw `os.Open(path+"-wal").Sync()` does not establish a driver/VFS file-identity or lifetime contract. It bypasses SQLite's `xSync`, connection-owned WAL descriptors and any VFS error handling. Opening a pathname after releasing the guard also needs a proof against close/reopen, migration/maintenance replacement and WAL disappearance, not just a checkpoint argument. Syncing an old descriptor is not proof that the registry-selected current file or a replacement VFS's storage is durable. Neither the mattn database/sql connection nor modernc's exposes a supported portable WAL-fence API that supplies those guarantees here.

For mattn/cgo, SQLite's Unix VFS owns the fd and its native `xSync` implementation. For modernc/nocgo, the translated SQLite Unix VFS likewise owns its descriptor and syscall dispatch. The application would be coupling two separate implementations to their private fd/VFS behavior. On Darwin there is an additional strength mismatch: a NORMAL checkpoint on another ordinary connection can sync the main DB with plain fsync, reset the WAL, and leave a later F_FULLFSYNC on the WAL unable to establish the same main-DB stable-media barrier. Making all checkpointers fullfsync would spread the cost beyond pull installs, contrary to this brief. FULL on the transaction's actual SQLite connection avoids needing these new cross-driver and cross-file proofs.

We therefore choose the simpler supported SQLite durability contract for both drivers, knowingly holding updateLock through a potentially slow fsync. Scan/index writers may wait hundreds of milliseconds on a slow disk. The measurements below expose that cost instead of claiming it is free. A future outside-lock implementation needs an explicit driver/VFS fence and Darwin reset proof, with tests, before replacing this contract.

### Darwin

Go's temp-file Sync uses F_FULLFSYNC on Darwin. SQLite's default plain fsync is weaker with respect to disk write caches. Set `PRAGMA fullfsync=ON` only on the durable pull connection so the SQLite VFS requests F_FULLFSYNC too. SQLite documents this at `https://www.sqlite.org/pragma.html#pragma_fullfsync`. The pinned mattn SQLite amalgamation and modernc `lib/sqlite_darwin_arm64.go` both route their full-sync path to F_FULLFSYNC, with the SQLite-defined fallback if unsupported. The actual Mac measurement probe observed 50 F_FULLFSYNC WAL calls for the 50-file trickle and 501 for the burst, plus two ordinary checkpoint WAL fsync calls. This measures the native cgo driver, while modernc correctness/configuration is separately tested locally and used by the yarlung test binary.

## Hazard handling

- **Genuine edit after commit:** recheck immediately before publication. Keep the changed target and move incoming bytes to patch/60's bounded conflict name. Persist the resolution before consuming the deterministic recovery temp. An intervening local deletion is treated as a change too, preserving absence and incoming conflict bytes. The existing check is metadata-based, not a new atomic filesystem compare-and-swap.
- **Crash after commit, before rename:** before the first scan or pull, query only pending entries using existing counts and local-flag columns, consume rows and release updateLock, then validate installed bytes or the deterministic temp against the durable blocklist. Matching installed bytes are idempotently acknowledged. Otherwise preserve an uncertain predecessor as a bounded conflict copy, publish validated temp bytes, sync its directory and clear the bit. The original sender vector remains unchanged. Fresh SQLite-handle tests cover absent/file/directory/symlink/conflict predecessors. A corrupt or missing temp blocks the scan rather than minting receiver authorship from predecessor bytes. `lib/model/install_pending.go:79`, `lib/model/folder.go:870`, `lib/model/folder_sendrecv.go:188`.
- **Encrypted folders:** encrypted block hashes are opaque tokens, so neither the existing encrypted downloader nor scanner can authenticate ciphertext hashes. Recovery instead requires exact file size and the existing serialized FileInfo trailer identity, including its encrypted authenticated FileInfo and block tokens. It normalizes only index sequence and the optional DB-computed BlocksHash. This rejects predecessor identities without a new format, but is not ciphertext-integrity proof. `TestInstallAfterCommitEncryptedRecovery` covers an opaque non-SHA256 block token and rejection of a different trailer identity. `lib/model/install_pending.go:39`.
- **Conflict moves and type replacement:** all live-path destination removals, conflict moves and directory/symlink replacement happen only after durable commit. Recovery conservatively preserves any uncertain old target, including a complete directory or symlink. No source of a rename shortcut is removed before the destination commits.
- **Non-lock failure and cancellation:** do not publish. Preserve old bytes/index and reusable temp. Refresh the failed download mtime so a subsequent real scan does not expire a temp carrying an ancient sender timestamp. Drain producers and complete every handed-off state, including queued jobs not attempted after a terminal failure. The next pull reuses the temp and completes. The existing never-drop retry and heartbeat behavior remain.
- **Incident fixtures:** the gateway_state.json and jsonb_stats lib.rs patch/58 tests now require disk/index S0 on terminal precommit error, no receiver counter, then successful subsequent S1 installation and no false S3 conflict, without any journal.

## Before/after and gates

The exact same `lib/model/install_ordering_regression_test.go` runs on rebased-66 and the candidate. `tmp/install-after-commit/before-identical.log` records behavioral failures, not compilation failures:

```text
    install_ordering_regression_test.go:36: rename happened before durable commit returned
--- FAIL: TestInstallOrderingRegressionBarrier (0.02s)
    install_ordering_regression_test.go:59: failed commit published file
--- FAIL: TestInstallOrderingRegressionNonLockError (0.02s)
FAIL
FAIL	github.com/syncthing/syncthing/lib/model	3.421s
FAIL
```

`after-identical.log` records both PASS lines with the identical source. `after-final.log` additionally records source-retention and encrypted controls. These are selected verbatim lines, with routine model logging and RUN lines omitted. Full command lines, final gate output and commit hashes are recorded in `tmp/install-after-commit/REPORT.md`. Historical patch/63-66 before/after evidence remains in their respective patch notes, explicitly distinguished from this replacement's final gates.

## Historical v1 gates

These lines describe the superseded finisher-blocking implementation, not the v2 validation. Current per-commit and final gates are recorded in `tmp/install-after-commit/REPORT-v2.md`.

Final gate lines on committed production source `9fe5ae1d6`, before documentation-only amendment:

```text
$ go build ./...
go build ./... exit=0
$ GOOS=linux GOARCH=amd64 go build ./...
GOOS=linux GOARCH=amd64 go build ./... exit=0
$ CGO_ENABLED=0 go test ./internal/db/sqlite
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.836s
$ go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1
ok  	github.com/syncthing/syncthing/lib/model	73.978s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.498s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.061s
$ go test -race ./lib/model ./internal/db/sqlite -run '^(TestInstallAfterCommit.*|TestInstallOrderingRegression.*|TestPullCommitConnection.*|TestPullDB.*|TestSQLWriteInventory|TestSingleWriter.*|TestPullRetryFailedCommitRetainsStateAndDrains|TestDeleteConflictLongNameCurrentlyStaysNeeded|TestConflictName.*|TestConflictLongNamePruning|TestPostPull.*|TestDripHeldScanAcrossPull|TestMetabatchHeldScanAcrossPull)$' -count=20
ok  	github.com/syncthing/syncthing/lib/model	136.404s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	46.068s
```

## Historical v1 measurements

The tables below are retained as the cost that triggered the v2 repair. They are not measurements of the nonblocking implementation. Rerun baseline/fixed-67 tables, syscall totals and default-syncfs bursts are in `tmp/install-after-commit/REPORT-v2.md` and `MEASUREMENTS-v2.md`.

Identical opt-in `TestInstallPipelineMeasurement` overlaid onto baseline `bc986c263`, without production-code changes there. Both versions use real temps, real SQLite and the real copier/finisher/updater pipeline with eight finishers. Payload is 576 bytes. Trickle is 50 files with a 50 ms idle gap after each visibility event. Burst is 2000 files. Files/s includes those gaps and waits until final updater completion. p50/p99 measures actual successful temp Sync return to actual rename visibility, not enqueue time or total end-to-end transfer time. Temp/dir counts wrap real File.Sync calls, not mocked fsyncs. WAL counts are actual syscall observations within the printed nanosecond interval and isolated DB directory. Setup/teardown is excluded. Main DB checkpoint sync counts are separately retained in the reducer output.

Explicit `STSYNCMODE=fsync` is used on both trees. Rebased-66 defaults to Linux batch syncfs after rename, for which a pre-visibility temp-flush timestamp does not exist. This setting isolates the requested ordering comparison and is a qualification, not a claim to measure Linux's default syncfs configuration. STDRIPMODE=0 and STMETABATCH=0 isolate this path identically. Candidate install jobs do not additionally invoke batch syncfs.

Mac: local APFS, native cgo SQLite, dynamic C interposition for SQLite fsync/F_FULLFSYNC. The first nice-launched run stripped DYLD_INSERT_LIBRARIES through SIP and produced no WAL probe data, so it is not used in these tables. The reported run invokes the test binary directly with the validated probe. Yarlung: Linux amd64, /var/tmp on Btrfs, CGO_ENABLED=0 modernc SQLite, nice priority 15 and strace -f -ttt -yy on only the isolated test binary. Binary/data/trace directory was removed after copying results. No running service was inspected, restarted or modified.

### Mac

| Tree | Mode | Files/s | Temp fsync/file | WAL fsync/file | Dir fsync/file | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| rebased-66 | trickle 50 | 17.203 | 1.0000 | 0.0000 | 0.0200 | 0.262 | 0.754 |
| patch/67 | trickle 50 | 13.560 | 1.0000 | 1.0000 | 2.0000 | 12.046 | 25.804 |
| rebased-66 | burst 2000 | 172.599 | 1.0000 | 0.0000 | 0.0015 | 7.667 | 40.242 |
| patch/67 | burst 2000 | 103.719 | 1.0000 | 0.2515 | 0.5010 | 39.224 | 129.314 |

### Yarlung

| Tree | Mode | Files/s | Temp fsync/file | WAL fsync/file | Dir fsync/file | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| rebased-66 | trickle 50 | 11.635 | 1.0000 | 0.0000 | 0.0200 | 1.005 | 1.677 |
| patch/67 | trickle 50 | 7.777 | 1.0000 | 1.0000 | 2.0000 | 46.401 | 76.596 |
| rebased-66 | burst 2000 | 117.095 | 1.0000 | 0.0000 | 0.0020 | 3.090 | 12.709 |
| patch/67 | burst 2000 | 30.476 | 1.0000 | 0.2510 | 0.5000 | 110.238 | 493.858 |

Baseline zero WAL calls within the measured interval is an observed result of NORMAL commits and the checkpoint threshold, not a durability claim. Candidate burst counts include the periodic checkpoint: 501 full + 2 normal WAL calls on Mac, 502 WAL calls on yarlung, and one main DB checkpoint sync each. Candidate directory counts include BOTH temp-name-before-commit and rename-after-commit fences. Durability is materially more expensive here: about 1.66x Mac burst elapsed time and 3.84x yarlung burst elapsed time. These single-run traced measurements are evidence of cost, not a statistical capacity benchmark or a production speed prediction.

The final candidate was compiled from committed source `9fe5ae1d6`, then only documentation was amended. The initial candidate pass is retained separately, not substituted for that final committed-code measurement. Baseline remains `bc986c263` with the identical opt-in measurement source overlaid.

Raw artifacts and reproduction: `tmp/install-after-commit/baseline.log`, `baseline.trace`, `candidate-committed.log`, `candidate-committed.trace`, `mac-baseline-traced.log`, `mac-baseline-traced-sync.log`, `mac-committed-traced.log`, `mac-committed-traced-sync.log`, `provenance-sha256.txt`, `COMMANDS.md`, `measure_sync.c`, `analyze_measurements.py`, `MEASUREMENTS.md`, `yarlung-cleanup.log`. The opt-in pipeline measurement remains in the regression source and is skipped unless STINSTALLBENCH=1.

## Open risks

- FULL holds updateLock through slow storage flushes, and the measured throughput/visibility cost is substantial. No fairness bound or bounded shutdown of an in-flight OS/SQLite call is claimed.
- The postcommit filesystem recheck is not atomic with rename against an arbitrary external editor. Same-metadata content edits can evade the existing scanIfItemChanged check. These are pre-existing filesystem race limits, not solved by transaction ordering.
- Ordinary metadata-only updates, deletes, directory and symlink pull operations are outside the new regular-file durable-install contract. A rename shortcut also retires its source only after destination publication, so a crash can temporarily leave both names.
- Recovery preserves uncertain old targets as extra conflict copies. Existing occupied bounded-conflict destinations are rejected rather than overwritten. Missing/corrupt recovery temps refuse scans and need operator repair or future re-fetch support.
- Encrypted recovery proves existing trailer identity, not ciphertext integrity, which an untrusted receiver cannot derive from opaque block tokens. No stronger cryptographic property is claimed.
- DisableFsync retains the user's explicit filesystem durability opt-out, so that configuration does not provide the temp/directory stable-media guarantee. Unsupported directory Sync errors are surfaced, not silently treated as durable. Mac/Linux are validated, not Windows or network filesystems.
- A rare edited-target resolution durably clears the pending bit before moving its incoming temp to a conflict name. A crash in that short interval retains the genuine local target and temp, but may leave the incoming temp untracked for later expiration. No predecessor is falsely adopted, but this is not a transactional filesystem conflict-copy guarantee.
- SQLite FULL/fsync relies on the storage stack honoring sync. Tests model the crash boundary with fresh handles, not physical power-cut injection.
