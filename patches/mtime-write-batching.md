# mtime-write-batching: batched mtime bookkeeping, visible until committed

## In-flight visibility (2026-11-24)

### Problem

`flushLocked` swapped the pending map out under the batcher mutex, released
the mutex, then waited for the folder `updateLock`. `GetMtime` consulted only
the new pending map and committed SQL, so for the whole wait a mapping
written before the flush read back as absent. On a whole-second filesystem
the mapping is what restores a pulled file's sender timestamp. A post-pull
scan in that window saw the physical timestamp and generated a receiver
version of unchanged sender bytes. Two further defects sat in the same
function: overlapping flushes could commit out of swap order (an older batch
overwriting a newer one), and a failed transaction discarded its batch.

### Change

- Swapped batches go on `inflight` (oldest first) instead of being local
  to one flush. `lookup` reads `pending`, then `inflight` newest first.
- The flush that holds `updateLock` snapshots the in-flight list and applies
  every batch, oldest first, in one transaction (`apply`). A later flush that
  finds its batch already committed returns. Batches appended while the
  transaction runs stay for the next flush.
- On failure the applied batches are merged (oldest first, so memory stays
  bounded by distinct names) and kept ahead of newer ones, and the timer is
  re-armed, except after `close`.
- `write_inventory.json`: `flushLocked` no longer contains SQL; `apply` is
  the new boundary, reviewed as "only caller flushLocked holds updateLock
  across the whole transaction".

### Tests (fail before, pass after)

Before, on the previous batcher (`64614eb00`), with the new test files:

```text
    db_mtimes_visibility_test.go:86: mapping not visible while flush waits for the write lock: 0001-01-01 00:00:00 +0000 UTC -> 0001-01-01 00:00:00 +0000 UTC
--- FAIL: TestMtimeBatcherInFlightReadVisibility (0.02s)
    db_mtimes_visibility_test.go:125: read during overlapping flushes = 0001-01-01 00:00:00 +0000 UTC, want newer 1970-01-01 01:03:20 +0100 CET
--- FAIL: TestMtimeBatcherOverlappingFlushOrder (0.01s)
    db_mtimes_visibility_test.go:161: failed flush did not arm a retry
--- FAIL: TestMtimeBatcherFailedFlushRetained (0.02s)
    folder_sendrecv_inflight_test.go:206: mapping not visible while its flush waits for the write lock: 0001-01-01 00:00:00 +0000 UTC -> 0001-01-01 00:00:00 +0000 UTC
    folder_sendrecv_inflight_test.go:211: scanner filesystem exposed 2026-11-03 00:02:26 +0100 CET during flush, want virtual 2026-11-03 00:02:26.123456789 +0100 CET
    folder_sendrecv_inflight_test.go:223: scan during mtime flush minted a receiver version of unchanged sender content: version={[{AIR6LPZ 1} {ZNWFSWE 1795519717}]} ModifiedBy=ZNWFSWE
--- FAIL: TestPostPullScanDuringMtimeFlushKeepsSenderVersion (0.03s)
```

After:

```text
--- PASS: TestMtimeBatcherInFlightReadVisibility (0.01s)
--- PASS: TestMtimeBatcherOverlappingFlushOrder (0.01s)
--- PASS: TestMtimeBatcherFailedFlushRetained (0.01s)
--- PASS: TestPostPullScanDuringMtimeFlushKeepsSenderVersion (0.06s)
```

`TestPostPullScanDuringMtimeFlushKeepsSenderVersion` is the former
`inflight-mtime-rescan-tests` integration test, inverted: real finish, real
512-operation flush held at the write lock, real scan. It now requires the
scan to write nothing and the next sender version to install as a plain
successor (no conflict, no rescan).

### Limits

The data stays advisory: a crash still loses up to 2 s of bookkeeping. This
fix does not establish what caused the 2026-11-03 receiver-authored version
on yarlung; it closes one demonstrated path to that outcome.

## Investigation: receiver finish, post-pull scan, and timestamp precision (2026-11-05)

The investigation that found the hole, kept as written. "Current" below means
the batcher before the in-flight fix; its visibility test was inverted by the
fix, and its precision test now lives in `mtime-only-metabatch`.

Test-only investigation on 2026-11-05, based on f9dca720b (patch/install-after-durable-commit).
No production behavior changes, deployment, push, or receiver file edits.

### Verdict

**No bug reproduced in the ordinary completed-pull plus post-pull scan sequence.**
The remote vector survives unchanged with nanosecond, microsecond, and
whole-second sender timestamps, including actual whole-second truncation by the
fake filesystem. Permission application, ignored permissions, and remote
`NoPermissions` also preserve the remote vector. The native BasicFilesystem
controls pass on the local macOS machine, not on yarlung.

**A conditional receiver-local version is reproduced without editing the file:**
whole-second storage plus a missing virtual-mtime mapping exposes a 123456789 ns
mtime difference. The real scanner then assigns the receiver counter and
`ModifiedBy`, keeping identical content blocks and setting `PreviousBlocksHash`
to the downloaded content hash. Both metabatch settings reproduce this.
The model test explicitly deletes only the mapping to inject this condition.
That deletion is NOT claimed to occur naturally in production.

**A separate deterministic SQLite test proves a real mapping-visibility hole:**
an in-flight mtime batch removes the pending mapping before obtaining the DB
write lock and committing it. `GetMtime` returns absent in that interval, then
returns the mapping again after a successful commit. No crash, transaction
failure, or file edit is necessary for that DB behavior. These two tests prove
separate links, not one end-to-end reproduction of the actual flush interleaved
with the model scanner. Yarlung incident causality remains unproven.

### Exact production semantics inspected

- `lib/config/folderconfiguration.go:78,140-153`: `RawModTimeWindowS` has no
  nonzero default tag. Zero means exact mtime matching on macOS and Linux.
  Configured values are seconds. Android alone has an automatic two-second
  fallback except for detected ext2/3/4. `lib/model/folder.go:179-180` constructs
  the mtime-wrapped filesystem and initializes `f.modTimeWindow` from this config.
- `lib/protocol/bep_fileinfo.go:491-553`: optional equivalence compares name,
  type, deletion/invalid state, enabled ownership/xattrs and permissions, then
  regular-file size and mtime and (unless ignored) blocks. Mtime equality is
  `a.Equal(b)` OR `abs(a-b) < window`, strictly less, not less-or-equal. Vector
  counters and `ModifiedBy` are not part of that comparison. A nonzero difference
  is significant at the default zero window.
- `lib/model/folder_sendrecv.go:2131-2188`: `performFinish` applies permissions
  and platform data, checks the existing file before replacing it, renames or
  copies the temp file, calls `f.mtimefs.Chtimes` with the incoming Unix
  seconds/nanoseconds, then queues the incoming FileInfo unchanged for DB update.
  The Chtimes result is discarded at this call site.
- `lib/fs/basicfs.go:151-157`: BasicFilesystem calls `os.Chtimes`. Physical
  timestamp precision can differ from the incoming timestamp. The test's fake
  filesystem option `timeprecisionsecond=true` really truncates Chtimes to whole
  seconds (`lib/fs/fakefs.go:264-276`). Microsecond sender timestamps are tested
  on exact storage and second-truncating storage. A separate microsecond-only
  filesystem and round-to-nearest filesystem are not emulated here.
- `lib/fs/mtimefs.go:70-105,158-181`: Chtimes attempts the underlying operation,
  immediately stats the result, and saves the physical-to-requested mapping if
  they differ. Lstat/Stat substitute the requested virtual time only if the
  mapping's physical timestamp still equals the actual timestamp. This also
  compensates for failed underlying Chtimes, provided Lstat and mapping storage
  work. No timezone arithmetic is involved. Wire timestamps are absolute Unix
  seconds/nanoseconds. The tests construct the sender time in UTC+01 and assert
  instant equality against the disk and scanner times.
- `lib/model/folder_sendrecv.go:2608-2643`: `scanIfItemChanged` creates the disk
  FileInfo using the mtime-wrapped filesystem and ignores content blocks during
  the safety comparison. A mismatch returns `errModified` and queues a scan.
- `lib/scanner/walk.go:579-606,789-803`: the regular-file scanner skips equivalent
  metadata. Otherwise it hashes the file, and the generated FileInfo uses
  `src.Version.Update(w.ShortID)`, receiver `ModifiedBy`, and
  `src.BlocksHash` as `PreviousBlocksHash`. Unchanged content alone does not
  suppress a metadata-driven receiver version.

### Real receiver sequence test

`lib/model/folder_sendrecv_postpull_test.go`:
`TestPostPullScanTimestampPrecision` has 14 cases, each with metabatch off/on
(28 leaf subtests).

1. Start the real send-receive model with a real SQLite index, complete its
   initial scan, and stop its event loop for deterministic execution.
2. Write only the pull temp file, then install it with real `performFinish`.
   Commit the actual emitted job with `updateLocalsFromPulling` and assert that
   its sender-only vector is unchanged.
3. Assert actual raw mtime, virtual mtime, mapping (where needed), and applied
   permissions. There is no subsequent content edit, chmod, or Chtimes.
4. In fault cases only, remove the virtual-mtime mapping, not the file or its
   index entry. Test the real `scanIfItemChanged` result and rescan request.
5. Run real `scanSubdirs` and its scanner/DB update path twice. Assert unchanged
   bytes, blocks, no conflict, and preserved remote vector or the expected local
   vector. The redundant second scan must not change the resulting vector.
   Metadata-only held results are explicitly flushed through `commitScanBatch`
   rather than sleeping for the metabatch deadline.

`scanSubdirs` is the scanner body called by `Scan`. Calling `Scan` on the stopped
model would wait on the stopped event loop. The production
`pullScannerRoutine` deduplicates requested paths and calls `Scan` after the
pull scan channel closes (`folder_sendrecv.go:2345-2365`). It does not automatically
request every successfully downloaded filename. A watcher notification or other
queued rescan can nevertheless scan that filename after pulling. This test
executes that scan body directly after committing the finish job. It does not
claim to test watcher scheduling/deduplication itself. Two sequential scans are
an additional idempotence check, not a claim that production must scan twice.

| Condition | Result after scan |
| --- | --- |
| Exact native/fake timestamp, ns/us/whole-second sender | Remote vector preserved |
| Second-truncating FS, ns/us/whole-second sender, mapping available | Remote vector preserved |
| IgnorePerms or incoming NoPermissions | Remote vector preserved |
| Second-truncating FS, missing mapping, zero window | Receiver counter, identical content |
| Same missing mapping, 1 s window | Remote vector preserved |
| Same missing mapping, window exactly 123456789 ns | Receiver counter, strict boundary |

### Actual mapping disappearance, independently reproduced

`internal/db/sqlite/db_mtimes_visibility_test.go`:
`TestMtimeBatcherInFlightReadVisibility` uses the real SQLite DB and batcher.
It stops the latency timer, holds the actual folder DB write lock, and starts
`mtimes.flush()`. It waits for the pending map to become empty, not for an
arbitrary sleep. While flush is blocked at the write lock, reads return zero
mapping. Releasing the lock lets the same flush commit successfully and restores
read visibility. A five-second deadline is only a hung-test guard.

Source: `internal/db/sqlite/folderdb_mtimes.go:90-98` swaps out `pending` and
unlocks the batcher before taking `updateLock`. `GetMtime` at lines 142-160 checks
only the new pending map and committed SQL state, not the swapped-out in-flight
map. This contradicts continuous read-your-writes visibility during a flush.
The test intentionally pins current undesirable behavior and will need its
expectation inverted when fixed.

### Delays and next fix candidate

Drip, metabatch, and retry cooldown are not necessary to create the metadata
version once a mismatch exists. The model cases run without sleeps, finish any
metabatch hold directly, and reproduce with `STMETABATCH=0` as well as enabled.
Only one metadata change is introduced, below drip's burst threshold. Direct
finish/scanner execution does not traverse retry admission, so cooldown cannot
be its prerequisite. This does not test all possible delay interactions.

Next fix candidate: the mtime batcher should preserve continuous
read-your-writes visibility for in-flight operations, with newer pending
operations taking precedence, and preserve ordering across overlapping flushes.
Also inspect failure handling, which currently discards the swapped batch on
transaction errors. No such fix is included here.

Before attributing this to yarlung, establish whether the affected file needed
any virtual mapping at all, capture raw/index/virtual mtimes and permission,
ownership, and xattr differences at the mismatch, and establish a flush overlap.
Native nanosecond storage with a successful exact Chtimes normally needs no
mapping, so the demonstrated hole would not explain that case. A single
integrated model+in-flight-flush test is also still needed. Other candidates
remain Chtimes/Lstat or mapping-write failures, stale mappings, platform metadata
normalization, or stale held scan updates crossing a newer pull. None is claimed
proven by these tests. No conflict transport or incident replay is claimed.

### Validation

Full verbatim transcripts are retained locally in `tmp/patch55/` (ignored build
artifacts, not committed): `targeted.log`, `model.log`, `visibility.log`,
`sqlite.log`, `repeat-model.log`, and `repeat-visibility.log`.
The targeted run includes patch/install-after-durable-commit's conflict test as well as this patch.
Verbatim final result lines:

```text
$ go test ./lib/model -run 'TestPostPullScanTimestampPrecision|TestPullConflictFromReceiverLocalVersionDivergence' -count=1 -v
PASS
ok  	github.com/syncthing/syncthing/lib/model	0.972s

$ go test ./lib/model -count=1
ok  	github.com/syncthing/syncthing/lib/model	64.708s

$ go test ./internal/db/sqlite -run TestMtimeBatcherInFlightReadVisibility -count=1 -v
=== RUN   TestMtimeBatcherInFlightReadVisibility
    db_mtimes_visibility_test.go:73: mapping visible before flush, absent while flush waits for DB write lock
    db_mtimes_visibility_test.go:81: mapping visible again after successful commit
--- PASS: TestMtimeBatcherInFlightReadVisibility (0.01s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	0.538s

$ go test ./internal/db/sqlite -count=1
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.577s

$ go test ./lib/model -run TestPostPullScanTimestampPrecision -count=20
ok  	github.com/syncthing/syncthing/lib/model	6.644s

$ go test ./internal/db/sqlite -run TestMtimeBatcherInFlightReadVisibility -count=100
ok  	github.com/syncthing/syncthing/internal/db/sqlite	1.091s
```

The verbose model transcript also contains the individual subtests and model
logging. Only its terminal result lines are excerpted above. No production file
is changed by this patch.

## Investigation: integrated in-flight mtime rescan stops at finish (2026-11-05)

Kept as written. The fix above inverts this test: the scan no longer
generates a receiver version, so the finish-time safety check is no longer
reached.

Test-only investigation on 2026-11-05, base `2ef5943c47e437f773ae7e4f1e52f63d48c67dc0`.
No production changes, receiver file edits, push, or incident-causality claim.

### Finding

`TestPostPullInFlightMtimeStopsAtFinish` in
`lib/model/folder_sendrecv_inflight_test.go` establishes steps 1-3, with the
necessary distinction that the scanner generates its receiver-local FileInfo
while the writer is locked and commits it only after unlock. The exact proposed
sequence **does not reach the conflict rename**: step 4 returns
`checking existing file: file modified but not rescanned; will try again later`.
Steps 5-6 are not reached. No conflict exists to index or announce, so a fake-peer
exchange is not attempted. The test deliberately stops instead of adding another
rescan, deleting the restored mapping, or bypassing the safety check.

1. Real `performFinish` installs sender v1 onto whole-second fake storage and
   creates a mapping for its 123456789 ns timestamp. Its real completion job is
   committed with the sender-only vector unchanged.
2. Hold the actual folder DB `updateLock`, as in the receiver post-pull investigation above. Disable the latency
   timer before pulling, then reach the real 512-operation threshold using
   unrelated advisory mappings. Both triggers call the same `flushLocked`
   (`internal/db/sqlite/folderdb_mtimes.go:54-78`). Wait for the pending map to
   become empty under its mutex, not for a sleep. The swapped batch cannot
   commit while the writer is held (`folderdb_mtimes.go:90-98`), and actual
   `GetMtime` returns no mapping (`folderdb_mtimes.go:142-159`).
3. Run real `scanSubdirs`, the body called by post-pull `Scan`, on the stopped
   model. Its scanner uses `f.mtimefs` (`lib/model/folder.go:1198-1205`). Observe
   its generated FileInfo at the DB Update boundary: physical whole-second
   mtime, receiver counter and ModifiedBy, sender v1 counter, identical blocks,
   v1 PreviousBlocksHash and unchanged file bytes. Metadata comparison and
   version generation are in `lib/scanner/walk.go:578-599,789-798`.
   A test-only forwarding DB gate pauses before the actual write without
   replacing any scanner result. A write cannot complete while this lock is
   held (`internal/db/sqlite/folderdb_update.go:37-39`). Release the lock, join
   the successful flush, assert mapping visibility returns, and forward the
   unchanged scanner result into real SQLite. Assert that vector was indexed.
4. Deliver sender v4 with PreviousBlocksHash naming unseen v3. Although
   `v4.InConflictWith(cur)` is true, real `performFinish` first Lstats through
   the now-restored mapping (`lib/fs/mtimefs.go:102-114`). Its virtual fractional
   mtime differs from the physical whole-second mtime just indexed by the scan.
   `scanIfItemChanged` returns `errModified` and queues `lib.rs`
   (`lib/model/folder_sendrecv.go:2608-2636`). Finish returns at
   `folder_sendrecv.go:2144-2149`, before conflict handling at lines 2152-2160.
   Assert no finish job, no conflict, unchanged v1 bytes and unchanged index.

The private SQLite mutex/timer fields are accessed by a narrow reflection/unsafe
helper in the test only, under their owning locks. This avoids introducing any
production test hook. The pending map is never replaced by the test, and the
file's mapping is never deleted or artificially hidden. Five-second deadlines
are hung-test guards only. This tests a deterministically scheduled real
scanner/DB interleaving, not watcher/event-loop scheduling or later retries.
It proves a possible metadata-only receiver version, not a complete conflict
mechanism for this sequence and not the cause of the 2026-11-03 incident.

### Native nanosecond storage

**No mapping is needed after a normal pull on APFS or btrfs when Chtimes stores
the requested timestamp exactly. This visibility-hole mechanism cannot explain
the incident on that storage under those normal conditions.** BasicFilesystem
calls `os.Chtimes` (`lib/fs/basicfs.go:151-156`). The mtime wrapper stats the result
(`lib/fs/mtimefs.go:70-81`), and equal physical/requested times cause DeleteMtime,
not PutMtime (`mtimefs.go:171-183`). Storage names alone do not prove successful
Chtimes: failures or other actual timestamp discrepancies could still require
a mapping even on nanosecond-capable storage. No such discrepancy is established
here, and btrfs on yarlung was not exercised by this local macOS test.

### Validation

Full local logs are under ignored `tmp/patch56/`: `initial.log` records the
exploratory run that first exposed step 4's rejection; `targeted.log` pins that
observed rejection as the stopping result, rather than claiming the conflict
chain passed. Required validation result lines, verbatim:

```text
$ go test ./lib/model -run '^TestPostPullInFlightMtimeStopsAtFinish$' -count=1 -v
--- PASS: TestPostPullInFlightMtimeStopsAtFinish (0.02s)
PASS
ok  	github.com/syncthing/syncthing/lib/model	0.872s

$ go test ./lib/model ./internal/db/sqlite -count=1
ok  	github.com/syncthing/syncthing/lib/model	66.836s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.751s

$ go test ./lib/model -run '^TestPostPullInFlightMtimeStopsAtFinish$' -count=100
ok  	github.com/syncthing/syncthing/lib/model	1.908s

$ go test -race ./lib/model -run '^TestPostPullInFlightMtimeStopsAtFinish$' -count=100
ok  	github.com/syncthing/syncthing/lib/model	4.909s
```

The optional initial race check caught a captured test loop counter shared with
its observation loop (`race-initial.log`). Passing its initial value by argument
fixed that test-only race. All result lines above are from the final test file.
