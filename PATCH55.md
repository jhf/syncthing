# patch/55: receiver finish, post-pull scan, and timestamp precision

Test-only investigation on 2026-11-05, based on f9dca720b (patch/54).
No production behavior changes, deployment, push, or receiver file edits.

## Verdict

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

## Exact production semantics inspected

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

## Real receiver sequence test

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

## Actual mapping disappearance, independently reproduced

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

## Delays and next fix candidate

Drip, metabatch, and retry cooldown are not necessary to create the metadata
version once a mismatch exists. The model cases run without sleeps, finish any
metabatch hold directly, and reproduce with `STMETABATCH=0` as well as enabled.
Only one metadata change is introduced, below drip's burst threshold. Direct
finish/scanner execution does not traverse retry admission, so cooldown cannot
be its prerequisite. This does not test all possible delay interactions.

Next fix candidate: patch/37's mtime batcher should preserve continuous
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

## Validation

Full verbatim transcripts are retained locally in `tmp/patch55/` (ignored build
artifacts, not committed): `targeted.log`, `model.log`, `visibility.log`,
`sqlite.log`, `repeat-model.log`, and `repeat-visibility.log`.
The targeted run includes patch/54's conflict test as well as patch/55.
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
