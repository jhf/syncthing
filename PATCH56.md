# patch/56: integrated in-flight mtime rescan stops at finish

Test-only investigation on 2026-11-05, base `2ef5943c47e437f773ae7e4f1e52f63d48c67dc0`.
No production changes, receiver file edits, push, or incident-causality claim.

## Finding

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
2. Hold the actual folder DB `updateLock`, as in patch/55. Disable the latency
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

## Native nanosecond storage

**No mapping is needed after a normal pull on APFS or btrfs when Chtimes stores
the requested timestamp exactly. This visibility-hole mechanism cannot explain
the incident on that storage under those normal conditions.** BasicFilesystem
calls `os.Chtimes` (`lib/fs/basicfs.go:151-156`). The mtime wrapper stats the result
(`lib/fs/mtimefs.go:70-81`), and equal physical/requested times cause DeleteMtime,
not PutMtime (`mtimefs.go:171-183`). Storage names alone do not prove successful
Chtimes: failures or other actual timestamp discrepancies could still require
a mapping even on nanosecond-capable storage. No such discrepancy is established
here, and btrfs on yarlung was not exercised by this local macOS test.

## Validation

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
