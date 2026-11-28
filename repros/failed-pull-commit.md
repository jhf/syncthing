# DRAFT, not posted. Needs the user's review and word before anything goes upstream.

**Title:** Pulled file is renamed into place before its index commit; a failed commit is silently dropped and later turns into a false conflict

## Summary

In `sendReceiveFolder`, a pulled file becomes visible on disk (`performFinish`: `RenameOrCopy` temp to target) before its index entry is committed. The commit happens later, in `dbUpdaterRoutine`, batched for up to `maxBatchTime` = 2 s. If that commit fails, the error is discarded:

```go
// lib/model/folder_sendrecv.go @ 1107b3cd1, dbUpdaterRoutine
batch := NewFileInfoBatch(func(files []protocol.FileInfo) error {
    ...
    f.updateLocalsFromPulling(files)   // error ignored
    ...
    return nil
})
```

The disk then holds the sender's new version, and the index still holds the old one. Nothing retries it. A crash between the rename and the commit leaves the same state.

The next scan sees content that differs from the index and records it as a **local change by this device** (a new counter for the local device, `ModifiedBy` = local). When the sender's following version arrives, based on a version this device never indexed, conflict detection fires. The device's own copy of what Syncthing itself installed becomes a `.sync-conflict-` file and is synced back to the other devices. No user or program ever wrote the file on the receiving side.

## Seen in practice

Two devices: a Mac, the only writer, and a Linux receiver on which nothing but Syncthing writes. Over a few days the receiver produced conflict copies of files the Mac had changed: an application state JSON, a SQLite WAL and a Rust source file. The receiver's log showed, for the same files:

1. `Synced file`: the rename happened.
2. A pull batch failing with `database is locked`, with no retry.
3. Later, `file modified but not rescanned`, then a scan.
4. Then the conflict, when the next Mac version arrived.

The conflict copies' mtimes matched the installed versions to the nanosecond.

(The `database is locked` itself came from a long write transaction elsewhere in that build. Any failed or interrupted commit, including a crash or power loss in the up-to-2 s window, has the same effect.)

## Reproduction on 1107b3cd1 (main, 2026-11-28)

The test is `lib/model/failed_pull_commit_test.go`. It uses the real `performFinish`, the real `dbUpdaterRoutine`, the real scanner and the real SQLite database. The only injection is one failed `DB.Update` call, which models the atomic no-commit outcome of `SQLITE_BUSY`. It asserts what a fix must give, and on main all three checks fail:

```text
--- FAIL: TestFailedPullCommitKeepsDiskAndIndexAgreed (0.01s)
    failed_pull_commit_test.go:121: 1: a failed pull commit left disk and index apart: disk holds "{\"state\":\"S1 installed by Syncthing\"}\n", the index still describes version {[{AIR6LPZ 1793916000}]}
    failed_pull_commit_test.go:128: 2: the scan recorded content Syncthing installed as a local edit by the receiver: version {[{AIR6LPZ 1793916000} {ZNWFSWE 1795868879}]}, modifiedBy ZNWFSWE
    failed_pull_commit_test.go:137: 3: false conflict [state.sync-conflict-20261128-132759-AIR6LPZ.json] holds content Syncthing itself installed
```

## Possible directions (for discussion)

1. **Minimal:** do not drop a failed pull batch. Retry the commit while the batch is still in memory, with backoff and no retry limit, and surface a persistent failure instead of discarding it. This closes the dropped-commit case, but not the crash window.
2. **Order:** commit the index entry durably before the rename makes the file visible. A crash then leaves a committed entry plus a complete temp file, which can be finished or re-pulled at startup, never scanned as a local edit. We run this in a fork: a pending-install bit is set in the committed entry and cleared after the rename. Installs are grouped so that the extra fsync per file costs little on spinning disks. We can share measurements and code if this direction is of interest.

Either way, a failed or interrupted pull commit should never turn into a version authored by the receiving device.
