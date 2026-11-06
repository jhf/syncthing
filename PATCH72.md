# patch/72: install renames work in read-only directories

## Problem

Yarlung's `home` folder went into error at 2026-11-06 19:28:56 UTC and stayed there until this fix (3288 files needed at 20:53):

```text
ERR Failed initial scan (error="rename .../go/pkg/mod/github.com/dlclark/regexp2@v1.11.5/syntax/workdir/corpus/.syncthing.8298a532a08a20aa8eb1afad0d2506717b33f1b1-2.tmp .../corpus/8298a532a08a20aa8eb1afad0d2506717b33f1b1-2: permission denied")
```

The Go module cache makes its directories `0555`. Patch/67's crash recovery (`RecoverPendingInstalls`) renamed the pending temp with a bare `Rename`. Every other install and delete path makes a read-only parent writable for the operation (`inWritableDir`, or `RenameOrCopy`'s `withPreparedTarget`). Recovery runs before every scan and pull, so this one file blocked the whole folder: the initial scan failed, the folder entered its error state, and the retry loop repeated the failure 281 times.

Evidence from the folder DB: exactly one local entry carried the pending bit (`local_flags & 128`), and it was this file. 1816 sibling files in the same directory had installed normally. The pending entry was most likely left by a live publish that hit `EACCES` while a concurrent copier's `inWritableDir` restored the directory's `0555` mode. That restore is not serialized with `RenameOrCopy`'s rename lock. The failed publish leaves the index committed with the pending bit set, which is the intended crash-safe state. Recovery is the step that must then succeed.

## Change

- `RecoverPendingInstalls`: both renames (old target to its conflict name, and temp to target) run inside `inWritableDir`.
- `publishInstall`: when `RenameOrCopy` returns a permission error, retry once with the directory held writable around the rename itself.
- `preserveIncomingConflict`: the conflict rename runs inside `inWritableDir`.
- `inWritableDir` restores the original mode afterwards.

## Regression test

`TestInstallAfterCommitRecoveryReadOnlyDir`: a real FULL commit is followed by a simulated crash before the rename, with the parent at `0555`, for an absent and an existing target.

- On a6047e5c3: FAIL with the production error, `rename .../ro/.syncthing.pending.tmp .../ro/pending: permission denied`, and for the existing target `rename .../ro/pending .../ro/pending.sync-conflict-...: permission denied`.
- After: PASS. The file is installed, the pending bit is cleared, and the directory mode is back at `0555`.

Full `go test ./lib/model -count=1`: `ok 75.277s`.
