# Patch 59: overlong conflict names keep remote deletions pending

## Scope and verdict

Test-only. No production code change or fix. The hypothesis is confirmed for the default case-checking filesystem configuration: a 248-byte ASCII `.md` basename produces a 286-byte conflict basename (38 additional bytes with a seven-character short device ID), exceeding the host's verified 255-byte component limit. The reported `lstat` is on the **conflict destination**, not the original. The original remains intact and the SQLite remote deletion remains needed on every attempt.

This reproduces the failure once a conflict is required. It does not establish why the receiver's original version vector diverged in production. The test deliberately supplies concurrent local/remote vectors and a winning remote deletion. Nothing writes the fixture after its initial creation and scan.

## Exact path and misleading logs

References are to production code at parent `0ec3bf87d`:

1. `lib/model/folder_sendrecv.go:402` walks `AllNeededGlobalFiles`. Remote regular-file deletions are collected at lines 445-463, then `processDeletions` calls `deleteFile` at line 698. `deleteFile` reads the local SQLite entry at lines 1308-1314 and calls `deleteFileWithCurrent`.
2. `deleteFileWithCurrent` checks the original disk state at line 1351, detects `InConflictWith(cur)` at line 1367, and calls `moveForConflict` at line 1371. The writable-directory wrapper is `lib/model/util.go:24-65` and does not transform this error.
3. `conflictName`, `lib/model/folder_sendrecv.go:2737-2740`, preserves the entire basename and inserts the timestamp/device suffix before the extension with no component-length check. `moveForConflict` builds that name at line 2381 and calls `f.mtimefs.Rename(name, newName)` at line 2382.
4. With `CaseSensitiveFS=false`, `lib/config/folderconfiguration.go:133-134` installs case-conflict detection even on a case-sensitive filesystem. `lib/fs/casefs.go:259-272` checks the source, then calls `checkCase(newpath)` at line 264. **The failing call is `f.Filesystem.Lstat(name)` in `checkCase`, `lib/fs/casefs.go:392`, where `name` is the overlong conflict destination.** `BasicFilesystem.Lstat`, `lib/fs/basicfs.go:180-187`, passes it to the host. On this macOS test host, `lib/fs/basicfs_lstat_regular.go:14` calls `os.Lstat`. The error is ENAMETOOLONG and is not swallowed as ENOENT. The underlying `BasicFilesystem.Rename` / `os.Rename` has not run. With case detection disabled, an overlong rename destination would instead reach the rename syscall; that configuration is not the `lstat` failure reproduced here.
5. `moveForConflict` returns the error at line 2406 without scheduling a conflict scan. `deleteFileWithCurrent` does not enqueue a completion at lines 1383-1386. Its second `Lstat`, line 1389, **is on the original**, but succeeds, so the conflict-path error remains in `err`.
6. The unconditional `slog.Info("Deleted file", ...)` at **line 1398** emits the misleading first line despite failure. The deferred handler then records `delete file: <error>` at line 1337 and logs **"Failed to delete file" at line 1338**. `newPullError` adds the `syncing:` prefix at line 2433. `pull` later emits **"Failed to sync" at line 260** (subject to churn warning throttling).
7. `deleteItemOnDisk`, lines 2451-2473, is the replacement/removal helper, called by `handleDir` at line 1028, `handleSymlinkCheckExisting` at line 1254, and `performFinish` at line 2162. It archives/removes a replaced item, and does not construct conflict names. Each caller has a separate conflict-preservation branch. It is not the failing call for this remote-deletion reproduction.

## Why it keeps retrying

There is no deletion completion job, so the local file/version is unchanged and SQLite continues returning the remote tombstone as needed. `pullerIteration` resets per-iteration errors at lines 290-293 and reads need again. The deletion branch at lines 441-463 does not use the regular-file retry gate at line 477. Each attempt builds another timestamped, still-overlong destination.

`sendReceiveFolder.pull` stops a no-progress round at lines 228-231, but returns unsuccessful when pull errors remain at line 282. The enclosing folder schedules another attempt with `pullFailTimer.Reset(delay)` at `lib/model/folder.go:673-684`. The service consumes that timer at lines 364-365. `serviceRetryPull` doubles the pause up to its bounded backoff condition at lines 863-864, not a terminal retry count. Thus it can retry indefinitely while this state persists and the folder keeps running, not spin forever inside a single call. New timestamps can explain distinct attempted conflict names in logs, not successfully created conflict files.

## Regression and control

`TestDeleteConflictLongNameCurrentlyStaysNeeded` in `lib/model/folder_sendrecv_longname_test.go`:

- Probes the real temporary filesystem: a 255-byte component must be creatable, and 256 bytes must fail with ENAMETOOLONG. Otherwise skips with an explicit reason. No fake filesystem limit or injected failure.
- Uses the real configured `sendReceiveFolder`, scanner, and SQLite opened by `newModel`. Stops folder services before creating/indexing the two files, eliminating peer/watcher scheduling.
- Uses exactly 248 ASCII bytes ending in `.md`, and `short.md`, in the same SQLite remote-deletion sequence. Both vectors require a conflict move.
- Runs three complete real `pullerIteration` calls. Every long-name attempt must record `syncing: delete file: lstat <conflict destination>: file name too long`, leave bytes and local version unchanged, emit no completion, and remain in `AllNeededGlobalFiles`.
- The first iteration must complete only the short control: original gone, exactly one conflict containing the original bytes, one scan request, and the incoming tombstone/version committed to SQLite. Later iterations have no completions or scan requests.
- Asserts the current bad outcome intentionally. The name/comment explicitly require flipping the long-name expectations when the fix lands. The timestamp itself is not asserted and there are no sleeps.

## Concurrent patch 58

The other agent's `0ec3bf87d` commit landed on shared `main` before this commit. Patch 59 is based directly on it, so no separate rebase is needed. Neither `PATCH58.md` nor `lib/model/folder_sendrecv_dbbusy_test.go` was edited or staged by this task.

## Validation

Host: Darwin arm64, `go version go1.26.4 darwin/arm64`. The real filesystem probe passed, so the regression executed rather than skipping. All three commands exited 0, in the requested order.

```text
$ go test ./lib/model -run '^TestDeleteConflictLongNameCurrentlyStaysNeeded$' -count=1 -v
--- PASS: TestDeleteConflictLongNameCurrentlyStaysNeeded (0.03s)
PASS
ok  	github.com/syncthing/syncthing/lib/model	0.639s

$ go test ./lib/model -count=1
ok  	github.com/syncthing/syncthing/lib/model	71.506s

$ go test -race ./lib/model -run '^TestDeleteConflictLongNameCurrentlyStaysNeeded$' -count=20
ok  	github.com/syncthing/syncthing/lib/model	2.917s
```

No fix has been implemented. The original content is preserved on failure, but remote deletion convergence remains broken until a separately authorized production fix.
