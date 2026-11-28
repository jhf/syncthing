# DRAFT, not posted. Needs the user's review and word before anything goes upstream.

**Title:** Conflict copy name can exceed 255 bytes; a remote delete of a long-named file then fails forever

## Summary

`conflictName` adds `.sync-conflict-YYYYMMDD-HHMMSS-XXXXXXX` (38 bytes with a 7-character device ID) to the base name, with no length check:

```go
// lib/model/folder_sendrecv.go @ 1107b3cd1
func conflictName(name, lastModBy string) string {
    ext := filepath.Ext(name)
    return name[:len(name)-len(ext)] + time.Now().Format(".sync-conflict-20060102-150405-") + lastModBy + ext
}
```

Any file whose name is longer than 217 bytes therefore gets a conflict name over 255 bytes, which most filesystems refuse. When a remote deletion of such a file needs a conflict copy (the local version is not an ancestor of the deletion), the move fails with `file name too long`, the original stays, and the deletion stays needed. Every retry builds a new timestamped name and fails the same way: we saw 213 distinct failing names in 41 minutes for one file. A conflicting pulled update would hit the same `moveForConflict` path.

Also, `deleteFileWithCurrent` logs `Deleted file` unconditionally at its end, including after this failure, so the log claims a deletion that did not happen.

## Reproduction on 1107b3cd1 (main, 2026-11-28)

The test is `lib/model/conflict_longname_test.go`. It uses the real puller iterations, the real SQLite need list and the real basic filesystem. It skips unless the host enforces a 255-byte name limit. The fixture is:
- a 248-byte `.md` name, which needs a 286-byte conflict name;
- a short control file with the same history.

Each file is deleted remotely with a version that conflicts with the local one. The test asserts what a fix must give: both deletions converge on the first puller iteration, each original's bytes are kept in one conflict copy whose name fits in 255 bytes, and two more iterations change nothing. On main it fails on the first iteration (directory shortened):

```text
--- FAIL: TestDeleteConflictLongNameConverges (0.03s)
    conflict_longname_test.go:128: attempt 1: the deletion did not converge: map[aaaa….md:syncing: delete file: lstat <dir>/aaaa….sync-conflict-20261128-132759-AIR6LPZ.md: file name too long]
```

## Possible fix

Shorten the base so that the whole name fits in 255 bytes, cutting at a UTF-8 boundary and keeping the extension and the conflict suffix intact. Add a short hash of the full original base to the shortened name, so that different long originals in one directory cannot collide. Our fork does this. If the filesystem still answers `ENAMETOOLONG`, for example a lower limit on an encrypted filesystem, it retries once with a hash-only name of about 143 bytes. `Deleted file` should be logged only on success.
