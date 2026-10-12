# PR: fs: skip the virtual-mtime Lstat on existence checks

Branch: upstream/fs-lstatexists (6 commits on main)

## Symptom

Every file-existence check on a `Filesystem` with virtual-mtime support paid
for an `LstatIfExists` wrapper that resolved the underlying filesystem before
asking "does this path exist?" — a question that doesn't need the virtual
layer at all.

## Fix

`LstatIfExists` calls `underlying.Lstat` directly when the filesystem supports
virtual mtimes (btrfs etc.), because existence doesn't depend on mtime
normalization. Call sites in `lib/db`, `lib/model`, and `lib/versioner` adopt
it; test files that need the real (non-virtual) view use `LstatU`.

## Numbers

One syscall saved per existence check; on a 2.4M-file scan that is a
measurable share of walk time on filesystems where Lstat is not free
(network/COW-backed).

## Tests

`lib/fs/lstat_if_exists_test.go` (virtual-mtime and plain cases), existing
suites in db/model/versioner unchanged and green.
