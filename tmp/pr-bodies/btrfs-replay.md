# PR: model: initial scan from btrfs snapshot diff on btrfs folders

Branch: upstream/btrfs-replay (9 commits, stacked on the fsevents-replay PR)

## Symptom

On a folder living on a btrfs filesystem, syncthing re-walks the entire tree
at every startup ("initial scan"). On a 5.7 GB / 900k-file subvolume that walk
takes ~10 minutes, during which the folder is unavailable for pulls and watch
events (even with chunked scans, the files have to be re-hashed).

## Fix

If the folder is on btrfs and we have a recorded baseline, skip the initial
walk and apply the snapshot diff instead:

1. At startup, if `meta.btrfsBaseline` exists, snapshot the subvolume and run
   `btrfs send --no-data` against the baseline snapshot in a dedicated
   temporary directory. Parse the stream (self-contained `lib/btrfsreceive`
   parser, no dependency on btrfs-progs at runtime beyond the `btrfs` CLI).
2. Apply each diff entry (create/delete/rename/attr) to the database the same
   way watch events would — with stale-file detection so concurrent writes
   during the diff don't lose data (anything written after the snapshot is
   targeted-scanned).
3. On shutdown, atomically promote the new baseline snapshot: create
   baseline.new, swap, delete the old one.

Detection is opt-out: a folder on btrfs uses the fast path automatically; a
missing/corrupt baseline or a failed diff falls back to the normal initial
scan. Requires permission to create snapshots (a user-owned subvolume, or the
folder itself being a subvolume).

## Numbers / evidence

- Before: initial scan of the 5.7 GB subvolume: ~10 min.
- After: the diff of a typical session's changes replays in seconds — observed
  on a real boot: 16 changed paths, initial scan skipped entirely, folder
  usable immediately.

## Tests

- `lib/model/folder_btrfsreplay_test.go`: end-to-end against a loopback btrfs
  volume (skipped unless a test helper is available) — diff applied, stale
  writes preserved, baseline swap on shutdown.
- `lib/btrfsreceive`: stream parser unit tests.
