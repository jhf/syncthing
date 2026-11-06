# patch/68: directory deletes wait for their contents

## Problem

On 2026-11-06 the user removed several build trees under `Projects/GenderedGames` on the Mac. Yarlung logged `Failed to delete directory ... is not empty; the contents are probably ignored on that remote device, but not locally` 228 times over 62 directories between 11:32 and 11:34 UTC, and the folder went onto its failure timer each round. Nothing was ignored. The directories were early.

Evidence for `prototypes/godot/out/root-visible-check/frames` (241 files), from yarlung's folder DB and log:

- Mac sequence: the directory tombstone is `29776576`, the files `29776577` to `29776817`. A device announces a removed tree directory first.
- Yarlung's local DB sequence for the Mac's entries: directory `64456913`, files from `64456914`. The index arrived in that order, in several messages.
- Yarlung's local commits: 34 files deleted at `64458220`..`64458872`, then the directory failed (11:32:23), the other 207 files were deleted at 11:32:50, and only then the directory (`64462683`).

So a pull started while the index was still arriving. It saw the directory deleted but 207 of its files still alive on the Mac, and failed the directory as "not empty".

A second, independent hole: `processDeletions` deleted directories "in reverse order to delete depth first", relying on the need walk's order. The default pull order is `random` (both hosts), so a parent could be tried before its child and fail the same way within one round.

## Principle

A directory delete that is only blocked by deletions still on their way is not a failure. It waits, with no error, no failure timer and no retry storm. It goes in the same round when its contents do, or in the pull that the contents' index update schedules anyway. Genuine blockers (content the deleting device never had, ignored files, local changes) keep their existing errors.

## Change

- `deleteDir` uses `deleteDirOnDiskPending` with a predicate from `deletionPendingFn`. An entry that keeps the directory is pending when either:
  - its global version is deleted (it goes this round, possibly in the deferred pass, or fails with its own error), or
  - a device that announced the directory's deletion (`GetGlobalAvailability` on the directory tombstone, still the global version) lists the entry as valid and not deleted. That device cannot have the entry without the directory, so its tombstone for the entry is still on the way.
- When every blocking entry is pending, the result is `errDirWaitingForContents`: no pull error, no `Failed to delete directory` line, a debug line and the churn counter `delete_dir.waiting_for_contents`.
- `processDeletions` returns the waiting directories. `pullerIteration` retries them once after the deferred file deletions, deepest first.
- Directory deletes are sorted by name descending, so every descendant comes before its ancestor regardless of pull order.
- Unchanged: ignored contents (`errDirHasIgnored`, patch/69's subject), changed files (`errDirHasToBeScanned`), receive-only changes, and "not empty" for anything not pending. Database errors count as not pending, which keeps the error.

## Regression tests

`lib/model/folder_dirdelete_order_test.go`, real model, real DB, real pull:

| Test | Base (d041da701) | After |
|---|---|---|
| `TestDirDeleteWaitsForContents`: dir tombstones plus half the files' tombstones arrive, pull, then the rest | FAIL `directories waiting for announced-but-unarrived deletions were recorded as failures` | PASS: no failure, tree still needed, then gone after the second message with `success=true` |
| `TestDirDeleteDepthFirstInRandomOrder`: parent tombstone listed before child | FAIL `map[tree:syncing: delete dir: directory has been deleted on a remote device but is not empty; ...]` | PASS |
| `TestDirDeleteGenuineNotEmptyStillFails`: a local-only file the deleting device never had | PASS | PASS (control: "not empty" kept, file survives) |

Full `go test ./lib/model -count=1`: `ok  github.com/syncthing/syncthing/lib/model 72.199s`.

## Cost

Only on the path where the directory is still non-empty after its own walk: one `GetGlobalFile` per blocking entry, one `GetGlobalFile` plus one `GetGlobalAvailability` per directory, and one `GetDeviceFile` per deleting device per blocking entry. The common path, an empty directory, is unchanged.
