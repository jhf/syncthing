# upstream-repros

This branch is upstream syncthing `main` plus tests that **fail on it**,
one per bug we found while running the jhf/syncthing fork. Each test
asserts the behaviour a fix must give, so:

- a test failing means the bug is still in upstream;
- a test that starts passing after a rebase means upstream fixed it, and
  the matching fork patch may be droppable.

Nothing here has been posted upstream. Each `repros/*.md` is an issue
draft that waits for Jørgen's word.

| Test (`lib/model/`) | Draft | Fork patch that fixes it |
|---|---|---|
| `TestDeleteConflictLongNameConverges` (`conflict_longname_test.go`) | `repros/conflict-longname.md` | `bounded-conflict-names` |
| `TestFailedPullCommitKeepsDiskAndIndexAgreed` (`failed_pull_commit_test.go`) | `repros/failed-pull-commit.md` | `install-after-durable-commit` |

Run:

    GOWORK=off go test -count=1 -run 'TestDeleteConflictLongNameConverges|TestFailedPullCommitKeepsDiskAndIndexAgreed' ./lib/model

The long-name test skips unless the host refuses a 256-byte file name
(macOS APFS and Linux ext4/btrfs both do).

Upkeep: rebase onto `upstream/main` with the weekly upstream watch and
rerun. The branch only adds files, so the rebase does not conflict unless
upstream renames the helpers the tests call.
