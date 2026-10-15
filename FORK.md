# How this fork is maintained

This repository is a soft fork of
[syncthing/syncthing](https://github.com/syncthing/syncthing). It tracks
upstream releases and carries the patches listed in `PATCHES.md`. The hope is
that upstream merges the patches so this fork can dissolve; until then it is
maintained for our own production use.

## Branch structure

- `main` — our production branch: upstream `main` + all patches + this
  fork's own documentation (`PATCHES.md`, this file, and the README header
  below). This is the default branch; what we build and deploy from.
- `patch/NN-*` — one branch per patch, in dependency order. Each contains
  all previous patches plus its own piece, so the diff between consecutive
  branches is exactly one piece. These are the upstreamable units: a PR to
  syncthing/syncthing is one of these branches (possibly split further).
- `feature/btrfs-scan-replay` — frozen ops archive: the full development
  history including incident reports, deployment records, dev-build
  binaries, and temporary instrumentation. Not for building; for history.

## Rebase procedure (per upstream release)

1. `git fetch origin` (origin = syncthing/syncthing).
2. Rebase the chain: check out `patch/01-skip-mtime-on-exists`,
   `git rebase --onto origin/main upstream-main-old patch/01-...`, then
   rebase each subsequent `patch/NN` onto the new `patch/NN-1` in order.
   The table in `PATCHES.md` documents the commit grouping if a full
   rebuild of the chain is ever needed.
3. Rebuild `main` from the chain tip; re-apply the fork-only documentation
   commit (README header, PATCHES.md, this file).
4. Run the teeth tests: `go test ./lib/fs/ -run 'Replay|Barrier|Poll' -count=1`
   and `go build ./cmd/syncthing`.
5. Push with force-with-lease: `git push --force-with-lease fork main 'patch/*'`.
6. Build, deploy, bump the dev tag.

## Watching upstream

We do not review every upstream PR. We watch changes under `lib/fs` (watch
backends), `lib/scanner`, `lib/model` (folder scan/pull loops), and
`lib/db` — the areas our patches touch. The rebase will surface conflicts
there anyway; the watch exists to give early warning.

## Relationship to upstream

Everything not listed in `PATCHES.md` is unmodified upstream. File bugs
against upstream unless they concern one of the listed patches. Upstream
issue tracking this work:
https://github.com/syncthing/syncthing/issues/10921
