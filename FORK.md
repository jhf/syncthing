# How this fork is maintained

This repository is a soft fork of
[syncthing/syncthing](https://github.com/syncthing/syncthing). It tracks
upstream releases and carries the patches listed in `PATCHES.md`. The hope is
that upstream merges the patches so this fork can dissolve; until then it is
maintained for our own production use.

## Why a fork at all

The North Star is stated in the README: fast sync of one Mac home tree
(2.38M files, 262 GiB) to a remote Linux/BTRFS mirror. Upstream syncthing
moves deliberately and merges few outside PRs; our development loop is
AI-assisted and measured in hours, not release cycles. Some proposed fixes
have met maintainer pushback (the mtime existence-check skip,
`patch/01-skip-mtime-on-exists`), and the tracking issue that presents the
headline work with measurements
([syncthing#10921](https://github.com/syncthing/syncthing/issues/10921))
states plainly where that leaves us: we run these patches in production
regardless, so we maintain them ourselves, in the open, as cleanly separable
per-patch branches that upstream can take at any time. If upstream merges
them, this fork's reason to exist shrinks accordingly.

## Branch structure

- `main` — the only living branch: upstream `main` + all patches + this
  fork's own documentation (`PATCHES.md`, this file, and the README header
  below). Day-to-day work commits directly to `main`; we build and deploy
  from it.
- `patch/NN-*` — milestone views, not living lines. Each points at a commit
  inside `main`'s history such that the chain nests in order:
  upstream < patch/01 < patch/02 < ... < patch/NN < main, every segment
  contiguous. The compare view `patch/NN-1...patch/NN` is exactly one patch,
  which keeps each patch a standalone upstreamable unit and keeps
  `PATCHES.md`'s links meaningful. Because the pointers live inside `main`,
  they cannot drift from it; they are refreshed (reset to the segment
  boundaries) at each milestone, and may be left stale between milestones.
- `feature/btrfs-scan-replay` — frozen ops archive: the full development
  history including incident reports, deployment records, dev-build
  binaries, and temporary instrumentation. Not for building; for history.

## Rebase procedure (per upstream release)

The hard part is the grouping judgment; the pointers are bookkeeping.

1. `git fetch origin` (origin = syncthing/syncthing).
2. Group first: interactive-rebase `main` so each patch's commits form one
   contiguous, in-order segment (autosquash fixups into their patch). Do
   this while the base is still the old upstream — grouping is independent
   of the base bump.
3. Rebase `main` onto the new upstream in one pass: `git rebase origin/main
   main`. A clean replay here means every patch applies cleanly, in order.
4. Re-derive the stack: for each patch, find its boundary commit inside
   `main` (match by tree hash: `git rev-parse patch/NN^{tree}` against
   `git log main --format='%H %T'`), verify `git diff --quiet old new`,
   then `git branch -f patch/NN <boundary>`. Verify the nesting with
   `git merge-base --is-ancestor patch/NN-1 patch/NN` for all NN and
   `patch/20` ⊂ `main`.
5. Run the teeth tests: `go test ./lib/fs/ -run 'Replay|Barrier|Poll' -count=1`,
   `go test ./lib/model/ ./internal/db/... -count=1`, and
   `go build ./cmd/syncthing`.
6. Push with force-with-lease: `git push --force-with-lease fork main 'patch/*'`.
7. Build, deploy, bump the dev tag.

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

## Tracked vs machine-local

Fresh-clone rule: `git clone` + `go run build.go build` must work, and no
secret may ever be in the repo.

Tracked (besides upstream content):

- `PATCHES.md`, `FORK.md`, the README header — the fork's identity.
- `ops/custom-syncthing/btrfs/` — the remote receiver's BTRFS helper, socket
  adapter, and systemd units: deployment-critical, rebuildable from source,
  no credentials.
- `tmp/CUSTOM-SERVE.md`, `tmp/btrfs-scan-replay.md`,
  `tmp/syncpreparing-linger.md` — design/incident documents referenced by
  path from other docs. `tmp/` is otherwise ignored (allowlist in
  `.gitignore`; anything else under `tmp/` needs `git add -f`).

Machine-local (never committed; live in the working directory only):

- `custom-syncthing-*` dev-build binaries at the repo root.
- `tmp/` operational state: `CURRENT-BINARY` (points the launcher at the
  live binary), `run-custom-syncthing.sh`, `watch-dev.sh`, `*.log`,
  incident evidence, PR body drafts.
- Anything containing API keys, device config, or host-specific paths with
  credentials. Scripts that need the GUI API key read it from
  `config.xml` at runtime (`grep -o '<apikey>...'`), never from a checked-in
  value. Audited 2026-10-15: zero API-key material on any pushed branch of
  either fork (syncthing + notify); the only matches upstream-wide are
  upstream's own test fixtures.

The `feature/btrfs-scan-replay` branch is the full-history ops archive,
including the binaries and evidence that `main` deliberately does not
carry.
