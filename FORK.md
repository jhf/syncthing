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

## Tracked vs machine-local

Fresh-clone rule: `git clone` + `go run build.go build` must work, and no
secret may ever be in the repo.

Tracked (besides upstream content):

- `PATCHES.md`, `FORK.md`, the README header — the fork's identity.
- `ops/custom-syncthing/btrfs/` — the yarlung BTRFS helper, socket adapter,
  and systemd units: deployment-critical, rebuildable from source, no
  credentials.
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
