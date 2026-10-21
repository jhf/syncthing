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
per-patch commit segments that upstream can take at any time. If upstream merges
them, this fork's reason to exist shrinks accordingly.

## Branch structure

- `main` — the ONLY branch: upstream `main` + all patches + this fork's
  own documentation (`PATCHES.md`, this file, and the README header
  below). Day-to-day work commits directly to `main`; we build and deploy
  from it. Each patch's commits form one contiguous, in-order segment in
  `main`'s history; `PATCHES.md`'s Commits column gives the segment size,
  so `patch/NN` is `git log` boundaries, not a ref. To upstream a single
  patch, extract its segment (format-patch / cherry-pick onto upstream)
  at that time.
- Per-patch branches were retired 2026-10-21: they duplicated what the
  linear history already records, and 60+ refs had become cruft. Git is
  the historian; no commits were lost (the branches all pointed inside
  `main`). The `feature/btrfs-scan-replay` ops archive (raw dev history
  with binaries and incident evidence, deliberately not carried on
  `main`) was retired the same day; its tip commit `82ee012c2` (2026-10-15)
  remains in the object store while any pre-cleanup clone or reflog holds
  it.

## Rebase procedure (per upstream release)

1. `git fetch upstream` (upstream = syncthing/syncthing).
2. Group first if needed: interactive-rebase `main` so each patch's
   commits form one contiguous, in-order segment (autosquash fixups into
   their patch). Do this while the base is still the old upstream —
   grouping is independent of the base bump. Day-to-day commits already
   land inside their patch's segment, so this is usually a no-op.
3. Rebase `main` onto the new upstream in one pass: `git rebase
   upstream/main`. A clean replay means every patch applies cleanly, in
   order.
4. Run the teeth tests: `go test ./lib/fs/ -run 'Replay|Barrier|Poll'
   -count=1`, `go test ./lib/model/ ./internal/db/... -count=1`, and
   `go build ./cmd/syncthing`. Linux-only test binaries are cross-compiled
   (`GOOS=linux go test -c`) and run on yarlung.
5. Push with force-with-lease: `git push --force-with-lease fork main`.
   We always push to the fork; it is the off-machine backup of record.
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
