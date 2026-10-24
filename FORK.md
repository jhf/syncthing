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
`patch/01-skip-mtime-on-exists` — proposed with measurements as issue
[#10909](https://github.com/syncthing/syncthing/issues/10909), closed as
not-planned), and the tracking issue that presents the
headline work with measurements
([syncthing#10921](https://github.com/syncthing/syncthing/issues/10921))
states plainly where that leaves us: we run these patches in production
regardless, so we maintain them ourselves, in the open, as cleanly separable
per-patch commit segments that upstream can take at any time. If upstream merges
them, this fork's reason to exist shrinks accordingly.

Long-term posture: so far the protocol (BEP) has never been the issue —
every patch sits above or below it, and we are wire-compatible with stock
syncthing today. If upstream remains uninterested, the fallback is a
maintained distribution: our own builds, tracking upstream releases, for
as long as that remains true. Protocol-level work is parked; the triggers
that would make us look at BEP are encoding efficiency, protocol chatter,
and bulk binary transfer efficiency — or an S3-style store-and-forward
target (under investigation). None of these has become a problem yet.

## Branch structure

- `main` — the ONLY branch: upstream `main` + all patches + this fork's
  own documentation (`PATCHES.md`, this file, and the README header
  below). Day-to-day work commits directly to `main`; we build and deploy
  from it. There are no per-patch branches and never will be again: the
  fork is one long, structured series of commits.
- Each patch's commits form one contiguous, in-order segment in `main`'s
  history, and every commit in the segment carries the patch's slug in
  its subject: `patch/NN: ...` (NN is the stable key; the long name —
  e.g. `skip-mtime-on-exists` — lives in PATCHES.md's slug column).
  That prefix discipline is what makes regrouping and rebasing
  mechanical. Non-patch commits (pure docs/ops/tests that belong to no
  patch) keep their conventional prefixes (`docs:`, `ops:`, `test:`).
  To upstream a single patch, extract its segment (format-patch /
  cherry-pick onto upstream) at that time.

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

## The two production machines (and why their numbers differ)

Same workload both ends — the Mac's home tree (2.38M files, 394k
directories, 262 GiB) mirroring to yarlung over WAN — on very different
hardware:

| | Mac (sender) | yarlung (receiver) |
|---|---|---|
| CPU | Apple M1 Max, 10-core (2021) | Intel Xeon E3-1245 v2, 4C/8T @3.4 GHz (Ivy Bridge, 2012) |
| RAM | 64 GiB | 16 GiB |
| Storage | NVMe SSD | btrfs RAID1 on 2×3 TB 7200 rpm HDD, no SSD |
| syncthing RSS (2026-10-24, both ~13 h uptime, idle, 0 errors) | 105 MB | 870 MB |
| of which Go runtime (`sys` from /rest/system/status) | 62 MB | 447 MB |
| of which C-side (RSS minus Go sys: sqlite caches etc.) | ~43 MB | ~423 MB |
| CPU while idle-ish | 0.0 % | ~1.5 % |

Both deltas are expected and by design:

**C-side memory (sqlite page cache).** This fork sets an explicit 256 MiB
page-cache budget per database pool on every connection
(`internal/db/sqlite/db_pagecache.go`, `STDBPAGECACHEMIB`), because the
stock 8 MiB cache on a multi-GiB database on spinning rust made every
random probe hit disk (observed 2026-10-19: ~400 rows/min at 1 % CPU, all
I/O-wait, against the 8.8 GiB folder DB). The cache grows on demand.
Yarlung's two pools (folder DB + main DB) actually fill: its scans and
pulls walk big page ranges and every page read is precious because the
alternative is a spindle seek. The Mac has the same budget but touches far
fewer distinct pages in a quiet window and its NVMe makes refills cheap,
so the cache stays small. Spending 400 MB of RAM to avoid disk seeks is
the correct trade on a 16 GiB machine that still has ~13 GiB of page
cache free.

**Go-side memory.** Same code, same 2.38M-file tree, but every operation
takes longer in wall time on yarlung — scans take minutes instead of
seconds, commits wait on the spindle — so in-flight buffers, batches, and
queues live longer, and the Go heap grows correspondingly larger before
the scavenger returns it (alloc 258 MB vs 19 MB at the same idle moment).

**CPU.** Identical work on a 2012 4-core/8-thread Ivy Bridge versus a
2021 10-core M1 Max: a ~5x per-core gap, and slow I/O stretches every
task's occupancy. 1.5 % of 8 threads is 0.12 cores busy — trivially fine,
and the number to watch is disk-io wait, not CPU.

**Deployment** (2026-10-23+): both ends deploy via
`~/syncthing-custom/deploy-mac.sh` / `deploy-yarlung.sh` (synced between
the machines, deliberately not committed: they read the local GUI key at
runtime on the Mac and carry yarlung's on yarlung). Builds are named per
machine — `custom-syncthing-jhf-macbook-pro.dev.N` and
`custom-syncthing-yarlung.dev.N` — so both machines see both builds in
the synced folder. `current` and `CURRENT-BINARY` are per-machine
pointers, `(?d)`-ignored in `.stignore-shared` since 2026-10-20 after the
Mac's `current` overwrote yarlung's through sync once. Yarlung builds
natively (go1.26.4 in `~/.local/go-sdk`, clone in `~/.local/src/syncthing`
pulling `fork/main`), which keeps its binary out of the sync path during
the build and gives it the C sqlite backend like the Mac.
