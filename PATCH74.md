# patch/74: one durability model for Linux and macOS, chosen by medians measured on this machine

## Problem

Patch/71 chose between one `syncfs(2)` per install group (the "barrier") and one `fsync` per item using EWMA costs that expired after 10 minutes. Without fresh data it fell back to a fixed rule: barrier iff n ≥ 8. The user rejected that fallback: 8 is not a sensible number for every machine.

Measured with the same system calls (scratch `fsprobe`, 2026-11-07):

| Machine | per-item | barrier | break-even | predicted winner correct for n = 1, 2, 4, 8, 16, 32 |
|---|---|---|---|---|
| yarlung, btrfs on 2× HDD | fsync median 25.8 ms | syncfs median 131.0 ms | 5.08 | 6/6 |
| Mac, APFS SSD | F_FULLFSYNC median 6.1 ms | plain fsyncs + one F_FULLFSYNC, median 5.6 ms | 0.91 | 6/6 |

Under overnight sync load yarlung's syncfs rose to 2.6 s, so its break-even moves with load. A fixed 8 is wrong in both directions.

Two properties of patch/71, shown failing on c57190dad by a before-run test:
- `warm-up never measured the barrier on small groups`: for n < 8 it never tried syncfs, so it never had the data to choose it.
- `decision fell back to the fixed n>=8 rule instead of measuring`: an estimate older than 10 minutes returned to the constant.

## Change (agreed design, 2026-11-07 10:03 to 12:44)

**One concept on both platforms.** Each fence chooses between a full sync per item and the platform's **group barrier** (`installBarrierFn`):

| | per-item | group barrier |
|---|---|---|
| Linux | fsync | one `syncfs(2)` of the folder's filesystem |
| macOS | `F_FULLFSYNC` (Go's `File.Sync`) | a plain `fsync(2)` per item (no cache flush), then ONE `F_FULLFSYNC` |
| other | fsync | none, always per-item |

Barrier mode needs a basic (local) filesystem; `STSYNCMODE=fsync` turns it off. On macOS this replaces one `F_FULLFSYNC` per temp, which was the only option before. Apple documents `F_FULLFSYNC` as flushing all buffered data to permanent storage.

**Measurement, per fence.** Precommit and postrename keep separate measurements, as the user decided.
- **Rings:** the last 5 samples of each method. Barrier samples are barrier durations. Per-item samples are a per-item fence's total divided by its number of items.
- **Warm-up:** until each ring holds 3 samples, fence k uses the barrier iff frac(k·φ) < ½, a 50/50 golden-ratio alternation. A group of one item always uses per-item.
- **Decision:** the barrier iff n · median(per-item) > median(barrier), over what each ring holds. That is 3 samples at first, growing to 5 through the refresh.
- **Refresh:** every 17th fence makes a refresh of the losing method due.
  - A per-item refresh runs at once; on a large group it syncs only 3 items, timed, and the barrier covers the rest.
  - A barrier refresh waits for a group where the barrier costs at most twice the per-item fence. Until then the fence stays per-item, which is always durable.
- **Logging:** an INF `Install durability cost measured (fence, syncfsFromGroup, syncfs, fsyncPerItem)` once both rings can decide, and again when the break-even moves by 2× or more.
- **Removed:** the EWMA, the 10-minute expiry and the fixed n ≥ 8 fallback.

**Why 3 samples, and 50/50.** Simulated over 60 fences with the measured costs (yarlung trickle n=2 and burst n=32, yarlung loaded n=8, Mac n=2 and n=32), total excess over always choosing the best method:

| warm-up | total extra |
|---|---|
| 50/50, 5 samples | 18941 ms |
| 20/80 (barrier 20 %), 5 samples | 33194 ms |
| 80/20 (barrier 80 %), 5 samples | 53563 ms |
| **50/50, 3 samples (chosen)** | **12213 ms** |

A lopsided split takes longer to collect the rarer method, so it decides later. The observations do not favour one method either: per-item wins small groups on yarlung, the barrier wins large ones, and on the Mac the barrier always wins. A median of 3 still ignores a single outlier such as the first syncfs after startup (1.19 s against a 131 ms median on yarlung).

**Why a barrier refresh waits.** The first candidate refreshed the barrier on whatever group came next. On yarlung's single-file trickle groups (2 items, a temp and its directory), that added a full syncfs to a 50 ms fence. Trickle dropped from 6.16 to 4.89 files/s across all three runs. `TestInstallSyncCostBarrierRefreshWaitsForCheapGroup` pins the gate.

## Regression tests

| Test | Shows |
|---|---|
| `TestInstallSyncCostWarmupAlternates` | both methods measured early, including n=2; on patch/71 the barrier was never tried for n < 8, and stale data fell back to n ≥ 8 (before-run failures quoted above) |
| `TestInstallSyncCostMedianDecides` | yarlung costs: per-item for n ≤ 5, barrier from 6, the 1.19 s outlier ignored; Mac-like costs: barrier from n=2 |
| `TestInstallSyncCostRefreshFollowsLoad` | refreshes on fences 17 and 34; after three 2.6 s syncfs samples, n=8 switches to per-item |
| `TestInstallSyncCostBarrierRefreshWaitsForCheapGroup` | 40 fences of n=2 stay per-item; the pending refresh runs on n=4 |
| `TestInstallAfterCommitAdaptsToSlowSyncfs` | real pipeline, barrier measured at 5 s: `files=200 syncfs=0 perFileSyncs=200` |
| `TestInstallAfterCommitSyncfsGroupBarriers` | cheap barrier: `files=200 syncfs=1 perFileSyncs=1` |
| `TestDarwinGroupBarrierOneFullFsync` (macOS) | real files: a 5-item group gets exactly 1 `F_FULLFSYNC`; a missing item fails the barrier, so the fence falls back |
| `TestDarwinCostModelGroupsFromTwo` (macOS) | Mac costs: barrier for n = 2, 4, 32 |
| `TestDarwinInstallBarrierEnabled` (macOS) | barrier mode on a basic filesystem; off with `STSYNCMODE=fsync` (it was off on macOS before) |
| `TestDarwinBarrierCrashRecovery` (macOS) | real FULL commit with the temp made durable by the group barrier (exactly 1 `F_FULLFSYNC`), crash before rename, reopen: the sender's exact content and version are recovered, pending bit cleared |

Gates:
- macOS: `ok lib/model 87.713s`, `ok internal/db/sqlite 10.941s`, `ok lib/syncthing 1.425s`; race detector, 5 runs on the install, cost and darwin tests: `ok 31.309s`.
- Linux on yarlung (candidate binary, install, cost, pull-DB, dir-delete and leftover tests): `PASS`.
- Linux and Windows cross-builds pass.

## Benchmarks (`TestInstallPipelineMeasurement`, default mode, `nice -n 19`, `GOMAXPROCS=2`)

base = patch/73 (c57190dad), cand = patch/74. Medians of 3 runs, files/s:

| | base | cand | ratio |
|---|---|---|---|
| Mac burst, 2000 files | 122.91 | 1634.72 | **13.30** |
| Mac trickle, 50 files | 14.49 | 14.42 | 0.99 |
| yarlung burst, 2000 files | 496.18 | 554.10 | 1.12 |
| yarlung trickle, 50 files | 7.55 | 6.89 | 0.91 |

Mac burst runs: base 100.2, 122.9, 139.2 (`temp_sync=2000 dir_sync=~580`); cand 1525.0, 1634.7, 1647.0 (`temp_sync=7 dir_sync=7..8`). The 2000 per-temp `F_FULLFSYNC`s became a handful of group flushes.

Yarlung trickle, cand `temp_sync=47`: 3 of the 50 single-file groups took a syncfs while the barrier ring filled. That is the agreed warm-up, paid once after each start. This benchmark starts a fresh process for 50 files, so it measures mostly warm-up.
