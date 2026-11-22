# measured-install-durability: one barrier per install group, chosen by measured cost

This record merges the design notes of the revisions folded into the
`measured-install-durability` patch, in the order they were written.

## `adaptive-syncfs-threshold`: Choose syncfs or per-file fsync by measured cost

### Problem

patch/install-after-durable-commit makes each install group durable through two fences: before the index commit, and after the renames. On Linux it chose one `syncfs(2)` when the group had 8 or more items, and one `fsync` per item otherwise. The 8 rested on a comment claiming syncfs was "measured ~0.5 s" against "~33 ms per small-file fsync". The 0.5 s was inferred, not measured. The true cost of a syncfs is whatever the whole filesystem has dirty at that moment, and that changes all the time.

Measured on yarlung (btrfs on 2x ST33000650NS, live Syncthing, 2026-11-06 20:47 UTC, 15 samples, 1 s apart):

```text
fsync(small file) ms: median 30.5 min 19.4 max 34.9
syncfs ms:            median 226.2 min 176.0 max 749.4
break-even group (syncfs/fsync, medians): 7.4
```

So the fixed 8 happened to be close at that moment. A syncfs sample of 749 ms gives a break-even near 25 files, though, and a quiet disk gives one near 2. A fixed number is wrong in both directions.

### Principle

Decide by what each method has actually cost here recently. Self-adjusting, no tuning knob, cheap.

### Change

- `installSyncCost` per fence (precommit, postrename) keeps an EWMA (alpha 1/4) of the syncfs duration and of the per-item fsync duration, each timed where it already runs.
- The fence uses syncfs when `n * perItem > syncfs`. A group of one always fsyncs.
- Measurements expire after 10 minutes (`installCostWindow`). Without a fresh pair, the fence falls back to patch/install-after-durable-commit's fixed rule (n >= 8), which also re-measures whichever method had stopped being chosen.
- An INF `Install durability cost measured (fence=..., syncfsFromGroup=N, syncfs=..., fsyncPerItem=...)` is logged when the break-even moves by a factor of two or more. It is not logged per group.
- The incorrect "measured ~0.5 s" comment is replaced by the measurement above.
- Unchanged: durability ordering, fallbacks on syncfs failure, macOS and `STSYNCMODE=fsync` behaviour.

### Regression tests

| Test | Base (6ee05cd0c, patch/ignored-leftover-warning) | After |
|---|---|---|
| `TestInstallAfterCommitAdaptsToSlowSyncfs`: real copier, 8 finishers and updater, 200 files, syncfs measured at 5 s and fsync at 1 ms per item | FAIL (base variant without the cost model): `syncfs measured at 5 s must not be chosen over 1 ms fsyncs, got 1 syncfs calls` | PASS: `files=200 syncfs=0 perFileSyncs=200` |
| `TestInstallSyncCostDecision` | n/a (new type) | PASS: unmeasured gives the fixed rule; quiet disk gives syncfs from 2; busy disk gives fsync up to ~130; stale gives the fixed rule |
| `TestInstallSyncCostLogsOnMoveOnly` | n/a | PASS: one line for steady costs, a new line on a 10x move |
| `TestInstallAfterCommitSyncfsGroupBarriers`, `...SyncfsFailureFallsBack` | PASS | PASS (unchanged behaviour without measurements) |

### Yarlung benchmark (default syncfs mode, `nice -n 19`, `GOMAXPROCS=2`)

`TestInstallPipelineMeasurement`, 3 runs each, base = patch/ignored-leftover-warning, cand = the `adaptive-syncfs-threshold` revision:

| Case | base files/s | cand files/s | temp fsyncs base / cand |
|---|---|---|---|
| burst, 2000 files | 475.3, 544.3, 571.1 (median 544.3) | 466.2, 482.0, 574.2 (median 482.0) | 1 / 1 |
| trickle, 50 files | 7.77, 8.06, 8.11 (median 8.06) | 7.62, 8.06, 7.92 (median 7.92) | 50 / 50 |

Both builds made the same choices: the burst groups are far above any break-even, and trickle groups are single files. The spread within each column (burst 475 to 571 on base alone) is larger than the difference between the medians, so the difference is noise. The benchmark does not exercise the 2 to 25 file band where the rule changes the outcome. The live log line will show which side real groups fall on.

## `durability-median-choice`: One durability model for Linux and macOS, chosen by medians measured on this machine

### Problem

The `adaptive-syncfs-threshold` revision chose between one `syncfs(2)` per install group (the "barrier") and one `fsync` per item using EWMA costs that expired after 10 minutes. Without fresh data it fell back to a fixed rule: barrier iff n ≥ 8. The user rejected that fallback: 8 is not a sensible number for every machine.

Measured with the same system calls (scratch `fsprobe`, 2026-11-07):

| Machine | per-item | barrier | break-even | predicted winner correct for n = 1, 2, 4, 8, 16, 32 |
|---|---|---|---|---|
| yarlung, btrfs on 2× HDD | fsync median 25.8 ms | syncfs median 131.0 ms | 5.08 | 6/6 |
| Mac, APFS SSD | F_FULLFSYNC median 6.1 ms | plain fsyncs + one F_FULLFSYNC, median 5.6 ms | 0.91 | 6/6 |

Under overnight sync load yarlung's syncfs rose to 2.6 s, so its break-even moves with load. A fixed 8 is wrong in both directions.

Two properties of the `adaptive-syncfs-threshold` revision, shown failing on c57190dad by a before-run test:
- `warm-up never measured the barrier on small groups`: for n < 8 it never tried syncfs, so it never had the data to choose it.
- `decision fell back to the fixed n>=8 rule instead of measuring`: an estimate older than 10 minutes returned to the constant.

### Change (agreed design, 2026-11-07 10:03 to 12:44)

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

### Regression tests

| Test | Shows |
|---|---|
| `TestInstallSyncCostWarmupAlternates` | both methods measured early, including n=2; on the `adaptive-syncfs-threshold` revision the barrier was never tried for n < 8, and stale data fell back to n ≥ 8 (before-run failures quoted above) |
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

### Benchmarks (`TestInstallPipelineMeasurement`, default mode, `nice -n 19`, `GOMAXPROCS=2`)

base = patch/prefix-sweep-name-range (c57190dad), cand = the `durability-median-choice` revision. Medians of 3 runs, files/s:

| | base | cand | ratio |
|---|---|---|---|
| Mac burst, 2000 files | 122.91 | 1634.72 | **13.30** |
| Mac trickle, 50 files | 14.49 | 14.42 | 0.99 |
| yarlung burst, 2000 files | 496.18 | 554.10 | 1.12 |
| yarlung trickle, 50 files | 7.55 | 6.89 | 0.91 |

Mac burst runs: base 100.2, 122.9, 139.2 (`temp_sync=2000 dir_sync=~580`); cand 1525.0, 1634.7, 1647.0 (`temp_sync=7 dir_sync=7..8`). The 2000 per-temp `F_FULLFSYNC`s became a handful of group flushes.

Yarlung trickle, cand `temp_sync=47`: 3 of the 50 single-file groups took a syncfs while the barrier ring filled. That is the agreed warm-up, paid once after each start. This benchmark starts a fresh process for 50 files, so it measures mostly warm-up.

## `durability-refresh-on-schedule`: The durability refresh always runs on schedule, never gated by the stale median

### Problem

The `durability-median-choice` revision keeps the last 5 timings of each durability method (group barrier, per-item fsync) and uses the barrier iff n · median(per-item) > median(barrier). A method's ring only changes when that method runs. So every 17th fence re-measures the losing method.

The `durability-median-choice` revision also gated the barrier refresh: it ran only on a group where 2 · n · median(per-item) ≥ median(barrier). That condition reads the very median the refresh is supposed to replace.

Observed on yarlung (dev.212, log `custom-serve*.log`, UTC):
- 2026-11-14 05:43:23, right after the 12m56s offline ANALYZE: precommit `syncfs=10.181452623s`, per-item 1.11 s (disk busy flushing).
- Every precommit `Install durability cost measured` line from then until at least 2026-11-15 07:22 shows the same `syncfs=10.181452623s`. Per-item moved between 7 ms and 98 ms in the same period. Postrename syncfs, ungated in practice because its groups are larger relative to cost, read 0.72 to 0.86 s.
- Consequence: `syncfsFromGroup` 105 to 1458, so virtually every precommit fence used per-item fsync. Durable, slower.

At 40 ms per item the gate needs n ≥ 128 before the barrier is timed again. The more inflated the stored median, the larger the group it demands, so the error is self-sustaining.

### Principle

Re-measurement must not be conditioned on the value being re-measured. A fixed schedule already bounds the cost.

### Change

`installSyncCost.choose`: on every 17th fence (`installCostRefresh`), the losing method runs, whatever the group size. A per-item refresh on a large group still fsyncs only 3 items before the barrier. The `refreshDue` field and the 2× gate are removed.

Cost bound: at most one losing-method run per 17 fences. On yarlung's single-file trickle that is one extra syncfs (about 0.6 to 1 s measured this week) per 17 groups. The `durability-median-choice` revision's argument for the gate was a trickle bench of a fresh process, which mostly measures warm-up; the gate's cost here was 26 hours of per-file fsync.

Recovery from a stale outlier: the ring holds 5 samples and decides by the median, so 3 refreshes replace it. At 17 fences per refresh that is fence 51 in the test below.

### Alternatives considered (user discussion 2026-11-15)

- **Time decay / expiry.** The `adaptive-syncfs-threshold` revision had a 10-minute expiry and fell back to a constant. Expiry empties the rings during idle periods and forces a fresh warm-up, which costs more than one scheduled refresh, and needs a clock. Rejected again.
- **Outlier rejection at startup** (ignore the first minutes). Fixes this instance only; any later load spike can plant the same outlier. The schedule handles all of them.
- **Fixed refresh period** (kept). 17 is prime so the refresh does not phase-lock with any periodic group pattern.

### Regression tests

| Test | On a73fcbea7 (the `durability-median-choice` revision) | After |
|---|---|---|
| `TestInstallSyncCostStaleBarrierMedianRecovers`: rings at 10.18 s / 40 ms, true syncfs 0.6 s, n=20 | `barrier median still 10.181452623s after 102 fences of n=20; the refresh never re-measured it` | 3 refreshes, median recovered at fence 51, then n=20 picks the barrier |
| `TestInstallSyncCostBarrierRefreshOnSchedule`: n=2 trickle at yarlung costs | `n=2 trickle: barrier refreshes at fences [], want [17 34 51]` | refreshes at 17, 34, 51 |

Replaces `TestInstallSyncCostBarrierRefreshWaitsForCheapGroup`, which pinned the gate.

Gates: `go vet ./lib/model`; `go test -p 2 ./lib/model/... ./internal/db/... ./lib/syncthing/...` all ok (model 72.3 s).

### Build note

The Mac's Command Line Tools updated to SDK 27.2 on 2026-11-15 20:33. Xcode's `ld-1267` rejects its `.tbd` files (`unknown architecture`). Mac builds use `SDKROOT=.../MacOSX26.5.sdk CC=/usr/bin/clang` until Xcode is updated.
