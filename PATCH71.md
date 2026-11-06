# patch/71: choose syncfs or per-file fsync by measured cost

## Problem

Patch/67 makes each install group durable through two fences: before the index commit, and after the renames. On Linux it chose one `syncfs(2)` when the group had 8 or more items, and one `fsync` per item otherwise. The 8 rested on a comment claiming syncfs was "measured ~0.5 s" against "~33 ms per small-file fsync". The 0.5 s was inferred, not measured. The true cost of a syncfs is whatever the whole filesystem has dirty at that moment, and that changes all the time.

Measured on yarlung (btrfs on 2x ST33000650NS, live Syncthing, 2026-11-06 20:47 UTC, 15 samples, 1 s apart):

```text
fsync(small file) ms: median 30.5 min 19.4 max 34.9
syncfs ms:            median 226.2 min 176.0 max 749.4
break-even group (syncfs/fsync, medians): 7.4
```

So the fixed 8 happened to be close at that moment. A syncfs sample of 749 ms gives a break-even near 25 files, though, and a quiet disk gives one near 2. A fixed number is wrong in both directions.

## Principle

Decide by what each method has actually cost here recently. Self-adjusting, no tuning knob, cheap.

## Change

- `installSyncCost` per fence (precommit, postrename) keeps an EWMA (alpha 1/4) of the syncfs duration and of the per-item fsync duration, each timed where it already runs.
- The fence uses syncfs when `n * perItem > syncfs`. A group of one always fsyncs.
- Measurements expire after 10 minutes (`installCostWindow`). Without a fresh pair, the fence falls back to patch/67's fixed rule (n >= 8), which also re-measures whichever method had stopped being chosen.
- An INF `Install durability cost measured (fence=..., syncfsFromGroup=N, syncfs=..., fsyncPerItem=...)` is logged when the break-even moves by a factor of two or more. It is not logged per group.
- The incorrect "measured ~0.5 s" comment is replaced by the measurement above.
- Unchanged: durability ordering, fallbacks on syncfs failure, macOS and `STSYNCMODE=fsync` behaviour.

## Regression tests

| Test | Base (6ee05cd0c, patch/69) | After |
|---|---|---|
| `TestInstallAfterCommitAdaptsToSlowSyncfs`: real copier, 8 finishers and updater, 200 files, syncfs measured at 5 s and fsync at 1 ms per item | FAIL (base variant without the cost model): `syncfs measured at 5 s must not be chosen over 1 ms fsyncs, got 1 syncfs calls` | PASS: `files=200 syncfs=0 perFileSyncs=200` |
| `TestInstallSyncCostDecision` | n/a (new type) | PASS: unmeasured gives the fixed rule; quiet disk gives syncfs from 2; busy disk gives fsync up to ~130; stale gives the fixed rule |
| `TestInstallSyncCostLogsOnMoveOnly` | n/a | PASS: one line for steady costs, a new line on a 10x move |
| `TestInstallAfterCommitSyncfsGroupBarriers`, `...SyncfsFailureFallsBack` | PASS | PASS (unchanged behaviour without measurements) |

## Yarlung benchmark (default syncfs mode, `nice -n 19`, `GOMAXPROCS=2`)

`TestInstallPipelineMeasurement`, 3 runs each, base = patch/69, cand = patch/71:

| Case | base files/s | cand files/s | temp fsyncs base / cand |
|---|---|---|---|
| burst, 2000 files | 475.3, 544.3, 571.1 (median 544.3) | 466.2, 482.0, 574.2 (median 482.0) | 1 / 1 |
| trickle, 50 files | 7.77, 8.06, 8.11 (median 8.06) | 7.62, 8.06, 7.92 (median 7.92) | 50 / 50 |

Both builds made the same choices: the burst groups are far above any break-even, and trickle groups are single files. The spread within each column (burst 475 to 571 on base alone) is larger than the difference between the medians, so the difference is noise. The benchmark does not exercise the 2 to 25 file band where the rule changes the outcome. The live log line will show which side real groups fall on.
