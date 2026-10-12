# PR: model: chunked, resumable scans with mid-scan pull/watch servicing

Branch: upstream/scan-chunking (16 commits on main)

## Symptom

A full scan of a large folder holds the folder's serve loop for its entire
duration: while scanning, nothing is pulled and no watch events are serviced.
Measured on a 2.4M-file folder: a 585-second scan with a 300-file pull queue
frozen behind it; the GUI showed "Preparing to Sync" for 15+ minutes with
needFiles=0 because state transitions queued behind the scan.

## Fix

- `lib/scanner`: a walk can resume from a marker (`ResumeFrom`), reports its
  enumeration frontier when cut, and reports aborted files instead of dropping
  them silently. `lib/fs`: Walk honours its documented lexical order (the
  resume predicate depends on it).
- `lib/model`: the scan walk runs in bounded chunks with a time budget
  enforced by a timer (not only per result); between chunks the folder
  services one queued pull and the queued watch events, handing its I/O token
  back first so a pull can't deadlock at maxFolderConcurrency=1. The deleted-
  files sweep services the same queues. Chunk progress is checkpointed so a
  restart (or a later rescan) resumes at the marker rather than walking from
  the root.
- Budget no-progress is handled by growing the budget (100ms floor, 5min cap),
  and only budget-cut aborts hold the resume marker — scan errors don't.

## Also in this branch (state bugs exposed by the testing)

- `fix(model): restore the scanning state after mid-sweep pull servicing` —
  the "Preparing to Sync" label leak above.
- `fix(model): keep the end-of-sync summary nudge instead of dropping it`.

## Numbers / evidence

- Before: pull latency during a full scan = scan duration (585 s observed).
- After: pulls and watch events run at chunk boundaries (seconds). Scan wall
  time is unchanged (the budget only interleaves).
- Live-confirmed over weeks of the fork running the 2.4M-file home folder:
  small changes sync end-to-end in 1-2 s even with scans running.

## Tests

`lib/model/folder_chunkedscan_test.go`: chunked scans deliver everything,
pulls and watch events are serviced mid-scan and mid-sweep, resume markers
survive restarts, budget growth on no-progress chunks. Teeth-verified by
reverting individual pieces and watching the tests fail.
