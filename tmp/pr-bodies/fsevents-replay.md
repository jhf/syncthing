# PR: model: heal FSEvents overflow by journal replay instead of full scan

Branch: upstream/fsevents-replay (6 commits, stacked on the scan-chunking PR;
requires the notify PR first — syncthing/notify with SetSinceEventID)

## Symptom

When macOS fseventsd flags a watched stream as dropped (kFSEventStreamEvent-
FlagMustScanSubDirs / UserDropped), the whole folder is scheduled for a full
rescan. On a 2.4M-file folder that is minutes of lost sync latency and I/O —
triggered by, e.g., a burst of build output or a brief busy period.

## Fix (two layers)

**notify** (https://github.com/jhf/notify branch fsevents-replay):

- The watcher can resume from an event ID: `SetSinceEventID` /
  `ResetSinceEventID` / `CurrentEventID`.
- The delivery queue never blocks: a full queue drops with a synthetic rescan
  flag for the affected paths instead of stalling the stream (which only makes
  overflow more likely).
- A dropped-flag event is healed by replaying the persisted event journal from
  the last delivered event ID rather than being forwarded as a full rescan;
  mid-replay holes (flags raised during the heal itself) are tracked and
  healed by one follow-up replay at HistoryDone.

**syncthing** (this PR):

- `lib/fs`: `FilesystemTypeSupportsReplay` / `WatchReplayArgs`; the fsevents
  watcher attempts `ReplayFrom` when the notify backend reports overflow.
- `lib/model`: per-folder watch-replay bookkeeping in folder meta; only the
  flagged subtrees get targeted scans; if replay itself overflows, escalate
  to the usual full scan. Drain the watch queue on shutdown before tear-down.

If replay is unsupported (non-macOS backends, or the event watermark was too
far back and fseventsd pruned the journal), behavior falls back to today's
full scan.

## Numbers / evidence

- Before: every overflow flag = full scan of 2.4M files (~10 min wall time on
  this machine).
- After: an overflow containing a burst of N changed paths replays those N
  paths and scans only the touched subtrees — observed: 16 changed paths,
  initial scan skipped entirely.
- Watch-delivery stalls: fseventsd CPU observed at 42.8% during busy periods
  before the never-block fix; gone after.

## Tests

- `lib/fs/fsevents_backend_test.go`: replay recovery on overflow, fallback
  when replay unsupported, queue-drain on shutdown.
- `lib/model/folder_watchevents_test.go` + `folder_watchevents_replay_test.go`:
  replay bookkeeping, escalation, meta persistence.

## Re-arming replay after a completed full scan

Previously replay could arm only at boot (tryArmWatchReplay), so a watermark
aged past maxWatchReplayAge — e.g. after an overnight gap — disarmed replay
for the whole process lifetime, and every fseventsd drop flag escalated to a
full rescan. Measured live 2026-10-13 on a busy 2.4M-file home tree: ~22
drop flags/min against ~20 min full scans, so every scan contained hundreds
of drops and the next rescan was justified the moment one finished — an
endless scan loop (5566 folder state transitions in 2h47m, zero replay
engagements). The new commit arms replay at any completed full scan (the
database is then complete as of the journal's current position, which is
all arming requires); drop flags after that heal by replaying the journal
gap instead of walking the tree. TestWatchReplayArmsAfterFullScan fails
when the arming call is removed.
