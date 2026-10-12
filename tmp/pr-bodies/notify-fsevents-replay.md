# PR: notify: fsevents journal replay + never-block delivery

Fork: https://github.com/jhf/notify branch fsevents-replay
PR target: syncthing/notify

## Symptom

1. When fseventsd reports a dropped event (kFSEventStreamEventFlagUserDropped
   / MustScanSubDirs), the only option the watcher exposes is forwarding the
   flag to the consumer, whose only recourse is a full rescan of the tree.
2. When the consumer is briefly busy, the delivery goroutine blocks on a full
   channel — stalling the FSEvents stream itself and making further drops
   more likely (observed: fseventsd at 42.8% CPU during busy periods).

## Fix

- The watcher can resume delivery from a specific event ID:
  `SetSinceEventID(id)` / `ResetSinceEventID()` / `CurrentEventID()`. Uses
  the documented FSEventStreamCreate sinceWhen parameter; only the
  kFSEventStreamEventIdSinceNow special case is disallowed.
- The delivery loop never blocks: a full queue drops the event with a
  synthetic `rescan` flag for its path instead.
- On a dropped-flag event, the watcher replays the persisted journal from the
  last delivered event ID and suppresses the rescan flag if replay covers the
  gap. Holes raised *during* replay are tracked and healed by one follow-up
  replay at HistoryDone; a lossy threshold keeps degenerate cases bounded.

Consumers that don't use replay are unaffected (flags forward as before).
Consumers that do (syncthing, via WatchReplayArgs) turn a minutes-long full
rescan into a targeted replay — observed on a 2.4M-file folder: 16 changed
paths replayed, full scan skipped.

## Note

One commit (`NOTIFY_DEBUG_HEAL` debug logging) is marked "instrumentation,
revert before upstreaming" in its message — it is opt-in via an env var and
harmless, but happy to drop it from the PR if you prefer.
