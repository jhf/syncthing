# sync-preparing linger + long "Scanning" states — root cause and fix

2026-10-09/10, Mac serve (home folder, 2.39M files).

## User observation

Folder rarely shows "Up to Date": long "Scanning" stretches, and a
"Preparing to Sync" label that sat for 15+ minutes with needFiles=0.

## Root cause 1: the "Preparing to Sync" linger (fixed, 46c1226b2)

A pull serviced from the deleted-files sweep's budget branch
(serviceQueuedPull, added in eba4e939d) moves the folder state to
sync-preparing/sync-waiting/syncing. The walk chunk loop re-sets
FolderScanning at the top of every chunk iteration, but the sweep loop has
no per-item restore, so the label stayed at sync-preparing for the rest of
the sweep — 862s observed (23:23:48→23:38:10), ending exactly when the next
queued pull was serviced at a budget tick. Nothing was blocked: pulls worked
(probe file pulled in 15s while the label was stuck), completion was 100,
the sweep was running the whole time. Pure state-label leak.

Evidence trail:
- events: `idle -> sync-preparing` at 23:23:48, no transition for 862s,
  then sync-preparing→sync-waiting→idle→sync-preparing within one second
  (the folder.pull() entry sequence: CountNeed gate passed, sync-waiting;
  health check setError(nil), idle; puller.pull, sync-preparing).
- live `sample` stack: serve goroutine inside
  scanSubdirsDeletedAndIgnored → per-item serviceQueuedWatch.
- code: sweep budget branch at folder.go (scanSubdirsDeletedAndIgnored)
  lacked the restore the walk loop gets from its per-chunk setState.

Fix: `f.setState(FolderScanning)` right after the servicing + I/O-token
retake in the sweep's budget branch. Test:
TestSweepRestoresScanningStateAfterPull (lib/model/folder_scanstate_test.go)
drives the sweep directly with a queued pull behind a state-dirtying fake
puller; teeth-verified — without the restore it fails with
`state after sweep = sync-preparing, want scanning`.

## Root cause 2: what makes "Scanning" last minutes

Watch batches whose paths include `.` (an FSEvents event for the folder
root itself, emitted when a direct child of /Users/jhf is created/deleted —
my socktest.py and .sync-preparing-probe.txt at the root did exactly this)
make the aggregator escalate to a full-folder scan ("Scan entire folder",
aggregator.go). On this tree that is a ~15-20 min chunked walk+sweep. The
FSEvents journal watermark only advances when that scan completes, so
killing the serve mid-scan re-delivers the `.` batch at the next boot and
the full scan starts over (seen at 23:53:47 and 00:13:50/00:14:04 after
two restarts). This is correct behavior (coverage over speed), just
expensive; the fix is to not put churn at the folder root.

Amplifier found and removed: 13 orphaned watch-dev.sh loops (accumulated
over days of serve restarts, each run-custom-syncthing.sh launch added one,
none exited) each writing tmp/dev-watch.log *inside the synced tree* every
second — a constant watch-event stream keeping batches coming every ~10s.
All killed; one instance remains, writing to /tmp (outside the tree). The
watchdog tmp/catch-syncpreparing.sh also logs to /tmp.

seid-lang worktree churn (swarm sessions) produces legitimately large
watch scans (e.g. 400-640s) — that is real work, not a bug.

## Instrumentation (temporary, revert before upstreaming)

Commit 8d0cefd30 adds DBG lines: scanSubdirs entry logs
`nilSubs/subs/sample` (first 3 + last 2 paths), sweep exit logs duration.
Running on the Mac with STTRACE=model → /tmp/custom-serve-sttrace.log.

## Overnight setup (running now)

- serve: dev.43.g46c1226b (fix + instrumentation), STTRACE=model, log
  /tmp/custom-serve-sttrace.log.
- /tmp/dev-watch.log: state timeline every 2s (single instance).
- tmp/catch-syncpreparing.sh: if sync-preparing persists >3min, SIGQUITs
  the worker (goroutine dump into the serve log), saves a copy, restarts
  the serve from CURRENT-BINARY with STTRACE=model.
- yarlung also updated to dev.43.g46c1226b (build-remote HEAD install).

## Update 00:40: the "." full scans form a self-sustaining loop during churn

Four consecutive ["."] full scans observed (00:14, 00:23:43, 00:31:35, each
completing cleanly, sweep changes=0, watermark advancing), each followed
instantly by another. Root mtime of /Users/jhf unchanged since the probe
delete at 23:38:54, so no new real root-level mutations. The mechanism is
almost certainly backend-channel overflow:

- basicfs_watch.go: when len(backendChan) == backendBuffer (500), the watch
  loop drains it and emits a synthetic Event{Name: "."} ("Event overflow,
  send ."), which the aggregator turns into a full-folder scan
  (aggregateEvent: event.Name == "." -> scan entire folder).
- During a 8-10 min chunked full scan the serve loop is busy and the
  aggregator's 10s flush blocks on the full watchChan; events back up into
  the 500-entry backend channel; active seid-lang churn (changes=47 seen in
  a scan chunk at 00:34) easily exceeds 500 events in that window.
- So: long scan -> overflow -> full scan -> overflow -> ... while churn
  continues. Behavior is conservative-correct (lost events are healed by
  the rescan), but it means the GUI shows Scanning for as long as churn
  lasts, and CPU/IO stay busy.

No "Watch: Backend dropped events" lines in the STTRACE=model log because
those come from the fs facility (not enabled). Morning analysis should
enable STTRACE=model,fs or correlate scan start times with churn to confirm,
and consider: larger backendBuffer, draining the channel instead of
full-scan on overflow, or faster full scans (budget pacing makes the walk
~10 min; continuous would be far shorter).

## Update 01:10: mechanism CONFIRMED by direct observation (dev.44 INF logging)

The ["."] chain is driven by fseventsd-level drops, not syncthing's
backendChan (which overflowed exactly once). dev.44 logs every
overflow-flagged event at INF:

- 4953+ "Watch: Backend dropped events (flags=notify.FSEventsUserDropped /
  FSEventsMustScanSubDirs path=.)" lines between 01:02:55 and 01:08,
  arriving ~15/s, sustained. Each logged line is one split flag of a flagged
  physical event (notify's Dispatch splits multi-flag events).
- Controlled experiments: create+delete of a root file in one aggregator
  window does NOT escalate (targeted scan of the path); 600-file burst in
  one dir scans just that dir (per-dir aggregation); `touch /Users/jhf`
  (event for the root dir itself) deterministically produces ["."].
- The fork's serial dispatch queue stalls when the aggregator's 10s flush
  blocks on the folder watchChan during long scans; fseventsd's per-stream
  client buffer overflows under a churn burst (my 2000-event test burst),
  and from then on fseventsd delivers flagged events; each batch containing
  a flagged event heals via a full ["."] scan, and the scan itself keeps the
  pipeline slow enough that drops continue. Self-sustaining at the FSEvents
  layer. Original seed tonight: the 13-watcher storm (fseventsd overload).
- Chain break: restart the serve. The watermark had advanced past the
  backlog via completed scans, so replay is light and the fresh stream is
  healthy.
- Real fix candidate (needs user decision): with the FSEvents journal, an
  overflow heal does not need a blind full walk — replaying the journal
  from the last persisted watermark covers the gap exactly. FSEvents
  UserDropped means the CLIENT failed to read; the on-disk journal still
  has the events.

## Update 01:30: restart AMPLIFIES the chain — do not restart to fix it

Restarting the serve at 01:09 made it worse: the boot replay from the last
watermark re-delivered the journaled flagged backlog (83803 INF flag lines
in 3 min, ~460/s), and the replay flood itself stalled the fresh stream's
serial queue, causing NEW fseventsd drops, which get journaled and
re-flagged. The loop is: flagged events in journal -> replay flood -> new
drops -> new flagged events. It converges only by letting scans COMPLETE
 uninterrupted (each completed full scan advances the watermark past the
flagged range; observed: 01:09:34 scan completed 01:24:07, sweep 7m30s).
Residual live flag rate after drain: ~9/s at 01:25-01:29 (was 460/s).

Controlled-trigger results (dev.44):
- create+delete same root file in one 10s window: targeted scan, no ["."].
- 600-file burst in one subdir: targeted scan of that dir, no ["."].
- touch of the folder root dir itself: deterministic ["."] full scan.
- 2000-event burst during a full scan: backendChan overflow fired ONCE;
  the real damage was fseventsd-level drops ~7 min later (01:02:55 flood).

Morning decision for user: on overflow flags, heal by journal replay from
the last watermark (exact, bounded) instead of a blind ["."] full walk.
Also consider coalescing flagged batches while a full scan is already
running (a second ["."] scan queued behind an identical one in progress is
pure waste), and whether flagged events should be journaled/replayed at
all on the syncthing side.
