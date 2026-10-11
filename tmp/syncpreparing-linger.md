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

(The convergence prediction in this section was WRONG — see the 07:31
update below.)

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

## Update 07:31 (morning): the chain does NOT converge — it is a stable limit cycle

Overnight measurements (dev.44, uninterrupted since 01:09):

- Idle time per hour (dev-watch, 2s samples): 01:00h 10s, 02:00-07:31 ZERO.
  The folder showed Scanning continuously for 6.5 hours.
- ["."] full scans chained all night, one every ~11-13 min (03:42, 03:54,
  04:05, 04:18 ... 07:14, 07:29; sweeps 5-8 min each).
- Flagged-event rate decayed from 59k/10min (01:10, post-restart replay)
  to hundreds/10min, but never reached zero (06:20 block: 5136 — some
  nightly churn burst re-seeded it). As long as ANY flagged events arrive,
  each aggregator batch escalates to ["."] and the next full scan keeps the
  pipeline slow enough that fseventsd drops a few more. Limit cycle.
- The sync-preparing fix HELD: watchdog streak=0 all night, no dumps, no
  lingering sync-preparing. needFiles=0 throughout; pulls serviced.
- yarlung: 0 warnings/errors in 8h, syncthing@jhf + btrfs socket active.

Interpretation: healing every flagged event with a full 2.39M-file walk is
too expensive for the flag rate a busy home dir generates. The flagged
events are almost all redundant: the FSEvents journal on disk contains the
events the client dropped (UserDropped is a client-side read failure), so a
journal replay from the last persisted watermark covers the gap exactly,
and replayed-from-journal flagged events are redundant by construction
(the replay itself is the heal).

## Proposed fixes (for user decision)

A. Fork (notify): drop overflow-flagged events delivered during historical
   replay (their gap is covered by the replay). Breaks the restart
   amplifier. Care: MustScanSubDirs on the FIRST batch of a seeded stream
   means "journal does not cover your seed" — that one must still heal.
B. Syncthing: heal a LIVE overflow flag by re-seeding the fork from the
   last persisted watermark and restarting the stream (exact, seconds),
   falling back to a full walk when replay reports incomplete. Replaces the
   11-minute blind walk per flag batch. Needs watermark plumbing from
   model to fs layer.
C. Fork: absorb bursts so fseventsd never drops — the stall is Dispatch
   blocking on the Go channel while the aggregator's 10s flush waits on a
   busy scan; a large async buffer inside the fork breaks that coupling.
   Root-cause fix for flag GENERATION; A/B fix the expensive RESPONSE.
D. Operational: ignore .pgadmin/sessions in .stignore (per-minute session
   heartbeat file causes a targeted scan + sync every ~60s forever).

Recommended: C+A first (small, fork-local), B as the semantic improvement.
Instrumentation commits to revert before upstreaming: 8d0cefd30, 093f3bf9.

## Update 08:05: proposal C implemented, teeth-tested, deployed (dev.48)

Root cause of flag GENERATION found in the fork: Dispatch (running on the
serial fseventsd delivery queue) sent to the tree channel with a BLOCKING
send; the tree buffer was 128 entries (~280ms at the observed 460 events/s
replay-flood rate). Any consumer stall longer than that stalled the serial
queue, and fseventsd responded by dropping events for the stream and
flagging all later events MustScanSubDirs/UserDropped.

Fix (notify fork commit 79d5886, branch fsevents-replay):
- tree buffer 128 -> 65536 (1 MB, absorbs multi-minute floods).
- Dispatch sends non-blocking; a full channel counts drops and emits ONE
  synthetic MustScanSubDirs for the watch root when there is room - the
  same drop-and-signal contract fseventsd itself uses, never silent loss.
- Teeth test TestDispatchDropsWithSyntheticFlag: with the old blocking
  send it times out (verified by reverting); with the fix it passes and
  the synthetic flag arrives in order. TestWatchDispatchBoundedHistory
  updated to the new contract (channel sized for full delivery).
- Full fork suite passes (29s).

Syncthing side (commit 664b9cacf): backendBuffer 500 -> 65536 to match,
so the model-side channel no longer overflows before the fork's buffer
does. lib/fs tests pass (38s).

Deployed 07:47 as dev.48.g664b9cac. Result after 15 min: flag rate
collapsed from 460/s (07:47 replay flood) to ~2/min and decaying (the
tail of the journaled backlog; no evidence of NEW flag generation).
The backlog drains as a few queued full scans (~10 min each), then the
folder should hold idle. Proposals A (drop replayed flags / coalesce
replay heals to one scan), B (heal-by-replay), D (pgadmin ignore) remain
open for the user.

## Update 08:30: proposal A analyzed — insufficient alone; B is the real fix

A ("drop redundant ["."] scans while one runs/queued") only removes the
nested-scan amplification. While ANY flag trickle exists, every ~10-min
scan window contains flags, so the chain persists at one scan per window
even with coalescing. Breaking the chain requires healing flags without a
full walk (B: journal replay from the last persisted watermark) or the
trickle hitting zero. The trickle is NOT the fork's synthetic backstop
(those would be MustScanSubDirs-only; observed flags are
UserDropped+MustScanSubDirs pairs on ".", i.e. fseventsd coalesced
records - either late-flushing journal history, which drains, or live
drops, which need B). The 90-min verification wake distinguishes: flags
stopped -> drained -> idle holds; flags continue -> implement B.

Also observed: serviceQueuedWatch runs a queued batch INLINE between scan
chunks (watchScanDepth caps nesting at 1). A ["."] batch serviced mid-scan
runs a complete nested full walk inside the outer one; the outer then
resumes and re-walks the same tree. Tonight's 10-15 min scans were
multiple full walks sharing one budget. Any A-style change should skip
nested ["."] scans outright (the outer walk covers the tree; the flagged
gap is backstopped by the watermark not advancing past that batch).

## 2026-10-11 14:20 - A and B deployed (dev.52)

**A (syncthing 965fe8ea4, teeth-tested)**: `fullScanActive` marks a running
full-tree scan (nil subs or `["."]`); `serviceQueuedWatch` drops a
full-folder watch batch while it is set instead of nesting a second full
walk inside the first scan's chunk budget. Targeted batches still service.
The skipped batch's replay watermark stays unpersisted, so a restart still
replays anything the running scan walked past.
`TestServiceQueuedWatchSkipsNestedFullScan` fails without the guard (the
`["."]` batch scans and indexes the probe file).

**B (notify fork 37a442f, teeth-tested)**: on a live `UserDropped` the fork
restarts the FSEvents stream from the last examined event ID and strips
`UserDropped|MustScanSubDirs` from replayed records up to the heal target,
so a flagged batch heals by bounded journal replay instead of a 10-minute
full walk. Boot replay, `KernelDropped`, `EventIdsWrapped`, `RootChanged`,
and heal-restart failure all keep the old full-scan fallback.
`TestDispatchHealsUserDroppedByReplay` fails with the heal branch disabled.

Deployed as dev.52 (fb73048d + guard; fork 37a442f; sha256
11134a3dc5d4c3b56967715c5270445bdff40ce337da32d98313503e2fb12b14), REST
`system/version` confirms. Watch: flag rate (`MustScanSubDirs` in
/tmp/custom-serve-sttrace.log), full-scan cadence (`fullScanCount`),
heal count, idle fraction. If fseventsd keeps flagging under load, the heal
may need a rate limit.

## 2026-10-11 15:42 - B2: mid-replay flags deferred, boot scan skipped, idle

Instrumentation (NOTIFY_DEBUG_HEAL, fork 0f37af6) showed the first heal
design churning: heals engaged (24 in 8 min) but every replay window
contained NEW UserDropped-flagged records (0.2-4% of replayed records;
firehose run proved them delivery-time drops: delivered IDs jump over
holes at flagged records). Forwarded mid-replay flags kept feeding
full-scan signals; making them heal immediately was worse (1757 stream
restarts in 10 min, no convergence).

Fix B2 (fork 75296c8, teeth-tested): mid-replay flagged records are
stripped and collected (earliest hole start, highest flagged ID); ONE
follow-up replay starts at HistoryDone covering all holes. Restart
failure re-forwards a synthetic root rescan so nothing vanishes.
Flagged FIRST record of a replay still forwards (journal may not reach
the seed; only a full scan covers that).

Result on dev.54 (sha 3250962...): "Skipping initial scan, resuming
filesystem watching from the journal" - NO boot full scan at all. The
replay drains the storm backlog (~1.7M events/min) with 0 forwarded
flags, 0 root-scan signals, folder IDLE, targeted scans from replayed
content only.

Known limitation: the persisted watch watermark advances past unhealed
holes; a restart before the follow-up heal completes loses those holes
until the next periodic full scan (1h). Acceptable: holes are rare
outside storm backlogs, and the window closes at HistoryDone.

Open follow-up: fsevents stream latency is 0 + NoDefer in the fork;
raising it may reduce the delivery-time drop rate itself.
