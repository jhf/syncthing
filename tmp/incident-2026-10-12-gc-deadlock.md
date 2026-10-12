# Incident 2026-10-12: yarlung "Syncing 99% stuck" — phantom connection root cause

## Verdict

The GC service deadlocked against itself on `folderDB.updateLock` (the
need-tables commit re-acquired the caller-held mutex). Every later
`folderDB.Update` wedged behind it; a peer connection whose dispatcherLoop was
parked in `Update` when the peer went away could never finish `internalClose`
(which waits for `dispatcherLoopStopped`), so `model.Closed` never ran. The
model kept a phantom "primary" connection: frozen counters, no kernel socket,
no close event. Fixed in 50db385b7 (fork), squashed into the need-tables unit
(ea35bd45b, force-pushed).

## Evidence (preserved)

- `tmp/incident-2026-10-12-yarlung-goroutines.txt` — support-bundle goroutine
  dump taken 19:47 UTC while the state was live. Key stacks:
  - `Service.periodic → forEachFolder → garbageCollectOldDeletedLocked →
    Mutex.Lock` — the GC service waiting on the mutex IT holds (the holder
    and a waiter in one goroutine; that is why no separate holder appears).
  - `folder.scanSubdirs → updateLocals → folderDB.Update → Mutex.Lock` —
    folder scan wedged.
  - `dispatcherLoop → handleIndexUpdate → model.IndexUpdate →
    folderDB.Update → Mutex.Lock` — the phantom connection's dispatcher,
    parked since before the peer left.
  - `pingReceiver → internalClose → Once.doSlow → internalClose.func1 →
    chanrecv` — pingReceiver fired ErrTimeout, closed the socket, then
    blocked on `<-c.dispatcherLoopStopped` forever; `model.Closed` (the
    "Lost device connection" event source) is after that wait.
- `tmp/incident-2026-10-12-yarlung-connections.json` — `/rest/system/connections`
  at 19:43 UTC: "primary" = tcp :54779 started 19:01:35, in=878 out=98,868,
  frozen across polls.
- `/proc/127011/net/tcp{,6}` on yarlung at 19:49 UTC: no socket for remote
  port 54779 (0xD5FB) — kernel side gone, model side present.
- Goroutine counts: readerLoop=3, writerLoop=3, pingSender=3, but
  dispatcherLoop=4 and pingReceiver=4 — the phantom's partial teardown
  (the 3 IO goroutines exited on `c.closed`; dispatcher + pingReceiver
  leaked).
- yarlung `~/syncthing-custom/support.zip` — original bundle.

## Timeline (CEST, yarlung log)

- 13:56:13 dev.61 boot; schema 45 background need build starts.
- 13:56:37–50 six YMIQQYL connections killed by `deviceidxlocked: database is
  locked` (separate issue: no busy_timeout; fixed in e6d29b4f3).
- ~19:01 trap restart (STTRACE=model,connections, STPROFILER attempted —
  6060 was stolen by crowdsec; moved to 6061 for next boot).
- ~19:11 first periodic GC with needBuilt=true → self-deadlock, updateLock
  held forever.
- 19:11:36 CC-move from YMIQQYL handled ("Abandoning old index handler"),
  but the connection :54779 whose dispatcher was parked in Update became the
  phantom.
- 19:47 support bundle captured the whole state.
- Sync state showed "100%" but no Updates applied after ~19:11.

## Why the first incident (pre-restart) matches

Same mechanism: "Mac's index backlog grew, yarlung's remoteSequence froze,
phantom with frozen counters and no close event" = updateLock wedged by the
GC deadlock + dispatcher parked in Update. The restart cleared it; it
recurred at the next GC pass.

## Why the Mac survived 6h

Same code, same retention default (10920h; the 2160h override was misspelled
`STDBDELETERETIONINTERVAL` in tmp/run-custom-syncthing.sh and never applied —
fixed). The Mac's first GC pass ran before the background need build flipped
needBuilt=true, taking the non-deadlocking branch. Maintenance interval is
8h from boot (13:44), so the Mac's deadlock ETA was ~21:45 CEST. Deploy
dev.62 before that.

## Fixes

- 50db385b7 — drop the re-lock (teeth: test deadlocks on timeout with the
  re-lock restored).
- e6d29b4f3 — busy_timeout(5000) on both sqlite driver DSNs (modernc
  defaults to 0; mattn has a nonzero default). Covers the 13:56
  connection-kill window.
- Both on the need-tables unit branch; busy_timeout also on db-gc-robustness
  (its chunked GC is the other second-writer source).
