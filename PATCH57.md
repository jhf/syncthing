# patch/57: held scanner results crossing pull commits

Test-only investigation on 2026-11-05, base `f3bcaecf618b2b660ccba7ee37f34081a084a46c`.
No production changes or push. This does not explain the 2026-11-03 incident.

## Findings and exact scope

**Neither drip nor metabatch invalidates a held same-name result on a pull DB
commit. Both can overwrite a newer pulled index entry when explicitly supplied
that boundary state.** The tests assert the complete captured version vector,
ModifiedBy, and BlocksHash before and after the pull and flush, not just a local
counter. The stale entry has sender counter 1 plus a receiver counter, receiver
ModifiedBy, and content A's hash. Disk retains sender v3's different content B.

**This is a conditional boundary-state reproduction, not a live Serve race.**
To generate a receiver version without editing an installed receiver file, the
test explicitly runs a real scanner between `performFinish(v2)` and committing
its emitted completion job. Normal scheduling does not provide that scan gap:
scan and pull run in the Serve goroutine (`lib/model/folder.go:829-840`), the
pull iteration waits for DB updates (`lib/model/folder_sendrecv.go:373-376`),
and the post-pull scanner schedules Scan only after pulling
(`folder_sendrecv.go:2345-2360`). The test does not establish that its held-entry
origin is reachable in the normal event loop. In particular, the five-second DB
batch window alone is NOT proof a scan can run inside it.

A control in every case also proves that a new finish is rejected while the
held mtime-only scan differs from the current index. An already-produced v2
completion is deliberately committed to reconcile disk/index before v3 is
installed. Without that explicit boundary setup, the control stops at
`errModified`, not at a successful crossing pull. Thus lack of invalidation is
proven, but a naturally arising, no-receiver-edit stale overwrite remains
unproven. These tests must not be described as an end-to-end production race.

## Code paths

- `lib/model/folder.go:2288-2295`: only scan updates enter drip filtering.
  `lib/model/drip.go:129-150`: patch/51 discards held entries for immediate
  directory/deleted/invalid scan results, or replaces them for regular scan
  results. Pull updates do not call this code.
- `lib/model/folder.go:2303-2323,2342-2355`: only scan commits enter metabatch
  diversion and `drainExcept(fileNameSet(fs), ...)`. Its same-name supersession
  excludes the pending result only for a real scan update in that batch
  (`lib/model/metabatch.go:179-192`). Pull commits do not count.
- `lib/model/folder.go:2399-2413`: pull commits call `updateLocals` directly,
  then emit remote-change events. Neither held map is touched. SQLite's
  `internal/db/sqlite/folderdb_update.go:89-92` uses INSERT OR REPLACE for the
  per-device file entry. No stale-vector rejection saves the tested flush.
- Drip timer paths call `commitLocalsFromScanning`
  (`lib/model/folder.go:783-795`), metabatch timer paths call `commitScanBatch`
  (`folder.go:800-813`), and shutdown drains both (`folder.go:249-258`). These
  commit already-held data without validating it against the newer pull.
- The next finish does NOT create a conflict in this sequence. Although its
  vector is concurrent and its predecessor hash is unseen, `performFinish`
  calls `scanIfItemChanged` before conflict handling
  (`lib/model/folder_sendrecv.go:2144-2160`). Disk B differs in size and mtime
  from held A. `scanIfItemChanged` compares metadata, ignoring blocks, returns
  `errModified`, and queues the path (`folder_sendrecv.go:2608-2636`). The exact
  returned error is `checking existing file: file modified but not rescanned;
  will try again later`. No finish job, conflict copy, disk change, or index
  change results. No later retry/rescan is injected to force a conflict.

## Tests

`lib/model/folder_sendrecv_heldscan_test.go`:

- `TestDripHeldScanAcrossPull`: quiet, max-hold, shutdown.
- `TestMetabatchHeldScanAcrossPull`: window, real unrelated scanner-update
  piggyback, shutdown.

All six cases use the real sendReceiveFolder, scanner `scanSubdirs` body, and
SQLite DB. Initial sender v1 is installed and indexed normally. Sender v2 has
identical bytes and a newer mtime. Its finish installs the file, then the test
scans before committing its completion, generating the actual held FileInfo
from indexed v1. After the safety-control rejection, v2 completes and v3 is
installed/indexed normally. Held maps are checked to survive both commits. The
flush regresses the index, and incoming v4 stops at the safety check. Only pull
temp files and the unrelated piggyback fixture are written by the test, never
installed receiver content or metadata. No timestamp mapping is removed.

Injected hold clocks avoid sleeps. Drip detectN=1 models a hot path, and the
max-hold case uses a supported quiet-greater-than-max-hold override. Shutdown
uses the same two drain/commit stages in the same order as Serve, rather than
restarting the event loop. Tests pin current undesirable boundary behavior and
will need inversion if pull-side invalidation is added.

## Validation

Full local transcripts: ignored `tmp/patch57/targeted.log`, `full-final.log`,
and `race.log`. Final verbatim result lines:

```text
$ go test ./lib/model -run 'Test(Drip|Metabatch)HeldScanAcrossPull' -count=1 -v
--- PASS: TestDripHeldScanAcrossPull (0.06s)
--- PASS: TestMetabatchHeldScanAcrossPull (0.05s)
PASS
ok  	github.com/syncthing/syncthing/lib/model	1.322s

$ go test ./lib/model ./internal/db/sqlite -count=1
ok  	github.com/syncthing/syncthing/lib/model	67.239s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	5.999s

$ go test -race ./lib/model -run 'Test(Drip|Metabatch)HeldScanAcrossPull' -count=20
ok  	github.com/syncthing/syncthing/lib/model	4.954s
```
