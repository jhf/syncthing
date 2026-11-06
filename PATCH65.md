# patch/65: queued, device-scaled need build ranges

## Change and bound

Each range transaction now takes the folder's updateLock before BeginTxx,
keeps it through commit/rollback, and releases it before the next range.
It rechecks the build generation after acquiring the mutex, so a reset
winning the mutex cannot be followed by an old-generation range write.

The old range width was 100000 names regardless of device fanout. The new
name-device work budget is 256: width=max(1,256/snapshotted-device-count).
Device count and the maximum covered device ID are read together, rather
than dividing by an ID that can be sparse after UPSERTs. The ID high-water
mark still restricts which devices this builder covers. The fixture's nine
devices including local yield 28 names/range, at most 224 remote detail rows
per range. If there are more than 256 devices, one name is the minimum
quantum and can exceed the nominal pair budget. Two INSERT SELECTs share
one transaction. A name's elected global is unique, so both branches cannot
contribute the same name's valid and deleted state in one snapshot.

Debug per-chunk telemetry records lo, hi, elapsed lock-held time, device
count and high-water mark. It is not INF because multi-million-name builds
have many thousands of chunks. Work limits are not hard wall-clock bounds:
slow storage/fsync and the unchanged final pass can still take real time.
Small ranges add commits and can lengthen total build time. Sparse name
index ranges can incur empty transactions. No timeout repeatedly rolls back
an expensive range, which would risk permanent lack of progress.

## Correctness with concurrent Updates

The existing protocol is retained, not replaced by range locking:

1. ensureNeedBuild publishes running before the initial snapshots. Updates
   journal every touched name while running is true.
2. Each range reads one transactional snapshot of globals and covered
   devices. Mutex serialization ensures no Update/reset changes it during
   this transaction. Updates between ranges still journal their names.
3. An Update affecting an already visited name is repaired by the final
   journal drain. One affecting a future range may be represented by that
   range and is harmlessly re-derived again in the drain. New names beyond
   the original name high-water mark are included by that same journal.
4. The final pass still acquires updateLock before its transaction and holds
   it through aggregate rebuild, journal re-derivation/deletion, covered
   device marks, completion flag commit, and publishing needBuilt=true.
   No Update can slip between drain and flag publication. Devices created
   beyond the covered high-water mark remain unknown and fall back until
   their first post-build Update backfills them.
5. A generation reset clears tables and changes gen under the same mutex.
   The inner generation recheck prevents any further stale range. The final
   pass's existing generation recheck prevents stale flag publication.

Per-chunk locking therefore strengthens isolation without weakening the
journal's correctness argument or holding a transaction while waiting for
the final mutex acquisition.

## Regression and measurement

TestNeedBuildSingleWriterChunks uses the real driver observer introduced in
patch/64. Setup goes through real Updates: 20000 global names, eight remote
devices, 160000 potential detail rows. An actual concurrent Update changes
an already covered name, a future name and a new name beyond the snapshot.
The test requires it to finish before a later range, requires zero unlocked
write entry points, then compares all eight maintained counts/sizes with
the existing full-scan oracle and checks that the journal is empty.

The observer measures complete transaction lifetimes and distinguishes
range transactions from the final pass and Update. It does not include
waiting for the Go mutex or the 5 ms test-only fair-handoff pause. The
production chunk timer does include that artificial pause in this test.

Pre65 production (7925128aa), with exactly the new test and observer:

```text
    folderdb_need_singlewriter_test.go:123: no Update completed between range chunks
    folderdb_need_singlewriter_test.go:129: unlocked writes=3, first=BEGIN IMMEDIATE
    folderdb_need_singlewriter_test.go:135: oversized range chunks: 2 statements
    folderdb_need_singlewriter_test.go:163: need fixtureNames=20000 remoteDevices=8 rangeStatements=2 commits=3 maxTransaction=388.927666ms maxRangeTransaction=388.927666ms maxRangeSQL=367.602167ms updateInterleaved=false
--- FAIL: TestNeedBuildSingleWriterChunks (2.61s)
FAIL
FAIL	github.com/syncthing/syncthing/internal/db/sqlite	3.154s
FAIL
```

After:

```text
    folderdb_need_singlewriter_test.go:163: need fixtureNames=20000 remoteDevices=8 rangeStatements=1430 commits=717 maxTransaction=98.027459ms maxRangeTransaction=43.872417ms maxRangeSQL=43.167375ms updateInterleaved=true
--- PASS: TestNeedBuildSingleWriterChunks (3.48s)
PASS
ok  	github.com/syncthing/syncthing/internal/db/sqlite	4.100s
```

This is local APFS/NVMe evidence, not a yarlung latency prediction. The
initial test harness held its statistics mutex while invoking oracle SQL,
which called the observer recursively and deadlocked. The test-only mutex
was fixed before the recorded before/after runs and all gates. No production
deadlock fix is claimed from that harness error.

## Required gates (verbatim)

`go build ./...` and `go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1`:

```text
go build ./... exit=0
ok  	github.com/syncthing/syncthing/lib/model	76.081s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	11.193s
ok  	github.com/syncthing/syncthing/internal/slogutil	1.541s
```

`go test ./internal/db/sqlite -run '^(TestTidySingleWriter|TestNeedBuildSingleWriterChunks)$' -race -count=20 -timeout=10m`:

```text
ok  	github.com/syncthing/syncthing/internal/db/sqlite	234.840s
```

Logs: worktree `tmp/overnight-2026-11-06/65-{before,after,build,gates,race}.log`.
No live Syncthing, database or remote host was touched.
