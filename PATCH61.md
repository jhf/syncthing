# patch/61: retry locked pull database batches

## Evidence and scope

PATCH58.md records the user-confirmed receiver invariant: only Syncthing writes
inside the synced folder on yarlung. A receiver counter on sender-only content
there is a bug, not evidence of a legitimate competing local editor. Other
peers may have genuine local content and metadata edits and must retain them.

The supplied gateway incident was pulled at 21:32:55 UTC on 2026-11-05, its DB
batch failed at 21:33:51, and the receiver counter was minted at 22:12:57.
The other named fixture is `ssb/jsonb_stats/src/lib.rs` from the 2026-11-03
conflict-copy case. PATCH58.md reproduced the failure mechanism using real
finish, updater, scanner and SQLite paths, but synthetic payloads and vectors.
It does not prove incident causality or exact-vector replay. This patch claims
no post-fix production measurement. No push or deployment is performed.

## Primary fix

The `dbUpdaterRoutine` batch callback now retries `updateLocalsFromPulling`
while `internal/db.IsLockContention` classifies the error as database lock
contention. It uses the existing `indexLockBackoff` schedule, also used by
index updates: 250 ms, 500 ms, 1 s, 2 s, 4 s, 8 s, 16 s. This is a bounded
initial attempt plus seven retries, on top of each driver's busy timeout.
The callback keeps exactly the same FileInfos alive across those attempts.
It never re-runs performFinish, renames the disk file again, or changes a
version vector to make a retry succeed.

Retry is inside the callback because FileInfoBatch permanently latches any
error returned by its callback. Retrying Flush after that would not retry the
write. Pull durability is flushed before entering the retry loop. Cooldown
clearing and ReceivedFile remain after confirmed callback success only.
SQLite's Update is transactional with rollback on error. The existing
updateLocals path has a sequence read after Update, so a lock failure there
can also cause a retry of the same FileInfos. That is an idempotent re-index
of the same version, not a content install or minted local version. Local
index and disk-change events are emitted only after the successful path.

Non-lock errors return immediately, exactly as before. When the retry schedule
is exhausted on a lock error, the callback returns the final original error,
logs `Pull database update lock retries exhausted`, and increments the bounded
churn counter `db_update.lock_retry_exhausted` once per exhausted batch, not
per file or per attempt. It appears in the existing schema-1 churn summaries
and totals. The updater then keeps draining producers without accepting new
batch work, just as on today's terminal failure path. It does not report
ReceivedFile or clear retry holds for that failed batch.

**Limit:** exhaustion, a non-lock error, or process death can still leave disk
ahead of the index. This is not durable recovery. The explicit exhaustion
control keeps the documented stale-index, receiver-authorship and possible
later false-conflict outcome rather than silently blessing it as fixed.

## Why secondary scan adoption is not implemented

Per the scope decision, this patch implements the primary retry only. Neither
current-global adoption nor a durable install-intent mechanism is implemented.

1. A rescan may record a version V instead of minting a local counter only if
   three checkable conditions hold:
   - (a) Disk BlocksHash equals V's BlocksHash, computed at V's block size.
   - (b) Every versioned field matches V under Syncthing's own equivalence with
     blocks included: size, type, permissions unless ignored, ownership and
     xattrs when synced, and mtime within the configured window. A genuine
     local chmod or touch is itself a version. Content hash equality alone
     must not swallow it. The existing comparison is
     `protocol.FileInfo.IsEquivalentOptional` with `IgnoreBlocks` false and
     the folder's comparison policy, not a new content-only definition.
   - (c) V dominates or equals the local index entry, so our own counter never
     goes backwards.
   Under (a)-(c), the recorded state is identical to a successful pull of V.
   Any failed check falls back to today's behaviour: false negatives only.

2. With V equal to current global this is provable, but it would not have fired
   in either observed incident. At rescan time global had already moved past
   the version on disk. The supplied lib.rs evidence has disk S1 from
   23:02:26 UTC and global moving on from 23:09. The gateway evidence has disk
   from 21:31:59, global 22:12:14 at the 22:12:57 rescan. These are the
   incident timestamps supplied in the scope decision, not timings measured
   by this patch's tests or substitutes for PATCH58.md's pull-completion time.

3. The general provable form needs the evidence that the failed batch
   discarded: a **durable install-intent record**, path plus the full FileInfo
   being installed, written after performFinish's conflict decision and before
   rename, and deleted on successful index commit. A rescan runs (a) and (b)
   against that record; (c) holds by construction of the install target after
   the conflict decision, and any implementation must preserve the rule that
   the then-current local counter never regresses. This is a candidate
   **patch/62 pending the user's decision**, not an implementation or an
   authorised additional change in this task.

## Regression and validation protocol

`TestPullDBBusyRescanCreatesFalseConflict` now has transient, exhausted and
non-lock controls, each running both named paths and both predecessor-hash
cases. With a transient lock, the second Update commits sender S1, a real
rescan keeps exactly S1's sender vector with zero receiver counter, and S3
creates no conflict even when its predecessor is unseen S2. With an exhausted
lock, all scheduled attempts fail, the exhaustion counter is one, disk stays
S1 and the index S0, and no success bookkeeping is emitted. After the injected
lock is released, the real scan and next pull follow today's documented
terminal failure behaviour, including the predecessor-hash control. Non-lock
errors make one attempt and do not increment the exhaustion counter.

`TestPullDBBusyExhaustionDrains` fills a real updater batch and sends ten jobs
past its failure. It checks bounded attempts, producer draining, the same
returned error, one exhausted-batch count, no indexed entry, unchanged
ReceivedFile and retained cooldown. The tests shorten the existing backoff
schedule to milliseconds, as the index-handler retry tests already do. They
exercise failed DB.Update boundaries, not physical SQLite lock timing.

After the local commit, run in order:

```sh
go build ./...
go test ./lib/model ./internal/db/sqlite ./internal/slogutil -count=1
go test -race ./lib/model -run '^(TestPullDBBusyRescanCreatesFalseConflict|TestPullDBBusyExhaustionDrains|TestPullRetryFailedCommitRetainsStateAndDrains|TestIndexUpdateRetriesLockContention|TestIndexUpdateGivesUpAfterBackoffExhausted|TestIndexUpdateDoesNotRetryStructuralErrors|TestIsLockContentionMatchesObservedDriverMessages)$' -count=20
```

Full local logs are retained under ignored `tmp/patch61/`. Results are reported
verbatim in the completion report. The preceding patch/60 was separately
committed and its build, the same three package suites and its touched tests
under `-race -count=20` passed before this implementation began.
