# patch/58: failed pull index commit creates receiver-authored sender content

Test-only investigation on 2026-11-05, base `8a969ef3d731f1777097105bf50d5aaa669d2d8e`.
No production changes, fix, push, or installed receiver file edits.

## Findings

### Receiver-only invariant and incident fixtures

The user confirmed on 2026-11-05 at 22:25 UTC that **NOTHING but Syncthing
writes to the synced folder on yarlung**, by design. In particular `.hermes/*`
and `ssb/statbus/*` are written only on the Mac. Thus a receiver-side counter
(device WFF4CNJ) on these sender files is minted by Syncthing itself, never a
legitimate concurrent local edit. This invariant is why the receiver authorship
asserted here is a bug even when the predecessor-hash shortcut avoids a copy.

Both named fixture paths run both predecessor-hash cases (four leaf subtests):

- `.hermes/gateway_state.json`: live 2026-11-05 case, pulled 21:32:55, DB batch
  failed 21:33:51, receiver counter `WFF4CNJ:1793916777` minted 22:12:57 on
  Syncthing's own write, as supplied in the incident report.
- `ssb/jsonb_stats/src/lib.rs`: the 2026-11-03 conflict-copy case.

These paths identify the real cases. Test payloads, device IDs and counters
remain synthetic, and no incident timing or exact-vector replay is claimed.

`TestPullDBBusyRescanCreatesFalseConflict` in
`lib/model/folder_sendrecv_dbbusy_test.go` passes against the current bug with
both `previous_hash_matches_S1=false` and `previous_hash_matches_S1=true`.
Its bad-outcome assertions are explicitly marked for inversion when recovery
is fixed.

All requested goal steps a-e are reached through real model code:

1. Install and index S0 with real `performFinish` and `dbUpdaterRoutine`.
   Install S1 the same way, but return a one-shot SQLITE_BUSY-equivalent error
   at `DB.Update`. Assert the real updater returns that error after exactly one
   boundary call, disk contains S1, and the local index retains S0's vector,
   sender ModifiedBy and content hash. The channel is closed to trigger the
   real final batch flush without waiting for the five-second timer.
2. After the failed updater has returned, run real `scanSubdirs`. Assert the
   indexed S1 content hash, exact unchanged S1 bytes and timestamp, receiver
   counter, and receiver ModifiedBy. No content edit, Chtimes, mapping deletion,
   or fabricated scanner result is used.
3. Supply S3 whose PreviousBlocksHash names unseen S2, write only its pull temp
   file, and run real `performFinish` and `dbUpdaterRoutine`. Assert S3 is
   installed/indexed and exactly one conflict contains S1 bytes.
4. In the separate control, supply S3 with PreviousBlocksHash equal to S1's
   content hash. The same real finish succeeds with no conflict or rescan.
5. Consume the conflict's actual scan request and run `scanSubdirs`. Assert a
   new independent FileInfo: different name, valid nondeleted regular content,
   S1 size/hash, receiver ModifiedBy, a receiver-only vector with no sender
   counter, and no PreviousBlocksHash. The original remains indexed as S3.

**Important distinction from the incident narrative:** the scanner inherits
S0's sender counter and S0's PreviousBlocksHash, not S1's. S0 is the surviving
local index entry. The test asserts this observed result rather than claiming
`[receiver:N, sender:S1-counter]` was produced. The goal of receiver authorship
on byte-identical S1 nevertheless occurs, and both conflict outcomes occur.

## Scope and code references

- `lib/model/folder_sendrecv.go:2144-2160`: real safety check runs before conflict
  handling. Unlike patch/56 and patch/57, this sequence passes that check after
  the completed rescan; nothing bypasses it.
- `lib/model/folder_sendrecv.go:2173-2181`: actual rename, timestamp application,
  and completion job emission precede the index write.
- `lib/model/folder_sendrecv.go:2254-2262,2318-2321`: the real batch callback
  propagates the failed pull update and the real updater returns its error.
- `lib/model/folder.go:2399-2413`: pull updates reach the wrapped DB.Update.
- `lib/scanner/walk.go:789-798`: the real scanner updates the indexed source
  vector and takes the indexed source's BlocksHash as PreviousBlocksHash.
- `lib/protocol/bep_fileinfo.go:190-207`: the matching predecessor content hash
  suppresses conflict despite the receiver counter.

The minimal test-only wrapper embeds the real DB, fails one Update without
writing, and forwards every later Update unchanged. Its error string identifies
SQLITE_BUSY and the updater's returned error is checked with errors.Is. This
models the atomic failed-write boundary, not a real SQLite lock, driver error
classification, lock timing, or partial commits. No production test hook exists.

The actual SQLite index, real scanner body, real finish and real batch updater
are used on a stopped model with drip/metabatch disabled. There are no sleeps
or injected scans inside an unfinished successful pull. Sender versions use
fixed Unix-second counters two minutes apart. Peer transport, sender S2 request
failure (`pull: no such file`), global peer index updates, watcher scheduling,
finisher logging and later producer draining after a mid-stream batch failure
are not exercised. S2 is deliberately never installed. Those omitted transport
steps are not required to reproduce a-e and are not claimed proven. This is a
reproduction of the failure mechanism, not proof of incident causality.

## Validation

Full local logs are retained under ignored `tmp/patch58/`: `targeted.log`,
`full.log`, and `race.log`. Verbatim terminal result lines:

```text
$ go test ./lib/model -run '^TestPullDBBusyRescanCreatesFalseConflict$' -count=1 -v
--- PASS: TestPullDBBusyRescanCreatesFalseConflict (0.18s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/previous_hash_matches_S1=false (0.17s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/previous_hash_matches_S1=true (0.02s)
PASS
ok  	github.com/syncthing/syncthing/lib/model	0.878s

$ go test ./lib/model ./internal/db/sqlite -count=1
ok  	github.com/syncthing/syncthing/lib/model	67.581s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	6.108s

$ go test -race ./lib/model -run '^TestPullDBBusyRescanCreatesFalseConflict$' -count=20
ok  	github.com/syncthing/syncthing/lib/model	2.672s
```

### Follow-up: confirmed invariant and both incident paths

The initial results above are from `7dfc38d11`, before expanding to four leaf
subtests. After adding the user-confirmed invariant and both named fixtures,
all required validation was rerun. Logs: `tmp/patch58/fixtures-targeted.log`,
`fixtures-full.log`, and `fixtures-race.log`. Verbatim results:

```text
$ go test ./lib/model -run '^TestPullDBBusyRescanCreatesFalseConflict$' -count=1 -v
--- PASS: TestPullDBBusyRescanCreatesFalseConflict (0.06s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/.hermes/gateway_state.json/previous_hash_matches_S1=false (0.02s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/.hermes/gateway_state.json/previous_hash_matches_S1=true (0.01s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/ssb/jsonb_stats/src/lib.rs/previous_hash_matches_S1=false (0.02s)
    --- PASS: TestPullDBBusyRescanCreatesFalseConflict/ssb/jsonb_stats/src/lib.rs/previous_hash_matches_S1=true (0.02s)
PASS
ok  	github.com/syncthing/syncthing/lib/model	0.655s

$ go test ./lib/model ./internal/db/sqlite -count=1
ok  	github.com/syncthing/syncthing/lib/model	67.777s
ok  	github.com/syncthing/syncthing/internal/db/sqlite	5.708s

$ go test -race ./lib/model -run '^TestPullDBBusyRescanCreatesFalseConflict$' -count=20
ok  	github.com/syncthing/syncthing/lib/model	3.808s
```
