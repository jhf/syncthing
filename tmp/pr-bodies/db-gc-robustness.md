# PR: db/sqlite: robust GC — chunked vacuum, no update-time blocklist work

Branch: upstream/db-gc-robustness (4 commits on main)

## Symptom

Two long stalls around the sqlite backend's blocklist garbage collection:

1. GC ran as one big transaction; on a large db it held the write lock long
   enough to stall sync. (Related upstream discussion: #10868.)
2. Blocklist retirement happened inside `Update` — i.e., on the sync hot
   path.

## Fix

- GC runs chunked: one `VACUUM`-worthy pass split into small transactions,
  each removing at most `gcBatchSize` blocks.
- Blocklist retirement moves out of `Update` into the GC hook, so index
  batches never pay for it.
- The GC trigger keeps a strictly-ordered queue (removes can't be reordered
  into a future trigger) and drains the pending-remove backlog in bounded
  chunks.
- `blocklistHashGlobalInsert` parameterized for testing.

## Numbers / evidence

On the live 2.8M-name db, GC no longer appears as a multi-second write-lock
hold; index batches no longer touch the blocklist. Details and queue
invariants discussed in the #10868 comment thread.

## Tests

- `TestGarbageCollectionBlocksInChunks`: 3 blocks removed in chunks of 2.
- `TestBlocklistRetiredInGCNotInUpdate`: 20-file update leaves the blocklist
  untouched until the GC hook runs.
## Companion commit: sqlite busy_timeout

Chunked GC deliberately releases `updateLock` between chunks, which makes the
GC a second writer on the folder database file. On modernc (nocgo) builds
`busy_timeout` defaults to zero, so an index update racing a live GC chunk
failed instantly with SQLITE_BUSY, and the protocol layer treats any handler
error as connection-fatal: observed live on a Linux receiver, six consecutive
connections killed in 13 s with `handling index-update for home: update:
deviceidxlocked: database is locked` while a background writer held the lock
in bursts. The final commit in this PR sets `busy_timeout(5000)` on both
driver DSNs (the mattn cgo driver already has a nonzero default) with
teeth-verified tests: removed, they fail with `database is locked (5)
(SQLITE_BUSY)`.
