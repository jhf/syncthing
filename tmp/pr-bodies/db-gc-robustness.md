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
