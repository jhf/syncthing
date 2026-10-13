-- Copyright (C) 2026 The Syncthing Authors.
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this file,
-- You can obtain one at https://mozilla.org/MPL/2.0/.

-- Retired-blocklist GC deletes blocks by blocklist_hash. Without a
-- blocklist_hash-first index that delete is a full scan of the blocks table
-- per batch; on a multi-million-block folder the scan takes minutes, and the
-- updateLock held across it wedges every file update behind it (observed
-- live on yarlung 2026-10-13 as a ~10 minute stall). The index makes the
-- batched delete index-driven. Bulk block inserts pay one extra B-tree
-- insert; measured acceptable against the alternative of locking out all
-- updates for the duration of every retired-GC run.
CREATE INDEX IF NOT EXISTS blocks_blocklist_hash ON blocks (blocklist_hash);
