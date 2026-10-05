-- Copyright (C) 2026 The Syncthing Authors.
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this file,
-- You can obtain one at https://mozilla.org/MPL/2.0/.

-- Hashes that a file update, drop, or delete just stopped referring to.
-- Block GC considers this set instead of scanning every live blocklist/block
-- row. A hash may still be live via another file or device; GC always
-- re-checks files before deleting.
CREATE TABLE IF NOT EXISTS gc_retired_blocklists (
    blocklist_hash BLOB NOT NULL PRIMARY KEY
) STRICT, WITHOUT ROWID
;
