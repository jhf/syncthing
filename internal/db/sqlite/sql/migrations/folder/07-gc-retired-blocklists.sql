-- Copyright (C) 2026 The Syncthing Authors.
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this file,
-- You can obtain one at https://mozilla.org/MPL/2.0/.

-- Created empty. Existing databases still need one full block GC after
-- upgrade; later runs use the queue filled by file updates.
CREATE TABLE IF NOT EXISTS gc_retired_blocklists (
    blocklist_hash BLOB NOT NULL PRIMARY KEY
) STRICT, WITHOUT ROWID
;
