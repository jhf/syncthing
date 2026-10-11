-- Copyright (C) 2026 The Syncthing Authors.
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this file,
-- You can obtain one at https://mozilla.org/MPL/2.0/.

-- Incrementally maintained "what does device X still need" state. The full
-- answer requires probing every global file against the device's rows, which
-- takes seconds on a folder with millions of files; these tables keep the
-- answer precomputed. need_detail holds one row per (device, name) the device
-- needs, need_totals the aggregate per (device, type, deleted) for the
-- completion count. Both are rebuilt lazily in the background after upgrade
-- (see need_build_journal and the needTrackingBuilt kv flag) and maintained
-- per touched file name by recalcGlobalForFile afterwards.

CREATE TABLE IF NOT EXISTS need_detail (
    device_idx INTEGER NOT NULL,
    name_idx INTEGER NOT NULL,
    type INTEGER NOT NULL,
    size INTEGER NOT NULL,
    deleted INTEGER NOT NULL, -- boolean: the need is to delete the file remotely
    PRIMARY KEY (device_idx, name_idx),
    FOREIGN KEY(device_idx) REFERENCES devices(idx) ON DELETE CASCADE
) STRICT;

CREATE TABLE IF NOT EXISTS need_totals (
    device_idx INTEGER NOT NULL,
    type INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    count INTEGER NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (device_idx, type, deleted),
    FOREIGN KEY(device_idx) REFERENCES devices(idx) ON DELETE CASCADE
) STRICT;

-- Names touched by updates while the initial build is running. Drained by
-- the builder's final pass; stays empty once the tables are live.
CREATE TABLE IF NOT EXISTS need_build_journal (
    name_idx INTEGER PRIMARY KEY
) STRICT;

-- Devices whose need_detail coverage is complete. A device absent here has
-- no usable detail rows (never announced, or created after the build
-- snapshot) and need reads for it fall back to the full-scan queries until
-- its next update backfills it.
CREATE TABLE IF NOT EXISTS need_devices (
    device_idx INTEGER PRIMARY KEY,
    FOREIGN KEY(device_idx) REFERENCES devices(idx) ON DELETE CASCADE
) STRICT;
