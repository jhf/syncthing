// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"os"
	"strconv"
)

// SQLite's default page cache is 2000 pages (~8 MiB) per connection. For a
// multi-GB folder database on slow storage that is nothing: every random
// probe (the need-list scan joins fileinfos, blocklists and file_names per
// row, plus a GetDeviceFile point lookup per row) goes to disk, and the
// post-restart pull scan on a spinning disk runs at hundreds of rows per
// minute (observed on yarlung 2026-10-19: ~400 rows/min, 1% CPU, all I/O
// wait, against an 8.8 GiB folder DB).
//
// We therefore set an explicit, larger cache size on every connection of
// the pool (via the DSN, so it applies to connections opened later, not
// just the first). The cache grows on demand up to the limit, so a generous
// limit costs nothing on small databases.
//
// STDBPAGECACHEMIB overrides the total budget in MiB across the connection
// pool; it is divided by maxOpenConns to get the per-connection limit,
// never below the floor below.
const (
	defaultPageCacheMiB    = 256
	minPerConnPageCacheKiB = 8192
)

func pageCachePerConnKiB(maxOpenConns int) int {
	totalMiB := defaultPageCacheMiB
	if s := os.Getenv("STDBPAGECACHEMIB"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			totalMiB = v
		}
	}
	perConnKiB := totalMiB * 1024 / max(1, maxOpenConns)
	return max(perConnKiB, minPerConnPageCacheKiB)
}
