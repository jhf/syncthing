// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"testing"
)

func TestPageCachePerConnKiB(t *testing.T) {
	t.Setenv("STDBPAGECACHEMIB", "")
	if got := pageCachePerConnKiB(16); got != defaultPageCacheMiB*1024/16 {
		t.Errorf("default: got %d KiB, want %d KiB", got, defaultPageCacheMiB*1024/16)
	}
	t.Setenv("STDBPAGECACHEMIB", "4096")
	if got := pageCachePerConnKiB(16); got != 4096*1024/16 {
		t.Errorf("override: got %d KiB, want %d KiB", got, 4096*1024/16)
	}
	t.Setenv("STDBPAGECACHEMIB", "notanumber")
	if got := pageCachePerConnKiB(16); got != defaultPageCacheMiB*1024/16 {
		t.Errorf("invalid override ignored: got %d KiB", got)
	}
	// Never below the floor, even with a tiny budget and a huge pool.
	t.Setenv("STDBPAGECACHEMIB", "16")
	if got := pageCachePerConnKiB(1024); got != minPerConnPageCacheKiB {
		t.Errorf("floor: got %d KiB, want %d KiB", got, minPerConnPageCacheKiB)
	}
}

func TestPageCacheApplied(t *testing.T) {
	t.Setenv("STDBPAGECACHEMIB", "1024")
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var cacheSize int
	if err := db.sql.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatal(err)
	}
	// Negative value means KiB. Pool for the main DB is maxOpenConns wide.
	want := -(1024 * 1024 / maxOpenConns)
	if cacheSize != want {
		t.Errorf("PRAGMA cache_size = %d, want %d", cacheSize, want)
	}
}
