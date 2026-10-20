// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"path/filepath"
	"testing"
)

func TestWalAutocheckpointPages(t *testing.T) {
	// Non-CoW path (t.TempDir is tmpfs/APFS on test machines): the CoW
	// default applies.
	nonCoWPath := filepath.Join(t.TempDir(), "test.db")

	t.Setenv("STDBWALAUTOCHECKPOINTMIB", "")
	if got := walAutocheckpointPagesFor(nonCoWPath); got != defaultWalAutocheckpointMiB*pagesPerMiBAt4KiB {
		t.Errorf("default: got %d pages, want %d pages", got, defaultWalAutocheckpointMiB*pagesPerMiBAt4KiB)
	}
	t.Setenv("STDBWALAUTOCHECKPOINTMIB", "256")
	if got := walAutocheckpointPagesFor(nonCoWPath); got != 256*pagesPerMiBAt4KiB {
		t.Errorf("override: got %d pages, want %d pages", got, 256*pagesPerMiBAt4KiB)
	}
	t.Setenv("STDBWALAUTOCHECKPOINTMIB", "notanumber")
	if got := walAutocheckpointPagesFor(nonCoWPath); got != defaultWalAutocheckpointMiB*pagesPerMiBAt4KiB {
		t.Errorf("invalid override ignored: got %d pages", got)
	}
}

func TestWalAutocheckpointApplied(t *testing.T) {
	t.Setenv("STDBWALAUTOCHECKPOINTMIB", "")
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var pages int
	if err := db.sql.QueryRow("PRAGMA wal_autocheckpoint").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if want := defaultWalAutocheckpointMiB * pagesPerMiBAt4KiB; pages != want {
		t.Errorf("PRAGMA wal_autocheckpoint = %d, want %d", pages, want)
	}
}
