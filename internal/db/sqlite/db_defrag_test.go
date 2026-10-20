// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShouldDefrag(t *testing.T) {
	const (
		size    = 1 << 30 // 1 GiB database
		total   = size + 64<<20
		availOK = 2 << 30
	)
	// Fragmented baseline: 10k extents on 1 GiB = ~100 KiB average.
	fragmented := int64(10000)
	clean := int64(8) // ~128 MiB extents, as a fresh sequential write lands

	cases := []struct {
		name          string
		size, extents int64
		total, avail  int64
		backlogKiB    int64
		want          bool
	}{
		{"fragmented idle enough space", size, fragmented, total, availOK, 0, true},
		{"small file never", defragMinSizeBytes - 1, 1 << 20, total, availOK, 0, false},
		{"clean file never", size, clean, total, availOK, 0, false},
		{"backlog blocks", size, fragmented, total, availOK, defragMaxBacklogKiB + 1, false},
		{"backlog at limit fires", size, fragmented, total, availOK, defragMaxBacklogKiB, true},
		{"tight space blocks", size, fragmented, total, total + total/10 - 1, 0, false},
		{"exact headroom fires", size, fragmented, total, total + total/10, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldDefrag(tc.size, tc.extents, tc.total, tc.avail, tc.backlogKiB); got != tc.want {
				t.Errorf("shouldDefrag = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMeminfoDirtyPlusWritebackKiB(t *testing.T) {
	meminfo := `MemTotal:       32768104 kB
MemFree:         1954456 kB
Dirty:             12345 kB
Writeback:          6789 kB
Buffers:           41996 kB
`
	if got, want := meminfoDirtyPlusWritebackKiB([]byte(meminfo)), int64(12345+6789); got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if got := meminfoDirtyPlusWritebackKiB([]byte("MemTotal: 1 kB\n")); got != 0 {
		t.Errorf("missing fields: got %d, want 0", got)
	}
}

// patch/43: the startup recopy runs only when the maintenance scheduler
// armed it via the pending flag (or STDBDEFRAG=1 forces it manually).
// The flag is consumed by the attempt; STDBDEFRAG=0 leaves it in place.
func TestDefragPendingFlagLifecycle(t *testing.T) {
	db := filepath.Join(t.TempDir(), "folder.test.db")
	if err := os.WriteFile(db, []byte("not a real db"), 0o600); err != nil {
		t.Fatal(err)
	}
	flag := db + DefragPendingSuffix

	// No flag, default env: gate returns without touching anything.
	maybeDefragDB(db)
	if DefragPending(db) {
		t.Error("flag appeared out of nowhere")
	}

	// Armed by the scheduler: the attempt runs and consumes the flag.
	// (On this platform the NOCOW probe declines, so no copy happens —
	// what we verify is authorization and consumption.)
	if err := os.WriteFile(flag, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	maybeDefragDB(db)
	if DefragPending(db) {
		t.Error("flag not consumed by the attempt")
	}

	// STDBDEFRAG=0 opts out entirely and leaves the flag for later.
	t.Setenv("STDBDEFRAG", "0")
	if err := os.WriteFile(flag, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	maybeDefragDB(db)
	if !DefragPending(db) {
		t.Error("STDBDEFRAG=0 consumed the flag; it must be left in place")
	}
	os.Remove(flag)
}
