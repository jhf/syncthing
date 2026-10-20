// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import "testing"

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
