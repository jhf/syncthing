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

// SQLite checkpoints the WAL every 1000 pages (~4 MiB at 4 KiB pages) by
// default. On a write-heavy multi-GB folder database on slow storage that is
// ruinous: every few seconds the WAL wraps in place while the checkpoint
// flushes a thousand random pages into the main database file, and on a CoW
// filesystem each of those page writes is amplified again (observed on
// yarlung 2026-10-20: 8.5 MB/s of process writes for 0.2 MB/s of pulled
// data, ~40x amplification, WAL pinned at its 8 MiB high-water mark).
//
// We therefore raise the autocheckpoint threshold so checkpoints are rare
// and amortize thousands of commits each. The WAL only grows with actual
// write volume, so a large threshold costs nothing on quiet databases; the
// price is a longer crash-recovery replay, bounded by the threshold.
//
// STDBWALAUTOCHECKPOINTMIB overrides the threshold in MiB of WAL (converted
// to pages assuming the usual 4 KiB page size).
const (
	defaultWalAutocheckpointMiB = 1024
	pagesPerMiBAt4KiB           = 256
)

func walAutocheckpointPages() int {
	totalMiB := defaultWalAutocheckpointMiB
	if s := os.Getenv("STDBWALAUTOCHECKPOINTMIB"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			totalMiB = v
		}
	}
	return totalMiB * pagesPerMiBAt4KiB
}
