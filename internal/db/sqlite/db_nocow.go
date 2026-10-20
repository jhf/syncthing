// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// On copy-on-write filesystems every random page write to the database is
// amplified: the old block is copied to a new location and the metadata
// tree updated. For a write-heavy multi-GiB folder database that is a large
// multiplier on an already slow disk (btrfs RAID1 on yarlung, 2026-10).
// chattr +C (FS_NOCOW_FL) disables CoW for new allocations; we apply it
// automatically when the database lives on a CoW filesystem. The directory
// gets the flag too, so the WAL and shared-memory files created later
// inherit it — as does a recreated database file. btrfs silently ignores
// NOCOW on a file that already has extents, so a pre-existing database file
// keeps CoW until it is recreated (stop syncthing, copy the file within the
// flagged directory with --reflink=never, swap, restart); we detect that
// case and leave the flag alone rather than claiming success.
//
// The tradeoff matches the manual chattr +C operators apply today: NOCOW
// implies NODATASUM for new writes, so btrfs data checksums are off for
// those blocks. Set STDBNOCOW=0 to keep CoW (and checksums).

// applyNoCOW sets FS_NOCOW_FL on the database directory and file when they
// live on a copy-on-write filesystem. Best effort: failures are logged at
// debug and never prevent opening the database.
func applyNoCOW(path string) {
	if os.Getenv("STDBNOCOW") == "0" {
		return
	}
	dir := filepath.Dir(path)
	if changed, err := setNoCOW(dir); err != nil {
		slog.Debug("NOCOW not applied to database directory", slogutil.FilePath(dir), slogutil.Error(err))
	} else if changed {
		slog.Info("Database directory on CoW filesystem, NOCOW applied", slogutil.FilePath(dir))
	}
	if changed, err := setNoCOW(path); err != nil {
		slog.Debug("NOCOW not applied to database file", slogutil.FilePath(path), slogutil.Error(err))
	} else if changed {
		slog.Info("Database file on CoW filesystem, NOCOW applied", slogutil.FilePath(path))
	}
}

// isCoWFilesystem reports whether a statfs filesystem type magic is a
// copy-on-write filesystem where NOCOW is meaningful.
func isCoWFilesystem(magic int64) bool {
	return magic == btrfsSuperMagic || magic == bcachefsSuperMagic
}
