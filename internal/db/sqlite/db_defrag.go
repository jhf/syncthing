// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// A database file can end up badly fragmented on btrfs even with NOCOW
// active: the NOCOW migration (db_nocow_migrate.go) copies through the page
// cache, and when the copy runs while the kernel is flushing a large dirty
// backlog, writeback allocates in small interleaved pieces — observed on
// yarlung 2026-10-20: the freshly migrated 9.4 GiB folder database landed in
// 210,094 extents (~45 KiB average) versus 2 extents for a 1 GiB sequential
// write on the idle filesystem. Writes are unaffected (NOCOW overwrites in
// place, no allocation), but sequential whole-file reads — the startup
// cache-warm, full-table scans — become seek-bound on spindles.
//
// maybeDefragDB therefore recopies a fragmented database at startup, before
// the database is opened, reusing the migration's byte-identical
// copy-temp-fsync-rename machinery (every crash-interrupted mix is
// consistent; the WAL is swapped as a set). The gate is a pure function of
// observable state, evaluated once at startup:
//
//   - the database runs NOCOW (CoW filesystem, flag effective) — otherwise
//     the fragmentation question is either moot or the migration's job
//   - the file is large enough for fragmentation to matter (>= 256 MiB)
//   - average extent size below 1 MiB (a clean sequential write lands in
//     ~128 MiB extents; two orders of magnitude below that is pathological)
//   - the writeback backlog (Dirty+Writeback in /proc/meminfo) is below
//     64 MiB — the condition that fragmented the original copy, so recopying
//     under it would just re-fragment
//   - free space holds the set plus 10% headroom
//
// Self-terminating: a successful recopy lands in tens of extents and the
// gate never fires again. STDBDEFRAG=0 opts out.
func maybeDefragDB(path string) {
	if os.Getenv("STDBDEFRAG") == "0" {
		return
	}
	fi, err := os.Stat(path)
	if err != nil {
		return // no database yet
	}
	if !dbRunsNoCOW(path) {
		return // not on CoW with the flag effective — moot or the migration's job
	}
	extents, err := extentCount(path)
	if err != nil {
		slog.Debug("Database defrag check failed", slogutil.FilePath(path), slogutil.Error(err))
		return
	}

	// Swap the WAL as a set with the database, exactly as the NOCOW
	// migration does: byte-identical copies make every crash-interrupted
	// mix consistent, and WAL frame checksums validate or discard the WAL
	// independently of which copy survives.
	paths := []string{path}
	total := fi.Size()
	if wi, err := os.Stat(path + "-wal"); err == nil && wi.Size() > 0 {
		paths = append(paths, path+"-wal")
		total += wi.Size()
	}

	avail := freeSpaceBytes(filepath.Dir(path))
	backlog := writebackBacklogKiB()
	if !shouldDefrag(fi.Size(), extents, total, avail, backlog) {
		return
	}

	slog.Info("Defragmenting database (one-time sequential recopy)",
		slogutil.FilePath(path),
		slog.Int64("bytes", total),
		slog.Int64("extentsBefore", extents))
	start := time.Now()
	if err := copyReplaceAll(paths); err != nil {
		slog.Warn("Database defrag failed; continuing with fragmented database", slogutil.FilePath(path), slogutil.Error(err))
		return
	}
	after, err := extentCount(path)
	if err != nil {
		after = -1
	}
	slog.Info("Database defrag complete",
		slogutil.FilePath(path),
		slog.Int64("bytes", total),
		slog.Int64("extentsBefore", extents),
		slog.Int64("extentsAfter", after),
		slog.Duration("duration", time.Since(start)))
}

const (
	// defragMinSizeBytes: fragmentation is irrelevant below this size.
	defragMinSizeBytes = 256 << 20
	// defragAvgExtentFloorBytes: fragmented means average extent smaller
	// than this (clean sequential allocation is ~128 MiB extents).
	defragAvgExtentFloorBytes = 1 << 20
	// defragMaxBacklogKiB: only recopy when the kernel writeback backlog
	// (Dirty+Writeback) is below this — the congestion that fragmented the
	// original copy would fragment the recopy the same way.
	defragMaxBacklogKiB = 64 * 1024
)

// shouldDefrag is the pure decision, separated for tests.
func shouldDefrag(size, extents, total, avail, backlogKiB int64) bool {
	if size < defragMinSizeBytes {
		return false
	}
	if extents <= size/defragAvgExtentFloorBytes {
		return false // not fragmented
	}
	if backlogKiB > defragMaxBacklogKiB {
		return false
	}
	return avail >= total+total/10
}

// meminfoDirtyPlusWritebackKiB parses Dirty+Writeback from /proc/meminfo
// content. Missing fields count as zero.
func meminfoDirtyPlusWritebackKiB(data []byte) int64 {
	var total int64
	for _, line := range bytes.Split(data, []byte("\n")) {
		var name string
		var val int64
		if n, _ := fmt.Sscanf(string(line), "%s %d", &name, &val); n != 2 {
			continue
		}
		if name == "Dirty:" || name == "Writeback:" {
			total += val
		}
	}
	return total
}
