// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// patch/36 applies FS_NOCOW_FL automatically, but btrfs silently ignores it
// on files that already have extents — a pre-existing database stays CoW,
// and every WAL checkpoint then rewrites random pages as freshly allocated
// extents (allocate + free + metadata update per page, doubled by RAID1).
// On yarlung's 9.3 GiB folder DB that made checkpoints the dominant write
// burst (measured 2026-10-20: ~0.8 GiB/min device writes during syncing).
//
// The only way to get NOCOW on such a file is to recreate it: copy the
// bytes to a new file (which inherits NOCOW from the directory flag that
// patch/36 already applied) and atomically rename it over the original.
// That is what migrateToNoCOW does, once, at startup before the database is
// opened. The copy is sequential I/O at device speed — minutes on a
// spinner, seconds on SSD — versus the permanent checkpoint amplification
// it removes.
//
// Safety: copies go to sibling temp files followed by fsync and atomic
// renames. Because each copy is byte-identical to its source, a crash at
// ANY point leaves a consistent set: every mix of {old, new} files is
// content-identical, and SQLite's WAL frame checksums validate or discard
// the WAL regardless of which copy of the pair survives. A stale temp from
// an interrupted attempt is removed on the next start. Skipped entirely
// with STDBNOCOW=0 or when free space is tight.

// migrateToNoCOW replaces the database file at path with a NOCOW copy when
// it lives on a CoW filesystem without the NOCOW flag. Best effort: all
// failures are logged and opening proceeds with the original file.
func migrateToNoCOW(path string) {
	if os.Getenv("STDBNOCOW") == "0" {
		return
	}
	fi, err := os.Stat(path)
	if err != nil {
		return // no database yet; it will be created with the directory flag
	}
	needed, err := lacksNoCOW(path)
	if err != nil {
		slog.Debug("NOCOW migration check failed", slogutil.FilePath(path), slogutil.Error(err))
		return
	}
	if !needed {
		return
	}

	// The WAL survives even a graceful shutdown (the connection pool does
	// not checkpoint-remove it at close), so the database and WAL migrate
	// as a set: gating on an empty WAL would defer forever.
	paths := []string{path}
	total := fi.Size()
	if wi, err := os.Stat(path + "-wal"); err == nil && wi.Size() > 0 {
		paths = append(paths, path+"-wal")
		total += wi.Size()
	}
	avail := freeSpaceBytes(filepath.Dir(path))
	if !shouldMigrateNoCOW(total, avail) {
		slog.Warn("NOCOW migration skipped: insufficient free space", slogutil.FilePath(path), slog.Int64("dbBytes", total), slog.Int64("availBytes", avail))
		return
	}

	slog.Info("Migrating database to NOCOW (one-time sequential copy)", slogutil.FilePath(path), slog.Int64("bytes", total), slog.Int("files", len(paths)))
	start := time.Now()
	if err := copyReplaceAll(paths); err != nil {
		slog.Warn("NOCOW migration failed; continuing with CoW database", slogutil.FilePath(path), slogutil.Error(err))
		return
	}
	slog.Info("NOCOW migration complete",
		slogutil.FilePath(path),
		slog.Int64("bytes", total),
		slog.Duration("duration", time.Since(start)))
}

// shouldMigrateNoCOW is the pure decision, separated for tests: migrate
// only when there is something to copy and the filesystem holds the set
// plus headroom.
func shouldMigrateNoCOW(size, avail int64) bool {
	if size == 0 {
		return false
	}
	return avail >= size+size/10
}

// copyReplaceAll copies each path to a sibling temp, fsyncs each, then
// atomically renames all of them and fsyncs the directory. Two-phase so
// that the window for a mixed crash state contains only renames of
// byte-identical content, which is harmless.
func copyReplaceAll(paths []string) error {
	temps := make([]string, 0, len(paths))
	cleanup := func() {
		for _, t := range temps {
			os.Remove(t) //nolint:errcheck
		}
	}
	for _, path := range paths {
		tmp, err := copyToTemp(path)
		if err != nil {
			cleanup()
			return err
		}
		temps = append(temps, tmp)
	}
	for i, path := range paths {
		if err := os.Rename(temps[i], path); err != nil {
			cleanup()
			return fmt.Errorf("rename: %w", err)
		}
	}
	if dir, err := os.Open(filepath.Dir(paths[0])); err == nil {
		dir.Sync()  //nolint:errcheck
		dir.Close() //nolint:errcheck
	}
	return nil
}

// readerOnly hides an *os.File behind io.Reader so io.CopyBuffer cannot
// take the zero-copy paths: with two plain files Go resolves the copy to
// copy_file_range(2), which btrfs implements as an extent-SHARING clone —
// instant, but the "copy" keeps the source's physical extents, defeating
// both this recreation (the new file must own fresh NOCOW extents) and the
// defrag recopy (the point is replacing a fragmented layout). Observed
// 2026-10-20 on yarlung (kernel 7.0): the 9.5 GiB defrag "copied" in 2.5 s
// with the extent count unchanged at 235,240.
type readerOnly struct{ io.Reader }

// copyToTemp copies path to a fresh sibling temp file and fsyncs it. The
// temp inherits the directory's NOCOW flag on CoW filesystems.
func copyToTemp(path string) (string, error) {
	tmp := path + ".nocow-migrate"
	os.Remove(tmp) // residue from an interrupted earlier attempt

	src, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer src.Close() //nolint:errcheck
	si, err := src.Stat()
	if err != nil {
		return "", err
	}

	dst, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, si.Mode())
	if err != nil {
		return "", err
	}
	buf := make([]byte, 4<<20)
	if _, err := io.CopyBuffer(dst, readerOnly{src}, buf); err != nil {
		dst.Close()    //nolint:errcheck
		os.Remove(tmp) //nolint:errcheck
		return "", fmt.Errorf("copy: %w", err)
	}
	if err := dst.Sync(); err != nil {
		dst.Close()    //nolint:errcheck
		os.Remove(tmp) //nolint:errcheck
		return "", fmt.Errorf("sync: %w", err)
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmp) //nolint:errcheck
		return "", err
	}
	if err := os.Chmod(tmp, si.Mode()); err != nil {
		os.Remove(tmp) //nolint:errcheck
		return "", err
	}
	return tmp, nil
}
