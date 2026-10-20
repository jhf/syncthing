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

// patch/44: move the database directory into its own nested btrfs
// subvolume at startup, before open.
//
// Why: the scan-replay baseline (lib/model/folder_btrfs.go) snapshots the
// synced folder's subvolume at every boot/shutdown, and a btrfs snapshot
// shares every extent of every file in it — including the database, which
// lives inside the synced tree on yarlung (~/.config/syncthing/index-v2
// under /home). btrfs honors NOCOW only on UNSHARED extents, so after
// every snapshot the first write to each database extent is forced CoW
// (documented upstream behavior), allocating fresh small extents:
// measured live 2026-10-20, the folder DB re-fragmented from 14 extents
// to 127,579 within 45 minutes of catch-up sync, at constant file size.
// NOCOW, and therefore patches 36/38/40/41, can never win while the
// database shares a subvolume with a snapshot source.
//
// A nested subvolume is excluded from parent snapshots (the snapshot diff
// code already relies on that exclusion), so moving the database
// directory into one permanently removes it from the sharing cycle.
// Snapshots of the database are of no value: the WAL validates itself and
// the database rebuilds from peers in the worst case.
//
// The move copies every byte — rename(2) does not cross a btrfs
// subvolume boundary at all (EXDEV for files and directories alike,
// observed live on yarlung) — so a 10 GiB database directory costs one
// sequential copy, once, ever (self-terminating: the inode-256 probe
// never fires again). The copy reuses patch/41's disciplines: readerOnly
// defeats the copy_file_range reflink trap, the subvolume root is flagged
// NOCOW before content lands so files inherit it from birth, and the
// writeback-backlog and free-space gates postpone under the same
// congestion rules as defrag. Every step is idempotent and crash-safe:
// interruption leaves the original directory intact (a partial copy in
// the sibling is discarded on retry; the source is authoritative because
// the database is closed at startup) or the completed sibling subvolume
// waiting to be renamed into place. Subvolume creation is unprivileged on
// btrfs (write permission on the parent suffices); removal of the empty
// old directory is a plain rmdir. STDBSUBVOL=0 opts out.
func ensureDBSubvolume(dir string) {
	if os.Getenv("STDBSUBVOL") == "0" {
		return
	}
	if root, err := isSubvolumeRoot(dir); err != nil || root {
		return // already a subvolume, or not a filesystem where it matters
	}
	if !isCoWFilesystemForPath(dir) {
		return
	}
	// Same writeback-congestion rule as the defrag gate (patch/41): a
	// multi-GiB copy under a large dirty backlog allocates in small
	// interleaved extents. Postpone to the next startup instead.
	if backlog := writebackBacklogKiB(); backlog > defragMaxBacklogKiB {
		slog.Info("Database subvolume migration postponed: writeback backlog", slogutil.FilePath(dir), slog.Int64("backlogKiB", backlog))
		return
	}
	if err := migrateDirToSubvolume(dir, createSubvolume); err != nil {
		slog.Warn("Database subvolume migration failed; continuing without it", slogutil.FilePath(dir), slogutil.Error(err))
	}
}

// migrateDirToSubvolume moves every entry of dir into a freshly created
// subvolume at dir+".subvol-tmp" and renames it into place. create is
// injectable for tests (a plain Mkdir reproduces the rename choreography
// on any filesystem).
func migrateDirToSubvolume(dir string, create func(string) error) error {
	tmp := dir + ".subvol-tmp"

	// Crash recovery: a previous attempt may have died at any point.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if _, terr := os.Stat(tmp); terr == nil {
			// Died between rmdir(dir) and rename(tmp, dir).
			return os.Rename(tmp, dir)
		}
		return err // no database directory yet: nothing to migrate
	}
	fresh := false
	if _, err := os.Stat(tmp); os.IsNotExist(err) {
		if err := create(tmp); err != nil {
			return err
		}
		fresh = true
	}

	// Space gate: the move copies every byte (renames do not cross a
	// subvolume boundary — btrfs EXDEV, observed live on dev.139/140), so
	// the parent filesystem must hold the set plus headroom.
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	if avail := freeSpaceBytes(filepath.Dir(dir)); avail < total+total/10 {
		return fmt.Errorf("insufficient free space for subvolume migration: need %d, have %d", total+total/10, avail)
	}
	if total > 0 {
		slog.Info("Migrating database directory into nested btrfs subvolume (physical copy)",
			slogutil.FilePath(dir), slog.Int64("bytes", total))
	}
	start := time.Now()

	// New files inherit the directory NOCOW flag, so flag the subvolume
	// root BEFORE content lands: an extent-bearing file cannot gain the
	// flag afterwards (patch/36). On non-CoW filesystems (tests) this is
	// a no-op.
	if _, err := setNoCOW(tmp); err != nil {
		slog.Debug("NOCOW flag on new subvolume root failed", slogutil.Error(err))
	}

	for _, e := range entries {
		// A name present on BOTH sides is residue of a crash between copy
		// and source removal; the source is authoritative (the database
		// is closed at startup), so the partial copy is discarded.
		if _, err := os.Stat(filepath.Join(tmp, e.Name())); err == nil {
			if err := os.RemoveAll(filepath.Join(tmp, e.Name())); err != nil {
				return fmt.Errorf("discarding partial copy of %s: %w", e.Name(), err)
			}
		}
		if err := moveEntryAcrossSubvol(filepath.Join(dir, e.Name()), filepath.Join(tmp, e.Name())); err != nil {
			return err
		}
	}
	if err := os.Remove(dir); err != nil { // empty now: rmdir
		return err
	}
	if err := os.Rename(tmp, dir); err != nil {
		return err
	}
	syncParentDir(filepath.Dir(dir))
	if fresh || total > 0 {
		slog.Info("Database directory migrated to nested btrfs subvolume (excluded from folder snapshots)",
			slogutil.FilePath(dir), slog.Int64("bytes", total), slog.Duration("duration", time.Since(start)))
	}
	return nil
}

// moveEntryAcrossSubvol moves one entry into the new subvolume. Renames
// do not cross a subvolume boundary at all on btrfs (EXDEV for files and
// directories alike — observed live), so files are physically copied
// (readerOnly defeats the copy_file_range reflink trap, patch/41) and
// directories are recreated with their contents moved recursively.
func moveEntryAcrossSubvol(src, dst string) error {
	fi, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		if err := os.Mkdir(dst, fi.Mode().Perm()); err != nil {
			return err
		}
		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := moveEntryAcrossSubvol(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
				return err
			}
		}
		return os.Remove(src)
	}
	return copyFilePhysical(src, dst, fi.Mode())
}

// copyFilePhysical copies src to dst (which must not exist), fsyncs, and
// removes src. The readerOnly shim keeps io.CopyBuffer from resolving to
// copy_file_range(2), which btrfs implements as an extent-SHARING clone —
// a shared extent would defeat the NOCOW flag the destination directory
// provides.
func copyFilePhysical(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close() //nolint:errcheck

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	buf := make([]byte, 4<<20)
	if _, err := io.CopyBuffer(out, readerOnly{in}, buf); err != nil {
		out.Close()    //nolint:errcheck
		os.Remove(dst) //nolint:errcheck
		return fmt.Errorf("copy %s: %w", src, err)
	}
	if err := out.Sync(); err != nil {
		out.Close()    //nolint:errcheck
		os.Remove(dst) //nolint:errcheck
		return fmt.Errorf("sync %s: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		os.Remove(dst) //nolint:errcheck
		return err
	}
	return os.Remove(src)
}
