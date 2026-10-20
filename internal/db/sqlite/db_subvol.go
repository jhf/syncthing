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
// The move is pure metadata — renames within one filesystem — so 10 GiB
// "moves" in milliseconds. Every step is idempotent and crash-safe:
// interruption leaves either the original directory intact (some entries
// possibly already moved; the next boot moves the rest) or the completed
// sibling subvolume waiting to be renamed into place. Subvolume creation
// is unprivileged on btrfs (write permission on the parent suffices);
// removal of the empty old directory is a plain rmdir. STDBSUBVOL=0 opts
// out.
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
	if _, err := os.Stat(tmp); os.IsNotExist(err) {
		if err := create(tmp); err != nil {
			return err
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.Rename(filepath.Join(dir, e.Name()), filepath.Join(tmp, e.Name())); err != nil {
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
	slog.Info("Database directory migrated to nested btrfs subvolume (excluded from folder snapshots)", slogutil.FilePath(dir))
	return nil
}
