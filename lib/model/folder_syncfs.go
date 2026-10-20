// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"os"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/fs"
)

// On Linux, durability for a batch of pulled files is provided by one
// syncfs(2) of the folder filesystem instead of an fsync per pulled file
// plus an fsync per changed directory. On a journaling/CoW filesystem each
// fsync forces a journal or log-tree commit whose fixed cost dwarfs the
// payload of a small-file pull stream (measured on yarlung 2026-10-20,
// btrfs RAID1: ~450 files/min at ~22 KiB each cost ~550 MB/min of block
// writes, ~0.8 MB per fsync, io pressure >40%, pull latency p99 21 s). One
// syncfs per database batch (≤2 s) covers everything the batch wrote —
// temp file contents, renames, and directory entries alike — so the commit
// count collapses from twice the file count to one per batch, with
// durability ordering unchanged: storage sync still happens before the
// index update commits, and a crash before the sync leaves temp residue
// that is simply re-pulled. STSYNCMODE=fsync restores the per-file/per-dir
// behavior.

// batchSyncfs reports whether pull durability comes from a per-batch
// syncfs(2) rather than per-file and per-directory fsyncs.
func (f *sendReceiveFolder) batchSyncfs() bool {
	if !syncfsSupported || f.DisableFsync {
		return false
	}
	if f.Filesystem().Type() != fs.FilesystemTypeBasic {
		// syncfs(2) needs a real local path.
		return false
	}
	return os.Getenv("STSYNCMODE") != "fsync"
}

// flushPullDurability makes the current database batch's pulled files
// durable before their index updates commit.
func (f *sendReceiveFolder) flushPullDurability(changedDirs map[string]struct{}) {
	if f.DisableFsync {
		return
	}
	if f.batchSyncfs() {
		if err := syncfsFn(f.Filesystem().URI()); err == nil {
			return
		} else {
			f.sl.Warn("syncfs failed, falling back to per-directory fsyncs", slogutil.Error(err))
		}
	}
	f.fsyncDirs(changedDirs)
}
