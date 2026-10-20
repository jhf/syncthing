// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux

package sqlite

import (
	"errors"
	"os"
)

var errNoSubvolumes = errors.New("subvolumes unsupported on this platform")

// isSubvolumeRoot: no btrfs subvolumes off Linux.
func isSubvolumeRoot(string) (bool, error) { return false, errNoSubvolumes }

// isCoWFilesystemForPath is false off Linux (APFS CoW is free on flash).
func isCoWFilesystemForPath(string) bool { return false }

// createSubvolume is unreachable off Linux (ensureDBSubvolume returns at
// the filesystem gate first); present so migrateDirToSubvolume links.
func createSubvolume(string) error { return errNoSubvolumes }

// syncParentDir is a best-effort dir fsync, meaningful everywhere.
func syncParentDir(dir string) {
	if fd, err := os.Open(dir); err == nil {
		fd.Sync() //nolint:errcheck
		fd.Close()
	}
}
