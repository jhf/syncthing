// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package sqlite

import (
	"os"

	"golang.org/x/sys/unix"
)

// lacksNoCOW reports whether path is an existing file on a CoW filesystem
// that does not carry FS_NOCOW_FL.
func lacksNoCOW(path string) (bool, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false, err
	}
	if !isCoWFilesystem(st.Type) {
		return false, nil
	}
	fd, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer fd.Close() //nolint:errcheck
	flags, err := unix.IoctlGetInt(int(fd.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil {
		return false, err
	}
	return flags&fsNoCOWFlag == 0, nil
}

// freeSpaceBytes returns the bytes available to unprivileged users on the
// filesystem holding dir.
func freeSpaceBytes(dir string) int64 {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}
