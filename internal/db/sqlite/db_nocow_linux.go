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

const (
	btrfsSuperMagic    = 0x9123683e
	bcachefsSuperMagic = 0xca451a4e
	// fsNoCOWFlag is FS_NOCOW_FL from linux/fs.h, not exported by x/sys/unix.
	fsNoCOWFlag = 0x00800000
)

// setNoCOW sets FS_NOCOW_FL on path if it lives on a CoW filesystem and the
// flag is not already set. changed reports whether the flag was newly
// applied. Returns (false, nil) on non-CoW filesystems.
func setNoCOW(path string) (changed bool, err error) {
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
	defer fd.Close()

	flags, err := unix.IoctlGetInt(int(fd.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil {
		return false, err
	}
	if flags&fsNoCOWFlag != 0 {
		return false, nil
	}
	if err := unix.IoctlSetInt(int(fd.Fd()), unix.FS_IOC_SETFLAGS, flags|fsNoCOWFlag); err != nil {
		return false, err
	}
	return true, nil
}
