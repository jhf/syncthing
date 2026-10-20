// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package sqlite

import (
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/unix"
)

// btrfsFirstFreeObjectID: every btrfs subvolume root inode has this
// number, so detecting "is this directory a subvolume root" is a plain
// stat — no ioctls, no privileges.
const btrfsFirstFreeObjectID = 256

// isSubvolumeRoot reports whether path is a btrfs subvolume root.
func isSubvolumeRoot(path string) (bool, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return false, err
	}
	return st.Ino == btrfsFirstFreeObjectID, nil
}

// isCoWFilesystemForPath reports whether path sits on btrfs/bcachefs.
func isCoWFilesystemForPath(path string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false
	}
	return isCoWFilesystem(st.Type)
}

// btrfsIOCSubvolCreate is BTRFS_IOC_SUBVOL_CREATE =
// _IOW(0x94, 14, struct btrfs_ioctl_vol_args) with the v1 args struct
// (int64 fd, char name[4088]) = 4096 bytes. Same value across linux
// architectures. Unprivileged: write permission on the parent directory
// suffices (verified live on yarlung 2026-10-20).
const btrfsIOCSubvolCreate = 0x5000940E

// btrfsIOCVolArgs mirrors struct btrfs_ioctl_vol_args (v1).
type btrfsIOCVolArgs struct {
	fd   int64
	name [4088]byte
}

// createSubvolume creates a btrfs subvolume at path (which must not
// exist). The ioctl targets the PARENT directory and takes a plain
// relative name.
func createSubvolume(path string) error {
	parent := filepath.Dir(path)
	name := filepath.Base(path)
	if len(name) > 255 {
		return unix.ENAMETOOLONG
	}
	fd, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer fd.Close() //nolint:errcheck

	var args btrfsIOCVolArgs
	copy(args.name[:], name)
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, fd.Fd(), btrfsIOCSubvolCreate, uintptr(unsafe.Pointer(&args)))
	if errno != 0 {
		return errno
	}
	return nil
}

// syncParentDir fsyncs the parent directory after the rename swap.
func syncParentDir(dir string) {
	if fd, err := os.Open(dir); err == nil {
		fd.Sync() //nolint:errcheck
		fd.Close()
	}
}
