// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package sqlite

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// fiemapPrefix is the header of struct fiemap (linux/fiemap.h); with
// extentCount=0 the kernel only reports the mapped-extent count, which is
// all the fragmentation gate needs.
type fiemapPrefix struct {
	start         uint64
	length        uint64
	flags         uint32
	mappedExtents uint32
	extentCount   uint32
	reserved      uint32
}

// fsIOCFIEMAP is FS_IOC_FIEMAP = _IOWR('f', 11, struct fiemap), not
// exported by x/sys/unix. Same value across linux architectures.
const fsIOCFIEMAP = 0xC020660B

// extentCount returns the number of physical extents of path via FIEMAP.
// Without FIEMAP_FLAG_SYNC the count may miss not-yet-allocated delalloc
// ranges; the gate runs at startup before the database is opened, when no
// such ranges exist, so the cheaper unsynced query is exact here.
func extentCount(path string) (int64, error) {
	fd, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer fd.Close() //nolint:errcheck

	fm := fiemapPrefix{
		length:      ^uint64(0), // whole file
		extentCount: 0,          // count only
	}
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, fd.Fd(), fsIOCFIEMAP, uintptr(unsafe.Pointer(&fm)))
	if errno != 0 {
		return 0, errno
	}
	return int64(fm.mappedExtents), nil
}

// writebackBacklogKiB returns Dirty+Writeback from /proc/meminfo: the
// kernel writeback backlog. Copying a multi-GiB file while the backlog is
// large allocates in small interleaved extents (the fragmentation this
// gate defragments); the gate refuses to recopy under the same condition.
func writebackBacklogKiB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0 // unknown backlog: treated as idle
	}
	return meminfoDirtyPlusWritebackKiB(data)
}
