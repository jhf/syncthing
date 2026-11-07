// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build darwin

package model

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Install-group barrier on macOS (patch/74). macOS has no syncfs(2), and
// Go's File.Sync on darwin is fcntl(F_FULLFSYNC): it writes the file and
// flushes the whole drive cache, every time. A group does better: a plain
// fsync(2) per item hands each file's data and metadata to the drive
// without flushing its cache, then ONE F_FULLFSYNC flushes the cache for
// everything written so far. Apple documents F_FULLFSYNC as asking the
// drive to flush all buffered data to permanent storage.
//
// Measured on the Mac (APFS, SSD, 2026-11-07, scratch fsprobe): per-item
// F_FULLFSYNC median 6.1 ms; group of 32 plain fsyncs + one F_FULLFSYNC
// 6.3 ms vs 156.6 ms for 32 F_FULLFSYNCs.
const installBarrierSupported = true

const installBarrierName = "fsync+F_FULLFSYNC"

// installBarrierFn makes every path in paths durable with one cache flush.
// root is the folder root (unused on darwin). A variable so tests can stub
// it and count barriers.
var installBarrierFn = func(root string, paths []string) error {
	return darwinGroupBarrier(root, paths)
}

func darwinGroupBarrier(root string, paths []string) error {
	var last *os.File
	defer func() {
		if last != nil {
			last.Close()
		}
	}()
	for _, p := range paths {
		fd, err := os.Open(filepath.Join(root, p))
		if err != nil {
			return err
		}
		if err := unix.Fsync(int(fd.Fd())); err != nil {
			fd.Close()
			return err
		}
		if last != nil {
			last.Close()
		}
		last = fd
	}
	if last == nil {
		var err error
		if last, err = os.Open(root); err != nil {
			return err
		}
	}
	return fullFsyncFn(last)
}

// fullFsyncFn issues the one drive-cache flush; a variable so tests can
// count it.
var fullFsyncFn = func(fd *os.File) error {
	_, err := unix.FcntlInt(fd.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
