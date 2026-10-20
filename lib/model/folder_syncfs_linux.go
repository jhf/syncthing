// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package model

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncfsSupported reports whether batch durability via syncfs(2) is
// available on this platform.
const syncfsSupported = true

// syncfsFn commits all dirty data and metadata of the filesystem containing
// root to stable storage. A variable so tests can stub it.
var syncfsFn = func(root string) error {
	fd, err := os.Open(root)
	if err != nil {
		return err
	}
	defer fd.Close()
	return unix.Syncfs(int(fd.Fd()))
}
