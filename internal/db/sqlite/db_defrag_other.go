// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux

package sqlite

import "errors"

// extentCount is unsupported off Linux (FIEMAP is a Linux ioctl); the
// defrag gate only fires on CoW filesystems, which this probe never sees
// on other platforms, so the error path is unreachable in practice.
func extentCount(string) (int64, error) {
	return 0, errors.New("extent count unsupported on this platform")
}

// writebackBacklogKiB is zero off Linux (the gate never reaches it).
func writebackBacklogKiB() int64 {
	return 0
}
