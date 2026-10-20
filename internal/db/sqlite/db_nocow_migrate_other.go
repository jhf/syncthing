// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux

package sqlite

// lacksNoCOW is always false off Linux: FS_NOCOW_FL is a Linux ioctl, so no
// migration is ever needed or possible.
func lacksNoCOW(string) (bool, error) {
	return false, nil
}

func freeSpaceBytes(string) int64 {
	// Off Linux the space gates never gate a real operation (the callers'
	// filesystem checks return first), but tests exercise the copy
	// choreography cross-platform; report "unconstrained".
	return 1 << 62
}
