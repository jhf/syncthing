// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux

package sqlite

const (
	btrfsSuperMagic    = 0x9123683e
	bcachefsSuperMagic = 0xca451a4e
)

// setNoCOW is a no-op off Linux (FS_NOCOW_FL is a Linux ioctl).
func setNoCOW(string) (changed bool, err error) {
	return false, nil
}
