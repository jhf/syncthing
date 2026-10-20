// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !linux

package model

import "errors"

// syncfsSupported reports whether batch durability via syncfs(2) is
// available on this platform.
const syncfsSupported = false

var errSyncfsUnsupported = errors.New("syncfs not supported on this platform")

// syncfsFn always fails on platforms without syncfs(2); batchSyncfs()
// excludes them via syncfsSupported, so this is never called in practice.
// A variable so tests can stub it.
var syncfsFn = func(string) error {
	return errSyncfsUnsupported
}
