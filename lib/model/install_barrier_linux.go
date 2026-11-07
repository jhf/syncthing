// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build linux

package model

// Install-group barrier on Linux: one syncfs(2) of the folder's filesystem
// covers every temp's data and every directory entry written so far, so the
// per-item list is not needed (patch/67, patch/74).
const installBarrierSupported = true

const installBarrierName = "syncfs"

var installBarrierFn = func(root string, _ []string) error {
	return syncfsFn(root)
}
