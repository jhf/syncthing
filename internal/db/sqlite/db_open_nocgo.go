// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !cgo && !wazero

package sqlite

import (
	"github.com/syncthing/syncthing/lib/build"
	_ "modernc.org/sqlite" // register sqlite database driver
)

const (
	dbDriver = "sqlite"
	// busy_timeout: see db_open_cgo.go. Two writers on one file are
	// legitimate; instant SQLITE_BUSY is connection-fatal at the protocol
	// layer.
	commonOptions = "_pragma=foreign_keys(1)&_pragma=recursive_triggers(1)&_pragma=synchronous(1)&_txlock=immediate&_pragma=busy_timeout(5000)"
)

func init() {
	build.AddTag("modernc-sqlite")
}
