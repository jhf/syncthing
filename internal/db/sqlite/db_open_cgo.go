// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo

package sqlite

import (
	_ "github.com/mattn/go-sqlite3" // register sqlite3 database driver
)

const (
	dbDriver = "sqlite3"
	// busy_timeout, pinned explicitly rather than relying on the driver
	// default: two writers on the same database file are legitimate (e.g. an
	// index update racing the background need-tracking build or a chunked
	// GC pass). Without a timeout the second writer fails instantly with
	// SQLITE_BUSY, which the protocol layer treats as connection-fatal.
	// Thirty seconds because a scan's chunk commits on slow storage (HDD
	// RAID) hold the write lock well past five seconds — observed live on
	// yarlung 2026-10-14, a ~20 s connection-flap loop while a fallback
	// full scan ran. The index handler additionally retries contention,
	// so this value bounds one attempt's wait, not the total patience.
	commonOptions = "_fk=true&_rt=true&_sync=1&_txlock=immediate&_busy_timeout=30000"
)
