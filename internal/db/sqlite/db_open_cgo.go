// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo

package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

const (
	dbDriver = "syncthing_sqlite3"
	// Live same-baseDB writers serialize on updateLock, not SQLite's busy
	// handler. Retain this timeout for a foreign process/separately opened
	// handle and exclusive startup/migration. It bounds one SQLite wait,
	// not time waiting for the Go mutex or the pull batch's retry patience.
	commonOptions = "_journal_mode=WAL&_fk=true&_rt=true&_sync=1&_txlock=immediate&_busy_timeout=30000"
)

// pageCacheDSNOption returns a DSN option setting the page cache size, in
// KiB, for every connection the pool opens (mattn/go-sqlite3 _cache_size is
// passed verbatim to PRAGMA cache_size on each new connection).
func pageCacheDSNOption(kib int) string {
	return fmt.Sprintf("&_cache_size=-%d", kib)
}

// walCheckpointDSNOption returns no DSN option for the cgo driver:
// mattn/go-sqlite3 (v1.14.52) supports neither _wal_autocheckpoint nor
// generic _pragma options, so the threshold is set by the ConnectHook
// below, which runs on every connection the pool opens.
func walCheckpointDSNOption(pages int) string {
	return ""
}

func init() {
	sql.Register(dbDriver, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// The DSN already put the database in WAL mode
			// (_journal_mode=WAL), so the threshold sticks; setting it on a
			// non-WAL database is silently ignored.
			_, err := conn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint(%d)", autocheckpointPagesForConns.Load()), nil)
			return err
		},
	})
}
