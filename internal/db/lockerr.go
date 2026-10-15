// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package db

import "strings"

// IsLockContention reports whether err is a transient SQLite lock
// contention failure (SQLITE_BUSY / SQLITE_LOCKED): another writer held the
// database write lock past busy_timeout. Such an error is worth retrying —
// the contender will commit and release the lock. All other database errors
// are structural and must not be retried.
//
// The three sqlite drivers (mattn cgo, modernc nocgo, wazero) share no
// exported error type for this, so the check matches the stable message
// text. Observed forms:
//
//	cgo:     update: database is locked
//	nocgo:   update: deviceidxlocked: database is locked (5) (SQLITE_BUSY)
func IsLockContention(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "database is locked") ||
		strings.Contains(s, "database table is locked") ||
		strings.Contains(s, "SQLITE_BUSY")
}
