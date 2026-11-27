// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo

package sqlite

/*
// The SQLite amalgamation is compiled into github.com/mattn/go-sqlite3 and
// its API symbols are global, so this package links against the same
// library instance. Declared here rather than included: the mattn header
// is not on this package's include path.
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_api_routines sqlite3_api_routines;
extern int sqlite3_auto_extension(void (*xEntryPoint)(void));
extern int sqlite3_db_config(sqlite3 *db, int op, ...);

#define ST_DBCONFIG_NO_CKPT_ON_CLOSE 1006

static int stNoCkptOnClose(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
	(void)pzErrMsg;
	(void)pApi;
	return sqlite3_db_config(db, ST_DBCONFIG_NO_CKPT_ON_CLOSE, 1, (int *)0);
}

static int stRegisterNoCkptOnClose(void) {
	return sqlite3_auto_extension((void (*)(void))stNoCkptOnClose);
}
*/
import "C"

import (
	"fmt"
	"log/slog"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// Close does not checkpoint. SQLite's last-connection close copies the whole
// WAL backlog into the database before it returns. On yarlung (spinning
// disks, a serve thread in D state in 96% of 1 s samples, 2026-11-27) that
// copy queues behind btrfs metadata reads, so close time follows disk
// contention, not backlog size: 1,149 frames took 5.283 s (01:36), 20,448
// frames passed the app's 10 s close limit (08:59), while the Mac closed
// 3,328 frames in 0.391 s. Nothing is lost by skipping it: with
// synchronous=1 a committed transaction is in the WAL, the next open
// replays the WAL, and the running process checkpoints it as usual
// (autocheckpoint, tidy). The 09:00:05 exit that abandoned the close left a
// 136 MB WAL, and the next start was ready to sync 1 s after launch.
//
// The option is per connection (SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE) and the
// driver exposes no db_config, so an auto extension sets it on every
// connection SQLite opens in this process. Registered at init, before any
// open.
func init() {
	if err := registerNoCheckpointOnClose(); err != nil {
		slog.Warn("Database close will checkpoint the WAL", slogutil.Error(err))
	}
}

// registerNoCheckpointOnClose makes every connection opened from now on
// skip the checkpoint SQLite runs when the last connection closes.
func registerNoCheckpointOnClose() error {
	if rc := C.stRegisterNoCkptOnClose(); rc != 0 {
		return fmt.Errorf("sqlite3_auto_extension: rc=%d", int(rc))
	}
	return nil
}
