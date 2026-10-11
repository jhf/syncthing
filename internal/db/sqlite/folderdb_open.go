// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

type folderDB struct {
	*baseDB

	folderID string

	localDeviceIdx  int64
	deleteRetention time.Duration

	// needCache holds the expensive per-device need count. It has its own
	// locking and must be invalidated by every write to the files table.
	needCache needCountCache

	// needListCache holds the remote need list for the out-of-sync view. Same
	// locking and invalidation contract as needCache, and invalidated with it.
	needListCache needListCache

	// needBuilt marks the incrementally maintained need_detail/need_totals
	// tables as authoritative; until it is set (background build after
	// upgrade, see folderdb_need.go) need reads fall back to the full-scan
	// queries. needBuild coordinates the builder goroutine.
	needBuilt atomic.Bool
	needBuild needBuildState
}

// Close shuts the folder database down. The background need-table builder
// (ensureNeedBuild) holds database handles while it runs, and on Windows an
// open handle blocks removing the database file - observed in CI, where a
// test's TempDir cleanup failed with "file being used" because the builder
// was still mid-build when the pool closed. Bump the generation so the
// builder aborts at its next chunk boundary, and wait for it to leave before
// closing the pool.
func (s *folderDB) Close() error {
	s.needBuild.mu.Lock()
	s.needBuild.gen++
	for s.needBuild.running {
		s.needBuild.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		s.needBuild.mu.Lock()
	}
	s.needBuild.mu.Unlock()
	return s.baseDB.Close()
}

func openFolderDB(folder, path string, deleteRetention time.Duration) (*folderDB, error) {
	pragmas := []string{
		"journal_mode = WAL",
		"optimize = 0x10002",
		"auto_vacuum = INCREMENTAL",
		fmt.Sprintf("application_id = %d", applicationIDFolder),
	}
	schemas := []string{
		"sql/schema/common/*",
		"sql/schema/folder/*",
	}
	migrations := []string{
		"sql/migrations/common/*",
		"sql/migrations/folder/*",
	}

	base, err := openBase(path, maxOpenConns, maxIdleConns, pragmas, schemas, migrations)
	if err != nil {
		return nil, err
	}

	fdb := &folderDB{
		folderID:        folder,
		baseDB:          base,
		deleteRetention: deleteRetention,
	}

	_ = fdb.PutKV("folderID", []byte(folder))

	// The need tracking tables are authoritative only if the background build
	// completed; see folderdb_need.go.
	if val, err := fdb.GetKV(needTrackingBuiltKey); err == nil && len(val) > 0 {
		fdb.needBuilt.Store(true)
	}

	// Touch device IDs that should always exist and have a low index
	// numbers, and will never change
	fdb.localDeviceIdx, _ = fdb.deviceIdxLocked(protocol.LocalDeviceID)
	fdb.tplInput["LocalDeviceIdx"] = fdb.localDeviceIdx

	return fdb, nil
}

// Open the database with options suitable for the migration inserts. This
// is not a safe mode of operation for normal processing, use only for bulk
// inserts with a close afterwards.
func openFolderDBForMigration(folder, path string, deleteRetention time.Duration) (*folderDB, error) {
	pragmas := []string{
		"journal_mode = OFF",
		"foreign_keys = 0",
		"synchronous = 0",
		"locking_mode = EXCLUSIVE",
		fmt.Sprintf("application_id = %d", applicationIDFolder),
	}
	schemas := []string{
		"sql/schema/common/*",
		"sql/schema/folder/*",
	}

	base, err := openBase(path, 1, 1, pragmas, schemas, nil)
	if err != nil {
		return nil, err
	}

	fdb := &folderDB{
		folderID:        folder,
		baseDB:          base,
		deleteRetention: deleteRetention,
	}

	// Touch device IDs that should always exist and have a low index
	// numbers, and will never change
	fdb.localDeviceIdx, _ = fdb.deviceIdxLocked(protocol.LocalDeviceID)
	fdb.tplInput["LocalDeviceIdx"] = fdb.localDeviceIdx

	return fdb, nil
}

func (s *folderDB) deviceIdxLocked(deviceID protocol.DeviceID) (int64, error) {
	devStr := deviceID.String()
	var idx int64
	if err := s.stmt(`
		INSERT INTO devices(device_id)
		VALUES (?)
		ON CONFLICT(device_id) DO UPDATE
			SET device_id = excluded.device_id
		RETURNING idx
	`).Get(&idx, devStr); err != nil {
		return 0, wrap(err)
	}

	return idx, nil
}
