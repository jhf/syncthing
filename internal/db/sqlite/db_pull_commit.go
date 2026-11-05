// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"

	"github.com/jmoiron/sqlx"
	"github.com/syncthing/syncthing/lib/protocol"
)

// pullCommitConnectionLocked reserves one physical connection. The caller holds
// this folder's updateLock through BEGIN, COMMIT (including SQLite's WAL sync),
// rollback and release. Connection-local settings cannot leak to scan/index
// writers: restore them before returning the slot, or discard the connection.
func (s *folderDB) pullCommitConnectionLocked() (*sqlx.Conn, func(), error) {
	ctx := context.Background()
	conn, err := s.sql.Connx(ctx)
	if err != nil {
		return nil, nil, err
	}
	var synchronous, fullfsync int
	if err = conn.GetContext(ctx, &synchronous, "PRAGMA synchronous"); err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err = conn.GetContext(ctx, &fullfsync, "PRAGMA fullfsync"); err != nil {
		conn.Close()
		return nil, nil, err
	}
	release := func() {
		_, err1 := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA synchronous = %d", synchronous))
		_, err2 := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA fullfsync = %d", fullfsync))
		if err1 != nil || err2 != nil {
			// database/sql discards a physical connection when Raw returns ErrBadConn.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}
	if _, err = conn.ExecContext(ctx, "PRAGMA synchronous = FULL"); err != nil {
		release()
		return nil, nil, err
	}
	if runtime.GOOS == "darwin" {
		if _, err = conn.ExecContext(ctx, "PRAGMA fullfsync = ON"); err != nil {
			release()
			return nil, nil, err
		}
	}
	return conn, release, nil
}

// PendingInstalls uses existing file-local flags, protobufs and blocklists. No
// sidecar, schema change or recovery log is involved. Rows are fully consumed
// before the caller performs filesystem I/O or clears flags.
func (s *DB) PendingInstalls(folder string) ([]protocol.FileInfo, error) {
	fdb, err := s.getFolderDB(folder, false)
	if errors.Is(err, errNoSuchFolder) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fdb.updateLock.Lock()
	defer fdb.updateLock.Unlock()
	// Existing count triggers track every local-flag combination. Almost all
	// calls are empty, so avoid a folder-sized files-table scan in that case.
	var count int64
	if err := fdb.stmt(`SELECT COALESCE(SUM(count),0) FROM counts
		WHERE device_idx = {{.LocalDeviceIdx}} AND local_flags & ? != 0`).Get(&count, protocol.FlagLocalInstallPending); err != nil {
		return nil, wrap(err)
	}
	if count == 0 {
		return nil, nil
	}
	var rows []indirectFI
	err = fdb.stmt(`
  SELECT fi.fiprotobuf, bl.blprotobuf FROM files f
  INNER JOIN fileinfos fi ON fi.sequence = f.sequence
  LEFT JOIN blocklists bl ON bl.blocklist_hash = f.blocklist_hash
  WHERE f.device_idx = {{.LocalDeviceIdx}} AND f.local_flags & ? != 0
 `).Select(&rows, protocol.FlagLocalInstallPending)
	if err != nil {
		return nil, wrap(err)
	}
	files := make([]protocol.FileInfo, 0, len(rows))
	for _, row := range rows {
		fi, err := row.FileInfo()
		if err != nil {
			return nil, wrap(err)
		}
		files = append(files, fi)
	}
	return files, nil
}
