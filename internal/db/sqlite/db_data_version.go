// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/syncthing/syncthing/internal/db"
)

type dataVersionObserver struct{ conn *sql.Conn }

// OpenDataVersionObserver reserves exactly one physical connection. The caller
// bounds acquisition with ctx, then retains it for all comparable samples.
// Queries are scalar autocommit reads, never a read transaction across waits.
func (s *DB) OpenDataVersionObserver(ctx context.Context, folder string) (db.DataVersionObserver, error) {
	// Observation must not queue behind a potentially slow folder opener.
	// The failed updater already opened its folder. Missing/busy cache means
	// unknown diagnostics, never another DB creation or unbounded mutex wait.
	if !s.folderDBsMut.TryRLock() {
		return nil, fmt.Errorf("observer folder registry busy")
	}
	f, ok := s.folderDBs[folder]
	s.folderDBsMut.RUnlock()
	if !ok {
		return nil, fmt.Errorf("observer folder database not open")
	}
	conn, err := f.sql.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &dataVersionObserver{conn}, nil
}

func (o *dataVersionObserver) DataVersion(ctx context.Context) (uint64, error) {
	var version uint64
	err := o.conn.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version)
	return version, err
}

func (o *dataVersionObserver) Close() error { return o.conn.Close() }
