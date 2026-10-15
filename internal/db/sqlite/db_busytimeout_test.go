// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// A second writer on the same database file must wait for the first
// writer's transaction to commit (up to busy_timeout), not fail with
// SQLITE_BUSY. Instant failure is connection-fatal at the protocol layer:
// an incoming index update racing a background writer (need-tracking build
// chunk, chunked GC pass) kills the device connection for no reason —
// observed live on yarlung 2026-10-12, six connections killed in 13 s
// ("handling index-update for home: update: deviceidxlocked: database is
// locked") while the background need-tracking build held the write lock in
// bursts.
//
// Teeth: with busy_timeout removed from commonOptions the competing INSERT
// returns "database is locked" and the test fails.
func TestBusyTimeoutWaitsForCompetingWriter(t *testing.T) {
	t.Parallel()

	dsn := (&url.URL{
		Scheme:   "file",
		Path:     fileToUriPath(filepath.Join(t.TempDir(), "test.db")),
		RawQuery: commonOptions,
	}).String()

	ctx := context.Background()
	first := sqlx.MustOpen(dbDriver, dsn)
	defer first.Close()
	if _, err := first.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ExecContext(ctx, `CREATE TABLE t (v INTEGER)`); err != nil {
		t.Fatal(err)
	}

	// First writer holds the write lock (the DSN sets _txlock=immediate).
	tx, err := first.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	// Second writer on a separate handle, like an index update on the
	// folder handle racing a background build chunk, must block, then
	// succeed once the first commits.
	second := sqlx.MustOpen(dbDriver, dsn)
	defer second.Close()

	const holdTime = 300 * time.Millisecond
	start := time.Now()
	errCh := make(chan error, 1)
	go func() {
		_, err := second.ExecContext(ctx, `INSERT INTO t VALUES (2)`)
		errCh <- err
	}()

	time.Sleep(holdTime)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("competing writer failed instead of waiting: %v", err)
	}
	if elapsed := time.Since(start); elapsed < holdTime {
		t.Fatalf("competing writer returned after %v, before the lock was released (no busy_timeout?)", elapsed)
	}
}

// The shipped DSN must carry the timeout; a regression to zero reopens the
// connection-killing race above.
func TestBusyTimeoutIsSet(t *testing.T) {
	t.Parallel()

	dsn := (&url.URL{
		Scheme:   "file",
		Path:     fileToUriPath(filepath.Join(t.TempDir(), "test.db")),
		RawQuery: commonOptions,
	}).String()

	db := sqlx.MustOpen(dbDriver, dsn)
	defer db.Close()

	var timeout int
	if err := db.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	// Pinned at 30 s: a scan's chunk commits on slow storage hold the write
	// lock well past the previous 5 s (yarlung 2026-10-14 connection-flap
	// loop). A regression below this reopens the connection-killing race.
	if timeout < 30000 {
		t.Fatalf("busy_timeout is %d, want at least 30000", timeout)
	}
}
