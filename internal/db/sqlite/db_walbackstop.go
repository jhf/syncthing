// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
)

// WAL backstop (patch/73). Every in-process checkpoint is PASSIVE: it copies
// what it can and never waits, because a waiting checkpoint run from the
// writer's goroutine can wait on that same goroutine's open read (the
// 2026-10-19 self-deadlock). The cost is that a reader that never ends keeps
// every checkpoint short of the WAL's end, SQLite can never reset the WAL,
// journal_size_limit never gets a chance to truncate it, and the WAL grows
// with the write volume until restart (yarlung 2026-11-07: 824 MiB).
//
// The backstop waits for a gap instead of forcing one. When the WAL file
// passes walBackstopBytes and the last periodic checkpoint fell short, one
// goroutine retries a two-step checkpoint (tryTruncateCheckpoint) with
// backoff until it resets the WAL or the WAL is back under the limit. Once
// the long readers are fixed (patch/73's query), the gap comes quickly.
//
// Vars, not consts, so tests can shrink them.
var (
	walBackstopBytes      int64 = 512 << 20 // 4 x the 128 MiB autocheckpoint
	walBackstopBusyMS           = 200
	walBackstopFirstDelay       = 5 * time.Second
	walBackstopMaxDelay         = 5 * time.Minute
	walBackstopWarnAfter        = 30 * time.Minute
)

// walBackstop: at most one backstop goroutine per database.
type walBackstop struct {
	running atomic.Bool
}

func walSize(path string) int64 {
	fi, err := os.Stat(path + "-wal")
	if err != nil {
		return 0
	}
	return fi.Size()
}

// maybeStartWALBackstop is called after a PASSIVE checkpoint that did not
// reach the end of the WAL. It never blocks the caller.
func (s *baseDB) maybeStartWALBackstop() {
	size := walSize(s.path)
	if size < walBackstopBytes {
		return
	}
	if !s.walBackstop.running.CompareAndSwap(false, true) {
		return
	}
	slog.Info("WAL over backstop limit, waiting for a reader gap to truncate it",
		slog.String("db", s.baseName), slog.Int64("walBytes", size), slog.Int64("limitBytes", walBackstopBytes))
	go s.runWALBackstop(size)
}

func (s *baseDB) runWALBackstop(startSize int64) {
	defer s.walBackstop.running.Store(false)
	t0 := time.Now()
	delay := walBackstopFirstDelay
	warned := false
	for attempt := 1; ; attempt++ {
		if walSize(s.path) < walBackstopBytes {
			// Something else (a full PASSIVE plus journal_size_limit, or a
			// restart of the connection pool) already brought it down.
			slog.Info("WAL back under backstop limit", slog.String("db", s.baseName),
				slog.Int64("walBytes", walSize(s.path)), slog.Duration("after", time.Since(t0)), slog.Int("attempts", attempt-1))
			return
		}
		done, busy, err := s.tryTruncateCheckpoint()
		switch {
		case err != nil && errors.Is(err, sql.ErrConnDone),
			err != nil && strings.Contains(err.Error(), "database is closed"):
			return // the database closed; the close itself checkpoints
		case err != nil:
			slog.Debug("WAL backstop checkpoint error", slog.String("db", s.baseName), slogutil.Error(err))
		case done:
			slog.Info("WAL truncated by backstop", slog.String("db", s.baseName),
				slog.Int64("walBytesBefore", startSize), slog.Int64("walBytesAfter", walSize(s.path)),
				slog.Duration("waited", time.Since(t0)), slog.Int("attempts", attempt))
			return
		case busy:
			// A reader still pins the WAL; try again later.
		}
		if !warned && time.Since(t0) > walBackstopWarnAfter {
			warned = true
			slog.Warn("WAL backstop still waiting for a reader gap", slog.String("db", s.baseName),
				slog.Int64("walBytes", walSize(s.path)), slog.Duration("waiting", time.Since(t0)), slog.Int("attempts", attempt))
		}
		time.Sleep(delay)
		delay = min(2*delay, walBackstopMaxDelay)
	}
}

// tryTruncateCheckpoint makes one attempt in two steps, so the write lock is
// only ever held briefly (patch/66 single-writer invariant; patch/70's lesson
// that a long step under updateLock stalls pulls and index handling):
//
//  1. PASSIVE checkpoint with no lock, exactly like the periodic one. It
//     copies every frame no reader pins; on a WAL of hundreds of MiB this is
//     the slow part, and it never blocks a writer.
//  2. Only if step 1 reached the end of the WAL: under updateLock, a
//     TRUNCATE checkpoint with a short busy timeout. It has at most the
//     frames written since step 1 to copy, then resets and truncates the
//     file. If a reader appeared meanwhile it gives up after
//     walBackstopBusyMS and the next attempt starts over.
//
// done means the WAL was reset; busy means a reader was in the way.
func (s *baseDB) tryTruncateCheckpoint() (done, busy bool, err error) {
	ctx := context.Background()
	conn, err := s.sql.Conn(ctx)
	if err != nil {
		return false, false, err
	}
	defer conn.Close()

	var res, logPages, moved int
	if err := conn.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&res, &logPages, &moved); err != nil {
		return false, false, err
	}
	if res != 0 || moved < logPages {
		return false, true, nil
	}

	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	var prev int
	if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&prev); err != nil {
		return false, false, err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA busy_timeout = %d`, walBackstopBusyMS)); err != nil {
		return false, false, err
	}
	defer func() { _, _ = conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA busy_timeout = %d`, prev)) }()
	if err := conn.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&res, &logPages, &moved); err != nil {
		if db.IsLockContention(err) {
			return false, true, nil
		}
		return false, false, err
	}
	return res == 0, res != 0, nil
}
