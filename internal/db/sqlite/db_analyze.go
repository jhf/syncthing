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
	"log/slog"
	"os"
	"time"

	"github.com/syncthing/syncthing/internal/activityhist"
	"github.com/syncthing/syncthing/internal/slogutil"
)

// Planner statistics policy (patch/70).
//
// Full ANALYZE reads every row of every index. analysis_limit caps the rows
// SAMPLED per index, but on yarlung's 10 GB folder DB on spinning disks
// ANALYZE "blocks" (4M rows, WITHOUT ROWID) still took 7m48s, and any
// writer waiting for the database waited that long too. Statistics barely
// move day to day, so the expensive refresh runs only where nothing waits:
//
//   - Daily tidy (db_service.go): PRAGMA optimize only, under updateLock,
//     interrupted after tidyOptimizeBudget. optimize re-analyzes a table only
//     when its row count drifted materially since the last ANALYZE. An
//     interrupted optimize is logged and retried by the next day's tidy.
//   - Planned maintenance restart (patch/43/53 scheduler, AnalyzePendingSuffix
//     flag) and a database with no statistics at all: full ANALYZE in
//     openBase, before the database is published to any writer.
//
// STDBANALYZE=0 disables the offline ANALYZE; STDBANALYZE=1 forces it once
// at the next startup (manual override), mirroring STDBDEFRAG.

// tidyOptimizeBudget bounds how long daily PRAGMA optimize may hold
// updateLock. A var so tests can shorten it.
var tidyOptimizeBudget = 10 * time.Second

// AnalyzePendingSuffix marks a database the maintenance scheduler has armed
// for a full offline ANALYZE at the next startup.
const AnalyzePendingSuffix = ".analyze-pending"

// AnalyzePending reports whether the scheduler armed this database.
func AnalyzePending(path string) bool {
	_, err := os.Stat(path + AnalyzePendingSuffix)
	return err == nil
}

// tidyBudgetedStep runs one statement under updateLock, cancelling it after
// budget. Both drivers map context cancellation to sqlite3_interrupt, which
// stops the statement at its next VDBE step and rolls back its partial
// statistics write. An interrupt is reported, not returned as an error.
func tidyBudgetedStep(ctx context.Context, db *baseDB, conn *sql.Conn, stmt string, budget time.Duration) error {
	sctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	db.updateLock.Lock()
	start := time.Now()
	_, err := conn.ExecContext(sctx, stmt)
	duration := time.Since(start)
	db.updateLock.Unlock()
	interrupted := err != nil && ctx.Err() == nil && (errors.Is(sctx.Err(), context.DeadlineExceeded) || isInterrupt(err))
	slog.InfoContext(ctx, "Database tidy statement", "db", db.path, "statement", stmt, "duration", duration, "budget", budget, "interrupted", interrupted, "error", err)
	if interrupted {
		return nil
	}
	return wrap(err)
}

// isInterrupt matches SQLITE_INTERRUPT across both drivers.
func isInterrupt(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		containsFold(s, "interrupted") || containsFold(s, "SQLITE_INTERRUPT")
}

func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if 'A' <= a && a <= 'Z' {
				a += 'a' - 'A'
			}
			if 'A' <= b && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// hasStatistics reports whether ANALYZE has ever run on this database.
func hasStatistics(conn *sql.Conn, ctx context.Context) bool {
	var n int
	if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'`).Scan(&n); err != nil || n == 0 {
		return false
	}
	if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_stat1`).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// analyzeOffline runs a full ANALYZE during exclusive initialization,
// before the database is published: at a planned maintenance restart, on
// first open of a database that has no statistics, or when forced. Nothing
// waits for it. Its duration is recorded per byte so the scheduler's
// admission control can budget it alongside the defrag.
func (s *baseDB) analyzeOffline() error {
	mode := os.Getenv("STDBANALYZE")
	if mode == "0" {
		return nil
	}
	ctx := context.Background()
	conn, err := s.sql.Conn(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()

	pending := AnalyzePending(s.path)
	reason := ""
	switch {
	case mode == "1":
		reason = "forced"
	case pending:
		reason = "scheduled"
	case !hasStatistics(conn, ctx):
		reason = "no-statistics"
	default:
		// Existing statistics from before patch/70 count as fresh: seed the
		// marker so the first night does not take a restart just for that.
		if _, err := os.Stat(s.path + analyzedMarkerSuffix); os.IsNotExist(err) {
			if fd, err := os.Create(s.path + analyzedMarkerSuffix); err == nil {
				fd.Close() //nolint:errcheck
			}
		}
		return nil
	}
	if pending {
		// Consume the flag before running: a crash mid-ANALYZE leaves
		// complete old statistics (ANALYZE is one transaction), and the
		// scheduler re-arms at the next window.
		_ = os.Remove(s.path + AnalyzePendingSuffix)
	}

	var size int64
	if fi, err := os.Stat(s.path); err == nil {
		size = fi.Size()
	}
	slog.Info("Analyzing database statistics (offline)", slogutil.FilePath(s.path), slog.String("reason", reason), slog.Int64("bytes", size))
	start := time.Now()
	if _, err := conn.ExecContext(ctx, `PRAGMA analysis_limit = 1000`); err != nil {
		return wrap(err)
	}
	if _, err := conn.ExecContext(ctx, `ANALYZE`); err != nil {
		slog.Warn("Offline database ANALYZE failed; keeping previous statistics", slogutil.FilePath(s.path), slogutil.Error(err))
		return nil
	}
	duration := time.Since(start)
	activityhist.Global.RecordDurationBytes("db-analyze", duration, size)
	// Record completion for the scheduler's staleness check.
	if fd, err := os.Create(s.path + analyzedMarkerSuffix); err == nil {
		fd.Close() //nolint:errcheck
	}
	slog.Info("Offline database ANALYZE complete", slogutil.FilePath(s.path), slog.String("reason", reason), slog.Duration("duration", duration))
	return nil
}

// analyzedMarkerSuffix's mtime records the last completed offline ANALYZE.
const analyzedMarkerSuffix = ".analyzed"

// analyzeMaxAge is how stale statistics may get before the scheduler takes
// a maintenance restart for ANALYZE alone (no defrag due).
const analyzeMaxAge = 7 * 24 * time.Hour

// AnalyzeDue reports whether this database's offline ANALYZE is older than
// analyzeMaxAge, or has never run. The scheduler uses it to decide whether a
// maintenance restart is worth having when no defrag is due.
func AnalyzeDue(path string, now time.Time) bool {
	if os.Getenv("STDBANALYZE") == "0" {
		return false
	}
	if _, err := os.Stat(path); err != nil {
		return false
	}
	fi, err := os.Stat(path + analyzedMarkerSuffix)
	if err != nil {
		return true
	}
	return now.Sub(fi.ModTime()) >= analyzeMaxAge
}
