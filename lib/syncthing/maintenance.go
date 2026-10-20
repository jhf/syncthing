// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package syncthing

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/syncthing/syncthing/internal/activityhist"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/svcutil"
)

// Cyclical maintenance scheduling (patch/43).
//
// Expensive database work — currently the patch/41 defrag recopy, which
// costs minutes of startup time — should not happen at an arbitrary
// restart. This scheduler watches the clock against the activity
// histogram (internal/activityhist, fed by the log sink): each night, at
// the middle of the deepest predicted idle run (minute-of-day precision,
// hour-of-week weighting, weekly decay, deterministic per-day jitter,
// default sleep window 01:00-06:00 until ~7 days of data exist), it
// evaluates the fragmentation gate for every database. If any database
// is due, it arms it via a pending flag file and gracefully restarts
// itself through the standard ExitRestart path — the monitor re-execs,
// the defrag runs at 03:00, and daytime restarts stay fast.
//
// Admission control: a job starts only when the predicted idle run is at
// least durationSafetyFactor times the job's EMA duration estimate
// (recorded by the job itself into the histogram). A too-short night is
// skipped with an INF line rather than spending the window on a job that
// cannot finish.
//
// Static overrides, per fork convention: STMAINTENANCETIME=HH:MM pins a
// fixed daily time (histogram ignored), STDBDEFRAG=0 disables the whole
// mechanism.

const (
	activityHistFile    = "activityhist.json"
	maintenanceLastFile = "maintenance.last"

	// minMaintenanceInterval: evaluate at most once per this period, so a
	// restart inside the window does not retrigger.
	minMaintenanceInterval = 20 * time.Hour

	// durationSafetyFactor: the idle run must fit this many expected
	// durations before we spend it.
	durationSafetyFactor = 2

	maintenanceTick      = time.Minute
	activityHistSaveTick = 5 * time.Minute
)

func (a *App) loadActivityHist() {
	path := filepath.Join(locations.Get(locations.Database), activityHistFile)
	h, err := activityhist.Load(path)
	if err != nil {
		slog.Warn("Activity history unreadable; starting fresh", slogutil.FilePath(path), slogutil.Error(err))
		return
	}
	activityhist.Global.MergeFrom(h)
}

func (a *App) startMaintenanceScheduler() {
	if os.Getenv("STDBDEFRAG") == "0" {
		return
	}
	go a.maintenanceLoop()
}

func (a *App) maintenanceLoop() {
	dir := locations.Get(locations.Database)
	histPath := filepath.Join(dir, activityHistFile)
	tick := time.NewTicker(maintenanceTick)
	defer tick.Stop()
	saveTick := time.NewTicker(activityHistSaveTick)
	defer saveTick.Stop()
	for {
		select {
		case <-a.stopped:
			_ = activityhist.Global.Save(histPath)
			return
		case <-saveTick.C:
			_ = activityhist.Global.Save(histPath)
		case <-tick.C:
			a.maintenanceEval(dir, time.Now())
		}
	}
}

func (a *App) maintenanceEval(dir string, now time.Time) {
	lastPath := filepath.Join(dir, maintenanceLastFile)
	if fi, err := os.Stat(lastPath); err == nil && now.Sub(fi.ModTime()) < minMaintenanceInterval {
		return
	}

	var pick time.Time
	var runLenMin int
	if fixed := os.Getenv("STMAINTENANCETIME"); fixed != "" {
		t, err := time.Parse("15:04", fixed)
		if err != nil {
			slog.Warn("STMAINTENANCETIME unparsable, ignoring", slog.String("value", fixed))
			return
		}
		y, m, d := now.Date()
		pick = time.Date(y, m, d, t.Hour(), t.Minute(), 0, 0, now.Location())
		runLenMin = 60 // fixed window: assume an hour fits
	} else {
		pick, runLenMin = activityhist.Global.Pick(now)
	}
	if now.Before(pick) {
		return
	}

	// The window is here and the last evaluation is old: evaluate.
	_ = os.Chtimes(lastPath, now, now) // mark evaluation even when nothing is due

	var due []string
	dbs, _ := filepath.Glob(filepath.Join(dir, "*.db"))
	for _, db := range dbs {
		if sqlite.DefragDue(db) {
			due = append(due, db)
		}
	}
	if len(due) == 0 {
		return
	}

	if expected := activityhist.Global.ExpectedDuration("db-defrag"); expected > 0 &&
		time.Duration(runLenMin)*time.Minute < durationSafetyFactor*expected {
		slog.Info("Maintenance skipped: predicted idle run too short for database defrag",
			slog.Int("idleRunMinutes", runLenMin),
			slog.Duration("expectedDuration", expected),
			slog.Int("databasesDue", len(due)))
		return
	}

	for _, db := range due {
		fd, err := os.Create(db + sqlite.DefragPendingSuffix)
		if err != nil {
			slog.Warn("Maintenance arming failed", slogutil.FilePath(db), slogutil.Error(err))
			return
		}
		fd.Close() //nolint:errcheck
	}
	slog.Info("Maintenance window: restarting for scheduled database defrag",
		slog.Time("picked", pick),
		slog.Int("idleRunMinutes", runLenMin),
		slog.Int("databasesDue", len(due)))
	_ = activityhist.Global.Save(filepath.Join(dir, activityHistFile))
	a.Stop(svcutil.ExitRestart)
}
