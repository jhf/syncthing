// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package syncthing

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/svcutil"
)

func TestInMaintenanceWindow(t *testing.T) {
	pick := time.Date(2026, 10, 21, 3, 17, 0, 0, time.Local)
	cases := []struct {
		name   string
		now    time.Time
		runLen int
		want   bool
	}{
		{"before pick", pick.Add(-time.Hour), 300, false},
		{"at pick", pick, 300, true},
		{"inside", pick.Add(90 * time.Minute), 300, true},
		{"at capped end (3h)", pick.Add(3 * time.Hour), 300, false},
		{"inside short run", pick.Add(45 * time.Minute), 60, true},
		{"past short run", pick.Add(61 * time.Minute), 60, false},
		// The dev.137 bug: first boot at 21:15 with a 03:30 pick must not fire.
		{"evening of the same day", time.Date(2026, 10, 21, 21, 15, 0, 0, time.Local), 300, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inMaintenanceWindow(tc.now, pick, tc.runLen); got != tc.want {
				t.Errorf("inMaintenanceWindow(%v, pick, %d) = %v, want %v", tc.now, tc.runLen, got, tc.want)
			}
		})
	}
}

// An evaluation inside the window leaves its mark on disk, so the
// minMaintenanceInterval throttle holds across the restart it may cause.
// The original os.Chtimes on a missing file failed silently: the mark
// never existed and every minute of the window evaluated again.
func TestMaintenanceEvalMarksWindow(t *testing.T) {
	t.Setenv("STDBDEFRAG", "")
	dir := t.TempDir()
	now := time.Now().Truncate(time.Minute)
	t.Setenv("STMAINTENANCETIME", now.Format("15:04"))
	a := &App{stopped: make(chan struct{}), mainServiceCancel: func() {}}
	close(a.stopped)
	a.maintenanceEval(dir, now)
	fi, err := os.Stat(filepath.Join(dir, maintenanceLastFile))
	if err != nil {
		t.Fatalf("evaluation left no mark: %v", err)
	}
	if !fi.ModTime().Equal(now) {
		t.Errorf("mark time %v, want %v", fi.ModTime(), now)
	}
}

// One maintenance window takes one restart. The restarted process runs
// the scheduler again inside the same window, so the evaluation mark must
// exist on disk (the original os.Chtimes never created it), and every
// database must be armed together so ANALYZE markers written seconds
// apart do not come due seconds apart a week later. Observed on the Mac
// 2026-11-21: restarts at 09:12:33 (main.db) and 09:13:35 (folder DB).
func TestMaintenanceOneRestartPerWindow(t *testing.T) {
	cases := []struct {
		name         string
		analyzeFails bool // ANALYZE writes no marker, so the DBs stay due
	}{
		{"markers a minute apart", false},
		{"analyze fails", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("STDBDEFRAG", "")
			t.Setenv("STDBANALYZE", "")
			dir := t.TempDir()
			now := time.Now().Truncate(time.Minute)
			t.Setenv("STMAINTENANCETIME", now.Format("15:04"))

			// Last week's offline ANALYZE: main.db finished 62 s before
			// the folder database, as on the Mac on 2026-11-14.
			week := 7 * 24 * time.Hour
			dbs := map[string]time.Time{
				"main.db":             now.Add(-week),
				"folder.0001-test.db": now.Add(-week + 62*time.Second),
			}
			for name, analyzed := range dbs {
				p := filepath.Join(dir, name)
				if err := os.WriteFile(p, nil, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p+".analyzed", nil, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(p+".analyzed", analyzed, analyzed); err != nil {
					t.Fatal(err)
				}
			}

			// Each minute a fresh process evaluates once; a restart plays
			// the startup ANALYZE of every armed database.
			restarts := 0
			for m := 0; m < 5; m++ {
				at := now.Add(time.Duration(m) * time.Minute)
				a := &App{stopped: make(chan struct{}), mainServiceCancel: func() {}}
				close(a.stopped)
				a.maintenanceEval(dir, at)
				if a.exitStatus != svcutil.ExitRestart {
					continue
				}
				restarts++
				for name := range dbs {
					p := filepath.Join(dir, name)
					if sqlite.AnalyzePending(p) {
						_ = os.Remove(p + sqlite.AnalyzePendingSuffix)
						if !tc.analyzeFails {
							_ = os.Chtimes(p+".analyzed", at, at)
						}
					}
				}
			}
			if restarts != 1 {
				t.Errorf("maintenance window took %d restarts, want 1", restarts)
			}
			if tc.analyzeFails {
				return
			}
			for name := range dbs {
				if sqlite.AnalyzeDue(filepath.Join(dir, name), now.Add(5*time.Minute)) {
					t.Errorf("%s still due for ANALYZE after the window", name)
				}
			}
		})
	}
}
