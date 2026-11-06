// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

func openTestFolderDB(t *testing.T) (*DB, *folderDB) {
	t.Helper()
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sdb.Close() })
	fdb, err := sdb.getFolderDB("test", true)
	if err != nil {
		t.Fatal(err)
	}
	return sdb, fdb
}

// Daily tidy must run no ANALYZE: on a 10 GB HDD folder DB, ANALYZE
// "blocks" alone held updateLock for 7m48s (yarlung, 2026-11-06).
func TestTidyRunsNoAnalyze(t *testing.T) {
	_, fdb := openTestFolderDB(t)
	p := attachWriterProbe(t, fdb.baseDB)
	var mu sync.Mutex
	var analyzes []string
	p.before = func(q string) {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), "ANALYZE") {
			mu.Lock()
			analyzes = append(analyzes, q)
			mu.Unlock()
		}
	}
	if err := runProbedTidy(context.Background(), fdb.baseDB); err != nil {
		t.Fatal(err)
	}
	if len(analyzes) != 0 {
		t.Fatalf("daily tidy ran ANALYZE: %v", analyzes)
	}
}

// PRAGMA optimize is the only statistics work left in daily tidy. A slow
// one is interrupted at its budget, so an Update waits at most that long.
func TestTidyOptimizeBudgetBoundsUpdateWait(t *testing.T) {
	_, fdb := openTestFolderDB(t)
	old := tidyOptimizeBudget
	tidyOptimizeBudget = 300 * time.Millisecond
	t.Cleanup(func() { tidyOptimizeBudget = old })

	p := attachWriterProbe(t, fdb.baseDB)
	var once sync.Once
	waited := make(chan time.Duration, 1)
	p.before = func(q string) {
		if strings.ToUpper(strings.TrimSpace(q)) != "PRAGMA OPTIMIZE" {
			return
		}
		once.Do(func() {
			go func() {
				start := time.Now()
				if err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{genFile("during-optimize", 1, 1)}, db.UpdateOptions{}); err != nil {
					t.Error(err)
				}
				waited <- time.Since(start)
			}()
			// Simulate an optimize that would run far past its budget.
			time.Sleep(50 * time.Millisecond)
		})
	}
	if err := runProbedTidy(context.Background(), fdb.baseDB); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-waited:
		t.Logf("Update waited %s behind optimize (budget %s)", d, tidyOptimizeBudget)
		if d > tidyOptimizeBudget+2*time.Second {
			t.Fatalf("Update waited %s, budget %s", d, tidyOptimizeBudget)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Update never completed")
	}
}

// A scheduled maintenance restart produces fresh statistics before the
// database is published, consumes the flag, and records completion.
func TestOfflineAnalyzeAtScheduledRestart(t *testing.T) {
	dir := t.TempDir()
	sdb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB("test", true)
	if err != nil {
		t.Fatal(err)
	}
	path := fdb.path
	if err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{genFile("a", 1, 1), genFile("b", 1, 2)}, db.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fdb.sql.Exec(`DELETE FROM sqlite_stat1`); err != nil && !strings.Contains(err.Error(), "no such table") {
		t.Fatal(err)
	}
	sdb.Close()

	if err := os.WriteFile(path+AnalyzePendingSuffix, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sdb, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	fdb, err = sdb.getFolderDB("test", false)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := fdb.sql.Get(&n, `SELECT count(*) FROM sqlite_stat1`); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("scheduled offline ANALYZE produced no statistics")
	}
	if AnalyzePending(path) {
		t.Fatal("analyze-pending flag not consumed")
	}
	if AnalyzeDue(path, time.Now()) {
		t.Fatal("completed ANALYZE not recorded for the scheduler")
	}
	if !AnalyzeDue(path, time.Now().Add(analyzeMaxAge+time.Hour)) {
		t.Fatal("statistics never become due again")
	}
}
