// Copyright (C) 2026 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

func needWriterFixture(t *testing.T, names, devices int) (*DB, *folderDB) {
	t.Helper()
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Error(err)
		}
	})
	fdb, err := sdb.getFolderDB("test", true)
	if err != nil {
		t.Fatal(err)
	}
	fs := make([]protocol.FileInfo, names)
	for i := range fs {
		fs[i] = genFile(fmt.Sprintf("name-%06d", i), 0, i+1)
		fs[i].Size = int64(i + 1)
	}
	if err := fdb.Update(protocol.LocalDeviceID, fs, db.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < devices; i++ {
		if err := fdb.Update(protocol.DeviceID{byte(i + 2)}, nil, db.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	return sdb, fdb
}

func TestNeedBuildSingleWriterChunks(t *testing.T) {
	const names, devices = 20000, 8
	_, fdb := needWriterFixture(t, names, devices)
	p := attachWriterProbe(t, fdb.baseDB)
	// Mimic ensureNeedBuild's publication so concurrent Updates journal names.
	fdb.needBuild.mu.Lock()
	fdb.needBuild.running = true
	gen := fdb.needBuild.gen
	fdb.needBuild.mu.Unlock()
	var once sync.Once
	done := make(chan error, 1)
	var updated, interleaved atomic.Bool
	var mu sync.Mutex
	var maxTx, maxSQL, maxRangeTx time.Duration
	var rangeStatements, commits int
	p.before = func(q string) {
		if strings.Contains(q, "CROSS JOIN devices d") {
			mu.Lock()
			rangeStatements++
			mu.Unlock()
			if updated.Load() {
				interleaved.Store(true)
			}
		}
	}
	p.after = func(q string, d time.Duration) {
		mu.Lock()
		if strings.HasPrefix(q, "COMMIT") {
			commits++
			if d > maxTx {
				maxTx = d
			}
			if q == "COMMIT RANGE" && d > maxRangeTx {
				maxRangeTx = d
			}
		}
		if strings.Contains(q, "CROSS JOIN devices d") && d > maxSQL {
			maxSQL = d
		}
		mu.Unlock()
		if !strings.HasPrefix(q, "COMMIT") {
			return
		}
		once.Do(func() {
			go func() {
				// One name in an already built range, one in a future range, and one
				// beyond the builder's original high-water mark must all be repaired.
				fs := []protocol.FileInfo{genFile("name-000000", 0, 1), genFile("name-019999", 0, 2), genFile("added-during-build", 0, 3)}
				for i := range fs {
					fs[i].Size = 777
					fs[i].Version = fs[i].Version.Update(1)
				}
				err := fdb.Update(protocol.LocalDeviceID, fs, db.UpdateOptions{})
				updated.Store(err == nil)
				done <- err
			}()
			time.Sleep(5 * time.Millisecond)
		})
	}
	fdb.buildNeedTracking(gen)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update failed to interleave")
	}
	if !fdb.needBuilt.Load() {
		t.Fatal("build did not finish")
	}
	if !interleaved.Load() {
		t.Error("no Update completed between range chunks")
	}
	p.mu.Lock()
	violations := append([]string(nil), p.violations...)
	p.mu.Unlock()
	if len(violations) > 0 {
		t.Errorf("unlocked writes=%d, first=%s", len(violations), violations[0])
	}
	mu.Lock()
	observedRangeStatements, observedCommits, observedMaxTx, observedMaxSQL, observedMaxRangeTx := rangeStatements, commits, maxTx, maxSQL, maxRangeTx
	mu.Unlock()
	if observedRangeStatements < 20 {
		t.Errorf("oversized range chunks: %d statements", observedRangeStatements)
	}
	if observedMaxTx > 2*time.Second {
		t.Errorf("transaction too long: %s", observedMaxTx)
	}
	// Compare all maintained remote counts to the pre-existing full-scan
	// oracle after the final locked journal drain, not only the built flag.
	for i := 0; i < devices; i++ {
		dev := protocol.DeviceID{byte(i + 2)}
		got, err := fdb.needTotalsFromDetail(dev)
		if err != nil {
			t.Fatal(err)
		}
		want, err := fdb.needSizeRemote(dev)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("device %d maintained=%v oracle=%v", i, got, want)
		}
	}
	var journal int
	if err := fdb.sql.Get(&journal, `SELECT COUNT(*) FROM need_build_journal`); err != nil {
		t.Fatal(err)
	}
	if journal != 0 {
		t.Fatalf("journal not drained: %d", journal)
	}
	t.Logf("need fixtureNames=%d remoteDevices=%d rangeStatements=%d commits=%d maxTransaction=%s maxRangeTransaction=%s maxRangeSQL=%s updateInterleaved=%v", names, devices, observedRangeStatements, observedCommits, observedMaxTx, observedMaxRangeTx, observedMaxSQL, interleaved.Load())
}
