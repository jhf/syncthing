// Copyright (C) 2026 The Syncthing Authors.
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

// A deterministic slow-storage window is injected after SQLite has actually
// executed the first range INSERT, while its real immediate transaction still
// holds the writer lock. Before 64/65 this exhausts the 50 ms SQLite timeout.
// Afterwards every competitor waits in Go, not in SQLite. No fake SQL or
// SQLite errors are injected. The same test compiles on base 371de9964.
func TestSingleWriterNoBusy(t *testing.T) {
	sdb, fdb := needWriterFixture(t, 2000, 4)
	// Ensure the real retirement-queue GC has work, not just an empty scan.
	first := genFile("retired-blocks", 1, 3001)
	if err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{first}, db.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	second := genFile("retired-blocks", 2, 3002)
	second.Version = second.Version.Update(1)
	if err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{second}, db.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := fdb.sql.Get(&pending, `SELECT COUNT(*) FROM gc_retired_blocklists`); err != nil {
		t.Fatal(err)
	}
	if pending == 0 {
		t.Fatal("fixture did not seed retirement GC work")
	}
	p := attachWriterProbe(t, fdb.baseDB)
	mainProbe := attachWriterProbe(t, sdb.baseDB)
	held, release := make(chan struct{}), make(chan struct{})
	var holdOnce sync.Once
	var gcWrote atomic.Bool
	p.before = func(q string) {
		if strings.Contains(q, "DELETE FROM gc_retired_blocklists") {
			gcWrote.Store(true)
		}
	}
	p.after = func(q string, _ time.Duration) {
		if strings.Contains(q, "CROSS JOIN devices d") {
			holdOnce.Do(func() { close(held); <-release })
		}
	}
	fdb.needBuild.mu.Lock()
	fdb.needBuild.running = true
	gen := fdb.needBuild.gen
	fdb.needBuild.mu.Unlock()
	buildDone := make(chan struct{})
	go func() { fdb.buildNeedTracking(gen); close(buildDone) }()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		close(release)
		<-buildDone
		t.Fatal("need range never acquired SQLite writer lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var operations atomic.Int64
	errs := make(chan error, 64)
	run := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				errs <- fmt.Errorf("%s: %w", name, err)
			}
			operations.Add(1)
		}()
	}
	run("folder tidy", func() error { return runProbedTidy(ctx, fdb.baseDB) })
	run("main tidy", func() error { return runProbedTidy(ctx, sdb.baseDB) })
	run("queue GC", func() error { return garbageCollectBlocklistsAndBlocks(ctx, fdb) })
	run("live GC", func() error {
		fdb.updateLock.Lock()
		defer fdb.updateLock.Unlock()
		if err := garbageCollectOldDeletedLocked(ctx, fdb); err != nil {
			return err
		}
		return garbageCollectNamesAndVersions(ctx, fdb)
	})
	for worker := 0; worker < 4; worker++ {
		run(fmt.Sprintf("Updates %d", worker), func() error {
			for i := 0; i < 8; i++ {
				fi := genFile(fmt.Sprintf("concurrent-%d-%d", worker, i), 0, 4000+worker*8+i)
				if err := fdb.Update(protocol.LocalDeviceID, []protocol.FileInfo{fi}, db.UpdateOptions{}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	run("mtimes", func() error {
		for i := 0; i < 8; i++ {
			if err := fdb.PutMtime(fmt.Sprintf("mtime-%d", i), time.Unix(1, 0), time.Unix(2, 0)); err != nil {
				return err
			}
		}
		fdb.mtimes.flush()
		return nil
	})
	run("folder KV and index IDs", func() error {
		for i := 0; i < 8; i++ {
			if err := fdb.PutKV("writer-test", []byte{byte(i)}); err != nil {
				return err
			}
			if err := fdb.SetIndexID(protocol.DeviceID{2}, protocol.IndexID(i+1)); err != nil {
				return err
			}
		}
		return fdb.DeleteKV("writer-test")
	})
	run("main KV and registry", func() error {
		for i := 0; i < 8; i++ {
			if err := sdb.PutKV("writer-test", []byte{byte(i)}); err != nil {
				return err
			}
		}
		if _, err := sdb.getFolderDB("registry-test", true); err != nil {
			return err
		}
		if err := sdb.DropFolder("registry-test"); err != nil {
			return err
		}
		return sdb.DeleteKV("writer-test")
	})
	// All competitors are running against a real open SQLite writer. Release
	// after 3x the per-connection timeout, regardless of mutex wait duration.
	const hold = 150 * time.Millisecond
	time.Sleep(hold)
	close(release)
	wg.Wait()
	<-buildDone
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !fdb.needBuilt.Load() {
		t.Error("need build did not finish")
	}
	if !gcWrote.Load() {
		t.Error("retirement GC did not execute its write path")
	}
	p.mu.Lock()
	folderBusy, folderUnlocked := len(p.busyErrors), len(p.violations)
	folderBusyErrors := append([]string(nil), p.busyErrors...)
	p.mu.Unlock()
	mainProbe.mu.Lock()
	mainBusy, mainUnlocked := len(mainProbe.busyErrors), len(mainProbe.violations)
	mainProbe.mu.Unlock()
	t.Logf("busy_timeout_ms=50 forcedWriterHold=%s operations=%d folderBusy=%d mainBusy=%d folderUnlocked=%d mainUnlocked=%d", hold, operations.Load(), folderBusy, mainBusy, folderUnlocked, mainUnlocked)
	if folderBusy+mainBusy != 0 {
		t.Errorf("SQLITE_BUSY observed: folder=%v mainCount=%d", folderBusyErrors, mainBusy)
	}
	if folderUnlocked+mainUnlocked != 0 {
		t.Errorf("unlocked production writer entries: folder=%d main=%d", folderUnlocked, mainUnlocked)
	}
}

func TestWriterProbeDetectsUnheldEntries(t *testing.T) {
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	p := attachWriterProbe(t, sdb.baseDB)
	if _, err := sdb.sql.Exec(`INSERT INTO kv(key,value) VALUES ('unheld-exec',x'01')`); err != nil {
		t.Fatal(err)
	}
	var key string
	if err := sdb.sql.Get(&key, `INSERT INTO kv(key,value) VALUES ('unheld-query',x'02') RETURNING key`); err != nil {
		t.Fatal(err)
	}
	tx, err := sdb.sql.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := sdb.PutKV("held", []byte{3}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.violations) != 3 {
		t.Fatalf("probe must detect Exec, Query RETURNING and Begin, got %v", p.violations)
	}
	t.Logf("probe detected unheld Exec/Query-RETURNING/Begin=%d; protected PutKV accepted", len(p.violations))
}
