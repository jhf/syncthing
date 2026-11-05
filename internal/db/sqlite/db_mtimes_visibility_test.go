// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMtimeBatcherInFlightReadVisibility pins the current visibility hole,
// not desired behavior. A reader loses a previously visible mapping while a
// flush waits to commit, without a crash, failed transaction, or file edit.
// The model's TestPostPullScanTimestampPrecision separately exercises what a
// missing mapping does to a real receiver finish + scan + index update.
func TestMtimeBatcherInFlightReadVisibility(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	const folder, name = "receiver", "lib.rs"
	disk := time.Unix(1793660546, 0)
	virtual := disk.Add(123456789 * time.Nanosecond)
	if err := db.PutMtime(folder, name, disk, virtual); err != nil {
		t.Fatal(err)
	}
	fdb, err := db.getFolderDB(folder, false)
	if err != nil {
		t.Fatal(err)
	}
	// Disarm the latency timer. The test drives the exact production flush
	// synchronously up to a real DB write lock, without timing a two-second race.
	fdb.mtimes.mut.Lock()
	if fdb.mtimes.timer != nil {
		fdb.mtimes.timer.Stop()
		fdb.mtimes.timer = nil
	}
	fdb.mtimes.mut.Unlock()
	gotDisk, gotVirtual := db.GetMtime(folder, name)
	if !gotDisk.Equal(disk) || !gotVirtual.Equal(virtual) {
		t.Fatal("mapping not visible before flush")
	}
	fdb.updateLock.Lock()
	done := make(chan struct{})
	go func() { fdb.mtimes.flush(); close(done) }()
	// Always release the writer and join it before closing the DB, even if
	// an assertion fails. The deadline is only a hung-test guard, not a trigger.
	release := sync.OnceFunc(fdb.updateLock.Unlock)
	defer func() { release(); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		fdb.mtimes.mut.Lock()
		pending := len(fdb.mtimes.pending)
		fdb.mtimes.mut.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flush did not reach write lock")
		}
		runtime.Gosched()
	}
	gotDisk, gotVirtual = db.GetMtime(folder, name)
	if !gotDisk.IsZero() || !gotVirtual.IsZero() {
		t.Fatalf("current in-flight hole not reproduced: %s -> %s", gotDisk, gotVirtual)
	}
	t.Log("mapping visible before flush, absent while flush waits for DB write lock")
	// Let the same successful flush complete, then prove visibility returns.
	release()
	<-done
	gotDisk, gotVirtual = db.GetMtime(folder, name)
	if !gotDisk.Equal(disk) || !gotVirtual.Equal(virtual) {
		t.Fatalf("committed mapping missing: %s -> %s", gotDisk, gotVirtual)
	}
	t.Log("mapping visible again after successful commit")
}
