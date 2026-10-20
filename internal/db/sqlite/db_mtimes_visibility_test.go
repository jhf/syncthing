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

// stopMtimeTimer disarms the latency timer so a test drives flushes itself.
func stopMtimeTimer(fdb *folderDB) {
	fdb.mtimes.mut.Lock()
	if fdb.mtimes.timer != nil {
		fdb.mtimes.timer.Stop()
		fdb.mtimes.timer = nil
	}
	fdb.mtimes.mut.Unlock()
}

// waitMtimeSwapped waits until a flush has swapped the pending batch out and
// is waiting for the folder write lock (which the test holds).
func waitMtimeSwapped(t *testing.T, fdb *folderDB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		fdb.mtimes.mut.Lock()
		pending := len(fdb.mtimes.pending)
		fdb.mtimes.mut.Unlock()
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("flush did not reach the write lock")
		}
		runtime.Gosched()
	}
}

func openMtimeFolder(t *testing.T, folder, seed string) (*DB, *folderDB) {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PutMtime(folder, seed, time.Unix(1, 0), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	fdb, err := db.getFolderDB(folder, false)
	if err != nil {
		t.Fatal(err)
	}
	stopMtimeTimer(fdb)
	return db, fdb
}

// A mapping must stay visible while its flush waits for the folder write
// lock. Before the fix the flush swapped the pending map out and GetMtime
// returned nothing until the commit: on a whole-second filesystem a scan in
// that window saw the physical timestamp and minted a receiver version of
// unchanged sender content.
func TestMtimeBatcherInFlightReadVisibility(t *testing.T) {
	const folder, name = "receiver", "lib.rs"
	db, fdb := openMtimeFolder(t, folder, "seed")
	disk := time.Unix(1793660546, 0)
	virtual := disk.Add(123456789 * time.Nanosecond)
	if err := db.PutMtime(folder, name, disk, virtual); err != nil {
		t.Fatal(err)
	}
	stopMtimeTimer(fdb)

	fdb.updateLock.Lock()
	release := sync.OnceFunc(fdb.updateLock.Unlock)
	done := make(chan struct{})
	go func() { fdb.mtimes.flush(); close(done) }()
	defer func() { release(); <-done }()
	waitMtimeSwapped(t, fdb)

	if gotDisk, gotVirtual := db.GetMtime(folder, name); !gotDisk.Equal(disk) || !gotVirtual.Equal(virtual) {
		t.Fatalf("mapping not visible while flush waits for the write lock: %s -> %s", gotDisk, gotVirtual)
	}
	release()
	<-done
	if gotDisk, gotVirtual := db.GetMtime(folder, name); !gotDisk.Equal(disk) || !gotVirtual.Equal(virtual) {
		t.Fatalf("committed mapping missing: %s -> %s", gotDisk, gotVirtual)
	}
}

// Two flushes waiting for the write lock at once must commit in the order
// they swapped out their batches, whichever goroutine wins the lock: the
// newer Put survives, and reads see it throughout.
func TestMtimeBatcherOverlappingFlushOrder(t *testing.T) {
	const folder, name = "receiver", "dir"
	db, fdb := openMtimeFolder(t, folder, "seed")
	older, newer := time.Unix(100, 0), time.Unix(200, 0)

	fdb.updateLock.Lock()
	release := sync.OnceFunc(fdb.updateLock.Unlock)
	var wg sync.WaitGroup
	defer func() { release(); wg.Wait() }()

	if err := db.PutMtime(folder, name, older, older); err != nil {
		t.Fatal(err)
	}
	stopMtimeTimer(fdb)
	wg.Add(1)
	go func() { defer wg.Done(); fdb.mtimes.flush() }()
	waitMtimeSwapped(t, fdb)

	if err := db.PutMtime(folder, name, newer, newer); err != nil {
		t.Fatal(err)
	}
	stopMtimeTimer(fdb)
	wg.Add(1)
	go func() { defer wg.Done(); fdb.mtimes.flush() }()
	waitMtimeSwapped(t, fdb)

	if got, _ := db.GetMtime(folder, name); !got.Equal(newer) {
		t.Fatalf("read during overlapping flushes = %v, want newer %v", got, newer)
	}
	release()
	wg.Wait()
	var ondisk int64
	if err := fdb.sql.Get(&ondisk, `SELECT ondisk FROM mtimes WHERE name = ?`, name); err != nil {
		t.Fatal(err)
	}
	if ondisk != newer.UnixNano() {
		t.Fatalf("committed ondisk = %v, want newer %v", time.Unix(0, ondisk), newer)
	}
}

// A failed flush must keep its operations visible and retry them, not
// discard them.
func TestMtimeBatcherFailedFlushRetained(t *testing.T) {
	const folder, name = "receiver", "kept"
	db, fdb := openMtimeFolder(t, folder, "seed")
	fdb.mtimes.flush() // commit the seed so the table exists with content
	stamp := time.Unix(300, 0)
	if err := db.PutMtime(folder, name, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	stopMtimeTimer(fdb)

	fdb.updateLock.Lock()
	_, err := fdb.sql.Exec(`ALTER TABLE mtimes RENAME TO mtimes_away`)
	fdb.updateLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fdb.mtimes.flush()
	fdb.mtimes.mut.Lock()
	armed := fdb.mtimes.timer != nil
	fdb.mtimes.mut.Unlock()
	if !armed {
		t.Fatal("failed flush did not arm a retry")
	}
	if got, _ := db.GetMtime(folder, name); !got.Equal(stamp) {
		t.Fatalf("read after failed flush = %v, want %v", got, stamp)
	}

	stopMtimeTimer(fdb)
	fdb.updateLock.Lock()
	_, err = fdb.sql.Exec(`ALTER TABLE mtimes_away RENAME TO mtimes`)
	fdb.updateLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fdb.mtimes.flush()
	var ondisk int64
	if err := fdb.sql.Get(&ondisk, `SELECT ondisk FROM mtimes WHERE name = ?`, name); err != nil {
		t.Fatalf("retained operation not committed by the next flush: %v", err)
	}
	if ondisk != stamp.UnixNano() {
		t.Fatalf("committed ondisk = %v, want %v", time.Unix(0, ondisk), stamp)
	}
}
