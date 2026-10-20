// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"testing"
	"time"
)

// The batcher must answer reads from its pending map before any flush.
func TestMtimeBatcherReadYourWrites(t *testing.T) {
	t.Parallel()

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	t0 := time.Now().Truncate(time.Second)
	t1 := t0.Add(1234567890)

	if err := db.PutMtime("foo", "a", t0, t1); err != nil {
		t.Fatal(err)
	}
	gt0, gt1 := db.GetMtime("foo", "a")
	if !gt0.Equal(t0) || !gt1.Equal(t1) {
		t.Fatalf("unflushed read: got %v, %v", gt0, gt1)
	}

	// A pending delete reads back as absent.
	if err := db.DeleteMtime("foo", "a"); err != nil {
		t.Fatal(err)
	}
	if gt0, gt1 := db.GetMtime("foo", "a"); !gt0.IsZero() || !gt1.IsZero() {
		t.Fatalf("pending delete: got %v, %v", gt0, gt1)
	}
}

// Reaching the op threshold must flush synchronously and atomically.
func TestMtimeBatcherThresholdFlush(t *testing.T) {
	t.Parallel()

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	t0 := time.Now().Truncate(time.Second)
	for i := 0; i < mtimeBatchMaxOps; i++ {
		if err := db.PutMtime("foo", fmt.Sprintf("file-%d", i), t0, t0); err != nil {
			t.Fatal(err)
		}
	}

	fdb, ok := db.folderDBs["foo"]
	if !ok {
		t.Fatal("folder DB not open")
	}
	fdb.mtimes.mut.Lock()
	pending := len(fdb.mtimes.pending)
	fdb.mtimes.mut.Unlock()
	if pending != 0 {
		t.Fatalf("pending after threshold flush = %d, want 0", pending)
	}

	// Spot-check persistence through the database (not just the map).
	gt0, _ := db.GetMtime("foo", "file-0")
	if !gt0.Equal(t0) {
		t.Fatalf("file-0 ondisk = %v, want %v", gt0, t0)
	}
	gt0, _ = db.GetMtime("foo", fmt.Sprintf("file-%d", mtimeBatchMaxOps-1))
	if !gt0.Equal(t0) {
		t.Fatalf("last file ondisk = %v, want %v", gt0, t0)
	}
}

// The latency timer must flush pending ops AND leave the batcher usable.
// Regression: flush() once returned without releasing the mutex, so the
// first timer-fired flush wedged every later operation (found by
// TestRequestRemoteRenameConflict, which sleeps past the timer latency).
func TestMtimeBatcherTimerFlush(t *testing.T) {
	t.Parallel()

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	t0 := time.Now().Truncate(time.Second)
	if err := db.PutMtime("foo", "timed", t0, t0); err != nil {
		t.Fatal(err)
	}

	fdb, ok := db.folderDBs["foo"]
	if !ok {
		t.Fatal("folder DB not open")
	}

	// Wait for the timer to fire, with margin.
	deadline := time.Now().Add(5 * time.Second)
	for {
		fdb.mtimes.mut.Lock()
		pending := len(fdb.mtimes.pending)
		fdb.mtimes.mut.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timer flush never happened")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The batcher must still accept and serve operations (mutex not wedged).
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := db.PutMtime("foo", "after-timer", t0, t0); err != nil {
			t.Error(err)
		}
		gt0, _ := db.GetMtime("foo", "timed")
		if !gt0.Equal(t0) {
			t.Errorf("timed ondisk = %v, want %v", gt0, t0)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("batcher wedged after timer flush")
	}
}

// A close must flush whatever is still pending so nothing is lost.
func TestMtimeBatcherCloseFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	t0 := time.Now().Truncate(time.Second)
	t1 := t0.Add(42)
	if err := db.PutMtime("foo", "a", t0, t1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	gt0, gt1 := db.GetMtime("foo", "a")
	if !gt0.Equal(t0) || !gt1.Equal(t1) {
		t.Fatalf("after reopen: got %v, %v, want %v, %v", gt0, gt1, t0, t1)
	}
}
