// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// barrierTestFolder builds a folder on a temp dir with a buffered watchChan
// that the test feeds synchronously, and no aggregator (watchFlush nil, so
// flush requests report unavailable and the barrier relies on fed batches).
func barrierTestFolder(t *testing.T) (*testModel, *folder, string) {
	t.Helper()
	w, fcfg := newDefaultCfgWrapper(t)
	dir := t.TempDir()
	fcfg.FilesystemType = config.FilesystemTypeBasic
	fcfg.Path = dir
	if _, err := w.Modify(func(cfg *config.Configuration) { cfg.SetFolder(fcfg) }); err != nil {
		t.Fatal(err)
	}
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	r, _ := m.folderRunners.Get(fcfg.ID)
	f := r.(*sendReceiveFolder).folder
	f.watchMut.Lock()
	f.watchChan = make(chan watchaggregator.Batch, 8)
	f.watchFlush = nil
	f.watchMut.Unlock()
	return m, f, dir
}

// waitForCanary waits for the barrier's canary file to appear on disk and
// returns its folder-relative name.
func waitForCanary(t *testing.T, ffs fs.Filesystem) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		names, err := ffs.DirNames(".")
		if err == nil {
			for _, n := range names {
				if strings.HasPrefix(n, fs.UnixTempPrefix+"barrier-") && strings.HasSuffix(n, ".tmp") {
					return n
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("canary file never appeared")
	return ""
}

// TestWatchBarrierWaitsForCanary pins the core property: the barrier must
// not pass on quietness or on unrelated events, only on observing its own
// canary. An unrelated batch arriving while the canary is still in flight
// must be scanned, not mistaken for the barrier.
//
// Teeth: a wall-clock/quiet-period implementation (or one returning after
// any batch) returns before the canary batch and the test fails at the
// "barrier passed without its canary" check.
func TestWatchBarrierWaitsForCanary(t *testing.T) {
	m, f, _ := barrierTestFolder(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() { done <- f.watchBarrier(ctx) }()

	canary := waitForCanary(t, f.Filesystem())

	// An unrelated event must be consumed and scanned, not end the barrier.
	writeFile(t, f.Filesystem(), "unrelated.txt", []byte("x"))
	f.watchChan <- watchaggregator.Batch{Paths: []string{"unrelated.txt"}}
	select {
	case <-done:
		t.Fatal("barrier passed without its canary")
	case <-time.After(250 * time.Millisecond):
	}

	// The canary's own event passes the barrier.
	f.watchChan <- watchaggregator.Batch{Paths: []string{canary}}
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("barrier reported failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier did not pass after canary event")
	}

	// Both batches were scanned: the unrelated file is in the index.
	if _, ok := liveLocalNames(t, m.model, f.folderID)["unrelated.txt"]; !ok {
		t.Fatal("unrelated.txt not scanned before barrier passed")
	}
	// The canary itself is a temp file: never indexed, and cleaned up.
	if _, ok := liveLocalNames(t, m.model, f.folderID)[canary]; ok {
		t.Fatal("canary leaked into the index")
	}
	if _, err := f.Filesystem().Lstat(canary); !fs.IsNotExist(err) {
		t.Fatal("canary not removed after barrier")
	}
}

// TestWatchBarrierScansPreSnapshotEvent pins the pairing invariant: an event
// generated before the snapshot but still in flight must be scanned before
// the barrier passes (FIFO: it arrives before the canary's event).
//
// Teeth: if the barrier passed without waiting for delivery order (e.g. on
// the flush ack alone), "pre-snapshot.txt" would be missing from the index
// when pairing proceeds and the test fails.
func TestWatchBarrierScansPreSnapshotEvent(t *testing.T) {
	m, f, _ := barrierTestFolder(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// The change exists on disk before the "snapshot" but its watch event
	// is still in flight.
	writeFile(t, f.Filesystem(), "pre-snapshot.txt", []byte("x"))

	done := make(chan bool, 1)
	go func() { done <- f.watchBarrier(ctx) }()
	canary := waitForCanary(t, f.Filesystem())

	// In-flight pre-snapshot event first, then the canary (FIFO order).
	f.watchChan <- watchaggregator.Batch{Paths: []string{"pre-snapshot.txt"}}
	f.watchChan <- watchaggregator.Batch{Paths: []string{canary}}

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("barrier reported failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier did not pass after canary event")
	}
	if _, ok := liveLocalNames(t, m.model, f.folderID)["pre-snapshot.txt"]; !ok {
		t.Fatal("pre-snapshot change not in the index when the barrier passed")
	}
}

// TestRefreshBtrfsBaselineGates pins the runtime refresh preconditions:
// without a confirmed-eligible btrfs setup and a live watch, the refresh
// must not attempt a pairing, must not write the marker, and must not
// advance the refresh timestamp.
//
// Teeth: without the gates the refresh would attempt a pairing (observable
// here as a failed snapshot on a non-btrfs test dir, still not writing the
// marker) or would advance lastBtrfsRefresh without a successful pairing.
func TestRefreshBtrfsBaselineGates(t *testing.T) {
	_, f, _ := barrierTestFolder(t)
	ctx := t.Context()

	// Ineligible: btrfsOK false.
	f.refreshBtrfsBaseline(ctx)
	if !f.lastBtrfsRefresh.IsZero() {
		t.Fatal("refresh timestamp advanced without a successful pairing")
	}
	if _, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err == nil {
		t.Fatal("marker written without a successful pairing")
	}

	// Eligible setup but no aggregator (watchFlush nil): pairing cannot
	// start, so no marker, no timestamp.
	f.btrfsOK = true
	f.FSWatcherEnabled = true
	f.refreshBtrfsBaseline(ctx)
	if !f.lastBtrfsRefresh.IsZero() {
		t.Fatal("refresh timestamp advanced though pairing never ran")
	}
	if _, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err == nil {
		t.Fatal("marker written though pairing never ran")
	}
}

// The post-replay re-arm must keep retrying while the aggregator is still
// in its setup pass, then give up cleanly at the bound: pending clears,
// and no refresh timestamp or marker is recorded without a successful
// pairing. Observed on yarlung 2026-10-16: patch/19's original inline
// pairing failed "watch not running" 15s after boot, leaving the next
// restart uncovered (rescanIntervalS=86400, so the full-scan refresh was
// a day away).
//
// Teeth: with the inline pairing this handler does not exist; with a
// handler that never gives up the bound assertion fails.
func TestBtrfsRearmRetriesThenGivesUp(t *testing.T) {
	old := watchFlushTimeout
	t.Cleanup(func() { watchFlushTimeout = old })
	watchFlushTimeout = time.Millisecond

	_, f, _ := barrierTestFolder(t)
	ctx := t.Context()

	// Simulate the replay path arming the retry.
	f.btrfsRearmPending = true
	f.btrfsRearmTries = 0

	// No aggregator (watchFlush unset): every pairing attempt fails.
	for i := 1; i <= btrfsRearmMaxTries; i++ {
		f.btrfsRearmTimerFired(ctx)
		if i < btrfsRearmMaxTries && !f.btrfsRearmPending {
			t.Fatalf("retry %d: pending cleared before the bound", i)
		}
	}
	if f.btrfsRearmPending {
		t.Fatal("pending still set after btrfsRearmMaxTries failures")
	}
	if f.btrfsRearmTries != btrfsRearmMaxTries {
		t.Fatalf("tries = %d, want %d", f.btrfsRearmTries, btrfsRearmMaxTries)
	}
	if !f.lastBtrfsRefresh.IsZero() {
		t.Fatal("refresh timestamp recorded without a successful pairing")
	}
	if _, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err == nil {
		t.Fatal("marker written without a successful pairing")
	}
}
