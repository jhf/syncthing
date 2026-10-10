// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package model

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// pairingFakeHelper serves the helper protocol on a unix socket, recording
// every request. setbaseline can be told to fail. Unlike the fs package's
// sweep fake this one answers snap/setbaseline, which pairing needs.
type pairingFakeHelper struct {
	mu       sync.Mutex
	calls    []string
	failSet  bool
	failRm   bool
	sockPath string
}

func startPairingFakeHelper(t *testing.T) *pairingFakeHelper {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "helper.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	h := &pairingFakeHelper{sockPath: sock}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go h.serve(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
	})
	t.Setenv("SYNCTHING_BTRFS_SOCK", sock)
	return h
}

func (h *pairingFakeHelper) serve(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	req := strings.TrimSpace(string(buf[:n]))
	h.mu.Lock()
	h.calls = append(h.calls, req)
	failSet, failRm := h.failSet, h.failRm
	h.mu.Unlock()
	out, code := "", 0
	switch {
	case req == "status":
		out = "baseline=/home/.snapshots/syncthing/baseline"
	case strings.HasPrefix(req, "snap "):
		out = "snap=/home/.snapshots/syncthing/" + strings.TrimPrefix(req, "snap ")
	case strings.HasPrefix(req, "setbaseline "):
		if failSet {
			code = 1
		} else {
			out = "baseline=/home/.snapshots/syncthing/baseline"
		}
	case strings.HasPrefix(req, "rmsnap "):
		if failRm {
			code = 1
		}
	default:
		code = 2
	}
	fmt.Fprintf(conn, "%s\x00status=%d", out, code)
}

func (h *pairingFakeHelper) got(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

// drainFlush acks every watch flush request instantly.
func drainFlush(f *folder) {
	ch := make(chan chan struct{}, 16)
	f.watchMut.Lock()
	f.watchFlush = ch
	f.watchMut.Unlock()
	go func() {
		for ack := range ch {
			close(ack)
		}
	}()
}

// feedCanary watches for the barrier's canary file and delivers its watch
// batch, completing the barrier.
func feedCanary(t *testing.T, f *folder) {
	t.Helper()
	go func() {
		name := waitForCanary(t, f.Filesystem())
		f.watchChan <- watchaggregator.Batch{Paths: []string{name}}
	}()
}

// A successful pairing must NOT rmsnap the staging snapshot: setbaseline
// renamed it to the baseline, so the name no longer exists and the removal
// would be a false-failure WRN (helper exit 1 on a missing path). Observed
// live on yarlung 2026-10-21 (dev.145/146): every successful pairing logged
// "Failed to remove btrfs pairing snapshot ... rmsnap ... status 1".
//
// Teeth: revert `consumed` and the rmsnap assertion fails; the pre-fix code
// also registered the defer before the snapshot existed.
func TestPairBtrfsBaselineDoesNotRemoveConsumedSnapshot(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	drainFlush(f)
	feedCanary(t, f)

	ctx := t.Context()
	if !f.pairBtrfsBaseline(ctx, "post-replay") {
		t.Fatal("pairing failed against fake helper")
	}
	if !helper.got("snap post-replay-") {
		t.Errorf("helper never got snap request: %v", helper.calls)
	}
	if !helper.got("setbaseline post-replay-") {
		t.Errorf("helper never got setbaseline request: %v", helper.calls)
	}
	if helper.got("rmsnap post-replay-") {
		t.Errorf("consumed snapshot was removed (false-failure path): %v", helper.calls)
	}
}

// A FAILED pairing (setbaseline refused) must still rmsnap the staging
// snapshot: it was created, never consumed, and would leak otherwise.
//
// Teeth: dropping the deferred removal entirely passes the success test
// above but fails this one.
func TestPairBtrfsBaselineRemovesSnapshotOnFailure(t *testing.T) {
	helper := startPairingFakeHelper(t)
	helper.failSet = true
	_, f, _ := barrierTestFolder(t)
	drainFlush(f)
	feedCanary(t, f)

	ctx := t.Context()
	if f.pairBtrfsBaseline(ctx, "post-replay") {
		t.Fatal("pairing succeeded despite failing setbaseline")
	}
	// The deferred removal runs when pairBtrfsBaseline returns.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if helper.got("rmsnap post-replay-") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("failed pairing leaked its staging snapshot: %v", helper.calls)
}

// markReady installs a closed watch-ready channel (a running watch
// session) on a folder whose aggregator the test fakes.
func markReady(f *folder) {
	ready := make(chan struct{})
	close(ready)
	f.watchMut.Lock()
	f.watchReady = ready
	f.watchMut.Unlock()
}

// The periodic runtime pairing keeps a recent baseline so that a restart
// whose shutdown pairing fails (2026-11-25 02:00 on yarlung: the helper
// slice stopped first during an unattended-upgrades reboot) diffs against
// it instead of full-scanning. It pairs only while the index is provably
// complete and the watch is ready, and re-arms its timer either way.
//
// Teeth: without btrfsPeriodicPair nothing pairs between boots, so the
// marker assertion fails; without the fullScanOK gate the "not provably
// complete" round writes a marker.
func TestBtrfsPeriodicPairing(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	ctx := t.Context()
	f.btrfsOK = true
	f.FSWatcherEnabled = true
	drainFlush(f)
	markReady(f)

	// Index not provably complete (e.g. the watch died): no pairing.
	f.fullScanOK.Store(false)
	f.btrfsPeriodicPair(ctx)
	if helper.got("snap ") {
		t.Fatalf("paired while the index was not provably complete: %v", helper.calls)
	}

	// Watch not ready yet: no pairing, and no 15 s flush timeout either.
	f.fullScanOK.Store(true)
	f.watchMut.Lock()
	f.watchReady = make(chan struct{})
	f.watchMut.Unlock()
	start := time.Now()
	f.btrfsPeriodicPair(ctx)
	if helper.got("snap ") {
		t.Fatalf("paired before the watch was ready: %v", helper.calls)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("not-ready skip took %v (waited for the flush timeout?)", d)
	}

	// Ready and complete: pairs, writes the marker.
	markReady(f)
	feedCanary(t, f)
	f.btrfsPeriodicPair(ctx)
	if !helper.got("setbaseline runtime-") {
		t.Fatalf("periodic pairing did not set a baseline: %v", helper.calls)
	}
	if _, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err != nil {
		t.Fatalf("periodic pairing wrote no marker: %v", err)
	}
	if f.lastBtrfsRefresh.IsZero() {
		t.Fatal("periodic pairing did not record its time")
	}

	// A pending post-replay re-arm owns the pairing: the periodic one
	// stands down (and still re-arms its own timer).
	f.lastBtrfsRefresh = time.Time{}
	f.btrfsRearmPending = true
	before := len(helper.calls)
	f.btrfsPeriodicPair(ctx)
	helper.mu.Lock()
	after := len(helper.calls)
	helper.mu.Unlock()
	if after != before {
		t.Fatalf("periodic pairing ran while a re-arm was pending: %v", helper.calls[before:])
	}
	select {
	case <-f.btrfsPairTimer.C:
		t.Fatal("periodic timer fired immediately; want re-armed for btrfsPairInterval")
	default:
	}
	if !f.btrfsPairTimer.Stop() {
		t.Fatal("periodic timer not re-armed")
	}
}

// The shutdown pairing runs under the folder's ONE shutdown budget, not its
// own 90 s: a barrier that never completes (canary never seen) must end at
// the budget, and the staging snapshot must still be removed although the
// budget's context is spent. Before, the pairing's private 90 s deadline
// outran the supervisors' stop timeout and the process exited mid-barrier.
//
// Teeth: restore the private 90 s context in shutdownBtrfsBaseline and the
// call outlives the 5 s bound; drop the detached cleanup context and the
// rmsnap never reaches the helper.
func TestShutdownPairingHonoursBudget(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	old := folderShutdownBudget
	t.Cleanup(func() { folderShutdownBudget = old })
	folderShutdownBudget = 300 * time.Millisecond
	f.btrfsOK = true
	f.FSWatcherEnabled = true
	f.fullScanOK.Store(true)
	f.done = make(chan struct{}) // barrierTestFolder's Serve already closed it
	drainFlush(f)
	markReady(f)
	// No feedCanary: the barrier never completes.

	returned := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		f.shutdownWork()
		returned <- time.Since(start)
	}()
	select {
	case d := <-returned:
		if d < folderShutdownBudget {
			t.Fatalf("shutdown work returned after %v, before its %v budget: barrier did not wait", d, folderShutdownBudget)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("shutdown pairing ignored the %v shared budget", folderShutdownBudget)
	}
	if !helper.got("snap shutdown-") {
		t.Fatalf("pairing never snapshotted: %v", helper.calls)
	}
	if helper.got("setbaseline ") {
		t.Fatalf("paired without a completed barrier: %v", helper.calls)
	}
	if !helper.got("rmsnap shutdown-") {
		t.Fatalf("staging snapshot leaked after the budget ran out: %v", helper.calls)
	}
}

// A runtime pairing that finds deferred watch batches must not scan them
// itself: it stands down, marks itself due, and pairs once the Serve loop
// has scanned them. On yarlung 2026-11-27 16:28:34 the pairing right after
// a 2h2m full scan found 28 deferred directories and failed drain-pending
// at 2m1.343s against its 2-minute deadline; the Serve loop then scanned
// the same directories in 7m33s with changes=0.
//
// Teeth: drain the pending set inside the runtime pairing (the old
// drain-pending path) and the first round snapshots with batches pending.
func TestRuntimePairingWaitsForDeferredBatches(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	ctx := t.Context()
	f.btrfsOK = true
	f.FSWatcherEnabled = true
	f.fullScanOK.Store(true)
	drainFlush(f)
	markReady(f)
	f.deferWatchPaths([]string{"pending-dir"}, 7)

	f.btrfsPeriodicPair(ctx)
	if helper.got("snap ") {
		t.Fatalf("runtime pairing ran with deferred watch batches pending: %v", helper.calls)
	}
	if f.watchPending == nil {
		t.Fatal("runtime pairing scanned the deferred watch batches itself")
	}

	// Still pending: the due pairing keeps waiting.
	f.runDueBtrfsPairing(ctx)
	if helper.got("snap ") {
		t.Fatalf("due pairing ran with deferred watch batches pending: %v", helper.calls)
	}

	// The Serve loop scans the backlog; the due pairing then runs.
	if err := f.servicePendingWatch(ctx); err != nil {
		t.Fatal(err)
	}
	feedCanary(t, f)
	f.runDueBtrfsPairing(ctx)
	if !helper.got("setbaseline runtime-") {
		t.Fatalf("due pairing did not run once nothing was pending: %v", helper.calls)
	}
	if f.lastBtrfsRefresh.IsZero() {
		t.Fatal("due pairing did not record its time")
	}
}

// The runtime pairing runs without a deadline, so the barrier must end on
// its own when the canary stops being on its way: the watch died
// (monitorWatch clears fullScanOK and stops the aggregator).
//
// Teeth: without the watch-death check the barrier waits for a canary
// that never comes, past the test's bound (before: until the old 2-minute
// runtime deadline).
func TestPairingBarrierEndsOnWatchDeath(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	f.btrfsOK = true
	f.FSWatcherEnabled = true
	f.fullScanOK.Store(true)
	drainFlush(f)
	markReady(f)
	// No feedCanary: the watch "dies" once the barrier has started.
	go func() {
		waitForCanary(t, f.Filesystem())
		f.fullScanOK.Store(false)
	}()

	returned := make(chan struct{})
	go func() {
		f.refreshBtrfsBaseline(t.Context())
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("barrier kept waiting after the watch died")
	}
	if helper.got("setbaseline ") {
		t.Fatalf("paired although the watch died during the barrier: %v", helper.calls)
	}
	if !f.lastBtrfsRefresh.IsZero() {
		t.Fatal("failed pairing recorded a refresh")
	}
}

// A full-folder watch batch dequeued inside a pairing is the Serve loop's
// work, not the pairing's: the aggregator dropped every event it tracked
// when it escalated, so the canary may never come, and the full walk took
// 2h2m on yarlung. The pairing defers it and fails.
//
// Teeth: scan the root batch in the barrier (the old path) and the pending
// set stays empty.
func TestPairingDefersRootWatchBatch(t *testing.T) {
	helper := startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	// Bounded so the old path (scan the root batch, then wait for a canary
	// the escalation dropped) fails here instead of hanging.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	drainFlush(f)
	go func() {
		waitForCanary(t, f.Filesystem())
		f.watchChan <- watchaggregator.Batch{Paths: []string{"."}, MaxEventID: 9}
	}()

	if f.pairBtrfsBaseline(ctx, "runtime") {
		t.Fatal("pairing succeeded across a full-folder escalation")
	}
	if helper.got("setbaseline ") {
		t.Fatalf("set a baseline across a full-folder escalation: %v", helper.calls)
	}
	if f.watchPending == nil || !f.watchPending.set.Root() {
		t.Fatal("full-folder batch not deferred to the Serve loop")
	}
	if f.watchPending.maxID != 9 {
		t.Fatalf("deferred watermark %d, want 9", f.watchPending.maxID)
	}
}
