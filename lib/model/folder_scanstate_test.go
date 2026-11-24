// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

// stateDirtyingPuller simulates a pull that ran: the real puller moves the
// folder state to FolderSyncPreparing/FolderSyncing while it works.
type stateDirtyingPuller struct {
	f       *folder
	calls   atomic.Int32
	replace puller
}

func (p *stateDirtyingPuller) pull(ctx context.Context) (bool, error) {
	p.calls.Add(1)
	p.f.setState(FolderSyncPreparing)
	return true, nil
}

// TestSweepRestoresScanningStateAfterPull pins the fix for the stuck
// "Preparing to Sync" label: a pull serviced inside the deleted-files sweep
// leaves the folder state at sync-preparing, and unlike the walk chunk loop
// (which re-sets FolderScanning at the top of every chunk iteration) the
// sweep has no per-item restore, so the GUI reported "Preparing to Sync" for
// the rest of the sweep - observed on the Mac as a label stuck 862s with
// needFiles=0 while all work was scan work (2026-10-09).
//
// The sweep is driven directly, holding the I/O token like scanSubdirs does.
// Teeth: the fake puller records that it ran and dirties the state, so
// without the restore the final state is sync-preparing and the test fails;
// a pull that never ran cannot fake a pass either (calls is asserted).
func TestSweepRestoresScanningStateAfterPull(t *testing.T) {
	// Not parallel: mutates a package-level budget.
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()
	scanChunkBudget = 0 // service at every sweep item

	m, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()

	must(t, ffs.MkdirAll("a", 0o755))
	writeFile(t, ffs, "a/file1", []byte("a"))
	must(t, f.scanSubdirs(ctx, []string{"a"})) // index it, so the sweep has a row to iterate

	// Something must be needed or folder.pull returns before touching the
	// state. A file with a block no peer can serve passes the CountNeed gate;
	// the fake puller never looks at the need.
	must(t, m.sdb.Update(f.ID, device1, []protocol.FileInfo{{
		Name:     "needed",
		Size:     5,
		Version:  protocol.Vector{}.Update(device1.Short()),
		Sequence: 1,
		Blocks:   []protocol.BlockInfo{{Hash: []byte("01234567890123456789012345678901"), Size: 5}},
	}}))
	if c, err := f.db.CountNeed(f.ID, protocol.LocalDeviceID); err != nil || c.TotalItems() == 0 {
		t.Fatalf("fixture broken: nothing is needed (count=%v, err=%v)", c, err)
	}

	fake := &stateDirtyingPuller{f: f.folder, replace: f.puller}
	f.puller = fake
	defer func() { f.puller = fake.replace }()

	// SchedulePull's nonblocking send matters: setup may have left a pull
	// pending in the 1-buffered channel already.
	f.SchedulePull()

	// The sweep is normally called holding an I/O token; its budget branch
	// hands the token back around the servicing, so take one here.
	must(t, f.ioLimiter.TakeWithContext(ctx, 1))
	_, err := f.scanSubdirsDeletedAndIgnored(ctx, []string{"a"}, f.newScanBatch())
	f.ioLimiter.Give(1)
	must(t, err)

	if fake.calls.Load() == 0 {
		t.Fatal("the queued pull was never serviced during the sweep")
	}
	if state, _, _ := f.getState(); state != FolderScanning {
		t.Errorf("state after sweep = %s, want %s (the serviced pull left its state behind)", state, FolderScanning)
	}
}

// TestServiceQueuedWatchSkipsNestedFullScan pins the guard against nested
// full-folder scans: while a full-tree scan is running (fullScanActive
// set), a full-folder watch batch serviced between chunks must be skipped -
// running it would walk the whole tree a second time inside the first
// scan's chunk budget (observed doubling full scans to 10-15min during the
// 2026-10-10 fseventsd drop-flag storm). A targeted batch must still be
// serviced. Teeth: without the guard the ["."] batch scans and the new
// file is indexed; with it, only the targeted batch's file is.
func TestServiceQueuedWatchSkipsNestedFullScan(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()

	// The fixture's folder never served, so watchChan is nil; give it a
	// buffered stand-in so the batch can be queued deterministically.
	f.watchChan = make(chan watchaggregator.Batch, 1)

	must(t, ffs.MkdirAll("skipme", 0o755))
	writeFile(t, ffs, "skipme/newfile", []byte("x"))
	must(t, ffs.MkdirAll("target", 0o755))
	writeFile(t, ffs, "target/newfile", []byte("x"))

	f.fullScanActive.Store(true)
	defer f.fullScanActive.Store(false)

	f.watchChan <- watchaggregator.Batch{Paths: []string{"."}}
	_, err := f.serviceQueuedWatch(ctx)
	must(t, err)

	if _, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, "skipme/newfile"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Error("full-folder watch batch was serviced during a running full scan")
	}

	f.watchChan <- watchaggregator.Batch{Paths: []string{"target"}}
	_, err = f.serviceQueuedWatch(ctx)
	must(t, err)
	// No scan coverage is installed here, so the targeted batch is deferred
	// (never scanned nested); the pending scan indexes it.
	must(t, f.servicePendingWatch(ctx))

	if _, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, "target/newfile"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("targeted watch batch dequeued during a running full scan was not scanned by the pending scan")
	}
}

// TestServiceQueuedWatchCountsRootBatches pins the consumer-side accounting
// of full-folder watch batches (2026-11-24 Mac: a root batch dequeued
// between the chunks of a subtree scan ran a full walk and sweep nested
// inside it, unlogged, for 2 h 38 min). A root batch dequeued between
// chunks while no full-tree scan runs is deferred to the pending set and
// counted as "between-chunks-deferred"; during a full-tree scan the guard
// skips it and it is counted as "between-chunks-skipped".
func TestServiceQueuedWatchCountsRootBatches(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	ctx := t.Context()
	f.watchChan = make(chan watchaggregator.Batch, 1)

	deferred := metricFolderRootWatchBatches.WithLabelValues(f.folderID, "between-chunks-deferred")
	skipped := metricFolderRootWatchBatches.WithLabelValues(f.folderID, "between-chunks-skipped")
	deferred0, skipped0 := testutil.ToFloat64(deferred), testutil.ToFloat64(skipped)

	f.fullScanActive.Store(false)
	f.watchChan <- watchaggregator.Batch{Paths: []string{"."}}
	_, err := f.serviceQueuedWatch(ctx)
	must(t, err)
	if got := testutil.ToFloat64(deferred) - deferred0; got != 1 {
		t.Errorf("deferred root batches = %v, want 1", got)
	}
	if f.watchPending == nil || !f.watchPending.set.Root() {
		t.Fatal("root batch not deferred to the pending set")
	}
	f.watchPending = nil

	f.fullScanActive.Store(true)
	defer f.fullScanActive.Store(false)
	f.watchChan <- watchaggregator.Batch{Paths: []string{"."}}
	_, err = f.serviceQueuedWatch(ctx)
	must(t, err)
	if got := testutil.ToFloat64(skipped) - skipped0; got != 1 {
		t.Errorf("skipped root batches = %v, want 1", got)
	}

	// A targeted batch is not a root batch.
	f.watchChan <- watchaggregator.Batch{Paths: []string{"target"}}
	_, err = f.serviceQueuedWatch(ctx)
	must(t, err)
	if got := testutil.ToFloat64(deferred) - deferred0; got != 1 {
		t.Errorf("deferred root batches after a targeted batch = %v, want still 1", got)
	}
}

// TestServiceQueuedWatchNeverNests pins that a batch dequeued between scan
// chunks is never scanned there: measured 2026-11-24, a 41-path batch
// nested inside a subtree scan took 13 min and a root batch nested inside
// one held the folder in "scanning" for 2 h 38 min. The batch is deferred
// (nothing indexed yet, the Serve loop is signalled) and the pending scan
// indexes it and persists its watermark. Teeth: before, serviceQueuedWatch
// called scanSubdirs itself and target/newfile was indexed immediately.
func TestServiceQueuedWatchNeverNests(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()
	f.watchChan = make(chan watchaggregator.Batch, 1)

	must(t, ffs.MkdirAll("target", 0o755))
	writeFile(t, ffs, "target/newfile", []byte("x"))
	// The fixture's full scan may have persisted the live journal position.
	base := f.lastReplayWatermark.Load()

	f.watchChan <- watchaggregator.Batch{Paths: []string{"target"}, MaxEventID: base + 4242}
	ran, err := f.serviceQueuedWatch(ctx)
	must(t, err)
	if !ran {
		t.Fatal("queued batch not dequeued")
	}
	if _, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, "target/newfile"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("batch scanned nested inside the running scan's chunk boundary")
	}
	select {
	case <-f.watchPendingReady:
	default:
		t.Fatal("Serve loop not signalled about the deferred batch")
	}

	must(t, f.servicePendingWatch(ctx))
	if _, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, "target/newfile"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("deferred batch not scanned by the pending scan")
	}
	if got := f.lastReplayWatermark.Load(); got != base+4242 {
		t.Errorf("watermark = %d after the pending scan, want %d", got, base+4242)
	}
	if f.watchPending != nil {
		t.Error("pending set not cleared by a successful pending scan")
	}
}

// TestWatchBatchIntegratesAheadOfWalkFrontier pins integration: a batch
// whose paths the running walk has not reached yet is covered by that walk
// and not scanned again, while a batch behind the frontier (or outside the
// scanned subtree) is deferred. The integrated batch's watermark is
// persisted when the scan succeeds.
func TestWatchBatchIntegratesAheadOfWalkFrontier(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	ctx := t.Context()

	p := filepath.FromSlash
	base := f.lastReplayWatermark.Load()
	cover, prev := f.beginScanCoverage([]string{"d"})
	cover.walking = true
	cover.frontier = p("d/m")

	f.handleWatchBatchMidScan(ctx, watchaggregator.Batch{Paths: []string{p("d/z"), p("d/m/x")}, MaxEventID: base + 10})
	if f.watchPending != nil {
		t.Fatalf("paths ahead of the frontier deferred: %q", f.watchPending.set.Paths())
	}
	f.handleWatchBatchMidScan(ctx, watchaggregator.Batch{Paths: []string{p("d/a")}, MaxEventID: base + 11})
	f.handleWatchBatchMidScan(ctx, watchaggregator.Batch{Paths: []string{"e"}, MaxEventID: base + 12})
	if f.watchPending == nil {
		t.Fatal("paths behind the frontier / outside the subtree not deferred")
	}
	if got := f.watchPending.set.Paths(); len(got) != 2 || got[0] != p("d/a") || got[1] != "e" {
		t.Fatalf("pending = %q, want [d/a e]", got)
	}

	// The scan succeeds while deferred batches are pending: the integrated
	// watermark must not be persisted ahead of them.
	f.endScanCoverage(cover, prev, nil)
	if got := f.lastReplayWatermark.Load(); got != base {
		t.Fatalf("watermark moved %d -> %d ahead of pending batches", base, got)
	}
	if f.watchPending.maxID != base+12 {
		t.Fatalf("pending watermark = %d, want %d", f.watchPending.maxID, base+12)
	}
}

// TestFolderRegistersRootWatchBatchSeriesAtZero pins that a new folder
// exposes its root-watch-batch series at zero, so their absence from
// /metrics can no longer be mistaken for "no full-folder batch yet".
func TestFolderRegistersRootWatchBatchSeriesAtZero(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	mfs, err := prometheus.DefaultGatherer.Gather()
	must(t, err)
	found := make(map[string]bool)
	for _, mf := range mfs {
		if mf.GetName() != "syncthing_model_folder_root_watch_batches_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var folder, where string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "folder":
					folder = lp.GetValue()
				case "where":
					where = lp.GetValue()
				}
			}
			if folder == f.folderID {
				found[where] = true
			}
		}
	}
	for _, where := range []string{"serve", "between-chunks-skipped", "between-chunks-deferred", "pending"} {
		if !found[where] {
			t.Errorf("root watch batch series where=%q not registered for folder %q", where, f.folderID)
		}
	}
}
