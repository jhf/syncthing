// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build darwin && !kqueue && cgo && !ios

package model

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
)

// drainWatchBatches mimics the folder Serve loop's watcher arm: scan each
// flushed batch and persist the journal watermark it carries.
func drainWatchBatches(ctx context.Context, f *folder) {
	for {
		select {
		case b := <-f.watchChan:
			if err := f.scanSubdirs(ctx, b.Paths); err == nil {
				f.persistWatchReplayWatermark(b.MaxEventID)
			}
		default:
			return
		}
	}
}

// TestWatchReplaySkipsInitialScan proves the restart path end to end: a
// folder whose previous run recorded a journal watermark starts watching
// from that position instead of walking the tree, and still indexes the
// changes that happened while it was down. Phase 1 doubles as the control:
// without a watermark the same code must do (and count) a full scan.
func TestWatchReplaySkipsInitialScan(t *testing.T) {
	w, fcfg := newDefaultCfgWrapper(t)
	dir := t.TempDir()
	fcfg.FilesystemType = config.FilesystemTypeBasic
	fcfg.Path = dir
	fcfg.FSWatcherEnabled = true
	fcfg.FSWatcherDelayS = 1 // seconds; keep the test fast
	if _, err := w.Modify(func(cfg *config.Configuration) { cfg.SetFolder(fcfg) }); err != nil {
		t.Fatal(err)
	}
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	r, _ := m.folderRunners.Get(fcfg.ID)
	fA := r.(*sendReceiveFolder).folder
	ffs := fA.Filesystem()
	ctx := t.Context()

	// --- Phase 1: first ever run. No watermark exists, so arming must
	// refuse and an explicit initial full scan must happen (control; the
	// model setup may already have scanned on its own, so compare deltas).
	if fA.tryArmWatchReplay(ctx) {
		t.Fatal("armed replay without a recorded watermark")
	}
	baseScans := fA.fullScanCount.Load()
	if err := fA.scanTimerFired(ctx); err != nil {
		t.Fatal(err)
	}
	if n := fA.fullScanCount.Load(); n != baseScans+1 {
		t.Fatalf("control: expected exactly one more full scan, got %d -> %d", baseScans, n)
	}

	// Watch, change a file, and wait until the change is scanned and the
	// watermark is persisted.
	fA.startWatch(ctx)
	writeFile(t, ffs, "a.txt", []byte("a"))
	deadline := time.Now().Add(30 * time.Second)
	for {
		drainWatchBatches(ctx, fA)
		if wm, _, err := fA.watchReplayWatermark(); err == nil && wm != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no journal watermark persisted while watching")
		}
		time.Sleep(100 * time.Millisecond)
	}
	wm, _, err := fA.watchReplayWatermark()
	if err != nil || wm == 0 {
		t.Fatalf("watermark: %d, %v", wm, err)
	}
	fA.stopWatch()

	// --- Downtime: changes no watcher sees.
	writeFile(t, ffs, "b.txt", []byte("b"))
	must(t, ffs.Remove("a.txt"))
	// Age the changes past the stream-creation redelivery race window, so
	// their later delivery can only be journal replay (see the fs-level
	// replay tests).
	time.Sleep(5 * time.Second)

	// --- Phase 2: the restart. A fresh folder object over the same
	// database and config, like a new process would construct.
	fB := newFolder(m.model, fA.ignores, fA.FolderConfiguration, fA.evLogger, fA.ioLimiter, nil)
	if !fB.tryArmWatchReplay(ctx) {
		t.Fatal("replay not armed despite recorded watermark and completed initial scan")
	}
	select {
	case <-fB.initialScanFinished:
	default:
		t.Fatal("armed replay must mark the initial scan as finished")
	}
	fB.startWatch(ctx)
	defer fB.stopWatch()

	deadline = time.Now().Add(30 * time.Second)
	for {
		drainWatchBatches(ctx, fB)
		names := liveLocalNames(t, m.model, fcfg.ID)
		_, hasB := names["b.txt"]
		_, hasA := names["a.txt"]
		if hasB && !hasA {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("downtime changes not indexed via replay (b.txt: %v, a.txt still present: %v)", hasB, hasA)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := fB.fullScanCount.Load(); n != 0 {
		t.Fatalf("full scan ran on the replay path: %d", n)
	}
}
