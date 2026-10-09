// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
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
