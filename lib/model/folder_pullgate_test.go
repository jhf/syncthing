// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

// TestPullGateDuringInitialScan pins the rule for pulling while the initial scan is still
// running.
//
// The stall this comes from: after a restart the initial scan of a large folder holds the serve
// loop for minutes, and upstream's blanket "no pulls until the initial scan has finished" meant a
// peer that knew exactly what it needed (needFiles > 0, measured) moved nothing for the whole
// scan. The gate exists for the first scan ever, when the local index is empty and every remote
// file would look needed, including files that exist on disk but are not indexed yet. Whether the
// folder has ever completed an initial scan is recorded in the database (initialScanDoneKVPrefix),
// and the puller checks anything it is about to change against the index (scanIfItemChanged,
// backing off with errModified where disk and index disagree), so once the marker exists pulling
// during a re-scan is safe.
//
// The discriminator is pull's success return: a held pull returns (true, nil) untouched, while a
// pull that runs and cannot finish (here: the needed file has a block and there is no peer to
// fetch it from) returns (false, nil).
func TestPullGateDuringInitialScan(t *testing.T) {
	ctx := t.Context()

	// A file the remote has and we do not: a block is required so the puller actually tries
	// (and fails, there being no connection), instead of succeeding trivially on an empty file.
	needed := []protocol.FileInfo{{
		Name:     "needed",
		Size:     5,
		Version:  protocol.Vector{}.Update(device1.Short()),
		Sequence: 1,
		Blocks:   []protocol.BlockInfo{{Hash: []byte("01234567890123456789012345678901"), Size: 5}},
	}}

	addNeed := func(t *testing.T, m *testModel, f *sendReceiveFolder) {
		t.Helper()
		must(t, m.sdb.Update(f.ID, device1, needed))
		c, err := f.db.CountNeed(f.ID, protocol.LocalDeviceID)
		must(t, err)
		if c.TotalItems() == 0 {
			t.Fatal("fixture broken: nothing is needed, the test cannot tell a held pull from a completed one")
		}
	}

	// setupSendReceiveFolder starts the model briefly, so the folder has already completed a
	// (trivial) initial scan: initialScanFinished is closed and the marker is written. To test
	// the gate we reopen the channel by hand; to test the first-scan-ever case we additionally
	// remove the marker. The control case uses the closed channel setup leaves behind.
	reopenInitialScan := func(f *sendReceiveFolder) {
		f.initialScanFinished = make(chan struct{})
	}
	removeMarker := func(t *testing.T, m *testModel, f *sendReceiveFolder) {
		t.Helper()
		must(t, m.sdb.DeleteKV(initialScanDoneKVPrefix+f.ID))
		if done, err := f.initialScanEverCompleted(); err != nil || done {
			t.Fatalf("marker should be gone after DeleteKV, done=%v err=%v", done, err)
		}
	}

	// A pull that runs here cannot succeed (no peer to fetch the block from), so success=false
	// means "the gate let it through" and success=true means "the gate held it".
	t.Run("never completed an initial scan holds the pull", func(t *testing.T) {
		m, f := setupSendReceiveFolder(t)
		removeMarker(t, m, f)
		addNeed(t, m, f)
		reopenInitialScan(f)
		success, err := f.folder.pull(ctx)
		must(t, err)
		if !success {
			t.Error("with no completed initial scan on record the pull should be held (return untouched success)")
		}
	})

	t.Run("completed initial scan on record lets the pull run", func(t *testing.T) {
		m, f := setupSendReceiveFolder(t)
		if done, err := f.initialScanEverCompleted(); err != nil || !done {
			t.Fatalf("setup's initial scan should have recorded the marker, done=%v err=%v", done, err)
		}
		addNeed(t, m, f)
		reopenInitialScan(f)
		success, err := f.folder.pull(ctx)
		must(t, err)
		if success {
			t.Error("with a completed initial scan on record the pull should run during the re-scan")
		}
	})

	t.Run("finished initial scan lets the pull run", func(t *testing.T) {
		m, f := setupSendReceiveFolder(t)
		removeMarker(t, m, f) // the marker is irrelevant once initialScanFinished is closed
		addNeed(t, m, f)
		success, err := f.folder.pull(ctx)
		must(t, err)
		if success {
			t.Error("with the initial scan finished the pull should run")
		}
	})
}
