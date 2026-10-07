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

// TestPullRescanDuringInitialScan pins what happens to a pull's rescan
// request while the initial scan is still running.
//
// The stall this comes from, yarlung 2026-11-25 18:47 to 11-26 16:30: an
// initial full scan ran 21h43m. The pull runs between its chunks, and for
// 1,988 files the index still listed a file that was already gone from
// disk while the peer had deleted it. checkToBeDeleted answers that with
// errModified and asks for a rescan of the name, but the rescan goes
// through Scan, which waits for initialScanFinished. So no rescan could run
// until the scan ended, and every pull pass failed the same deletes again:
// 6,471,843 "Failed to delete file" lines over 1,823 failed pull passes.
//
// The fixture is that case in miniature: "gone" is live in the local index,
// absent on disk, and deleted by the peer. The initial scan is reopened, a
// pull runs, then a chunked scan of an unrelated directory runs. The
// deletion must be recorded by the time that scan returns, not deferred
// until the initial scan finishes. The test checks only that outcome, so it
// holds for a fix in the pull or in the scan.
func TestPullRescanDuringInitialScan(t *testing.T) {
	// Not parallel: mutates a package-level budget.
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()
	scanChunkBudget = 0

	local := protocol.FileInfo{
		Name:     "gone",
		Size:     1,
		Version:  protocol.Vector{}.Update(myID.Short()),
		Sequence: 1,
		Blocks:   []protocol.BlockInfo{{Hash: []byte("01234567890123456789012345678901"), Size: 1}},
	}
	m, f := setupSendReceiveFolder(t, local)
	ctx := t.Context()

	remote := local
	remote.Deleted = true
	remote.Blocks = nil
	remote.Size = 0
	remote.Version = local.Version.Update(device1.Short())
	must(t, m.sdb.Update(f.ID, device1, []protocol.FileInfo{remote}))
	if c, err := f.db.CountNeed(f.ID, protocol.LocalDeviceID); err != nil || c.TotalItems() == 0 {
		t.Fatalf("fixture broken: the delete is not needed (count=%v err=%v)", c, err)
	}
	if _, ok := liveLocalNames(t, f.model, f.ID)["gone"]; !ok {
		t.Fatal("fixture broken: gone is not live in the local index")
	}

	ffs := f.Filesystem()
	must(t, ffs.MkdirAll("a", 0o755))
	for _, n := range []string{"a/1", "a/2", "a/3", "a/4"} {
		writeFile(t, ffs, n, []byte(n))
	}

	// The initial scan is running again, as after a restart.
	f.initialScanFinished = make(chan struct{})
	t.Cleanup(func() { close(f.initialScanFinished) })

	if _, err := f.folder.pull(ctx); err != nil {
		t.Fatal(err)
	}
	must(t, f.scanSubdirs(ctx, []string{"a"}))

	if _, ok := liveLocalNames(t, f.model, f.ID)["gone"]; ok {
		t.Error("gone is still live in the local index after a pull and a scan that ran past it: the deletion waits for the initial scan to finish")
	}

	// The livelock itself: the next pull pass must not fail the same
	// delete again.
	if _, err := f.folder.pull(ctx); err != nil {
		t.Fatal(err)
	}
	for _, fe := range f.Errors() {
		if fe.Path == "gone" {
			t.Errorf("the pull after the rescan still fails gone: %s", fe.Err)
		}
	}
}
