// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build linux

package model

import (
	"context"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

// stubBootDiff stands in the helper's diff-baseline answer, returning paths
// (nil: an unchanged filesystem) at once.
func stubBootDiff(t *testing.T, paths ...string) {
	t.Helper()
	old := btrfsScanDiffBaseline
	t.Cleanup(func() { btrfsScanDiffBaseline = old })
	btrfsScanDiffBaseline = func(context.Context, string, string) ([]string, error) { return paths, nil }
}

// pairForMarker runs a successful pairing against the fake helper, writing
// the marker exactly as production does.
func pairForMarker(t *testing.T, f *folder) {
	t.Helper()
	drainFlush(f)
	feedCanary(t, f)
	if !f.pairBtrfsBaseline(t.Context(), "runtime") {
		t.Fatal("pairing failed against the fake helper")
	}
}

// bootFromMarker runs the start-of-Serve marker load and replay the way a
// fresh process does.
func bootFromMarker(t *testing.T, f *folder) bool {
	t.Helper()
	f.initialScanFinished = make(chan struct{})
	f.btrfsChecked, f.btrfsOK = true, true
	f.btrfsCleanStart = f.consumeBtrfsBaselineMarker(t.Context())
	return f.tryArmBtrfsScanReplay(t.Context())
}

// A restart before the boot's own re-pairing must still replay. On yarlung
// 2026-11-27 a restart 1m53s after boot (watch setup takes ~34 min) found
// no marker: the boot had deleted it on read, the shutdown pairing failed
// "watch not running", and the next boot full-scanned (1h21m44s last
// time). The marker now outlives the boot that read it.
//
// Teeth: delete the marker on read (the old consume) and the second boot
// reports no pairing marker and does not arm.
func TestBtrfsMarkerSurvivesBootUntilNextPairing(t *testing.T) {
	_ = startPairingFakeHelper(t)
	stubBootDiff(t)
	_, f, _ := barrierTestFolder(t)
	if err := f.db.PutKV(initialScanDoneKVPrefix+f.folderID, []byte{1}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f.Filesystem(), "kept.txt", []byte("x"))
	if err := f.scanSubdirs(t.Context(), []string{"kept.txt"}); err != nil {
		t.Fatal(err)
	}
	pairForMarker(t, f)

	if !bootFromMarker(t, f) {
		t.Fatal("first boot after pairing did not replay")
	}
	// The re-pairing never happens (watch still in setup) and the process
	// restarts.
	if !bootFromMarker(t, f) {
		t.Fatal("second boot before any new pairing did not replay: the first boot consumed the marker")
	}
}

// The index can hold a name the snapshot diff cannot see: created,
// indexed, and removed again after the pairing, so it is absent from both
// the baseline and the boot snapshot. Replaying from the diff alone would
// leave it live in the index and announce it to peers again. The boot
// rescans every name the index wrote since the pairing, which settles it
// as deleted.
//
// Teeth: drop the index rescan from tryArmBtrfsScanReplay and the stale
// name is still live after the replay.
func TestBtrfsReplayRescansIndexWritesSincePairing(t *testing.T) {
	_ = startPairingFakeHelper(t)
	stubBootDiff(t)
	m, f, _ := barrierTestFolder(t)
	if err := f.db.PutKV(initialScanDoneKVPrefix+f.folderID, []byte{1}); err != nil {
		t.Fatal(err)
	}
	pairForMarker(t, f)

	// After the pairing: created and indexed...
	writeFile(t, f.Filesystem(), "ghost.txt", []byte("boo"))
	if err := f.scanSubdirs(t.Context(), []string{"ghost.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := liveLocalNames(t, m.model, f.folderID)["ghost.txt"]; !ok {
		t.Fatal("fixture: ghost.txt not indexed")
	}
	// ...then removed on disk with the deletion never indexed (the
	// process stopped before the watch delivered it).
	if err := f.Filesystem().Remove("ghost.txt"); err != nil {
		t.Fatal(err)
	}

	if !bootFromMarker(t, f) {
		t.Fatal("boot did not replay")
	}
	if _, ok := liveLocalNames(t, m.model, f.folderID)["ghost.txt"]; ok {
		t.Fatal("ghost.txt still live in the index after the replay: index writes since the pairing were not rescanned")
	}
}

// A marker that no longer describes this index (local sequence below what
// the pairing saw: index writes were lost, or the database was recreated)
// is dropped and the boot full-scans.
//
// Teeth: skip the sequence check and the boot replays on an index that
// lost writes the pairing relied on.
func TestBtrfsMarkerDroppedWhenIndexBehind(t *testing.T) {
	_ = startPairingFakeHelper(t)
	_, f, _ := barrierTestFolder(t)
	if err := f.db.PutKV(initialScanDoneKVPrefix+f.folderID, []byte{1}); err != nil {
		t.Fatal(err)
	}
	pairForMarker(t, f)
	val, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID)
	if err != nil {
		t.Fatal(err)
	}
	mk, ok := decodeBtrfsMarker(val)
	if !ok || !mk.durable {
		t.Fatalf("pairing wrote a non-durable marker (%d bytes)", len(val))
	}
	seq, err := f.db.GetDeviceSequence(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	mk.seqAfter = seq + 10
	if err := f.db.PutKV(btrfsBaselineKVPrefix+f.folderID, mk.encode()); err != nil {
		t.Fatal(err)
	}

	f.btrfsChecked, f.btrfsOK = true, true
	if f.consumeBtrfsBaselineMarker(t.Context()) {
		t.Fatal("marker ahead of the index was accepted")
	}
	if _, err := f.db.GetKV(btrfsBaselineKVPrefix + f.folderID); err == nil {
		t.Fatal("stale marker was kept")
	}
}
