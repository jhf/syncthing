// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

func TestDripHeldScanAcrossPull(t *testing.T) {
	for _, trigger := range []string{"quiet", "max-hold", "shutdown"} {
		t.Run(trigger, func(t *testing.T) { testHeldScanAcrossPull(t, false, trigger) })
	}
}

func TestMetabatchHeldScanAcrossPull(t *testing.T) {
	for _, trigger := range []string{"window", "piggyback", "shutdown"} {
		t.Run(trigger, func(t *testing.T) { testHeldScanAcrossPull(t, true, trigger) })
	}
}

// testHeldScanAcrossPull uses the real finish -> queued DB completion gap:
// sender v2 is installed but not indexed when a scanner runs against indexed
// v1. It changes only mtime, making its real scanner result eligible for both
// holders. No installed receiver file is edited by the test. After v2's DB
// completion, another whole pull installs and indexes v3 before the hold flush.
// This explicitly schedules a scan between finish and DB completion. Normal
// Serve serializes scans and pulls, so this is a boundary-state regression,
// NOT evidence that this interleaving occurs in the live event loop. It pins
// current undesirable behavior conditional on an already-held result.
// The stopped folder permits explicit scheduling without sleeps or test hooks.
func testHeldScanAcrossPull(t *testing.T, metabatch bool, trigger string) {
	t.Setenv("STDRIPMODE", "0")
	t.Setenv("STMETABATCH", "0")
	w, cfg := newDefaultCfgWrapper(t)
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	runner, _ := m.folderRunners.Get(cfg.ID)
	f := runner.(*sendReceiveFolder)
	f.MaxConflicts = -1
	const name = "lib.rs"
	oldContent := []byte("// sender content A\n")
	newContent := []byte("// sender content B, newer and longer\n")
	now := time.Unix(1793660546, 0)
	makeInfo := func(content []byte, version uint64) protocol.FileInfo {
		t.Helper()
		blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(content), protocol.MinBlockSize, int64(len(content)), nil)
		must(t, err)
		return protocol.FileInfo{Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(content)), Permissions: 0o640,
			ModifiedS: now.Unix() + int64(version), ModifiedBy: device1.Short(),
			Version: protocol.Vector{Counters: []protocol.Counter{{ID: device1.Short(), Value: version}}},
			Blocks:  blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
	}
	current := func() protocol.FileInfo {
		t.Helper()
		fi, ok, err := f.db.GetDeviceFile(cfg.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok {
			t.Fatal("missing local SQLite entry")
		}
		return fi
	}
	assertIndex := func(want protocol.FileInfo) {
		t.Helper()
		got := current()
		if !got.Version.Equal(want.Version) || got.ModifiedBy != want.ModifiedBy || !bytes.Equal(got.BlocksHash, want.BlocksHash) {
			t.Fatalf("index version=%v ModifiedBy=%v BlocksHash=%x, want version=%v ModifiedBy=%v BlocksHash=%x", got.Version, got.ModifiedBy, got.BlocksHash, want.Version, want.ModifiedBy, want.BlocksHash)
		}
	}
	jobs := make(chan dbUpdateJob, 1)
	scans := make(chan string, 4)
	finish := func(fi, cur protocol.FileInfo, hasCur bool, content []byte) protocol.FileInfo {
		t.Helper()
		temp := fs.TempName(name)
		writeFile(t, f.mtimefs, temp, content)
		must(t, f.performFinish(fi, cur, hasCur, temp, jobs, scans))
		if len(jobs) != 1 || len(scans) != 0 {
			t.Fatal("unexpected finish job or rescan")
		}
		job := <-jobs
		if job.jobType != dbUpdateHandleFile {
			t.Fatal("unexpected finish job type")
		}
		return job.file
	}
	v1 := makeInfo(oldContent, 1)
	must(t, f.updateLocalsFromPulling([]protocol.FileInfo{finish(v1, protocol.FileInfo{}, false, oldContent)}))
	assertIndex(v1)

	// Configure fake hold time only after setup/shutdown, with all activity now
	// synchronous. detectN=1 deterministically models an already hot drip path.
	f.drip.now = func() time.Time { return now }
	f.metaBatch.now = func() time.Time { return now }
	f.drip.params = dripParams{enabled: !metabatch, detectN: 1, window: time.Minute, quiet: 20 * time.Second, maxHold: time.Minute}
	if trigger == "max-hold" {
		// Supported override: max-hold expires before quiet.
		f.drip.params.quiet = 2 * time.Minute
	}
	f.metaBatch.params = metaBatchParams{enabled: metabatch, window: time.Minute, cap: 1000}
	f.dripArmed.Store(!metabatch)
	f.metaArmed.Store(metabatch)
	defer f.dripFlushTimer.Stop()
	defer f.metaFlushTimer.Stop()

	v2 := makeInfo(oldContent, 2)
	v2.PreviousBlocksHash = v1.BlocksHash
	completion := finish(v2, current(), true, oldContent)
	// Disk v2, DB v1. Use the actual scanner body, not a fabricated FileInfo.
	must(t, f.scanSubdirs(t.Context(), []string{name}))
	var held protocol.FileInfo
	if metabatch {
		if f.metaBatch.len() != 1 || f.drip.heldCount() != 0 {
			t.Fatal("scanner did not enter metabatch exclusively")
		}
		held = f.metaBatch.pending[name].fi
	} else {
		if f.drip.heldCount() != 1 || f.metaBatch.len() != 0 {
			t.Fatal("scanner did not enter drip exclusively")
		}
		held = f.drip.held[name].fi
	}
	if held.Version.Counter(f.shortID) == 0 || held.Version.Counter(device1.Short()) != 1 || len(held.Version.Counters) != 2 || held.ModifiedBy != f.shortID || !bytes.Equal(held.BlocksHash, v1.BlocksHash) || !held.ModTime().Equal(v2.ModTime()) {
		t.Fatalf("unexpected real scanner result: %+v", held)
	}
	assertIndex(v1)
	// Control: while disk differs from indexed v1, an ordinary finish cannot
	// cross this held metadata scan. Only the already-produced v2 completion
	// below reconciles disk/index without a new same-name scan superseding it.
	probe := makeInfo(newContent, 3)
	writeFile(t, f.mtimefs, fs.TempName(name), newContent)
	probeErr := f.performFinish(probe, current(), true, fs.TempName(name), jobs, scans)
	if !errors.Is(probeErr, errModified) || len(jobs) != 0 || len(scans) != 1 || <-scans != name {
		t.Fatalf("expected pre-pull safety guard, got %v", probeErr)
	}
	assertIndex(v1)
	must(t, f.updateLocalsFromPulling([]protocol.FileInfo{completion}))
	assertIndex(v2)
	v3 := makeInfo(newContent, 3)
	v3.PreviousBlocksHash = v2.BlocksHash
	must(t, f.updateLocalsFromPulling([]protocol.FileInfo{finish(v3, current(), true, newContent)}))
	assertIndex(v3)
	if metabatch && f.metaBatch.len() != 1 || !metabatch && f.drip.heldCount() != 1 {
		t.Fatal("pull cleared held scan")
	}

	switch trigger {
	case "quiet":
		now = now.Add(f.drip.params.quiet)
		f.serviceDripFlush()
	case "max-hold":
		now = now.Add(f.drip.params.maxHold)
		f.serviceDripFlush()
	case "window":
		now = now.Add(f.metaBatch.params.window)
		f.serviceMetaFlush()
	case "piggyback":
		writeFile(t, f.mtimefs, "unrelated.txt", []byte("real scanner piggyback\n"))
		must(t, f.scanSubdirs(t.Context(), []string{"unrelated.txt"}))
	case "shutdown":
		// Same two stages and order as folder.Serve's deferred shutdown flush.
		must(t, f.commitLocalsFromScanning(f.drip.flushAll()))
		must(t, f.commitScanBatch(f.metaBatch.flushAll()))
	}
	if f.drip.heldCount() != 0 || f.metaBatch.len() != 0 {
		t.Fatal("flush left pending entries")
	}
	assertIndex(held)
	fd, err := f.mtimefs.Open(name)
	must(t, err)
	data, err := io.ReadAll(fd)
	must(t, err)
	must(t, fd.Close())
	if !bytes.Equal(data, newContent) {
		t.Fatal("held flush changed installed sender v3 bytes")
	}
	t.Logf("stale scan wins: version=%v ModifiedBy=%v BlocksHash=%x; disk retains sender v3 BlocksHash=%x", current().Version, current().ModifiedBy, current().BlocksHash, v3.BlocksHash)

	v4 := makeInfo([]byte("// next sender version\n"), 4)
	// Name an unseen predecessor so the block-history shortcut cannot mask
	// vector concurrency. Do not bypass the actual finish safety check.
	v4.PreviousBlocksHash = makeInfo([]byte("// unseen predecessor\n"), 3).BlocksHash
	if !v4.InConflictWith(current()) {
		t.Fatal("next sender version is not concurrent")
	}
	temp := fs.TempName(name)
	writeFile(t, f.mtimefs, temp, []byte("// next sender version\n"))
	err = f.performFinish(v4, current(), true, temp, jobs, scans)
	if !errors.Is(err, errModified) || err.Error() != "checking existing file: file modified but not rescanned; will try again later" {
		t.Fatalf("unexpected next finish result: %v", err)
	}
	if len(jobs) != 0 || len(scans) != 1 || <-scans != name || len(existingConflicts(name, f.mtimefs)) != 0 {
		t.Fatal("rejected finish completed, omitted rescan, or created conflict")
	}
	t.Logf("next sender version: %v; no conflict copy", err)
	assertIndex(held)
	fd, err = f.mtimefs.Open(name)
	must(t, err)
	data, err = io.ReadAll(fd)
	must(t, err)
	must(t, fd.Close())
	if !bytes.Equal(data, newContent) {
		t.Fatal("rejected finish changed installed bytes")
	}
}
