// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
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

// Before patch/67 the queued completion represented visible, unindexed bytes.
// A real scanner could mistake that state for a receiver-authored mtime edit,
// hold it, then flush it over a later sender version. Now queued installation
// leaves both disk and index at the predecessor. Keep the exact boundary scan
// and both hold mechanisms, requiring that there is no local work to hold.
func testHeldScanAcrossPull(t *testing.T, metabatch bool, trigger string) {
	t.Setenv("STDRIPMODE", "0")
	t.Setenv("STMETABATCH", "0")
	_, f := setupSendReceiveFolder(t)
	f.MaxConflicts = -1
	const name = "lib.rs"
	oldContent := []byte("// sender content A\n")
	newContent := []byte("// sender content B, newer and longer\n")
	now := time.Unix(1793660546, 0)
	makeInfo := func(content []byte, version uint64) protocol.FileInfo {
		blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(content), protocol.MinBlockSize, int64(len(content)), nil)
		must(t, err)
		return protocol.FileInfo{Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(content)), Permissions: 0o640,
			ModifiedS: now.Unix() + int64(version), ModifiedBy: device1.Short(), Version: protocol.Vector{Counters: []protocol.Counter{{ID: device1.Short(), Value: version}}},
			Blocks: blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
	}
	current := func() protocol.FileInfo {
		fi, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok {
			t.Fatal("missing local SQLite entry")
		}
		return fi
	}
	assertIndex := func(want protocol.FileInfo) {
		got := current()
		if !got.Version.Equal(want.Version) || got.ModifiedBy != want.ModifiedBy || !bytes.Equal(got.BlocksHash, want.BlocksHash) || got.Version.Counter(f.shortID) != 0 {
			t.Fatalf("sender identity changed: %+v", got)
		}
	}
	jobs := make(chan dbUpdateJob, 1)
	scans := make(chan string, 4)
	prepare := func(fi, cur protocol.FileInfo, hasCur bool, content []byte) dbUpdateJob {
		temp := fs.TempName(name)
		writeFile(t, f.mtimefs, temp, content)
		must(t, f.performFinish(fi, cur, hasCur, temp, jobs, scans))
		if len(jobs) != 1 || len(scans) != 0 {
			t.Fatal("unexpected finish job or rescan")
		}
		return <-jobs
	}
	v1 := makeInfo(oldContent, 1)
	commitQueuedFinishForTest(t, f, prepare(v1, protocol.FileInfo{}, false, oldContent))
	assertIndex(v1)
	f.drip.now = func() time.Time { return now }
	f.metaBatch.now = func() time.Time { return now }
	f.drip.params = dripParams{enabled: !metabatch, detectN: 1, window: time.Minute, quiet: 20 * time.Second, maxHold: time.Minute}
	if trigger == "max-hold" {
		f.drip.params.quiet = 2 * time.Minute
	}
	f.metaBatch.params = metaBatchParams{enabled: metabatch, window: time.Minute, cap: 1000}
	f.dripArmed.Store(!metabatch)
	f.metaArmed.Store(metabatch)
	defer f.dripFlushTimer.Stop()
	defer f.metaFlushTimer.Stop()
	v2 := makeInfo(oldContent, 2)
	v2.PreviousBlocksHash = v1.BlocksHash
	completion := prepare(v2, current(), true, oldContent)
	// The scanner is still deliberately scheduled at the queued-completion
	// boundary, not after the updater. No installed receiver edit is fabricated.
	must(t, f.scanSubdirs(t.Context(), []string{name}))
	assertIndex(v1)
	if f.metaBatch.len() != 0 || f.drip.heldCount() != 0 {
		t.Fatal("unpublished sender temp created a held local scan")
	}
	commitQueuedFinishForTest(t, f, completion)
	assertIndex(v2)
	v3 := makeInfo(newContent, 3)
	v3.PreviousBlocksHash = v2.BlocksHash
	commitQueuedFinishForTest(t, f, prepare(v3, current(), true, newContent))
	assertIndex(v3)
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
		must(t, f.commitLocalsFromScanning(f.drip.flushAll()))
		must(t, f.commitScanBatch(f.metaBatch.flushAll()))
	}
	assertIndex(v3)
	if f.metaBatch.len() != 0 || f.drip.heldCount() != 0 {
		t.Fatal("flush left pending entries")
	}
	v4 := makeInfo([]byte("// next sender version\n"), 4)
	v4.PreviousBlocksHash = makeInfo([]byte("// unseen predecessor\n"), 3).BlocksHash
	if v4.InConflictWith(current()) {
		t.Fatal("sender-only versions became concurrent")
	}
	commitQueuedFinishForTest(t, f, prepare(v4, current(), true, []byte("// next sender version\n")))
	assertIndex(v4)
	if len(scans) != 0 || len(existingConflicts(name, f.mtimefs)) != 0 {
		t.Fatal("sender installation created a false conflict or rescan")
	}
}
