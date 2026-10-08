// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

// liveLocalNames returns the names of the local files that are present and not deleted, which is
// what a scan is expected to bring in line with the filesystem.
func liveLocalNames(t *testing.T, m *model, folder string) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	it, errFn := m.LocalFiles(folder, protocol.LocalDeviceID)
	for fi := range it {
		if fi.IsDeleted() || fi.IsDirectory() {
			continue
		}
		names[fi.Name] = struct{}{}
	}
	if err := errFn(); err != nil {
		t.Fatal(err)
	}
	return names
}

// TestScanChunkedServicesWatchEvents checks that a watch event queued while a chunked scan runs
// is serviced between chunks, instead of waiting for the whole scan to finish. The Mac showed why
// this matters: it scanned back to back for hours on a busy tree, and a directory created on disk
// was never announced to the peer at all - the watch events waited behind every scan and were
// lost before they were processed.
//
// The outer scan only covers "a"; the watch event names "b", which that walk can never reach, so
// "b/file2" being indexed afterwards proves the event was serviced mid-scan. The send on the
// unbuffered watch channel doubles as the handshake: it completes exactly when the chunk loop
// accepts the event.
func TestScanChunkedServicesWatchEvents(t *testing.T) {
	// Not parallel: mutates a package-level budget.
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()
	scanChunkBudget = 0

	_, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()

	must(t, ffs.MkdirAll("a", 0o755))
	writeFile(t, ffs, "a/file1", []byte("a"))
	must(t, ffs.MkdirAll("b", 0o755))
	writeFile(t, ffs, "b/file2", []byte("b"))

	// The watch channel is normally created by startWatch, which is not running here.
	f.watchChan = make(chan []string)
	delivered := make(chan struct{})
	go func() {
		select {
		case f.watchChan <- []string{"b"}:
			close(delivered)
		case <-ctx.Done():
		}
	}()

	must(t, f.scanSubdirs(ctx, []string{"a"}))

	select {
	case <-delivered:
	default:
		t.Fatal("the watch event was never serviced during the chunked scan")
	}

	got := liveLocalNames(t, f.model, f.ID)
	if _, ok := got["a/file1"]; !ok {
		t.Error("a/file1 missing after scanning a")
	}
	if _, ok := got["b/file2"]; !ok {
		t.Error("b/file2 missing: the queued watch event was not serviced between chunks")
	}
}

// TestScanChunkedBoundaryWithoutResults checks that a chunk boundary still happens when the walk
// produces nothing at all. The scanner reports no result for an unchanged file, so a re-scan of an
// unchanged tree can walk for minutes without a single result - and a budget check that runs only
// when a result arrives then never fires. Measured on the Mac's 2.35M-file tree: the first chunk
// boundary came 2m53s into the scan, when one changed file happened to produce a result, and a
// directory created on disk in the meantime was never announced.
//
// The second scan here covers only "d", which is unchanged, so its walk produces no results. The
// watch event naming "d2" is outside that walk, so "d2/new" can only be indexed if the budget cuts
// the silent walk and the event is serviced between chunks. The send on the unbuffered watch
// channel doubles as the handshake: it completes exactly when the chunk loop accepts the event.
func TestScanChunkedBoundaryWithoutResults(t *testing.T) {
	// Not parallel: mutates a package-level budget.
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()
	scanChunkBudget = 5 * time.Millisecond

	_, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()

	// Enough files that walking them unchanged takes clearly longer than the budget.
	must(t, ffs.MkdirAll("d", 0o755))
	for i := range 2000 {
		writeFile(t, ffs, fmt.Sprintf("d/f%04d", i), []byte("x"))
	}
	must(t, f.scanSubdirs(ctx, nil))

	must(t, ffs.MkdirAll("d2", 0o755))
	writeFile(t, ffs, "d2/new", []byte("y"))

	f.watchChan = make(chan []string)
	delivered := make(chan struct{})
	go func() {
		select {
		case f.watchChan <- []string{"d2"}:
			close(delivered)
		case <-ctx.Done():
		}
	}()

	must(t, f.scanSubdirs(ctx, []string{"d"}))

	select {
	case <-delivered:
	default:
		t.Fatal("no chunk boundary happened during a walk without results; the watch event was not serviced")
	}

	got := liveLocalNames(t, f.model, f.ID)
	if _, ok := got["d2/new"]; !ok {
		t.Error("d2/new missing: the queued watch event was not serviced between chunks")
	}
}

// TestScanChunkMarkerAdvancesThroughSilence checks that the resume marker advances when a chunk
// is cut in the middle of a silent stretch. Unchanged files produce no walk result, and on a quiet
// tree the hashing pipeline is usually empty at the cut, so abort reports alone cannot tell the
// next chunk how far the walk got: the walker reports its enumeration frontier on cancellation
// instead. Without that report a silent chunk makes no progress, and the scan degrades into
// re-walking the same stretch with an ever-growing budget - seen on the Mac, where the budget
// ballooned to 32s while the scan crawled.
func TestScanChunkMarkerAdvancesThroughSilence(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	ffs := f.Filesystem()
	ctx := t.Context()

	must(t, ffs.MkdirAll("d", 0o755))
	// The tree must be large enough that one small budget cannot walk it, or the loop below
	// passes by finishing rather than by advancing through the silent stretch - which is
	// exactly the case that regressed. It must also be small enough to set up quickly,
	// because hashing the initial scan dominates the test time.
	for i := range 2000 {
		writeFile(t, ffs, fmt.Sprintf("d/f%05d", i), []byte("x"))
	}
	must(t, f.scanSubdirs(ctx, nil))

	// A 1ms budget cuts the unchanged re-walk long before it finishes. The marker must still
	// advance, or the next chunk restarts from the beginning.
	batch := f.newScanBatch()
	_, last, more, progressed, err := f.scanSubdirsChangedAndNew(ctx, nil, batch, "", time.Millisecond)
	must(t, err)
	if !more {
		t.Fatal("the 1ms budget did not cut the walk; the test setup is too fast to be meaningful")
	}
	if !progressed || last == "" {
		t.Fatalf("a chunk cut in a silent stretch made no progress (last=%q): the walk frontier was not reported", last)
	}

	// The production failure, pinned directly: resume from that marker with a budget that
	// is past walk startup but far short of the remaining walk (a full re-walk takes tens
	// of milliseconds here). The walk gets well beyond the resume point, the budget cuts it
	// in silence, and the marker must advance all the same. With the frontier merely minned
	// into the marker like an abort this never advances - the frontier sorts after the
	// resume point and the minimum keeps the marker where it started.
	batch = f.newScanBatch()
	_, next, more, progressed, err := f.scanSubdirsChangedAndNew(ctx, nil, batch, last, 8*time.Millisecond)
	must(t, err)
	if !more {
		t.Fatal("the 8ms budget walked the whole remaining tree; the test setup is too fast to be meaningful")
	}
	if !progressed {
		t.Fatalf("a resumed chunk cut in a silent stretch made no progress (marker stuck at %q): the frontier does not advance the resume marker", next)
	}
	last = next

	// Then the whole walk must finish under chunked budgets. A chunk legitimately stalls
	// when the budget expires while the walk is still catching up to the resume point - the
	// covered region is re-enumerated before new ground is reached - so do what the caller
	// does and double the budget on a stall.
	budget := 2 * time.Millisecond
	for i := range 300 {
		batch := f.newScanBatch()
		_, next, more, progressed, err := f.scanSubdirsChangedAndNew(ctx, nil, batch, last, budget)
		must(t, err)
		if !more {
			return // the walk finished; the marker chain reached the end
		}
		if progressed {
			last = next
			continue
		}
		if budget *= 2; budget > 256*time.Millisecond {
			t.Fatalf("chunk %d made no progress even with a %s budget (marker stuck at %q): the frontier does not advance the resume marker", i, budget, last)
		}
	}
	t.Fatalf("the walk did not finish within 300 chunks (marker stuck at %q)", last)
}

// TestScanChunkedDeliversEverything forces the walk to yield over and over by shrinking the chunk
// budget, and checks the two properties that make chunking safe: every file still ends up indexed,
// and a file deleted between scans is still noticed. It runs at several budgets, from the
// degenerate zero (every chunk boundary is cut at the first result) up to an hour (never chunks,
// the control).
//
// The deletion half is the one that matters most. An earlier attempt at this, chunking by top-level
// directory name in the folder instead of resuming the walk, passed a simple add test and then timed
// out on TestIgnoreDeleteUnignore: entries that vanish at the top level were never scanned, so their
// deletion was never seen. That is why the checkpoint lives in the scanner's walk and why this test
// deletes rather than only adds.
//
// The tree also puts a directory beside a sibling file with the same prefix ("docNN" containing a
// file, next to "docNN.txt"), because that is where walk order (lexical, depth first) and string
// order over full paths disagree: "." (0x2E) sorts before "/" (0x2F), so "docNN.txt" sorts before
// "docNN/file" as strings but is walked after it. A resume predicate that compares whole path
// strings then skips the unvisited sibling file for good. The dirNN/fNN names alone cannot catch
// this: they differ only in digits, and "/" sorts before every digit, so their DFS order and their
// string order happen to agree.
func TestScanChunkedDeliversEverything(t *testing.T) {
	// Not parallel: mutates a package-level budget.
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()

	for _, budget := range []time.Duration{0, time.Millisecond, 5 * time.Millisecond, time.Hour} {
		t.Run(fmt.Sprintf("budget=%s", budget), func(t *testing.T) {
			scanChunkBudget = budget
			defer func() { scanChunkBudget = old }()

			_, f := setupSendReceiveFolder(t)
			ffs := f.Filesystem()
			ctx := t.Context()

			var want []string
			// writeFile does not create parent directories, and these files are deliberately nested.
			writeIn := func(name string) {
				if dir := filepath.Dir(name); dir != "." {
					must(t, ffs.MkdirAll(dir, 0o755))
				}
				writeFile(t, ffs, name, []byte("contents"))
				want = append(want, name)
			}
			for i := range 40 {
				writeIn(fmt.Sprintf("dir%02d/f%02d", i, i))
			}
			for i := range 8 {
				writeIn(fmt.Sprintf("doc%02d/file", i))
				writeIn(fmt.Sprintf("doc%02d.txt", i))
			}
			must(t, f.scanSubdirs(ctx, nil))

			got := liveLocalNames(t, f.model, f.ID)
			if len(got) != len(want) {
				var missing []string
				for _, name := range want {
					if _, ok := got[name]; !ok {
						missing = append(missing, name)
					}
				}
				t.Fatalf("after the first scan there are %d live local files, want %d; missing %v", len(got), len(want), missing)
			}

			// Delete one file and add another in a directory that did not exist, then scan again. Both
			// changes have to be reflected, however many chunks the walk is cut into.
			must(t, ffs.Remove("dir00/f00"))
			writeIn("newdir/fresh")
			must(t, f.scanSubdirs(ctx, nil))

			got = liveLocalNames(t, f.model, f.ID)
			if _, ok := got["dir00/f00"]; ok {
				t.Error("a file deleted from disk is still indexed as live after a chunked scan")
			}
			if _, ok := got["newdir/fresh"]; !ok {
				t.Error("a file added on disk is missing after a chunked scan")
			}
			if len(got) != len(want)-1 {
				t.Errorf("after deleting one file and adding one there are %d live local files, want %d", len(got), len(want)-1)
			}
		})
	}
}
