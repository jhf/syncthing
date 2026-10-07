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
