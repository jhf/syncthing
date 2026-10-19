// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"testing"

	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// collapseWave builds the tombstone set for everything under root: every live local index
// entry becomes a deleted version that dominates the local one, files in the returned map and
// directories in the returned slice.
func collapseWave(t *testing.T, f *sendReceiveFolder, root string) (map[string]protocol.FileInfo, []protocol.FileInfo) {
	t.Helper()
	fileDeletions := map[string]protocol.FileInfo{}
	var dirDeletions []protocol.FileInfo
	for fi, err := range itererr.Zip(f.model.sdb.AllLocalFilesWithPrefix(f.folderID, protocol.LocalDeviceID, root)) {
		must(t, err)
		tomb := fi
		tomb.SetDeleted(f.shortID)
		tomb.Version = fi.Version.Update(f.shortID)
		if fi.IsDirectory() {
			dirDeletions = append(dirDeletions, tomb)
		} else {
			fileDeletions[tomb.Name] = tomb
		}
	}
	return fileDeletions, dirDeletions
}

// setupCollapseTree creates root with two subdirectories and files on disk, scans them into
// the local index, and returns the total member count (files + directories + root).
func setupCollapseTree(t *testing.T, f *sendReceiveFolder, root string, filesPerDir int) int {
	t.Helper()
	ffs := f.mtimefs
	for _, sub := range []string{"d1", "d2"} {
		must(t, ffs.MkdirAll(root+"/"+sub, 0o755))
		for i := range filesPerDir {
			writeFile(t, ffs, fmt.Sprintf("%s/%s/f%03d", root, sub, i), []byte("x"))
		}
	}
	writeFile(t, ffs, root+"/top", []byte("x"))
	must(t, f.scanSubdirs(t.Context(), []string{root}))
	return 2*filesPerDir + 1 + 3 // files + root + two subdirs
}

func TestCollapseDeletionsCollapses(t *testing.T) {
	t.Parallel()
	_, f := setupSendReceiveFolder(t)
	members := setupCollapseTree(t, f, "tree", 60)
	if members < collapseDeletionsMinSet {
		t.Fatalf("test tree too small for the collapse threshold: %d", members)
	}

	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	dbUpdateChan := make(chan dbUpdateJob, 2*members)
	scanChan := make(chan string, 10)

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, dbUpdateChan, scanChan)

	if len(fileDeletions) != 0 {
		t.Errorf("%d file deletions left for the per-file path after a full collapse", len(fileDeletions))
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Errorf("tree should be gone from disk, stat err = %v", err)
	}
	if got := len(dbUpdateChan); got != members {
		t.Errorf("expected %d index updates, got %d", members, got)
	}
}

func TestCollapseDeletionsKeepsUntracked(t *testing.T) {
	t.Parallel()
	_, f := setupSendReceiveFolder(t)
	setupCollapseTree(t, f, "tree", 60)

	// A file the index never saw is user content and must survive.
	writeFile(t, f.mtimefs, "tree/d2/userfile", []byte("mine"))

	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	dbUpdateChan := make(chan dbUpdateJob, 1000)
	scanChan := make(chan string, 100)

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, dbUpdateChan, scanChan)

	if _, err := f.mtimefs.Lstat("tree/d2/userfile"); err != nil {
		t.Errorf("untracked user file must survive the wave, stat err = %v", err)
	}
}

func TestCollapseDeletionsKeepsLocalModification(t *testing.T) {
	t.Parallel()
	_, f := setupSendReceiveFolder(t)
	setupCollapseTree(t, f, "tree", 60)

	// A file modified on disk after the scan is unscanned local work and must survive.
	writeFile(t, f.mtimefs, "tree/d1/f000", []byte("changed content, much longer than before"))

	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	dbUpdateChan := make(chan dbUpdateJob, 1000)
	scanChan := make(chan string, 100)

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, dbUpdateChan, scanChan)

	if _, err := f.mtimefs.Lstat("tree/d1/f000"); err != nil {
		t.Errorf("locally modified file must survive the wave, stat err = %v", err)
	}
}
