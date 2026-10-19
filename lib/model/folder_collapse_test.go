// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"testing"
	"time"

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

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, nil, dbUpdateChan, scanChan)

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

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, nil, dbUpdateChan, scanChan)

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

	f.processDeletions(t.Context(), fileDeletions, dirDeletions, nil, dbUpdateChan, scanChan)

	if _, err := f.mtimefs.Lstat("tree/d1/f000"); err != nil {
		t.Errorf("locally modified file must survive the wave, stat err = %v", err)
	}
}

func TestCollapseDeletionsCoversDeferred(t *testing.T) {
	t.Parallel()
	_, f := setupSendReceiveFolder(t)
	members := setupCollapseTree(t, f, "tree", 60)

	// One file of the tree defers to the post-drain pass (rename source for an
	// in-flight pull). It is still deleted this round, so the collapse must not
	// treat it as user content and abandon the whole tree to the per-file path.
	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	deferred := map[string]protocol.FileInfo{"tree/d1/f000": fileDeletions["tree/d1/f000"]}
	delete(fileDeletions, "tree/d1/f000")

	dbUpdateChan := make(chan dbUpdateJob, 2*members)
	scanChan := make(chan string, 10)
	f.processDeletions(t.Context(), fileDeletions, dirDeletions, deferred, dbUpdateChan, scanChan)

	if len(fileDeletions) != 0 {
		t.Errorf("%d file deletions left for the per-file path despite the collapse", len(fileDeletions))
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Errorf("tree should be gone from disk, stat err = %v", err)
	}
	// The deferred entry's index update is not the collapse's business: the
	// post-drain pass deletes it from the index (tolerating the file already
	// being gone from disk).
	if got, want := len(dbUpdateChan), members-1; got != want {
		t.Errorf("expected %d index updates (tree minus the deferred entry), got %d", want, got)
	}
	if _, ok := deferred["tree/d1/f000"]; !ok {
		t.Error("the deferred set must be left for the post-drain pass")
	}
}

func TestCollapseDeletionsCoversResidueDir(t *testing.T) {
	t.Parallel()
	m, f := setupSendReceiveFolder(t)
	members := setupCollapseTree(t, f, "tree", 60)

	// Simulate residue from an interrupted earlier pass: the directory's
	// tombstone was already consumed (the index says deleted) but the
	// directory itself is still on disk. It must not block the collapse:
	// the walk descends and verifies its contents individually anyway.
	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	var kept []protocol.FileInfo
	for _, d := range dirDeletions {
		if d.Name == "tree/d1" {
			must(t, m.sdb.Update(f.folderID, protocol.LocalDeviceID, []protocol.FileInfo{d}))
			continue
		}
		kept = append(kept, d)
	}
	dirDeletions = kept

	dbUpdateChan := make(chan dbUpdateJob, 2*members)
	scanChan := make(chan string, 10)
	f.processDeletions(t.Context(), fileDeletions, dirDeletions, nil, dbUpdateChan, scanChan)

	if len(fileDeletions) != 0 {
		t.Errorf("%d file deletions left for the per-file path despite the collapse", len(fileDeletions))
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Errorf("tree should be gone from disk, stat err = %v", err)
	}
	// The residue directory's index entry is already deleted; the collapse
	// must not emit a second update for it.
	if got, want := len(dbUpdateChan), members-1; got != want {
		t.Errorf("expected %d index updates (tree minus the residue dir), got %d", want, got)
	}
}

func TestCollapseDeletionsSkipsQueuedPullTree(t *testing.T) {
	t.Parallel()
	_, f := setupSendReceiveFolder(t)
	setupCollapseTree(t, f, "tree", 60)

	// A pull queued under the deleted tree (multi-device inconsistency: the
	// tree deleted on one device, an entry inside alive on another) must skip
	// the collapse: the RemoveAll would destroy the freshly pulled file.
	f.queue.Push("tree/d1/pulled", 100, time.Now())

	fileDeletions, dirDeletions := collapseWave(t, f, "tree")
	dbUpdateChan := make(chan dbUpdateJob, 1000)

	if collapsed := f.collapseDeletions(t.Context(), fileDeletions, dirDeletions, nil, dbUpdateChan); len(collapsed) != 0 {
		t.Errorf("collapse ran with a pull queued under the tree: %v", collapsed)
	}
	if _, err := f.mtimefs.Lstat("tree"); err != nil {
		t.Errorf("tree must survive while a pull is queued under it, stat err = %v", err)
	}

	// Once the queue no longer touches the tree, the collapse proceeds.
	name, ok := f.queue.Pop()
	if !ok || name != "tree/d1/pulled" {
		t.Fatalf("expected to pop the queued pull, got %q, %v", name, ok)
	}
	f.queue.Done(name)

	fileDeletions2, dirDeletions2 := collapseWave(t, f, "tree")
	if collapsed := f.collapseDeletions(t.Context(), fileDeletions2, dirDeletions2, nil, dbUpdateChan); len(collapsed) == 0 {
		t.Error("collapse should proceed once the queue no longer touches the tree")
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Errorf("tree should be gone after the queue cleared, stat err = %v", err)
	}
}
