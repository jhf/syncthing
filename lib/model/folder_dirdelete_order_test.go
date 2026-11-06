// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// dirDeleteWave scans tree (root, root/sub, files in both) into the local
// index, has device1 announce the same entries, and returns device1's
// tombstones for them: the directories and the files separately, in the
// order a device announces a removed tree (directory before its contents).
func dirDeleteWave(t *testing.T, m *testModel, f *sendReceiveFolder, root string, files int) (dirs, contents []protocol.FileInfo) {
	t.Helper()
	ffs := f.mtimefs
	must(t, ffs.MkdirAll(root+"/sub", 0o755))
	for i := range files {
		writeFile(t, ffs, fmt.Sprintf("%s/f%02d", root, i), []byte("x"))
		writeFile(t, ffs, fmt.Sprintf("%s/sub/g%02d", root, i), []byte("y"))
	}
	must(t, f.scanSubdirs(t.Context(), nil))

	var live []protocol.FileInfo
	for _, name := range []string{root, root + "/sub"} {
		cur, ok := m.testCurrentFolderFile(f.ID, name)
		if !ok {
			t.Fatalf("%s missing from local index", name)
		}
		live = append(live, cur)
	}
	for i := range files {
		for _, name := range []string{fmt.Sprintf("%s/f%02d", root, i), fmt.Sprintf("%s/sub/g%02d", root, i)} {
			cur, ok := m.testCurrentFolderFile(f.ID, name)
			if !ok {
				t.Fatalf("%s missing from local index", name)
			}
			live = append(live, cur)
		}
	}
	// device1 has exactly what we have (same versions), so it is a valid
	// source for every entry before the wave.
	for i := range live {
		live[i] = prepareFileInfoForIndex(live[i])
	}
	must(t, m.sdb.Update(f.folderID, device1, live))

	for _, cur := range live {
		tomb := cur
		tomb.SetDeleted(device1.Short())
		tomb.Version = cur.Version.Update(device1.Short())
		if cur.IsDirectory() {
			dirs = append(dirs, tomb)
		} else {
			contents = append(contents, tomb)
		}
	}
	return dirs, contents
}

func pullFailures(f *sendReceiveFolder, prefix string) []string {
	var out []string
	for _, fe := range f.Errors() {
		if strings.HasPrefix(fe.Path, prefix) {
			out = append(out, fe.Path+": "+fe.Err)
		}
	}
	return out
}

// A device that removes a tree announces the directories' tombstones before
// most of their contents', and a large index arrives in several messages, so
// a pull can see a directory deleted while some of its contents are still
// alive on that very device. Before patch/68 every such directory failed
// with "not empty ... probably ignored" and the folder went onto its failure
// timer, once per directory of the tree (228 failures over 62 directories in
// the 2026-11-06 GenderedGames wave). The directory must instead wait
// silently for its contents and go in the pull their tombstones trigger.
func TestDirDeleteWaitsForContents(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)
	dirs, contents := dirDeleteWave(t, m, f, "tree", 4)

	// First message: both directory tombstones and half the files'.
	first := append(append([]protocol.FileInfo{}, dirs...), contents[:len(contents)/2]...)
	must(t, m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: f.ID, Files: first}))

	_, err := f.pull(t.Context())
	must(t, err)
	if fails := pullFailures(f, "tree"); len(fails) != 0 {
		t.Fatalf("directories waiting for announced-but-unarrived deletions were recorded as failures:\n%s", strings.Join(fails, "\n"))
	}
	// Nothing failed, so the round may report success (no failure timer);
	// the directories simply stay needed until the rest arrives.
	need, err := m.sdb.CountNeed(f.ID, protocol.LocalDeviceID)
	must(t, err)
	if need.TotalItems() == 0 {
		t.Fatal("the tree's remaining entries must stay needed")
	}
	for _, d := range dirs {
		if _, err := f.mtimefs.Lstat(d.Name); err != nil {
			t.Fatalf("%s must survive while contents remain: %v", d.Name, err)
		}
	}

	// Second message: the rest of the tombstones. The tree goes entirely.
	must(t, m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: f.ID, Files: contents[len(contents)/2:]}))
	success, err := f.pull(t.Context())
	must(t, err)
	if fails := pullFailures(f, "tree"); len(fails) != 0 {
		t.Fatalf("unexpected failures after the full wave:\n%s", strings.Join(fails, "\n"))
	}
	if !success {
		t.Fatal("pull not in sync after the full wave")
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Fatalf("tree should be gone, stat err = %v", err)
	}
}

// Directory tombstones in random pull order: the parent must not be tried
// before its child (the old code relied on the need walk's order, which is
// random by default), and with every tombstone present nothing fails.
func TestDirDeleteDepthFirstInRandomOrder(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	dirs, contents := dirDeleteWave(t, m, f, "tree", 2)

	fileDeletions := map[string]protocol.FileInfo{}
	for _, c := range contents {
		fileDeletions[c.Name] = c
	}
	// Parent first: the order the old reverse-iteration got wrong half the
	// time under random pull order.
	dirOrder := []protocol.FileInfo{dirs[1], dirs[0]} // "tree/sub", "tree" -> reversed by old code = "tree" first
	if dirOrder[1].Name != "tree" {
		t.Fatalf("fixture order: %s", dirOrder[1].Name)
	}
	dbUpdateChan := make(chan dbUpdateJob, 100)
	waiting := f.processDeletions(t.Context(), fileDeletions, dirOrder, nil, dbUpdateChan, make(chan string, 100))
	if len(waiting) != 0 {
		t.Fatalf("%d directories still waiting with every tombstone applied", len(waiting))
	}
	if fails := pullFailures(f, "tree"); len(fails) != 0 || len(f.tempPullErrors) != 0 {
		t.Fatalf("depth-first deletion failed: %v %v", fails, f.tempPullErrors)
	}
	if _, err := f.mtimefs.Lstat("tree"); !fs.IsNotExist(err) {
		t.Fatalf("tree should be gone, stat err = %v", err)
	}
}

// Content the deleting device never had (it does not list the entry), with no
// tombstone for it, is genuinely in the way: the "not empty" failure stays.
func TestDirDeleteGenuineNotEmptyStillFails(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)
	dirs, contents := dirDeleteWave(t, m, f, "tree", 1)

	// A local-only file inside the tree, indexed locally but never seen by
	// device1.
	writeFile(t, f.mtimefs, "tree/localonly", []byte("mine"))
	must(t, f.scanSubdirs(t.Context(), []string{"tree"}))

	must(t, m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: f.ID, Files: append(append([]protocol.FileInfo{}, dirs...), contents...)}))
	_, err := f.pull(t.Context())
	must(t, err)
	fails := pullFailures(f, "tree")
	found := false
	for _, fe := range fails {
		if strings.HasPrefix(fe, "tree: ") && strings.Contains(fe, "not empty") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a 'not empty' failure for tree, got %v", fails)
	}
	if _, err := f.mtimefs.Lstat("tree/localonly"); err != nil {
		t.Fatalf("local-only file must survive: %v", err)
	}
}
