// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// A deleted file whose blocks are needed by pulls queued in the same round
// must be deferred to the post-drain deletion pass, while unrelated deletions
// stay in the immediate (concurrent-with-pulls) set.
func TestProcessNeededDefersBlockOverlappingDeletions(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)

	// Local files, scanned into the index. "overlap" consists of exactly one
	// minimum-size block so that a larger remote file can share it.
	sharedBlock := bytes.Repeat([]byte("shared block content "), (protocol.MinBlockSize/21)+1)[:protocol.MinBlockSize]
	writeFile(t, f.mtimefs, "overlap", sharedBlock)
	writeFile(t, f.mtimefs, "plain", []byte("unrelated content"))
	must(t, f.scanSubdirs(t.Context(), nil))

	// Remote state: both local files deleted, plus a new file whose first
	// block is the shared block and which has one more block after it.
	pulledContent := append(append([]byte{}, sharedBlock...), []byte("extra tail")...)
	blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(pulledContent), protocol.MinBlockSize, int64(len(pulledContent)), nil)
	must(t, err)
	pulled := protocol.FileInfo{
		Name:         "pulled",
		Type:         protocol.FileInfoTypeFile,
		Size:         int64(len(pulledContent)),
		RawBlockSize: int32(protocol.MinBlockSize),
		Blocks:       blocks,
	}
	pulled.Version = pulled.Version.Update(device1.Short())

	toDelete := make([]protocol.FileInfo, 0, 2)
	for _, name := range []string{"overlap", "plain"} {
		cur, ok := m.testCurrentFolderFile(f.ID, name)
		if !ok {
			t.Fatalf("%s missing from local index", name)
		}
		tomb := cur
		tomb.SetDeleted(device1.Short())
		tomb.Version = cur.Version.Update(device1.Short())
		toDelete = append(toDelete, tomb)
	}
	must(t, m.Index(conn, &protocol.Index{Folder: f.ID, Files: append(toDelete, pulled)}))

	dbUpdateChan := make(chan dbUpdateJob, 100)
	copyChan := make(chan copyBlocksState, 10)
	scanChan := make(chan string, 10)
	fileDeletions, _, deferred, err := f.processNeeded(t.Context(), dbUpdateChan, copyChan, scanChan)
	must(t, err)

	if _, ok := fileDeletions["plain"]; !ok {
		t.Error("non-overlapping delete should stay in the immediate set")
	}
	if _, ok := fileDeletions["overlap"]; ok {
		t.Error("block-overlapping delete must not be in the immediate set")
	}
	if _, ok := deferred["overlap"]; !ok {
		t.Error("block-overlapping delete missing from the deferred set")
	}
	if _, ok := deferred["plain"]; ok {
		t.Error("non-overlapping delete must not be deferred")
	}
}

// A pull round with only deletions applies them via the concurrent deletion
// pass and still removes the files from disk by round end.
func TestPullerIterationDeletesConcurrently(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)

	writeFile(t, f.mtimefs, "gone", []byte("short lived"))
	must(t, f.scanSubdirs(t.Context(), nil))

	cur, ok := m.testCurrentFolderFile(f.ID, "gone")
	if !ok {
		t.Fatal("file missing from local index")
	}
	tomb := cur
	tomb.SetDeleted(device1.Short())
	tomb.Version = cur.Version.Update(device1.Short())
	must(t, m.Index(conn, &protocol.Index{Folder: f.ID, Files: []protocol.FileInfo{tomb}}))

	if _, err := f.pullerIteration(context.Background(), make(chan string)); err != nil {
		t.Fatal(err)
	}

	if _, err := f.mtimefs.Lstat("gone"); err == nil {
		t.Error("deleted file still exists on disk")
	}
}
