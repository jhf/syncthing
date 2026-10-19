// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
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
	fileDeletions, _, deferred, _, err := f.processNeeded(t.Context(), dbUpdateChan, copyChan, scanChan)
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

// A deletion with no block overlap must be applied while a pull in the same
// round is still in flight, not after the pipeline drains. The fake remote
// refuses to serve the pulled block until the deleted file is gone from
// disk, so a round that runs deletions after the drain fails to pull.
func TestDeletionsRunConcurrentWithPulls(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	conn := addFakeConn(m, device1, f.ID)

	writeFile(t, f.mtimefs, "victim", []byte("delete me, I share nothing"))
	must(t, f.scanSubdirs(t.Context(), nil))

	pulledContent := []byte("fresh content only the remote has")
	blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(pulledContent), protocol.MinBlockSize, int64(len(pulledContent)), nil)
	must(t, err)
	pulled := protocol.FileInfo{
		Name:         "blocked",
		Type:         protocol.FileInfoTypeFile,
		Size:         int64(len(pulledContent)),
		RawBlockSize: int32(protocol.MinBlockSize),
		Blocks:       blocks,
	}
	pulled.Version = pulled.Version.Update(device1.Short())
	conn.fileData = map[string][]byte{"blocked": pulledContent}
	conn.RequestCalls(func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		if req.Name == "blocked" {
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := f.mtimefs.Lstat("victim"); fs.IsNotExist(err) {
					break
				}
				if time.Now().After(deadline) {
					return nil, errors.New("victim still present: deletions did not run concurrently with the pull")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		return conn.fileData[req.Name], nil
	})

	cur, ok := m.testCurrentFolderFile(f.ID, "victim")
	if !ok {
		t.Fatal("victim missing from local index")
	}
	tomb := cur
	tomb.SetDeleted(device1.Short())
	tomb.Version = cur.Version.Update(device1.Short())
	must(t, m.Index(conn, &protocol.Index{Folder: f.ID, Files: []protocol.FileInfo{tomb, pulled}}))

	if _, err := f.pullerIteration(t.Context(), make(chan string)); err != nil {
		t.Fatal(err)
	}
	for name, e := range f.tempPullErrors {
		t.Logf("pull error: %s: %s", name, e)
	}
	for _, fe := range f.pullErrors {
		t.Logf("pull error: %s: %s", fe.Path, fe.Err)
	}

	if _, err := f.mtimefs.Lstat("victim"); !fs.IsNotExist(err) {
		t.Error("deleted file still exists on disk")
	}
	if fd, err := f.mtimefs.Open("blocked"); err != nil {
		t.Error("pulled file missing:", err)
	} else {
		got := make([]byte, len(pulledContent))
		if _, err := fd.Read(got); err != nil {
			t.Fatal(err)
		}
		fd.Close()
		if !bytes.Equal(got, pulledContent) {
			t.Error("pulled content mismatch")
		}
	}
}
