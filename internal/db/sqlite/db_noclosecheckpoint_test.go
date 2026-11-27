// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo

package sqlite

import (
	"os"
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

// Close leaves the WAL backlog in place instead of checkpointing it, and
// the next open replays it: nothing written before Close is lost. Teeth:
// with SQLite's default the last connection's close checkpoints the
// backlog and deletes the WAL, so the -wal file is gone after Close.
func TestCloseSkipsCheckpointAndReopenReplays(t *testing.T) {
	const folderID = "test"
	dir := t.TempDir()
	sdb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := closeObsFiles(300)
	if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}
	path := fdb.path
	before, _, ok := walBacklog(path)
	if !ok || before == 0 {
		t.Fatalf("fixture left no WAL backlog before close (backlog=%d ok=%v)", before, ok)
	}
	if err := sdb.Close(); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("WAL gone after close: the close checkpointed it (%v)", err)
	}
	if fi.Size() == 0 {
		t.Fatal("WAL empty after close: the close checkpointed it")
	}
	t.Logf("backlog before close %d frames, WAL after close %d bytes", before, fi.Size())

	sdb, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sdb.Close() })
	for _, f := range files {
		got, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, f.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || got.Sequence == 0 {
			t.Fatalf("%s missing after reopen: the WAL was not replayed", f.Name)
		}
	}
}
