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

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// failOnceDB fails exactly one Update before forwarding any writes, modelling
// SQLITE_BUSY's atomic no-commit outcome. All other reads and writes use the
// real SQLite database.
type failOnceDB struct {
	db.DB
	err   error
	calls int
}

func (d *failOnceDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	d.calls++
	if d.err != nil {
		err := d.err
		d.err = nil
		return err
	}
	return d.DB.Update(folder, device, files, opts...)
}

// TestFailedPullCommitKeepsDiskAndIndexAgreed asserts the behaviour a fix must
// give and fails on upstream. A pulled file is renamed into place
// (performFinish) before its index entry is committed (dbUpdaterRoutine, up
// to 2 s later); if that commit fails, upstream discards the error from
// updateLocalsFromPulling and the batch is lost. Disk then holds the sender's
// S1 while the index says S0, the next scan records S1 as a local edit by the
// receiver, and the sender's S3 (based on an S2 the receiver never indexed)
// turns the receiver's own copy of S1 into a conflict file. Nothing but
// Syncthing ever wrote here. One failed DB.Update models SQLITE_BUSY's atomic
// no-commit outcome; everything else is the real puller, scanner and SQLite.
func TestFailedPullCommitKeepsDiskAndIndexAgreed(t *testing.T) {
	w, cfg := newDefaultCfgWrapper(t)
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	runner, _ := m.folderRunners.Get(cfg.ID)
	f := runner.(*sendReceiveFolder)
	f.MaxConflicts = -1
	const name = "state.json"
	sender, receiver := device1.Short(), f.shortID
	s0Bytes := []byte("{\"state\":\"S0\"}\n")
	s1Bytes := []byte("{\"state\":\"S1 installed by Syncthing\"}\n")
	s3Bytes := []byte("{\"state\":\"S3 newest sender state\"}\n")
	makeInfo := func(data []byte, n uint64) protocol.FileInfo {
		t.Helper()
		blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(data), protocol.MinBlockSize, int64(len(data)), nil)
		must(t, err)
		counter := uint64(1793916000) + n*120
		return protocol.FileInfo{Name: name, Type: protocol.FileInfoTypeFile, Size: int64(len(data)), Permissions: 0o640,
			ModifiedS: int64(counter), ModifiedBy: sender, Version: protocol.Vector{Counters: []protocol.Counter{{ID: sender, Value: counter}}},
			Blocks: blocks, BlocksHash: protocol.BlocksHash(blocks), RawBlockSize: protocol.MinBlockSize}
	}
	current := func(path string) protocol.FileInfo {
		t.Helper()
		fi, ok, err := f.db.GetDeviceFile(cfg.ID, protocol.LocalDeviceID, path)
		must(t, err)
		if !ok {
			t.Fatalf("missing local index %q", path)
		}
		return fi
	}
	read := func(path string) []byte {
		t.Helper()
		fd, err := f.mtimefs.Open(path)
		must(t, err)
		data, err := io.ReadAll(fd)
		must(t, err)
		must(t, fd.Close())
		return data
	}
	scans := make(chan string, 4)
	finish := func(fi, cur protocol.FileInfo, hasCur bool, data []byte) chan dbUpdateJob {
		t.Helper()
		jobs := make(chan dbUpdateJob, 1)
		temp := fs.TempName(name)
		writeFile(t, f.mtimefs, temp, data)
		must(t, f.performFinish(fi, cur, hasCur, temp, jobs, scans))
		close(jobs)
		return jobs
	}

	// S0 pulled and indexed normally.
	s0 := makeInfo(s0Bytes, 0)
	f.dbUpdaterRoutine(finish(s0, protocol.FileInfo{}, false, s0Bytes))
	if !current(name).Version.Equal(s0.Version) {
		t.Fatal("S0 setup failed")
	}

	// S1 pulled; its index commit fails once (database is locked).
	original := f.db
	boundary := &failOnceDB{DB: original, err: errors.New("database is locked")}
	f.db = boundary
	defer func() { f.db = original }()
	s1 := makeInfo(s1Bytes, 1)
	s1.PreviousBlocksHash = s0.BlocksHash
	f.dbUpdaterRoutine(finish(s1, current(name), true, s1Bytes))
	if boundary.calls < 1 {
		t.Fatal("fixture: the pull never reached the failing commit")
	}
	indexed := current(name)
	if !bytes.Equal(protocol.BlocksHash(mustBlocks(t, read(name))), indexed.BlocksHash) {
		t.Errorf("1: a failed pull commit left disk and index apart: disk holds %q, the index still describes version %v", read(name), indexed.Version)
	}

	// A normal scan: no file was touched by anyone but Syncthing.
	must(t, f.scanSubdirs(t.Context(), []string{name}))
	cur := current(name)
	if cur.Version.Counter(receiver) != 0 || cur.ModifiedBy == receiver {
		t.Errorf("2: the scan recorded content Syncthing installed as a local edit by the receiver: version %v, modifiedBy %v", cur.Version, cur.ModifiedBy)
	}

	// The sender's next version, based on an S2 the receiver never indexed.
	s2 := makeInfo([]byte("{\"state\":\"S2 never received\"}\n"), 2)
	s3 := makeInfo(s3Bytes, 3)
	s3.PreviousBlocksHash = s2.BlocksHash
	f.dbUpdaterRoutine(finish(s3, cur, true, s3Bytes))
	if conflicts := existingConflicts(name, f.mtimefs); len(conflicts) != 0 {
		t.Errorf("3: false conflict %v holds content Syncthing itself installed", conflicts)
	}
}

func mustBlocks(t *testing.T, data []byte) []protocol.BlockInfo {
	t.Helper()
	blocks, err := scanner.Blocks(t.Context(), bytes.NewReader(data), protocol.MinBlockSize, int64(len(data)), nil)
	must(t, err)
	return blocks
}
