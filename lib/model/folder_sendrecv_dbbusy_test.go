// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// patch58BusyDB fails exactly one real Update boundary, before forwarding any
// writes. It models SQLITE_BUSY's atomic no-commit outcome, not SQLite locking
// or its retry timing. All reads and subsequent writes use the actual SQLite DB.
type patch58BusyDB struct {
	db.DB
	busy  error
	calls int
}

func (d *patch58BusyDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	d.calls++
	if d.busy != nil {
		err := d.busy
		d.busy = nil
		return err
	}
	return d.DB.Update(folder, device, files, opts...)
}

// TestPullDBBusyRescanCreatesFalseConflict pins the BUG: a failed pull index
// commit turns Syncthing's own installed content into receiver-authored work.
// Flip these bad-outcome expectations when pull failure recovery is fixed.
// The stopped model serializes complete pull batches and later scans, rather
// than inserting a scanner inside a successful pull's uncommitted batch gap.
func TestPullDBBusyRescanCreatesFalseConflict(t *testing.T) {
	for _, matching := range []bool{false, true} {
		t.Run(fmt.Sprintf("previous_hash_matches_S1=%t", matching), func(t *testing.T) {
			t.Setenv("STDRIPMODE", "0")
			t.Setenv("STMETABATCH", "0")
			w, cfg := newDefaultCfgWrapper(t)
			m := setupModel(t, w)
			m.cancel()
			<-m.stopped
			runner, _ := m.folderRunners.Get(cfg.ID)
			f := runner.(*sendReceiveFolder)
			f.MaxConflicts = -1
			const name = ".hermes/gateway_state.json"
			must(t, f.mtimefs.MkdirAll(".hermes", 0o755))
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
				if len(jobs) != 1 {
					t.Fatal("finish did not emit exactly one DB job")
				}
				close(jobs)
				return jobs
			}
			s0 := makeInfo(s0Bytes, 0)
			changed, err := f.dbUpdaterRoutine(finish(s0, protocol.FileInfo{}, false, s0Bytes))
			must(t, err)
			if changed != 1 || !current(name).Version.Equal(s0.Version) {
				t.Fatal("S0 setup failed")
			}
			original := f.db
			busy := errors.New("SQLITE_BUSY: database is locked")
			boundary := &patch58BusyDB{DB: original, busy: busy}
			f.db = boundary
			defer func() { f.db = original }()
			s1 := makeInfo(s1Bytes, 1)
			s1.PreviousBlocksHash = s0.BlocksHash
			changed, err = f.dbUpdaterRoutine(finish(s1, current(name), true, s1Bytes))
			if changed != 1 || !errors.Is(err, busy) || boundary.calls != 1 || boundary.busy != nil {
				t.Fatalf("real updater did not return one-shot DB failure: changed=%d calls=%d err=%v", changed, boundary.calls, err)
			}
			stale := current(name)
			if !bytes.Equal(read(name), s1Bytes) || !stale.Version.Equal(s0.Version) || stale.ModifiedBy != sender || !bytes.Equal(stale.BlocksHash, s0.BlocksHash) {
				t.Fatal("failed batch did not leave disk S1 and index S0")
			}
			t.Log("a: real performFinish installed S1; real dbUpdaterRoutine returned SQLITE_BUSY; index remains S0")

			// No further receiver writes, timestamp changes, or mapping manipulation.
			must(t, f.scanSubdirs(t.Context(), []string{name}))
			cur := current(name)
			if cur.Version.Counter(receiver) == 0 || cur.Version.Counter(sender) != s0.Version.Counter(sender) || len(cur.Version.Counters) != 2 || cur.ModifiedBy != receiver || !bytes.Equal(cur.BlocksHash, s1.BlocksHash) || !bytes.Equal(cur.PreviousBlocksHash, s0.BlocksHash) || !bytes.Equal(read(name), s1Bytes) || !cur.ModTime().Equal(s1.ModTime()) {
				t.Fatalf("unexpected rescan result: %+v", cur)
			}
			t.Log("b: real rescan attributed byte-identical S1 to receiver, inheriting indexed S0 sender counter (not S1)")
			s2 := makeInfo([]byte("{\"state\":\"S2 never received\"}\n"), 2)
			s3 := makeInfo(s3Bytes, 3)
			s3.PreviousBlocksHash = s2.BlocksHash
			if matching {
				s3.PreviousBlocksHash = s1.BlocksHash
			}
			if s3.InConflictWith(cur) != !matching {
				t.Fatal("unexpected conflict predicate")
			}
			changed, err = f.dbUpdaterRoutine(finish(s3, cur, true, s3Bytes))
			must(t, err)
			if changed != 1 || !bytes.Equal(read(name), s3Bytes) || !current(name).Version.Equal(s3.Version) {
				t.Fatal("S3 did not finish/index unchanged")
			}
			conflicts := existingConflicts(name, f.mtimefs)
			if matching {
				if len(conflicts) != 0 || len(scans) != 0 {
					t.Fatalf("control created conflict/rescan: %v", conflicts)
				}
				t.Log("d: matching S1 predecessor completed without conflict")
				return
			}
			if len(conflicts) != 1 || !bytes.Equal(read(conflicts[0]), s1Bytes) {
				t.Fatalf("expected S1 false conflict: %v", conflicts)
			}
			if len(scans) != 1 || <-scans != conflicts[0] {
				t.Fatal("conflict not scheduled for real scan")
			}
			must(t, f.scanSubdirs(t.Context(), conflicts))
			copyInfo := current(conflicts[0])
			if copyInfo.Name == name || copyInfo.IsDeleted() || copyInfo.IsInvalid() || copyInfo.ModifiedBy != receiver || copyInfo.Version.Counter(receiver) == 0 || copyInfo.Version.Counter(sender) != 0 || len(copyInfo.Version.Counters) != 1 || len(copyInfo.PreviousBlocksHash) != 0 || !bytes.Equal(copyInfo.BlocksHash, s1.BlocksHash) || copyInfo.Size != s1.Size {
				t.Fatalf("conflict not indexed as new independent receiver file: %+v", copyInfo)
			}
			if !current(name).Version.Equal(s3.Version) {
				t.Fatal("conflict scan altered original index")
			}
			t.Log("c,e: real finish made S1 conflict; real scan indexed independent receiver-only FileInfo")
		})
	}
}
