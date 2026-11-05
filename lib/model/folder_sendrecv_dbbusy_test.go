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
	"path"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// patch58BusyDB fails real Update boundaries atomically without writing, then
// forwards to SQLite. It models a failed write, not actual lock timing or a
// driver-specific error type. Negative failures means fail every attempt.
type patch58BusyDB struct {
	db.DB
	fail     error
	failures int
	calls    int
}

func TestPullDBBusyExhaustionDrains(t *testing.T) {
	shrinkLockBackoff(t)
	_, f := setupSendReceiveFolder(t)
	f.churn = churnTestTracker()
	t.Cleanup(f.retry.clearAll)
	f.retry.failure("held", "remote_no_such_file", "block_request", "fixture hold")
	before, err := f.GetLastFile()
	must(t, err)
	busy := errors.New("update: database table is locked")
	original := f.db
	boundary := &patch58BusyDB{DB: original, fail: busy, failures: -1}
	f.db = boundary
	defer func() { f.db = original }()
	jobs := make(chan dbUpdateJob)
	type result struct {
		changed int
		err     error
	}
	done := make(chan result, 1)
	go func() {
		n, err := f.dbUpdaterRoutine(jobs)
		done <- result{n, err}
	}()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	func() {
		defer close(jobs)
		for range dbUpdateBatchSizeFiles + 10 {
			select {
			case jobs <- dbUpdateJob{protocol.FileInfo{Name: "held", Version: protocol.Vector{}.Update(1)}, dbUpdateHandleFile}:
			case <-deadline.C:
				t.Fatal("exhausted updater stopped draining producers")
			}
		}
	}()
	select {
	case got := <-done:
		if got.changed != dbUpdateBatchSizeFiles || !errors.Is(got.err, busy) || boundary.calls != 1+len(indexLockBackoff) {
			t.Fatalf("exhaustion: changed=%d calls=%d err=%v", got.changed, boundary.calls, got.err)
		}
	case <-deadline.C:
		t.Fatal("exhausted updater did not return after draining")
	}
	if got := f.churn.total.values[churnIndex("db_update.lock_retry_exhausted")]; got != 1 {
		t.Fatalf("exhausted counter=%d, want one batch", got)
	}
	after, err := f.GetLastFile()
	must(t, err)
	if after != before || len(f.retry.entries) != 1 {
		t.Fatal("exhaustion reported success or cleared uncommitted retry state")
	}
	_, ok, err := original.GetDeviceFile(f.folderID, protocol.LocalDeviceID, "held")
	must(t, err)
	if ok {
		t.Fatal("failed full batch reached local index")
	}
}

func (d *patch58BusyDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	d.calls++
	if d.failures != 0 {
		d.failures--
		return d.fail
	}
	return d.DB.Update(folder, device, files, opts...)
}

// TestPullDBBusyRescanCreatesFalseConflict guards the former transient-lock bug:
// retry must index Syncthing's own S1 before a later scan, with no receiver
// authorship or false conflict on S3. Explicit exhausted-lock and non-lock
// controls retain today's terminal failure path rather than pretending it heals.
// The stopped model serializes complete pull batches and later real scans.
// User-confirmed invariant (2026-11-05 22:25 UTC): NOTHING but Syncthing writes
// to the synced folder on yarlung. Fixtures name the gateway incident (pulled
// 21:32:55, DB failed 21:33:51, receiver counter minted 22:12:57) and the
// 2026-11-03 lib.rs conflict. Payloads, IDs and vectors are synthetic, not a
// replay of incident timing. Genuine edits on other peers remain scanner work.
func TestPullDBBusyRescanCreatesFalseConflict(t *testing.T) {
	shrinkLockBackoff(t)
	busy := errors.New("SQLITE_BUSY: database is locked")
	structural := errors.New("injected non-lock database failure")
	for _, mode := range []struct {
		name     string
		err      error
		failures int
		terminal bool
	}{
		{"transient", busy, 1, false},
		{"exhausted", busy, -1, true},
		{"non_lock", structural, -1, true},
	} {
		for _, name := range []string{".hermes/gateway_state.json", "ssb/jsonb_stats/src/lib.rs"} {
			for _, matching := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/previous_hash_matches_S1=%t", mode.name, name, matching), func(t *testing.T) {
					t.Setenv("STDRIPMODE", "0")
					t.Setenv("STMETABATCH", "0")
					w, cfg := newDefaultCfgWrapper(t)
					m := setupModel(t, w)
					m.cancel()
					<-m.stopped
					runner, _ := m.folderRunners.Get(cfg.ID)
					f := runner.(*sendReceiveFolder)
					f.MaxConflicts = -1
					f.churn = churnTestTracker()
					must(t, f.mtimefs.MkdirAll(path.Dir(name), 0o755))
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
					before, err := f.GetLastFile()
					must(t, err)
					// A failed batch must not clear an existing retry hold. Only a
					// confirmed commit may do so, even after transient contention.
					f.retry.beginWalk()
					f.retry.failure(name, "remote_no_such_file", "block_request", "fixture hold")
					original := f.db
					boundary := &patch58BusyDB{DB: original, fail: mode.err, failures: mode.failures}
					f.db = boundary
					defer func() { f.db = original }()
					s1 := makeInfo(s1Bytes, 1)
					s1.PreviousBlocksHash = s0.BlocksHash
					changed, err = f.dbUpdaterRoutine(finish(s1, current(name), true, s1Bytes))
					wantCalls := 2
					if mode.name == "exhausted" {
						wantCalls = 1 + len(indexLockBackoff)
					} else if mode.name == "non_lock" {
						wantCalls = 1
					}
					if changed != 1 || boundary.calls != wantCalls || (mode.terminal && !errors.Is(err, mode.err)) || (!mode.terminal && err != nil) {
						t.Fatalf("updater: changed=%d calls=%d want=%d err=%v", changed, boundary.calls, wantCalls, err)
					}
					wantExhausted := uint64(0)
					if mode.name == "exhausted" {
						wantExhausted = 1
					}
					if got := f.churn.total.values[churnIndex("db_update.lock_retry_exhausted")]; got != wantExhausted {
						t.Fatalf("exhausted batch counter=%d, want %d", got, wantExhausted)
					}
					after, err := f.GetLastFile()
					must(t, err)
					indexed := current(name)
					if !bytes.Equal(read(name), s1Bytes) {
						t.Fatal("disk does not contain installed S1")
					}
					if mode.terminal {
						if !indexed.Version.Equal(s0.Version) || indexed.ModifiedBy != sender || !bytes.Equal(indexed.BlocksHash, s0.BlocksHash) || after != before || len(f.retry.entries) != 1 {
							t.Fatal("terminal failure did not preserve stale index, hold and ReceivedFile state")
						}
						t.Log("terminal failure: disk S1, index S0, no ReceivedFile or cooldown clear")
					} else {
						if !indexed.Version.Equal(s1.Version) || indexed.ModifiedBy != sender || !bytes.Equal(indexed.BlocksHash, s1.BlocksHash) || after.Filename != name || len(f.retry.entries) != 0 {
							t.Fatal("transient retry did not commit sender S1 and clear its hold")
						}
						t.Log("transient lock: same batch retried and sender S1 committed")
					}
					boundary.failures = 0 // the later scan is after the failure/retry path

					// No receiver content or metadata edits and no fabricated scan result.
					must(t, f.scanSubdirs(t.Context(), []string{name}))
					cur := current(name)
					if !bytes.Equal(cur.BlocksHash, s1.BlocksHash) || !bytes.Equal(cur.PreviousBlocksHash, s0.BlocksHash) || !bytes.Equal(read(name), s1Bytes) || !cur.ModTime().Equal(s1.ModTime()) {
						t.Fatalf("rescan changed S1 content, timestamp or predecessor: %+v", cur)
					}
					if mode.terminal {
						if cur.Version.Counter(receiver) == 0 || cur.Version.Counter(sender) != s0.Version.Counter(sender) || len(cur.Version.Counters) != 2 || cur.ModifiedBy != receiver {
							t.Fatalf("terminal rescan no longer follows documented failure path: %+v", cur)
						}
					} else if !cur.Version.Equal(s1.Version) || cur.Version.Counter(receiver) != 0 || cur.ModifiedBy != sender {
						t.Fatalf("rescan minted receiver work despite successful retry: %+v", cur)
					}
					s2 := makeInfo([]byte("{\"state\":\"S2 never received\"}\n"), 2)
					s3 := makeInfo(s3Bytes, 3)
					s3.PreviousBlocksHash = s2.BlocksHash
					if matching {
						s3.PreviousBlocksHash = s1.BlocksHash
					}
					wantConflict := mode.terminal && !matching
					if s3.InConflictWith(cur) != wantConflict {
						t.Fatal("unexpected conflict predicate")
					}
					changed, err = f.dbUpdaterRoutine(finish(s3, cur, true, s3Bytes))
					must(t, err)
					if changed != 1 || !bytes.Equal(read(name), s3Bytes) || !current(name).Version.Equal(s3.Version) {
						t.Fatal("S3 did not finish/index unchanged")
					}
					conflicts := existingConflicts(name, f.mtimefs)
					if !wantConflict {
						if len(conflicts) != 0 || len(scans) != 0 {
							t.Fatalf("unexpected conflict/rescan: %v", conflicts)
						}
						t.Log("S3 installed without conflict or conflict scan")
						return
					}
					if len(conflicts) != 1 || !bytes.Equal(read(conflicts[0]), s1Bytes) {
						t.Fatalf("terminal control must retain documented S1 false conflict: %v", conflicts)
					}
					if len(scans) != 1 || <-scans != conflicts[0] {
						t.Fatal("conflict not scheduled for real scan")
					}
					must(t, f.scanSubdirs(t.Context(), conflicts))
					copyInfo := current(conflicts[0])
					if copyInfo.Name == name || copyInfo.IsDeleted() || copyInfo.IsInvalid() || copyInfo.ModifiedBy != receiver || copyInfo.Version.Counter(receiver) == 0 || copyInfo.Version.Counter(sender) != 0 || len(copyInfo.Version.Counters) != 1 || len(copyInfo.PreviousBlocksHash) != 0 || !bytes.Equal(copyInfo.BlocksHash, s1.BlocksHash) || copyInfo.Size != s1.Size {
						t.Fatalf("conflict not indexed as independent receiver file: %+v", copyInfo)
					}
					if !current(name).Version.Equal(s3.Version) {
						t.Fatal("conflict scan altered original index")
					}
				})
			}
		}
	}
}
